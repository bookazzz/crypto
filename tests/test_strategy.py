from backend.models import Candle, Signal
from backend.strategy import VARIANTS, clean_rr, gate, trend_filter, volume_filter, prepare_signal, Variant
from backend.research import warm_from_cache, enrich_signal_report
import json
from backend.portfolio_backtest import Cache, MINUTE
from backend.engine import Engine
from backend.models import Instrument, Settings
from backend.research import simulate
import asyncio


def bars(closes, volumes=None):
    volumes=volumes or [10.]*len(closes)
    return [Candle(i*86_400_000,x,x+1,x-1,x,v) for i,(x,v) in enumerate(zip(closes,volumes))]


def test_trend_needs_confirmed_history_and_uses_only_prefix():
    short=bars([100+i for i in range(260)])
    assert trend_filter(short[:200],'long')[0] is None
    assert trend_filter(short,'long')[0] is True
    assert trend_filter(short,'short')[0] is False
    falling=bars([400-i for i in range(260)])
    assert trend_filter(falling,'short')[0] is True


def test_volume_uses_only_previous_twenty_closed_bars():
    candles=bars([100.]*21, [10.]*20+[15.])
    ok,details=volume_filter(candles)
    assert ok is True and details['multiple']==1.5
    assert volume_filter(candles[:20])[0] is None


def test_cost_adjusted_rr_and_fixed_variant_space():
    rr,risk,reward=clean_rr(100,98,104,'long',.00055,.0005)
    assert risk>0 and reward>0 and rr>0
    assert len(VARIANTS)==12
    assert len({v.id for v in VARIANTS})==12


def test_gate_rejects_unprofitable_or_high_drawdown_results():
    passed,reason=gate({'closed':120,'net':10,'profit_factor':1.3,'max_drawdown':.09},5)
    assert passed and 'допущена' in reason
    assert gate({'closed':99,'net':10,'profit_factor':2,'max_drawdown':.01},5)[1]=='недостаточно данных'
    assert not gate({'closed':120,'net':10,'profit_factor':1.3,'max_drawdown':.11},5)[0]


def test_target_is_reduced_to_required_net_rr_but_cannot_exceed_projection():
    signal=Signal('BTCUSDT','5','ascending_triangle','long',5*MINUTE,0,100,98,120,1)
    ok,reason,details=prepare_signal(signal,Variant('immediate',1.5,None),100,.00055,.0005)
    assert ok,reason
    assert 100<signal.target<=120 and details['net_rr']>=1.5


def test_warm_cache_uses_only_closed_candles_before_split(tmp_path):
    cache=Cache(tmp_path/'market.sqlite3')
    rows=[Candle(i*MINUTE,100+i,101+i,99+i,100+i,1) for i in range(30)]
    cache.save_segment('BTCUSDT',0,30*MINUTE,rows)
    warm=warm_from_cache(cache,'BTCUSDT',5,30*MINUTE)
    assert len(warm)==6 and all(c.time+5*MINUTE<=30*MINUTE for c in warm)
    assert warm[0].open==100 and warm[-1].close==129
    cache.db.close()


def test_research_portfolio_replays_entry_and_target_with_shared_engine(tmp_path):
    cache=Cache(tmp_path/'market.sqlite3')
    data=[]
    for i in range(20):
        high=121 if i==16 else 101
        data.append(Candle(i*MINUTE,100,high,99,100,100))
    cache.save_segment('BTCUSDT',0,20*MINUTE,data)
    signal=Signal('BTCUSDT','15','ascending_triangle','long',15*MINUTE,0,100,98,120,1,
                  filter_results={'auto_eligible':True,'volume_pass':True})
    manifest={'symbols':['BTCUSDT'],'settings':Settings().__dict__,
        'instruments':{'BTCUSDT':Instrument('BTCUSDT',.001,.001,5,.01).__dict__}}
    class NoFundingBybit: pass
    result,trades,curve=asyncio.run(simulate(cache,NoFundingBybit(),manifest,{},(0,20*MINUTE),
        Variant('immediate',1.,None),.0005,[signal]))
    assert result['closed']==1 and trades[0]['reason']=='target'
    assert result['net']>0 and curve
    cache.db.close()


def test_variant_simulations_do_not_mutate_or_depend_on_shared_signal_order(tmp_path):
    cache=Cache(tmp_path/'variant-cache.sqlite3')
    rows=[Candle(i*MINUTE,100,121 if i==6 else 101,99,100,100) for i in range(20)]
    cache.save_segment('BTCUSDT',0,20*MINUTE,rows)
    source=Signal('BTCUSDT','5','ascending_triangle','long',5*MINUTE,0,100,98,120,1,
        filter_results={'trend_pass':True,'volume_pass':True})
    manifest={'symbols':['BTCUSDT'],'settings':Settings().__dict__,
        'instruments':{'BTCUSDT':Instrument('BTCUSDT',.001,.001,5,.01).__dict__}}
    class NoFundingBybit: pass
    variants=[Variant('immediate',1.,None),Variant('immediate',1.5,None)]
    outcomes={}
    for variant in variants:
        result,_,_=asyncio.run(simulate(cache,NoFundingBybit(),manifest,{},(0,20*MINUTE),variant,.0005,[source]))
        outcomes[variant.id]=result['net']
    reversed_outcomes={}
    for variant in reversed(variants):
        result,_,_=asyncio.run(simulate(cache,NoFundingBybit(),manifest,{},(0,20*MINUTE),variant,.0005,[source]))
        reversed_outcomes[variant.id]=result['net']
    assert source.target==120
    assert outcomes==reversed_outcomes
    cache.db.close()


def test_pending_retest_restores_and_triggers_once_after_restart(tmp_path):
    from backend.service import Scanner
    from backend.storage import Store
    store=Store(str(tmp_path/'state.sqlite3'))
    engine=Engine(store,Settings())
    signal=Signal('BTCUSDT','15','ascending_triangle','long',15*MINUTE,0,100,98,110,1,
        lines=[{'label':'support','points':[{'time':0,'value':99},{'time':15*MINUTE,'value':99.5}]},
               {'label':'resistance','points':[{'time':0,'value':100},{'time':15*MINUTE,'value':100}]}],
        status='pending_retest')
    engine.strategy_approved=True
    engine.strategy_pending=[signal.dict()|{'retest_bars':0,'retest_extreme':None}]
    engine.persist([signal])
    assert not store.has_signal(signal.id)
    restored=Engine(store,Settings())
    scanner=Scanner(restored,object())
    restored.strategy_approved=True
    scanner.active_variant=Variant('retest',1.,None)
    scanner.advance_retests('BTCUSDT','5',Candle(5*MINUTE,100,101,99.4,100.2,1))
    assert len(scanner.pending)==1 and scanner.pending[0].id==signal.id
    assert scanner.pending[0].stop==99.4-(1/3)*.2
    assert not restored.strategy_pending
    assert not store.has_signal(signal.id)
    store.close()


def test_research_report_includes_breakout_filter_counts(tmp_path):
    signal=Signal('BTCUSDT','15','ascending_triangle','long',15*MINUTE,0,100,98,110,1,
        filter_results={'trend_pass':False,'volume_pass':True,
                        'market_context':{'eligible':False,'scenario':'unconfirmed'}})
    (tmp_path/'summary.json').write_text(json.dumps({'periods':{'holdout':{'start':0,'end':20*MINUTE}},
        'variants':[]}),encoding='utf-8')
    (tmp_path/'features.json').write_text(json.dumps({'signals':[signal.dict()]}),encoding='utf-8')
    report=enrich_signal_report(tmp_path)
    assert report['holdout_signals']['confirmed']==1
    assert report['holdout_signals']['setup_rejected']==1
    rows=json.loads((tmp_path/'signals.json').read_text(encoding='utf-8'))
    assert rows[0]['status']=='rejected'
