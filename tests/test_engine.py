import pytest
from backend.models import Signal, Instrument, Settings, Candle
from backend.engine import Engine
from backend.storage import Store

T=1800000000000


def make_engine(path=':memory:',settings=None):
    e=Engine(Store(str(path)),settings)
    e.instruments['BTCUSDT']=Instrument('BTCUSDT',.001,.001,5,.01)
    e.set_fee('BTCUSDT',.0006,T)
    e.connected=True
    return e


def test_saved_market_universe_expands_to_forty_on_restart():
    store=Store(':memory:')
    engine=Engine(store,Settings(top_n=20))
    engine.persist()
    restored=Engine(store)
    assert restored.settings.top_n==40


def signal(side='long',time=T,pattern='ascending_triangle',start=None,symbol='BTCUSDT'):
    return Signal(symbol,'15',pattern,side,time,time-30000000 if start is None else start,
                  100,98 if side=='long' else 102,110 if side=='long' else 90,1)


def open_position(e,s=None,time=T):
    s=s or signal()
    e.tick(s.symbol,100,time)
    e.process([s],{s.symbol:{'price':100,'time':time}},time,{s.symbol:.0001})
    return s


@pytest.mark.parametrize('side',['long','short'])
def test_risk_rounding_fees_and_net(side):
    e=make_engine()
    s=open_position(e,signal(side))
    assert s.status=='open'
    assert s.risk<=5
    assert s.rr>=2
    assert s.entry*s.qty+e.positions[0]['entry_fee']<=1000
    p=e.positions[0].copy()
    e.tick('BTCUSDT',s.target,T+1000)
    t=e.trades[0]
    assert t['net']==pytest.approx((t['exit']-t['entry'])*t['qty']*p['sign']-t['entry_fee']-t['exit_fee'])
    assert e.cash-1000==pytest.approx(t['net'])
    assert e.store.signal(s.id)['status']=='closed_tp'


def test_account_fee_mode_rejects_a_saved_standard_rate():
    e=make_engine()
    e.fee_mode='account'
    e.fees['BTCUSDT']['source']='bybit_vip0_standard'
    assert e.quote_plan(signal(),100,T)== 'Нет актуальной персональной комиссии аккаунта'


def test_duplicate_and_restart(tmp_path):
    path=tmp_path/'db.sqlite'
    e=make_engine(path)
    s=open_position(e)
    assert e.process([signal()],{'BTCUSDT':{'price':100,'time':T}},T)==[]
    e.persist()
    e.store.close()
    again=make_engine(path)
    assert len(again.positions)==1
    assert again.cash==pytest.approx(1000-s.entry*s.qty*.0006)
    assert again.process([signal()],{'BTCUSDT':{'price':100,'time':T}},T)==[]


def test_funding_long_short_and_idempotence():
    for side in ['long','short']:
        e=make_engine()
        open_position(e,signal(side))
        p=e.positions[0]
        initial=e.cash
        expected=-p['sign']*p['qty']*100*.001
        e.funding_payment('BTCUSDT',.001,T+1000,100)
        e.funding_payment('BTCUSDT',.001,T+1000,100)
        assert e.cash==pytest.approx(initial+expected)
        e.tick('BTCUSDT',p['target'],T+2000)
        assert e.trades[0]['funding']==pytest.approx(expected)
        assert e.cash-1000==pytest.approx(e.trades[0]['net'])


def test_recovery_both_levels_stop_first_and_gap():
    e=make_engine()
    open_position(e)
    e.bar('BTCUSDT',Candle(T,100,120,90,105),T+59999)
    assert e.trades[0]['reason']=='stop'
    assert e.trades[0]['recovered']
    assert e.store.signals()[0]['status']=='closed_sl'
    e=make_engine()
    open_position(e)
    e.bar('BTCUSDT',Candle(T+60000,90,100,89,99),T+119999)
    assert e.trades[0]['exit']<90


@pytest.mark.parametrize('case,reason',[('missing_fee','комиссии'),('old_fee','комиссии'),('stale','котировка'),('disconnected','отключен'),('paused','приостановлены'),('unprofitable','порога'),('capital','капитала'),('quote_before','до подтверждения')])
def test_entry_blockers(case,reason):
    e=make_engine()
    s=signal()
    time=T
    qt=T
    if case=='missing_fee':e.fees={}
    if case=='old_fee':e.fees['BTCUSDT']['time']=T-86400001
    if case=='stale':qt=T-31000
    if case=='disconnected':e.connected=False
    if case=='paused':e.paused=True
    if case=='unprofitable':s.target=101
    if case=='capital':e.cash=.1
    if case=='quote_before':qt=T-1
    e.process([s],{'BTCUSDT':{'price':100,'time':qt}},time)
    assert s.status=='rejected'
    assert reason in s.reason
    assert not e.positions


def test_conflicts_and_expense_share_priority():
    e=make_engine()
    a,b=signal(),signal('short',pattern='double_top')
    e.process([a,b],{'BTCUSDT':{'price':100,'time':T}},T)
    assert all(s.reason=='Конфликт направлений' for s in (a,b))
    e=make_engine()
    a,b=signal(),signal(pattern='double_bottom')
    b.target=120
    e.process([a,b],{'BTCUSDT':{'price':100,'time':T}},T)
    assert a.status=='open' and b.status=='rejected'


def test_simultaneous_pairs_ranked_by_expense_share_globally_and_stably():
    def execute(order):
        e=make_engine(settings=Settings(max_positions=1))
        for symbol in ('ETHUSDT','SOLUSDT'):
            e.instruments[symbol]=Instrument(symbol,.001,.001,5,.01)
            e.set_fee(symbol,.0006,T)
            e.tick(symbol,100,T)
        signals={symbol:signal(symbol=symbol) for symbol in ('ETHUSDT','SOLUSDT')}
        signals['SOLUSDT'].target=120
        quotes={symbol:{'price':100,'time':T} for symbol in signals}
        e.process([signals[s] for s in order],quotes,T)
        return e.positions[0]['symbol'],signals
    first,signals=execute(['ETHUSDT','SOLUSDT'])
    second,_=execute(['SOLUSDT','ETHUSDT'])
    assert first==second=='ETHUSDT'
    assert signals['SOLUSDT'].status=='rejected'
    assert 'лимит позиций' in signals['SOLUSDT'].reason


def test_simultaneous_equal_rr_uses_symbol_tie_break():
    e=make_engine(settings=Settings(max_positions=1))
    e.instruments['ETHUSDT']=Instrument('ETHUSDT',.001,.001,5,.01)
    e.set_fee('ETHUSDT',.0006,T)
    e.tick('BTCUSDT',100,T)
    e.tick('ETHUSDT',100,T)
    eth=signal(symbol='ETHUSDT')
    btc=signal(symbol='BTCUSDT')
    quotes={s:{'price':100,'time':T} for s in ('ETHUSDT','BTCUSDT')}
    e.process([eth,btc],quotes,T)
    assert btc.status=='open' and eth.status=='rejected'


def test_position_limit_and_one_per_symbol():
    e=make_engine(settings=Settings(max_positions=1))
    open_position(e)
    other=signal(start=T-40000000)
    e.process([other],{'BTCUSDT':{'price':100,'time':T}},T)
    assert 'уже есть' in other.reason
    e.instruments['ETHUSDT']=Instrument('ETHUSDT',.001,.001,5,.01)
    e.set_fee('ETHUSDT',.0006,T)
    s=signal(symbol='ETHUSDT')
    e.process([s],{'ETHUSDT':{'price':100,'time':T}},T)
    assert 'лимит позиций' in s.reason


def test_daily_halt_latched_until_next_moscow_day():
    e=make_engine()
    e.daily(T)
    e.cash=979
    e.daily(T+1000)
    assert e.day_halted
    e.cash=1005
    e.daily(T+2000)
    assert e.day_halted
    e.daily(T+86400000)
    assert not e.day_halted


def test_settings_persist(tmp_path):
    e=make_engine(tmp_path/'db.sqlite')
    e.settings.risk_fraction=.01
    e.paused=True
    e.persist()
    e2=Engine(Store(str(tmp_path/'db.sqlite')))
    assert e2.settings.risk_fraction==.01 and e2.paused


def test_duplicates_in_same_batch_do_not_overwrite_open_status():
    e=make_engine()
    s=signal()
    e.process([s,signal()],{'BTCUSDT':{'price':100,'time':T}},T)
    assert len(e.positions)==1
    assert e.store.signal(s.id)['status']=='open'


def test_stale_open_position_prices_block_other_entries():
    e=make_engine()
    open_position(e)
    e.instruments['ETHUSDT']=Instrument('ETHUSDT',.001,.001,5,.01)
    e.set_fee('ETHUSDT',.0006,T)
    s=signal(symbol='ETHUSDT',time=T+31000)
    e.process([s],{'ETHUSDT':{'price':100,'time':T+31000}},T+31000)
    assert s.status=='rejected' and 'открытой позиции' in s.reason


def test_existing_settings_gain_five_minute_timeframe(tmp_path):
    from dataclasses import asdict
    path=tmp_path/'old-settings.sqlite'
    old=Settings(timeframes=['15','60','240'])
    store=Store(str(path))
    store.save({'cash':1000,'positions':[],'trades':[],'fees':{},'paused':False,'day':'',
        'day_equity':1000,'day_halted':False,'prices':{},'market_times':{},'peak':1000,
        'max_drawdown':0,'settings':asdict(old)})
    migrated=Engine(Store(str(path)))
    assert migrated.settings.timeframes==['5','15','60','240']
