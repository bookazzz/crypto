import pytest

from backend.models import Candle, Instrument, Settings, Signal
from backend.engine import Engine
from backend.portfolio_backtest import Cache, MINUTE, aggregate_minute, load_checkpoint, report, save_checkpoint


def test_minute_aggregation_only_after_complete_bucket():
    builder={}
    # First bucket starts before the requested period and is incomplete.
    for i in (2,3,4):
        assert aggregate_minute(builder,'BTCUSDT',5,Candle(i*MINUTE,100+i,110+i,90+i,100+i,1)) is None
    result=None
    for i in range(5,10):
        result=aggregate_minute(builder,'BTCUSDT',5,Candle(i*MINUTE,100+i,110+i,90+i,100+i,1))
    assert result==Candle(5*MINUTE,105,119,95,109,5,535.)


def test_missing_bars_after_listing_are_rejected(tmp_path):
    cache=Cache(tmp_path/'market.sqlite3')
    cache.save_segment('NEWUSDT',0,5*MINUTE,[Candle(i*MINUTE,1,2,.5,1) for i in (2,3,4)])
    coverage=cache.coverage('NEWUSDT',0,5*MINUTE)
    assert coverage['first']==2*MINUTE and not coverage['full_60_days']
    cache.save_segment('NEWUSDT',5*MINUTE,7*MINUTE,[Candle(6*MINUTE,1,2,.5,1)])
    with pytest.raises(RuntimeError,match='пропуски'):
        cache.coverage('NEWUSDT',0,7*MINUTE)
    cache.db.close()


def test_checkpoint_restores_shared_portfolio(tmp_path):
    store,_=load_checkpoint(tmp_path)
    engine=Engine(store,Settings())
    engine.instruments['BTCUSDT']=Instrument('BTCUSDT',.001,.001,5,.01)
    engine.cash=987
    buffers={('BTCUSDT',tf):[Candle(0,1,2,.5,1)] for tf in (5,15,60,240)}
    save_checkpoint(store,engine,tmp_path/'checkpoint.sqlite3',MINUTE,buffers,{'BTCUSDT':.001})
    engine.cash=100
    engine.persist()
    store.close()
    resumed,checkpoint=load_checkpoint(tmp_path)
    assert Engine(resumed).cash==987
    assert checkpoint['next_time']==MINUTE
    assert checkpoint['latest_funding']['BTCUSDT']==.001
    resumed.close()


def test_report_counts_unique_confirmed_breakouts_and_closed_wins(tmp_path):
    store,_=load_checkpoint(tmp_path)
    engine=Engine(store,Settings())
    accepted=Signal('BTCUSDT','5','double_bottom','long',MINUTE,0,100,98,110,1)
    accepted.status='closed_tp'
    rejected=Signal('BTCUSDT','15','double_top','short',MINUTE,0,100,102,90,1)
    rejected.status='rejected'
    rejected.reason='Достигнут лимит позиций'
    store.save({'settings':Settings().__dict__,'cash':1002,'positions':[],
                'trades':[],'fees':{},'paused':False,'day':'','day_equity':1000,
                'day_halted':False,'prices':{},'market_times':{},'peak':1002,
                'max_drawdown':0},[accepted.dict(),rejected.dict()])
    engine.trades=[{'id':accepted.id,'net':2.,'funding':0.}]
    engine.cash=1002
    manifest={'symbols':['BTCUSDT'],'captured_at':0,'start':0,'end':MINUTE,'days':1}
    result=report(store,engine,manifest,{'BTCUSDT':{'days':1}},tmp_path)
    assert len(result['patterns'])==13
    assert result['total']['signals']==2
    assert result['total']['opened']==result['total']['closed']==result['total']['wins']==1
    assert result['total']['rejected']==1
    assert result['rejection_reasons']['Достигнут лимит позиций']==1
    assert next(p for p in result['patterns'] if p['pattern']=='double_bottom')['win_rate']==1
    store.close()
