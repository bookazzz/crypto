from backend.backtest import replay
from backend.models import Candle, Instrument, Settings
from tests.test_patterns import swings


def test_replay_warmup_next_open_and_expenses():
    c=swings('head_shoulders')
    start=c[-1].time+900000
    minutes=[Candle(start,96,97,95,96),Candle(start+60000,95,140,40,90)]
    ins=Instrument('BTCUSDT',.001,.001,5,.01)
    report=replay('BTCUSDT','15',c,minutes,ins,.0006,[(start+60000,.001,100)],Settings(min_rr=1))
    assert report['signals']
    assert report['trades_count']==1
    t=report['trades'][0]
    assert t['opened']==start
    assert t['entry']<96
    assert t['entry_fee']>0 and t['exit_fee']>0
    assert t['funding']>0  # short receives positive funding
    assert t['reason']=='stop'  # both extremes touched in the second minute
    assert report['net']==t['net'] or abs(report['net']-t['net'])<1e-9


def test_no_warmup_orders_and_no_future_signal():
    c=swings('double_top')
    start=c[-2].time+900000
    report=replay('BTCUSDT','15',c,[Candle(start,110,111,109,110)],Instrument('BTCUSDT',.001,.001,5,.01),.0006)
    assert report['signals']==[] and report['trades_count']==0
