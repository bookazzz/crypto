import pytest

from backend.models import Candle, Instrument, Settings
from backend.target_comparison import capped_target, simulate_exit

T = 1800000000000
INSTRUMENT = Instrument('BTCUSDT', .001, .001, 5, .01)


def original(side='long'):
    return {'id': 'fixture', 'symbol': 'BTCUSDT', 'side': side,
            'sign': 1 if side == 'long' else -1, 'entry': 100.,
            'stop': 98. if side == 'long' else 102.,
            'target': 110. if side == 'long' else 90., 'qty': 1.,
            'fee_rate': .00055, 'entry_fee': .055, 'opened': T,
            'timeframe': '5', 'pattern': 'symmetric_triangle'}


@pytest.mark.parametrize('side,target', [('long',102.),('short',98.)])
def test_closer_target_exits_before_original_stop_with_costs_and_funding(side, target):
    trade = original(side)
    minutes = [Candle(T,100,100.5,99.5,100),
               Candle(T+60000,100,102.5 if side=='long' else 100.5,
                      99.5 if side=='long' else 97.5,100),
               Candle(T+120000,100,103,97,100)]
    # The first payment is at entry and must not charge this new position.
    # The last payment is after the closer exit and must not be charged either.
    funding = [(T,.01,100), (T+60000,.001,100), (T+120000,.01,None)]
    actual = simulate_exit(trade,capped_target(trade,1.,INSTRUMENT),minutes,
                           funding,INSTRUMENT,Settings())
    assert actual['target']==target and actual['reason']=='target'
    assert actual['closed']==T+119999
    assert actual['entry']==trade['entry'] and actual['qty']==trade['qty'] and actual['stop']==trade['stop']
    assert actual['funding']==pytest.approx(-trade['sign']*.1)
    assert actual['net']==pytest.approx(actual['gross']-actual['entry_fee']-actual['exit_fee']+actual['funding'])


def test_same_minute_stop_and_closer_target_remains_stop_first():
    trade=original()
    actual=simulate_exit(trade,102.,[Candle(T,100,103,97,101)],[],INSTRUMENT,Settings())
    assert actual['reason']=='stop' and actual['net']<0


def test_cap_never_moves_target_farther_and_rounds_inward():
    trade=original()
    assert capped_target(trade,100.,INSTRUMENT)==trade['target']
    assert capped_target(trade,.3333,INSTRUMENT)==100.66
    trade=original('short')
    assert capped_target(trade,.3333,INSTRUMENT)==99.34


def test_unclosed_trade_is_valued_without_a_fabricated_exit_fee():
    trade=original()
    actual=simulate_exit(trade,102.,[Candle(T,100,101,99,100.5)],[],INSTRUMENT,Settings())
    assert actual['status']=='open' and actual['net'] is None
    assert actual['marked_net']==pytest.approx(.5-trade['entry_fee'])
    assert 'exit_fee' not in actual
