from unittest.mock import patch

import pytest

from backend.models import Candle
from backend.patterns import Pivot
from backend.scalp import assess_scalp_plan


NOW = 1_800_000_000_000


def inputs(side='long', *, target=102.0, stop_level=100.8, quote_age=0,
           book_age=0, fee=.0001, funding=0.0, next_funding=NOW + 3_600_000):
    sign = 1 if side == 'long' else -1
    candles = [Candle(NOW - 48 * 300_000 + i * 300_000,
                      100, 100.2, 99.8, 100) for i in range(48)]
    candles[-1] = Candle(candles[-1].time, 100, 101 if sign > 0 else 100,
                        99 if sign < 0 else 100, 101 if sign > 0 else 99)
    entry = 101.05 if sign > 0 else 98.95
    pivots = [
        Pivot(35, stop_level if sign > 0 else 200 - stop_level,
              'low' if sign > 0 else 'high'),
        Pivot(40, target if sign > 0 else 200 - target,
              'high' if sign > 0 else 'low'),
    ]
    book = {
        'time': NOW - book_age,
        'bids': {entry - .01: 10},
        'asks': {entry + .01: 10},
    }
    return {
        'setup': {'side': side, 'event_at': candles[0].time},
        'candles': candles,
        'boundary': 100,
        'price': entry,
        'quote_time': NOW - quote_age,
        'now': NOW,
        'book': book,
        'book_now': NOW,
        'fee': fee,
        'fee_time': NOW,
        'fee_source': 'standard',
        'fee_mode': 'standard',
        'funding_time': NOW,
        'slippage': .0001,
        'funding_rate': funding,
        'next_funding': next_funding,
        'tick_size': .01,
    }, pivots


@pytest.mark.parametrize('side', ['long', 'short'])
def test_scalp_plan_builds_nearby_levels_for_both_directions(side):
    values, local_pivots = inputs(side)
    with patch('backend.scalp.atr', return_value=.5), \
         patch('backend.scalp.pivots', return_value=local_pivots):
        plan = assess_scalp_plan(**values)
    assert plan['status'] == 'available'
    assert plan['net_rr'] >= 1.5
    assert plan['entry'] is not None and plan['stop'] is not None and plan['target'] is not None
    assert (plan['stop'] < plan['entry'] < plan['target']) if side == 'long' else (
        plan['target'] < plan['entry'] < plan['stop'])
    assert plan['costs_pct'] > 0


def test_scalp_plan_uses_confirmed_historical_pivots_and_ignores_open_future_bar():
    values, _ = inputs()
    candles = [Candle(NOW - 48 * 300_000 + i * 300_000,
                      100, 100.01, 99.99, 100) for i in range(48)]
    candles[38] = Candle(candles[38].time, 100, 100.01, 99.95, 100)
    candles[42] = Candle(candles[42].time, 100, 101.05, 99.99, 100)
    candles[-1] = Candle(candles[-1].time, 100, 100.3, 100, 100.2)
    values.update(candles=candles, boundary=100.1, price=100.22,
        quote_time=NOW, book={'time': NOW, 'bids': {100.21: 10}, 'asks': {100.23: 10}})
    # The final open candle contains an artificial future extreme. It must not
    # affect pivots or the target until its five-minute close.
    values['candles'] = candles + [Candle(NOW, 100.2, 150, 50, 120)]
    with patch('backend.scalp.atr', return_value=.5):
        plan = assess_scalp_plan(**values)
    assert plan['status'] == 'available'
    assert plan['stop_level'] == pytest.approx(99.95)
    assert plan['target_level'] == pytest.approx(101.05)


def test_scalp_plan_waits_for_a_closed_candle_after_setup():
    values, local_pivots = inputs()
    values['setup']['event_at'] = NOW
    with patch('backend.scalp.atr', return_value=.5), \
         patch('backend.scalp.pivots', return_value=local_pivots):
        plan = assess_scalp_plan(**values)
    assert plan['status'] == 'waiting_breakout'
    assert plan['preliminary'] is True
    assert plan['entry'] is not None and plan['stop'] is not None and plan['target'] is not None
    assert plan['net_rr'] is not None and plan['costs_pct'] is not None
    assert plan['horizon_minutes'] == [5, 30]


def test_scalp_plan_uses_immutable_formation_time_and_live_sloping_boundary():
    values, local_pivots = inputs()
    values['setup'].update(event_at=NOW, formed_at=values['candles'][0].time)
    values['current_boundary'] = 101.0
    values['price'] = 100.9
    values['quote_time'] = NOW
    with patch('backend.scalp.atr', return_value=.5), \
         patch('backend.scalp.pivots', return_value=local_pivots):
        plan = assess_scalp_plan(**values)
    assert plan['status'] == 'waiting_breakout'
    assert plan['preliminary'] is True
    assert 'вернулась' in plan['confirmation_reason']
    assert plan['entry'] is not None and plan['stop'] is not None and plan['target'] is not None


def test_scalp_plan_rejects_stale_latest_candle():
    values, local_pivots = inputs()
    values['now'] = NOW + 10 * 60_000
    values['quote_time'] = values['now']
    values['book_now'] = values['now']
    values['book']['time'] = values['now']
    with patch('backend.scalp.atr', return_value=.5), \
         patch('backend.scalp.pivots', return_value=local_pivots):
        assert assess_scalp_plan(**values)['status'] == 'waiting_data'


def test_scalp_plan_rejects_missing_and_stale_market_data():
    values, local_pivots = inputs(quote_age=5_001)
    with patch('backend.scalp.atr', return_value=.5), \
         patch('backend.scalp.pivots', return_value=local_pivots):
        assert assess_scalp_plan(**values)['status'] == 'quote_stale'
        values['quote_time'] = NOW
        values['price'] = 101.05
        values['book']['time'] = NOW - 5_001
        assert assess_scalp_plan(**values)['status'] == 'book_stale'


def test_unchanged_book_remains_usable_while_its_orderbook_stream_is_live():
    values, local_pivots = inputs(book_age=60_000)
    values['book_stream_live'] = True
    with patch('backend.scalp.atr', return_value=.5), \
         patch('backend.scalp.pivots', return_value=local_pivots):
        plan = assess_scalp_plan(**values)
    assert plan['status'] != 'book_stale'


def test_scalp_plan_rejects_a_return_inside_the_breakout_boundary():
    values, local_pivots = inputs()
    values['price'] = 99.99
    with patch('backend.scalp.atr', return_value=.5), \
         patch('backend.scalp.pivots', return_value=local_pivots):
        assert assess_scalp_plan(**values)['status'] == 'waiting_breakout'


def test_scalp_plan_requires_a_new_close_after_confirmation_was_invalidated():
    values, local_pivots = inputs()
    values['setup']['scalp_confirmation'] = {'invalidated_at': NOW}
    with patch('backend.scalp.atr', return_value=.5), \
         patch('backend.scalp.pivots', return_value=local_pivots):
        plan = assess_scalp_plan(**values)
    assert plan['status'] == 'waiting_breakout'
    assert plan['preliminary'] is True
    assert plan['entry'] is not None and plan['stop'] is not None and plan['target'] is not None


def test_scalp_plan_rejects_wide_stop_and_unprofitable_nearby_target():
    values, local_pivots = inputs(stop_level=99.5)
    with patch('backend.scalp.atr', return_value=.5), \
         patch('backend.scalp.pivots', return_value=local_pivots):
        assert assess_scalp_plan(**values)['status'] == 'stop_too_wide'
        values, local_pivots = inputs(target=101.6)
        with patch('backend.scalp.atr', return_value=.5), \
             patch('backend.scalp.pivots', return_value=local_pivots):
            plan = assess_scalp_plan(**values)
    assert plan['status'] == 'insufficient_rr'


def test_scalp_plan_rejects_gaps_and_absent_targets():
    values, local_pivots = inputs()
    values['candles'][10].time += 300_000
    with patch('backend.scalp.atr', return_value=.5), \
         patch('backend.scalp.pivots', return_value=local_pivots):
        assert assess_scalp_plan(**values)['status'] == 'waiting_data'
        values, stop_only = inputs()
        with patch('backend.scalp.atr', return_value=.5), \
             patch('backend.scalp.pivots', return_value=stop_only[:1]):
            assert assess_scalp_plan(**values)['status'] == 'no_target'


def test_scalp_plan_reserves_only_nearby_adverse_funding():
    values, local_pivots = inputs(funding=.01, next_funding=NOW + 15 * 60_000)
    with patch('backend.scalp.atr', return_value=.5), \
         patch('backend.scalp.pivots', return_value=local_pivots):
        plan = assess_scalp_plan(**values)
    assert plan['funding_rate_reserved'] == .01
    assert plan['costs_pct'] > .01 * 100


def test_scalp_plan_requires_fresh_fee_and_funding_data():
    values, local_pivots = inputs()
    values['fee'] = None
    with patch('backend.scalp.atr', return_value=.5), \
         patch('backend.scalp.pivots', return_value=local_pivots):
        assert assess_scalp_plan(**values)['status'] == 'costs_unavailable'
        values['fee'] = .0001
        values['funding_time'] = NOW - 86_400_001
        assert assess_scalp_plan(**values)['status'] == 'costs_unavailable'
        values['funding_time'] = NOW
        values['next_funding'] = NOW
        assert assess_scalp_plan(**values)['status'] == 'costs_unavailable'
