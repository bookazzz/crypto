from backend.market_background import DAY_MS, FOUR_HOURS_MS, build_market_background
from backend.models import Candle


NOW = 240 * DAY_MS


def market_buffers(coin_returns=None, btc_returns=None):
    coin_returns = coin_returns or ([-.001] * 120 + [-.002] * 42 + [.001] * 18)
    btc_returns = btc_returns or [value * 1.2 for value in coin_returns]
    assert len(coin_returns) == len(btc_returns) == 180
    buffers = {}

    def four_hour(values):
        candles = []
        close = 100.
        closes = [close]
        for value in values:
            close *= 1 + value
            closes.append(close)
        for index, price in enumerate(closes):
            close_at = NOW - (len(closes) - index - 1) * FOUR_HOURS_MS
            previous = closes[max(0, index - 1)]
            candles.append(Candle(close_at - FOUR_HOURS_MS, previous,
                max(previous, price) * 1.0001, min(previous, price) * .9999,
                price, 10))
        return candles

    buffers['ENAUSDT', '240'] = four_hour(coin_returns)
    buffers['BTCUSDT', '240'] = four_hour(btc_returns)
    first_close = NOW // DAY_MS * DAY_MS - 14 * DAY_MS
    for symbol in ('ENAUSDT', 'BTCUSDT'):
        buffers[symbol, 'D'] = [
            Candle(first_close - DAY_MS + index * DAY_MS, 100, 100.1, 99.9, 100, 10)
            for index in range(15)
        ]
    return buffers


def test_long_countertrend_warns_even_when_three_day_move_bounces():
    context = build_market_background('ENAUSDT', 'long', NOW, market_buffers())

    assert context['periods']['3']['direction'] == 'up'
    assert context['periods']['10']['direction'] == 'down'
    assert context['periods']['30']['direction'] == 'down'
    assert context['btc_correlation']['strength'] == 'strong'
    assert context['periods']['10']['relative_return'] > 0
    assert context['advisory'] is not None
    assert any('10' in warning for warning in context['warnings'])
    assert any('BTC' in warning for warning in context['warnings'])


def test_short_countertrend_uses_mirrored_direction_and_preserves_plan_data():
    returns = [.001] * 120 + [.002] * 42 + [-.001] * 18
    context = build_market_background('ENAUSDT', 'short', NOW,
        market_buffers(returns, [value * 1.2 for value in returns]))

    assert context['periods']['3']['direction'] == 'down'
    assert context['periods']['10']['direction'] == 'up'
    assert context['warnings']
    assert 'шорт' in context['advisory']


def test_thirty_day_correlation_uses_aligned_returns_and_supports_negative_link():
    coin = [-.001] * 120 + [-.002] * 42 + [.001] * 18
    positive = build_market_background('ENAUSDT', 'long', NOW,
        market_buffers(coin, [value * 1.2 for value in coin]))
    negative = build_market_background('ENAUSDT', 'long', NOW,
        market_buffers(coin, [-value * 1.2 for value in coin]))

    assert positive['btc_correlation']['value'] > .99
    assert negative['btc_correlation']['value'] < -.99
    assert positive['btc_correlation']['bars'] == 180
    assert negative['btc_correlation']['strength'] == 'strong'


def test_neutral_or_missing_market_data_does_not_invent_a_direction_or_warning():
    buffers = market_buffers([0.] * 180, [0.] * 180)
    context = build_market_background('ENAUSDT', 'long', NOW, buffers)
    assert context['periods']['10']['direction'] == 'mixed'
    assert context['btc_correlation']['value'] is None
    assert not context['warnings']

    del buffers['ENAUSDT', '240']
    missing = build_market_background('ENAUSDT', 'long', NOW, buffers)
    assert missing['periods']['10']['quality'] == 'unavailable'
    assert missing['periods']['10']['instrument_return'] is None
    assert missing['advisory'] is None


def test_gaps_and_open_candles_are_never_used_to_claim_a_complete_window():
    buffers = market_buffers()
    middle_close = NOW - 15 * DAY_MS
    buffers['ENAUSDT', '240'] = [row for row in buffers['ENAUSDT', '240']
                                  if row.time + FOUR_HOURS_MS != middle_close]
    open_bar = Candle(NOW, 100, 110, 90, 109, 10)
    buffers['ENAUSDT', '240'].append(open_bar)

    context = build_market_background('ENAUSDT', 'long', NOW, buffers)

    assert context['periods']['3']['quality'] == 'valid'
    assert context['periods']['10']['quality'] == 'valid'
    assert context['periods']['30']['quality'] == 'warming_up'
    assert context['periods']['30']['instrument_return'] is None
    assert context['btc_correlation']['value'] is None
    assert context['bars_as_of'] == NOW


def test_btc_and_non_crypto_contexts_do_not_compare_a_symbol_with_itself():
    buffers = market_buffers()
    btc = build_market_background('BTCUSDT', 'long', NOW, buffers)
    equity = build_market_background('NVDAUSDT', 'long', NOW, buffers, 'equity')

    assert btc['periods']['10']['btc_return'] is None
    assert btc['periods']['10']['relative_return'] is None
    assert btc['btc_correlation']['value'] is None
    assert equity['status'] == 'not_applicable'
    assert not equity['warnings']
