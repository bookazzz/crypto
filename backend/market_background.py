"""Closed-candle price trends and BTC-relative market context."""
from math import isfinite, sqrt

from .patterns import atr


FOUR_HOURS_MS = 4 * 60 * 60_000
DAY_MS = 24 * 60 * 60_000
PERIOD_DAYS = (3, 10, 30)
MIN_CORRELATION_BARS = 180


def _ordered_closed(rows, timestamp, interval):
    rows = sorted((c for c in rows if c.time + interval <= timestamp), key=lambda c: c.time)
    by_close = {c.time + interval: c for c in rows}
    return rows, by_close


def _quality(required, present, latest_close, expected_close, complete=False):
    if not present:
        return 'unavailable'
    if latest_close != expected_close:
        return 'stale'
    if present < required:
        return 'warming_up'
    if not complete:
        return 'partial'
    return 'valid'


def _returns(closes):
    result = []
    for previous, current in zip(closes, closes[1:]):
        if previous <= 0 or current <= 0:
            return None
        value = current / previous - 1
        if not isfinite(value):
            return None
        result.append(value)
    return result


def _correlation(left, right):
    if len(left) != len(right) or len(left) < MIN_CORRELATION_BARS:
        return None
    left_mean = sum(left) / len(left)
    right_mean = sum(right) / len(right)
    left_var = sum((v - left_mean) ** 2 for v in left)
    right_var = sum((v - right_mean) ** 2 for v in right)
    if left_var <= 0 or right_var <= 0:
        return None
    covariance = sum((a - left_mean) * (b - right_mean) for a, b in zip(left, right))
    result = covariance / sqrt(left_var * right_var)
    return max(-1., min(1., result)) if isfinite(result) else None


def _strength(value):
    if value is None:
        return 'unavailable'
    magnitude = abs(value)
    if magnitude >= .7:
        return 'strong'
    if magnitude >= .4:
        return 'moderate'
    return 'weak'


def build_market_background(symbol, side, timestamp, buffers, asset_class='crypto'):
    """Build causal 3/10/30-day returns and aligned 30-day 4h correlation."""
    symbol = str(symbol or '').upper()
    timestamp = int(timestamp)
    base = {'symbol': symbol, 'as_of': timestamp, 'source_timeframe': '240',
            'periods': {}, 'btc_correlation': {'value': None, 'strength': 'unavailable',
                'bars': 0, 'quality': 'unavailable'}, 'warnings': [], 'advisory': None}
    if asset_class != 'crypto':
        return base | {'status': 'not_applicable', 'reason': 'Сравнение с BTC применимо только к криптоактивам.'}

    expected_close = timestamp // FOUR_HOURS_MS * FOUR_HOURS_MS
    symbol_rows, symbol_by_close = _ordered_closed(buffers.get((symbol, '240'), ()), timestamp,
                                                    FOUR_HOURS_MS)
    btc_rows, btc_by_close = _ordered_closed(buffers.get(('BTCUSDT', '240'), ()), timestamp,
                                             FOUR_HOURS_MS)
    daily_rows, daily_by_close = _ordered_closed(buffers.get((symbol, 'D'), ()), timestamp, DAY_MS)
    btc_daily_rows, btc_daily_by_close = _ordered_closed(
        buffers.get(('BTCUSDT', 'D'), ()), timestamp, DAY_MS)
    expected_day_close = timestamp // DAY_MS * DAY_MS
    daily_times = [expected_day_close - DAY_MS * i for i in range(14, -1, -1)]
    daily_complete = all(close_time in daily_by_close for close_time in daily_times)
    daily_candles = [daily_by_close[t] for t in daily_times] if daily_complete else []
    daily_atr_pct = (atr(daily_candles) / daily_candles[-1].close
                     if daily_candles and daily_candles[-1].close > 0 else None)
    if daily_atr_pct is not None and (not isfinite(daily_atr_pct) or daily_atr_pct <= 0):
        daily_atr_pct = None
    btc_daily_complete = all(close_time in btc_daily_by_close for close_time in daily_times)
    btc_daily_candles = [btc_daily_by_close[t] for t in daily_times] if btc_daily_complete else []
    btc_daily_atr_pct = (atr(btc_daily_candles) / btc_daily_candles[-1].close
                         if btc_daily_candles and btc_daily_candles[-1].close > 0 else None)
    if btc_daily_atr_pct is not None and (not isfinite(btc_daily_atr_pct) or btc_daily_atr_pct <= 0):
        btc_daily_atr_pct = None

    if symbol == 'BTCUSDT':
        btc_rows, btc_by_close = symbol_rows, symbol_by_close

    for days in PERIOD_DAYS:
        steps = days * DAY_MS // FOUR_HOURS_MS
        close_times = [expected_close - FOUR_HOURS_MS * i for i in range(steps, -1, -1)]
        own_complete = all(t in symbol_by_close for t in close_times)
        own_closes = [symbol_by_close[t].close for t in close_times] if own_complete else []
        own_return = own_closes[-1] / own_closes[0] - 1 if own_closes and own_closes[0] > 0 else None
        btc_complete = all(t in btc_by_close for t in close_times)
        btc_closes = [btc_by_close[t].close for t in close_times] if btc_complete else []
        btc_return = btc_closes[-1] / btc_closes[0] - 1 if btc_closes and btc_closes[0] > 0 else None
        own_latest_close = symbol_rows[-1].time + FOUR_HOURS_MS if symbol_rows else None
        own_quality = _quality(steps + 1, len(symbol_rows),
            own_latest_close, expected_close, complete=own_complete)
        btc_latest_close = btc_rows[-1].time + FOUR_HOURS_MS if btc_rows else None
        btc_quality = _quality(steps + 1, len(btc_rows),
            btc_latest_close, expected_close, complete=btc_complete)
        relative_return = (1 + own_return) / (1 + btc_return) - 1 if (
            symbol != 'BTCUSDT' and own_return is not None and btc_return is not None and btc_return > -1
        ) else None
        btc_direction = None
        if btc_return is not None and btc_daily_atr_pct is not None and btc_quality == 'valid':
            btc_direction = ('up' if btc_return > btc_daily_atr_pct else
                             'down' if btc_return < -btc_daily_atr_pct else 'mixed')
        direction = None
        if own_return is not None and daily_atr_pct is not None and own_quality == 'valid':
            direction = ('up' if own_return > daily_atr_pct else
                         'down' if own_return < -daily_atr_pct else 'mixed')
        base['periods'][str(days)] = {
            'days': days, 'instrument_return': own_return,
            'btc_return': None if symbol == 'BTCUSDT' else btc_return,
            'btc_direction': btc_direction,
            'relative_return': relative_return, 'direction': direction,
            'bars': len(own_closes), 'quality': own_quality,
            'btc_quality': btc_quality,
        }

    correlation_times = [expected_close - FOUR_HOURS_MS * i
                         for i in range(MIN_CORRELATION_BARS, -1, -1)]
    correlation = None
    correlation_bars = 0
    correlation_quality = 'unavailable'
    if symbol != 'BTCUSDT':
        aligned = all(t in symbol_by_close and t in btc_by_close for t in correlation_times)
        if aligned:
            own_returns = _returns([symbol_by_close[t].close for t in correlation_times])
            btc_returns = _returns([btc_by_close[t].close for t in correlation_times])
            if own_returns is not None and btc_returns is not None:
                correlation_bars = len(own_returns)
                correlation = _correlation(own_returns, btc_returns)
                correlation_quality = 'valid' if correlation is not None else 'invalid'
        else:
            have_history = bool(symbol_by_close and btc_by_close)
            correlation_quality = 'partial' if have_history else 'unavailable'
    base['btc_correlation'] = {'value': correlation, 'strength': _strength(correlation),
        'bars': correlation_bars, 'quality': correlation_quality,
        'window_days': 30 if correlation is not None else None}
    base['daily_atr_pct'] = daily_atr_pct
    base['daily_atr_quality'] = 'valid' if daily_atr_pct is not None else (
        'stale' if daily_rows and daily_rows[-1].time + DAY_MS != expected_day_close else
        'partial' if daily_rows and not daily_complete else 'unavailable')
    base['btc_daily_atr_pct'] = btc_daily_atr_pct
    base['btc_daily_atr_quality'] = ('valid' if btc_daily_atr_pct is not None else
        'stale' if btc_daily_rows and btc_daily_rows[-1].time + DAY_MS != expected_day_close else
        'partial' if btc_daily_rows and not btc_daily_complete else 'unavailable')
    base['bars_as_of'] = (min(symbol_rows[-1].time + FOUR_HOURS_MS,
        btc_rows[-1].time + FOUR_HOURS_MS) if symbol_rows and btc_rows else None)
    base['status'] = ('valid' if base['periods']['3']['quality'] == 'valid' and
        base['periods']['10']['quality'] == 'valid' and
        base['periods']['30']['quality'] == 'valid' and daily_atr_pct is not None and
        btc_daily_atr_pct is not None else
        'partial' if symbol_rows else 'unavailable')

    if symbol == 'BTCUSDT':
        return base

    recent = base['periods']['10']
    broad = base['periods']['30']
    expected = 'up' if side == 'long' else 'down'
    opposite = 'down' if side == 'long' else 'up'
    direction_name = lambda value: 'растёт' if value == 'up' else 'снижается'
    side_name = 'лонг' if side == 'long' else 'шорт'
    if recent['quality'] == 'valid' and recent['direction'] == opposite:
        move = 'вверх' if side == 'long' else 'вниз'
        pct = recent['instrument_return'] * 100
        base['warnings'].append(
            f"Сценарий на {side_name} против движения {symbol} за 10 дней ({pct:+.2f}%)."
        )
        base['advisory'] = (
            f"Сценарий на {side_name} идёт против движения {symbol} за 10 дней ({pct:+.2f}%). "
            f"Возможен краткосрочный отскок против общего движения; не затягивайте удержание. "
            f"Пересмотрите сценарий при потере импульса 5м или закрытии свечи {move} за границей пробоя."
        )
    elif (recent['quality'] == 'valid' and broad['quality'] == 'valid' and
          recent['direction'] == expected and broad['direction'] == opposite):
        base['advisory'] = (
            f"Движение {symbol} за 10 дней поддерживает сценарий на {side_name}, "
            f"но за 30 дней монета {direction_name(broad['direction'])}: "
            f"это восстановление внутри более широкого снижения."
            if broad['direction'] == 'down' else
            f"Движение {symbol} за 10 дней поддерживает сценарий на {side_name}, "
            f"но за 30 дней монета {direction_name(broad['direction'])}: "
            f"это откат внутри более широкого роста."
        )

    if (correlation is not None and abs(correlation) >= .7 and
            base['periods']['10']['btc_quality'] == 'valid'):
        btc_direction = recent['btc_direction']
        direction_expected_from_btc = ('up' if correlation > 0 else 'down') if side == 'long' else (
            'down' if correlation > 0 else 'up')
        adverse = btc_direction in ('up', 'down') and btc_direction != direction_expected_from_btc
        if adverse:
            btc_move = direction_name(btc_direction)
            warning = (f"Связь с BTC сильная (r={correlation:+.2f}); BTC за 10 дней {btc_move}, "
                       f"что неблагоприятно для сценария на {side_name}.")
            base['warnings'].append(warning)
            base['advisory'] = f"{base['advisory']} {warning}" if base['advisory'] else warning
    return base


def compact_market_advisory(background, limit=140):
    """Return a short, optional feed-card annotation."""
    if not background or background.get('status') in ('not_applicable', 'unavailable'):
        return None
    text = background.get('advisory')
    if not text:
        recent = (background.get('periods') or {}).get('10') or {}
        if recent.get('quality') == 'valid' and recent.get('instrument_return') is not None:
            direction = {'up': 'рост', 'down': 'снижение', 'mixed': 'боковое движение'}.get(
                recent.get('direction'))
            if direction:
                text = f"Движение за 10 дней: {direction} ({recent['instrument_return'] * 100:+.2f}%)."
    if not text:
        return None
    return text if len(text) <= limit else text[:limit - 1].rstrip() + '…'
