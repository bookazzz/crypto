"""Short-horizon advisory levels for manually reviewed early setups."""
from __future__ import annotations

from math import isfinite

from .models import Candle, rounded
from .patterns import atr, pivots
from .strategy import clean_rr

LOOKBACK = 48
INTERVAL_MS = 5 * 60_000
MIN_NET_RR = 1.5
MIN_STOP_ATR = .5
MAX_STOP_ATR = 1.5
MAX_TARGET_ATR = 2.0
MAX_DATA_AGE_MS = 5_000


def assess_scalp_plan(*, setup: dict, candles: list[Candle], boundary: float,
                      price: float | None, quote_time: int | None, now: int,
                      book: dict | None, book_now: int, fee: float | None, fee_time: int | None,
                      fee_source: str | None, fee_mode: str, funding_time: int | None,
                      slippage: float, funding_rate: float | None,
                      next_funding: int | None, tick_size: float,
                      current_boundary: float | None = None,
                      book_stream_live: bool = False) -> dict:
    """Return fresh preliminary or confirmed levels; never submit orders."""
    side = setup['side']
    sign = 1 if side == 'long' else -1
    current_boundary = boundary if current_boundary is None else current_boundary
    result = {
        'status': 'waiting_data', 'reason': 'Ожидаем закрытые 5-минутные свечи',
        'side': side, 'timeframe': '5', 'horizon_minutes': [5, 30],
        'preliminary': True, 'confirmation_reason': 'Пробой ещё не подтверждён',
        'as_of': int(now), 'boundary': current_boundary, 'entry': None,
        'stop': None, 'target': None, 'net_rr': None,
        'min_net_rr': MIN_NET_RR, 'atr': None, 'stop_pct': None,
        'target_pct': None, 'costs_pct': None,
        'levels': [], 'confirmed_candle_at': None,
    }

    def done(status: str, reason: str) -> dict:
        result.update(status=status, reason=reason)
        return result

    if (price is None or quote_time is None
            or not 0 <= now - int(quote_time) <= MAX_DATA_AGE_MS):
        return done('quote_stale', 'Котировка отсутствует или старше 5 секунд')

    age = book_now - int((book or {}).get('time', 0) or 0)
    bids = (book or {}).get('bids') or {}
    asks = (book or {}).get('asks') or {}
    if (not 0 <= age <= MAX_DATA_AGE_MS and not book_stream_live) or not bids or not asks:
        return done('book_stale', 'Ждём свежий стакан не старше 5 секунд')
    try:
        bid, ask = max(map(float, bids)), min(map(float, asks))
    except (TypeError, ValueError):
        return done('book_stale', 'Не удалось прочитать лучшие цены стакана')
    if bid <= 0 or ask <= bid:
        return done('book_stale', 'Стакан пустой или пересечён')
    spread = ask - bid
    entry = ask if sign > 0 else bid
    result.update(entry=entry, spread=spread, bid=bid, ask=ask)

    fee_error = None
    if (fee is None or fee_time is None or not 0 <= now - int(fee_time) <= 86_400_000
            or (fee_mode == 'account' and fee_source != 'account')):
        fee_error = 'Нет актуальной taker-комиссии'
    elif (funding_rate is None or next_funding is None or funding_time is None
          or not 0 <= now - int(funding_time) <= 86_400_000):
        fee_error = 'Нет актуальных данных funding'
    elif int(next_funding) <= now:
        fee_error = 'Время следующего funding устарело'

    closed = sorted((c for c in candles if c.time + INTERVAL_MS <= now),
                    key=lambda c: c.time)[-LOOKBACK:]
    if len(closed) < LOOKBACK:
        return done('waiting_data', 'Для локальных уровней нужны 48 закрытых 5-минутных свечей')
    if any(b.time - a.time != INTERVAL_MS for a, b in zip(closed, closed[1:])):
        return done('waiting_data', 'В истории 5-минутных свечей есть пропуск')

    last = closed[-1]
    close_at = last.time + INTERVAL_MS
    if now - close_at > INTERVAL_MS:
        return done('waiting_data', 'Последняя закрытая 5-минутная свеча устарела')
    atr_value = atr(closed[-15:])
    result.update(atr=atr_value, confirmed_candle_at=close_at)
    if atr_value <= 0 or not isfinite(atr_value):
        return done('waiting_data', 'Не удалось рассчитать ATR по закрытым свечам')

    snapshot = setup.get('snapshot') or {}
    timeframe_ms = int(setup.get('timeframe') or 5) * 60_000
    delivery = setup.get('delivery') or {}
    formed_at = int(delivery.get('exchange_event_at') or setup.get('formed_at') or
                    (snapshot.get('detected_on_bar', 0) + timeframe_ms if snapshot.get('detected_on_bar') else 0) or
                    setup.get('event_at') or setup.get('detected_at') or 0)
    tick = tick_size or max(abs(boundary) * 1e-8, 1e-8)
    threshold = max(tick, .1 * atr_value)
    after_setup = close_at > formed_at
    candle_confirmed = after_setup and sign * (last.close - boundary) >= threshold
    held_beyond = sign * (price - current_boundary) > 0
    confirmation_state = setup.get('scalp_confirmation') or {}
    invalidated_at = int(confirmation_state.get('invalidated_at') or 0)
    if invalidated_at and close_at <= invalidated_at:
        candle_confirmed = False
    result['preliminary'] = not (candle_confirmed and held_beyond)
    if not after_setup:
        result['confirmation_reason'] = 'Ждём закрытую 5-минутную свечу после формирования фигуры'
    elif invalidated_at and close_at <= invalidated_at:
        result['confirmation_reason'] = 'После возврата за границу ждём новое закрытие 5-минутной свечи'
    elif not candle_confirmed:
        result['confirmation_reason'] = 'Последняя закрытая 5-минутная свеча ещё не подтвердила пробой'
    elif not held_beyond:
        result['confirmation_reason'] = 'Цена вернулась за наклонную границу пробоя'
    else:
        result['confirmation_reason'] = 'Пробой подтверждён закрытием 5-минутной свечи и удерживается ценой'

    confirmed = pivots(closed)
    pad = max(2 * tick, .15 * atr_value)
    opposing = [p for p in confirmed if p.kind == ('low' if sign > 0 else 'high')
                and sign * (entry - p.price) > 0]
    if not opposing:
        return done('no_stop_level', 'Нет подтверждённого локального экстремума для стопа')
    stop_pivot = max(opposing, key=lambda p: (p.index, p.price * sign))
    stop = rounded(stop_pivot.price - sign * pad, tick, up=sign < 0)
    risk_distance = sign * (entry - stop)
    result.update(stop=stop, stop_level=stop_pivot.price,
                  stop_pct=100 * risk_distance / entry if entry else None)

    ahead = [p for p in confirmed if p.kind == ('high' if sign > 0 else 'low')
             and sign * (p.price - entry) > 0
             and sign * (p.price - entry) <= MAX_TARGET_ATR * atr_value]
    target_error = None
    if not ahead:
        target_error = 'В пределах 2 ATR₅ₘ нет ближайшего локального уровня'
    else:
        target_pivot = min(ahead, key=lambda p: (sign * (p.price - entry), -p.index))
        target = rounded(target_pivot.price - sign * pad, tick, up=sign < 0)
        if sign * (target - entry) <= 0:
            target_error = 'До ближайшего уровня не осталось места для цели'
        else:
            result.update(target=target, target_level=target_pivot.price,
                          target_pct=100 * sign * (target - entry) / entry if entry else None,
                          levels=[{'kind': 'stop', 'price': stop_pivot.price},
                                  {'kind': 'target', 'price': target_pivot.price}])

    if risk_distance < MIN_STOP_ATR * atr_value:
        return done('stop_too_tight', 'Локальный стоп ближе 0,5 ATR₅ₘ')
    if risk_distance > MAX_STOP_ATR * atr_value:
        return done('stop_too_wide', 'Локальный стоп дальше 1,5 ATR₅ₘ')
    if target_error:
        return done('no_target', target_error)
    if fee_error:
        return done('costs_unavailable', fee_error)

    funding = float(funding_rate) if now <= int(next_funding) <= now + 30 * 60_000 else 0.0
    adverse_funding = max(0.0, sign * funding)
    # Entry is executable on its side of the book; estimate the exit spread
    # once, while clean_rr accounts for the two taker fees and slippage legs.
    exit_shift = sign * spread / 2
    stop_reference = stop - exit_shift
    target_reference = target - exit_shift
    net_rr, risk, reward = clean_rr(entry, stop_reference, target_reference,
                                    side, float(fee), float(slippage),
                                    adverse_funding, tick)
    result['net_rr'] = net_rr
    result['costs_pct'] = 100 * (2 * float(fee) + 2 * float(slippage)
        + spread / ((ask + bid) / 2) + adverse_funding)
    result['risk_per_unit'] = risk
    result['reward_per_unit'] = reward
    result['funding_rate_reserved'] = adverse_funding
    if reward <= 0 or net_rr < MIN_NET_RR:
        return done('insufficient_rr', 'Недостаточно пространства до цели: чистое RR ниже 1,5:1')
    if result['preliminary']:
        return done('waiting_breakout', result['confirmation_reason'])
    if sign * (price - stop) <= 0:
        return done('scenario_invalidated', 'Цена уже прошла локальный стоп до входа')
    if sign * (target - price) <= 0:
        return done('target_reached', 'Цена уже достигла локальной цели до входа')
    return done('available', 'Подтверждение, локальные уровни и чистое RR 1,5:1 пройдены')
