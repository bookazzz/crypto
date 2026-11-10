"""One conservative live-entry assessment shared by feeds and context views."""
from functools import lru_cache

from .models import rounded
from .strategy import clean_rr

LIVE_QUOTE_MAX_AGE_MS = 5_000
FEE_MAX_AGE_MS = 86_400_000
FUNDING_MAX_AGE_MS = 86_400_000


@lru_cache(maxsize=8192)
def _allowed_entry_range(side, boundary, stop, target, fee, slippage,
                         funding_rate, tick, min_rr):
    """Cache the geometry/cost-only search; live ticks need one RR check."""
    sign = 1 if side == "long" else -1
    first = rounded(boundary + sign * tick, tick, up=sign > 0)
    if sign * (target - first) <= 0:
        return None, None, None
    first_rr = clean_rr(first, stop, target, side, fee, slippage,
                        funding_rate, tick)[0]
    if first_rr is None or first_rr < min_rr:
        return None, None, None
    limit = rounded(target - sign * tick, tick, up=sign < 0)
    if sign * (limit - first) < 0:
        limit = first
    tick_count = max(0, int(round(abs(limit - first) / tick)))
    lo, hi = 0, tick_count
    while lo < hi:
        steps = (lo + hi + 1) // 2
        candidate = rounded(first + sign * steps * tick, tick, up=sign > 0)
        rr = clean_rr(candidate, stop, target, side, fee, slippage,
                      funding_rate, tick)[0]
        if rr is not None and rr >= min_rr:
            lo = steps
        else:
            hi = steps - 1
    furthest = rounded(first + sign * lo * tick, tick, up=sign > 0)
    return min(first, furthest), max(first, furthest), furthest


def assess_entry(*, side, price, quote_time, now, boundary, stop, target, atr,
                 fee, fee_time, fee_source, fee_mode, slippage, funding_rate,
                 funding_time, tick_size, min_rr, requires_breakout=False):
    sign = 1 if side == "long" else -1
    age = max(0, int(now) - int(quote_time or 0)) if quote_time else None
    base = {
        "as_of": int(now), "price": price, "quote_time": int(quote_time or 0),
        "quote_age_ms": age, "boundary": boundary, "stop": stop, "target": target,
        "price_min": None, "price_max": None, "net_rr": None,
        "required_net_rr": float(min_rr),
    }

    def result(status, reason):
        return base | {"status": status, "reason": reason}

    if price is None or not quote_time or age is None or age > LIVE_QUOTE_MAX_AGE_MS:
        return result("unavailable", "Нет котировки не старше 5 секунд")
    if not fee_time or int(now) - int(fee_time) > FEE_MAX_AGE_MS or int(fee_time) > int(now):
        return result("costs_unavailable", "Комиссия отсутствует или устарела")
    if fee_mode == "account" and fee_source != "account":
        return result("costs_unavailable", "Нет актуальной персональной комиссии аккаунта")
    if funding_rate is None or not funding_time or int(now) - int(funding_time) > FUNDING_MAX_AGE_MS:
        return result("costs_unavailable", "Funding отсутствует или устарел")
    if atr <= 0 or stop <= 0 or target <= 0 or tick_size < 0:
        return result("unavailable", "Не хватает данных для расчёта зоны входа")
    if sign * (price - stop) <= 0:
        return result("scenario_invalidated", "Цена достигла стопового уровня")
    if sign * (target - price) <= 0:
        return result("target_reached", "Цель уже достигнута")
    if not requires_breakout and sign * (price - boundary) < -0.1 * atr:
        return result("scenario_invalidated", "Цена вернулась за границу сценария")

    tick = tick_size or max(abs(boundary) * 1e-8, 1e-8)
    try:
        # Directional rounding preserves the first permissible price while
        # reusing this zone until a sloped boundary moves by a price tick.
        zone_boundary = rounded(boundary, tick, up=sign > 0)
        price_min, price_max, furthest = _allowed_entry_range(
            side, zone_boundary, stop, target, float(fee), float(slippage),
            float(funding_rate), tick, float(min_rr),
        )
        if price_min is None:
            return result("insufficient_rr", "Чистое RR ниже минимума: допустимой зоны входа нет")
        base = base | {"price_min": price_min, "price_max": price_max}
        current_rr = clean_rr(
            float(price), stop, target, side, float(fee), float(slippage),
            float(funding_rate), tick,
        )[0]
        base["net_rr"] = current_rr
    except (TypeError, ValueError, OverflowError, ZeroDivisionError):
        return result("costs_unavailable", "Не удалось проверить чистое RR")

    tick_tolerance = max(tick * 1e-8, 1e-12)
    if requires_breakout and sign * (price - boundary) < tick - tick_tolerance:
        return result("waiting", "Ожидается пересечение границы")
    if sign * (price - boundary) < 0:
        return result("waiting", "Ожидается удержание границы пробоя")
    if sign * (price - furthest) > tick * 0.5:
        return result("late", "Цена вышла за допустимую зону входа")
    if current_rr is None or current_rr < float(min_rr):
        return result("late", "Чистое RR ниже заданного минимума")
    return result("available", "Цена находится в зоне входа, чистое RR соответствует минимуму")
