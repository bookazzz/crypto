"""Causal geometric detectors. Every pivot requires three CLOSED right-hand bars.

These are explicit heuristics, not a claim of predictive accuracy. See README.
"""
from dataclasses import dataclass
from .models import Candle, Signal

NAMES = {
    'ascending_triangle': 'Восходящий треугольник',
    'descending_triangle': 'Нисходящий треугольник',
    'symmetric_triangle': 'Симметричный треугольник',
    'double_top': 'Двойная вершина', 'double_bottom': 'Двойное дно',
    'bull_flag': 'Бычий флаг', 'bear_flag': 'Медвежий флаг',
    'bull_pennant': 'Бычий вымпел', 'bear_pennant': 'Медвежий вымпел',
    'rising_wedge': 'Восходящий клин', 'falling_wedge': 'Нисходящий клин',
    'head_shoulders': 'Голова и плечи', 'inverse_head_shoulders': 'Обратная голова и плечи',
}
EXPERIMENTAL_NAMES = {
    'rectangle': 'Прямоугольник',
    'ascending_channel': 'Восходящий канал',
    'descending_channel': 'Нисходящий канал',
}


@dataclass
class Pivot:
    index: int
    price: float
    kind: str


def atr(c: list[Candle], period: int = 14) -> float:
    if len(c) < period + 1:
        return 0
    ranges = [max(c[i].high - c[i].low, abs(c[i].high - c[i-1].close),
                  abs(c[i].low - c[i-1].close)) for i in range(1, len(c))]
    value = sum(ranges[:period]) / period
    for r in ranges[period:]:
        value = (value * (period - 1) + r) / period
    return value


def pivots(c: list[Candle]) -> list[Pivot]:
    result = []
    for i in range(3, len(c) - 3):
        neighbors = c[i-3:i] + c[i+1:i+4]
        if all(c[i].high > x.high for x in neighbors):
            result.append(Pivot(i, c[i].high, 'high'))
        if all(c[i].low < x.low for x in neighbors):
            result.append(Pivot(i, c[i].low, 'low'))
    return sorted(result, key=lambda p: (p.index, p.kind))


def fit(ps: list[Pivot]):
    mx = sum(p.index for p in ps) / len(ps)
    my = sum(p.price for p in ps) / len(ps)
    denom = sum((p.index - mx) ** 2 for p in ps)
    if not denom:
        return None
    slope = sum((p.index - mx) * (p.price - my) for p in ps) / denom
    return slope, my - slope * mx


def detect(symbol: str, timeframe: str, candles: list[Candle], *, stage: str = 'breakout',
           sensitivity: str = 'standard', include_experimental: bool = False,
           _prepared=None) -> list[Signal]:
    """Detect confirmed breakouts, or (stage='setup') formed unbroken geometry.

    The default path deliberately preserves the original breakout detector.
    Setup mode uses the same geometric rules and confirmed pivots, but does not
    treat an intrabar/current boundary touch as a breakout.
    """
    if sensitivity not in ('standard', 'expanded', 'early'):
        raise ValueError("sensitivity must be 'standard', 'expanded', or 'early'")
    if sensitivity == 'early' and stage != 'setup':
        raise ValueError('early sensitivity is only available for setup observations')
    # Expanded is intended for manual review: geometries may be noisier, while
    # pivots still need three closed bars on the right and breakouts still need
    # a closed candle beyond the boundary. Backtests keep the standard default.
    tolerance = 1.0 if sensitivity == 'early' else .5 if sensitivity == 'standard' else .75
    flat_tolerance = .5 if sensitivity == 'early' else tolerance
    min_height = 1.0 if sensitivity == 'standard' else .75
    breakout_atr = .1 if sensitivity == 'standard' else .05
    convergence_ratio = .85 if sensitivity == 'standard' else .95
    impulse_atr = 3.0 if sensitivity == 'standard' else 2.0
    impulse_width = .6 if sensitivity == 'standard' else .85
    if len(candles) < 25:
        return []
    # Extra history only initializes ATR. Geometry stays within the last 120 bars.
    # Never let the bar being evaluated widen its own early-pattern tolerances.
    atr_source=candles[:-1] if sensitivity=='early' else candles
    a = atr(atr_source[-250:])
    if a <= 0:
        return []
    c,ps=_prepared if _prepared is not None else (candles[-124:],None)
    if ps is None:
        ps=pivots(c[:-1])  # decision bar is NEVER part of the shape
    end = len(c) - 1
    out: dict[tuple, Signal] = {}

    def emit(pattern, side, start, boundary, height, lines, used):
        if not 20 <= end - start <= 120 or height <= min_height*a or not used:
            return
        sign = 1 if side == 'long' else -1
        # Fresh crossing; no repeated entries on bars already outside the shape.
        prior_boundary = lines[-1]['previous'] if lines else boundary
        crossed = sign * (c[-1].close - boundary) >= breakout_atr*a
        if stage == 'breakout':
            if not crossed or sign * (c[-2].close - prior_boundary) > breakout_atr*a:
                return
        elif stage == 'setup':
            # A structure is an early setup only while the last closed candle
            # has not confirmed its breakout. The current live candle is never
            # part of this input.
            if crossed:
                return
        else:
            raise ValueError("stage must be 'breakout' or 'setup'")
        stops = [p for p in used if p.kind == ('low' if side == 'long' else 'high')]
        if not stops:
            return
        stop = max(stops, key=lambda p: p.index).price - sign * .2*a
        target = boundary + sign * height
        if sign * (c[-1].close - stop) <= 0 or (stage == 'breakout' and sign * (target - c[-1].close) <= 0):
            return
        sig = Signal(symbol, timeframe, pattern, side,
                     c[-1].time + int(timeframe)*60000, c[start].time,
                     boundary, stop, target, a,
                     [{k: v for k, v in line.items() if k != 'previous'} for line in lines],
                     [{'time': c[p.index].time, 'price': p.price, 'kind': p.kind} for p in used])
        key = (pattern, side)
        # Prefer the longest valid shape, deterministic across scans.
        if key not in out or sig.start < out[key].start:
            out[key] = sig

    def line(start, slope, intercept, label):
        return {'label': label, 'points': [{'time': c[start].time, 'value': slope*start+intercept},
                {'time': c[-1].time, 'value': slope*end+intercept}],
                'previous': slope*(end-1)+intercept}

    # Swing patterns must alternate; consecutive same-kind extrema collapse.
    swings: list[Pivot] = []
    for p in ps:
        if swings and swings[-1].kind == p.kind:
            if (p.price > swings[-1].price) == (p.kind == 'high'):
                swings[-1] = p
        else:
            swings.append(p)
    for size in (3, 5):
        if len(swings) < size:
            continue
        used = swings[-size:]
        start = used[0].index
        for kind, side, double, head in [('high','short','double_top','head_shoulders'),
                                          ('low','long','double_bottom','inverse_head_shoulders')]:
            if used[0].kind != kind:
                continue
            sign = 1 if kind == 'high' else -1
            if size == 3:
                p, valley, q = used
                if abs(p.price-q.price) > tolerance*a or min(sign*p.price,sign*q.price)-sign*valley.price <= 0:
                    continue
                height = abs((p.price+q.price)/2-valley.price)
                emit(double,side,start,valley.price,height,[line(start,0,valley.price,'Шея')],used)
            else:
                left, v1, headp, v2, right = used
                if abs(left.price-right.price) > tolerance*a or sign*(headp.price-left.price) < a or sign*(headp.price-right.price) < a:
                    continue
                neck = fit([v1,v2])
                if neck:
                    m,b = neck
                    emit(head,side,start,m*end+b,abs(headp.price-(m*headp.index+b)),[line(start,m,b,'Шея')],used)

    for length in range(20, min(120, end) + 1):
        start = end-length
        used = [p for p in ps if p.index >= start]
        highs = [p for p in used if p.kind == 'high']
        lows = [p for p in used if p.kind == 'low']
        if len(highs) < 2 or len(lows) < 2:
            continue
        upper, lower = fit(highs), fit(lows)
        if not upper or not lower:
            continue
        um, ub = upper
        lm, lb = lower
        if any(abs(p.price-(um*p.index+ub)) > tolerance*a for p in highs) or any(abs(p.price-(lm*p.index+lb)) > tolerance*a for p in lows):
            continue
        width0 = um*start+ub-(lm*start+lb)
        width1 = um*end+ub-(lm*end+lb)
        if width0 <= min_height*a or width1 <= .1*a:
            continue
        # All formation bars must stay within the fitted boundaries.
        if any(c[i].high > um*i+ub+tolerance*a or c[i].low < lm*i+lb-tolerance*a for i in range(start,end)):
            continue
        flat_u, flat_l = abs(um*length) <= flat_tolerance*a, abs(lm*length) <= flat_tolerance*a
        converges = width1 < convergence_ratio*width0
        lines = [line(start,lm,lb,'Поддержка'),line(start,um,ub,'Сопротивление')]
        def channel_emit(name,side,height):
            if side == 'short':
                selected = [lines[1],lines[0]]
                boundary = lm*end+lb
            else:
                selected = lines
                boundary = um*end+ub
            emit(name,side,start,boundary,height,selected,used)
        if converges:
            if flat_u and lm*length > .5*a:
                channel_emit('ascending_triangle','long',width0)
            elif flat_l and um*length < -.5*a:
                channel_emit('descending_triangle','short',width0)
            elif um*length < -.5*a and lm*length > .5*a:
                channel_emit('symmetric_triangle','long',width0)
                channel_emit('symmetric_triangle','short',width0)
            elif um > 0 and lm > um and not flat_u:
                channel_emit('rising_wedge','short',width0)
            elif lm < 0 and um < lm and not flat_l:
                channel_emit('falling_wedge','long',width0)
        if include_experimental:
            parallel = abs((um-lm)*length) <= tolerance*a
            contained = all(c[i].high <= um*i+ub+tolerance*a and
                            c[i].low >= lm*i+lb-tolerance*a for i in range(start,end))
            if contained and width1 > .1*a:
                name = None
                if flat_u and flat_l:
                    name = 'rectangle'
                elif parallel and not flat_u and not flat_l and um*lm > 0:
                    name = 'ascending_channel' if um > 0 else 'descending_channel'
                if name:
                    # Existing patterns are more specific when they use the
                    # same confirmed pivots and breakout side.
                    anchors={(c[p.index].time,p.kind) for p in used}
                    for side in ('long','short'):
                        if name == 'rectangle':
                            channel_emit(name,side,width0)
                        else:
                            channel_emit(name,side,width0)
                        key=(name,side)
                        candidate=out.get(key)
                        if candidate:
                            candidate_anchors={(p['time'],p['kind']) for p in candidate.pivots}
                            specific=any(other_side==side and other_name not in EXPERIMENTAL_NAMES
                                and {(p['time'],p['kind']) for p in row.pivots}==anchors
                                for (other_name,other_side),row in out.items())
                            if specific:
                                out.pop(key,None)
        # Pole is strictly before consolidation; 5..20 bars, >= 3 ATR.
        for pole_len in range(5,21):
            if start-pole_len < 0:
                continue
            impulse = c[start].close-c[start-pole_len].close
            if abs(impulse) < impulse_atr*a or width0 > impulse_width*abs(impulse):
                continue
            side = 'long' if impulse > 0 else 'short'
            parallel = abs((um-lm)*length) <= tolerance*a
            countertrend = (um+lm)*impulse <= 0
            if parallel and countertrend:
                channel_emit('bull_flag' if side=='long' else 'bear_flag',side,abs(impulse))
            if converges and um < 0 < lm:
                channel_emit('bull_pennant' if side=='long' else 'bear_pennant',side,abs(impulse))
    return list(out.values())


def detect_combined(symbol: str, timeframe: str, candles: list[Candle], *,
                    breakout_sensitivity='standard', setup_sensitivity='early',
                    include_experimental=False):
    """Run signal and early-setup profiles using the same closed-bar pivots."""
    c=candles[-124:]
    prepared=(c,pivots(c[:-1])) if len(candles)>=25 else (c,[])
    breakout=detect(symbol,timeframe,candles,stage='breakout',
        sensitivity=breakout_sensitivity,include_experimental=include_experimental,
        _prepared=prepared)
    setup=detect(symbol,timeframe,candles,stage='setup',sensitivity=setup_sensitivity,
        include_experimental=include_experimental,_prepared=prepared)
    return breakout,setup
