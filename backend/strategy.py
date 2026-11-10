"""Causal entry filters and fixed research variants for figure breakouts."""
from dataclasses import asdict, dataclass
from math import isfinite
import os

from .models import Candle, Signal, rounded
from .patterns import atr, pivots

VERSION = 'figures-context-v4'
CONTEXT = {'5': '60', '15': '60', '60': '240', '240': 'D'}
FIGURE_TF = '15'
ENTRY_TF = '5'
BIAS_TF = '60'
STRUCTURE_TF = '15'
EMA_FAST = 50
EMA_SLOW = 200
VOLUME_MULTIPLE = 1.5
DEFAULT_MIN_BREAKOUT_NOTIONAL_USDT_PER_5M = 10_000.
RETEST_ATR = .25
RETEST_CONFIRM_ATR = .1
MAX_RETEST_BARS = 3
MIN_STOP_ATR = .8
TIME_EXIT_BARS = 12


def market_structure(candles: list[Candle]) -> dict:
    """Classify 15m swing structure using only pivots confirmed by closed bars."""
    if len(candles) < 20:
        return {'ready': False, 'direction': None, 'bars': len(candles), 'required': 20}
    sample = candles[-120:]
    swing_points = pivots(sample)
    highs = [p for p in swing_points if p.kind == 'high'][-2:]
    lows = [p for p in swing_points if p.kind == 'low'][-2:]
    if len(highs) < 2 or len(lows) < 2:
        return {'ready': False, 'direction': None, 'bars': len(candles),
                'confirmed_highs': len(highs), 'confirmed_lows': len(lows)}
    tolerance = .1 * atr(sample[-30:])
    higher_highs = highs[-1].price > highs[-2].price + tolerance
    higher_lows = lows[-1].price > lows[-2].price + tolerance
    lower_highs = highs[-1].price < highs[-2].price - tolerance
    lower_lows = lows[-1].price < lows[-2].price - tolerance
    direction = ('up' if higher_highs and higher_lows else
                 'down' if lower_highs and lower_lows else 'mixed')
    return {'ready': True, 'direction': direction, 'bars': len(candles),
            'last_two_highs': [p.price for p in highs],
            'last_two_lows': [p.price for p in lows],
            'as_of': candles[-1].time + 15 * 60_000}


def classify_market_setup(hourly: list[Candle], fifteen_minute: list[Candle], side: str) -> dict:
    """Separate trend continuation from a 15m structure reversal against 1h bias."""
    long_ok, long_data = trend_filter(hourly, 'long')
    short_ok, short_data = trend_filter(hourly, 'short')
    structure = market_structure(fifteen_minute)
    if long_ok is None or short_ok is None or not structure['ready']:
        return {'scenario': 'unconfirmed', 'eligible': False,
                'hourly_bias': None if long_ok is None or short_ok is None else 'neutral',
                'hourly_long': long_data, 'hourly_short': short_data,
                'structure_15m': structure}
    hourly_bias = 'up' if long_ok else 'down' if short_ok else 'neutral'
    side_direction = 'up' if side == 'long' else 'down'
    structure_direction = structure['direction']
    if hourly_bias == side_direction and structure_direction == side_direction:
        scenario = 'continuation'
    elif (hourly_bias in ('up', 'down') and hourly_bias != side_direction
          and structure_direction == side_direction):
        scenario = 'reversal'
    else:
        scenario = 'unconfirmed'
    return {'scenario': scenario, 'eligible': scenario in ('continuation', 'reversal'),
            'hourly_bias': hourly_bias, 'structure_15m': structure,
            'confirmation_rule': ('15m_structure_shift_and_5m_close'
                                  if scenario == 'reversal' else '5m_close_beyond_boundary')}


def confirm_breakout_5m(candles: list[Candle], signal: Signal) -> tuple[bool, dict]:
    """Require the 5m candle ending at the 15m figure close to confirm its break."""
    closed = [c for c in candles if c.time + 5 * 60_000 <= signal.time]
    if signal.timeframe != FIGURE_TF:
        return False, {'ready': False, 'reason': 'figure_timeframe_is_not_15m'}
    if not closed or closed[-1].time + 5 * 60_000 != signal.time:
        return False, {'ready': False, 'reason': '5m_confirmation_candle_missing_or_stale'}
    last = closed[-1]
    atr_5m = atr(closed[-30:])
    if atr_5m <= 0:
        return False, {'ready': False, 'reason': 'insufficient_5m_atr_history'}
    sign = 1 if signal.side == 'long' else -1
    boundary = line_at(signal, signal.time)
    distance = sign * (last.close - boundary)
    required = .1 * atr_5m
    passed = distance >= required
    return passed, {'ready': True, 'passed': passed, 'close': last.close,
                    'boundary': boundary, 'atr_5m': atr_5m,
                    'distance_atr': distance / atr_5m,
                    'required_distance_atr': .1, 'bar_time': last.time + 5 * 60_000}


@dataclass(frozen=True)
class Variant:
    entry: str
    net_rr: float
    time_exit: int | None

    @property
    def id(self):
        time = 'unlimited' if self.time_exit is None else f'{self.time_exit}bars'
        return f'{self.entry}-rr{self.net_rr:g}-{time}'

    def dict(self):
        return asdict(self) | {'id': self.id}


VARIANTS = tuple(Variant(entry, rr, timeout)
                 for entry in ('immediate', 'retest')
                 for rr in (1., 1.5, 2.)
                 for timeout in (None, TIME_EXIT_BARS))


def ema(values: list[float], period: int) -> list[float]:
    if not values:
        return []
    alpha = 2 / (period + 1)
    out = [values[0]]
    for value in values[1:]:
        out.append(alpha * value + (1 - alpha) * out[-1])
    return out


def trend_filter(candles: list[Candle], side: str) -> tuple[bool | None, dict]:
    """Return None when the higher-timeframe history is not yet sufficient."""
    if len(candles) < EMA_SLOW + 3:
        return None, {'ready': False, 'bars': len(candles), 'required': EMA_SLOW + 3}
    closes = [c.close for c in candles]
    fast = ema(closes, EMA_FAST)
    slow = ema(closes, EMA_SLOW)
    rising = fast[-1] > fast[-4]
    passed = (closes[-1] > slow[-1] and fast[-1] > slow[-1] and rising
              if side == 'long' else
              closes[-1] < slow[-1] and fast[-1] < slow[-1] and not rising)
    return passed, {'ready': True, 'close': closes[-1], 'ema50': fast[-1],
                    'ema200': slow[-1], 'ema50_rising_3': rising,
                    'direction': side}


def volume_filter(candles: list[Candle], timeframe: str | int | None = None) -> tuple[bool | None, dict]:
    """Require both relative volume and minimum quote turnover on the breakout bar."""
    if len(candles) < 21:
        return None, {'ready': False, 'bars': len(candles), 'required': 21}
    prior_volume = sorted(c.volume for c in candles[-21:-1])
    median_volume = (prior_volume[9] + prior_volume[10]) / 2
    prior_turnover = sorted((getattr(c,'turnover',None) if getattr(c,'turnover',None) is not None
                             else c.close*c.volume)
                            for c in candles[-21:-1])
    median_turnover = (prior_turnover[9] + prior_turnover[10]) / 2
    last = candles[-1]
    last_turnover = getattr(last,'turnover',None)
    turnover = last_turnover if last_turnover is not None else last.close*last.volume
    multiple = turnover / median_turnover if median_turnover > 0 else float('inf')
    minimum_notional = None
    if timeframe is not None:
        try:
            interval_minutes = int(timeframe)
            minimum_5m = float(os.getenv('BREAKOUT_MIN_NOTIONAL_USDT_PER_5M',
                                         DEFAULT_MIN_BREAKOUT_NOTIONAL_USDT_PER_5M))
        except (TypeError, ValueError):
            interval_minutes = 0
            minimum_5m = DEFAULT_MIN_BREAKOUT_NOTIONAL_USDT_PER_5M
        if interval_minutes > 0 and isfinite(minimum_5m) and minimum_5m > 0:
            minimum_notional = minimum_5m * interval_minutes / 5
    relative_pass = multiple >= VOLUME_MULTIPLE
    notional_pass = minimum_notional is None or turnover >= minimum_notional
    return relative_pass and notional_pass, {'ready': True,
        'breakout_volume': last.volume, 'breakout_turnover_usdt': turnover,
        'median_previous_20': median_volume,
        'median_turnover_previous_20_usdt': median_turnover,
        'multiple': multiple, 'minimum': VOLUME_MULTIPLE,
        'minimum_notional_usdt': minimum_notional,
        'relative_pass': relative_pass, 'notional_pass': notional_pass}


def line_at(signal: Signal, timestamp: int) -> float:
    """Continue the detected boundary using its original two points."""
    line = signal.lines[-1] if signal.lines else None
    if not line or len(line.get('points', [])) < 2:
        return signal.boundary
    a, b = line['points'][:2]
    dx = b['time'] - a['time']
    return a['value'] if not dx else a['value'] + (b['value'] - a['value']) * (timestamp - a['time']) / dx


def clean_rr(entry: float, stop: float, target: float, side: str, fee: float,
             slippage: float, funding_rate: float = 0., tick_size: float = 0.) -> tuple[float, float, float]:
    sign = 1 if side == 'long' else -1
    actual_entry = rounded(entry * (1 + sign * slippage),tick_size,up=sign>0) if tick_size else entry*(1+sign*slippage)
    rounded_stop = rounded(stop,tick_size,up=sign<0) if tick_size else stop
    rounded_target = rounded(target,tick_size,up=sign<0) if tick_size else target
    stop_exit_raw=rounded_stop*(1-sign*slippage)
    target_exit_raw=rounded_target*(1-sign*slippage)
    stop_exit = rounded(stop_exit_raw,tick_size,up=sign<0) if tick_size else stop_exit_raw
    target_exit = rounded(target_exit_raw,tick_size,up=sign<0) if tick_size else target_exit_raw
    reserve = max(0., sign * funding_rate) * actual_entry
    risk = sign * (actual_entry - stop_exit) + fee * (actual_entry + stop_exit) + reserve
    reward = sign * (target_exit - actual_entry) - fee * (actual_entry + target_exit) - reserve
    return (reward / risk if risk > 0 else 0.), risk, reward


def prepare_signal(signal: Signal, variant: Variant, entry: float, fee: float,
                   slippage: float, funding_rate: float = 0., tick_size: float = 0.) -> tuple[bool, str, dict]:
    """Cap the requested reward at the shape projection; reject if net RR fails."""
    sign = 1 if signal.side == 'long' else -1
    stop = signal.stop
    entry_atr=float(signal.filter_results.get('entry_atr',signal.atr) or signal.atr)
    if not isfinite(entry) or entry_atr <= 0 or sign * (entry - stop) < MIN_STOP_ATR * entry_atr:
        return False, 'Дистанция до стопа меньше 0,8 ATR', {}
    projection = signal.target
    rr, risk, reward = clean_rr(entry, stop, projection, signal.side, fee, slippage, funding_rate,tick_size)
    if reward <= 0 or rr + 1e-12 < variant.net_rr:
        return False, f'Проекция фигуры не обеспечивает чистое RR {variant.net_rr:g}:1', {
            'net_rr': rr, 'risk_per_unit': risk, 'reward_per_unit': reward,
            'projection': projection, 'required_net_rr': variant.net_rr}
    # Find the nearest executable target that achieves the selected net RR,
    # while keeping the original measured projection as a hard ceiling.
    lo, hi = entry, projection
    for _ in range(60):
        mid=(lo+hi)/2
        mid_rr,_,_=clean_rr(entry,stop,mid,signal.side,fee,slippage,funding_rate,tick_size)
        if mid_rr >= variant.net_rr:
            hi=mid
        else:
            lo=mid
    target=hi
    selected_rr=0.;selected_reward=0.
    for ticks in range(0,7):
        candidate=hi+sign*ticks*tick_size
        actual=rounded(candidate,tick_size,up=sign<0) if tick_size else candidate
        if sign*(projection-actual)<-1e-12:
            break
        selected_rr,_,selected_reward=clean_rr(entry,stop,actual,signal.side,fee,slippage,funding_rate,tick_size)
        target=actual
        if selected_rr+1e-12>=variant.net_rr:
            break
    if selected_rr+1e-12<variant.net_rr:
        return False, f'Округление цены не сохраняет чистое RR {variant.net_rr:g}:1', {
            'net_rr':selected_rr,'projection':projection,'required_net_rr':variant.net_rr}
    signal.target=target
    signal.filter_results['required_net_rr']=variant.net_rr
    return True, '', {'net_rr': selected_rr, 'risk_per_unit': risk,
                      'reward_per_unit': selected_reward, 'projection': projection,
                      'target': target, 'required_net_rr': variant.net_rr}


def gate(metrics: dict, doubled_slippage_net: float) -> tuple[bool, str]:
    if metrics.get('closed', 0) < 100:
        return False, 'недостаточно данных'
    if (metrics.get('net', 0) <= 0 or metrics.get('profit_factor', 0) < 1.2
            or metrics.get('max_drawdown', 1) > .10 or doubled_slippage_net <= 0):
        return False, 'проверка не пройдена'
    return True, 'допущена к виртуальной торговле'


def choose_variant(results: list[dict]) -> dict | None:
    """Select on validation only; require positive net and DD <= 10%."""
    eligible = [r for r in results if r.get('validation', {}).get('net', 0) > 0
                and r.get('validation', {}).get('max_drawdown', 1) <= .10]
    return max(eligible, key=lambda r: (r['validation']['net'], r['id']), default=None)
