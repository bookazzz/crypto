"""Causal, immutable outcome snapshots and summaries for confirmed 15m forecasts."""
from __future__ import annotations

from collections import defaultdict
from math import isfinite
from statistics import mean

from .models import Candle, rounded

VERSION = 'forecast-outcomes-v1'
RULE_VERSION = 'geometry-v1+figures-context-v4'
HORIZONS_MINUTES = (5, 15, 30)
MIN_SAMPLE_SIZE = 100
BAR_MS = 5 * 60_000


def make_observation(signal: dict, context: dict, tick_size: float = 0.) -> dict | None:
    """Freeze the available input for a new, live 15m forecast."""
    filters = signal.get('filter_results') or {}
    delivery = filters.get('delivery') or {}
    confirmation = filters.get('confirmation_5m') or {}
    if (signal.get('timeframe') != '15' or filters.get('confirmation_pass') is not True
            or delivery.get('mode') == 'history' or signal.get('recovered')
            or delivery.get('recovered')):
        return None
    price = context.get('current_price')
    available_at = int(context.get('as_of') or 0)
    quote_at = int(context.get('current_quote_time') or 0)
    side = signal.get('side')
    stop = signal.get('stop')
    target = signal.get('target')
    try:
        price, stop, target = float(price), float(stop), float(target)
    except (TypeError, ValueError):
        price = stop = target = None
    sign = 1 if side == 'long' else -1
    status, reason = 'pending', ''
    entry = price
    if not available_at:
        status, reason = 'data_missing', 'В снимке нет времени доступности прогноза'
    elif entry is None or not isfinite(entry) or entry <= 0:
        status, reason = 'no_fill', 'При публикации не было актуальной цены входа'
    elif stop is None or target is None or sign * (entry - stop) <= 0 or sign * (target - entry) <= 0:
        status, reason = 'no_fill', 'На момент публикации сценарий уже вышел за стоп или цель'
    elif (context.get('quality') or {}).get('quote', {}).get('status') != 'valid':
        status, reason = 'no_fill', 'Котировка при публикации была устаревшей или недоступной'
    elif quote_at < int(signal.get('event_at') or signal.get('time') or 0):
        status, reason = 'no_fill', 'После подтверждения сценария ещё не было новой цены для входа'

    market_context = filters.get('market_context') or {}
    background = context.get('market_background') or {}
    period10 = (background.get('periods') or {}).get('10') or {}
    own_direction = period10.get('direction')
    correlation = (background.get('btc_correlation') or {}).get('value')
    btc_direction = period10.get('btc_direction')
    try:
        btc_correlation = float(correlation) if correlation is not None else None
    except (TypeError, ValueError):
        btc_correlation = None
    adverse_btc = None
    if btc_correlation is not None and abs(btc_correlation) >= .7 and btc_direction in ('up', 'down'):
        adverse_btc = ((1 if btc_direction == 'up' else -1) * (1 if side == 'long' else -1)
                       * (1 if btc_correlation >= 0 else -1)) < 0
    features = {
        'market_scenario': market_context.get('scenario', 'unconfirmed'),
        'hourly_context_pass': (market_context.get('eligible') if
            market_context.get('scenario') in ('continuation','reversal') else None),
        'volume_pass': filters.get('volume_pass'),
        'coin_10d_direction': own_direction,
        'coin_10d_supports_side': (own_direction == ('up' if side == 'long' else 'down')
                                   if own_direction in ('up', 'down') else None),
        'btc_10d_direction': btc_direction,
        'btc_correlation_30d': btc_correlation,
        'btc_10d_adverse_when_strongly_correlated': adverse_btc,
        'confirmation_5m_pass': filters.get('confirmation_pass'),
        'detection_profile': filters.get('detection_profile'),
        'data_quality': context.get('quality'),
    }
    cost_context = {
        'fee_per_side': context.get('fee_per_side') if context.get('fee_available') else None,
        'slippage_per_side': context.get('slippage_per_side'),
        'funding_rate': (context.get('funding') or {}).get('rate'),
        'funding_next_at': (context.get('funding') or {}).get('next_time'),
        'funding_as_of': (context.get('funding') or {}).get('as_of'),
        'tick_size': float(tick_size or 0.),
        'execution_model': 'entry_at_first_available_context_quote_then_5m_closed_ohlc; adverse_slippage; taker_fee; estimated_funding',
    }
    return {
        'id': signal.get('id'), 'symbol': signal.get('symbol'), 'timeframe': '15',
        'pattern': signal.get('pattern'), 'side': side,
        'event_at': int(signal.get('event_at') or signal.get('time') or 0),
        'available_at': available_at,
        'rules_version': f"geometry-v1+{signal.get('strategy_version') or RULE_VERSION.split('+',1)[-1]}",
        'evaluation_version': VERSION,
        'entry_price': entry, 'stop': stop, 'target': target,
        'features': features, 'costs': cost_context,
        'status': status, 'reason': reason,
    }


def _exit_result(observation: dict, label: str, raw_exit: float | None,
                 exit_at: int, horizon: int, *, ambiguous=False, conservative=False) -> dict:
    side = observation.get('side')
    sign = 1 if side == 'long' else -1
    raw_entry = float(observation['entry_price'])
    costs = observation.get('costs') or {}
    fee = costs.get('fee_per_side')
    slip = costs.get('slippage_per_side')
    tick = float(costs.get('tick_size') or 0.)
    try:
        fee = float(fee) if fee is not None else None
        slip = float(slip) if slip is not None else None
    except (TypeError, ValueError):
        fee = slip = None
    entry = (rounded(raw_entry * (1 + sign * slip),tick,up=sign>0)
             if slip is not None and tick>0 else
             raw_entry * (1 + sign * slip) if slip is not None else raw_entry)
    exit_price = None
    net = net_r = None
    price_pnl = None
    if raw_exit is not None and isfinite(float(raw_exit)):
        exit_price = (rounded(float(raw_exit) * (1 - sign * slip), tick, up=sign < 0)
                      if slip is not None and tick > 0 else
                      float(raw_exit) * (1 - sign * slip) if slip is not None else float(raw_exit))
        price_pnl = sign * (exit_price - entry)
        if fee is not None and slip is not None:
            rate = float(costs.get('funding_rate') or 0.)
            settlement = int(costs.get('funding_next_at') or 0)
            funding_cash = -sign * entry * rate if settlement and settlement <= exit_at else 0.
            net = price_pnl - fee * (entry + exit_price) + funding_cash
            stop = float(observation['stop'])
            stop_exit_raw = stop * (1 - sign * slip)
            stop_exit = (rounded(stop_exit_raw, tick, up=sign < 0)
                         if tick > 0 else stop_exit_raw)
            risk_funding = max(0., -funding_cash)
            risk_per_unit = sign * (entry - stop_exit) + fee * (entry + stop_exit) + risk_funding
            if risk_per_unit > 0:
                net_r = net / risk_per_unit
    return {
        'horizon_minutes': horizon, 'status': 'ambiguous' if ambiguous and not conservative else label,
        'label': label, 'exit_at': int(exit_at), 'exit_price': exit_price,
        'price_pnl_per_unit': price_pnl, 'net_pnl_per_unit': net,
        'net_r': net_r, 'ambiguous': bool(ambiguous),
        'conservative_label': label if ambiguous else None,
    }


def evaluate_horizons(observation: dict, candles: list[Candle], now: int) -> list[dict]:
    """Return any newly complete 5/15/30m outcomes using only closed 5m bars."""
    if observation.get('status') in ('no_fill', 'data_missing'):
        return [{'horizon_minutes': h, 'status': observation['status'],
                 'label': observation['status'], 'reason': observation.get('reason')}
                for h in HORIZONS_MINUTES]
    try:
        entry = float(observation['entry_price'])
        stop, target = float(observation['stop']), float(observation['target'])
        available = int(observation['available_at'])
    except (KeyError, TypeError, ValueError):
        return [{'horizon_minutes': h, 'status': 'data_missing', 'label': 'data_missing',
                 'reason': 'В снимке недостаточно значений для расчёта'} for h in HORIZONS_MINUTES]
    if available <= 0 or entry <= 0:
        return []
    sign = 1 if observation.get('side') == 'long' else -1
    start_at = ((available + BAR_MS - 1) // BAR_MS) * BAR_MS
    closed = {c.time: c for c in candles if c.time >= start_at and c.time + BAR_MS <= now}
    results = []
    for horizon in HORIZONS_MINUTES:
        deadline = start_at + horizon * 60_000
        if now < deadline:
            continue
        expected_times = range(start_at, deadline, BAR_MS)
        bars = [closed.get(t) for t in expected_times]
        if not bars or any(c is None for c in bars):
            results.append({'horizon_minutes': horizon, 'status': 'data_missing',
                            'label': 'data_missing', 'reason': 'Нет полного непрерывного ряда закрытых 5м свечей'})
            continue
        exit_result = None
        for bar in bars:
            stop_hit = bar.low <= stop if sign > 0 else bar.high >= stop
            target_hit = bar.high >= target if sign > 0 else bar.low <= target
            if stop_hit and target_hit:
                exit_raw = min(bar.open, stop) if sign > 0 else max(bar.open, stop)
                exit_result = _exit_result(observation, 'stop', exit_raw, bar.time + BAR_MS,
                                           horizon, ambiguous=True)
                exit_result['conservative'] = _exit_result(observation, 'stop', exit_raw,
                    bar.time + BAR_MS, horizon, ambiguous=True, conservative=True)
                break
            if stop_hit:
                raw = min(bar.open, stop) if sign > 0 else max(bar.open, stop)
                exit_result = _exit_result(observation, 'stop', raw, bar.time + BAR_MS, horizon)
                break
            if target_hit:
                raw = max(bar.open, target) if sign > 0 else min(bar.open, target)
                exit_result = _exit_result(observation, 'target', raw, bar.time + BAR_MS, horizon)
                break
        if exit_result is None:
            last = bars[-1]
            exit_result = _exit_result(observation, 'timeout', last.close,
                                       last.time + BAR_MS, horizon)
        results.append(exit_result)
    return results


def _cohort_summary(rows: list[dict]) -> dict:
    completed = [r for r in rows if r.get('status') in ('target', 'stop', 'timeout', 'ambiguous')]
    fully_labelled = [r for r in completed if r.get('status') != 'ambiguous']
    target_count = sum(r.get('label') == 'target' for r in fully_labelled)
    usable_r = [float(r['net_r']) for r in fully_labelled
                if r.get('net_r') is not None and isfinite(float(r['net_r']))]
    conservative_r = [float((r.get('conservative') or {}).get('net_r')) for r in completed
                      if r.get('status') == 'ambiguous'
                      and (r.get('conservative') or {}).get('net_r') is not None]
    times = [int(r['available_at']) for r in rows if r.get('available_at')]
    return {
        'status': 'ready' if len(fully_labelled) >= MIN_SAMPLE_SIZE else 'insufficient_data',
        'sample_size': len(fully_labelled), 'completed': len(completed),
        'target_count': target_count,
        'target_rate': target_count / len(fully_labelled) if fully_labelled else None,
        'stop_count': sum(r.get('label') == 'stop' for r in fully_labelled),
        'timeout_count': sum(r.get('label') == 'timeout' for r in fully_labelled),
        'ambiguous_count': sum(r.get('status') == 'ambiguous' for r in completed),
        'no_fill_count': sum(r.get('status') == 'no_fill' for r in rows),
        'data_missing_count': sum(r.get('status') == 'data_missing' for r in rows),
        'avg_net_r': mean(usable_r) if usable_r else None,
        'ambiguous_conservative_avg_net_r': mean(conservative_r) if conservative_r else None,
        'period_days': ((max(times) - min(times)) / 86_400_000 if len(times) > 1 else None),
        'minimum_sample_size': MIN_SAMPLE_SIZE,
    }


def summarize_forecasts(rows: list[dict], *, symbol: str, pattern: str, side: str,
                        rule_version: str, market_scenario: str | None = None) -> dict:
    """Summarize the same pattern/direction/rules and one frozen filter cohort."""
    observations = [r for r in rows if r.get('symbol') and r.get('pattern') == pattern
        and r.get('side') == side and r.get('rules_version') == rule_version]
    if market_scenario in ('continuation', 'reversal'):
        observations = [r for r in observations
                        if (r.get('features') or {}).get('market_scenario') == market_scenario]
    all_by_horizon = defaultdict(list)
    symbols_by_horizon = defaultdict(lambda: defaultdict(list))
    for observation in observations:
        for outcome in observation.get('outcomes', []):
            all_by_horizon[outcome['horizon_minutes']].append(outcome | {
                'available_at': observation.get('available_at')})
            symbols_by_horizon[observation.get('symbol')][outcome['horizon_minutes']].append(outcome | {
                'available_at': observation.get('available_at')})
    symbol_summaries={name:{str(h):_cohort_summary(by_horizon[h]) for h in HORIZONS_MINUTES}
                      for name,by_horizon in symbols_by_horizon.items() if name}
    return {
        'status': 'ready' if any(_cohort_summary(all_by_horizon[h])['status'] == 'ready'
                                 for h in HORIZONS_MINUTES) else 'insufficient_data',
        'minimum_sample_size': MIN_SAMPLE_SIZE,
        'basis': {'pattern': pattern, 'side': side, 'rules_version': rule_version,
                  'market_scenario': market_scenario, 'symbol': symbol},
        'horizons': {str(h): {'all_symbols': _cohort_summary(all_by_horizon[h]),
                              'this_symbol': _cohort_summary(symbols_by_horizon.get(symbol,{}).get(h,[]))}
                     for h in HORIZONS_MINUTES},
        'symbols':symbol_summaries,
    }


def build_statistics_index(rows: list[dict]) -> dict:
    grouped=defaultdict(list)
    for observation in rows:
        pattern,side,rules_version=(observation.get(key) for key in
                                   ('pattern','side','rules_version'))
        if not pattern or side not in ('long','short') or not rules_version:
            continue
        grouped[(pattern,side,rules_version,None)].append(observation)
        scenario=(observation.get('features') or {}).get('market_scenario')
        if scenario in ('continuation','reversal'):
            grouped[(pattern,side,rules_version,scenario)].append(observation)
    return {key:summarize_forecasts(values,symbol='',pattern=key[0],side=key[1],
                                    rule_version=key[2],market_scenario=key[3])
            for key,values in grouped.items()}


def _factor_rows(observations, horizon, factor):
    present, rejected, unknown = [], [], 0
    for observation in observations:
        value = (observation.get('features') or {}).get(factor)
        outcomes = [row for row in observation.get('outcomes', [])
                    if row.get('horizon_minutes') == horizon]
        for outcome in outcomes:
            if value is True:
                present.append(outcome | {'available_at': observation.get('available_at')})
            elif value is False:
                rejected.append(outcome | {'available_at': observation.get('available_at')})
            else:
                unknown += 1
    kept = _cohort_summary(present)
    filtered = _cohort_summary(rejected)
    return {
        'factor': factor, 'passing': kept, 'failing': filtered, 'unknown_count': unknown,
        'excluded_targets': filtered['target_count'], 'excluded_stops': filtered['stop_count'],
        'interpretation': 'Описательное сравнение одинаковых сигналов; не доказывает причинный эффект.',
    }


def summarize_filter_ablation(rows: list[dict], *, rule_version: str | None = None) -> dict:
    """Compare frozen, one-factor cohorts without changing any entry parameters."""
    observations = [r for r in rows if not rule_version or r.get('rules_version') == rule_version]
    factors = ('hourly_context_pass', 'volume_pass', 'coin_10d_supports_side',
               'btc_10d_adverse_when_strongly_correlated')
    by_horizon = {}
    for horizon in HORIZONS_MINUTES:
        all_rows = [outcome | {'available_at': observation.get('available_at')}
                    for observation in observations for outcome in observation.get('outcomes', [])
                    if outcome.get('horizon_minutes') == horizon]
        by_horizon[str(horizon)] = {
            'all_confirmed': _cohort_summary(all_rows),
            'factors': {factor: _factor_rows(observations, horizon, factor) for factor in factors},
        }
    return {'rules_version': rule_version, 'horizons': by_horizon,
            'candidate_count': len(observations),
            'warning': 'Факторные группы описывают наблюдения; для оценки эффекта нужна независимая хронологическая проверка.'}
