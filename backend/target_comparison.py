"""Compare closer take-profits on the original entries, sizes and stops.

This is a fixed-entry counterfactual, not a new portfolio strategy backtest:
earlier exits do not cause additional entries or recalculate position sizes.
The live scanner and original portfolio reports are never modified.
"""
import argparse
from collections import deque
from dataclasses import asdict
import json
import math
from pathlib import Path
import sqlite3
import statistics

from .engine import Engine
from .models import Candle, Instrument, Settings, rounded
from .portfolio_backtest import DEFAULT_DIR, MINUTE, csv_file, write_json
from .storage import Store

CAPS = (0.5, 0.75, 1.0, 1.5, 2.0)


def capped_target(position, cap, instrument):
    """R is the price distance from filled entry to the unchanged stop."""
    if cap is None:
        return position['target']
    if not math.isfinite(cap) or cap <= 0:
        raise ValueError('Target cap must be positive')
    sign = position['sign']
    risk_distance = sign * (position['entry'] - position['stop'])
    original_distance = sign * (position['target'] - position['entry'])
    distance = min(original_distance, cap * risk_distance)
    target = rounded(position['entry'] + sign * distance,
                     instrument.tick_size, up=sign < 0)
    if sign * (target - position['entry']) <= 0:
        raise ValueError('Target is smaller than the instrument price step')
    return target


def simulate_exit(original, target, minutes, funding, instrument, settings):
    """Use Engine's actual gap, stop-first, slippage, fee and funding rules."""
    store = Store(':memory:')
    engine = Engine(store, settings)
    symbol = original['symbol']
    engine.instruments[symbol] = instrument
    engine.connected = True
    engine.fees[symbol] = {'rate': original['fee_rate'],
                           'time': original['opened'], 'source': 'bybit_vip0_standard'}
    fields = ('id', 'symbol', 'side', 'sign', 'entry', 'stop', 'qty', 'fee_rate',
              'entry_fee', 'opened', 'timeframe', 'pattern')
    position = {key: original[key] for key in fields}
    position.update(target=target, funding=0., funding_cursor=original['opened'],
                    last_time=original['opened'], recovered=False)
    engine.positions.append(position)
    engine.cash -= position['entry_fee']
    # Original entries occur after the first minute's opening tick. Do not
    # manufacture a second opening tick after seeding that same entry.
    engine.market_times[symbol] = position['opened']
    engine.prices[symbol] = minutes[0].open if minutes else position['entry']
    payments = deque(row for row in funding if row[0] > position['opened'])
    try:
        for candle in minutes:
            while payments and payments[0][0] <= candle.time:
                timestamp, rate, mark = payments.popleft()
                if engine.positions:
                    if mark is None:
                        raise ValueError(f'Missing cached funding mark: {symbol} {timestamp}')
                    engine.funding_payment(symbol, rate, timestamp, mark)
            engine.tick(symbol, candle.open, candle.time)
            engine.bar(symbol, candle, candle.time + MINUTE - 1, False)
            if engine.trades:
                trade = engine.trades[0]
                return trade | {'status': 'closed', 'marked_net': trade['net']}
        marked_net = (engine.prices[symbol] - position['entry']) * position['qty'] * position['sign']
        marked_net += position['funding'] - position['entry_fee']
        return position | {'status': 'open', 'net': None, 'marked_net': marked_net}
    finally:
        store.close()


def summarize(label, cap, results, original_by_id):
    closed = [row for row in results if row['status'] == 'closed']
    wins = [row for row in closed if row['net'] > 0]
    losses = [row for row in closed if row['net'] <= 0]
    gains = sum(row['net'] for row in wins)
    lost = -sum(row['net'] for row in losses)
    return {
        'variant': label, 'cap_r': cap, 'entries': len(results),
        'closed': len(closed), 'open': len(results) - len(closed),
        'wins': len(wins), 'win_rate': len(wins) / len(closed) if closed else 0.,
        'tp_count': sum(row['reason'] == 'target' for row in closed),
        'sl_count': sum(row['reason'] == 'stop' for row in closed),
        'tp_net_nonpositive': sum(row['reason'] == 'target' and row['net'] <= 0 for row in closed),
        'realized_net': sum(row['net'] for row in closed),
        'net_including_open': sum(row['marked_net'] for row in results),
        'entry_fees': sum(row['entry_fee'] for row in results),
        'exit_fees': sum(row['exit_fee'] for row in closed),
        'funding': sum(row['funding'] for row in results),
        'avg_win': statistics.mean(row['net'] for row in wins) if wins else 0.,
        'avg_loss': statistics.mean(row['net'] for row in losses) if losses else 0.,
        'profit_factor': gains / lost if lost else None,
        'median_hold_minutes': statistics.median((row['closed'] - row['opened']) / MINUTE
                                                 for row in closed) if closed else None,
        'original_stops_to_tp': sum(row.get('reason') == 'target'
                                   and original_by_id[row['id']].get('reason') == 'stop'
                                   for row in results),
        'original_tp_to_stop': sum(row.get('reason') == 'stop'
                                  and original_by_id[row['id']].get('reason') == 'target'
                                  for row in results),
    }


def validate_baseline(results, originals):
    for row in results:
        original = originals[row['id']]
        if 'closed' in original:
            if row['status'] != 'closed' or row['closed'] != original['closed'] or row['reason'] != original['reason']:
                raise AssertionError(f'Baseline exit differs: {row["id"]}')
            for key in ('entry', 'exit', 'qty', 'gross', 'entry_fee', 'exit_fee', 'funding', 'net'):
                if not math.isclose(row[key], original[key], rel_tol=1e-10, abs_tol=1e-8):
                    raise AssertionError(f'Baseline {key} differs: {row["id"]}')
        elif row['status'] != 'open' or not math.isclose(row['funding'], original['funding'], abs_tol=1e-8):
            raise AssertionError(f'Baseline open position differs: {row["id"]}')


def compare(directory=DEFAULT_DIR, caps=CAPS):
    directory = Path(directory)
    summary = json.loads((directory / 'summary.json').read_text(encoding='utf-8'))
    originals = json.loads((directory / 'trades.json').read_text(encoding='utf-8'))
    originals += summary['total']['open_positions']
    original_by_id = {row['id']: row for row in originals}
    if len(originals) != len(original_by_id):
        raise ValueError('Duplicate original entries')
    settings = Settings(**summary['manifest']['settings'])
    end = summary['manifest']['end']
    market = sqlite3.connect((directory / 'market.sqlite3').resolve().as_uri() + '?mode=ro', uri=True)
    variants = [('original', None), *((f'cap_{cap:g}R', cap) for cap in caps)]
    results = {label: [] for label, _ in variants}
    funding = {symbol: market.execute('''
        SELECT f.time,f.rate,m.price FROM funding f LEFT JOIN marks m
        ON f.symbol=m.symbol AND f.time=m.time WHERE f.symbol=? ORDER BY f.time
        ''', (symbol,)).fetchall() for symbol in summary['manifest']['symbols']}
    try:
        for number, original in enumerate(originals, 1):
            instrument = Instrument(**summary['manifest']['instruments'][original['symbol']])
            horizon = original.get('closed', end - 1) // MINUTE * MINUTE
            rows = market.execute('SELECT time,o,h,l,c,v FROM minutes WHERE symbol=? AND time>=? AND time<=? ORDER BY time',
                                  (original['symbol'], original['opened'], horizon)).fetchall()
            if not rows or len(rows) != (horizon - original['opened']) // MINUTE + 1:
                raise ValueError(f'Incomplete execution history: {original["id"]}')
            minutes = [Candle(*row) for row in rows]
            for label, cap in variants:
                target = capped_target(original, cap, instrument)
                result = simulate_exit(original, target, minutes, funding[original['symbol']], instrument, settings)
                if 'closed' in original and (result['status'] != 'closed' or result['closed'] > original['closed']):
                    raise AssertionError('A closer target must not prolong an original closed position')
                results[label].append(result | {'variant': label, 'original_target': original['target'],
                                               'original_reason': original.get('reason', 'open')})
            if number % 50 == 0 or number == len(originals):
                print(f'Compared {number}/{len(originals)} original entries', flush=True)
    finally:
        market.close()
    validate_baseline(results['original'], original_by_id)
    rows = [summarize(label, cap, results[label], original_by_id) for label, cap in variants]
    closed_ids = {row['id'] for row in originals if 'closed' in row}
    closed_rows = [summarize(label, cap, [row for row in results[label] if row['id'] in closed_ids],
                             original_by_id) for label, cap in variants]
    baseline = rows[0]
    if not math.isclose(baseline['realized_net'], summary['total']['realized_net'], abs_tol=1e-8):
        raise AssertionError('Baseline realized result differs from the source report')
    if not math.isclose(baseline['net_including_open'], summary['total']['net_including_open'], abs_tol=1e-8):
        raise AssertionError('Baseline portfolio valuation differs from the source report')
    output = directory / 'closer_targets'
    output.mkdir(parents=True, exist_ok=True)
    report = {
        'mode': 'fixed_original_entries', 'period': {key: summary['manifest'][key] for key in ('start', 'end', 'symbols')},
        'settings': asdict(settings),
        'method': 'Original 324 entries, sizes and stops; target capped at a multiple of entry-to-stop price distance. Same minute candles, fees, slippage, funding and stop-first execution. Entry RR filter and capital decisions remain those of the original run; no extra trades are introduced after earlier exits.',
        'baseline_verified': True, 'variants': rows,
        'original_closed_cohort': {'entries': len(closed_ids), 'variants': closed_rows},
    }
    write_json(output / 'summary.json', report)
    write_json(output / 'trades.json', results)
    csv_file(output / 'comparison.csv', rows, list(rows[0]))
    csv_file(output / 'closed_comparison.csv', closed_rows, list(closed_rows[0]))
    csv_file(output / 'trades.csv', (row for label, _ in variants for row in results[label]),
             ['variant', 'id', 'symbol', 'timeframe', 'pattern', 'side', 'opened', 'closed', 'status',
              'entry', 'stop', 'target', 'original_target', 'qty', 'reason', 'original_reason',
              'entry_fee', 'exit_fee', 'funding', 'gross', 'net', 'marked_net'])
    return report


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--directory', type=Path, default=DEFAULT_DIR)
    args = parser.parse_args()
    result = compare(args.directory)
    print(json.dumps(result['variants'], ensure_ascii=False, indent=2))
