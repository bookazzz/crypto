"""Synthetic scanner load check: 40x4 streams, 200 active and 12k saved setups."""
import asyncio
import json
import math
from pathlib import Path
import sys
import tempfile
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from backend.engine import Engine
from backend.models import Candle, Instrument, Signal, now_ms
from backend.service import Scanner
from backend.storage import Store


class QuietBybit:
    offset = 0
    fee_mode = "standard"

    async def close(self):
        pass


def main():
    async def run():
        tempdir=tempfile.TemporaryDirectory(prefix="crypto-load-smoke-")
        store = Store(str(Path(tempdir.name)/"load.sqlite3"))
        engine = Engine(store)
        scanner = Scanner(engine, QuietBybit())
        now = now_ms()
        symbols = [f"SYN{i:03}USDT" for i in range(40)]
        timeframes = engine.settings.timeframes
        scanner.symbols = symbols
        engine.connected = True

        for symbol in symbols:
            engine.instruments[symbol] = Instrument(symbol, .001, .001, 5, .01)
            engine.fees[symbol] = {"rate": .00055, "time": now, "source": "test"}

        rows = []
        for index in range(12_000):
            symbol = symbols[index % len(symbols)]
            timeframe = timeframes[(index // len(symbols)) % len(timeframes)]
            active = index < 200
            interval = int(timeframe) * 60_000
            # The known shape must exist before the candle that confirms it.
            # Every timeframe closes on a different boundary in this burst.
            detected_bar = now // interval * interval - 2 * interval
            detected = detected_bar + interval if active else now - index
            rows.append({
                "id": f"setup-{index:05}", "symbol": symbol,
                "timeframe": timeframe, "pattern": "ascending_triangle",
                "side": "long", "status": "wait_breakout" if active else "expired",
                "detected_at": detected, "updated_at": detected,
                "event_at": detected, "reason": "", "transitions": [],
                "snapshot": {"start": detected_bar - 5 * interval,
                    "detected_on_bar": detected_bar, "boundary": 100.0,
                    "stop": 95.0, "target": 112.0, "atr": 2.0,
                    "height": 12.0, "lines": [], "pivots": []},
            })
        store.save_setups(rows)
        scanner.active_setup_cache = None

        geometry=[{"label":"support","points":[{"time":index*300_000,"value":100+index/100}
            for index in range(40)]}]
        signals=[Signal(f"LOAD{i:02}USDT","15","ascending_triangle","long",now-i,0,
            100,98,112,2,lines=geometry,pivots=[{"time":j*300_000,"price":100+j/100}
            for j in range(60)],filter_results={"decision_snapshot":{"bars":[[j,100,101,99,100]
            for j in range(120)]},"volume_pass":True}) for i in range(50)]
        engine.persist(signals)
        summary_snapshot=scanner.snapshot_payload(limit=50,setup_limit=50,view="summary")
        summary_bytes=len(json.dumps(summary_snapshot,separators=(",",":"),ensure_ascii=False).encode())
        summary_section_sizes={key:len(json.dumps(value,separators=(",",":"),ensure_ascii=False).encode())
                               for key,value in summary_snapshot.items()}

        samples = []
        crossing_cards = set()
        for index, symbol in enumerate(symbols):
            event_time = now + index + 1
            message = {"topic": f"tickers.{symbol}", "ts": event_time,
                "data": {"lastPrice": "100.10", "fundingRate": "0",
                         "nextFundingTime": str(event_time + 60_000)}}
            started = time.perf_counter()
            await scanner.message(message)
            snapshot = scanner.snapshot_payload(limit=50, setup_limit=300)
            elapsed_ms = (time.perf_counter() - started) * 1000
            samples.append(elapsed_ms)
            assert snapshot["setups"]
            if any(row["symbol"] == symbol and row.get("current", {}).get("status") == "crossing"
                   for row in snapshot["setups"]):
                crossing_cards.add(symbol)

        ordered = sorted(samples)
        p95 = ordered[math.ceil(.95 * len(ordered)) - 1]

        # Exercise real confirmation and both geometry passes across all
        # 40 x 4 streams, with enough closed history for pattern detection.
        active_setups_before_burst = len(store.active_setups())
        scanner.detection_workers = [asyncio.create_task(scanner.detection_worker())
                                     for _ in range(scanner.detection_processes)]
        scanner.candle_workers = [asyncio.create_task(scanner.candle_worker())
                                  for _ in range(16)]
        messages = []
        for symbol in symbols:
            for timeframe in timeframes:
                interval = int(timeframe) * 60_000
                close_bar = now // interval * interval - interval
                prior_time = close_bar - interval
                history=[Candle(prior_time-(299-index)*interval,100,100.1,99.9,100,1)
                    for index in range(300)]
                prior=history[-1]
                scanner.buffers[(symbol, timeframe)] = history
                store.set_cursor(symbol, timeframe, prior.time)
                messages.append({"topic": f"kline.{timeframe}.{symbol}", "data": [{
                    "start": close_bar, "open": 100, "high": 101, "low": 99,
                    "close": 100.5, "volume": 2, "confirm": True}]})
        close_started = time.perf_counter()
        await asyncio.gather(*(scanner.enqueue_candle(message, wait=True)
                               for message in messages))
        close_processing_ms = (time.perf_counter() - close_started) * 1000
        confirmed_rows = sorted(scanner.live_candle_transition_latency_ms)
        confirm_p95_ms = (confirmed_rows[math.ceil(.95 * len(confirmed_rows)) - 1]
                          if confirmed_rows else 0)
        await scanner.detection_queue.join()
        confirmed_setup_count = store.db.execute(
            "SELECT count(*) FROM setups WHERE status='breakout_confirmed'").fetchone()[0]
        for task in scanner.candle_workers + scanner.detection_workers:
            task.cancel()
        await asyncio.gather(*(scanner.candle_workers + scanner.detection_workers),
                             return_exceptions=True)
        scanner.candle_workers = []
        scanner.detection_workers = []
        assert len({(symbol, timeframe) for symbol in symbols for timeframe in timeframes
                    if store.scan_cursor(symbol, timeframe) is not None}) == 160
        detection_rows=sorted(scanner.live_detection_latency_ms)
        detection_p95_ms=(detection_rows[math.ceil(.95*len(detection_rows))-1]
                          if detection_rows else 0)
        await asyncio.to_thread(scanner.detection_executor.shutdown,wait=True,cancel_futures=True)
        scanner.detection_executor=None
        result = {"streams": len(symbols) * len(timeframes),
            "saved_setups": store.db.execute("SELECT count(*) FROM setups").fetchone()[0],
            "active_setups_before_burst": active_setups_before_burst,
            "confirmed_setups_after_burst": confirmed_setup_count,
            "quote_to_early_card_p95_ms": round(p95, 2),
            "quote_to_early_card_max_ms": round(max(samples), 2),
            "symbols_with_crossing_cards": len(crossing_cards),
            "summary_snapshot_bytes_50_signals_50_setups": summary_bytes,
            "summary_section_sizes": summary_section_sizes,
            "160_candle_burst_enqueue_ms": round(close_processing_ms, 2),
            "candle_to_transition_p95_ms": confirm_p95_ms,
            "candle_to_detection_publish_p95_ms": detection_p95_ms,
            "live_candle_queue_wait_p95_ms": (sorted(scanner.live_candle_queue_wait_ms)[
                math.ceil(.95 * len(scanner.live_candle_queue_wait_ms)) - 1]
                if scanner.live_candle_queue_wait_ms else 0),
            "detection_queue_wait_p95_ms": (sorted(scanner.detection_queue_wait_ms)[
                math.ceil(.95 * len(scanner.detection_queue_wait_ms)) - 1]
                if scanner.detection_queue_wait_ms else 0)}
        print(json.dumps(result))
        store.close()
        tempdir.cleanup()
        assert result["saved_setups"] >= 12_000
        assert active_setups_before_burst == 200
        assert summary_bytes <= 120 * 1024, (
            f"summary snapshot exceeded 120 KB: {summary_bytes} bytes")
        assert confirmed_setup_count == 200, (
            f"only {confirmed_setup_count} of 200 known setups were confirmed")
        assert len(crossing_cards) == len(symbols), (
            f"early cards missed on {len(symbols)-len(crossing_cards)} symbols")
        assert p95 <= 500, f"quote-to-early-card p95 exceeded 500 ms: {p95:.2f} ms"
        assert confirm_p95_ms <= 1_000, (
            f"candle-to-confirmed-card p95 exceeded 1 s: {confirm_p95_ms:.2f} ms")
        assert detection_p95_ms <= 10_000, (
            f"candle-to-detection p95 exceeded 10 s: {detection_p95_ms:.2f} ms")

    asyncio.run(run())


if __name__ == "__main__":
    main()
