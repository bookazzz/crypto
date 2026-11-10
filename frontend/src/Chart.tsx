import { useEffect, useRef, useState } from 'react';
import {
  createChart, CandlestickSeries, LineSeries, createSeriesMarkers,
  ColorType, CrosshairMode, type UTCTimestamp, type Time,
} from 'lightweight-charts';
import type { Candle, Signal, Setup } from './types';
import { decimal } from './number_format';

type Bar = Candle & { time: UTCTimestamp };
type PriceLine = { key: string; price: number; color: string; title: string; width?: number; style?: number };
const CACHE_LIMIT = 32;
const candleCache = new Map<string, Candle[]>();

function remember(key: string, rows: Candle[]) {
  candleCache.delete(key);
  candleCache.set(key, rows.slice(-1500));
  while (candleCache.size > CACHE_LIMIT) candleCache.delete(candleCache.keys().next().value!);
}

function mergeRows(...groups: (Candle[] | null | undefined)[]) {
  const map = new Map<number, Candle>();
  for (const group of groups) for (const row of group ?? []) map.set(row.time, row);
  return [...map.values()].sort((a, b) => a.time - b.time).slice(-1500);
}

function sameCandle(a?: Candle, b?: Candle) {
  return !!a && !!b && a.time === b.time && a.open === b.open && a.high === b.high
    && a.low === b.low && a.close === b.close && a.volume === b.volume;
}

const toBar = (c: Candle): Bar => ({ ...c, time: Math.floor(c.time / 1000) as UTCTimestamp });

export function Chart({ signal, setup, symbol, tf, liveCandle, closedCandle,
  context, feedUpdatedAt }: {
    signal: Signal | null; setup: Setup | null; symbol: string; tf: string;
    liveCandle: Candle | null; closedCandle?: Candle | null; context?: any; feedUpdatedAt?: number;
  }) {
  const element = useRef<HTMLDivElement>(null);
  const chartRef = useRef<any>(null);
  const seriesRef = useRef<any>(null);
  const overlays = useRef(new Map<string, { series: any; signature: string }>());
  const priceLines = useRef(new Map<string, any>());
  const markerRef = useRef<any>(null);
  const rowsRef = useRef<Candle[]>([]);
  const sourceRef = useRef('');
  const requestRef = useRef<AbortController | null>(null);
  const paginationRef = useRef(false);
  const lastCandleAt = useRef(0);
  const restRepairAt = useRef(0);
  const loadedRangeRef = useRef('');
  const liveRef = useRef<Candle | null>(liveCandle);
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);
  const [showFigureLevels, setShowFigureLevels] = useState(false);
  const [hoverPrice, setHoverPrice] = useState<{ value: number; x: number; y: number } | null>(null);

  liveRef.current = liveCandle;
  useEffect(() => setShowFigureLevels(false), [setup?.id]);

  useEffect(() => {
    if (!element.current) return;
    const chart = createChart(element.current, {
      autoSize: true, height: 390,
      layout: { background: { type: ColorType.Solid, color: '#12151a' }, textColor: '#848e9c', fontFamily: 'Manrope, sans-serif', fontSize: 11 },
      grid: { vertLines: { color: '#20242b' }, horzLines: { color: '#20242b' } },
      crosshair: { mode: CrosshairMode.Normal,
        vertLine: { color: '#68717d', width: 1, style: 2, visible: true, labelVisible: true, labelBackgroundColor: '#2b3139' },
        horzLine: { color: '#68717d', width: 1, style: 2, visible: true, labelVisible: true, labelBackgroundColor: '#2b3139' } },
      timeScale: { timeVisible: true, tickMarkFormatter: (time: Time) => typeof time === 'number'
        ? new Date(time * 1000).toLocaleTimeString('ru-RU', { timeZone: 'Europe/Moscow', hour: '2-digit', minute: '2-digit' }) : null },
      localization: { locale: 'ru-RU', timeFormatter: (time: Time) => typeof time === 'number'
        ? new Date(time * 1000).toLocaleString('ru-RU', { timeZone: 'Europe/Moscow' }) : String(time) },
    });
    void document.fonts.load('500 12px Manrope', 'Русский 0123456789')
      .finally(() => chart.applyOptions({ layout: { fontFamily: 'Manrope, sans-serif', fontSize: 11 } }));
    const series = chart.addSeries(CandlestickSeries, {
      upColor: '#20b26c', downColor: '#ef454a', borderVisible: false,
      wickUpColor: '#20b26c', wickDownColor: '#ef454a',
      priceFormat: { type: 'custom', minMove: 0.00000001, formatter: (value: number) => decimal(value) },
    });
    const onCrosshairMove = (event: any) => {
      if (!event.point) { setHoverPrice(null); return; }
      const value = series.coordinateToPrice(event.point.y);
      if (value == null || !Number.isFinite(value)) { setHoverPrice(null); return; }
      const width = element.current?.clientWidth ?? 500, height = element.current?.clientHeight ?? 390;
      setHoverPrice({ value, x: Math.max(8, Math.min(event.point.x + 12, width - 118)),
        y: Math.max(8, Math.min(event.point.y - 18, height - 30)) });
    };
    chart.subscribeCrosshairMove(onCrosshairMove);
    chartRef.current = chart;
    seriesRef.current = series;

    const loadOlder = async (range: any) => {
      if (!range || range.from > 24 || paginationRef.current || rowsRef.current.length === 0) return;
      const source = sourceRef.current;
      const [pair, timeframe] = source.split('|');
      const first = rowsRef.current[0], visible = chart.timeScale().getVisibleRange();
      const signal = requestRef.current?.signal;
      paginationRef.current = true;
      try {
        const response = await fetch(`/api/candles/${pair}/${timeframe}?limit=300&end=${first.time}`,
          { signal });
        if (!response.ok) throw Error();
        const older: Candle[] = await response.json();
        if (signal?.aborted || sourceRef.current !== source) return;
        if (older.length) {
          rowsRef.current = mergeRows(older, rowsRef.current);
          series.setData(rowsRef.current.map(toBar));
          remember(source, rowsRef.current);
          if (visible) chart.timeScale().setVisibleRange(visible);
        }
      } catch { /* History can be retried on the next scroll. */ }
      finally { paginationRef.current = false; }
    };
    chart.timeScale().subscribeVisibleLogicalRangeChange(loadOlder);
    return () => {
      requestRef.current?.abort();
      chart.timeScale().unsubscribeVisibleLogicalRangeChange(loadOlder);
      chart.unsubscribeCrosshairMove(onCrosshairMove);
      overlays.current.forEach(({ series: overlay }) => { try { chart.removeSeries(overlay); } catch { /* already removed */ } });
      chart.remove();
      chartRef.current = null; seriesRef.current = null;
      overlays.current.clear(); priceLines.current.clear(); markerRef.current = null;
      setHoverPrice(null);
    };
  }, []);

  const shapeStart = setup?.snapshot.start ?? signal?.start ?? null;
  const shapeEnd = setup?.snapshot.detected_on_bar ?? signal?.time ?? null;
  const rangeToken = `${symbol}|${tf}|${setup?.id ?? signal?.id ?? 'latest'}|${shapeStart ?? ''}|${shapeEnd ?? ''}`;
  useEffect(() => {
    const chart = chartRef.current, series = seriesRef.current;
    if (!chart || !series || !symbol) return;
    const pairKey = `${symbol}|${tf}`;
    const interval = Number(tf) * 60_000;
    const start = shapeStart == null ? null : Math.max(0, shapeStart - 24 * interval);
    const endBase = shapeEnd ?? Date.now();
    const end = shapeEnd == null ? null : endBase + 12 * interval;
    const rangeKey = `${pairKey}|${start ?? 'latest'}|${end ?? ''}`;
    const sourceChanged = sourceRef.current !== pairKey;
    requestRef.current?.abort();
    const abort = new AbortController();
    requestRef.current = abort;
    sourceRef.current = pairKey;
    setError('');
    setLoading(true);

    const cached = candleCache.get(pairKey) ?? [];
    if (rowsRef.current.length === 0 || sourceChanged) {
      rowsRef.current = cached;
      series.setData(cached.map(toBar));
    }
    const visibleBeforeLoad = chart.timeScale().getVisibleRange();
    const params = new URLSearchParams({ limit: '1000' });
    if (start != null && end != null) { params.set('start', String(start)); params.set('end', String(end)); }
    else params.set('limit', '300');
    fetch(`/api/candles/${symbol}/${tf}?${params}`, { signal: abort.signal })
      .then(response => { if (!response.ok) throw Error('Не удалось загрузить свечи'); return response.json(); })
      .then((loaded: Candle[]) => {
        if (abort.signal.aborted || sourceRef.current !== pairKey) return;
        const samePair = rowsRef.current.length > 0 && rowsRef.current.some(c => loaded.some(r => r.time === c.time));
        const before = chart.timeScale().getVisibleRange();
        const merged = mergeRows(rowsRef.current, loaded);
        rowsRef.current = merged;
        series.setData(merged.map(toBar));
        remember(pairKey, merged);
        if (!merged.length) setError('Для этого участка пока нет сохранённых свечей.');
        if (start != null && end != null && rangeKey !== loadedRangeRef.current) {
          try { chart.timeScale().setVisibleRange({ from: Math.floor(start / 1000) as UTCTimestamp,
            to: Math.floor(end / 1000) as UTCTimestamp }); } catch { /* wait for candle data */ }
          loadedRangeRef.current = rangeKey;
        } else if (start == null && end == null && (sourceChanged || rangeKey !== loadedRangeRef.current)) {
          chart.timeScale().fitContent();
          loadedRangeRef.current = rangeKey;
        } else if (!sourceChanged && (before ?? visibleBeforeLoad) && samePair) {
          try { chart.timeScale().setVisibleRange(before ?? visibleBeforeLoad!); } catch { /* chart range changed */ }
        }
        lastCandleAt.current = Date.now();
      })
      .catch(error => { if (error.name !== 'AbortError') setError(error.message); })
      .finally(() => { if (!abort.signal.aborted) setLoading(false); });
    return () => abort.abort();
  }, [symbol, tf, rangeToken, shapeStart, shapeEnd]);

  useEffect(() => {
    const series = seriesRef.current;
    const incoming = liveCandle ?? closedCandle;
    if (!series || !incoming || incoming.time <= 0) return;
    const interval = Number(tf) * 60_000;
    const selectedEnd = shapeEnd == null ? null : shapeEnd + 12 * interval;
    if (selectedEnd != null && incoming.time > selectedEnd) return;
    try {
      const last = rowsRef.current.at(-1);
      if (last && incoming.time > last.time + interval) return; // REST repair fills gaps below.
      if (last && incoming.time < last.time) return;
      if (!sameCandle(last, incoming)) {
        series.update(toBar(incoming));
        rowsRef.current = mergeRows(rowsRef.current, [incoming]);
        remember(sourceRef.current, rowsRef.current);
      }
      lastCandleAt.current = Date.now();
    } catch { /* data can race an interval switch */ }
  }, [liveCandle, closedCandle, symbol, tf, shapeEnd]);

  useEffect(() => {
    const timer = setInterval(() => {
      const chart = chartRef.current, series = seriesRef.current;
      const last = rowsRef.current.at(-1);
      if (!chart || !series || !last || Date.now() - lastCandleAt.current < 5_000
          || Date.now() - restRepairAt.current < 5_000) return;
      const interval = Number(tf) * 60_000;
      const selectedEnd = shapeEnd == null ? null : shapeEnd + 12 * interval;
      if (selectedEnd != null && selectedEnd < Date.now() - interval) return;
      restRepairAt.current = Date.now();
      const [pair, timeframe] = sourceRef.current.split('|');
      fetch(`/api/candles/${pair}/${timeframe}?after=${last.time}&limit=300`)
        .then(response => response.ok ? response.json() : [])
        .then((fresh: Candle[]) => {
          if (!fresh.length || sourceRef.current !== `${symbol}|${tf}`) return;
          const relevant = selectedEnd == null ? fresh : fresh.filter(c => c.time <= selectedEnd);
          if (!relevant.length) return;
          const before = rowsRef.current;
          const merged = mergeRows(before, relevant);
          if (merged.length === before.length && merged.every((c, i) => sameCandle(c, before[i]))) return;
          rowsRef.current = merged;
          series.setData(merged.map(toBar));
          remember(sourceRef.current, merged);
          lastCandleAt.current = Date.now();
        }).catch(() => { /* retry after the next stale interval */ });
    }, 1_000);
    return () => clearInterval(timer);
  }, [symbol, tf, shapeEnd]);

  // Geometry is loaded lazily after selection, while the signal/setup ID stays
  // the same. Key the chart effect by the geometry itself so those lines render.
  const lineSignature = JSON.stringify(setup?.snapshot.lines ?? signal?.lines ?? []);
  const pivotSignature = JSON.stringify(setup?.snapshot.pivots ?? signal?.pivots ?? []);
  useEffect(() => {
    const chart = chartRef.current, series = seriesRef.current;
    if (!chart || !series) return;
    const lines: { key: string; label: string; points: { time: number; value: number }[] }[] =
      JSON.parse(lineSignature).map((line: any, index: number) => ({
        key: `${setup?.id ?? signal?.id ?? 'figure'}:${index}`,
        label: line.label ?? '', points: line.points ?? [],
      }));
    const keep = new Set<string>();
    for (const line of lines) {
      if (line.points.length < 2) continue;
      keep.add(line.key);
      const pts = line.points.slice();
      const last = pts.at(-1)!;
      const end = showFigureLevels && setup
        ? Math.max(last.time, setup.snapshot.detected_on_bar + Number(tf) * 60_000 * 12) : last.time;
      if (end > last.time) {
        const a = pts[0], b = last;
        pts.push({ time: end, value: b.value + (b.value - a.value) * (end - b.time) / (b.time - a.time) });
      }
      const signature = JSON.stringify(pts);
      let item = overlays.current.get(line.key);
      if (!item) {
        const overlay = chart.addSeries(LineSeries, { color: '#f4c542', lineWidth: 2,
          priceLineVisible: false, lastValueVisible: false, title: line.label });
        item = { series: overlay, signature: '' };
        overlays.current.set(line.key, item);
      }
      if (item.signature !== signature) {
        item.series.setData(pts.map(p => ({ time: Math.floor(p.time / 1000) as UTCTimestamp, value: p.value })));
        item.signature = signature;
      }
    }
    for (const [key, item] of overlays.current) {
      if (!keep.has(key)) { chart.removeSeries(item.series); overlays.current.delete(key); }
    }
    const pivots: { time: number; price: number; kind: string }[] = JSON.parse(pivotSignature);
    const markers = pivots.map(p => ({ time: Math.floor(p.time / 1000) as UTCTimestamp,
      position: p.kind === 'high' ? 'aboveBar' as const : 'belowBar' as const,
      color: '#f4c542', shape: 'circle' as const }));
    if (markerRef.current) markerRef.current.setMarkers(markers);
    else markerRef.current = createSeriesMarkers(series, markers);
  }, [lineSignature, pivotSignature, setup?.id, signal?.id, tf, showFigureLevels]);

  const scalp = context?.current?.scalp_plan;
  const contextLinesSignature = JSON.stringify([
    context?.current?.chart_levels?.map((level: any) => [level.kind, level.name, level.price, level.ahead]) ?? [],
    context?.current?.vwap_utc_5m ?? null,
    scalp ? [scalp.status, scalp.entry, scalp.stop, scalp.target, scalp.net_rr, scalp.costs_pct, scalp.reason] : null,
    context?.current?.entry_plans?.map((plan: any) => [plan.scenario, plan.price_min, plan.price_max]) ?? [],
  ]);
  useEffect(() => {
    const series = seriesRef.current;
    if (!series) return;
    const next: PriceLine[] = [];
    if (signal) {
      next.push({ key: 'signal-entry', price: signal.entry ?? signal.boundary, color: '#aab4c0', title: 'Вход' },
        { key: 'signal-stop', price: signal.stop, color: '#ef454a', title: 'Стоп' },
        { key: 'signal-target', price: signal.target, color: '#20b26c', title: 'Цель' });
    }
    for (const level of context?.current?.chart_levels ?? []) {
      const support = level.kind === 'support';
      next.push({ key: `local-${level.kind}-${level.name}`, price: level.price,
        color: support ? '#20b26c' : level.ahead ? '#f7a600' : '#657282',
        title: support ? `Поддержка · ${level.name}` : level.name, width: support ? 2 : 1, style: 2 });
    }
    if (scalp?.stop != null) next.push({ key: 'scalp-stop', price: scalp.stop, color: '#ef454a', title: 'Скальп · стоп', width: 2 });
    if (scalp?.target != null) next.push({ key: 'scalp-target', price: scalp.target, color: '#20b26c', title: 'Скальп · цель', width: 2 });
    if (context?.current?.vwap_utc_5m) next.push({ key: 'vwap', price: context.current.vwap_utc_5m,
      color: '#c4a448', title: 'VWAP UTC', width: 1, style: 2 });
    const plan = (context?.current?.entry_plans ?? []).find((p: any) => p.scenario === 'early_breakout' && p.status === 'preliminary')
      ?? (context?.current?.entry_plans ?? []).find((p: any) => p.scenario === 'confirmed_breakout');
    if (plan) for (const [key, title, value] of [['entry-low', 'Зона входа от', plan.price_min], ['entry-high', 'Зона входа до', plan.price_max]] as const) {
      if (value != null) next.push({ key, price: value, color: '#8ea7d2', title, width: 1, style: 2 });
    }
    if (setup && showFigureLevels) {
      next.push({ key: 'figure-stop', price: setup.snapshot.stop, color: '#ef454a', title: 'Стоп фигуры', width: 1 },
        { key: 'figure-target', price: setup.snapshot.target, color: '#20b26c', title: 'Цель фигуры', width: 1 });
    }
    const keep = new Set(next.map(item => item.key));
    for (const [key, line] of priceLines.current) {
      if (!keep.has(key)) { series.removePriceLine(line); priceLines.current.delete(key); }
    }
    for (const item of next) {
      const options = { price: item.price, color: item.color, title: item.title,
        lineWidth: item.width ?? 1, lineStyle: item.style ?? 0, axisLabelVisible: true, priceLineVisible: true };
      const current = priceLines.current.get(item.key);
      if (current) current.applyOptions(options);
      else priceLines.current.set(item.key, series.createPriceLine(options));
    }
  }, [signal?.id, signal?.entry, signal?.stop, signal?.target, setup?.id, setup?.snapshot.stop,
    setup?.snapshot.target, showFigureLevels, contextLinesSignature]);

  return <div className="chart-wrap">
    {setup && <button className="figure-level-toggle" aria-pressed={showFigureLevels}
      onClick={() => setShowFigureLevels(value => !value)}>
      {showFigureLevels ? 'Скрыть уровни фигуры' : 'Уровни фигуры'}
    </button>}
    <div ref={element} style={{ height: 390, width: '100%' }} />
    {hoverPrice && <div className="chart-hover-price" style={{ left: hoverPrice.x, top: hoverPrice.y }}>
      Цена {symbol} · {decimal(hoverPrice.value)}
    </div>}
    {(loading || error) && <div className="chart-note">{loading ? 'Обновляем историю свечей…' : error}</div>}
  </div>;
}
