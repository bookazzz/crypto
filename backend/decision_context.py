"""Descriptive, causal market context for manual review of a figure signal."""
from datetime import datetime, timezone
from math import isfinite

from .patterns import atr, pivots
from .strategy import BIAS_TF, FIGURE_TF, ENTRY_TF, CONTEXT, trend_filter
from .market_quality import quality
from .market_background import build_market_background


_PATTERN_NAMES = {
    'ascending_triangle': 'восходящий треугольник',
    'descending_triangle': 'нисходящий треугольник',
    'symmetric_triangle': 'симметричный треугольник',
    'double_top': 'двойная вершина',
    'double_bottom': 'двойное дно',
    'bull_flag': 'бычий флаг',
    'bear_flag': 'медвежий флаг',
    'bull_pennant': 'бычий вымпел',
    'bear_pennant': 'медвежий вымпел',
    'rising_wedge': 'восходящий клин',
    'falling_wedge': 'нисходящий клин',
    'head_shoulders': 'голова и плечи',
    'inverse_head_shoulders': 'обратная голова и плечи',
}


def _plain_summary(signal, scenario, entry_checks, volume, btc_trend, obstacle,
                   fee_available=True, extension_atr=None, current_rr=None,
                   required_rr=2.0, data_issue=None):
    """Short, factual explanation for a person deciding whether to enter."""
    side = 'лонг' if signal.side == 'long' else 'шорт'
    direction = 'вверх' if signal.side == 'long' else 'вниз'
    pattern = _PATTERN_NAMES.get(signal.pattern, 'фигура')
    def explain(text):
        if fee_available:
            return text
        return f'{text} Комиссия аккаунта недоступна, чистое RR пока не рассчитано.'

    def market_context_text():
        expected='up' if signal.side=='long' else 'down'
        side_name='лонг' if signal.side=='long' else 'шорт'
        side_genitive='лонга' if signal.side=='long' else 'шорта'
        hourly=entry_checks.get('hourly_bias')
        structure=entry_checks.get('structure_15m')
        facts=[]
        if hourly in ('up','down'):
            direction='вверх' if hourly=='up' else 'вниз'
            relation=(f'совпадает с направлением {side_genitive}' if hourly==expected
                      else f'направлено против {side_name}')
            facts.append(f'направление 1ч {direction} {relation}')
        elif hourly=='neutral':
            facts.append(f'направление 1ч нейтральное и само по себе не подтверждает {side_name}')
        if structure==expected:
            facts.append(f'структура 15м подтверждает {side_name}')
        elif structure=='mixed':
            facts.append(f'структура 15м смешанная и пока не подтверждает {side_name}')
        elif structure in ('up','down'):
            direction='вверх' if structure=='up' else 'вниз'
            facts.append(f'структура 15м направлена {direction} против {side_name}')
        elif entry_checks.get('structure_15m_ready') is False:
            facts.append('структура 15м ещё не сформирована')
        return '; '.join(facts) or 'контекст 1ч/15м не прошёл фильтр'

    if scenario == 'insufficient_data':
        cause=f' Причина: {data_issue}.' if data_issue else ' Не хватает свежих данных.'
        return explain(f'Сейчас нельзя оценить {side}.{cause} Сервис не предлагает вход.')
    if scenario == 'context_only':
        return explain('\u0424\u0438\u0433\u0443\u0440\u0430 \u043d\u0430 \u044d\u0442\u043e\u043c \u0442\u0430\u0439\u043c\u0444\u0440\u0435\u0439\u043c\u0435 \u043f\u043e\u043a\u0430\u0437\u0430\u043d\u0430 \u043a\u0430\u043a \u043a\u043e\u043d\u0442\u0435\u043a\u0441\u0442. \u0420\u0430\u0431\u043e\u0447\u0438\u0435 \u0441\u0435\u0442\u0430\u043f\u044b \u043e\u0446\u0435\u043d\u0438\u0432\u0430\u044e\u0442\u0441\u044f \u043f\u043e \u0444\u0438\u0433\u0443\u0440\u0435 15m \u0438 \u0432\u0445\u043e\u0434\u0443 5m.')
    if scenario == 'wait_for_confirmation':
        return explain('\u0424\u0438\u0433\u0443\u0440\u0430 \u0438 \u0440\u044b\u043d\u043e\u0447\u043d\u044b\u0439 \u043a\u043e\u043d\u0442\u0435\u043a\u0441\u0442 \u043f\u043e\u0434\u0445\u043e\u0434\u044f\u0442, \u043d\u043e \u0437\u0430\u043a\u0440\u044b\u0442\u0430\u044f \u0441\u0432\u0435\u0447\u0430 5m \u0435\u0449\u0451 \u043d\u0435 \u043f\u043e\u0434\u0442\u0432\u0435\u0440\u0434\u0438\u043b\u0430 \u043f\u0440\u043e\u0431\u043e\u0439. \u0412\u0445\u043e\u0434 \u043f\u043e\u043a\u0430 \u043d\u0435 \u043f\u0440\u0435\u0434\u043b\u0430\u0433\u0430\u0435\u0442\u0441\u044f.')
    if scenario == 'late_entry':
        extension=f' Цена уже прошла {extension_atr:.2f} ATR от границы пробоя.' if extension_atr is not None else ''
        rr=f' Чистое отношение прибыли к риску по текущей цене — {current_rr:.2f}:1 при требовании {required_rr:.2f}:1.' if current_rr is not None else ''
        return explain(f'Пробой {direction} был подтверждён, но вход в {side} сейчас запоздал.{extension}{rr} Не стоит догонять цену; дождитесь нового сетапа или ретеста.')
    if scenario == 'insufficient_rr':
        return explain(f'Для пробоя {direction} нет допустимой зоны входа: чистое отношение прибыли к риску ниже установленного минимума {required_rr:.2f}:1. Вход не предлагается.')
    if scenario == 'target_reached':
        return explain(f'Цель фигуры уже достигнута. Сценарий на {side} завершён, входить по этому сигналу поздно.')
    if scenario == 'scenario_invalidated':
        invalidation=entry_checks.get('invalidation_reason') or 'цена вернулась за границу пробоя или прошла стоповый уровень'
        context=market_context_text()
        return explain(f'Сигнал зафиксировал пробой {direction} по фигуре «{pattern}»; направление сигнала — {side}. '
            f'Но текущий сценарий отменён: {invalidation}. Контекст при пробое: {context}. '
            'Вход по этому сигналу не предлагается; дождитесь нового сетапа.')
    if scenario == 'wait_for_retest':
        return explain(f'Рассматривается {side}: у фигуры «{pattern}» подтверждён пробой {direction}. '
                'Сервис ждёт возврата к границе фигуры и нового подтверждения, поэтому вход пока не предлагается.')
    if scenario == 'manual_review_filters_unavailable':
        return explain(f'Фигура «{pattern}» указывает на пробой {direction}, поэтому рассматривается {side}. '
                'Проверки тренда или объёма недоступны; данных недостаточно, чтобы предлагать вход.')

    failed = []
    if entry_checks.get('trend') is False:
        failed.append(market_context_text())
    if entry_checks.get('volume') is False:
        if volume.get('notional_pass') is False:
            actual=volume.get('breakout_turnover_usdt',0.)
            required=volume.get('minimum_notional_usdt',0.)
            failed.append(f'оборот свечи {actual:.0f} USDT ниже минимума {required:.0f} USDT')
        else:
            multiple = volume.get('multiple')
            minimum = volume.get('minimum', 1.5)
            if multiple is not None and isfinite(multiple):
                failed.append(f'объём пробоя {multiple:.2f}× от обычного, нужно не меньше {minimum:.2f}×')
            else:
                failed.append('объём пробойной свечи слишком низкий')
    if failed:
        return explain(f'Фигура «{pattern}» дала пробой {direction}, но {" и ".join(failed)}. '
                f'Поэтому вход в {side} сейчас не предлагается.')

    reasons = [f'фигура «{pattern}» закрылась за границей пробоя {direction}']
    multiple = volume.get('multiple')
    if entry_checks.get('volume') is True and multiple is not None and isfinite(multiple):
        reasons.append(f'объём был выше обычного в {multiple:.2f} раза')
    market_scenario=entry_checks.get('market_scenario')
    if market_scenario=='continuation':
        reasons.append('направление 1ч и структура 15м совпадают с сетапом')
    elif market_scenario=='reversal':
        reasons.append('структура 15м подтверждает возможный разворот против направления 1ч')
    elif entry_checks.get('trend') is False:
        reasons.append(market_context_text())
    if btc_trend is False:
        reasons.append('тренд BTC направлен против')
    if obstacle:
        distance = obstacle.get('distance_atr')
        if distance is not None:
            reasons.append(f'рядом есть уровень-препятствие в {distance:.2f} ATR')
    return explain(f'Сервис рассматривает {side}, потому что {"; ".join(reasons)}. '
            'Это аргументы для вашей оценки, а не команда открывать сделку.')


def _true_ranges(candles):
    return [max(c.high-c.low,abs(c.high-candles[i-1].close),
                abs(c.low-candles[i-1].close)) for i,c in enumerate(candles) if i]


def _atr_percentile(candles, current_atr):
    if len(candles)<30 or current_atr<=0:
        return None
    values=[]
    for end in range(max(15,len(candles)-100),len(candles)+1):
        sample=candles[:end]
        value=atr(sample[-30:])
        if value>0:
            values.append(value)
    if not values:
        return None
    return sum(value<=current_atr for value in values)/len(values)


def _closed_return(buffers,symbol,timeframe,timestamp,window_minutes):
    interval=int(timeframe)*60_000
    closed=[c for c in buffers.get((symbol,timeframe),[]) if c.time+interval<=timestamp]
    if len(closed)<2:return None
    current=closed[-1]
    cutoff=timestamp-window_minutes*60_000
    base=next((c for c in reversed(closed) if c.time+interval<=cutoff),None)
    if base is None or base.close<=0:return None
    return current.close/base.close-1


def _levels(symbol, timestamp, price, side, timeframe, atr_value, buffers):
    levels=[]
    sign=1 if side=='long' else -1
    daily=[c for c in buffers.get((symbol,'D'),[]) if c.time+86_400_000<=timestamp]
    if daily:
        previous=daily[-1]
        levels.extend([{'name':'Максимум предыдущего дня','kind':'resistance','price':previous.high},
                       {'name':'Минимум предыдущего дня','kind':'support','price':previous.low}])
        current_week=datetime.fromtimestamp(timestamp/1000,timezone.utc).isocalendar()[:2]
        weeks={}
        for candle in daily:
            week=datetime.fromtimestamp(candle.time/1000,timezone.utc).isocalendar()[:2]
            weeks.setdefault(week,[]).append(candle)
        closed=[(key,rows) for key,rows in weeks.items() if key<current_week]
        if closed:
            _,rows=closed[-1]
            levels.extend([{'name':'Максимум предыдущей недели','kind':'resistance','price':max(c.high for c in rows)},
                           {'name':'Минимум предыдущей недели','kind':'support','price':min(c.low for c in rows)}])
    context_tf=BIAS_TF
    higher=[c for c in buffers.get((symbol,context_tf),[]) if
            c.time+(1440 if context_tf=='D' else int(context_tf))*60_000<=timestamp]
    for pivot in pivots(higher[-250:]):
        point=higher[-250:][pivot.index]
        levels.append({'name':f"Экстремум {context_tf}м ({'сопротивление' if pivot.kind=='high' else 'поддержка'})",
                       'kind':'resistance' if pivot.kind=='high' else 'support',
                       'price':pivot.price,'time':point.time})
    unique=[];seen=set()
    for level in levels:
        value=level['price']
        ahead=sign*(value-price)>0
        key=(level['name'],round(value/max(atr_value,1e-12),5))
        if key in seen: continue
        seen.add(key)
        unique.append(level|{'distance_atr':abs(value-price)/atr_value if atr_value>0 else None,
                             'ahead':ahead})
    unique.sort(key=lambda x:(not x['ahead'],abs(x['price']-price)))
    obstacles=[x for x in unique if x['ahead'] and sign*(x['price']-price)>0]
    return unique,obstacles[0] if obstacles else None


def support_chart_levels(symbol, timestamp, price, side, timeframe, atr_value, buffers):
    """Return nearby closed-candle support levels for an early setup chart."""
    if price is None or atr_value is None or atr_value<=0:
        return []
    levels,_=_levels(symbol,timestamp,price,side,timeframe,atr_value,buffers)
    supports=[level for level in levels if level.get('kind')=='support'
              and level['price']<=price+.25*atr_value
              and level.get('distance_atr') is not None and level['distance_atr']<=5]
    supports.sort(key=lambda level:(abs(level['price']-price),level.get('name','')))
    return supports[:6]


def _flow(symbol, timestamp, store, live_flow, connected=True, connected_since=None):
    minute=timestamp//60_000*60_000
    start=minute-15*60_000
    rows=store.market_minutes(symbol,start,minute+60_000)
    if (symbol,minute) in live_flow:
        live=live_flow[(symbol,minute)]
        rows=[r for r in rows if r['time']!=minute]+[{'time':minute,'buy':live['buy'],'sell':live['sell'],
            'updated':live.get('updated',timestamp)}]
    output={}
    for window in (1,5,15):
        values=[r for r in rows if minute-window*60_000<r['time']<=minute]
        buy=sum(r['buy'] for r in values);sell=sum(r['sell'] for r in values);total=buy+sell
        coverage=max(0,min(window*60_000,timestamp-max(minute-window*60_000,
            int(connected_since or timestamp)))) if connected else 0
        event_at=max((int(r.get('updated') or r['time']) for r in values),default=None)
        q=quality('bybit_public_trade',event_at,event_at,timestamp,coverage_ms=coverage,
            window_ms=window*60_000,connected=connected,value_present=connected_since is not None,
            reasons=[] if connected else ['reconnect_required'])
        output[str(window)]={'buy_usdt':buy,'sell_usdt':sell,'net_usdt':buy-sell,
            'buy_share':buy/total if total else None,'minutes_with_data':len(values),
            'includes_open_minute':any(r['time']==minute for r in values),
            'coverage_ms':coverage,'window_ms':window*60_000,'quality':q}
    return output


def _oi(symbol, timestamp, store, price, ticker=None):
    if hasattr(store,'open_interest_records'):
        records=store.open_interest_records(symbol,timestamp-65*60_000,timestamp+1)
        rows=[(r['time'],r['value'],r.get('value_usdt'),r.get('source'),r.get('received_at')) for r in records]
    else:
        rows=[(t,v,None,'rest',t) for t,v in store.open_interest(symbol,timestamp-65*60_000,timestamp+1)]
    ticker=ticker or {}
    try:
        ticker_time=int(ticker.get('_openInterest_time',0) or 0)
        ticker_value=float(ticker.get('openInterest'))
        ticker_usdt=float(ticker['openInterestValue']) if ticker.get('openInterestValue') not in (None,'') else None
        if ticker_time and timestamp-ticker_time>=0 and ticker_value>0:
            rows=[row for row in rows if row[0]!=ticker_time]
            rows.append((ticker_time,ticker_value,ticker_usdt,'ticker',ticker.get('_openInterest_received_at')))
            rows.sort(key=lambda row:row[0])
    except (TypeError,ValueError):
        pass
    if not rows:
        return {'available':False,'quality':quality('unknown',None,None,timestamp,value_present=False)}
    latest_time,latest,value_usdt,source,received_at=rows[-1]
    def change(window):
        candidates=[r for r in rows if r[0]<=latest_time-window*60_000]
        if not candidates:return None
        base=candidates[-1][1]
        return (latest-base)/base if base else None
    age=max(0,timestamp-latest_time)
    return {'available':True,'as_of':latest_time,'received_at':received_at,'age_ms':age,
            'value':latest,'value_usdt':value_usdt or (latest*price if price>0 else None),
            'change_5m':change(5),'change_15m':change(15),'change_1h':change(60),
            'quality':quality(source or 'rest',latest_time,received_at,timestamp,max_age_ms=600_000)}


def _book(book, side='long', quantity=None, price=None):
    if not book or not book.get('bids') or not book.get('asks'):
        return {'available':False,'quality':quality('bybit_orderbook',None,None,
            book.get('now',0) if book else 0,value_present=False)}
    bids=sorted(book['bids'].items(),reverse=True)[:10]
    asks=sorted(book['asks'].items())[:10]
    bid,ask=bids[0][0],asks[0][0]
    mid=(bid+ask)/2
    spread=(ask-bid)/mid*10_000 if mid else None
    bid_depth=sum(p*q for p,q in bids);ask_depth=sum(p*q for p,q in asks)
    now=int(book.get('now',book.get('received_at',0)) or 0)
    age=max(0,now-int(book.get('time',0)))
    out={'available':True,'as_of':book.get('time'),'received_at':book.get('received_at'),'age_ms':age,
        'best_bid':bid,'best_ask':ask,'spread_bps':spread,'bid_depth_usdt_top10':bid_depth,
        'ask_depth_usdt_top10':ask_depth,
        'depth_by_band_bps':{str(bps):{'bid_usdt':sum(p*q for p,q in book['bids'].items() if mid*(1-bps/10_000)<=p<=mid),
            'ask_usdt':sum(p*q for p,q in book['asks'].items() if mid<=p<=mid*(1+bps/10_000))}
            for bps in (5,10,25)},
        'coverage_by_band_bps':{str(bps):{'bid':min(book['bids'])<=mid*(1-bps/10_000),
            'ask':max(book['asks'])>=mid*(1+bps/10_000)} for bps in (5,10,25)},
        'generation':book.get('generation'),'sequence':book.get('sequence'),
        'quality':quality('bybit_orderbook',book.get('time'),book.get('received_at'),now,
            max_age_ms=5_000,value_present=bool(book.get('valid',True)))}
    # Optional quantity-aware estimate is reserved for the virtual execution
    # gate. The manual context calls _book without quantity.
    if quantity is not None and price is not None and quantity>0:
        levels=asks if side=='long' else bids
        remaining=float(quantity);filled=0.;cost=0.
        for level_price,level_qty in levels:
            used=min(remaining,level_qty)
            filled+=used;cost+=used*level_price;remaining-=used
            if remaining<=1e-12:break
        vwap=cost/filled if filled else None
        out.update({'estimated_quantity':float(quantity),'estimated_notional_usdt':float(quantity)*mid,
            'estimated_impact_bps':(vwap-mid)/mid*10_000*(1 if side=='long' else -1) if vwap and mid else None,
            'estimated_fill_ratio':filled/float(quantity)})
    return out


def _liquidation_summary(rows, symbol, timestamp, connected=True, connected_since=None,store=None):
    output={}
    for window in (5,15):
        start=timestamp-window*60_000
        buckets=[value for (coin,minute),value in rows.items()
                 if coin==symbol and minute+60_000>start and minute<=timestamp]
        if store is not None and hasattr(store,'liquidation_minutes'):
            persisted=store.liquidation_minutes(symbol,start,timestamp+1)
            persisted_by_minute={row['time']:row for row in persisted}
            for (coin,minute),value in rows.items():
                if coin==symbol:persisted_by_minute[minute]=value
            buckets=list(persisted_by_minute.values())
            buckets=[row for row in buckets if row.get('time',0)+60_000>start and row.get('time',0)<=timestamp]
        long_usdt=sum(row.get('long_usdt',0) for row in buckets)
        short_usdt=sum(row.get('short_usdt',0) for row in buckets)
        count=sum(row.get('count',0) for row in buckets)
        coverage=max(0,min(window*60_000,timestamp-max(start,int(connected_since or timestamp)))) if connected else 0
        q=quality('bybit_all_liquidation',max((r.get('updated',0) for r in buckets),default=None),
            max((r.get('received_at',0) for r in buckets),default=None),timestamp,
            coverage_ms=coverage,window_ms=window*60_000,connected=connected,
            value_present=connected_since is not None)
        output[str(window)]={'long_liquidated_usdt':long_usdt,'short_liquidated_usdt':short_usdt,
            'count':count,'coverage_ms':coverage,'window_ms':window*60_000,'quality':q}
    return output


def build_context(signal, timestamp, buffers, tickers, symbols, store, live_flow, books,
                  oi, fees, slippage, fee_mode='standard', spot_matches=None,
                  spot_books=None, spot_error='', min_rr=2.0,
                  quote_unavailable_reason=None,tick_size=0.,flow_connected=True,
                  flow_connected_since=None,liquidations=None,liquidation_connected=True,
                  liquidation_connected_since=None,instruments=None,market_background=None):
    symbol=signal.symbol
    if market_background is None:
        instrument=(instruments or {}).get(symbol)
        market_background=build_market_background(symbol,signal.side,timestamp,buffers,
            getattr(instrument,'asset_class','crypto'))
    current=tickers.get(symbol,{})
    try:
        raw_price=float(current.get('lastPrice'))
        quote_time=int(current.get('_quote_time',0) or 0)
        quote_present=isfinite(raw_price) and raw_price>0 and quote_time>0
    except (ValueError,TypeError):
        raw_price=None;quote_time=0;quote_present=False
    if quote_unavailable_reason:
        quote_present=False
    quote_age=max(0,timestamp-quote_time) if quote_present else None
    quote_fresh=quote_present and quote_age<=30_000
    candles=[c for c in buffers.get((symbol,signal.timeframe),[]) if
             c.time+int(signal.timeframe)*60_000<=timestamp]
    issues=[]
    quote_issue=None
    candle_issue=None
    if not quote_present:
        quote_issue=quote_unavailable_reason or 'Нет котировки по инструменту'
        issues.append(quote_issue)
    elif not quote_fresh:
        quote_issue=f'Котировка устарела: возраст {quote_age//1000} с, допустимо не более 30 с'
        issues.append(quote_issue)
    if not candles:
        candle_issue=f'Нет закрытых свечей таймфрейма {signal.timeframe}м для текущей оценки'
    elif timestamp-(candles[-1].time+int(signal.timeframe)*60_000)>int(signal.timeframe)*60_000+30_000:
        candle_issue=f'Закрытые свечи таймфрейма {signal.timeframe}м устарели'
    if candle_issue:
        issues.append(candle_issue)
    if not quote_present:
        missing=issues[0]
        plain=_plain_summary(signal,'insufficient_data',{}, {},None,None,
            fee_mode!='account',required_rr=min_rr,data_issue=missing)
        return {'signal_id':signal.id,'symbol':symbol,'timeframe':signal.timeframe,'side':signal.side,
            'as_of':timestamp,'scenario':'insufficient_data','entry_checks':{},'plain_summary':plain,
            'market_background':market_background,
            'entry_plans':[{'scenario':name,'status':'unavailable','reason':missing,
                'price_min':None,'price_max':None,'stop':signal.stop,'target':signal.target,
                'projection':signal.target,'net_rr':None} for name in
                ('confirmed_breakout','retest')],
            'entry':signal.entry or signal.boundary,'current_price':None,'current_quote_time':None,
            'breakout_extension_atr':None,'current_entry_state':'insufficient_data',
            'required_net_rr':float(signal.filter_results.get('required_net_rr',min_rr) or min_rr),
            'stop':signal.stop,'target':signal.target,'atr':signal.atr,'atr_percentile_100':None,
            'relative_strength_24h':None,'quote_age_ms':None,
            'market_breadth':{'up':0,'down':0,'symbols_with_data':0},
            'btc_trend':{'passed':None,'indicators':[]},'vwap_utc_5m':None,
            'distance_to_vwap_atr':None,'levels':[],'chart_levels':[],'nearest_obstacle':None,
            'net_rr_to_obstacle':None,'net_rr_to_target':None,'fee_available':False,
            'fee_source':None,'fee_age_ms':None,
            'costs_per_unit':{'round_trip_fee':None,'round_trip_slippage':None,
                'reserved_funding':None,'net_reward':None,'net_risk':None},
            'fee_per_side':None,'slippage_per_side':slippage,
            'funding':{'available':False,'rate':None,'next_time':None,'estimated_cost_per_unit':None},
            'orderbook':{'available':False},'spot_futures':{'status':'unavailable',
                'explanation':'Нет актуальной котировки фьючерса'},
            'trade_flow':{},'open_interest':{'available':False},'liquidations':{},'quality':{},'supporting_factors':[],
            'risks':[],'unknown_data':issues,'signal_reason':signal.reason,
            'strategy_approval_required':not bool(signal.strategy_version)}
    price=raw_price
    indicators=[];risks=[];unknown=[]
    fee_record=fees.get(symbol,{})
    fee_time=int(fee_record.get('time',0) or 0)
    fee_source=fee_record.get('source','')
    if fee_mode=='account':
        fee_available=(fee_source=='account' and fee_time>0 and
                       0<=timestamp-fee_time<=86400000)
    else:
        fee_available=(fee_source in ('bybit_vip0_standard','bybit_vip0_standard_fallback') and
                       fee_time>0 and 0<=timestamp-fee_time<=86400000)
    fee=float(fee_record['rate']) if fee_available else None
    if not fee_available:
        unknown_fee=('Актуальная комиссия аккаунта не загружена или устарела'
                     if fee_mode=='account' else 'Актуальная комиссия не загружена или устарела')
    if quote_issue: unknown.append(quote_issue)
    if candle_issue: unknown.append(candle_issue)
    instrument=(instruments or {}).get(symbol)
    asset_class=getattr(instrument,'asset_class','crypto') if instrument else 'crypto'
    peers=[coin for coin in symbols if getattr((instruments or {}).get(coin),'asset_class','crypto')==asset_class]
    trend_ok=None;btc_trend=[]
    if asset_class=='crypto':
        btc_hourly=[c for c in buffers.get(('BTCUSDT','60'),[]) if c.time+3_600_000<=timestamp]
        trend_ok,btc_trend=trend_filter(btc_hourly,signal.side)
        if trend_ok is None: unknown.append('Недостаточно истории для тренда BTC на 1ч')
        elif trend_ok: indicators.append('Тренд BTC на 1ч совпадает с направлением сигнала')
        else: risks.append('Тренд BTC на 1ч направлен против сигнала')
    changes=[]
    for coin in peers:
        ticker=tickers.get(coin,{})
        try:
            if 'price24hPcnt' in ticker: changes.append((coin,float(ticker['price24hPcnt'] or 0)))
        except (ValueError,TypeError): pass
    up=sum(value>0 for _,value in changes);down=sum(value<0 for _,value in changes)
    if 'price24hPcnt' not in current: unknown.append('Нет 24-часового изменения для оценки относительной силы')
    own_change=float(current.get('price24hPcnt',0) or 0)
    btc_change=float(tickers.get('BTCUSDT',{}).get('price24hPcnt',0) or 0)
    relative=round(own_change-btc_change,8) if asset_class=='crypto' else None
    sign=1 if signal.side=='long' else -1
    indicators.append(f"Ширина рынка: {up} вверх / {down} вниз")
    if relative is not None:
        (indicators if sign*relative>0 else risks).append(
            f"Относительная сила к BTC за 24ч {relative*100:+.2f}%")
    atr_percentile=_atr_percentile(candles,signal.atr)
    if atr_percentile is None: unknown.append('Недостаточно закрытых свечей для ранга волатильности')
    elif atr_percentile>=.9: risks.append('ATR в верхних 10% последних значений')
    vwap_candles=[c for c in buffers.get((symbol,'5'),[]) if
        c.time>=timestamp//86_400_000*86_400_000 and c.time+300_000<=timestamp]
    total_volume=sum(c.volume for c in vwap_candles)
    vwap=sum(((c.high+c.low+c.close)/3)*c.volume for c in vwap_candles)/total_volume if total_volume else None
    if vwap is None: unknown.append('VWAP с начала UTC-суток пока недоступен')
    levels,obstacle=_levels(symbol,timestamp,price,signal.side,signal.timeframe,signal.atr,buffers)
    if obstacle and sign*(signal.target-obstacle['price'])>=0:
        distance=abs(obstacle['price']-price)
        risks.append(f"Ближайшее препятствие {obstacle['name']} в {distance/signal.atr:.2f} ATR")
    else:
        obstacle=None
    anchors=[x for x in levels if 'предыдущего дня' in x['name'] or 'предыдущей недели' in x['name']]
    ahead_levels=sorted((x for x in levels if x['ahead'] and x not in anchors),key=lambda x:x['distance_atr'])[:3]
    behind_levels=sorted((x for x in levels if not x['ahead'] and x not in anchors),key=lambda x:x['distance_atr'])[:3]
    display_levels=[];seen_levels=set()
    for level in anchors+ahead_levels+behind_levels:
        key=(level['name'],level['price'])
        if key not in seen_levels:display_levels.append(level);seen_levels.add(key)
    chart_levels=[x for x in display_levels if x['distance_atr']<=5]
    funding_available='fundingRate' in current and int(current.get('nextFundingTime',0) or 0)>0
    if not funding_available: unknown.append('Funding или время следующего расчёта недоступны')
    funding=float(current.get('fundingRate',0) or 0)
    next_funding=int(current.get('nextFundingTime',0) or 0)
    funding_time=int(current.get('_funding_time',0) or 0)
    sign=1 if signal.side=='long' else -1
    funding_cost=max(0,sign*funding)*price
    expected_rr=None
    from .strategy import clean_rr
    if fee_available:
        target_rr,unit_risk,unit_reward=clean_rr(price,signal.stop,signal.target,signal.side,fee,slippage,funding)
    else:
        target_rr,unit_risk,unit_reward=None,0.,0.
        unknown.append(unknown_fee)
    if fee_available and obstacle and sign*(obstacle['price']-price)>0:
        expected_rr=clean_rr(price,signal.stop,obstacle['price'],signal.side,fee,slippage,funding)[0]
    required_rr=float(signal.filter_results.get('required_net_rr',min_rr) or min_rr)
    breakout_extension=sign*(price-signal.boundary)/signal.atr if signal.atr>0 else None
    current_entry_state='candidate'
    candles_fresh=bool(candles) and not candle_issue
    if not quote_fresh or not candles_fresh:
        current_entry_state='insufficient_data'
    elif sign*(price-signal.stop)<=0:
        current_entry_state='scenario_invalidated'
    elif sign*(signal.target-price)<=0:
        current_entry_state='target_reached'
    elif sign*(price-signal.boundary)<-.1*signal.atr:
        current_entry_state='scenario_invalidated'
    elif target_rr is not None and target_rr<required_rr:
        current_entry_state='late_entry'
    elif not fee_available and breakout_extension is not None and breakout_extension>=.5:
        current_entry_state='late_entry'
    elif not fee_available:
        current_entry_state='costs_unavailable'
    current_book=books.get(symbol)
    if current_book: current_book=current_book|{'now':timestamp}
    book=_book(current_book)
    if not book['available']:unknown.append('Стакан пока недоступен')
    elif book['age_ms']>5_000: unknown.append('Стакан устарел более чем на 5 секунд')
    flow=_flow(symbol,timestamp,store,live_flow,flow_connected,flow_connected_since)
    for window,values in flow.items():
        if values['quality']['status']!='valid': unknown.append(f"Покрытие потока сделок за {window} минут: {values['quality']['status']}")
    oi_data=_oi(symbol,timestamp,store,price,current)
    if not oi_data['available']: unknown.append('Открытый интерес ещё не загружен')
    elif oi_data['age_ms']>10*60_000: unknown.append('Открытый интерес устарел более 10 минут')
    checks=signal.filter_results
    market=checks.get('market_context',{})
    structure_15m=market.get('structure_15m') or {}
    invalidation_reason=None
    if current_entry_state=='scenario_invalidated':
        if sign*(price-signal.stop)<=0:
            invalidation_reason='цена достигла стоп-уровня'
        else:
            crossed='выше' if signal.side=='short' else 'ниже'
            invalidation_reason=f'цена вернулась {crossed} границы фигуры'
    entry_checks={'trend':market.get('eligible',checks.get('trend_pass')),
        'market_scenario':market.get('scenario'),'volume':checks.get('volume_pass'),'confirmation':checks.get('confirmation_pass'),
        'hourly_bias':market.get('hourly_bias'),'structure_15m':structure_15m.get('direction'),
        'structure_15m_ready':structure_15m.get('ready'),'invalidation_reason':invalidation_reason,
        'retest':signal.status=='pending_retest' if signal.entry_method=='retest' else None}
    if not quote_fresh or not candles_fresh: scenario='insufficient_data'
    elif signal.timeframe!=FIGURE_TF: scenario='context_only'
    elif signal.status=='pending_retest': scenario='wait_for_retest'
    elif current_entry_state=='scenario_invalidated': scenario='scenario_invalidated'
    elif current_entry_state=='target_reached': scenario='target_reached'
    elif not entry_checks['trend']: scenario='rules_rejected'
    elif not entry_checks['confirmation']: scenario='wait_for_confirmation'
    elif entry_checks['volume'] is False: scenario='rules_rejected'
    elif current_entry_state!='candidate': scenario=current_entry_state
    else: scenario='conditions_met_review_manually'
    plain_summary=_plain_summary(signal,scenario,entry_checks,checks.get('volume',{}),trend_ok,obstacle,
        fee_available,breakout_extension,target_rr,required_rr,quote_issue or candle_issue)
    if market_background.get('advisory') and scenario in (
            'conditions_met_review_manually','wait_for_confirmation','wait_for_retest'):
        plain_summary=f"{plain_summary} {market_background['advisory']}"
    risks.extend(market_background.get('warnings') or [])
    if not changes: unknown.append('Нет достаточного набора котировок для ширины рынка')
    from .market_comparison import compare_spot_perpetual
    spot_futures=(compare_spot_perpetual(symbol,(spot_matches or {}).get(symbol),timestamp,
        books,spot_books or {},spot_error) if asset_class=='crypto' else
        {'status':'unavailable','explanation':'Сопоставление со спотом не применяется к этому классу инструмента.'})
    tick=max(abs(signal.boundary)*1e-8,1e-8)
    entry_min=signal.boundary+sign*tick
    entry_max=None
    plan_target=obstacle['price'] if obstacle and sign*(obstacle['price']-signal.boundary)>0 else signal.target
    if fee_available and sign*(plan_target-entry_min)>0:
        lo,hi=0.,abs(plan_target-entry_min)
        for _ in range(32):
            distance=(lo+hi)/2;entry_price=entry_min+sign*distance
            rr=clean_rr(entry_price,signal.stop,plan_target,signal.side,fee,slippage,funding,tick_size)[0]
            if rr is not None and rr>=required_rr:lo=distance
            else:hi=distance
        entry_max=entry_min+sign*lo
    entry_low=min(entry_min,entry_max) if entry_max is not None else entry_min
    entry_high=max(entry_min,entry_max) if entry_max is not None else None
    plan_status='available' if current_entry_state=='candidate' else current_entry_state
    entry_atr=float(signal.filter_results.get('entry_atr',signal.atr/3) or signal.atr/3)
    entry_plans=[
        {'scenario':'confirmed_breakout','status':plan_status,
         'activation':'\u0417\u0430\u043a\u0440\u044b\u0442\u0438\u0435 5m \u0437\u0430 \u0433\u0440\u0430\u043d\u0438\u0446\u0435\u0439 \u0444\u0438\u0433\u0443\u0440\u044b \u043f\u043e\u0441\u043b\u0435 \u043f\u043e\u0434\u0442\u0432\u0435\u0440\u0436\u0434\u0435\u043d\u0438\u044f \u0441\u0442\u0440\u0443\u043a\u0442\u0443\u0440\u044b 15m',
         'price_min':entry_low,'price_max':entry_high,'stop':signal.stop,'target':plan_target,
         'projection':signal.target,'net_rr':target_rr,
         'cancel_if':'\u0421\u0442\u043e\u043f \u0434\u043e\u0441\u0442\u0438\u0433\u043d\u0443\u0442, \u0446\u0435\u043d\u0430 \u0432\u0435\u0440\u043d\u0443\u043b\u0430\u0441\u044c \u0437\u0430 \u0433\u0440\u0430\u043d\u0438\u0446\u0443 \u0444\u0438\u0433\u0443\u0440\u044b \u0438\u043b\u0438 \u0447\u0438\u0441\u0442\u043e\u0435 RR \u043d\u0438\u0436\u0435 \u043c\u0438\u043d\u0438\u043c\u0443\u043c\u0430'},
        {'scenario':'retest','status':'waiting' if current_entry_state=='candidate' else plan_status,
         'activation':'\u041a\u0430\u0441\u0430\u043d\u0438\u0435 \u0433\u0440\u0430\u043d\u0438\u0446\u044b \u00b10,25 ATR 5m \u0438 \u0437\u0430\u043a\u0440\u044b\u0442\u0438\u0435 \u0437\u0430 \u043d\u0435\u0439 \u043d\u0430 0,1 ATR; \u043c\u0430\u043a\u0441\u0438\u043c\u0443\u043c \u0442\u0440\u0438 \u0441\u0432\u0435\u0447\u0438',
         'price_min':signal.boundary-.25*entry_atr,'price_max':signal.boundary+.25*entry_atr,
         'stop':signal.stop,'target':plan_target,'projection':signal.target,'net_rr':target_rr,
         'cancel_if':'\u0421\u0442\u043e\u043f \u0434\u043e\u0441\u0442\u0438\u0433\u043d\u0443\u0442, \u0446\u0435\u043d\u0430 \u0432\u0435\u0440\u043d\u0443\u043b\u0430\u0441\u044c \u0432\u043d\u0443\u0442\u0440\u044c \u0444\u0438\u0433\u0443\u0440\u044b \u0438\u043b\u0438 \u0438\u0441\u0442\u0435\u043a\u043b\u0438 \u0442\u0440\u0438 \u0441\u0432\u0435\u0447\u0438 5m'}]
    liquidation_data=_liquidation_summary(liquidations or {},symbol,timestamp,
        liquidation_connected,liquidation_connected_since,store)
    confirmation=signal.filter_results.get('confirmation_5m',{})
    own_15m=_closed_return(buffers,symbol,'15',timestamp,15)
    own_1h=_closed_return(buffers,symbol,'60',timestamp,60)
    btc_15m=_closed_return(buffers,'BTCUSDT','15',timestamp,15) if asset_class=='crypto' else None
    btc_1h=_closed_return(buffers,'BTCUSDT','60',timestamp,60) if asset_class=='crypto' else None
    try:
        mark=float(current.get('markPrice'))
        index=float(current.get('indexPrice'))
        mark_index_basis_pct=(mark/index-1)*100 if mark>0 and index>0 else None
    except (TypeError,ValueError):mark_index_basis_pct=None
    quality_data={'quote':quality('bybit_ticker',quote_time,current.get('_quote_received_at'),timestamp,
            max_age_ms=30_000,value_present=quote_present),
        'figure_candles':quality('bybit_kline',candles[-1].time+int(signal.timeframe)*60_000 if candles else None,
            timestamp,timestamp,max_age_ms=int(signal.timeframe)*60_000+30_000,
            value_present=bool(candles),reasons=[candle_issue] if candle_issue else []),
        'orderbook':book.get('quality'),'open_interest':oi_data.get('quality'),
        'trade_flow':{window:item['quality'] for window,item in flow.items()},
        'liquidations':{window:item['quality'] for window,item in liquidation_data.items()},
        'funding':quality('bybit_ticker',funding_time,current.get('_ticker_received_at'),timestamp,
            max_age_ms=300_000,value_present=funding_available),
        'mark_index':quality('bybit_ticker',current.get('_ticker_event_at'),current.get('_ticker_received_at'),
            timestamp,max_age_ms=30_000,value_present=mark_index_basis_pct is not None)}
    quality_data['timeframes']={}
    for tf in ('60','15','5'):
        interval=int(tf)*60_000
        recent=[c for c in buffers.get((symbol,tf),[]) if c.time+interval<=timestamp]
        quality_data['timeframes'][tf]=quality('bybit_kline',
            recent[-1].time+interval if recent else None,timestamp,timestamp,
            max_age_ms=interval+30_000,value_present=bool(recent),
            reasons=['waiting_for_closed_candle'] if not recent else [])
    return {'signal_id':signal.id,'symbol':symbol,'timeframe':signal.timeframe,'side':signal.side,
        'as_of':timestamp,'scenario':scenario,'market_context':signal.filter_results.get('market_context'),
        'market_background':market_background,
        'entry_checks':entry_checks,
        'entry_plans':entry_plans,
        'confirmation_5m':confirmation,'volume_confirmation':signal.filter_results.get('volume',{}),
        'plain_summary':plain_summary,
        'entry':signal.entry or signal.boundary,'current_price':price,'current_quote_time':quote_time,
        'breakout_extension_atr':breakout_extension,'current_entry_state':current_entry_state,
        'required_net_rr':required_rr,'stop':signal.stop,'target':signal.target,
        'atr':signal.atr,'atr_percentile_100':atr_percentile,'relative_strength_24h':relative,
        'relative_strength':{'15m':own_15m-btc_15m if own_15m is not None and btc_15m is not None else None,
            '1h':own_1h-btc_1h if own_1h is not None and btc_1h is not None else None,
            '24h':relative,'own_15m':own_15m,'own_1h':own_1h,'asset_class':asset_class},
        'confirmation_5m':confirmation,
        'volume_confirmation':signal.filter_results.get('volume',{}),
        'mark_index_basis_pct':mark_index_basis_pct,
        'quote_age_ms':quote_age,
        'market_breadth':{'up':up,'down':down,'symbols_with_data':len(changes)},
        'btc_trend':{'passed':trend_ok,'indicators':btc_trend},
        'vwap_utc_5m':vwap,'distance_to_vwap_atr':abs(price-vwap)/signal.atr if vwap and signal.atr else None,
        'levels':display_levels,'chart_levels':chart_levels,'nearest_obstacle':obstacle,'net_rr_to_obstacle':expected_rr,
        'net_rr_to_target':target_rr if unit_risk>0 and unit_reward>0 else None,
        'fee_available':fee_available,'fee_source':fee_source if fee_available else None,
        'fee_age_ms':timestamp-fee_time if fee_available else None,
        'costs_per_unit':{'round_trip_fee':fee*(price+signal.target) if fee_available else None,
            'round_trip_slippage':slippage*(price+signal.target),'reserved_funding':funding_cost,
            'net_reward':unit_reward if fee_available else None,'net_risk':unit_risk if fee_available else None},
        'fee_per_side':fee,'slippage_per_side':slippage,'funding':{'available':funding_available,'rate':funding,
            'next_time':next_funding,'as_of':funding_time,
            'quality':quality('bybit_ticker',funding_time,current.get('_ticker_received_at'),timestamp,
                max_age_ms=300_000,value_present=funding_available),
            'estimated_cost_per_unit':funding_cost},
        'orderbook':book,'spot_futures':spot_futures,'trade_flow':flow,'open_interest':oi_data,
        'liquidations':liquidation_data,'quality':quality_data,'asset_class':asset_class,
        'instrument':({'symbol':instrument.symbol,'name':instrument.display_name,
            'asset_class':instrument.asset_class,'symbol_type':instrument.symbol_type,
            'market_region':instrument.market_region,'status':instrument.status,
            'launch_time':instrument.launch_time,'pre_listing':instrument.pre_listing}
            if instrument else {'symbol':symbol,'asset_class':asset_class,'status':'unknown'}),
        'compatible_statistics':{'status':'insufficient_data','sample_size':0,'period_days':None},
        'context_sources':{'news':'unavailable','macro_events':'unavailable'},
        'supporting_factors':indicators,'risks':risks,'unknown_data':sorted(set(unknown)),
        'signal_reason':signal.reason,'strategy_approval_required':not bool(signal.strategy_version)}
