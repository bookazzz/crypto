from backend.decision_context import build_context, support_chart_levels, _book, _plain_summary
from backend.models import Candle, Signal
from backend.storage import Store


def test_order_book_reports_spread_depth_and_estimated_execution():
    book={'bids':{99.9:10,99.8:20},'asks':{100.1:10,100.2:20},'time':1_000,'now':2_000}
    result=_book(book,'long',5,100)
    assert result['available']
    assert result['spread_bps']>0
    assert result['ask_depth_usdt_top10']>0
    assert result['estimated_fill_ratio']==1
    assert result['estimated_impact_bps']>0
    assert result['age_ms']==1_000


def test_context_uses_closed_levels_and_persisted_market_observations():
    store=Store(':memory:')
    timestamp=4*86_400_000
    daily=[Candle(i*86_400_000,100,105 if i==3 else 102,95,100,10) for i in range(5)]
    daily[-1].high=900  # Current unclosed UTC day must not become a prior-day level.
    hourly=[Candle(i*3_600_000,100+i*.01,101+i*.01,99+i*.01,100+i*.01,1) for i in range(260)]
    five=[Candle(timestamp-86_400_000+i*300_000,100,101,99,100,10) for i in range(288)]
    fifteen=[Candle(timestamp-86_400_000+i*900_000,100,101,99,100,30) for i in range(96)]
    signal=Signal('ETHUSDT','15','ascending_triangle','long',timestamp,0,100,98,120,1,
        filter_results={'trend_pass':True,'volume_pass':True,'confirmation_pass':True,
                        'market_context':{'eligible':True,'scenario':'continuation'}})
    store.save_open_interest('ETHUSDT',[(timestamp-3_600_000,10),(timestamp-900_000,11),(timestamp,12)])
    for minute in range(timestamp//60_000-15,timestamp//60_000+1):
        store.save_market_minute('ETHUSDT',minute*60_000,100,50,updated=minute*60_000)
    tickers={'ETHUSDT':{'lastPrice':'100','price24hPcnt':'0.03','fundingRate':'0.0001',
                        'nextFundingTime':str(timestamp+4*3_600_000),'_quote_time':timestamp},
             'BTCUSDT':{'lastPrice':'100','price24hPcnt':'0.01'}}
    books={'ETHUSDT':{'bids':{99.9:100},'asks':{100.1:100},'time':timestamp-1_000,'now':timestamp}}
    result=build_context(signal,timestamp,
        {('ETHUSDT','D'):daily,('ETHUSDT','60'):hourly,('BTCUSDT','60'):hourly,
         ('ETHUSDT','5'):five,('ETHUSDT','15'):fifteen},
        tickers,['BTCUSDT','ETHUSDT'],store,{},books,store,
        {'ETHUSDT':{'rate':.00055,'time':timestamp,'source':'bybit_vip0_standard'}},.0005)
    assert result['relative_strength_24h']==0.02
    assert result['open_interest']['change_15m']>0
    assert result['open_interest']['change_1h']>0
    assert result['trade_flow']['5']['net_usdt']>0
    assert result['orderbook']['available']
    assert result['nearest_obstacle']['name']=='Максимум предыдущего дня'
    assert result['nearest_obstacle']['price']==105
    assert all(level['price']!=900 for level in result['levels'])
    assert result['scenario']=='conditions_met_review_manually'
    assert result['current_entry_state']=='candidate'
    assert result['fee_available']
    assert result['net_rr_to_target'] is not None
    assert result['spot_futures']['status']=='unmatched'
    late_tickers={**tickers,'ETHUSDT':tickers['ETHUSDT']|{'lastPrice':'119.5'}}
    late=build_context(signal,timestamp,
        {('ETHUSDT','D'):daily,('ETHUSDT','60'):hourly,('BTCUSDT','60'):hourly,
         ('ETHUSDT','5'):five,('ETHUSDT','15'):fifteen},
        late_tickers,['BTCUSDT','ETHUSDT'],store,{},books,store,
        {'ETHUSDT':{'rate':.00055,'time':timestamp,'source':'bybit_vip0_standard'}},.0005)
    assert late['scenario']=='late_entry'
    assert late['net_rr_to_target']<2
    assert 'вход' in late['plain_summary'] and 'запоздал' in late['plain_summary']
    assert 'лонг' in result['plain_summary']
    assert 'восходящий треугольник' in result['plain_summary']
    account_context=build_context(signal,timestamp,
        {('ETHUSDT','D'):daily,('ETHUSDT','60'):hourly,('BTCUSDT','60'):hourly,
         ('ETHUSDT','5'):five,('ETHUSDT','15'):fifteen},
        tickers,['BTCUSDT','ETHUSDT'],store,{},books,store,{},.0005,'account')
    assert not account_context['fee_available']
    assert account_context['fee_per_side'] is None
    assert account_context['net_rr_to_target'] is None
    assert account_context['costs_per_unit']['net_reward'] is None
    assert account_context['spot_futures']['status']=='unmatched'
    assert 'Комиссия аккаунта недоступна' in account_context['plain_summary']
    store.close()


def test_early_chart_support_levels_use_only_closed_daily_candles():
    day=86_400_000
    timestamp=4*day+12*3_600_000
    daily=[Candle(i*day,100,105,95,100,10) for i in range(5)]
    daily[4].low=1  # The current UTC day is not a support level yet.
    levels=support_chart_levels('ETHUSDT',timestamp,100,'long','5',2,
        {('ETHUSDT','D'):daily})

    assert levels
    assert all(level['kind']=='support' for level in levels)
    assert any(level['price']==95 for level in levels)
    assert all(level['price']!=1 for level in levels)
    assert all(level['distance_atr']<=5 for level in levels)


def test_plain_summary_explains_both_directions_and_failed_filters():
    long = Signal('ETHUSDT','15','ascending_triangle','long',0,0,100,98,110,1)
    short = Signal('ETHUSDT','15','double_top','short',0,0,100,102,90,1)
    passed = {'trend':True,'volume':True}
    assert 'лонг' in _plain_summary(long,'conditions_met_review_manually',passed,
        {'multiple':1.8},True,None)
    assert 'вверх' in _plain_summary(long,'conditions_met_review_manually',passed,
        {'multiple':1.8},True,None)
    assert 'шорт' in _plain_summary(short,'conditions_met_review_manually',passed,
        {'multiple':1.8},True,None)
    assert 'вниз' in _plain_summary(short,'conditions_met_review_manually',passed,
        {'multiple':1.8},True,None)
    rejected = _plain_summary(long,'rules_rejected',{'trend':False,'volume':False},
        {'multiple':1.1,'minimum':1.5},False,None)
    assert 'не предлагается' in rejected
    assert 'контекст 1ч/15м не прошёл фильтр' in rejected
    assert '1.10×' in rejected
    mixed = _plain_summary(short,'rules_rejected',{
        'trend':False,'hourly_bias':'down','structure_15m':'mixed'},
        {'multiple':1.8},False,None)
    assert 'направление 1ч вниз совпадает с направлением шорта' in mixed
    assert 'структура 15м смешанная' in mixed
    assert 'направление 1ч и структура 15м не подтверждают' not in mixed
    invalidated = _plain_summary(short,'scenario_invalidated',{
        'hourly_bias':'down','structure_15m':'mixed',
        'invalidation_reason':'цена вернулась выше границы фигуры'}, {},None,None)
    assert 'направление сигнала — шорт' in invalidated
    assert 'сценарий отменён' in invalidated
    assert 'цена вернулась выше границы фигуры' in invalidated
    assert 'структура 15м смешанная' in invalidated
    insufficient = _plain_summary(short,'insufficient_data',{}, {},None,None)
    assert 'не хватает свежих данных' in insufficient.lower()
    assert 'не предлагает вход' in insufficient


def test_invalidated_short_breakout_summary_distinguishes_signal_from_entry():
    store=Store(':memory:')
    timestamp=10*86_400_000
    signal=Signal('NEARUSDT','15','symmetric_triangle','short',timestamp,0,
        100,102,90,1,filter_results={
            'trend_pass':False,'volume_pass':True,'confirmation_pass':True,
            'market_context':{'scenario':'unconfirmed','eligible':False,
                'hourly_bias':'down','structure_15m':{'ready':True,'direction':'mixed'}}})
    candles={('NEARUSDT','15'):[Candle(timestamp-15*60_000,100,101,99,100)]}
    context=build_context(signal,timestamp,candles,
        {'NEARUSDT':{'lastPrice':'100.2','_quote_time':timestamp}},['NEARUSDT'],
        store,{}, {},store,{},.0005)
    assert signal.side=='short'
    assert context['entry_checks']['hourly_bias']=='down'
    assert context['entry_checks']['structure_15m']=='mixed'
    assert context['current_entry_state']=='scenario_invalidated'
    assert context['scenario']=='scenario_invalidated'
    assert 'направление сигнала — шорт' in context['plain_summary']
    assert 'совпадает с направлением шорта' in context['plain_summary']
    assert 'структура 15м смешанная' in context['plain_summary']
    assert 'цена вернулась выше границы фигуры' in context['plain_summary']
    store.close()


def test_context_does_not_treat_missing_quote_as_signal_price():
    store=Store(':memory:')
    timestamp=10*86_400_000
    signal=Signal('OLDUSDT','5','ascending_triangle','long',timestamp,0,100,98,110,1)
    result=build_context(signal,timestamp,{}, {}, ['BTCUSDT'],store,{}, {},store,{},.0005)
    assert result['scenario']=='insufficient_data'
    assert result['current_price'] is None
    assert result['current_quote_time'] is None
    assert result['quote_age_ms'] is None
    assert result['current_entry_state']=='insufficient_data'
    assert 'Нет котировки по инструменту' in result['plain_summary']
    store.close()


def test_context_explains_stale_quote_and_missing_closed_candles():
    store=Store(':memory:')
    timestamp=10*86_400_000
    signal=Signal('OLDUSDT','5','ascending_triangle','long',timestamp,0,100,98,110,1)
    stale=build_context(signal,timestamp,{},
        {'OLDUSDT':{'lastPrice':'101','_quote_time':timestamp-30_001}},['BTCUSDT'],
        store,{}, {},store,{},.0005)
    assert stale['scenario']=='insufficient_data'
    assert stale['current_price']==101
    assert stale['current_quote_time']==timestamp-30_001
    assert any('Котировка устарела' in item for item in stale['unknown_data'])
    assert 'возраст 30 с' in stale['plain_summary']
    missing_candles=build_context(signal,timestamp,
        {('OLDUSDT','60'):[Candle(timestamp-3_600_000,100,101,99,100)]},
        {'OLDUSDT':{'lastPrice':'101','_quote_time':timestamp}},['BTCUSDT'],
        store,{}, {},store,{},.0005)
    assert missing_candles['scenario']=='insufficient_data'
    assert any('Нет закрытых свечей таймфрейма 5м' in item for item in missing_candles['unknown_data'])
    stale_candles=build_context(signal,timestamp,
        {('OLDUSDT','5'):[Candle(timestamp-3*5*60_000,100,101,99,100)]},
        {'OLDUSDT':{'lastPrice':'101','_quote_time':timestamp}},['BTCUSDT'],
        store,{}, {},store,{},.0005)
    assert stale_candles['scenario']=='insufficient_data'
    assert stale_candles['current_entry_state']=='insufficient_data'
    assert 'Закрытые свечи таймфрейма 5м устарели' in stale_candles['unknown_data']
    store.close()
