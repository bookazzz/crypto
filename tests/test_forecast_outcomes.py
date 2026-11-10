import asyncio

from backend.forecast_outcomes import (
    HORIZONS_MINUTES, MIN_SAMPLE_SIZE, _cohort_summary, build_statistics_index,
    evaluate_horizons, make_observation, summarize_filter_ablation,
    summarize_forecasts,
)
from backend.models import Candle
from backend.storage import Store


def observation(side='long'):
    return {
        'id': 'signal-1', 'symbol': 'ENAUSDT', 'timeframe': '15',
        'pattern': 'bull_flag' if side == 'long' else 'bear_flag', 'side': side,
        'event_at': 0, 'available_at': 1_000, 'rules_version': 'geometry-v1+figures-context-v4',
        'evaluation_version': 'forecast-outcomes-v1', 'entry_price': 100.,
        'stop': 95. if side == 'long' else 105.,
        'target': 105. if side == 'long' else 95., 'status': 'pending',
        'features': {'market_scenario': 'continuation', 'hourly_context_pass': True,
            'volume_pass': True, 'coin_10d_supports_side': True,
            'btc_10d_adverse_when_strongly_correlated': False},
        'costs': {'fee_per_side': .001, 'slippage_per_side': .001,
                  'funding_rate': 0., 'funding_next_at': 0, 'tick_size': .01},
    }


def bars(side='long', *, target_at=None, stop_at=None, ambiguous_at=None, count=6):
    result=[]
    for index in range(count):
        start=300_000*(index+1)
        o=c=100.
        h=l=100.5
        if index==target_at:
            h,l,c=(106.,99.,105.5) if side=='long' else (101.,94.,94.5)
        if index==stop_at:
            h,l,c=(101.,94.,94.5) if side=='long' else (106.,99.,105.5)
        if index==ambiguous_at:
            h,l,c=106.,94.,100.
        result.append(Candle(start,o,h,l,c))
    return result


def test_long_horizons_only_use_complete_closed_bars_and_target_stays_final():
    rows=evaluate_horizons(observation(),bars(target_at=1),now=2_100_000)
    assert [row['horizon_minutes'] for row in rows]==list(HORIZONS_MINUTES)
    assert rows[0]['label']=='timeout'
    assert rows[1]['label']=='target'
    assert rows[2]['label']=='target'
    assert rows[1]['net_pnl_per_unit']>0
    assert rows[1]['net_r']>0


def test_short_target_and_stop_are_mirrored():
    target=evaluate_horizons(observation('short'),bars('short',target_at=0),2_100_000)
    stop=evaluate_horizons(observation('short'),bars('short',stop_at=0),2_100_000)
    assert target[0]['label']=='target' and target[0]['net_r']>0
    assert stop[0]['label']=='stop' and stop[0]['net_r']<0


def test_simultaneous_stop_and_target_is_ambiguous_with_conservative_stop_result():
    rows=evaluate_horizons(observation(),bars(ambiguous_at=0),2_100_000)
    assert rows[0]['status']=='ambiguous'
    assert rows[0]['label']=='stop'
    assert rows[0]['conservative']['status']=='stop'
    assert rows[0]['net_r']<0


def test_missing_or_future_candles_are_not_filled_with_zero_or_open_bar():
    future=Candle(300_000,100.,110.,90.,105.)
    assert evaluate_horizons(observation(),[future],599_999)==[]
    rows=evaluate_horizons(observation(),[],2_100_000)
    assert all(row['status']=='data_missing' for row in rows)
    gap=evaluate_horizons(observation(),bars(count=5),2_100_000)
    assert gap[-1]['status']=='data_missing'


def test_outside_window_bars_do_not_change_completed_horizon():
    baseline=evaluate_horizons(observation(),bars(count=1),600_000)[0]
    added=evaluate_horizons(observation(),bars(target_at=1),600_000)[0]
    assert baseline['label']==added['label']=='timeout'


def test_observation_requires_confirmed_current_15m_signal_and_freezes_factors():
    signal={'id':'s','symbol':'ENAUSDT','timeframe':'15','pattern':'bull_flag','side':'long',
        'event_at':100,'time':100,'stop':95,'target':105,
        'filter_results':{'confirmation_pass':True,'detection_profile':'expanded',
            'market_context':{'scenario':'continuation','eligible':True},'volume_pass':False,
            'delivery':{'mode':'current'}}}
    context={'as_of':120,'current_price':100,'current_quote_time':119,
        'quality':{'quote':{'status':'valid'}},'fee_available':True,'fee_per_side':.001,
        'slippage_per_side':.001,'market_background':{'periods':{'10':{
            'direction':'down','btc_direction':'down'}},'btc_correlation':{'value':.8}},
        'funding':{'rate':.001,'next_time':200,'as_of':119}}
    result=make_observation(signal,context,.01)
    assert result['features']['volume_pass'] is False
    assert result['features']['coin_10d_supports_side'] is False
    assert result['features']['btc_10d_adverse_when_strongly_correlated'] is True
    assert result['rules_version']=='geometry-v1+figures-context-v4'
    assert (result['entry_price'],result['stop'],result['target'])==(100.,95.,105.)
    short=make_observation(signal|{'id':'short','pattern':'bear_flag','side':'short',
            'stop':105,'target':95},
        context|{'market_background':{'periods':{'10':{'direction':'down','btc_direction':'up'}},
            'btc_correlation':{'value':.8}}},.01)
    assert short['features']['coin_10d_supports_side'] is True
    assert short['features']['btc_10d_adverse_when_strongly_correlated'] is True
    late_quote=make_observation(signal|{'event_at':200},context)
    assert late_quote['status']=='no_fill'
    assert make_observation(signal|{'timeframe':'5'},context) is None
    assert make_observation(signal|{'filter_results':signal['filter_results']|{'confirmation_pass':False}},context) is None
    assert make_observation(signal|{'filter_results':signal['filter_results']|{'delivery':{'mode':'history'}}},context) is None


def test_store_keeps_first_snapshot_and_resolves_each_horizon_once():
    async def run():
        store=Store(':memory:')
        first=observation()
        assert await store.arecord_forecast_observation(first)
        changed=first|{'entry_price':125.}
        assert not await store.arecord_forecast_observation(changed)
        pending=await store.apending_forecast_observations()
        assert pending[0]['entry_price']==100.
        outcomes=evaluate_horizons(first,bars(target_at=1),2_100_000)
        assert await store.aupdate_forecast_outcomes(first['id'],outcomes,2_100_000)==3
        assert await store.aupdate_forecast_outcomes(first['id'],[],2_200_000)==0
        rows=await store.aforecast_dataset()
        assert rows[0]['entry_price']==100.
        assert [x['label'] for x in rows[0]['outcomes']]==['timeout','target','target']
        store.close()
    asyncio.run(run())


def test_statistics_show_sample_threshold_symbol_breakdown_and_filter_misses():
    rows=[]
    for index in range(MIN_SAMPLE_SIZE):
        item=observation()
        item['id']=str(index);item['symbol']='ENAUSDT' if index%2 else 'BTCUSDT'
        item['available_at']=index*86_400_000
        item['features']=item['features']|{'volume_pass':index%2==0}
        item['outcomes']=[{'horizon_minutes':5,'status':'target' if index%2 else 'stop',
            'label':'target' if index%2 else 'stop','net_r':1. if index%2 else -1.}]
        rows.append(item)
    stats=summarize_forecasts(rows,symbol='ENAUSDT',pattern='bull_flag',side='long',
        rule_version='geometry-v1+figures-context-v4',market_scenario='continuation')
    assert stats['horizons']['5']['all_symbols']['status']=='ready'
    assert stats['horizons']['5']['this_symbol']['sample_size']==50
    assert stats['horizons']['5']['all_symbols']['target_rate']==.5
    index=build_statistics_index(rows)
    indexed=index[('bull_flag','long','geometry-v1+figures-context-v4','continuation')]
    assert indexed['horizons']['5']['all_symbols']['sample_size']==MIN_SAMPLE_SIZE
    report=summarize_filter_ablation(rows)
    factor=report['horizons']['5']['factors']['volume_pass']
    assert factor['excluded_targets']==50
    assert factor['excluded_stops']==0


def test_cohort_summary_keeps_ambiguous_and_data_quality_counts_out_of_target_rate():
    summary=_cohort_summary([
        {'status':'target','label':'target','net_r':1,'available_at':0},
        {'status':'timeout','label':'timeout','net_r':0,'available_at':10},
        {'status':'ambiguous','label':'stop','net_r':-1,'conservative':{'net_r':-1},'available_at':20},
        {'status':'data_missing','label':'data_missing','available_at':30},
        {'status':'no_fill','label':'no_fill','available_at':40},
    ])
    assert summary['sample_size']==2
    assert summary['target_rate']==.5
    assert summary['ambiguous_count']==1
    assert summary['data_missing_count']==1
    assert summary['no_fill_count']==1
