from backend.market_comparison import apply_orderbook, book_metrics, compare_spot_perpetual


NOW = 1_800_000_000_000


def book(mid, timestamp=NOW, cover=True):
    edge = mid * 0.001 if cover else mid * 0.0002
    inner = edge / 2
    return {'valid': True, 'time': timestamp, 'bids': {mid-edge: 1, mid-inner: 1},
            'asks': {mid+inner: 1, mid+edge: 1}}


def test_book_depth_is_usdt_within_midpoint_band_and_imbalance_sign():
    metrics = book_metrics(book(100), NOW)
    assert metrics['mid_price'] == 100
    assert abs(metrics['bid_depth_usdt'] - 199.85) < 1e-9
    assert abs(metrics['ask_depth_usdt'] - 200.15) < 1e-9
    assert metrics['imbalance_pct'] < 0
    assert metrics['depth_complete'] is True


def test_comparison_basis_sign_and_distinct_book_prices():
    result = compare_spot_perpetual('BTCUSDT', 'BTCUSDT', NOW,
        {'BTCUSDT': book(100.12, NOW)}, {'BTCUSDT': book(100, NOW-500)})
    assert result['status'] == 'ready'
    assert round(result['price_basis_pct'], 3) == 0.12
    expected = result['perpetual']['imbalance_pct'] - result['spot']['imbalance_pct']
    assert abs(result['imbalance_difference_pp'] - expected) < 1e-9
    assert 'дороже спота' in result['explanation']


def test_unmatched_stale_unsynchronized_and_partial_are_explicit():
    assert compare_spot_perpetual('X', None, NOW, {}, {})['status'] == 'unmatched'
    stale = compare_spot_perpetual('X', 'X', NOW+5_001,
        {'X': book(100)}, {'X': book(100)})
    assert stale['status'] == 'stale'
    skewed = compare_spot_perpetual('X', 'X', NOW,
        {'X': book(100)}, {'X': book(100, NOW-2_001)})
    assert skewed['status'] == 'unsynchronized'
    partial = compare_spot_perpetual('X', 'X', NOW,
        {'X': book(100, cover=False)}, {'X': book(100)})
    assert partial['status'] == 'partial'
    assert partial['spot']['bid_depth_usdt'] > 0
    assert partial['imbalance_difference_pp'] is None


def test_orderbook_requires_snapshot_applies_delta_and_discards_duplicates():
    books = {}
    snapshot = {'type': 'snapshot', 'ts': NOW, 'data': {'u': 10,
        'b': [['99', '2'], ['98', '3']], 'a': [['101', '4'], ['102', '5']]}}
    assert apply_orderbook(books, 'BTCUSDT', snapshot, NOW)
    assert not apply_orderbook(books, 'X', {'type': 'delta', 'ts': NOW,
        'data': {'u': 11, 'b': [['99', '2']], 'a': []}}, NOW)
    delta = {'type': 'delta', 'ts': NOW+1, 'data': {'u': 11,
        'b': [['99', '0'], ['97', '1']], 'a': [['101', '6']]}}
    assert apply_orderbook(books, 'BTCUSDT', delta, NOW+1)
    assert 99 not in books['BTCUSDT']['bids']
    assert books['BTCUSDT']['bids'][97] == 1
    assert books['BTCUSDT']['asks'][101] == 6
    assert not apply_orderbook(books, 'BTCUSDT', delta, NOW+2)
    reset = {'type': 'delta', 'ts': NOW+3, 'data': {'u': 1,
        'b': [['96', '2']], 'a': [['104', '3']]}}
    assert apply_orderbook(books, 'BTCUSDT', reset, NOW+3)
    assert set(books['BTCUSDT']['bids']) == {96}


def test_orderbook_cross_sequence_jump_is_not_treated_as_missing_delta():
    books={}
    assert apply_orderbook(books,'BTCUSDT',{'type':'snapshot','ts':NOW,'data':{
        'u':10,'seq':100,'b':[['99','2']],'a':[['101','2']]}},NOW)
    assert apply_orderbook(books,'BTCUSDT',{'type':'delta','ts':NOW+1,'data':{
        'u':11,'seq':900,'b':[['98','1']],'a':[]}},NOW+1)
    assert 98 in books['BTCUSDT']['bids']
