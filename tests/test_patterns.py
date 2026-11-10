import math
import pytest
from backend.models import Candle
from backend.patterns import detect, detect_combined, pivots, atr, NAMES


def interpolate(points,x):
    if x<=points[0][0]:
        return points[0][1]
    for (a,y),(b,z) in zip(points,points[1:]):
        if x<=b:
            return y+(z-y)*(x-a)/(b-a)
    return points[-1][1]


def channel(pattern):
    directions={
        'ascending_triangle':(0,.25,'long'),
        'descending_triangle':(-.25,0,'short'),
        'symmetric_triangle':(-.15,.15,'long'),
        'rising_wedge':(.1,.35,'short'),
        'falling_wedge':(-.35,-.1,'long'),
        'bull_flag':(-.08,-.08,'long'),
        'bear_flag':(.08,.08,'short'),
        'bull_pennant':(-.15,.15,'long'),
        'bear_pennant':(-.15,.15,'short'),
        'rectangle':(0,0,'long'),
        'ascending_channel':(.2,.2,'long'),
        'descending_channel':(-.2,-.2,'short'),
    }
    um,lm,side=directions[pattern]
    pole=pattern in ('bull_flag','bear_flag','bull_pennant','bear_pennant')
    prefix=20 if pole else 0
    c=[]
    if pole:
        delta=100 if side=='long' else -100
        for i in range(prefix):
            price=112.5-delta*(prefix-i)/prefix
            c.append(Candle(i*900000,price,price+.7,price-.7,price))
    for i in range(65):
        upper=125+um*i
        lower=100+lm*i
        # Clear peaks and troughs with local, strictly unique touches.
        hd=min(abs(i-j) for j in (4,16,28,40,52))
        ld=min(abs(i-j) for j in (10,22,34,46,58))
        high=upper-min(hd,6)*.9
        low=lower+min(ld,6)*.9
        mid=(high+low)/2
        if i==64:
            mid=upper+3 if side=='long' else lower-3
            high=max(high,mid+.1)
            low=min(low,mid-.1)
        c.append(Candle((i+prefix)*900000,mid,high,low,mid))
    return c


def swings(pattern):
    head='head_shoulders' in pattern
    inverse=pattern in ('double_bottom','inverse_head_shoulders')
    top=[(0,110),(5,120),(17,108),(29,150 if head else 120),(41,108),(53,120),(63,109)] if head else [(0,110),(5,120),(25,108),(45,120),(63,110)]
    bottom=[(0,105),(5,110),(17,100),(29,110),(41,100),(53,110),(63,104)] if head else [(0,105),(5,110),(25,100),(45,110),(63,104)]
    c=[]
    for i in range(65):
        hi,lo=interpolate(top,i),interpolate(bottom,i)
        mid=(hi+lo)/2
        if i==64:
            mid=96
            lo=95.9
        if inverse:
            hi,lo,mid=240-lo,240-hi,240-mid
        c.append(Candle(i*900000,mid,hi,lo,mid))
    return c


@pytest.mark.parametrize('pattern',list(NAMES))
def test_each_pattern_positive_and_no_unconfirmed_breakout(pattern):
    candles=swings(pattern) if pattern.startswith('double') or 'head_shoulders' in pattern else channel(pattern)
    signals=detect('BTCUSDT','15',candles)
    assert pattern in {s.pattern for s in signals}, (pattern,atr(candles),[(p.index,p.price,p.kind) for p in pivots(candles[:-1])])
    assert not any(s.pattern==pattern for s in detect('BTCUSDT','15',candles[:-1]))
    for s in signals:
        assert s.time==candles[-1].time+900000
        assert all(p['time']<=candles[-1].time-4*900000 for p in s.pivots)


@pytest.mark.parametrize('pattern',list(NAMES))
def test_no_signal_without_breakout(pattern):
    candles=swings(pattern) if pattern.startswith('double') or 'head_shoulders' in pattern else channel(pattern)
    last=candles[-1]
    last.close=candles[-2].close
    last.high=max(last.close,last.open)
    last.low=min(last.close,last.open)
    assert not any(s.pattern==pattern for s in detect('BTCUSDT','15',candles))


@pytest.mark.parametrize('pattern',['rectangle','ascending_channel','descending_channel'])
def test_experimental_patterns_are_manual_only_and_use_closed_breakouts(pattern):
    candles=channel(pattern)
    assert not any(s.pattern==pattern for s in detect('BTCUSDT','15',candles))
    found=detect('BTCUSDT','15',candles,include_experimental=True)
    assert pattern in {s.pattern for s in found}
    formed=detect('BTCUSDT','15',candles[:-1],stage='setup',include_experimental=True)
    assert pattern in {s.pattern for s in formed}


@pytest.mark.parametrize('pattern',list(NAMES))
def test_each_formed_pattern_is_available_before_breakout(pattern):
    candles=swings(pattern) if pattern.startswith(('double','head_shoulders','inverse')) else channel(pattern)
    formed=detect('BTCUSDT','15',candles[:-1],stage='setup')
    assert pattern in {s.pattern for s in formed}
    assert all(s.time==candles[-2].time+900000 for s in formed)
    assert all(p['time']<=candles[-2].time-4*900000 for s in formed for p in s.pivots)


def test_flat_price_does_not_form_early_pattern():
    candles=[Candle(i*900000,100,101,99,100) for i in range(160)]
    assert detect('BTCUSDT','15',candles,stage='setup')==[]


def test_pivots_require_three_right_bars():
    c=[Candle(i,1,2,0,1) for i in range(10)]
    c[7].high=10
    assert not pivots(c)
    c.append(Candle(10,1,2,0,1))
    assert [(p.index,p.kind) for p in pivots(c)]==[(7,'high')]


def test_prefix_result_unaffected_by_future_data():
    c=channel('ascending_triangle')
    original=[s.dict() for s in detect('BTCUSDT','15',c)]
    extended=c+[Candle(65*900000,500,600,400,500)]
    assert original==[s.dict() for s in detect('BTCUSDT','15',extended[:65])]


@pytest.mark.parametrize(('stage','sensitivity'),[
    ('breakout','standard'),('breakout','expanded'),('setup','early')])
def test_detector_input_can_be_cropped_to_atr_and_geometry_window(stage,sensitivity):
    prefix=[Candle(i*900000,100,101,99,100) for i in range(300)]
    suffix=channel('ascending_triangle')
    shifted=[Candle((i+300)*900000,c.open,c.high,c.low,c.close,c.volume)
             for i,c in enumerate(suffix)]
    full=prefix+shifted
    assert [s.dict() for s in detect('BTCUSDT','15',full,stage=stage,
        sensitivity=sensitivity,include_experimental=True)] == [
        s.dict() for s in detect('BTCUSDT','15',full[-251:],stage=stage,
            sensitivity=sensitivity,include_experimental=True)]


def test_expanded_profile_accepts_smaller_closed_candle_breakout():
    candles=channel('ascending_triangle')
    standard=detect('BTCUSDT','15',candles)
    source=next(s for s in standard if s.pattern=='ascending_triangle')
    last=candles[-1]
    last.close=source.boundary+.075*source.atr
    last.high=last.close+.1
    last.low=min(last.low,last.close-.1)
    assert not any(s.pattern=='ascending_triangle' for s in detect('BTCUSDT','15',candles))
    expanded=detect('BTCUSDT','15',candles,sensitivity='expanded')
    assert any(s.pattern=='ascending_triangle' for s in expanded)


def test_unknown_sensitivity_is_rejected():
    with pytest.raises(ValueError):
        detect('BTCUSDT','15',[Candle(i,100,101,99,100) for i in range(30)],
               sensitivity='unbounded')


def test_early_setup_uses_only_pre_decision_atr_and_closed_pivots():
    candles=channel('ascending_triangle')[:-1]
    found=detect('BTCUSDT','15',candles,stage='setup',sensitivity='early')
    signal=next(s for s in found if s.pattern=='ascending_triangle')
    assert signal.atr==pytest.approx(atr(candles[:-1]))
    assert signal.time==candles[-1].time+900000
    assert all(p['time']<=candles[-2].time-4*900000 for p in signal.pivots)
    with pytest.raises(ValueError):
        detect('BTCUSDT','15',candles,sensitivity='early')


@pytest.mark.parametrize('pattern',['ascending_triangle','bull_flag','double_bottom'])
def test_combined_detector_preserves_each_profiles_original_geometry(pattern):
    candles=(swings(pattern) if pattern.startswith(('double','head_shoulders','inverse'))
             else channel(pattern))
    breakout,setup=detect_combined('BTCUSDT','15',candles)
    assert [signal.dict() for signal in breakout]==[
        signal.dict() for signal in detect('BTCUSDT','15',candles,stage='breakout')]
    assert [signal.dict() for signal in setup]==[
        signal.dict() for signal in detect('BTCUSDT','15',candles,stage='setup',sensitivity='early')]


def test_flat_market_has_no_figures():
    assert detect('BTCUSDT','15',[Candle(i*900000,100,101,99,100) for i in range(150)])==[]


@pytest.mark.parametrize('pattern',['double_top','double_bottom','head_shoulders','inverse_head_shoulders'])
def test_mismatched_shoulders_or_tops_rejected(pattern):
    c=swings(pattern)
    # Move the right shoulder/top by much more than the matching tolerance.
    index=53 if 'head_shoulders' in pattern else 45
    if pattern in ('double_top','head_shoulders'):
        c[index].high+=15
    else:
        c[index].low-=15
    assert not any(s.pattern==pattern for s in detect('BTCUSDT','15',c))
