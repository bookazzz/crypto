"""Common, explicit quality metadata for market measurements."""
from math import isfinite


QUALITY_STATES = {'valid', 'warming_up', 'partial', 'stale', 'invalid', 'unavailable'}


def quality(source, event_at, received_at, now, *, max_age_ms=None,
            coverage_ms=None, window_ms=None, connected=True, value_present=True,
            reasons=None):
    event_at=int(event_at) if event_at is not None else None
    received_at=int(received_at) if received_at is not None else None
    age_ms=max(0,int(now)-event_at) if event_at is not None else None
    status='valid'
    notes=list(reasons or [])
    if not value_present:
        status='unavailable'
    elif not connected:
        status='partial'
        notes.append('stream_disconnected')
    elif max_age_ms is not None and age_ms is not None and age_ms>max_age_ms:
        status='stale'
    elif window_ms is not None and (coverage_ms or 0)<window_ms:
        status='warming_up' if not coverage_ms else 'partial'
    return {'status':status,'source':source,'event_at':event_at,'received_at':received_at,
            'age_ms':age_ms,'coverage_ms':coverage_ms,'window_ms':window_ms,'reasons':notes}


def valid_number(value, *, minimum=None, allow_zero=True):
    try:
        number=float(value)
    except (TypeError,ValueError):
        return None
    if not isfinite(number) or (minimum is not None and number<minimum):
        return None
    if not allow_zero and number==0:
        return None
    return number


def valid_ohlc(row):
    """Return (valid, reason) for one Bybit candle before it enters state."""
    get=row.get if hasattr(row,'get') else lambda key,default=None:getattr(row,key,default)
    values=[]
    for key in ('open','high','low','close','volume'):
        number=valid_number(get(key),minimum=0 if key=='volume' else None)
        if number is None:
            return False,f'non_finite_or_invalid_{key}'
        values.append(number)
    opening,high,low,close,volume=values
    if min(opening,high,low,close)<=0:
        return False,'non_positive_price'
    if high<max(opening,close,low) or low>min(opening,close,high):
        return False,'inconsistent_ohlc'
    try:
        if int(get('start',get('time')))<0:
            return False,'invalid_timestamp'
    except (KeyError,TypeError,ValueError):
        return False,'invalid_timestamp'
    if get('turnover') not in (None,'') and valid_number(get('turnover'),minimum=0) is None:
        return False,'invalid_turnover'
    return True,''
