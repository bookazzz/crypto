"""Order book synchronization and spot/perpetual comparison helpers."""
from math import isfinite


DEPTH_BAND = 0.001
MAX_BOOK_AGE_MS = 5_000
MAX_BOOK_SKEW_MS = 2_000


def apply_orderbook(books, symbol, message, received_at, clock_offset=0):
    """Apply a Bybit orderbook snapshot/delta; return False for unusable deltas."""
    data=message.get('data') or {}
    try:
        update_id=int(data.get('u',0) or 0)
        sequence=int(data.get('seq',0) or 0)
    except (TypeError,ValueError):
        return False
    snapshot=message.get('type')=='snapshot' or update_id==1
    book=books.get(symbol)
    if not snapshot:
        if not book or not book.get('valid'):
            return False
        if update_id and book.get('update_id') and update_id<=book['update_id']:
            return False
        # seq is a cross-symbol sequence, not a contiguous counter for this book.
        # Reject duplicates only; gaps are not evidence that this symbol lost a delta.
        if sequence and book.get('sequence') and sequence<=book['sequence']:
            return False
    if snapshot:
        generation=int(book.get('generation',0) or 0)+1 if book else 1
        book={'bids':{},'asks':{},'valid':True,'generation':generation}
    for key,side in (('b','bids'),('a','asks')):
        for raw_price,raw_size in data.get(key,[]):
            try:
                price=float(raw_price);size=float(raw_size)
            except (TypeError,ValueError):
                continue
            if not isfinite(price) or not isfinite(size) or price<=0:
                continue
            if size<=0:
                book[side].pop(price,None)
            else:
                book[side][price]=size
    if not book['bids'] or not book['asks']:
        book['valid']=False
    else:
        book['valid']=max(book['bids'])<min(book['asks'])
    try:
        event_time=int(message.get('ts',data.get('ts',received_at+clock_offset)))
    except (TypeError,ValueError):
        event_time=received_at+clock_offset
    book.update({'time':event_time,'now':received_at+clock_offset,
                 'received_at':received_at,'update_id':update_id,'sequence':sequence,
                 'generation':int(book.get('generation',0) or 0)+1 if not snapshot
                    else int(book.get('generation',1) or 1)})
    books[symbol]=book
    return True


def book_metrics(book, now, band=DEPTH_BAND):
    if not book or not book.get('valid') or not book.get('bids') or not book.get('asks'):
        return {'available':False,'age_ms':None}
    bids=book['bids'];asks=book['asks']
    best_bid=max(bids);best_ask=min(asks)
    if best_bid<=0 or best_ask<=0 or best_bid>=best_ask:
        return {'available':False,'age_ms':None}
    mid=(best_bid+best_ask)/2
    coverage={}
    depths={}
    for bps in (5,10,25):
        fraction=bps/10_000
        lower=mid*(1-fraction);upper=mid*(1+fraction)
        band_bids=[(price,size) for price,size in bids.items() if lower<=price<=mid]
        band_asks=[(price,size) for price,size in asks.items() if mid<=price<=upper]
        depths[str(bps)]={'bid_usdt':sum(price*size for price,size in band_bids),
                          'ask_usdt':sum(price*size for price,size in band_asks)}
        coverage[str(bps)]={'bid':min(bids)<=lower,'ask':max(asks)>=upper}
    first_bids=sorted(bids.items(),reverse=True)[:10]
    first_asks=sorted(asks.items())[:10]
    bid_depth=sum(price*size for price,size in first_bids)
    ask_depth=sum(price*size for price,size in first_asks)
    complete=coverage[str(round(band*10_000))]['bid'] and coverage[str(round(band*10_000))]['ask'] if round(band*10_000) in (5,10,25) else (
        min(bids)<=mid*(1-band) and max(asks)>=mid*(1+band))
    imbalance_band=str(round(band*10_000))
    band_depth=depths.get(imbalance_band)
    total=(band_depth['bid_usdt']+band_depth['ask_usdt']) if band_depth else bid_depth+ask_depth
    band_imbalance=(100*(band_depth['bid_usdt']-band_depth['ask_usdt'])/total
                    if band_depth and total else None)
    return {'available':True,'mid_price':mid,'spread_bps':(best_ask-best_bid)/mid*10_000,
        'bid_depth_usdt':bid_depth,'ask_depth_usdt':ask_depth,
        'imbalance_pct':band_imbalance if complete else None,
        'depth_complete':complete,'depth_by_band_bps':depths,'coverage_by_band_bps':coverage,
        'age_ms':max(0,now-int(book.get('time',0))),
        'as_of':int(book.get('time',0)),'received_at':book.get('received_at'),
        'generation':int(book.get('generation',0) or 0),'sequence':book.get('sequence')}


def compare_spot_perpetual(symbol, spot_symbol, timestamp, perpetual_books, spot_books,
                           match_error=''):
    perpetual=book_metrics(perpetual_books.get(symbol),timestamp)
    result={'symbol':symbol,'spot_symbol':spot_symbol,'status':'waiting_snapshot',
        'price_basis_pct':None,'imbalance_difference_pp':None,
        'perpetual':perpetual,'spot':book_metrics(spot_books.get(spot_symbol),timestamp) if spot_symbol else None,
        'explanation':''}
    if match_error:
        result.update(status='unavailable',explanation=f'Не удалось обновить список спотовых пар: {match_error}.')
        return result
    if spot_symbol is None:
        result.update(status='unmatched',explanation='Для этого контракта не найдена точная спотовая пара USDT.')
        return result
    spot=result['spot']
    if not perpetual['available'] or not spot['available']:
        result['explanation']='Ждём полный снимок стакана спота и бессрочного контракта.'
        return result
    if not 0<=perpetual['age_ms']<=MAX_BOOK_AGE_MS or not 0<=spot['age_ms']<=MAX_BOOK_AGE_MS:
        result.update(status='stale',explanation='Один из стаканов старше 5 секунд; сравнение цены не обновлено.')
        return result
    if abs(perpetual['as_of']-spot['as_of'])>MAX_BOOK_SKEW_MS:
        result.update(status='unsynchronized',explanation='Данные стаканов спота и контракта получены в разное время.')
        return result
    basis=100*(perpetual['mid_price']/spot['mid_price']-1)
    result['price_basis_pct']=basis
    if basis>0:
        explanation=f'Контракт дороже спота на {basis:.2f}%.'
    elif basis<0:
        explanation=f'Контракт дешевле спота на {abs(basis):.2f}%.'
    else:
        explanation='Средние цены спота и контракта совпадают.'
    if perpetual['depth_complete'] and spot['depth_complete']:
        delta=perpetual['imbalance_pct']-spot['imbalance_pct']
        result['imbalance_difference_pp']=delta
        spot_side='покупку' if spot['imbalance_pct']>0 else 'продажу' if spot['imbalance_pct']<0 else None
        perp_side='покупку' if perpetual['imbalance_pct']>0 else 'продажу' if perpetual['imbalance_pct']<0 else None
        if spot_side and perp_side and spot_side!=perp_side:
            explanation+=f' В спотовом стакане больше заявок на {spot_side}, в контракте — на {perp_side}.'
        elif spot_side and perp_side:
            explanation+=f' В обоих стаканах больше видимых заявок на {spot_side}.'
        else:
            explanation+=' В одном из стаканов заявки на покупку и продажу примерно равны.'
        result['status']='ready'
    else:
        result['status']='partial'
        explanation+=' Диапазона стаканов не хватает для полного сравнения дисбаланса.'
    result['explanation']=explanation+' Это описание видимых заявок, а не прогноз цены.'
    return result
