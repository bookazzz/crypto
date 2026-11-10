"""Versioned snapshot and delta envelopes for live clients."""
from __future__ import annotations

import uuid


class VersionedPublisher:
    def __init__(self):
        self.stream_id=uuid.uuid4().hex
        self.revision=0
        self.previous=None

    def initial(self,payload):
        return payload|{'stream_id':self.stream_id,'revision':self.revision}

    def publish(self,payload):
        previous=self.previous
        self.revision+=1
        payload=payload|{'stream_id':self.stream_id,'revision':self.revision}
        self.previous=payload
        if previous is None or previous.get('stream_id')!=self.stream_id:
            return payload

        def changed_rows(key):
            old={row['id']:row for row in previous.get(key,[]) if row.get('id')}
            new={row['id']:row for row in payload.get(key,[]) if row.get('id')}
            return ([row for key,row in new.items() if old.get(key)!=row],
                    [key for key in old if key not in new])

        signals,removed_signals=changed_rows('signals')
        setups,removed_setups=changed_rows('setups')
        trades,removed_trades=changed_rows('trades')
        return {'type':'delta','stream_id':self.stream_id,'revision':self.revision,
            'published_at':payload['published_at'],'state':payload['state'],
            'signals_upsert':signals,'signals_remove':removed_signals,
            'setups_upsert':setups,'setups_remove':removed_setups,
            'trades_upsert':trades,'trades_remove':removed_trades}
