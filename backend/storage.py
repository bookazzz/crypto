import asyncio
import json
import logging
import sqlite3
from pathlib import Path
from datetime import datetime, timezone
from dataclasses import asdict
from contextlib import contextmanager
from .models import Candle
from .write_queue import SQLiteWriteQueue


class Store:
    SCHEMA_VERSION = 7

    def __init__(self, path: str):
        self.path = path
        if path != ':memory:':
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path, check_same_thread=False)
        self._invalid_setup_payloads_logged=set()
        self.migration_backup = self._backup_before_migration()
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.executescript('''
            CREATE TABLE IF NOT EXISTS state (id INTEGER PRIMARY KEY CHECK(id=1), payload TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS signals (id TEXT PRIMARY KEY, time INTEGER NOT NULL, payload TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS candles (symbol TEXT, timeframe TEXT, time INTEGER, payload TEXT,
                PRIMARY KEY(symbol,timeframe,time));
            CREATE TABLE IF NOT EXISTS candles_archive (symbol TEXT, timeframe TEXT, time INTEGER, payload TEXT,
                PRIMARY KEY(symbol,timeframe,time));
            CREATE TABLE IF NOT EXISTS market_minutes (symbol TEXT, time INTEGER, buy REAL NOT NULL,
                sell REAL NOT NULL, spread_bps REAL, bid_depth REAL, ask_depth REAL, updated INTEGER,
                coverage_ms INTEGER NOT NULL DEFAULT 0, window_ms INTEGER NOT NULL DEFAULT 60000,
                source TEXT NOT NULL DEFAULT 'legacy', quality TEXT NOT NULL DEFAULT 'unknown',
                PRIMARY KEY(symbol,time));
            CREATE TABLE IF NOT EXISTS open_interest (symbol TEXT, time INTEGER, value REAL NOT NULL,
                value_usdt REAL, source TEXT NOT NULL DEFAULT 'legacy', received_at INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY(symbol,time));
            CREATE TABLE IF NOT EXISTS liquidation_minutes (symbol TEXT NOT NULL, time INTEGER NOT NULL,
                long_usdt REAL NOT NULL DEFAULT 0, short_usdt REAL NOT NULL DEFAULT 0,
                count INTEGER NOT NULL DEFAULT 0, quality TEXT NOT NULL DEFAULT 'unknown',
                PRIMARY KEY(symbol,time));
            CREATE TABLE IF NOT EXISTS spot_futures_minutes (
                symbol TEXT NOT NULL, time INTEGER NOT NULL, payload TEXT NOT NULL,
                PRIMARY KEY(symbol,time));
            CREATE TABLE IF NOT EXISTS setups (
                id TEXT PRIMARY KEY, symbol TEXT NOT NULL, timeframe TEXT NOT NULL,
                detected_at INTEGER NOT NULL, status TEXT NOT NULL, payload TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS scanner_cursors (
                symbol TEXT NOT NULL, timeframe TEXT NOT NULL, candle_time INTEGER NOT NULL,
                PRIMARY KEY(symbol,timeframe));
            CREATE TABLE IF NOT EXISTS scan_cursors (
                symbol TEXT NOT NULL, timeframe TEXT NOT NULL, candle_time INTEGER NOT NULL,
                PRIMARY KEY(symbol,timeframe));
            CREATE TABLE IF NOT EXISTS live_scan_cursors (
                symbol TEXT NOT NULL, timeframe TEXT NOT NULL, candle_time INTEGER NOT NULL,
                PRIMARY KEY(symbol,timeframe));
            CREATE TABLE IF NOT EXISTS historical_setup_states (
                id TEXT PRIMARY KEY, symbol TEXT NOT NULL, timeframe TEXT NOT NULL,
                status TEXT NOT NULL, payload TEXT NOT NULL);
            CREATE INDEX IF NOT EXISTS historical_setup_states_stream
                ON historical_setup_states(symbol,timeframe,id);
            CREATE TABLE IF NOT EXISTS setup_assessments (
                setup_id TEXT NOT NULL, as_of INTEGER NOT NULL, payload TEXT NOT NULL,
                PRIMARY KEY(setup_id,as_of));
            CREATE INDEX IF NOT EXISTS setup_assessments_recent ON setup_assessments(setup_id,as_of DESC);
            CREATE TABLE IF NOT EXISTS account_executions (
                exec_id TEXT PRIMARY KEY, exec_time INTEGER NOT NULL, symbol TEXT NOT NULL, payload TEXT NOT NULL);
            CREATE INDEX IF NOT EXISTS account_executions_recent ON account_executions(exec_time DESC,exec_id DESC);
            CREATE TABLE IF NOT EXISTS account_orders (
                order_id TEXT PRIMARY KEY, updated_at INTEGER NOT NULL, symbol TEXT NOT NULL, payload TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS account_positions (
                category TEXT NOT NULL, symbol TEXT NOT NULL, position_idx INTEGER NOT NULL,
                updated_at INTEGER NOT NULL, payload TEXT NOT NULL,
                PRIMARY KEY(category,symbol,position_idx));
            CREATE TABLE IF NOT EXISTS account_links (
                exec_id TEXT PRIMARY KEY, setup_id TEXT NOT NULL, updated_at INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS forecast_observations (
                signal_id TEXT PRIMARY KEY, symbol TEXT NOT NULL, timeframe TEXT NOT NULL,
                pattern TEXT NOT NULL, side TEXT NOT NULL, event_at INTEGER NOT NULL,
                available_at INTEGER NOT NULL, rules_version TEXT NOT NULL,
                snapshot TEXT NOT NULL, created_at INTEGER NOT NULL);
            CREATE INDEX IF NOT EXISTS forecast_observations_cohort
                ON forecast_observations(pattern,side,rules_version,available_at);
            CREATE TABLE IF NOT EXISTS forecast_outcomes (
                signal_id TEXT NOT NULL, horizon_minutes INTEGER NOT NULL,
                status TEXT NOT NULL, exit_at INTEGER, label TEXT,
                net_pnl_per_unit REAL, net_r REAL, payload TEXT NOT NULL,
                updated_at INTEGER NOT NULL,
                PRIMARY KEY(signal_id,horizon_minutes));
            CREATE INDEX IF NOT EXISTS forecast_outcomes_status
                ON forecast_outcomes(status,horizon_minutes,updated_at);
        ''')
        self._migrate()
        self.writer = None if path == ':memory:' else SQLiteWriteQueue(path)

    def _backup_before_migration(self):
        """Take a consistent SQLite backup before changing an existing schema."""
        if self.path == ':memory:' or not Path(self.path).is_file() or Path(self.path).stat().st_size == 0:
            return None
        version = int(self.db.execute('PRAGMA user_version').fetchone()[0])
        if version >= self.SCHEMA_VERSION:
            return None
        tables = {row[0] for row in self.db.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        if not tables.intersection({'state', 'signals', 'setups', 'candles'}):
            return None
        stamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
        target = Path(self.path).with_name(
            f'{Path(self.path).name}.pre-migration-v{version}-{stamp}.bak')
        with sqlite3.connect(str(target)) as backup:
            self.db.backup(backup, pages=256, sleep=.02)
            integrity = backup.execute('PRAGMA integrity_check').fetchone()[0]
            if integrity != 'ok':
                raise RuntimeError(f'SQLite migration backup failed integrity check: {integrity}')
        return str(target)

    def _migrate(self):
        """Add indexed market event times while preserving existing payloads."""
        previous_version=int(self.db.execute('PRAGMA user_version').fetchone()[0])
        signal_columns={row[1] for row in self.db.execute('PRAGMA table_info(signals)')}
        if 'event_at' not in signal_columns:
            self.db.execute('ALTER TABLE signals ADD COLUMN event_at INTEGER NOT NULL DEFAULT 0')
        setup_columns={row[1] for row in self.db.execute('PRAGMA table_info(setups)')}
        if 'event_at' not in setup_columns:
            self.db.execute('ALTER TABLE setups ADD COLUMN event_at INTEGER NOT NULL DEFAULT 0')
        for sid, candle_time, event_at, payload in self.db.execute(
                'SELECT id,time,event_at,payload FROM signals WHERE event_at=0').fetchall():
            row=json.loads(payload)
            event_at=int(row.get('event_at') or row.get('time') or candle_time)
            row['event_at']=event_at
            self.db.execute('UPDATE signals SET event_at=?,payload=? WHERE id=?',
                            (event_at,json.dumps(row,separators=(',',':')),sid))
        for sid, detected_at, event_at, payload in self.db.execute(
                'SELECT id,detected_at,event_at,payload FROM setups WHERE event_at=0').fetchall():
            row=json.loads(payload)
            delivery=row.get('delivery') or {}
            event_at=int(row.get('event_at') or row.get('confirmed_at') or
                         delivery.get('exchange_event_at') or detected_at)
            row['event_at']=event_at
            self.db.execute('UPDATE setups SET event_at=?,payload=? WHERE id=?',
                            (event_at,json.dumps(row,separators=(',',':')),sid))
        self.db.executescript('''
            CREATE INDEX IF NOT EXISTS signals_event_at_id ON signals(event_at DESC,id DESC);
            CREATE INDEX IF NOT EXISTS setups_symbol_tf_status ON setups(symbol,timeframe,status,detected_at DESC);
            CREATE INDEX IF NOT EXISTS setups_event_at_id ON setups(event_at DESC,id DESC);
        ''')
        # Databases older than v4 had no independent scan cursor. Do this
        # legacy conversion once; on later restarts a missing scan cursor must
        # not be silently advanced to a newer candle cursor.
        if previous_version<4:
            self.db.execute('''INSERT OR IGNORE INTO scan_cursors(symbol,timeframe,candle_time)
                SELECT symbol,timeframe,candle_time FROM scanner_cursors''')
        market_columns={row[1] for row in self.db.execute('PRAGMA table_info(market_minutes)')}
        for name,definition in (('coverage_ms','INTEGER NOT NULL DEFAULT 0'),
                ('window_ms','INTEGER NOT NULL DEFAULT 60000'),("source","TEXT NOT NULL DEFAULT 'legacy'"),
                ("quality","TEXT NOT NULL DEFAULT 'unknown'")):
            if name not in market_columns:
                self.db.execute(f'ALTER TABLE market_minutes ADD COLUMN {name} {definition}')
        oi_columns={row[1] for row in self.db.execute('PRAGMA table_info(open_interest)')}
        for name,definition in (('value_usdt','REAL'),("source","TEXT NOT NULL DEFAULT 'legacy'"),
                                ('received_at','INTEGER NOT NULL DEFAULT 0')):
            if name not in oi_columns:
                self.db.execute(f'ALTER TABLE open_interest ADD COLUMN {name} {definition}')
        self.db.executescript('''
            CREATE INDEX IF NOT EXISTS setups_symbol_tf_event_id
                ON setups(symbol,timeframe,event_at DESC,id DESC);
            CREATE INDEX IF NOT EXISTS setups_active_detected
                ON setups(detected_at DESC)
                WHERE status IN ('wait_breakout','near_boundary','crossing');
        ''')
        if previous_version<6:
            self.db.execute('''INSERT OR IGNORE INTO live_scan_cursors(symbol,timeframe,candle_time)
                SELECT symbol,timeframe,candle_time FROM scan_cursors''')
        self.db.execute('PRAGMA user_version=7')
        self.db.commit()

    @staticmethod
    def _json(value):
        return json.dumps(value,separators=(',',':'),allow_nan=False)

    @contextmanager
    def _reader(self):
        """Use a separate read-only connection outside the market event loop."""
        if self.path==':memory:':
            yield self.db
            return
        uri=Path(self.path).resolve().as_uri()+'?mode=ro'
        db=sqlite3.connect(uri,uri=True,timeout=1)
        try:
            yield db
        finally:
            db.close()

    def _decode_setup_payload(self, payload, setup_id=None):
        """Decode persisted setup data without letting one bad legacy row stop scanning."""
        try:
            value=json.loads(payload)
        except (TypeError,ValueError,UnicodeDecodeError) as exc:
            key=setup_id or '<unknown>'
            if key not in self._invalid_setup_payloads_logged:
                logging.getLogger(__name__).warning(
                    'Skipping invalid setup payload id=%s (%s)',key,type(exc).__name__)
                if len(self._invalid_setup_payloads_logged)>=1000:
                    self._invalid_setup_payloads_logged.clear()
                self._invalid_setup_payloads_logged.add(key)
            return None
        if not isinstance(value,dict):
            key=setup_id or '<unknown>'
            if key not in self._invalid_setup_payloads_logged:
                logging.getLogger(__name__).warning(
                    'Skipping non-object setup payload id=%s',key)
                if len(self._invalid_setup_payloads_logged)>=1000:
                    self._invalid_setup_payloads_logged.clear()
                self._invalid_setup_payloads_logged.add(key)
            return None
        return value

    async def arecord_setup_assessment(self, setup_id, as_of, payload):
        value=self._json(payload)
        await self._awrite(lambda db:db.execute(
            'INSERT OR IGNORE INTO setup_assessments(setup_id,as_of,payload) VALUES(?,?,?)',
            (setup_id,int(as_of),value)))

    def setup_assessments(self, setup_id, limit=100):
        rows=self.db.execute('SELECT payload FROM setup_assessments WHERE setup_id=? '
            'ORDER BY as_of DESC LIMIT ?',(setup_id,max(1,min(int(limit),200)))).fetchall()
        return [json.loads(row[0]) for row in reversed(rows)]

    def scalp_setups(self, symbol, since_ms=None):
        if since_ms is None:
            rows=self.db.execute("SELECT id,payload FROM setups WHERE symbol=? AND status IN "
                "('wait_breakout','near_boundary','crossing','breakout_confirmed') "
                'ORDER BY detected_at DESC LIMIT 100',(symbol,)).fetchall()
        else:
            rows=self.db.execute("SELECT id,payload FROM setups WHERE symbol=? AND ("
                "status IN ('wait_breakout','near_boundary','crossing') OR "
                "(status='breakout_confirmed' AND event_at>=?)) "
                'ORDER BY detected_at DESC LIMIT 100',(symbol,int(since_ms))).fetchall()
        return [value for row in rows
                if (value:=self._decode_setup_payload(row[1],row[0])) is not None]

    async def arecord_account_executions(self, rows):
        values=[(str(row['execId']),int(row['execTime']),str(row['symbol']),self._json(row))
                for row in rows if row.get('execId') and row.get('symbol')]
        if values:
            def write(db):
                inserted=[]
                for value in values:
                    cursor=db.execute('INSERT OR IGNORE INTO account_executions '
                        '(exec_id,exec_time,symbol,payload) VALUES(?,?,?,?)',value)
                    if cursor.rowcount:inserted.append(json.loads(value[3]))
                return inserted
            return await self._awrite(write)
        return []

    async def arecord_account_orders(self, rows):
        values=[(str(row['orderId']),int(row.get('updatedTime') or row.get('createdTime') or 0),
                 str(row['symbol']),self._json(row)) for row in rows if row.get('orderId') and row.get('symbol')]
        if values:
            def write(db):
                for order_id,updated,symbol,payload in values:
                    db.execute('''INSERT INTO account_orders(order_id,updated_at,symbol,payload) VALUES(?,?,?,?)
                        ON CONFLICT(order_id) DO UPDATE SET updated_at=excluded.updated_at,
                        symbol=excluded.symbol,payload=excluded.payload
                        WHERE excluded.updated_at>=account_orders.updated_at''',
                        (order_id,updated,symbol,payload))
            await self._awrite(write)

    async def areplace_account_positions(self, rows, as_of):
        values=[]
        for row in rows:
            try:
                key=(str(row.get('category') or 'linear'),str(row['symbol']),int(row.get('positionIdx') or 0),
                     int(as_of),self._json(row))
            except (KeyError,TypeError,ValueError):
                continue
            values.append(key)
        def write(db):
            db.execute('DELETE FROM account_positions')
            if values:
                db.executemany('INSERT INTO account_positions(category,symbol,position_idx,updated_at,payload) '
                               'VALUES(?,?,?,?,?)',values)
        await self._awrite(write)

    def account_positions(self):
        return [json.loads(row[0]) for row in self.db.execute(
            'SELECT payload FROM account_positions ORDER BY symbol,position_idx')]

    def account_orders(self, limit=100, symbol=None):
        limit=max(1,min(int(limit),500))
        if symbol:
            rows=self.db.execute('SELECT payload FROM account_orders WHERE symbol=? '
                'ORDER BY updated_at DESC,order_id DESC LIMIT ?',(symbol,limit)).fetchall()
        else:
            rows=self.db.execute('SELECT payload FROM account_orders '
                'ORDER BY updated_at DESC,order_id DESC LIMIT ?',(limit,)).fetchall()
        return [json.loads(row[0]) for row in rows]

    def account_order(self, order_id):
        if not order_id:return None
        row=self.db.execute('SELECT payload FROM account_orders WHERE order_id=?',(str(order_id),)).fetchone()
        return json.loads(row[0]) if row else None

    def account_setups(self, symbol, side=None, limit=100):
        rows=self.db.execute('SELECT id,payload FROM setups WHERE symbol=? '
            'ORDER BY detected_at DESC LIMIT ?',(symbol,max(1,min(int(limit),500)))).fetchall()
        values=[value for row in rows
                if (value:=self._decode_setup_payload(row[1],row[0])) is not None]
        return [row for row in values if side is None or row.get('side')==side]

    def account_executions(self, limit=100, before_time=None, before_id=None):
        limit=max(1,min(int(limit),500));where='';params=[]
        if before_time is not None and before_id:
            where=' WHERE exec_time<? OR (exec_time=? AND exec_id<?)'
            params=[int(before_time),int(before_time),str(before_id)]
        rows=self.db.execute('SELECT payload FROM account_executions'+where+
            ' ORDER BY exec_time DESC,exec_id DESC LIMIT ?',(*params,limit)).fetchall()
        result=[]
        for row in rows:
            value=json.loads(row[0])
            link=self.db.execute('SELECT setup_id FROM account_links WHERE exec_id=?',
                                 (str(value.get('execId')),)).fetchone()
            value['setup_id']=link[0] if link else None
            result.append(value)
        return result

    def latest_account_execution_time(self):
        row=self.db.execute('SELECT MAX(exec_time) FROM account_executions').fetchone()
        return int(row[0]) if row and row[0] is not None else None

    async def alink_account_execution(self, exec_id, setup_id, updated_at):
        def write(db):
            if not db.execute('SELECT 1 FROM account_executions WHERE exec_id=?',(str(exec_id),)).fetchone():
                return False
            if not db.execute('SELECT 1 FROM setups WHERE id=?',(str(setup_id),)).fetchone():
                return False
            db.execute('INSERT INTO account_links(exec_id,setup_id,updated_at) VALUES(?,?,?) '
                'ON CONFLICT(exec_id) DO UPDATE SET setup_id=excluded.setup_id,updated_at=excluded.updated_at',
                (str(exec_id),str(setup_id),int(updated_at)))
            return True
        return await self._awrite(write)

    def _write(self, operation, *, priority=0):
        if priority not in (0,1):
            raise ValueError('SQLite writer priority must be 0 or 1')
        if self.writer:
            return self.writer.submit(operation,priority=priority).result(timeout=10)
        with self.db:
            return operation(self.db)

    async def _awrite(self, operation, *, priority=0):
        if self.writer:
            return await asyncio.wrap_future(self.writer.submit(operation,priority=priority))
        return self._write(operation,priority=priority)

    def load(self):
        row = self.db.execute('SELECT payload FROM state WHERE id=1').fetchone()
        return json.loads(row[0]) if row else None

    @staticmethod
    def _save_signal_on_db(db, row):
        """Keep published timing when a pending Signal is persisted again."""
        signal_id,candle_time,payload,event_at=row
        saved=db.execute('SELECT payload FROM signals WHERE id=?',(signal_id,)).fetchone()
        if saved:
            try:
                previous=json.loads(saved[0])
                incoming=json.loads(payload)
            except (TypeError,ValueError,UnicodeDecodeError):
                previous=incoming=None
            if isinstance(previous,dict) and isinstance(incoming,dict):
                old_filters=previous.get('filter_results')
                new_filters=incoming.get('filter_results')
                old_filters=old_filters if isinstance(old_filters,dict) else {}
                new_filters=new_filters if isinstance(new_filters,dict) else {}
                old_delivery=old_filters.get('delivery')
                new_delivery=new_filters.get('delivery')
                old_delivery=old_delivery if isinstance(old_delivery,dict) else {}
                new_delivery=new_delivery if isinstance(new_delivery,dict) else {}
                old_mode=old_delivery.get('mode')
                new_mode=new_delivery.get('mode')
                is_history=(new_mode=='history' or
                    (new_mode is None and (new_delivery.get('recovered') is True or incoming.get('recovered') is True)))
                if old_mode=='current' and is_history:
                    return False
                if (old_delivery.get('timing_version')==2 and new_delivery.get('timing_version')==2
                        and old_mode==new_mode
                        and int(previous.get('event_at') or previous.get('time') or 0)==int(incoming.get('event_at') or incoming.get('time') or 0)
                        and previous.get('published_at') and not incoming.get('published_at')):
                    incoming['published_at']=previous['published_at']
                    for key in ('published_at','publish_delay_ms'):
                        if key in old_delivery:new_delivery[key]=old_delivery[key]
                    new_filters['delivery']=new_delivery
                    incoming['filter_results']=new_filters
                    payload=json.dumps(incoming,separators=(',',':'))
        db.execute('INSERT OR REPLACE INTO signals(id,time,payload,event_at) VALUES(?,?,?,?)',
                   (signal_id,candle_time,payload,event_at))
        return True

    def _save_state_and_signals_on_db(self,db,state_json,rows):
        db.execute('INSERT OR REPLACE INTO state VALUES(1,?)',(state_json,))
        for row in rows:self._save_signal_on_db(db,row)

    def save(self, state: dict, signals: list[dict] = ()):
        state_json=json.dumps(state,separators=(',',':'))
        rows=[(s['id'],s.get('time',0),json.dumps(s,separators=(',',':')),
               int(s.get('event_at') or s.get('time') or 0)) for s in signals]
        self._write(lambda db:self._save_state_and_signals_on_db(db,state_json,rows))

    async def asave(self, state: dict, signals: list[dict] = ()):
        state_json=json.dumps(state,separators=(',',':'))
        rows=[(s['id'],s.get('time',0),json.dumps(s,separators=(',',':')),
               int(s.get('event_at') or s.get('time') or 0)) for s in signals]
        await self._awrite(lambda db:self._save_state_and_signals_on_db(db,state_json,rows))

    async def asave_historical_signals(self, signals):
        """Insert archived signals without overwriting a live signal or engine state."""
        rows=[(s['id'],int(s.get('time',0) or 0),self._json(s),
               int(s.get('event_at') or s.get('time') or 0)) for s in signals]
        if not rows:return []
        def insert(db,batch):
            inserted=[]
            for row in batch:
                if db.execute('INSERT OR IGNORE INTO signals(id,time,payload,event_at) VALUES(?,?,?,?)',row).rowcount:
                    inserted.append(row[0])
            return inserted
        inserted=[]
        for offset in range(0,len(rows),32):
            batch=rows[offset:offset+32]
            inserted.extend(await self._awrite(lambda db:insert(db,batch),priority=1))
        return inserted

    async def asave_historical_setups(self, setups):
        """Insert terminal historical setups without altering active setups."""
        values=list(setups)
        if not values:return []
        active={'wait_breakout','near_boundary','crossing'}
        if any(s.get('status') in active for s in values):
            raise ValueError('Historical setups must not have an active status')
        rows=[(s['id'],s['symbol'],s['timeframe'],int(s['detected_at']),s['status'],
               self._json(s),int(s.get('event_at') or s.get('detected_at') or 0)) for s in values]
        def insert(db,batch):
            inserted=[]
            for row in batch:
                if db.execute('INSERT OR IGNORE INTO setups(id,symbol,timeframe,detected_at,status,payload,event_at) '
                              'VALUES(?,?,?,?,?,?,?)',row).rowcount:
                    inserted.append(row[0])
            return inserted
        inserted=[]
        for offset in range(0,len(rows),32):
            batch=rows[offset:offset+32]
            inserted.extend(await self._awrite(lambda db:insert(db,batch),priority=1))
        return inserted

    async def ahistory_setups(self,symbol,timeframe):
        """Read isolated historical lifecycle states for one stream."""
        def read():
            with self._reader() as db:
                rows=db.execute('SELECT id,payload FROM historical_setup_states '
                                'WHERE symbol=? AND timeframe=? ORDER BY id',
                                (symbol,timeframe)).fetchall()
                return [value for row in rows
                        if (value:=self._decode_setup_payload(row[1],row[0])) is not None]
        return read() if self.path==':memory:' else await asyncio.to_thread(read)

    @staticmethod
    def _stale_setup_update(previous,value):
        previous_close=previous.get('last_closed_candle')
        incoming_close=value.get('last_closed_candle')
        if previous_close is not None and incoming_close is not None:
            try:
                if int(incoming_close)<int(previous_close):return True
            except (TypeError,ValueError):
                return True
        return (previous.get('status') in ('breakout_confirmed','cancelled','expired','target_reached') and
                value.get('status') in ('wait_breakout','near_boundary','crossing'))

    def _save_history_setup_on_db(self,db,setup):
        value=dict(setup)
        delivery=value.get('delivery') or {}
        if isinstance(delivery,dict) and delivery.get('mode')=='current':
            raise ValueError('Current setup cannot be saved as historical state')
        row=db.execute('SELECT payload FROM historical_setup_states WHERE id=?',(value['id'],)).fetchone()
        previous=self._decode_setup_payload(row[0],value['id']) if row else None
        if row and previous is None:
            db.execute('DELETE FROM historical_setup_states WHERE id=?',(value['id'],))
        if previous is not None:
            if (previous.get('symbol')!=value.get('symbol') or
                    previous.get('timeframe')!=value.get('timeframe') or
                    self._stale_setup_update(previous,value)):
                return False
            value['snapshot']=previous['snapshot']
            value['detected_at']=previous['detected_at']
            history=previous.get('transitions',[])
            if value.get('status')!=previous.get('status'):
                history=history+[{'time':value.get('updated_at'),'from':previous.get('status'),
                                  'to':value.get('status'),'reason':value.get('reason','')}]
            value['transitions']=history[-50:]
            if value==previous:return False
        db.execute('INSERT INTO historical_setup_states(id,symbol,timeframe,status,payload) VALUES(?,?,?,?,?) '
                   'ON CONFLICT(id) DO UPDATE SET symbol=excluded.symbol,timeframe=excluded.timeframe,'
                   'status=excluded.status,payload=excluded.payload',
                   (value['id'],value['symbol'],value['timeframe'],value['status'],self._json(value)))
        return True

    async def asave_history_setup_states(self,setups):
        """Persist independent historical states in small background batches."""
        values=list(setups)
        changed=[]
        for offset in range(0,len(values),32):
            batch=values[offset:offset+32]
            results=await self._awrite(lambda db:[self._save_history_setup_on_db(db,item) for item in batch],
                                       priority=1)
            changed.extend(item['id'] for item,result in zip(batch,results) if result)
        return changed

    async def apublish_historical_setups(self,setups):
        """Publish terminal history without touching an existing live figure."""
        values=list(setups)
        terminal={'breakout_confirmed','cancelled','expired','target_reached'}
        if any(item.get('status') not in terminal for item in values):
            raise ValueError('Only terminal historical setups can be published')
        if any(isinstance(item.get('delivery'),dict) and item['delivery'].get('mode')=='current'
               for item in values):
            raise ValueError('Current setup cannot be published as history')
        def publish(db,batch):
            changed=[]
            for value in batch:
                row=db.execute('SELECT payload FROM setups WHERE id=?',(value['id'],)).fetchone()
                previous=self._decode_setup_payload(row[0],value['id']) if row else None
                if previous is not None:
                    delivery=previous.get('delivery') or {}
                    mode=delivery.get('mode') if isinstance(delivery,dict) else None
                    if mode=='current' or not (mode=='history' or (mode is None and previous.get('recovered') is True)):
                        continue
                    if self._save_setup_on_db(db,value):changed.append(value['id'])
                else:
                    record=(value['id'],value['symbol'],value['timeframe'],int(value['detected_at']),
                        value['status'],self._json(value),int(value.get('event_at') or value.get('detected_at') or 0))
                    if db.execute('INSERT OR IGNORE INTO setups(id,symbol,timeframe,detected_at,status,payload,event_at) '
                                  'VALUES(?,?,?,?,?,?,?)',record).rowcount:
                        changed.append(value['id'])
            return changed
        changed=[]
        for offset in range(0,len(values),32):
            batch=values[offset:offset+32]
            changed.extend(await self._awrite(lambda db:publish(db,batch),priority=1))
        return changed

    async def aupdate_signal_snapshot(self, signal_id, snapshot):
        def write(db):
            row=db.execute('SELECT payload FROM signals WHERE id=?',(signal_id,)).fetchone()
            if not row:return False
            value=json.loads(row[0])
            filters=value.setdefault('filter_results',{})
            filters['decision_snapshot']=snapshot
            payload=json.dumps(value,separators=(',',':'))
            db.execute('UPDATE signals SET payload=? WHERE id=?',(payload,signal_id))
            return True
        return await self._awrite(write)

    async def aupdate_signal_deliveries(self, rows, *, priority=0):
        """Merge delivery timestamps into saved signals in one transaction."""
        return await self._aupdate_deliveries('signals',rows,priority=priority)

    async def aupdate_setup_deliveries(self, rows, *, priority=0):
        """Merge delivery timestamps into saved setups in one transaction."""
        return await self._aupdate_deliveries('setups',rows,priority=priority)

    async def _aupdate_deliveries(self, table, rows, *, priority=0):
        if table not in {'signals','setups'}:raise ValueError('Unknown delivery table')
        updates=[dict(row) for row in rows]
        if not updates:return []
        def write(db):
            changed=[]
            for update in updates:
                item_id=update['id']
                row=db.execute(f'SELECT payload FROM {table} WHERE id=?',(item_id,)).fetchone()
                if not row:continue
                value=json.loads(row[0])
                filters=value.get('filter_results') if table=='signals' else value
                if not isinstance(filters,dict):filters={}
                previous=filters.get('delivery')
                if not isinstance(previous,dict):previous={}
                incoming=update.get('delivery')
                if incoming is not None and not isinstance(incoming,dict):
                    raise ValueError('Delivery must be an object')
                # A late historical pass cannot relabel an already published
                # current observation or change its reception timestamps.
                if (previous.get('mode')=='current' and
                        isinstance(incoming,dict) and incoming.get('mode')=='history'):
                    continue
                for key in ('received_at','detected_at','published_at'):
                    if key in update:value[key]=update[key]
                if incoming is not None:
                    filters['delivery']={**previous,**incoming}
                    if table=='signals':value['filter_results']=filters
                db.execute(f'UPDATE {table} SET payload=? WHERE id=?',(self._json(value),item_id))
                changed.append(item_id)
            return changed
        return await self._awrite(write,priority=priority)

    def has_signal(self, sid):
        row=self.db.execute('SELECT payload FROM signals WHERE id=?',(sid,)).fetchone()
        return bool(row and json.loads(row[0]).get('status')!='pending_retest')

    async def aexisting_signal_ids(self, ids):
        """Fetch signal deduplication keys in batches on a read connection."""
        values=list(dict.fromkeys(ids))
        if not values:return set()
        def read():
            found=set()
            with self._reader() as db:
                for offset in range(0,len(values),500):
                    batch=values[offset:offset+500]
                    query='SELECT id,payload FROM signals WHERE id IN ('+','.join('?' for _ in batch)+')'
                    for sid,payload in db.execute(query,batch):
                        if json.loads(payload).get('status')!='pending_retest':found.add(sid)
            return found
        return read() if self.path==':memory:' else await asyncio.to_thread(read)

    async def afetch_signals(self, ids):
        """Fetch saved signals by id using a separate read-only connection."""
        values=list(dict.fromkeys(ids))
        if not values:return {}
        def read():
            found={}
            with self._reader() as db:
                for offset in range(0,len(values),500):
                    batch=values[offset:offset+500]
                    query='SELECT id,payload FROM signals WHERE id IN ('+','.join('?' for _ in batch)+')'
                    for sid,payload in db.execute(query,batch):found[sid]=json.loads(payload)
            return found
        return read() if self.path==':memory:' else await asyncio.to_thread(read)

    def signals(self, limit=200, before_time=None, before_id=None, statuses=None,*,after_time=None):
        return self._signals_from(self.db,limit,before_time,before_id,statuses,after_time)

    def _signals_from(self,db,limit,before_time,before_id,statuses,after_time):
        query='SELECT payload FROM signals'
        clauses=[];params=[]
        if before_time is not None and before_id is not None:
            clauses.append('(event_at<? OR (event_at=? AND id<?))')
            params.extend((before_time,before_time,before_id))
        elif before_time is not None:
            clauses.append('event_at<?');params.append(before_time)
        if after_time is not None:
            clauses.append('event_at>=?');params.append(int(after_time))
        if statuses:
            clauses.append("json_extract(payload,'$.status') IN ("+','.join('?' for _ in statuses)+')')
            params.extend(statuses)
        if clauses:query+=' WHERE '+' AND '.join(clauses)
        query+=' ORDER BY event_at DESC,id DESC LIMIT ?';params.append(limit)
        return [json.loads(r[0]) for r in db.execute(query,params)]

    async def asignals(self,limit=200,before_time=None,before_id=None,statuses=None,*,after_time=None):
        if self.path==':memory:':
            return self.signals(limit,before_time,before_id,statuses,after_time=after_time)
        def read():
            with self._reader() as db:
                return self._signals_from(db,limit,before_time,before_id,statuses,after_time)
        return await asyncio.to_thread(read)

    def signal(self, sid):
        row = self.db.execute('SELECT payload FROM signals WHERE id=?',(sid,)).fetchone()
        return json.loads(row[0]) if row else None

    async def arecord_forecast_observation(self, observation):
        """Insert a first-seen forecast snapshot; later signal edits never replace it."""
        values=(str(observation['id']),str(observation['symbol']),str(observation['timeframe']),
            str(observation['pattern']),str(observation['side']),int(observation['event_at']),
            int(observation['available_at']),str(observation['rules_version']),self._json(observation),
            int(observation.get('available_at') or 0))
        def write(db):
            inserted=db.execute('''INSERT OR IGNORE INTO forecast_observations
                (signal_id,symbol,timeframe,pattern,side,event_at,available_at,rules_version,snapshot,created_at)
                VALUES(?,?,?,?,?,?,?,?,?,?)''',values).rowcount==1
            if inserted:
                terminal=observation.get('status') in ('no_fill','data_missing')
                for horizon in (5,15,30):
                    status=observation.get('status') if terminal else 'pending'
                    outcome={'horizon_minutes':horizon,'status':status,
                             'label':status if terminal else None,'reason':observation.get('reason','')}
                    db.execute('''INSERT INTO forecast_outcomes
                        (signal_id,horizon_minutes,status,label,payload,updated_at)
                        VALUES(?,?,?,?,?,?)''',(values[0],horizon,status,outcome['label'],
                        self._json(outcome),values[9]))
            return inserted
        return await self._awrite(write)

    async def apending_forecast_observations(self, limit=256):
        limit=max(1,min(int(limit),1000))
        def read():
            with self._reader() as db:
                return [json.loads(row[0]) for row in db.execute('''SELECT snapshot
                    FROM forecast_observations o WHERE EXISTS (
                        SELECT 1 FROM forecast_outcomes x WHERE x.signal_id=o.signal_id AND x.status='pending')
                    ORDER BY available_at,signal_id LIMIT ?''',(limit,))]
        return read() if self.path==':memory:' else await asyncio.to_thread(read)

    async def aupdate_forecast_outcomes(self, signal_id, outcomes, updated_at):
        if not outcomes:return 0
        rows=[]
        for outcome in outcomes:
            status=str(outcome.get('status') or 'data_missing')
            horizon=int(outcome['horizon_minutes'])
            payload=self._json(outcome)
            net=outcome.get('net_pnl_per_unit');net_r=outcome.get('net_r')
            rows.append((str(signal_id),horizon,status,outcome.get('exit_at'),outcome.get('label'),
                         net,net_r,payload,int(updated_at)))
        def write(db):
            changed=0
            for row in rows:
                changed+=db.execute('''UPDATE forecast_outcomes SET status=?,exit_at=?,label=?,
                    net_pnl_per_unit=?,net_r=?,payload=?,updated_at=?
                    WHERE signal_id=? AND horizon_minutes=? AND status='pending' ''',
                    (row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[0],row[1])).rowcount
            return changed
        return await self._awrite(write)

    async def aforecast_dataset(self, *, pattern=None, side=None, rules_version=None, limit=20_000):
        limit=max(1,min(int(limit),50_000))
        clauses=[];params=[]
        for column,value in (('pattern',pattern),('side',side),('rules_version',rules_version)):
            if value is not None:
                clauses.append(f'o.{column}=?');params.append(str(value))
        where=' WHERE '+' AND '.join(clauses) if clauses else ''
        def read():
            with self._reader() as db:
                values={}
                query=('''SELECT o.snapshot,x.payload FROM forecast_observations o
                    LEFT JOIN forecast_outcomes x ON x.signal_id=o.signal_id'''+where+
                    ' ORDER BY o.available_at DESC,o.signal_id DESC,x.horizon_minutes LIMIT ?')
                params.append(limit * 3)
                for snapshot,payload in db.execute(query,params):
                    observation=values.setdefault((json.loads(snapshot).get('id')),json.loads(snapshot))
                    if payload:
                        observation.setdefault('outcomes',[]).append(json.loads(payload))
                return list(values.values())
        return read() if self.path==':memory:' else await asyncio.to_thread(read)

    async def aforecast_report(self):
        """Return saved candidate inputs and outcomes for descriptive comparisons."""
        return await self.aforecast_dataset()

    @staticmethod
    def _prune_candles(db, symbol, tf):
        interval=(1440 if tf=='D' else int(tf))*60_000
        # Pending detection and its historical context must remain available
        # if the process stops during a long backfill.
        db.execute('DELETE FROM candles WHERE symbol=? AND timeframe=? AND time NOT IN '
            '(SELECT time FROM candles WHERE symbol=? AND timeframe=? ORDER BY time DESC LIMIT 1500) '
            'AND time < COALESCE((SELECT candle_time FROM scan_cursors WHERE symbol=? AND timeframe=?)-?, -1)',
            (symbol,tf,symbol,tf,symbol,tf,interval*250))

    def put_candles(self, symbol, tf, candles):
        rows=[(symbol,tf,c.time,json.dumps(asdict(c),separators=(',',':'))) for c in candles]
        self._write(lambda db:(db.executemany('INSERT OR REPLACE INTO candles VALUES(?,?,?,?)',rows),
            self._prune_candles(db,symbol,tf)))

    async def aput_candles(self, symbol, tf, candles, *, priority=0):
        rows=[(symbol,tf,c.time,json.dumps(asdict(c),separators=(',',':'))) for c in candles]
        if not rows:return
        size=32 if priority==1 else len(rows)
        for offset in range(0,len(rows),size):
            batch=rows[offset:offset+size]
            await self._awrite(lambda db:(db.executemany('INSERT OR REPLACE INTO candles VALUES(?,?,?,?)',batch),
                self._prune_candles(db,symbol,tf)),priority=priority)

    async def acommit_current_candle(self,symbol,tf,candle,setups):
        """Save the current candle and its known-setup transitions together."""
        row=(symbol,tf,candle.time,self._json(asdict(candle)))
        values=list(setups)
        def commit(db):
            db.execute('INSERT OR REPLACE INTO candles VALUES(?,?,?,?)',row)
            self._prune_candles(db,symbol,tf)
            return [self._save_setup_on_db(db,setup) for setup in values]
        return await self._awrite(commit,priority=0)

    async def acommit_candle_transition(self, symbol, tf, candle, setups, cursor_time,
                                        scan_floor=None, *, priority=0):
        """Commit a closed candle, its setup transitions, and its cursor atomically."""
        candle_row=(symbol,tf,candle.time,json.dumps(asdict(candle),separators=(',',':')))
        setup_values=list(setups)
        def commit(db):
            db.execute('INSERT OR REPLACE INTO candles VALUES(?,?,?,?)',candle_row)
            self._prune_candles(db,symbol,tf)
            results=[self._save_setup_on_db(db,item) for item in setup_values]
            db.execute('''INSERT INTO scanner_cursors(symbol,timeframe,candle_time)
                VALUES(?,?,?) ON CONFLICT(symbol,timeframe) DO UPDATE SET
                candle_time=MAX(candle_time,excluded.candle_time)''',
                (symbol,tf,int(cursor_time)))
            if scan_floor is not None:
                db.execute('''INSERT OR IGNORE INTO scan_cursors(symbol,timeframe,candle_time)
                    VALUES(?,?,?)''',(symbol,tf,int(scan_floor)))
            return results
        return await self._awrite(commit,priority=priority)

    def candles(self, symbol, tf, limit=1000):
        rows = self.db.execute('SELECT payload FROM candles WHERE symbol=? AND timeframe=? ORDER BY time DESC LIMIT ?',
                               (symbol,tf,limit)).fetchall()
        return [Candle(**json.loads(r[0])) for r in reversed(rows)]

    def candles_range(self, symbol, tf, *, limit=1000, start=None, end=None, after=None):
        """Read an arbitrary candle window on an independent, short-lived connection."""
        with self._reader() as db:
            rows = db.execute(
                'SELECT payload FROM (SELECT time,payload FROM candles WHERE symbol=? AND timeframe=? '
                'UNION SELECT time,payload FROM candles_archive WHERE symbol=? AND timeframe=?) '
                'WHERE (? IS NULL OR time>=?) AND (? IS NULL OR time<?) '
                'AND (? IS NULL OR time>?) ORDER BY time LIMIT ?',
                (symbol, tf, symbol, tf, start, start, end, end, after, after, limit)).fetchall()
        return [Candle(**json.loads(r[0])) for r in rows]

    def put_candles_secondary(self, symbol, tf, candles):
        rows=[(symbol,tf,c.time,json.dumps(asdict(c))) for c in candles]
        if not rows:return
        if self.path==':memory:':
            self.put_candles(symbol,tf,candles);return
        self._write(lambda db:db.executemany('INSERT OR REPLACE INTO candles_archive VALUES(?,?,?,?)',rows))

    def save_market_minute(self, symbol, time, buy, sell, book=None, updated=0):
        book=book or {}
        self._write(lambda db:(db.execute('''INSERT OR REPLACE INTO market_minutes
                (symbol,time,buy,sell,spread_bps,bid_depth,ask_depth,updated,coverage_ms,window_ms,source,quality)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?)''',(symbol,time,buy,sell,book.get('spread_bps'),
                book.get('bid_depth_usdt'),book.get('ask_depth_usdt'),updated,book.get('coverage_ms',0),
                book.get('window_ms',60000),book.get('source','legacy'),book.get('quality','unknown'))),
            db.execute('''DELETE FROM market_minutes WHERE symbol=? AND time NOT IN
                (SELECT time FROM market_minutes WHERE symbol=? ORDER BY time DESC LIMIT 43200)''',
                (symbol,symbol))))

    def save_market_minute_batch(self, rows):
        if not rows:return
        payload=[(r['symbol'],r['time'],r['buy'],r['sell'],r.get('spread_bps'),
                  r.get('bid_depth_usdt'),r.get('ask_depth_usdt'),r.get('updated',0),
                  r.get('coverage_ms',0),r.get('window_ms',60000),r.get('source','legacy'),
                  r.get('quality','unknown')) for r in rows]
        def write(db):
            db.executemany('''INSERT OR REPLACE INTO market_minutes
                (symbol,time,buy,sell,spread_bps,bid_depth,ask_depth,updated,coverage_ms,window_ms,source,quality)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?)''',payload)
            for symbol in {r['symbol'] for r in rows}:
                db.execute('DELETE FROM market_minutes WHERE symbol=? AND time NOT IN '
                    '(SELECT time FROM market_minutes WHERE symbol=? ORDER BY time DESC LIMIT 43200)',
                    (symbol,symbol))
        self._write(write)

    def save_spot_futures_batch(self, rows):
        if not rows:return
        values=[(r['symbol'],int(r['time']),json.dumps(r['payload'])) for r in rows]
        cutoff=min(x[1] for x in values)-30*24*60*60_000
        self._write(lambda db:(db.executemany('INSERT OR REPLACE INTO spot_futures_minutes VALUES(?,?,?)',values),
                              db.execute('DELETE FROM spot_futures_minutes WHERE time<?',(cutoff,))))

    def market_minutes(self, symbol, start, end):
        return [dict(zip(('time','buy','sell','spread_bps','bid_depth_usdt','ask_depth_usdt',
                'updated','coverage_ms','window_ms','source','quality'),row))
            for row in self.db.execute('''SELECT time,buy,sell,spread_bps,bid_depth,ask_depth,updated,
                coverage_ms,window_ms,source,quality
                FROM market_minutes WHERE symbol=? AND time>=? AND time<? ORDER BY time''',
                (symbol,start,end))]

    def save_open_interest(self, symbol, rows):
        values=[]
        for row in rows:
            timestamp,value,*extra=row
            value_usdt=extra[0] if extra else None
            source=extra[1] if len(extra)>1 else 'rest'
            received=extra[2] if len(extra)>2 else int(timestamp)
            values.append((symbol,int(timestamp),float(value),
                float(value_usdt) if value_usdt is not None else None,str(source),int(received)))
        self._write(lambda db:(db.executemany('''INSERT OR REPLACE INTO open_interest
                (symbol,time,value,value_usdt,source,received_at) VALUES(?,?,?,?,?,?)''',values),
            db.execute('''DELETE FROM open_interest WHERE symbol=? AND time NOT IN
                (SELECT time FROM open_interest WHERE symbol=? ORDER BY time DESC LIMIT 43200)''',
                (symbol,symbol))))

    async def asave_open_interest(self, rows):
        values=[(row['symbol'],int(row['time']),float(row['value']),
                 float(row['value_usdt']) if row.get('value_usdt') is not None else None,
                 str(row.get('source','ticker')),int(row.get('received_at',row['time'])))
                for row in rows if row.get('symbol') and row.get('value') is not None]
        if not values:
            return
        def write(db):
            db.executemany('''INSERT OR REPLACE INTO open_interest
                (symbol,time,value,value_usdt,source,received_at) VALUES(?,?,?,?,?,?)''',values)
            for symbol in {row[0] for row in values}:
                db.execute('DELETE FROM open_interest WHERE symbol=? AND time NOT IN '
                    '(SELECT time FROM open_interest WHERE symbol=? ORDER BY time DESC LIMIT 43200)',
                    (symbol,symbol))
        await self._awrite(write)

    def open_interest(self, symbol, start, end):
        return self.db.execute('''SELECT time,value FROM open_interest
            WHERE symbol=? AND time>=? AND time<? ORDER BY time''',(symbol,start,end)).fetchall()

    def open_interest_records(self, symbol, start, end):
        return [dict(zip(('time','value','value_usdt','source','received_at'),row))
            for row in self.db.execute('''SELECT time,value,value_usdt,source,received_at
                FROM open_interest WHERE symbol=? AND time>=? AND time<? ORDER BY time''',
                (symbol,start,end))]

    async def asave_liquidation_minutes(self, rows):
        values=[(r['symbol'],int(r['time']),float(r.get('long_usdt',0)),
                 float(r.get('short_usdt',0)),int(r.get('count',0)),str(r.get('quality','partial')))
                for r in rows]
        if values:
            def write(db):
                db.executemany('''INSERT OR REPLACE INTO liquidation_minutes
                    (symbol,time,long_usdt,short_usdt,count,quality) VALUES(?,?,?,?,?,?)''',values)
                cutoff=min(row[1] for row in values)-30*24*60*60_000
                db.execute('DELETE FROM liquidation_minutes WHERE time<?',(cutoff,))
            await self._awrite(write)

    def liquidation_minutes(self,symbol,start,end):
        return [dict(zip(('time','long_usdt','short_usdt','count','quality'),row))
            for row in self.db.execute('''SELECT time,long_usdt,short_usdt,count,quality
                FROM liquidation_minutes WHERE symbol=? AND time>=? AND time<? ORDER BY time''',
                (symbol,int(start),int(end)))]

    def liquidation_minutes(self, symbol, start, end):
        return [dict(zip(('time','long_usdt','short_usdt','count','quality'),row))
            for row in self.db.execute('''SELECT time,long_usdt,short_usdt,count,quality
                FROM liquidation_minutes WHERE symbol=? AND time>=? AND time<? ORDER BY time''',
                (symbol,start,end))]

    def save_spot_futures_minute(self,symbol,time,payload):
        cutoff=int(time)-30*24*60*60_000
        value=json.dumps(payload,separators=(',',':'))
        self._write(lambda db:(db.execute('INSERT OR REPLACE INTO spot_futures_minutes VALUES(?,?,?)',
                                          (symbol,int(time),value)),
                               db.execute('DELETE FROM spot_futures_minutes WHERE time<?',(cutoff,))))

    def spot_futures_minutes(self,symbol,start,end):
        return [{'time':row[0],**json.loads(row[1])} for row in self.db.execute(
            '''SELECT time,payload FROM spot_futures_minutes
               WHERE symbol=? AND time>=? AND time<? ORDER BY time''',(symbol,start,end))]

    def save_setup(self, setup):
        """Persist lifecycle while keeping the first geometry snapshot immutable."""
        return self.save_setups([setup])[0]

    @staticmethod
    def _save_setup_on_db(db, setup):
        row=db.execute('SELECT payload FROM setups WHERE id=?',(setup['id'],)).fetchone()
        value=dict(setup)
        if row:
            try:
                previous=json.loads(row[0])
            except (TypeError,ValueError,UnicodeDecodeError):
                previous=None
            if not isinstance(previous,dict):
                previous=None
            if previous is None:
                db.execute('DELETE FROM setups WHERE id=?',(setup['id'],))
                row=None
        if row:
            previous_status=previous.get('status')
            incoming_status=value.get('status')
            previous_close=previous.get('last_closed_candle')
            incoming_close=value.get('last_closed_candle')
            if previous_close is not None and incoming_close is not None:
                try:
                    if int(incoming_close)<int(previous_close):
                        return False
                except (TypeError,ValueError):
                    return False
            if (previous_status in ('breakout_confirmed','cancelled','expired','target_reached') and
                    incoming_status in ('wait_breakout','near_boundary','crossing')):
                return False
            value['snapshot']=previous['snapshot']
            value['detected_at']=previous['detected_at']
            value['event_at']=int(value.get('event_at') or previous.get('event_at') or previous['detected_at'])
            history=previous.get('transitions',[])
            if value.get('status')!=previous.get('status'):
                history=history+[{'time':value.get('updated_at'), 'from':previous.get('status'),
                                  'to':value.get('status'), 'reason':value.get('reason','')}]
            value['transitions']=history[-50:]
            if value==previous:
                return False
            result='status' if value.get('status')!=previous_status else 'updated'
        else:
            value['event_at']=int(value.get('event_at') or value.get('detected_at') or 0)
            result='created'
        payload=json.dumps(value,separators=(',',':'))
        db.execute('INSERT OR REPLACE INTO setups(id,symbol,timeframe,detected_at,status,payload,event_at) VALUES(?,?,?,?,?,?,?)',
            (value['id'],value['symbol'],value['timeframe'],value['detected_at'],value['status'],payload,value['event_at']))
        return result

    def save_setups(self, setups):
        values=list(setups)
        if not values:return []
        return self._write(lambda db:[self._save_setup_on_db(db,item) for item in values])

    async def asave_setups(self, setups, *, priority=0):
        values=list(setups)
        if not values:return []
        size=32 if priority==1 else len(values)
        results=[]
        for offset in range(0,len(values),size):
            batch=values[offset:offset+size]
            results.extend(await self._awrite(lambda db:[self._save_setup_on_db(db,item) for item in batch],
                                               priority=priority))
        return results

    def _save_current_setup_on_db(self,db,setup):
        """Promote an archive of the same event without overwriting live state."""
        value=dict(setup)
        row=db.execute('SELECT payload FROM setups WHERE id=?',(value['id'],)).fetchone()
        previous=self._decode_setup_payload(row[0],value['id']) if row else None
        if previous is None:
            return self._save_setup_on_db(db,value)
        previous_delivery=previous.get('delivery') or {}
        current_delivery=value.get('delivery') or {}
        previous_mode=previous_delivery.get('mode') if isinstance(previous_delivery,dict) else None
        current_mode=current_delivery.get('mode') if isinstance(current_delivery,dict) else None
        # A duplicate live discovery must never reset a newer live lifecycle.
        if previous_mode=='current':
            return False
        previous_close=previous.get('last_closed_candle')
        incoming_close=value.get('last_closed_candle')
        if previous_close is not None and incoming_close is not None:
            try:
                if int(incoming_close)<int(previous_close):return False
            except (TypeError,ValueError):
                return False
        archived=(previous_mode=='history' or (previous_mode is None and previous.get('recovered') is True))
        try:
            previous_event=int(previous.get('event_at') or 0)
            current_event=int(value.get('event_at') or 0)
        except (TypeError,ValueError):
            previous_event=current_event=0
        if (archived and current_mode=='current' and previous_event>0 and previous_event==current_event
                and previous.get('symbol')==value.get('symbol')
                and previous.get('timeframe')==value.get('timeframe')):
            # This is the same exchange event observed live: replace the
            # archive's synthetic terminal state with the live observation.
            db.execute('UPDATE setups SET symbol=?,timeframe=?,detected_at=?,status=?,payload=?,event_at=? WHERE id=?',
                (value['symbol'],value['timeframe'],int(value['detected_at']),value['status'],
                 self._json(value),current_event,value['id']))
            return 'status' if value.get('status')!=previous.get('status') else 'updated'
        return self._save_setup_on_db(db,value)

    async def asave_current_setups(self,setups,*,priority=0):
        """Atomically insert live discoveries or promote matching archive rows."""
        values=list(setups)
        if not values:return []
        return await self._awrite(lambda db:[self._save_current_setup_on_db(db,value)
                                             for value in values],priority=priority)

    def setup(self, setup_id):
        row=self.db.execute('SELECT payload FROM setups WHERE id=?',(setup_id,)).fetchone()
        return self._decode_setup_payload(row[0],setup_id) if row else None

    async def afetch_setups(self, ids):
        """Return saved setups by id without blocking the market event loop."""
        values=list(dict.fromkeys(ids))
        if not values:return {}
        def read():
            found={}
            with self._reader() as db:
                for offset in range(0,len(values),500):
                    batch=values[offset:offset+500]
                    query='SELECT id,payload FROM setups WHERE id IN ('+','.join('?' for _ in batch)+')'
                    for sid,payload in db.execute(query,batch):
                        value=self._decode_setup_payload(payload,sid)
                        if value is not None:found[sid]=value
            return found
        return read() if self.path==':memory:' else await asyncio.to_thread(read)

    def setups(self, limit=300, before_time=None, before_id=None, statuses=None):
        query='SELECT id,payload FROM setups';clauses=[];params=[]
        if before_time is not None and before_id is not None:
            clauses.append('(event_at<? OR (event_at=? AND id<?))')
            params.extend((before_time,before_time,before_id))
        elif before_time is not None:
            clauses.append('event_at<?');params.append(before_time)
        if statuses:
            clauses.append('status IN ('+','.join('?' for _ in statuses)+')')
            params.extend(statuses)
        if clauses:query+=' WHERE '+' AND '.join(clauses)
        query+=' ORDER BY event_at DESC,id DESC LIMIT ?';params.append(limit)
        return [value for row in self.db.execute(query,params)
                if (value:=self._decode_setup_payload(row[1],row[0])) is not None]

    def active_setups(self,symbol=None,timeframe=None):
        return self._active_setups_from(self.db,symbol,timeframe)

    def _active_setups_from(self,db,symbol=None,timeframe=None):
        query="SELECT id,payload FROM setups WHERE status IN ('wait_breakout','near_boundary','crossing')"
        params=[]
        if symbol is not None:
            query+=' AND symbol=?';params.append(symbol)
        if timeframe is not None:
            query+=' AND timeframe=?';params.append(timeframe)
        rows=db.execute(query+' ORDER BY detected_at DESC',params).fetchall()
        return [value for row in rows
                if (value:=self._decode_setup_payload(row[1],row[0])) is not None]

    async def aactive_setups(self,symbol=None,timeframe=None):
        if self.path==':memory:':return self.active_setups(symbol,timeframe)
        def read():
            with self._reader() as db:return self._active_setups_from(db,symbol,timeframe)
        return await asyncio.to_thread(read)

    def breakout_setups(self,symbol,timeframe,limit=2000):
        return [value for row in self.db.execute(
            "SELECT id,payload FROM setups WHERE symbol=? AND timeframe=? AND status='breakout_confirmed' "
            'ORDER BY detected_at DESC LIMIT ?', (symbol,timeframe,limit))
            if (value:=self._decode_setup_payload(row[1],row[0])) is not None]

    def recent_setups(self,symbol,timeframe,limit=200):
        return self._recent_setups_from(self.db,symbol,timeframe,limit)

    def _recent_setups_from(self,db,symbol,timeframe,limit):
        return [value for row in db.execute(
            'SELECT id,payload FROM setups WHERE symbol=? AND timeframe=? '
            'ORDER BY event_at DESC,id DESC LIMIT ?', (symbol,timeframe,limit))
            if (value:=self._decode_setup_payload(row[1],row[0])) is not None]

    async def arecent_setups(self,symbol,timeframe,limit=200):
        if self.path==':memory:':return self.recent_setups(symbol,timeframe,limit)
        def read():
            with self._reader() as db:return self._recent_setups_from(db,symbol,timeframe,limit)
        return await asyncio.to_thread(read)

    def cursor(self,symbol,timeframe):
        row=self.db.execute('SELECT candle_time FROM scanner_cursors WHERE symbol=? AND timeframe=?',
                             (symbol,timeframe)).fetchone()
        return int(row[0]) if row else None

    async def _aget_cursor(self,table,symbol,timeframe):
        if table not in {'scanner_cursors','scan_cursors','live_scan_cursors'}:
            raise ValueError('Unknown scanner cursor')
        def read():
            with self._reader() as db:
                row=db.execute(f'SELECT candle_time FROM {table} WHERE symbol=? AND timeframe=?',
                               (symbol,timeframe)).fetchone()
                return int(row[0]) if row else None
        return read() if self.path==':memory:' else await asyncio.to_thread(read)

    async def aget_cursor(self,symbol,timeframe):
        return await self._aget_cursor('scanner_cursors',symbol,timeframe)

    def set_cursor(self,symbol,timeframe,candle_time):
        self._write(lambda db:db.execute('''INSERT INTO scanner_cursors(symbol,timeframe,candle_time)
            VALUES(?,?,?) ON CONFLICT(symbol,timeframe) DO UPDATE SET candle_time=MAX(candle_time,excluded.candle_time)''',
            (symbol,timeframe,int(candle_time))))

    async def aset_cursor(self,symbol,timeframe,candle_time,*,priority=0):
        await self._awrite(lambda db:db.execute('''INSERT INTO scanner_cursors(symbol,timeframe,candle_time)
            VALUES(?,?,?) ON CONFLICT(symbol,timeframe) DO UPDATE SET candle_time=MAX(candle_time,excluded.candle_time)''',
            (symbol,timeframe,int(candle_time))),priority=priority)

    def scan_cursor(self,symbol,timeframe):
        row=self.db.execute('SELECT candle_time FROM scan_cursors WHERE symbol=? AND timeframe=?',
                             (symbol,timeframe)).fetchone()
        return int(row[0]) if row else None

    async def aget_scan_cursor(self,symbol,timeframe):
        return await self._aget_cursor('scan_cursors',symbol,timeframe)

    async def aset_scan_cursor(self,symbol,timeframe,candle_time,*,priority=0):
        await self._awrite(lambda db:db.execute('''INSERT INTO scan_cursors(symbol,timeframe,candle_time)
            VALUES(?,?,?) ON CONFLICT(symbol,timeframe) DO UPDATE SET
            candle_time=MAX(candle_time,excluded.candle_time)''',
            (symbol,timeframe,int(candle_time))),priority=priority)

    def live_scan_cursor(self,symbol,timeframe):
        row=self.db.execute('SELECT candle_time FROM live_scan_cursors WHERE symbol=? AND timeframe=?',
                            (symbol,timeframe)).fetchone()
        return int(row[0]) if row else None

    async def aget_live_scan_cursor(self,symbol,timeframe):
        return await self._aget_cursor('live_scan_cursors',symbol,timeframe)

    async def aset_live_scan_cursor(self,symbol,timeframe,candle_time,*,priority=0):
        await self._awrite(lambda db:db.execute('''INSERT INTO live_scan_cursors(symbol,timeframe,candle_time)
            VALUES(?,?,?) ON CONFLICT(symbol,timeframe) DO UPDATE SET
            candle_time=MAX(candle_time,excluded.candle_time)''',
            (symbol,timeframe,int(candle_time))),priority=priority)

    def set_cursors(self,values):
        rows=[(symbol,timeframe,int(candle_time)) for (symbol,timeframe),candle_time in values.items()]
        if rows:self._write(lambda db:db.executemany('''INSERT INTO scanner_cursors(symbol,timeframe,candle_time)
            VALUES(?,?,?) ON CONFLICT(symbol,timeframe) DO UPDATE SET candle_time=MAX(candle_time,excluded.candle_time)''',rows))

    async def aset_cursors(self,values,*,priority=0):
        rows=[(symbol,timeframe,int(candle_time)) for (symbol,timeframe),candle_time in values.items()]
        if rows:await self._awrite(lambda db:db.executemany('''INSERT INTO scanner_cursors(symbol,timeframe,candle_time)
            VALUES(?,?,?) ON CONFLICT(symbol,timeframe) DO UPDATE SET candle_time=MAX(candle_time,excluded.candle_time)''',rows),priority=priority)

    def close(self):
        if self.writer:
            self.writer.close()
        self.db.close()
