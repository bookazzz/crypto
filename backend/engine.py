from dataclasses import asdict
from datetime import datetime, timezone, timedelta
import math
from .models import Settings, Signal, Instrument, Candle, rounded
from .storage import Store

MOSCOW = timezone(timedelta(hours=3))


class Engine:
    def __init__(self, store: Store, settings: Settings | None = None):
        self.store = store
        state = store.load()
        if state:
            saved_settings=state['settings']
            saved_settings['timeframes']=list(dict.fromkeys(['5',*saved_settings.get('timeframes',['15','60','240'])]))
            self.settings=Settings(**saved_settings)
        else:
            self.settings=settings or Settings()
        # Existing local databases persisted top_n=20; expand them to the
        # requested 40 markets while retaining any already larger value.
        self.settings.top_n=max(40,self.settings.top_n)
        self.cash = state['cash'] if state else self.settings.initial_balance
        self.positions = state['positions'] if state else []
        self.trades = state['trades'] if state else []
        self.fees = state.get('fees',{}) if state else {}
        self.paused = state.get('paused',False) if state else False
        self.day = state.get('day','') if state else ''
        self.day_equity = state.get('day_equity',self.cash) if state else self.cash
        self.day_halted = state.get('day_halted',False) if state else False
        self.prices = state.get('prices',{}) if state else {}
        self.market_times = state.get('market_times',{}) if state else {}
        self.peak = state.get('peak',self.cash) if state else self.cash
        self.max_drawdown = state.get('max_drawdown',0) if state else 0
        self.strategy_approved = state.get('strategy_approved',False) if state else False
        self.strategy_version = state.get('strategy_version','') if state else ''
        self.strategy_variant = state.get('strategy_variant',{}) if state else {}
        self.strategy_pending = state.get('strategy_pending',[]) if state else []
        self.instruments: dict[str, Instrument] = {}
        self.connected = False

    def persistence_state(self):
        return {key:getattr(self,key) for key in ('cash','positions','trades','fees','paused','day',
            'day_equity','day_halted','prices','market_times','peak','max_drawdown','strategy_approved',
            'strategy_version','strategy_variant','strategy_pending')} | {'settings':asdict(self.settings)}

    @staticmethod
    def persistence_signals(signals):
        return [s.dict() if isinstance(s,Signal) else s for s in signals]

    def persist(self, signals=()):
        self.store.save(self.persistence_state(),self.persistence_signals(signals))

    async def persist_async(self, signals=()):
        await self.store.asave(self.persistence_state(),self.persistence_signals(signals))

    def equity(self):
        return self.cash + sum(p['qty'] * (self.prices.get(p['symbol'],p['entry'])-p['entry']) * p['sign'] for p in self.positions)

    def available(self):
        return max(0,self.equity()-sum(p['entry']*p['qty']/self.settings.leverage for p in self.positions))

    def daily(self, time):
        day = datetime.fromtimestamp(time/1000,MOSCOW).date().isoformat()
        eq = self.equity()
        if day != self.day:
            self.day,self.day_equity,self.day_halted = day,eq,False
        if eq <= self.day_equity*(1-self.settings.daily_loss_fraction):
            self.day_halted = True
        self.peak = max(self.peak,eq)
        if self.peak > 0:
            self.max_drawdown = max(self.max_drawdown,(self.peak-eq)/self.peak)

    def set_fee(self, symbol, rate, time, source='account', persist=True):
        if not math.isfinite(rate) or not 0 <= rate < .1:
            raise ValueError('Invalid fee rate')
        self.fees[symbol] = {'rate':rate,'time':time,'source':source}
        if persist:self.persist()

    def quote_plan(self, s: Signal, price: float, time: int, funding_rate: float = 0):
        ins = self.instruments.get(s.symbol)
        fee = self.fees.get(s.symbol)
        if not ins:
            return 'Нет спецификации инструмента'
        if not fee or time-fee['time'] > 86400000 or fee['time'] > time:
            return 'Нет актуальной комиссии аккаунта'
        if getattr(self,'fee_mode','standard')=='account' and fee.get('source')!='account':
            return 'Нет актуальной персональной комиссии аккаунта'
        sign = 1 if s.side=='long' else -1
        f,slip = fee['rate'],self.settings.slippage
        entry = rounded(price*(1+sign*slip),ins.tick_size,up=sign>0)
        stop = rounded(s.stop,ins.tick_size,up=sign<0)
        target = rounded(s.target,ins.tick_size,up=sign<0)
        exit_stop = rounded(stop*(1-sign*slip),ins.tick_size,up=sign<0)
        exit_tp = rounded(target*(1-sign*slip),ins.tick_size,up=sign<0)
        if sign*(entry-stop) <= 0 or sign*(target-entry) <= 0:
            return 'Цена уже вне допустимого входа'
        funding_per_unit = max(0,sign*funding_rate)*entry
        risk_unit = sign*(entry-exit_stop)+f*(entry+exit_stop)+funding_per_unit
        reward_unit = sign*(exit_tp-entry)-f*(entry+exit_tp)-funding_per_unit
        if risk_unit <= 0 or reward_unit <= 0:
            return 'Расходы превышают потенциальную прибыль'
        budget = max(0,self.equity())*self.settings.risk_fraction
        margin_unit = entry/self.settings.leverage + entry*f
        qty = rounded(min(budget/risk_unit,self.available()/margin_unit,ins.max_qty),ins.qty_step)
        s.entry,s.stop,s.target,s.qty = entry,stop,target,qty
        s.risk,s.expected_net = qty*risk_unit,qty*reward_unit
        s.expected_fees = qty*f*(entry+exit_tp)
        s.funding_reserve = qty*funding_per_unit
        s.rr = reward_unit/risk_unit
        slippage_cost=qty*slip*(entry+target)
        s.expense_risk_fraction = (s.expected_fees+slippage_cost+s.funding_reserve)/s.risk if s.risk else 1.
        minimum_rr = float(s.filter_results.get('required_net_rr', self.settings.min_rr))
        if s.rr < minimum_rr:
            return 'Чистое отношение прибыли к риску ниже порога'
        if qty < ins.min_qty or qty*entry < ins.min_notional or qty <= 0:
            return 'Недостаточно капитала для минимального ордера'
        return ''

    def process(self, signals: list[Signal], quotes: dict, time: int,
                funding: dict | None = None, *, persist=True):
        self.daily(time)
        fresh = list({s.id:s for s in signals if not self.store.has_signal(s.id)}.values())
        groups: dict[str,list[Signal]] = {}
        for s in fresh:
            groups.setdefault(s.symbol,[]).append(s)
        eligible: list[Signal] = []
        for symbol, group in groups.items():
            conflict = len({s.side for s in group})>1
            for s in group:
                quote = quotes.get(symbol)
                reason = ('Конфликт направлений' if conflict else
                          'Входы приостановлены' if self.paused else
                          'Дневной лимит убытка' if self.day_halted else
                          'Поток котировок отключен' if not self.connected else
                          'Устаревшая котировка открытой позиции' if any(time-self.market_times.get(p['symbol'],0)>
                              self.settings.stale_seconds*1000 for p in self.positions) else
                          'Устаревшая котировка' if not quote or time-quote['time']>self.settings.stale_seconds*1000 else '')
                # Entries must occur on a quote published after the breakout close.
                if not reason and quote['time'] < s.time:
                    reason = 'Котировка получена до подтверждения пробоя'
                if not reason:
                    reason = self.quote_plan(s,quote['price'],time,(funding or {}).get(symbol,0))
                if reason:
                    s.status,s.reason='rejected',reason
            eligible.extend(s for s in group if s.status=='found')
        # Rank the entire simultaneous batch. Symbol iteration order must not
        # determine which two signals receive the shared portfolio capital.
        chosen_symbols: set[str] = set()
        for s in sorted(eligible,key=lambda s:(s.expense_risk_fraction,-s.rr,s.symbol,int(s.timeframe),s.pattern,s.id)):
            symbol=s.symbol
            reason = ('Выбран сигнал с лучшим отношением прибыли к риску' if symbol in chosen_symbols else
                      'По монете уже есть позиция' if any(p['symbol']==symbol for p in self.positions) else
                      'Достигнут лимит позиций' if len(self.positions)>=self.settings.max_positions else '')
            if not reason:
                reason = self.quote_plan(s,quotes[symbol]['price'],time,(funding or {}).get(symbol,0))
            if reason:
                s.status,s.reason='rejected',reason
                continue
            fee = self.fees[symbol]['rate']
            entry_fee = s.entry*s.qty*fee
            self.cash -= entry_fee
            self.positions.append({'id':s.id,'symbol':symbol,'side':s.side,'sign':1 if s.side=='long' else -1,
                'entry':s.entry,'stop':s.stop,'target':s.target,'qty':s.qty,'fee_rate':fee,'entry_fee':entry_fee,
                'funding':0.,'opened':time,'funding_cursor':time,'last_time':time,'timeframe':s.timeframe,
                'pattern':s.pattern,'recovered':False,
                'strategy_version':s.strategy_version,'strategy_variant':s.strategy_variant,
                'entry_method':s.entry_method,
                'market_scenario':s.filter_results.get('market_context',{}).get('scenario'),
                'exit_reason':'','max_hold_ms':int(s.filter_results.get('max_hold_ms',0))})
            chosen_symbols.add(symbol)
            self.prices[symbol]=quotes[symbol]['price']
            self.market_times[symbol]=time
            s.status='open'
        self.daily(time)
        if persist:self.persist(fresh)
        return fresh

    def close_position(self,p,raw_price,time,reason,recovered=False,*,persist=True,changed_signals=None):
        ins = self.instruments.get(p['symbol'])
        tick = ins.tick_size if ins else .00000001
        exit_price=rounded(raw_price*(1-p['sign']*self.settings.slippage),tick,up=p['sign']<0)
        rate = self.fees.get(p['symbol'],{}).get('rate',p['fee_rate'])
        fee=exit_price*p['qty']*rate
        gross=(exit_price-p['entry'])*p['qty']*p['sign']
        self.cash += gross-fee
        trade=p | {'exit':exit_price,'closed':time,'exit_fee':fee,'gross':gross,
                   'net':gross-p['entry_fee']-fee+p['funding'],'reason':reason,'recovered':recovered or p['recovered']}
        self.trades.append(trade)
        self.positions.remove(p)
        sig=self.store.signal(p['id'])
        if sig:
            sig['status']='closed_sl' if reason=='stop' else 'closed_tp' if reason=='target' else 'closed_time'
            if changed_signals is not None:changed_signals.append(sig)
            if persist:self.persist([sig])
        return trade

    def tick(self,symbol,price,time,recovered=False,*,persist=True):
        if time <= self.market_times.get(symbol,0):
            return []
        self.prices[symbol]=price
        self.market_times[symbol]=time
        changed_signals=[]
        for p in list(self.positions):
            if p['symbol']!=symbol or time < p['opened']:
                continue
            p['last_time']=time
            if p['sign']*(price-p['stop'])<=0:
                self.close_position(p,price,time,'stop',recovered,persist=False,changed_signals=changed_signals)
            elif p['sign']*(price-p['target'])>=0:
                self.close_position(p,price,time,'target',recovered,persist=False,changed_signals=changed_signals)
            elif p.get('max_hold_ms') and time-p['opened']>=p['max_hold_ms']:
                self.close_position(p,price,time,'time',recovered,persist=False,changed_signals=changed_signals)
        self.daily(time)
        if persist and changed_signals:self.persist(changed_signals)
        return changed_signals

    def bar(self,symbol,c: Candle,end_time:int,recovered=True):
        # A recovery minute can partly overlap the last known tick; conservatively
        # inspect its full range and disclose reconstruction in the trade.
        if end_time <= self.market_times.get(symbol,0):
            return
        self.prices[symbol]=c.close
        for p in list(self.positions):
            if p['symbol']!=symbol or end_time<=p['opened']:
                continue
            stop_hit = c.low<=p['stop'] if p['sign']>0 else c.high>=p['stop']
            tp_hit = c.high>=p['target'] if p['sign']>0 else c.low<=p['target']
            p['last_time']=end_time
            p['recovered'] |= recovered
            if stop_hit:
                raw = min(c.open,p['stop']) if p['sign']>0 else max(c.open,p['stop'])
                self.close_position(p,raw,end_time,'stop',recovered)
            elif tp_hit:
                self.close_position(p,p['target'],end_time,'target',recovered)
            elif p.get('max_hold_ms') and end_time-p['opened']>=p['max_hold_ms']:
                self.close_position(p,c.close,end_time,'time',recovered)
        self.market_times[symbol]=end_time
        self.daily(end_time)

    def funding_payment(self,symbol,rate,time,mark_price):
        for p in self.positions:
            if p['symbol']!=symbol or time<=p['funding_cursor'] or time<=p['opened']:
                continue
            amount=-p['sign']*p['qty']*mark_price*rate
            self.cash+=amount
            p['funding']+=amount
            p['funding_cursor']=time
        self.daily(time)
        self.persist()

    def status(self,time):
        eq=self.equity()
        fee_sources=sorted({fee.get('source','account') for fee in self.fees.values()
                            if 0<=time-fee['time']<=86400000})
        return {'mode':'paper','balance':self.cash,'equity':eq,'available':self.available(),
                'unrealized':eq-self.cash,'realized':sum(t['net'] for t in self.trades),
                'funding':sum(t['funding'] for t in self.trades)+sum(p['funding'] for p in self.positions),
                'max_drawdown':self.max_drawdown,'daily_drawdown':max(0,1-eq/self.day_equity) if self.day_equity else 0,
                'paused':self.paused,'day_halted':self.day_halted,'connected':self.connected,
                'positions':self.positions,'fees_ready':sum(0<=time-f['time']<=86400000 for f in self.fees.values()),
                'fee_sources':fee_sources,
                'settings':asdict(self.settings)}
