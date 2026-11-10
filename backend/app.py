import asyncio
import contextlib
import json
import math
import re
import shutil
import subprocess
from contextlib import asynccontextmanager
from dataclasses import asdict
import os
from pathlib import Path
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import FileResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, ConfigDict
from .models import Settings, now_ms
from .storage import Store
from .process_lock import ScannerProcessLock
from .engine import Engine
from .bybit import Bybit
from .service import Scanner
from .backtest import download_replay
from .patterns import NAMES, EXPERIMENTAL_NAMES

ROOT=Path(__file__).resolve().parents[1]
load_dotenv(ROOT/'.env')


def find_chrome():
    candidates=[]
    path=shutil.which('chrome.exe') or shutil.which('chrome')
    if path:candidates.append(Path(path))
    for root in (os.getenv('PROGRAMFILES'),os.getenv('PROGRAMFILES(X86)'),os.getenv('LOCALAPPDATA')):
        if root:candidates.append(Path(root)/'Google'/'Chrome'/'Application'/'chrome.exe')
    return next((path for path in candidates if path.is_file()),None)


class SettingsUpdate(BaseModel):
    model_config=ConfigDict(extra='forbid')
    risk_fraction: float=Field(default=.005,gt=0,le=.02)
    max_positions: int=Field(default=2,ge=1,le=10)
    daily_loss_fraction: float=Field(default=.02,gt=0,le=.1)
    slippage: float=Field(default=.0005,ge=0,le=.01)
    min_rr: float=Field(default=2,ge=1,le=10)


class BacktestRequest(BaseModel):
    model_config=ConfigDict(extra='forbid')
    symbol: str=Field(pattern=r'^[A-Z0-9]{2,30}USDT$')
    timeframe: str=Field(pattern=r'^(5|15|60|240)$')
    days: int=Field(default=7,ge=1,le=30)


class AccountSignalLink(BaseModel):
    model_config=ConfigDict(extra='forbid')
    setup_id: str=Field(min_length=1,max_length=64,pattern=r'^[A-Za-z0-9_-]+$')


def create_app(scanner=None):
    @asynccontextmanager
    async def lifespan(app):
        owned=scanner is None
        service=scanner
        process_lock=None
        store=None
        if owned:
            database_path=os.getenv('DATABASE_PATH',str(ROOT/'data/crypto.sqlite3'))
            process_lock=ScannerProcessLock(database_path).acquire()
            try:
                store=Store(database_path)
                service=Scanner(Engine(store),Bybit())
            except BaseException:
                if store is not None:
                    store.close()
                process_lock.release()
                raise
        app.state.scanner=service
        app.state.backtest_task=None
        app.state.backtest_result=None
        app.state.backtest_error=''
        try:
            if owned:
                await service.start()
            yield
        finally:
            task=app.state.backtest_task
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
            if owned:
                try:
                    await service.stop()
                finally:
                    service.engine.store.close()
                    process_lock.release()

    app=FastAPI(title='Фигуры · Bybit Paper',lifespan=lifespan)

    @app.middleware('http')
    async def local_access(request:Request,call_next):
        host=request.headers.get('host','').split(':')[0]
        origin=request.headers.get('origin')
        if host not in ('127.0.0.1','localhost','testserver') or (origin and origin not in
                ('http://127.0.0.1:8000','http://localhost:8000','http://127.0.0.1:8001','http://localhost:8001',
                 'http://127.0.0.1:5173','http://localhost:5173')):
            return JSONResponse({'detail':'Только локальный доступ'},status_code=403)
        response=await call_next(request)
        response.headers['X-Content-Type-Options']='nosniff'
        return response

    @app.get('/api/state')
    async def state():
        return await asyncio.to_thread(app.state.scanner.status)

    @app.get('/api/snapshot')
    async def snapshot(view:str='full',signal_scope:str='all'):
        if view not in ('full','summary'):raise HTTPException(400,'Invalid snapshot view')
        if signal_scope not in ('current','history','all'):raise HTTPException(400,'Invalid signal scope')
        payload=await asyncio.to_thread(app.state.scanner.snapshot_payload,
            limit=50,setup_limit=50,view=view,signal_scope=signal_scope)
        return app.state.scanner.stamp_publications(payload)

    @app.get('/api/signals')
    async def signals(limit:int=200,before_time:int|None=None,before_id:str|None=None,
                      status:str|None=None,view:str='full',signal_scope:str='all'):
        if view not in ('full','summary'):raise HTTPException(400,'Invalid signal view')
        if signal_scope not in ('current','history','all'):raise HTTPException(400,'Invalid signal scope')
        if before_id is not None and len(before_id)>64:
            raise HTTPException(400,'Invalid signal cursor id')
        statuses=[item for item in (status or '').split(',') if item]
        service=app.state.scanner
        def load():
            rows=service.signal_feed(max(1,min(limit,1000)),before_time,before_id,statuses,signal_scope=signal_scope)
            return [service.summary_signal(row) for row in rows] if view=='summary' else rows
        rows=await asyncio.to_thread(load)
        service.stamp_publications({'signals':rows})
        return rows

    @app.post('/api/open-bybit/{symbol}')
    async def open_bybit_in_chrome(symbol:str):
        if not re.fullmatch(r'[A-Z0-9]{2,30}USDT',symbol):
            raise HTTPException(400,'Некорректная USDT-пара')
        service=app.state.scanner
        known=(symbol in service.symbols or symbol in service.engine.instruments or
               any(row.get('symbol')==symbol for row in service.signal_feed(10000)))
        if not known:
            raise HTTPException(404,'Пара отсутствует среди отслеживаемых инструментов и сигналов')
        chrome=find_chrome()
        if chrome is None:
            raise HTTPException(503,'Google Chrome не найден на этом компьютере')
        url=f'https://www.bybit.com/trade/usdt/{symbol}'
        try:
            subprocess.Popen([str(chrome),'--new-tab',url],stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,close_fds=True,
                creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0))
        except OSError:
            raise HTTPException(500,'Не удалось запустить Google Chrome') from None
        return {'opened':True,'symbol':symbol}

    @app.get('/api/setups')
    async def setups(limit:int=300,before_time:int|None=None,before_id:str|None=None,
                     status:str|None=None,view:str='full'):
        if view not in ('full','summary'):raise HTTPException(400,'Invalid setup view')
        if before_id is not None and len(before_id)>64:
            raise HTTPException(400,'Invalid setup cursor id')
        service=app.state.scanner
        statuses=[item for item in (status or '').split(',') if item]
        values=service.engine.store.setups(max(1,min(limit,1000)),before_time,before_id,statuses)
        result=[]
        for item in values:
            quote=service.quotes.get(item['symbol'],{})
            if quote and service.market_now()-quote.get('time',0)<=5000 and item['status'] not in ('breakout_confirmed','cancelled','expired','target_reached'):
                current=service.setup_price_state(item,quote['price'],quote['time'],persist=False)
                item['current']=current
            else:
                item['current']={'status':'stale' if quote else item['status'],'quote_stale':True}
            result.append(service.decorate_setup(item))
        return [service.summary_setup(row) for row in result] if view=='summary' else result

    @app.get('/api/setups/{setup_id}/context')
    async def setup_context(setup_id:str):
        if len(setup_id)>64:raise HTTPException(400,'Неверный ID фигуры')
        service=app.state.scanner
        service.request_setup_context(setup_id)
        result=service.setup_context(setup_id)
        if result is None:raise HTTPException(404,'Фигура не найдена')
        return result

    @app.get('/api/signals/{signal_id}/context')
    async def signal_context(signal_id:str):
        if len(signal_id)>64:
            raise HTTPException(400,'РќРµРІРµСЂРЅС‹Р№ ID СЃРёРіРЅР°Р»Р°')
        result=await app.state.scanner.refresh_decision_context(signal_id)
        if result is None:
            raise HTTPException(404,'РЎРёРіРЅР°Р» РЅРµ РЅР°Р№РґРµРЅ')
        return result

    @app.get('/api/forecast-outcomes')
    async def forecast_outcomes():
        return await app.state.scanner.forecast_outcome_report()

    @app.get('/api/trades')
    async def trades():
        e=app.state.scanner.engine
        return {'open':e.positions,'closed':e.trades[-1000:]}

    @app.get('/api/account/status')
    async def account_status():
        service=app.state.scanner
        return {'status':service.account_monitor.public_status(),
                'updated_at':service.market_now()}

    @app.get('/api/account/positions')
    async def account_positions():
        return app.state.scanner.engine.store.account_positions()

    @app.get('/api/account/setups/{symbol}')
    async def account_setups(symbol:str):
        if not re.fullmatch(r'[A-Z0-9]{2,30}USDT',symbol):
            raise HTTPException(400,'Некорректная USDT-пара')
        service=app.state.scanner
        return [{'id':row['id'],'symbol':row['symbol'],'timeframe':row['timeframe'],
                 'pattern':row['pattern'],'side':row['side'],'status':row['status'],
                 'event_at':row.get('event_at') or row.get('detected_at')}
                for row in service.engine.store.account_setups(symbol,None,300)]

    @app.get('/api/account/trades')
    async def account_trades(limit:int=100,before_time:int|None=None,before_id:str|None=None):
        if before_id is not None and len(before_id)>100:
            raise HTTPException(400,'Неверный курсор исполнения')
        service=app.state.scanner
        return {'executions':service.engine.store.account_executions(
                    max(1,min(limit,500)),before_time,before_id),
                'orders':service.engine.store.account_orders(100),
                'status':service.account_monitor.public_status()}

    @app.get('/api/account/trades/{exec_id}')
    async def account_trade_review(exec_id:str):
        if not exec_id or len(exec_id)>100:
            raise HTTPException(400,'Неверный ID исполнения')
        result=app.state.scanner.account_trade_review(exec_id)
        if result is None:raise HTTPException(404,'Исполнение не найдено')
        return result

    @app.put('/api/account/trades/{exec_id}/signal')
    async def link_account_trade(exec_id:str,body:AccountSignalLink):
        service=app.state.scanner
        execution=next((row for row in service.engine.store.account_executions(500)
                        if row.get('execId')==exec_id),None)
        setup=service.engine.store.setup(body.setup_id)
        if not execution or not setup:raise HTTPException(404,'Исполнение или сценарий не найден')
        if execution.get('symbol')!=setup.get('symbol'):
            raise HTTPException(409,'Исполнение и сценарий относятся к разным парам')
        try:
            executed=float(execution.get('execQty') or 0)
            closed=float(execution.get('closedSize') or 0)
        except (TypeError,ValueError):
            executed=closed=0
        if executed>0 and closed>=executed:
            raise HTTPException(409,'Pure close executions cannot link to an entry setup')
        position_idx=int(execution.get('positionIdx') or 0)
        execution_side=('long' if position_idx==1 else 'short' if position_idx==2 else
                        {'Buy':'long','Sell':'short'}.get(execution.get('side')))
        if execution_side!=setup.get('side'):
            raise HTTPException(409,'Execution side does not match the setup side')
        if not await service.engine.store.alink_account_execution(exec_id,body.setup_id,service.market_now()):
            raise HTTPException(409,'Не удалось связать исполнение со сценарием')
        service.broadcast_event.set()
        return service.account_trade_review(exec_id)

    @app.get('/api/candles/{symbol}/{tf}')
    async def candles(symbol:str,tf:str,limit:int=1000,start:int|None=None,end:int|None=None,after:int|None=None):
        if tf not in ('5','15','60','240') or len(symbol)>40:
            raise HTTPException(400,'Неверная пара или таймфрейм')
        if not 1<=limit<=1000 or (start is not None and end is not None and start>=end):
            raise HTTPException(400,'Invalid candle range or limit')
        if after is not None and (start is not None or end is not None):
            raise HTTPException(400,'after cannot be combined with start/end')
        service=app.state.scanner
        if start is None and end is None and after is None:
            rows=await asyncio.to_thread(service.engine.store.candles,symbol,tf,limit)
        else:
            rows=await asyncio.to_thread(service.engine.store.candles_range,symbol,tf,
                limit=limit,start=start,end=end,after=after)
        by_time={c.time:c for c in rows}
        buffers=(getattr(service,'live_buffers',{}) if getattr(service,'split_mode',False)
                 else getattr(service,'buffers',{}))
        for c in buffers.get((symbol,tf),()):
            if ((start is None or c.time>=start) and (end is None or c.time<end)
                    and (after is None or c.time>after)):
                by_time[c.time]=c
        if start is not None and end is not None:
            interval=int(tf)*60_000
            expected=min(limit,max(0,(end-start+interval-1)//interval))
            if len(by_time)<expected:
                service.request_chart_history(symbol,tf,start,end)
        ordered=sorted(by_time.values(),key=lambda c:c.time)
        return [asdict(c) for c in ordered[-limit:]]

    @app.get('/api/settings')
    async def settings():
        return asdict(app.state.scanner.engine.settings)

    @app.put('/api/settings')
    async def update_settings(body:SettingsUpdate):
        e=app.state.scanner.engine
        e.settings=Settings(**(asdict(e.settings)|body.model_dump()))
        e.persist()
        return asdict(e.settings)

    @app.post('/api/pause')
    async def pause():
        e=app.state.scanner.engine
        e.paused=True
        e.persist()
        return {'paused':True}

    @app.post('/api/resume')
    async def resume():
        e=app.state.scanner.engine
        e.paused=False
        e.persist()
        return {'paused':False}

    @app.get('/api/patterns')
    async def patterns():
        return NAMES | EXPERIMENTAL_NAMES

    @app.post('/api/backtest',status_code=202)
    async def backtest(body:BacktestRequest):
        task=app.state.backtest_task
        if task and not task.done():
            raise HTTPException(409,'Исторический прогон уже выполняется')
        scanner=app.state.scanner
        fee=scanner.engine.fees.get(body.symbol)
        if not fee or now_ms()-fee['time']>86400000:
            raise HTTPException(400,'Нет актуальной комиссии для выбранной пары')
        app.state.backtest_result=None
        app.state.backtest_error=''
        async def execute():
            try:
                app.state.backtest_result=await download_replay(scanner.bybit,body.symbol,body.timeframe,
                    body.days,fee['rate'],Settings(**asdict(scanner.engine.settings)))
            except Exception as exc:
                app.state.backtest_error=scanner.safe_error(exc)
        app.state.backtest_task=asyncio.create_task(execute())
        return {'status':'running'}

    @app.get('/api/backtest')
    async def backtest_status():
        task=app.state.backtest_task
        return {'status':'running' if task and not task.done() else 'error' if app.state.backtest_error else
            'complete' if app.state.backtest_result is not None else 'idle',
            'error':app.state.backtest_error,'result':app.state.backtest_result}

    @app.get('/api/portfolio-backtest')
    async def portfolio_backtest():
        directory=ROOT/'data'/'portfolio_60d'
        def read(name):
            path=directory/name
            return __import__('json').loads(path.read_text(encoding='utf-8')) if path.exists() else None
        return {'progress':read('progress.json'),'report':read('summary.json')}

    @app.get('/api/research')
    async def research():
        directory=ROOT/'data'/'research'
        def read(name):
            path=directory/name
            return __import__('json').loads(path.read_text(encoding='utf-8')) if path.exists() else None
        return {'progress':read('progress.json'),'report':read('summary.json'),
                'approval':app.state.scanner.research,
                'active_variant':app.state.scanner.engine.strategy_variant}

    @app.get('/api/research/file/{name}')
    async def research_file(name:str):
        allowed={'summary.json','variants.csv','signals.csv','signals_by_pattern.csv','trades.csv','equity.csv','coverage.json'}
        if name not in allowed:
            raise HTTPException(404)
        path=ROOT/'data'/'research'/name
        if not path.is_file():
            raise HTTPException(404)
        return FileResponse(path,filename=name)

    @app.get('/api/portfolio-backtest/file/{name}')
    async def portfolio_backtest_file(name:str):
        allowed={'summary.json','patterns.csv','timeframes.csv','pairs.csv',
                 'signals.csv','trades.csv','trades.json','coverage.json'}
        if name not in allowed:
            raise HTTPException(404)
        path=ROOT/'data'/'portfolio_60d'/name
        if not path.is_file():
            raise HTTPException(404)
        return FileResponse(path,filename=name)

    @app.websocket('/ws')
    async def websocket(ws:WebSocket):
        if ws.headers.get('origin') not in ('http://localhost:8000','http://127.0.0.1:8000',
                'http://localhost:8001','http://127.0.0.1:8001',
                'http://localhost:5173','http://127.0.0.1:5173'):
            await ws.close(code=1008)
            return
        signal_scope=ws.query_params.get('signal_scope','all')
        if signal_scope not in ('current','history','all'):
            await ws.close(code=1008)
            return
        await ws.accept()
        service=app.state.scanner
        version=ws.query_params.get('v','2')
        versioned=version in ('2','3')
        service.client_scopes[ws]=signal_scope
        try:
            if version in ('2','3'):
                publisher=(service.v2_publishers if version=='2' else service.v3_publishers)[signal_scope]
                # Capture the revision before leaving the event loop. If other
                # clients advance it during the read or send, the next delta
                # exposes a revision gap and this client requests a resync.
                revision=publisher.revision
                initial=await asyncio.to_thread(service.snapshot_payload,limit=50,setup_limit=50,
                    view='summary' if version=='3' else 'full',signal_scope=signal_scope)
                service.stamp_publications(initial)
                initial=initial|{'stream_id':publisher.stream_id,'revision':revision}
                await ws.send_json(initial)
                service.clients.add(ws)
                if version=='2':
                    service.v2_clients[ws]=revision
                else:
                    queue=asyncio.Queue(maxsize=2)
                    service.v3_clients[ws]=queue
                    service.v3_sender_tasks[ws]=asyncio.create_task(
                        service.v3_send_loop(ws,queue),name='websocket-v3-sender')
            else:
                service.clients.add(ws)
            while True:
                raw=await ws.receive_text()
                try:
                    message=json.loads(raw)
                except (ValueError,TypeError):
                    continue
                if isinstance(message,dict) and message.get('type')=='watch_signal':
                    service.watch_context(ws,message.get('id'))
                elif isinstance(message,dict) and message.get('type')=='watch_setup':
                    service.watch_setup(ws,message.get('id'))
                elif isinstance(message,dict) and message.get('type')=='watch_chart':
                    service.watch_chart(ws,message.get('symbol'),message.get('timeframe'))
                elif isinstance(message,dict) and message.get('type')=='watch_account':
                    service.watch_account(ws,bool(message.get('enabled')))
                elif isinstance(message,dict) and message.get('type')=='render_ack':
                    try:
                        render_ms=float(message.get('render_ms'))
                        if math.isfinite(render_ms) and 0<=render_ms<=10_000:
                            service.browser_render_ms.append(render_ms)
                    except (TypeError,ValueError):
                        pass
                elif versioned and isinstance(message,dict) and message.get('type')=='resync':
                    if version=='2':
                        publisher=service.v2_publishers[signal_scope]
                        revision=publisher.revision
                        initial=await asyncio.to_thread(service.snapshot_payload,
                            limit=50,setup_limit=50,signal_scope=signal_scope)
                        service.stamp_publications(initial)
                        service.v2_clients[ws]=revision
                        await ws.send_json(initial|{'stream_id':publisher.stream_id,'revision':revision})
                    elif version=='3':
                        queue=service.v3_clients.get(ws)
                        if queue:
                            publisher=service.v3_publishers[signal_scope]
                            revision=publisher.revision
                            initial=await asyncio.to_thread(service.snapshot_payload,
                                limit=50,setup_limit=50,view='summary',signal_scope=signal_scope)
                            service.stamp_publications(initial)
                            while not queue.empty():
                                with contextlib.suppress(asyncio.QueueEmpty):
                                    queue.get_nowait();queue.task_done()
                            initial=initial|{'stream_id':publisher.stream_id,'revision':revision}
                            queue.put_nowait((initial,asyncio.get_running_loop().time()))
        except WebSocketDisconnect:
            pass
        finally:
            service.clients.discard(ws)
            service.client_scopes.pop(ws,None)
            service.v2_clients.pop(ws,None)
            service.v3_clients.pop(ws,None)
            sender=service.v3_sender_tasks.pop(ws,None)
            if sender:
                sender.cancel()
                try:await sender
                except asyncio.CancelledError:pass
            service.watched_context.pop(ws,None)
            service.watched_setups.pop(ws,None)
            service.watched_charts.pop(ws,None)
            service.account_watchers.discard(ws)
            service.context_refresh_event.set()

    dist=ROOT/'frontend/dist'
    if (dist/'assets').exists():
        app.mount('/assets',StaticFiles(directory=dist/'assets'),name='assets')
    @app.get('/')
    def index():
        if (dist/'index.html').exists():
            return FileResponse(dist/'index.html')
        return JSONResponse({'message':'Соберите интерфейс: cd frontend; npm.cmd install; npm.cmd run build'})
    return app


app=create_app()
