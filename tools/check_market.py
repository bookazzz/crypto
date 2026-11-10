"""Public network smoke test; does not access API credentials."""
import asyncio
import json
import sys
import argparse
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from backend.bybit import Bybit


async def main(scanner_check=False):
    bybit=Bybit()
    bybit.key=bybit.secret=''
    try:
        instruments,symbols,tickers=await bybit.universe(20)
        candles=await bybit.candles(symbols[0],'15')
        print(json.dumps({'instruments':len(instruments),'symbols':symbols,'candles':len(candles)}))
        import websockets
        async with websockets.connect(bybit.ws_url,ping_interval=20) as ws:
            await ws.send(json.dumps({'op':'subscribe','args':[f'tickers.{symbols[0]}']}))
            for _ in range(10):
                data=json.loads(await asyncio.wait_for(ws.recv(),15))
                if data.get('topic'):
                    print(json.dumps({'websocket':data['topic'],'type':data.get('type')}))
                    break
            else:
                raise RuntimeError('No ticker received')
        if scanner_check:
            from backend.storage import Store
            from backend.engine import Engine
            from backend.service import Scanner
            scanner=Scanner(Engine(Store(':memory:')),bybit)
            await scanner.start()
            try:
                for _ in range(90):
                    if scanner.phase=='live' and len(scanner.quotes)>=20:
                        print(json.dumps({'scanner':scanner.phase,'buffers':len(scanner.buffers),'quotes':len(scanner.quotes),
                            'fee_entries_blocked':scanner.engine.status(0)['fees_ready']==0,'fee_error_present':bool(scanner.fee_error)}))
                        return
                    await asyncio.sleep(1)
                raise RuntimeError('Scanner did not become live: '+scanner.error)
            finally:
                await scanner.stop()
                scanner.engine.store.close()
    finally:
        await bybit.close()


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--scanner',action='store_true')
    args=parser.parse_args()
    asyncio.run(main(args.scanner))
