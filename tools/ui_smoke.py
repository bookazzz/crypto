"""Browser acceptance check with an isolated in-memory synthetic market."""
import json, sys, threading, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import uvicorn
from playwright.sync_api import sync_playwright
from backend.app import create_app, find_chrome
from backend.engine import Engine
from backend.models import Candle, Signal, now_ms
from backend.service import Scanner
from backend.storage import Store

class QuietBybit:
    offset = 0
    fee_mode = "standard"
    async def tickers(self): return []
    async def close(self): pass

def main():
    engine = Engine(Store(":memory:"))
    scanner = Scanner(engine, QuietBybit())
    scanner.phase = "live"
    scanner.symbols = ["BTCUSDT"]
    engine.connected = True
    now = now_ms()
    scanner.buffers["BTCUSDT","D"]=[Candle(
        (now//86_400_000-1)*86_400_000,100,105,95,100,10)]
    scanner.quotes["BTCUSDT"]={"price":100.5,"time":now}
    scanner.tickers["BTCUSDT"]={"fundingRate":"0",
        "nextFundingTime":now+60_000,"_funding_time":now}
    engine.fees["BTCUSDT"]={"rate":.00055,"time":now,"source":"bybit_vip0_standard"}
    signal = Signal("BTCUSDT","5","ascending_triangle","long",now-60_000,
        now-3_600_000,100,98,106,2,id="browser-signal",
        event_at=now-30_000,received_at=now-29_000,detected_at=now-28_000,
        published_at=now-27_000,filter_results={'delivery':{'timing_version':2,
            'source':'websocket','mode':'current','received_at':now-29_000,
            'detected_at':now-28_000,'published_at':now-27_000,
            'publish_delay_ms':3000,'delay_ms':2000,'recovered':False}})
    historical = Signal("ETHUSDT","5","ascending_triangle","long",now-600_000,
        now-3_600_000,100,98,106,2,id="history-signal",
        event_at=now-600_000,detected_at=now-1000,
        filter_results={'delivery':{'recovered':True,'detected_at':now-1000,'delay_ms':599_000}})
    engine.persist([signal,historical])
    scanner.engine.store.save_setup({
        "id":"browser-setup","symbol":"BTCUSDT","timeframe":"5",
        "pattern":"ascending_triangle","side":"long","status":"crossing",
        "detected_at":now-20_000,"updated_at":now,"event_at":now-10_000,
        "reason":"","transitions":[],"confirmation_profile":"expanded",
        "snapshot":{"start":now-3_600_000,"detected_on_bar":now-20_000,
            "boundary":100,"stop":98,"target":102,"atr":2,"height":2,
            "lines":[],"pivots":[]}})
    server = uvicorn.Server(uvicorn.Config(create_app(scanner),
        host="127.0.0.1",port=8001,log_level="error"))
    thread = threading.Thread(target=server.run,daemon=True)
    thread.start()
    errors = []
    try:
        for _ in range(100):
            if server.started: break
            time.sleep(.05)
        if not server.started: raise RuntimeError("Smoke server did not start")
        with sync_playwright() as p:
            executable = None if Path(p.chromium.executable_path).is_file() else find_chrome()
            browser = p.chromium.launch(headless=True, executable_path=str(executable) if executable else None)
            page = browser.new_page(viewport={"width":1440,"height":1000})
            page.add_init_script("""(() => {
              const Native=window.WebSocket; window.__codexSockets=[];
              function Tracked(...args){const ws=new Native(...args);window.__codexSockets.push(ws);return ws}
              Tracked.prototype=Native.prototype;Object.setPrototypeOf(Tracked,Native);
              window.WebSocket=Tracked;
            })()""")
            page.on("pageerror",lambda e:errors.append(str(e)))
            page.goto("http://127.0.0.1:8001",wait_until="networkidle")
            quote_now=now_ms()
            scanner.quotes["BTCUSDT"]={"price":100.5,"time":quote_now}
            scanner.tickers["BTCUSDT"]={"fundingRate":"0",
                "nextFundingTime":quote_now+60_000,"_funding_time":quote_now}
            setup_context=page.request.get("http://127.0.0.1:8001/api/setups/browser-setup/context").json()
            assert any(level["kind"]=="support" and level["price"]==95
                for level in setup_context["current"]["chart_levels"])
            assert setup_context["current"]["entry_assessment"]["status"]=="insufficient_rr", setup_context["current"]["entry_assessment"]
            page.locator(".signal-card").first.wait_for()
            assert page.get_by_label("Выбор ленты сигналов").input_value()=="current"
            assert "BTCUSDT" in page.locator(".signal-card").first.inner_text()
            assert "получено" in page.locator(".signal-card .signal-delay").first.inner_text()
            page.get_by_label("Выбор ленты сигналов").select_option("history")
            page.locator(".signal-card").filter(has_text="ETHUSDT").wait_for()
            assert page.locator(".signal-card").count()==1
            page.get_by_label("Выбор ленты сигналов").select_option("current")
            page.locator(".signal-card").filter(has_text="BTCUSDT").wait_for()
            page.get_by_label("Статус сигнала").select_option("found")
            assert page.locator(".signal-card").count()==1
            page.get_by_label("Статус сигнала").select_option("all")
            page.locator(".signal-card").first.click()
            page.locator(".signal-card.selected").wait_for()
            page.locator(".signal-chart canvas").first.wait_for()
            alert_text=page.locator(".early-alert-row").first.inner_text()
            assert "BTCUSDT" in alert_text, alert_text
            scanner.quotes["BTCUSDT"]={"price":100.5,"time":now_ms()}
            scanner.tickers["BTCUSDT"]={"fundingRate":"0",
                "nextFundingTime":now_ms()+60_000,"_funding_time":now_ms()}
            page.locator(".early-alerts .setup-choice").first.click()
            page.locator(".early-layout .setup-choice.selected").wait_for()
            page.locator(".early-chart canvas").first.wait_for()
            entry_plan_text=page.locator(".entry-plan").first.inner_text()
            assert "RR" in entry_plan_text, json.dumps(entry_plan_text,ensure_ascii=True)
            page.wait_for_function("window.__codexSockets.length>=1")
            page.evaluate("window.__codexSockets[0].close()")
            page.wait_for_function("window.__codexSockets.length>=2",timeout=8000)
            page.locator(".early-layout .setup-choice.selected").wait_for()
            page.set_viewport_size({"width":390,"height":844})
            page.wait_for_timeout(300)
            if not page.evaluate("document.documentElement.scrollWidth<=innerWidth"):
                raise AssertionError("Mobile layout has horizontal overflow")
            if errors: raise AssertionError(errors)
            browser.close()
        print(json.dumps({"acceptance":["current/history signal scopes, status filter, and chart",
            "early observation, support levels, and insufficient RR label",
            "v3 websocket reconnect keeps selection",
            "mobile viewport"],"browser_errors":errors}))
    finally:
        server.should_exit=True
        thread.join(5)
        engine.store.close()

if __name__=="__main__": main()
