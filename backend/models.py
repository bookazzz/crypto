from dataclasses import asdict, dataclass, field
from decimal import Decimal, ROUND_DOWN, ROUND_CEILING, ROUND_FLOOR
import hashlib
import time


def now_ms() -> int:
    return int(time.time() * 1000)


def rounded(value: float, step: float, up: bool = False) -> float:
    return float((Decimal(str(value)) / Decimal(str(step))).to_integral_value(
        rounding=ROUND_CEILING if up else ROUND_FLOOR) * Decimal(str(step)))


@dataclass
class Candle:
    time: int
    open: float
    high: float
    low: float
    close: float
    volume: float = 0
    turnover: float | None = None


@dataclass
class Instrument:
    symbol: str
    qty_step: float
    min_qty: float
    min_notional: float
    tick_size: float
    funding_interval: int = 480
    max_qty: float = 1e15
    symbol_type: str = ''
    asset_class: str = 'crypto'
    market_region: str = ''
    display_name: str = ''
    underlying_ticker: str = ''
    status: str = 'Trading'
    launch_time: int = 0
    pre_listing: bool = False


@dataclass
class Settings:
    initial_balance: float = 1000
    risk_fraction: float = .005
    leverage: float = 1
    max_positions: int = 2
    daily_loss_fraction: float = .02
    slippage: float = .0005
    min_rr: float = 2
    top_n: int = 40
    timeframes: list[str] = field(default_factory=lambda: ['5', '15', '60', '240'])
    stale_seconds: int = 30


@dataclass
class DataQuality:
    """Provenance and coverage for a market value shown to a trader."""
    status: str
    source: str | None = None
    event_at: int | None = None
    received_at: int | None = None
    age_ms: int | None = None
    coverage_ms: int | None = None
    window_ms: int | None = None
    reasons: list[str] = field(default_factory=list)


@dataclass
class Signal:
    symbol: str
    timeframe: str
    pattern: str
    side: str
    time: int
    start: int
    boundary: float
    stop: float
    target: float
    atr: float
    lines: list[dict] = field(default_factory=list)
    pivots: list[dict] = field(default_factory=list)
    id: str = ''
    status: str = 'found'
    reason: str = ''
    entry: float | None = None
    qty: float = 0
    expected_net: float = 0
    expected_fees: float = 0
    funding_reserve: float = 0
    risk: float = 0
    rr: float = 0
    strategy_version: str = ''
    strategy_variant: str = ''
    entry_method: str = ''
    filter_results: dict = field(default_factory=dict)
    expense_risk_fraction: float = 0
    event_at: int = 0
    setup_id: str = ''
    received_at: int = 0
    detected_at: int = 0
    published_at: int = 0

    def __post_init__(self):
        if not self.id:
            self.id = hashlib.sha256(f'{self.symbol}:{self.timeframe}:{self.pattern}:{self.side}:{self.start}'.encode()).hexdigest()[:24]
        if not self.event_at:
            self.event_at=self.time

    def dict(self):
        return asdict(self)
