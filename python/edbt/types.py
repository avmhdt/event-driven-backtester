"""Public enums, dataclasses and errors of the Python API (spec section 10).

Prices and quantities are floats in instrument units at this boundary
(derived from integer ticks and lots); timestamps are integer nanoseconds.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import IntEnum, StrEnum
from typing import Literal


class Side(IntEnum):
    """Order side."""

    BUY = 1
    SELL = -1


class OrdType(StrEnum):
    """Order type."""

    LIMIT = "limit"
    MARKET = "market"


class TimeInForce(StrEnum):
    """Time in force; GTX is post-only."""

    GTC = "gtc"
    IOC = "ioc"
    FOK = "fok"
    GTX = "gtx"


class OrderStatus(StrEnum):
    """Order status (FR-07)."""

    PENDING_NEW = "pending_new"
    NEW = "new"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    PENDING_CANCEL = "pending_cancel"
    CANCELED = "canceled"
    REJECTED = "rejected"
    EXPIRED = "expired"


class Liquidity(StrEnum):
    """Maker or taker."""

    MAKER = "maker"
    TAKER = "taker"


class Mode(StrEnum):
    """Run mode."""

    BACKTEST = "backtest"
    PAPER = "paper"
    TESTNET = "testnet"


class EdbtError(Exception):
    """Base class of edbt errors."""


class ConfigError(EdbtError):
    """Invalid configuration."""


class DataError(EdbtError):
    """Missing or invalid market data."""


class InvalidOrderError(EdbtError):
    """Malformed order intent (off-grid price or size, unknown symbol)."""


class ReconciliationError(EdbtError):
    """The ledger failed a reconciliation identity."""


class LiveGuardError(EdbtError):
    """A non-allowlisted host or a mainnet host was refused."""


@dataclass(frozen=True, slots=True)
class Trade:
    """A trade as seen locally."""

    ts_exchange: int
    ts_local: int
    symbol: str
    price: float
    qty: float
    side: int


@dataclass(frozen=True, slots=True)
class OrderUpdate:
    """The state of an order."""

    client_order_id: int
    venue_order_id: str | None
    symbol: str
    side: Side
    ord_type: OrdType
    tif: TimeInForce
    price: float | None
    qty: float
    filled_qty: float
    leaves_qty: float
    avg_fill_price: float | None
    status: OrderStatus
    reason: str | None
    tag: str | None
    ts_local: int
    ts_exchange: int | None


@dataclass(frozen=True, slots=True)
class Fill:
    """A fill; `fee` is negative for a rebate received."""

    client_order_id: int
    symbol: str
    side: Side
    price: float
    qty: float
    liquidity: Liquidity
    fee: float
    ts_exchange: int
    ts_local: int
    queue_ahead_at_entry: float | None
    tag: str | None


@dataclass(frozen=True, slots=True)
class Position:
    """A position."""

    symbol: str
    qty: float
    avg_entry_price: float | None
    realized_pnl: float
    unrealized_pnl: float
    fees: float
    mark_price: float | None


@dataclass(frozen=True, slots=True)
class RiskEvent:
    """A risk event."""

    kind: Literal["reject", "kill_switch", "kill_reset", "warning"]
    rule: str
    detail: str
    client_order_id: int | None
    ts_local: int


@dataclass(frozen=True, slots=True)
class RiskState:
    """Risk state."""

    killed: bool
    kill_reason: str | None
    open_orders: int
    orders_last_sec: int
    position_lots: dict[str, int]
    session_pnl: float


@dataclass(frozen=True, slots=True)
class Subscriptions:
    """Callback filtering, applied in Rust."""

    book: Literal["none", "bbo_change", "every_update"] = "bbo_change"
    trades: bool = True
    depth_levels: int = 10
    symbols: tuple[str, ...] | None = None


@dataclass(frozen=True, slots=True)
class Summary:
    """Deterministic run metrics."""

    pnl_total: float
    pnl_realized: float
    pnl_unrealized: float
    fees_total: float
    rebates_total: float
    n_events: int
    n_orders: int
    n_cancels: int
    n_rejects: int
    n_fills: int
    fill_ratio: float
    maker_fill_share: float
    traded_notional: float
    max_drawdown: float
    final_positions: dict[str, float]
    reconciliation_violations: int
    book_gaps: int
    kill_switch_triggered: bool


@dataclass(frozen=True, slots=True)
class Perf:
    """Non-deterministic performance numbers."""

    events_processed: int
    wall_time_s: float
    events_per_s: float
    python_callbacks: int
    peak_rss_mb: float
