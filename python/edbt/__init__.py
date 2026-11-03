"""Event-driven backtester with realistic execution.

The Rust core (`edbt._edbt`) replays canonical market data through one
deterministic engine with explicit latency, queue-position and partial-fill
models; this package is its Python API (spec section 10, API 1.0.0). The
same `Strategy` runs in backtest, paper and testnet modes.
"""

from edbt._edbt import __version__
from edbt.config import Config, Settings
from edbt.results import Backtest, BacktestResult, load_result, run_backtest, run_live
from edbt.strategy import BookView, Context, Strategy
from edbt.types import (
    ConfigError,
    DataError,
    EdbtError,
    Fill,
    InvalidOrderError,
    Liquidity,
    LiveGuardError,
    Mode,
    OrderStatus,
    OrderUpdate,
    OrdType,
    Perf,
    Position,
    ReconciliationError,
    RiskEvent,
    RiskState,
    Side,
    Subscriptions,
    Summary,
    TimeInForce,
    Trade,
)

API_VERSION = "1.0.0"
"""Semantic version of the Python API contract (NFR-14)."""

__all__ = [
    "API_VERSION",
    "Backtest",
    "BacktestResult",
    "BookView",
    "Config",
    "ConfigError",
    "Context",
    "DataError",
    "EdbtError",
    "Fill",
    "InvalidOrderError",
    "Liquidity",
    "LiveGuardError",
    "Mode",
    "OrdType",
    "OrderStatus",
    "OrderUpdate",
    "Perf",
    "Position",
    "ReconciliationError",
    "RiskEvent",
    "RiskState",
    "Settings",
    "Side",
    "Strategy",
    "Subscriptions",
    "Summary",
    "TimeInForce",
    "Trade",
    "__version__",
    "load_result",
    "run_backtest",
    "run_live",
]
