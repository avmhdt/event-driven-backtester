"""Type stubs for the Rust extension module `edbt._edbt`."""

from collections.abc import Mapping
from typing import Any, final

import numpy as np
import numpy.typing as npt

__all__ = [
    "Backtest",
    "BookView",
    "Context",
    "__version__",
    "cli_main",
    "read_events",
    "resolve_config",
    "run_backtest",
]

__version__: str

def cli_main(argv: list[str]) -> int:
    """Run the edbt CLI with ``argv`` (including the program name)."""

def run_backtest(
    config_json: str,
    strategy: object | None,
    params: object,
    out_dir: str | None = None,
    log_experiment: bool = True,
) -> dict[str, str]:
    """Run a whole backtest; returns ``run_id``, ``out_dir`` and ``run_hash``."""

def resolve_config(config_json: str) -> tuple[str, str, str]:
    """Resolve a JSON config: (resolved TOML, resolved JSON, config hash)."""

def read_events(path: str, kinds: list[str] | None = None) -> dict[str, list[Any]]:
    """Decode an event log into columns ``seq, ts, rank, kind, detail``."""

@final
class BookView:
    """A view of one symbol's local book."""

    @property
    def symbol(self) -> str: ...
    @property
    def ts_local(self) -> int: ...
    @property
    def ts_exchange(self) -> int: ...
    @property
    def valid(self) -> bool: ...
    @property
    def tick_size(self) -> float: ...
    @property
    def lot_size(self) -> float: ...
    def best_bid(self) -> tuple[float, float] | None:
        """``(price, qty)`` of the best bid."""
    def best_ask(self) -> tuple[float, float] | None:
        """``(price, qty)`` of the best ask."""
    def mid(self) -> float | None:
        """Mid price."""
    def spread_ticks(self) -> int | None:
        """Ask minus bid, ticks."""
    def depth(self, levels: int = 10) -> tuple[npt.NDArray[np.float64], npt.NDArray[np.float64]]:
        """``(bids, asks)`` as ``(n, 2)`` read-only arrays of ``[price, qty]``."""
    def depth_ticks(self, levels: int = 10) -> tuple[npt.NDArray[np.int64], npt.NDArray[np.int64]]:
        """Like ``depth`` in integer ticks and lots."""
    def snapshot(self) -> BookView:
        """An owned copy, safe to keep."""

@final
class Context:
    """What a strategy can see and do."""

    @property
    def mode(self) -> Any: ...
    @property
    def params(self) -> Mapping[str, Any]: ...
    def now(self) -> int:
        """Local time, ns."""
    def book(self, symbol: str) -> BookView:
        """The local book of ``symbol``."""
    def position(self, symbol: str) -> Any:
        """Position of ``symbol``."""
    def positions(self) -> dict[str, Any]:
        """Positions of every configured symbol."""
    def open_orders(self, symbol: str | None = None) -> list[Any]:
        """Open orders, optionally of one symbol."""
    def order(self, client_order_id: int) -> Any | None:
        """Latest state of an order."""
    def submit_limit(
        self,
        symbol: str,
        side: Any,
        price: float,
        qty: float,
        *,
        tif: Any | None = None,
        tag: str | None = None,
    ) -> int:
        """Submit a limit order."""
    def submit_limit_ticks(
        self,
        symbol: str,
        side: Any,
        price_ticks: int,
        qty_lots: int,
        *,
        tif: Any | None = None,
        tag: str | None = None,
    ) -> int:
        """Submit a limit order in ticks and lots."""
    def submit_market(self, symbol: str, side: Any, qty: float, *, tag: str | None = None) -> int:
        """Submit a market order."""
    def cancel(self, client_order_id: int) -> None:
        """Cancel an order."""
    def cancel_all(self, symbol: str | None = None) -> None:
        """Cancel every open order."""
    def modify(self, client_order_id: int, *, price: float | None = None, qty: float | None = None) -> int:
        """Cancel-replace; returns the new order's id."""
    def set_timer(self, *, at_ns: int | None = None, every_ns: int | None = None) -> int:
        """Set a timer."""
    def cancel_timer(self, timer_id: int) -> None:
        """Cancel a timer."""
    def random(self) -> float:
        """Uniform in [0, 1)."""
    def rng_normal(self, mu: float = 0.0, sigma: float = 1.0) -> float:
        """Normal draw."""
    def log(self, msg: str, **fields: Any) -> None:
        """Write a line into the event log."""
    def risk_state(self) -> Any:
        """Risk state."""
    def kill_switch(self, reason: str) -> None:
        """Trigger the kill switch."""

@final
class Backtest:
    """An engine driven step by step."""

    def __new__(
        cls, config_json: str, strategy: object | None, params: object, out_dir: str, run_id: str
    ) -> Backtest: ...
    @property
    def ctx(self) -> Context: ...
    @property
    def events_processed(self) -> int: ...
    def elapse(self, duration_ns: int) -> bool:
        """Advance local time."""
    def wait_next(self, timeout_ns: int | None = None) -> str:
        """Step to the next strategy-visible event."""
    def run_to_end(self) -> None:
        """Process every remaining event."""
    def finish(self, log_experiment: bool = True) -> dict[str, str]:
        """Finish and write the run files."""
