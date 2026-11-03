"""The Python strategy base class (FR-19).

Subclass `Strategy` and override the callbacks you need; callbacks that are
not overridden are never called, and book and trade events are filtered in
Rust by `subscriptions` before any Python code runs. The same class runs in
backtest, paper and testnet modes.
"""

from __future__ import annotations

import importlib
import importlib.util
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar

from edbt._edbt import BookView, Context
from edbt.types import ConfigError, Fill, OrderUpdate, RiskEvent, Subscriptions, Trade

if TYPE_CHECKING:
    from edbt.config import Config

__all__ = ["BookView", "Context", "Strategy", "load_strategy"]


class Strategy:
    """Base class of Python strategies. All callbacks run in local time."""

    subscriptions: ClassVar[Subscriptions] = Subscriptions()

    def __init__(self, **params: Any) -> None:
        """Receive `[strategy.params]` as keyword arguments."""

    def on_start(self, ctx: Context) -> None:
        """Called once at `run.start`."""

    def on_book(self, ctx: Context, book: BookView) -> None:
        """A book update (filtered by `subscriptions.book`)."""

    def on_trade(self, ctx: Context, trade: Trade) -> None:
        """A trade (when `subscriptions.trades`)."""

    def on_order_update(self, ctx: Context, update: OrderUpdate) -> None:
        """An order changed state (after `on_fill` for fills)."""

    def on_fill(self, ctx: Context, fill: Fill) -> None:
        """A fill; the ledger already includes it."""

    def on_timer(self, ctx: Context, timer_id: int) -> None:
        """A timer set with `ctx.set_timer` fired."""

    def on_risk_event(self, ctx: Context, event: RiskEvent) -> None:
        """A risk rejection, warning or kill-switch event."""

    def on_stop(self, ctx: Context) -> None:
        """Called once at the end of the run."""


def load_strategy(config: Config) -> Strategy:
    """Instantiate the Python strategy named by `[strategy]` (`path`, `class`).

    `path` is a `.py` file or an importable module; `class` defaults to the
    only `Strategy` subclass defined there.
    """
    s = config.to_dict()["strategy"]
    path, cls_name, params = s["path"], s.get("class"), s.get("params", {})
    if path.endswith(".py"):
        file = Path(path)
        spec = importlib.util.spec_from_file_location(f"edbt_user_{file.stem}", file)
        if spec is None or spec.loader is None:
            raise ConfigError(f"cannot load strategy file {path}")
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        try:
            spec.loader.exec_module(module)
        except OSError as e:
            raise ConfigError(f"cannot load strategy file {path}: {e}") from e
    else:
        try:
            module = importlib.import_module(path)
        except ImportError as e:
            raise ConfigError(f"cannot import strategy module {path}: {e}") from e
    candidate: object
    if cls_name is None:
        found = [
            v
            for v in vars(module).values()
            if isinstance(v, type) and issubclass(v, Strategy) and v is not Strategy
        ]
        if len(found) != 1:
            raise ConfigError(f"{path}: set strategy.class (found {len(found)} Strategy classes)")
        candidate = found[0]
    else:
        candidate = getattr(module, cls_name, None)
    if not (isinstance(candidate, type) and issubclass(candidate, Strategy)):
        raise ConfigError(f"{path}: {cls_name} is not an edbt.Strategy subclass")
    instance: Strategy = candidate(**params)
    return instance
