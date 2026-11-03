"""mm_simple in Python: quotes the touch with an offset and inventory skew.

Operation for operation the same as the Rust `mm_simple`
(`crates/edbt-strategies/src/mm_simple.rs`), so both produce identical
orders on the same data (AC-W-07).
"""

from __future__ import annotations

import math

from edbt import BookView, Context, Side, Strategy, Subscriptions, TimeInForce


class MarketMaker(Strategy):
    """Post-only quotes at `best +- (half_spread_ticks - 1) - skew`."""

    subscriptions = Subscriptions(book="bbo_change", trades=False)

    def __init__(
        self,
        half_spread_ticks: int = 1,
        order_qty: float = 0.001,
        max_inventory: float = 0.01,
        skew_ticks_per_unit: float = 100.0,
        refresh_ns: int = 100_000_000,
        **_: object,
    ) -> None:
        """Quote parameters, as in `[strategy.params]`."""
        self.half_spread_ticks = half_spread_ticks
        self.order_qty = order_qty
        self.max_inventory = max_inventory
        self.skew_ticks_per_unit = skew_ticks_per_unit
        self.refresh_ns = refresh_ns
        self._next_refresh: dict[str, int] = {}

    def on_book(self, ctx: Context, book: BookView) -> None:
        """Re-quote at most once per `refresh_ns` on a valid book."""
        if not book.valid:
            return
        now = ctx.now()
        if now < self._next_refresh.get(book.symbol, -(2**63)):
            return
        bids, asks = book.depth_ticks(1)
        if len(bids) == 0 or len(asks) == 0:
            return
        self._next_refresh[book.symbol] = now + self.refresh_ns
        qty = math.floor(self.order_qty / book.lot_size + 0.5)
        if qty <= 0:
            return
        pos = float(ctx.position(book.symbol).qty)
        skew = math.floor(self.skew_ticks_per_unit * pos + 0.5)
        h = self.half_spread_ticks - 1
        bid_px = int(bids[0, 0]) - h - skew
        ask_px = int(asks[0, 0]) + h - skew
        ctx.cancel_all(book.symbol)
        if pos < self.max_inventory:
            ctx.submit_limit_ticks(book.symbol, Side.BUY, bid_px, qty, tif=TimeInForce.GTX)
        if pos > -self.max_inventory:
            ctx.submit_limit_ticks(book.symbol, Side.SELL, ask_px, qty, tif=TimeInForce.GTX)
