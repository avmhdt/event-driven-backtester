"""taker_imbalance in Python: crosses the spread on order-book imbalance.

The same rules as the Rust `taker_imbalance`.
"""

from __future__ import annotations

import math

from edbt import BookView, Context, Side, Strategy, Subscriptions


class TakerImbalance(Strategy):
    """Market orders when the top-of-book size imbalance passes a threshold."""

    subscriptions = Subscriptions(book="every_update", trades=False)

    def __init__(
        self,
        levels: int = 5,
        threshold: float = 0.6,
        order_qty: float = 0.001,
        max_inventory: float = 0.01,
        cooldown_ns: int = 1_000_000_000,
        **_: object,
    ) -> None:
        """Parameters, as in `[strategy.params]`."""
        self.levels = levels
        self.threshold = threshold
        self.order_qty = order_qty
        self.max_inventory = max_inventory
        self.cooldown_ns = cooldown_ns
        self._next: dict[str, int] = {}

    def on_book(self, ctx: Context, book: BookView) -> None:
        """Buy on bid-heavy books, sell on ask-heavy books."""
        if not book.valid:
            return
        now = ctx.now()
        if now < self._next.get(book.symbol, -(2**63)):
            return
        bids, asks = book.depth_ticks(self.levels)
        b, a = int(bids[:, 1].sum()), int(asks[:, 1].sum())
        if a + b == 0:
            return
        imb = (b - a) / (b + a)
        qty = math.floor(self.order_qty / book.lot_size + 0.5)
        pos = float(ctx.position(book.symbol).qty)
        if imb > self.threshold and pos < self.max_inventory:
            side = Side.BUY
        elif imb < -self.threshold and pos > -self.max_inventory:
            side = Side.SELL
        else:
            return
        if qty <= 0:
            return
        self._next[book.symbol] = now + self.cooldown_ns
        ctx.submit_market(book.symbol, side, qty * book.lot_size, tag="imbalance")
