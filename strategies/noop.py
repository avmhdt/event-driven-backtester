"""noop in Python: receives every book update and never trades."""

from __future__ import annotations

from edbt import Strategy, Subscriptions


class Noop(Strategy):
    """Receives events, does nothing."""

    subscriptions = Subscriptions(book="every_update", trades=True)
