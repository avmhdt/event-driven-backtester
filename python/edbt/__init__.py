"""Event-driven backtester with realistic execution.

The Rust core (``edbt._edbt``) replays canonical market data through one
deterministic engine; this package is its Python API. The full surface of
spec section 10 arrives with API 1.0.0 in M4.
"""

from edbt._edbt import __version__
from edbt.config import Settings

API_VERSION = "0.1.0"
"""Semantic version of the Python API contract (NFR-14)."""

__all__ = ["API_VERSION", "Settings", "__version__"]
