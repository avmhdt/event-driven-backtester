"""Settings shared with the Rust side (``.env`` and environment, spec section 3).

Rust (dotenvy) and Python (pydantic-settings) read the same variable names.
Secrets are never read here: testnet keys are loaded only by the Rust live
adapter, from ``BINANCE_TESTNET_*``.
"""

from __future__ import annotations

import os
from pathlib import Path

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


def _expand(value: str | Path) -> Path:
    return Path(os.path.expandvars(os.path.expanduser(str(value))))


class Settings(BaseSettings):
    """Path and logging settings from ``.env`` and the environment."""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    data_root: Path = Path("~/data/event-driven-backtester")
    edbt_data_dir: Path | None = None
    edbt_external_data_roots: str = ""
    edbt_results_dir: Path = Path("./results")
    rust_log: str = "info"
    edbt_log_format: str = "text"

    @field_validator("data_root", "edbt_results_dir", mode="after")
    @classmethod
    def _expand_path(cls, v: Path) -> Path:
        return _expand(v)

    @field_validator("edbt_data_dir", mode="before")
    @classmethod
    def _empty_is_none(cls, v: object) -> object:
        return None if v in ("", None) else _expand(str(v))

    @property
    def data_dir(self) -> Path:
        """Root of raw/, interim/, processed/ and canonical datasets."""
        return self.edbt_data_dir or self.data_root

    @property
    def external_roots(self) -> list[Path]:
        """Read-only sibling-repo data roots from ``EDBT_EXTERNAL_DATA_ROOTS``."""
        return [_expand(p.strip()) for p in self.edbt_external_data_roots.split(":") if p.strip()]
