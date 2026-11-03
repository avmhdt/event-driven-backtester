"""Run configuration (`edbt.Config`) and environment settings (`edbt.Settings`).

`Config` holds a config with every default filled in. Parsing, defaults and
validation happen in Rust, so a config means the same thing from Python and
from the `edbt` CLI, and `config_hash()` matches the run id's hash.

`Settings` reads the path and logging variables of `.env` (spec section 3)
with the same names as the Rust side. Secrets are never read here: testnet
keys are loaded only by the Rust live adapter, from `BINANCE_TESTNET_*`.
"""

from __future__ import annotations

import copy
import datetime as dt
import json
import os
import tomllib
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from edbt._edbt import resolve_config
from edbt.types import ConfigError, Mode

__all__ = ["Config", "Settings"]


def _expand(value: str | Path) -> Path:
    return Path(os.path.expandvars(os.path.expanduser(str(value))))


def _json_default(v: object) -> str:
    if isinstance(v, dt.datetime | dt.date | dt.time):
        return v.isoformat().replace("+00:00", "Z")
    raise TypeError(f"cannot serialise {type(v).__name__} in a config")


def _set_dotted(d: dict[str, Any], key: str, value: Any) -> None:
    parts = key.split(".")
    node = d
    for p in parts[:-1]:
        nxt = node.setdefault(p, {})
        if not isinstance(nxt, dict):
            raise ConfigError(f"{p} in {key!r} is not a table")
        node = nxt
    node[parts[-1]] = value


class Config:
    """A resolved run configuration (spec section 10)."""

    __slots__ = ("_data", "_hash", "_toml")

    def __init__(self, data: Mapping[str, Any]) -> None:
        """Resolve `data` (a TOML-shaped mapping); see `from_dict`."""
        try:
            text = json.dumps(dict(data), default=_json_default)
        except TypeError as e:
            raise ConfigError(str(e)) from e
        toml_text, resolved, digest = resolve_config(text)
        self._data: dict[str, Any] = json.loads(resolved)
        self._toml = toml_text
        self._hash = digest

    @classmethod
    def from_toml(cls, path: str | Path) -> Config:
        """Load a TOML config file."""
        try:
            with Path(path).open("rb") as f:
                data = tomllib.load(f)
        except (OSError, tomllib.TOMLDecodeError) as e:
            raise ConfigError(f"{path}: {e}") from e
        return cls(data)

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]) -> Config:
        """Build from a mapping shaped like the TOML file."""
        return cls(d)

    def to_dict(self) -> dict[str, Any]:
        """The resolved config as a new dict."""
        return copy.deepcopy(self._data)

    def to_toml(self) -> str:
        """The resolved config as TOML (the content of `config.resolved.toml`)."""
        return self._toml

    def with_overrides(self, overrides: Mapping[str, Any]) -> Config:
        """A copy with dotted keys replaced, e.g. `{"queue.model": "none"}`."""
        d = self.to_dict()
        for k, v in overrides.items():
            _set_dotted(d, k, v)
        return Config(d)

    def config_hash(self) -> str:
        """BLAKE3 of the resolved TOML."""
        return self._hash

    @property
    def mode(self) -> Mode:
        """Run mode."""
        return Mode(self._data["run"]["mode"])

    @property
    def seed(self) -> int:
        """Run seed."""
        return int(self._data["run"]["seed"])

    def __repr__(self) -> str:
        """Short form with the hash."""
        return f"Config({self._data['run'].get('name', '')!r}, hash={self._hash[:8]})"


class Settings(BaseSettings):
    """Path and logging settings from `.env` and the environment."""

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
        """Read-only sibling-repo data roots from `EDBT_EXTERNAL_DATA_ROOTS`."""
        return [_expand(p.strip()) for p in self.edbt_external_data_roots.split(":") if p.strip()]
