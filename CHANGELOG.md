# Changelog

Python API changes are flagged **[API]** for `market-making-sim`, which codes
against `edbt` (`docs/python_api.md`). `edbt.API_VERSION` follows semver
(NFR-14); 1.0.0 is the first stable contract (M4).

## Unreleased

### M1: scaffolding, CI and data-access smoke test

- Cargo workspace with every crate of spec section 6; `edbt` CLI skeleton.
- Canonical Parquet readers and writers for `trades`, `book_l2` and
  `book_l3` with schema validation (DQ-01), point-in-time instruments
  (DQ-07) and tick/lot conversion (DQ-05).
- `edbt data synth` (seeded synthetic L2/L3 market, fixture CSV conversion)
  and `edbt data validate` (DQ-01, DQ-02, DQ-03, DQ-05).
- Testnet host guard and secret redaction (AC-W-12).
- **[API]** Python package `edbt` (distribution `edbt-sim`) with
  `API_VERSION = "0.1.0"` (pre-contract), `Settings`, `edbt-ingest recordings`.
