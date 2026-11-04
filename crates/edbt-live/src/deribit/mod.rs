//! Deribit testnet protocol (D-02 fallback venue, owner ruling 2026-11-03).
//!
//! Venue facts come from the Deribit API docs read on 2026-11-03 (JSON-RPC
//! reference, OpenAPI and AsyncAPI specs) and are recorded in
//! `docs/decisions.md`: testnet REST `https://test.deribit.com/api/v2` and
//! WebSocket `wss://test.deribit.com/ws/api/v2`; OAuth `public/auth` with
//! `client_credentials`; `book.<instrument>.100ms` (first notification a
//! full book, then changes with `change_id` / `prev_change_id`),
//! `trades.<instrument>.100ms`, `quote.<instrument>`, and the private
//! `user.orders.<instrument>.raw` / `user.trades.<instrument>.raw`;
//! `public/set_heartbeat` with `test_request` answered by `public/test`;
//! error `11054` (`post_only_reject`) for a post-only order sent with
//! `reject_post_only`.

pub mod rest;
pub mod streams;
pub mod translate;
pub mod types;
