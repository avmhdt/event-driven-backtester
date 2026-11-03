# Fill rules

Every rule below is enforced by the simulated exchange
(`crates/edbt-exchange/src/sim_gateway.rs`) and has a test of the same name in
`crates/edbt-exchange/tests/fill_rules.rs` (AC-W-03).
`scripts/check_fill_rule_coverage.py` checks that each id listed here has a
test and that each test is listed here. Hand-computed end-to-end cases are in
`tests/fixtures/scenarios/*.toml`.

Conventions: the exchange works in exchange time. An order reaches it
`entry` latency after it was sent; every response reaches the strategy
`response` latency later, in the order produced. Our orders are not inserted
into the replayed book; their queue position is modelled.

## Aggressive orders (FR-08)

| Rule id | Rule |
|---|---|
| `crossing_limit_fills_at_best_and_walks_levels` | A limit order that crosses takes the visible opposite depth level by level, best price first, at each level's price, up to its limit price. |
| `crossing_partial_then_rests_remainder` | A GTC order that crosses partly fills what is available up to its limit and rests the remainder at its limit price, where it queues behind the displayed size at that price. |
| `ioc_remainder_cancelled` | An IOC order fills what it can at once; the remainder expires (`IOC_REMAINDER`). |
| `fok_rejected_if_not_fully_fillable` | A FOK order is rejected (`FOK_NOT_FILLABLE`) unless the visible depth up to its limit covers its whole size; otherwise it fills completely at once. |
| `market_order_walks_book_taker_fee` | A market order walks the opposite side without a price limit, pays the taker fee on every fill, and its unfilled remainder expires (`NO_LIQUIDITY`). |
| `post_only_rejected_when_crossing` | A post-only (GTX) order that would take any liquidity on arrival is rejected (`POST_ONLY_WOULD_CROSS`). |
| `consumed_liquidity_not_refilled_until_level_update` | Liquidity we took at a level is subtracted from that level's displayed size until the feed next updates the level (the consumed-liquidity shadow, `exchange.consume_liquidity`). |
| `self_trade_prevention_cancel_newest` | An incoming order that would trade with one of our own resting orders is expired (`STP`) under `cancel_newest`; under `cancel_oldest` the resting order is cancelled instead; `none` allows it. |

## Timing (FR-05)

| Rule id | Rule |
|---|---|
| `order_not_fillable_before_arrival` | An order cannot fill before `ts_send + entry`: market events before its arrival do not touch it. |
| `cancel_in_flight_can_still_fill` | A cancel takes effect only when it reaches the exchange; a fill in between stands and the cancel is refused (`UNKNOWN_ORDER`). |
| `modify_is_cancel_replace_and_loses_priority` | A modify is a cancel plus a new order with a new client order id; the new order queues behind the displayed size at its arrival. |

## Passive orders and queue position (FR-09, FR-10, FR-11)

| Rule id | Rule |
|---|---|
| `passive_fill_requires_queue_ahead_exhausted` | A resting order fills from trades at its price only after the quantity ahead of it is used up. |
| `passive_partial_fill_when_trade_smaller_than_leaves` | A trade larger than the queue ahead fills us with the excess, up to our remaining size; several trades can fill one order. |
| `trade_through_fills_resting_order` | A trade strictly through our price (a sell below our bid, a buy above our ask) fills our whole remainder at our price (`queue.trade_through_fills`). |
| `book_cross_fills_resting_order_at_own_price` | When the opposite best moves onto or through our price, our whole remainder fills at our price (`exchange.fill_on_book_cross`). Liquidity we already consumed does not count. |
| `unknown_side_trade_does_not_advance_queue` | A trade without an aggressor side is classified against the exchange BBO (at or below the bid: seller-initiated; at or above the ask: buyer-initiated, DQ-10); a trade that stays unknown neither advances a queue nor fills. |
| `level_increase_goes_behind` | An increase of the displayed size at our price joins behind us. |
| `l2_risk_averse_cancel_does_not_advance` | Under `risk_averse`, decreases not explained by trades never advance us; the quantity ahead is only capped at the level size. |
| `l2_prob_power_cancel_advances_expected_fraction` | Under `prob_power(n)`, of `c` lots cancelled at our level, `c * a^n / (a^n + b^n)` are taken from the `a` lots ahead (`b` behind). |
| `l2_prob_log_cancel_advances_expected_fraction` | Under `prob_log`, the share is `ln(1 + a) / (ln(1 + a) + ln(1 + b))`. |
| `l3_cancel_ahead_advances_queue` | Under `l3_fifo`, executions, cancels and deletes of orders that were ahead of us at arrival advance us exactly; once nothing is ahead, an execution at our price fills us. |
| `l3_cancel_behind_does_not_advance` | Events on orders that joined after us do not advance us. |
| `l3_replace_loses_priority` | A replace removes the old order (advancing us if it was ahead) and its replacement joins behind us. |
| `l3_partial_cancel_keeps_priority` | A partial cancel keeps the order's place ahead of us; only the cancelled quantity advances us. |
| `no_fill_while_book_invalid` | While the exchange book is invalid (sequence gap, crossed book, no snapshot yet; DQ-04, DQ-06, DQ-11) resting orders are not filled and new orders are rejected (`BOOK_INVALID`). |

## Fees (FR-12)

| Rule id | Rule |
|---|---|
| `maker_rebate_is_negative_fee` | A passive fill pays the maker rate; a negative maker rate is a rebate, recorded as a negative fee. |
| `taker_fee_on_aggressive_fill` | An aggressive fill pays the taker rate on its notional, rounded half-even to the nano-quote. |
| `fees_use_point_in_time_schedule` | The rate is the one in force at the fill's exchange time (DQ-07). |

## The optimistic reference (FR-13)

`queue.model = "none"` fills the whole order on any trade at or through its
price, ignoring the queue. It exists for the inflation study and is covered
by `crates/edbt-exchange/tests/queue_models.rs`.
