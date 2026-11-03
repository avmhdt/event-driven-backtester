# Queue models

How the simulator decides when a resting order of ours is filled. Our orders
are never inserted into the replayed book, so their place in the queue is a
model (L2 data) or an exact reconstruction (L3 data). Configured by
`[queue]` (FR-09, FR-10, FR-13); code in `crates/edbt-exchange/src/queue/`.

## L2 data: the quantity ahead

On arrival at the exchange, the quantity ahead of our order is the displayed
size at our price. It is kept in micro-lots, so fractional shares of cancels
can be applied exactly in integers. Then:

- **Trades at our price** (by the opposite aggressor) take from the quantity
  ahead first; any excess fills us, up to our remaining size.
- **Increases** of the displayed size at our price join behind us.
- **Decreases not explained by trades** are cancels. Trades seen at our price
  since the last update of the level are subtracted from the decrease first.
  Of the remaining cancelled quantity `c`, the share assumed to be ahead of
  us is `f(a) / (f(a) + f(b))`, where `a` is the quantity ahead and `b` the
  quantity behind (the level size before the update minus `a`).
- The quantity ahead never exceeds the displayed level size and never
  increases (property test `queue_ahead_never_negative_never_increases`).

| `queue.model` | `f(x)` | Meaning |
|---|---|---|
| `risk_averse` | 0 | Cancels are always behind us. The most conservative. |
| `prob_power` | `x^n` (`queue.power`, default 2) | Cancels are more likely to come from the larger side. |
| `prob_log` | `ln(1 + x)` | A flatter version of the same idea. |
| `none` | — | Fill on touch: any trade at or through our price fills the whole order (the optimistic reference for the inflation study, FR-13). |

The expected share is applied, not a random draw, so these models are
deterministic. The functional forms follow the probabilistic queue models in
the hftbacktest documentation; no code was taken from it. Which L2 model is
closest to the truth is measured against L3 data in M6 (`queue_validation`).

Two further fills apply to every L2 model:

- **Trade through** (`queue.trade_through_fills`, default on): a trade
  strictly beyond our price (a sell below our bid) means every order at our
  price was taken, so our whole remainder fills at our price.
- **Book cross** (`exchange.fill_on_book_cross`, default on): when the
  opposite best moves onto or through our price, we would have been hit, so
  our whole remainder fills at our price.

Updates are applied per L2 batch: the level size before and after a batch is
compared once the batch is complete, which also covers snapshot batches.

## L3 data: `l3_fifo`

With order-level data the queue is exact. On arrival our order is placed
behind every order present at its price level. Executions, cancels and
deletes of those orders advance us by exactly their quantity; a partial
cancel keeps the order ahead of us; a replace takes the order out of our
queue and its replacement joins behind us. Once nothing is ahead, an
execution at our price reaches us and fills us. Events on orders behind us
never advance us.

## When nothing fills

- While the exchange book is invalid (a sequence gap, a crossed batch, or no
  snapshot yet) resting orders are not filled (DQ-04).
- A trade without an aggressor side that cannot be classified against the
  exchange BBO (it printed inside the spread) is ignored (DQ-10).
