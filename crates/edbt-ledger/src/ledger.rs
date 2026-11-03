//! Per-symbol positions, integer cost basis, cash, realized and unrealized
//! P&L, fees (FR-16).
//!
//! Sign conventions: a buy is `side = +1`. Cash falls by the signed notional
//! plus the fee. The cost basis is the signed nano-quote value of the open
//! position at entry. On a (partial) close the removed basis is
//! `basis * closed / |position|` rounded half-even and the remainder stays in
//! the basis, so every identity in [`crate::recon`] holds exactly.

use crate::fixed::{NanoQuote, div_round_half_even};

/// Ledger state of one symbol.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Position {
    /// Value of one lot at a price of one tick, nano-quote.
    pub nq_per_tick_lot: i128,
    /// Signed position, lots.
    pub position: i64,
    /// Signed entry value of the open position, nano-quote.
    pub basis: NanoQuote,
    /// Realized P&L before fees, nano-quote.
    pub realized: NanoQuote,
    /// Fees paid (negative for net rebates), nano-quote.
    pub fees: NanoQuote,
    /// Rebates received (positive), nano-quote.
    pub rebates: NanoQuote,
    /// Mark price, ticks.
    pub mark: Option<i64>,
    /// Independent running sum of fill cash flows before fees,
    /// `sum(-side * price * qty * k)`; used only by reconciliation.
    pub flow: NanoQuote,
    /// Traded notional, nano-quote (absolute).
    pub traded: NanoQuote,
}

impl Position {
    /// Mark value of the position, nano-quote. Unmarked positions are valued
    /// at their basis.
    #[must_use]
    pub fn market_value(&self) -> NanoQuote {
        match self.mark {
            Some(m) => i128::from(self.position) * i128::from(m) * self.nq_per_tick_lot,
            None => self.basis,
        }
    }

    /// Unrealized P&L, nano-quote.
    #[must_use]
    pub fn unrealized(&self) -> NanoQuote {
        self.market_value() - self.basis
    }

    /// Average entry price in ticks (float, derived), if a position is open.
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn avg_entry_ticks(&self) -> Option<f64> {
        (self.position != 0).then(|| self.basis as f64 / (self.position as f64 * self.nq_per_tick_lot as f64))
    }
}

/// A fill as the ledger sees it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LedgerFill {
    /// Symbol index.
    pub symbol: usize,
    /// `+1` buy, `-1` sell.
    pub side: i8,
    /// Price, ticks.
    pub price: i64,
    /// Quantity, lots (positive).
    pub qty: i64,
    /// Fee, nano-quote (negative = rebate).
    pub fee: NanoQuote,
}

/// Errors from ledger updates.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum LedgerError {
    /// Unknown symbol index.
    #[error("unknown symbol index {0}")]
    UnknownSymbol(usize),
    /// Non-positive quantity or invalid side.
    #[error("invalid fill: {0}")]
    InvalidFill(String),
}

/// The portfolio ledger.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Ledger {
    initial_cash: NanoQuote,
    cash: NanoQuote,
    symbols: Vec<Position>,
}

impl Ledger {
    /// A ledger with `initial_cash` and one entry per symbol, given the value
    /// of one lot at one tick for each.
    #[must_use]
    pub fn new(initial_cash: NanoQuote, nq_per_tick_lot: &[i128]) -> Self {
        Self {
            initial_cash,
            cash: initial_cash,
            symbols: nq_per_tick_lot.iter().map(|k| Position { nq_per_tick_lot: *k, ..Position::default() }).collect(),
        }
    }

    /// Apply a fill.
    ///
    /// # Errors
    /// Unknown symbol, non-positive quantity or a side other than +-1.
    pub fn apply_fill(&mut self, f: &LedgerFill) -> Result<(), LedgerError> {
        if f.qty <= 0 || (f.side != 1 && f.side != -1) {
            return Err(LedgerError::InvalidFill(format!("{f:?}")));
        }
        let p = self.symbols.get_mut(f.symbol).ok_or(LedgerError::UnknownSymbol(f.symbol))?;
        let k = p.nq_per_tick_lot;
        let s = i128::from(f.side);
        let notional = i128::from(f.price) * i128::from(f.qty) * k;
        self.cash -= s * notional + f.fee;
        p.flow -= s * notional;
        p.traded += notional.abs();
        p.fees += f.fee;
        if f.fee < 0 {
            p.rebates -= f.fee;
        }
        let mut rem = f.qty;
        if p.position != 0 && p.position.signum() != i64::from(f.side) {
            let close = rem.min(p.position.abs());
            let removed = div_round_half_even(p.basis * i128::from(close), i128::from(p.position.abs()));
            p.realized += -s * i128::from(f.price) * i128::from(close) * k - removed;
            p.basis -= removed;
            p.position += i64::from(f.side) * close;
            rem -= close;
            if p.position == 0 {
                // Any rounding remainder is realized when the position is flat.
                p.realized -= p.basis;
                p.basis = 0;
            }
        }
        if rem > 0 {
            p.position += i64::from(f.side) * rem;
            p.basis += s * i128::from(f.price) * i128::from(rem) * k;
        }
        if p.mark.is_none() {
            p.mark = Some(f.price);
        }
        Ok(())
    }

    /// Set a symbol's mark price in ticks.
    ///
    /// # Errors
    /// Unknown symbol.
    pub fn set_mark(&mut self, symbol: usize, mark: i64) -> Result<(), LedgerError> {
        self.symbols.get_mut(symbol).ok_or(LedgerError::UnknownSymbol(symbol))?.mark = Some(mark);
        Ok(())
    }

    /// Initial cash.
    #[must_use]
    pub fn initial_cash(&self) -> NanoQuote {
        self.initial_cash
    }

    /// Cash.
    #[must_use]
    pub fn cash(&self) -> NanoQuote {
        self.cash
    }

    /// Per-symbol state.
    #[must_use]
    pub fn positions(&self) -> &[Position] {
        &self.symbols
    }

    /// One symbol's state.
    #[must_use]
    pub fn position(&self, symbol: usize) -> Option<&Position> {
        self.symbols.get(symbol)
    }

    /// Cash plus the mark value of every position.
    #[must_use]
    pub fn equity(&self) -> NanoQuote {
        self.cash + self.symbols.iter().map(Position::market_value).sum::<i128>()
    }

    /// Total realized P&L before fees.
    #[must_use]
    pub fn realized(&self) -> NanoQuote {
        self.symbols.iter().map(|p| p.realized).sum()
    }

    /// Total unrealized P&L.
    #[must_use]
    pub fn unrealized(&self) -> NanoQuote {
        self.symbols.iter().map(Position::unrealized).sum()
    }

    /// Total fees (net of rebates).
    #[must_use]
    pub fn fees(&self) -> NanoQuote {
        self.symbols.iter().map(|p| p.fees).sum()
    }

    /// Total rebates received.
    #[must_use]
    pub fn rebates(&self) -> NanoQuote {
        self.symbols.iter().map(|p| p.rebates).sum()
    }

    /// P&L after fees: equity minus initial cash.
    #[must_use]
    pub fn pnl(&self) -> NanoQuote {
        self.equity() - self.initial_cash
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixed::NQ_PER_UNIT;

    fn fill(side: i8, price: i64, qty: i64, fee: i128) -> LedgerFill {
        LedgerFill { symbol: 0, side, price, qty, fee }
    }

    #[test]
    fn round_trip_trade() {
        // BTCUSDT-like: tick 0.1, lot 0.001 -> k = 1e-4 quote = 100_000 nq.
        let mut l = Ledger::new(1_000 * NQ_PER_UNIT, &[100_000]);
        l.apply_fill(&fill(1, 1_000_000, 10, 50)).unwrap(); // buy 0.01 @ 100000.0
        assert_eq!(l.position(0).unwrap().position, 10);
        l.set_mark(0, 1_000_100).unwrap(); // +10.0
        assert_eq!(l.unrealized(), 10 * 100 * 100_000);
        l.apply_fill(&fill(-1, 1_000_200, 4, 20)).unwrap(); // sell 0.004 @ +20.0
        let p = *l.position(0).unwrap();
        assert_eq!(p.position, 6);
        assert_eq!(p.realized, 4 * 200 * 100_000);
        assert_eq!(p.fees, 70);
        l.apply_fill(&fill(-1, 999_900, 16, 0)).unwrap(); // flip to short 10
        let p = *l.position(0).unwrap();
        assert_eq!(p.position, -10);
        assert_eq!(p.basis, -10 * 999_900 * 100_000);
        assert_eq!(p.realized, 4 * 200 * 100_000 + 6 * -100 * 100_000);
        assert_eq!(l.equity(), l.initial_cash() + l.realized() + l.unrealized() - l.fees());
    }

    #[test]
    fn partial_close_rounds_half_even_and_flat_realizes_remainder() {
        let mut l = Ledger::new(0, &[1]);
        l.apply_fill(&fill(1, 1, 1, 0)).unwrap();
        l.apply_fill(&fill(1, 2, 2, 0)).unwrap(); // basis 5 for 3 lots
        l.apply_fill(&fill(-1, 2, 1, 0)).unwrap(); // removes round(5/3)=2
        assert_eq!(l.position(0).unwrap().basis, 3);
        assert_eq!(l.position(0).unwrap().realized, 0);
        l.apply_fill(&fill(-1, 2, 2, 0)).unwrap(); // flat
        let p = *l.position(0).unwrap();
        assert_eq!((p.position, p.basis, p.realized), (0, 0, 1));
        assert_eq!(l.pnl(), 1);
    }

    #[test]
    fn rejects_bad_fills() {
        let mut l = Ledger::new(0, &[1]);
        assert!(l.apply_fill(&fill(1, 1, 0, 0)).is_err());
        assert!(l.apply_fill(&fill(0, 1, 1, 0)).is_err());
        assert!(l.apply_fill(&LedgerFill { symbol: 3, ..fill(1, 1, 1, 0) }).is_err());
        assert!(l.set_mark(3, 1).is_err());
        l.apply_fill(&fill(1, 10, 2, -3)).unwrap();
        assert_eq!(l.rebates(), 3);
        assert_eq!(l.fees(), -3);
        assert_eq!(l.position(0).unwrap().avg_entry_ticks(), Some(10.0));
        assert_eq!(l.positions().len(), 1);
        assert_eq!(l.cash(), -17);
    }
}
