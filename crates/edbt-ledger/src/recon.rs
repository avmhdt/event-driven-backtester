//! Reconciliation identities checked after every event (FR-16, AC-W-02).
//!
//! 1. `cash + sum(position * mark * k) == initial_cash + realized + unrealized - fees`
//! 2. `realized + unrealized == sum(fill cash flows) + sum(position * mark * k)`,
//!    where the fill cash flows are an independent running sum kept beside the
//!    cost-basis accounting.
//!
//! Both are integer equalities in nano-quote.

use crate::fixed::NanoQuote;
use crate::ledger::Ledger;

/// A failed identity, with both sides.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("reconciliation violation ({identity}): {left} != {right}")]
pub struct Violation {
    /// Which identity failed.
    pub identity: &'static str,
    /// Left-hand side, nano-quote.
    pub left: NanoQuote,
    /// Right-hand side, nano-quote.
    pub right: NanoQuote,
}

/// Check both identities.
///
/// # Errors
/// The first identity that does not hold.
pub fn check(l: &Ledger) -> Result<(), Violation> {
    let mv: NanoQuote = l.positions().iter().map(crate::ledger::Position::market_value).sum();
    let left = l.cash() + mv;
    let right = l.initial_cash() + l.realized() + l.unrealized() - l.fees();
    if left != right {
        return Err(Violation { identity: "cash+marked == initial+pnl-fees", left, right });
    }
    let flow: NanoQuote = l.positions().iter().map(|p| p.flow).sum();
    let left = l.realized() + l.unrealized();
    let right = flow + mv;
    if left != right {
        return Err(Violation { identity: "realized+unrealized == fill flows+marked", left, right });
    }
    Ok(())
}

/// How a run reacts to a violation (`ledger.on_violation`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OnViolation {
    /// Abort the run.
    Abort,
    /// Count and continue.
    Count,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ledger::LedgerFill;

    #[test]
    fn holds_on_a_simple_sequence() {
        let mut l = Ledger::new(5, &[3, 7]);
        check(&l).unwrap();
        l.apply_fill(&LedgerFill { symbol: 0, side: 1, price: 11, qty: 3, fee: 2 }).unwrap();
        l.apply_fill(&LedgerFill { symbol: 1, side: -1, price: 5, qty: 2, fee: -1 }).unwrap();
        l.set_mark(0, 13).unwrap();
        check(&l).unwrap();
        let v = Violation { identity: "x", left: 1, right: 2 };
        assert!(v.to_string().contains("1 != 2"));
        assert_ne!(OnViolation::Abort, OnViolation::Count);
    }
}
