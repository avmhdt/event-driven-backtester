//! Probabilistic L2 queue models `prob_power(n)` with `f(x) = x^n` and
//! `prob_log` with `f(x) = ln(1 + x)` (FR-09). The share of a cancel assumed
//! ahead of us is `f(ahead) / (f(ahead) + f(behind))`; the expected share is
//! applied, so the models are deterministic. The functional forms follow the
//! probabilistic queue models described in the hftbacktest documentation.

use super::QueueModel;

fn share(fa: f64, fb: f64) -> f64 {
    let total = fa + fb;
    if total > 0.0 && total.is_finite() { fa / total } else { 0.0 }
}

/// `f(x) = x^n`.
#[derive(Clone, Copy, Debug)]
pub struct ProbPower {
    /// Power `n`.
    pub n: f64,
}

impl QueueModel for ProbPower {
    fn cancel_share(&self, ahead: f64, behind: f64) -> f64 {
        share(libm::pow(ahead, self.n), libm::pow(behind, self.n))
    }
}

/// `f(x) = ln(1 + x)`.
#[derive(Clone, Copy, Debug, Default)]
pub struct ProbLog;

impl QueueModel for ProbLog {
    fn cancel_share(&self, ahead: f64, behind: f64) -> f64 {
        share(libm::log1p(ahead), libm::log1p(behind))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shares() {
        let p = ProbPower { n: 2.0 };
        assert!((p.cancel_share(1.0, 1.0) - 0.5).abs() < 1e-12);
        assert!((p.cancel_share(3.0, 1.0) - 0.9).abs() < 1e-12);
        assert!(p.cancel_share(0.0, 0.0).abs() < 1e-12);
        assert!((ProbLog.cancel_share(1.0, 0.0) - 1.0).abs() < 1e-12);
        assert!((ProbLog.cancel_share(4.0, 4.0) - 0.5).abs() < 1e-12);
    }
}
