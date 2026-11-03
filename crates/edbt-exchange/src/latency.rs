//! Feed and order-path latency models (FR-04, FR-05).
//!
//! Feed latency is sampled once per venue message (an L2 batch, a trade, an
//! L3 event) so the rows of one batch arrive together; the scheduler then
//! makes local arrival times non-decreasing per stream.

use edbt_core::Nanos;
use edbt_core::config::{FeedLatencyCfg, FeedModel};
use edbt_core::event::MarketEvent;
use edbt_core::rng;
use rand_chacha::ChaCha8Rng;

/// Latency errors.
#[derive(Debug, thiserror::Error)]
#[error("latency model: {0}")]
pub struct LatencyError(pub String);

/// Feed latency: exchange time to local arrival.
pub trait FeedLatency {
    /// Latency of `ev`, ns (non-negative).
    fn sample(&mut self, ev: &MarketEvent, rng: &mut ChaCha8Rng) -> Nanos;
}

/// `ts_recv - ts_exchange` from the data, clamped below at `min_ns`.
#[derive(Clone, Copy, Debug)]
pub struct RecordedFeed {
    /// Lower clamp, ns.
    pub min_ns: Nanos,
}

impl FeedLatency for RecordedFeed {
    fn sample(&mut self, ev: &MarketEvent, _rng: &mut ChaCha8Rng) -> Nanos {
        (ev.ts_recv - ev.ts_exchange).max(self.min_ns)
    }
}

/// A constant latency.
#[derive(Clone, Copy, Debug)]
pub struct ConstantFeed {
    /// Latency, ns.
    pub ns: Nanos,
}

impl FeedLatency for ConstantFeed {
    fn sample(&mut self, _ev: &MarketEvent, _rng: &mut ChaCha8Rng) -> Nanos {
        self.ns
    }
}

/// `exp(mu + sigma * z)` ns, clamped below at `min_ns`.
#[derive(Clone, Copy, Debug)]
pub struct LogNormalFeed {
    /// Mean of the log, ln(ns).
    pub mu: f64,
    /// Standard deviation of the log.
    pub sigma: f64,
    /// Lower clamp, ns.
    pub min_ns: Nanos,
}

/// Round a non-negative float of nanoseconds to `Nanos`, saturating.
#[must_use]
#[allow(clippy::cast_possible_truncation)]
pub fn to_nanos(x: f64) -> Nanos {
    if x.is_nan() {
        return 0;
    }
    x.round().clamp(0.0, 9.0e18) as Nanos
}

impl FeedLatency for LogNormalFeed {
    fn sample(&mut self, _ev: &MarketEvent, r: &mut ChaCha8Rng) -> Nanos {
        to_nanos(rng::lognormal(r, self.mu, self.sigma)).max(self.min_ns)
    }
}

/// Sampled uniformly from measured latencies.
#[derive(Clone, Debug)]
pub struct EmpiricalFeed {
    /// Measured latencies, ns.
    pub samples: Vec<Nanos>,
    /// Lower clamp, ns.
    pub min_ns: Nanos,
}

impl FeedLatency for EmpiricalFeed {
    fn sample(&mut self, _ev: &MarketEvent, r: &mut ChaCha8Rng) -> Nanos {
        let i = usize::try_from(rng::below(r, self.samples.len() as u64)).unwrap_or(0);
        self.samples.get(i).copied().unwrap_or(0).max(self.min_ns)
    }
}

/// Build the feed model of `[latency.feed]`. `empirical` needs its samples
/// loaded by the caller (`samples`).
///
/// # Errors
/// Missing parameters.
pub fn feed_model(cfg: &FeedLatencyCfg, samples: Option<Vec<Nanos>>) -> Result<Box<dyn FeedLatency>, LatencyError> {
    let need = |what: &str| LatencyError(format!("latency.feed.{what} is required for model {:?}", cfg.model));
    Ok(match cfg.model {
        FeedModel::Recorded => Box::new(RecordedFeed { min_ns: cfg.min_ns }),
        FeedModel::Constant => {
            Box::new(ConstantFeed { ns: cfg.constant_ns.ok_or_else(|| need("constant_ns"))?.max(0) })
        }
        FeedModel::Lognormal => Box::new(LogNormalFeed {
            mu: cfg.mu.ok_or_else(|| need("mu"))?,
            sigma: cfg.sigma.ok_or_else(|| need("sigma"))?,
            min_ns: cfg.min_ns,
        }),
        FeedModel::Empirical => {
            let samples = samples.filter(|s| !s.is_empty()).ok_or_else(|| need("file"))?;
            Box::new(EmpiricalFeed { samples, min_ns: cfg.min_ns })
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use edbt_core::SymbolId;
    use edbt_core::event::MarketKind;

    fn ev(te: Nanos, tr: Nanos) -> MarketEvent {
        MarketEvent {
            ts_exchange: te,
            ts_recv: tr,
            symbol: SymbolId(0),
            kind: MarketKind::Trade { side: 1, price: 1, qty: 1, trade_id: 1 },
        }
    }

    #[test]
    fn models() {
        let mut r = rng::stream(1, rng::LATENCY_FEED);
        assert_eq!(RecordedFeed { min_ns: 0 }.sample(&ev(10, 25), &mut r), 15);
        assert_eq!(RecordedFeed { min_ns: 3 }.sample(&ev(10, 5), &mut r), 3);
        assert_eq!(ConstantFeed { ns: 7 }.sample(&ev(0, 0), &mut r), 7);
        let mut ln = LogNormalFeed { mu: 14.5, sigma: 0.4, min_ns: 0 };
        let x = ln.sample(&ev(0, 0), &mut r);
        assert!(x > 100_000 && x < 100_000_000, "{x}");
        let mut emp = EmpiricalFeed { samples: vec![5, 9], min_ns: 0 };
        assert!((0..100).map(|_| emp.sample(&ev(0, 0), &mut r)).all(|v| v == 5 || v == 9));
        assert_eq!(to_nanos(f64::NAN), 0);
        assert_eq!(to_nanos(-3.0), 0);
    }

    #[test]
    fn config_builds_models() {
        let mut cfg = FeedLatencyCfg::default();
        assert!(feed_model(&cfg, None).is_ok());
        cfg.model = FeedModel::Constant;
        assert!(feed_model(&cfg, None).is_err());
        cfg.constant_ns = Some(5);
        assert!(feed_model(&cfg, None).is_ok());
        cfg.model = FeedModel::Lognormal;
        assert!(feed_model(&cfg, None).is_err());
        cfg.mu = Some(1.0);
        cfg.sigma = Some(0.1);
        assert!(feed_model(&cfg, None).is_ok());
        cfg.model = FeedModel::Empirical;
        assert!(feed_model(&cfg, Some(vec![])).is_err());
        assert!(feed_model(&cfg, Some(vec![1])).is_ok());
    }
}
