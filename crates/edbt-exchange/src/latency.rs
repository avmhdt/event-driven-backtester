//! Feed and order-path latency models (FR-04, FR-05).
//!
//! Feed latency is sampled once per venue message (an L2 batch, a trade, an
//! L3 event) so the rows of one batch arrive together; the scheduler then
//! makes local arrival times non-decreasing per stream.

use edbt_core::Nanos;
use edbt_core::config::{FeedLatencyCfg, FeedModel, OrderLatencyCfg, OrderModel};
use edbt_core::event::MarketEvent;
use edbt_core::gateway::ExchangeResponse;
use edbt_core::order::OrderRequest;
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

/// Order-path latency: entry (local to exchange) and response (exchange to
/// local), FR-05.
pub trait OrderLatency {
    /// Entry latency of a request sent at `ts_send`, ns.
    fn entry(&mut self, req: Option<&OrderRequest>, ts_send: Nanos, rng: &mut ChaCha8Rng) -> Nanos;
    /// Response latency of a response produced at exchange time `ts_exch`, ns.
    fn response(&mut self, resp: &ExchangeResponse, ts_exch: Nanos, rng: &mut ChaCha8Rng) -> Nanos;
}

/// Constant entry and response latency.
#[derive(Clone, Copy, Debug)]
pub struct ConstantOrder {
    /// Entry latency, ns.
    pub entry_ns: Nanos,
    /// Response latency, ns.
    pub response_ns: Nanos,
}

impl OrderLatency for ConstantOrder {
    fn entry(&mut self, _req: Option<&OrderRequest>, _ts: Nanos, _rng: &mut ChaCha8Rng) -> Nanos {
        self.entry_ns
    }
    fn response(&mut self, _resp: &ExchangeResponse, _ts: Nanos, _rng: &mut ChaCha8Rng) -> Nanos {
        self.response_ns
    }
}

/// Lognormal entry and response latency (of ns).
#[derive(Clone, Copy, Debug)]
pub struct LogNormalOrder {
    /// Entry mu.
    pub entry_mu: f64,
    /// Entry sigma.
    pub entry_sigma: f64,
    /// Response mu.
    pub response_mu: f64,
    /// Response sigma.
    pub response_sigma: f64,
}

impl OrderLatency for LogNormalOrder {
    fn entry(&mut self, _req: Option<&OrderRequest>, _ts: Nanos, r: &mut ChaCha8Rng) -> Nanos {
        to_nanos(rng::lognormal(r, self.entry_mu, self.entry_sigma))
    }
    fn response(&mut self, _resp: &ExchangeResponse, _ts: Nanos, r: &mut ChaCha8Rng) -> Nanos {
        to_nanos(rng::lognormal(r, self.response_mu, self.response_sigma))
    }
}

/// One measured order round trip (from an `order_latency` file).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OrderLatencySample {
    /// Local send time, ns.
    pub ts_send: Nanos,
    /// Entry latency, ns.
    pub entry_ns: Nanos,
    /// Response latency, ns.
    pub response_ns: Nanos,
}

/// Entry and response latency drawn together from measured round trips.
#[derive(Clone, Debug)]
pub struct EmpiricalOrder {
    samples: Vec<OrderLatencySample>,
    pending_response: Option<Nanos>,
}

impl EmpiricalOrder {
    /// From measured samples (must be non-empty).
    #[must_use]
    pub fn new(samples: Vec<OrderLatencySample>) -> Self {
        Self { samples, pending_response: None }
    }

    fn draw(&self, r: &mut ChaCha8Rng) -> OrderLatencySample {
        let i = usize::try_from(rng::below(r, self.samples.len() as u64)).unwrap_or(0);
        self.samples.get(i).copied().unwrap_or(OrderLatencySample { ts_send: 0, entry_ns: 0, response_ns: 0 })
    }
}

impl OrderLatency for EmpiricalOrder {
    fn entry(&mut self, _req: Option<&OrderRequest>, _ts: Nanos, r: &mut ChaCha8Rng) -> Nanos {
        let s = self.draw(r);
        self.pending_response = Some(s.response_ns);
        s.entry_ns
    }
    fn response(&mut self, _resp: &ExchangeResponse, _ts: Nanos, r: &mut ChaCha8Rng) -> Nanos {
        match self.pending_response {
            Some(v) => v,
            None => self.draw(r).response_ns,
        }
    }
}

/// Time-varying latency interpolated linearly by send time from measured
/// round trips (in the spirit of hftbacktest's interpolated order latency).
/// Outside the measured range the nearest measurement is used.
#[derive(Clone, Debug)]
pub struct InterpolatedOrder {
    samples: Vec<OrderLatencySample>,
}

impl InterpolatedOrder {
    /// From measured samples in any order (must be non-empty).
    #[must_use]
    pub fn new(mut samples: Vec<OrderLatencySample>) -> Self {
        samples.sort_by_key(|s| s.ts_send);
        Self { samples }
    }

    /// `(entry, response)` at local time `ts`.
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn at(&self, ts: Nanos) -> (Nanos, Nanos) {
        let samples = &self.samples;
        let Some(first) = samples.first() else { return (0, 0) };
        let idx = samples.partition_point(|x| x.ts_send <= ts);
        if idx == 0 {
            return (first.entry_ns, first.response_ns);
        }
        if idx == samples.len() {
            let last = samples[samples.len() - 1];
            return (last.entry_ns, last.response_ns);
        }
        let (lo, hi) = (samples[idx - 1], samples[idx]);
        let span = (hi.ts_send - lo.ts_send) as f64;
        let weight = if span > 0.0 { (ts - lo.ts_send) as f64 / span } else { 0.0 };
        let lerp = |x: Nanos, y: Nanos| to_nanos(x as f64 + (y - x) as f64 * weight);
        (lerp(lo.entry_ns, hi.entry_ns), lerp(lo.response_ns, hi.response_ns))
    }
}

impl OrderLatency for InterpolatedOrder {
    fn entry(&mut self, _req: Option<&OrderRequest>, ts: Nanos, _rng: &mut ChaCha8Rng) -> Nanos {
        self.at(ts).0
    }
    fn response(&mut self, _resp: &ExchangeResponse, ts: Nanos, _rng: &mut ChaCha8Rng) -> Nanos {
        self.at(ts).1
    }
}

/// Build the order model of `[latency.order]`. `empirical` and
/// `interpolated` need the measured samples loaded by the caller.
///
/// # Errors
/// Missing parameters or samples.
pub fn order_model(
    cfg: &OrderLatencyCfg,
    samples: Option<Vec<OrderLatencySample>>,
) -> Result<Box<dyn OrderLatency>, LatencyError> {
    let need = |what: &str| LatencyError(format!("latency.order.{what} is required for model {:?}", cfg.model));
    Ok(match cfg.model {
        OrderModel::Constant => Box::new(ConstantOrder {
            entry_ns: cfg.entry_ns.ok_or_else(|| need("entry_ns"))?.max(0),
            response_ns: cfg.response_ns.ok_or_else(|| need("response_ns"))?.max(0),
        }),
        OrderModel::Lognormal => Box::new(LogNormalOrder {
            entry_mu: cfg.entry_mu.ok_or_else(|| need("entry_mu"))?,
            entry_sigma: cfg.entry_sigma.ok_or_else(|| need("entry_sigma"))?,
            response_mu: cfg.response_mu.ok_or_else(|| need("response_mu"))?,
            response_sigma: cfg.response_sigma.ok_or_else(|| need("response_sigma"))?,
        }),
        OrderModel::Empirical => {
            Box::new(EmpiricalOrder::new(samples.filter(|s| !s.is_empty()).ok_or_else(|| need("file"))?))
        }
        OrderModel::Interpolated => {
            Box::new(InterpolatedOrder::new(samples.filter(|s| !s.is_empty()).ok_or_else(|| need("file"))?))
        }
    })
}

#[cfg(test)]
mod order_tests {
    use super::*;
    use edbt_core::ClientOrderId;
    use edbt_core::gateway::ResponseKind;

    #[test]
    fn order_models() {
        let mut r = rng::stream(1, rng::LATENCY_ORDER);
        let resp = ExchangeResponse { id: ClientOrderId(1), ts_exchange: 0, kind: ResponseKind::Canceled };
        let mut c = ConstantOrder { entry_ns: 5, response_ns: 7 };
        assert_eq!((c.entry(None, 0, &mut r), c.response(&resp, 0, &mut r)), (5, 7));
        let s = vec![
            OrderLatencySample { ts_send: 100, entry_ns: 10, response_ns: 20 },
            OrderLatencySample { ts_send: 0, entry_ns: 0, response_ns: 0 },
        ];
        let mut i = InterpolatedOrder::new(s.clone());
        assert_eq!(i.at(-5), (0, 0));
        assert_eq!(i.at(50), (5, 10));
        assert_eq!(i.at(500), (10, 20));
        assert_eq!(i.entry(None, 50, &mut r), 5);
        assert_eq!(i.response(&resp, 100, &mut r), 20);
        let mut e = EmpiricalOrder::new(s);
        let en = e.entry(None, 0, &mut r);
        let re = e.response(&resp, 0, &mut r);
        assert!((en, re) == (10, 20) || (en, re) == (0, 0));
        let mut ln = LogNormalOrder { entry_mu: 10.0, entry_sigma: 0.1, response_mu: 10.0, response_sigma: 0.1 };
        assert!(ln.entry(None, 0, &mut r) > 0 && ln.response(&resp, 0, &mut r) > 0);
        let mut cfg = OrderLatencyCfg::default();
        assert!(order_model(&cfg, None).is_ok());
        cfg.model = OrderModel::Lognormal;
        assert!(order_model(&cfg, None).is_err());
        cfg.model = OrderModel::Interpolated;
        assert!(order_model(&cfg, None).is_err());
        let one = vec![OrderLatencySample { ts_send: 0, entry_ns: 1, response_ns: 1 }];
        assert!(order_model(&cfg, Some(one.clone())).is_ok());
        cfg.model = OrderModel::Empirical;
        assert!(order_model(&cfg, Some(one)).is_ok());
    }
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
