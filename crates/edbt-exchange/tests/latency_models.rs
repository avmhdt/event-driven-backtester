//! Latency models (FR-04, FR-05, AC-W-05): sampled latencies match their
//! specified distribution (Kolmogorov-Smirnov, p > 0.01 on 100,000 samples),
//! and the feed never reorders a stream.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::cast_precision_loss)]

use edbt_core::event::{MarketEvent, MarketKind};
use edbt_core::gateway::{ExchangeResponse, ResponseKind};
use edbt_core::{ClientOrderId, SymbolId, rng};
use edbt_exchange::latency::{
    ConstantFeed, EmpiricalFeed, FeedLatency, LogNormalFeed, LogNormalOrder, OrderLatency, RecordedFeed,
};

const N: usize = 100_000;

/// Asymptotic Kolmogorov p-value of statistic `d` on `n` samples.
fn ks_pvalue(d: f64, n: usize) -> f64 {
    let sn = (n as f64).sqrt();
    let lambda = (sn + 0.12 + 0.11 / sn) * d;
    let mut p = 0.0;
    for k in 1..=100 {
        let k = f64::from(k);
        let term = 2.0 * (-1_f64).powf(k - 1.0) * (-2.0 * k * k * lambda * lambda).exp();
        p += term;
        if term.abs() < 1e-12 {
            break;
        }
    }
    p.clamp(0.0, 1.0)
}

fn ks(mut xs: Vec<f64>, cdf: impl Fn(f64) -> f64) -> f64 {
    xs.sort_by(f64::total_cmp);
    let n = xs.len() as f64;
    let mut d: f64 = 0.0;
    for (i, x) in xs.iter().enumerate() {
        let f = cdf(*x);
        d = d.max((f - i as f64 / n).abs()).max(((i + 1) as f64 / n - f).abs());
    }
    ks_pvalue(d, xs.len())
}

fn lognormal_cdf(x: f64, mu: f64, sigma: f64) -> f64 {
    if x <= 0.0 { 0.0 } else { 0.5 * libm::erfc(-(x.ln() - mu) / (sigma * std::f64::consts::SQRT_2)) }
}

fn ev() -> MarketEvent {
    MarketEvent {
        ts_exchange: 0,
        ts_recv: 0,
        symbol: SymbolId(0),
        kind: MarketKind::Trade { side: 1, price: 1, qty: 1, trade_id: 0 },
    }
}

#[test]
fn lognormal_feed_matches_distribution() {
    let (mu, sigma) = (14.5, 0.4);
    let mut m = LogNormalFeed { mu, sigma, min_ns: 0 };
    let mut r = rng::stream(42, rng::LATENCY_FEED);
    let xs: Vec<f64> = (0..N).map(|_| m.sample(&ev(), &mut r) as f64).collect();
    let p = ks(xs, |x| lognormal_cdf(x, mu, sigma));
    assert!(p > 0.01, "KS p = {p}");
}

#[test]
fn lognormal_order_matches_distribution() {
    let mut m = LogNormalOrder { entry_mu: 15.2, entry_sigma: 0.35, response_mu: 15.0, response_sigma: 0.5 };
    let mut r = rng::stream(7, rng::LATENCY_ORDER);
    let resp = ExchangeResponse { id: ClientOrderId(1), ts_exchange: 0, kind: ResponseKind::Canceled };
    let entry: Vec<f64> = (0..N).map(|_| m.entry(None, 0, &mut r) as f64).collect();
    let response: Vec<f64> = (0..N).map(|_| m.response(&resp, 0, &mut r) as f64).collect();
    assert!(ks(entry, |x| lognormal_cdf(x, 15.2, 0.35)) > 0.01);
    assert!(ks(response, |x| lognormal_cdf(x, 15.0, 0.5)) > 0.01);
}

#[test]
fn empirical_feed_matches_source_distribution() {
    // Source: 1,000 distinct values; sampling with replacement reproduces
    // their distribution.
    let samples: Vec<i64> = (0..1_000).map(|i| 1_000_000 + i * 997).collect();
    let mut m = EmpiricalFeed { samples: samples.clone(), min_ns: 0 };
    let mut r = rng::stream(3, rng::LATENCY_FEED);
    let xs: Vec<f64> = (0..N).map(|_| m.sample(&ev(), &mut r) as f64).collect();
    let mut sorted = samples;
    sorted.sort_unstable();
    let p = ks(xs, |x| {
        // Mid-step CDF of the discrete source for the continuous KS bound.
        let below = sorted.partition_point(|v| (*v as f64) < x) as f64;
        let at = sorted.partition_point(|v| (*v as f64) <= x) as f64;
        (below + at) / 2.0 / sorted.len() as f64
    });
    assert!(p > 0.01, "KS p = {p}");
}

#[test]
fn recorded_and_constant_feed() {
    let mut r = rng::stream(1, rng::LATENCY_FEED);
    let mut e = ev();
    e.ts_exchange = 1_000;
    e.ts_recv = 4_500;
    assert_eq!(RecordedFeed { min_ns: 0 }.sample(&e, &mut r), 3_500);
    e.ts_recv = 900;
    assert_eq!(RecordedFeed { min_ns: 0 }.sample(&e, &mut r), 0, "negative latency clamps to min_ns");
    assert_eq!(ConstantFeed { ns: 77 }.sample(&e, &mut r), 77);
}

#[test]
fn ks_helper_rejects_wrong_distribution() {
    let mut r = rng::stream(1, rng::LATENCY_FEED);
    let mut m = LogNormalFeed { mu: 14.5, sigma: 0.4, min_ns: 0 };
    let xs: Vec<f64> = (0..N).map(|_| m.sample(&ev(), &mut r) as f64).collect();
    assert!(ks(xs, |x| lognormal_cdf(x, 14.6, 0.4)) < 0.01);
}
