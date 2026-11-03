//! Per-component ChaCha8 streams derived from the run seed (FR-17), and
//! portable samplers.
//!
//! Each component gets its own stream, seeded with
//! `blake3(seed.to_le_bytes() || label)`, so adding draws in one component
//! never shifts another. Samplers use `libm` for `ln`, `exp`, `sqrt` and
//! `cos`, which give the same results on Linux and Windows.

use rand_chacha::ChaCha8Rng;
use rand_core::{RngCore, SeedableRng};

/// Stream label of the feed-latency model.
pub const LATENCY_FEED: &str = "latency.feed";
/// Stream label of the order-latency model.
pub const LATENCY_ORDER: &str = "latency.order";
/// Stream label of the queue models.
pub const QUEUE: &str = "queue";
/// Stream label of the strategy (`ctx.random()`, `ctx.rng_normal()`).
pub const STRATEGY: &str = "strategy";

/// The ChaCha8 stream for `label` under `seed`.
#[must_use]
pub fn stream(seed: u64, label: &str) -> ChaCha8Rng {
    let mut h = blake3::Hasher::new();
    h.update(&seed.to_le_bytes());
    h.update(label.as_bytes());
    ChaCha8Rng::from_seed(*h.finalize().as_bytes())
}

/// Uniform in `[0, 1)` with 53 random bits.
#[allow(clippy::cast_precision_loss)]
pub fn uniform(rng: &mut ChaCha8Rng) -> f64 {
    (rng.next_u64() >> 11) as f64 * (1.0 / 9_007_199_254_740_992.0)
}

/// Standard normal by Box-Muller (one value per call).
pub fn normal(rng: &mut ChaCha8Rng) -> f64 {
    let u1 = 1.0 - uniform(rng); // (0, 1]
    let u2 = uniform(rng);
    libm::sqrt(-2.0 * libm::log(u1)) * libm::cos(std::f64::consts::TAU * u2)
}

/// `exp(mu + sigma * z)`.
pub fn lognormal(rng: &mut ChaCha8Rng, mu: f64, sigma: f64) -> f64 {
    libm::exp(mu + sigma * normal(rng))
}

/// Uniform integer in `[0, n)`; `n` must be positive.
pub fn below(rng: &mut ChaCha8Rng, n: u64) -> u64 {
    // Lemire's multiply-shift; the tiny bias is irrelevant here and the
    // result is deterministic.
    let x = u128::from(rng.next_u64()) * u128::from(n.max(1));
    u64::try_from(x >> 64).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn streams_are_seeded_and_independent() {
        let a = stream(7, QUEUE).next_u64();
        assert_eq!(a, stream(7, QUEUE).next_u64());
        assert_ne!(a, stream(8, QUEUE).next_u64());
        assert_ne!(a, stream(7, STRATEGY).next_u64());
        assert_ne!(stream(7, LATENCY_FEED).next_u64(), stream(7, LATENCY_ORDER).next_u64());
    }

    #[test]
    #[allow(clippy::cast_precision_loss)]
    fn samplers_have_the_right_moments() {
        let mut r = stream(1, "test");
        let n = 200_000;
        let (mut s, mut s2) = (0.0, 0.0);
        for _ in 0..n {
            let z = normal(&mut r);
            s += z;
            s2 += z * z;
        }
        let mean = s / f64::from(n);
        let var = s2 / f64::from(n) - mean * mean;
        assert!(mean.abs() < 0.01, "{mean}");
        assert!((var - 1.0).abs() < 0.02, "{var}");
        let u = uniform(&mut r);
        assert!((0.0..1.0).contains(&u));
        assert!(lognormal(&mut r, 0.0, 0.0) > 0.999_999);
        assert!((0..1000).all(|_| below(&mut r, 3) < 3));
    }
}
