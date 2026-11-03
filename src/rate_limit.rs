//! Per-client request limiting.
//!
//! One token bucket per resolved client address. The limiter is in memory
//! and per process: a restart resets every bucket, and a second replica
//! keeps its own. It is a flood guard, not an access control. A client
//! behind NAT shares a bucket with everyone else behind that address, and
//! a client that can change its source address gets a fresh one.
//!
//! The bucket is keyed on [`crate::client_addr::ClientAddr`], the same
//! value the auth log uses, so a trusted proxy's forwarding header is
//! honoured consistently.

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// The most client buckets kept at once.
///
/// The map is keyed by an address a stranger can choose, so it must not
/// grow without bound. At this size the map costs well under a megabyte.
pub const MAX_BUCKETS: usize = 10_000;

/// A bucket idle for this long is dropped when the map is full.
const IDLE_TTL: Duration = Duration::from_secs(10 * 60);

/// How often a full map is swept for idle buckets.
///
/// Sweeping on every new client would make a rotating-address flood pay
/// for a full scan per request. One sweep a minute is enough to release
/// clients that have gone away, and a full map evicts one bucket at a
/// time between sweeps.
const SWEEP_INTERVAL: Duration = Duration::from_secs(60);

/// The window that `DAYFLY_RATE_LIMIT_PER_MINUTE` refers to.
const WINDOW: Duration = Duration::from_secs(60);

/// A refusal and the wait before the next request can pass.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RateLimitError {
    /// Time until one token is available again.
    pub retry_after: Duration,
}

struct Bucket {
    tokens: f64,
    last_refill: Instant,
    last_seen: Instant,
}

struct State {
    buckets: HashMap<IpAddr, Bucket>,
    last_sweep: Instant,
}

/// The in-memory limiter. One instance is shared by every request.
pub struct RateLimiter {
    state: Mutex<State>,
}

impl Default for RateLimiter {
    fn default() -> Self {
        Self::new()
    }
}

impl RateLimiter {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(State {
                buckets: HashMap::new(),
                last_sweep: Instant::now(),
            }),
        }
    }

    /// Check one request from `client`.
    ///
    /// A limit of zero disables the limiter and allocates no bucket.
    pub fn check(&self, client: IpAddr, limit_per_minute: u32) -> Result<(), RateLimitError> {
        self.check_at(client, limit_per_minute, Instant::now())
    }

    /// The clock-injected form, so tests can control the refill instead of
    /// sleeping.
    fn check_at(
        &self,
        client: IpAddr,
        limit_per_minute: u32,
        now: Instant,
    ) -> Result<(), RateLimitError> {
        if limit_per_minute == 0 {
            return Ok(());
        }
        let capacity = f64::from(limit_per_minute);
        let tokens_per_second = capacity / WINDOW.as_secs_f64();
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !state.buckets.contains_key(&client) && state.buckets.len() >= MAX_BUCKETS {
            make_room(&mut state, now);
        }
        let bucket = state.buckets.entry(client).or_insert_with(|| Bucket {
            tokens: capacity,
            last_refill: now,
            last_seen: now,
        });
        let elapsed = now.saturating_duration_since(bucket.last_refill);
        if !elapsed.is_zero() {
            bucket.tokens =
                (bucket.tokens + elapsed.as_secs_f64() * tokens_per_second).min(capacity);
            bucket.last_refill = now;
        }
        bucket.last_seen = now;
        if bucket.tokens >= 1.0 {
            bucket.tokens -= 1.0;
            Ok(())
        } else {
            let seconds = (1.0 - bucket.tokens) / tokens_per_second;
            Err(RateLimitError {
                retry_after: Duration::from_secs_f64(seconds.max(1.0)),
            })
        }
    }

    #[cfg(test)]
    fn bucket_count(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .buckets
            .len()
    }
}

/// Make room for one new bucket.
///
/// When the map is full, an idle sweep runs at most once per minute. Any
/// bucket still over the bound is evicted, so the map never grows past
/// [`MAX_BUCKETS`]. Evicting a fresh bucket under a rotating-address flood
/// only costs that client its current budget.
fn make_room(state: &mut State, now: Instant) {
    if now.saturating_duration_since(state.last_sweep) >= SWEEP_INTERVAL {
        state
            .buckets
            .retain(|_, bucket| now.saturating_duration_since(bucket.last_seen) < IDLE_TTL);
        state.last_sweep = now;
    }
    while state.buckets.len() >= MAX_BUCKETS {
        let Some(address) = state.buckets.keys().next().copied() else {
            break;
        };
        state.buckets.remove(&address);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn client(index: u32) -> IpAddr {
        IpAddr::V4(std::net::Ipv4Addr::from(index))
    }

    #[test]
    fn a_zero_limit_allocates_no_buckets() {
        let limiter = RateLimiter::new();
        let now = Instant::now();
        for _ in 0..1000 {
            limiter
                .check_at(client(1), 0, now)
                .expect("a zero limit is disabled");
        }
        assert_eq!(limiter.bucket_count(), 0);
    }

    #[test]
    fn a_burst_up_to_the_limit_passes() {
        let limiter = RateLimiter::new();
        let now = Instant::now();
        for _ in 0..3 {
            limiter
                .check_at(client(1), 3, now)
                .expect("a request inside the burst must pass");
        }
        let error = limiter
            .check_at(client(1), 3, now)
            .expect_err("the request above the burst must be refused");
        assert!(error.retry_after >= Duration::from_secs(1));
    }

    #[test]
    fn the_bucket_refills_at_the_configured_rate() {
        let limiter = RateLimiter::new();
        let start = Instant::now();
        for _ in 0..60 {
            limiter
                .check_at(client(1), 60, start)
                .expect("the burst must pass");
        }
        assert!(limiter.check_at(client(1), 60, start).is_err());

        // One token per second at 60 per minute.
        limiter
            .check_at(client(1), 60, start + Duration::from_secs(1))
            .expect("one token has refilled");
        assert!(limiter
            .check_at(client(1), 60, start + Duration::from_secs(1))
            .is_err());

        limiter
            .check_at(client(1), 60, start + Duration::from_secs(2))
            .expect("a second token has refilled");
    }

    #[test]
    fn retry_after_names_the_wait() {
        let limiter = RateLimiter::new();
        let start = Instant::now();
        for _ in 0..60 {
            limiter
                .check_at(client(1), 60, start)
                .expect("the burst must pass");
        }
        let error = limiter
            .check_at(client(1), 60, start + Duration::from_millis(500))
            .expect_err("the bucket is empty");
        assert!(
            error.retry_after >= Duration::from_millis(500),
            "retry_after: {:?}",
            error.retry_after
        );
        assert!(
            error.retry_after <= Duration::from_secs(1),
            "retry_after: {:?}",
            error.retry_after
        );
    }

    #[test]
    fn clients_have_independent_buckets() {
        let limiter = RateLimiter::new();
        let now = Instant::now();
        limiter.check_at(client(1), 1, now).expect("first client");
        limiter.check_at(client(2), 1, now).expect("second client");
        assert!(limiter.check_at(client(1), 1, now).is_err());
    }

    #[test]
    fn the_bucket_map_is_bounded() {
        let limiter = RateLimiter::new();
        let now = Instant::now();
        for index in 0..(MAX_BUCKETS as u32 + 100) {
            limiter
                .check_at(client(index), 1, now)
                .expect("each new client starts with a full bucket");
        }
        assert!(
            limiter.bucket_count() <= MAX_BUCKETS,
            "bucket count: {}",
            limiter.bucket_count()
        );
    }

    #[test]
    fn idle_buckets_are_evicted() {
        let limiter = RateLimiter::new();
        let start = Instant::now();
        for index in 0..(MAX_BUCKETS as u32) {
            limiter
                .check_at(client(index), 1, start)
                .expect("the first population must fit");
        }
        // Ten minutes later the whole population is idle. A new client
        // must have room without growing the map past the bound.
        let later = start + IDLE_TTL + Duration::from_secs(1);
        limiter
            .check_at(client(MAX_BUCKETS as u32 + 1), 1, later)
            .expect("a new client must be admitted");
        assert!(
            limiter.bucket_count() < MAX_BUCKETS,
            "idle buckets must be released, count: {}",
            limiter.bucket_count()
        );
    }
}
