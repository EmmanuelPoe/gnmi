//! Admission control for reconnect floods (e.g. thousands of devices
//! re-dialing after a collector restart).
//!
//! Four stages, each optional:
//! - connections: above `max-connections`, new connections are shed (HTTP/2
//!   GOAWAY on cleartext listeners, TCP close on TLS listeners), so devices
//!   back off and reconnect later;
//! - TLS handshakes: at most `max-concurrent-handshakes` in flight;
//! - registrations: a token bucket of `registration-rate`/s (bursts of
//!   `registration-burst`); a device waits in FIFO order up to
//!   `registration-queue-timeout`, then gets RESOURCE_EXHAUSTED;
//! - target setups: at most `max-concurrent-setups` targets dial and receive
//!   their initial sync at once; the others queue.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::Deserialize;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::config::duration_opt;

const DEFAULT_HANDSHAKES: usize = 128;
const DEFAULT_SETUPS: usize = 256;
const DEFAULT_QUEUE_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_SETUP_TIMEOUT: Duration = Duration::from_secs(60);
const REPORT_EVERY: Duration = Duration::from_secs(10);
const MAX_SHEDDERS: usize = 256;

#[derive(Debug, Default, Clone, Deserialize)]
#[serde(rename_all = "kebab-case", default, deny_unknown_fields)]
pub struct AdmissionConfig {
    /// Concurrent device connections (0: unlimited).
    pub max_connections: usize,
    /// New registrations per second (0: unlimited).
    pub registration_rate: f64,
    /// Registrations allowed back to back (default: one second of rate).
    pub registration_burst: usize,
    /// How long a registration may wait for its turn before being refused.
    #[serde(deserialize_with = "duration_opt")]
    pub registration_queue_timeout: Option<Duration>,
    /// TLS handshakes in progress at once (default 128, 0: unlimited).
    pub max_concurrent_handshakes: Option<usize>,
    /// Targets dialing and syncing at once (default 256, 0: unlimited).
    pub max_concurrent_setups: Option<usize>,
    /// A setup slot is released at the first sync-response or after this.
    #[serde(deserialize_with = "duration_opt")]
    pub setup_timeout: Option<Duration>,
}

impl AdmissionConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        let r = self.registration_rate;
        if !(r == 0.0 || (0.001..=1_000_000.0).contains(&r)) {
            anyhow::bail!("admission: registration-rate must be 0 (unlimited) or between 0.001 and 1000000");
        }
        if self.registration_burst > 1_000_000 {
            anyhow::bail!("admission: registration-burst must be at most 1000000");
        }
        Ok(())
    }
}

fn semaphore(limit: usize) -> Option<Arc<Semaphore>> {
    (limit > 0).then(|| Arc::new(Semaphore::new(limit)))
}

/// Generic cell rate algorithm: the virtual time at which the bucket is
/// next empty. Requests are admitted in arrival order.
struct Gcra {
    interval: Duration,
    tolerance: Duration,
    max_wait: Duration,
    tat: Mutex<Instant>,
}

pub struct Admission {
    max_connections: usize,
    connections: AtomicUsize,
    rate: Option<Gcra>,
    handshakes: Option<Arc<Semaphore>>,
    /// Bounds the connections being shed with GOAWAY at once.
    pub shedders: Arc<Semaphore>,
    setups: Option<Arc<Semaphore>>,
    pub setup_timeout: Duration,
    shed_connections: AtomicU64,
    shed_registrations: AtomicU64,
    waiting_setups: AtomicUsize,
}

/// Counts an admitted connection until dropped.
pub struct ConnectionGuard(Arc<Admission>);

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.0.connections.fetch_sub(1, Ordering::Relaxed);
    }
}

impl Admission {
    pub fn new(cfg: &AdmissionConfig) -> Arc<Self> {
        let rate = (cfg.registration_rate > 0.0).then(|| {
            let interval = Duration::from_secs_f64(1.0 / cfg.registration_rate);
            let burst = match cfg.registration_burst {
                0 => cfg.registration_rate.ceil() as u32,
                b => b as u32,
            };
            Gcra {
                interval,
                tolerance: interval * burst.saturating_sub(1),
                max_wait: cfg.registration_queue_timeout.unwrap_or(DEFAULT_QUEUE_TIMEOUT),
                tat: Mutex::new(Instant::now()),
            }
        });
        Arc::new(Admission {
            max_connections: cfg.max_connections,
            connections: AtomicUsize::new(0),
            rate,
            handshakes: semaphore(cfg.max_concurrent_handshakes.unwrap_or(DEFAULT_HANDSHAKES)),
            shedders: Arc::new(Semaphore::new(MAX_SHEDDERS)),
            setups: semaphore(cfg.max_concurrent_setups.unwrap_or(DEFAULT_SETUPS)),
            setup_timeout: cfg.setup_timeout.unwrap_or(DEFAULT_SETUP_TIMEOUT),
            shed_connections: AtomicU64::new(0),
            shed_registrations: AtomicU64::new(0),
            waiting_setups: AtomicUsize::new(0),
        })
    }

    /// Admits a new connection, or None when it must be shed.
    pub fn try_connection(self: &Arc<Self>) -> Option<ConnectionGuard> {
        let admitted = self
            .connections
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                (self.max_connections == 0 || n < self.max_connections).then_some(n + 1)
            })
            .is_ok();
        if !admitted {
            self.shed_connections.fetch_add(1, Ordering::Relaxed);
        }
        admitted.then(|| ConnectionGuard(self.clone()))
    }

    pub async fn handshake_permit(&self) -> Option<OwnedSemaphorePermit> {
        match &self.handshakes {
            Some(s) => s.clone().acquire_owned().await.ok(),
            None => None,
        }
    }

    /// Waits for this registration's turn; Err(retry_after) when the queue
    /// is longer than the configured timeout.
    pub async fn admit_registration(&self) -> Result<(), Duration> {
        let Some(g) = &self.rate else { return Ok(()) };
        /// Gives the reserved slot back if the registration is abandoned
        /// while waiting (the device cancelled or timed out).
        struct Reservation<'a>(&'a Gcra, bool);
        impl Drop for Reservation<'_> {
            fn drop(&mut self) {
                if !self.1 {
                    let mut tat = self.0.tat.lock().unwrap_or_else(|e| e.into_inner());
                    if let Some(t) = tat.checked_sub(self.0.interval) {
                        *tat = t;
                    }
                }
            }
        }
        let wait = {
            let mut tat = g.tat.lock().unwrap_or_else(|e| e.into_inner());
            let now = Instant::now();
            let start = (*tat).max(now);
            let wait = (start.checked_sub(g.tolerance).unwrap_or(now)).saturating_duration_since(now);
            if wait > g.max_wait {
                self.shed_registrations.fetch_add(1, Ordering::Relaxed);
                return Err(wait);
            }
            *tat = start + g.interval;
            wait
        };
        let mut reservation = Reservation(g, false);
        if !wait.is_zero() {
            tokio::time::sleep(wait).await;
        }
        reservation.1 = true;
        Ok(())
    }

    /// A setup slot, waiting in FIFO order; None when setups are unlimited.
    pub async fn setup_permit(&self) -> Option<OwnedSemaphorePermit> {
        let s = self.setups.as_ref()?;
        // Counted while waiting, also when the wait is cancelled.
        struct Waiting<'a>(&'a AtomicUsize);
        impl Drop for Waiting<'_> {
            fn drop(&mut self) {
                self.0.fetch_sub(1, Ordering::Relaxed);
            }
        }
        self.waiting_setups.fetch_add(1, Ordering::Relaxed);
        let _waiting = Waiting(&self.waiting_setups);
        s.clone().acquire_owned().await.ok()
    }

    /// Logs a summary while connections or registrations are being shed or
    /// targets are queued.
    pub async fn report(self: Arc<Self>, shutdown: CancellationToken) {
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => return,
                _ = tokio::time::sleep(REPORT_EVERY) => {}
            }
            let conns = self.shed_connections.swap(0, Ordering::Relaxed);
            let regs = self.shed_registrations.swap(0, Ordering::Relaxed);
            let waiting = self.waiting_setups.load(Ordering::Relaxed);
            if conns > 0 || regs > 0 || waiting > 0 {
                tracing::warn!(
                    shed_connections = conns,
                    shed_registrations = regs,
                    waiting_setups = waiting,
                    connections = self.connections.load(Ordering::Relaxed),
                    "admission control active (last {REPORT_EVERY:?})"
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rate_bounds() {
        let cfg = |r: f64, b: usize| AdmissionConfig {
            registration_rate: r,
            registration_burst: b,
            ..Default::default()
        };
        assert!(cfg(1e-20, 0).validate().is_err());
        assert!(cfg(f64::NAN, 0).validate().is_err());
        assert!(cfg(0.001, 10_000_000_000).validate().is_err());
        assert!(cfg(0.0, 0).validate().is_ok());
        Admission::new(&cfg(0.001, 1_000_000));
    }

    #[tokio::test(start_paused = true)]
    async fn abandoned_registration_returns_its_slot() {
        let a = Admission::new(&AdmissionConfig {
            registration_rate: 1.0,
            registration_burst: 1,
            registration_queue_timeout: Some(Duration::from_secs(5)),
            ..Default::default()
        });
        a.admit_registration().await.unwrap();
        // Queued for 1 s, then abandoned.
        let _ = tokio::time::timeout(Duration::from_millis(10), a.admit_registration()).await;
        let start = Instant::now();
        a.admit_registration().await.unwrap();
        assert!(start.elapsed() <= Duration::from_secs(1), "{:?}", start.elapsed());
    }

    #[tokio::test]
    async fn cancelled_setup_wait_is_uncounted() {
        let a = Admission::new(&AdmissionConfig {
            max_concurrent_setups: Some(1),
            ..Default::default()
        });
        let _p = a.setup_permit().await;
        let _ = tokio::time::timeout(Duration::from_millis(10), a.setup_permit()).await;
        assert_eq!(a.waiting_setups.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn connection_cap() {
        let a = Admission::new(&AdmissionConfig {
            max_connections: 2,
            ..Default::default()
        });
        let g1 = a.try_connection().unwrap();
        let _g2 = a.try_connection().unwrap();
        assert!(a.try_connection().is_none());
        drop(g1);
        assert!(a.try_connection().is_some());
        assert_eq!(a.shed_connections.load(Ordering::Relaxed), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn registration_rate_queue_and_shed() {
        let a = Admission::new(&AdmissionConfig {
            registration_rate: 10.0,
            registration_burst: 5,
            registration_queue_timeout: Some(Duration::from_millis(500)),
            ..Default::default()
        });
        let start = Instant::now();
        // The burst passes immediately.
        for _ in 0..5 {
            a.admit_registration().await.unwrap();
        }
        assert_eq!(start.elapsed(), Duration::ZERO);
        // The next ones are paced at 100 ms, up to 500 ms of queueing...
        for _ in 0..5 {
            a.admit_registration().await.unwrap();
        }
        assert!(start.elapsed() >= Duration::from_millis(500));
        // ...and a request that would wait longer is refused, right away.
        let a2 = a.clone();
        let queued: Vec<_> = (0..6)
            .map(|_| {
                tokio::spawn({
                    let a = a2.clone();
                    async move { a.admit_registration().await }
                })
            })
            .collect();
        let mut refused = 0;
        for q in queued {
            if q.await.unwrap().is_err() {
                refused += 1;
            }
        }
        assert_eq!(refused, 1);
    }

    #[tokio::test]
    async fn setups_are_limited() {
        let a = Admission::new(&AdmissionConfig {
            max_concurrent_setups: Some(1),
            ..Default::default()
        });
        let p = a.setup_permit().await.unwrap();
        let waiter = tokio::spawn({
            let a = a.clone();
            async move { a.setup_permit().await.is_some() }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(a.waiting_setups.load(Ordering::Relaxed), 1);
        drop(p);
        assert!(waiter.await.unwrap());
        let unlimited = Admission::new(&AdmissionConfig {
            max_concurrent_setups: Some(0),
            ..Default::default()
        });
        assert!(unlimited.setup_permit().await.is_none());
    }
}
