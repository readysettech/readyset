//! Adapter-side health and connection counts for an [`UpstreamServers`] list.
//!
//! Each field is its own atomic, so only replacing the list takes a lock. A reader can see a mix
//! of two updates to one server; at worst a probe runs early or late.

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, RwLock};
use std::time::{Duration, Instant, SystemTime};

use rand::RngExt;

use crate::upstream_servers::{UpstreamServer, UpstreamServers};

/// Shortest wait before a server marked down is probed again.
pub const PROBE_BACKOFF_MIN: Duration = Duration::from_secs(1);
/// Longest wait between probes of a server that stays down.
pub const PROBE_BACKOFF_MAX: Duration = Duration::from_secs(60);

/// The same moment read from the monotonic and the wall clock, at least a millisecond in the
/// past so that [`now_millis`] is never zero.
static EPOCH: LazyLock<(Instant, SystemTime)> = LazyLock::new(|| {
    const OFFSET: Duration = Duration::from_millis(1);
    let (instant, wall) = (Instant::now(), SystemTime::now());
    match instant.checked_sub(OFFSET) {
        Some(epoch) => (epoch, wall - OFFSET),
        None => {
            std::thread::sleep(OFFSET);
            (instant, wall)
        }
    }
});

/// Monotonic milliseconds since [`EPOCH`]. Never zero, so zero can mean "unset".
fn now_millis() -> u64 {
    EPOCH.0.elapsed().as_millis() as u64
}

/// The wall-clock time of a [`now_millis`] value, or `None` for zero.
fn millis_to_time(millis: u64) -> Option<SystemTime> {
    (millis != 0).then(|| EPOCH.1 + Duration::from_millis(millis))
}

/// Wait before the next probe after `failures` failed probes in a row.
fn probe_backoff(failures: u32) -> Duration {
    PROBE_BACKOFF_MIN
        .saturating_mul(1u32 << failures.min(16))
        .min(PROBE_BACKOFF_MAX)
}

/// Live health and connection count for one server.
#[derive(Debug, Default)]
pub struct UpstreamServerState {
    /// [`now_millis`] when the server was marked down, or zero while it is up.
    down_since: AtomicU64,
    /// [`now_millis`] after which the monitor may next try a server that is down.
    next_probe: AtomicU64,
    /// Consecutive failed probes, driving the probe backoff.
    failures: AtomicU32,
    /// [`now_millis`] when the server last answered a connect or a ping, or zero for never.
    last_ok: AtomicU64,
    /// Live connections this process holds to the server.
    connections: AtomicU64,
}

impl UpstreamServerState {
    pub fn is_up(&self) -> bool {
        self.down_since.load(Ordering::Relaxed) == 0
    }

    pub fn down_since(&self) -> Option<SystemTime> {
        millis_to_time(self.down_since.load(Ordering::Relaxed))
    }

    pub fn last_ok(&self) -> Option<SystemTime> {
        millis_to_time(self.last_ok.load(Ordering::Relaxed))
    }

    pub fn connections(&self) -> u64 {
        self.connections.load(Ordering::Relaxed)
    }
}

/// Health and connection counts for one version of the server list. Editing the list builds a
/// new registry; servers kept by the edit share their state with it.
#[derive(Debug)]
pub struct UpstreamServerRegistry {
    servers: UpstreamServers,
    states: Box<[Arc<UpstreamServerState>]>,
}

impl UpstreamServerRegistry {
    pub fn new(servers: UpstreamServers) -> Self {
        let states = (0..servers.len()).map(|_| Arc::default()).collect();
        Self { servers, states }
    }

    /// Make a new registry for `servers` in which any server that was in `previous` will share
    /// the server state pointed to by that registry.
    pub fn new_inherit(servers: UpstreamServers, previous: &Self) -> Self {
        let mut next = Self::new(servers);
        for (idx, server) in next.servers.iter().enumerate() {
            if let Some(old) = previous.servers.position(server.display()) {
                next.states[idx] = Arc::clone(&previous.states[old]);
            }
        }
        next
    }

    /// State of the server at `idx`, a position in this registry's list.
    pub fn state(&self, idx: usize) -> &UpstreamServerState {
        debug_assert!(
            idx < self.len(),
            "server index {idx} is not in this registry"
        );
        &self.states[idx]
    }

    pub fn servers(&self) -> &UpstreamServers {
        &self.servers
    }

    /// Number of servers. The list is never empty.
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.servers.len()
    }

    pub fn get(&self, idx: usize) -> Option<&UpstreamServer> {
        self.servers.get(idx)
    }

    pub fn is_up(&self, idx: usize) -> bool {
        self.state(idx).is_up()
    }

    /// Indices of the online servers, in weight order.
    pub fn healthy(&self) -> Vec<usize> {
        (0..self.len()).filter(|&i| self.is_up(i)).collect()
    }

    /// A random server not in `exclude`, preferring online ones. `None` if all are excluded.
    pub fn pick_excluding(&self, exclude: &[usize]) -> Option<usize> {
        let mut candidates: Vec<usize> = (0..self.len())
            .filter(|i| !exclude.contains(i) && self.is_up(*i))
            .collect();
        if candidates.is_empty() {
            candidates = (0..self.len()).filter(|i| !exclude.contains(i)).collect();
        }
        if candidates.is_empty() {
            return None;
        }
        let pick = rand::rng().random_range(0..candidates.len());
        Some(candidates[pick])
    }

    /// A random online server, or any server when all are down.
    pub fn pick(&self) -> usize {
        // The list is never empty.
        self.pick_excluding(&[]).unwrap_or(0)
    }

    /// Mark `idx` down after a connection to it failed. Returns `true` if the server was up.
    /// Marking an up server down resets its probe backoff; marking a server that is already down
    /// leaves it as is.
    pub fn mark_down(&self, idx: usize) -> bool {
        let health = self.state(idx);
        let went_down = health
            .down_since
            .compare_exchange(0, now_millis(), Ordering::Relaxed, Ordering::Relaxed)
            .is_ok();
        if went_down {
            health.failures.store(0, Ordering::Relaxed);
            health.next_probe.store(
                now_millis() + PROBE_BACKOFF_MIN.as_millis() as u64,
                Ordering::Relaxed,
            );
        }
        went_down
    }

    /// Mark `idx` up after it answered. Returns `true` if the server was down.
    pub fn mark_up(&self, idx: usize) -> bool {
        let health = self.state(idx);
        health.last_ok.store(now_millis(), Ordering::Relaxed);
        health.failures.store(0, Ordering::Relaxed);
        health.next_probe.store(0, Ordering::Relaxed);
        health.down_since.swap(0, Ordering::Relaxed) != 0
    }

    /// Record a failed probe of `idx`. The wait before the next probe doubles with each failure,
    /// from `PROBE_BACKOFF_MIN` up to `PROBE_BACKOFF_MAX`.
    pub fn record_probe_failure(&self, idx: usize) {
        let health = self.state(idx);
        let failures = health.failures.fetch_add(1, Ordering::Relaxed) + 1;
        health.next_probe.store(
            now_millis() + probe_backoff(failures).as_millis() as u64,
            Ordering::Relaxed,
        );
    }

    /// Whether the monitor may probe `idx` now: it is down and its backoff has elapsed.
    pub fn probe_due(&self, idx: usize) -> bool {
        !self.is_up(idx) && self.state(idx).next_probe.load(Ordering::Relaxed) <= now_millis()
    }

    /// Live connections this process holds to `idx`.
    pub fn connections(&self, idx: usize) -> u64 {
        self.state(idx).connections()
    }

    /// Return a guard that tracks a connection to `idx`. Drop the guard when the connection is
    /// closed to decrement the connection count.
    pub fn acquire(self: &Arc<Self>, idx: usize) -> UpstreamConnectionGuard {
        self.state(idx).connections.fetch_add(1, Ordering::Relaxed);
        UpstreamConnectionGuard {
            registry: Arc::clone(self),
            idx,
        }
    }
}

/// A guard that tracks a connection to a server. Drop the guard when the connection is closed
/// to decrement the connection count.
#[derive(Debug)]
pub struct UpstreamConnectionGuard {
    registry: Arc<UpstreamServerRegistry>,
    idx: usize,
}

impl UpstreamConnectionGuard {
    pub fn server(&self) -> &UpstreamServer {
        self.registry
            .get(self.idx)
            .expect("guard index came from this registry")
    }

    pub fn index(&self) -> usize {
        self.idx
    }

    pub fn registry(&self) -> &Arc<UpstreamServerRegistry> {
        &self.registry
    }
}

impl Drop for UpstreamConnectionGuard {
    fn drop(&mut self) {
        self.registry
            .state(self.idx)
            .connections
            .fetch_sub(1, Ordering::Relaxed);
    }
}

/// The adapter's current registry, replaceable at runtime.
#[derive(Debug, Default)]
pub struct SharedUpstreamRegistry {
    current: RwLock<Option<Arc<UpstreamServerRegistry>>>,
}

impl SharedUpstreamRegistry {
    pub fn new(servers: Option<UpstreamServers>) -> Self {
        Self {
            current: RwLock::new(servers.map(|s| Arc::new(UpstreamServerRegistry::new(s)))),
        }
    }

    /// The current registry, or `None` while no upstream is configured.
    pub fn current(&self) -> Option<Arc<UpstreamServerRegistry>> {
        self.current
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// Replace the list, carrying over state for servers that stay. Returns the new registry.
    pub fn replace(&self, servers: Option<UpstreamServers>) -> Option<Arc<UpstreamServerRegistry>> {
        let mut guard = self.current.write().unwrap_or_else(|e| e.into_inner());
        let next = match (guard.as_ref(), servers) {
            (_, None) => None,
            (Some(current), Some(servers)) if current.servers() == &servers => {
                return Some(Arc::clone(current));
            }
            (Some(current), Some(servers)) => Some(Arc::new(UpstreamServerRegistry::new_inherit(
                servers, current,
            ))),
            (None, Some(servers)) => Some(Arc::new(UpstreamServerRegistry::new(servers))),
        };
        *guard = next.clone();
        next
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn registry(list: &str) -> Arc<UpstreamServerRegistry> {
        Arc::new(UpstreamServerRegistry::new(
            list.parse().expect("list parses"),
        ))
    }

    const THREE: &str = "mysql://u:p@a/db|60,mysql://u:p@b/db|50,mysql://u:p@c/db|40";

    #[test]
    fn pick_skips_down_servers_and_honors_exclusions() {
        let r = registry(THREE);
        assert!(r.mark_down(1));
        for _ in 0..200 {
            assert_ne!(r.pick(), 1);
            assert_eq!(r.pick_excluding(&[0]), Some(2));
        }
        assert_eq!(r.pick_excluding(&[0, 1, 2]), None);
    }

    #[test]
    fn pick_reaches_every_healthy_server() {
        let r = registry(THREE);
        let mut seen = [false; 3];
        for _ in 0..500 {
            seen[r.pick()] = true;
        }
        assert_eq!(seen, [true, true, true]);
    }

    #[test]
    fn all_down_still_returns_a_candidate() {
        let r = registry(THREE);
        for i in 0..3 {
            r.mark_down(i);
        }
        assert!(r.healthy().is_empty());
        let picked = r.pick();
        assert!(picked < 3);
        assert_eq!(r.pick_excluding(&[0, 1]), Some(2));
    }

    #[test]
    fn marks_flip_once_and_report_transitions() {
        let r = registry(THREE);
        assert!(r.is_up(0));
        assert!(r.mark_down(0));
        assert!(!r.mark_down(0), "a second failure does not flip again");
        assert!(!r.is_up(0));
        assert!(r.mark_up(0));
        assert!(
            !r.mark_up(0),
            "an up server marked up again is not a transition"
        );
        assert!(r.is_up(0));
        let state = r.state(0);
        assert!(state.down_since().is_none());
        assert!(state.last_ok().is_some());
    }

    #[test]
    fn probe_backoff_doubles_up_to_the_cap() {
        let secs = Duration::from_secs;
        assert_eq!(probe_backoff(1), secs(2));
        assert_eq!(probe_backoff(2), secs(4));
        assert_eq!(probe_backoff(5), secs(32));
        assert_eq!(probe_backoff(6), PROBE_BACKOFF_MAX);
        assert_eq!(probe_backoff(100), PROBE_BACKOFF_MAX);
    }

    #[test]
    fn repeat_mark_down_keeps_the_backoff_and_mark_up_clears_it() {
        let r = registry(THREE);
        r.mark_down(2);
        assert!(!r.probe_due(2), "a fresh mark waits before the first probe");
        for _ in 0..10 {
            r.record_probe_failure(2);
        }
        let read = || r.state(2).next_probe.load(Ordering::Relaxed);
        let grown = read();
        r.mark_down(2);
        assert_eq!(read(), grown, "a repeat mark keeps the grown backoff");
        r.mark_up(2);
        assert!(!r.probe_due(2));
    }

    #[test]
    fn connection_guards_count_while_alive() {
        let r = registry(THREE);
        assert_eq!(r.connections(1), 0);
        let a = r.acquire(1);
        let b = r.acquire(1);
        assert_eq!(r.connections(1), 2);
        assert_eq!(a.server().display(), "b:3306");
        drop(a);
        assert_eq!(r.connections(1), 1);
        drop(b);
        assert_eq!(r.connections(1), 0);
    }

    #[test]
    fn replacement_inherits_state_for_servers_that_stay() {
        let shared = SharedUpstreamRegistry::new(Some(THREE.parse().unwrap()));
        let first = shared.current().unwrap();
        first.mark_down(2);
        let held = first.acquire(0);

        let next = shared
            .replace(Some(
                "mysql://u:p@c/db|40,mysql://u:p@a/db|60,mysql://u:p@d/db|10"
                    .parse()
                    .unwrap(),
            ))
            .unwrap();
        assert_eq!(
            next.servers().displays(),
            vec!["a:3306", "c:3306", "d:3306"]
        );
        assert!(next.is_up(0));
        assert_eq!(next.connections(0), 1);
        assert!(!next.is_up(1));
        assert!(next.is_up(2));
        assert!(Arc::ptr_eq(&shared.current().unwrap(), &next));

        let same = shared.replace(Some(next.servers().clone())).unwrap();
        assert!(Arc::ptr_eq(&same, &next));

        drop(held);
        assert_eq!(next.connections(0), 0);
        first.mark_up(2);
        assert!(next.is_up(1));
    }

    #[test]
    fn shared_registry_starts_and_ends_empty_when_no_upstream_is_configured() {
        let shared = SharedUpstreamRegistry::default();
        assert!(shared.current().is_none());
        let registry = shared.replace(Some(THREE.parse().unwrap())).unwrap();
        assert_eq!(registry.len(), 3);
        assert!(shared.replace(None).is_none());
        assert!(shared.current().is_none());
    }
}
