//! Choosing which upstream server an adapter-side connection goes to.
//!
//! The pick is uniform at random among healthy servers, ignoring weights, to spread the adapter's
//! connections evenly.

use std::future::Future;
use std::sync::Arc;

use database_utils::{
    SharedUpstreamRegistry, UpstreamConfig, UpstreamConnectionGuard, UpstreamFailure,
    UpstreamServer, UpstreamServerRegistry,
};
use metric::{UPSTREAM_CONNECT_FAILURES, UPSTREAM_SERVER_CONNECTIONS, UPSTREAM_SERVER_UP};
use metrics::{counter, gauge};
use tracing::{info, warn};

use crate::upstream_database::{IsFatalError, UpstreamDatabase};

/// The server a live upstream connection was picked from.
#[derive(Debug)]
pub struct PickedServer {
    guard: UpstreamConnectionGuard,
}

impl PickedServer {
    fn new(registry: &Arc<UpstreamServerRegistry>, idx: usize) -> Self {
        let guard = registry.acquire(idx);
        gauge!(UPSTREAM_SERVER_CONNECTIONS, "server" => guard.server().display().to_string())
            .increment(1.0);
        Self { guard }
    }

    pub fn server(&self) -> &UpstreamServer {
        self.guard.server()
    }

    /// Whether the server is still in the current list with the same URL.
    pub fn still_listed(&self, shared: &SharedUpstreamRegistry) -> bool {
        shared.current().is_some_and(|current| {
            current
                .servers()
                .iter()
                .any(|s| s.raw_url() == self.server().raw_url())
        })
    }
}

impl Drop for PickedServer {
    fn drop(&mut self) {
        gauge!(UPSTREAM_SERVER_CONNECTIONS, "server" => self.server().display().to_string())
            .decrement(1.0);
    }
}

/// Record a server-down failure against `idx`.
pub fn mark_down(registry: &UpstreamServerRegistry, idx: usize, error: &dyn std::fmt::Display) {
    let name = registry.get(idx).map(|s| s.display().to_string());
    let Some(name) = name else { return };
    counter!(
        UPSTREAM_CONNECT_FAILURES,
        "server" => name.clone(),
        "reason" => UpstreamFailure::Down.metric_reason()
    )
    .increment(1);
    if registry.mark_down(idx) {
        warn!(server = %name, %error, "Upstream server marked down");
        gauge!(UPSTREAM_SERVER_UP, "server" => name).set(0.0);
    }
}

/// Record that `idx` answered.
pub fn mark_up(registry: &UpstreamServerRegistry, idx: usize) {
    let Some(name) = registry.get(idx).map(|s| s.display().to_string()) else {
        return;
    };
    if registry.mark_up(idx) {
        info!(server = %name, "Upstream server is back up");
    }
    gauge!(UPSTREAM_SERVER_UP, "server" => name).set(1.0);
}

/// Pick servers from `registry` until `connect` succeeds. A server that fails with
/// [`UpstreamFailure::Down`] is marked down and skipped; any other error is returned right away,
/// since another server would fail the same way.
///
/// After the healthy servers, one more attempt ignores the down marks in case they are stale.
pub async fn connect_picked_with<T, E, F, Fut>(
    registry: &Arc<UpstreamServerRegistry>,
    classify: fn(&E) -> UpstreamFailure,
    mut connect: F,
) -> Result<(T, PickedServer), E>
where
    E: std::fmt::Display,
    F: FnMut(&UpstreamServer) -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    let mut tried = Vec::new();
    let mut last_error = None;
    let mut blind_pick_left = true;
    loop {
        let idx = match registry.pick_healthy_excluding(&tried) {
            Some(idx) => idx,
            None if blind_pick_left => {
                blind_pick_left = false;
                match registry.pick_excluding(&tried) {
                    Some(idx) => idx,
                    None => break,
                }
            }
            None => break,
        };
        let Some(server) = registry.get(idx) else {
            break;
        };
        match connect(server).await {
            Ok(conn) => {
                mark_up(registry, idx);
                return Ok((conn, PickedServer::new(registry, idx)));
            }
            Err(error) => {
                let failure = classify(&error);
                if failure != UpstreamFailure::Down {
                    counter!(
                        UPSTREAM_CONNECT_FAILURES,
                        "server" => server.display().to_string(),
                        "reason" => failure.metric_reason()
                    )
                    .increment(1);
                    return Err(error);
                }
                mark_down(registry, idx, &error);
                tried.push(idx);
                last_error = Some(error);
            }
        }
    }
    Err(last_error.expect("pick loop ran at least once"))
}

/// [`connect_picked_with`] for an [`UpstreamDatabase`].
pub async fn connect_picked<DB: UpstreamDatabase>(
    registry: &Arc<UpstreamServerRegistry>,
    config: &UpstreamConfig,
    username: Option<String>,
    password: Option<String>,
    interactive: bool,
) -> Result<(DB, PickedServer), DB::Error> {
    connect_picked_with(registry, IsFatalError::upstream_failure, |server| {
        DB::connect(
            config.for_server(server),
            username.clone(),
            password.clone(),
            interactive,
        )
    })
    .await
}

/// The shared registry if it has a list, else one built from `config`.
pub fn registry_for(
    shared: Option<&SharedUpstreamRegistry>,
    config: &UpstreamConfig,
) -> Option<Arc<UpstreamServerRegistry>> {
    if let Some(current) = shared.and_then(|s| s.current()) {
        return Some(current);
    }
    // The list is validated at startup and on every edit.
    let servers = config.upstream_servers().ok().flatten()?;
    Some(Arc::new(UpstreamServerRegistry::new(servers)))
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;

    #[derive(Debug)]
    struct TestError(UpstreamFailure, &'static str);

    impl std::fmt::Display for TestError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str(self.1)
        }
    }

    fn registry() -> Arc<UpstreamServerRegistry> {
        Arc::new(UpstreamServerRegistry::new(
            "mysql://u:p@a/db|60,mysql://u:p@b/db|50,mysql://u:p@c/db|40"
                .parse()
                .unwrap(),
        ))
    }

    #[tokio::test]
    async fn down_servers_are_marked_and_skipped_until_one_answers() {
        let registry = registry();
        let attempts = Mutex::new(Vec::new());
        let (conn, picked) = connect_picked_with(
            &registry,
            |e: &TestError| e.0,
            |server| {
                attempts.lock().unwrap().push(server.display().to_string());
                let ok = server.display() == "c:3306";
                async move {
                    if ok {
                        Ok("connected")
                    } else {
                        Err(TestError(UpstreamFailure::Down, "refused"))
                    }
                }
            },
        )
        .await
        .unwrap();
        assert_eq!(conn, "connected");
        assert_eq!(picked.server().display(), "c:3306");
        assert!(registry.is_up(2));
        assert_eq!(registry.connections(2), 1);
        let attempts = attempts.into_inner().unwrap();
        assert_eq!(attempts.last().map(String::as_str), Some("c:3306"));
        let mut unique = attempts.clone();
        unique.sort();
        unique.dedup();
        assert_eq!(unique.len(), attempts.len(), "{attempts:?}");
        for (idx, display) in ["a:3306", "b:3306"].into_iter().enumerate() {
            assert_eq!(registry.is_up(idx), !attempts.iter().any(|a| a == display));
        }
        drop(picked);
        assert_eq!(registry.connections(2), 0);
    }

    #[tokio::test]
    async fn auth_failures_stop_the_walk_without_marking_anything_down() {
        let registry = registry();
        let err = connect_picked_with(
            &registry,
            |e: &TestError| e.0,
            |_| async { Err::<(), _>(TestError(UpstreamFailure::Auth, "denied")) },
        )
        .await
        .unwrap_err();
        assert_eq!(err.1, "denied");
        assert_eq!(registry.healthy().len(), 3);
    }

    #[tokio::test]
    async fn every_server_down_returns_the_last_error() {
        let registry = registry();
        let err = connect_picked_with(
            &registry,
            |e: &TestError| e.0,
            |_| async { Err::<(), _>(TestError(UpstreamFailure::Down, "refused")) },
        )
        .await
        .unwrap_err();
        assert_eq!(err.1, "refused");
        assert!(registry.healthy().is_empty());
    }

    /// A list already known to be down costs one attempt, not one per server.
    #[tokio::test]
    async fn a_list_already_marked_down_costs_one_attempt() {
        let registry = registry();
        for idx in 0..registry.len() {
            registry.mark_down(idx);
        }
        let attempts = Mutex::new(0);
        let err = connect_picked_with(
            &registry,
            |e: &TestError| e.0,
            |_| {
                *attempts.lock().unwrap() += 1;
                async { Err::<(), _>(TestError(UpstreamFailure::Down, "refused")) }
            },
        )
        .await
        .unwrap_err();
        assert_eq!(err.1, "refused");
        assert_eq!(attempts.into_inner().unwrap(), 1);
    }

    #[tokio::test]
    async fn the_walk_takes_the_healthy_server_before_a_server_marked_down() {
        let registry = registry();
        registry.mark_down(0);
        registry.mark_down(1);
        let attempts = Mutex::new(Vec::new());
        connect_picked_with(
            &registry,
            |e: &TestError| e.0,
            |server| {
                attempts.lock().unwrap().push(server.display().to_string());
                async { Err::<(), _>(TestError(UpstreamFailure::Down, "refused")) }
            },
        )
        .await
        .unwrap_err();
        let attempts = attempts.into_inner().unwrap();
        assert_eq!(attempts.len(), 2, "{attempts:?}");
        assert_eq!(attempts[0], "c:3306");
        assert_ne!(attempts[1], attempts[0]);
    }

    #[test]
    fn still_listed_tracks_the_current_registry_by_url() {
        let shared =
            SharedUpstreamRegistry::new(Some("mysql://u:p@a/db,mysql://u:p@b/db".parse().unwrap()));
        let first = shared.current().unwrap();
        let picked = PickedServer::new(&first, 1);
        assert!(picked.still_listed(&shared));
        shared.replace(Some(
            "mysql://u:new@a/db,mysql://u:new@b/db".parse().unwrap(),
        ));
        assert!(!picked.still_listed(&shared));
        shared.replace(Some(
            "mysql://u:p@b/db|10,mysql://u:p@a/db".parse().unwrap(),
        ));
        assert!(picked.still_listed(&shared));
        shared.replace(None);
        assert!(!picked.still_listed(&shared));
    }
}
