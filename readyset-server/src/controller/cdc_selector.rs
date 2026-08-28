//! Which server of the CDC list replication is using, and when it moves to the next one.
//!
//! The selector starts at the highest-weight server and moves down the list once a server's
//! retries are used up, wrapping at the end. It does not move back up on its own. It follows
//! its server across list edits, and restarts at the top when that server leaves the list.

use database_utils::{UpstreamServer, UpstreamServers};

#[derive(Debug)]
pub(super) struct CdcServerSelector {
    /// `host:port` of the server in use, or `None` before the first pick.
    current: Option<String>,
    /// Consecutive attempts against the current server that failed before it streamed.
    failures: u32,
    /// Failed attempts allowed after the first before moving on.
    retries: u32,
}

impl CdcServerSelector {
    pub(super) fn new(retries: u32) -> Self {
        Self {
            current: None,
            failures: 0,
            retries,
        }
    }

    /// The server replication should use now.
    pub(super) fn current<'a>(&mut self, servers: &'a UpstreamServers) -> &'a UpstreamServer {
        let index = self.index(servers);
        servers.get(index).expect("index is within the list")
    }

    /// Position of the current server in `servers`. If it left the list, the selector restarts
    /// at the top.
    fn index(&mut self, servers: &UpstreamServers) -> usize {
        if let Some(index) = self.current.as_deref().and_then(|d| servers.position(d)) {
            return index;
        }
        let top = servers.get(0).expect("the list is never empty");
        self.current = Some(top.display().to_string());
        self.failures = 0;
        0
    }

    /// The current server streamed the binlog.
    pub(super) fn record_success(&mut self) {
        self.failures = 0;
    }

    /// An attempt against the current server failed before it streamed. Returns the failover if
    /// the selector moved on.
    pub(super) fn record_failure<'a>(
        &mut self,
        servers: &'a UpstreamServers,
    ) -> Option<&'a UpstreamServer> {
        let index = self.index(servers);
        self.failures += 1;
        if self.failures <= self.retries || servers.is_single() {
            return None;
        }
        let next = servers
            .get((index + 1) % servers.len())
            .expect("index is within the list");
        self.current = Some(next.display().to_string());
        self.failures = 0;
        Some(next)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn servers(list: &str) -> UpstreamServers {
        list.parse().expect("list parses")
    }

    #[test]
    fn starts_at_the_highest_weight_and_walks_down_after_the_budget() {
        let list = servers("mysql://u:p@a/db|10,mysql://u:p@b/db|90,mysql://u:p@c/db|50");
        let mut selector = CdcServerSelector::new(2);
        assert_eq!(selector.current(&list).display(), "b:3306");
        assert_eq!(selector.record_failure(&list), None);
        assert_eq!(selector.record_failure(&list), None);
        assert_eq!(
            selector.record_failure(&list).map(|s| s.display()),
            Some("c:3306")
        );
        assert_eq!(selector.current(&list).display(), "c:3306");
        assert_eq!(selector.record_failure(&list), None);
        selector.record_success();
        assert_eq!(selector.record_failure(&list), None);
        assert_eq!(selector.record_failure(&list), None);
        assert_eq!(
            selector.record_failure(&list).map(|s| s.display()),
            Some("a:3306")
        );
    }

    #[test]
    fn wraps_at_the_end_instead_of_running_out() {
        let list = servers("mysql://u:p@a/db|90,mysql://u:p@b/db|50");
        let mut selector = CdcServerSelector::new(0);
        assert_eq!(selector.record_failure(&list).unwrap().display(), "b:3306");
        assert_eq!(selector.record_failure(&list).unwrap().display(), "a:3306");
    }

    #[test]
    fn a_single_server_is_retried_forever() {
        let list = servers("mysql://u:p@a/db");
        let mut selector = CdcServerSelector::new(0);
        for _ in 0..5 {
            assert_eq!(selector.record_failure(&list), None);
        }
        assert_eq!(selector.current(&list).display(), "a:3306");
    }

    #[test]
    fn follows_the_server_across_list_edits() {
        let list = servers("mysql://u:p@a/db|90,mysql://u:p@b/db|50");
        let mut selector = CdcServerSelector::new(0);
        selector.record_failure(&list);
        assert_eq!(selector.current(&list).display(), "b:3306");

        let edited = servers("mysql://u:p@b/db|95,mysql://u:p@a/db|90,mysql://u:p@c/db|10");
        assert_eq!(selector.current(&edited).display(), "b:3306");

        let without_b = servers("mysql://u:p@a/db|90,mysql://u:p@c/db|10");
        assert_eq!(selector.current(&without_b).display(), "a:3306");
    }
}
