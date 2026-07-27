//! Shared, dependency-light types for the RLS crate.

use std::time::Duration;

/// Configuration knobs the operator can tune for catalog polling. Built from
/// `--rls-poll-interval-secs` (default 60s) and passed to the poller.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RlsConfig {
    /// Interval between catalog fingerprint scans. Clamped to
    /// `[1s, 24h]` at construction time.
    pub poll_interval: Duration,
}

impl Default for RlsConfig {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_secs(60),
        }
    }
}

impl RlsConfig {
    /// Construct an `RlsConfig`, clamping `poll_interval` into the
    /// `[1s, 24h]` range to keep the poller from melting upstream
    /// catalogs with sub-second scans or going effectively-disabled.
    pub fn with_poll_interval(mut self, interval: Duration) -> Self {
        let min = Duration::from_secs(1);
        let max = Duration::from_secs(24 * 60 * 60);
        self.poll_interval = interval.clamp(min, max);
        if self.poll_interval != interval {
            tracing::warn!(
                requested = ?interval,
                effective = ?self.poll_interval,
                "RLS catalog poll interval outside the supported [1s, 24h] range; clamped"
            );
        }
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn poll_interval_clamps_to_supported_range() {
        assert_eq!(RlsConfig::default().poll_interval, Duration::from_secs(60));

        let one_sec = Duration::from_secs(1);
        let one_day = Duration::from_secs(24 * 60 * 60);
        assert_eq!(
            RlsConfig::default()
                .with_poll_interval(Duration::ZERO)
                .poll_interval,
            one_sec
        );
        assert_eq!(
            RlsConfig::default()
                .with_poll_interval(Duration::from_secs(u64::from(u32::MAX)))
                .poll_interval,
            one_day
        );
        assert_eq!(
            RlsConfig::default()
                .with_poll_interval(Duration::from_secs(5))
                .poll_interval,
            Duration::from_secs(5)
        );
    }
}

/// A session-scoped input a policy expression reads. The analyzer emits
/// the set of these a scoped cache must fold into its lookup key; the
/// adapter resolves each against the session's state at lookup time.
///
/// `Ord` provides the canonical ordering the cache key uses so two equal
/// input sets always produce equal partitions.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SessionInputType {
    /// A claim inside the `request.jwt.claims` JSON blob, addressed by
    /// path segments (e.g. `["app_metadata", "tenant"]`). Empty segments
    /// address the whole blob. Segments carry claim keys verbatim, so
    /// OIDC namespace keys like `https://example.com/role` stay a single
    /// segment and cannot alias a nested path.
    JwtClaim(Box<[Box<str>]>),
    /// An allowlisted GUC read via `current_setting`, e.g.
    /// `request.jwt.claim.sub` or `request.method`.
    Guc(Box<str>),
}

impl SessionInputType {
    pub fn jwt_claim(segments: &[&str]) -> Self {
        Self::JwtClaim(segments.iter().map(|s| Box::from(*s)).collect())
    }

    pub fn guc(name: &str) -> Self {
        Self::Guc(name.into())
    }
}

/// Metadata for a function in the analyzer's hard-coded allowlist.
///
/// The set of allowlisted functions is fixed at compile time; only the in-tree
/// initialisers populate it.
#[derive(Debug, Clone)]
pub struct FunctionMeta {
    /// Schema-qualified function name, e.g. `auth.uid`.
    pub qualified_name: &'static str,
    /// Session inputs the function reads; folded into the cache key
    /// wherever the function appears in a policy.
    pub reads: Box<[SessionInputType]>,
}
