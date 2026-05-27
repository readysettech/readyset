//! Catalog poller.
//!
//! Periodically fingerprints `pg_policy` / `pg_class` / `pg_roles` (three
//! independent hashes), diffs each tick against the previous one, and applies
//! surgical reload events to the [`PolicyRegistry`]: it reloads only the
//! relations and roles whose fingerprint changed, then routes invalidation
//! events into the shallow cache through the [`crate::InvalidationSink`].

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use metrics::{counter, gauge};
use thiserror::Error;
use tokio_postgres::types::Oid as PgOid;
use tracing::{info, warn};

use crate::policy_registry::PolicyRegistry;
use crate::types::RlsConfig;

/// SQL for the three fingerprint queries. The diff helper compares old vs new maps keyed by OID.
pub(crate) mod sql {
    /// Per-relation hash over all of `pg_policy`'s columns that affect evaluation. A change in
    /// policy text, role list, command type, or permissive flag bumps the relation's hash.
    // `pg_policy.polcmd` is Postgres's internal `\"char\"` type with no implicit cast to text, so
    // `||` rejects it without an explicit `::text`.
    pub(crate) const POLICY_FINGERPRINT: &str = "\
        SELECT p.polrelid, \
               md5(string_agg( \
                 p.oid::text || ':' || p.polname::text || ':' || \
                 p.polcmd::text || ':' || \
                 p.polpermissive::text || ':' || p.polroles::text || ':' || \
                 coalesce(pg_get_expr(p.polqual,      p.polrelid), '') || ':' || \
                 coalesce(pg_get_expr(p.polwithcheck, p.polrelid), ''), \
                 ',' ORDER BY p.oid)) AS policy_hash \
        FROM   pg_catalog.pg_policy p \
        JOIN   pg_catalog.pg_class  c ON c.oid = p.polrelid \
        WHERE  c.relnamespace NOT IN \
               (SELECT oid FROM pg_catalog.pg_namespace \
                WHERE  nspname IN ('pg_catalog','information_schema','pg_toast')) \
        GROUP BY p.polrelid";

    /// Per-table RLS-flag hash. A change in relrowsecurity / relforcerowsecurity bumps this
    /// relation's hash even when no policy text changed.
    pub(crate) const RLS_FLAG_FINGERPRINT: &str = "\
        SELECT c.oid, \
               md5(c.relrowsecurity::text || ':' || c.relforcerowsecurity::text) \
                 AS rls_flag_hash \
        FROM   pg_catalog.pg_class c \
        WHERE  c.relkind IN ('r','p','v','m') \
          AND  c.relnamespace NOT IN \
               (SELECT oid FROM pg_catalog.pg_namespace \
                WHERE  nspname IN ('pg_catalog','information_schema','pg_toast'))";

    /// Per-role attribute hash. `rolbypassrls` changes are the material RLS-affecting event;
    /// `rolsuper` flips bypass implicitly.
    pub(crate) const ROLE_FINGERPRINT: &str = "\
        SELECT oid, \
               md5(rolsuper::text || ':' || rolbypassrls::text) AS role_hash \
        FROM   pg_catalog.pg_roles";
}

#[derive(Debug, Error)]
pub enum PollerError {
    /// `tokio_postgres::Error`'s Display truncates to "db error"; extract the SQLSTATE and
    /// server-side message so operators see what actually went wrong.
    #[error("upstream query failed: {}", format_pg_error(.0))]
    Upstream(#[from] tokio_postgres::Error),
}

fn format_pg_error(e: &tokio_postgres::Error) -> String {
    match e.as_db_error() {
        Some(db) => format!(
            "{} (SQLSTATE {}{})",
            db.message(),
            db.code().code(),
            db.detail()
                .map(|d| format!(", detail: {d}"))
                .unwrap_or_default(),
        ),
        None => e.to_string(),
    }
}

/// A change event emitted by [`Fingerprints::diff`] when a poll detects DDL since the previous
/// tick.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChangeEvent {
    /// A relation's policy set changed.
    RelationPolicyChanged(crate::Oid),
    /// A role's `rolbypassrls` (or `rolsuper`) flipped.
    RoleChanged(crate::Oid),
    /// A table's RLS-on flag flipped.
    RlsFlagChanged(crate::Oid),
    /// A row that previously existed has been dropped.
    Removed { kind: ChangeKind, oid: crate::Oid },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangeKind {
    Policy,
    Role,
    RlsFlag,
}

/// A coalesced fingerprint snapshot for a single poll tick. The three maps are independent.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Fingerprints {
    pub policies: HashMap<PgOid, [u8; 16]>,
    pub rls_flags: HashMap<PgOid, [u8; 16]>,
    pub roles: HashMap<PgOid, [u8; 16]>,
}

impl Fingerprints {
    /// Fetch all three fingerprints in one connection round trip.
    pub async fn load(client: &tokio_postgres::Client) -> Result<Self, PollerError> {
        let policies =
            load_hash_map(client, sql::POLICY_FINGERPRINT, "polrelid", "policy_hash").await?;
        let rls_flags =
            load_hash_map(client, sql::RLS_FLAG_FINGERPRINT, "oid", "rls_flag_hash").await?;
        let roles = load_hash_map(client, sql::ROLE_FINGERPRINT, "oid", "role_hash").await?;
        Ok(Self {
            policies,
            rls_flags,
            roles,
        })
    }

    /// Diff `self` (previous) against `next` (current). Emits an event for every OID whose hash
    /// changed and every OID present in exactly one side.
    pub fn diff(&self, next: &Self) -> Vec<ChangeEvent> {
        let mut out = Vec::new();
        diff_map(
            &self.policies,
            &next.policies,
            ChangeEvent::RelationPolicyChanged,
            ChangeKind::Policy,
            &mut out,
        );
        diff_map(
            &self.rls_flags,
            &next.rls_flags,
            ChangeEvent::RlsFlagChanged,
            ChangeKind::RlsFlag,
            &mut out,
        );
        diff_map(
            &self.roles,
            &next.roles,
            ChangeEvent::RoleChanged,
            ChangeKind::Role,
            &mut out,
        );
        out
    }
}

fn diff_map(
    prev: &HashMap<PgOid, [u8; 16]>,
    next: &HashMap<PgOid, [u8; 16]>,
    on_change: impl Fn(crate::Oid) -> ChangeEvent,
    kind: ChangeKind,
    out: &mut Vec<ChangeEvent>,
) {
    for (oid, hash) in next {
        match prev.get(oid) {
            Some(prev_hash) if prev_hash == hash => {}
            _ => out.push(on_change(*oid)),
        }
    }
    for oid in prev.keys() {
        if !next.contains_key(oid) {
            out.push(ChangeEvent::Removed { kind, oid: *oid });
        }
    }
}

async fn load_hash_map(
    client: &tokio_postgres::Client,
    query: &str,
    oid_col: &str,
    hash_col: &str,
) -> Result<HashMap<PgOid, [u8; 16]>, PollerError> {
    let rows = client.query(query, &[]).await?;
    let mut map = HashMap::with_capacity(rows.len());
    for row in rows {
        let oid: PgOid = row.try_get(oid_col)?;
        let hash_text: String = row.try_get(hash_col)?;
        if let Some(hash) = parse_md5(&hash_text) {
            map.insert(oid, hash);
        }
    }
    Ok(map)
}

fn parse_md5(s: &str) -> Option<[u8; 16]> {
    if s.len() != 32 {
        return None;
    }
    let mut out = [0u8; 16];
    for i in 0..16 {
        let byte = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).ok()?;
        out[i] = byte;
    }
    Some(out)
}

/// Shared state the poller publishes for the metrics layer. Updated once per tick; metrics emit
/// from these fields so the `metrics` dependency stays out of the poller's loop hot path.
#[derive(Debug, Default)]
pub struct PollerHealth {
    /// Wall-clock instant of the most recent successful poll.
    last_success: parking_lot::Mutex<Option<Instant>>,
    /// Monotonic count of failed poll attempts. Fail-open: a failed tick does not invalidate the
    /// registry; operators alert on this counter growing.
    pub errors: std::sync::atomic::AtomicU64,
}

impl PollerHealth {
    /// Seconds since the last successful poll, or `u64::MAX` when no
    /// successful poll has yet completed.
    pub fn last_poll_age_seconds(&self) -> u64 {
        let guard = self.last_success.lock();
        match *guard {
            Some(t) => t.elapsed().as_secs(),
            None => u64::MAX,
        }
    }

    /// Seconds since the last successful poll, or `None` before the first successful poll. Callers
    /// suppress the gauge during the pre-initialised window so alerts don't fire on the
    /// `u64::MAX`-cast-to-f64 sentinel.
    pub fn last_poll_age_seconds_if_initialised(&self) -> Option<u64> {
        self.last_success.lock().map(|t| t.elapsed().as_secs())
    }
}

/// Drive a poll loop against `config.poll_interval`. Fail-open: a poll error increments
/// `health.errors` but does not drop the registry; operators alert on a growing
/// `last_poll_age_seconds`.
///
/// `reconnect` is invoked when the active client is closed (network blip, password rotation,
/// upstream restart). On a successful reconnect the loop re-runs the snapshot loader so the
/// registry catches up on the view-dependency and role-default-GUC changes the fingerprint diff
/// never sees, and retains the pre-outage fingerprints so the next tick's diff surgically applies
/// any policy, RLS-flag, or role change that landed during the outage.
///
/// `shutdown` stops the poller cleanly: when the watch receiver observes `true`, the current tick
/// (or sleep) completes and the loop returns, dropping the upstream client. Without it the runtime
/// would abort the task mid-query during graceful shutdown, leaving the upstream connection in an
/// undefined protocol state.
pub async fn run_poller<F, Fut>(
    initial_client: tokio_postgres::Client,
    registry: Arc<PolicyRegistry>,
    config: RlsConfig,
    health: Arc<PollerHealth>,
    sink: Option<Arc<dyn crate::InvalidationSink>>,
    mut reconnect: F,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<tokio_postgres::Client, crate::ConnectError>>,
{
    let mut client = initial_client;
    let mut prev = Fingerprints::default();
    // The bootstrap snapshot already loaded the current catalog, so the first fingerprint load is
    // the baseline, not a change set: seed `prev` from it without emitting events. Diffing an empty
    // `prev` would report every relation and role as "changed" on the first tick, reloading the
    // whole catalog for nothing.
    let mut seeded = false;
    loop {
        if *shutdown.borrow() {
            tracing::info!("RLS poller received shutdown signal; exiting");
            return;
        }
        if client.is_closed() {
            warn!("RLS upstream connection closed; reconnecting");
            tokio::select! {
                biased;
                _ = shutdown.changed() => {
                    tracing::info!("RLS poller shutdown during reconnect");
                    return;
                }
                result = reconnect() => match result {
                    Ok(fresh) => {
                        client = fresh;
                        // Reseed the full registry: the surgical diff path only reloads
                        // fingerprinted objects (policies, RLS flags, roles), so reconnect is our
                        // chance to also refresh view dependencies and role-default GUCs a diff
                        // never observes. Retain the pre-outage fingerprints in `prev` so the next
                        // tick diffs against them and drives any change that landed during the
                        // outage through `apply_events`, refreshing descriptors before bumping the
                        // generation. Keeping `prev` is also what purges a relation dropped during
                        // the outage: `load_snapshot` only inserts and updates, so the diff-driven
                        // `Removed` path is the only thing that removes it. Bumping the generation
                        // here would publish it while descriptors are still stale, pairing the
                        // fresh generation with an old partition and colliding tenants.
                        if let Err(e) = crate::loader::load_snapshot(&client, &registry).await {
                            health
                                .errors
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            counter!(metric::RLS_POLL_ERRORS_TOTAL).increment(1);
                            warn!(error = %e, "RLS snapshot reseed failed after reconnect");
                            if sleep_until_shutdown(&mut shutdown, config.poll_interval).await {
                                return;
                            }
                            continue;
                        }
                        *health.last_success.lock() = Some(Instant::now());
                    }
                    Err(e) => {
                        health
                            .errors
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        counter!(metric::RLS_POLL_ERRORS_TOTAL).increment(1);
                        warn!(error = %e, "RLS reconnect failed");
                        if sleep_until_shutdown(&mut shutdown, config.poll_interval).await {
                            return;
                        }
                        continue;
                    }
                }
            }
        }

        tokio::select! {
            biased;
            _ = shutdown.changed() => {
                tracing::info!("RLS poller shutdown during poll");
                return;
            }
            result = Fingerprints::load(&client) => match result {
                Ok(next) => {
                    // First load after start / reconnect is the baseline, not a change set.
                    let events = if seeded { prev.diff(&next) } else { Vec::new() };
                    match apply_events(&client, &registry, &events, sink.as_deref()).await {
                        Ok(()) => {
                            prev = next;
                            seeded = true;
                            *health.last_success.lock() = Some(Instant::now());
                        }
                        Err(()) => {
                            // A surgical reload failed. Keep `prev` at the old snapshot so the
                            // next tick re-emits the same events and retries; the generation is
                            // not bumped, so caches keep serving the last-known-good state.
                            health
                                .errors
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            counter!(metric::RLS_POLL_ERRORS_TOTAL).increment(1);
                        }
                    }
                }
                Err(e) => {
                    health
                        .errors
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    counter!(metric::RLS_POLL_ERRORS_TOTAL).increment(1);
                    warn!(error = %e, "RLS catalog poll failed");
                }
            }
        }
        // Only emit `RLS_POLL_AGE_SECONDS` after the first successful poll: before that the value
        // is the `u64::MAX` sentinel, which renders as ~1.8e19 in Prometheus and breaks any alert
        // keyed on `age > N`. The paired `RLS_POLL_INITIALISED` gauge lets alerts gate on
        // initialisation.
        match health.last_poll_age_seconds_if_initialised() {
            Some(age) => {
                gauge!(metric::RLS_POLL_AGE_SECONDS).set(age as f64);
                gauge!(metric::RLS_POLL_INITIALISED).set(1.0);
            }
            None => {
                gauge!(metric::RLS_POLL_INITIALISED).set(0.0);
            }
        }
        if sleep_until_shutdown(&mut shutdown, config.poll_interval).await {
            return;
        }
    }
}

/// Sleep for `dur` or until `shutdown` fires, whichever is first. Returns `true` if shutdown fired
/// (caller should exit).
async fn sleep_until_shutdown(
    shutdown: &mut tokio::sync::watch::Receiver<bool>,
    dur: std::time::Duration,
) -> bool {
    tokio::select! {
        biased;
        _ = shutdown.changed() => true,
        _ = tokio::time::sleep(dur) => *shutdown.borrow(),
    }
}

/// Apply each [`ChangeEvent`] by reloading the affected row(s) into `registry` and bumping the
/// generation. The bump happens after every reload completes so cache lookups never observe a
/// stale registry slot under a new generation.
///
/// Returns `Err(())` when any surgical reload returned an upstream error. In that case the caller
/// must not advance its fingerprint snapshot: the failed events stay in the next tick's diff and
/// retry. The generation is left unchanged and `sink` is not invoked, so caches keep serving the
/// last-known-good state rather than invalidating against a partially-reloaded registry.
pub async fn apply_events(
    client: &tokio_postgres::Client,
    registry: &Arc<PolicyRegistry>,
    events: &[ChangeEvent],
    sink: Option<&dyn crate::InvalidationSink>,
) -> Result<(), ()> {
    if events.is_empty() {
        return Ok(());
    }
    let mut all_ok = true;
    for event in events {
        match event {
            ChangeEvent::RelationPolicyChanged(_)
            | ChangeEvent::Removed {
                kind: ChangeKind::Policy,
                ..
            } => counter!(metric::RLS_POLICY_RELOADS_TOTAL, "kind" => "relation").increment(1),
            ChangeEvent::RlsFlagChanged(_)
            | ChangeEvent::Removed {
                kind: ChangeKind::RlsFlag,
                ..
            } => counter!(metric::RLS_POLICY_RELOADS_TOTAL, "kind" => "rls_flag").increment(1),
            ChangeEvent::RoleChanged(_)
            | ChangeEvent::Removed {
                kind: ChangeKind::Role,
                ..
            } => counter!(metric::RLS_POLICY_RELOADS_TOTAL, "kind" => "role").increment(1),
        }
        let result = match event {
            ChangeEvent::RelationPolicyChanged(relid)
            | ChangeEvent::Removed {
                kind: ChangeKind::Policy | ChangeKind::RlsFlag,
                oid: relid,
            } => crate::loader::reload_relation(client, registry, *relid).await,
            ChangeEvent::RlsFlagChanged(relid) => {
                // A relrowsecurity flip means any pre-existing Plain cache against this table is
                // no longer safe to serve. Reload the registry slot so the analyzer sees the new
                // state on the next CREATE CACHE; the sink dispatch below drops the pre-existing
                // Plain caches via on_rls_flag_enabled.
                crate::loader::reload_relation(client, registry, *relid).await
            }
            ChangeEvent::RoleChanged(roleid)
            | ChangeEvent::Removed {
                kind: ChangeKind::Role,
                oid: roleid,
            } => crate::loader::reload_role(client, registry, *roleid).await,
        };
        if let Err(e) = result {
            warn!(?event, error = %e, "RLS surgical reload failed");
            all_ok = false;
        }
    }
    if !all_ok {
        // Leave the generation untouched so caches keep serving the last-known-good state; the
        // caller retries on the next tick.
        return Err(());
    }
    // Refresh cache descriptors (their keyed GUC sets) via the sink before publishing the
    // generation bump. The lookup path reads the generation (Acquire) before the descriptor, and
    // this bump is a Release store, so refreshing descriptors ahead of the bump guarantees a
    // concurrent lookup never pairs a freshly-bumped generation with a stale partition, which
    // would collide two sessions' partitions under one key and leak rows across tenants. The
    // poller is the sole writer of the generation, so the post-dispatch bump lands exactly at
    // this prospective value.
    let new_gen = registry.generation() + 1;
    if let Some(sink) = sink {
        for event in events {
            match event {
                ChangeEvent::RelationPolicyChanged(relid)
                | ChangeEvent::Removed {
                    kind: ChangeKind::Policy,
                    oid: relid,
                } => sink.on_relation_changed(*relid),
                ChangeEvent::RlsFlagChanged(relid)
                | ChangeEvent::Removed {
                    kind: ChangeKind::RlsFlag,
                    oid: relid,
                } => {
                    // Also drop pre-existing Plain caches that referenced this relation when it
                    // was not RLS-active. Scoped caches are handled by on_relation_changed's
                    // deps_gen bump path.
                    sink.on_relation_changed(*relid);
                    let now_rls_active = registry
                        .flags_for(*relid)
                        .map(|f| f.relrowsecurity)
                        .unwrap_or(false);
                    if now_rls_active {
                        sink.on_rls_flag_enabled(*relid);
                    }
                }
                ChangeEvent::RoleChanged(roleid)
                | ChangeEvent::Removed {
                    kind: ChangeKind::Role,
                    oid: roleid,
                } => sink.on_role_changed(*roleid),
            }
        }
    }

    // Publish the generation only now that every descriptor refresh above has landed. The Release
    // store synchronizes with the lookup path's Acquire load, so a reader that observes this
    // generation also observes the refreshed descriptors.
    let bumped = registry.bump_generation();
    debug_assert_eq!(bumped, new_gen, "poller is the sole generation writer");

    // One aggregate line per poll rather than one per relation/role, so a bulk catalog change
    // stays a single log line.
    let mut relations: Vec<crate::Oid> = events
        .iter()
        .filter_map(|e| match e {
            ChangeEvent::RelationPolicyChanged(oid)
            | ChangeEvent::RlsFlagChanged(oid)
            | ChangeEvent::Removed {
                kind: ChangeKind::Policy | ChangeKind::RlsFlag,
                oid,
            } => Some(*oid),
            _ => None,
        })
        .collect();
    relations.sort_unstable();
    relations.dedup();
    let mut roles: Vec<crate::Oid> = events
        .iter()
        .filter_map(|e| match e {
            ChangeEvent::RoleChanged(oid)
            | ChangeEvent::Removed {
                kind: ChangeKind::Role,
                oid,
            } => Some(*oid),
            _ => None,
        })
        .collect();
    roles.sort_unstable();
    roles.dedup();
    info!(
        generation = new_gen,
        relation_count = relations.len(),
        role_count = roles.len(),
        ?relations,
        ?roles,
        "RLS catalog change applied; reloaded affected relations/roles and invalidated caches"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hash_with(seed: u8) -> [u8; 16] {
        let mut h = [0u8; 16];
        for (i, b) in h.iter_mut().enumerate() {
            *b = seed.wrapping_add(i as u8);
        }
        h
    }

    #[test]
    fn empty_fingerprints_emit_no_events() {
        let a = Fingerprints::default();
        let b = Fingerprints::default();
        assert!(a.diff(&b).is_empty());
    }

    #[test]
    fn changed_policy_hash_emits_relation_policy_changed() {
        let mut prev = Fingerprints::default();
        prev.policies.insert(42, hash_with(1));
        let mut next = Fingerprints::default();
        next.policies.insert(42, hash_with(2));
        let events = prev.diff(&next);
        assert_eq!(events, vec![ChangeEvent::RelationPolicyChanged(42)]);
    }

    #[test]
    fn rls_flag_flip_emits_rls_flag_changed() {
        let mut prev = Fingerprints::default();
        prev.rls_flags.insert(7, hash_with(0));
        let mut next = Fingerprints::default();
        next.rls_flags.insert(7, hash_with(1));
        let events = prev.diff(&next);
        assert_eq!(events, vec![ChangeEvent::RlsFlagChanged(7)]);
    }

    #[test]
    fn role_change_emits_role_changed() {
        let mut prev = Fingerprints::default();
        prev.roles.insert(100, hash_with(0));
        let mut next = Fingerprints::default();
        next.roles.insert(100, hash_with(5));
        let events = prev.diff(&next);
        assert_eq!(events, vec![ChangeEvent::RoleChanged(100)]);
    }

    #[test]
    fn dropped_relation_emits_removed_event() {
        let mut prev = Fingerprints::default();
        prev.policies.insert(99, hash_with(3));
        let next = Fingerprints::default();
        let events = prev.diff(&next);
        assert_eq!(
            events,
            vec![ChangeEvent::Removed {
                kind: ChangeKind::Policy,
                oid: 99,
            }]
        );
    }

    #[test]
    fn parse_md5_round_trip() {
        let h = [
            0xab, 0xcd, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x10, 0x20, 0x30, 0x40,
            0x50, 0x60,
        ];
        let hex = h.iter().map(|b| format!("{b:02x}")).collect::<String>();
        assert_eq!(parse_md5(&hex), Some(h));
        assert_eq!(parse_md5("not-hex"), None);
        assert_eq!(parse_md5(""), None);
    }

    #[test]
    fn fresh_health_reports_max_age() {
        let h = PollerHealth::default();
        assert_eq!(h.last_poll_age_seconds(), u64::MAX);
    }
}
