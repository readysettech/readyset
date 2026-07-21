//! The cache-ACL freshness worker (REA-6708).
//!
//! A single background task drains [`AclMessage`]s: a periodic tick enqueues a
//! full pass (per-identity grant-fingerprint check, row re-probe on a flip,
//! then a fill of any cell still `Unknown`), and cache/user lifecycle events
//! converge the affected row or column immediately. Every probe runs on the
//! worker's own upstream connections, authenticated as the probed user with
//! the allowed-users store's credentials; client sessions are never borrowed
//! for authorization work.
//!
//! Connections live only for the message being handled: users are probed over
//! one connection cycled with `change_user` where the protocol supports it
//! (MySQL), or lazy per-user connections otherwise (Postgres), plus one
//! admin-credential connection that assumes roles around probes for role
//! rows. Steady state is one connection and one grants read per user per
//! interval; statement probes run only on a flip or a lifecycle event.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use database_utils::UpstreamConfig;
use metric::{
    CACHE_ACL_FINGERPRINT_FLIPS, CACHE_ACL_LOOP_RUN_TIME, CACHE_ACL_LOOP_RUNS,
    CACHE_ACL_PROBE_FAILURES, CACHE_ACL_STALENESS, CACHE_ACL_UNKNOWN_PAIRS,
    CACHE_ACL_VERDICT_FLIPS,
};
use metrics::{counter, gauge, histogram};
use readyset_client::query::QueryId;
use readyset_shallow::{CacheInfo, CacheManager};
use readyset_sql::Dialect;
use readyset_sql::DialectDisplay;
use readyset_sql::ast::{Relation, SqlIdentifier};
use readyset_sql_passes::shallow::{convert_placeholders_to_question_marks, max_placeholder_index};
use readyset_util::logging::*;
use tokio::sync::RwLock;
use tokio::sync::mpsc::{Receiver, UnboundedReceiver};
use tracing::warn;

use crate::backend::{AllowedUsers, READYSET_ACL_POOLER};
use crate::cache_acl::{AclMatrix, AclMessage, CacheCreator, PassTrigger, Verdict};
use crate::rls_relations::extract_referenced_relation_names;
use crate::shallow_key::ShallowKey;
use crate::upstream_database::{AclProbeOutcome, UpstreamDatabase};

/// Failed identities are retried with exponentially spaced passes, capped so a
/// dropped upstream account is re-checked at most this many passes apart.
const MAX_BACKOFF_PASSES: u64 = 32;

/// What the worker needs to probe one cache.
#[derive(Clone, Debug)]
struct ProbeTarget {
    cache: QueryId,
    /// The cached statement as submitted to the engine.
    sql: String,
    n_params: usize,
    path: Vec<SqlIdentifier>,
}

impl ProbeTarget {
    fn from_cache_info(info: &CacheInfo, dialect: Dialect) -> Self {
        // The pipeline stores statements with normalized `$n` placeholders;
        // Postgres prepares that form directly, MySQL needs `?` (the same
        // conversion `upstream_supports` applies before its prepare). The
        // count feeds the Postgres `EXPLAIN EXECUTE p(NULL, ...)`; the MySQL
        // probe is the prepare alone and ignores it.
        let n_params = max_placeholder_index(&info.query);
        let sql = match dialect {
            Dialect::MySQL => {
                let mut query = info.query.clone();
                convert_placeholders_to_question_marks(&mut query);
                query.display(dialect).to_string()
            }
            Dialect::PostgreSQL => info.query.display(dialect).to_string(),
        };
        Self {
            cache: info.query_id,
            sql,
            n_params,
            path: info.schema_search_path.clone(),
        }
    }
}

/// Exponential retry spacing, in full passes, for identities whose probe
/// session cannot open. Keeps a dropped upstream account from being hammered
/// with failed logins every interval.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct BackoffState {
    failures: u32,
    next_attempt_pass: u64,
}

impl BackoffState {
    fn should_attempt(&self, pass_seq: u64) -> bool {
        pass_seq >= self.next_attempt_pass
    }

    fn record_failure(&mut self, pass_seq: u64) {
        self.failures = self.failures.saturating_add(1);
        let wait = (1u64 << self.failures.min(63)).min(MAX_BACKOFF_PASSES);
        self.next_attempt_pass = pass_seq.saturating_add(wait);
    }
}

/// Order a column probe so the creator -- the identity most likely to read
/// the new cache first -- resolves before any other.
fn column_probe_order(
    mut users: Vec<SqlIdentifier>,
    creator: Option<&SqlIdentifier>,
) -> Vec<SqlIdentifier> {
    if let Some(creator) = creator
        && let Some(pos) = users.iter().position(|u| u == creator)
    {
        users.swap(0, pos);
    }
    users
}

/// The loop-runs metric tag for a message.
fn msg_trigger(msg: &AclMessage) -> &'static str {
    match msg {
        AclMessage::FullPass { trigger } => trigger.as_str(),
        AclMessage::CacheCreated { .. } => "cache_created",
        AclMessage::CacheDropped { .. } => "cache_dropped",
        AclMessage::UserAltered { .. } => "user_altered",
        AclMessage::UserDropped { .. } => "user_dropped",
        AclMessage::ResolveIdentity { .. } => "resolve_identity",
    }
}

/// Upstream connections for one message-handling call. Torn down when the
/// call finishes, so the worker holds no idle upstream sessions between
/// passes.
struct PassConns<DB: UpstreamDatabase> {
    config: UpstreamConfig,
    /// The `change_user`-cycled connection and the user it is currently
    /// authenticated as. Used when the protocol supports an in-session
    /// re-authenticating switch (MySQL).
    cycled: Option<(SqlIdentifier, DB)>,
    /// Lazy per-user connections for protocols without a user switch
    /// (Postgres).
    per_user: HashMap<SqlIdentifier, DB>,
    /// Admin-credential connection used to assume roles for role-row probes.
    admin: Option<DB>,
}

impl<DB: UpstreamDatabase> PassConns<DB> {
    fn new(mut config: UpstreamConfig) -> Self {
        config.program_name = Some(READYSET_ACL_POOLER.to_string());
        Self {
            config,
            cycled: None,
            per_user: HashMap::new(),
            admin: None,
        }
    }

    fn uses_change_user() -> bool {
        matches!(DB::SQL_DIALECT, Dialect::MySQL)
    }

    /// Open a connection and force the handshake to complete. `DB` may be a
    /// lazily-connecting wrapper whose `connect` only stores credentials; the
    /// A7 "session cannot open" signal requires the failure to surface here,
    /// not inside the first probe (where it would read as transient).
    async fn open(
        config: UpstreamConfig,
        user: Option<&SqlIdentifier>,
        password: Option<&str>,
    ) -> Result<DB, DB::Error> {
        let mut conn = DB::connect(
            config,
            user.map(ToString::to_string),
            password.map(ToString::to_string),
            false,
        )
        .await?;
        conn.is_connected().await?;
        Ok(conn)
    }

    /// A connection authenticated as `user`. `Err` means the session could
    /// not open -- the A7 "session cannot open" signal.
    async fn for_user(&mut self, user: &SqlIdentifier, password: &str) -> Result<&mut DB, ()> {
        if Self::uses_change_user() {
            match &mut self.cycled {
                Some((current, _)) if current == user => {}
                Some((current, conn)) => {
                    let database = conn.database().unwrap_or_default().to_string();
                    match conn.change_user(user.as_str(), password, &database).await {
                        Ok(()) => *current = user.clone(),
                        Err(e) => {
                            // The in-session switch can fail for reasons short of "this
                            // session cannot open" -- e.g. an auth plugin the driver only
                            // supports on a fresh handshake -- so a fresh connection as
                            // the user is the authoritative check.
                            rate_limit(true, ADAPTER_ACL_PROBE_CONNECT, || {
                                warn!(
                                    error = %e,
                                    user = %user,
                                    "ACL prober failed to switch user, reconnecting"
                                )
                            });
                            self.cycled = None;
                        }
                    }
                }
                None => {}
            }
            if self.cycled.is_none() {
                let conn = Self::open(self.config.clone(), Some(user), Some(password))
                    .await
                    .map_err(|e| {
                        rate_limit(
                            true,
                            ADAPTER_ACL_PROBE_CONNECT,
                            || warn!(error = %e, user = %user, "ACL prober failed to connect"),
                        );
                    })?;
                self.cycled = Some((user.clone(), conn));
            }
            Ok(&mut self.cycled.as_mut().expect("just ensured above").1)
        } else {
            if !self.per_user.contains_key(user) {
                let conn = Self::open(self.config.clone(), Some(user), Some(password))
                    .await
                    .map_err(|e| {
                        rate_limit(
                            true,
                            ADAPTER_ACL_PROBE_CONNECT,
                            || warn!(error = %e, user = %user, "ACL prober failed to connect"),
                        );
                    })?;
                self.per_user.insert(user.clone(), conn);
            }
            Ok(self.per_user.get_mut(user).expect("just inserted above"))
        }
    }

    /// The admin-credential connection role rows are probed through.
    async fn admin(&mut self) -> Result<&mut DB, ()> {
        if self.admin.is_none() {
            let conn = Self::open(self.config.clone(), None, None)
                .await
                .map_err(|e| {
                    rate_limit(
                        true,
                        ADAPTER_ACL_PROBE_CONNECT,
                        || warn!(error = %e, "ACL prober failed to open admin connection"),
                    );
                })?;
            self.admin = Some(conn);
        }
        Ok(self.admin.as_mut().expect("just ensured above"))
    }
}

/// The background freshness worker. See the module docs for the loop's
/// shape; all matrix writes happen here or in the opportunistic creator
/// write, and probe I/O always completes before the corresponding write.
pub struct AclWorker<DB: UpstreamDatabase> {
    matrix: Arc<AclMatrix>,
    /// Cache and user lifecycle events. Unbounded, so none is ever lost.
    lifecycle_rx: UnboundedReceiver<AclMessage>,
    /// Demand resolution from the serve path, at query rate and droppable.
    demand_rx: Receiver<AclMessage>,
    users: Arc<AllowedUsers>,
    upstream_config: Arc<RwLock<UpstreamConfig>>,
    shallow: Arc<CacheManager<ShallowKey, DB::CacheEntry>>,
    interval: Duration,
    /// Per-identity fingerprint and when it was last read. Private to the
    /// worker; the hot path never reads these.
    fingerprints: HashMap<SqlIdentifier, (u64, Instant)>,
    /// Per-identity connect-failure backoff, in pass counts.
    backoff: HashMap<SqlIdentifier, BackoffState>,
    /// Identities whose rows were denied because their probe session could
    /// not open. A connect failure can be transient, and the fingerprint
    /// alone cannot notice recovery (nothing about the grants changed), so
    /// the next successful fingerprint read re-probes these rows.
    connect_denied: std::collections::HashSet<SqlIdentifier>,
    /// Role rows the worker maintains: identities resolved on demand that are
    /// not allowed users. Discarded when their upstream state cannot be read
    /// for several consecutive passes (role dropped upstream).
    roles: HashMap<SqlIdentifier, RoleRowState>,
    pass_seq: u64,
}

/// Consecutive failed passes after which a role row is discarded.
const ROLE_DISCARD_FAILURES: u32 = 3;

/// Worker-side state for a role row.
#[derive(Debug, Default)]
struct RoleRowState {
    /// A login user whose upstream-accepted SET ROLE proved membership of the
    /// role. Their connection is the preferred probe vehicle: a non-superuser
    /// admin may not be able to assume the role at all, which would read as a
    /// spurious deny.
    via: Option<SqlIdentifier>,
    /// Consecutive passes whose fingerprint read failed; the row is discarded
    /// at [`ROLE_DISCARD_FAILURES`] (role dropped upstream).
    failures: u32,
}

impl<DB: UpstreamDatabase + 'static> AclWorker<DB> {
    pub fn new(
        matrix: Arc<AclMatrix>,
        lifecycle_rx: UnboundedReceiver<AclMessage>,
        demand_rx: Receiver<AclMessage>,
        users: Arc<AllowedUsers>,
        upstream_config: Arc<RwLock<UpstreamConfig>>,
        shallow: Arc<CacheManager<ShallowKey, DB::CacheEntry>>,
        interval: Duration,
    ) -> Self {
        Self {
            matrix,
            lifecycle_rx,
            demand_rx,
            users,
            upstream_config,
            shallow,
            interval,
            fingerprints: HashMap::new(),
            backoff: HashMap::new(),
            connect_denied: std::collections::HashSet::new(),
            roles: HashMap::new(),
            pass_seq: 0,
        }
    }

    /// Run until every [`crate::cache_acl::AclHandle`] sender is dropped.
    pub async fn run(mut self) {
        let mut tick = tokio::time::interval(self.interval);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // The first tick fires immediately: the initial fill after a restart.
        loop {
            let msg = tokio::select! {
                _ = tick.tick() => AclMessage::FullPass { trigger: PassTrigger::Periodic },
                msg = self.lifecycle_rx.recv() => match msg {
                    Some(msg) => msg,
                    None => break,
                },
                msg = self.demand_rx.recv() => match msg {
                    Some(msg) => msg,
                    None => break,
                },
            };
            let trigger = msg_trigger(&msg);
            counter!(CACHE_ACL_LOOP_RUNS, "trigger" => trigger).increment(1);
            let started = Instant::now();
            self.handle(msg).await;
            histogram!(CACHE_ACL_LOOP_RUN_TIME, "trigger" => trigger)
                .record(started.elapsed().as_micros() as f64);
            self.update_gauges();
        }
    }

    async fn handle(&mut self, msg: AclMessage) {
        match msg {
            AclMessage::FullPass { .. } => self.full_pass().await,
            AclMessage::CacheCreated { cache, creator } => self.probe_column(cache, creator).await,
            AclMessage::CacheDropped { cache } => self.matrix.discard_column(cache),
            AclMessage::UserAltered { user } => {
                self.fingerprints.remove(&user);
                self.backoff.remove(&user);
                self.connect_denied.remove(&user);
                self.matrix.discard_row(&user);
                let targets = self.probe_targets();
                let mut conns = self.pass_conns().await;
                self.probe_row(&mut conns, &user, &targets).await;
            }
            AclMessage::UserDropped { user } => {
                self.fingerprints.remove(&user);
                self.backoff.remove(&user);
                self.connect_denied.remove(&user);
                self.matrix.discard_row(&user);
            }
            AclMessage::ResolveIdentity { identity, via } => {
                self.resolve_identity(identity, via).await
            }
        }
    }

    async fn pass_conns(&self) -> PassConns<DB> {
        PassConns::new(self.upstream_config.read().await.clone())
    }

    fn probe_targets(&self) -> Vec<ProbeTarget> {
        self.shallow
            .list_caches(None, None)
            .iter()
            .map(|info| ProbeTarget::from_cache_info(info, DB::SQL_DIALECT))
            .collect()
    }

    /// The relations current caches reference, for fingerprint scoping.
    fn fingerprint_relations(&self) -> Vec<Relation> {
        let mut relations: Vec<Relation> = self
            .shallow
            .list_caches(None, None)
            .iter()
            .flat_map(|info| extract_referenced_relation_names(&info.query))
            .map(|(schema, name)| Relation {
                schema: schema.map(Into::into),
                name: name.into(),
            })
            .collect();
        relations.sort();
        relations.dedup();
        relations
    }

    /// The full pass: per-identity fingerprint check, row re-probe on a flip,
    /// then probe every cell still `Unknown`.
    async fn full_pass(&mut self) {
        self.pass_seq += 1;
        let targets = self.probe_targets();
        let relations = self.fingerprint_relations();
        let users = self.users.snapshot();
        let mut conns = self.pass_conns().await;

        // Drop matrix rows for identities that are neither allowed users nor
        // tracked roles (users removed while the worker was down).
        let live: Vec<SqlIdentifier> = self
            .matrix
            .snapshot()
            .into_iter()
            .map(|(identity, _, _)| identity)
            .collect();
        for identity in live {
            if !users.contains_key(identity.as_str()) && !self.roles.contains_key(&identity) {
                self.matrix.discard_row(&identity);
                self.fingerprints.remove(&identity);
                self.backoff.remove(&identity);
                self.connect_denied.remove(&identity);
            }
        }

        // Roles discovered as assumable by some user this pass. Complete
        // discovery (every user read) licenses pruning roles nobody can
        // assume anymore; a partial pass must not mass-discard live rows.
        let mut discovered: HashSet<SqlIdentifier> = HashSet::new();
        let mut discovery_complete = true;
        for (user, password) in &users {
            let user = SqlIdentifier::from(user.as_str());
            if !self
                .backoff
                .get(&user)
                .copied()
                .unwrap_or_default()
                .should_attempt(self.pass_seq)
            {
                discovery_complete = false;
                continue;
            }
            let Ok(conn) = conns.for_user(&user, password).await else {
                self.mark_row_denied(&user, &targets);
                discovery_complete = false;
                continue;
            };
            let fingerprint = conn.grant_fingerprint(&relations, None).await;
            // Discover the roles this user could assume, so their rows are
            // probed before any session's first read as that role and the
            // member vehicle is known without a resolve message.
            let assumable = match &fingerprint {
                Ok(_) => match conn.assumable_roles().await {
                    Ok(assumable) => assumable,
                    Err(e) => {
                        counter!(CACHE_ACL_PROBE_FAILURES, "kind" => "fingerprint").increment(1);
                        rate_limit(
                            true,
                            ADAPTER_ACL_FINGERPRINT,
                            || warn!(error = %e, user = %user, "ACL role discovery failed"),
                        );
                        discovery_complete = false;
                        Vec::new()
                    }
                },
                Err(_) => Vec::new(),
            };
            for role in assumable {
                let role = SqlIdentifier::from(role.as_str());
                // Roles that are themselves allowed login users already have
                // a user row.
                if users.contains_key(role.as_str()) {
                    continue;
                }
                discovered.insert(role.clone());
                let state = self.roles.entry(role).or_default();
                if state.via.is_none() {
                    state.via = Some(user.clone());
                }
            }
            match fingerprint {
                Ok(fingerprint) => {
                    self.backoff.remove(&user);
                    // A row force-denied by a connect failure recovers here:
                    // the probe session opens again, so re-probe even though
                    // the fingerprint (the user's grants) never changed.
                    let flipped = self.connect_denied.remove(&user)
                        || match self.fingerprints.get(&user) {
                            Some((prior, _)) => *prior != fingerprint,
                            None => false,
                        };
                    self.fingerprints
                        .insert(user.clone(), (fingerprint, Instant::now()));
                    if flipped {
                        counter!(CACHE_ACL_FINGERPRINT_FLIPS).increment(1);
                        self.matrix.discard_row(&user);
                        self.probe_row(&mut conns, &user, &targets).await;
                    }
                }
                Err(e) => {
                    counter!(CACHE_ACL_PROBE_FAILURES, "kind" => "fingerprint").increment(1);
                    rate_limit(
                        true,
                        ADAPTER_ACL_FINGERPRINT,
                        || warn!(error = %e, user = %user, "ACL fingerprint read failed"),
                    );
                    discovery_complete = false;
                }
            }
        }

        // Prune role rows no allowed user can assume anymore. Demand-created
        // rows reappear on the next resolve message if a live session still
        // uses one.
        if discovery_complete {
            let stale: Vec<SqlIdentifier> = self
                .roles
                .keys()
                .filter(|role| !discovered.contains(*role))
                .cloned()
                .collect();
            for role in stale {
                self.roles.remove(&role);
                self.fingerprints.remove(&role);
                self.backoff.remove(&role);
                self.connect_denied.remove(&role);
                self.matrix.discard_row(&role);
            }
        }

        // Refresh role rows the same way, preferring a member's connection.
        let roles: Vec<SqlIdentifier> = self.roles.keys().cloned().collect();
        for role in roles {
            let member = self.prepare_role_member(&mut conns, &role).await;
            let conn = match &member {
                Some((user, password)) => match conns.for_user(user, password).await {
                    Ok(conn) => conn,
                    Err(()) => continue,
                },
                None => match conns.admin().await {
                    Ok(conn) => conn,
                    Err(()) => break,
                },
            };
            match conn
                .grant_fingerprint(&relations, Some(role.as_str()))
                .await
            {
                Ok(fingerprint) => {
                    self.roles.entry(role.clone()).or_default().failures = 0;
                    let flipped = match self.fingerprints.get(&role) {
                        Some((prior, _)) => *prior != fingerprint,
                        None => false,
                    };
                    self.fingerprints
                        .insert(role.clone(), (fingerprint, Instant::now()));
                    if flipped {
                        counter!(CACHE_ACL_FINGERPRINT_FLIPS).increment(1);
                        self.matrix.discard_row(&role);
                        self.probe_role_row(&mut conns, &role, &targets).await;
                    }
                }
                Err(e) => {
                    counter!(CACHE_ACL_PROBE_FAILURES, "kind" => "fingerprint").increment(1);
                    rate_limit(
                        true,
                        ADAPTER_ACL_FINGERPRINT,
                        || warn!(error = %e, role = %role, "ACL role fingerprint read failed"),
                    );
                    let state = self.roles.entry(role.clone()).or_default();
                    state.failures += 1;
                    if state.failures >= ROLE_DISCARD_FAILURES {
                        self.roles.remove(&role);
                        self.fingerprints.remove(&role);
                        self.matrix.discard_row(&role);
                    }
                }
            }
        }

        // Unknown fill: initial fill after a restart, retry after a transient
        // probe failure.
        let mut identities: Vec<SqlIdentifier> = users
            .keys()
            .map(|u| SqlIdentifier::from(u.as_str()))
            .collect();
        identities.extend(self.roles.keys().cloned());
        let caches: Vec<QueryId> = targets.iter().map(|t| t.cache).collect();
        for (identity, cache) in self.matrix.unknown_pairs(&identities, &caches) {
            if !self
                .backoff
                .get(&identity)
                .copied()
                .unwrap_or_default()
                .should_attempt(self.pass_seq)
            {
                continue;
            }
            let Some(target) = targets.iter().find(|t| t.cache == cache) else {
                continue;
            };
            if let Some(password) = users.get(identity.as_str()) {
                let password = password.clone();
                self.probe_cell_as_user(&mut conns, &identity, &password, target)
                    .await;
            } else {
                self.probe_cell_as_role(&mut conns, &identity, target).await;
            }
        }
    }

    /// Probe one cache's column across all users, the creator first (A6).
    async fn probe_column(&mut self, cache: QueryId, creator: Option<CacheCreator>) {
        let Some(target) = self
            .shallow
            .list_caches(Some(cache), None)
            .first()
            .map(|info| ProbeTarget::from_cache_info(info, DB::SQL_DIALECT))
        else {
            return;
        };
        let users = self.users.snapshot();
        let identities: Vec<SqlIdentifier> = users
            .keys()
            .map(|u| SqlIdentifier::from(u.as_str()))
            .collect();
        let mut conns = self.pass_conns().await;
        for identity in column_probe_order(identities, creator.as_ref().map(|c| &c.identity)) {
            let Some(password) = users.get(identity.as_str()).cloned() else {
                continue;
            };
            self.probe_cell_as_user(&mut conns, &identity, &password, &target)
                .await;
        }
        // A creator that is not an allowed user is a role assumed via SET ROLE. Nothing
        // else establishes its cell before the next full pass, so start its row here,
        // through the member whose accepted SET ROLE proved membership.
        if let Some(creator) = creator
            && !users.contains_key(creator.identity.as_str())
            && !PassConns::<DB>::uses_change_user()
        {
            let state = self.roles.entry(creator.identity.clone()).or_default();
            if state.via.is_none() {
                state.via = creator.via;
            }
            self.probe_cell_as_role(&mut conns, &creator.identity, &target)
                .await;
        }
    }

    /// Resolve an identity the serve path saw with no row: a known user gets
    /// a row probe on their own connection; anything else is treated as a
    /// role and probed through a member's connection where one is known,
    /// the admin connection otherwise. Backoff-deduplicated.
    async fn resolve_identity(&mut self, identity: SqlIdentifier, via: Option<SqlIdentifier>) {
        if self.matrix.has_row(&identity) {
            return;
        }
        if !self
            .backoff
            .get(&identity)
            .copied()
            .unwrap_or_default()
            .should_attempt(self.pass_seq)
        {
            return;
        }
        let targets = self.probe_targets();
        let mut conns = self.pass_conns().await;
        if self.users.password_for(identity.as_str()).is_some() {
            self.probe_row(&mut conns, &identity, &targets).await;
            return;
        }
        // Role rows only exist on upstreams with an in-session role switch.
        if PassConns::<DB>::uses_change_user() {
            return;
        }
        let state = self.roles.entry(identity.clone()).or_default();
        if state.via.is_none() {
            state.via = via;
        }
        self.probe_role_row(&mut conns, &identity, &targets).await;
        if !self.matrix.has_row(&identity) {
            // The probe produced nothing (connect failure or transient
            // errors); back off so query-rate misses cannot storm upstream.
            self.backoff
                .entry(identity)
                .or_default()
                .record_failure(self.pass_seq);
        }
    }

    /// Probe a user's whole row on their own connection.
    async fn probe_row(
        &mut self,
        conns: &mut PassConns<DB>,
        user: &SqlIdentifier,
        targets: &[ProbeTarget],
    ) {
        let Some(password) = self.users.password_for(user.as_str()) else {
            return;
        };
        for target in targets {
            self.probe_cell_as_user(conns, user, &password, target)
                .await;
        }
    }

    /// Prepare and return the member credentials for probing `role`, if a
    /// member is known and their probe session opens.
    async fn prepare_role_member(
        &self,
        conns: &mut PassConns<DB>,
        role: &SqlIdentifier,
    ) -> Option<(SqlIdentifier, String)> {
        let via = self.roles.get(role)?.via.clone()?;
        let password = self.users.password_for(via.as_str())?;
        conns.for_user(&via, &password).await.ok()?;
        Some((via, password))
    }

    /// Probe a role's whole row.
    async fn probe_role_row(
        &mut self,
        conns: &mut PassConns<DB>,
        role: &SqlIdentifier,
        targets: &[ProbeTarget],
    ) {
        for target in targets {
            self.probe_cell_as_role(conns, role, target).await;
        }
    }

    async fn probe_cell_as_user(
        &mut self,
        conns: &mut PassConns<DB>,
        user: &SqlIdentifier,
        password: &str,
        target: &ProbeTarget,
    ) {
        let Ok(conn) = conns.for_user(user, password).await else {
            self.mark_row_denied(user, std::slice::from_ref(target));
            return;
        };
        // Reaching the statement means entering its schema first, and that is itself
        // privileged on MySQL: a user with no grant left on the database cannot `USE`
        // it. Classify that refusal as the denial it is, so the cell resolves instead
        // of retrying a failure that will never clear on its own.
        if let Err(e) = conn.set_schema_search_path(&target.path).await {
            if DB::is_privilege_error(&e) {
                self.record(user.clone(), target.cache, Verdict::Denied);
            } else {
                counter!(CACHE_ACL_PROBE_FAILURES, "kind" => "probe").increment(1);
            }
            return;
        }
        let outcome = conn.acl_probe(&target.sql, target.n_params, None).await;
        self.record_outcome(user, target.cache, outcome);
    }

    async fn probe_cell_as_role(
        &mut self,
        conns: &mut PassConns<DB>,
        role: &SqlIdentifier,
        target: &ProbeTarget,
    ) {
        let member = self.prepare_role_member(conns, role).await;
        let conn = match &member {
            Some((user, password)) => match conns.for_user(user, password).await {
                Ok(conn) => conn,
                Err(()) => return,
            },
            None => match conns.admin().await {
                Ok(conn) => conn,
                Err(()) => {
                    counter!(CACHE_ACL_PROBE_FAILURES, "kind" => "connect").increment(1);
                    return;
                }
            },
        };
        if let Err(e) = conn.set_schema_search_path(&target.path).await {
            if DB::is_privilege_error(&e) {
                self.record(role.clone(), target.cache, Verdict::Denied);
            } else {
                counter!(CACHE_ACL_PROBE_FAILURES, "kind" => "probe").increment(1);
            }
            return;
        }
        let outcome = conn
            .acl_probe(&target.sql, target.n_params, Some(role.as_str()))
            .await;
        self.record_outcome(role, target.cache, outcome);
    }

    fn record_outcome(
        &mut self,
        identity: &SqlIdentifier,
        cache: QueryId,
        outcome: Result<AclProbeOutcome, DB::Error>,
    ) {
        let verdict = match outcome {
            Ok(AclProbeOutcome::Authorized) => Verdict::Allowed,
            Ok(AclProbeOutcome::Denied) => Verdict::Denied,
            Err(e) => {
                // Transient: leave the prior cell in place and retry on the
                // next pass.
                counter!(CACHE_ACL_PROBE_FAILURES, "kind" => "probe").increment(1);
                rate_limit(
                    true,
                    ADAPTER_ACL_PROBE,
                    || warn!(error = %e, identity = %identity, cache = %cache, "ACL probe failed"),
                );
                return;
            }
        };
        self.record(identity.clone(), cache, verdict);
    }

    fn record(&self, identity: SqlIdentifier, cache: QueryId, verdict: Verdict) {
        let prior = self.matrix.record(identity, cache, verdict);
        if let Some(prior) = prior
            && prior != verdict
        {
            counter!(
                CACHE_ACL_VERDICT_FLIPS,
                "from" => prior.as_str(),
                "to" => verdict.as_str(),
            )
            .increment(1);
        }
    }

    /// The A7 fallback: a user whose probe session cannot open is served
    /// through the proxy path only.
    fn mark_row_denied(&mut self, user: &SqlIdentifier, targets: &[ProbeTarget]) {
        counter!(CACHE_ACL_PROBE_FAILURES, "kind" => "connect").increment(1);
        for target in targets {
            self.record(user.clone(), target.cache, Verdict::Denied);
        }
        self.connect_denied.insert(user.clone());
        self.on_connect_failure(user);
    }

    fn on_connect_failure(&mut self, user: &SqlIdentifier) {
        let pass_seq = self.pass_seq;
        self.backoff
            .entry(user.clone())
            .or_default()
            .record_failure(pass_seq);
    }

    fn update_gauges(&self) {
        let staleness = self
            .fingerprints
            .values()
            .map(|(_, at)| at.elapsed().as_secs_f64())
            .fold(0.0, f64::max);
        gauge!(CACHE_ACL_STALENESS).set(staleness);

        let mut identities: Vec<SqlIdentifier> = self
            .users
            .snapshot()
            .keys()
            .map(|u| SqlIdentifier::from(u.as_str()))
            .collect();
        identities.extend(self.roles.keys().cloned());
        let caches: Vec<QueryId> = self
            .shallow
            .list_caches(None, None)
            .iter()
            .map(|info| info.query_id)
            .collect();
        gauge!(CACHE_ACL_UNKNOWN_PAIRS)
            .set(self.matrix.unknown_pairs(&identities, &caches).len() as f64);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn column_probe_order_puts_creator_first() {
        let users: Vec<SqlIdentifier> = vec!["alice".into(), "bob".into(), "carol".into()];
        let creator: SqlIdentifier = "carol".into();
        let ordered = column_probe_order(users.clone(), Some(&creator));
        assert_eq!(ordered[0], creator);
        assert_eq!(ordered.len(), 3);

        // A creator that is not an allowed user leaves the order untouched.
        let outsider: SqlIdentifier = "mallory".into();
        assert_eq!(column_probe_order(users.clone(), Some(&outsider)), users);
        assert_eq!(column_probe_order(users.clone(), None), users);
    }

    #[test]
    fn backoff_spacing_doubles_and_caps() {
        let mut state = BackoffState::default();
        assert!(state.should_attempt(1));
        state.record_failure(1);
        assert!(!state.should_attempt(2));
        assert!(state.should_attempt(3));
        state.record_failure(3);
        assert!(!state.should_attempt(6));
        assert!(state.should_attempt(7));
        for pass in 0..10 {
            state.record_failure(pass);
        }
        // Spacing is capped at MAX_BACKOFF_PASSES.
        assert!(state.should_attempt(9 + MAX_BACKOFF_PASSES));
    }
}
