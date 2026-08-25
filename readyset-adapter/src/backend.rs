//!
//! [`Backend`] handles the execution of queries and prepared statements. Queries and
//! statements can be executed either on ReadySet itself, or on the upstream when applicable.
//! In general if an upstream (fallback) connection is available queries and statements
//! will execute as follows:
//!
//! * `INSERT`, `DELETE`, `UPDATE` - on upstream
//! * Anything inside a transaction - on upstream
//! * Cached statements created with "always" - on ReadySet
//! * `SELECT` - on ReadySet
//! * Anything that failed on ReadySet, or while a migration is ongoing - on upstream
//!
//! # The execution flow
//!
//! ## Prepare
//!
//! When an upstream is available we will only try to prepare `SELECT` statements on ReadySet and
//! forward all other prepare requests to the upstream. For `SELECT` statements we will attempt
//! to prepare on both ReadySet and the upstream. The if ReadySet select fails we will perform a
//! fallback execution on the upstream (`execute_cascade`).
//!
//! ## Queries
//!
//! Queries are handled in a similar way to prepare statements. with the exception that additional
//! overhead is required to parse and rewrite them prior to their execution.
//!
//! ## Migrations
//!
//! When a prepared statement is not immediately available for execution on ReadySet, we will
//! perform a migration, migrations can happen in one of three ways:
//!
//! * Explicit migrations: only `CREATE CACHE` and `CREATE VIEW` will cause migrations. A `CREATE
//!   PREPARED STATEMENT` will not cause a migration, and queries will go to upstream fallback.
//!   Enabled with the `--query-caching=explicit` argument. However if a migration already happened,
//!   we will use it.
//! * Async migration: prepared statements will be put in a [`QueryStatusCache`] and another thread
//!   will perform migrations in the background. Once a statement finished migration it will execute
//!   on ReadySet, while it is waiting for a migration to happen it will execute on fallback.
//!   Enabled with the `--query-caching=async` flag.
//! * In request path: migrations will happen when either `CREATE CACHE` or `CREATE PREPARED
//!   STATEMENT` are called. It is also the only available option when a upstream fallback is not
//!   available.
//!
//! ## Caching
//!
//! Since we don't want to pay a penalty every time we execute a prepared statement, either
//! on ReadySet or on the upstream fallback, we aggressively cache all the information required
//! for immediate execution. This way a statement can be immediately forwarded to either ReadySet
//! or upstream with no additional overhead.
//!
//! ## Handling unsupported queries
//!
//! Queries are marked with MigrationState::Unsupported when they fail to prepare on ReadySet
//! with an Unsupported ReadySetError. These queries should not be tried again against ReadySet,
//! however, if a fallback database exists, may be executed against the fallback.
//!
//! ## Handling component outage
//!
//! In a distributed deployment, a component (such as a readyset-server instance) may go down,
//! causing some queries that rely on that server instance to fail. To help direct all affected
//! queries immediately to fallback when this happens, you can configure the
//! --query-max-failure-seconds flag to provide a maximum time in seconds that any given query may
//! continuously fail for before entering into a fallback only recovery period. You can configure
//! the --fallback-recovery-seconds flag to configure how long you would like this recovery period
//! to be enabled for, before allowing affected queries to be retried against noria.
//!
//! The metadata for this feature is tracked in the QueryStatusCache for each query. We currently
//! only trigger on networking related errors specifically to try to prevent this feature from
//! being too heavy handed.

use std::borrow::{Borrow, Cow};
use std::collections::HashMap;
use std::fmt::{self, Debug};
use std::marker::PhantomData;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::{
    Arc, OnceLock, PoisonError, RwLock as StdRwLock, RwLockReadGuard as StdRwLockReadGuard,
};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::cache_acl::{AclHandle, AclMessage, CacheCreator, Verdict};
use crate::rls_coordinator::RlsCoordinator;
use crate::session_context::SessionContext;
use crate::shallow_key::ShallowKey;
use anyhow::bail;
use clap::ValueEnum;
use crossbeam_skiplist::SkipSet;
use database_utils::UpstreamConfig;
use failpoint_macros::set_failpoint;
use lru::LruCache;
use metrics::{counter, gauge};
use mysql_common::row::convert::{FromRow, FromRowError};
use readyset_adapter_types::{ParsedCommand, PreparedStatementType};
use readyset_client::consensus::{Authority, AuthorityControl, CacheDDLRequest};
use readyset_client::post_processing::Results;
use readyset_client::schema::{ColumnSchema, SelectSchema};
use readyset_client::{CacheMode, ViewCreateRequest};
use readyset_client::{ShallowViewRequest, query::*};
pub use readyset_client_metrics::QueryDestination;
use readyset_client_metrics::{QueryExecutionEvent, QueryLogMode};
use readyset_data::upstream_system_props::UpstreamCollation;
use readyset_data::{DfType, DfValue};
use readyset_errors::ReadySetError;
use readyset_errors::{ReadySetResult, internal, internal_err, unsupported};
use readyset_metrics::metrics_handle;
use readyset_schema::{ReadysetSchema, ReadysetSchemaSession};
use readyset_shallow::{CacheInfo, CacheInsertGuard, CacheManager, ContentHash};
use readyset_sql::ast::{
    self, CacheInner, CacheType, CreateCacheOptions, CreateCacheStatement, ReadysetHintDirective,
    Relation, ShallowCacheQuery, SqlIdentifier, SqlQuery, TrxCachePolicy, UseStatement,
};
use readyset_sql::{Dialect, DialectDisplay, TryFromDialect};
use readyset_sql_parsing::ParsingPreset;
use readyset_sql_passes::adapter_rewrites::{AdapterRewriteParams, ShallowQueryParameters};
use readyset_sql_passes::detect_references::{references_schema, references_variables};
use readyset_sql_passes::shallow::{
    ShallowCacheAllowlists, ShallowCacheEligibility, rewrite_shallow,
};
use readyset_telemetry_reporter::{TelemetryEvent, TelemetrySender};
use readyset_util::SizeOf;
#[cfg(feature = "failure_injection")]
use readyset_util::failpoints;
use readyset_util::logging::{ADAPTER_ACL_DECLINED, rate_limit};
use readyset_util::redacted::{RedactedString, Sensitive};
use readyset_util::retry_with_exponential_backoff;
use readyset_version::READYSET_VERSION;
use tokio::sync::RwLock;
use tokio::sync::mpsc::UnboundedSender;
use tracing::{debug, error, info, trace, warn};

use crate::query_status_cache::QueryStatusCache;
use crate::status_reporter::ReadySetStatusReporter;
pub use crate::upstream_database::UpstreamPrepare;
use crate::utils::{create_dummy_column, time_or_null};
use crate::{QueryHandler, UpstreamDatabase, UpstreamDestination, create_dummy_schema};
use schema_catalog::{RewriteContext, SchemaCatalogHandle};

mod adhoc;
mod extensions;
pub mod noria_connector;
mod prepared;
mod routing;
mod set_handler;
mod shallow;

use self::noria_connector::MetaVariable;
pub use self::noria_connector::NoriaConnector;
use self::prepared::PreparedStatements;
pub use self::routing::ProxyState;
use self::routing::SessionWriteTracker;

/// Reserved program/application name used by ReadySet components to identify internal connections
pub const READYSET_QUERY_SAMPLER: &str = "READYSET_QUERY_SAMPLER";

/// Reserved program/application name reported by the shallow cache refresher on its upstream
/// connections so they are identifiable on the upstream database.
pub(crate) const READYSET_SHALLOW_REFRESHER: &str = "READYSET_SHALLOW_REFRESHER";

/// Reserved program/application name reported by the cache-ACL prober on its per-user upstream
/// connections so probe traffic is identifiable in the processlist and audit logs.
pub const READYSET_ACL_POOLER: &str = "READYSET_ACL_POOLER";

const UNSUPPORTED_CACHE_DDL_MSG: &str = "This instance has been provisioned through Readyset Cloud. Please use the Readyset Cloud UI to manage caches. You may continue to use the SQL interface to run other 'read' commands.";

/// Placeholder username for connections that have not yet authenticated
const UNAUTHENTICATED_USER: &str = "unauthenticated";

/// `ConnectionClosed` makes the protocol layer end the session, so the client's reconnect
/// performs a fresh upstream connection attempt.
fn no_upstream_err(message: &str) -> ReadySetError {
    ReadySetError::ConnectionClosed(message.into())
}

/// `EXPLAIN LAST STATEMENT` reason for a query upstream served while filling a shallow
/// cache, distinguishing it from the fallbacks that share its destination.
const SHALLOW_CACHE_MISS: &str = "shallow cache miss";

/// Unique identifier for a prepared statement, local to a single [`Backend`].
type StatementId = u32;

use crate::ROUTING_CHECK_INTERVAL;
use crate::shallow_refresh_pool::ShallowRefreshPool;
pub use crate::shallow_refresh_pool::ShallowRefreshRequest;

/// Information about an active connection
#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Ord)]
pub struct ConnectionInfo {
    /// The remote address of the connection
    pub addr: SocketAddr,
    /// The authenticated username for this connection
    pub username: String,
}

impl ConnectionInfo {
    pub fn new(addr: SocketAddr, username: String) -> Self {
        Self { addr, username }
    }
}

impl std::fmt::Display for ConnectionInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}@{}", self.username, self.addr)
    }
}

/// How to behave when receiving unsupported `SET` statements
#[derive(Copy, Clone, Debug, PartialEq, Eq, ValueEnum)]
pub enum UnsupportedSetMode {
    /// Return an error to the client (the default)
    Error,
    /// Proxy all subsequent statements to the upstream
    Proxy,
    /// Allow all unsupported set statements
    Allow,
}

/// The identity the cache ACL judges this session by: the effective role from the session
/// mirror where one exists (Postgres), the authenticated username otherwise (MySQL). `None`
/// means the identity cannot be established -- an untrusted mirror or a pre-auth connection
/// -- and the caller must fail closed.
fn acl_identity(
    session: Option<&Arc<SessionContext>>,
    client_identity: Option<&SqlIdentifier>,
) -> Option<SqlIdentifier> {
    match session {
        Some(session) => session.acl_identity(),
        None => client_identity.cloned(),
    }
}

/// The creator to attribute a new cache to: the identity the ACL judges the creating session
/// by, plus the login user it was assumed from, which is the connection the worker probes an
/// assumed role through.
fn acl_creator(
    session: Option<&Arc<SessionContext>>,
    client_identity: Option<&SqlIdentifier>,
) -> Option<CacheCreator> {
    Some(CacheCreator {
        identity: acl_identity(session, client_identity)?,
        via: session.map(|session| session.startup_user.clone()),
    })
}

/// The cache-ACL gate (deny-means-proxy): whether this session may be served from the
/// shallow cache identified by `query_id`. Anything but an `Allowed` verdict for the
/// session's effective identity declines -- returning the decline reason for
/// `EXPLAIN LAST STATEMENT` -- and the query falls through to upstream, which
/// re-authorizes it. Runs before RLS scoping and independent of `TrxCachePolicy`: the
/// verdict overrides even an `ALWAYS` pin, since a pin to the cache cannot pin a user who
/// lost access. Inert when authentication is off (no user identities to authorize).
fn acl_decline_reason(
    acl: &AclHandle,
    session: Option<&Arc<SessionContext>>,
    client_identity: Option<&SqlIdentifier>,
    require_authentication: bool,
    query_id: QueryId,
) -> Option<&'static str> {
    if !require_authentication {
        return None;
    }
    let Some(identity) = acl_identity(session, client_identity) else {
        record_acl_decline(query_id, "untrusted");
        return Some("cache_acl_untrusted");
    };
    match acl.matrix().verdict_for(&identity, query_id) {
        Verdict::Allowed => None,
        verdict => {
            if verdict == Verdict::Unknown {
                // Ask the worker to resolve the identity (a role assumed via SET ROLE has
                // no row until first sight), carrying the login user whose accepted SET
                // ROLE proved membership. Non-blocking, and deduplicated worker-side.
                let via = session.map(|session| session.startup_user.clone());
                acl.send_demand(AclMessage::ResolveIdentity { identity, via });
            }
            record_acl_decline(query_id, verdict.as_str());
            Some(match verdict {
                Verdict::Denied => "cache_acl_denied",
                _ => "cache_acl_unknown",
            })
        }
    }
}

/// Count and (rate-limited) log a shallow serve declined by the cache ACL, so an individual
/// user's off-cache routing is traceable without letting a denied hot user flood the log.
fn record_acl_decline(query_id: QueryId, verdict: &'static str) {
    counter!(
        metric::CACHE_ACL_DECLINED,
        "query_id" => query_id.to_string(),
        "verdict" => verdict
    )
    .increment(1);
    rate_limit(true, ADAPTER_ACL_DECLINED, || {
        debug!(%query_id, verdict, "Cache ACL declined shallow serve");
    });
}

/// Notified when the adapter's allowed-users map changes at runtime so protocol-level caches
/// (today: MySQL `caching_sha2_password` fast-auth digests) can be kept in sync.
pub trait UsersSync: Send + Sync + std::fmt::Debug {
    /// Replace any cached state with one entry per `(user, password)` in `users`.
    fn refresh(&self, users: &HashMap<String, String>);
}

/// Process-wide allowed-users map paired with an optional sync hook that keeps protocol-level
/// fast-auth caches in step. Mutated by `ALTER READYSET ADD|MODIFY|DROP USER`.
#[derive(Debug)]
pub struct AllowedUsers {
    /// Username to plaintext password for every user allowed to authenticate. Read on each
    /// authentication attempt and written only by [`AllowedUsers::replace`]; reads are never held
    /// across an await, so a std read-write lock fits this read-mostly hot path.
    map: StdRwLock<HashMap<String, String>>,
    /// Fast-auth refresh hook, invoked by [`AllowedUsers::replace`] under the map's write lock.
    /// Write-once: production installs it at construction, the test harness right after, and
    /// every later read is lock-free.
    sync: OnceLock<Arc<dyn UsersSync>>,
    /// Serializes runtime mutations so the in-memory map and the Authority never diverge under
    /// concurrent `ALTER READYSET ... USER` statements. Held across the whole
    /// snapshot -> persist -> replace sequence, which spans an Authority await, hence a tokio
    /// Mutex rather than a std lock.
    mutation_guard: tokio::sync::Mutex<()>,
}

impl AllowedUsers {
    pub fn new(initial: HashMap<String, String>, sync: Option<Arc<dyn UsersSync>>) -> Self {
        let sync_cell = OnceLock::new();
        if let Some(sync) = sync {
            let _ = sync_cell.set(sync);
        }
        Self {
            map: StdRwLock::new(initial),
            sync: sync_cell,
            mutation_guard: tokio::sync::Mutex::new(()),
        }
    }

    /// Install the [`UsersSync`] hook after construction, for callers that create the fast-auth
    /// cache only after the allowed-users handle exists (the test harness). A no-op if a hook is
    /// already set; production wires the hook up front via [`AllowedUsers::new`].
    pub fn set_users_sync(&self, sync: Arc<dyn UsersSync>) {
        let _ = self.sync.set(sync);
    }

    /// Empty users map with no sync hook. Used as the default for [`BackendBuilder`].
    pub fn empty() -> Arc<Self> {
        Arc::new(Self::new(HashMap::new(), None))
    }

    /// Look up `user`'s plaintext password, cloning it out so the read lock isn't held by the
    /// caller. A poisoned lock is recovered rather than propagated so a single panic elsewhere
    /// cannot turn into a blanket authentication outage.
    pub(crate) fn password_for(&self, user: &str) -> Option<String> {
        self.map
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(user)
            .cloned()
    }

    /// Read-lock the underlying map. Intended for one-shot startup work (e.g. priming the MySQL
    /// `AuthCache`). Recovers a poisoned lock rather than panicking.
    pub fn read(&self) -> StdRwLockReadGuard<'_, HashMap<String, String>> {
        self.map.read().unwrap_or_else(PoisonError::into_inner)
    }

    /// Clone the current map, e.g. to seed an Authority `read_modify_write` or list usernames.
    /// Recovers a poisoned lock rather than panicking.
    pub fn snapshot(&self) -> HashMap<String, String> {
        self.map
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Replace the whole map and notify the sync hook while still under the write lock, so an
    /// observer never sees the map and the fast-auth cache disagree. Recovers a poisoned lock
    /// rather than panicking.
    pub fn replace(&self, new: HashMap<String, String>) {
        let mut map = self.map.write().unwrap_or_else(PoisonError::into_inner);
        *map = new;
        if let Some(sync) = self.sync.get() {
            sync.refresh(&map);
        }
    }

    /// Acquire the mutation guard. Callers hold the returned guard across the whole
    /// snapshot -> persist -> replace sequence of a single `ALTER READYSET ... USER`.
    async fn lock_mutations(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.mutation_guard.lock().await
    }
}

impl readyset_schema::virtual_relation::UsersInfo for AllowedUsers {
    fn usernames(&self) -> Vec<String> {
        self.snapshot().into_keys().collect()
    }
}

/// Builder for a [`Backend`]
#[must_use]
#[derive(Clone)]
pub struct BackendBuilder {
    client_addr: SocketAddr,
    slowlog: bool,
    dialect: Dialect,
    parsing_preset: ParsingPreset,
    users: Arc<AllowedUsers>,
    cache_acl: AclHandle,
    require_authentication: bool,
    query_log_sender: Option<UnboundedSender<QueryExecutionEvent>>,
    query_log_mode: Option<QueryLogMode>,
    unsupported_set_mode: UnsupportedSetMode,
    migration_mode: MigrationMode,
    query_max_failure_seconds: u64,
    fallback_recovery_seconds: u64,
    telemetry_sender: Option<TelemetrySender>,
    placeholder_inlining: bool,
    connections: Option<Arc<SkipSet<ConnectionInfo>>>,
    allow_cache_ddl: bool,
    sampler_tx:
        Option<tokio::sync::mpsc::Sender<(QueryExecutionEvent, String, Vec<SqlIdentifier>)>>,
    db_version: Option<String>,
    cache_mode: CacheMode,
    default_ttl_ms: u64,
    default_coalesce_ms: u64,
    /// Opportunistic read-your-writes window (ms). Applies only *outside* transactions:
    /// after any write on a session, reads on that same session bypass the cache for
    /// this many milliseconds. In-transaction routing is governed by the per-cache
    /// `TrxCachePolicy`, not this window. Opportunistic only: once the window elapses,
    /// the cache may still hold a pre-write value (TTL not yet expired, refresh not yet
    /// caught up), so subsequent reads can flip back to a stale result.
    /// `None` (the default) disables the window.
    opportunistic_ryw_ms: Option<u64>,
    upstream_config: Option<Arc<RwLock<UpstreamConfig>>>,
    replication_enabled: bool,
    readyset_schema: Option<Arc<ReadysetSchema>>,
    shallow_cache_eligibility: ShallowCacheEligibility,
    shallow_cache_allowlists: ShallowCacheAllowlists,
    /// Process-shared RLS policy registry. The adapter binary
    /// constructs one at startup, hands it to the catalog poller, and
    /// passes it through here so every per-connection Backend
    /// consults the same view of pg_policy / pg_class / pg_roles.
    /// `None` disables RLS: the analyzer gate is skipped and every
    /// shallow cache is created Plain. MySQL deployments and
    /// Postgres setups without a catalog poller (no upstream URL,
    /// test harnesses) run in this mode; `readyset::NoriaAdapter::run`
    /// refuses to start a Postgres adapter whose RLS bootstrap
    /// failed, so production Postgres always carries `Some`.
    policy_registry: Option<Arc<readyset_rls::PolicyRegistry>>,
}

impl Default for BackendBuilder {
    fn default() -> Self {
        BackendBuilder {
            client_addr: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), 0)),
            slowlog: false,
            dialect: Dialect::MySQL,
            parsing_preset: ParsingPreset::for_prod(),
            users: AllowedUsers::empty(),
            cache_acl: AclHandle::disabled(),
            require_authentication: true,
            query_log_sender: None,
            query_log_mode: None,
            unsupported_set_mode: UnsupportedSetMode::Error,
            migration_mode: MigrationMode::InRequestPath,
            query_max_failure_seconds: (i64::MAX / 1000) as u64,
            fallback_recovery_seconds: 0,
            telemetry_sender: None,
            placeholder_inlining: false,
            connections: None,
            allow_cache_ddl: true,
            sampler_tx: None,
            db_version: None,
            cache_mode: CacheMode::Deep,
            default_ttl_ms: 10_000,
            default_coalesce_ms: 5_000,
            opportunistic_ryw_ms: None,
            upstream_config: None,
            replication_enabled: true,
            readyset_schema: None,
            shallow_cache_eligibility: ShallowCacheEligibility::default(),
            shallow_cache_allowlists: ShallowCacheAllowlists::default(),
            policy_registry: None,
        }
    }
}

impl BackendBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn build<DB: UpstreamDatabase + 'static, Handler: 'static>(
        self,
        noria: NoriaConnector,
        authority: Arc<Authority>,
        query_status_cache: &'static QueryStatusCache,
        schema_handle: SchemaCatalogHandle,
        status_reporter: ReadySetStatusReporter<DB>,
        adapter_start_time: SystemTime,
        shallow: Arc<CacheManager<ShallowKey, DB::CacheEntry>>,
        rls_coordinator: Option<Arc<RlsCoordinator<DB::CacheEntry>>>,
        shallow_refresh_pool: Option<Arc<ShallowRefreshPool<DB>>>,
    ) -> Backend<DB, Handler> {
        gauge!(metric::CONNECTED_CLIENTS).increment(1.0);
        counter!(metric::CLIENT_CONNECTIONS_OPENED).increment(1);

        // This session proxies to an upstream iff one is configured with a URL. The connection
        // itself is opened later, at auth, by `connect_upstream`.
        let upstream_configured = match &self.upstream_config {
            Some(config) => config.read().await.upstream_db_url.is_some(),
            None => false,
        };
        let proxy_state = if upstream_configured {
            ProxyState::Fallback
        } else {
            ProxyState::Never
        };

        if let Some(connections) = &self.connections {
            connections.insert(ConnectionInfo::new(
                self.client_addr,
                UNAUTHENTICATED_USER.to_string(),
            ));
        }

        let last_upstream_url = match &self.upstream_config {
            Some(config) => config.read().await.upstream_db_url.clone(),
            None => None,
        };

        Backend {
            connectors: BackendConnectors {
                noria,
                upstream: None,
                readyset_schema_session: None,
                session: None,
            },
            state: BackendState {
                client_addr: self.client_addr,
                proxy_state,
                write_tracker: SessionWriteTracker::new(
                    self.opportunistic_ryw_ms.map(Duration::from_millis),
                ),
                last_query: None,
                pending_proxy_reason: None,
                parsed_query_cache: LruCache::new(10_000.try_into().expect("10000 is not 0")),
                prepared: Default::default(),
                query_status_cache,
                schema_handle,
                users: self.users,
                acl: self.cache_acl,
                query_log_sender: self.query_log_sender,
                query_log_mode: self.query_log_mode,
                telemetry_sender: self.telemetry_sender,
                connections: self.connections,
                client_username: None,
                client_identity: None,
                status_reporter,
                sampler_tx: self.sampler_tx,
                is_internal_connection: false,
                shallow,
                policy_registry: self.policy_registry.clone(),
                rls_coordinator,
                shallow_refresh_pool,
                db_version: self.db_version,
                upstream_config: self.upstream_config,
                last_upstream_url,
                last_routing_check: Instant::now(),
                routing_changed: false,
                authority,
                adapter_start_time,
                readyset_schema: self.readyset_schema,
                readyset_schema_route_all: false,
                shallow_cache_allowlists: self.shallow_cache_allowlists,
            },
            settings: BackendSettings {
                slowlog: self.slowlog,
                dialect: self.dialect,
                parsing_preset: self.parsing_preset,
                require_authentication: self.require_authentication,
                unsupported_set_mode: self.unsupported_set_mode,
                migration_mode: self.migration_mode,
                query_max_failure_duration: Duration::new(self.query_max_failure_seconds, 0),
                fallback_recovery_duration: Duration::new(self.fallback_recovery_seconds, 0),
                placeholder_inlining: self.placeholder_inlining,
                cache_mode: self.cache_mode,
                default_ttl_ms: self.default_ttl_ms,
                default_coalesce_ms: self.default_coalesce_ms,
                replication_enabled: self.replication_enabled,
                allow_cache_ddl: self.allow_cache_ddl,
                shallow_cache_eligibility: self.shallow_cache_eligibility,
            },
            _query_handler: PhantomData,
        }
    }

    pub fn client_addr(mut self, client_addr: SocketAddr) -> Self {
        self.client_addr = client_addr;
        self
    }

    pub fn slowlog(mut self, slowlog: bool) -> Self {
        self.slowlog = slowlog;
        self
    }

    pub fn dialect(mut self, dialect: Dialect) -> Self {
        self.dialect = dialect;
        self
    }

    pub fn parsing_preset(mut self, parsing_preset: ParsingPreset) -> Self {
        self.parsing_preset = parsing_preset;
        self
    }

    pub fn query_log_sender(
        mut self,
        query_log_sender: Option<UnboundedSender<QueryExecutionEvent>>,
    ) -> Self {
        self.query_log_sender = query_log_sender;
        self
    }

    pub fn query_log_mode(mut self, query_log_mode: Option<QueryLogMode>) -> Self {
        self.query_log_mode = query_log_mode;
        self
    }

    pub fn users(mut self, users: Arc<AllowedUsers>) -> Self {
        self.users = users;
        self
    }

    pub fn cache_acl(mut self, cache_acl: AclHandle) -> Self {
        self.cache_acl = cache_acl;
        self
    }

    /// Returns the shared users handle configured on this builder.
    pub fn get_users(&self) -> &Arc<AllowedUsers> {
        &self.users
    }

    pub fn get_cache_mode(&self) -> CacheMode {
        self.cache_mode
    }

    pub fn get_default_ttl_ms(&self) -> u64 {
        self.default_ttl_ms
    }

    pub fn get_default_coalesce_ms(&self) -> u64 {
        self.default_coalesce_ms
    }

    pub fn require_authentication(mut self, require_authentication: bool) -> Self {
        self.require_authentication = require_authentication;
        self
    }

    pub fn get_require_authentication(&self) -> bool {
        self.require_authentication
    }

    /// Whether or not to allow cache ddl statements to be executed. If false, cache ddl statements
    /// received will instead return an error prompting the user to use ReadySet cloud to manage
    /// their caches.
    pub fn allow_cache_ddl(mut self, allow_cache_ddl: bool) -> Self {
        self.allow_cache_ddl = allow_cache_ddl;
        self
    }

    /// Per-category opt-ins for shallow-cache auto-creation eligibility (which
    /// classes of otherwise-ineligible query the in-request-path filter should
    /// permit). This is adapter-local config sourced from CLI flags, not from
    /// the server-provided [`AdapterRewriteParams`].
    pub fn shallow_cache_eligibility(mut self, eligibility: ShallowCacheEligibility) -> Self {
        self.shallow_cache_eligibility = eligibility;
        self
    }

    /// Seed the three shallow-cache allowlists (function, variable, schema)
    /// shared with the eligibility filter. Cloned into each connection's
    /// [`BackendState`]; all clones share the same underlying sets, so a runtime
    /// `ALTER READYSET ... SHALLOW CACHE ALLOWED ...` is visible to every
    /// connection at once.
    pub fn shallow_cache_allowlists(mut self, allowlists: ShallowCacheAllowlists) -> Self {
        self.shallow_cache_allowlists = allowlists;
        self
    }

    pub fn unsupported_set_mode(mut self, unsupported_set_mode: UnsupportedSetMode) -> Self {
        self.unsupported_set_mode = unsupported_set_mode;
        self
    }

    pub fn migration_mode(mut self, q: MigrationMode) -> Self {
        self.migration_mode = q;
        self
    }

    pub fn query_max_failure_seconds(mut self, secs: u64) -> Self {
        self.query_max_failure_seconds = secs;
        self
    }

    pub fn fallback_recovery_seconds(mut self, secs: u64) -> Self {
        self.fallback_recovery_seconds = secs;
        self
    }

    pub fn telemetry_sender(mut self, telemetry_sender: TelemetrySender) -> Self {
        self.telemetry_sender = Some(telemetry_sender);
        self
    }

    pub fn set_placeholder_inlining(mut self, placeholder_inlining: bool) -> Self {
        self.placeholder_inlining = placeholder_inlining;
        self
    }

    pub fn connections(mut self, connections: Arc<SkipSet<ConnectionInfo>>) -> Self {
        self.connections = Some(connections);
        self
    }

    /// Set the sender used to enqueue original queries for background sampling/verification
    pub fn sampler_tx(
        mut self,
        tx: Option<tokio::sync::mpsc::Sender<(QueryExecutionEvent, String, Vec<SqlIdentifier>)>>,
    ) -> Self {
        self.sampler_tx = tx;
        self
    }

    pub fn db_version(mut self, db_version: String) -> Self {
        self.db_version = Some(db_version);
        self
    }

    pub fn cache_mode(mut self, cache_mode: CacheMode) -> Self {
        self.cache_mode = cache_mode;
        self
    }

    pub fn default_ttl_ms(mut self, default_ttl_ms: u64) -> Self {
        self.default_ttl_ms = default_ttl_ms;
        self
    }

    pub fn default_coalesce_ms(mut self, default_coalesce_ms: u64) -> Self {
        self.default_coalesce_ms = default_coalesce_ms;
        self
    }

    /// Configure the opportunistic read-your-writes window (in milliseconds). `None`
    /// (the default) disables the feature. A `Some(0)` is treated the same as `None`.
    pub fn opportunistic_ryw_ms(mut self, opportunistic_ryw_ms: Option<u64>) -> Self {
        self.opportunistic_ryw_ms = opportunistic_ryw_ms.filter(|&ms| ms > 0);
        self
    }

    pub fn upstream_config(mut self, upstream_config: Option<Arc<RwLock<UpstreamConfig>>>) -> Self {
        self.upstream_config = upstream_config;
        self
    }

    pub fn replication_enabled(mut self, replication_enabled: bool) -> Self {
        self.replication_enabled = replication_enabled;
        self
    }

    pub fn get_upstream_config(&self) -> Option<&Arc<RwLock<UpstreamConfig>>> {
        self.upstream_config.as_ref()
    }

    pub fn readyset_schema(mut self, readyset_schema: Arc<ReadysetSchema>) -> Self {
        self.readyset_schema = Some(readyset_schema);
        self
    }

    /// Plumb a process-shared RLS policy registry into every Backend
    /// the builder produces. Called by the adapter binary at startup
    /// after it has spawned the catalog poller against the same
    /// `Arc<PolicyRegistry>`; downstream connections then see policy
    /// updates without per-connection state.
    pub fn policy_registry(mut self, registry: Arc<readyset_rls::PolicyRegistry>) -> Self {
        self.policy_registry = Some(registry);
        self
    }
}

fn parse_query(settings: &BackendSettings, query: &str) -> ReadySetResult<SqlQuery> {
    trace!(query = %Sensitive(&query), "Parsing query");
    readyset_sql_parsing::parse_query_with_config(
        settings.parsing_preset.into_config().log_only_selects(true),
        settings.dialect,
        query,
    )
    .map_err(Into::into)
}

fn parse_shallow_query(
    settings: &BackendSettings,
    query: &str,
) -> (
    ReadySetResult<ShallowCacheQuery>,
    Option<ReadysetHintDirective>,
) {
    trace!(%query, "Parsing shallow query");
    match readyset_sql_parsing::parse_shallow_query(settings.dialect, query) {
        Ok((q, directive)) => (Ok(q), directive),
        Err(e) => (Err(e.into()), None),
    }
}

/// Derives the Readyset AST from a sqlparser AST retained by the shallow parse, avoiding a
/// second parse of the query text. Callers must gate retaining the AST on
/// [`BackendSettings::retain_shallow_ast`]. Falls back to [`parse_query`] when no AST was
/// retained or the conversion fails, since a full parse handles constructs the conversion
/// does not.
fn convert_or_parse_query(
    settings: &BackendSettings,
    shallow_ast: Option<sqlparser::ast::Query>,
    query: &str,
) -> ReadySetResult<SqlQuery> {
    if let Some(ast) = shallow_ast
        && let Ok(parsed) = SqlQuery::try_from_dialect(ast, settings.dialect)
    {
        return Ok(parsed);
    }
    parse_query(settings, query)
}

pub struct Backend<DB, Handler>
where
    DB: UpstreamDatabase,
{
    /// Connectors to noria and the upstream database
    pub connectors: BackendConnectors<DB>,

    /// Encapsulates the inner state of this [`Backend`]
    state: BackendState<DB>,
    /// The settings with which the [`Backend`] was started
    settings: BackendSettings,

    _query_handler: PhantomData<Handler>,
}

/// Connectors to noria and the upstream database.
///
/// This struct is separated from [`Backend`] to enable split borrows: methods that
/// return `QueryResult<'a, DB>` borrow from these connectors, while other fields
/// remain available for subsequent borrows.
pub struct BackendConnectors<DB>
where
    DB: UpstreamDatabase,
{
    /// Readyset connector used for reads, and writes when no upstream DB is present
    pub noria: NoriaConnector,
    /// Optional connector to the upstream DB. Used for fallback reads and all writes if it exists
    upstream: Option<DB>,
    /// A current session with the Readyset schema.
    readyset_schema_session: Option<ReadysetSchemaSession>,
    /// Per-Postgres-session security context, populated from
    /// `StartupMessage.user` and then mutated by `SET` /
    /// `set_config(...)` traffic. `None` on MySQL connections and on
    /// Postgres connections that have not reached the per-session
    /// initialisation hook yet.
    pub session: Option<Arc<SessionContext>>,
}

impl<DB> BackendConnectors<DB>
where
    DB: UpstreamDatabase,
{
    /// Whether or not we have fallback enabled.
    fn has_fallback(&self) -> bool {
        self.upstream.is_some()
    }

    /// Rewrite and wrap a shallow query into a [`ShallowViewRequest`].
    fn prepare_shallow_query(
        &self,
        shallow: Result<ShallowCacheQuery, ReadySetError>,
    ) -> Option<(ShallowViewRequest, ShallowQueryParameters)> {
        let Ok(mut shallow) = shallow else {
            return None;
        };
        let Ok(params) = rewrite_shallow(&mut shallow, self.noria.rewrite_params()) else {
            return None;
        };
        let shallow =
            ShallowViewRequest::new(shallow, self.noria.schema_search_path().to_owned(), None);
        Some((shallow, params))
    }

    /// Responds to a `SHOW REPLAY PATHS` query
    /// Returns replay paths data as a result set with columns and rows
    async fn show_replay_paths(&mut self) -> ReadySetResult<noria_connector::QueryResult<'static>> {
        // Get replay paths from the controller (already flattened and sorted)
        let replay_paths = self.noria.replay_paths().await?;

        // Create schema with all columns
        let schema = create_dummy_schema!(
            "domain",
            "tag",
            "source",
            "destination_index",
            "target_index",
            "path",
            "trigger_type",
            "trigger_index",
            "trigger_source_options"
        );

        // Convert each ReplayPathInfo into a row
        let rows: Vec<Vec<DfValue>> = replay_paths
            .into_iter()
            .map(|info| {
                vec![
                    info.domain.to_string().into(),
                    info.tag.to_string().into(),
                    info.source
                        .map(|s| s.to_string())
                        .unwrap_or_else(|| "None".to_string())
                        .into(),
                    info.destination_index.unwrap_or_default().into(),
                    info.target_index.unwrap_or_default().into(),
                    info.path_segments.join(" → ").into(),
                    info.trigger_type.into(),
                    info.trigger_index.unwrap_or_default().into(),
                    info.trigger_source_options.into(),
                ]
            })
            .collect();

        Ok(noria_connector::QueryResult::from_owned(
            schema,
            vec![Results::new(rows)],
        ))
    }

    /// Determines via running PREPARE if the upstream can support this literal query text.
    ///
    /// Prepares the original query in order to avoid additional parameterization we may do that
    /// could otherwise introduce a placeholder in an invalid PREPARE position.
    async fn upstream_supports(&mut self, sql: &str) -> anyhow::Result<()> {
        let Some(upstream) = self.upstream.as_mut() else {
            bail!("No upstream database found");
        };

        upstream.can_prepare(sql).await
    }

    /// Initialize the search_path by reading it from the upstream.
    pub async fn init_schema_search_path(&mut self) {
        let Some(upstream) = self.upstream.as_mut() else {
            return;
        };
        let search_path = match upstream.schema_search_path().await {
            Ok(search_path) => search_path,
            Err(error) => {
                warn!(%error, "Failed to read schema_search_path from upstream");
                return;
            }
        };
        self.noria.set_schema_search_path(search_path);
    }
}

/// Variables that keep track of the [`Backend`] state
struct BackendState<DB>
where
    DB: UpstreamDatabase,
{
    /// Socket of the connected client.
    client_addr: SocketAddr,
    /// Tracks information related to our decision to proxy or not.
    proxy_state: ProxyState,
    /// Tracks when the last write on this session happened, driving
    /// [`TrxCachePolicy::UntilWrite`] (in-txn) and read-your-own-writes (cross-txn).
    write_tracker: SessionWriteTracker,
    /// Information regarding the last query sent over this connection. If None, then no queries
    /// have been handled using this connection (Backend) yet.
    last_query: Option<QueryInfo>,
    /// Why the in-flight statement was routed off-cache, staged by the
    /// serve-or-proxy seams and folded into [`Self::last_query`] when the
    /// statement finishes.
    pending_proxy_reason: Option<&'static str>,
    /// A cache of queries that we've seen, and their current state, used for processing
    query_status_cache: &'static QueryStatusCache,
    /// A cache of all previously parsed queries
    parsed_query_cache: LruCache<String, SqlQuery>,
    /// Statements this connection has prepared on noria or upstream.
    prepared: PreparedStatements<DB>,
    /// Handle to access the cached schema catalog
    schema_handle: SchemaCatalogHandle,
    /// Process-wide allowed-users handle. Owns the map and (optionally) a sync hook that keeps
    /// protocol-level fast-auth caches in step. Mutated by `ALTER READYSET ADD|MODIFY|DROP USER`.
    users: Arc<AllowedUsers>,
    /// Process-wide cache-ACL handle: the verdict matrix consulted before every shallow serve,
    /// plus the freshness worker's queue. Disabled (always Unknown, sends dropped) when
    /// authentication is off, where the enforcement seams never consult it.
    acl: AclHandle,
    query_log_sender: Option<UnboundedSender<QueryExecutionEvent>>,
    query_log_mode: Option<QueryLogMode>,
    /// Provides the ability to send [`TelemetryEvent`]s to Segment
    telemetry_sender: Option<TelemetrySender>,
    /// Set of active connections to this adapter
    connections: Option<Arc<SkipSet<ConnectionInfo>>>,
    /// The authenticated username for this connection
    client_username: Option<String>,
    /// The authenticated username as an identifier, cached so the per-query ACL lookup on
    /// upstreams without a session mirror (MySQL) never converts or allocates.
    client_identity: Option<SqlIdentifier>,
    status_reporter: ReadySetStatusReporter<DB>,
    /// Optional sender to enqueue original queries for background sampling/verification
    sampler_tx:
        Option<tokio::sync::mpsc::Sender<(QueryExecutionEvent, String, Vec<SqlIdentifier>)>>,
    /// true if the backend connection is an internal connection (eg. from Query Sampler)
    is_internal_connection: bool,
    /// The adapter's shallow cache manager.
    shallow: Arc<CacheManager<ShallowKey, DB::CacheEntry>>,
    /// Process-shared RLS policy registry. Populated by the catalog
    /// poller; consulted by the analyzer at `CREATE CACHE` to decide
    /// `Scoped` vs `Plain` backing and the set of GUCs to fold into
    /// the lookup key. `None` disables RLS: the analyzer gate is
    /// skipped and every shallow cache is created Plain.
    policy_registry: Option<Arc<readyset_rls::PolicyRegistry>>,
    /// RLS coordinator bridging the generic shallow cache to the policy
    /// registry: owns the per-cache scoped descriptors and the
    /// relation/role reverse indices, builds lookup keys, and serves as
    /// the catalog poller's invalidation sink. `None` disables RLS
    /// scoping (MySQL, or Postgres without a catalog poller).
    rls_coordinator: Option<Arc<RlsCoordinator<DB::CacheEntry>>>,
    /// Pool for shallow refresh workers
    shallow_refresh_pool: Option<Arc<ShallowRefreshPool<DB>>>,
    /// Memoized upstream database version.
    db_version: Option<String>,
    /// Shared upstream config, updated by ALTER READYSET CHANGE UPSTREAM.
    upstream_config: Option<Arc<RwLock<UpstreamConfig>>>,
    /// The upstream URL this connection last connected to, used to detect routing changes.
    last_upstream_url: Option<RedactedString>,
    /// When we last checked the shared config for upstream changes (rate-limited to once/sec).
    last_routing_check: Instant,
    /// Set to true when routing changes are detected; causes all subsequent operations to error
    /// so the client disconnects and reconnects to the new upstream.
    routing_changed: bool,
    /// Handle to the Authority. Used to find the Controller.
    authority: Arc<Authority>,
    /// The time at which the adapter started.
    adapter_start_time: SystemTime,
    /// Access to the Readyset schema.
    readyset_schema: Option<Arc<ReadysetSchema>>,
    /// Wether or not to route all queries to the Readyset schema.
    readyset_schema_route_all: bool,
    /// Operator-managed shallow-cache allowlists (function, variable, schema),
    /// shared across connections. Consulted by the auto-create eligibility
    /// filter and mutated by `ALTER READYSET {ADD|DROP} SHALLOW CACHE ALLOWED
    /// {FUNCTION|VARIABLE|SCHEMA}`.
    shallow_cache_allowlists: ShallowCacheAllowlists,
}

impl<DB> BackendState<DB>
where
    DB: UpstreamDatabase,
{
    /// Consume the off-cache reason the serve seams staged for the statement just
    /// finished. Empty when a cache was consulted, or when Readyset holds none.
    fn take_proxy_reason(&mut self) -> String {
        self.pending_proxy_reason
            .take()
            .unwrap_or_default()
            .to_string()
    }

    /// Generates response to the `EXPLAIN LAST STATEMENT` query
    fn explain_last_statement(&mut self) -> ReadySetResult<noria_connector::QueryResult<'static>> {
        let (destination, reason) = self
            .last_query
            .as_ref()
            .map(|info| {
                (
                    info.destination.to_string(),
                    match &info.reason {
                        s if s.is_empty() => "ok".to_string(),
                        s => s.clone(),
                    },
                )
            })
            .unwrap_or_else(|| ("unknown".to_string(), "ok".to_string()));

        Ok(noria_connector::QueryResult::Meta(vec![
            ("Query_destination", destination).into(),
            ("Readyset_reason", reason).into(),
        ]))
    }

    /// Handles a `DROP ALL PROXIED QUERIES` request
    async fn drop_all_proxied_queries(
        &self,
    ) -> ReadySetResult<noria_connector::QueryResult<'static>> {
        self.query_status_cache.clear_proxied_queries();
        Ok(noria_connector::QueryResult::Empty)
    }

    fn drop_view_request(&mut self, view_request: &ViewCreateRequest) {
        self.query_status_cache.update_query_migration_state(
            view_request,
            MigrationState::Pending,
            None,
        );
        self.query_status_cache
            .set_trx_cache_policy(view_request, TrxCachePolicy::Never);
        self.prepared.invalidate(view_request);
    }

    fn drop_shallow_view_request(&self, shallow: &ShallowViewRequest) {
        self.query_status_cache.update_query_migration_state(
            shallow,
            MigrationState::Pending,
            None,
        );
        self.query_status_cache
            .set_trx_cache_policy(shallow, TrxCachePolicy::Never);
    }

    async fn drop_shallow_cached_query(
        &self,
        name: Option<&Relation>,
        query_id: Option<QueryId>,
    ) -> ReadySetResult<()> {
        let info = self
            .shallow
            .get(name, query_id.as_ref())
            .map(|cache| cache.get_info());

        self.shallow.drop_cache(name, query_id.as_ref())?;

        let dropped_id = query_id.or_else(|| info.as_ref().map(|i| i.query_id));
        if let Some(dropped_id) = dropped_id {
            if let Some(coordinator) = &self.rls_coordinator {
                coordinator.unregister(&dropped_id);
            }
            self.acl
                .send_lifecycle(AclMessage::CacheDropped { cache: dropped_id });
        }

        // The cache held the exact `CREATE CACHE` request that was persisted; remove that entry.
        // Matching the stored request (rather than the drop statement) also handles entries
        // written before `cache_name` existed.
        let Some(CacheInfo {
            query,
            schema_search_path,
            ddl_req,
            ..
        }) = info
        else {
            return Ok(());
        };

        let view_request = ShallowViewRequest::new(query, schema_search_path, None);
        self.drop_shallow_view_request(&view_request);

        if let Err(e) = retry_with_exponential_backoff!(
            || async {
                self.authority
                    .remove_shallow_cache_ddl_request(ddl_req.clone())
                    .await
            },
            retries: 5,
            delay: 1,
            backoff: 2,
        ) {
            warn!(error = %e, "Failed to remove shallow cache DDL request");
        }

        Ok(())
    }

    fn readyset_adapter_status(&self) -> ReadySetResult<noria_connector::QueryResult<'static>> {
        let mut statuses = Vec::new();
        if let Some(h) = metrics_handle() {
            let [connected_clients, upstream_connections] = h.gauges(
                [
                    metric::CONNECTED_CLIENTS,
                    metric::CLIENT_UPSTREAM_CONNECTIONS,
                ],
                [],
            );
            let [parse_errors, set_disallowed, view_not_found, rpc_errors] = h.counters(
                [
                    metric::QUERY_LOG_PARSE_ERRORS,
                    metric::QUERY_LOG_SET_DISALLOWED,
                    metric::QUERY_LOG_VIEW_NOT_FOUND,
                    metric::QUERY_LOG_RPC_ERRORS,
                ],
                [],
            );
            statuses.extend([
                (
                    "Connected clients count".into(),
                    connected_clients.get().to_string(),
                ),
                (
                    "Upstream database connection count".into(),
                    upstream_connections.get().to_string(),
                ),
                (
                    "Query parse failures".into(),
                    parse_errors.get().to_string(),
                ),
                (
                    "SET statement disallowed count".into(),
                    set_disallowed.get().to_string(),
                ),
                (
                    "View not found count".into(),
                    view_not_found.get().to_string(),
                ),
                ("RPC error count".into(), rpc_errors.get().to_string()),
            ]);
        }
        let time_ms = self
            .adapter_start_time
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        statuses.push((
            "Process start time".to_string(),
            time_or_null(Some(time_ms)),
        ));

        Ok(noria_connector::QueryResult::MetaVariables(
            statuses.into_iter().map(MetaVariable::from).collect(),
        ))
    }

    fn show_connections(&self) -> Result<noria_connector::QueryResult<'static>, ReadySetError> {
        let schema = SelectSchema {
            schema: Cow::Owned(vec![
                ColumnSchema {
                    column: ast::Column {
                        name: "client_addr".into(),
                        table: None,
                    },
                    column_type: DfType::DEFAULT_TEXT,
                    base: None,
                },
                ColumnSchema {
                    column: ast::Column {
                        name: "username".into(),
                        table: None,
                    },
                    column_type: DfType::DEFAULT_TEXT,
                    base: None,
                },
            ]),
            columns: Cow::Owned(vec!["client_addr".into(), "username".into()]),
        };

        let data = self
            .connections
            .iter()
            .flat_map(|c| c.iter())
            .map(|conn| vec![conn.addr.to_string().into(), conn.username.clone().into()])
            .collect::<Vec<_>>();

        Ok(noria_connector::QueryResult::from_owned(
            schema,
            vec![Results::new(data)],
        ))
    }

    /// Responds to a `SHOW SHALLOW CACHE ENTRIES` query
    async fn show_shallow_entries(
        &self,
        query_id: Option<&str>,
        limit: Option<u64>,
    ) -> ReadySetResult<noria_connector::QueryResult<'static>> {
        let query_id = query_id.map(|q| q.parse()).transpose()?;
        let limit = limit.map(|l| l as usize);
        let shallow = Arc::clone(&self.shallow);

        let rows: Vec<Vec<DfValue>> = tokio::task::spawn_blocking(move || {
            let entries = shallow.list_entries(query_id, limit);
            entries
                .into_iter()
                .map(|entry| {
                    vec![
                        DfValue::from(entry.query_id.to_string()),
                        DfValue::from(format!("{:016x}", entry.entry_id)),
                        time_or_null(Some(entry.last_accessed_ms)).into(),
                        time_or_null(Some(entry.last_refreshed_ms)).into(),
                        entry.refresh_time_ms.into(),
                        entry
                            .refresh_period_ms
                            .map(DfValue::from)
                            .unwrap_or(DfValue::None),
                        entry.bytes.into(),
                    ]
                })
                .collect()
        })
        .await
        .map_err(|e| internal_err!("spawn_blocking failed: {}", e))?;

        let mut select_schema =
            create_dummy_schema!("query_id", "entry_id", "last_accessed", "last_refreshed");
        // refresh_time_ms/refresh_period_ms/bytes carry integer DfValues; the dummy-schema
        // macro only emits text, so declare them with the matching integer type.
        for name in ["refresh_time_ms", "refresh_period_ms", "bytes"] {
            select_schema.schema.to_mut().push(ColumnSchema {
                column: ast::Column {
                    name: name.into(),
                    table: None,
                },
                column_type: DfType::UnsignedBigInt,
                base: None,
            });
            select_schema.columns.to_mut().push(name.into());
        }

        Ok(noria_connector::QueryResult::from_owned(
            select_schema,
            vec![Results::new(rows)],
        ))
    }

    /// Update our tracking of whether to route all queries to the Readyset schema.
    ///
    /// Returns true if we should route all queries to the Readyset schema.
    fn update_readyset_schema_routing(&mut self, search_path: &[SqlIdentifier]) -> bool {
        let Some(readyset_schema) = &self.readyset_schema else {
            return false;
        };
        let readyset_schema = SqlIdentifier::from(readyset_schema.name());
        self.readyset_schema_route_all = [readyset_schema] == search_path;
        self.readyset_schema_route_all
    }

    fn select_should_query_readyset_schema(
        &self,
        settings: &BackendSettings,
        query: &ShallowCacheQuery,
    ) -> bool {
        if !settings.allow_cache_ddl {
            return false;
        }
        let Some(readyset_schema) = &self.readyset_schema else {
            return false;
        };
        if references_schema(query, readyset_schema.name()) {
            return true;
        }
        self.readyset_schema_route_all && !references_variables(query)
    }

    fn non_select_should_query_readyset_schema(
        &self,
        settings: &BackendSettings,
        query: &ReadySetResult<SqlQuery>,
    ) -> bool {
        if !settings.allow_cache_ddl {
            return false;
        }
        if self.readyset_schema.is_none() {
            return false;
        }
        if let Ok(query) = query
            && query.is_readyset_extension()
        {
            return false;
        }
        if let Ok(
            SqlQuery::Select(_)
            | SqlQuery::CompoundSelect(_)
            | SqlQuery::Set(_)
            | SqlQuery::StartTransaction(_)
            | SqlQuery::Commit(_)
            | SqlQuery::Rollback(_)
            | SqlQuery::Comment(_)
            | SqlQuery::Discard(_),
        ) = query
        {
            return false;
        }
        self.readyset_schema_route_all
    }
}

/// Settings that have no state and are constant for a given [`Backend`]
struct BackendSettings {
    /// SQL dialect to use when parsing queries from clients
    dialect: Dialect,
    /// Parsing mode that determines which parser(s) to use and how to handle conflicts
    parsing_preset: ParsingPreset,
    slowlog: bool,
    require_authentication: bool,
    /// How to behave when receiving unsupported `SET` statements
    unsupported_set_mode: UnsupportedSetMode,
    /// How this backend handles migrations, See MigrationMode.
    migration_mode: MigrationMode,
    /// The maximum duration that a query can continuously fail for before we enter into a recovery
    /// period.
    query_max_failure_duration: Duration,
    /// The recovery period that we enter into for a given query, when that query has
    /// repeatedly failed for query_max_failure_duration.
    fallback_recovery_duration: Duration,
    /// Whether to automatically create inlined migrations for queries with unsupported
    /// placeholders.
    placeholder_inlining: bool,
    /// How Readyset handles CREATE CACHE statements without explicit DEEP or SHALLOW modifiers.
    cache_mode: CacheMode,
    /// Specifies the default TTL for shallow caches when no TTL is specified.
    default_ttl_ms: u64,
    /// Specifies the default coalesce interval for shallow caches when none is specified.
    default_coalesce_ms: u64,
    /// Whether replication is enabled. When true, CHANGE UPSTREAM is disallowed.
    replication_enabled: bool,
    /// Whether or not to allow cache ddl statements to be executed. If false, cache ddl statements
    /// received will instead return an error prompting the user to use Readyset cloud to manage
    /// their caches.
    allow_cache_ddl: bool,
    /// Per-category opt-ins for shallow-cache auto-creation eligibility. Adapter-local config
    /// (from CLI flags), consulted by the in-request-path auto-create filter.
    shallow_cache_eligibility: ShallowCacheEligibility,
}

impl BackendSettings {
    /// Whether to keep a copy of the sqlparser AST from a successful shallow parse so that when
    /// the shallow path declines, the Readyset AST can be derived by conversion instead of a
    /// second text parse. Requires a parsing preset for which the conversion is equivalent to a
    /// full parse, and a cache mode where deep caching is in play: in shallow-only mode the
    /// fall-through is already a single sqlparser parse, which doesn't justify taxing the
    /// shallow hit path with an AST clone.
    fn retain_shallow_ast(&self) -> bool {
        self.parsing_preset.prefers_sqlparser_ast() && !self.cache_mode.is_shallow()
    }
}

/// QueryInfo holds information regarding the last query that was sent along this connection
/// (Backend).
#[derive(Debug, Default)]
pub struct QueryInfo {
    pub destination: QueryDestination,
    pub reason: String,
}

impl QueryInfo {
    /// Fold a finished execution into the last-query record, if it reached a destination
    /// at all. A ReadySet error outranks the event's own reason: a query that errored has
    /// already said why it landed where it did.
    fn from_event(event: &QueryExecutionEvent) -> Option<Self> {
        Some(QueryInfo {
            destination: event.destination.clone()?,
            reason: event
                .noria_error
                .as_ref()
                .map(|e| e.to_string())
                .or_else(|| event.reason.clone())
                .unwrap_or_default(),
        })
    }

    /// Supply the seams' staged off-cache reason when the execution recorded none of its
    /// own, so a statement Readyset declined to serve from a cache it holds still reports
    /// why it went upstream.
    fn or_reason(mut self, staged: Option<&'static str>) -> Self {
        if self.reason.is_empty()
            && let Some(staged) = staged
        {
            self.reason = staged.to_string();
        }
        self
    }

    /// [`QueryInfo::from_event`], but moving the fields out of the event. For call sites where
    /// the event goes unsent afterwards, so the last-query record doesn't cost clones.
    fn take_from_event(event: &mut QueryExecutionEvent) -> Option<Self> {
        Some(QueryInfo {
            destination: event.destination.take()?,
            reason: event
                .noria_error
                .as_ref()
                .map(|e| e.to_string())
                .or_else(|| event.reason.take())
                .unwrap_or_default(),
        })
    }
}

impl FromRow for QueryInfo {
    fn from_row_opt(row: mysql_common::row::Row) -> Result<Self, FromRowError> {
        let mut res = QueryInfo::default();

        // Parse each column into its respective QueryInfo field.
        for (i, c) in row.columns_ref().iter().enumerate() {
            if let mysql_common::value::Value::Bytes(d) = row.as_ref(i).unwrap() {
                let dest = std::str::from_utf8(d).map_err(|_| FromRowError(row.clone()))?;

                if c.name_str() == "Query_destination" {
                    res.destination =
                        QueryDestination::try_from(dest).map_err(|_| FromRowError(row.clone()))?;
                } else if c.name_str() == "Readyset_reason" {
                    res.reason = std::str::from_utf8(d)
                        .map_err(|_| FromRowError(row.clone()))?
                        .to_string();
                } else {
                    return Err(FromRowError(row.clone()));
                }
            }
        }

        Ok(res)
    }
}

/// How to handle a migration in the adapter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MigrationMode {
    /// Handle migrations as part of the query process, if a query has not been
    /// successfully migrated when we are processing the query, attempt to
    /// perform the migration as part of the query.
    InRequestPath,
    /// Never perform migrations in the query path. If a query has not been
    /// migrated yet, send it to fallback if fallback exists, otherwise reject
    /// the query.
    ///
    /// This mode is used when some other operation is performing the
    /// migrations and updating a query's migration status. Either
    /// --query-caching=async which runs migrations in a separate thread,
    /// or --query-caching=explicit which enables special syntax to perform
    /// migrations "CREATE CACHE ..." may be used.
    OutOfBand,
}

/// Adapter clients need only one of the prepare results returned from prepare().
/// PrepareResult provides upstream_biased() to get the single relevant prepare result from
/// `PrepareResult`.
pub enum SinglePrepareResult<'a, DB: UpstreamDatabase> {
    Noria(&'a noria_connector::PrepareResult),
    Upstream(&'a UpstreamPrepare<DB>),
}

enum PrepareResultInner<DB: UpstreamDatabase> {
    Noria(noria_connector::PrepareResult),
    Upstream(UpstreamPrepare<DB>),
    NoriaAndUpstream(noria_connector::PrepareResult, UpstreamPrepare<DB>),
    Shallow(UpstreamPrepare<DB>),
}

impl<DB: UpstreamDatabase> Debug for PrepareResultInner<DB> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Noria(r) => f.debug_tuple("Noria").field(r).finish(),
            Self::Upstream(r) => f.debug_tuple("Upstream").field(r).finish(),
            Self::NoriaAndUpstream(nr, ur) => f
                .debug_tuple("NoriaAndUpstream")
                .field(nr)
                .field(ur)
                .finish(),
            Self::Shallow(r) => f.debug_tuple("Shallow").field(r).finish(),
        }
    }
}

/// The type returned when a query is prepared by `Backend` through the `prepare` function.
#[derive(Debug)]
pub struct PrepareResult<DB: UpstreamDatabase> {
    pub statement_id: StatementId,
    inner: PrepareResultInner<DB>,
}

impl<DB: UpstreamDatabase> PrepareResult<DB> {
    fn new(statement_id: StatementId, inner: PrepareResultInner<DB>) -> Self {
        Self {
            statement_id,
            inner,
        }
    }

    pub fn upstream_biased(&self) -> SinglePrepareResult<'_, DB> {
        match &self.inner {
            PrepareResultInner::Upstream(res)
            | PrepareResultInner::NoriaAndUpstream(_, res)
            | PrepareResultInner::Shallow(res) => SinglePrepareResult::Upstream(res),
            PrepareResultInner::Noria(res) => SinglePrepareResult::Noria(res),
        }
    }

    fn into_upstream(self) -> Option<UpstreamPrepare<DB>> {
        match self.inner {
            PrepareResultInner::Upstream(ur)
            | PrepareResultInner::NoriaAndUpstream(_, ur)
            | PrepareResultInner::Shallow(ur) => Some(ur),
            _ => None,
        }
    }

    /// If this [`PrepareResult`] is a [`PrepareResult::NoriaAndUpstream`], convert it into only a
    /// [`PrepareResult::Upstream`]
    fn make_upstream_only(&mut self) {
        match &mut self.inner {
            PrepareResultInner::Noria(_)
            | PrepareResultInner::Upstream(_)
            | PrepareResultInner::Shallow(_) => {}
            PrepareResultInner::NoriaAndUpstream(_, u) => {
                self.inner = PrepareResultInner::Upstream(u.clone())
            }
        }
    }
}

/// The type returned when a query is carried out by `Backend`, through either the `query` or
/// `execute` functions.
#[allow(clippy::large_enum_variant)]
pub enum QueryResult<'a, DB>
where
    DB: UpstreamDatabase + 'a,
{
    /// Results from noria
    Noria(noria_connector::QueryResult<'a>),
    /// Results from a Readyset shallow cache
    Shallow(readyset_shallow::QueryResult<DB::CacheEntry>),
    /// Results from upstream with optional pending shallow cache insert
    Upstream(
        DB::QueryResult<'a>,
        Option<CacheInsertGuard<ShallowKey, DB::CacheEntry>>,
        Option<&'a DB::ExecMeta>,
    ),
    /// Results from upstream that are explicitly buffered in a Vec (from postgres' Simple Query
    /// Protocol)
    UpstreamBufferedInMemory(DB::QueryResult<'a>),
    /// Results from parsing a SQL statement and determining that it's a command that should
    /// be handled at an outer layer.
    Parser(ParsedCommand),
    /// Results from a readyset-schema metadata query
    ReadysetSchema(readyset_schema::ReadysetSchemaResult),
}

impl<'a, DB: UpstreamDatabase> From<noria_connector::QueryResult<'a>> for QueryResult<'a, DB> {
    fn from(r: noria_connector::QueryResult<'a>) -> Self {
        Self::Noria(r)
    }
}

impl<DB> Debug for QueryResult<'_, DB>
where
    DB: UpstreamDatabase,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Noria(r) => f.debug_tuple("Noria").field(r).finish(),
            Self::Upstream(r, _, _) => f.debug_tuple("Upstream").field(r).finish(),
            Self::UpstreamBufferedInMemory(r) => {
                f.debug_tuple("UpstreamBufferedInMemory").field(r).finish()
            }
            Self::Parser(r) => f.debug_tuple("Parser").field(r).finish(),
            Self::Shallow(r) => f.debug_tuple("Shallow").field(r).finish(),
            Self::ReadysetSchema(r) => f.debug_tuple("ReadysetSchema").field(r).finish(),
        }
    }
}

/// What caused [`Backend::try_auto_create_shallow_cache`] to fire.  Used for
/// log/telemetry labels and to decide whether an eligibility rejection is
/// remembered in the in-request-path skip set; explicit hints are always
/// re-evaluated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AutoCreateTrigger {
    /// Explicit `/*rs+ CREATE SHALLOW CACHE */` hint — user opt-in.
    Hint,
    /// Implicit auto-create driven by `--query-caching=inrequestpath` +
    /// `--cache-mode=shallow`.
    InRequestPath,
}

impl AutoCreateTrigger {
    fn as_str(self) -> &'static str {
        match self {
            AutoCreateTrigger::Hint => "hint",
            AutoCreateTrigger::InRequestPath => "in-request-path",
        }
    }
}

/// TODO: The ideal approach for query handling is as follows:
/// 1. If we know we can't support a query, send it to fallback.
/// 2. If we think we can support a query, try to send it to ReadySet. If that hits an error that
///    should be retried, retry.    If not, try fallback without dropping the connection inbetween.
/// 3. If that fails and we got a MySQL error code, send that back to the client and keep the
///    connection open. This is a real correctness bug. 4. If we got another kind of error that is
///    retryable from fallback, retry. 5. If we got a non-retry related error that's not a MySQL
///    error code already, convert it to the most appropriate MySQL error code and write    that
///    back to the caller without dropping the connection.
impl<DB, Handler> Backend<DB, Handler>
where
    DB: 'static + UpstreamDatabase,
    DB::CacheEntry: SizeOf,
    Handler: 'static + QueryHandler,
{
    /// The name of the Readyset schema, when one is configured.
    pub fn readyset_schema_name(&self) -> Option<&str> {
        self.state
            .readyset_schema
            .as_ref()
            .map(|schema| schema.name())
    }

    pub fn version(&self) -> String {
        if let Some(version) = &self.state.db_version
            && !version.is_empty()
        {
            return version.clone();
        }

        self.connectors
            .upstream
            .as_ref()
            .map(|upstream| upstream.version())
            .unwrap_or_else(|| DB::DEFAULT_DB_VERSION.to_string())
    }

    /// Check if the upstream URL has changed and close the connection if so.
    ///
    /// When a routing change is detected, this returns an error to force the client to
    /// disconnect and reconnect, picking up the new upstream on the fresh connection.
    /// Rate-limited to at most once per second for the initial detection.
    async fn check_routing(
        connectors: &BackendConnectors<DB>,
        state: &mut BackendState<DB>,
    ) -> Result<(), DB::Error> {
        let err = || -> DB::Error {
            ReadySetError::ConnectionClosed(
                "upstream routing changed; reconnect to reach the new upstream".into(),
            )
            .into()
        };

        if state.routing_changed {
            return Err(err());
        }

        if state.upstream_config.is_none()
            || connectors.upstream.is_none()
            || state.last_routing_check.elapsed() < ROUTING_CHECK_INTERVAL
        {
            return Ok(());
        }
        state.last_routing_check = Instant::now();

        let shared = state
            .upstream_config
            .as_ref()
            .ok_or_else(|| internal_err!("upstream config is not configured"))?;
        let current_config = shared.read().await;
        if current_config.upstream_db_url == state.last_upstream_url {
            return Ok(());
        }

        state.routing_changed = true;
        Err(err())
    }

    /// Uses the provided query to update our tracking of whether to route all queries to the
    /// Readyset schema.
    ///
    /// If we should stop processing the current query, returns a result to be immediately returned
    /// to the client.
    fn check_readyset_schema_routing<'a>(
        state: &mut BackendState<DB>,
        query: &ReadySetResult<SqlQuery>,
    ) -> Option<QueryResult<'a, DB>> {
        state.readyset_schema.as_ref()?;

        let search_path = match query {
            Ok(SqlQuery::Set(s)) => Handler::handle_set_statement(s).set_search_path?,
            Ok(SqlQuery::Use(UseStatement { database })) => vec![database.into()],
            Ok(..) | Err(..) => return None,
        };

        state
            .update_readyset_schema_routing(search_path.as_slice())
            .then(|| QueryResult::Noria(noria_connector::QueryResult::Empty))
    }

    /// Get a session to the Readyset schema (backed by DataFusion).
    fn readyset_schema_session<'a>(
        connectors: &'a mut BackendConnectors<DB>,
        state: &BackendState<DB>,
    ) -> ReadySetResult<&'a ReadysetSchemaSession> {
        let Some(readyset_schema) = &state.readyset_schema else {
            internal!("Readyset schema not initialized");
        };
        Ok(connectors
            .readyset_schema_session
            .get_or_insert_with(|| readyset_schema.session()))
    }

    /// Set the session's connection charset and collation on the upstream connection, if one
    /// exists, so proxied literal semantics, result metadata, and result rows follow the
    /// client's charset. Return the collation the upstream fell back to when it does not
    /// support the requested one.
    pub async fn set_upstream_connection_charset(
        &mut self,
        charset: &str,
        collation: &str,
    ) -> Result<Option<UpstreamCollation>, DB::Error> {
        if let Some(upstream) = self.connectors.upstream.as_mut() {
            upstream.set_connection_charset(charset, collation).await
        } else {
            Ok(None)
        }
    }

    /// Send ping on the upstream connection, if it exists
    pub async fn ping(&mut self) -> Result<(), DB::Error> {
        Self::check_routing(&self.connectors, &mut self.state).await?;
        if let Some(upstream) = &mut self.connectors.upstream {
            upstream.ping().await
        } else {
            Ok(())
        }
    }
    /// Reset the current upstream connection
    pub async fn reset(&mut self) -> Result<(), DB::Error> {
        Self::check_routing(&self.connectors, &mut self.state).await?;
        if let Some(upstream) = &mut self.connectors.upstream {
            upstream.reset().await?;
            self.state.proxy_state = ProxyState::Fallback;
            Ok(())
        } else {
            // proxy_state is already Never when no upstream exists
            Ok(())
        }
    }

    /// Switch the active database for this backend to the given named database.
    ///
    /// Internally, this will set the schema search path to a single-element vector with the
    /// database, and send a `USE` command to the upstream, if any.
    pub async fn set_database(&mut self, db: &str) -> Result<(), DB::Error> {
        set_failpoint!(failpoints::SET_DATABASE, |_| Err(ReadySetError::Internal(
            "set-database failpoint injected".to_string()
        )
        .into()));

        Self::check_routing(&self.connectors, &mut self.state).await?;
        if self.state.update_readyset_schema_routing(&[db.into()]) {
            return Ok(());
        }

        if let Some(upstream) = &mut self.connectors.upstream {
            upstream
                .query(
                    &UseStatement {
                        database: db.into(),
                    }
                    .to_string(),
                )
                .await?;
        }
        self.connectors
            .noria
            .set_schema_search_path(vec![db.into()]);
        Ok(())
    }

    /// Updates connection tracking when the authenticated user changes.
    ///
    /// This removes the old connection entry (if any) and inserts a new entry
    /// with the updated username.
    fn update_connection_username(&mut self, new_username: &str) {
        if let Some(connections) = &self.state.connections {
            // Remove old connection entry
            let old_username = self
                .state
                .client_username
                .as_deref()
                .unwrap_or(UNAUTHENTICATED_USER);
            connections.remove(&ConnectionInfo::new(
                self.state.client_addr,
                old_username.to_string(),
            ));

            // Insert new connection entry with updated username
            connections.insert(ConnectionInfo::new(
                self.state.client_addr,
                new_username.to_string(),
            ));
        }

        self.state.client_username = Some(new_username.to_string());
        self.state.client_identity = Some(SqlIdentifier::from(new_username));
    }

    /// Open the upstream fallback connection for this session, if one is configured.
    pub async fn connect_upstream(
        &mut self,
        user: &str,
        password: Option<RedactedString>,
        interactive: bool,
    ) -> Result<(), DB::Error> {
        if password.is_some() {
            self.update_connection_username(user);
        }

        let Some(config) = &self.state.upstream_config else {
            return Ok(());
        };
        let config = config.read().await.clone();
        if config.upstream_db_url.is_none() {
            return Ok(());
        }

        let (username, password) = if self.settings.require_authentication {
            let password = password.ok_or_else(|| {
                internal_err!("authenticated connection reached upstream setup without a password")
            })?;
            (Some(user.to_string()), Some(password.to_string()))
        } else {
            (None, None)
        };

        // An unreachable upstream does not reject the connection: the session runs without an
        // upstream (statements needing one fail individually), so clients keep access to the
        // Readyset schema and its commands during an upstream outage.
        match DB::connect(config, username, password, interactive).await {
            Ok(upstream) => {
                self.connectors.upstream = Some(upstream);
                if let Some(telemetry_sender) = &self.state.telemetry_sender
                    && let Err(error) =
                        telemetry_sender.send_event(TelemetryEvent::UpstreamConnected)
                {
                    warn!(%error, "Failed to send upstream connected metric");
                }
            }
            Err(error) => {
                debug!(%error, "Failed to connect to the upstream; serving without one");
            }
        }
        Ok(())
    }

    pub async fn change_user(
        &mut self,
        user: &str,
        password: &str,
        database: &str,
    ) -> Result<(), DB::Error> {
        Self::check_routing(&self.connectors, &mut self.state).await?;

        if let Some(readyset_schema) = &self.state.readyset_schema
            && readyset_schema.name() == database
        {
            unsupported!("Change to Readyset schema is disallowed: {database}");
        }

        if let Some(upstream) = &mut self.connectors.upstream {
            upstream.change_user(user, password, database).await?;
        }
        if !database.is_empty() {
            self.connectors
                .noria
                .set_schema_search_path(vec![database.into()]);
        }

        // Update connection tracking with new authenticated username
        self.update_connection_username(user);

        Ok(())
    }

    /// Executes query on the upstream database, for when it cannot be parsed or executed by noria.
    /// Returns the query result, or an error if fallback is not configured
    async fn query_fallback<'a>(
        upstream: Option<&'a mut DB>,
        query: &'a str,
        event: &mut QueryExecutionEvent,
        cache: Option<CacheInsertGuard<ShallowKey, DB::CacheEntry>>,
    ) -> Result<QueryResult<'a, DB>, DB::Error> {
        let upstream =
            upstream.ok_or_else(|| no_upstream_err("Un-prepared fallback requires an upstream"))?;
        let _t = event.start_upstream_timer();
        let result = upstream.query(query).await;
        drop(_t);
        if let Some(cache) = &cache {
            event.reason = Some(SHALLOW_CACHE_MISS.to_string());
            event.destination = Some(QueryDestination::ReadysetThenUpstream(cache.cache_name()));
        } else {
            event.destination = Some(match &result {
                Ok(qr) => qr.destination(),
                Err(_) => QueryDestination::Upstream,
            });
        }
        result.map(|r| QueryResult::Upstream(r, cache, None))
    }

    /// Execute a prepared statement on upstream using the statement ID
    #[allow(clippy::too_many_arguments)]
    async fn execute_upstream<'a>(
        upstream: &'a mut DB,
        prep: &UpstreamPrepare<DB>,
        params: &[DfValue],
        exec_meta: &'a DB::ExecMeta,
        shallow_exec_meta: Option<&DB::ShallowExecMeta>,
        event: &mut QueryExecutionEvent,
        is_fallback: bool,
        cache: Option<CacheInsertGuard<ShallowKey, DB::CacheEntry>>,
    ) -> Result<QueryResult<'a, DB>, DB::Error> {
        if is_fallback {
            event.destination = Some(QueryDestination::ReadysetThenUpstream(None));
        } else if let Some(cache) = &cache {
            event.reason = Some(SHALLOW_CACHE_MISS.to_string());
            event.destination = Some(QueryDestination::ReadysetThenUpstream(cache.cache_name()));
        } else {
            event.destination = Some(QueryDestination::Upstream);
        }

        let _t = event.start_upstream_timer();

        let meta = shallow_exec_meta.map_or(exec_meta, |m| m.borrow());
        let result = upstream.execute(&prep.statement_id, params, meta).await?;

        let client_exec_meta = if cache.is_some() && shallow_exec_meta.is_some() {
            Some(exec_meta)
        } else {
            None
        };

        Ok(QueryResult::Upstream(result, cache, client_exec_meta))
    }

    /// Executes query on the upstream database using the "simple query" protocol, which buffers
    /// results in memory before returning. Note that this only applies to PostgreSQL backends, and
    /// for MySQL will return an error.
    pub async fn simple_query_upstream<'a>(
        &'a mut self,
        query: &'a str,
    ) -> Result<QueryResult<'a, DB>, DB::Error> {
        Self::check_routing(&self.connectors, &mut self.state).await?;
        let upstream = self
            .connectors
            .upstream
            .as_mut()
            .ok_or_else(|| no_upstream_err("Simple query requires an upstream"))?;
        let result = upstream.simple_query(query).await;
        result.map(QueryResult::UpstreamBufferedInMemory)
    }

    /// Prepares query on the upstream database, if present, when it cannot be parsed or prepared by
    /// noria.
    async fn prepare_fallback(
        &mut self,
        query: &str,
        data: DB::PrepareData<'_>,
        statement_type: PreparedStatementType,
    ) -> Result<UpstreamPrepare<DB>, DB::Error> {
        Self::check_routing(&self.connectors, &mut self.state).await?;
        let upstream = self
            .connectors
            .upstream
            .as_mut()
            .ok_or_else(|| no_upstream_err("Prepare fallback requires an upstream"))?;
        upstream.prepare(query, data, statement_type).await
    }

    /// Attach a per-Postgres-connection [`SessionContext`] populated
    /// from the authenticated `startup_user`.
    ///
    /// Called by the PG-specific backend after `set_auth_info` so that
    /// every subsequent code path that mirrors session state into the
    /// cache key (textual SET, set_config, COMMIT/ROLLBACK, etc.) has
    /// somewhere to write. The MySQL path leaves the field as `None`.
    pub fn attach_session(&mut self, startup_user: &str) {
        // Snapshot the login role's default GUCs once, here at connection time:
        // Postgres applies `ALTER ROLE ... SET` defaults at login and does not
        // reprocess them on later SET ROLE / SET SESSION AUTHORIZATION, so the
        // snapshot is frozen for the session's life.
        let (role_default_gucs, role_defaults_available) = match self.state.policy_registry.as_ref()
        {
            Some(registry) => (
                registry
                    .role_default_gucs_for(startup_user)
                    .map(|g| (*g).clone())
                    .unwrap_or_default(),
                registry.role_defaults_available(),
            ),
            None => (HashMap::new(), false),
        };
        self.connectors.session = Some(SessionContext::with_role_defaults(
            readyset_sql::ast::SqlIdentifier::from(startup_user),
            role_default_gucs,
            role_defaults_available,
        ));
    }

    /// Should only be called with a SqlQuery that is of type StartTransaction, Commit, or
    /// Rollback. Used to handle transaction boundary queries. Updates both the
    /// `ProxyState` machine and the [`SessionWriteTracker`] timestamp lifecycle (BEGIN
    /// clears, COMMIT refreshes-if-set, ROLLBACK clears).
    fn update_transaction_boundaries(
        proxy_state: &mut ProxyState,
        write_tracker: &mut SessionWriteTracker,
        query: &SqlQuery,
    ) {
        match query {
            SqlQuery::StartTransaction(_) => {
                proxy_state.start_transaction();
                write_tracker.on_start_transaction();
            }
            SqlQuery::Commit(_) => {
                proxy_state.end_transaction();
                write_tracker.on_commit();
            }
            SqlQuery::Rollback(rollback_stmt) if rollback_stmt.ends_transaction() => {
                proxy_state.end_transaction();
                write_tracker.on_rollback();
            }
            _ => (),
        }
    }

    /// Should only be called with a SqlQuery that is of type StartTransaction, Commit, or
    /// Rollback. Used to handle transaction boundary queries. Updates both the
    /// `ProxyState` machine and the [`SessionWriteTracker`] timestamp lifecycle.
    async fn handle_transaction_boundaries<'a>(
        upstream: Option<&'a mut DB>,
        proxy_state: &mut ProxyState,
        write_tracker: &mut SessionWriteTracker,
        query: &SqlQuery,
        raw_query: &'a str,
    ) -> Result<QueryResult<'a, DB>, DB::Error> {
        let upstream = upstream
            .ok_or_else(|| no_upstream_err("Transaction boundary fallback requires an upstream"))?;

        match query {
            SqlQuery::StartTransaction(_) => {
                // Forward the client's original text so modifiers the AST does not model
                // (isolation level, read-only, deferrable) are not silently dropped upstream.
                let result = QueryResult::Upstream(upstream.start_tx(raw_query).await?, None, None);
                proxy_state.start_transaction();
                write_tracker.on_start_transaction();
                Ok(result)
            }
            SqlQuery::Commit(_) => {
                let result = QueryResult::Upstream(upstream.commit().await?, None, None);
                proxy_state.end_transaction();
                write_tracker.on_commit();
                Ok(result)
            }
            SqlQuery::Rollback(rollback_stmt) => {
                if rollback_stmt.ends_transaction() {
                    let result = QueryResult::Upstream(upstream.rollback().await?, None, None);
                    proxy_state.end_transaction();
                    write_tracker.on_rollback();
                    Ok(result)
                } else {
                    // ROLLBACK TO SAVEPOINT does NOT end the transaction - it only rolls back
                    // to the savepoint. We must use query() to preserve the savepoint name.
                    let result =
                        QueryResult::Upstream(upstream.query(raw_query).await?, None, None);
                    Ok(result)
                }
            }
            _ => {
                error!(
                    "handle_transaction_boundary was called with a SqlQuery that was not of type \
                     StartTransaction, Commit, or Rollback"
                );
                internal!(
                    "handle_transaction_boundary was called with a SqlQuery that was not of type \
                     StartTransaction, Commit, or Rollback"
                );
            }
        }
    }

    /// Build the single-row result returned by a successful `CREATE CACHE`.
    fn create_cache_result(
        query_id: QueryId,
        name: &Relation,
        query: String,
        cache_type: CacheType,
    ) -> noria_connector::QueryResult<'static> {
        noria_connector::QueryResult::Meta(vec![
            MetaVariable {
                name: "query_id".into(),
                value: query_id.to_string(),
            },
            MetaVariable {
                name: "name".into(),
                value: name.display_unquoted().to_string(),
            },
            MetaVariable {
                name: "query".into(),
                value: query,
            },
            MetaVariable {
                name: "cache_type".into(),
                value: cache_type.to_string(),
            },
        ])
    }

    /// Mark or unmark this backend connection as an internal ReadySet connection
    pub fn set_internal_connection(&mut self, is_internal: bool) {
        self.state.is_internal_connection = is_internal;
    }

    pub fn does_require_authentication(&self) -> bool {
        self.settings.require_authentication
    }

    /// Whether anything downstream consumes a query execution event: the query logger, the
    /// slow-query log, or the background sampler.
    fn event_recording(&self) -> bool {
        self.state.query_log_sender.is_some()
            || self.settings.slowlog
            || self.state.sampler_tx.is_some()
    }

    /// Look up the plaintext password for `user`, if `user` is allowed to authenticate against
    /// this adapter.
    pub fn password_for_user(&self, user: &str) -> Option<String> {
        self.state.users.password_for(user)
    }

    /// The process-wide allowed-users handle for this backend.
    pub fn users(&self) -> &Arc<AllowedUsers> {
        &self.state.users
    }

    /// Prettify queries above an arbitrary length.
    /// Don't do it for MySQL because the terminal client doesn't handle newlines.
    fn format_query_text(query: String) -> String {
        if DB::SQL_DIALECT != readyset_sql::Dialect::MySQL && query.len() > 40 {
            sqlformat::format(&query, &Default::default(), &Default::default())
        } else {
            query
        }
    }

    /// Returns the current `ProxyState`, which protocol-specific backends
    /// can use to derive connection status flags (e.g. MySQL status flags).
    pub fn proxy_state(&self) -> ProxyState {
        self.state.proxy_state
    }

    pub fn in_transaction(&self) -> bool {
        self.state.proxy_state.in_transaction()
    }

    /// Returns true when autocommit is effectively on.
    /// This is true for all states except `AutocommitOff`.
    pub fn is_autocommit(&self) -> bool {
        self.state.proxy_state.is_autocommit()
    }

    /// Returns true when inside any transaction -- explicit (`BEGIN`) or
    /// implicit (`autocommit=0`). Distinct from [`in_transaction()`] which only
    /// covers explicit transactions (used by the PostgreSQL path).
    pub fn in_transaction_or_implicit(&self) -> bool {
        self.state.proxy_state.in_transaction_or_implicit()
    }

    async fn rewrite_context(
        connectors: &BackendConnectors<DB>,
        settings: &BackendSettings,
        state: &BackendState<DB>,
        search_path: Option<Vec<SqlIdentifier>>,
    ) -> ReadySetResult<RewriteContext> {
        Ok(RewriteContext::new(
            settings.dialect.into(),
            state.schema_handle.get_catalog_retrying().await?,
            search_path.unwrap_or_else(|| connectors.noria.schema_search_path().to_vec()),
        ))
    }
}

impl<DB, Handler> Drop for Backend<DB, Handler>
where
    DB: UpstreamDatabase,
{
    fn drop(&mut self) {
        if let Some(connections) = &self.state.connections {
            let username = self
                .state
                .client_username
                .as_deref()
                .unwrap_or(UNAUTHENTICATED_USER);
            connections.remove(&ConnectionInfo::new(
                self.state.client_addr,
                username.to_string(),
            ));
        }
        gauge!(metric::CONNECTED_CLIENTS).decrement(1.0);
        counter!(metric::CLIENT_CONNECTIONS_CLOSED).increment(1);
    }
}

/// Offloads recording query metrics to a separate thread. Sends a
/// message over a mpsc channel.
fn log_query(
    sender: Option<&UnboundedSender<QueryExecutionEvent>>,
    event: QueryExecutionEvent,
    slowlog: bool,
    dialect: Dialect,
) {
    const SLOW_DURATION: Duration = Duration::from_millis(5);

    let readyset_duration = event
        .readyset_event
        .as_ref()
        .map(|e| e.duration())
        .unwrap_or_default();

    if slowlog
        && (event.upstream_duration.unwrap_or_default() > SLOW_DURATION
            || readyset_duration > SLOW_DURATION)
        && let Some(query) = &event.query
    {
        warn!(
            query = %Sensitive(&query.display(dialect)),
            readyset_time = ?readyset_duration,
            upstream_time = ?event.upstream_duration,
            "slow query"
        );
    }

    if let Some(sender) = sender {
        // Drop the error if something goes wrong with query logging.
        if let Err(e) = sender.send(event) {
            warn!("Error logging query with query logging enabled: {}", e);
        }
    }
}

fn readyset_version() -> ReadySetResult<noria_connector::QueryResult<'static>> {
    Ok(noria_connector::QueryResult::MetaWithHeader(
        <Vec<(String, String)>>::from(READYSET_VERSION.clone())
            .into_iter()
            .map(MetaVariable::from)
            .collect(),
    ))
}

fn resolve_eviction_policy(
    policy: Option<ast::EvictionPolicy>,
    default_ttl_ms: u64,
) -> readyset_shallow::EvictionPolicy {
    match policy {
        Some(ast::EvictionPolicy::Ttl { ttl }) => readyset_shallow::EvictionPolicy::Ttl { ttl },
        Some(ast::EvictionPolicy::TtlAndPeriod {
            ttl,
            refresh,
            schedule,
        }) => readyset_shallow::EvictionPolicy::TtlAndPeriod {
            ttl,
            refresh,
            schedule,
        },
        None => readyset_shallow::EvictionPolicy::Ttl {
            ttl: Duration::from_millis(default_ttl_ms),
        },
    }
}

/// Build a synthetic `CREATE SHALLOW CACHE ...` DDL string for hint-based creation.
///
/// Emits the trx-cache-policy keyword so the policy survives a restart: caches reload by
/// re-parsing this persisted DDL via `recreate_shallow_caches`.
fn build_hint_ddl_string(dialect: Dialect, opts: &CreateCacheOptions, query_text: &str) -> String {
    // Hints create shallow caches only, so force the type: a bare `CREATE CACHE` hint still
    // materializes as shallow. The `CREATE [type] CACHE [name] WITH (...)` head renders through the
    // same `CreateCacheOptions` display as `CREATE CACHE` DDL, so every option carries through here
    // without per-option wiring; we only append the hint-specific `FROM <query>` tail.
    let opts = CreateCacheOptions {
        cache_type: Some(CacheType::Shallow),
        ..opts.clone()
    };
    format!("{} FROM {query_text}", opts.display(dialect))
}

fn resolve_coalesce(coalesce: Option<Duration>, default_coalesce_ms: u64) -> Option<Duration> {
    coalesce.or_else(|| match default_coalesce_ms {
        0 => None,
        _ => Some(Duration::from_millis(default_coalesce_ms)),
    })
}

/// Outcome of a single DDL's recovery attempt.
#[derive(Debug)]
enum RecoveryOutcome {
    /// The cache was recreated.
    Done,
    /// Recovery couldn't complete: one or more referenced relations
    /// aren't in the registry yet (the catalog poller hasn't seen
    /// them). The caller's retry loop tries again after the next
    /// poll tick.
    Deferred { unknown: Vec<String> },
    /// Recovery is permanently impossible — the analyzer Refused,
    /// or the parsed statement is non-recoverable. DDL is dropped.
    Skipped { reason: String },
}

/// Recreate shallow caches from stored DDL requests on adapter
/// startup. DDLs that couldn't be recovered immediately (because
/// the catalog poller hasn't observed the referenced relations yet)
/// are retried by a background task that re-attempts every
/// `retry_interval` until either success or exhaustion of the retry
/// budget.
#[allow(clippy::too_many_arguments)]
pub async fn recreate_shallow_caches<V>(
    shallow: Arc<CacheManager<ShallowKey, V>>,
    query_status_cache: &'static QueryStatusCache,
    ddl_requests: Vec<CacheDDLRequest>,
    parsing_preset: ParsingPreset,
    rewrite_params: AdapterRewriteParams,
    default_ttl_ms: u64,
    default_coalesce_ms: u64,
    cache_mode: CacheMode,
    policy_registry: Option<Arc<readyset_rls::PolicyRegistry>>,
    coordinator: Option<Arc<RlsCoordinator<V>>>,
) -> ReadySetResult<()>
where
    V: ContentHash + Debug + Send + Sync + SizeOf + 'static,
{
    let mut deferred: Vec<CacheDDLRequest> = Vec::new();
    for req in ddl_requests {
        let schema = req
            .schema_search_path
            .first()
            .map(|s| s.as_str().to_owned())
            .unwrap_or_default();
        match handle_shallow_cache_statement(
            &shallow,
            coordinator.as_ref(),
            query_status_cache,
            req.clone(),
            parsing_preset,
            rewrite_params,
            default_ttl_ms,
            default_coalesce_ms,
            cache_mode,
            policy_registry.as_ref(),
        )
        .await
        {
            Ok(RecoveryOutcome::Done) => {}
            Ok(RecoveryOutcome::Deferred { unknown }) => {
                info!(
                    schema = %schema,
                    unknown = ?unknown,
                    "deferring shallow cache recovery until next poll tick"
                );
                deferred.push(req);
            }
            Ok(RecoveryOutcome::Skipped { reason }) => {
                warn!(
                    schema = %schema,
                    reason = %reason,
                    "skipping recovery of shallow cache; upstream schema state makes it uncacheable"
                );
            }
            Err(e) => {
                warn!(error = %e, "Failed to handle shallow cache statement");
            }
        }
    }

    if let Some(registry) = policy_registry.clone()
        && !deferred.is_empty()
    {
        let shallow = Arc::clone(&shallow);
        tokio::spawn(retry_deferred_recoveries(
            shallow,
            query_status_cache,
            deferred,
            parsing_preset,
            rewrite_params,
            default_ttl_ms,
            default_coalesce_ms,
            cache_mode,
            registry,
            coordinator.clone(),
        ));
    }
    Ok(())
}

/// Interval between retry attempts for deferred recoveries.
const RECOVERY_RETRY_INTERVAL: std::time::Duration = std::time::Duration::from_secs(30);
/// Total budget for retry attempts before giving up. With a 30s
/// interval, this covers ~5 minutes of upstream catalog lag at boot.
const RECOVERY_RETRY_MAX_ATTEMPTS: u32 = 10;

#[allow(clippy::too_many_arguments)]
async fn retry_deferred_recoveries<V>(
    shallow: Arc<CacheManager<ShallowKey, V>>,
    query_status_cache: &'static QueryStatusCache,
    mut pending: Vec<CacheDDLRequest>,
    parsing_preset: ParsingPreset,
    rewrite_params: AdapterRewriteParams,
    default_ttl_ms: u64,
    default_coalesce_ms: u64,
    cache_mode: CacheMode,
    policy_registry: Arc<readyset_rls::PolicyRegistry>,
    coordinator: Option<Arc<RlsCoordinator<V>>>,
) where
    V: ContentHash + Debug + Send + Sync + SizeOf + 'static,
{
    for attempt in 1..=RECOVERY_RETRY_MAX_ATTEMPTS {
        tokio::time::sleep(RECOVERY_RETRY_INTERVAL).await;
        if pending.is_empty() {
            return;
        }
        let mut still_pending = Vec::new();
        for req in pending.drain(..) {
            match handle_shallow_cache_statement(
                &shallow,
                coordinator.as_ref(),
                query_status_cache,
                req.clone(),
                parsing_preset,
                rewrite_params,
                default_ttl_ms,
                default_coalesce_ms,
                cache_mode,
                Some(&policy_registry),
            )
            .await
            {
                Ok(RecoveryOutcome::Done) => {
                    info!(
                        attempt,
                        "deferred shallow cache recovery succeeded on retry"
                    );
                }
                Ok(RecoveryOutcome::Deferred { .. }) => {
                    still_pending.push(req);
                }
                Ok(RecoveryOutcome::Skipped { reason }) => {
                    warn!(
                        attempt,
                        reason = %reason,
                        "deferred recovery permanently refused on retry"
                    );
                }
                Err(e) => {
                    warn!(attempt, error = %e, "deferred recovery retry errored");
                }
            }
        }
        pending = still_pending;
    }
    if !pending.is_empty() {
        warn!(
            pending = pending.len(),
            attempts = RECOVERY_RETRY_MAX_ATTEMPTS,
            "abandoning deferred shallow cache recoveries; referenced relations \
             never reached the registry within the retry budget"
        );
    }
}

#[allow(clippy::too_many_arguments)]
async fn handle_shallow_cache_statement<V>(
    shallow: &CacheManager<ShallowKey, V>,
    coordinator: Option<&Arc<RlsCoordinator<V>>>,
    query_status_cache: &'static QueryStatusCache,
    req: CacheDDLRequest,
    parsing_preset: ParsingPreset,
    rewrite_params: AdapterRewriteParams,
    default_ttl_ms: u64,
    default_coalesce_ms: u64,
    cache_mode: CacheMode,
    policy_registry: Option<&Arc<readyset_rls::PolicyRegistry>>,
) -> ReadySetResult<RecoveryOutcome>
where
    V: ContentHash + Debug + Send + Sync + SizeOf + 'static,
{
    let query = readyset_sql_parsing::parse_query_with_config(
        parsing_preset,
        req.dialect.into(),
        &req.unparsed_stmt,
    )?;

    match query {
        SqlQuery::CreateCache(create_stmt) => {
            recover_shallow_cache_create(
                shallow,
                coordinator,
                query_status_cache,
                create_stmt,
                req.schema_search_path.clone(),
                rewrite_params,
                req,
                default_ttl_ms,
                default_coalesce_ms,
                cache_mode,
                policy_registry,
            )
            .await
        }
        _ => internal!("Unexpected statement: {:?}", query),
    }
}

#[allow(clippy::too_many_arguments)]
async fn recover_shallow_cache_create<V>(
    shallow: &CacheManager<ShallowKey, V>,
    coordinator: Option<&Arc<RlsCoordinator<V>>>,
    query_status_cache: &'static QueryStatusCache,
    stmt: CreateCacheStatement,
    schema_search_path: Vec<SqlIdentifier>,
    rewrite_params: AdapterRewriteParams,
    ddl_req: CacheDDLRequest,
    default_ttl_ms: u64,
    default_coalesce_ms: u64,
    cache_mode: CacheMode,
    policy_registry: Option<&Arc<readyset_rls::PolicyRegistry>>,
) -> ReadySetResult<RecoveryOutcome>
where
    V: ContentHash + Debug + Send + Sync + SizeOf + 'static,
{
    if !(matches!(stmt.cache_type, Some(CacheType::Shallow))
        || stmt.cache_type.is_none() && cache_mode.is_shallow())
    {
        internal!("Not a shallow cache");
    }

    let mut select_stmt = match stmt.inner {
        CacheInner::Statement { shallow: Ok(s), .. } => *s,
        CacheInner::Statement {
            shallow: Err(e), ..
        } => internal!("Failed to parse SELECT: {e}"),
        CacheInner::Id(_) => internal!("Cannot recreate from query ID"),
    };

    rewrite_shallow(&mut select_stmt, rewrite_params)?;

    let query_id = QueryId::from_shallow_query(&select_stmt, &schema_search_path);
    let name = stmt.name.unwrap_or_else(|| query_id.into());
    let display_name = name.display_unquoted().to_string();

    // Run the RLS analyzer at recovery time too. Without this a cache
    // persisted under a previous run that targeted a now-RLS-protected
    // table would come back as `Plain` and serve cross-tenant rows on
    // startup.
    let registration =
        match analyze_recovered_cache(policy_registry, &select_stmt, &schema_search_path) {
            RecoveryDeps::Plain => None,
            RecoveryDeps::PlainTracked { relations } => Some((relations, None)),
            RecoveryDeps::Scoped {
                relations,
                session_rls_inputs,
            } => Some((relations, Some(session_rls_inputs))),
            RecoveryDeps::WaitForPoll { unknown } => {
                return Ok(RecoveryOutcome::Deferred {
                    unknown: unknown.iter().map(|u| u.qualified()).collect(),
                });
            }
            RecoveryDeps::Skip { reason } => {
                return Ok(RecoveryOutcome::Skipped {
                    reason: format!("{display_name}: {reason}"),
                });
            }
        };

    shallow.create_cache(
        Some(name),
        query_id,
        select_stmt.clone(),
        schema_search_path.clone(),
        resolve_eviction_policy(stmt.policy, default_ttl_ms),
        ddl_req,
        stmt.trx_cache_policy,
        resolve_coalesce(stmt.coalesce_ms, default_coalesce_ms),
        stmt.adaptive,
    )?;

    if let (Some(coordinator), Some((relations, session_rls_inputs))) = (coordinator, registration)
    {
        match session_rls_inputs {
            Some(inputs) => {
                coordinator.register_scoped(query_id, inputs, relations);
            }
            None => coordinator.register_relations(query_id, relations),
        }
    }

    query_status_cache.update_query_migration_state(
        &ShallowViewRequest::new(select_stmt.clone(), schema_search_path.clone(), None),
        MigrationState::Successful(CacheType::Shallow),
        None,
    );
    query_status_cache.set_trx_cache_policy(
        &ShallowViewRequest::new(select_stmt, schema_search_path, None),
        stmt.trx_cache_policy,
    );

    Ok(RecoveryOutcome::Done)
}

enum RecoveryDeps {
    /// RLS disabled: plain cache, not coordinator-tracked.
    Plain,
    /// Plain cache that references RLS-eligible relations. Register the
    /// relations so an RLS flag flip on one of them later drops the cache.
    PlainTracked { relations: Vec<readyset_rls::Oid> },
    /// RLS-active: scoped cache keyed on `session_rls_inputs`.
    Scoped {
        relations: Vec<readyset_rls::Oid>,
        session_rls_inputs: Arc<[readyset_rls::SessionInputType]>,
    },
    /// Registry doesn't have the referenced relations yet (catalog
    /// poller hasn't observed them) but they may become resolvable
    /// after the next successful poll. Caller defers this DDL and
    /// retries later.
    WaitForPoll {
        unknown: Vec<crate::rls_relations::UnknownRelation>,
    },
    /// Permanent skip: analyzer Refused or the cache type is
    /// otherwise unrecoverable. Caller drops the DDL.
    Skip { reason: String },
}

fn analyze_recovered_cache(
    registry: Option<&Arc<readyset_rls::PolicyRegistry>>,
    select_stmt: &readyset_sql::ast::ShallowCacheQuery,
    schema_search_path: &[SqlIdentifier],
) -> RecoveryDeps {
    let Some(registry) = registry else {
        return RecoveryDeps::Plain;
    };
    let referenced_relations = match crate::rls_relations::extract_referenced_relation_oids(
        select_stmt,
        registry,
        schema_search_path,
    ) {
        Ok(oids) => oids,
        Err(unknown) => {
            // Registry may not have caught up yet. Defer rather
            // than permanently dropping; the retry loop tries
            // again after the next poll tick.
            return RecoveryDeps::WaitForPoll { unknown };
        }
    };
    let deps = readyset_rls::analyze_cache(registry, &referenced_relations);
    // Include the analyzer's expanded RLS tables so invalidation keys on
    // a view's underlying base tables, not just the query-referenced
    // relation (the view itself).
    let mut relations: Vec<readyset_rls::Oid> = referenced_relations;
    for &t in deps.rls_active_for_tables.iter() {
        if !relations.contains(&t) {
            relations.push(t);
        }
    }
    match deps.cacheability {
        readyset_rls::Cacheability::Cacheable if !deps.rls_active_for_tables.is_empty() => {
            RecoveryDeps::Scoped {
                relations,
                session_rls_inputs: deps.session_rls_inputs,
            }
        }
        readyset_rls::Cacheability::Cacheable => RecoveryDeps::PlainTracked { relations },
        readyset_rls::Cacheability::Refuse(reason) => RecoveryDeps::Skip {
            reason: reason.structured_display(),
        },
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use readyset_sql::Dialect;
    use readyset_sql::ast::{CreateCacheOptions, EvictionPolicy};
    use readyset_sql_parsing::parse_query;

    use super::*;

    fn acl_with(verdict: Option<Verdict>, identity: &str, cache: QueryId) -> AclHandle {
        let handle = AclHandle::disabled();
        if let Some(verdict) = verdict {
            handle.matrix().record(identity.into(), cache, verdict);
        }
        handle
    }

    #[test]
    fn acl_gate_inert_without_authentication() {
        let cache = QueryId::from_unparsed_select("select 1");
        let acl = acl_with(None, "alice", cache);
        // No identity at all, verdict Unknown: still allowed, the gate does not apply.
        assert!(acl_decline_reason(&acl, None, None, false, cache).is_none());
    }

    #[test]
    fn acl_gate_by_verdict() {
        let cache = QueryId::from_unparsed_select("select 1");
        let identity: SqlIdentifier = "alice".into();
        for (verdict, expected) in [
            (Some(Verdict::Allowed), true),
            (Some(Verdict::Denied), false),
            (Some(Verdict::Unknown), false),
            (None, false),
        ] {
            let acl = acl_with(verdict, "alice", cache);
            assert_eq!(
                acl_decline_reason(&acl, None, Some(&identity), true, cache).is_none(),
                expected,
                "verdict {verdict:?}"
            );
        }
    }

    #[test]
    fn acl_gate_fails_closed_without_identity() {
        let cache = QueryId::from_unparsed_select("select 1");
        let acl = acl_with(Some(Verdict::Allowed), "alice", cache);
        // Authenticated mode but no established identity: deny.
        assert_eq!(
            acl_decline_reason(&acl, None, None, true, cache),
            Some("cache_acl_untrusted")
        );
    }

    #[test]
    fn acl_gate_keys_on_effective_identity() {
        let cache = QueryId::from_unparsed_select("select 1");
        let acl = acl_with(Some(Verdict::Allowed), "alice", cache);
        let session = SessionContext::new("alice".into());
        assert!(acl_decline_reason(&acl, Some(&session), None, true, cache).is_none());

        // A role switch is judged by the assumed role's (absent) row.
        session.set_effective_role_scoped("limited".into(), false, false);
        assert_eq!(
            acl_decline_reason(&acl, Some(&session), None, true, cache),
            Some("cache_acl_unknown")
        );

        // An untrusted mirror fails closed even with an Allowed cell.
        let session = SessionContext::new("alice".into());
        session.mark_session_untrusted();
        assert_eq!(
            acl_decline_reason(&acl, Some(&session), None, true, cache),
            Some("cache_acl_untrusted")
        );
    }

    /// The verdict is the ACL's alone: the gate takes no transaction policy and no proxy
    /// state, so an `ALWAYS` pin has nothing to override a non-Allowed verdict with.
    #[test]
    fn acl_gate_independent_of_trx_policy() {
        let cache = QueryId::from_unparsed_select("select 1");
        let identity: SqlIdentifier = "alice".into();
        let denied = acl_with(Some(Verdict::Denied), "alice", cache);
        assert_eq!(
            acl_decline_reason(&denied, None, Some(&identity), true, cache),
            Some("cache_acl_denied")
        );
        let allowed = acl_with(Some(Verdict::Allowed), "alice", cache);
        assert!(acl_decline_reason(&allowed, None, Some(&identity), true, cache).is_none());
    }

    #[test]
    fn hint_ddl_string_includes_coalesce() {
        let opts = CreateCacheOptions {
            coalesce_ms: Some(Duration::from_secs(17)),
            ..Default::default()
        };
        let ddl = build_hint_ddl_string(Dialect::MySQL, &opts, "SELECT RAND()");
        assert_eq!(
            ddl,
            "CREATE SHALLOW CACHE WITH (COALESCE 17 SECONDS) FROM SELECT RAND()"
        );
    }

    #[test]
    fn hint_ddl_string_includes_policy_and_coalesce() {
        let opts = CreateCacheOptions {
            policy: Some(EvictionPolicy::Ttl {
                ttl: Duration::from_secs(271),
            }),
            coalesce_ms: Some(Duration::from_secs(17)),
            ..Default::default()
        };
        let ddl = build_hint_ddl_string(Dialect::MySQL, &opts, "SELECT RAND()");
        assert!(
            ddl.contains("COALESCE 17 SECONDS"),
            "DDL missing COALESCE: {ddl}"
        );
        assert!(
            ddl.contains("POLICY TTL 271 SECONDS"),
            "DDL missing POLICY: {ddl}"
        );
    }

    #[test]
    fn hint_ddl_coalesce_roundtrip() {
        for coalesce in [Duration::from_secs(17), Duration::from_millis(250)] {
            let opts = CreateCacheOptions {
                policy: Some(EvictionPolicy::Ttl {
                    ttl: Duration::from_secs(271),
                }),
                coalesce_ms: Some(coalesce),
                ..Default::default()
            };
            let ddl = build_hint_ddl_string(Dialect::MySQL, &opts, "SELECT RAND()");

            // Re-parse the generated DDL — this is the path taken on restart.
            let parsed = parse_query(Dialect::MySQL, &ddl).expect("DDL should parse");
            let SqlQuery::CreateCache(stmt) = parsed else {
                panic!("Expected CreateCache, got: {parsed:?}");
            };
            assert_eq!(
                stmt.coalesce_ms,
                Some(coalesce),
                "Coalesce must survive DDL round-trip: {ddl}"
            );
        }
    }

    #[test]
    fn hint_ddl_name_roundtrip() {
        let opts = CreateCacheOptions {
            name: Some("mycache".into()),
            trx_cache_policy: TrxCachePolicy::Always,
            ..Default::default()
        };
        let ddl = build_hint_ddl_string(Dialect::MySQL, &opts, "SELECT RAND()");

        let parsed = parse_query(Dialect::MySQL, &ddl).expect("DDL should parse");
        let SqlQuery::CreateCache(stmt) = parsed else {
            panic!("Expected CreateCache, got: {parsed:?}");
        };
        assert_eq!(
            stmt.name,
            Some("mycache".into()),
            "cache name must survive DDL round-trip: {ddl}"
        );
    }

    #[test]
    fn hint_ddl_concurrently_roundtrip() {
        // CONCURRENTLY is valid for shallow caches but was dropped by the old hand-rolled hint
        // serializer. Rendering through the shared header carries it through the round-trip.
        let opts = CreateCacheOptions {
            concurrently: true,
            ..Default::default()
        };
        let ddl = build_hint_ddl_string(Dialect::MySQL, &opts, "SELECT RAND()");

        let parsed = parse_query(Dialect::MySQL, &ddl).expect("DDL should parse");
        let SqlQuery::CreateCache(stmt) = parsed else {
            panic!("Expected CreateCache, got: {parsed:?}");
        };
        assert!(
            stmt.concurrently,
            "CONCURRENTLY must survive DDL round-trip: {ddl}"
        );
    }

    #[test]
    fn hint_ddl_emits_until_write() {
        let opts = CreateCacheOptions {
            trx_cache_policy: TrxCachePolicy::UntilWrite,
            ..Default::default()
        };
        let ddl = build_hint_ddl_string(Dialect::MySQL, &opts, "SELECT 1");
        assert!(
            ddl.contains("UNTIL WRITE"),
            "DDL missing UNTIL WRITE: {ddl}"
        );
    }

    #[test]
    fn hint_ddl_adaptive_roundtrip() {
        let opts = CreateCacheOptions {
            adaptive: true,
            trx_cache_policy: TrxCachePolicy::UntilWrite,
            ..Default::default()
        };
        let ddl = build_hint_ddl_string(Dialect::MySQL, &opts, "SELECT 1");
        assert!(ddl.contains("ADAPTIVE"), "DDL missing ADAPTIVE: {ddl}");

        // Re-parse the generated DDL — this is the path taken on restart.
        let parsed = parse_query(Dialect::MySQL, &ddl).expect("DDL should parse");
        let SqlQuery::CreateCache(stmt) = parsed else {
            panic!("Expected CreateCache, got: {parsed:?}");
        };
        assert!(stmt.adaptive, "adaptive must survive DDL round-trip: {ddl}");
    }

    #[test]
    fn hint_ddl_until_write_roundtrip() {
        for policy in [
            TrxCachePolicy::Never,
            TrxCachePolicy::UntilWrite,
            TrxCachePolicy::Always,
        ] {
            let opts = CreateCacheOptions {
                trx_cache_policy: policy,
                ..Default::default()
            };
            let ddl = build_hint_ddl_string(Dialect::MySQL, &opts, "SELECT 1");
            let parsed = parse_query(Dialect::MySQL, &ddl).expect("DDL should parse");
            let SqlQuery::CreateCache(stmt) = parsed else {
                panic!("Expected CreateCache, got: {parsed:?}");
            };
            assert_eq!(
                stmt.trx_cache_policy, policy,
                "policy must survive DDL round-trip: {ddl}"
            );
        }
    }
}
