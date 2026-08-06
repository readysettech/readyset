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

use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt::{self, Debug};
use std::marker::PhantomData;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::{
    Arc, OnceLock, PoisonError, RwLock as StdRwLock, RwLockReadGuard as StdRwLockReadGuard,
};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::rls_coordinator::RlsCoordinator;
use crate::session_context::SessionContext;
use crate::shallow_key::{SessionInputValues, ShallowKey};
use anyhow::bail;
use clap::ValueEnum;
use crossbeam_skiplist::SkipSet;
use database_utils::UpstreamConfig;
use failpoint_macros::set_failpoint;
use lru::LruCache;
use metrics::{counter, gauge};
use mysql_common::row::convert::{FromRow, FromRowError};
use readyset_adapter_types::{DeallocateId, ParsedCommand, PreparedStatementType};
use readyset_client::consensus::{Authority, AuthorityControl, CacheDDLRequest};
use readyset_client::post_processing::Results;
use readyset_client::schema::{ColumnSchema, SelectSchema};
use readyset_client::{CacheMode, ViewCreateRequest};
use readyset_client::{ShallowViewRequest, query::*};
pub use readyset_client_metrics::QueryDestination;
use readyset_client_metrics::{
    EventType, QueryExecutionEvent, QueryLogMode, ReadysetExecutionEvent, SqlQueryType,
};
use readyset_data::{DfType, DfValue};
use readyset_errors::ReadySetError;
use readyset_errors::{ReadySetResult, internal, internal_err, unsupported};
use readyset_metrics::metrics_handle;
use readyset_schema::{ReadysetSchema, ReadysetSchemaSession};
use readyset_shallow::{CacheInfo, CacheInsertGuard, CacheManager, CacheResult, ContentHash};
use readyset_sql::ast::{
    self, CacheInner, CacheType, CreateCacheOptions, CreateCacheStatement, DeallocateStatement,
    DiscardObject, ReadysetHintDirective, Relation, SessionAuthorizationValue,
    SetSessionAuthorization, SetStatement, ShallowCacheQuery, SqlIdentifier, SqlQuery,
    StatementIdentifier, TrxCachePolicy, UseStatement,
};
use readyset_sql::{Dialect, DialectDisplay, TryFromDialect};
use readyset_sql_parsing::ParsingPreset;
use readyset_sql_passes::adapter_rewrites::{
    AdapterRewriteParams, DfQueryParameters, QueryParameters, ShallowQueryParameters,
};
use readyset_sql_passes::shallow::{
    ShallowCacheAllowlists, ShallowCacheEligibility, auto_cache_skip_reasons, rewrite_shallow,
};
use readyset_sql_passes::{adapter_rewrites, detect_schema_references};
use readyset_telemetry_reporter::{TelemetryBuilder, TelemetryEvent, TelemetrySender};
use readyset_util::SizeOf;
#[cfg(feature = "failure_injection")]
use readyset_util::failpoints;
use readyset_util::redacted::{RedactedString, Sensitive};
use readyset_util::retry_with_exponential_backoff;
use readyset_version::READYSET_VERSION;
use tokio::sync::RwLock;
use tokio::sync::mpsc::UnboundedSender;
use tracing::{debug, error, info, trace, warn};

use crate::backend::noria_connector::ExecuteSelectContext;
use crate::query_handler::{SetBehavior, UpstreamSetRewrite};
use crate::query_status_cache::QueryStatusCache;
use crate::session_mutation;
use crate::status_reporter::ReadySetStatusReporter;
pub use crate::upstream_database::UpstreamPrepare;
use crate::utils::{create_dummy_column, time_or_null};
use crate::{QueryHandler, UpstreamDatabase, UpstreamDestination, create_dummy_schema};
use schema_catalog::{RewriteContext, SchemaCatalogHandle, SchemaGeneration};

mod extensions;
pub mod noria_connector;
mod prepared;
mod routing;

use self::noria_connector::MetaVariable;
pub use self::noria_connector::NoriaConnector;
use self::prepared::PreparedStatements;
pub use self::routing::ProxyState;
use self::routing::{SelectRouter, SessionWriteTracker, ShouldTrySelect, record_skip_cache};

/// Reserved program/application name used by ReadySet components to identify internal connections
pub const READYSET_QUERY_SAMPLER: &str = "READYSET_QUERY_SAMPLER";

/// Reserved program/application name reported by the shallow cache refresher on its upstream
/// connections so they are identifiable on the upstream database.
pub(crate) const READYSET_SHALLOW_REFRESHER: &str = "READYSET_SHALLOW_REFRESHER";

const UNSUPPORTED_CACHE_DDL_MSG: &str = "This instance has been provisioned through Readyset Cloud. Please use the Readyset Cloud UI to manage caches. You may continue to use the SQL interface to run other 'read' commands.";

/// Placeholder username for connections that have not yet authenticated
const UNAUTHENTICATED_USER: &str = "unauthenticated";

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
    fn password_for(&self, user: &str) -> Option<String> {
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
        upstream: Option<DB>,
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

        let proxy_state = if upstream.is_some() {
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
                upstream,
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
                parsed_query_cache: LruCache::new(10_000.try_into().expect("10000 is not 0")),
                prepared: Default::default(),
                query_status_cache,
                schema_handle,
                users: self.users,
                query_log_sender: self.query_log_sender,
                query_log_mode: self.query_log_mode,
                telemetry_sender: self.telemetry_sender,
                connections: self.connections,
                client_username: None,
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
    query_log_sender: Option<UnboundedSender<QueryExecutionEvent>>,
    query_log_mode: Option<QueryLogMode>,
    /// Provides the ability to send [`TelemetryEvent`]s to Segment
    telemetry_sender: Option<TelemetrySender>,
    /// Set of active connections to this adapter
    connections: Option<Arc<SkipSet<ConnectionInfo>>>,
    /// The authenticated username for this connection
    client_username: Option<String>,
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

        if let Some(coordinator) = &self.rls_coordinator {
            let dropped_id = query_id.or_else(|| info.as_ref().map(|i| i.query_id));
            if let Some(dropped_id) = dropped_id {
                coordinator.unregister(&dropped_id);
            }
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

    fn should_query_readyset_schema(
        &self,
        settings: &BackendSettings,
        query: &ReadySetResult<ShallowCacheQuery>,
    ) -> bool {
        if !settings.allow_cache_ddl {
            return false;
        }
        let Some(readyset_schema) = &self.readyset_schema else {
            return false;
        };
        let Ok(query) = query else {
            return self.readyset_schema_route_all;
        };
        if detect_schema_references::references_schema(query, readyset_schema.name()) {
            return true;
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

    /// Set the session's `character_set_results` on the upstream connection, if one exists, so
    /// proxied result rows come back in the client's charset
    pub async fn set_upstream_results_character_set(
        &mut self,
        charset: &str,
    ) -> Result<(), DB::Error> {
        if let Some(upstream) = self.connectors.upstream.as_mut() {
            upstream.set_results_character_set(charset).await?;
        }
        Ok(())
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
    }

    /// Change the user for the upstream connection, if it exists
    ///
    /// This is called when the client authenticates to the server.
    pub async fn set_user(
        &mut self,
        user: &str,
        password: RedactedString,
    ) -> Result<(), DB::Error> {
        Self::check_routing(&self.connectors, &mut self.state).await?;
        if let Some(upstream) = &mut self.connectors.upstream {
            let _ = upstream.set_user(user, password).await;
        }

        // Update connection tracking with authenticated username
        self.update_connection_username(user);

        Ok(())
    }

    /// Mark whether the client session is interactive, so the upstream connection (if any)
    /// is established with the matching capability when it is lazily opened.
    pub fn set_interactive(&mut self, interactive: bool) {
        if let Some(upstream) = &mut self.connectors.upstream {
            upstream.set_interactive(interactive);
        }
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
        let upstream = upstream.ok_or_else(|| {
            ReadySetError::Internal("Un-prepared fallback requires an upstream".to_string())
        })?;
        let _t = event.start_upstream_timer();
        let result = upstream.query(query).await;
        drop(_t);
        if let Some(cache) = &cache {
            event.reason = Some(SHALLOW_CACHE_MISS.to_string());
            event.destination = Some(QueryDestination::ReadysetThenUpstream(
                cache.cache_display_name(),
            ));
        } else {
            event.destination = Some(match &result {
                Ok(qr) => qr.destination(),
                Err(_) => QueryDestination::Upstream,
            });
        }
        result.map(|r| QueryResult::Upstream(r, cache, None))
    }

    /// Executes query on the upstream database using the "simple query" protocol, which buffers
    /// results in memory before returning. Note that this only applies to PostgreSQL backends, and
    /// for MySQL will return an error.
    pub async fn simple_query_upstream<'a>(
        &'a mut self,
        query: &'a str,
    ) -> Result<QueryResult<'a, DB>, DB::Error> {
        Self::check_routing(&self.connectors, &mut self.state).await?;
        let upstream = self.connectors.upstream.as_mut().ok_or_else(|| {
            ReadySetError::Internal("Simple query requires an upstream".to_string())
        })?;
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
        let upstream = self.connectors.upstream.as_mut().ok_or_else(|| {
            ReadySetError::Internal("Prepare fallback requires an upstream".to_string())
        })?;
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
        let upstream = upstream.ok_or_else(|| {
            ReadySetError::Internal(
                "Transaction boundary fallback requires an upstream".to_string(),
            )
        })?;

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

    /// Check whether a shallow cache exists for this query and should be used for routing.  If no
    /// cache exists and a `CreateCache` hint directive is present, attempt to create one first.
    /// Returns `(query_id, always)` when the query should be served from the shallow cache, `None`
    /// otherwise.
    ///
    /// If we haven't seen this query before, add it as pending to the query status cache.
    async fn should_query_shallow(
        connectors: &mut BackendConnectors<DB>,
        settings: &BackendSettings,
        state: &mut BackendState<DB>,
        shallow: &ShallowViewRequest,
        shallow_orig: &str,
        hint_directive: Option<ReadysetHintDirective>,
    ) -> Option<(QueryId, TrxCachePolicy)> {
        let (query_id, migration) =
            match state.query_status_cache.try_query_migration_state(shallow) {
                (id, Some(migration)) => (id, migration),
                (_, None) => state.query_status_cache.insert(ShallowViewRequest::new(
                    shallow.query.clone(),
                    shallow.schema_search_path.clone(),
                    Some(shallow_orig.to_string()),
                )),
            };

        if matches!(&hint_directive, Some(ReadysetHintDirective::SkipCache)) {
            if migration == MigrationState::Successful(CacheType::Shallow) {
                record_skip_cache(query_id.to_string(), "shallow", "hint");
            }
            return None;
        }
        if migration != MigrationState::Successful(CacheType::Shallow) {
            // No cache yet — try auto-creation (hint-driven or in-request-path).
            let migration = Self::try_auto_create_shallow_cache(
                connectors,
                settings,
                state,
                shallow,
                shallow_orig,
                hint_directive,
            )
            .await;
            if migration != Some(MigrationState::Successful(CacheType::Shallow)) {
                return None;
            }
        }
        let trx_cache_policy = state
            .query_status_cache
            .try_query_status(shallow)
            .map(|status| status.trx_cache_policy)
            .unwrap_or_default();
        if !SelectRouter::may_serve_from_cache(
            state.proxy_state,
            &mut state.write_tracker,
            trx_cache_policy,
            "shallow",
            true,
            || query_id.to_string(),
        ) {
            return None;
        }
        Some((query_id, trx_cache_policy))
    }

    /// Attempt to auto-create a shallow cache via [`create_shallow_cache_core`]
    /// and return the resulting migration state. Two triggers are supported:
    ///
    /// 1. An explicit `/*rs+ CREATE SHALLOW CACHE */` hint directive.
    /// 2. Implicit in-request-path auto-creation when the adapter runs with
    ///    `--query-caching=inrequestpath` and `--cache-mode=shallow`, mirroring
    ///    the `create_if_missing` behaviour on the deep side.
    async fn try_auto_create_shallow_cache(
        connectors: &mut BackendConnectors<DB>,
        settings: &BackendSettings,
        state: &BackendState<DB>,
        shallow: &ShallowViewRequest,
        shallow_orig: &str,
        hint_directive: Option<ReadysetHintDirective>,
    ) -> Option<MigrationState> {
        let (mut opts, trigger) = match hint_directive {
            Some(ReadysetHintDirective::CreateCache(opts)) => {
                let wants_shallow = match opts.cache_type {
                    Some(CacheType::Shallow) => true,
                    Some(CacheType::Deep) => false,
                    None => settings.cache_mode.is_shallow(),
                };
                if !wants_shallow {
                    return None;
                }
                (opts, AutoCreateTrigger::Hint)
            }
            None if settings.migration_mode == MigrationMode::InRequestPath
                && settings.cache_mode.is_shallow() =>
            {
                (
                    CreateCacheOptions::default(),
                    AutoCreateTrigger::InRequestPath,
                )
            }
            _ => return None,
        };

        // Filter implicit in-request-path attempts to prevent driver/ORM
        // bootstrap traffic (system-schema introspection, session variables,
        // non-deterministic functions) from polluting the cache.  Explicit
        // hints opt the user in deliberately and bypass the filter.
        //
        // The skip set in `query_status_cache` is consulted only here, so a
        // remembered rejection cannot block an explicit `CREATE SHALLOW
        // CACHE` DDL or a `/*rs+ CREATE SHALLOW CACHE */` hint.
        let query_id = QueryId::from(shallow);
        if matches!(trigger, AutoCreateTrigger::InRequestPath) {
            if state
                .query_status_cache
                .is_shallow_auto_create_skipped(query_id)
            {
                debug!(
                    "Shallow cache auto-creation skipped: previously rejected by eligibility filter"
                );
                return None;
            }
            let skip_reasons = auto_cache_skip_reasons(
                &shallow.query,
                settings.dialect,
                &settings.shallow_cache_eligibility,
                &state.shallow_cache_allowlists,
            );
            if !skip_reasons.is_empty() {
                let reasons = skip_reasons
                    .iter()
                    .map(|reason| reason.to_string())
                    .collect::<Vec<_>>()
                    .join("; ");
                state
                    .query_status_cache
                    .record_shallow_auto_create_skip(query_id, reasons);
                for reason in &skip_reasons {
                    counter!(metric::SHALLOW_AUTO_CREATE_SKIPPED, "reason" => reason.reason)
                        .increment(1);
                }
                debug!(
                    ?skip_reasons,
                    "Shallow cache auto-creation skipped: query not eligible"
                );
                return None;
            }
        }

        // Caches created automatically (in-request-path or hint with no policy keyword)
        // default to `UntilWrite` so that read-only-so-far transactions can still serve from
        // cache. Hints that explicitly set `ALWAYS` or `UNTIL WRITE` are respected.
        if matches!(opts.trx_cache_policy, TrxCachePolicy::Never) {
            opts.trx_cache_policy = TrxCachePolicy::UntilWrite;
        }

        // Implicit auto-cache creation turns on adaptive refresh; hints configure it
        // explicitly.
        if matches!(trigger, AutoCreateTrigger::InRequestPath) {
            opts.adaptive = true;
        }

        if !settings.allow_cache_ddl {
            warn!(
                trigger = trigger.as_str(),
                "Shallow cache auto-creation skipped: cache DDL is disabled"
            );
            return None;
        }

        if let Err(error) = connectors.upstream_supports(shallow_orig).await {
            warn!(
                trigger = trigger.as_str(),
                %error,
                "Shallow cache auto-creation failed: upstream unsupported"
            );
            return None;
        }

        let (query_id, name) = Self::resolve_id_and_name(None, query_id);
        let query_text = shallow.query.display(DB::SQL_DIALECT).to_string();
        let ddl_stmt = build_hint_ddl_string(DB::SQL_DIALECT, &opts, &query_text);
        let ddl_req = CacheDDLRequest {
            unparsed_stmt: ddl_stmt,
            schema_search_path: connectors.noria.schema_search_path().to_owned(),
            dialect: settings.dialect.into(),
            cache_name: None,
        };

        match Self::create_shallow_cache_core(
            settings,
            state,
            query_id,
            name,
            shallow,
            opts.policy,
            opts.trx_cache_policy,
            opts.coalesce_ms,
            opts.adaptive,
            ddl_req,
            true,
        )
        .await
        {
            Ok(()) | Err(ReadySetError::ViewAlreadyExists(_)) => {}
            Err(e) => {
                warn!(trigger = trigger.as_str(), error = %e, "Shallow cache auto-creation failed");
            }
        }

        state
            .query_status_cache
            .try_query_migration_state(shallow)
            .1
    }

    async fn query_shallow<'a>(
        connectors: &'a mut BackendConnectors<DB>,
        state: &BackendState<DB>,
        query: &'a str,
        query_id: QueryId,
        event: &mut QueryExecutionEvent,
        params: ShallowQueryParameters,
    ) -> Result<QueryResult<'a, DB>, DB::Error> {
        let params_key = params.make_keys(&[])?;
        let start = Instant::now();

        let mut session_values = SessionInputValues::default();
        if let Some(coordinator) = state.rls_coordinator.as_ref()
            && coordinator
                .fill_rls_session_inputs(
                    &query_id,
                    connectors.session.as_ref().map(|s| s.as_ref()),
                    &mut session_values,
                )
                .is_err()
        {
            return Self::query_fallback(connectors.upstream.as_mut(), query, event, None).await;
        }
        let shallow_key = ShallowKey {
            params: params_key,
            session: session_values,
            charset: connectors.noria.results_encoding(),
        };

        // An entry keyed on session state does not refresh via the
        // session-less pool, which has no session to resolve those values.
        let session_keyed = !shallow_key.session.is_empty();

        let res = state
            .shallow
            .get_or_start_insert(&query_id, shallow_key, |_| true)
            .await;

        let cache_name = state
            .shallow
            .get(None, Some(&query_id))
            .and_then(|cache| cache.display_name());

        match res {
            CacheResult::Hit(values) => {
                event.readyset_event = Some(ReadysetExecutionEvent::Other {
                    duration: start.elapsed(),
                });
                event.destination = Some(QueryDestination::ReadysetShallow(cache_name));
                Ok(QueryResult::Shallow(values))
            }
            CacheResult::HitAndRefresh(values, cache) => {
                if let (false, Some(refresh)) = (session_keyed, state.shallow_refresh_pool.as_ref())
                {
                    let request = ShallowRefreshRequest {
                        query_id,
                        path: connectors.noria.schema_search_path().to_vec(),
                        query: query.to_string(),
                        cache,
                        shallow_exec_meta: None,
                    };
                    refresh.send(request).await;
                }

                event.readyset_event = Some(ReadysetExecutionEvent::Other {
                    duration: start.elapsed(),
                });
                event.destination = Some(QueryDestination::ReadysetShallow(cache_name));
                Ok(QueryResult::Shallow(values))
            }
            CacheResult::Miss(mut cache) => {
                if let (false, Some(refresh)) = (session_keyed, state.shallow_refresh_pool.as_ref())
                    && cache.is_scheduled()
                {
                    let refresh = refresh.clone();
                    let path = connectors.noria.schema_search_path().to_vec();
                    let q = query.to_string();
                    let callback = Arc::new(move |cache| {
                        let req = ShallowRefreshRequest::<DB::CacheEntry, DB::ShallowExecMeta> {
                            query_id,
                            path: path.clone(),
                            query: q.clone(),
                            cache,
                            shallow_exec_meta: None,
                        };
                        refresh.spawn_send(req);
                    });
                    cache.schedule_refresh(callback).await;
                };
                Self::query_fallback(connectors.upstream.as_mut(), query, event, Some(cache)).await
            }
            CacheResult::NotCached => Err(ReadySetError::NoCacheForQuery.into()),
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn try_noria_adhoc_select<'a>(
        connectors: &'a mut BackendConnectors<DB>,
        settings: &BackendSettings,
        state: &mut BackendState<DB>,
        query: &'a str,
        mut view_request: ViewCreateRequest,
        params: QueryParameters,
        schema_generation: SchemaGeneration,
        event: &mut QueryExecutionEvent,
        is_skip_cache: bool,
    ) -> Result<QueryResult<'a, DB>, DB::Error> {
        let verdict = SelectRouter::new(
            settings.dialect,
            connectors.noria.rewrite_params(),
            state.query_status_cache,
            state.proxy_state,
            &mut state.write_tracker,
        )
        .route(&mut view_request, params, schema_generation, is_skip_cache);
        match verdict {
            ShouldTrySelect::Yes {
                status,
                params,
                schema_generation,
            } => {
                Self::noria_adhoc_select(
                    connectors,
                    settings,
                    state,
                    query,
                    view_request,
                    status,
                    event,
                    params,
                    schema_generation,
                )
                .await
            }
            ShouldTrySelect::No { error } => {
                if connectors.upstream.is_none() {
                    Err(error
                        .unwrap_or(ReadySetError::InvalidUpstreamDatabase)
                        .into())
                } else {
                    Self::query_fallback(connectors.upstream.as_mut(), query, event, None).await
                }
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn noria_adhoc_select<'a>(
        connectors: &'a mut BackendConnectors<DB>,
        settings: &BackendSettings,
        state: &mut BackendState<DB>,
        original_query: &'a str,
        view_request: ViewCreateRequest,
        mut status: QueryStatus,
        event: &mut QueryExecutionEvent,
        mut params: DfQueryParameters,
        schema_generation: SchemaGeneration,
    ) -> Result<QueryResult<'a, DB>, DB::Error> {
        // Track the schema generation that was used to rewrite this query so that
        // CREATE CACHE FROM <query_id> can retrieve it later. Without this, ad-hoc
        // (text protocol) queries would never store their generation in the QSC.
        status.schema_generation = Some(schema_generation);

        let original_status = status.clone();
        let did_work = if let Some(ref mut i) = status.execution_info {
            i.reset_if_exceeded_recovery(
                settings.query_max_failure_duration,
                settings.fallback_recovery_duration,
            )
        } else {
            false
        };

        // A manually parameterized cache (`AUTOPARAM`) claiming this query's standard shape
        // serves the read regardless of the shape's own migration state.
        let manual_cache = state
            .query_status_cache
            .manual_cache(&QueryId::from(&view_request));
        let served_via_manual_cache = manual_cache.is_some();
        if let Some(mc) = &manual_cache {
            // The frozen literals travel with the params so the readsider can strip them from the
            // lookup key; if they don't match these params the cache doesn't serve this query, so
            // go straight upstream as a clean miss.
            params.set_frozen(mc.frozen.clone());
            if connectors.upstream.is_some() && !params.frozen_satisfied(&[])? {
                return Self::query_fallback(
                    connectors.upstream.as_mut(),
                    original_query,
                    event,
                    None,
                )
                .await;
            }
        }

        // Test several conditions to see if we should proxy
        let upstream_exists = connectors.upstream.is_some();
        let proxy_out_of_band = settings.migration_mode != MigrationMode::InRequestPath
            && !matches!(status.migration_state, MigrationState::Successful(_))
            && manual_cache.is_none();
        let unsupported = matches!(&status.migration_state, MigrationState::Unsupported(_))
            && manual_cache.is_none();
        let exceeded_network_failure = status
            .execution_info
            .as_mut()
            .map(|i| i.execute_network_failure_exceeded(settings.query_max_failure_duration))
            .unwrap_or(false);

        if !matches!(status.trx_cache_policy, TrxCachePolicy::Always)
            && (upstream_exists && (proxy_out_of_band || unsupported || exceeded_network_failure))
        {
            if did_work {
                state.query_status_cache.update_transition_time(
                    &view_request,
                    &status.execution_info.unwrap().last_transition_time,
                );
            }
            return Self::query_fallback(connectors.upstream.as_mut(), original_query, event, None)
                .await;
        }

        event.destination = Some(QueryDestination::Readyset(None));
        let create_if_missing = settings.migration_mode == MigrationMode::InRequestPath;

        let ctx = ExecuteSelectContext::AdHoc {
            statement: &view_request.statement,
            create_if_missing,
            processed_query_params: params,
            schema_generation,
            manual_cache_name: manual_cache.map(|mc| mc.name),
        };
        let res = connectors.noria.execute_select(ctx, event).await;
        if status.execution_info.is_none() {
            status.execution_info = Some(ExecutionInfo {
                state: ExecutionState::Failed,
                last_transition_time: Instant::now(),
            });
        }

        match res {
            Ok(noria_ok) => {
                // We managed to select on ReadySet, good for us. Don't promote the standard
                // (fully autoparameterized) shape's status when the read was served by a manual
                // cache: that shape has no deep cache of its own, so a stale `Successful` would
                // make later queries attempt a non-existent view once the manual cache is dropped.
                if !served_via_manual_cache {
                    status.migration_state = MigrationState::Successful(CacheType::Deep);
                }
                if let Some(i) = status.execution_info.as_mut() {
                    i.execute_succeeded()
                }
                if status != original_status {
                    state
                        .query_status_cache
                        .update_query_status(&view_request, status);
                }
                // Enqueue the original query for background sampling if enabled.
                if !state.is_internal_connection
                    && let Some(tx) = state.sampler_tx.as_ref()
                {
                    let schema_search_path = view_request.schema_search_path.clone();
                    let _ = tx.try_send((
                        event.clone(),
                        original_query.to_string(),
                        schema_search_path,
                    ));
                }
                Ok(noria_ok.into())
            }
            Err(noria_err) => {
                event.set_noria_error(&noria_err);

                if let Some(i) = status.execution_info.as_mut() {
                    if noria_err.is_networking_related() {
                        i.execute_network_failure();
                    } else if noria_err.caused_by_view_destroyed() {
                        i.execute_dropped();
                    }
                }

                if noria_err.caused_by_view_not_found() {
                    status.migration_state = MigrationState::Pending;
                } else if noria_err.caused_by_unsupported() {
                    status.migration_state = MigrationState::Unsupported(
                        noria_err.unsupported_cause().unwrap_or_default(),
                    );
                };

                let always = matches!(status.trx_cache_policy, TrxCachePolicy::Always);

                if status != original_status {
                    state
                        .query_status_cache
                        .update_query_status(&view_request, status);
                }

                // Try to execute on fallback if present, as long as query is not an `always`
                // query.
                match (always, connectors.upstream.as_mut()) {
                    (true, _) | (_, None) => {
                        // Enqueue the original query for background sampling if enabled.
                        if !state.is_internal_connection
                            && let Some(tx) = state.sampler_tx.as_ref()
                        {
                            let schema_search_path = view_request.schema_search_path.clone();
                            let _ = tx.try_send((
                                event.clone(),
                                original_query.to_string(),
                                schema_search_path,
                            ));
                        }
                        Err(noria_err.into())
                    }
                    (false, Some(fallback)) => {
                        event.destination = Some(QueryDestination::ReadysetThenUpstream(None));
                        let _t = event.start_upstream_timer();
                        fallback
                            .query(original_query)
                            .await
                            .map(|r| QueryResult::Upstream(r, None, None))
                    }
                }
            }
        }
    }

    /// Handles a parsed set statement by deferring to `Handler::handle_set_statement` and
    /// respecting `BackendSettings::unsupported_set_mode`. When the search path is changed
    /// (SetBehavior::SetSearchPath) or other sets need to be handled (certain variables being
    /// changed), the `noria` instance gets updated accordingly.
    ///
    /// - If upstream exists, valid set statements are forwarded to it.
    /// - If no upstream is present, statements are typically ignored.
    /// - Disallowed set statements always produce an error.
    fn handle_set(
        connectors: &mut BackendConnectors<DB>,
        settings: &BackendSettings,
        state: &mut BackendState<DB>,
        query: &str,
        set: &SetStatement,
        event: &mut QueryExecutionEvent,
    ) -> Result<UpstreamSetRewrite, DB::Error> {
        let SetBehavior {
            unsupported,
            proxy: _, // Basically ignored, caller will proxy unless we return an error
            set_autocommit,
            set_search_path,
            set_results_encoding,
            set_client_encoding,
            upstream_rewrite,
            set_timezone,
        } = Handler::handle_set_statement(set);

        // NOTE: The unsupported check runs before autocommit processing intentionally.
        // A compound SET like `SET autocommit=0, unknown_var=1` is rejected atomically
        // in Error mode — the autocommit state change is not applied. This matches
        // MySQL's all-or-nothing SET semantics.
        if unsupported {
            match settings.unsupported_set_mode {
                UnsupportedSetMode::Error => {
                    let e = ReadySetError::SetDisallowed {
                        statement: query.to_string(),
                    };
                    if connectors.upstream.is_some() {
                        event.set_noria_error(&e);
                    }
                    error!(
                        set = %set.display(settings.dialect),
                        "received unsupported SET statement."
                    );
                    return Err(e.into());
                }
                UnsupportedSetMode::Proxy => {
                    warn!(
                        set = %set.display(settings.dialect),
                        "received unsupported SET statement."
                    );
                    state.proxy_state = ProxyState::ProxyAlways;
                }
                UnsupportedSetMode::Allow => {}
            }
        }
        if let Some(enabled) = set_autocommit {
            let prev = state.proxy_state;
            state.proxy_state.set_autocommit(enabled);
            if state.proxy_state != prev {
                // `SET autocommit=1` from a transactional state does an implicit COMMIT;
                // refresh `last_write_at` so any RYW window fires from now.
                if enabled && matches!(prev, ProxyState::InTransaction | ProxyState::AutocommitOff)
                {
                    state.write_tracker.on_commit();
                }
                if matches!(state.proxy_state, ProxyState::AutocommitOff) {
                    debug!(
                        set = %set.display(settings.dialect),
                        "Autocommit disabled; all queries will be proxied upstream"
                    );
                    counter!(metric::SET_AUTOCOMMIT_DISABLED).increment(1);
                } else if matches!(prev, ProxyState::AutocommitOff) {
                    debug!(
                        set = %set.display(settings.dialect),
                        "Autocommit re-enabled"
                    );
                    counter!(metric::SET_AUTOCOMMIT_ENABLED).increment(1);
                }
            }
        }
        if let Some(search_path) = set_search_path {
            trace!(?search_path, "Setting search_path");
            connectors.noria.set_schema_search_path(search_path);
        }
        if let Some(encoding) = set_results_encoding {
            trace!(?encoding, "Setting results_encoding");
            connectors.noria.set_results_encoding(encoding);
        }
        if let Some(encoding) = set_client_encoding {
            trace!(?encoding, "Setting client_encoding");
            connectors.noria.set_client_encoding(encoding);
        }
        // The handler records `set_timezone` even for non-UTC values so a
        // future eval-side fix can read it unchanged; only apply it here when
        // the SET resolved to a UTC-equivalent zone — otherwise cached
        // results (UTC-wallclock today) would be silently localized.
        if let Some(tz) = set_timezone
            && !unsupported
        {
            trace!(?tz, "Setting timezone");
            connectors.noria.set_timezone(tz);
        }

        // Mirror the SET into the per-connection SessionContext so the RLS shallow cache can hash
        // by the relevant subset of session state. GUC sets and `RESET ROLE` are applied now; they
        // cannot be rejected as an authorization decision. `SET ROLE` (the `RoleSet` effect) is
        // deliberately not applied here -- role membership is an authorization boundary, so it is
        // mirrored only after upstream accepts it (`mirror_set_role`), matching
        // `SET SESSION AUTHORIZATION`. `apply_set_statement` does not mutate for `RoleSet`, so
        // discarding its result leaves the effective role untouched.
        if let Some(session) = connectors.session.as_ref() {
            let _ = session.apply_set_statement(set);
        }

        Ok(upstream_rewrite)
    }

    /// Mirror `SET [LOCAL] ROLE <role>` into the session context, called only after upstream
    /// accepted the statement. Resolves `bypass_rls` against the policy registry. Non-`SET ROLE`
    /// statements (and `RESET ROLE`, already applied by `handle_set`) are ignored.
    fn mirror_set_role(
        session: &SessionContext,
        policy_registry: Option<&Arc<readyset_rls::PolicyRegistry>>,
        set: &SetStatement,
    ) {
        if let Some((role, local)) = SessionContext::pending_set_role(set) {
            let bypass = policy_registry.is_some_and(|reg| reg.bypass_rls_for_role(role.as_str()));
            session.set_effective_role_scoped(role, bypass, local);
        }
    }

    /// Mirror a `SET [LOCAL] SESSION AUTHORIZATION` into the session context,
    /// called only after upstream accepted the statement.
    ///
    /// A session-scope change (`local = false`) resolves to a concrete identity
    /// -- `DEFAULT` (and `RESET SESSION AUTHORIZATION`) to the startup user, a
    /// named user directly -- with `bypass_rls` resolved against the policy
    /// registry, and updates the mirror so later reads partition by it. A
    /// transaction-local change (`local = true`) reverts at the transaction
    /// boundary, which the mirror cannot model for `session_user`, so it fails
    /// closed (transaction-scoped) until `COMMIT` / `ROLLBACK`.
    fn mirror_session_authorization(
        session: &SessionContext,
        policy_registry: Option<&Arc<readyset_rls::PolicyRegistry>>,
        auth: &SetSessionAuthorization,
    ) {
        if auth.local {
            session.mark_transaction_untrusted();
            return;
        }
        let role = match &auth.value {
            SessionAuthorizationValue::Default => session.startup_user.clone(),
            SessionAuthorizationValue::User(user) => user.clone(),
        };
        let bypass = policy_registry.is_some_and(|reg| reg.bypass_rls_for_role(role.as_str()));
        session.apply_session_authorization(role, bypass);
    }

    async fn query_adhoc_non_select<'a>(
        connectors: &'a mut BackendConnectors<DB>,
        settings: &BackendSettings,
        state: &mut BackendState<DB>,
        raw_query: &'a str,
        event: &mut QueryExecutionEvent,
        query: SqlQuery,
    ) -> Result<QueryResult<'a, DB>, DB::Error> {
        let mut upstream_set_rewrite = UpstreamSetRewrite::ProxyVerbatim;
        match &query {
            SqlQuery::Set(s) => {
                upstream_set_rewrite =
                    Self::handle_set(connectors, settings, state, raw_query, s, event)?
            }
            SqlQuery::Use(UseStatement { database }) => connectors
                .noria
                .set_schema_search_path(vec![database.clone()]),
            SqlQuery::Commit(_) | SqlQuery::Rollback(_) => {
                // `ROLLBACK TO SAVEPOINT` does not end the transaction, so it
                // must not revert transaction-local RLS state or clear a
                // transaction-scoped trust gap.
                let ends_transaction = match &query {
                    SqlQuery::Rollback(rollback_stmt) => rollback_stmt.ends_transaction(),
                    _ => true,
                };
                if ends_transaction && let Some(session) = connectors.session.as_ref() {
                    session.on_trx_end();
                }
            }
            _ => (),
        }

        {
            // Upstream reads are tried when noria reads produce an error. Upstream writes are done
            // by default when the upstream connector is present.
            if let Some(upstream) = connectors.upstream.as_mut() {
                match query {
                    SqlQuery::Select(_) => unreachable!("read path returns prior"),
                    SqlQuery::Insert(_) | SqlQuery::Update(_) | SqlQuery::Delete(_) => {
                        event.sql_type = SqlQueryType::Write;
                        event.destination = Some(QueryDestination::Upstream);
                        let _t = event.start_upstream_timer();

                        let query_result = upstream.query(raw_query).await;
                        query_result.map(|r| QueryResult::Upstream(r, None, None))
                    }

                    SqlQuery::CreateDatabase(_)
                    | SqlQuery::CreateView(_)
                    | SqlQuery::CreateTable(_)
                    | SqlQuery::DropTable(_)
                    | SqlQuery::DropView(_)
                    | SqlQuery::AlterTable(_)
                    | SqlQuery::RenameTable(_)
                    | SqlQuery::Truncate(_)
                    | SqlQuery::Use(_)
                    | SqlQuery::CreateIndex(_) => {
                        event.sql_type = SqlQueryType::Other;
                        upstream
                            .query(raw_query)
                            .await
                            .map(|r| QueryResult::Upstream(r, None, None))
                    }
                    SqlQuery::Set(set) => {
                        event.sql_type = SqlQueryType::Other;
                        match upstream_set_rewrite {
                            UpstreamSetRewrite::ProxyVerbatim => {}
                            UpstreamSetRewrite::Rewrite(stmt) => {
                                // The trait ties the result's lifetime to the query text, so a
                                // rewritten statement's result can't be returned; a SET only
                                // produces an OK, so respond as the no-upstream branch does.
                                let rewritten = stmt.display(settings.dialect).to_string();
                                upstream.query(&rewritten).await?;
                                return Ok(QueryResult::Noria(noria_connector::QueryResult::Empty));
                            }
                            UpstreamSetRewrite::Skip => {
                                return Ok(QueryResult::Noria(noria_connector::QueryResult::Empty));
                            }
                        }
                        let result = upstream.query(raw_query).await?;
                        // Mirror an identity change only now that upstream has accepted it: a
                        // rejected statement must not leave the session mirror pointing at an
                        // identity upstream never adopted. `SET ROLE` is an authorization boundary
                        // (role membership), so mirroring it before the forward would let a client
                        // assume a role upstream refused it.
                        if let Some(session) = connectors.session.as_ref() {
                            match &set {
                                SetStatement::SessionAuthorization(auth) => {
                                    Self::mirror_session_authorization(
                                        session,
                                        state.policy_registry.as_ref(),
                                        auth,
                                    );
                                }
                                _ => Self::mirror_set_role(
                                    session,
                                    state.policy_registry.as_ref(),
                                    &set,
                                ),
                            }
                        }
                        Ok(QueryResult::Upstream(result, None, None))
                    }
                    SqlQuery::CompoundSelect(_)
                    | SqlQuery::Show(_)
                    | SqlQuery::Discard(_)
                    | SqlQuery::Comment(_) => {
                        event.sql_type = SqlQueryType::Other;
                        upstream
                            .query(raw_query)
                            .await
                            .map(|r| QueryResult::Upstream(r, None, None))
                    }

                    SqlQuery::StartTransaction(_) | SqlQuery::Commit(_) | SqlQuery::Rollback(_) => {
                        Self::handle_transaction_boundaries(
                            Some(upstream),
                            &mut state.proxy_state,
                            &mut state.write_tracker,
                            &query,
                            raw_query,
                        )
                        .await
                    }

                    SqlQuery::CreateCache(_)
                    | SqlQuery::Deallocate(_)
                    | SqlQuery::DropCache(_)
                    | SqlQuery::DropAllCaches(_)
                    | SqlQuery::FlushAllShallowCaches(_)
                    | SqlQuery::FlushCache(_)
                    | SqlQuery::DropAllProxiedQueries(_)
                    | SqlQuery::AlterReadySet(_)
                    | SqlQuery::Explain(_)
                    | SqlQuery::CreateRls(_)
                    | SqlQuery::DropRls(_)
                    | SqlQuery::CreateMcpToken(_)
                    | SqlQuery::DropMcpToken(_)
                    | SqlQuery::AlterMcpToken(_) => {
                        unreachable!("path returns prior")
                    }
                }
            } else {
                event.destination = Some(QueryDestination::Readyset(None));
                let start = Instant::now();

                let res = match &query {
                    SqlQuery::Select(_) => unreachable!("read path returns prior"),
                    // CREATE VIEW will still trigger migrations with explicit-migrations enabled
                    SqlQuery::CreateView(q) => connectors.noria.handle_create_view(q).await,
                    SqlQuery::CreateTable(q) => {
                        connectors.noria.handle_table_operation(q.clone()).await
                    }
                    SqlQuery::AlterTable(q) => {
                        connectors.noria.handle_table_operation(q.clone()).await
                    }
                    SqlQuery::DropTable(q) => {
                        connectors.noria.handle_table_operation(q.clone()).await
                    }
                    SqlQuery::DropView(q) => {
                        connectors.noria.handle_table_operation(q.clone()).await
                    }
                    SqlQuery::Insert(q) => connectors.noria.handle_insert(q).await,
                    SqlQuery::Update(q) => connectors.noria.handle_update(q).await,
                    SqlQuery::Delete(q) => connectors.noria.handle_delete(q).await,
                    SqlQuery::Truncate(q) => connectors.noria.handle_truncate(q).await,
                    SqlQuery::Deallocate(_) => unreachable!("deallocate path returns prior"),

                    // Return an empty result as we are allowing unsupported set statements. Commit
                    // messages are dropped - we do not support transactions in noria standalone.
                    // We return an empty result set instead of an error to support test
                    // applications.
                    SqlQuery::Set(_)
                    | SqlQuery::Commit(_)
                    | SqlQuery::Use(_)
                    | SqlQuery::Comment(_) => Ok(noria_connector::QueryResult::Empty),
                    q => {
                        error!(query = ?q, "unsupported query");
                        unsupported!("query type unsupported: {q:?}");
                    }
                };

                event.readyset_event = Some(ReadysetExecutionEvent::Other {
                    duration: start.elapsed(),
                });
                event.noria_error = res.as_ref().err().cloned();
                Ok(QueryResult::Noria(res?))
            }
        }
    }

    fn handle_deallocate_statement<'a>(stmt: DeallocateStatement) -> QueryResult<'a, DB> {
        let dealloc_id = match stmt.identifier {
            StatementIdentifier::SingleStatement(name) => DeallocateId::from(name.clone()),
            StatementIdentifier::AllStatements => DeallocateId::All,
        };
        QueryResult::Parser(ParsedCommand::Deallocate(dealloc_id))
    }

    async fn query_inner<'a>(
        connectors: &'a mut BackendConnectors<DB>,
        settings: &BackendSettings,
        state: &mut BackendState<DB>,
        query: &'a str,
        query_shallow: &mut Option<ShallowViewRequest>,
        event: &mut QueryExecutionEvent,
    ) -> Result<QueryResult<'a, DB>, DB::Error> {
        let (shallow_parsed, hint) = {
            let _t = event.start_parse_timer();
            parse_shallow_query(settings, query)
        };

        let is_skip_cache = matches!(&hint, Some(ReadysetHintDirective::SkipCache));

        // A successful shallow parse is always SELECT-shaped, so the Set/Use handling in
        // `check_readyset_schema_routing` cannot apply: route and serve the query on the
        // shallow parse alone, deferring the cost of the full parse to the fall-through path.
        let mut deep_ast = None;
        let shallow_parsed: Option<ReadySetResult<ShallowCacheQuery>> = match shallow_parsed {
            Ok(shallow_query) => {
                // Keep a copy of the sqlparser AST before the shallow rewrite mutates it, so
                // the fall-through below can derive the Readyset AST without a second text
                // parse.
                if settings.retain_shallow_ast() {
                    deep_ast = Some((*shallow_query).clone());
                }
                let shallow_parsed = Ok(shallow_query);
                if state.should_query_readyset_schema(settings, &shallow_parsed) {
                    let session = Self::readyset_schema_session(connectors, state)?;
                    let result = session.query(query).await?;
                    return Ok(QueryResult::ReadysetSchema(result));
                }

                if let Some((shallow, params)) = connectors.prepare_shallow_query(shallow_parsed) {
                    if let Some((query_id, _)) = Self::should_query_shallow(
                        connectors, settings, state, &shallow, query, hint,
                    )
                    .await
                    {
                        let result =
                            Self::query_shallow(connectors, state, query, query_id, event, params)
                                .await;

                        event.sql_type = SqlQueryType::Read;
                        event.query_id = Some(query_id);
                        if let Err(e) = &result {
                            event.set_noria_error(&internal_err!("{e}"));
                        }
                        *query_shallow = Some(shallow);
                        return result;
                    }
                    *query_shallow = Some(shallow);
                }
                None
            }
            Err(e) => Some(Err(e)),
        };

        let parsed = {
            let _t = event.start_parse_timer();
            convert_or_parse_query(settings, deep_ast, query)
        };

        // Mirror full-session resets into the SessionContext before the query
        // runs. `DISCARD ALL` / `RESET ALL` fully reset run-time state. A `SET
        // [LOCAL] SESSION AUTHORIZATION` identity change is mirrored separately,
        // *after* upstream accepts it (see `query_adhoc_non_select`), so a
        // rejected statement cannot leave the mirror pointing at an identity
        // upstream never adopted.
        if let Some(session) = connectors.session.as_ref() {
            match &parsed {
                Ok(SqlQuery::Discard(d)) if d.object_type == DiscardObject::All => {
                    session.discard_all()
                }
                _ => {}
            }
        }

        // Mirror a simple-protocol `set_config(...)` batch into the session.
        // The extended protocol recognizes it at prepare and applies it at
        // execute with the bound parameters; the simple path carries no bound
        // params, so the literal-valued form is recognized and applied here
        // (an empty parameter vector resolves the literal sources, and any
        // stray `$N` source resolves to nothing and fails the batch closed).
        if let Some(session) = connectors.session.as_ref()
            && let Some(registry) = state.policy_registry.as_ref()
            && let Ok(q) = &parsed
            && let Some(template) = session_mutation::recognize(q)
        {
            session_mutation::apply(&template, &[], session, registry);
        }

        if let Some(result) = Self::check_readyset_schema_routing(state, &parsed) {
            return Ok(result);
        }

        // Statements that don't parse as a shallow SELECT still route to the readyset schema
        // when the session's search path points at it (`readyset_schema_route_all`). This must
        // stay after `check_readyset_schema_routing` so a SET/USE can first switch routing off.
        if let Some(shallow_parsed) = shallow_parsed
            && state.should_query_readyset_schema(settings, &shallow_parsed)
        {
            let session = Self::readyset_schema_session(connectors, state)?;
            let result = session.query(query).await?;
            return Ok(QueryResult::ReadysetSchema(result));
        }

        // Maintain the session-level `last_write_at` that gates
        // `TrxCachePolicy::UntilWrite` caches. Mark a write whenever the parsed query
        // is a write, and conservatively mark on parse failure inside any transaction
        // (some writes -- e.g. `SELECT ... FOR UPDATE`, stored-proc `CALL`,
        // CTE-embedded `INSERT` -- never reach an `SqlQuery` variant).
        match &parsed {
            Ok(q) if q.is_write() => state.write_tracker.mark_write(),
            Err(_) if state.proxy_state.in_transaction_or_implicit() => {
                state.write_tracker.mark_write()
            }
            _ => {}
        }

        match parsed {
            // Parse error, but no fallback exists
            Err(e) if !connectors.has_fallback() => {
                error!("{}", e);
                event.set_noria_error(&e);
                Err(e.into())
            }
            // Parse error, send to fallback
            Err(e) => {
                if !matches!(
                    e,
                    ReadySetError::ReaderMissingKey
                        | ReadySetError::NoCacheForQuery
                        | ReadySetError::UnparseableQuery { .. }
                ) {
                    warn!(error = %e, "Error received from noria, sending query to fallback");
                    event.set_noria_error(&e);
                }
                let fallback_res =
                    Self::query_fallback(connectors.upstream.as_mut(), query, event, None).await;
                if fallback_res.is_ok() {
                    let (id, _) = state
                        .query_status_cache
                        .insert(Query::ParseFailed(query.to_string().into(), e.to_string()));
                    if let Some(ref telemetry_sender) = state.telemetry_sender {
                        if let Err(e) = telemetry_sender.send_event_with_payload(
                            TelemetryEvent::QueryParseFailed,
                            TelemetryBuilder::new()
                                .server_version(
                                    option_env!("CARGO_PKG_VERSION").unwrap_or_default(),
                                )
                                .query_id(id.to_string())
                                .build(),
                        ) {
                            warn!(error = %e, "Failed to send parse failed metric");
                        }
                    } else {
                        trace!("No telemetry sender. not sending metric for {query}");
                    }
                }
                fallback_res
            }
            // Check for COMMIT+ROLLBACK before we check whether we should proxy, since we need to
            // know when a COMMIT or ROLLBACK happens so we can leave `ProxyState::InTransaction`
            Ok(parsed_query @ (SqlQuery::Commit(_) | SqlQuery::Rollback(_))) => {
                Self::query_adhoc_non_select(
                    connectors,
                    settings,
                    state,
                    query,
                    event,
                    parsed_query,
                )
                .await
            }
            Ok(ref parsed_query) if parsed_query.is_readyset_extension() => {
                Self::query_readyset_extensions(connectors, settings, state, parsed_query, event)
                    .await
                    .map(Into::into)
                    .map_err(Into::into)
            }
            // SET autocommit=1 needs to be handled explicitly or it will end up getting proxied in
            // most cases.
            Ok(SqlQuery::Set(s))
                if Handler::handle_set_statement(&s).set_autocommit == Some(true) =>
            {
                Self::query_adhoc_non_select(
                    connectors,
                    settings,
                    state,
                    query,
                    event,
                    SqlQuery::Set(s),
                )
                .await
            }
            // SET [LOCAL] SESSION AUTHORIZATION must reach `query_adhoc_non_select`
            // even inside a transaction (where the proxy path would otherwise
            // forward it without mirroring), so the identity is mirrored into the
            // session after upstream accepts it.
            Ok(SqlQuery::Set(s @ SetStatement::SessionAuthorization(_))) => {
                Self::query_adhoc_non_select(
                    connectors,
                    settings,
                    state,
                    query,
                    event,
                    SqlQuery::Set(s),
                )
                .await
            }
            Ok(ref parsed_query) if Handler::requires_fallback(parsed_query) => {
                if !Handler::return_default_response(parsed_query) && connectors.has_fallback() {
                    if let SqlQuery::Select(stmt) = parsed_query {
                        event.sql_type = SqlQueryType::Read;
                        event.query = Some(Arc::new(parsed_query.clone()));
                        event.query_id = Some(QueryId::from_select(
                            stmt,
                            connectors.noria.schema_search_path(),
                        ));
                    }

                    // Query requires a fallback and we can send it to fallback
                    Self::query_fallback(connectors.upstream.as_mut(), query, event, None).await
                } else {
                    // Query should return a default response or requires a fallback, but none is
                    // available
                    Handler::default_response(parsed_query)
                        .map(QueryResult::Noria)
                        .map_err(Into::into)
                }
            }
            Ok(SqlQuery::Select(mut stmt)) => {
                event.sql_type = SqlQueryType::Read;
                if settings.cache_mode.is_shallow() {
                    event.query_id = query_shallow.as_ref().map(QueryId::from);
                    return Self::query_fallback(connectors.upstream.as_mut(), query, event, None)
                        .await;
                }

                let rewrite_context =
                    Self::rewrite_context(connectors, settings, state, None).await?;
                let params = match adapter_rewrites::rewrite_equivalent_deep(
                    &mut stmt,
                    connectors.noria.rewrite_params(),
                    &rewrite_context,
                ) {
                    Ok(params) => params,
                    Err(_) if connectors.has_fallback() => {
                        let result =
                            Self::query_fallback(connectors.upstream.as_mut(), query, event, None)
                                .await;
                        return result;
                    }
                    Err(e) => return Err(e.into()),
                };

                let view_request =
                    ViewCreateRequest::new(stmt, connectors.noria.schema_search_path().to_owned());

                if let Some(QueryLogMode::Verbose) = state.query_log_mode {
                    event.query = Some(Arc::new(SqlQuery::Select(view_request.statement.clone())));
                }
                event.query_id = event.query_id.or(Some(QueryId::from(&view_request)));

                Self::try_noria_adhoc_select(
                    connectors,
                    settings,
                    state,
                    query,
                    view_request,
                    params,
                    rewrite_context.schema_generation(),
                    event,
                    is_skip_cache,
                )
                .await
            }
            Ok(SqlQuery::Deallocate(stmt)) => Ok(Self::handle_deallocate_statement(stmt)),
            Ok(_) if state.proxy_state.should_proxy() => {
                Self::query_fallback(connectors.upstream.as_mut(), query, event, None).await
            }
            Ok(parsed_query) => {
                let result = Self::query_adhoc_non_select(
                    connectors,
                    settings,
                    state,
                    query,
                    event,
                    parsed_query.clone(),
                )
                .await;

                if let SqlQuery::DropTable(drop_stmt) = &parsed_query
                    && result.is_ok()
                {
                    state
                        .query_status_cache
                        .invalidate_queries_referencing_tables(&drop_stmt.tables);
                }
                result
            }
        }
    }

    fn update_shallow_support(
        state: &BackendState<DB>,
        shallow: &Option<ShallowViewRequest>,
        error: Option<&DB::Error>,
    ) {
        if let Some(shallow) = shallow {
            state
                .query_status_cache
                .with_mut_migration_state(shallow, |state| {
                    if state.is_proxied() {
                        *state = match error {
                            None => MigrationState::Supported,
                            Some(e) => MigrationState::Unsupported(e.to_string()),
                        }
                    }
                });
        }
    }

    /// Executes `query` using the reader/writer belonging to the calling `Backend` struct.
    pub async fn query<'a>(
        &'a mut self,
        query: &'a str,
    ) -> Result<(QueryResult<'a, DB>, ProxyState), DB::Error> {
        Self::check_routing(&self.connectors, &mut self.state).await?;

        let mut query_shallow = None;
        let mut event = QueryExecutionEvent::new(EventType::Query);
        let result = Self::query_inner(
            &mut self.connectors,
            &self.settings,
            &mut self.state,
            query,
            &mut query_shallow,
            &mut event,
        )
        .await;

        Self::update_shallow_support(&self.state, &query_shallow, result.as_ref().err());

        self.state.last_query = QueryInfo::from_event(&event);

        log_query(
            self.state.query_log_sender.as_ref(),
            event,
            self.settings.slowlog,
            self.settings.dialect,
        );

        result.map(|r| (r, self.state.proxy_state))
    }

    /// Mark or unmark this backend connection as an internal ReadySet connection
    pub fn set_internal_connection(&mut self, is_internal: bool) {
        self.state.is_internal_connection = is_internal;
    }

    pub fn does_require_authentication(&self) -> bool {
        self.settings.require_authentication
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
