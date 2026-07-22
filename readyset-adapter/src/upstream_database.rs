use std::borrow::Borrow;
use std::error::Error;
use std::fmt::Debug;

use async_trait::async_trait;
pub use database_utils::UpstreamConfig;
use readyset_adapter_types::{DeallocateId, PreparedStatementType};
use readyset_client_metrics::QueryDestination;
use readyset_data::DfValue;
use readyset_data::encoding::Encoding;
use readyset_errors::ReadySetError;
use readyset_shallow::{CacheInsertGuard, ContentHash};
use readyset_sql::ast::{Relation, SqlIdentifier};
use readyset_util::SizeOf;

pub type UpstreamStatementId = u32;

/// Trait for refreshing a shallow cache from an upstream query result
#[async_trait]
pub trait Refresh {
    /// The type of value in the shallow cache.
    type Entry: Send + Sync + 'static;

    /// Populate the cache with data from this query result. `encoding` is the results charset
    /// of the entry being refreshed, which the connection that produced this result had mirrored
    /// upstream. Upstreams without a results charset concept ignore it.
    async fn refresh(
        self,
        cache: CacheInsertGuard<crate::shallow_key::ShallowKey, Self::Entry>,
        encoding: Encoding,
    ) -> std::io::Result<()>;
}

/// Information about a statement that has been prepared in an [`UpstreamDatabase`]
pub struct UpstreamPrepare<DB: UpstreamDatabase> {
    pub statement_id: UpstreamStatementId,
    pub meta: DB::StatementMeta,
}

impl<DB: UpstreamDatabase> Debug for UpstreamPrepare<DB> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UpstreamPrepare")
            .field("statement_id", &self.statement_id)
            .field("meta", &self.meta)
            .finish()
    }
}

impl<DB: UpstreamDatabase> Clone for UpstreamPrepare<DB> {
    fn clone(&self) -> Self {
        UpstreamPrepare {
            statement_id: self.statement_id,
            meta: self.meta.clone(),
        }
    }
}

pub trait IsFatalError {
    fn is_fatal(&self) -> bool;
}

/// The engine's answer to an authorization probe ([`UpstreamDatabase::acl_probe`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AclProbeOutcome {
    Authorized,
    Denied,
}

/// Order-insensitive hash of engine-reported privilege rows, used as a grant
/// fingerprint: a changed value means the identity's effective grants changed
/// and its cache-ACL verdicts must be re-probed. An invalidation signal only,
/// never an authorization decision.
pub fn fingerprint_rows(mut rows: Vec<String>) -> u64 {
    use std::hash::{Hash, Hasher};
    rows.sort_unstable();
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    rows.hash(&mut hasher);
    hasher.finish()
}

pub trait UpstreamDestination {
    fn destination(&self) -> QueryDestination {
        QueryDestination::Upstream
    }
}

/// A connector to some kind of upstream database which can be used for passthrough write queries
/// and fallback read queries.
///
/// An implementation of this trait can optionally be used to back a [`Reader`][] for fallback in
/// addition to ReadySet, or a [`Writer`][] for passthrough writes instead of ReadySet.
///
/// [`Reader`]: crate::backend::Reader
/// [`Writer`]: crate::backend::Writer
#[async_trait]
pub trait UpstreamDatabase: Sized + Send {
    /// The result returned by queries. Likely to be implemented as an enum containing a read or a
    /// write result.
    ///
    /// This type is used as the value inside of [`QueryResult::Upstream`][]
    ///
    /// [`QueryResult::Upstream`]: crate::backend::QueryResult::Upstream
    type QueryResult<'a>: Debug + Send + UpstreamDestination + Refresh<Entry = Self::CacheEntry>
    where
        Self: 'a;

    /// A type representing metadata about a prepared statement.
    ///
    /// This type is used as a field of [`UpstreamPrepare`], returned from
    /// [`prepare`](UpstreamDatabase::prepaare)
    type StatementMeta: Debug + Send + Clone + 'static;

    /// Extra data passed to [`prepare`] by the protocol shim
    ///
    /// [`prepare`](UpstreamDatabase::prepare)
    type PrepareData<'a>: Default + Send;

    /// Metadata passed to [`execute`] by the protocol shim
    ///
    /// [`execute`](UpstreamDatabase::execute)
    type ExecMeta: Send + Sync + ?Sized;

    /// Metadata to be used when executing during a shallow cache insertion.
    type ShallowExecMeta: Borrow<Self::ExecMeta> + Debug + Clone + Send + Sync + 'static;

    /// The type of data this protocol stores into an entry in a shallow cache.
    type CacheEntry: Debug + Send + Sync + SizeOf + ContentHash + 'static;

    /// Errors that can be returned from operations on this database
    ///
    /// This type, which must have at least one enum variant that includes a
    /// [`readyset_client::ReadySetError`], is used as the error type for all return values in the
    /// noria_client backend.
    type Error: From<ReadySetError> + IsFatalError + Error + Send + Sync + 'static;

    /// When there's no upstream DB to fetch the version from, default to this value. This features
    /// is only used for tests
    const DEFAULT_DB_VERSION: &'static str;

    /// Returns the SQL dialect to use for formatting queries
    const SQL_DIALECT: readyset_sql::Dialect;

    /// Create a new connection to this upstream database
    ///
    /// Connect will return an error if the upstream database is running an unsupported version.
    async fn connect(
        upstream_config: UpstreamConfig,
        username: Option<String>,
        password: Option<String>,
        interactive: bool,
    ) -> Result<Self, Self::Error>;

    /// Test the connection with the upstream database
    async fn is_connected(&mut self) -> Result<bool, Self::Error>;

    /// Reconnect using new user
    async fn change_user(
        &mut self,
        user: &str,
        password: &str,
        database: &str,
    ) -> Result<(), Self::Error>;

    /// Ping the upstream connection to see if it is still alive
    async fn ping(&mut self) -> Result<(), Self::Error>;

    /// Reset the connection to the upstream database
    async fn reset(&mut self) -> Result<(), Self::Error>;

    /// Returns a database name if it was included in the original connection string, or None if no
    /// database name was included in the original connection string.
    fn database(&self) -> Option<&str> {
        None
    }

    /// Returns the servers's version string, including modifications to indicate that the
    /// connection is running via ReadySet
    fn version(&self) -> String;

    /// Prepares the query for the sole purpose of checking if the prepare succeeds.
    async fn can_prepare<S>(&mut self, query: S) -> anyhow::Result<()>
    where
        S: AsRef<str> + Send + Sync;

    /// Whether `error` is the engine refusing for lack of privilege, as opposed to any
    /// other failure. Probing has to reach the statement through session setup that can
    /// itself be refused -- entering the cache's schema is a privileged act on MySQL --
    /// so the worker classifies those errors rather than treating them all as transient.
    fn is_privilege_error(error: &Self::Error) -> bool;

    /// Submit `query` in a form the engine authorizes but does not execute, as the
    /// session's current identity, or as `as_role` on upstreams that can assume a role
    /// in-session. `n_params` is the statement's placeholder count. Returns `Denied`
    /// only for the engine's permission error; any other failure is transient and
    /// surfaces as `Err`.
    async fn acl_probe(
        &mut self,
        query: &str,
        n_params: usize,
        as_role: Option<&str>,
    ) -> Result<AclProbeOutcome, Self::Error>;

    /// Engine-evaluated hash of the session identity's (or `as_role`'s) effective
    /// privileges over `relations`. See [`fingerprint_rows`] for the contract: an
    /// invalidation signal deciding when to re-probe, never what the answer is.
    async fn grant_fingerprint(
        &mut self,
        relations: &[Relation],
        as_role: Option<&str>,
    ) -> Result<u64, Self::Error>;

    /// The roles the session's current identity is a member of and could assume
    /// with `SET ROLE`, engine-evaluated. Empty on upstreams whose sessions the
    /// cache ACL never keys by an assumed role.
    async fn assumable_roles(&mut self) -> Result<Vec<String>, Self::Error>;

    /// Send a request to the upstream database to prepare the given query, returning a unique ID
    /// for that prepared statement
    ///
    /// Implementations of this trait can use any method they like to store prepared statements
    /// associated with statement IDs, as long as after calling `on_prepare` on one instance of an
    /// UpstreamDatabase a later call of [`on_execute`] on the same UpstreamDatabase with the same
    /// statement ID executes that statement.
    async fn prepare<'a, 'b, S>(
        &'a mut self,
        query: S,
        data: Self::PrepareData<'b>,
        statement_type: PreparedStatementType,
    ) -> Result<UpstreamPrepare<Self>, Self::Error>
    where
        S: AsRef<str> + Send + Sync + 'a;

    /// Execute a statement that was prepared earlier with ['on_prepare'], with the given params
    ///
    /// The `exec_meta` argument is database-specific, and is generally passed through by the
    /// caller of [`Backend::execute`] if that call ends up being passed to the upstream.
    ///
    /// If 'on_execute' is called with a 'statement_id' that was not previously passed to
    /// 'on_prepare', this method should return
    /// ['Err(Error::ReadySet(ReadySetError::PreparedStatementMissing))'
    /// ](readyset_client::ReadySetError:: PreparedStatementMissing)
    /// [`Backend::execute`](readyset_client::Backend::execute)
    async fn execute<'a>(
        &'a mut self,
        statement_id: &UpstreamStatementId,
        params: &[DfValue],
        exec_meta: &Self::ExecMeta,
    ) -> Result<Self::QueryResult<'a>, Self::Error>;

    /// Remove a prepared statement from the cache, and tell the upstream database to remove it and
    /// free any resources associated with it.
    ///
    /// Returns an error if the statement doesn't exist
    async fn remove_statement(&mut self, statement_id: DeallocateId) -> Result<(), Self::Error>;

    /// Execute a raw, un-prepared query
    async fn query<'a>(&'a mut self, query: &'a str) -> Result<Self::QueryResult<'a>, Self::Error>;

    /// Execute a raw, un-prepared query with execution metadata.
    async fn query_ext<'a>(
        &'a mut self,
        query: &'a str,
        exec_meta: &Self::ExecMeta,
    ) -> Result<Self::QueryResult<'a>, Self::Error>;

    /// Execute a raw, un-prepared query (or multiple queries concatenated in the provided `query`
    /// string, separated by semicolons) using the 'simple query' protocol flow[0],
    ///
    ///
    /// Note that the implementation of simple_query buffers results in memory before returning, so
    /// it should not be used for cases where there are large result sets. Use
    /// [`query`](Self::query) for most cases.
    ///
    /// Note that this is only relevant for PostgreSQL upstreams.
    ///
    /// [0] https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-SIMPLE-QUERY
    async fn simple_query<'a>(
        &'a mut self,
        query: &'a str,
    ) -> Result<Self::QueryResult<'a>, Self::Error>;

    /// Handle starting a transaction with the upstream database.
    ///
    /// Takes the client's original query text rather than a reconstructed statement so that
    /// modifiers the AST does not model (isolation level, read-only, deferrable) reach upstream.
    async fn start_tx<'a>(
        &'a mut self,
        query: &'a str,
    ) -> Result<Self::QueryResult<'a>, Self::Error>;

    /// Handle committing a transaction to the upstream database.
    async fn commit<'a>(&'a mut self) -> Result<Self::QueryResult<'a>, Self::Error>;

    /// Handle rolling back the ongoing transaction for this connection to the upstream db.
    async fn rollback<'a>(&'a mut self) -> Result<Self::QueryResult<'a>, Self::Error>;

    /// Query the upstream database for the currently configured schema search path.
    ///
    /// Note that the terminology used here is maximally general - while only PostgreSQL truly
    /// supports a multi-element schema search path, the concept of "currently connected database"
    /// in MySQL can be thought of as a schema search path that only has one element
    async fn schema_search_path(&mut self) -> Result<Vec<SqlIdentifier>, Self::Error>;

    /// Set the schema search path for future queries on the upstream database.
    async fn set_schema_search_path(&mut self, path: &[SqlIdentifier]) -> Result<(), Self::Error>;

    /// Set the session's `character_set_results` on the upstream connection so proxied result
    /// rows come back in the client's charset. The default implementation is a no-op for
    /// upstreams without that concept (PostgreSQL).
    async fn set_results_character_set(&mut self, _charset: &str) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Set the session's connection charset and collation on the upstream connection so
    /// proxied literal semantics, result metadata, and result rows follow the client's
    /// charset. The default implementation is a no-op for upstreams without that concept
    /// (PostgreSQL).
    async fn set_connection_charset(
        &mut self,
        _charset: &str,
        _collation: &str,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn timezone_name(&mut self) -> Result<SqlIdentifier, Self::Error>;

    async fn lower_case_database_names(&mut self) -> Result<bool, Self::Error>;
    async fn lower_case_table_names(&mut self) -> Result<bool, Self::Error>;

    /// Query the upstream database for its configured `group_concat_max_len` value.
    /// Defaults to MySQL's default of 1024.
    async fn group_concat_max_len(&mut self) -> Result<usize, Self::Error> {
        Ok(readyset_data::upstream_system_props::DEFAULT_GROUP_CONCAT_MAX_LEN)
    }

    /// Convert the supplied metadata into temporary metadata during a shallow cache insertion.
    async fn shallow_exec_meta(
        &mut self,
        meta: &Self::ExecMeta,
    ) -> Result<Self::ShallowExecMeta, Self::Error>;

    /// Is this cache entry compatible with the current query metadata.
    fn is_meta_compatible(cache: &Self::CacheEntry) -> bool;
}
