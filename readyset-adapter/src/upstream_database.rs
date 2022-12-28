use std::error::Error;
use std::fmt::Debug;

use async_trait::async_trait;
pub use database_utils::UpstreamConfig;
use nom_sql::SqlIdentifier;
use readyset_client::ColumnSchema;
use readyset_client_metrics::QueryDestination;
use readyset_data::DfValue;
use readyset_errors::ReadySetError;

use crate::fallback_cache::FallbackCache;

/// Information about a statement that has been prepared in an [`UpstreamDatabase`]
#[derive(Debug)]
pub struct UpstreamPrepare<DB: UpstreamDatabase> {
    pub statement_id: u32,
    pub meta: DB::StatementMeta,
}

impl<DB: UpstreamDatabase> Clone for UpstreamPrepare<DB> {
    fn clone(&self) -> Self {
        UpstreamPrepare {
            statement_id: self.statement_id,
            meta: self.meta.clone(),
        }
    }
}

/// An implementation of this trait allows the statement metadata from a
/// prepare result to be compared against the schema of the equivalent
/// noria prepare result. The compare function returns an Ok result if
/// the query schemas match, and otherwise returns an error with the
/// reason the schema match failed.
pub trait NoriaCompare {
    type Error: From<ReadySetError> + Error + Send + Sync + 'static;

    fn compare(&self, columns: &[ColumnSchema], params: &[ColumnSchema])
        -> Result<(), Self::Error>;
}

pub trait IsFatalError {
    fn is_fatal(&self) -> bool;
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
    type QueryResult<'a>: Debug + UpstreamDestination
    where
        Self: 'a;

    /// A read result that has been cached in the underlying FallbackCache.
    type CachedReadResult: Debug + Send + Sync + Clone;

    /// A type representing metadata about a prepared statement.
    ///
    /// This type is used as a field of [`UpstreamPrepare`], returned from
    /// [`prepare`](UpstreamDatabase::prepaare)
    type StatementMeta: NoriaCompare + Debug + Send + Clone + 'static;

    /// Errors that can be returned from operations on this database
    ///
    /// This type, which must have at least one enum variant that includes a
    /// [`readyset_client::ReadySetError`], is used as the error type for all return values in the
    /// noria_client backend.
    type Error: From<ReadySetError> + IsFatalError + Error + Send + Sync + 'static;

    /// When there's no upstream DB to fetch the version from, default to this value. This features
    /// is only used for tests
    const DEFAULT_DB_VERSION: &'static str;

    /// Create a new connection to this upstream database
    ///
    /// Connect will return an error if the upstream database is running an unsupported version.
    async fn connect(
        upstream_config: UpstreamConfig,
        fallback_cache: Option<FallbackCache<Self::CachedReadResult>>,
    ) -> Result<Self, Self::Error>;

    /// Resets the connection with the upstream database
    async fn reset(&mut self) -> Result<(), Self::Error>;

    /// Return a reference to the URL used when originally constructing this database via
    /// [`connect`]
    fn url(&self) -> &str;

    /// Returns a database name if it was included in the original connection string, or None if no
    /// database name was included in the original connection string.
    fn database(&self) -> Option<&str> {
        None
    }

    /// Returns the servers's version string, including modifications to indicate that the
    /// connection is running via ReadySet
    fn version(&self) -> String;

    /// Send a request to the upstream database to prepare the given query, returning a unique ID
    /// for that prepared statement
    ///
    /// Implementations of this trait can use any method they like to store prepared statements
    /// associated with statement IDs, as long as after calling `on_prepare` on one instance of an
    /// UpstreamDatabase a later call of [`on_execute`] on the same UpstreamDatabase with the same
    /// statement ID executes that statement.
    async fn prepare<'a, S>(&'a mut self, query: S) -> Result<UpstreamPrepare<Self>, Self::Error>
    where
        S: AsRef<str> + Send + Sync + 'a;

    /// Execute a statement that was prepared earlier with ['on_prepare'], with the given params
    ///
    /// If 'on_execute' is called with a 'statement_id' that was not previously passed to
    /// 'on_prepare', this method should return
    /// ['Err(Error::ReadySet(ReadySetError::PreparedStatementMissing))'
    /// ](readyset_client::ReadySetError:: PreparedStatementMissing)
    async fn execute<'a>(
        &'a mut self,
        statement_id: u32,
        params: &[DfValue],
    ) -> Result<Self::QueryResult<'a>, Self::Error>;

    /// Execute a raw, un-prepared query
    async fn query<'a, S>(&'a mut self, query: S) -> Result<Self::QueryResult<'a>, Self::Error>
    where
        S: AsRef<str> + Send + Sync + 'a;

    /// Execute a raw, un-prepared write query, constructing and returning a RYW ticket for the
    /// write
    // TODO: newtype RYW ticket, not just String
    async fn handle_ryw_write<'a, S>(
        &'a mut self,
        query: S,
    ) -> Result<(Self::QueryResult<'a>, String), Self::Error>
    where
        S: AsRef<str> + Send + Sync + 'a;

    /// Handle starting a transaction with the upstream database.
    async fn start_tx<'a>(&'a mut self) -> Result<Self::QueryResult<'a>, Self::Error>;

    /// Handle committing a transaction to the upstream database.
    async fn commit<'a>(&'a mut self) -> Result<Self::QueryResult<'a>, Self::Error>;

    /// Handle rolling back the ongoing transaction for this connection to the upstream db.
    async fn rollback<'a>(&'a mut self) -> Result<Self::QueryResult<'a>, Self::Error>;

    /// Return schema dump from the upstream database, for inclusion in a query analysis bundle.
    async fn schema_dump(&mut self) -> Result<Vec<u8>, anyhow::Error>;

    /// Query the upstream database for the currently configured schema search path.
    ///
    /// Note that the terminology used here is maximally general - while only PostgreSQL truly
    /// supports a multi-element schema search path, the concept of "currently connected database"
    /// in MySQL can be thought of as a schema search path that only has one element
    async fn schema_search_path(&mut self) -> Result<Vec<SqlIdentifier>, Self::Error>;
}
