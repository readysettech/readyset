use std::error::Error;
use std::fmt::Debug;

use async_trait::async_trait;
use noria::ColumnSchema;
use noria_data::DataType;
use noria_errors::ReadySetError;

/// Information about a statement that has been prepared in an [`UpstreamDatabase`]
#[derive(Debug)]
pub struct UpstreamPrepare<DB: UpstreamDatabase> {
    pub statement_id: u32,
    pub meta: DB::StatementMeta,
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

/// A connector to some kind of upstream database which can be used for passthrough write queries
/// and fallback read queries.
///
/// An implementation of this trait can optionally be used to back a [`Reader`][] for fallback in
/// addition to Noria, or a [`Writer`][] for passthrough writes instead of Noria.
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
    type QueryResult: Debug + Send + 'static;

    /// A type representing metadata about a prepared statement.
    ///
    /// This type is used as a field of [`UpstreamPrepare`], returned from
    /// [`prepare`](UpstreamDatabase::prepaare)
    type StatementMeta: NoriaCompare + Debug + Send + 'static;

    /// Errors that can be returned from operations on this database
    ///
    /// This type, which must have at least one enum variant that includes a
    /// [`noria::ReadySetError`], is used as the error type for all return values in the
    /// noria_client backend.
    type Error: From<ReadySetError> + IsFatalError + Error + Send + Sync + 'static;

    /// Create a new connection to this upstream database
    async fn connect(url: String) -> Result<Self, Self::Error>;

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
    /// ['Err(Error::ReadySet(ReadSetError::PreparedStatementMissing))'](noria::ReadSetError::PreparedStatementMissing)
    async fn execute(
        &mut self,
        statement_id: u32,
        params: &[DataType],
    ) -> Result<Self::QueryResult, Self::Error>;

    /// Execute a raw, un-prepared query
    async fn query<'a, S>(&'a mut self, query: S) -> Result<Self::QueryResult, Self::Error>
    where
        S: AsRef<str> + Send + Sync + 'a;

    /// Execute a raw, un-prepared write query, constructing and returning a RYW ticket for the
    /// write
    // TODO: newtype RYW ticket, not just String
    async fn handle_ryw_write<'a, S>(
        &'a mut self,
        query: S,
    ) -> Result<(Self::QueryResult, String), Self::Error>
    where
        S: AsRef<str> + Send + Sync + 'a;

    /// Handle starting a transaction with the upstream database.
    async fn start_tx(&mut self) -> Result<Self::QueryResult, Self::Error>;

    /// Return whether we are currently in a transaction or not.
    fn is_in_tx(&self) -> bool;

    /// Handle committing a transaction to the upstream database.
    async fn commit(&mut self) -> Result<Self::QueryResult, Self::Error>;

    /// Handle rolling back the ongoing transaction for this connection to the upstream db.
    async fn rollback(&mut self) -> Result<Self::QueryResult, Self::Error>;

    /// Return schema dump from the upstream database, for inclusion in a query analysis bundle.
    async fn schema_dump(&mut self) -> Result<Vec<u8>, anyhow::Error>;
}
