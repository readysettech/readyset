use std::fmt::Debug;

use async_trait::async_trait;
use noria::DataType;

use crate::Error;

#[derive(Debug)]
pub struct UpstreamPrepare {
    pub statement_id: u32,
    // TODO(grfn): Make Column an associated type on UpstreamDatabase
    pub params: Vec<msql_srv::Column>,
    pub schema: Vec<msql_srv::Column>,
    pub is_read: bool,
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
    /// The type of results returned by read queries
    ///
    /// This type is used as the value inside of [`QueryResult::UpstreamRead`][]
    ///
    /// [`QueryResult::UpstreamRead`]: crate::backend::QueryResult::UpstreamRead
    type ReadResult: Debug + Send + 'static;

    /// The type of results returned by write queries
    ///
    /// This type is used as the value inside of [`QueryResult::UpstreamWrite`][]
    ///
    /// [`QueryResult::UpstreamWrite`]: crate::backend::QueryResult::UpstreamWrite
    type WriteResult: Debug + Send + 'static;

    /// Create a new connection to this upstream database
    async fn connect(url: String) -> Result<Self, Error>;

    /// Return a reference to the URL used when originally constructing this database via
    /// [`connect`]
    fn url(&self) -> &str;

    /// Send a request to the upstream database to prepare the given query, returning a unique ID
    /// for that prepared statement
    ///
    /// Implementations of this trait can use any method they like to store prepared statements
    /// associated with statement IDs, as long as after calling `on_prepare` on one instance of an
    /// UpstreamDatabase a later call of [`on_execute`] on the same UpstreamDatabase with the same
    /// statement ID executes that statement.
    async fn prepare<'a, S>(&'a mut self, query: S) -> Result<UpstreamPrepare, Error>
    where
        S: AsRef<str> + Send + Sync + 'a;

    /// Execute a read statement that was prepared earlier with [`on_prepare`], with the given
    /// `params`
    ///
    /// If `on_execute` is called with a `statement_id` that was not previously passed to
    /// `on_prepare`, this method should return
    /// [`Err(Error::ReadySet(ReadySetError::PreparedStatementMissing))`](noria::ReadySetError::PreparedStatementMissing)
    async fn execute_read(
        &mut self,
        statement_id: u32,
        params: Vec<DataType>,
    ) -> Result<Self::ReadResult, Error>;

    /// Execute a write statement that was prepared earlier with [`on_prepare`], with the given
    /// `params`
    ///
    /// If `on_execute` is called with a `statement_id` that was not previously passed to
    /// `on_prepare`, this method should return
    /// [`Err(Error::ReadySet(ReadySetError::PreparedStatementMissing))`](noria::ReadySetError::PreparedStatementMissing)
    async fn execute_write(
        &mut self,
        statement_id: u32,
        params: Vec<DataType>,
    ) -> Result<Self::WriteResult, Error>;

    /// Execute a raw, un-prepared read query
    async fn handle_read<'a, S>(&'a mut self, query: S) -> Result<Self::ReadResult, Error>
    where
        S: AsRef<str> + Send + Sync + 'a;

    /// Execute a raw, un-prepared write query
    async fn handle_write<'a, S>(&'a mut self, query: S) -> Result<Self::WriteResult, Error>
    where
        S: AsRef<str> + Send + Sync + 'a;

    /// Execute a raw, un-prepared write query, constructing and returning a RYW ticket for the
    /// write
    // TODO: newtype RYW ticket, not just String
    async fn handle_ryw_write<'a, S>(
        &'a mut self,
        query: S,
    ) -> Result<(Self::WriteResult, String), Error>
    where
        S: AsRef<str> + Send + Sync + 'a;
}
