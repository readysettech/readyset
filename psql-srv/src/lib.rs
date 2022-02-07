#![feature(associated_type_bounds)]
//! Bindings for emulating a PostgreSQL server.
//!
//! When developing new databases or caching layers, it can be immensely useful to test your system
//! using existing applications. However, this often requires significant work modifying
//! applications to use your database over the existing ones. This crate solves that problem by
//! acting as a PostgreSQL server, and delegating operations such as querying and query execution
//! to user-defined logic.
//!
//! To start, implement `Backend` for your backend, and call `run_backend` on an instance of your
//! backend along with a connection stream. The appropriate methods will be called on
//! your backend whenever a client sends a `QUERY`, `PARSE`, `DESCRIBE`, `BIND` or `EXECUTE`
//! message, and you will have a chance to respond appropriately. See `lib.rs` for information on
//! implementing a `Backend` and `ServeOneBackend`, in the `serve_one.rs` example, for a very basic
//! implementation.

mod bytes;
mod channel;
mod codec;
mod error;
mod message;
mod protocol;
mod response;
mod runner;
mod value;

use std::convert::TryInto;

use async_trait::async_trait;
use postgres_types::Type;
use tokio::io::{AsyncRead, AsyncWrite};

pub use crate::error::Error;
pub use crate::value::Value;

/// A trait for implementing a SQL backend that produces responses to SQL query statements. This
/// trait is the primary interface for the `psql-srv` crate.
#[async_trait]
pub trait Backend {
    /// An associated type that can be converted into this crate's `Value` type.
    type Value: TryInto<Value, Error = Error>;

    /// An associated type representing a row returned by a SQL query, which can be iterated to
    /// produce `Self::Value`s.
    type Row: IntoIterator<Item = Self::Value>;

    /// An associated type representing a resultset returned by a SQL query, which can be iterated
    /// to produce `Self::Row`s.
    type Resultset: IntoIterator<Item = Self::Row>;

    /// Initializes the backend.
    ///
    /// * `database` - The name of the database that will be used for queries to this `Backend`
    ///   instance.
    async fn on_init(&mut self, database: &str) -> Result<(), Error>;

    /// Performs the specified SQL query.
    ///
    /// * `query` - The sql query to perform.
    /// * returns - A `QueryResponse` containing either the data retrieved (for a read query) or a
    ///   confirmation (for a write query), or else an `Error` if a failure occurs.
    async fn on_query(&mut self, query: &str) -> Result<QueryResponse<Self::Resultset>, Error>;

    /// Prepares the specified SQL query, creating a prepared statement.
    ///
    /// * `query` - The sql query to prepare.
    /// * returns - A `PrepareResponse` containing metadata about the new prepared statement, or an
    ///   `Error` if a failure occurs.
    async fn on_prepare(&mut self, query: &str) -> Result<PrepareResponse, Error>;

    /// Executes a previously prepared SQL query using the provided parameters.
    ///
    /// * `statement_id` - The identifier of the previously created prepared statement.
    /// * `params` - The values to substitue for the prepared statement's parameter placeholders.
    /// * returns - A `QueryResponse` containing either the data retrieved (for a read query) or a
    ///   confirmation (for a write query), or else an `Error` if a failure occurs.
    async fn on_execute(
        &mut self,
        statement_id: u32,
        params: &[Value],
    ) -> Result<QueryResponse<Self::Resultset>, Error>;

    /// Closes (deallocates) a prepared statement.
    ///
    /// * `statement_id` - The identifier of the prepared statement to close.
    async fn on_close(&mut self, statement_id: u32) -> Result<(), Error>;
}

/// A description of a column, either in the parameters to a query or in a resultset
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Column {
    /// The name of the column
    pub name: String,

    /// The type of the column
    pub col_type: Type,
}

/// A response produced by `Backend::on_prepare`, containing metadata about a newly created
/// prepared statement.
pub struct PrepareResponse {
    /// An identifier for the new prepared statement.
    pub prepared_statement_id: u32,
    /// The schema for parameters to be provided to the prepared statement.
    pub param_schema: Vec<Type>,
    /// The schema for rows to be returned when the prepared statement is executed.
    pub row_schema: Vec<Column>,
}

/// A response produced by `Backend::on_query` or `Backend::on_execute`, containing either data
/// that has been requested using a read statement or the confirmation of a write statement.
pub enum QueryResponse<S> {
    /// The response to a select statement.
    Select {
        /// The schema of the resultset produced by the select statement.
        schema: Vec<Column>,
        /// The actual resultset produced by the select statement.
        resultset: S,
    },
    /// The response to an insert statement, including the number of rows inserted.
    Insert(u64),
    /// The response to an update statement, including the number of rows updated.
    Update(u64),
    /// The response to a delete statement, including the number of rows deleted.
    Delete(u64),
    /// The response to a command statement such as "CREATE TABLE".
    Command,
}

/// Run a `Backend` on the provided bytestream until the bytestream is remotely closed.
///
/// * `backend` - A `Backend` object that emulates a PostgreSQL database as described above.
/// * `channel` - A bytestream channel connected to a PostgreSQL frontend. Requests sent by the
///   frontend on this channel will be forwarded to `backend`, and the `backend`'s responses will be
///   returned to the frontend. When `channel` is closed by the frontend, `run_backend` returns.
pub async fn run_backend<B: Backend, C: AsyncRead + AsyncWrite + Unpin>(backend: B, channel: C) {
    runner::Runner::run(backend, channel).await
}
