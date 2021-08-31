use std::collections::HashMap;
use std::str::FromStr;

use async_trait::async_trait;
use futures::TryStreamExt;
use noria::{unsupported, DataType, ReadySetError};
use noria_client::{UpstreamDatabase, UpstreamPrepare};
use pgsql::{Config, Row};
use tokio_postgres as pgsql;

use crate::Error;

/// A connector to an underlying PostgreSQL database
pub struct PostgreSqlUpstream {
    /// This is the underlying (regular) PostgreSQL client
    client: pgsql::Client,
    /// A tokio task that handles the connection, required by `tokio_postgres` to operate
    _connection_handle: tokio::task::JoinHandle<Result<(), pgsql::Error>>,
    /// Map from prepared statement IDs to prepared statements
    prepared_statements: HashMap<u32, pgsql::Statement>,
    /// ID for the next prepared statement
    statement_id_counter: u32,
    /// The original URL used to create the connection
    url: String,
    /// Indicates whether we are currently in a transaction.
    in_transaction: bool,
}

#[derive(Debug)]
pub enum QueryResult {
    ReadResult { data: Vec<Row> },
    WriteResult { num_rows_affected: u64 },
    None,
}

#[async_trait]
impl UpstreamDatabase for PostgreSqlUpstream {
    type QueryResult = QueryResult;
    type Error = Error;
    type StatementMeta = (); // TODO(grfn)

    async fn connect(url: String) -> Result<Self, Error> {
        let config = Config::from_str(&url)?;
        let connector = native_tls::TlsConnector::builder().build().unwrap(); // Never returns an error
        let tls = postgres_native_tls::MakeTlsConnector::new(connector);
        let (client, connection) = config.connect(tls).await?;
        let _connection_handle = tokio::spawn(connection);

        Ok(Self {
            client,
            _connection_handle,
            prepared_statements: Default::default(),
            statement_id_counter: 0,
            url,
            in_transaction: false,
        })
    }

    fn url(&self) -> &str {
        &self.url
    }

    async fn prepare<'a, S>(&'a mut self, query: S) -> Result<UpstreamPrepare<Self>, Error>
    where
        S: AsRef<str> + Send + Sync + 'a,
    {
        let query = query.as_ref();
        let statement = self.client.prepare(query).await?;
        self.statement_id_counter += 1;
        let statement_id = self.statement_id_counter;
        self.prepared_statements.insert(statement_id, statement);
        Ok(UpstreamPrepare {
            statement_id,
            is_read: false,
            meta: (),
        })
    }

    async fn handle_read<'a, S>(&'a mut self, query: S) -> Result<Self::QueryResult, Error>
    where
        S: AsRef<str> + Send + Sync + 'a,
    {
        let data = self.client.query(query.as_ref(), &[]).await?;
        Ok(QueryResult::ReadResult { data })
    }

    async fn handle_write<'a, S>(&'a mut self, query: S) -> Result<Self::QueryResult, Error>
    where
        S: AsRef<str> + Send + Sync + 'a,
    {
        Ok(QueryResult::WriteResult {
            num_rows_affected: self.client.execute(query.as_ref(), &[]).await?,
        })
    }

    async fn handle_ryw_write<'a, S>(
        &'a mut self,
        _query: S,
    ) -> Result<(Self::QueryResult, String), Error>
    where
        S: AsRef<str> + Send + Sync + 'a,
    {
        unsupported!("Read-Your-Write not yet implemented for PostgreSQL")
    }

    async fn execute_read(
        &mut self,
        statement_id: u32,
        params: Vec<DataType>,
    ) -> Result<Self::QueryResult, Error> {
        let statement = self
            .prepared_statements
            .get(&statement_id)
            .ok_or(ReadySetError::PreparedStatementMissing { statement_id })?;
        Ok(QueryResult::ReadResult {
            data: self
                .client
                .query_raw(statement, params)
                .await?
                .try_collect()
                .await?,
        })
    }

    async fn execute_write(
        &mut self,
        statement_id: u32,
        params: Vec<DataType>,
    ) -> Result<Self::QueryResult, Error> {
        let statement = self
            .prepared_statements
            .get(&statement_id)
            .ok_or(ReadySetError::PreparedStatementMissing { statement_id })?;
        Ok(QueryResult::WriteResult {
            num_rows_affected: self.client.execute_raw(statement, params).await?,
        })
    }

    /// Handle starting a transaction with the upstream database.
    async fn start_tx(&mut self) -> Result<Self::QueryResult, Error> {
        self.client.query("START TRANSACTION", &[]).await?;
        self.in_transaction = true;
        Ok(QueryResult::None)
    }

    /// Return whether we are currently in a transaction or not.
    fn is_in_tx(&self) -> bool {
        self.in_transaction
    }

    /// Handle committing a transaction to the upstream database.
    async fn commit(&mut self) -> Result<Self::QueryResult, Error> {
        self.client.query("COMMIT", &[]).await?;
        self.in_transaction = false;
        Ok(QueryResult::None)
    }

    /// Handle rolling back the ongoing transaction for this connection to the upstream db.
    async fn rollback(&mut self) -> Result<Self::QueryResult, Error> {
        self.client.query("ROLLBACK", &[]).await?;
        self.in_transaction = false;
        Ok(QueryResult::None)
    }
}
