use std::collections::HashMap;
use std::str::FromStr;

use async_trait::async_trait;
use futures::TryStreamExt;
use noria::{unsupported, DataType, ReadySetError};
use pgsql::{Config, Row};
use tokio_postgres as pgsql;

use crate::upstream_database::UpstreamPrepare;
use crate::{Error, UpstreamDatabase};

/// A connector to an underlying PostgreSQL database
pub struct PostgreSqlConnector {
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
}

#[async_trait]
impl UpstreamDatabase for PostgreSqlConnector {
    type ReadResult = Vec<Row>;
    type WriteResult = u64;

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
        })
    }

    fn url(&self) -> &str {
        &self.url
    }

    async fn prepare<'a, S>(&'a mut self, query: S) -> Result<UpstreamPrepare, Error>
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
            // TODO: fill these in
            params: vec![],
            schema: vec![],
            is_read: false,
        })
    }

    async fn handle_read<'a, S>(&'a mut self, query: S) -> Result<Self::ReadResult, Error>
    where
        S: AsRef<str> + Send + Sync + 'a,
    {
        let data = self.client.query(query.as_ref(), &[]).await?;
        Ok(data)
    }

    async fn handle_write<'a, S>(&'a mut self, query: S) -> Result<Self::WriteResult, Error>
    where
        S: AsRef<str> + Send + Sync + 'a,
    {
        Ok(self.client.execute(query.as_ref(), &[]).await?)
    }

    async fn handle_ryw_write<'a, S>(
        &'a mut self,
        _query: S,
    ) -> Result<(Self::WriteResult, String), Error>
    where
        S: AsRef<str> + Send + Sync + 'a,
    {
        unsupported!("Read-Your-Write not yet implemented for PostgreSQL")
    }

    async fn execute_read(
        &mut self,
        statement_id: u32,
        params: Vec<DataType>,
    ) -> Result<Self::ReadResult, Error> {
        let statement = self
            .prepared_statements
            .get(&statement_id)
            .ok_or(ReadySetError::PreparedStatementMissing { statement_id })?;
        Ok(self
            .client
            .query_raw(statement, params)
            .await?
            .try_collect()
            .await?)
    }

    async fn execute_write(
        &mut self,
        statement_id: u32,
        params: Vec<DataType>,
    ) -> Result<Self::WriteResult, Error> {
        let statement = self
            .prepared_statements
            .get(&statement_id)
            .ok_or(ReadySetError::PreparedStatementMissing { statement_id })?;
        Ok(self.client.execute_raw(statement, params).await?)
    }
}
