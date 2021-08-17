use std::str::FromStr;

use noria::DataType;
use pgsql::Config;
use tokio_postgres as pgsql;

use super::QueryResult;
use crate::backend::error::Error;

/// A connector to an underlying PostgreSQL database
pub struct PostgreSqlConnector {
    /// This is the underlying (regular) PostgreSQL client
    client: pgsql::Client,
    /// A tokio task that handles the connection, required by `tokio_postgres` to operate
    _connection_handle: tokio::task::JoinHandle<Result<(), pgsql::Error>>,
}

impl PostgreSqlConnector {
    pub async fn new<S>(url: S) -> Result<Self, Error>
    where
        S: AsRef<str>,
    {
        let config = Config::from_str(url.as_ref())?;
        let connector = native_tls::TlsConnector::builder().build().unwrap(); // Never returns an error
        let tls = postgres_native_tls::MakeTlsConnector::new(connector);
        let (client, connection) = config.connect(tls).await?;
        let _connection_handle = tokio::spawn(connection);

        Ok(Self {
            client,
            _connection_handle,
        })
    }

    /// Prepares the given query in postgres
    pub async fn on_prepare<S>(&mut self, query: S) -> Result<pgsql::Statement, Error>
    where
        S: AsRef<str>,
    {
        Ok(self.client.prepare(query.as_ref()).await?)
    }

    /// Execute the given prepared statement with the given params, returning the number of rows
    /// affected
    pub async fn on_execute(
        &mut self,
        statement: &pgsql::Statement,
        params: Vec<DataType>,
    ) -> Result<u64, Error> {
        Ok(self.client.execute_raw(statement, params).await?)
    }

    /// Execute the given parameter-free select query
    pub async fn handle_select<S>(&mut self, query: S) -> Result<QueryResult, Error>
    where
        S: AsRef<str>,
    {
        let data = self.client.query(query.as_ref(), &[]).await?;
        Ok(QueryResult::PgSqlSelect { data })
    }

    /// Executee the given write query
    pub async fn handle_write<S>(&mut self, query: S) -> Result<QueryResult, Error>
    where
        S: AsRef<str>,
    {
        let num_rows_affected = self.client.execute(query.as_ref(), &[]).await?;
        Ok(QueryResult::PgSqlWrite { num_rows_affected })
    }
}
