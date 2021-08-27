use async_trait::async_trait;
use mysql_async::consts::CapabilityFlags;
use mysql_async::prelude::Queryable;
use mysql_async::{Column, Conn, OptsBuilder, Row, TxOpts};
use std::collections::HashMap;
use std::convert::TryInto;
use std::sync::Arc;
use tracing::{debug, error};

use noria::errors::internal_err;
use noria::{DataType, ReadySetError};
use noria_client::{Error, UpstreamDatabase, UpstreamPrepare};

type StatementID = u32;

fn dt_to_value_params(dt: Vec<DataType>) -> Result<Vec<mysql_async::Value>, noria::ReadySetError> {
    dt.into_iter().map(|v| v.try_into()).collect()
}

#[derive(Debug)]
pub struct WriteResult {
    pub num_rows_affected: u64,
    pub last_inserted_id: u64,
}

#[derive(Debug)]
pub struct ReadResult {
    pub data: Vec<Row>,
    pub columns: Option<Arc<[Column]>>,
}

/// A connector to an underlying mysql store. This is really just a wrapper for the mysql crate.
pub struct MySqlUpstream {
    conn: Conn,
    prepared_statements: HashMap<StatementID, mysql_async::Statement>,
    url: String,
}

#[async_trait]
impl UpstreamDatabase for MySqlUpstream {
    type ReadResult = ReadResult;
    type WriteResult = WriteResult;
    type Column = Column;

    async fn connect(url: String) -> Result<Self, Error> {
        // CLIENT_SESSION_TRACK is required for GTID information to be sent in OK packets on commits
        // GTID information is used for RYW
        // Currently this causes rows affected to return an incorrect result, so this is feature
        // gated.
        let conn = if cfg!(feature = "ryw") {
            Conn::new(
                OptsBuilder::from_opts(url.clone())
                    .add_capability(CapabilityFlags::CLIENT_SESSION_TRACK),
            )
            .await
            .unwrap()
        } else {
            Conn::new(OptsBuilder::from_opts(url.clone()))
                .await
                .unwrap()
        };
        let prepared_statements = HashMap::new();
        Ok(Self {
            conn,
            prepared_statements,
            url,
        })
    }

    fn url(&self) -> &str {
        &self.url
    }

    /// Prepares the given query using the mysql connection. Note, queries are prepared on a
    /// per connection basis. They are not universal.
    async fn prepare<'a, S>(&'a mut self, query: S) -> Result<UpstreamPrepare<Self::Column>, Error>
    where
        S: AsRef<str> + Send + Sync + 'a,
    {
        let is_read = {
            // TODO(DAN): This is a bad hack to determine what type of result the query will
            // return. This method should be replaced by resultset parsing.
            let q = query.as_ref().to_string().trim_start().to_lowercase();
            q.starts_with("select") || q.starts_with("show") || q.starts_with("describe")
        };
        let statement = self.conn.prep(query).await?;
        self.prepared_statements
            .insert(statement.id(), statement.clone());
        Ok(UpstreamPrepare {
            statement_id: statement.id(),
            params: statement.params().to_owned(),
            schema: statement.columns().to_owned(),
            is_read,
        })
    }

    /// Executes the prepared select
    async fn execute_read(
        &mut self,
        id: u32,
        params: Vec<DataType>,
    ) -> Result<Self::ReadResult, Error> {
        let params = dt_to_value_params(params)?;
        let mut result = self
            .conn
            .exec_iter(
                self.prepared_statements.get(&id).ok_or(Error::ReadySet(
                    ReadySetError::PreparedStatementMissing { statement_id: id },
                ))?,
                params,
            )
            .await?;
        let columns = result.columns();
        let data = result.collect().await?;
        Ok(ReadResult { data, columns })
    }

    async fn execute_write(
        &mut self,
        id: u32,
        params: Vec<DataType>,
    ) -> Result<Self::WriteResult, Error> {
        let params = dt_to_value_params(params)?;
        let statement = self.prepared_statements.get(&id).ok_or(Error::ReadySet(
            ReadySetError::PreparedStatementMissing { statement_id: id },
        ))?;
        self.conn.exec_drop(statement, params).await?;
        Ok(WriteResult {
            num_rows_affected: self.conn.affected_rows(),
            last_inserted_id: self.conn.last_insert_id().unwrap_or(0),
        })
    }

    async fn handle_read<'a, S>(&'a mut self, query: S) -> Result<Self::ReadResult, Error>
    where
        S: AsRef<str> + Send + Sync + 'a,
    {
        let mut result = self.conn.query_iter(query).await?;
        let columns = result.columns();
        let data = result.collect().await?;
        Ok(ReadResult { data, columns })
    }

    /// Executes the given query on the mysql backend.
    async fn handle_write<'a, S>(&'a mut self, query: S) -> Result<Self::WriteResult, Error>
    where
        S: AsRef<str> + Send + Sync + 'a,
    {
        self.conn.query_drop(query).await.map_err(|e| {
            error!("Could not execute query in mysql : {:?}", e);
            e
        })?;
        debug!(
            "results : {:?}, {:?}",
            self.conn.affected_rows(),
            self.conn.last_insert_id()
        );
        Ok(WriteResult {
            num_rows_affected: self.conn.affected_rows(),
            last_inserted_id: self.conn.last_insert_id().unwrap_or(0),
        })
    }

    /// Executes the given query on the mysql backend.
    async fn handle_ryw_write<'a, S>(
        &'a mut self,
        query: S,
    ) -> Result<(Self::WriteResult, String), Error>
    where
        S: AsRef<str> + Send + Sync + 'a,
    {
        let mut transaction = self.conn.start_transaction(TxOpts::default()).await?;
        transaction.query_drop(query).await.map_err(|e| {
            error!("Could not execute query in mysql : {:?}", e);
            e
        })?;

        let affected_rows = transaction.affected_rows();
        let last_insert_id = transaction.last_insert_id();
        debug!("results : {:?}, {:?}", affected_rows, last_insert_id);

        let txid = transaction.commit_returning_gtid().await.map_err(|e| {
            internal_err(format!(
                "Error obtaining GTID from MySQL for RYW-enabled commit: {}",
                e
            ))
        })?;
        Ok((
            WriteResult {
                num_rows_affected: affected_rows,
                last_inserted_id: last_insert_id.unwrap_or(0),
            },
            txid,
        ))
    }
}
