use crate::backend::error::Error;
use crate::backend::{PrepareResult, QueryResult};
use mysql_async::consts::CapabilityFlags;
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, OptsBuilder, TxOpts};
use noria::errors::ReadySetError;
use noria::{errors::internal_err, DataType};
use std::collections::HashMap;
use std::convert::TryInto;

type StatementID = u32;

fn dt_to_value_params(dt: Vec<DataType>) -> Result<Vec<mysql_async::Value>, noria::ReadySetError> {
    dt.into_iter().map(|v| v.try_into()).collect()
}

/// A connector to an underlying mysql store. This is really just a wrapper for the mysql crate.
pub struct MySqlConnector {
    conn: Conn,
    prepared_statements: HashMap<StatementID, mysql_async::Statement>,
    url: String,
}

impl MySqlConnector {
    pub async fn new(url: String) -> Self {
        // CLIENT_SESSION_TRACK is required for GTID information to be sent in OK packets on commits
        // GTID information is used for RYW
        let conn = Conn::new(
            OptsBuilder::from_opts(url.clone())
                .add_capability(CapabilityFlags::CLIENT_SESSION_TRACK),
        )
        .await
        .unwrap();
        let prepared_statements = HashMap::new();
        MySqlConnector {
            conn,
            prepared_statements,
            url,
        }
    }

    /// Returns the url used to establish the connection
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Prepares the given query using the mysql connection. Note, queries are prepared on a
    /// per connection basis. They are not universal.
    pub async fn on_prepare(&mut self, query: &str) -> Result<PrepareResult, Error> {
        let statement = self.conn.prep(query).await?;
        self.prepared_statements
            .insert(statement.id(), statement.clone());
        Ok(PrepareResult::MySqlPrepare {
            statement_id: statement.id(),
            params: statement.params().iter().map(|c| c.into()).collect(),
            schema: statement.columns().iter().map(|c| c.into()).collect(),
            // TODO(DAN): This is a bad hack to determine what type of result the query will
            // return. This method should be replaced by resultset parsing.
            is_read: {
                let q = query.to_string().trim_start().to_lowercase();
                q.starts_with("select") || q.starts_with("show") || q.starts_with("describe")
            },
        })
    }

    /// Executes the prepared select
    pub async fn execute_read(
        &mut self,
        id: u32,
        params: Vec<DataType>,
    ) -> Result<QueryResult, Error> {
        let params = dt_to_value_params(params)?;
        let mut result = self
            .conn
            .exec_iter(
                self.prepared_statements
                    .get(&id)
                    .ok_or(Error::ReadySet(ReadySetError::PreparedStatementMissing))?,
                params,
            )
            .await?;
        let columns = result.columns();
        let data = result.collect().await?;
        Ok(QueryResult::MySqlSelect { data, columns })
    }

    // TODO(DAN): This function is used on execute fallback. However, we should instead prepare the
    // statement and use execute_read or execute_write
    /// Executes the prepared select using the query string
    pub async fn execute_select_query_string(
        &mut self,
        query: &str,
        params: Vec<DataType>,
    ) -> Result<QueryResult, Error> {
        let params = dt_to_value_params(params)?;
        let mut result = self.conn.exec_iter(query, params).await?;
        let columns = result.columns();
        let data = result.collect().await?;
        Ok(QueryResult::MySqlSelect { data, columns })
    }

    /// Executes the prepared write
    pub async fn execute_write(
        &mut self,
        id: u32,
        params: Vec<DataType>,
    ) -> Result<QueryResult, Error> {
        let params = dt_to_value_params(params)?;
        let statement = self
            .prepared_statements
            .get(&id)
            .ok_or(Error::ReadySet(ReadySetError::PreparedStatementMissing))?;
        self.conn.exec_drop(statement, params).await?;
        Ok(QueryResult::MySqlWrite {
            num_rows_affected: self.conn.affected_rows(),
            last_inserted_id: self.conn.last_insert_id().unwrap_or(0),
        })
    }

    pub async fn handle_select(&mut self, query: &str) -> Result<QueryResult, Error> {
        let q = query.to_string();
        let mut result = self.conn.query_iter(&q).await?;
        let columns = result.columns();
        let rows = result.collect().await?;
        Ok(QueryResult::MySqlSelect {
            data: rows,
            columns,
        })
    }

    /// Executes the given query on the mysql backend.
    /// If `create_identifier`, creates identifier for the write to be used by RYW timestamp service
    pub async fn handle_write(
        &mut self,
        query: &str,
        create_identifier: bool,
    ) -> Result<(QueryResult, Option<String>), Error> {
        let q = query.to_string();
        if create_identifier {
            let mut transaction = self.conn.start_transaction(TxOpts::default()).await?;
            transaction.query_drop(&q).await.map_err(|e| {
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
                QueryResult::MySqlWrite {
                    num_rows_affected: affected_rows,
                    last_inserted_id: last_insert_id.unwrap_or(0),
                },
                Some(txid),
            ))
        } else {
            self.conn.query_drop(&q).await.map_err(|e| {
                error!("Could not execute query in mysql : {:?}", e);
                e
            })?;
            debug!("Successfully executed query : {}", &q);
            debug!(
                "results : {:?}, {:?}",
                self.conn.affected_rows(),
                self.conn.last_insert_id()
            );
            Ok((
                QueryResult::MySqlWrite {
                    num_rows_affected: self.conn.affected_rows(),
                    last_inserted_id: self.conn.last_insert_id().unwrap_or(0),
                },
                None,
            ))
        }
    }
}
