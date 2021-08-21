use mysql_async::prelude::Queryable;
use mysql_async::*;
use mysql_async::{consts::CapabilityFlags, params::Params};

use crate::backend::error::Error;
use crate::backend::QueryResult;
use noria::{errors::internal_err, DataType};
use std::collections::HashMap;

type StatementID = u32;

/// A connector to an underlying mysql store. This is really just a wrapper for the mysql crate.
pub struct MySqlConnector {
    conn: Conn,
    prepared_statements: HashMap<StatementID, String>,
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
    pub async fn on_prepare(
        &mut self,
        query: &str,
        statement_id: u32,
    ) -> std::result::Result<u32, Error> {
        // FIXME: Actually use the prepared statement.
        // This isn't straightforward, because the statements take `self` by value when you try
        // and do anything with them.
        // So, for now, we just prepare it to check that it's valid and drop the result.
        self.conn.prep(query).await?;
        self.prepared_statements
            .insert(statement_id, query.to_owned());
        debug!("Successfully prepared statement : {}", query);
        Ok(statement_id)
    }

    /// Executes the prepared statement with the given ID, returning the number of rows affected and
    /// the last inserted ID.
    ///
    /// param parsing doesnt work so this will not work either. Clubhouse ticket attached.
    pub async fn on_execute(
        &mut self,
        id: u32,
        _params: Vec<DataType>,
    ) -> std::result::Result<(u64, u64), Error> {
        let stmt = self.prepared_statements.get(&id).unwrap().as_str();
        // todo : these params are incorrect. Clubhouse story here : https://app.clubhouse.io/readysettech/story/211/support-prepared-writes-and-executes-to-mysql
        //let mysql_params : Vec<Value> = params.into_iter().map(|p| {p.value}).collect();
        self.conn.exec_drop(stmt, Params::Empty).await?;
        Ok((
            self.conn.affected_rows(),
            self.conn.last_insert_id().unwrap_or(0),
        ))
    }

    pub async fn handle_select(&mut self, query: &str) -> std::result::Result<QueryResult, Error> {
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
    ) -> std::result::Result<(QueryResult, Option<String>), Error> {
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
