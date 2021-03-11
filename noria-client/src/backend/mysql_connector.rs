use mysql_async::params::Params;
use mysql_async::prelude::Queryable;
use mysql_async::*;

use msql_srv::{self, *};
use timestamp_service::client::TransactionId;

use crate::backend::error::Error;
use std::collections::HashMap;

type StatementID = u32;

const GET_TRXID_QUERY: &str = "SELECT tx.trx_id FROM information_schema.innodb_trx tx WHERE tx.trx_mysql_thread_id = connection_id()";
/// A connector to an underlying mysql store. This is really just a wrapper for the mysql crate.
pub struct MySqlConnector {
    pool: Pool,
    prepared_statements: HashMap<StatementID, String>,
}

impl MySqlConnector {
    pub async fn new(url: String) -> Self {
        let pool = Pool::new(url);
        let prepared_statements = HashMap::new();
        MySqlConnector {
            pool,
            prepared_statements,
        }
    }

    /// Prepares the given query using the mysql connection. Note, queries are prepared on a
    /// per connection basis. They are not universal.
    pub async fn on_prepare(
        &mut self,
        query: &str,
        statement_id: u32,
    ) -> std::result::Result<u32, Error> {
        let conn = self.pool.get_conn().await?;
        // FIXME: Actually use the prepared statement.
        // This isn't straightforward, because the statements take `self` by value when you try
        // and do anything with them.
        // So, for now, we just prepare it to check that it's valid and drop the result.
        let _ = conn.prepare(query).await?;
        self.prepared_statements
            .insert(statement_id, query.to_owned());
        debug!("Successfully prepared statement : {}", query);
        Ok(statement_id)
    }

    /// Executes the prepared statement with the given ID but param parsing doesnt work
    /// so this will not work either. Clubhouse ticket attached.
    pub async fn on_execute(
        &mut self,
        id: u32,
        _params: ParamParser<'_>,
    ) -> std::result::Result<(u64, u64), Error> {
        let conn = self.pool.get_conn().await?;
        let stmt = self.prepared_statements.get(&id).unwrap();
        // todo : these params are incorrect. Clubhouse story here : https://app.clubhouse.io/readysettech/story/211/support-prepared-writes-and-executes-to-mysql
        //let mysql_params : Vec<Value> = params.into_iter().map(|p| {p.value}).collect();
        let results = conn.prep_exec(stmt, Params::Empty).await?;
        Ok((
            results.affected_rows(),
            results.last_insert_id().unwrap_or(0),
        ))
    }

    /// Executes the given query on the mysql backend.
    /// If `create_identifier`, creates identifier for the write to be used by RYW timestamp service
    pub async fn on_query(
        &mut self,
        query: &str,
        create_identifier: bool,
    ) -> std::result::Result<(u64, u64, Option<TransactionId>), Error> {
        let q = query.to_string();

        if create_identifier {
            let transaction = self
                .pool
                .start_transaction(TransactionOptions::default())
                .await?;
            let results = transaction.query(&q).await.map_err(|e| {
                error!("Could not execute query in mysql : {:?}", e);
                e
            })?;

            let affected_rows = results.affected_rows();
            let last_insert_id = results.last_insert_id();

            debug!("results : {:?}, {:?}", affected_rows, last_insert_id);

            let transaction = results.drop_result().await?;
            // NOTE: MySQL connection must have proper privileges for this query to work!
            // For now, just get the trx_id, but could append query text or other info from table
            let trxid_results = transaction.query(GET_TRXID_QUERY).await?;
            let (transaction, trxids): (Transaction<Conn>, Vec<String>) = trxid_results
                .map_and_drop(|row| {
                    let trxid: String = from_row(row);
                    trxid
                })
                .await?;
            if trxids.len() < 1 {
                return Err(Error::UnsupportedError(String::from(
                    "RYW Write cannot proceed if no trxids found",
                )));
            };
            let trxid = &trxids[0];
            debug!(
                "Successfully executed query : {}, wrapped in transaction={}",
                &q, trxid
            );
            transaction.commit().await?;

            Ok((
                affected_rows,
                last_insert_id.unwrap_or(0),
                Some(trxid.to_string()),
            ))
        } else {
            let conn = self.pool.get_conn().await?;
            let results = conn.query(&q).await.map_err(|e| {
                error!("Could not execute query in mysql : {:?}", e);
                e
            })?;
            debug!("Successfully executed query : {}", &q);
            debug!(
                "results : {:?}, {:?}",
                results.affected_rows(),
                results.last_insert_id()
            );
            Ok((
                results.affected_rows(),
                results.last_insert_id().unwrap_or(0),
                None,
            ))
        }
    }
}
