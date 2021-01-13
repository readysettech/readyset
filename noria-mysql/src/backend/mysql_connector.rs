use mysql::params::Params;
use mysql::prelude::Queryable;
use mysql::*;

use msql_srv::{self, *};

use crate::backend::error::Error;
use std::collections::HashMap;

type StatementID = u32;

/// A connector to an underlying mysql store. This is really just a wrapper for the mysql crate.
pub struct MySqlConnector {
    db_conn: PooledConn,
    prepared_statement_cache: HashMap<StatementID, mysql::Statement>,
}

impl MySqlConnector {
    pub async fn new(url: String) -> Self {
        let pool = Pool::new(url).unwrap();
        let db_conn = pool.get_conn().unwrap();
        let prepared_statement_cache = HashMap::new();
        MySqlConnector {
            db_conn,
            prepared_statement_cache,
        }
    }

    /// Prepares the given query using the mysql connection. Note, queries are prepared on a
    /// per connection basis. They are not universal.
    pub fn on_prepare(
        &mut self,
        query: &str,
        statement_id: u32,
    ) -> std::result::Result<u32, Error> {
        // todo : we technically call prep but we dont support ever referencing the query in the future.
        // todo : https://app.clubhouse.io/readysettech/story/211/support-prepared-writes-and-executes-to-mysql
        let prepared_statement = self.db_conn.prep(query)?;
        self.prepared_statement_cache
            .insert(statement_id, prepared_statement);
        debug!("Successfully prepared statement : {}", query);
        Ok(statement_id)
    }

    /// Executes the prepared statement with the given ID but param parsing doesnt work
    /// so this will not work either. Clubhouse ticket attached.
    pub fn on_execute(
        &mut self,
        id: u32,
        _params: ParamParser<'_>,
    ) -> std::result::Result<(u64, u64), Error> {
        let stmt = self.prepared_statement_cache.get(&id).unwrap();
        // todo : these params are incorrect. Clubhouse story here : https://app.clubhouse.io/readysettech/story/211/support-prepared-writes-and-executes-to-mysql
        //let mysql_params : Vec<Value> = params.into_iter().map(|p| {p.value}).collect();
        let results = self.db_conn.exec_iter(stmt, Params::Empty)?;
        Ok((
            results.affected_rows(),
            results.last_insert_id().unwrap_or(0),
        ))
    }

    /// Executes the given query on the mysql backend.
    pub fn on_query(&mut self, query: &str) -> std::result::Result<(u64, u64), Error> {
        let q = query.to_string();
        let results = self.db_conn.exec_iter(&q, ()).map_err(|e| {
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
        ))
    }
}
