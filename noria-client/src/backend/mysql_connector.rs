use mysql_async::params::Params;
use mysql_async::prelude::Queryable;
use mysql_async::*;

use msql_srv::{self, *};

use crate::backend::error::Error;
use std::collections::HashMap;

type StatementID = u32;

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
    pub async fn on_query(&mut self, query: &str) -> std::result::Result<(u64, u64), Error> {
        let conn = self.pool.get_conn().await?;
        let q = query.to_string();
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
        ))
    }
}
