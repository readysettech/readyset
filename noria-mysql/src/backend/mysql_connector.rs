use noria::DataType;

use mysql::*;
use mysql::prelude::Queryable;

use msql_srv::{self, *};
use nom_sql::{self, ColumnConstraint, InsertStatement, Literal, SelectStatement, UpdateStatement, CreateViewStatement, SqlQuery, CreateTableStatement, DeleteStatement, SetStatement};

use std::io;
use std::collections::HashMap;
use std::result;

use crate::backend::writer::Writer;
use crate::backend::PreparedStatement;
use crate::utils;

pub struct MySqlConnector {
    db_conn: PooledConn,
    prepared_statement_cache: HashMap<PreparedStatement, mysql::Statement>
}

impl MySqlConnector {
    pub async fn new(
        url : &str,
    ) -> Self {
        let pool = Pool::new(url).unwrap();
        let mut db_conn = pool.get_conn().unwrap();
        let prepared_statement_cache = HashMap::new();
        MySqlConnector {
            db_conn,
            prepared_statement_cache
        }
    }

    /*
     * Returns the number of affected rows if successful and the underlying mysql error if not.
     */
    pub fn execute_query(&mut self, query: String) -> std::result::Result<(u64, Option<u64>), mysql::Error> {
        let results = self.db_conn.exec_iter(query, ())?;
        let row_count = results.affected_rows();
        let last_insert_id = results.last_insert_id();
        Ok((row_count, last_insert_id))
    }

    /*
     * Returns the number of affected rows if successful and the underlying mysql error if not.
     */
    // pub fn prepare_query(query: String)  -> std::result::Result<u64, mysql::Error> {
    //     //conn.prep(query)
    // }

    // pub fn execute_prepared_statement(statement: Statement)  -> std::result::Result<u64, mysql::Error> {
    //     //conn.exec(statement)
    // }
}

impl<W: io::Write> Writer<W> for MySqlConnector {
    fn handle_set(&mut self, q: SetStatement, results: QueryResultWriter<W>) -> io::Result<()> {
        unimplemented!();
    }

    fn handle_insert(&mut self, q: InsertStatement, results: QueryResultWriter<W>) -> io::Result<()> {
        let query = q.to_string();
        match self.execute_query(query) {
            Ok((affected_rows, insert_id)) => {
                return results.completed(affected_rows, insert_id.unwrap_or(0));
            }
            Err(e) => {
                return results.error(
                    msql_srv::ErrorKind::ER_UNKNOWN_ERROR,
                    format!("{:?}", e).as_bytes(),
                );
            }
        }
    }

    fn prepare_insert(&mut self, sql_q: SqlQuery, info: StatementMetaWriter<W>, statement_id: u32) -> io::Result<PreparedStatement> {
        unimplemented!()
    }

    fn execute_prepared_insert(&mut self, insert_statement: PreparedStatement, params: ParamParser, results: QueryResultWriter<W>) -> io::Result<()> {
        unimplemented!()
    }

    fn handle_delete(&mut self, q: DeleteStatement, results: QueryResultWriter<W>) -> io::Result<()> {
        let query = q.to_string();
        match self.execute_query(query) {
            Ok((affected_rows, insert_id)) => {
                return results.completed(affected_rows, insert_id.unwrap_or(0));
            }
            Err(e) => {
                return results.error(
                    msql_srv::ErrorKind::ER_UNKNOWN_ERROR,
                    format!("{:?}", e).as_bytes(),
                );
            }
        }
    }

    fn handle_update(&mut self, q: UpdateStatement, results: QueryResultWriter<W>) -> io::Result<()> {
        let query = q.to_string();
        match self.execute_query(query) {
            Ok((affected_rows, insert_id)) => {
                return results.completed(affected_rows, insert_id.unwrap_or(0));
            }
            Err(e) => {
                return results.error(
                    msql_srv::ErrorKind::ER_UNKNOWN_ERROR,
                    format!("{:?}", e).as_bytes(),
                );
            }
        }
    }

    fn prepare_update(&mut self, sql_q: SqlQuery, info: StatementMetaWriter<W>, statement_id: u32) -> io::Result<PreparedStatement> {
        let mysql_statement = self.db_conn.prep(sql_q.clone())?;
        let prepared_statement = PreparedStatement::Update(UpdateStatement(sql_q));

        let params: Vec<_> = {
            // extract parameter columns -- easy here, since they must all be in the same table
            let param_cols = utils::get_parameter_columns(&sql_q);
            param_cols
                .into_iter()
                .map(|c| {
                    schema
                        .iter()
                        .cloned()
                        .find(|mc| c.name == mc.column)
                        .expect(&format!("column '{}' missing in mutator schema", c))
                })
                .collect()
        };

        self.prepared_statement_cache.insert(prepared_statement, mysql_statement);

    }

    fn execute_prepared_update(&mut self, update_statement: PreparedStatement, params: ParamParser, results: QueryResultWriter<W>) -> io::Result<()> {
        unimplemented!()
    }

    fn handle_create_table(&mut self, q: CreateTableStatement, results: QueryResultWriter<W>) -> io::Result<()> {
        let query = q.to_string();
        match self.execute_query(query) {
            Ok((affected_rows, insert_id)) => {
                return results.completed(affected_rows, insert_id.unwrap_or(0));
            }
            Err(e) => {
                return results.error(
                    msql_srv::ErrorKind::ER_UNKNOWN_ERROR,
                    format!("{:?}", e).as_bytes(),
                );
            }
        }
    }
}

