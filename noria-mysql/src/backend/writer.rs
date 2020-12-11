use crate::backend::PreparedStatement;
use msql_srv::{self, *};
use nom_sql::{InsertStatement, UpdateStatement};
use noria::DataType;

use std::io;

pub trait Writer<W: io::Write> {
    fn handle_set(
        &mut self,
        q: nom_sql::SetStatement,
        results: QueryResultWriter<W>,
    ) -> io::Result<()>;

    fn handle_insert(
        &mut self,
        q: nom_sql::InsertStatement,
        results: QueryResultWriter<W>,
    ) -> io::Result<()>;

    fn prepare_insert(
        &mut self,
        sql_q: nom_sql::SqlQuery,
        info: StatementMetaWriter<W>,
        statement_id: u32,
    ) -> io::Result<PreparedStatement>;

    fn execute_insert(
        &mut self,
        q: &InsertStatement,
        data: Vec<DataType>,
        results: QueryResultWriter<W>,
    ) -> io::Result<()>;

    fn handle_delete(
        &mut self,
        q: nom_sql::DeleteStatement,
        results: QueryResultWriter<W>,
    ) -> io::Result<()>;

    fn handle_update(
        &mut self,
        q: nom_sql::UpdateStatement,
        results: QueryResultWriter<W>,
    ) -> io::Result<()>;

    fn prepare_update(
        &mut self,
        sql_q: nom_sql::SqlQuery,
        info: StatementMetaWriter<W>,
        statement_id: u32,
    ) -> io::Result<PreparedStatement>;

    fn execute_update(
        &mut self,
        q: &UpdateStatement,
        params: ParamParser,
        results: QueryResultWriter<W>,
    ) -> io::Result<()>;

    fn handle_create_table(
        &mut self,
        q: nom_sql::CreateTableStatement,
        results: QueryResultWriter<W>,
    ) -> io::Result<()>;

    fn handle_create_view(
        &mut self,
        q: nom_sql::CreateViewStatement,
        results: QueryResultWriter<W>,
    ) -> io::Result<()>;
}
