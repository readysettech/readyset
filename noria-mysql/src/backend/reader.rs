use crate::backend::PreparedStatement;
use msql_srv::{self, *};
use nom_sql::Literal;
use noria::DataType;

use std::io;

pub trait Reader<W: io::Write> {
    fn handle_select(
        &mut self,
        q: nom_sql::SelectStatement,
        use_params: Vec<Literal>,
        results: QueryResultWriter<W>,
    ) -> io::Result<()>;

    fn prepare_select(
        &mut self,
        sql_q: nom_sql::SqlQuery,
        info: StatementMetaWriter<W>,
        statement_id: u32,
    ) -> io::Result<PreparedStatement>;

    fn execute_select(
        &mut self,
        qname: &str,
        q: &nom_sql::SelectStatement,
        keys: Vec<Vec<DataType>>,
        schema: &[msql_srv::Column],
        key_column_indices: &[usize],
        results: QueryResultWriter<W>,
    ) -> io::Result<()>;
}
