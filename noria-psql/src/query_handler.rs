use nom_sql::SqlQuery;
use noria::results::Results;
use noria::ReadySetResult;
use noria_client::backend::noria_connector::QueryResult;
use noria_client::backend::{noria_connector, SelectSchema};
use noria_client::QueryHandler;
use std::sync::Arc;

/// PostgreSQL flavor of [`QueryHandler`].
pub struct PostgreSqlQueryHandler;

impl QueryHandler for PostgreSqlQueryHandler {
    fn requires_fallback(_: &SqlQuery) -> bool {
        false
    }

    fn default_response(_: &SqlQuery) -> ReadySetResult<QueryResult> {
        Ok(noria_connector::QueryResult::Select {
            data: vec![Results::new(vec![vec![]], Arc::new([]))],
            select_schema: SelectSchema {
                use_bogo: false,
                schema: vec![],
                columns: vec![],
            },
        })
    }
}
