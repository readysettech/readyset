use std::borrow::Cow;
use std::sync::Arc;

use nom_sql::SqlQuery;
use noria::results::Results;
use noria::ReadySetResult;
use noria_client::backend::noria_connector::QueryResult;
use noria_client::backend::{noria_connector, SelectSchema};
use noria_client::QueryHandler;

/// PostgreSQL flavor of [`QueryHandler`].
pub struct PostgreSqlQueryHandler;

impl QueryHandler for PostgreSqlQueryHandler {
    fn requires_fallback(_: &SqlQuery) -> bool {
        false
    }

    fn default_response(_: &SqlQuery) -> ReadySetResult<QueryResult<'static>> {
        Ok(noria_connector::QueryResult::Select {
            data: vec![Results::new(vec![vec![]], Arc::new([]))],
            select_schema: SelectSchema {
                use_bogo: false,
                schema: Cow::Owned(vec![]),
                columns: Cow::Owned(vec![]),
            },
        })
    }

    fn is_set_allowed(_: &nom_sql::SetStatement) -> bool {
        false
    }
}
