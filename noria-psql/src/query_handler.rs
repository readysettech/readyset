use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use lazy_static::lazy_static;
use nom_sql::{
    PostgresParameterValue, SetPostgresParameter, SetPostgresParameterValue, SetStatement, SqlQuery,
};
use noria::results::Results;
use noria::ReadySetResult;
use noria_client::backend::noria_connector::QueryResult;
use noria_client::backend::{noria_connector, SelectSchema};
use noria_client::QueryHandler;

lazy_static! {
    static ref ALLOWED_PARAMETERS_ANY_VALUE: HashSet<&'static str> =
        HashSet::from([
            "client_min_messages",

            // This parameter *would* matter, if we supported intervals - once we do we should move
            // this to WITH_VALUE and specify the value for the interval style we actually use
            "intervalstyle",
        ]);
    static ref ALLOWED_PARAMETERS_WITH_VALUE: HashMap<&'static str, PostgresParameterValue> =
        HashMap::from([
            (
                "client_encoding",
                PostgresParameterValue::literal("utf-8")
            ),
            ("timezone", PostgresParameterValue::literal("UTC")),
        ]);
}

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

    fn is_set_allowed(stmt: &SetStatement) -> bool {
        match stmt {
            SetStatement::PostgresParameter(SetPostgresParameter { name, .. })
                if ALLOWED_PARAMETERS_ANY_VALUE.contains(name.to_ascii_lowercase().as_str()) =>
            {
                true
            }
            SetStatement::PostgresParameter(SetPostgresParameter { name, value, .. }) => {
                if let Some(allowed_value) = ALLOWED_PARAMETERS_WITH_VALUE.get(name.as_str()) {
                    *value == SetPostgresParameterValue::Default
                        || matches!(value, SetPostgresParameterValue::Value(val) if val == allowed_value)
                } else {
                    false
                }
            }
            _ => false,
        }
    }
}
