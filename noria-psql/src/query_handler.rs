use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use lazy_static::lazy_static;
use nom_sql::{
    Literal, PostgresParameterValue, PostgresParameterValueInner, SetNames, SetPostgresParameter,
    SetPostgresParameterValue, SetStatement, SqlQuery,
};
use noria::results::Results;
use noria::ReadySetResult;
use noria_client::backend::noria_connector::QueryResult;
use noria_client::backend::{noria_connector, SelectSchema};
use noria_client::QueryHandler;

enum AllowedParameterValue {
    Literal(PostgresParameterValue),
    OneOf(HashSet<PostgresParameterValue>),
    Predicate(fn(&PostgresParameterValue) -> bool),
}

impl AllowedParameterValue {
    fn literal<L>(lit: L) -> Self
    where
        Literal: From<L>,
    {
        Self::Literal(PostgresParameterValue::literal(lit))
    }

    fn one_of<I, T>(allowed: I) -> Self
    where
        I: IntoIterator<Item = T>,
        PostgresParameterValue: From<T>,
    {
        Self::OneOf(allowed.into_iter().map(|v| v.into()).collect())
    }

    fn set_value_is_allowed(&self, value: &SetPostgresParameterValue) -> bool {
        match value {
            SetPostgresParameterValue::Default => true,
            SetPostgresParameterValue::Value(val) => match self {
                AllowedParameterValue::Literal(l) => val == l,
                AllowedParameterValue::OneOf(m) => m.contains(val),
                AllowedParameterValue::Predicate(p) => p(val),
            },
        }
    }
}

fn search_path_includes_public(val: &PostgresParameterValue) -> bool {
    match val {
        PostgresParameterValue::Single(PostgresParameterValueInner::Literal(Literal::String(
            s,
        ))) => s == "public",
        PostgresParameterValue::List(vals) => vals.first().iter().all(|v| {
            matches!(
                v,
                PostgresParameterValueInner::Literal(Literal::String(s)) if s == "public"
            )
        }),
        _ => false,
    }
}

lazy_static! {
    static ref ALLOWED_PARAMETERS_ANY_VALUE: HashSet<&'static str> =
        HashSet::from([
            "client_min_messages",

            // This parameter *would* matter, if we supported intervals - once we do we should move
            // this to WITH_VALUE and specify the value for the interval style we actually use
            "intervalstyle",

            // Similarly this parameter *would* matter if we supported arrays, and once we do we
            // should move this to WITH_VALUE
            "array_nulls",
        ]);

    static ref ALLOWED_PARAMETERS_WITH_VALUE: HashMap<&'static str, AllowedParameterValue> =
        HashMap::from([
            (
                "client_encoding",
                AllowedParameterValue::literal("utf-8")
            ),
            ("timezone", AllowedParameterValue::literal("UTC")),
            ("datestyle", AllowedParameterValue::literal("ISO")),
            ("extra_float_digits", AllowedParameterValue::literal(1)),
            ("TimeZone",  AllowedParameterValue::literal("Etc/UTC")),
            ("bytea_output",  AllowedParameterValue::literal("hex")),
            ("search_path", AllowedParameterValue::Predicate(search_path_includes_public)),
            ("transform_null_equals", AllowedParameterValue::literal(false)),
            ("backslash_quote", AllowedParameterValue::one_of([
                PostgresParameterValue::identifier("safe_encoding"),
                PostgresParameterValue::literal("safe_encoding"),
            ])),
            ("standard_conforming_strings", AllowedParameterValue::literal(true)),
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
                    allowed_value.set_value_is_allowed(value)
                } else {
                    false
                }
            }
            SetStatement::Names(SetNames { charset, .. }) => charset.to_lowercase() == "utf8",
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn search_path_with_public_is_allowed() {
        let stmt = SetStatement::PostgresParameter(SetPostgresParameter {
            scope: None,
            name: "search_path".into(),
            value: SetPostgresParameterValue::Value(PostgresParameterValue::list([
                "public", "other",
            ])),
        });
        assert!(PostgreSqlQueryHandler::is_set_allowed(&stmt));
    }

    #[test]
    fn search_path_not_starting_with_public_isnt_allowed() {
        let stmt = SetStatement::PostgresParameter(SetPostgresParameter {
            scope: None,
            name: "search_path".into(),
            value: SetPostgresParameterValue::Value(PostgresParameterValue::list([
                "other", "public",
            ])),
        });
        assert!(!PostgreSqlQueryHandler::is_set_allowed(&stmt));
    }
}
