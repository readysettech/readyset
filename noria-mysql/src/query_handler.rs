use std::borrow::Cow;
use std::str::FromStr;
use std::sync::Arc;

use nom_sql::{
    Column, ColumnSpecification, Expression, FieldDefinitionExpression, Literal, SqlIdentifier,
    SqlQuery, SqlType, VariableScope,
};
use noria::results::Results;
use noria::{ColumnSchema, ReadySetError};
use noria_client::backend::noria_connector::QueryResult;
use noria_client::backend::{noria_connector, SelectSchema};
use noria_client::QueryHandler;
use noria_data::DataType;
use noria_errors::ReadySetResult;
use tracing::warn;

const MAX_ALLOWED_PACKET_VARIABLE_NAME: &str = "max_allowed_packet";
const MAX_ALLOWED_PACKET_DEFAULT: DataType = DataType::UnsignedInt(67108864);

const ALLOWED_SQL_MODES: [SqlMode; 7] = [
    SqlMode::OnlyFullGroupBy,
    SqlMode::StrictTransTables,
    SqlMode::NoZeroInDate,
    SqlMode::NoZeroDate,
    SqlMode::ErrorForDivisionByZero,
    SqlMode::NoAutoCreateUser,
    SqlMode::NoEngineSubstitution,
];

// SqlMode holds the current list of known sql modes that we care to deal with.
// TODO(peter): expand this later to include ALL sql modes.
#[derive(PartialEq, Eq, Hash)]
enum SqlMode {
    OnlyFullGroupBy,
    StrictTransTables,
    NoZeroInDate,
    NoZeroDate,
    ErrorForDivisionByZero,
    NoAutoCreateUser,
    NoEngineSubstitution,
}

// TODO(vlad): replace with strum
impl FromStr for SqlMode {
    type Err = ReadySetError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let res = match &s.trim().to_ascii_lowercase()[..] {
            "only_full_group_by" => SqlMode::OnlyFullGroupBy,
            "strict_trans_tables" => SqlMode::StrictTransTables,
            "no_zero_in_date" => SqlMode::NoZeroInDate,
            "no_zero_date" => SqlMode::NoZeroDate,
            "error_for_division_by_zero" => SqlMode::ErrorForDivisionByZero,
            "no_auto_create_user" => SqlMode::NoAutoCreateUser,
            "no_engine_substitution" => SqlMode::NoEngineSubstitution,
            _ => {
                return Err(ReadySetError::SqlModeParseFailed(s.to_string()));
            }
        };
        Ok(res)
    }
}

fn raw_sql_modes_to_list(sql_modes: &str) -> Result<Vec<SqlMode>, ReadySetError> {
    sql_modes
        .split(',')
        .into_iter()
        .map(SqlMode::from_str)
        .collect::<Result<Vec<SqlMode>, ReadySetError>>()
}

/// MySQL flavor of [`QueryHandler`].
pub struct MySqlQueryHandler;

impl QueryHandler for MySqlQueryHandler {
    fn requires_fallback(query: &SqlQuery) -> bool {
        // Currently any query with variables requires a fallback
        match query {
            SqlQuery::Select(stmt) => stmt.fields.iter().any(|field| match field {
                FieldDefinitionExpression::Expression { expr, .. } => expr.contains_vars(),
                _ => false,
            }),
            _ => false,
        }
    }

    fn default_response(query: &SqlQuery) -> ReadySetResult<QueryResult<'static>> {
        // For now we only care if we are querying for the `@@max_allowed_packet`
        // (ignoring any other field), in which case we return a hardcoded result.
        // This hardcoded result is needed because some libraries expect it when
        // creating a new MySQL connection (i.e., when using `[mysql::Conn::new]`).
        // No matter how many fields appeared in the query, we only return the mentioned
        // hardcoded value.
        // If `@@max_allowed_packet` was not present in the fields, we return an empty set
        // of rows.
        let (data, schema) = match query {
            SqlQuery::Select(stmt)
                if stmt.fields.iter().any(|field| {
                    matches!(field, FieldDefinitionExpression::Expression {
                        expr: Expression::Variable(var),
                        ..
                    } if var.as_non_user_var() == Some(MAX_ALLOWED_PACKET_VARIABLE_NAME))
                }) =>
            {
                let field_name: SqlIdentifier =
                    format!("@@{}", MAX_ALLOWED_PACKET_VARIABLE_NAME).into();
                (
                    vec![Results::new(
                        vec![vec![MAX_ALLOWED_PACKET_DEFAULT]],
                        Arc::new([field_name.clone()]),
                    )],
                    SelectSchema {
                        use_bogo: false,
                        schema: Cow::Owned(vec![ColumnSchema {
                            spec: ColumnSpecification {
                                sql_type: SqlType::UnsignedInt(Some(8)),
                                constraints: Vec::new(),
                                column: Column {
                                    name: field_name.clone(),
                                    table: None,
                                },
                                comment: None,
                            },
                            base: None,
                        }]),
                        columns: Cow::Owned(vec![field_name]),
                    },
                )
            }
            _ => (
                vec![Results::new(vec![vec![]], Arc::new([]))],
                SelectSchema {
                    use_bogo: false,
                    schema: Cow::Owned(vec![]),
                    columns: Cow::Owned(vec![]),
                },
            ),
        };
        Ok(noria_connector::QueryResult::Select {
            data,
            select_schema: schema,
        })
    }

    fn is_set_allowed(stmt: &nom_sql::SetStatement) -> bool {
        match stmt {
            nom_sql::SetStatement::Variable(set) => set.variables.iter().all(|(variable, value)| {
                if variable.scope == VariableScope::User {
                    return false;
                }
                match variable.name.as_str() {
                    "time_zone" => {
                        matches!(value, Expression::Literal(Literal::String(ref s)) if s == "+00:00")
                    }
                    "autocommit" => {
                        matches!(value, Expression::Literal(Literal::Integer(i)) if *i == 1)
                    }
                    "sql_mode" => {
                        if let Expression::Literal(Literal::String(ref s)) = value {
                            match raw_sql_modes_to_list(&s[..]) {
                                Ok(sql_modes) => {
                                    sql_modes.iter().all(|sql_mode| ALLOWED_SQL_MODES.contains(sql_mode))
                                }
                                Err(e) => {
                                    warn!(%e, "unknown sql modes in set");
                                    false
                                }
                            }
                        } else {
                            false
                        }
                    }
                    "names" => {
                        if let Expression::Literal(Literal::String(ref s)) = value {
                            matches!(&s[..], "latin1" | "utf8" | "utf8mb4")
                        } else {
                            false
                        }
                    }
                    "foreign_key_checks" => true,
                    _ => false,
                }
            }),
            nom_sql::SetStatement::Names(names) => {
                names.collation.is_none()
                    && matches!(&names.charset[..], "latin1" | "utf8" | "utf8mb4")
            }
            nom_sql::SetStatement::PostgresParameter(_) => false,
        }
    }
}
