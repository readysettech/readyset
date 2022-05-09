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

/// The list of mysql `SQL_MODE`s that *must* be set by a client
const REQUIRED_SQL_MODES: [SqlMode; 3] = [
    SqlMode::NoZeroDate,
    SqlMode::NoZeroInDate,
    SqlMode::OnlyFullGroupBy,
];

/// The list of mysql `SQL_MODE`s that *may* be set by a client (because they don't affect query
/// semantics)
const ALLOWED_SQL_MODES: [SqlMode; 11] = [
    SqlMode::ErrorForDivisionByZero, // deprecated
    SqlMode::IgnoreSpace,            // TODO: I think this is fine, but I'm not 100% sure
    SqlMode::NoAutoValueOnZero,
    SqlMode::NoDirInCreate,
    SqlMode::NoEngineSubstitution,
    SqlMode::NoZeroDate,
    SqlMode::NoZeroInDate,
    SqlMode::OnlyFullGroupBy,
    SqlMode::StrictAllTables,
    SqlMode::StrictTransTables,
    SqlMode::TimeTruncateFractional,
];

/// Enum representing the various flags that can be set as part of the MySQL `SQL_MODE` parameter.
/// See [the official mysql documentation][mysql-docs] for more information.
///
/// Note that this enum only includes the SQL mode flags that are present as of mysql 8.0 - any SQL
/// modes that have been removed since earlier versions are omitted here, as they're unsupported
/// regardless.
///
/// [mysql-docs]: https://dev.mysql.com/doc/refman/8.0/en/sql-mode.html
#[derive(PartialEq, Eq, Hash)]
enum SqlMode {
    AllowInvalidDates,
    AnsiQuotes,
    ErrorForDivisionByZero,
    HighNotPrecedence,
    IgnoreSpace,
    NoAutoCreateUser,
    NoAutoValueOnZero,
    NoBackslashEscapes,
    NoDirInCreate,
    NoEngineSubstitution,
    NoUnsignedSubtraction,
    NoZeroDate,
    NoZeroInDate,
    OnlyFullGroupBy,
    PadCharToFullLength,
    PipesAsConcat,
    RealAsFloat,
    StrictAllTables,
    StrictTransTables,
    TimeTruncateFractional,
}

impl FromStr for SqlMode {
    type Err = ReadySetError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match &s.trim().to_ascii_lowercase()[..] {
            "allow_invalid_dates" => Ok(SqlMode::AllowInvalidDates),
            "ansi_quotes" => Ok(SqlMode::AnsiQuotes),
            "error_for_division_by_zero" => Ok(SqlMode::ErrorForDivisionByZero),
            "high_not_precedence" => Ok(SqlMode::HighNotPrecedence),
            "ignore_space" => Ok(SqlMode::IgnoreSpace),
            "no_auto_create_user" => Ok(SqlMode::NoAutoCreateUser),
            "no_auto_value_on_zero" => Ok(SqlMode::NoAutoValueOnZero),
            "no_backslash_escapes" => Ok(SqlMode::NoBackslashEscapes),
            "no_dir_in_create" => Ok(SqlMode::NoDirInCreate),
            "no_engine_substitution" => Ok(SqlMode::NoEngineSubstitution),
            "no_unsigned_subtraction" => Ok(SqlMode::NoUnsignedSubtraction),
            "no_zero_date" => Ok(SqlMode::NoZeroDate),
            "no_zero_in_date" => Ok(SqlMode::NoZeroInDate),
            "only_full_group_by" => Ok(SqlMode::OnlyFullGroupBy),
            "pad_char_to_full_length" => Ok(SqlMode::PadCharToFullLength),
            "pipes_as_concat" => Ok(SqlMode::PipesAsConcat),
            "real_as_float" => Ok(SqlMode::RealAsFloat),
            "strict_all_tables" => Ok(SqlMode::StrictAllTables),
            "strict_trans_tables" => Ok(SqlMode::StrictTransTables),
            "time_truncate_fractional" => Ok(SqlMode::TimeTruncateFractional),
            _ => Err(ReadySetError::SqlModeParseFailed(s.to_string())),
        }
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
                match variable.name.to_ascii_lowercase().as_str() {
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
                                    REQUIRED_SQL_MODES.iter().all(|m| sql_modes.contains(m))
                                        && sql_modes.iter().all(|sql_mode| {
                                            ALLOWED_SQL_MODES.contains(sql_mode)
                                        })
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

#[cfg(test)]
mod tests {
    use nom_sql::{SetStatement, SetVariables, Variable};

    use super::*;

    #[test]
    fn supported_sql_mode() {
        let m = "NO_ZERO_DATE,STRICT_ALL_TABLES,ONLY_FULL_GROUP_BY,NO_ZERO_IN_DATE";
        let stmt = SetStatement::Variable(SetVariables {
            variables: vec![(
                Variable {
                    scope: VariableScope::Session,
                    name: "sql_mode".into(),
                },
                Expression::Literal(Literal::from(m)),
            )],
        });
        assert!(MySqlQueryHandler::is_set_allowed(&stmt));
    }

    #[test]
    fn unsupported_sql_mode() {
        let m = "NO_ZERO_IN_DATE,STRICT_ALL_TABLES,ONLY_FULL_GROUP_BY,NO_ZERO_IN_DATE,ANSI_QUOTES";
        let stmt = SetStatement::Variable(SetVariables {
            variables: vec![(
                Variable {
                    scope: VariableScope::Session,
                    name: "sql_mode".into(),
                },
                Expression::Literal(Literal::from(m)),
            )],
        });
        assert!(!MySqlQueryHandler::is_set_allowed(&stmt));
    }

    #[test]
    fn all_required_sql_modes_are_allowed() {
        for mode in REQUIRED_SQL_MODES {
            assert!(ALLOWED_SQL_MODES.contains(&mode))
        }
    }
}
