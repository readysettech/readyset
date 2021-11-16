use nom_sql::{
    Column, ColumnSpecification, Expression, FieldDefinitionExpression, SqlQuery, SqlType,
};
use noria::results::Results;
use noria::{ColumnSchema, DataType, ReadySetResult};
use noria_client::backend::noria_connector::QueryResult;
use noria_client::backend::{noria_connector, SelectSchema};
use noria_client::QueryHandler;
use std::borrow::Cow;
use std::sync::Arc;

const MAX_ALLOWED_PACKET_VARIABLE_NAME: &str = "max_allowed_packet";
const MAX_ALLOWED_PACKET_DEFAULT: DataType = DataType::UnsignedInt(67108864u32);

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
                let field_name = format!("@@{}", MAX_ALLOWED_PACKET_VARIABLE_NAME);
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
                                    function: None,
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
}
