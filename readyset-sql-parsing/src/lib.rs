use readyset_sql::{ast::SqlQuery, Dialect};

#[cfg(feature = "sqlparser")]
fn sqlparser_dialect_from_readyset_dialect(
    dialect: Dialect,
) -> Box<dyn sqlparser::dialect::Dialect> {
    match dialect {
        Dialect::PostgreSQL => Box::new(sqlparser::dialect::PostgreSqlDialect {}),
        Dialect::MySQL => Box::new(sqlparser::dialect::MySqlDialect {}),
    }
}

#[cfg(feature = "sqlparser")]
pub fn parse_query(dialect: Dialect, input: impl AsRef<str>) -> Result<SqlQuery, String> {
    let nom_result = nom_sql::parse_query(dialect, input.as_ref());
    let sqlparser_dialect = sqlparser_dialect_from_readyset_dialect(dialect);
    let sqlparser_result: Result<SqlQuery, _> =
        sqlparser::parser::Parser::new(sqlparser_dialect.as_ref())
            .try_with_sql(input.as_ref())
            .and_then(|mut p| p.parse_statement())
            .map_err(|e| format!("failed to parse: {e}"))
            .and_then(|q| {
                q.try_into()
                    .map_err(|e| format!("failed to convert AST: {e}"))
            });
    match (&nom_result, sqlparser_result) {
        (Ok(nom_ast), Ok(sqlparser_ast)) => {
            pretty_assertions::assert_eq!(
                nom_ast,
                &sqlparser_ast,
                "nom-sql AST differs from sqlparser-rs AST. input: {:?}",
                input.as_ref()
            );
        }
        (Ok(nom_ast), Err(sqlparser_error)) => {
            panic!(
                "nom-sql succeeded but sqlparser-rs failed: {}\ninput: {}\nnom_ast: {:?}",
                sqlparser_error,
                input.as_ref(),
                nom_ast
            )
        }
        (Err(nom_error), Ok(sqlparser_ast)) => {
            tracing::warn!(%nom_error, ?sqlparser_ast, "sqlparser-rs succeeded but nom-sql failed")
        }
        (Err(nom_error), Err(sqlparser_error)) => {
            tracing::warn!(%nom_error, %sqlparser_error, "both nom-sql and sqlparser-rs failed");
        }
    }
    nom_result
}

#[cfg(not(feature = "sqlparser"))]
pub fn parse_query(dialect: Dialect, input: impl AsRef<str>) -> Result<SqlQuery, String> {
    nom_sql::parse_query(dialect, input.as_ref())
}
