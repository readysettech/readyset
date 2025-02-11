use readyset_sql::{ast::SqlQuery, Dialect};

#[cfg(feature = "sqlparser")]
use readyset_sql::ast::CacheInner;
#[cfg(feature = "sqlparser")]
use sqlparser::{keywords::Keyword, parser::Parser};

#[cfg(feature = "sqlparser")]
#[derive(Debug, thiserror::Error)]
pub enum ReadysetParsingError {
    #[error("sqlparser error: {0}")]
    SqlparserError(#[from] sqlparser::parser::ParserError),
    #[error("AST conversion error: {0}")]
    AstConversionError(#[from] readyset_sql::AstConversionError),
}

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
fn parse_create_cache(
    parser: &mut Parser,
    input: impl AsRef<str>,
) -> Result<SqlQuery, ReadysetParsingError> {
    let name = parser.parse_object_name(false).ok().map(Into::into);
    parser.expect_keyword(Keyword::FROM)?;
    let query = parser
        .parse_statement()
        .map_err(|e| format!("failed to parse statement: {e}"))
        .and_then(|q| {
            q.try_into()
                .map_err(|e| format!("failed to convert AST: {e}"))
        })
        .and_then(|q: SqlQuery| q.into_select().ok_or_else(|| "expected SELECT".into()))
        .map(|q| CacheInner::Statement(Box::new(q)));
    Ok(SqlQuery::CreateCache(
        readyset_sql::ast::CreateCacheStatement {
            name,
            inner: query,
            unparsed_create_cache_statement: Some(input.as_ref().to_string()),
            always: false,
            concurrently: false,
        },
    ))
}

#[cfg(feature = "sqlparser")]
fn parse_readyset_query(
    parser: &mut Parser,
    input: impl AsRef<str>,
) -> Result<SqlQuery, ReadysetParsingError> {
    if parser.parse_keywords(&[Keyword::CREATE, Keyword::CACHE]) {
        parse_create_cache(parser, input)
    } else {
        Ok(parser.parse_statement()?.try_into()?)
    }
}

#[cfg(feature = "sqlparser")]
pub fn parse_query(dialect: Dialect, input: impl AsRef<str>) -> Result<SqlQuery, String> {
    let nom_result = nom_sql::parse_query(dialect, input.as_ref());
    let sqlparser_dialect = sqlparser_dialect_from_readyset_dialect(dialect);
    let sqlparser_result = Parser::new(sqlparser_dialect.as_ref())
        .try_with_sql(input.as_ref())
        .map_err(Into::into)
        .and_then(|mut p| parse_readyset_query(&mut p, input.as_ref()));

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
