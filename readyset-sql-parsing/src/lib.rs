use pretty_assertions::Comparison;
use readyset_sql::{
    ast::{CacheInner, DropCacheStatement, SqlQuery},
    Dialect,
};
use sqlparser::{
    keywords::Keyword,
    parser::Parser,
    tokenizer::{Token, TokenWithSpan, Word},
};
use tracing::warn;

#[derive(Debug, thiserror::Error)]
pub enum ReadysetParsingError {
    #[error("sqlparser error: {0}")]
    SqlparserError(#[from] sqlparser::parser::ParserError),
    #[error("AST conversion error: {0}")]
    AstConversionError(#[from] readyset_sql::AstConversionError),
    #[error("readyset parsing error: {0}")]
    ReadysetParsingError(String),
}

fn sqlparser_dialect_from_readyset_dialect(
    dialect: Dialect,
) -> Box<dyn sqlparser::dialect::Dialect> {
    match dialect {
        Dialect::PostgreSQL => Box::new(sqlparser::dialect::PostgreSqlDialect {}),
        Dialect::MySQL => Box::new(sqlparser::dialect::MySqlDialect {}),
    }
}

#[expect(clippy::upper_case_acronyms, reason = "SQL keywords are capitalized")]
enum ReadysetKeyword {
    CACHES,
    MIGRATION,
    READYSET,
    SIMPLIFIED,
}

impl ReadysetKeyword {
    fn as_str(&self) -> &str {
        match self {
            Self::CACHES => "CACHES",
            Self::MIGRATION => "MIGRATION",
            Self::READYSET => "READYSET",
            Self::SIMPLIFIED => "SIMPLIFIED",
        }
    }
}

/// Returns whether the next token matches the given keyword. Works similarly to
/// [`Parser::parse_keyword`], but allows for keywords that don't exist in [`Keyword`]: if the next
/// token matches, it is consumed and `true` is returned; otherwise, it is not consumed and `false`
/// is returned.
fn parse_readyset_keyword(parser: &mut Parser, keyword: ReadysetKeyword) -> bool {
    match parser.peek_token_ref() {
        TokenWithSpan {
            token: Token::Word(Word { value, .. }),
            ..
        } if value.eq_ignore_ascii_case(keyword.as_str()) => {
            parser.advance_token();
            true
        }
        _ => false,
    }
}

/// Expects `CREATE CACHE` was already parsed. Attempts to parse a Readyset-specific create cache
/// statement. Will simply error if it fails, since there's no relevant standard SQL to fall back to.
///
/// CREATE CACHE [ALWAYS] [CONCURRENTLY] <name> FROM <SELECT statement>
///
/// TODO: support query id instead of select statement
fn parse_create_cache(
    parser: &mut Parser,
    input: impl AsRef<str>,
) -> Result<SqlQuery, ReadysetParsingError> {
    let mut always = false;
    let mut concurrently = false;
    match parser.parse_one_of_keywords(&[Keyword::ALWAYS, Keyword::CONCURRENTLY]) {
        Some(Keyword::ALWAYS) => {
            always = true;
            concurrently = parser.parse_keyword(Keyword::CONCURRENTLY);
        }
        Some(Keyword::CONCURRENTLY) => {
            concurrently = true;
            always = parser.parse_keyword(Keyword::ALWAYS);
        }
        _ => {}
    }
    let from = parser.parse_keyword(Keyword::FROM);
    let name = if !from {
        let name = parser.parse_object_name(false).ok().map(Into::into);
        parser.expect_keyword(Keyword::FROM)?;
        name
    } else {
        None
    };
    let query = parse_query_for_create_cache(parser);
    Ok(SqlQuery::CreateCache(
        readyset_sql::ast::CreateCacheStatement {
            name,
            inner: query,
            unparsed_create_cache_statement: Some(input.as_ref().to_string()),
            always,
            concurrently,
        },
    ))
}

fn parse_query_for_create_cache(parser: &mut Parser) -> Result<CacheInner, String> {
    parser
        .parse_statement()
        .map_err(|e| format!("failed to parse statement: {e}"))
        .and_then(|q| {
            q.try_into()
                .map_err(|e| format!("failed to convert AST: {e}"))
        })
        .and_then(|q: SqlQuery| q.into_select().ok_or_else(|| "expected SELECT".into()))
        .map(|q| CacheInner::Statement(Box::new(q)))
}

fn parse_explain(
    parser: &mut Parser,
    input: impl AsRef<str>,
) -> Result<SqlQuery, ReadysetParsingError> {
    if parser.parse_keywords(&[Keyword::LAST, Keyword::STATEMENT]) {
        return Ok(SqlQuery::Explain(
            readyset_sql::ast::ExplainStatement::LastStatement,
        ));
    }
    let simplified = parse_readyset_keyword(parser, ReadysetKeyword::SIMPLIFIED);
    if parser.parse_keyword(Keyword::GRAPHVIZ) {
        let for_cache = if parser.parse_keywords(&[Keyword::FOR, Keyword::CACHE]) {
            Some(parser.parse_object_name(false)?.into())
        } else {
            None
        };
        return Ok(SqlQuery::Explain(
            readyset_sql::ast::ExplainStatement::Graphviz {
                simplified,
                for_cache,
            },
        ));
    }
    if parser.parse_keywords(&[Keyword::CREATE, Keyword::CACHE, Keyword::FROM]) {
        return Ok(SqlQuery::Explain(
            readyset_sql::ast::ExplainStatement::CreateCache {
                inner: parse_query_for_create_cache(parser),
                unparsed_explain_create_cache_statement: input
                    .as_ref()
                    .strip_prefix("EXPLAIN ")
                    .unwrap()
                    .to_string(),
            },
        ));
    }
    Ok(parser
        .parse_explain(sqlparser::ast::DescribeAlias::Explain)?
        .try_into()?)
}

/// Expects `SHOW` was already parsed. Attempts to parse a Readyset-specific SHOW statement,
/// otherwise falls back to [`Parser::parse_show`].
///
/// SHOW READYSET VERSION
fn parse_readyset_show(parser: &mut Parser) -> Result<SqlQuery, ReadysetParsingError> {
    if parse_readyset_keyword(parser, ReadysetKeyword::READYSET) {
        if parser.parse_keyword(Keyword::VERSION) {
            Ok(SqlQuery::Show(
                readyset_sql::ast::ShowStatement::ReadySetVersion,
            ))
        } else if parse_readyset_keyword(parser, ReadysetKeyword::MIGRATION)
            && parser.parse_keyword(Keyword::STATUS)
        {
            let id = parser.parse_literal_uint()?;
            Ok(SqlQuery::Show(
                readyset_sql::ast::ShowStatement::ReadySetMigrationStatus(id),
            ))
        } else {
            Err(ReadysetParsingError::ReadysetParsingError(
                "expected SHOW READYSET VERSION or SHOW READYSET MIGRATION STATUS".into(),
            ))
        }
    } else {
        Ok(parser.parse_show()?.try_into()?)
    }
}

/// Expects `DROP` was already parsed. Attempts to parse a Readyset-specific drop statement,
/// otherwise falls back to [`Parser::parse_drop`].
fn parse_readyset_drop(parser: &mut Parser) -> Result<SqlQuery, ReadysetParsingError> {
    if parser.parse_keyword(Keyword::ALL) && parse_readyset_keyword(parser, ReadysetKeyword::CACHES)
    {
        Ok(SqlQuery::DropAllCaches(
            readyset_sql::ast::DropAllCachesStatement {},
        ))
    } else if parser.parse_keyword(Keyword::CACHE) {
        let name = parser.parse_object_name(false)?.into();
        Ok(SqlQuery::DropCache(DropCacheStatement { name }))
    } else {
        Ok(parser.parse_drop()?.try_into()?)
    }
}

/// Attempts to parse a Readyset-specific statement, and falls back to [`Parser::parse_statement`] if it fails.
fn parse_readyset_query(
    parser: &mut Parser,
    input: impl AsRef<str>,
) -> Result<SqlQuery, ReadysetParsingError> {
    if parser.parse_keywords(&[Keyword::CREATE, Keyword::CACHE]) {
        parse_create_cache(parser, input)
    } else if parser.parse_keyword(Keyword::EXPLAIN) {
        parse_explain(parser, input)
    } else if parser.parse_keyword(Keyword::SHOW) {
        parse_readyset_show(parser)
    } else if parser.parse_keyword(Keyword::DROP) {
        parse_readyset_drop(parser)
    } else {
        Ok(parser.parse_statement()?.try_into()?)
    }
}

/// Parse SQL using the specified parser implementation and dialect
pub fn parse_query(dialect: Dialect, input: impl AsRef<str>) -> Result<SqlQuery, String> {
    let nom_result = nom_sql::parse_query(dialect, input.as_ref());
    let sqlparser_dialect = sqlparser_dialect_from_readyset_dialect(dialect);
    let sqlparser_result = Parser::new(sqlparser_dialect.as_ref())
        .try_with_sql(input.as_ref())
        .map_err(Into::into)
        .and_then(|mut p| parse_readyset_query(&mut p, input.as_ref()));

    match (&nom_result, sqlparser_result) {
        (Ok(nom_ast), Ok(sqlparser_ast)) => {
            if nom_ast != &sqlparser_ast {
                let comparison = Comparison::new(nom_ast, &sqlparser_ast);
                warn!("nom-sql AST differs from sqlparser-rs AST:\n{comparison}");
                #[cfg(feature = "ast-conversion-errors")]
                pretty_assertions::assert_eq!(
                    nom_ast,
                    &sqlparser_ast,
                    "input: {:?}",
                    input.as_ref()
                );
            }
        }
        (Ok(nom_ast), Err(sqlparser_error)) => {
            warn!(%sqlparser_error, ?nom_ast, "nom-sql succeeded but sqlparser-rs failed");
            #[cfg(feature = "ast-conversion-errors")]
            panic!("nom-sql succeeded but sqlparser-rs failed: {sqlparser_error}")
        }
        (Err(nom_error), Ok(sqlparser_ast)) => {
            warn!(%nom_error, ?sqlparser_ast, "sqlparser-rs succeeded but nom-sql failed")
        }
        (Err(nom_error), Err(sqlparser_error)) => {
            warn!(%nom_error, %sqlparser_error, "both nom-sql and sqlparser-rs failed");
        }
    }
    nom_result
}
