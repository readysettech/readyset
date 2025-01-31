use pretty_assertions::Comparison;
use readyset_sql::{
    ast::{CacheInner, SqlQuery},
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
    READYSET,
    SIMPLIFIED,
}

impl ReadysetKeyword {
    fn as_str(&self) -> &str {
        match self {
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
            always,
            concurrently,
        },
    ))
}

fn parse_explain(parser: &mut Parser) -> Result<SqlQuery, ReadysetParsingError> {
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
    Ok(parser
        .parse_explain(sqlparser::ast::DescribeAlias::Explain)?
        .try_into()?)
}

/// Expects `SHOW` was already parsed. Attempts to parse a Readyset-specific SHOW statement,
/// otherwise falls back to [`Parser::parse_show`].
///
/// SHOW READYSET VERSION
fn parse_readyset_show(parser: &mut Parser) -> Result<SqlQuery, ReadysetParsingError> {
    if parse_readyset_keyword(parser, ReadysetKeyword::READYSET)
        && parser.parse_keyword(Keyword::VERSION)
    {
        Ok(SqlQuery::Show(
            readyset_sql::ast::ShowStatement::ReadySetVersion,
        ))
    } else {
        Ok(parser.parse_show()?.try_into()?)
    }
}

fn parse_readyset_query(
    parser: &mut Parser,
    input: impl AsRef<str>,
) -> Result<SqlQuery, ReadysetParsingError> {
    if parser.parse_keywords(&[Keyword::CREATE, Keyword::CACHE]) {
        parse_create_cache(parser, input)
    } else if parser.parse_keyword(Keyword::EXPLAIN) {
        parse_explain(parser)
    } else if parser.parse_keyword(Keyword::SHOW) {
        parse_readyset_show(parser)
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
        .and_then(|mut p| parse_readyset_query(&mut p, input));

    match (&nom_result, sqlparser_result) {
        (Ok(nom_ast), Ok(sqlparser_ast)) => {
            if nom_ast != &sqlparser_ast {
                let comparison = Comparison::new(nom_ast, &sqlparser_ast);
                warn!("nom-sql AST differs from sqlparser-rs AST:\n{comparison}");
                #[cfg(feature = "ast-conversion-errors")]
                pretty_assertions::assert_eq!(nom_ast, &sqlparser_ast);
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
