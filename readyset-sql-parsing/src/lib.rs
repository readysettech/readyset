use readyset_sql::ast::{
    AlterTableStatement, CreateTableStatement, CreateViewStatement, Expr, SelectStatement,
    SqlQuery, SqlType, TableKey,
};
use readyset_sql::Dialect;

#[cfg(feature = "sqlparser")]
use readyset_sql::ast::{
    AddTablesStatement, AlterReadysetStatement, CacheInner, DropCacheStatement,
    ResnapshotTableStatement,
};
#[cfg(feature = "sqlparser")]
use readyset_sql::{IntoDialect, TryIntoDialect};
#[cfg(feature = "sqlparser")]
use sqlparser::{
    keywords::Keyword,
    parser::Parser,
    tokenizer::{Token, TokenWithSpan, Word},
};

#[cfg(feature = "sqlparser")]
#[derive(Debug, thiserror::Error)]
pub enum ReadysetParsingError {
    #[error("sqlparser error: {0}")]
    SqlparserError(#[from] sqlparser::parser::ParserError),
    #[error("AST conversion error: {0}")]
    AstConversionError(#[from] readyset_sql::AstConversionError),
    #[error("readyset parsing error: {0}")]
    ReadysetParsingError(String),
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
#[expect(clippy::upper_case_acronyms, reason = "SQL keywords are capitalized")]
#[derive(Debug, Clone, Copy)]
enum ReadysetKeyword {
    CACHED,
    CACHES,
    ENTER,
    EXIT,
    MAINTENANCE,
    MIGRATION,
    PROXIED,
    QUERIES,
    READYSET,
    RESNAPSHOT,
    SIMPLIFIED,
    /// To match both Readyset and sqlparser keywords in one go, we want to be able to accept both
    /// in the same function. So here we just allow falling back to a sqlparser keyword.
    Standard(sqlparser::keywords::Keyword),
}

#[cfg(feature = "sqlparser")]
impl ReadysetKeyword {
    fn as_str(&self) -> &str {
        match self {
            Self::CACHED => "CACHED",
            Self::CACHES => "CACHES",
            Self::ENTER => "ENTER",
            Self::EXIT => "EXIT",
            Self::MAINTENANCE => "MAINTENANCE",
            Self::MIGRATION => "MIGRATION",
            Self::PROXIED => "PROXIED",
            Self::QUERIES => "QUERIES",
            Self::READYSET => "READYSET",
            Self::RESNAPSHOT => "RESNAPSHOT",
            Self::SIMPLIFIED => "SIMPLIFIED",
            Self::Standard(_) => panic!(
                "Standard sqlparser keywords should only be used with `parse_keyword`, not string comparison"
            ),
        }
    }
}

/// Returns whether the next token matches the given keyword. Works similarly to
/// [`Parser::parse_keyword`], but allows for keywords that don't exist in [`Keyword`]: if the next
/// token matches, it is consumed and `true` is returned; otherwise, it is not consumed and `false`
/// is returned.
#[cfg(feature = "sqlparser")]
fn parse_readyset_keyword(parser: &mut Parser, rs_keyword: ReadysetKeyword) -> bool {
    if let ReadysetKeyword::Standard(keyword) = rs_keyword {
        parser.parse_keyword(keyword)
    } else if let TokenWithSpan {
        token: Token::Word(Word { value, .. }),
        ..
    } = parser.peek_token_ref()
    {
        if value.eq_ignore_ascii_case(rs_keyword.as_str()) {
            parser.advance_token();
            true
        } else {
            false
        }
    } else {
        false
    }
}

/// Returns whether all Readyset keywords have been consumed. The point of this is to atomically
/// consume all keywords or none of them, so that subsequent parsing can try matching the first
/// token again.
///
/// This is similar to [`Parser::parse_keywords`], but their implementation relies on access to the
/// private `index` field. Our implementation is a little clunky because it replicates that behavior
/// but without being able to set `index` directly: instead, we just repeatedly set the token to the
/// previous one until it's back where we started.
#[cfg(feature = "sqlparser")]
fn parse_readyset_keywords(parser: &mut Parser, keywords: &[ReadysetKeyword]) -> bool {
    // XXX(mvzink): Ideally, we would mirror the rewinding logic in [`Parser::parse_keywords`] and
    // simply record and reset the [`Parser::index`] field. Since it's private, and the vec of
    // tokens it points into includes whitespace tokens which the token accessors skip over, we just
    // count and rewind however many tokens we parsed. This assumes the forward/backward accessors
    // (`advance_token` and `prev_token`) have symmetrical logic for skipping whitespace tokens.
    let mut num_tokens_parsed = 0;

    // Try to match all keywords in sequence
    for &keyword in keywords {
        if !parse_readyset_keyword(parser, keyword) {
            // Reset position
            while num_tokens_parsed > 0 {
                parser.prev_token();
                num_tokens_parsed -= 1;
            }
            return false;
        } else {
            num_tokens_parsed += 1;
        }
    }

    true
}

/// Parse a ReadySet `ALTER` statement, or fall back to [`Parser::parse_alter`].
///
/// ALTER READYSET
///     | ADD TABLES
///     | RESNAPSHOT TABLE
///     | {ENTER | EXIT} MAINTENANCE MODE
#[cfg(feature = "sqlparser")]
fn parse_alter(parser: &mut Parser, dialect: Dialect) -> Result<SqlQuery, ReadysetParsingError> {
    if parse_readyset_keyword(parser, ReadysetKeyword::READYSET) {
        if parse_readyset_keywords(
            parser,
            &[
                ReadysetKeyword::RESNAPSHOT,
                ReadysetKeyword::Standard(Keyword::TABLE),
            ],
        ) {
            let table_name = parser.parse_object_name(false)?.into_dialect(dialect);
            Ok(SqlQuery::AlterReadySet(
                AlterReadysetStatement::ResnapshotTable(ResnapshotTableStatement {
                    table: table_name,
                }),
            ))
        } else if parser.parse_keywords(&[Keyword::ADD, Keyword::TABLES]) {
            let tables = parser
                .parse_comma_separated(|p| p.parse_object_name(false))?
                .into_iter()
                .map(|table| table.into_dialect(dialect))
                .collect();
            Ok(SqlQuery::AlterReadySet(AlterReadysetStatement::AddTables(
                AddTablesStatement { tables },
            )))
        } else if parse_readyset_keywords(
            parser,
            &[
                ReadysetKeyword::ENTER,
                ReadysetKeyword::MAINTENANCE,
                ReadysetKeyword::Standard(Keyword::MODE),
            ],
        ) {
            Ok(SqlQuery::AlterReadySet(
                AlterReadysetStatement::EnterMaintenanceMode,
            ))
        } else if parse_readyset_keywords(
            parser,
            &[
                ReadysetKeyword::EXIT,
                ReadysetKeyword::MAINTENANCE,
                ReadysetKeyword::Standard(Keyword::MODE),
            ],
        ) {
            Ok(SqlQuery::AlterReadySet(
                AlterReadysetStatement::ExitMaintenanceMode,
            ))
        } else {
            Err(ReadysetParsingError::ReadysetParsingError(
                "expected RESNAPSHOT TABLE, or ADD TABLES after READYSET".into(),
            ))
        }
    } else {
        Ok(parser.parse_alter()?.try_into_dialect(dialect)?)
    }
}

/// Expects `CREATE CACHE` was already parsed. Attempts to parse a Readyset-specific create cache
/// statement. Will simply error if it fails, since there's no relevant standard SQL to fall back to.
///
/// CREATE CACHE
///     [cache_option [, cache_option] ...]
///     [<name>]
///     FROM
///     <SELECT statement>
///
/// cache_options:
///     | ALWAYS
///     | CONCURRENTLY
///
/// TODO: support query id instead of select statement
#[cfg(feature = "sqlparser")]
fn parse_create_cache(
    parser: &mut Parser,
    dialect: Dialect,
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
        let name = parser.parse_object_name(false).ok().into_dialect(dialect);
        parser.expect_keyword(Keyword::FROM)?;
        name
    } else {
        None
    };

    // FIXME(sqlparser): Remove this once we deprecate nom-sql and switch to sqlparser
    // This is here to match the behavior of nom-sql, where it returns the part of
    // the query that failed to parse.
    let remaining_query = parser
        .peek_tokens::<50>()
        .iter()
        .filter_map(|t| {
            if t == &Token::EOF {
                None
            } else {
                Some(t.to_string())
            }
        })
        .collect::<Vec<_>>()
        .join(" ");

    let query = parse_query_for_create_cache(parser, dialect);
    Ok(SqlQuery::CreateCache(
        readyset_sql::ast::CreateCacheStatement {
            name,
            inner: query.map_err(|_| remaining_query),
            unparsed_create_cache_statement: Some(input.as_ref().trim().to_string()),
            always,
            concurrently,
        },
    ))
}

#[cfg(feature = "sqlparser")]
fn parse_query_for_create_cache(
    parser: &mut Parser,
    dialect: Dialect,
) -> Result<CacheInner, String> {
    parser
        .parse_statement()
        .map_err(|e| format!("failed to parse statement: {e}"))
        .and_then(|q| {
            q.try_into_dialect(dialect)
                .map_err(|e| format!("failed to convert AST: {e}"))
        })
        .and_then(|q: SqlQuery| q.into_select().ok_or_else(|| "expected SELECT".into()))
        .map(|q| CacheInner::Statement(Box::new(q)))
}

#[cfg(feature = "sqlparser")]
fn parse_explain(
    parser: &mut Parser,
    dialect: Dialect,
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
            Some(parser.parse_object_name(false)?.into_dialect(dialect))
        } else {
            None
        };
        return Ok(SqlQuery::Explain(
            readyset_sql::ast::ExplainStatement::Graphviz {
                simplified,
                for_cache,
            },
        ));
    } else if simplified {
        return Err(ReadysetParsingError::ReadysetParsingError(
            "unexpected SIMPLIFIED without GRAPHVIZ".into(),
        ));
    }
    if parser.parse_keywords(&[Keyword::CREATE, Keyword::CACHE, Keyword::FROM]) {
        return Ok(SqlQuery::Explain(
            readyset_sql::ast::ExplainStatement::CreateCache {
                inner: parse_query_for_create_cache(parser, dialect),
                unparsed_explain_create_cache_statement: input
                    .as_ref()
                    .strip_prefix("EXPLAIN ")
                    .unwrap()
                    .trim()
                    .to_string(),
            },
        ));
    }
    Ok(parser
        .parse_explain(sqlparser::ast::DescribeAlias::Explain)?
        .try_into_dialect(dialect)?)
}

/// Expects `SHOW` was already parsed. Attempts to parse a Readyset-specific SHOW statement,
/// otherwise falls back to [`Parser::parse_show`].
///
/// SHOW READYSET
///     | VERSION
///     | MIGRATION STATUS <query_id>
///     | STATUS
///     | ALL TABLES
/// SHOW
///     | CACHES
///     | CACHED QUERIES
///     | PROXIED QUERIES
#[cfg(feature = "sqlparser")]
fn parse_show(parser: &mut Parser, dialect: Dialect) -> Result<SqlQuery, ReadysetParsingError> {
    if parse_readyset_keyword(parser, ReadysetKeyword::READYSET) {
        if parser.parse_keyword(Keyword::VERSION) {
            Ok(SqlQuery::Show(
                readyset_sql::ast::ShowStatement::ReadySetVersion,
            ))
        } else if parse_readyset_keywords(
            parser,
            &[
                ReadysetKeyword::MIGRATION,
                ReadysetKeyword::Standard(Keyword::STATUS),
            ],
        ) {
            let id = parser.parse_literal_uint()?;
            Ok(SqlQuery::Show(
                readyset_sql::ast::ShowStatement::ReadySetMigrationStatus(id),
            ))
        } else if parser.parse_keyword(Keyword::STATUS) {
            Ok(SqlQuery::Show(
                readyset_sql::ast::ShowStatement::ReadySetStatus,
            ))
        } else if parser.parse_keyword(Keyword::TABLES) {
            Ok(SqlQuery::Show(
                readyset_sql::ast::ShowStatement::ReadySetTables(
                    readyset_sql::ast::ReadySetTablesOptions { all: false },
                ),
            ))
        } else if parser.parse_keywords(&[Keyword::ALL, Keyword::TABLES]) {
            Ok(SqlQuery::Show(
                readyset_sql::ast::ShowStatement::ReadySetTables(
                    readyset_sql::ast::ReadySetTablesOptions { all: true },
                ),
            ))
        } else {
            Err(ReadysetParsingError::ReadysetParsingError(
                "expected VERSION, STATUS, TABLES, ALL TABLES, or MIGRATION STATUS after READYSET"
                    .into(),
            ))
        }
    } else if parse_readyset_keywords(
        parser,
        &[ReadysetKeyword::PROXIED, ReadysetKeyword::QUERIES],
    ) {
        // TODO: Parse extra options
        Ok(SqlQuery::Show(
            readyset_sql::ast::ShowStatement::ProxiedQueries(
                readyset_sql::ast::ProxiedQueriesOptions {
                    query_id: None,
                    only_supported: false,
                    limit: None,
                },
            ),
        ))
    } else if parse_readyset_keyword(parser, ReadysetKeyword::CACHES)
        || parse_readyset_keywords(parser, &[ReadysetKeyword::CACHED, ReadysetKeyword::QUERIES])
    {
        // TODO: Parse extra options
        Ok(SqlQuery::Show(
            readyset_sql::ast::ShowStatement::CachedQueries(None),
        ))
    } else {
        Ok(parser.parse_show()?.try_into_dialect(dialect)?)
    }
}

/// Expects `DROP` was already parsed. Attempts to parse a Readyset-specific drop statement,
/// otherwise falls back to [`Parser::parse_drop`].
///
/// DROP
///   | ALL PROXIED QUERIES
///   | ALL CACHES
///   | CACHE <query_id>
#[cfg(feature = "sqlparser")]
fn parse_drop(parser: &mut Parser, dialect: Dialect) -> Result<SqlQuery, ReadysetParsingError> {
    if parse_readyset_keywords(
        parser,
        &[
            ReadysetKeyword::Standard(Keyword::ALL),
            ReadysetKeyword::PROXIED,
            ReadysetKeyword::QUERIES,
        ],
    ) {
        Ok(SqlQuery::DropAllProxiedQueries(
            readyset_sql::ast::DropAllProxiedQueriesStatement {},
        ))
    } else if parse_readyset_keywords(
        parser,
        &[
            ReadysetKeyword::Standard(Keyword::ALL),
            ReadysetKeyword::CACHES,
        ],
    ) {
        Ok(SqlQuery::DropAllCaches(
            readyset_sql::ast::DropAllCachesStatement {},
        ))
    } else if parser.parse_keyword(Keyword::CACHE) {
        let name = parser.parse_object_name(false)?.into_dialect(dialect);
        Ok(SqlQuery::DropCache(DropCacheStatement { name }))
    } else {
        Ok(parser.parse_drop()?.try_into_dialect(dialect)?)
    }
}

/// Attempts to parse a Readyset-specific statement, and falls back to [`Parser::parse_statement`] if it fails.
#[cfg(feature = "sqlparser")]
fn parse_readyset_query(
    parser: &mut Parser,
    dialect: Dialect,
    input: impl AsRef<str>,
) -> Result<SqlQuery, ReadysetParsingError> {
    if parser.parse_keyword(Keyword::ALTER) {
        parse_alter(parser, dialect)
    } else if parser.parse_keywords(&[Keyword::CREATE, Keyword::CACHE]) {
        parse_create_cache(parser, dialect, input)
    } else if parser.parse_keyword(Keyword::DROP) {
        parse_drop(parser, dialect)
    } else if parser.parse_keyword(Keyword::EXPLAIN) {
        parse_explain(parser, dialect, input)
    } else if parser.parse_keyword(Keyword::SHOW) {
        parse_show(parser, dialect)
    } else {
        Ok(parser.parse_statement()?.try_into_dialect(dialect)?)
    }
}

#[cfg(feature = "sqlparser")]
fn parse_readyset_expr(
    parser: &mut Parser,
    dialect: Dialect,
    _input: impl AsRef<str>,
) -> Result<Expr, ReadysetParsingError> {
    Ok(parser.parse_expr()?.try_into_dialect(dialect)?)
}

#[cfg(feature = "sqlparser")]
fn parse_readyset_create_table(
    parser: &mut Parser,
    dialect: Dialect,
    _input: impl AsRef<str>,
) -> Result<CreateTableStatement, ReadysetParsingError> {
    // parse_create expects CREATE keyword to be parsed already

    if parser.parse_keyword(Keyword::CREATE) {
        Ok(parser.parse_create()?.try_into_dialect(dialect)?)
    } else {
        Err(ReadysetParsingError::ReadysetParsingError(
            "expected a CREATE statement".into(),
        ))
    }
}

#[cfg(feature = "sqlparser")]
fn parse_both_inner<S, T, NP, SP>(
    dialect: Dialect,
    input: S,
    nom_parser: NP,
    sqlparser_parser: SP,
) -> Result<T, String>
where
    T: PartialEq + std::fmt::Debug,
    S: AsRef<str>,
    for<'a> NP: FnOnce(Dialect, &'a str) -> Result<T, String>,
    for<'a> SP: FnOnce(&mut Parser, Dialect, &'a str) -> Result<T, ReadysetParsingError>,
{
    let nom_result = nom_parser(dialect, input.as_ref());
    let sqlparser_dialect = sqlparser_dialect_from_readyset_dialect(dialect);
    let sqlparser_result = Parser::new(sqlparser_dialect.as_ref())
        .try_with_sql(input.as_ref())
        .map_err(Into::into)
        .and_then(|mut p| sqlparser_parser(&mut p, dialect, input.as_ref()));

    match (&nom_result, sqlparser_result) {
        (Ok(nom_ast), Ok(sqlparser_ast)) => {
            pretty_assertions::assert_eq!(
                nom_ast,
                &sqlparser_ast,
                "nom-sql AST differs from sqlparser-rs AST for {} input: {:?}",
                dialect,
                input.as_ref()
            );
        }
        (Ok(nom_ast), Err(sqlparser_error)) => {
            if !matches!(
                sqlparser_error,
                ReadysetParsingError::AstConversionError(
                    readyset_sql::AstConversionError::Skipped(_)
                        | readyset_sql::AstConversionError::Unsupported(_),
                ),
            ) {
                panic!(
                    "nom-sql succeeded but sqlparser-rs failed: {}\ninput: {}\nnom_ast: {:?}",
                    sqlparser_error,
                    input.as_ref(),
                    nom_ast
                )
            }
        }
        (Err(nom_error), Ok(sqlparser_ast)) => {
            tracing::warn!(%nom_error, ?sqlparser_ast, "sqlparser-rs succeeded but nom-sql failed")
        }
        (Err(nom_error), Err(sqlparser_error)) => {
            tracing::warn!(%nom_error, %sqlparser_error, "both nom-sql and sqlparser-rs failed");
        }
    };
    nom_result
}

#[cfg(feature = "sqlparser")]
pub fn parse_query(dialect: Dialect, input: impl AsRef<str>) -> Result<SqlQuery, String> {
    parse_both_inner(
        dialect,
        input,
        |d, s| nom_sql::parse_query(d, s),
        |p, d, s| parse_readyset_query(p, d, s),
    )
}

#[cfg(not(feature = "sqlparser"))]
pub fn parse_query(dialect: Dialect, input: impl AsRef<str>) -> Result<SqlQuery, String> {
    nom_sql::parse_query(dialect, input.as_ref())
}

/// Parses a single expression; only intended for use in tests.
#[cfg(feature = "sqlparser")]
pub fn parse_expr(dialect: Dialect, input: impl AsRef<str>) -> Result<Expr, String> {
    parse_both_inner(
        dialect,
        input,
        |d, s| nom_sql::parse_expr(d, s),
        |p, d, s| parse_readyset_expr(p, d, s),
    )
}

#[cfg(feature = "sqlparser")]
fn parse_readyset_select(
    parser: &mut Parser,
    dialect: Dialect,
    _input: impl AsRef<str>,
) -> Result<SelectStatement, ReadysetParsingError> {
    // SQLParser has this weird behaviour where `parse_select` subparser
    // doesn't parse limits or offsets....
    Ok(parser.parse_query()?.try_into_dialect(dialect)?)
}

#[cfg(not(feature = "sqlparser"))]
pub fn parse_expr(dialect: Dialect, input: impl AsRef<str>) -> Result<Expr, String> {
    nom_sql::parse_expr(dialect, input.as_ref())
}

#[cfg(not(feature = "sqlparser"))]
pub fn parse_select(dialect: Dialect, input: impl AsRef<str>) -> Result<SelectStatement, String> {
    nom_sql::parse_select(dialect, input.as_ref())
}

/// Parses a single expression; only intended for use in tests.
#[cfg(feature = "sqlparser")]
pub fn parse_select(dialect: Dialect, input: impl AsRef<str>) -> Result<SelectStatement, String> {
    parse_both_inner(
        dialect,
        input,
        |d, s| nom_sql::parse_select(d, s),
        |p, d, s| parse_readyset_select(p, d, s),
    )
}

#[cfg(feature = "sqlparser")]
pub fn parse_alter_table(
    dialect: Dialect,
    input: impl AsRef<str>,
) -> Result<AlterTableStatement, String> {
    parse_both_inner(
        dialect,
        input,
        |d, s| nom_sql::parse_alter_table(d, s),
        |p, d, s| parse_readyset_alter_table(p, d, s),
    )
}

#[cfg(feature = "sqlparser")]
fn parse_readyset_alter_table(
    parser: &mut Parser,
    dialect: Dialect,
    _input: impl AsRef<str>,
) -> Result<AlterTableStatement, ReadysetParsingError> {
    // parse_alter expects ALTER keyword to be parsed already
    if parser.parse_keyword(Keyword::ALTER) {
        Ok(parser.parse_alter()?.try_into_dialect(dialect)?)
    } else {
        Err(ReadysetParsingError::ReadysetParsingError(
            "expected an ALTER statement".into(),
        ))
    }
}

#[cfg(not(feature = "sqlparser"))]
pub fn parse_alter_table(
    dialect: Dialect,
    input: impl AsRef<str>,
) -> Result<AlterTableStatement, String> {
    nom_sql::parse_alter_table(dialect, input.as_ref())
}

#[cfg(not(feature = "sqlparser"))]
pub fn parse_create_table(
    dialect: Dialect,
    input: impl AsRef<str>,
) -> Result<CreateTableStatement, String> {
    nom_sql::parse_create_table(dialect, input.as_ref())
}

#[cfg(feature = "sqlparser")]
pub fn parse_create_table(
    dialect: Dialect,
    input: impl AsRef<str>,
) -> Result<CreateTableStatement, String> {
    parse_both_inner(
        dialect,
        input,
        |d, s| nom_sql::parse_create_table(d, s),
        |p, d, s| parse_readyset_create_table(p, d, s),
    )
}

#[cfg(not(feature = "sqlparser"))]
pub fn parse_create_view(
    dialect: Dialect,
    input: impl AsRef<str>,
) -> Result<CreateViewStatement, String> {
    nom_sql::parse_create_view(dialect, input.as_ref())
}

#[cfg(feature = "sqlparser")]
fn parse_readyset_create_view(
    parser: &mut Parser,
    dialect: Dialect,
    _input: impl AsRef<str>,
) -> Result<CreateViewStatement, ReadysetParsingError> {
    // parse_create expects CREATE keyword to be parsed already
    if parser.parse_keyword(Keyword::CREATE) {
        Ok(parser.parse_create()?.try_into_dialect(dialect)?)
    } else {
        Err(ReadysetParsingError::ReadysetParsingError(
            "expected a CREATE statement".into(),
        ))
    }
}

#[cfg(feature = "sqlparser")]
pub fn parse_create_view(
    dialect: Dialect,
    input: impl AsRef<str>,
) -> Result<CreateViewStatement, String> {
    parse_both_inner(
        dialect,
        input,
        |d, s| nom_sql::parse_create_view(d, s),
        |p, d, s| parse_readyset_create_view(p, d, s),
    )
}

#[cfg(not(feature = "sqlparser"))]
pub fn parse_sql_type(dialect: Dialect, input: impl AsRef<str>) -> Result<SqlType, String> {
    nom_sql::parse_sql_type(dialect, input.as_ref())
}

#[cfg(feature = "sqlparser")]
fn parse_readyset_sql_type(
    parser: &mut Parser,
    dialect: Dialect,
    _input: impl AsRef<str>,
) -> Result<SqlType, ReadysetParsingError> {
    // parse_create expects CREATE keyword to be parsed already
    Ok(parser.parse_data_type()?.try_into_dialect(dialect)?)
}

#[cfg(feature = "sqlparser")]
pub fn parse_sql_type(dialect: Dialect, input: impl AsRef<str>) -> Result<SqlType, String> {
    parse_both_inner(
        dialect,
        input,
        |d, s| nom_sql::parse_sql_type(d, s),
        |p, d, s| parse_readyset_sql_type(p, d, s),
    )
}

#[cfg(not(feature = "sqlparser"))]
pub fn parse_key_specification(
    dialect: Dialect,
    input: impl AsRef<str>,
) -> Result<TableKey, String> {
    nom_sql::parse_key_specification(dialect, input.as_ref())
}

#[cfg(feature = "sqlparser")]
fn parse_readyset_key_specification(
    parser: &mut Parser,
    dialect: Dialect,
    _input: impl AsRef<str>,
) -> Result<TableKey, ReadysetParsingError> {
    Ok(parser
        .parse_optional_table_constraint()?
        .ok_or_else(|| {
            ReadysetParsingError::ReadysetParsingError("expected a table constraint".into())
        })?
        .try_into_dialect(dialect)?)
}

#[cfg(feature = "sqlparser")]
pub fn parse_key_specification(
    dialect: Dialect,
    input: impl AsRef<str>,
) -> Result<TableKey, String> {
    parse_both_inner(
        dialect,
        input,
        |d, s| nom_sql::parse_key_specification(d, s),
        |p, d, s| parse_readyset_key_specification(p, d, s),
    )
}
