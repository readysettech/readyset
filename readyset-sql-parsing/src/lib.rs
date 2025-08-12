use std::any::Any;
use std::num::ParseIntError;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use clap::ValueEnum;
use readyset_errors::ReadySetError;
use readyset_sql::ast::{
    AddTablesStatement, AlterReadysetStatement, AlterTableStatement, CacheInner,
    CreateTableStatement, CreateViewStatement, DropCacheStatement, Expr, ResnapshotTableStatement,
    SelectStatement, SqlQuery, SqlType, TableKey,
};
use readyset_sql::{Dialect, IntoDialect, TryIntoDialect};
use serde::{Deserialize, Serialize};
use sqlparser::{
    keywords::Keyword,
    parser::Parser,
    tokenizer::{Token, TokenWithSpan, Word},
};

/// Parsing configuration that determines which parser(s) to use and how to handle conflicts,
/// errors, and warnings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParsingConfig {
    nom: bool,
    sqlparser: bool,
    prefer_sqlparser: bool,
    error_on_mismatch: bool,
    panic_on_mismatch: bool,
    log_on_mismatch: bool,
    rate_limit_logging: bool,
    log_only_selects: bool,
}

impl Default for ParsingConfig {
    fn default() -> Self {
        Self {
            nom: true,
            sqlparser: true,
            prefer_sqlparser: false,
            error_on_mismatch: false,
            panic_on_mismatch: false,
            log_on_mismatch: true,
            rate_limit_logging: true,
            log_only_selects: false,
        }
    }
}

impl ParsingConfig {
    pub fn nom(self, nom: bool) -> Self {
        Self { nom, ..self }
    }

    pub fn sqlparser(self, sqlparser: bool) -> Self {
        Self { sqlparser, ..self }
    }

    pub fn prefer_sqlparser(self, prefer_sqlparser: bool) -> Self {
        Self {
            prefer_sqlparser,
            ..self
        }
    }

    pub fn error_on_mismatch(self, error_on_mismatch: bool) -> Self {
        Self {
            error_on_mismatch,
            ..self
        }
    }

    pub fn panic_on_mismatch(self, panic_on_mismatch: bool) -> Self {
        Self {
            panic_on_mismatch,
            ..self
        }
    }

    pub fn log_on_mismatch(self, log_on_mismatch: bool) -> Self {
        Self {
            log_on_mismatch,
            ..self
        }
    }

    pub fn rate_limit_logging(self, rate_limit_logging: bool) -> Self {
        Self {
            rate_limit_logging,
            ..self
        }
    }

    pub fn log_only_selects(self, log_only_selects: bool) -> Self {
        Self {
            log_only_selects,
            ..self
        }
    }
}

/// A preset for parsing that can be turned into a [`ParsingConfig`]. This should be used at the top
/// level, e.g. in CLI flags and when setting up subsystems like the replication connector.
/// Subsystems can then turn this into a [`ParsingConfig`] and modify certain settings. For example,
/// DDL should always log on mismatch, but parsing ad-hoc queries should not, and this way both code
/// paths can use the same preset as a starting point.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Serialize, Deserialize)]
pub enum ParsingPreset {
    /// Parse only with nom-sql
    OnlyNom,
    /// Parse only with sqlparser
    OnlySqlparser,
    /// Parse with both, prefer nom-sql, warn if sqlparser errors or differs
    BothPreferNom,
    /// Parse with both, prefer sqlparser, warn if nom-sql errors or differs
    BothPreferSqlparser,
    /// Parse with both and return an error if they differ.
    BothErrorOnMismatch,
    /// Parse with both and panic if they differ; only warns if sqlparser succeeds and nom fails,
    /// because we don't consider that a regression
    BothPanicOnMismatch,
}

impl ParsingPreset {
    /// The default parsing mode to use in tests.
    pub fn for_tests() -> Self {
        Self::BothPanicOnMismatch
    }

    /// The default parsing mode to use in the actual server. Usage of this will be replaced with a
    /// runtime-configurable value.
    pub fn for_prod() -> Self {
        Self::BothPreferNom
    }

    pub fn into_config(self) -> ParsingConfig {
        match self {
            Self::OnlyNom => ParsingConfig::default().sqlparser(false),
            Self::OnlySqlparser => ParsingConfig::default().nom(false),
            Self::BothPreferNom => ParsingConfig::default(),
            Self::BothPreferSqlparser => ParsingConfig::default().prefer_sqlparser(true),
            Self::BothErrorOnMismatch => ParsingConfig::default().error_on_mismatch(true),
            Self::BothPanicOnMismatch => ParsingConfig::default().panic_on_mismatch(true),
        }
    }
}

impl From<ParsingPreset> for ParsingConfig {
    fn from(value: ParsingPreset) -> Self {
        value.into_config()
    }
}

static RATE_LIMIT_INTERVAL: LazyLock<Duration> = LazyLock::new(|| {
    let secs = std::env::var("PARSING_LOG_RATE_LIMIT_SECS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(60);
    Duration::from_secs(secs)
});

fn check_rate_limit() -> bool {
    static NEXT_LOG_TIME: AtomicU64 = AtomicU64::new(0);

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Couldn't get system time");

    let next_log = NEXT_LOG_TIME.load(Ordering::Relaxed);
    let should_log = now.as_secs() >= next_log;

    if should_log {
        NEXT_LOG_TIME.store((now + *RATE_LIMIT_INTERVAL).as_secs(), Ordering::Relaxed);
    }

    should_log
}

macro_rules! rate_limit {
    ($should_limit:expr, $body:expr) => {
        if !$should_limit || check_rate_limit() {
            $body
        }
    };
}

#[derive(Debug, thiserror::Error)]
pub enum ReadysetParsingError {
    #[error("nom-sql error: {0}")]
    NomError(String),
    #[error("sqlparser error: {0}")]
    SqlparserError(#[from] sqlparser::parser::ParserError),
    #[error("AST conversion error: {0}")]
    AstConversionError(#[from] readyset_sql::AstConversionError),
    #[error("readyset parsing error: {0}")]
    ReadysetParsingError(String),
    #[error("both parsers failed - nom-sql: {nom_error}, sqlparser: {sqlparser_error}")]
    BothFailed {
        nom_error: String,
        sqlparser_error: String,
    },
}

impl From<ReadysetParsingError> for ReadySetError {
    fn from(err: ReadysetParsingError) -> Self {
        Self::UnparseableQuery(err.into())
    }
}

impl From<ReadysetParsingError> for String {
    fn from(err: ReadysetParsingError) -> Self {
        err.to_string()
    }
}

impl From<ParseIntError> for ReadysetParsingError {
    fn from(value: ParseIntError) -> Self {
        Self::ReadysetParsingError(value.to_string())
    }
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
#[derive(Debug, Clone, Copy)]
enum ReadysetKeyword {
    CACHED,
    CACHES,
    DOMAINS,
    ENTER,
    EXIT,
    MAINTENANCE,
    MATERIALIZATIONS,
    MIGRATION,
    PROXIED,
    QUERIES,
    READYSET,
    RESNAPSHOT,
    SIMPLIFIED,
    SUPPORTED,
    /// To match both Readyset and sqlparser keywords in one go, we want to be able to accept both
    /// in the same function. So here we just allow falling back to a sqlparser keyword.
    Standard(sqlparser::keywords::Keyword),
}

impl ReadysetKeyword {
    fn as_str(&self) -> &str {
        match self {
            Self::CACHED => "CACHED",
            Self::CACHES => "CACHES",
            Self::DOMAINS => "DOMAINS",
            Self::ENTER => "ENTER",
            Self::EXIT => "EXIT",
            Self::MAINTENANCE => "MAINTENANCE",
            Self::MATERIALIZATIONS => "MATERIALIZATIONS",
            Self::MIGRATION => "MIGRATION",
            Self::PROXIED => "PROXIED",
            Self::QUERIES => "QUERIES",
            Self::READYSET => "READYSET",
            Self::RESNAPSHOT => "RESNAPSHOT",
            Self::SIMPLIFIED => "SIMPLIFIED",
            Self::SUPPORTED => "SUPPORTED",
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
        } else if parser.parse_keywords(&[Keyword::SET, Keyword::LOG, Keyword::LEVEL]) {
            let directives = parser.parse_literal_string()?;
            Ok(SqlQuery::AlterReadySet(
                AlterReadysetStatement::SetLogLevel(directives),
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

fn parse_query_for_create_cache(
    parser: &mut Parser,
    dialect: Dialect,
) -> Result<CacheInner, String> {
    if let Ok(statement) = parser.try_parse(|p| p.parse_statement()) {
        let query: SqlQuery = statement
            .try_into_dialect(dialect)
            .map_err(ReadysetParsingError::from)?;
        let select = query
            .into_select()
            .ok_or_else(|| "expected SELECT".to_string())?;
        Ok(CacheInner::Statement(Box::new(select)))
    } else {
        let id = parser
            .parse_identifier()
            .map_err(ReadysetParsingError::from)?
            .into_dialect(dialect);
        Ok(CacheInner::Id(id))
    }
}

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
    if parse_readyset_keyword(parser, ReadysetKeyword::MATERIALIZATIONS) {
        return Ok(SqlQuery::Explain(
            readyset_sql::ast::ExplainStatement::Materializations,
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
    if parser.parse_keywords(&[Keyword::CREATE, Keyword::CACHE]) {
        return match parse_create_cache(parser, dialect, input)? {
            SqlQuery::CreateCache(inner) => {
                return Ok(SqlQuery::Explain(
                    readyset_sql::ast::ExplainStatement::CreateCache {
                        inner: inner.inner,
                        unparsed_explain_create_cache_statement: inner
                            .unparsed_create_cache_statement
                            .unwrap_or_default(),
                    },
                ));
            }
            _ => Err(ReadysetParsingError::ReadysetParsingError(
                "unexpected statement after CREATE CACHE".into(),
            )),
        };
    }
    if parse_readyset_keyword(parser, ReadysetKeyword::DOMAINS) {
        return Ok(SqlQuery::Explain(
            readyset_sql::ast::ExplainStatement::Domains,
        ));
    }
    if parse_readyset_keyword(parser, ReadysetKeyword::CACHES) {
        return Ok(SqlQuery::Explain(
            readyset_sql::ast::ExplainStatement::Caches,
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
    } else if parse_readyset_keyword(parser, ReadysetKeyword::PROXIED) {
        let only_supported = parse_readyset_keyword(parser, ReadysetKeyword::SUPPORTED);
        if !parse_readyset_keyword(parser, ReadysetKeyword::QUERIES) {
            return Err(ReadysetParsingError::ReadysetParsingError(
                "expected QUERIES after PROXIED [SUPPORTED]".into(),
            ));
        }
        let query_id = if parser.parse_keyword(Keyword::WHERE) {
            let lhs = parser.parse_identifier()?;
            if lhs.value != "query_id" {
                return Err(ReadysetParsingError::ReadysetParsingError(
                    "expected 'query_id' after WHERE".into(),
                ));
            }
            parser.expect_token(&Token::Eq)?;
            Some(parser.parse_identifier()?.value)
        } else {
            None
        };
        let limit = if parser.parse_keyword(Keyword::LIMIT) {
            Some(parser.parse_number()?.to_string().parse()?)
        } else {
            None
        };
        Ok(SqlQuery::Show(
            readyset_sql::ast::ShowStatement::ProxiedQueries(
                readyset_sql::ast::ProxiedQueriesOptions {
                    query_id,
                    only_supported,
                    limit,
                },
            ),
        ))
    } else if parse_readyset_keyword(parser, ReadysetKeyword::CACHES)
        || parse_readyset_keywords(parser, &[ReadysetKeyword::CACHED, ReadysetKeyword::QUERIES])
    {
        let query_id = if parser.parse_keyword(Keyword::WHERE) {
            let lhs = parser.parse_identifier()?;
            if lhs.value != "query_id" {
                return Err(ReadysetParsingError::ReadysetParsingError(
                    "expected 'query_id' after WHERE".into(),
                ));
            }
            parser.expect_token(&Token::Eq)?;
            Some(parser.parse_identifier()?.value)
        } else {
            None
        };
        Ok(SqlQuery::Show(
            readyset_sql::ast::ShowStatement::CachedQueries(query_id),
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
fn parse_readyset_query(
    parser: &mut Parser,
    dialect: Dialect,
    input: &str,
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

fn parse_readyset_expr(
    parser: &mut Parser,
    dialect: Dialect,
    _input: &str,
) -> Result<Expr, ReadysetParsingError> {
    Ok(parser.parse_expr()?.try_into_dialect(dialect)?)
}

fn parse_readyset_select(
    parser: &mut Parser,
    dialect: Dialect,
    _input: &str,
) -> Result<SelectStatement, ReadysetParsingError> {
    // SQLParser has this weird behaviour where `parse_select` subparser
    // doesn't parse limits or offsets....
    Ok(parser.parse_query()?.try_into_dialect(dialect)?)
}

fn parse_readyset_alter_table(
    parser: &mut Parser,
    dialect: Dialect,
    _input: &str,
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

fn parse_readyset_create_table(
    parser: &mut Parser,
    dialect: Dialect,
    _input: &str,
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

fn parse_readyset_create_view(
    parser: &mut Parser,
    dialect: Dialect,
    _input: &str,
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

fn parse_readyset_sql_type(
    parser: &mut Parser,
    dialect: Dialect,
    _input: &str,
) -> Result<SqlType, ReadysetParsingError> {
    Ok(parser.parse_data_type()?.try_into_dialect(dialect)?)
}

fn parse_readyset_key_specification(
    parser: &mut Parser,
    dialect: Dialect,
    _input: &str,
) -> Result<TableKey, ReadysetParsingError> {
    Ok(parser
        .parse_optional_table_constraint()?
        .ok_or_else(|| {
            ReadysetParsingError::ReadysetParsingError("expected a table constraint".into())
        })?
        .try_into_dialect(dialect)?)
}

fn parse_sqlparser_inner<S, T, SP>(
    dialect: Dialect,
    input: S,
    sqlparser_parser: SP,
) -> Result<T, ReadysetParsingError>
where
    T: PartialEq + std::fmt::Debug + Clone,
    S: AsRef<str>,
    for<'a> SP: FnOnce(&mut Parser, Dialect, &'a str) -> Result<T, ReadysetParsingError>,
{
    let sqlparser_dialect = sqlparser_dialect_from_readyset_dialect(dialect);
    let mut parser = Parser::new(sqlparser_dialect.as_ref())
        .try_with_sql(input.as_ref())
        .map_err(ReadysetParsingError::SqlparserError)?;
    let sqlparser_result = sqlparser_parser(&mut parser, dialect, input.as_ref());
    // Strip trailing semicolons and make sure we consumed everything by checking for the virtual
    // EOF token
    if sqlparser_result.is_ok() {
        while parser.consume_token(&Token::SemiColon) {}
        parser.expect_token(&Token::EOF)?;
    }
    sqlparser_result
}

/// If this is a [`SqlQuery`], only log if it's not a SELECT statement. We use some dynamic type
/// nonsense to do so because I can't think of a better way that doesn't involve lots of plumbing.
/// As far as I can tell, monomorphization does the right thing and there's no overhead. If it's not
/// a [`SqlQuery`], we are in test code and always want to log it.
fn is_not_query_or_should_log<T: Any>(ast: &T) -> bool {
    let ast: &dyn Any = ast;
    ast.downcast_ref::<SqlQuery>()
        .map(|query| query.is_select() || query.is_readyset_extension())
        .unwrap_or(true)
}

fn parse_both_inner<C, S, T, NP, SP>(
    config: C,
    dialect: Dialect,
    input: S,
    nom_parser: NP,
    sqlparser_parser: SP,
) -> Result<T, ReadysetParsingError>
where
    C: Into<ParsingConfig>,
    T: PartialEq + std::fmt::Debug + Clone + Any,
    S: AsRef<str>,
    for<'a> NP: FnOnce(Dialect, &'a str) -> Result<T, String>,
    for<'a> SP: FnOnce(&mut Parser, Dialect, &'a str) -> Result<T, ReadysetParsingError>,
{
    let config: ParsingConfig = config.into();
    if !config.sqlparser {
        nom_parser(dialect, input.as_ref()).map_err(ReadysetParsingError::NomError)
    } else if !config.nom {
        parse_sqlparser_inner(dialect, input, sqlparser_parser)
    } else {
        let nom_result = nom_parser(dialect, input.as_ref());
        let sqlparser_result = parse_sqlparser_inner(dialect, input.as_ref(), sqlparser_parser);

        match (nom_result, sqlparser_result) {
            (Ok(nom_ast), Ok(sqlparser_ast)) if nom_ast != sqlparser_ast => {
                if config.panic_on_mismatch {
                    pretty_assertions::assert_eq!(
                        nom_ast,
                        sqlparser_ast,
                        "nom-sql AST differs from sqlparser-rs AST for {} input: {:?}",
                        dialect,
                        input.as_ref()
                    );
                }
                if config.log_on_mismatch
                    && (!config.log_only_selects
                        || (is_not_query_or_should_log(&nom_ast)
                            || is_not_query_or_should_log(&sqlparser_ast)))
                {
                    rate_limit!(
                        config.rate_limit_logging,
                        tracing::warn!(
                            ?dialect,
                            input = %input.as_ref(),
                            ?nom_ast,
                            ?sqlparser_ast,
                            "nom-sql AST differs from sqlparser-rs AST",
                        )
                    );
                }
                if config.error_on_mismatch {
                    Err(ReadysetParsingError::ReadysetParsingError(format!(
                        "nom-sql AST differs from sqlparser-rs AST for {} input: {:?}",
                        dialect,
                        input.as_ref()
                    )))
                } else if config.prefer_sqlparser {
                    Ok(sqlparser_ast)
                } else {
                    Ok(nom_ast)
                }
            }
            (Ok(nom_ast), Ok(_)) => Ok(nom_ast),
            (Ok(nom_ast), Err(sqlparser_error)) => {
                // Ignore errors that signify we explicitly don't support something, even though nom-sql implements it.
                if !matches!(
                    sqlparser_error,
                    ReadysetParsingError::AstConversionError(
                        readyset_sql::AstConversionError::Skipped(_)
                            | readyset_sql::AstConversionError::Unsupported(_),
                    ),
                ) {
                    if config.panic_on_mismatch {
                        panic!(
                            "nom-sql succeeded but sqlparser-rs failed for {}: {}\ninput: {}\nnom_ast: {:?}",
                            dialect,
                            sqlparser_error,
                            input.as_ref(),
                            nom_ast
                        );
                    }
                    if config.log_on_mismatch
                        && (!config.log_only_selects || is_not_query_or_should_log(&nom_ast))
                    {
                        rate_limit!(
                            config.rate_limit_logging,
                            tracing::warn!(
                                ?dialect,
                                input = %input.as_ref(),
                                %sqlparser_error,
                                ?nom_ast,
                                "sqlparser-rs failed but nom-sql succeeded"
                            )
                        );
                    }
                    if config.error_on_mismatch || config.prefer_sqlparser {
                        Err(sqlparser_error)
                    } else {
                        Ok(nom_ast)
                    }
                } else {
                    Ok(nom_ast)
                }
            }
            (Err(nom_error), Ok(sqlparser_ast)) => {
                if config.log_on_mismatch
                    && (!config.log_only_selects || is_not_query_or_should_log(&sqlparser_ast))
                {
                    rate_limit!(
                        config.rate_limit_logging,
                        tracing::debug!(
                            ?dialect,
                            input = %input.as_ref(),
                            %nom_error,
                            ?sqlparser_ast,
                            "nom-sql failed but sqlparser-rs succeeded"
                        )
                    );
                }
                if config.prefer_sqlparser {
                    Ok(sqlparser_ast)
                } else {
                    Err(ReadysetParsingError::NomError(nom_error))
                }
            }
            (Err(nom_error), Err(sqlparser_error)) => {
                if config.log_on_mismatch {
                    tracing::debug!(
                        ?dialect,
                        input = %input.as_ref(),
                        %nom_error,
                        %sqlparser_error,
                        "both nom-sql and sqlparser-rs failed"
                    );
                }
                Err(ReadysetParsingError::BothFailed {
                    nom_error,
                    sqlparser_error: format!("{sqlparser_error}"),
                })
            }
        }
    }
}

macro_rules! export_parser {
    ($(#[doc = $doc:expr])+ $parser:ident, $ty:ident) => {
        paste::paste! {
            $(
                #[doc = $doc]
            )+
            pub fn [<parse_ $parser _with_config>](
                config: impl Into<ParsingConfig>,
                dialect: Dialect,
                input: impl AsRef<str>,
            ) -> Result<$ty, ReadysetParsingError> {
                parse_both_inner(
                    config,
                    dialect,
                    input,
                    |d, s| nom_sql::[<parse_ $parser>](d, s),
                    [<parse_readyset_ $parser>],
                )
            }

            $(
                #[doc = $doc]
            )+
            ///
            /// Uses the default parsing config for tests, which can panic (see
            /// [`ParsingPreset::for_tests()`]); so should only be used in tests; otherwise use
            #[doc = concat!("[`", stringify!([<parse_ $parser _with_config>]), "`]")]
            pub fn [<parse_ $parser>](
                dialect: Dialect,
                input: impl AsRef<str>,
            ) -> Result<$ty, ReadysetParsingError> {
                [<parse_ $parser _with_config>](ParsingPreset::for_tests(), dialect, input)
            }
        }
    };
}

export_parser!(
    /// Parse a SQL query, including custom Readyset extension.
    query,
    SqlQuery
);
export_parser!(
    /// Parse a single SQL expression.
    expr,
    Expr
);
export_parser!(
    /// Parse a select statement. Since sqlparser does not parse LIMIT/OFFSET with the select
    /// statement subparser, this parses it as a generic query *without* Readyset extensions and
    /// attempts to convert it to a `SelectStatement`.
    select,
    SelectStatement
);
export_parser!(
    /// Parse a blah.
    alter_table,
    AlterTableStatement
);
export_parser!(
    /// Parse a blah.
    create_table,
    CreateTableStatement
);
export_parser!(
    /// Parse a blah.
    create_view,
    CreateViewStatement
);
export_parser!(
    /// Parse a blah.
    sql_type,
    SqlType
);
export_parser!(
    /// Parse a blah.
    key_specification,
    TableKey
);
