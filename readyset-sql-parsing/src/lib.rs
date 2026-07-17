use std::any::Any;
use std::num::ParseIntError;
use std::time::Duration;

use clap::ValueEnum;
use readyset_errors::ReadySetError;
use readyset_sql::ast::{
    AddTablesStatement, AddUserStatement, AlterReadysetStatement, AlterTableStatement,
    AutoparamControl, CacheInner, CacheType, ChangeCdcStatement, ChangeUpstreamStatement,
    CreateCacheOptions, CreateCacheStatement, CreateTableStatement, CreateViewStatement,
    DropCacheStatement, DropUserStatement, EvictionPolicy, Expr, FlushAllShallowCachesStatement,
    FlushCacheStatement, ModifyUserStatement, ReadysetHintDirective, ResnapshotTableStatement,
    SelectStatement, SessionAuthorizationValue, SetEviction, SetReplicationPositionStatement,
    SetSessionAuthorization, SetStatement, ShallowCacheAllowlistChange, ShallowCacheAllowlistKind,
    ShallowCacheQuery, SqlQuery, SqlType, TableKey, TrxCachePolicy,
};
use readyset_sql::{Dialect, IntoDialect, TryIntoDialect};
use readyset_util::logging::{PARSING_LOG_PARSING_MISMATCH_SQLPARSER_FAILED, rate_limit};
use serde::{Deserialize, Serialize};
use sqlparser::ast::SetExpr;
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
        Self::BothPreferSqlparser
    }

    /// Whether parsing returns the sqlparser-derived AST whenever the sqlparser parse and AST
    /// conversion succeed. When this is true, converting an already-parsed sqlparser AST is
    /// equivalent to a full parse of the query text for such queries.
    pub fn prefers_sqlparser_ast(self) -> bool {
        matches!(self, Self::OnlySqlparser | Self::BothPreferSqlparser)
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
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, Copy)]
enum ReadysetKeyword {
    ADAPTIVE,
    ALLOWED,
    CACHES,
    CDC,
    DEEP,
    DOMAINS,
    ENTER,
    ENTRIES,
    EVICTION,
    EXIT,
    EXPIRES,
    MAINTENANCE,
    MATERIALIZATIONS,
    MCP,
    MEMORY,
    MIGRATION,
    MS,
    NEVER,
    PERIOD,
    PATHS,
    POLICY,
    PROXIED,
    QUERIES,
    READYSET,
    REFRESH,
    REPLAY,
    RESNAPSHOT,
    RSA,
    SCOPE,
    SHALLOW,
    SIMPLIFIED,
    STOP,
    SUPPORTED,
    TOKEN,
    TOKENS,
    TTL,
    UPSTREAM,
    /// To match both Readyset and sqlparser keywords in one go, we want to be able to accept both
    /// in the same function. So here we just allow falling back to a sqlparser keyword.
    Standard(sqlparser::keywords::Keyword),
}

impl ReadysetKeyword {
    fn as_str(&self) -> &str {
        match self {
            Self::ADAPTIVE => "ADAPTIVE",
            Self::ALLOWED => "ALLOWED",
            Self::CACHES => "CACHES",
            Self::CDC => "CDC",
            Self::DEEP => "DEEP",
            Self::DOMAINS => "DOMAINS",
            Self::ENTER => "ENTER",
            Self::ENTRIES => "ENTRIES",
            Self::EVICTION => "EVICTION",
            Self::EXIT => "EXIT",
            Self::EXPIRES => "EXPIRES",
            Self::MAINTENANCE => "MAINTENANCE",
            Self::MATERIALIZATIONS => "MATERIALIZATIONS",
            Self::MCP => "MCP",
            Self::MEMORY => "MEMORY",
            Self::MIGRATION => "MIGRATION",
            Self::MS => "MS",
            Self::NEVER => "NEVER",
            Self::PERIOD => "PERIOD",
            Self::PATHS => "PATHS",
            Self::POLICY => "POLICY",
            Self::PROXIED => "PROXIED",
            Self::QUERIES => "QUERIES",
            Self::READYSET => "READYSET",
            Self::REFRESH => "REFRESH",
            Self::REPLAY => "REPLAY",
            Self::RESNAPSHOT => "RESNAPSHOT",
            Self::RSA => "RSA",
            Self::SCOPE => "SCOPE",
            Self::SHALLOW => "SHALLOW",
            Self::SIMPLIFIED => "SIMPLIFIED",
            Self::STOP => "STOP",
            Self::SUPPORTED => "SUPPORTED",
            Self::TOKEN => "TOKEN",
            Self::TOKENS => "TOKENS",
            Self::TTL => "TTL",
            Self::UPSTREAM => "UPSTREAM",
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
            let table_name = parser.parse_object_name(false)?.try_into_dialect(dialect)?;
            Ok(SqlQuery::AlterReadySet(
                AlterReadysetStatement::ResnapshotTable(ResnapshotTableStatement {
                    table: table_name,
                }),
            ))
        } else if parser.parse_keywords(&[Keyword::ADD, Keyword::TABLES]) {
            let tables = parser
                .parse_comma_separated(|p| p.parse_object_name(false))?
                .into_iter()
                .map(|table| table.try_into_dialect(dialect))
                .collect::<Result<Vec<_>, _>>()?;
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
        } else if parse_readyset_keywords(
            parser,
            &[
                ReadysetKeyword::Standard(Keyword::SET),
                ReadysetKeyword::EVICTION,
            ],
        ) {
            // Parse optional LIMIT clause
            let limit = if parse_readyset_keywords(
                parser,
                &[
                    ReadysetKeyword::MEMORY,
                    ReadysetKeyword::Standard(Keyword::LIMIT),
                ],
            ) {
                Some(parser.parse_literal_uint()?)
            } else {
                None
            };

            // Parse optional PERIOD clause
            let period = if parse_readyset_keyword(parser, ReadysetKeyword::PERIOD) {
                Some(parser.parse_literal_uint()?)
            } else {
                None
            };

            if limit.is_none() && period.is_none() {
                return Err(ReadysetParsingError::ReadysetParsingError(
                    "expected MEMORY LIMIT or PERIOD after SET EVICTION".into(),
                ));
            }

            Ok(SqlQuery::AlterReadySet(
                AlterReadysetStatement::SetEviction(SetEviction { limit, period }),
            ))
        } else if parse_readyset_keywords(
            parser,
            &[
                ReadysetKeyword::Standard(Keyword::CHANGE),
                ReadysetKeyword::UPSTREAM,
            ],
        ) {
            parser.expect_keyword(Keyword::TO)?;
            let url = parser.parse_literal_string()?;

            Ok(SqlQuery::AlterReadySet(
                AlterReadysetStatement::ChangeUpstream(ChangeUpstreamStatement { url }),
            ))
        } else if parse_readyset_keywords(
            parser,
            &[
                ReadysetKeyword::STOP,
                ReadysetKeyword::Standard(Keyword::REPLICATION),
            ],
        ) {
            Ok(SqlQuery::AlterReadySet(
                AlterReadysetStatement::StopReplication,
            ))
        } else if parse_readyset_keywords(
            parser,
            &[
                ReadysetKeyword::Standard(Keyword::START),
                ReadysetKeyword::Standard(Keyword::REPLICATION),
            ],
        ) {
            Ok(SqlQuery::AlterReadySet(
                AlterReadysetStatement::StartReplication,
            ))
        } else if parse_readyset_keywords(
            parser,
            &[
                ReadysetKeyword::Standard(Keyword::SET),
                ReadysetKeyword::Standard(Keyword::REPLICATION),
            ],
        ) {
            parser.expect_keyword(Keyword::POSITION)?;
            let position = parser.parse_literal_string()?;
            Ok(SqlQuery::AlterReadySet(
                AlterReadysetStatement::SetReplicationPosition(SetReplicationPositionStatement {
                    position,
                }),
            ))
        } else if parse_readyset_keywords(
            parser,
            &[
                ReadysetKeyword::Standard(Keyword::CHANGE),
                ReadysetKeyword::CDC,
            ],
        ) {
            parser.expect_keyword(Keyword::TO)?;
            let url = parser.parse_literal_string()?;
            Ok(SqlQuery::AlterReadySet(AlterReadysetStatement::ChangeCdc(
                ChangeCdcStatement { url },
            )))
        } else if parse_readyset_keywords(
            parser,
            &[
                ReadysetKeyword::Standard(Keyword::ADD),
                ReadysetKeyword::SHALLOW,
                ReadysetKeyword::Standard(Keyword::CACHE),
                ReadysetKeyword::ALLOWED,
            ],
        ) {
            parse_shallow_cache_allowlist_change(parser, true)
        } else if parse_readyset_keywords(
            parser,
            &[
                ReadysetKeyword::Standard(Keyword::DROP),
                ReadysetKeyword::SHALLOW,
                ReadysetKeyword::Standard(Keyword::CACHE),
                ReadysetKeyword::ALLOWED,
            ],
        ) {
            parse_shallow_cache_allowlist_change(parser, false)
        } else if parse_readyset_keywords(
            parser,
            &[
                ReadysetKeyword::Standard(Keyword::ADD),
                ReadysetKeyword::Standard(Keyword::USER),
            ],
        ) {
            let user = parser.parse_literal_string()?.into();
            parser.expect_keyword(Keyword::PASSWORD)?;
            let password = parser.parse_literal_string()?.into();
            Ok(SqlQuery::AlterReadySet(AlterReadysetStatement::AddUser(
                AddUserStatement { user, password },
            )))
        } else if parse_readyset_keywords(
            parser,
            &[
                ReadysetKeyword::Standard(Keyword::MODIFY),
                ReadysetKeyword::Standard(Keyword::USER),
            ],
        ) {
            let user = parser.parse_literal_string()?.into();
            parser.expect_keyword(Keyword::PASSWORD)?;
            let password = parser.parse_literal_string()?.into();
            Ok(SqlQuery::AlterReadySet(AlterReadysetStatement::ModifyUser(
                ModifyUserStatement { user, password },
            )))
        } else if parse_readyset_keywords(
            parser,
            &[
                ReadysetKeyword::Standard(Keyword::DROP),
                ReadysetKeyword::Standard(Keyword::USER),
            ],
        ) {
            let user = parser.parse_literal_string()?.into();
            Ok(SqlQuery::AlterReadySet(AlterReadysetStatement::DropUser(
                DropUserStatement { user },
            )))
        } else {
            Err(ReadysetParsingError::ReadysetParsingError(format!(
                "unexpected token after ALTER READYSET: {}",
                parser.peek_token()
            )))
        }
    } else if parse_readyset_keywords(parser, &[ReadysetKeyword::MCP, ReadysetKeyword::TOKEN]) {
        parse_alter_mcp_token_body(parser)
    } else {
        Ok(parser.parse_alter()?.try_into_dialect(dialect)?)
    }
}

/// Parse the tail of `ALTER READYSET {ADD | DROP} SHALLOW CACHE ALLOWED ...`:
/// the target-kind keyword (`FUNCTION`, `VARIABLE`, or `SCHEMA`) followed by a
/// comma-separated list of names. `add` is `true` for `ADD`, `false` for `DROP`.
fn parse_shallow_cache_allowlist_change(
    parser: &mut Parser,
    add: bool,
) -> Result<SqlQuery, ReadysetParsingError> {
    let kind = if parser.parse_keyword(Keyword::FUNCTION) {
        ShallowCacheAllowlistKind::Function
    } else if parser.parse_keyword(Keyword::VARIABLE) {
        ShallowCacheAllowlistKind::Variable
    } else if parser.parse_keyword(Keyword::SCHEMA) {
        ShallowCacheAllowlistKind::Schema
    } else {
        return Err(ReadysetParsingError::ReadysetParsingError(format!(
            "expected FUNCTION, VARIABLE, or SCHEMA after SHALLOW CACHE ALLOWED, got: {}",
            parser.peek_token()
        )));
    };
    let names = parser
        .parse_comma_separated(|p| p.parse_identifier())?
        .into_iter()
        .map(|id| id.value.into())
        .collect();
    Ok(SqlQuery::AlterReadySet(
        AlterReadysetStatement::ShallowCacheAllowlistChange(ShallowCacheAllowlistChange {
            kind,
            add,
            names,
        }),
    ))
}

/// Consume a duration unit keyword (`SECONDS`, `MILLISECONDS`, or `MS`) and turn `value` into a
/// [`Duration`]. `context` names the preceding clause for error messages (e.g. `"TTL"`,
/// `"REFRESH"`, `"COALESCE"`).
fn parse_duration_unit(
    parser: &mut Parser,
    value: u64,
    context: &str,
) -> Result<Duration, ReadysetParsingError> {
    if parser.parse_keyword(Keyword::SECONDS) {
        Ok(Duration::from_secs(value))
    } else if parser.parse_keyword(Keyword::MILLISECONDS)
        || parse_readyset_keyword(parser, ReadysetKeyword::MS)
    {
        Ok(Duration::from_millis(value))
    } else {
        Err(ReadysetParsingError::ReadysetParsingError(format!(
            "Expected SECONDS, MILLISECONDS, or MS after {context} duration"
        )))
    }
}

/// Parse `<uint> (SECONDS | MILLISECONDS | MS)` and return the corresponding [`Duration`].
fn parse_duration_with_unit(
    parser: &mut Parser,
    context: &str,
) -> Result<Duration, ReadysetParsingError> {
    let value = parser.parse_literal_uint().map_err(|_| {
        ReadysetParsingError::ReadysetParsingError(format!("couldn't parse {context} duration"))
    })?;
    parse_duration_unit(parser, value, context)
}

/// Parse the tail of `ALTER MCP TOKEN '<name>' SET (EXPIRES '<datetime>' | NEVER EXPIRES)`.
/// The `ALTER MCP TOKEN` keywords must already be consumed.
fn parse_alter_mcp_token_body(parser: &mut Parser) -> Result<SqlQuery, ReadysetParsingError> {
    let name = parser.parse_literal_string()?;
    parser.expect_keyword(Keyword::SET)?;

    let expires =
        if parse_readyset_keywords(parser, &[ReadysetKeyword::NEVER, ReadysetKeyword::EXPIRES]) {
            readyset_sql::ast::McpTokenExpiresChange::Never
        } else if parse_readyset_keyword(parser, ReadysetKeyword::EXPIRES) {
            let s = parser.parse_literal_string()?;
            readyset_sql::ast::McpTokenExpiresChange::At(s)
        } else {
            return Err(ReadysetParsingError::ReadysetParsingError(
                "expected EXPIRES or NEVER EXPIRES after SET".into(),
            ));
        };

    Ok(SqlQuery::AlterMcpToken(
        readyset_sql::ast::AlterMcpTokenStatement { name, expires },
    ))
}

/// A single parsed `CREATE CACHE` option, used internally before being applied to
/// [`CreateCacheOptions`].
enum CacheOptionKind {
    Always,
    UntilWrite,
    Concurrently,
    Policy(EvictionPolicy),
    Coalesce(Duration),
    Adaptive,
    /// `TOPK_BUFFER_MULTIPLIER = N`. Only accepted inside the `WITH (...)` umbrella.
    TopkBufferMultiplier(usize),
    /// `AUTOPARAM OFF` or `AUTOPARAM (EXCLUDE_JOINS, EXCLUDE_EXISTS)`. Suppresses
    /// autoparameterization for the named scope; not part of the public SQL reference.
    Autoparam(AutoparamControl),
}

/// Consume an unquoted identifier matching `name` (case-insensitive). Returns `true` and advances
/// the parser on a match; otherwise leaves the parser untouched and returns `false`.
fn consume_bare_ident(parser: &mut Parser, name: &str) -> bool {
    if let TokenWithSpan {
        token:
            Token::Word(Word {
                value,
                quote_style: None,
                ..
            }),
        ..
    } = parser.peek_token()
        && value.eq_ignore_ascii_case(name)
    {
        parser.next_token();
        return true;
    }
    false
}

/// Parse the `AUTOPARAM` option: `AUTOPARAM ON` (explicit default), `AUTOPARAM OFF` (suppress
/// autoparameterization entirely), or `AUTOPARAM (EXCLUDE_JOINS, EXCLUDE_EXISTS)` (suppress it for
/// literals originating in the named clause kinds). WITH-only, like `TOPK_BUFFER_MULTIPLIER` —
/// never accepted in the bare options form. The inner parens keep the scope-list commas from
/// colliding with the `WITH (...)` option separator. Returns `Ok(None)` (consuming nothing) when
/// the next token isn't `AUTOPARAM`.
fn parse_autoparam_option(
    parser: &mut Parser,
) -> Result<Option<AutoparamControl>, ReadysetParsingError> {
    if !consume_bare_ident(parser, "autoparam") {
        return Ok(None);
    }
    let mut ctrl = AutoparamControl::default();
    if consume_bare_ident(parser, "off") {
        ctrl.off = true;
        return Ok(Some(ctrl));
    }
    if consume_bare_ident(parser, "on") {
        // Explicit default (autoparameterize everything); lets generated DDL always emit an
        // AUTOPARAM clause rather than conditionally omitting it.
        return Ok(Some(ctrl));
    }
    parser.expect_token(&Token::LParen)?;
    loop {
        if parser.peek_token().token == Token::RParen {
            break;
        }
        if consume_bare_ident(parser, "exclude_joins") {
            ctrl.exclude_joins = true;
        } else if consume_bare_ident(parser, "exclude_exists") {
            ctrl.exclude_exists = true;
        } else if consume_bare_ident(parser, "exclude_subqueries") {
            ctrl.exclude_subqueries = true;
        } else {
            return Err(ReadysetParsingError::ReadysetParsingError(format!(
                "Unexpected token in AUTOPARAM (...): {}",
                parser.peek_token()
            )));
        }
        if !parser.consume_token(&Token::Comma) {
            break;
        }
    }
    parser.expect_token(&Token::RParen)?;
    if ctrl.is_default() {
        return Err(ReadysetParsingError::ReadysetParsingError(
            "AUTOPARAM requires OFF or a non-empty exclude list".into(),
        ));
    }
    Ok(Some(ctrl))
}

/// Try to consume `TOPK_BUFFER_MULTIPLIER = <uint>` from the parser. Returns `Ok(Some(value))`
/// on success, `Ok(None)` if the identifier doesn't match (parser is left untouched), and
/// `Err(_)` if the identifier matches but parsing fails after that point.
///
/// Only matches the bare (unquoted) identifier, so `"topk_buffer_multiplier"` and
/// backtick-quoted forms are not accepted — same convention as the other cache options.
fn try_parse_topk_buffer_multiplier(
    parser: &mut Parser,
) -> Result<Option<usize>, ReadysetParsingError> {
    let TokenWithSpan {
        token:
            Token::Word(Word {
                value,
                quote_style: None,
                ..
            }),
        ..
    } = parser.peek_token()
    else {
        return Ok(None);
    };
    if !value.eq_ignore_ascii_case("topk_buffer_multiplier") {
        return Ok(None);
    }
    parser.next_token();
    parser.expect_token(&Token::Eq)?;
    let value = parser.parse_literal_uint().map_err(|e| {
        ReadysetParsingError::ReadysetParsingError(format!(
            "TOPK_BUFFER_MULTIPLIER requires an unsigned integer literal: {e}"
        ))
    })?;
    let value = usize::try_from(value).map_err(|_| {
        ReadysetParsingError::ReadysetParsingError(format!(
            "TOPK_BUFFER_MULTIPLIER value {value} does not fit in usize"
        ))
    })?;
    Ok(Some(value))
}

/// Parse a single cache option (without leading `WITH (` or comma separators).
/// Returns `None` if the next token doesn't begin a recognized option.
fn parse_single_cache_option(
    parser: &mut Parser,
) -> Result<Option<CacheOptionKind>, ReadysetParsingError> {
    if parse_readyset_keyword(parser, ReadysetKeyword::POLICY) {
        if !parse_readyset_keyword(parser, ReadysetKeyword::TTL) {
            return Err(ReadysetParsingError::ReadysetParsingError(
                "Expected TTL after POLICY".into(),
            ));
        }
        let ttl = parse_duration_with_unit(parser, "TTL")?;

        let policy = if parse_readyset_keyword(parser, ReadysetKeyword::REFRESH) {
            let scheduled_refresh = parser.parse_keyword(Keyword::EVERY);
            match parser.parse_literal_uint() {
                Ok(value) => {
                    let refresh = parse_duration_unit(parser, value, "REFRESH")?;
                    if refresh >= ttl {
                        return Err(ReadysetParsingError::ReadysetParsingError(
                            "REFRESH period must be less than TTL".into(),
                        ));
                    }
                    EvictionPolicy::TtlAndPeriod {
                        ttl,
                        refresh,
                        schedule: scheduled_refresh,
                    }
                }
                _ => {
                    parser.prev_token();
                    EvictionPolicy::Ttl { ttl }
                }
            }
        } else {
            EvictionPolicy::Ttl { ttl }
        };
        Ok(Some(CacheOptionKind::Policy(policy)))
    } else if parser.parse_keyword(Keyword::COALESCE) {
        Ok(Some(CacheOptionKind::Coalesce(parse_duration_with_unit(
            parser, "COALESCE",
        )?)))
    } else if parser.parse_keyword(Keyword::ALWAYS) {
        Ok(Some(CacheOptionKind::Always))
    } else if parser.parse_keyword(Keyword::UNTIL) {
        if !parser.parse_keyword(Keyword::WRITE) {
            return Err(ReadysetParsingError::ReadysetParsingError(
                "expected WRITE after UNTIL".into(),
            ));
        }
        Ok(Some(CacheOptionKind::UntilWrite))
    } else if parser.parse_keyword(Keyword::CONCURRENTLY) {
        Ok(Some(CacheOptionKind::Concurrently))
    } else if parse_readyset_keyword(parser, ReadysetKeyword::ADAPTIVE) {
        Ok(Some(CacheOptionKind::Adaptive))
    } else {
        Ok(None)
    }
}

/// Apply a parsed option to the accumulator. Errors on duplicates and on shallow-only options
/// used with a non-shallow cache.
fn apply_cache_option(
    opts: &mut CreateCacheOptions,
    opt: CacheOptionKind,
    cache_type: Option<CacheType>,
) -> Result<(), ReadysetParsingError> {
    match opt {
        CacheOptionKind::Always => {
            if !matches!(opts.trx_cache_policy, TrxCachePolicy::Never) {
                return Err(ReadysetParsingError::ReadysetParsingError(
                    "transaction cache policy specified more than once".into(),
                ));
            }
            opts.trx_cache_policy = TrxCachePolicy::Always;
        }
        CacheOptionKind::UntilWrite => {
            if !matches!(opts.trx_cache_policy, TrxCachePolicy::Never) {
                return Err(ReadysetParsingError::ReadysetParsingError(
                    "transaction cache policy specified more than once".into(),
                ));
            }
            opts.trx_cache_policy = TrxCachePolicy::UntilWrite;
        }
        CacheOptionKind::Concurrently => {
            if std::mem::replace(&mut opts.concurrently, true) {
                return Err(ReadysetParsingError::ReadysetParsingError(
                    "CONCURRENTLY specified more than once".into(),
                ));
            }
        }
        CacheOptionKind::Policy(policy) => {
            if cache_type == Some(CacheType::Deep) {
                return Err(ReadysetParsingError::ReadysetParsingError(
                    "DEEP caches do not support caching policies".into(),
                ));
            }
            if opts.policy.replace(policy).is_some() {
                return Err(ReadysetParsingError::ReadysetParsingError(
                    "POLICY specified more than once".into(),
                ));
            }
        }
        CacheOptionKind::Coalesce(duration) => {
            if cache_type == Some(CacheType::Deep) {
                return Err(ReadysetParsingError::ReadysetParsingError(
                    "COALESCE is not supported for DEEP caches".into(),
                ));
            }
            if opts.coalesce_ms.replace(duration).is_some() {
                return Err(ReadysetParsingError::ReadysetParsingError(
                    "COALESCE specified more than once".into(),
                ));
            }
        }
        CacheOptionKind::Adaptive => {
            if cache_type == Some(CacheType::Deep) {
                return Err(ReadysetParsingError::ReadysetParsingError(
                    "ADAPTIVE is not supported for DEEP caches".into(),
                ));
            }
            if std::mem::replace(&mut opts.adaptive, true) {
                return Err(ReadysetParsingError::ReadysetParsingError(
                    "ADAPTIVE specified more than once".into(),
                ));
            }
        }
        CacheOptionKind::TopkBufferMultiplier(value) => {
            if cache_type == Some(CacheType::Shallow) {
                return Err(ReadysetParsingError::ReadysetParsingError(
                    "TOPK_BUFFER_MULTIPLIER is not supported for SHALLOW caches".into(),
                ));
            }
            if opts.topk_buffer_multiplier.replace(value).is_some() {
                return Err(ReadysetParsingError::ReadysetParsingError(
                    "TOPK_BUFFER_MULTIPLIER specified more than once".into(),
                ));
            }
        }
        CacheOptionKind::Autoparam(ctrl) => {
            if !opts.autoparam.is_default() {
                return Err(ReadysetParsingError::ReadysetParsingError(
                    "AUTOPARAM specified more than once".into(),
                ));
            }
            opts.autoparam = ctrl;
        }
    }
    Ok(())
}

/// Canonicalize options that have semantically-equivalent representations. Called after all
/// options are applied so duplicate detection isn't confused by intermediate values.
fn normalize_cache_options(opts: &mut CreateCacheOptions) {
    // `TOPK_BUFFER_MULTIPLIER = 1` is identical to the default (`buffered = k`). Normalize to
    // `None` so two statements that differ only in `Some(1)` vs `None` hash/compare equal.
    if opts.topk_buffer_multiplier == Some(1) {
        opts.topk_buffer_multiplier = None;
    }
}

/// Parse the legacy bare options (`[POLICY TTL ...] [COALESCE ...] [ALWAYS] [CONCURRENTLY]`) that
/// precede the cache name. The `CREATE [DEEP|SHALLOW] CACHE` keywords must already be consumed.
/// Returns the default (no options) when none are present.
fn parse_bare_cache_options(
    parser: &mut Parser,
    cache_type: Option<CacheType>,
) -> Result<CreateCacheOptions, ReadysetParsingError> {
    let mut opts = CreateCacheOptions {
        cache_type,
        ..Default::default()
    };
    while let Some(opt) = parse_single_cache_option(parser)? {
        apply_cache_option(&mut opts, opt, cache_type)?;
    }
    Ok(opts)
}

/// If the next token is `WITH`, parse a `WITH ( option [, option]... )` clause into `opts` and
/// return `true`. Returns `false` (consuming nothing) when there is no WITH clause. Empty
/// `WITH ()` is accepted as "no options", so callers generating SQL can always emit
/// `WITH (<options>)` without special-casing zero options.
fn parse_with_cache_clause(
    parser: &mut Parser,
    cache_type: Option<CacheType>,
    opts: &mut CreateCacheOptions,
) -> Result<bool, ReadysetParsingError> {
    if !parser.parse_keyword(Keyword::WITH) {
        return Ok(false);
    }
    parser.expect_token(&Token::LParen)?;
    loop {
        if parser.peek_token().token == Token::RParen {
            break;
        }
        // WITH-only knobs first, then fall back to the shared legacy-option parser.
        let opt = if let Some(value) = try_parse_topk_buffer_multiplier(parser)? {
            CacheOptionKind::TopkBufferMultiplier(value)
        } else if let Some(ctrl) = parse_autoparam_option(parser)? {
            CacheOptionKind::Autoparam(ctrl)
        } else {
            parse_single_cache_option(parser)?.ok_or_else(|| {
                ReadysetParsingError::ReadysetParsingError(format!(
                    "Unexpected token in WITH clause: {}",
                    parser.peek_token()
                ))
            })?
        };
        apply_cache_option(opts, opt, cache_type)?;
        if !parser.consume_token(&Token::Comma) {
            break;
        }
    }
    parser.expect_token(&Token::RParen)?;
    Ok(true)
}

/// Whether any cache option is set, used to reject combining the bare form with a `WITH (...)`
/// clause on the same statement.
fn cache_options_present(opts: &CreateCacheOptions) -> bool {
    opts.policy.is_some()
        || opts.coalesce_ms.is_some()
        || opts.adaptive
        || opts.concurrently
        || !matches!(opts.trx_cache_policy, TrxCachePolicy::Never)
        || opts.topk_buffer_multiplier.is_some()
        || !opts.autoparam.is_default()
}

/// Expects `CREATE CACHE` was already parsed. Attempts to parse a Readyset-specific create cache
/// statement. Will simply error if it fails, since there's no relevant standard SQL to fall back
/// to.
///
/// CREATE [type] CACHE
///     [POLICY TTL <num> SECONDS [REFRESH [EVERY] <num> SECONDS] [COALESCE <num> SECONDS]]
///     [cache_option [, cache_option] ...]
///     [<name>]
///     FROM
///     [<SELECT statement> | <query id>]
///
/// type:
///     | DEEP
///     | SHALLOW
///     | // empty -> default cache type (default default is DEEP)
///
/// cache_options:
///     | ALWAYS
///     | CONCURRENTLY
/// Parse the shared head of a `CREATE [DEEP|SHALLOW] CACHE` construct: the cache-type keywords,
/// the legacy bare options, the optional `[<name>]`, and the optional `WITH (...)` umbrella. The
/// `FROM <query>` tail is statement-only and parsed by the caller; hint directives have no `FROM`.
/// Keeping this in one place means new option or name syntax only has to be added once, rather
/// than duplicated between the DDL and hint parsers.
fn parse_cache_options(
    parser: &mut Parser,
    dialect: Dialect,
) -> Result<CreateCacheOptions, ReadysetParsingError> {
    let cache_type = parse_create_cache_keywords(parser)?;
    // Legacy bare options come before the name; the `WITH (...)` umbrella comes after it.
    let mut opts = parse_bare_cache_options(parser, cache_type)?;
    let bare_present = cache_options_present(&opts);

    // Optional cache name: absent when the next token is FROM (no name) or WITH (the umbrella,
    // which follows the name slot).
    opts.name = if parser.peek_keyword(Keyword::FROM) || parser.peek_keyword(Keyword::WITH) {
        None
    } else {
        parser
            .parse_object_name(false)
            .ok()
            .try_into_dialect(dialect)?
    };

    // Optional WITH (...) umbrella, after the name. Mutually exclusive with the bare form.
    if parse_with_cache_clause(parser, cache_type, &mut opts)? && bare_present {
        return Err(ReadysetParsingError::ReadysetParsingError(
            "CREATE CACHE cannot combine bare options with a WITH (...) clause".into(),
        ));
    }
    normalize_cache_options(&mut opts);
    Ok(opts)
}

fn parse_create_cache(
    parser: &mut Parser,
    dialect: Dialect,
    input: impl AsRef<str>,
) -> Result<SqlQuery, ReadysetParsingError> {
    let opts = parse_cache_options(parser, dialect)?;
    parser.expect_keyword(Keyword::FROM)?;

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

    let inner = parse_query_for_create_cache(parser, dialect, remaining_query);
    Ok(SqlQuery::CreateCache(CreateCacheStatement {
        name: opts.name,
        cache_type: opts.cache_type,
        policy: opts.policy,
        coalesce_ms: opts.coalesce_ms,
        adaptive: opts.adaptive,
        inner,
        unparsed_create_cache_statement: Some(input.as_ref().trim().to_string()),
        trx_cache_policy: opts.trx_cache_policy,
        concurrently: opts.concurrently,
        topk_buffer_multiplier: opts.topk_buffer_multiplier,
        autoparam: opts.autoparam,
    }))
}

/// Parse a readyset hint text (without `/*rs+` and `*/` markers) into a directive.
///
/// Dispatches to the appropriate parser based on the first keyword:
/// - `CREATE [DEEP|SHALLOW] CACHE ...` → [`ReadysetHintDirective::CreateCache`]
/// - `SKIP CACHE` → [`ReadysetHintDirective::SkipCache`]
///
/// Returns `Ok(None)` for unrecognized directives (forward-compatible).
/// Returns `Err(...)` for recognized but malformed directives.
pub fn parse_hint_directive(
    dialect: Dialect,
    text: &str,
) -> Result<Option<ReadysetHintDirective>, ReadysetParsingError> {
    let text = text.trim();
    if text.is_empty() {
        return Ok(None);
    }

    let sqlparser_dialect = sqlparser_dialect_from_readyset_dialect(dialect);
    let mut parser = Parser::new(sqlparser_dialect.as_ref())
        .try_with_sql(text)
        .map_err(|e| ReadysetParsingError::ReadysetParsingError(e.to_string()))?;

    let directive = if parser.parse_keywords(&[Keyword::SKIP, Keyword::CACHE]) {
        Some(ReadysetHintDirective::SkipCache)
    } else if parser.peek_keyword(Keyword::CREATE) {
        // A hint has no FROM (the query is the statement the hint annotates), so we parse the same
        // head as `CREATE CACHE` DDL and stop; anything left over trips the trailing-token check
        // below. Hints create shallow caches only.
        let opts = parse_cache_options(&mut parser, dialect)?;
        if opts.cache_type == Some(CacheType::Deep) {
            return Err(ReadysetParsingError::ReadysetParsingError(
                "DEEP caches are not supported in a hint; hints create shallow caches only".into(),
            ));
        }
        Some(ReadysetHintDirective::CreateCache(opts))
    } else {
        None
    };

    if directive.is_some() && parser.peek_token() != Token::EOF {
        return Err(ReadysetParsingError::ReadysetParsingError(format!(
            "Unexpected token in hint: {}",
            parser.peek_token()
        )));
    }

    Ok(directive)
}

fn peek_create_cache(parser: &mut Parser) -> bool {
    let backup = |parser: &mut Parser<'_>, n| {
        for _ in 0..n {
            parser.prev_token();
        }
    };

    if !parser.parse_keyword(Keyword::CREATE) {
        return false;
    }

    if parser.parse_keyword(Keyword::CACHE) {
        backup(parser, 2);
        return true;
    }

    if parse_readyset_keyword(parser, ReadysetKeyword::DEEP)
        || parse_readyset_keyword(parser, ReadysetKeyword::SHALLOW)
    {
        if parser.parse_keyword(Keyword::CACHE) {
            backup(parser, 3);
            return true;
        }
        backup(parser, 1);
    }

    backup(parser, 1);
    false
}

fn parse_optional_cache_type(parser: &mut Parser) -> Option<CacheType> {
    if parse_readyset_keyword(parser, ReadysetKeyword::DEEP) {
        Some(CacheType::Deep)
    } else if parse_readyset_keyword(parser, ReadysetKeyword::SHALLOW) {
        Some(CacheType::Shallow)
    } else {
        None
    }
}

/// Peek ahead to see if the next statement is `CREATE MCP TOKEN`. Restores the
/// parser position before returning.
fn peek_create_mcp_token(parser: &mut Parser) -> bool {
    let backup = |parser: &mut Parser<'_>, n| {
        for _ in 0..n {
            parser.prev_token();
        }
    };

    if !parser.parse_keyword(Keyword::CREATE) {
        return false;
    }
    if !parse_readyset_keyword(parser, ReadysetKeyword::MCP) {
        backup(parser, 1);
        return false;
    }
    if !parse_readyset_keyword(parser, ReadysetKeyword::TOKEN) {
        backup(parser, 2);
        return false;
    }
    backup(parser, 3);
    true
}

/// Parse a scope keyword: read_only | cache_admin | full.
fn parse_mcp_scope(
    parser: &mut Parser,
) -> Result<readyset_sql::ast::McpTokenScope, ReadysetParsingError> {
    let ident = parser.parse_identifier()?;
    match ident.value.to_ascii_lowercase().as_str() {
        "read_only" => Ok(readyset_sql::ast::McpTokenScope::ReadOnly),
        "cache_admin" => Ok(readyset_sql::ast::McpTokenScope::CacheAdmin),
        "full" => Ok(readyset_sql::ast::McpTokenScope::Full),
        other => Err(ReadysetParsingError::ReadysetParsingError(format!(
            "expected scope read_only, cache_admin, or full, got {other}"
        ))),
    }
}

/// Parse `CREATE MCP TOKEN '<name>' [WITH SCOPE <scope>] [EXPIRES '<datetime>']`.
/// The CREATE keyword must not yet be consumed.
fn parse_create_mcp_token(parser: &mut Parser) -> Result<SqlQuery, ReadysetParsingError> {
    if !parser.parse_keyword(Keyword::CREATE) {
        return Err(ReadysetParsingError::ReadysetParsingError(
            "expected CREATE".into(),
        ));
    }
    if !parse_readyset_keyword(parser, ReadysetKeyword::MCP) {
        return Err(ReadysetParsingError::ReadysetParsingError(
            "expected MCP after CREATE".into(),
        ));
    }
    if !parse_readyset_keyword(parser, ReadysetKeyword::TOKEN) {
        return Err(ReadysetParsingError::ReadysetParsingError(
            "expected TOKEN after CREATE MCP".into(),
        ));
    }
    let name = parser.parse_literal_string()?;

    let scope = if parser.parse_keyword(Keyword::WITH) {
        if !parse_readyset_keyword(parser, ReadysetKeyword::SCOPE) {
            return Err(ReadysetParsingError::ReadysetParsingError(
                "expected SCOPE after WITH".into(),
            ));
        }
        Some(parse_mcp_scope(parser)?)
    } else {
        None
    };

    let expires = if parse_readyset_keyword(parser, ReadysetKeyword::EXPIRES) {
        Some(parser.parse_literal_string()?)
    } else {
        None
    };

    Ok(SqlQuery::CreateMcpToken(
        readyset_sql::ast::CreateMcpTokenStatement {
            name,
            scope,
            expires,
        },
    ))
}

fn parse_create_cache_keywords(
    parser: &mut Parser,
) -> Result<Option<CacheType>, ReadysetParsingError> {
    if !parser.parse_keyword(Keyword::CREATE) {
        return Err(ReadysetParsingError::ReadysetParsingError(
            "Expected CREATE".into(),
        ));
    }

    let cache_type = parse_optional_cache_type(parser);

    if !parser.parse_keyword(Keyword::CACHE) {
        return Err(ReadysetParsingError::ReadysetParsingError(
            "Expected CACHE after CREATE [DEEP|SHALLOW]".into(),
        ));
    }

    Ok(cache_type)
}

fn parse_query_for_create_cache(
    parser: &mut Parser,
    dialect: Dialect,
    remaining_query: String,
) -> CacheInner {
    // Try to parse as statement first
    if let Ok(statement) = parser.try_parse(|p| p.parse_statement()) {
        let shallow = if let sqlparser::ast::Statement::Query(ref query) = statement {
            let mut sq: ShallowCacheQuery = (*query.clone()).into();
            sq.take_hints();
            Ok(Box::new(sq))
        } else {
            Err(remaining_query.clone())
        };
        let deep = statement
            .try_into_dialect(dialect)
            .ok()
            .and_then(|query: SqlQuery| query.into_select())
            .map(Box::new)
            .ok_or(remaining_query);

        return CacheInner::Statement { deep, shallow };
    }

    // Otherwise, try to parse as identifier
    parser
        .parse_identifier()
        .map(|id| CacheInner::Id(id.into_dialect(dialect)))
        .unwrap_or_else(|_| CacheInner::Statement {
            deep: Err(remaining_query.clone()),
            shallow: Err(remaining_query),
        })
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
        let for_cache = if parser.parse_keywords(&[Keyword::FOR, Keyword::CACHE]) {
            Some(parser.parse_object_name(false)?.try_into_dialect(dialect)?)
        } else {
            None
        };
        return Ok(SqlQuery::Explain(
            readyset_sql::ast::ExplainStatement::Materializations { for_cache },
        ));
    }
    let simplified = parse_readyset_keyword(parser, ReadysetKeyword::SIMPLIFIED);
    if parser.parse_keyword(Keyword::GRAPHVIZ) {
        let for_cache = if parser.parse_keywords(&[Keyword::FOR, Keyword::CACHE]) {
            Some(parser.parse_object_name(false)?.try_into_dialect(dialect)?)
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
    if parser.peek_keyword(Keyword::CREATE) {
        return match parse_create_cache(parser, dialect, input)? {
            SqlQuery::CreateCache(inner) => {
                return Ok(SqlQuery::Explain(
                    readyset_sql::ast::ExplainStatement::CreateCache {
                        inner: inner.inner,
                        cache_type: inner.cache_type,
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

fn parse_show_caches(
    parser: &mut Parser,
    cache_type: Option<CacheType>,
) -> Result<SqlQuery, ReadysetParsingError> {
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
        readyset_sql::ast::ShowStatement::CachedQueries(cache_type, query_id),
    ))
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
///     | [DEEP|SHALLOW] CACHES
///     | PROXIED [SUPPORTED] [DEEP|SHALLOW] QUERIES [WHERE query_id = <query_id>] [LIMIT <n>]
///     | REPLAY PATHS
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
        } else if parse_readyset_keywords(
            parser,
            &[
                ReadysetKeyword::RSA,
                ReadysetKeyword::Standard(Keyword::PUBLIC),
                ReadysetKeyword::Standard(Keyword::KEY),
            ],
        ) {
            Ok(SqlQuery::Show(
                readyset_sql::ast::ShowStatement::ReadySetRsaPublicKey,
            ))
        } else {
            Err(ReadysetParsingError::ReadysetParsingError(
                "expected VERSION, STATUS, TABLES, ALL TABLES, \
                 MIGRATION STATUS, or RSA PUBLIC KEY after READYSET"
                    .into(),
            ))
        }
    } else if parse_readyset_keyword(parser, ReadysetKeyword::PROXIED) {
        let only_supported = parse_readyset_keyword(parser, ReadysetKeyword::SUPPORTED);
        let cache_type = parse_optional_cache_type(parser);
        if !parse_readyset_keyword(parser, ReadysetKeyword::QUERIES) {
            return Err(ReadysetParsingError::ReadysetParsingError(
                "expected QUERIES after PROXIED [SUPPORTED] [DEEP|SHALLOW]".into(),
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
                    cache_type,
                },
            ),
        ))
    } else if parse_readyset_keyword(parser, ReadysetKeyword::CACHES) {
        parse_show_caches(parser, None)
    } else if parse_readyset_keywords(parser, &[ReadysetKeyword::DEEP, ReadysetKeyword::CACHES]) {
        parse_show_caches(parser, Some(CacheType::Deep))
    } else if parse_readyset_keywords(parser, &[ReadysetKeyword::SHALLOW, ReadysetKeyword::CACHES])
    {
        parse_show_caches(parser, Some(CacheType::Shallow))
    } else if parse_readyset_keywords(
        parser,
        &[
            ReadysetKeyword::SHALLOW,
            ReadysetKeyword::Standard(Keyword::CACHE),
            ReadysetKeyword::ENTRIES,
        ],
    ) {
        let query_id = if parser.parse_keyword(Keyword::WHERE) {
            let lhs = parser.parse_identifier()?;
            if lhs.value != "query_id" {
                return Err(ReadysetParsingError::ReadysetParsingError(format!(
                    "expected 'query_id' after WHERE, found '{}'",
                    lhs.value
                )));
            }
            parser.expect_token(&Token::Eq)?;
            // Use parse_literal_string to match nom-sql's string_literal() behavior
            Some(parser.parse_literal_string()?)
        } else {
            None
        };
        let limit = if parser.parse_keyword(Keyword::LIMIT) {
            Some(parser.parse_literal_uint()?)
        } else {
            None
        };
        Ok(SqlQuery::Show(
            readyset_sql::ast::ShowStatement::ShallowCacheEntries { query_id, limit },
        ))
    } else if parse_readyset_keywords(
        parser,
        &[
            ReadysetKeyword::SHALLOW,
            ReadysetKeyword::Standard(Keyword::CACHE),
            ReadysetKeyword::ALLOWED,
        ],
    ) {
        let kind = if parser.parse_keyword(Keyword::FUNCTIONS) {
            ShallowCacheAllowlistKind::Function
        } else if parser.parse_keyword(Keyword::VARIABLES) {
            ShallowCacheAllowlistKind::Variable
        } else if parser.parse_keyword(Keyword::SCHEMAS) {
            ShallowCacheAllowlistKind::Schema
        } else {
            return Err(ReadysetParsingError::ReadysetParsingError(format!(
                "expected FUNCTIONS, VARIABLES, or SCHEMAS after SHOW SHALLOW CACHE ALLOWED, got: {}",
                parser.peek_token()
            )));
        };
        Ok(SqlQuery::Show(
            readyset_sql::ast::ShowStatement::ShallowCacheAllowlist(kind),
        ))
    } else if parse_readyset_keywords(parser, &[ReadysetKeyword::REPLAY, ReadysetKeyword::PATHS]) {
        Ok(SqlQuery::Show(
            readyset_sql::ast::ShowStatement::ReplayPaths,
        ))
    } else if parse_readyset_keywords(parser, &[ReadysetKeyword::MCP, ReadysetKeyword::TOKENS]) {
        Ok(SqlQuery::Show(readyset_sql::ast::ShowStatement::McpTokens))
    } else {
        Ok(parser.parse_show()?.try_into_dialect(dialect)?)
    }
}

/// Expects `DROP` was already parsed. Attempts to parse a Readyset-specific drop statement,
/// otherwise falls back to [`Parser::parse_drop`].
///
/// DROP
///   | ALL PROXIED QUERIES
///   | ALL [DEEP|SHALLOW] CACHES
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
    } else if parser.parse_keyword(Keyword::ALL) {
        let cache_type = parse_optional_cache_type(parser);
        if !parse_readyset_keyword(parser, ReadysetKeyword::CACHES) {
            return Err(ReadysetParsingError::ReadysetParsingError(
                "expected CACHES".into(),
            ));
        }
        Ok(SqlQuery::DropAllCaches(
            readyset_sql::ast::DropAllCachesStatement { cache_type },
        ))
    } else if parser.parse_keyword(Keyword::CACHE) {
        let name = parser.parse_object_name(false)?.try_into_dialect(dialect)?;
        Ok(SqlQuery::DropCache(DropCacheStatement { name }))
    } else if parse_readyset_keywords(parser, &[ReadysetKeyword::MCP, ReadysetKeyword::TOKEN]) {
        let name = parser.parse_literal_string()?;
        Ok(SqlQuery::DropMcpToken(
            readyset_sql::ast::DropMcpTokenStatement { name },
        ))
    } else {
        Ok(parser.parse_drop()?.try_into_dialect(dialect)?)
    }
}

/// Expects `FLUSH` was already consumed. Parses a Readyset-specific flush statement.
///
/// FLUSH ALL SHALLOW CACHES
/// FLUSH CACHE <name>
fn parse_flush(parser: &mut Parser, dialect: Dialect) -> Result<SqlQuery, ReadysetParsingError> {
    if parse_readyset_keywords(
        parser,
        &[
            ReadysetKeyword::Standard(Keyword::ALL),
            ReadysetKeyword::SHALLOW,
            ReadysetKeyword::CACHES,
        ],
    ) {
        Ok(SqlQuery::FlushAllShallowCaches(
            FlushAllShallowCachesStatement,
        ))
    } else if parser.parse_keyword(Keyword::CACHE) {
        let name = parser.parse_object_name(false)?.try_into_dialect(dialect)?;
        Ok(SqlQuery::FlushCache(FlushCacheStatement { name }))
    } else {
        Err(ReadysetParsingError::ReadysetParsingError(
            "expected ALL SHALLOW CACHES or CACHE after FLUSH".into(),
        ))
    }
}

/// Parse a SQL statement as a bare sqlparser AST [`sqlparser::ast::Query`],
/// without converting to the Readyset AST, extracting and stripping any
/// `/*rs+ ... */` hint directive.
///
/// This is intended for shallow-cache matching and should be preferred over calling sqlparser
/// directly outside this crate.
///
/// Returns the hint-stripped query (for stable QueryId) and the parsed directive if present.
pub fn parse_shallow_query(
    dialect: Dialect,
    input: impl AsRef<str>,
) -> Result<(ShallowCacheQuery, Option<ReadysetHintDirective>), ReadysetParsingError> {
    let mut query: ShallowCacheQuery =
        parse_sqlparser_inner(dialect, input, |parser, _dialect, _input| {
            match parser.parse_query() {
                Ok(query) => match *query.body {
                    SetExpr::Select(..) | SetExpr::Query(..) | SetExpr::SetOperation { .. } => {
                        Ok((*query).into())
                    }
                    SetExpr::Values(..)
                    | SetExpr::Insert(..)
                    | SetExpr::Update(..)
                    | SetExpr::Delete(..)
                    | SetExpr::Merge(..)
                    | SetExpr::Table(..) => Err(ReadysetParsingError::ReadysetParsingError(
                        format!("Expected SELECT, but got: {query}"),
                    )),
                },
                Err(e) => Err(ReadysetParsingError::SqlparserError(e)),
            }
        })?;
    let hint = query.take_hints();
    let directive = match hint {
        Some(h) => match parse_hint_directive(dialect, &h.text) {
            Ok(d) => d,
            Err(e) => {
                tracing::warn!(error = %e, "Malformed readyset hint directive, ignoring");
                None
            }
        },
        None => None,
    };
    Ok((query, directive))
}

/// Attempts to parse a Readyset-specific statement, and falls back to [`Parser::parse_statement`] if it fails.
fn parse_readyset_query(
    parser: &mut Parser,
    dialect: Dialect,
    input: &str,
) -> Result<SqlQuery, ReadysetParsingError> {
    if parser.parse_keyword(Keyword::ALTER) {
        parse_alter(parser, dialect)
    } else if peek_create_mcp_token(parser) {
        parse_create_mcp_token(parser)
    } else if peek_create_cache(parser) {
        parse_create_cache(parser, dialect, input)
    } else if parser.parse_keyword(Keyword::DROP) {
        parse_drop(parser, dialect)
    } else if parser.parse_keyword(Keyword::EXPLAIN) {
        parse_explain(parser, dialect, input)
    } else if parser.parse_keyword(Keyword::SHOW) {
        parse_show(parser, dialect)
    } else if parser.parse_keyword(Keyword::FLUSH) {
        parse_flush(parser, dialect)
    } else if let Some(query) = parse_reset_session_authorization(parser)? {
        // `RESET SESSION AUTHORIZATION` is not handled by the upstream `parse_statement`, so we
        // intercept it here and model it as the `DEFAULT` authorization.
        Ok(query)
    } else if let Some(query) = parse_set_local_session_authorization(parser, dialect)? {
        // `SET LOCAL SESSION AUTHORIZATION ...` requires two scope keywords, which the upstream
        // `parse_set` does not accept; the bare `SET SESSION AUTHORIZATION ...` form is handled
        // natively and flows through `parse_statement` below.
        Ok(query)
    } else {
        Ok(parser.parse_statement()?.try_into_dialect(dialect)?)
    }
}

/// Parse `RESET SESSION AUTHORIZATION`, modeling it as `SET SESSION AUTHORIZATION DEFAULT`.
///
/// Returns `None` (without consuming tokens) when the input is not this statement, so other
/// `RESET` forms fall through to the default parser.
fn parse_reset_session_authorization(
    parser: &mut Parser,
) -> Result<Option<SqlQuery>, ReadysetParsingError> {
    let parsed = parser.maybe_parse(|parser| {
        if parser.parse_keywords(&[Keyword::RESET, Keyword::SESSION, Keyword::AUTHORIZATION]) {
            Ok(true)
        } else {
            parser.expected("RESET SESSION AUTHORIZATION", parser.peek_token())
        }
    })?;
    Ok(parsed.map(|_| {
        SqlQuery::Set(SetStatement::SessionAuthorization(
            SetSessionAuthorization {
                local: false,
                value: SessionAuthorizationValue::Default,
            },
        ))
    }))
}

/// Parse `SET LOCAL SESSION AUTHORIZATION { user | DEFAULT }`.
///
/// Returns `None` (without consuming tokens) when the input is not this statement, so other `SET`
/// forms fall through to the default parser.
fn parse_set_local_session_authorization(
    parser: &mut Parser,
    dialect: Dialect,
) -> Result<Option<SqlQuery>, ReadysetParsingError> {
    let value = parser.maybe_parse(|parser| {
        if !parser.parse_keywords(&[
            Keyword::SET,
            Keyword::LOCAL,
            Keyword::SESSION,
            Keyword::AUTHORIZATION,
        ]) {
            return parser.expected("SET LOCAL SESSION AUTHORIZATION", parser.peek_token());
        }
        if parser.parse_keyword(Keyword::DEFAULT) {
            Ok(SessionAuthorizationValue::Default)
        } else {
            let ident = parser.parse_identifier()?;
            Ok(SessionAuthorizationValue::User(ident.into_dialect(dialect)))
        }
    })?;
    Ok(value.map(|value| {
        SqlQuery::Set(SetStatement::SessionAuthorization(
            SetSessionAuthorization { local: true, value },
        ))
    }))
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
                        "AST mismatch (left = nom-sql, right = sqlparser-rs) for {} input: {:?}",
                        dialect,
                        input.as_ref()
                    );
                }
                if config.log_on_mismatch
                    && (!config.log_only_selects
                        || (is_not_query_or_should_log(&nom_ast)
                            || is_not_query_or_should_log(&sqlparser_ast)))
                {
                    rate_limit(
                        config.rate_limit_logging,
                        PARSING_LOG_PARSING_MISMATCH_SQLPARSER_FAILED,
                        || {
                            tracing::warn!(
                                ?dialect,
                                input = %input.as_ref(),
                                ?nom_ast,
                                ?sqlparser_ast,
                                "nom-sql AST differs from sqlparser-rs AST",
                            )
                        },
                    );
                }
                if config.error_on_mismatch {
                    Err(ReadysetParsingError::ReadysetParsingError(format!(
                        "AST mismatch (left = nom-sql, right = sqlparser-rs) for {} input: {:?}\n{}",
                        dialect,
                        input.as_ref(),
                        pretty_assertions::Comparison::new(&nom_ast, &sqlparser_ast),
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
                        rate_limit(
                            config.rate_limit_logging,
                            PARSING_LOG_PARSING_MISMATCH_SQLPARSER_FAILED,
                            || {
                                tracing::warn!(
                                    ?dialect,
                                    input = %input.as_ref(),
                                    %sqlparser_error,
                                    ?nom_ast,
                                    "sqlparser-rs failed but nom-sql succeeded"
                                )
                            },
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
                    rate_limit(
                        config.rate_limit_logging,
                        PARSING_LOG_PARSING_MISMATCH_SQLPARSER_FAILED,
                        || {
                            tracing::debug!(
                                ?dialect,
                                input = %input.as_ref(),
                                %nom_error,
                                ?sqlparser_ast,
                                "nom-sql failed but sqlparser-rs succeeded"
                            )
                        },
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
    ($(#[doc = $doc:expr])+ $parser:ident, $ty:ty) => {
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

/// Parse a SQL query, including custom Readyset extension.
///
/// This is a custom implementation (rather than using `export_parser!`) because we need to merge
/// `shallow_ast` from the sqlparser result into the nom result for `CreateCacheStatement` when
/// both parsers succeed. This ensures `shallow_ast` is available even when nom is preferred.
///
/// We similarly adopt sqlparser's `deep` AST into the nom result when nom failed to parse the
/// inner SELECT of a `CREATE DEEP CACHE` but sqlparser succeeded. Unlike `shallow` (which is
/// always produced by sqlparser and so overwrites unconditionally), `deep` is genuinely parsed
/// by both sides, so we only upgrade on asymmetric failure — this preserves the parity harness's
/// ability to catch real `Ok`-vs-`Ok` disagreements on `SelectStatement`. The upgrade is further
/// gated on `config.prefer_sqlparser` to match the top-level `(Err nom, Ok sqlparser)` branch in
/// [`parse_both_inner`], which only returns sqlparser's result when sqlparser is preferred.
pub fn parse_query_with_config(
    config: impl Into<ParsingConfig>,
    dialect: Dialect,
    input: impl AsRef<str>,
) -> Result<SqlQuery, ReadysetParsingError> {
    let config: ParsingConfig = config.into();

    // only-sqlparser: skip nom entirely (performance)
    if !config.nom {
        return parse_sqlparser_inner(dialect, input, parse_readyset_query);
    }

    // Run sqlparser to get shallow AST (needed even for only-nom mode)
    let sqlparser_result = parse_sqlparser_inner(dialect, input.as_ref(), parse_readyset_query);

    let shallow_ast = match &sqlparser_result {
        Ok(SqlQuery::CreateCache(cc)) => match &cc.inner {
            CacheInner::Statement { shallow, .. } => Some(shallow.clone()),
            _ => None,
        },
        Ok(SqlQuery::Explain(readyset_sql::ast::ExplainStatement::CreateCache {
            inner: CacheInner::Statement { shallow, .. },
            ..
        })) => Some(shallow.clone()),
        _ => None,
    };

    let sqlparser_deep_ok = match &sqlparser_result {
        Ok(SqlQuery::CreateCache(cc)) => match &cc.inner {
            CacheInner::Statement { deep: Ok(d), .. } => Some(d.clone()),
            _ => None,
        },
        Ok(SqlQuery::Explain(readyset_sql::ast::ExplainStatement::CreateCache {
            inner: CacheInner::Statement { deep: Ok(d), .. },
            ..
        })) => Some(d.clone()),
        _ => None,
    };

    let mut nom_result = nom_sql::parse_query(dialect, input.as_ref());

    if let Some(shallow_ast_val) = shallow_ast {
        let target = match nom_result.as_mut() {
            Ok(SqlQuery::CreateCache(cc)) => match &mut cc.inner {
                CacheInner::Statement { shallow, .. } => Some(shallow),
                _ => None,
            },
            Ok(SqlQuery::Explain(readyset_sql::ast::ExplainStatement::CreateCache {
                inner: CacheInner::Statement { shallow, .. },
                ..
            })) => Some(shallow),
            _ => None,
        };
        if let Some(target) = target {
            *target = shallow_ast_val;
        }
    }

    if config.prefer_sqlparser
        && let Some(sqlparser_deep) = sqlparser_deep_ok
    {
        let target = match nom_result.as_mut() {
            Ok(SqlQuery::CreateCache(cc)) => match &mut cc.inner {
                CacheInner::Statement { deep, .. } => Some(deep),
                _ => None,
            },
            Ok(SqlQuery::Explain(readyset_sql::ast::ExplainStatement::CreateCache {
                inner: CacheInner::Statement { deep, .. },
                ..
            })) => Some(deep),
            _ => None,
        };
        if let Some(target @ Err(_)) = target {
            tracing::debug!(
                ?dialect,
                input = %input.as_ref(),
                "nom-sql failed to parse DEEP CACHE inner SELECT; adopting sqlparser-rs result"
            );
            *target = Ok(sqlparser_deep);
        }
    }

    // only-nom: return nom result with shallow AST populated
    if !config.sqlparser {
        return nom_result.map_err(ReadysetParsingError::NomError);
    }

    parse_both_inner(
        config,
        dialect,
        "",
        move |_, _| nom_result,
        move |_, _, _| sqlparser_result,
    )
}

/// Uses the default parsing config for tests, which can panic (see
/// [`ParsingPreset::for_tests()`]); so should only be used in tests; otherwise use
/// [`parse_query_with_config`]
pub fn parse_query(
    dialect: Dialect,
    input: impl AsRef<str>,
) -> Result<SqlQuery, ReadysetParsingError> {
    parse_query_with_config(ParsingPreset::for_tests(), dialect, input)
}

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

#[cfg(test)]
mod tests {
    use super::*;
    use readyset_sql::ast::SqlQuery;
    use readyset_sql::{Dialect, DialectDisplay, TryFromDialect};

    /// Converting the sqlparser AST from a shallow parse must produce the same Readyset AST as
    /// a sqlparser-only full parse of the query text; the adapter relies on this to skip the
    /// second parse when the shallow path declines a query.
    #[test]
    fn shallow_ast_conversion_matches_full_parse() {
        for dialect in [Dialect::MySQL, Dialect::PostgreSQL] {
            for query in [
                "SELECT a, count(*) FROM t WHERE b = 1 AND c IN (2, 3) GROUP BY a ORDER BY a LIMIT 10",
                "SELECT /*rs+ CREATE SHALLOW CACHE */ a FROM t WHERE b = 1",
                "SELECT a FROM t UNION SELECT b FROM u",
                "WITH x AS (SELECT a FROM t) SELECT a FROM x",
            ] {
                let (shallow, _) =
                    parse_shallow_query(dialect, query).expect("shallow parse should succeed");
                let converted = SqlQuery::try_from_dialect((*shallow).clone(), dialect)
                    .expect("conversion should succeed");
                let parsed = parse_query_with_config(ParsingPreset::OnlySqlparser, dialect, query)
                    .expect("full parse should succeed");
                assert_eq!(converted, parsed, "{dialect} {query}");
            }
        }
    }

    fn parse_pg_sqlparser(input: &str) -> SqlQuery {
        parse_query_with_config(ParsingPreset::OnlySqlparser, Dialect::PostgreSQL, input)
            .unwrap_or_else(|e| panic!("failed to parse {input:?}: {e}"))
    }

    fn assert_pg_round_trip(input: &str, expected: &str) {
        let query = parse_pg_sqlparser(input);
        assert_eq!(
            query.display(Dialect::PostgreSQL).to_string(),
            expected,
            "round-trip mismatch for {input:?}",
        );
    }

    #[test]
    fn discard_statements_round_trip() {
        use readyset_sql::ast::{DiscardObject, DiscardStatement};

        for (input, object_type) in [
            ("DISCARD ALL", DiscardObject::All),
            ("DISCARD PLANS", DiscardObject::Plans),
            ("DISCARD SEQUENCES", DiscardObject::Sequences),
            ("DISCARD TEMPORARY", DiscardObject::Temporary),
        ] {
            let query = parse_pg_sqlparser(input);
            assert_eq!(query, SqlQuery::Discard(DiscardStatement { object_type }));
            assert_eq!(query.display(Dialect::PostgreSQL).to_string(), input);
        }
    }

    #[test]
    fn reset_all_models_as_full_discard() {
        use readyset_sql::ast::{DiscardObject, DiscardStatement};

        // `RESET ALL` is modeled as the session reset it triggers so the
        // adapter mirrors it the same way as `DISCARD ALL`; the original text
        // is still proxied upstream verbatim.
        let query = parse_pg_sqlparser("RESET ALL");
        assert_eq!(
            query,
            SqlQuery::Discard(DiscardStatement {
                object_type: DiscardObject::All
            })
        );
    }

    #[test]
    fn reset_parameter_models_as_set_default() {
        use readyset_sql::ast::{
            PostgresParameterScope, SetPostgresParameter, SetPostgresParameterValue,
        };

        // `RESET <name>` is `SET <name> TO DEFAULT`; model it so the adapter
        // resets the mirrored parameter. A namespaced GUC keeps its dotted
        // name so it keys against the `set_config` form.
        for (input, expected_name) in [
            ("RESET role", "role"),
            ("RESET app.tenant_id", "app.tenant_id"),
            ("RESET request.jwt.claims", "request.jwt.claims"),
        ] {
            let query = parse_pg_sqlparser(input);
            assert_eq!(
                query,
                SqlQuery::Set(SetStatement::PostgresParameter(SetPostgresParameter {
                    scope: Some(PostgresParameterScope::Session),
                    name: expected_name.into(),
                    value: SetPostgresParameterValue::Default,
                })),
                "unexpected parse for {input:?}",
            );
        }
    }

    #[test]
    fn set_namespaced_guc_rejoins_dotted_name() {
        use readyset_sql::ast::{SetPostgresParameter, SetPostgresParameterValue};

        // Namespaced GUCs (`request.jwt.claims`, `app.tenant_id`) must parse so
        // a psql / simple-protocol `SET` mirrors into the session the same way
        // `set_config('<name>', ...)` does. A plain `SET` carries no scope
        // keyword, so `scope` is `None` (session scope by default).
        for (input, expected_name) in [
            (
                "SET request.jwt.claims = '{\"sub\":\"bob\"}'",
                "request.jwt.claims",
            ),
            ("SET app.tenant_id = 'bob'", "app.tenant_id"),
        ] {
            let query = parse_pg_sqlparser(input);
            let SqlQuery::Set(SetStatement::PostgresParameter(SetPostgresParameter {
                scope,
                name,
                value,
            })) = query
            else {
                panic!("expected PostgresParameter for {input:?}, got {query:?}");
            };
            assert_eq!(name.as_str(), expected_name, "name for {input:?}");
            assert_eq!(scope, None, "scope for {input:?}");
            assert!(matches!(value, SetPostgresParameterValue::Value(_)));
        }
    }

    #[test]
    fn set_role_keyword_maps_to_role_parameter() {
        use readyset_sql::ast::{
            PostgresParameterScope, PostgresParameterValue, PostgresParameterValueInner,
            SetPostgresParameter, SetPostgresParameterValue,
        };

        // `SET [SESSION|LOCAL] ROLE <name>` is modeled as the `role` GUC so the
        // session mirror resolves the effective role + bypass; `SET ROLE NONE`
        // resets it to the startup role (`role = DEFAULT`).
        let query = parse_pg_sqlparser("SET ROLE authenticated");
        assert_eq!(
            query,
            SqlQuery::Set(SetStatement::PostgresParameter(SetPostgresParameter {
                scope: None,
                name: "role".into(),
                value: SetPostgresParameterValue::Value(PostgresParameterValue::Single(
                    PostgresParameterValueInner::Identifier("authenticated".into())
                )),
            }))
        );

        let session = parse_pg_sqlparser("SET SESSION ROLE authenticated");
        let SqlQuery::Set(SetStatement::PostgresParameter(SetPostgresParameter { scope, .. })) =
            session
        else {
            panic!("expected role parameter for SET SESSION ROLE");
        };
        assert_eq!(scope, Some(PostgresParameterScope::Session));

        let none = parse_pg_sqlparser("SET ROLE NONE");
        assert_eq!(
            none,
            SqlQuery::Set(SetStatement::PostgresParameter(SetPostgresParameter {
                scope: None,
                name: "role".into(),
                value: SetPostgresParameterValue::Default,
            }))
        );
    }

    #[test]
    fn set_session_authorization_round_trips() {
        let query = parse_pg_sqlparser("SET SESSION AUTHORIZATION alice");
        assert_eq!(
            query,
            SqlQuery::Set(SetStatement::SessionAuthorization(
                SetSessionAuthorization {
                    local: false,
                    value: SessionAuthorizationValue::User("alice".into()),
                }
            ))
        );
        assert_pg_round_trip(
            "SET SESSION AUTHORIZATION alice",
            "SET SESSION AUTHORIZATION alice",
        );
        assert_pg_round_trip(
            "SET SESSION AUTHORIZATION DEFAULT",
            "SET SESSION AUTHORIZATION DEFAULT",
        );
    }

    #[test]
    fn set_local_session_authorization_round_trips() {
        let query = parse_pg_sqlparser("SET LOCAL SESSION AUTHORIZATION DEFAULT");
        assert_eq!(
            query,
            SqlQuery::Set(SetStatement::SessionAuthorization(
                SetSessionAuthorization {
                    local: true,
                    value: SessionAuthorizationValue::Default,
                }
            ))
        );
        assert_pg_round_trip(
            "SET LOCAL SESSION AUTHORIZATION DEFAULT",
            "SET LOCAL SESSION AUTHORIZATION DEFAULT",
        );
        assert_pg_round_trip(
            "SET LOCAL SESSION AUTHORIZATION bob",
            "SET LOCAL SESSION AUTHORIZATION bob",
        );
    }

    #[test]
    fn reset_session_authorization_maps_to_default() {
        let query = parse_pg_sqlparser("RESET SESSION AUTHORIZATION");
        assert_eq!(
            query,
            SqlQuery::Set(SetStatement::SessionAuthorization(
                SetSessionAuthorization {
                    local: false,
                    value: SessionAuthorizationValue::Default,
                }
            ))
        );
        // `RESET SESSION AUTHORIZATION` is modeled as the `DEFAULT` authorization, so it
        // round-trips through the canonical `SET SESSION AUTHORIZATION DEFAULT` rendering.
        assert_eq!(
            query.display(Dialect::PostgreSQL).to_string(),
            "SET SESSION AUTHORIZATION DEFAULT",
        );
    }

    #[test]
    fn mysql_invisible_column_parsed() {
        // INVISIBLE is only supported by the sqlparser path; nom-sql strips versioned
        // comments and does not recognize the bare INVISIBLE keyword.
        let stmt = parse_create_table_with_config(
            ParsingPreset::OnlySqlparser,
            Dialect::MySQL,
            "CREATE TABLE foo (a INT, b INT INVISIBLE)",
        )
        .expect("failed to parse CREATE TABLE with INVISIBLE column");
        let body = stmt.body.expect("CREATE TABLE body should be Ok");
        assert_eq!(body.fields.len(), 2);
        assert!(!body.fields[0].invisible, "column 'a' should be visible");
        assert!(body.fields[1].invisible, "column 'b' should be invisible");
    }
}
