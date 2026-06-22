use std::{fmt, str};

use itertools::Itertools;
use readyset_util::fmt::fmt_with;
use serde::{Deserialize, Serialize};
use test_strategy::Arbitrary;

use crate::{AstConversionError, Dialect, DialectDisplay, TryFromDialect, TryIntoDialect, ast::*};

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
#[arbitrary(args = Option<Dialect>)]
#[allow(clippy::large_enum_variant)]
pub enum SqlQuery {
    #[weight(0)]
    CreateDatabase(CreateDatabaseStatement),
    CreateTable(CreateTableStatement),
    CreateView(CreateViewStatement),
    CreateIndex(CreateIndexStatement),
    CreateCache(CreateCacheStatement),
    DropCache(DropCacheStatement),
    DropAllCaches(DropAllCachesStatement),
    FlushAllShallowCaches(FlushAllShallowCachesStatement),
    FlushCache(FlushCacheStatement),
    DropAllProxiedQueries(DropAllProxiedQueriesStatement),
    AlterTable(AlterTableStatement),
    AlterReadySet(AlterReadysetStatement),
    Insert(InsertStatement),
    CompoundSelect(CompoundSelectStatement),
    Select(SelectStatement),
    Delete(DeleteStatement),
    DropTable(DropTableStatement),
    DropView(DropViewStatement),
    Update(UpdateStatement),
    Set(SetStatement),
    StartTransaction(StartTransactionStatement),
    Commit(CommitStatement),
    Rollback(RollbackStatement),
    RenameTable(RenameTableStatement),
    Use(UseStatement),
    Show(ShowStatement),
    Explain(ExplainStatement),
    // Postgres-only and not round-trippable through both dialects, so excluded from proptest
    // generation; mirrors the other `#[weight(0)]` variants here.
    #[weight(0)]
    Discard(DiscardStatement),
    // Unfortunately, weight(0) is a special case that removes this option from the generated
    // `prop_oneof!`, but actually having a weight of 0 is not supported. If that worked, we could
    // generate this only for PostgreSQL like so:
    //
    // ```
    // #[weight(u32::from(*args_shared == Some(Dialect::PostgreSQL)))]
    // ```
    //
    // We don't really care about round-tripping these statements, but could fix this later.
    #[weight(0)]
    Comment(CommentStatement),
    Deallocate(DeallocateStatement),
    Truncate(#[any(*args_shared)] TruncateStatement),
    #[weight(0)]
    CreateRls(CreateRlsStatement),
    #[weight(0)]
    DropRls(DropRlsStatement),
    #[weight(0)]
    CreateMcpToken(CreateMcpTokenStatement),
    #[weight(0)]
    DropMcpToken(DropMcpTokenStatement),
    #[weight(0)]
    AlterMcpToken(AlterMcpTokenStatement),
}

impl DialectDisplay for SqlQuery {
    fn display(&self, dialect: Dialect) -> impl fmt::Display + '_ {
        fmt_with(move |f| match self {
            Self::Select(select) => write!(f, "{}", select.display(dialect)),
            Self::Insert(insert) => write!(f, "{}", insert.display(dialect)),
            Self::CreateTable(create) => write!(f, "{}", create.display(dialect)),
            Self::CreateView(create) => write!(f, "{}", create.display(dialect)),
            Self::CreateIndex(create) => write!(f, "{}", create.display(dialect)),
            Self::CreateCache(create) => write!(f, "{}", create.display(dialect)),
            Self::DropCache(drop) => write!(f, "{}", drop.display(dialect)),
            Self::DropAllCaches(drop) => write!(f, "{drop}"),
            Self::FlushAllShallowCaches(flush) => write!(f, "{flush}"),
            Self::FlushCache(flush) => write!(f, "{}", flush.display(dialect)),
            Self::Delete(delete) => write!(f, "{}", delete.display(dialect)),
            Self::DropTable(drop) => write!(f, "{}", drop.display(dialect)),
            Self::DropView(drop) => write!(f, "{}", drop.display(dialect)),
            Self::Update(update) => write!(f, "{}", update.display(dialect)),
            Self::Set(set) => write!(f, "{}", set.display(dialect)),
            Self::AlterTable(alter) => write!(f, "{}", alter.display(dialect)),
            Self::AlterReadySet(alter) => write!(f, "{}", alter.display(dialect)),
            Self::CompoundSelect(compound) => write!(f, "{}", compound.display(dialect)),
            Self::StartTransaction(tx) => write!(f, "{tx}"),
            Self::Commit(commit) => write!(f, "{commit}"),
            Self::Rollback(rollback) => write!(f, "{}", rollback.display(dialect)),
            Self::RenameTable(rename) => write!(f, "{}", rename.display(dialect)),
            Self::Use(use_db) => write!(f, "{use_db}"),
            Self::Show(show) => write!(f, "{}", show.display(dialect)),
            Self::Explain(explain) => write!(f, "{}", explain.display(dialect)),
            Self::Discard(discard) => write!(f, "{discard}"),
            Self::Comment(c) => write!(f, "{}", c.display(dialect)),
            Self::DropAllProxiedQueries(drop) => write!(f, "{}", drop.display(dialect)),
            Self::Deallocate(dealloc) => write!(f, "{}", dealloc.display(dialect)),
            Self::Truncate(truncate) => write!(f, "{}", truncate.display(dialect)),
            Self::CreateDatabase(create) => write!(f, "{}", create.display(dialect)),
            Self::CreateRls(create_rls) => write!(f, "{}", create_rls.display(dialect)),
            Self::DropRls(drop_rls) => write!(f, "{}", drop_rls.display(dialect)),
            Self::CreateMcpToken(create) => write!(f, "{}", create.display(dialect)),
            Self::DropMcpToken(drop) => write!(f, "{}", drop.display(dialect)),
            Self::AlterMcpToken(a) => write!(f, "{}", a.display(dialect)),
        })
    }
}

impl From<SelectSpecification> for SqlQuery {
    fn from(s: SelectSpecification) -> Self {
        match s {
            SelectSpecification::Simple(s) => SqlQuery::Select(s),
            SelectSpecification::Compound(c) => SqlQuery::CompoundSelect(c),
        }
    }
}

impl TryFromDialect<sqlparser::ast::Statement> for SqlQuery {
    fn try_from_dialect(
        value: sqlparser::ast::Statement,
        dialect: Dialect,
    ) -> Result<Self, AstConversionError> {
        use sqlparser::ast::Statement::*;
        match value {
            Query(query) => Ok(query.try_into_dialect(dialect)?),
            ShowVariable { variable } => {
                let Some(name) = variable.first() else {
                    return not_yet_implemented!("unsupported SHOW variable with no variable");
                };
                if name.value.eq_ignore_ascii_case("CONNECTIONS") {
                    Ok(Self::Show(ShowStatement::Connections))
                } else if name.value.eq_ignore_ascii_case("EVENTS") {
                    Ok(Self::Show(ShowStatement::Events))
                } else {
                    not_yet_implemented!("unsupported SHOW variable name {variable:?}")
                }
            }
            ShowTables {
                full,
                show_options:
                    sqlparser::ast::ShowStatementOptions {
                        show_in,
                        filter_position,
                        ..
                    },
                ..
            } => Ok(Self::Show(crate::ast::ShowStatement::Tables(
                crate::ast::show::Tables {
                    full,
                    from_db: match show_in {
                        Some(sqlparser::ast::ShowStatementIn {
                            parent_name: Some(parent_name),
                            ..
                        }) => Some(parent_name.to_string()), // TODO: object name can be multipart
                        _ => None,
                    },
                    filter: match filter_position {
                        Some(sqlparser::ast::ShowStatementFilterPosition::Infix(filter))
                        | Some(sqlparser::ast::ShowStatementFilterPosition::Suffix(filter)) => {
                            Some(filter.try_into_dialect(dialect)?)
                        }
                        None => None,
                    },
                },
            ))),
            ShowDatabases { .. } => Ok(Self::Show(crate::ast::ShowStatement::Databases)),
            CreateTable(create) => Ok(Self::CreateTable(create.try_into_dialect(dialect)?)),
            CreateIndex(create) => Ok(Self::CreateIndex(create.try_into_dialect(dialect)?)),
            Insert(insert) => Ok(Self::Insert(insert.try_into_dialect(dialect)?)),
            Delete(delete) => Ok(Self::Delete(delete.try_into_dialect(dialect)?)),
            create @ CreateView { .. } => Ok(Self::CreateView(create.try_into_dialect(dialect)?)),
            update @ Update { .. } => Ok(Self::Update(update.try_into_dialect(dialect)?)),
            Use(use_statement) => Ok(Self::Use(use_statement.try_into_dialect(dialect)?)),
            Set(set_statement) => Ok(Self::Set(set_statement.try_into_dialect(dialect)?)),
            Drop {
                object_type,
                if_exists,
                names,
                temporary,
                ..
            } => match object_type {
                sqlparser::ast::ObjectType::Table => Ok(Self::DropTable(DropTableStatement {
                    tables: names.try_into_dialect(dialect)?,
                    if_exists,
                    temporary,
                })),
                sqlparser::ast::ObjectType::View => Ok(Self::DropView(DropViewStatement {
                    views: names.try_into_dialect(dialect)?,
                    if_exists,
                })),
                _ => not_yet_implemented!("drop statement type: {object_type:?}"),
            },
            StartTransaction { begin, .. } => Ok(Self::StartTransaction(if begin {
                StartTransactionStatement::Begin
            } else {
                StartTransactionStatement::Start
            })),
            CreateType { .. } => Err(AstConversionError::Skipped(format!("CREATE TYPE: {value}"))),
            Rollback { savepoint, .. } => Ok(Self::Rollback(RollbackStatement {
                savepoint: savepoint.map(|ident| SqlIdentifier::from(ident.value)),
            })),
            Commit { .. } => Ok(Self::Commit(CommitStatement {})),
            alter @ AlterTable { .. } => Ok(Self::AlterTable(alter.try_into_dialect(dialect)?)),
            truncate @ Truncate { .. } => Ok(Self::Truncate(truncate.try_into_dialect(dialect)?)),
            CreateDatabase { .. } => skipped!("CREATE DATABASE"),
            CreateSchema { .. } => skipped!("CREATE SCHEMA"),
            Deallocate { name, prepare: _ } => {
                let identifier = if name.value.eq_ignore_ascii_case("ALL") {
                    StatementIdentifier::AllStatements
                } else {
                    StatementIdentifier::SingleStatement(name.value)
                };
                Ok(Self::Deallocate(DeallocateStatement { identifier }))
            }
            Comment {
                object_type: sqlparser::ast::CommentObject::Table,
                object_name,
                comment,
                if_exists: _,
            } => Ok(Self::Comment(CommentStatement::Table {
                table_name: object_name
                    .0
                    .into_iter()
                    .exactly_one()
                    .map_err(|_| {
                        failed_err!("Expected unqualified table name in COMMENT ON TABLE")
                    })?
                    .try_into_dialect(dialect)?,
                comment: comment.unwrap_or("".to_owned()),
            })),
            Comment {
                object_type: sqlparser::ast::CommentObject::Column,
                object_name,
                comment,
                if_exists: _,
            } => {
                let Relation { schema, name } = object_name.try_into_dialect(dialect)?;
                Ok(Self::Comment(CommentStatement::Column {
                    column_name: name,
                    table_name: schema.ok_or_else(|| {
                        failed_err!("Expected qualified table name in COMMENT ON COLUMN")
                    })?,
                    comment: comment.unwrap_or("".to_owned()),
                }))
            }
            RenameTable(rename) => Ok(Self::RenameTable(RenameTableStatement {
                ops: rename.try_into_dialect(dialect)?,
            })),
            Discard { object_type } => Ok(Self::Discard(DiscardStatement {
                object_type: object_type.try_into()?,
            })),
            // `RESET ALL` resets all run-time parameters; PG documents it as a
            // strict subset of `DISCARD ALL`. Readyset observes it only to reset
            // mirrored session state and proxies the original text upstream
            // verbatim, so model it as the session reset it triggers.
            Reset(sqlparser::ast::ResetStatement {
                reset: sqlparser::ast::Reset::ALL,
            }) => Ok(Self::Discard(DiscardStatement {
                object_type: DiscardObject::All,
            })),
            // `RESET <name>` is `SET <name> TO DEFAULT` in Postgres; model it as
            // such so the session mirror resets the parameter (the role or a
            // policy-keyed GUC) through the same path as an explicit
            // `SET ... = DEFAULT`. The dotted parameter name is rejoined so a
            // namespaced GUC like `request.jwt.claims` keys against the same
            // name the `set_config` path stores.
            Reset(sqlparser::ast::ResetStatement {
                reset: sqlparser::ast::Reset::ConfigurationParameter(name),
            }) => {
                let name = name
                    .0
                    .iter()
                    .filter_map(|part| part.as_ident().map(|ident| ident.value.as_str()))
                    .join(".");
                Ok(Self::Set(SetStatement::PostgresParameter(
                    SetPostgresParameter {
                        scope: Some(PostgresParameterScope::Session),
                        name: name.into(),
                        value: SetPostgresParameterValue::Default,
                    },
                )))
            }
            _ => not_yet_implemented!("other query: {value:?}"),
        }
    }
}

impl TryFromDialect<sqlparser::ast::Query> for SqlQuery {
    fn try_from_dialect(
        value: sqlparser::ast::Query,
        dialect: Dialect,
    ) -> Result<Self, AstConversionError> {
        if matches!(*value.body, sqlparser::ast::SetExpr::Select(_)) {
            Ok(SqlQuery::Select(value.try_into_dialect(dialect)?))
        } else if matches!(*value.body, sqlparser::ast::SetExpr::SetOperation { .. }) {
            Ok(SqlQuery::CompoundSelect(value.try_into_dialect(dialect)?))
        } else {
            not_yet_implemented!("unsupported non-select query type {value:?}")
        }
    }
}

impl TryFromDialect<Box<sqlparser::ast::Query>> for SqlQuery {
    fn try_from_dialect(
        value: Box<sqlparser::ast::Query>,
        dialect: Dialect,
    ) -> Result<Self, AstConversionError> {
        (*value).try_into_dialect(dialect)
    }
}

impl SqlQuery {
    /// Returns the type of the query, e.g. "CREATE TABLE" or "SELECT"
    pub fn query_type(&self) -> &'static str {
        match self {
            Self::Select(_) => "SELECT",
            Self::Insert(_) => "INSERT",
            Self::CreateDatabase(cd) => {
                if cd.is_schema {
                    "CREATE SCHEMA"
                } else {
                    "CREATE DATABASE"
                }
            }
            Self::CreateTable(_) => "CREATE TABLE",
            Self::CreateView(_) => "CREATE VIEW",
            Self::CreateIndex(_) => "CREATE INDEX",
            Self::CreateCache(_) => "CREATE CACHE",
            Self::DropCache(_) => "DROP CACHE",
            Self::DropAllCaches(_) => "DROP ALL CACHES",
            Self::FlushAllShallowCaches(_) => "FLUSH ALL SHALLOW CACHES",
            Self::FlushCache(_) => "FLUSH CACHE",
            Self::DropAllProxiedQueries(_) => "DROP ALL PROXIED QUERIES",
            Self::Delete(_) => "DELETE",
            Self::DropTable(_) => "DROP TABLE",
            Self::DropView(_) => "DROP VIEW",
            Self::Update(_) => "UPDATE",
            Self::Set(_) => "SET",
            Self::AlterTable(_) => "ALTER TABLE",
            Self::AlterReadySet(_) => "ALTER READYSET",
            Self::CompoundSelect(_) => "SELECT",
            Self::StartTransaction(_) => "START TRANSACTION",
            Self::Commit(_) => "COMMIT",
            Self::Rollback(_) => "ROLLBACK",
            Self::RenameTable(_) => "RENAME",
            Self::Use(_) => "USE",
            Self::Show(_) => "SHOW",
            Self::Explain(_) => "EXPLAIN",
            Self::Discard(_) => "DISCARD",
            Self::Comment(_) => "COMMENT",
            Self::Deallocate(_) => "DEALLOCATE",
            Self::Truncate(_) => "TRUNCATE",
            Self::CreateRls(_) => "CREATE RLS",
            Self::DropRls(_) => "DROP RLS",
            Self::CreateMcpToken(_) => "CREATE MCP TOKEN",
            Self::DropMcpToken(_) => "DROP MCP TOKEN",
            Self::AlterMcpToken(_) => "ALTER MCP TOKEN",
        }
    }

    /// Returns whether the provided SqlQuery is a SELECT or not.
    pub fn is_select(&self) -> bool {
        matches!(self, Self::Select(_))
    }

    /// Returns true if this query is a write statement, used by the adapter to flip the
    /// per-transaction `had_write` flag that gates [`TrxCachePolicy::UntilWrite`] caches.
    ///
    /// "Write" is interpreted broadly: every variant that changes user-visible state of the
    /// upstream database (DML, DDL, cache lifecycle, RLS, and runtime configuration
    /// statements) counts. Read-only and transaction-control variants return `false`,
    /// as do Readyset-internal management statements that do not mutate upstream user
    /// data (e.g. MCP token DDL).
    ///
    /// Several writes never reach an `SqlQuery` variant because the parser rejects them
    /// upstream of this code (e.g. `SELECT ... FOR UPDATE`, `LOAD DATA`, stored-procedure
    /// `CALL`, CTE-embedded INSERT). Routing-layer callers must compensate by conservatively
    /// flipping `had_write` on parse failure inside a transaction; see the design doc.
    pub fn is_write(&self) -> bool {
        match self {
            // DML.
            Self::Insert(_) | Self::Update(_) | Self::Delete(_) | Self::Truncate(_) => true,
            // DDL on relations.
            Self::CreateTable(_)
            | Self::AlterTable(_)
            | Self::DropTable(_)
            | Self::RenameTable(_)
            | Self::CreateView(_)
            | Self::DropView(_)
            | Self::CreateIndex(_)
            | Self::CreateDatabase(_) => true,
            // Cache lifecycle.
            Self::CreateCache(_)
            | Self::DropCache(_)
            | Self::DropAllCaches(_)
            | Self::FlushCache(_)
            | Self::FlushAllShallowCaches(_) => true,
            // RLS DDL.
            Self::CreateRls(_) | Self::DropRls(_) => true,
            // Runtime control.
            Self::AlterReadySet(_) => true,
            // Reads, transaction control, session-only commands, and Readyset bookkeeping
            // (including MCP token management, which mutates Readyset state but not the
            // upstream user data that drives cache routing).
            Self::Select(_)
            | Self::CompoundSelect(_)
            | Self::Set(_)
            | Self::Show(_)
            | Self::Explain(_)
            | Self::Discard(_)
            | Self::Comment(_)
            | Self::Use(_)
            | Self::Deallocate(_)
            | Self::StartTransaction(_)
            | Self::Commit(_)
            | Self::Rollback(_)
            | Self::DropAllProxiedQueries(_)
            | Self::CreateMcpToken(_)
            | Self::DropMcpToken(_)
            | Self::AlterMcpToken(_) => false,
        }
    }

    /// Returns the query as a select statement, if it is a select statement.
    pub fn into_select(self) -> Option<SelectStatement> {
        match self {
            Self::Select(s) => Some(s),
            _ => None,
        }
    }

    /// Returns true if this is a query for a ReadySet extension and not regular SQL.
    pub fn is_readyset_extension(&self) -> bool {
        match self {
            SqlQuery::Explain(_)
            | SqlQuery::CreateCache(_)
            | SqlQuery::DropCache(_)
            | SqlQuery::DropAllCaches(_)
            | SqlQuery::FlushAllShallowCaches(_)
            | SqlQuery::FlushCache(_)
            | SqlQuery::AlterReadySet(_)
            | SqlQuery::DropAllProxiedQueries(_) => true,
            SqlQuery::Show(show_stmt) => match show_stmt {
                ShowStatement::Events | ShowStatement::Tables(_) | ShowStatement::Databases => {
                    false
                }
                ShowStatement::CachedQueries(..)
                | ShowStatement::ProxiedQueries(_)
                | ShowStatement::ReadySetStatus
                | ShowStatement::ReadySetStatusAdapter
                | ShowStatement::ReadySetMigrationStatus(_)
                | ShowStatement::ReadySetVersion
                | ShowStatement::ReadySetTables(..)
                | ShowStatement::Connections
                | ShowStatement::Rls(_)
                | ShowStatement::ReplayPaths
                | ShowStatement::ShallowCacheEntries { .. }
                | ShowStatement::ShallowCacheAllowedFunctions
                | ShowStatement::McpTokens => true,
                // Handled by the MySQL query handler via the
                // requires_fallback + return_default_response path.
                ShowStatement::ReadySetRsaPublicKey => false,
            },
            SqlQuery::CreateRls(_) | SqlQuery::DropRls(_) => true,
            SqlQuery::CreateMcpToken(_)
            | SqlQuery::DropMcpToken(_)
            | SqlQuery::AlterMcpToken(_) => true,
            SqlQuery::CreateDatabase(_)
            | SqlQuery::CreateTable(_)
            | SqlQuery::CreateView(_)
            | SqlQuery::CreateIndex(_)
            | SqlQuery::AlterTable(_)
            | SqlQuery::Insert(_)
            | SqlQuery::CompoundSelect(_)
            | SqlQuery::Select(_)
            | SqlQuery::Deallocate(_)
            | SqlQuery::Delete(_)
            | SqlQuery::DropTable(_)
            | SqlQuery::DropView(_)
            | SqlQuery::Update(_)
            | SqlQuery::Set(_)
            | SqlQuery::StartTransaction(_)
            | SqlQuery::Commit(_)
            | SqlQuery::Rollback(_)
            | SqlQuery::RenameTable(_)
            | SqlQuery::Use(_)
            | SqlQuery::Truncate(_)
            | SqlQuery::Discard(_)
            | SqlQuery::Comment(_) => false,
        }
    }
}
