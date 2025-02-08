use std::{fmt, str};

use readyset_util::fmt::fmt_with;
use serde::{Deserialize, Serialize};
use test_strategy::Arbitrary;

use crate::{ast::*, AstConversionError, Dialect, DialectDisplay};

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
#[allow(clippy::large_enum_variant)]
pub enum SqlQuery {
    CreateDatabase(CreateDatabaseStatement),
    CreateTable(CreateTableStatement),
    CreateView(CreateViewStatement),
    CreateCache(CreateCacheStatement),
    DropCache(DropCacheStatement),
    DropAllCaches(DropAllCachesStatement),
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
    Comment(CommentStatement),
    Deallocate(DeallocateStatement),
    Truncate(TruncateStatement),
}

impl DialectDisplay for SqlQuery {
    fn display(&self, dialect: Dialect) -> impl fmt::Display + '_ {
        fmt_with(move |f| match self {
            Self::Select(select) => write!(f, "{}", select.display(dialect)),
            Self::Insert(insert) => write!(f, "{}", insert.display(dialect)),
            Self::CreateTable(create) => write!(f, "{}", create.display(dialect)),
            Self::CreateView(create) => write!(f, "{}", create.display(dialect)),
            Self::CreateCache(create) => write!(f, "{}", create.display(dialect)),
            Self::DropCache(drop) => write!(f, "{}", drop.display(dialect)),
            Self::DropAllCaches(drop) => write!(f, "{}", drop),
            Self::Delete(delete) => write!(f, "{}", delete.display(dialect)),
            Self::DropTable(drop) => write!(f, "{}", drop.display(dialect)),
            Self::DropView(drop) => write!(f, "{}", drop.display(dialect)),
            Self::Update(update) => write!(f, "{}", update.display(dialect)),
            Self::Set(set) => write!(f, "{}", set.display(dialect)),
            Self::AlterTable(alter) => write!(f, "{}", alter.display(dialect)),
            Self::AlterReadySet(alter) => write!(f, "{}", alter.display(dialect)),
            Self::CompoundSelect(compound) => write!(f, "{}", compound.display(dialect)),
            Self::StartTransaction(tx) => write!(f, "{}", tx),
            Self::Commit(commit) => write!(f, "{}", commit),
            Self::Rollback(rollback) => write!(f, "{}", rollback),
            Self::RenameTable(rename) => write!(f, "{}", rename.display(dialect)),
            Self::Use(use_db) => write!(f, "{}", use_db),
            Self::Show(show) => write!(f, "{}", show.display(dialect)),
            Self::Explain(explain) => write!(f, "{}", explain.display(dialect)),
            Self::Comment(c) => write!(f, "{}", c.display(dialect)),
            Self::DropAllProxiedQueries(drop) => write!(f, "{}", drop.display(dialect)),
            Self::Deallocate(dealloc) => write!(f, "{}", dealloc.display(dialect)),
            Self::Truncate(truncate) => write!(f, "{}", truncate.display(dialect)),
            Self::CreateDatabase(create) => write!(f, "{}", create.display(dialect)),
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

/// TODO: This should be `TryFrom`, but I wanted to hard error with clickable line numbers while hacking
impl TryFrom<sqlparser::ast::Statement> for SqlQuery {
    type Error = AstConversionError;

    fn try_from(value: sqlparser::ast::Statement) -> Result<Self, Self::Error> {
        use sqlparser::ast::Statement::*;
        match value {
            Query(query) => Ok(query.try_into()?),
            // This is kind of crazy; neither sqlparser-rs nor nom-sql actually support MySQL's `SHOW DATABASES` syntax
            ShowVariable { ref variable } => {
                let variable = variable
                    .iter()
                    .map(|ident| ident.value.to_lowercase())
                    .collect::<Vec<_>>();
                if variable == vec!["databases"] {
                    Ok(Self::Show(crate::ast::ShowStatement::Databases))
                } else if variable == vec!["proxied", "queries"] {
                    // TODO: This should be handled by custom parsing in readyset-sql-parsing and not be handled here
                    Ok(Self::Show(crate::ast::ShowStatement::ProxiedQueries(
                        crate::ast::ProxiedQueriesOptions {
                            limit: None,
                            only_supported: false,
                            query_id: None,
                        },
                    )))
                } else if variable == vec!["readyset", "status"] {
                    Ok(Self::Show(crate::ast::ShowStatement::ReadySetStatus))
                } else if variable == vec!["cached", "queries"] || variable == vec!["caches"] {
                    Ok(Self::Show(crate::ast::ShowStatement::CachedQueries(None)))
                } else {
                    not_yet_implemented!("unsupported ShowVariables {value:?}")
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
                            Some(filter.try_into()?)
                        }
                        None => None,
                    },
                },
            ))),
            ShowDatabases { .. } => Ok(Self::Show(crate::ast::ShowStatement::Databases)),
            CreateTable(create) => Ok(Self::CreateTable(create.try_into()?)),
            Insert(insert) => Ok(Self::Insert(insert.try_into()?)),
            Delete(delete) => Ok(Self::Delete(delete.try_into()?)),
            create @ CreateView { .. } => Ok(Self::CreateView(create.try_into()?)),
            update @ Update { .. } => Ok(Self::Update(update.try_into()?)),
            Use(use_statement) => Ok(Self::Use(use_statement.into())),
            set_variable @ SetVariable { .. } => Ok(Self::Set(set_variable.into())),
            set_names @ SetNames { .. } => Ok(Self::Set(set_names.into())),
            Drop {
                object_type,
                if_exists,
                names,
                ..
            } => match object_type {
                sqlparser::ast::ObjectType::Table => Ok(Self::DropTable(DropTableStatement {
                    tables: names.into_iter().map(Into::into).collect(),
                    if_exists,
                })),
                sqlparser::ast::ObjectType::View => Ok(Self::DropView(DropViewStatement {
                    views: names.into_iter().map(Into::into).collect(),
                    if_exists,
                })),
                _ => not_yet_implemented!("drop statement tye: {object_type:?}"),
            },
            StartTransaction { begin, .. } => Ok(Self::StartTransaction(if begin {
                StartTransactionStatement::Begin
            } else {
                StartTransactionStatement::Start
            })),
            CreateType { .. } => Err(AstConversionError::Skipped(format!("CREATE TYPE: {value}"))),
            Rollback { .. } => Ok(Self::Rollback(RollbackStatement {})),
            Commit { .. } => Ok(Self::Commit(CommitStatement {})),
            _ => not_yet_implemented!("other query: {value:?}"),
        }
    }
}

impl TryFrom<sqlparser::ast::Query> for SqlQuery {
    type Error = AstConversionError;

    fn try_from(value: sqlparser::ast::Query) -> Result<Self, Self::Error> {
        if matches!(*value.body, sqlparser::ast::SetExpr::Select(_)) {
            Ok(SqlQuery::Select(value.try_into()?))
        } else {
            not_yet_implemented!("unsupported non-select query type {value:?}")
        }
    }
}

impl TryFrom<Box<sqlparser::ast::Query>> for SqlQuery {
    type Error = AstConversionError;

    fn try_from(value: Box<sqlparser::ast::Query>) -> Result<Self, Self::Error> {
        (*value).try_into()
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
            Self::CreateCache(_) => "CREATE CACHE",
            Self::DropCache(_) => "DROP CACHE",
            Self::DropAllCaches(_) => "DROP ALL CACHES",
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
            Self::Comment(_) => "COMMENT",
            Self::Deallocate(_) => "DEALLOCATE",
            Self::Truncate(_) => "TRUNCATE",
        }
    }

    /// Returns whether the provided SqlQuery is a SELECT or not.
    pub fn is_select(&self) -> bool {
        matches!(self, Self::Select(_))
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
            | SqlQuery::AlterReadySet(_)
            | SqlQuery::DropAllProxiedQueries(_) => true,
            SqlQuery::Show(show_stmt) => match show_stmt {
                ShowStatement::Events | ShowStatement::Tables(_) | ShowStatement::Databases => {
                    false
                }
                ShowStatement::CachedQueries(_)
                | ShowStatement::ProxiedQueries(_)
                | ShowStatement::ReadySetStatus
                | ShowStatement::ReadySetStatusAdapter
                | ShowStatement::ReadySetMigrationStatus(_)
                | ShowStatement::ReadySetVersion
                | ShowStatement::ReadySetTables(..)
                | ShowStatement::Connections => true,
            },
            SqlQuery::CreateDatabase(_)
            | SqlQuery::CreateTable(_)
            | SqlQuery::CreateView(_)
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
            | SqlQuery::Comment(_) => false,
        }
    }
}
