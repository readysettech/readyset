use std::fmt;

use itertools::Itertools;
use proptest::{
    prelude::{Strategy as _, any_with},
    sample::size_range,
};
use readyset_util::fmt::fmt_with;
use serde::{Deserialize, Serialize};
use test_strategy::Arbitrary;

use crate::{
    AstConversionError, Dialect, DialectDisplay, IntoDialect, TryFromDialect, TryIntoDialect,
    ast::*,
};

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub enum AlterColumnOperation {
    SetColumnDefault(Literal),
    DropColumnDefault,
}

impl TryFrom<sqlparser::ast::AlterColumnOperation> for AlterColumnOperation {
    type Error = AstConversionError;

    fn try_from(value: sqlparser::ast::AlterColumnOperation) -> Result<Self, Self::Error> {
        match value {
            sqlparser::ast::AlterColumnOperation::SetDefault {
                value: sqlparser::ast::Expr::Value(value),
            } => Ok(Self::SetColumnDefault(value.try_into()?)),
            sqlparser::ast::AlterColumnOperation::DropDefault => Ok(Self::DropColumnDefault),
            _ => unsupported!("ALTER COLUMN operation {value}"),
        }
    }
}

impl DialectDisplay for AlterColumnOperation {
    fn display(&self, dialect: Dialect) -> impl fmt::Display + '_ {
        fmt_with(move |f| match self {
            AlterColumnOperation::SetColumnDefault(val) => {
                write!(f, "SET DEFAULT {}", val.display(dialect))
            }
            AlterColumnOperation::DropColumnDefault => write!(f, "DROP DEFAULT"),
        })
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub enum DropBehavior {
    Cascade,
    Restrict,
}

impl From<sqlparser::ast::DropBehavior> for DropBehavior {
    fn from(value: sqlparser::ast::DropBehavior) -> Self {
        match value {
            sqlparser::ast::DropBehavior::Cascade => Self::Cascade,
            sqlparser::ast::DropBehavior::Restrict => Self::Restrict,
        }
    }
}

impl fmt::Display for DropBehavior {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            DropBehavior::Cascade => write!(f, "CASCADE"),
            DropBehavior::Restrict => write!(f, "RESTRICT"),
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub enum ReplicaIdentity {
    Default,
    UsingIndex {
        index_name: SqlIdentifier,
    },
    Full,
    // FIXME(REA-5862): Not correctly parsed by sqlparser
    #[weight(0)]
    Nothing,
}

impl fmt::Display for ReplicaIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ReplicaIdentity::Default => write!(f, "DEFAULT"),
            ReplicaIdentity::UsingIndex { index_name } => write!(f, "USING INDEX {index_name}"),
            ReplicaIdentity::Full => write!(f, "FULL"),
            ReplicaIdentity::Nothing => write!(f, "NOTHING"),
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub enum AlterTableLock {
    Default,
    None,
    Shared,
    Exclusive,
}

impl fmt::Display for AlterTableLock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AlterTableLock::Default => write!(f, "DEFAULT"),
            AlterTableLock::None => write!(f, "NONE"),
            AlterTableLock::Shared => write!(f, "SHARED"),
            AlterTableLock::Exclusive => write!(f, "EXCLUSIVE"),
        }
    }
}

impl From<sqlparser::ast::AlterTableLock> for AlterTableLock {
    fn from(lock: sqlparser::ast::AlterTableLock) -> Self {
        match lock {
            sqlparser::ast::AlterTableLock::Default => AlterTableLock::Default,
            sqlparser::ast::AlterTableLock::None => AlterTableLock::None,
            sqlparser::ast::AlterTableLock::Shared => AlterTableLock::Shared,
            sqlparser::ast::AlterTableLock::Exclusive => AlterTableLock::Exclusive,
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub enum AlterTableAlgorithm {
    Default,
    Instant,
    Inplace,
    Copy,
}

impl fmt::Display for AlterTableAlgorithm {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AlterTableAlgorithm::Default => write!(f, "DEFAULT"),
            AlterTableAlgorithm::Instant => write!(f, "INSTANT"),
            AlterTableAlgorithm::Inplace => write!(f, "INPLACE"),
            AlterTableAlgorithm::Copy => write!(f, "COPY"),
        }
    }
}

impl From<sqlparser::ast::AlterTableAlgorithm> for AlterTableAlgorithm {
    fn from(algorithm: sqlparser::ast::AlterTableAlgorithm) -> Self {
        match algorithm {
            sqlparser::ast::AlterTableAlgorithm::Default => AlterTableAlgorithm::Default,
            sqlparser::ast::AlterTableAlgorithm::Instant => AlterTableAlgorithm::Instant,
            sqlparser::ast::AlterTableAlgorithm::Inplace => AlterTableAlgorithm::Inplace,
            sqlparser::ast::AlterTableAlgorithm::Copy => AlterTableAlgorithm::Copy,
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub enum AlterTableDefinition {
    AddColumn(ColumnSpecification),
    AddKey(TableKey),
    AlterColumn {
        name: SqlIdentifier,
        operation: AlterColumnOperation,
    },
    DropColumn {
        name: SqlIdentifier,
        behavior: Option<DropBehavior>,
    },
    ChangeColumn {
        name: SqlIdentifier,
        spec: ColumnSpecification,
    },
    RenameColumn {
        name: SqlIdentifier,
        new_name: SqlIdentifier,
    },
    DropConstraint {
        name: SqlIdentifier,
        drop_behavior: Option<DropBehavior>,
    },
    ReplicaIdentity(ReplicaIdentity),
    DropForeignKey {
        name: SqlIdentifier,
    },
    // Though the following aren't really definitions/operations, from a parsing perspective it makes it
    // easier to treat them the same way.
    /// [DEFAULT | INPLACE | COPY | INSTANT]
    Lock {
        equals: bool,
        lock: AlterTableLock,
    },
    /// [DEFAULT | NONE | SHARED | EXCLUSIVE]
    Algorithm {
        equals: bool,
        algorithm: AlterTableAlgorithm,
    },
}

impl TryFromDialect<sqlparser::ast::AlterTableOperation> for AlterTableDefinition {
    fn try_from_dialect(
        value: sqlparser::ast::AlterTableOperation,
        dialect: Dialect,
    ) -> Result<Self, AstConversionError> {
        use sqlparser::ast::AlterTableOperation::*;
        match value {
            AddColumn {
                column_keyword: _,
                if_not_exists: _,
                column_def,
                column_position: _,
            } => Ok(Self::AddColumn(column_def.try_into_dialect(dialect)?)),
            DropConstraint {
                if_exists: _,
                name,
                drop_behavior,
            } => Ok(Self::DropConstraint {
                name: name.into_dialect(dialect),
                drop_behavior: drop_behavior.map(Into::into),
            }),
            DropColumn {
                column_name,
                if_exists: _,
                drop_behavior,
                has_column_keyword: _,
            } => Ok(Self::DropColumn {
                name: column_name.into_dialect(dialect),
                behavior: drop_behavior.map(Into::into),
            }),
            RenameColumn {
                old_column_name,
                new_column_name,
            } => Ok(Self::RenameColumn {
                name: old_column_name.into_dialect(dialect),
                new_name: new_column_name.into_dialect(dialect),
            }),
            ChangeColumn {
                old_name,
                new_name,
                data_type,
                options,
                column_position: _,
            } => Ok(Self::ChangeColumn {
                name: old_name.into_dialect(dialect),
                spec: sqlparser::ast::ColumnDef {
                    name: new_name,
                    data_type,
                    options: options
                        .into_iter()
                        .map(|option| sqlparser::ast::ColumnOptionDef { name: None, option })
                        .collect(),
                }
                .try_into_dialect(dialect)?,
            }),
            AlterColumn { column_name, op } => Ok(Self::AlterColumn {
                name: column_name.into_dialect(dialect),
                operation: op.try_into()?,
            }),
            Lock { equals, lock } => Ok(Self::Lock {
                equals,
                lock: lock.into(),
            }),
            Algorithm { equals, algorithm } => Ok(Self::Algorithm {
                equals,
                algorithm: algorithm.into(),
            }),
            _ => unsupported!("ALTER TABLE definition {value}"),
        }
    }
}

impl DialectDisplay for AlterTableDefinition {
    fn display(&self, dialect: Dialect) -> impl fmt::Display + '_ {
        fmt_with(move |f| match self {
            Self::AddColumn(col) => {
                write!(f, "ADD COLUMN {}", col.display(dialect))
            }
            Self::AddKey(index) => {
                write!(f, "ADD {}", index.display(dialect))
            }
            Self::AlterColumn { name, operation } => {
                write!(
                    f,
                    "ALTER COLUMN {} {}",
                    dialect.quote_identifier(name),
                    operation.display(dialect)
                )
            }
            Self::DropColumn { name, behavior } => {
                write!(f, "DROP COLUMN {}", dialect.quote_identifier(name))?;
                if let Some(behavior) = behavior {
                    write!(f, " {behavior}")?;
                }
                Ok(())
            }
            Self::ChangeColumn { name, spec } => {
                write!(
                    f,
                    "CHANGE COLUMN {} {}",
                    dialect.quote_identifier(name),
                    spec.display(dialect)
                )
            }
            Self::RenameColumn { name, new_name } => {
                write!(
                    f,
                    "RENAME COLUMN {} TO {}",
                    dialect.quote_identifier(name),
                    dialect.quote_identifier(new_name)
                )
            }
            Self::DropConstraint {
                name,
                drop_behavior,
            } => match drop_behavior {
                None => write!(f, "DROP CONSTRAINT {name}"),
                Some(d) => write!(f, "DROP CONSTRAINT {name} {d}"),
            },
            Self::ReplicaIdentity(replica_identity) => {
                write!(f, "REPLICA IDENTITY {replica_identity}")
            }
            Self::DropForeignKey { name } => write!(f, "DROP FOREIGN KEY {name}"),
            Self::Lock { equals, lock } => {
                write!(f, "LOCK")?;
                if *equals {
                    write!(f, " =")?;
                }
                write!(f, " {lock}")
            }
            Self::Algorithm { equals, algorithm } => {
                write!(f, "ALGORITHM")?;
                if *equals {
                    write!(f, " =")?;
                }
                write!(f, " {algorithm}")
            }
        })
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub struct AlterTableStatement {
    pub table: Relation,
    pub only: bool,
    /// The result of parsing the alter table definitions.
    ///
    /// If the parsing succeeded, then this will be an `Ok` result with the list of
    /// [`AlterTableDefinition`]s.  If it failed to parse, this will be an `Err` with the remainder
    /// [`String`] that could not be parsed.
    #[strategy(any_with::<Vec<AlterTableDefinition>>(size_range(1..16).lift()).prop_map(Ok))]
    pub definitions: Result<Vec<AlterTableDefinition>, String>,
}

impl TryFromDialect<sqlparser::ast::Statement> for AlterTableStatement {
    fn try_from_dialect(
        value: sqlparser::ast::Statement,
        dialect: Dialect,
    ) -> Result<Self, AstConversionError> {
        if let sqlparser::ast::Statement::AlterTable {
            name,
            if_exists: _,
            only,
            operations,
            location: _,
            on_cluster: _,
            iceberg: _,
        } = value
        {
            Ok(Self {
                table: name.into_dialect(dialect),
                only,
                definitions: Ok(operations.try_into_dialect(dialect)?),
            })
        } else {
            failed!("Expected ALTER TABLE statement")
        }
    }
}

impl DialectDisplay for AlterTableStatement {
    fn display(&self, dialect: Dialect) -> impl fmt::Display + '_ {
        fmt_with(move |f| {
            write!(f, "ALTER TABLE {} ", self.table.display(dialect))?;

            match &self.definitions {
                Ok(definitions) => {
                    write!(
                        f,
                        "{}",
                        definitions
                            .iter()
                            .map(|def| def.display(dialect))
                            .join(", ")
                    )?;
                }
                Err(unparsed) => {
                    write!(f, "{unparsed}")?;
                }
            }
            Ok(())
        })
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub struct ResnapshotTableStatement {
    pub table: Relation,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub struct AddTablesStatement {
    pub tables: Vec<Relation>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub enum AlterReadysetStatement {
    ResnapshotTable(ResnapshotTableStatement),
    AddTables(AddTablesStatement),
    EnterMaintenanceMode,
    ExitMaintenanceMode,
}

impl DialectDisplay for AlterReadysetStatement {
    fn display(&self, dialect: Dialect) -> impl fmt::Display + '_ {
        fmt_with(move |f| match self {
            Self::ResnapshotTable(stmt) => {
                write!(f, "RESNAPSHOT TABLE {}", stmt.table.display(dialect))
            }
            Self::AddTables(stmt) => {
                write!(
                    f,
                    "ADD TABLES {}",
                    stmt.tables.iter().map(|t| t.display(dialect)).join(", ")
                )
            }
            Self::EnterMaintenanceMode => {
                write!(f, "ENTER MAINTENANCE MODE")
            }
            Self::ExitMaintenanceMode => {
                write!(f, "EXIT MAINTENANCE MODE")
            }
        })
    }
}
