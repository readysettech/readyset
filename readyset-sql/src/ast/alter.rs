use std::fmt;

use itertools::Itertools;
use readyset_util::fmt::fmt_with;
use serde::{Deserialize, Serialize};
use test_strategy::Arbitrary;

use crate::{ast::*, Dialect, DialectDisplay};

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub enum AlterColumnOperation {
    SetColumnDefault(Literal),
    DropColumnDefault,
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
    UsingIndex { index_name: SqlIdentifier },
    Full,
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
    /* TODO(aspen): https://ronsavage.github.io/SQL/sql-2003-2.bnf.html#add%20table%20constraint%20definition
     * AddTableConstraint(..),
     * TODO(aspen): https://ronsavage.github.io/SQL/sql-2003-2.bnf.html#drop%20table%20constraint%20definition
     * DropTableConstraint(..), */
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
                    write!(f, " {}", behavior)?;
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
                    "RENAME COLUMN {} {}",
                    dialect.quote_identifier(name),
                    dialect.quote_identifier(new_name)
                )
            }
            Self::DropConstraint {
                name,
                drop_behavior,
            } => match drop_behavior {
                None => write!(f, "DROP CONSTRAINT {}", name),
                Some(d) => write!(f, "DROP CONSTRAINT {} {}", name, d),
            },
            Self::ReplicaIdentity(replica_identity) => {
                write!(f, "REPLICA IDENTITY {replica_identity}")
            }
        })
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub struct AlterTableStatement {
    pub table: Relation,
    /// The result of parsing the alter table definitions.
    ///
    /// If the parsing succeeded, then this will be an `Ok` result with the list of
    /// [`AlterTableDefinition`]s.  If it failed to parse, this will be an `Err` with the remainder
    /// [`String`] that could not be parsed.
    pub definitions: Result<Vec<AlterTableDefinition>, String>,
    pub only: bool,
    pub algorithm: Option<String>,
    pub lock: Option<String>,
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
                    write!(f, "{}", unparsed)?;
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
                    "ADD TABLEs {}",
                    stmt.tables.iter().map(|t| t.display(dialect)).join(", ")
                )
            }
        })
    }
}
