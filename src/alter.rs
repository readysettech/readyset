/// ALTER TABLE Statement AST and parsing (incomplete)
///
/// See https://dev.mysql.com/doc/refman/8.0/en/alter-table.html
use std::fmt;

use crate::{ColumnSpecification, Literal, Table};

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum AlterColumnOperation {
    SetColumnDefault(Literal),
    DropColumnDefault,
}

impl fmt::Display for AlterColumnOperation {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            AlterColumnOperation::SetColumnDefault(val) => {
                write!(f, "SET DEFAULT {}", val.to_string())
            }
            AlterColumnOperation::DropColumnDefault => write!(f, "DROP DEFAULT"),
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
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

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum AlterTableDefinition {
    AddColumn(ColumnSpecification),
    AlterColumn {
        name: String,
        operation: AlterColumnOperation,
    },
    DropColumn {
        name: String,
        behavior: Option<DropBehavior>,
    },
    // TODO(grfn): https://ronsavage.github.io/SQL/sql-2003-2.bnf.html#add%20table%20constraint%20definition
    // AddTableConstraint(..),
    // TODO(grfn): https://ronsavage.github.io/SQL/sql-2003-2.bnf.html#drop%20table%20constraint%20definition
    // DropTableConstraint(..),
}

impl fmt::Display for AlterTableDefinition {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            AlterTableDefinition::AddColumn(col) => {
                write!(f, "ADD COLUMN {}", col)
            }
            AlterTableDefinition::AlterColumn { name, operation } => {
                write!(f, "ALTER COLUMN {} {}", name, operation)
            }
            AlterTableDefinition::DropColumn { name, behavior } => {
                write!(f, "DROP COLUMN {}", name)?;
                if let Some(behavior) = behavior {
                    write!(f, " {}", behavior)?;
                }
                Ok(())
            }
        }
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct AlterTableStatement {
    pub table: Table,
    pub definitions: Vec<AlterTableDefinition>,
}

impl fmt::Display for AlterTableStatement {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ALTER TABLE {} ", escape_if_keyword(&self.table.name))?;
        write!(
            f,
            "{}",
            self.definitions
                .iter()
                .map(|def| format!("{}", def))
                .collect::<Vec<_>>()
                .join(", ")
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::{Column, SqlType};

    use super::*;

    #[test]
    fn display_add_column() {
        let stmt = AlterTableStatement {
            table: "t".into(),
            definitions: vec![AlterTableDefinition::AddColumn(ColumnSpecification {
                column: Column {
                    name: "c".into(),
                    alias: None,
                    table: None,
                    function: None,
                },
                sql_type: SqlType::Int(32),
                comment: None,
                constraints: vec![],
            })],
        };

        let result = format!("{}", stmt);
        assert_eq!(result, "ALTER TABLE t ADD COLUMN c INT(32)");
    }

    #[test]
    fn parse_add_column() {
        let qstring = "ALTER TABLE `t` ADD COLUMN `c` INT";
    }
}
