/// ALTER TABLE Statement AST and parsing (incomplete)
///
/// See https://dev.mysql.com/doc/refman/8.0/en/alter-table.html
use crate::{ColumnSpecification, Literal, Table};

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum AlterColumnOperation {
    SetColumnDefault(Literal),
    DropColumnDefault,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum DropBehavior {
    Cascade,
    Restrict,
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

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct AlterTableStatement {
    pub table: Table,
    pub definitions: Vec<AlterTableDefinition>,
}
