/// ALTER TABLE Statement AST and parsing (incomplete)
///
/// See https://dev.mysql.com/doc/refman/8.0/en/alter-table.html
use std::{fmt, str};

use nom::character::complete::{multispace0, multispace1};
use nom::{alt, do_parse, map_res, named, opt, preceded, separated_list, tag_no_case, terminated};

use crate::column::{column_specification, ColumnSpecification};
use crate::common::{
    literal, schema_table_reference, sql_identifier, statement_terminator, ws_sep_comma, Literal,
};
use crate::keywords::escape_if_keyword;
use crate::table::Table;

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

named!(
    add_column<AlterTableDefinition>,
    do_parse!(
        tag_no_case!("add")
            >> multispace1
            >> tag_no_case!("column")
            >> multispace1
            >> column: column_specification
            >> (AlterTableDefinition::AddColumn(column))
    )
);

named!(
    drop_behavior<DropBehavior>,
    alt!(
        tag_no_case!("cascade") => { |_| DropBehavior::Cascade} |
        tag_no_case!("restrict") => { |_| DropBehavior::Restrict }
    )
);

named!(
    drop_column<AlterTableDefinition>,
    do_parse!(
        tag_no_case!("drop")
            >> multispace1
            >> tag_no_case!("column")
            >> multispace1
            >> name: map_res!(sql_identifier, str::from_utf8)
            >> behavior: opt!(preceded!(multispace1, drop_behavior))
            >> (AlterTableDefinition::DropColumn {
                name: name.to_string(),
                behavior,
            })
    )
);

named!(
    set_default<AlterColumnOperation>,
    do_parse!(
        opt!(terminated!(tag_no_case!("set"), multispace1))
            >> tag_no_case!("default")
            >> multispace1
            >> value: literal
            >> (AlterColumnOperation::SetColumnDefault(value))
    )
);

named!(
    drop_default<AlterColumnOperation>,
    do_parse!(
        tag_no_case!("drop")
            >> multispace1
            >> tag_no_case!("default")
            >> (AlterColumnOperation::DropColumnDefault)
    )
);

named!(
    alter_column_operation<AlterColumnOperation>,
    alt!(set_default | drop_default)
);

named!(
    alter_column<AlterTableDefinition>,
    do_parse!(
        tag_no_case!("alter")
            >> multispace1
            >> tag_no_case!("column")
            >> multispace1
            >> name: map_res!(sql_identifier, str::from_utf8)
            >> multispace1
            >> operation: alter_column_operation
            >> (AlterTableDefinition::AlterColumn {
                name: name.to_string(),
                operation
            })
    )
);

named!(
    alter_table_definition<AlterTableDefinition>,
    alt!(add_column | drop_column | alter_column)
);

named!(
    pub alter_table_statement<AlterTableStatement>,
    do_parse!(
        tag_no_case!("alter")
            >> multispace1
            >> tag_no_case!("table")
            >> multispace1
            >> table: schema_table_reference
            >> multispace1
            >> definitions: separated_list!(ws_sep_comma, alter_table_definition)
            >> multispace0
            >> statement_terminator
            >> (AlterTableStatement { table, definitions })
    )
);

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
        let expected = AlterTableStatement {
            table: Table {
                name: "t".into(),
                schema: None,
                alias: None,
            },
            definitions: vec![AlterTableDefinition::AddColumn(ColumnSpecification {
                column: Column {
                    name: "c".into(),
                    alias: None,
                    table: None,
                    function: None,
                },
                sql_type: SqlType::Int(32),
                constraints: vec![],
                comment: None,
            })],
        };
        let result = alter_table_statement(qstring.as_bytes());
        assert_eq!(result.unwrap().1, expected);
    }

    #[test]
    fn parse_add_two_columns() {
        let qstring = "ALTER TABLE `t` ADD COLUMN `c` INT, ADD COLUMN `d` TEXT";
        let expected = AlterTableStatement {
            table: Table {
                name: "t".into(),
                schema: None,
                alias: None,
            },
            definitions: vec![
                AlterTableDefinition::AddColumn(ColumnSpecification {
                    column: Column {
                        name: "c".into(),
                        alias: None,
                        table: None,
                        function: None,
                    },
                    sql_type: SqlType::Int(32),
                    constraints: vec![],
                    comment: None,
                }),
                AlterTableDefinition::AddColumn(ColumnSpecification {
                    column: Column {
                        name: "d".into(),
                        alias: None,
                        table: None,
                        function: None,
                    },
                    sql_type: SqlType::Text,
                    constraints: vec![],
                    comment: None,
                }),
            ],
        };
        let result = alter_table_statement(qstring.as_bytes());
        assert_eq!(result.unwrap().1, expected);
    }

    #[test]
    fn parse_drop_column_no_behavior() {
        let qstring = "ALTER TABLE `t` DROP COLUMN c";
        let expected = AlterTableStatement {
            table: Table {
                name: "t".into(),
                schema: None,
                alias: None,
            },
            definitions: vec![AlterTableDefinition::DropColumn {
                name: "c".into(),
                behavior: None,
            }],
        };
        let result = alter_table_statement(qstring.as_bytes());
        print_trace!();
        assert_eq!(result.unwrap().1, expected);
    }

    #[test]
    fn parse_drop_column_cascade() {
        let qstring = "ALTER TABLE `t` DROP COLUMN c CASCADE";
        let expected = AlterTableStatement {
            table: Table {
                name: "t".into(),
                schema: None,
                alias: None,
            },
            definitions: vec![AlterTableDefinition::DropColumn {
                name: "c".into(),
                behavior: Some(DropBehavior::Cascade),
            }],
        };
        let result = alter_table_statement(qstring.as_bytes());
        assert_eq!(result.unwrap().1, expected);
    }

    #[test]
    fn parse_alter_column_set_default() {
        let qstring = "ALTER TABLE `t` ALTER COLUMN c SET DEFAULT 'foo'";
        let expected = AlterTableStatement {
            table: Table {
                name: "t".into(),
                schema: None,
                alias: None,
            },
            definitions: vec![AlterTableDefinition::AlterColumn {
                name: "c".into(),
                operation: AlterColumnOperation::SetColumnDefault(Literal::String("foo".into())),
            }],
        };
        let result = alter_table_statement(qstring.as_bytes());
        print_trace!();
        assert_eq!(result.unwrap().1, expected);
    }

    #[test]
    fn parse_alter_column_drop_default() {
        let qstring = "ALTER TABLE `t` ALTER COLUMN c DROP DEFAULT";
        let expected = AlterTableStatement {
            table: Table {
                name: "t".into(),
                schema: None,
                alias: None,
            },
            definitions: vec![AlterTableDefinition::AlterColumn {
                name: "c".into(),
                operation: AlterColumnOperation::DropColumnDefault,
            }],
        };
        let result = alter_table_statement(qstring.as_bytes());
        print_trace!();
        assert_eq!(result.unwrap().1, expected);
    }
}
