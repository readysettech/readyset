/// ALTER TABLE Statement AST and parsing (incomplete)
///
/// See https://dev.mysql.com/doc/refman/8.0/en/alter-table.html
use std::{fmt, str};

use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::character::complete::{multispace0, multispace1};
use nom::combinator::{map, opt};
use nom::multi::separated_list0;
use nom::sequence::{preceded, terminated};
use nom::IResult;
use serde::{Deserialize, Serialize};

use crate::column::{column_specification, ColumnSpecification};
use crate::common::{
    literal, schema_table_reference_no_alias, statement_terminator, ws_sep_comma, Literal, TableKey,
};
use crate::create::key_specification;
use crate::table::Table;
use crate::{Dialect, SqlIdentifier};

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum AlterColumnOperation {
    SetColumnDefault(Literal),
    DropColumnDefault,
}

impl fmt::Display for AlterColumnOperation {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            AlterColumnOperation::SetColumnDefault(val) => {
                write!(f, "SET DEFAULT {}", val)
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
#[allow(clippy::enum_variant_names)]
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
            AlterTableDefinition::AddKey(index) => {
                write!(f, "ADD {}", index)
            }
            AlterTableDefinition::AlterColumn { name, operation } => {
                write!(f, "ALTER COLUMN `{}` {}", name, operation)
            }
            AlterTableDefinition::DropColumn { name, behavior } => {
                write!(f, "DROP COLUMN `{}`", name)?;
                if let Some(behavior) = behavior {
                    write!(f, " {}", behavior)?;
                }
                Ok(())
            }
            AlterTableDefinition::ChangeColumn { name, spec } => {
                write!(f, "CHANGE COLUMN `{}` {}", name, spec)
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
        write!(f, "ALTER TABLE `{}` ", self.table.name)?;
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

fn add_column(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], AlterTableDefinition> {
    move |i| {
        let (i, _) = tag_no_case("add")(i)?;
        let (i, _) = opt(preceded(multispace1, tag_no_case("column")))(i)?;
        let (i, _) = multispace1(i)?;

        map(column_specification(dialect), |c| {
            AlterTableDefinition::AddColumn(c)
        })(i)
    }
}

fn add_key(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], AlterTableDefinition> {
    move |i| {
        let (i, _) = tag_no_case("add")(i)?;
        let (i, _) = multispace1(i)?;

        map(key_specification(dialect), |k| {
            AlterTableDefinition::AddKey(k)
        })(i)
    }
}

fn drop_behavior(i: &[u8]) -> IResult<&[u8], DropBehavior> {
    alt((
        map(tag_no_case("cascade"), |_| DropBehavior::Cascade),
        map(tag_no_case("restrict"), |_| DropBehavior::Restrict),
    ))(i)
}

fn drop_column(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], AlterTableDefinition> {
    move |i| {
        let (i, _) = tag_no_case("drop")(i)?;
        let (i, _) = multispace1(i)?;
        let (i, _) = tag_no_case("column")(i)?;
        let (i, _) = multispace1(i)?;

        let (i, name) = dialect.identifier()(i)?;
        let (i, behavior) = opt(preceded(multispace1, drop_behavior))(i)?;

        Ok((i, AlterTableDefinition::DropColumn { name, behavior }))
    }
}

fn set_default(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], AlterColumnOperation> {
    move |i| {
        let (i, _) = opt(terminated(tag_no_case("set"), multispace1))(i)?;
        let (i, _) = tag_no_case("default")(i)?;
        let (i, _) = multispace1(i)?;

        map(literal(dialect), |v| {
            AlterColumnOperation::SetColumnDefault(v)
        })(i)
    }
}

fn drop_default(i: &[u8]) -> IResult<&[u8], AlterColumnOperation> {
    let (i, _) = tag_no_case("drop")(i)?;
    let (i, _) = multispace1(i)?;
    let (i, _) = tag_no_case("default")(i)?;

    Ok((i, AlterColumnOperation::DropColumnDefault))
}

fn alter_column_operation(
    dialect: Dialect,
) -> impl Fn(&[u8]) -> IResult<&[u8], AlterColumnOperation> {
    move |i| alt((set_default(dialect), drop_default))(i)
}

fn alter_column(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], AlterTableDefinition> {
    move |i| {
        let (i, _) = tag_no_case("alter")(i)?;
        let (i, _) = multispace1(i)?;
        let (i, _) = tag_no_case("column")(i)?;
        let (i, _) = multispace1(i)?;

        let (i, name) = dialect.identifier()(i)?;
        let (i, _) = multispace1(i)?;
        let (i, operation) = alter_column_operation(dialect)(i)?;

        Ok((i, AlterTableDefinition::AlterColumn { name, operation }))
    }
}

fn change_column(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], AlterTableDefinition> {
    move |i| {
        let (i, _) = tag_no_case("change")(i)?;
        let (i, _) = opt(preceded(multispace1, tag_no_case("column")))(i)?;
        let (i, _) = multispace1(i)?;

        let (i, name) = dialect.identifier()(i)?;
        let (i, _) = multispace1(i)?;
        let (i, spec) = column_specification(dialect)(i)?;

        Ok((i, AlterTableDefinition::ChangeColumn { name, spec }))
    }
}

fn modify_column(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], AlterTableDefinition> {
    // TODO: FIRST, AFTER col_name
    move |i| {
        let (i, _) = tag_no_case("modify")(i)?;
        let (i, _) = opt(preceded(multispace1, tag_no_case("column")))(i)?;
        let (i, _) = multispace1(i)?;

        map(column_specification(dialect), |spec| {
            AlterTableDefinition::ChangeColumn {
                name: spec.column.name.clone(),
                spec,
            }
        })(i)
    }
}

fn alter_table_definition(
    dialect: Dialect,
) -> impl Fn(&[u8]) -> IResult<&[u8], AlterTableDefinition> {
    move |i| {
        alt((
            add_column(dialect),
            add_key(dialect),
            drop_column(dialect),
            alter_column(dialect),
            change_column(dialect),
            modify_column(dialect),
        ))(i)
    }
}

pub fn alter_table_statement(
    dialect: Dialect,
) -> impl Fn(&[u8]) -> IResult<&[u8], AlterTableStatement> {
    move |i| {
        let (i, _) = tag_no_case("alter")(i)?;
        let (i, _) = multispace1(i)?;
        let (i, _) = tag_no_case("table")(i)?;
        let (i, _) = multispace1(i)?;

        let (i, table) = schema_table_reference_no_alias(dialect)(i)?;
        let (i, _) = multispace1(i)?;
        let (i, definitions) = separated_list0(ws_sep_comma, alter_table_definition(dialect))(i)?;
        let (i, _) = multispace0(i)?;
        let (i, _) = statement_terminator(i)?;

        Ok((i, AlterTableStatement { table, definitions }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Column, Dialect, SqlType};

    #[test]
    fn display_add_column() {
        let stmt = AlterTableStatement {
            table: "t".into(),
            definitions: vec![AlterTableDefinition::AddColumn(ColumnSpecification {
                column: Column {
                    name: "c".into(),
                    table: None,
                },
                sql_type: SqlType::Int(Some(32)),
                comment: None,
                constraints: vec![],
            })],
        };

        let result = format!("{}", stmt);
        assert_eq!(result, "ALTER TABLE `t` ADD COLUMN `c` INT(32)");
    }

    #[test]
    fn parse_add_column_no_column_tag() {
        let qstring = b"ALTER TABLE employees ADD Email varchar(255), ADD snailmail TEXT";
        let expected = AlterTableStatement {
            table: Table {
                name: "employees".into(),
                schema: None,
                alias: None,
            },
            definitions: vec![
                AlterTableDefinition::AddColumn(ColumnSpecification {
                    column: Column {
                        name: "Email".into(),
                        table: None,
                    },
                    sql_type: SqlType::Varchar(Some(255)),
                    constraints: vec![],
                    comment: None,
                }),
                AlterTableDefinition::AddColumn(ColumnSpecification {
                    column: Column {
                        name: "snailmail".into(),
                        table: None,
                    },
                    sql_type: SqlType::Text,
                    constraints: vec![],
                    comment: None,
                }),
            ],
        };
        let result = alter_table_statement(Dialect::MySQL)(qstring);
        assert_eq!(result.unwrap().1, expected);
    }

    mod mysql {
        use super::*;
        use crate::common::ReferentialAction;
        use crate::{Column, ColumnConstraint, SqlType};

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
                        table: None,
                    },
                    sql_type: SqlType::Int(None),
                    constraints: vec![],
                    comment: None,
                })],
            };
            let result = alter_table_statement(Dialect::MySQL)(qstring.as_bytes());
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
                            table: None,
                        },
                        sql_type: SqlType::Int(None),
                        constraints: vec![],
                        comment: None,
                    }),
                    AlterTableDefinition::AddColumn(ColumnSpecification {
                        column: Column {
                            name: "d".into(),
                            table: None,
                        },
                        sql_type: SqlType::Text,
                        constraints: vec![],
                        comment: None,
                    }),
                ],
            };
            let result = alter_table_statement(Dialect::MySQL)(qstring.as_bytes());
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
            let result = alter_table_statement(Dialect::MySQL)(qstring.as_bytes());
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
            let result = alter_table_statement(Dialect::MySQL)(qstring.as_bytes());
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
                    operation: AlterColumnOperation::SetColumnDefault(Literal::String(
                        "foo".into(),
                    )),
                }],
            };
            let result = alter_table_statement(Dialect::MySQL)(qstring.as_bytes());
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
            let result = alter_table_statement(Dialect::MySQL)(qstring.as_bytes());
            assert_eq!(result.unwrap().1, expected);
        }

        #[test]
        fn flarum_alter_1() {
            let qstring = b"ALTER TABLE flags CHANGE time created_at DATETIME NOT NULL";
            let res = test_parse!(alter_table_statement(Dialect::MySQL), qstring);
            assert_eq!(
                res,
                AlterTableStatement {
                    table: Table::from("flags"),
                    definitions: vec![AlterTableDefinition::ChangeColumn {
                        name: "time".into(),
                        spec: ColumnSpecification {
                            column: Column::from("created_at"),
                            sql_type: SqlType::DateTime(None),
                            constraints: vec![ColumnConstraint::NotNull],
                            comment: None,
                        }
                    }]
                }
            );
        }

        #[test]
        fn alter_modify() {
            let qstring = b"ALTER TABLE t MODIFY f VARCHAR(255) NOT NULL PRIMARY KEY";
            let res = test_parse!(alter_table_statement(Dialect::MySQL), qstring);
            assert_eq!(
                res,
                AlterTableStatement {
                    table: Table::from("t"),
                    definitions: vec![AlterTableDefinition::ChangeColumn {
                        name: "f".into(),
                        spec: ColumnSpecification {
                            column: Column::from("f"),
                            sql_type: SqlType::Varchar(Some(255)),
                            constraints: vec![
                                ColumnConstraint::NotNull,
                                ColumnConstraint::PrimaryKey
                            ],
                            comment: None,
                        }
                    }]
                }
            );
        }

        #[test]
        fn alter_roundtrip_with_escaped_column() {
            let qstring = b"ALTER TABLE t CHANGE f `modify` DATETIME";
            let res = test_parse!(alter_table_statement(Dialect::MySQL), qstring);
            assert_eq!(
                res,
                AlterTableStatement {
                    table: Table::from("t"),
                    definitions: vec![AlterTableDefinition::ChangeColumn {
                        name: "f".into(),
                        spec: ColumnSpecification {
                            column: Column::from("modify"),
                            sql_type: SqlType::DateTime(None),
                            constraints: vec![],
                            comment: None,
                        }
                    }]
                }
            );
            assert_eq!(
                res.to_string(),
                "ALTER TABLE `t` CHANGE COLUMN `f` `modify` DATETIME"
            );
        }

        #[test]
        fn flarum_alter_2() {
            let qstring = b"alter table `posts_likes` add primary key `posts_likes_post_id_user_id_primary`(`post_id`, `user_id`)";
            let res = test_parse!(alter_table_statement(Dialect::MySQL), qstring);
            assert_eq!(
                res,
                AlterTableStatement {
                    table: Table::from("posts_likes"),
                    definitions: vec![AlterTableDefinition::AddKey(TableKey::PrimaryKey {
                        name: Some("posts_likes_post_id_user_id_primary".into()),
                        columns: vec![Column::from("post_id"), Column::from("user_id"),],
                    })]
                }
            );
        }

        #[test]
        fn flarum_alter_3() {
            let qstring = b"alter table `flags` add index `flags_created_at_index`(`created_at`)";
            let res = test_parse!(alter_table_statement(Dialect::MySQL), qstring);
            assert_eq!(
                res,
                AlterTableStatement {
                    table: Table::from("flags"),
                    definitions: vec![AlterTableDefinition::AddKey(TableKey::Key {
                        name: "flags_created_at_index".into(),
                        columns: vec![Column::from("created_at")],
                        index_type: None,
                    })]
                }
            );
        }

        #[test]
        fn flarum_alter_4() {
            let qstring = b"alter table `flags` add constraint `flags_post_id_foreign` foreign key (`post_id`) references `posts` (`id`) on delete cascade";
            let res = test_parse!(alter_table_statement(Dialect::MySQL), qstring);
            assert_eq!(
                res,
                AlterTableStatement {
                    table: Table::from("flags"),
                    definitions: vec![AlterTableDefinition::AddKey(TableKey::ForeignKey {
                        name: Some("flags_post_id_foreign".into()),
                        index_name: None,
                        columns: vec![Column::from("post_id")],
                        target_table: Table::from("posts"),
                        target_columns: vec![Column::from("id")],
                        on_delete: Some(ReferentialAction::Cascade),
                        on_update: None
                    })]
                }
            );
        }

        #[test]
        fn flarum_alter_5() {
            let qstring =
                b"alter table `discussion_user` add `subscription` enum('follow', 'ignore') null";
            let res = test_parse!(alter_table_statement(Dialect::MySQL), qstring);
            assert_eq!(
                res,
                AlterTableStatement {
                    table: Table::from("discussion_user"),
                    definitions: vec![AlterTableDefinition::AddColumn(ColumnSpecification {
                        column: Column::from("subscription"),
                        sql_type: SqlType::Enum(vec![
                            Literal::String("follow".into()),
                            Literal::String("ignore".into())
                        ]),
                        constraints: vec![ColumnConstraint::Null],
                        comment: None,
                    })],
                }
            );
            assert_eq!(
                res.to_string(),
                "ALTER TABLE `discussion_user` ADD COLUMN `subscription` ENUM('follow', 'ignore') NULL"
            );
        }
    }

    mod postgres {
        use super::*;
        use crate::{Column, SqlType};

        #[test]
        fn parse_add_column() {
            let qstring = "ALTER TABLE \"t\" ADD COLUMN \"c\" INT";
            let expected = AlterTableStatement {
                table: Table {
                    name: "t".into(),
                    schema: None,
                    alias: None,
                },
                definitions: vec![AlterTableDefinition::AddColumn(ColumnSpecification {
                    column: Column {
                        name: "c".into(),
                        table: None,
                    },
                    sql_type: SqlType::Int(None),
                    constraints: vec![],
                    comment: None,
                })],
            };
            let result = alter_table_statement(Dialect::PostgreSQL)(qstring.as_bytes());
            assert_eq!(result.unwrap().1, expected);
        }

        #[test]
        fn parse_add_two_columns() {
            let qstring = "ALTER TABLE \"t\" ADD COLUMN \"c\" INT, ADD COLUMN \"d\" TEXT";
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
                            table: None,
                        },
                        sql_type: SqlType::Int(None),
                        constraints: vec![],
                        comment: None,
                    }),
                    AlterTableDefinition::AddColumn(ColumnSpecification {
                        column: Column {
                            name: "d".into(),
                            table: None,
                        },
                        sql_type: SqlType::Text,
                        constraints: vec![],
                        comment: None,
                    }),
                ],
            };
            let result = alter_table_statement(Dialect::PostgreSQL)(qstring.as_bytes());
            assert_eq!(result.unwrap().1, expected);
        }

        #[test]
        fn parse_drop_column_no_behavior() {
            let qstring = "ALTER TABLE \"t\" DROP COLUMN c";
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
            let result = alter_table_statement(Dialect::PostgreSQL)(qstring.as_bytes());
            assert_eq!(result.unwrap().1, expected);
        }

        #[test]
        fn parse_drop_column_cascade() {
            let qstring = "ALTER TABLE \"t\" DROP COLUMN c CASCADE";
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
            let result = alter_table_statement(Dialect::PostgreSQL)(qstring.as_bytes());
            assert_eq!(result.unwrap().1, expected);
        }

        #[test]
        fn parse_alter_column_set_default() {
            let qstring = "ALTER TABLE \"t\" ALTER COLUMN c SET DEFAULT 'foo'";
            let expected = AlterTableStatement {
                table: Table {
                    name: "t".into(),
                    schema: None,
                    alias: None,
                },
                definitions: vec![AlterTableDefinition::AlterColumn {
                    name: "c".into(),
                    operation: AlterColumnOperation::SetColumnDefault(Literal::String(
                        "foo".into(),
                    )),
                }],
            };
            let result = alter_table_statement(Dialect::PostgreSQL)(qstring.as_bytes());
            assert_eq!(result.unwrap().1, expected);
        }

        #[test]
        fn parse_alter_column_drop_default() {
            let qstring = "ALTER TABLE \"t\" ALTER COLUMN c DROP DEFAULT";
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
            let result = alter_table_statement(Dialect::PostgreSQL)(qstring.as_bytes());
            assert_eq!(result.unwrap().1, expected);
        }
    }
}
