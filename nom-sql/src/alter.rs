/// ALTER TABLE Statement AST and parsing (incomplete)
///
/// See https://dev.mysql.com/doc/refman/8.0/en/alter-table.html
use std::{fmt, str};

use nom::character::complete::{multispace0, multispace1};
use nom::{
    alt, call, complete, do_parse, named, opt, preceded, separated_list, tag_no_case, terminated,
};
use serde::{Deserialize, Serialize};

use crate::column::{column_specification, ColumnSpecification};
use crate::common::{
    literal, schema_table_reference_no_alias, statement_terminator, ws_sep_comma, Literal, TableKey,
};
use crate::create::key_specification;
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
#[allow(clippy::enum_variant_names)]
pub enum AlterTableDefinition {
    AddColumn(ColumnSpecification),
    AddKey(TableKey),
    AlterColumn {
        name: String,
        operation: AlterColumnOperation,
    },
    DropColumn {
        name: String,
        behavior: Option<DropBehavior>,
    },
    ChangeColumn {
        name: String,
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

named_with_dialect!(
    add_column(dialect) -> AlterTableDefinition,
    do_parse!(
        tag_no_case!("add")
            >> opt!(preceded!(multispace1, tag_no_case!("column")))
            >> multispace1
            >> column: call!(column_specification(dialect))
            >> (AlterTableDefinition::AddColumn(column))
    )
);

named_with_dialect!(
    add_key(dialect) -> AlterTableDefinition,
    do_parse!(
        tag_no_case!("add")
            >> multispace1
            >> key: call!(key_specification(dialect))
            >> (AlterTableDefinition::AddKey(key))
    )
);

named!(
    drop_behavior<DropBehavior>,
    alt!(
        tag_no_case!("cascade") => { |_| DropBehavior::Cascade} |
        tag_no_case!("restrict") => { |_| DropBehavior::Restrict }
    )
);

named_with_dialect!(
    drop_column(dialect) -> AlterTableDefinition,
    do_parse!(
        tag_no_case!("drop")
            >> multispace1
            >> tag_no_case!("column")
            >> multispace1
            >> name: call!(dialect.identifier())
            >> behavior: opt!(preceded!(multispace1, drop_behavior))
            >> (AlterTableDefinition::DropColumn {
                name: name.to_string(),
                behavior,
            })
    )
);

named_with_dialect!(
    set_default(dialect) -> AlterColumnOperation,
    do_parse!(
        opt!(terminated!(tag_no_case!("set"), multispace1))
            >> tag_no_case!("default")
            >> multispace1
            >> value: call!(literal(dialect))
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

named_with_dialect!(
    alter_column_operation(dialect) -> AlterColumnOperation,
    alt!(call!(set_default(dialect)) | drop_default)
);

named_with_dialect!(
    alter_column(dialect) -> AlterTableDefinition,
    do_parse!(
        tag_no_case!("alter")
            >> multispace1
            >> tag_no_case!("column")
            >> multispace1
            >> name: call!(dialect.identifier())
            >> multispace1
            >> operation: call!(alter_column_operation(dialect))
            >> (AlterTableDefinition::AlterColumn {
                name: name.to_string(),
                operation
            })
    )
);

named_with_dialect!(
    change_column(dialect) -> AlterTableDefinition,
    do_parse!(
        tag_no_case!("change")
            >> opt!(preceded!(multispace1, tag_no_case!("column")))
            >> multispace1
            >> name: call!(dialect.identifier())
            >> multispace1
            >> spec: call!(column_specification(dialect))
            // TODO:  FIRST
            // TODO:  AFTER col_name
            >> (AlterTableDefinition::ChangeColumn{
                name: name.to_string(),
                spec
            })
    )
);

named_with_dialect!(
    modify_column(dialect) -> AlterTableDefinition,
    do_parse!(
        tag_no_case!("modify")
            >> opt!(preceded!(multispace1, tag_no_case!("column")))
            >> multispace1
            >> spec: call!(column_specification(dialect))
            // TODO:  FIRST
            // TODO:  AFTER col_name
            >> (AlterTableDefinition::ChangeColumn{
                name: spec.column.name.clone(),
                spec
            })
    )
);

named_with_dialect!(
    alter_table_definition(dialect) -> AlterTableDefinition,
    alt!(
        call!(add_column(dialect))
            | call!(add_key(dialect))
            | call!(drop_column(dialect))
            | call!(alter_column(dialect))
            | call!(change_column(dialect))
            | call!(modify_column(dialect))
    )
);

named_with_dialect!(
    pub alter_table_statement(dialect) -> AlterTableStatement,
    complete!(do_parse!(
        tag_no_case!("alter")
            >> multispace1
            >> tag_no_case!("table")
            >> multispace1
            >> table: call!(schema_table_reference_no_alias(dialect))
            >> multispace1
            >> definitions: separated_list!(ws_sep_comma, alter_table_definition(dialect))
            >> multispace0
            >> statement_terminator
            >> (AlterTableStatement { table, definitions })
    ))
);

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
                    function: None,
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
                        function: None,
                    },
                    sql_type: SqlType::Varchar(255),
                    constraints: vec![],
                    comment: None,
                }),
                AlterTableDefinition::AddColumn(ColumnSpecification {
                    column: Column {
                        name: "snailmail".into(),
                        table: None,
                        function: None,
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
        use crate::common::ReferentialAction;
        use crate::{Column, ColumnConstraint, SqlType};

        use super::*;

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
                        function: None,
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
                            function: None,
                        },
                        sql_type: SqlType::Int(None),
                        constraints: vec![],
                        comment: None,
                    }),
                    AlterTableDefinition::AddColumn(ColumnSpecification {
                        column: Column {
                            name: "d".into(),
                            table: None,
                            function: None,
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
                            sql_type: SqlType::Varchar(255),
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
                        function: None,
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
                            function: None,
                        },
                        sql_type: SqlType::Int(None),
                        constraints: vec![],
                        comment: None,
                    }),
                    AlterTableDefinition::AddColumn(ColumnSpecification {
                        column: Column {
                            name: "d".into(),
                            table: None,
                            function: None,
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
