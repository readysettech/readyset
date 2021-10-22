use nom::{
    bytes::complete::is_not,
    call,
    character::complete::{digit1, multispace0, multispace1},
    combinator::map_res,
};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str;
use std::str::FromStr;

use crate::column::{column_specification, Column, ColumnSpecification};
use crate::common::{
    column_identifier_no_alias, if_not_exists, schema_table_reference, statement_terminator,
    ws_sep_comma, IndexType, ReferentialAction, TableKey,
};
use crate::compound_select::{compound_selection, CompoundSelectStatement};
use crate::create_table_options::{table_options, CreateTableOption};
use crate::expression::expression;
use crate::order::{order_type, OrderType};
use crate::select::{nested_selection, SelectStatement};
use crate::table::Table;
use crate::{ColumnConstraint, Dialect};
use nom::branch::alt;
use nom::bytes::complete::{tag, tag_no_case};
use nom::combinator::{map, opt};
use nom::multi::{many0, many1};
use nom::sequence::{delimited, preceded, terminated, tuple};
use nom::{do_parse, opt, preceded, separated_list, tag, tag_no_case, terminated, IResult};

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct CreateTableStatement {
    pub table: Table,
    pub fields: Vec<ColumnSpecification>,
    pub keys: Option<Vec<TableKey>>,
    pub if_not_exists: bool,
    pub options: Vec<CreateTableOption>,
}

impl fmt::Display for CreateTableStatement {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CREATE TABLE `{}` ", self.table.name)?;
        write!(f, "(")?;
        write!(
            f,
            "{}",
            self.fields
                .iter()
                .map(|field| format!("{}", field))
                .collect::<Vec<_>>()
                .join(", ")
        )?;
        if let Some(ref keys) = self.keys {
            write!(
                f,
                ", {}",
                keys.iter()
                    .map(|key| format!("{}", key))
                    .collect::<Vec<_>>()
                    .join(", ")
            )?;
        }
        write!(f, ")")?;
        write!(
            f,
            "{}",
            self.options
                .iter()
                .map(|option| format!("{}", option))
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[allow(clippy::large_enum_variant)] // TODO: maybe this actually matters
pub enum SelectSpecification {
    Compound(CompoundSelectStatement),
    Simple(SelectStatement),
}

impl fmt::Display for SelectSpecification {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            SelectSpecification::Compound(ref csq) => write!(f, "{}", csq),
            SelectSpecification::Simple(ref sq) => write!(f, "{}", sq),
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct CreateViewStatement {
    pub name: String,
    pub fields: Vec<Column>,
    pub definition: Box<SelectSpecification>,
}

impl fmt::Display for CreateViewStatement {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CREATE VIEW `{}` ", self.name)?;
        if !self.fields.is_empty() {
            write!(f, "(")?;
            write!(
                f,
                "{}",
                self.fields
                    .iter()
                    .map(|field| format!("{}", field))
                    .collect::<Vec<_>>()
                    .join(", ")
            )?;
            write!(f, ") ")?;
        }
        write!(f, "AS ")?;
        write!(f, "{}", self.definition)
    }
}

// MySQL grammar element for index column definition (§13.1.18, index_col_name)
#[allow(clippy::type_complexity)]
pub fn index_col_name(
    dialect: Dialect,
) -> impl Fn(&[u8]) -> IResult<&[u8], (Column, Option<u16>, Option<OrderType>)> {
    move |i| {
        let (remaining_input, (column, len_u8, order)) = tuple((
            terminated(column_identifier_no_alias(dialect), multispace0),
            opt(delimited(
                tag("("),
                map_res(map_res(digit1, str::from_utf8), u16::from_str),
                tag(")"),
            )),
            opt(order_type),
        ))(i)?;

        Ok((remaining_input, (column, len_u8, order)))
    }
}

// Helper for list of index columns
pub fn index_col_list(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], Vec<Column>> {
    move |i| {
        many0(map(
            terminated(index_col_name(dialect), opt(ws_sep_comma)),
            // XXX(malte): ignores length and order
            |e| e.0,
        ))(i)
    }
}

// Parse rule for an individual key specification.
pub fn key_specification(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], TableKey> {
    move |i| {
        alt((
            check_constraint(dialect),
            full_text_key(dialect),
            primary_key(dialect),
            unique(dialect),
            key_or_index(dialect),
            foreign_key(dialect),
        ))(i)
    }
}

fn full_text_key(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], TableKey> {
    move |i| {
        let (remaining_input, (_, _, _, _, name, _, columns)) = tuple((
            tag_no_case("fulltext"),
            multispace1,
            alt((tag_no_case("key"), tag_no_case("index"))),
            multispace1,
            opt(dialect.identifier()),
            multispace0,
            delimited(
                tag("("),
                delimited(multispace0, index_col_list(dialect), multispace0),
                tag(")"),
            ),
        ))(i)?;

        match name {
            Some(name) => {
                let n = String::from(name);
                Ok((remaining_input, TableKey::FulltextKey(Some(n), columns)))
            }
            None => Ok((remaining_input, TableKey::FulltextKey(None, columns))),
        }
    }
}

fn primary_key(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], TableKey> {
    move |i| {
        let (remaining_input, (_, name, _, columns, _)) = tuple((
            tag_no_case("primary key"),
            opt(preceded(multispace1, dialect.identifier())),
            multispace0,
            delimited(
                tag("("),
                delimited(multispace0, index_col_list(dialect), multispace0),
                tag(")"),
            ),
            opt(map(
                preceded(multispace1, tag_no_case("auto_increment")),
                |_| (),
            )),
        ))(i)?;

        Ok((
            remaining_input,
            TableKey::PrimaryKey {
                name: name.map(|s| s.to_string()),
                columns,
            },
        ))
    }
}

fn referential_action(i: &[u8]) -> IResult<&[u8], ReferentialAction> {
    alt((
        map(tag_no_case("cascade"), |_| ReferentialAction::Cascade),
        map(
            tuple((tag_no_case("set"), multispace1, tag_no_case("null"))),
            |_| ReferentialAction::SetNull,
        ),
        map(tag_no_case("restrict"), |_| ReferentialAction::Restrict),
        map(
            tuple((tag_no_case("no"), multispace1, tag_no_case("action"))),
            |_| ReferentialAction::NoAction,
        ),
        map(
            tuple((tag_no_case("set"), multispace1, tag_no_case("default"))),
            |_| ReferentialAction::SetDefault,
        ),
    ))(i)
}

fn foreign_key(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], TableKey> {
    move |i| {
        do_parse!(
            i,
            opt!(preceded!(tag_no_case!("constraint"), multispace1))
                >> name: opt!(call!(dialect.identifier()))
                >> preceded!(opt!(multispace1), tag_no_case!("foreign"))
                >> multispace1
                >> tag_no_case!("key")
                >> index_name: opt!(preceded!(multispace1, call!(dialect.identifier())))
                >> multispace0
                >> tag!("(")
                >> columns:
                    separated_list!(
                        terminated!(tag!(","), multispace0),
                        call!(column_identifier_no_alias(dialect))
                    )
                >> tag!(")")
                >> multispace1
                >> tag_no_case!("references")
                >> multispace1
                >> target_table: call!(schema_table_reference(dialect))
                >> multispace0
                >> tag!("(")
                >> target_columns:
                    separated_list!(
                        terminated!(tag!(","), multispace0),
                        call!(column_identifier_no_alias(dialect))
                    )
                >> tag!(")")
                >> on_delete:
                    opt!(preceded!(
                        tuple((
                            multispace0,
                            tag_no_case("on"),
                            multispace1,
                            tag_no_case("delete"),
                            multispace1
                        )),
                        call!(referential_action)
                    ))
                >> on_update:
                    opt!(preceded!(
                        tuple((
                            multispace0,
                            tag_no_case("on"),
                            multispace1,
                            tag_no_case("update"),
                            multispace1
                        )),
                        call!(referential_action)
                    ))
                >> (TableKey::ForeignKey {
                    name: name.map(String::from),
                    index_name: index_name.map(String::from),
                    columns,
                    target_table,
                    target_columns,
                    on_delete,
                    on_update
                })
        )
    }
}

fn index_type(i: &[u8]) -> IResult<&[u8], IndexType> {
    alt((
        map(tag_no_case("btree"), |_| IndexType::BTree),
        map(tag_no_case("hash"), |_| IndexType::Hash),
    ))(i)
}

fn using_index(i: &[u8]) -> IResult<&[u8], IndexType> {
    let (i, _) = multispace1(i)?;
    let (i, _) = tag_no_case("using")(i)?;
    let (i, _) = multispace1(i)?;
    index_type(i)
}

fn unique(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], TableKey> {
    move |i| {
        let (i, _) = tag_no_case("unique")(i)?;
        let (i, _) = opt(preceded(
            multispace1,
            alt((tag_no_case("key"), tag_no_case("index"))),
        ))(i)?;
        let (i, _) = multispace0(i)?;
        let (i, name) = opt(dialect.identifier())(i)?;
        let (i, _) = multispace0(i)?;
        let (i, columns) = delimited(
            tag("("),
            delimited(multispace0, index_col_list(dialect), multispace0),
            tag(")"),
        )(i)?;
        let (i, index_type) = opt(using_index)(i)?;

        Ok((
            i,
            TableKey::UniqueKey {
                name: name.map(|n| n.into()),
                columns,
                index_type,
            },
        ))
    }
}

fn key_or_index(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], TableKey> {
    move |i| {
        let (i, _) = alt((tag_no_case("key"), tag_no_case("index")))(i)?;
        let (i, _) = multispace0(i)?;
        let (i, name) = map(dialect.identifier(), String::from)(i)?;
        let (i, _) = multispace0(i)?;
        let (i, columns) = delimited(
            tag("("),
            delimited(multispace0, index_col_list(dialect), multispace0),
            tag(")"),
        )(i)?;
        let (i, index_type) = opt(using_index)(i)?;

        Ok((
            i,
            TableKey::Key {
                name,
                columns,
                index_type,
            },
        ))
    }
}

fn check_constraint(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], TableKey> {
    move |i| {
        let (i, name) = map(
            opt(preceded(
                terminated(tag_no_case("constraint"), multispace1),
                opt(terminated(
                    map(dialect.identifier(), |c| c.to_string()),
                    multispace1,
                )),
            )),
            Option::flatten,
        )(i)?;
        let (i, _) = tag_no_case("check")(i)?;
        let (i, _) = multispace1(i)?;
        let (i, expr) = delimited(
            terminated(tag("("), multispace0),
            expression(dialect),
            preceded(multispace0, tag(")")),
        )(i)?;
        let (i, enforced) = opt(preceded(
            multispace1,
            terminated(
                map(opt(terminated(tag_no_case("not"), multispace1)), |n| {
                    n.is_none()
                }),
                tag_no_case("enforced"),
            ),
        ))(i)?;

        Ok((
            i,
            TableKey::CheckConstraint {
                name,
                expr,
                enforced,
            },
        ))
    }
}

// Parse rule for a comma-separated list.
pub fn key_specification_list(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], Vec<TableKey>> {
    move |i| many1(terminated(key_specification(dialect), opt(ws_sep_comma)))(i)
}

// Parse rule for a comma-separated list.
pub fn field_specification_list(
    dialect: Dialect,
) -> impl Fn(&[u8]) -> IResult<&[u8], Vec<ColumnSpecification>> {
    move |i| many1(terminated(column_specification(dialect), opt(ws_sep_comma)))(i)
}

// Parse rule for a column definition constraint.

// Parse rule for a SQL CREATE TABLE query.
// TODO(malte): support types, TEMPORARY tables, AS stmt
pub fn creation(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], CreateTableStatement> {
    move |i| {
        let (
            remaining_input,
            (_, _, _, _, if_not_exists, table, _, _, _, fields_list, _, keys_list, _, _, _, opt, _),
        ) = tuple((
            tag_no_case("create"),
            multispace1,
            tag_no_case("table"),
            multispace1,
            if_not_exists,
            schema_table_reference(dialect),
            multispace0,
            tag("("),
            multispace0,
            field_specification_list(dialect),
            multispace0,
            opt(key_specification_list(dialect)),
            multispace0,
            tag(")"),
            multispace0,
            table_options(dialect),
            statement_terminator,
        ))(i)?;

        // "table AS alias" isn't legal in CREATE statements
        assert!(table.alias.is_none());

        let mut primary_key = None;

        // attach table names to columns:
        let fields = fields_list
            .into_iter()
            .map(|field| {
                let column = Column {
                    table: Some(table.name.clone()),
                    ..field.column
                };

                if field.constraints.contains(&ColumnConstraint::PrimaryKey) {
                    // If there is a row that was defined with the PRIMARY KEY constraint, then it will be the primary key
                    // there can only be one such key, but we don't check this is the case
                    primary_key.replace(TableKey::PrimaryKey {
                        name: None,
                        columns: vec![column.clone()],
                    });
                }

                ColumnSpecification { column, ..field }
            })
            .collect();

        // and to keys:
        let mut keys: Option<Vec<_>> = keys_list.map(|ks| {
            ks.into_iter()
                .map(|key| {
                    let attach_names = |columns: Vec<Column>| {
                        columns
                            .into_iter()
                            .map(|column| Column {
                                table: Some(table.name.clone()),
                                ..column
                            })
                            .collect()
                    };

                    match key {
                        TableKey::PrimaryKey { name, columns } => TableKey::PrimaryKey {
                            name,
                            columns: attach_names(columns),
                        },
                        TableKey::UniqueKey {
                            name,
                            columns,
                            index_type,
                        } => TableKey::UniqueKey {
                            name,
                            columns: attach_names(columns),
                            index_type,
                        },
                        TableKey::FulltextKey(name, columns) => {
                            TableKey::FulltextKey(name, attach_names(columns))
                        }
                        TableKey::Key {
                            name,
                            columns,
                            index_type,
                        } => TableKey::Key {
                            name,
                            columns: attach_names(columns),
                            index_type,
                        },
                        TableKey::ForeignKey {
                            name,
                            columns: column,
                            target_table,
                            target_columns: target_column,
                            index_name,
                            on_delete,
                            on_update,
                        } => TableKey::ForeignKey {
                            name,
                            columns: attach_names(column),
                            target_table,
                            target_columns: target_column,
                            index_name,
                            on_delete,
                            on_update,
                        },
                        constraint => constraint,
                    }
                })
                .collect()
        });

        // Add the previously found key to the list
        if let Some(primary_key) = primary_key {
            if let Some(keys) = &mut keys {
                keys.push(primary_key);
            } else {
                keys = Some(vec![primary_key]);
            }
        }
        // TODO: check that there is only one PRIMARY KEY?

        Ok((
            remaining_input,
            CreateTableStatement {
                table,
                fields,
                keys,
                if_not_exists,
                options: opt,
            },
        ))
    }
}

// Parse the optional CREATE VIEW parameters and discard, ideally we would want to check user permissions
pub fn create_view_params(i: &[u8]) -> IResult<&[u8], ()> {
    /*
    [ALGORITHM = {UNDEFINED | MERGE | TEMPTABLE}]
    [DEFINER = user]
    [SQL SECURITY { DEFINER | INVOKER }]

    If the DEFINER clause is present, the user value should be a MySQL account specified
    as 'user_name'@'host_name', CURRENT_USER, or CURRENT_USER()
     */
    map(
        tuple((
            opt(tuple((
                tag_no_case("ALGORITHM"),
                multispace0,
                tag("="),
                multispace0,
                alt((
                    tag_no_case("UNDEFINED"),
                    tag_no_case("MERGE"),
                    tag_no_case("TEMPTABLE"),
                )),
                multispace1,
            ))),
            opt(tuple((
                tag_no_case("DEFINER"),
                multispace0,
                tag("="),
                multispace0,
                delimited(tag("`"), is_not("`"), tag("`")),
                tag("@"),
                delimited(tag("`"), is_not("`"), tag("`")),
                multispace1,
            ))),
            opt(tuple((
                tag_no_case("SQL"),
                multispace1,
                tag_no_case("SECURITY"),
                multispace1,
                alt((tag_no_case("DEFINER"), tag_no_case("INVOKER"))),
                multispace1,
            ))),
        )),
        |_| (),
    )(i)
}

// Parse rule for a SQL CREATE VIEW query.
pub fn view_creation(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], CreateViewStatement> {
    /*
       CREATE
       [OR REPLACE]
       [ALGORITHM = {UNDEFINED | MERGE | TEMPTABLE}]
       [DEFINER = user]
       [SQL SECURITY { DEFINER | INVOKER }]
       VIEW view_name [(column_list)]
       AS select_statement
       [WITH [CASCADED | LOCAL] CHECK OPTION]
    */
    // Sample query:
    // CREATE ALGORITHM=UNDEFINED DEFINER=`mysqluser`@`%` SQL SECURITY DEFINER VIEW `myquery2` AS SELECT * FROM employees

    move |i| {
        let (remaining_input, (_, _, _, _, _, name_slice, _, _, _, def, _)) = tuple((
            tag_no_case("create"),
            multispace1,
            opt(create_view_params),
            tag_no_case("view"),
            multispace1,
            dialect.identifier(),
            multispace1,
            tag_no_case("as"),
            multispace1,
            alt((
                map(compound_selection(dialect), SelectSpecification::Compound),
                map(nested_selection(dialect), SelectSpecification::Simple),
            )),
            statement_terminator,
        ))(i)?;

        let name = String::from(name_slice);
        let fields = vec![]; // TODO(malte): support
        let definition = Box::new(def);

        Ok((
            remaining_input,
            CreateViewStatement {
                name,
                fields,
                definition,
            },
        ))
    }
}

#[cfg(test)]
mod tests {
    use crate::{common::type_identifier, ColumnConstraint, Literal, SqlType};
    use crate::{BinaryOperator, Expression};

    use super::*;
    use crate::column::Column;
    use crate::table::Table;

    #[test]
    fn sql_types() {
        let type0 = "bigint(20)";
        let type1 = "varchar(255) binary";
        let type2 = "bigint(20) unsigned";
        let type3 = "bigint(20) signed";

        let res = type_identifier(Dialect::MySQL)(type0.as_bytes());
        assert_eq!(res.unwrap().1, SqlType::Bigint(Some(20)));
        let res = type_identifier(Dialect::MySQL)(type1.as_bytes());
        assert_eq!(res.unwrap().1, SqlType::Varchar(255));
        let res = type_identifier(Dialect::MySQL)(type2.as_bytes());
        assert_eq!(res.unwrap().1, SqlType::UnsignedBigint(Some(20)));
        let res = type_identifier(Dialect::MySQL)(type3.as_bytes());
        assert_eq!(res.unwrap().1, SqlType::Bigint(Some(20)));
        let res = type_identifier(Dialect::MySQL)(type2.as_bytes());
        assert_eq!(res.unwrap().1, SqlType::UnsignedBigint(Some(20)));
    }

    #[test]
    fn field_spec() {
        // N.B. trailing comma here because field_specification_list! doesn't handle the eof case
        // because it is never validly the end of a query
        let qstring = "id bigint(20), name varchar(255),";

        let res = field_specification_list(Dialect::MySQL)(qstring.as_bytes());
        assert_eq!(
            res.unwrap().1,
            vec![
                ColumnSpecification::new(Column::from("id"), SqlType::Bigint(Some(20))),
                ColumnSpecification::new(Column::from("name"), SqlType::Varchar(255)),
            ]
        );
    }

    #[test]
    fn simple_create() {
        let qstring = "CREATE TABLE if Not  ExistS users (id bigint(20), name varchar(255), email varchar(255));";

        let res = creation(Dialect::MySQL)(qstring.as_bytes());
        assert_eq!(
            res.unwrap().1,
            CreateTableStatement {
                table: Table::from("users"),
                fields: vec![
                    ColumnSpecification::new(Column::from("users.id"), SqlType::Bigint(Some(20))),
                    ColumnSpecification::new(Column::from("users.name"), SqlType::Varchar(255)),
                    ColumnSpecification::new(Column::from("users.email"), SqlType::Varchar(255)),
                ],
                if_not_exists: true,
                ..Default::default()
            }
        );
    }

    #[test]
    fn create_without_space_after_tablename() {
        let qstring = "CREATE TABLE t(x integer);";
        let res = creation(Dialect::MySQL)(qstring.as_bytes());
        assert_eq!(
            res.unwrap().1,
            CreateTableStatement {
                table: Table::from("t"),
                fields: vec![ColumnSpecification::new(
                    Column::from("t.x"),
                    SqlType::Int(None)
                ),],
                ..Default::default()
            }
        );
    }

    #[test]
    fn create_tablename_with_schema() {
        let qstring = "CREATE TABLE db1.t(x integer);";
        let res = creation(Dialect::MySQL)(qstring.as_bytes());
        assert_eq!(
            res.unwrap().1,
            CreateTableStatement {
                table: Table::from(("db1", "t")),
                fields: vec![ColumnSpecification::new(
                    Column::from("t.x"),
                    SqlType::Int(None)
                ),],
                ..Default::default()
            }
        );
    }

    #[test]
    fn keys() {
        // simple primary key
        let qstring = "CREATE TABLE users (id bigint(20), name varchar(255), email varchar(255), \
                       PRIMARY KEY (id));";

        let res = creation(Dialect::MySQL)(qstring.as_bytes());
        assert_eq!(
            res.unwrap().1,
            CreateTableStatement {
                table: Table::from("users"),
                fields: vec![
                    ColumnSpecification::new(Column::from("users.id"), SqlType::Bigint(Some(20))),
                    ColumnSpecification::new(Column::from("users.name"), SqlType::Varchar(255)),
                    ColumnSpecification::new(Column::from("users.email"), SqlType::Varchar(255)),
                ],
                keys: Some(vec![TableKey::PrimaryKey {
                    name: None,
                    columns: vec![Column::from("users.id")]
                }]),
                ..Default::default()
            }
        );

        // named unique key
        let qstring = "CREATE TABLE users (id bigint(20), name varchar(255), email varchar(255), \
                       UNIQUE KEY id_k (id));";

        let res = creation(Dialect::MySQL)(qstring.as_bytes());
        assert_eq!(
            res.unwrap().1,
            CreateTableStatement {
                table: Table::from("users"),
                fields: vec![
                    ColumnSpecification::new(Column::from("users.id"), SqlType::Bigint(Some(20))),
                    ColumnSpecification::new(Column::from("users.name"), SqlType::Varchar(255)),
                    ColumnSpecification::new(Column::from("users.email"), SqlType::Varchar(255)),
                ],
                keys: Some(vec![TableKey::UniqueKey {
                    name: Some(String::from("id_k")),
                    columns: vec![Column::from("users.id")],
                    index_type: None
                },]),
                ..Default::default()
            }
        );
    }

    #[test]
    fn compound_create_view() {
        use crate::common::FieldDefinitionExpression;
        use crate::compound_select::{CompoundSelectOperator, CompoundSelectStatement};

        let qstring = "CREATE VIEW v AS SELECT * FROM users UNION SELECT * FROM old_users;";

        let res = view_creation(Dialect::MySQL)(qstring.as_bytes());
        assert_eq!(
            res.unwrap().1,
            CreateViewStatement {
                name: String::from("v"),
                fields: vec![],
                definition: Box::new(SelectSpecification::Compound(CompoundSelectStatement {
                    selects: vec![
                        (
                            None,
                            SelectStatement {
                                tables: vec![Table::from("users")],
                                fields: vec![FieldDefinitionExpression::All],
                                ..Default::default()
                            },
                        ),
                        (
                            Some(CompoundSelectOperator::DistinctUnion),
                            SelectStatement {
                                tables: vec![Table::from("old_users")],
                                fields: vec![FieldDefinitionExpression::All],
                                ..Default::default()
                            },
                        ),
                    ],
                    order: None,
                    limit: None,
                })),
            }
        );
    }

    #[test]
    fn foreign_key() {
        let qstring = b"CREATE TABLE users (
          id int,
          group_id int,
          primary key (id),
          constraint users_group foreign key (group_id) references `groups` (id),
        ) AUTO_INCREMENT=1000";

        let (rem, res) = creation(Dialect::MySQL)(qstring).unwrap();
        assert!(rem.is_empty());
        let col = |n: &str| Column {
            name: n.into(),
            table: Some("users".into()),
            function: None,
        };
        assert_eq!(
            res,
            CreateTableStatement {
                table: "users".into(),
                fields: vec![
                    ColumnSpecification::new(col("id"), SqlType::Int(None),),
                    ColumnSpecification::new(col("group_id"), SqlType::Int(None),),
                ],
                keys: Some(vec![
                    TableKey::PrimaryKey {
                        name: None,
                        columns: vec![col("id")],
                    },
                    TableKey::ForeignKey {
                        name: Some("users_group".into()),
                        columns: vec![col("group_id")],
                        target_table: "groups".into(),
                        target_columns: vec!["id".into()],
                        index_name: None,
                        on_delete: None,
                        on_update: None,
                    }
                ]),
                if_not_exists: false,
                options: vec![CreateTableOption::AutoIncrement(1000)],
            }
        )
    }

    /// Tests that CONSTRAINT is not required for FOREIGN KEY
    #[test]
    fn foreign_key_no_constraint_keyword() {
        // Test query borrowed from debezeum MySQL docker example
        let qstring = b"CREATE TABLE addresses (
                        id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
                        customer_id INTEGER NOT NULL,
                        street VARCHAR(255) NOT NULL,
                        city VARCHAR(255) NOT NULL,
                        state VARCHAR(255) NOT NULL,
                        zip VARCHAR(255) NOT NULL,
                        type enum(\'SHIPPING\',\'BILLING\',\'LIVING\') NOT NULL,
                        FOREIGN KEY (customer_id) REFERENCES customers(id) )
                        AUTO_INCREMENT = 10";

        let (rem, res) = creation(Dialect::MySQL)(qstring).unwrap();
        assert!(rem.is_empty());
        let col = |n: &str| Column {
            name: n.into(),
            table: Some("addresses".into()),
            function: None,
        };
        let non_null_col = |n: &str, t: SqlType| {
            ColumnSpecification::with_constraints(col(n), t, vec![ColumnConstraint::NotNull])
        };

        assert_eq!(
            res,
            CreateTableStatement {
                table: "addresses".into(),
                fields: vec![
                    ColumnSpecification::with_constraints(
                        col("id"),
                        SqlType::Int(None),
                        vec![
                            ColumnConstraint::NotNull,
                            ColumnConstraint::AutoIncrement,
                            ColumnConstraint::PrimaryKey,
                        ]
                    ),
                    non_null_col("customer_id", SqlType::Int(None)),
                    non_null_col("street", SqlType::Varchar(255)),
                    non_null_col("city", SqlType::Varchar(255)),
                    non_null_col("state", SqlType::Varchar(255)),
                    non_null_col("zip", SqlType::Varchar(255)),
                    non_null_col(
                        "type",
                        SqlType::Enum(vec![
                            Literal::String("SHIPPING".into()),
                            Literal::String("BILLING".into()),
                            Literal::String("LIVING".into()),
                        ]),
                    ),
                ],
                keys: Some(vec![
                    TableKey::ForeignKey {
                        name: None,
                        columns: vec![col("customer_id")],
                        target_table: "customers".into(),
                        target_columns: vec!["id".into()],
                        index_name: None,
                        on_delete: None,
                        on_update: None,
                    },
                    TableKey::PrimaryKey {
                        name: None,
                        columns: vec![col("id")]
                    },
                ]),
                if_not_exists: false,
                options: vec![CreateTableOption::AutoIncrement(10)],
            }
        )
    }

    /// Tests that index_name is parsed properly for FOREIGN KEY
    #[test]
    fn foreign_key_with_index() {
        let qstring = b"CREATE TABLE orders (
                        order_number INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
                        purchaser INTEGER NOT NULL,
                        product_id INTEGER NOT NULL,
                        FOREIGN KEY order_customer (purchaser) REFERENCES customers(id),
                        FOREIGN KEY ordered_product (product_id) REFERENCES products(id) )";

        let (rem, res) = creation(Dialect::MySQL)(qstring).unwrap();
        assert!(rem.is_empty());
        let col = |n: &str| Column {
            name: n.into(),
            table: Some("orders".into()),
            function: None,
        };

        assert_eq!(
            res,
            CreateTableStatement {
                table: "orders".into(),
                fields: vec![
                    ColumnSpecification::with_constraints(
                        col("order_number"),
                        SqlType::Int(None),
                        vec![
                            ColumnConstraint::NotNull,
                            ColumnConstraint::AutoIncrement,
                            ColumnConstraint::PrimaryKey,
                        ]
                    ),
                    ColumnSpecification::with_constraints(
                        col("purchaser"),
                        SqlType::Int(None),
                        vec![ColumnConstraint::NotNull]
                    ),
                    ColumnSpecification::with_constraints(
                        col("product_id"),
                        SqlType::Int(None),
                        vec![ColumnConstraint::NotNull]
                    ),
                ],
                keys: Some(vec![
                    TableKey::ForeignKey {
                        name: None,
                        columns: vec![col("purchaser")],
                        target_table: "customers".into(),
                        target_columns: vec!["id".into()],
                        index_name: Some("order_customer".into()),
                        on_delete: None,
                        on_update: None,
                    },
                    TableKey::ForeignKey {
                        name: None,
                        columns: vec![col("product_id")],
                        target_table: "products".into(),
                        target_columns: vec!["id".into()],
                        index_name: Some("ordered_product".into()),
                        on_delete: None,
                        on_update: None,
                    },
                    TableKey::PrimaryKey {
                        name: None,
                        columns: vec![col("order_number")]
                    },
                ]),
                if_not_exists: false,
                options: vec![],
            }
        )
    }

    /// Tests that UNIQUE KEY column constraint is parsed properly
    #[test]
    fn test_unique_key() {
        let qstring = b"CREATE TABLE customers (
                        id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
                        last_name VARCHAR(255) NOT NULL UNIQUE,
                        email VARCHAR(255) NOT NULL UNIQUE KEY )
                        AUTO_INCREMENT=1001";

        let (rem, res) = creation(Dialect::MySQL)(qstring).unwrap();
        assert!(rem.is_empty());
        let col = |n: &str| Column {
            name: n.into(),
            table: Some("customers".into()),
            function: None,
        };

        assert_eq!(
            res,
            CreateTableStatement {
                table: "customers".into(),
                fields: vec![
                    ColumnSpecification::with_constraints(
                        col("id"),
                        SqlType::Int(None),
                        vec![
                            ColumnConstraint::NotNull,
                            ColumnConstraint::AutoIncrement,
                            ColumnConstraint::PrimaryKey,
                        ]
                    ),
                    ColumnSpecification::with_constraints(
                        col("last_name"),
                        SqlType::Varchar(255),
                        vec![ColumnConstraint::NotNull, ColumnConstraint::Unique,]
                    ),
                    ColumnSpecification::with_constraints(
                        col("email"),
                        SqlType::Varchar(255),
                        vec![ColumnConstraint::NotNull, ColumnConstraint::Unique,]
                    ),
                ],
                keys: Some(vec![TableKey::PrimaryKey {
                    name: None,
                    columns: vec![col("id")]
                },]),
                if_not_exists: false,
                options: vec![CreateTableOption::AutoIncrement(1001)],
            }
        )
    }

    #[test]
    fn key_with_index_type() {
        let res = test_parse!(
            creation(Dialect::MySQL),
            b"CREATE TABLE users (
                  age INTEGER,
                  KEY age_key (age) USING BTREE
              )"
        );
        assert_eq!(
            res.keys,
            Some(vec![TableKey::Key {
                name: "age_key".into(),
                columns: vec!["users.age".into()],
                index_type: Some(IndexType::BTree),
            }])
        );
    }

    #[test]
    fn check_constraint_no_name() {
        let qs: &[&[u8]] = &[b"CHECK (x > 1)", b"CONSTRAINT CHECK (x > 1)"];
        for q in qs {
            let res = test_parse!(key_specification(Dialect::MySQL), q);
            assert_eq!(
                res,
                TableKey::CheckConstraint {
                    name: None,
                    expr: Expression::BinaryOp {
                        lhs: Box::new(Expression::Column("x".into())),
                        op: BinaryOperator::Greater,
                        rhs: Box::new(Expression::Literal(1.into())),
                    },
                    enforced: None
                }
            )
        }
    }

    #[test]
    fn check_constraint_with_name() {
        let qstr = b"CONSTRAINT foo CHECK (x > 1)";
        let res = test_parse!(key_specification(Dialect::MySQL), qstr);
        assert_eq!(
            res,
            TableKey::CheckConstraint {
                name: Some("foo".to_owned()),
                expr: Expression::BinaryOp {
                    lhs: Box::new(Expression::Column("x".into())),
                    op: BinaryOperator::Greater,
                    rhs: Box::new(Expression::Literal(1.into())),
                },
                enforced: None
            }
        )
    }

    #[test]
    fn check_constraint_not_enforced() {
        let qstr = b"CONSTRAINT foo CHECK (x > 1) NOT ENFORCED";
        let res = test_parse!(key_specification(Dialect::MySQL), qstr);
        assert_eq!(
            res,
            TableKey::CheckConstraint {
                name: Some("foo".to_owned()),
                expr: Expression::BinaryOp {
                    lhs: Box::new(Expression::Column("x".into())),
                    op: BinaryOperator::Greater,
                    rhs: Box::new(Expression::Literal(1.into())),
                },
                enforced: Some(false)
            }
        )
    }

    mod mysql {
        use crate::{ColumnConstraint, Literal, SqlType};

        use super::*;
        use crate::column::Column;
        use crate::table::Table;

        #[test]
        fn create_view_with_security_params() {
            let qstring = "CREATE ALGORITHM=UNDEFINED DEFINER=`mysqluser`@`%` SQL SECURITY DEFINER VIEW `myquery2` AS SELECT * FROM employees";
            view_creation(Dialect::MySQL)(qstring.as_bytes()).unwrap();
        }

        #[test]
        fn double_precision_column() {
            let (rem, res) =
                creation(Dialect::MySQL)(b"create table t(x double precision)").unwrap();
            assert_eq!(str::from_utf8(rem).unwrap(), "");
            assert_eq!(
                res,
                CreateTableStatement {
                    table: "t".into(),
                    fields: vec![ColumnSpecification {
                        column: "t.x".into(),
                        sql_type: SqlType::Double,
                        constraints: vec![],
                        comment: None,
                    }],
                    keys: None,
                    if_not_exists: false,
                    options: vec![],
                }
            );
        }

        #[test]
        fn django_create() {
            let qstring = "CREATE TABLE `django_admin_log` (
                       `id` integer AUTO_INCREMENT NOT NULL PRIMARY KEY,
                       `action_time` datetime NOT NULL,
                       `user_id` integer NOT NULL,
                       `content_type_id` integer,
                       `object_id` longtext,
                       `object_repr` varchar(200) NOT NULL,
                       `action_flag` smallint UNSIGNED NOT NULL,
                       `change_message` longtext NOT NULL);";
            let res = creation(Dialect::MySQL)(qstring.as_bytes());
            assert_eq!(
                res.unwrap().1,
                CreateTableStatement {
                    table: Table::from("django_admin_log"),
                    fields: vec![
                        ColumnSpecification::with_constraints(
                            Column::from("django_admin_log.id"),
                            SqlType::Int(None),
                            vec![
                                ColumnConstraint::AutoIncrement,
                                ColumnConstraint::NotNull,
                                ColumnConstraint::PrimaryKey,
                            ],
                        ),
                        ColumnSpecification::with_constraints(
                            Column::from("django_admin_log.action_time"),
                            SqlType::DateTime(None),
                            vec![ColumnConstraint::NotNull],
                        ),
                        ColumnSpecification::with_constraints(
                            Column::from("django_admin_log.user_id"),
                            SqlType::Int(None),
                            vec![ColumnConstraint::NotNull],
                        ),
                        ColumnSpecification::new(
                            Column::from("django_admin_log.content_type_id"),
                            SqlType::Int(None),
                        ),
                        ColumnSpecification::new(
                            Column::from("django_admin_log.object_id"),
                            SqlType::Longtext,
                        ),
                        ColumnSpecification::with_constraints(
                            Column::from("django_admin_log.object_repr"),
                            SqlType::Varchar(200),
                            vec![ColumnConstraint::NotNull],
                        ),
                        ColumnSpecification::with_constraints(
                            Column::from("django_admin_log.action_flag"),
                            SqlType::UnsignedSmallint(None),
                            vec![ColumnConstraint::NotNull],
                        ),
                        ColumnSpecification::with_constraints(
                            Column::from("django_admin_log.change_message"),
                            SqlType::Longtext,
                            vec![ColumnConstraint::NotNull],
                        ),
                    ],
                    keys: Some(vec![TableKey::PrimaryKey {
                        name: None,
                        columns: vec![Column {
                            name: "id".into(),
                            table: Some("django_admin_log".into()),
                            function: None,
                        }]
                    }]),
                    if_not_exists: false,
                    options: vec![],
                }
            );

            let qstring = "CREATE TABLE `auth_group` (
                       `id` integer AUTO_INCREMENT NOT NULL PRIMARY KEY,
                       `name` varchar(80) NOT NULL UNIQUE)";
            let res = creation(Dialect::MySQL)(qstring.as_bytes());
            assert_eq!(
                res.unwrap().1,
                CreateTableStatement {
                    table: Table::from("auth_group"),
                    fields: vec![
                        ColumnSpecification::with_constraints(
                            Column::from("auth_group.id"),
                            SqlType::Int(None),
                            vec![
                                ColumnConstraint::AutoIncrement,
                                ColumnConstraint::NotNull,
                                ColumnConstraint::PrimaryKey,
                            ],
                        ),
                        ColumnSpecification::with_constraints(
                            Column::from("auth_group.name"),
                            SqlType::Varchar(80),
                            vec![ColumnConstraint::NotNull, ColumnConstraint::Unique],
                        ),
                    ],
                    keys: Some(vec![TableKey::PrimaryKey {
                        name: None,
                        columns: vec![Column {
                            name: "id".into(),
                            table: Some("auth_group".into()),
                            function: None,
                        }]
                    }]),
                    if_not_exists: false,
                    options: vec![],
                }
            );
        }

        #[test]
        fn format_create() {
            let qstring = "CREATE TABLE `auth_group` (
                       `id` integer AUTO_INCREMENT NOT NULL PRIMARY KEY,
                       `name` varchar(80) NOT NULL UNIQUE) ENGINE=InnoDB AUTO_INCREMENT=495209 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci";
            // TODO(malte): INTEGER isn't quite reflected right here, perhaps
            let expected = "CREATE TABLE `auth_group` (\
                        `id` INT AUTO_INCREMENT NOT NULL, \
                        `name` VARCHAR(80) NOT NULL UNIQUE, PRIMARY KEY (`id`))\
                        ENGINE=InnoDB, AUTO_INCREMENT=495209, DEFAULT CHARSET=utf8mb4, COLLATE=utf8mb4_unicode_ci";
            let res = creation(Dialect::MySQL)(qstring.as_bytes());
            assert_eq!(format!("{}", res.unwrap().1), expected);
        }

        #[test]
        fn simple_create_view() {
            use crate::common::FieldDefinitionExpression;
            use crate::{BinaryOperator, Expression};

            let qstring = "CREATE VIEW v AS SELECT * FROM users WHERE username = \"bob\";";

            let res = view_creation(Dialect::MySQL)(qstring.as_bytes());
            assert_eq!(
                res.unwrap().1,
                CreateViewStatement {
                    name: String::from("v"),
                    fields: vec![],
                    definition: Box::new(SelectSpecification::Simple(SelectStatement {
                        tables: vec![Table::from("users")],
                        fields: vec![FieldDefinitionExpression::All],
                        where_clause: Some(Expression::BinaryOp {
                            lhs: Box::new(Expression::Column("username".into())),
                            rhs: Box::new(Expression::Literal(Literal::String("bob".into()))),
                            op: BinaryOperator::Equal,
                        }),
                        ..Default::default()
                    })),
                }
            );
        }

        #[test]
        fn format_create_view() {
            let qstring = "CREATE VIEW `v` AS SELECT * FROM `t`;";
            let expected = "CREATE VIEW `v` AS SELECT * FROM `t`";
            let res = view_creation(Dialect::MySQL)(qstring.as_bytes());
            assert_eq!(format!("{}", res.unwrap().1), expected);
        }

        #[test]
        fn lobsters_indexes() {
            let qstring = "CREATE TABLE `comments` (
            `id` int unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY,
            `hat_id` int,
            fulltext INDEX `index_comments_on_comment`  (`comment`),
            INDEX `confidence_idx`  (`confidence`),
            UNIQUE INDEX `short_id`  (`short_id`),
            INDEX `story_id_short_id`  (`story_id`, `short_id`),
            INDEX `thread_id`  (`thread_id`),
            INDEX `index_comments_on_user_id`  (`user_id`))
            ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;";
            let res = creation(Dialect::MySQL)(qstring.as_bytes());
            assert_eq!(
                res.unwrap().1,
                CreateTableStatement {
                    table: Table::from("comments"),
                    fields: vec![
                        ColumnSpecification::with_constraints(
                            Column::from("comments.id"),
                            SqlType::UnsignedInt(None),
                            vec![
                                ColumnConstraint::NotNull,
                                ColumnConstraint::AutoIncrement,
                                ColumnConstraint::PrimaryKey,
                            ],
                        ),
                        ColumnSpecification::new(
                            Column::from("comments.hat_id"),
                            SqlType::Int(None),
                        ),
                    ],
                    keys: Some(vec![
                        TableKey::FulltextKey(
                            Some("index_comments_on_comment".into()),
                            vec![Column::from("comments.comment")]
                        ),
                        TableKey::Key {
                            name: "confidence_idx".into(),
                            columns: vec![Column::from("comments.confidence")],
                            index_type: None
                        },
                        TableKey::UniqueKey {
                            name: Some("short_id".into()),
                            columns: vec![Column::from("comments.short_id")],
                            index_type: None
                        },
                        TableKey::Key {
                            name: "story_id_short_id".into(),
                            columns: vec![
                                Column::from("comments.story_id"),
                                Column::from("comments.short_id")
                            ],
                            index_type: None
                        },
                        TableKey::Key {
                            name: "thread_id".into(),
                            columns: vec![Column::from("comments.thread_id")],
                            index_type: None,
                        },
                        TableKey::Key {
                            name: "index_comments_on_user_id".into(),
                            columns: vec![Column::from("comments.user_id")],
                            index_type: None
                        },
                        TableKey::PrimaryKey {
                            name: None,
                            columns: vec![Column {
                                name: "id".into(),
                                table: Some("comments".into()),
                                function: None,
                            }]
                        },
                    ]),
                    if_not_exists: false,
                    options: vec![
                        CreateTableOption::Engine(Some("InnoDB".to_string())),
                        CreateTableOption::Charset("utf8mb4".to_string())
                    ],
                }
            );
        }

        #[test]
        fn mediawiki_create() {
            let qstring =
                "CREATE TABLE user_newtalk (  user_id int(5) NOT NULL default '0',  user_ip \
                       varchar(40) NOT NULL default '') TYPE=MyISAM;";
            let res = creation(Dialect::MySQL)(qstring.as_bytes());
            assert_eq!(
                res.unwrap().1,
                CreateTableStatement {
                    table: Table::from("user_newtalk"),
                    fields: vec![
                        ColumnSpecification::with_constraints(
                            Column::from("user_newtalk.user_id"),
                            SqlType::Int(Some(5)),
                            vec![
                                ColumnConstraint::NotNull,
                                ColumnConstraint::DefaultValue(Literal::String(String::from("0"))),
                            ],
                        ),
                        ColumnSpecification::with_constraints(
                            Column::from("user_newtalk.user_ip"),
                            SqlType::Varchar(40),
                            vec![
                                ColumnConstraint::NotNull,
                                ColumnConstraint::DefaultValue(Literal::String(String::from(""))),
                            ],
                        ),
                    ],
                    options: vec![CreateTableOption::Other],
                    ..Default::default()
                }
            );
        }

        #[test]
        fn mediawiki_create2() {
            let qstring = "CREATE TABLE `user` (
                        user_id int unsigned NOT NULL PRIMARY KEY AUTO_INCREMENT,
                        user_name varchar(255) binary NOT NULL default '',
                        user_real_name character varying(255) binary NOT NULL default '',
                        user_password tinyblob NOT NULL,
                        user_newpassword tinyblob NOT NULL,
                        user_newpass_time binary(14),
                        user_email tinytext NOT NULL,
                        user_touched binary(14) NOT NULL default '',
                        user_token binary(32) NOT NULL default '',
                        user_email_authenticated binary(14),
                        user_email_token binary(32),
                        user_email_token_expires binary(14),
                        user_registration binary(14),
                        user_editcount int,
                        user_password_expires varbinary(14) DEFAULT NULL
                       ) ENGINE=, DEFAULT CHARSET=utf8";
            if let Err(nom::Err::Error((s, _))) = creation(Dialect::MySQL)(qstring.as_bytes()) {
                panic!("{}", std::str::from_utf8(s).unwrap());
            }
        }

        #[test]
        fn mediawiki_create3() {
            let qstring = "CREATE TABLE `interwiki` (
 iw_prefix varchar(32) NOT NULL,
 iw_url blob NOT NULL,
 iw_api blob NOT NULL,
 iw_wikiid varchar(64) NOT NULL,
 iw_local bool NOT NULL,
 iw_trans tinyint NOT NULL default 0
 ) ENGINE=, DEFAULT CHARSET=utf8";
            creation(Dialect::MySQL)(qstring.as_bytes()).unwrap();
        }

        #[test]
        fn mediawiki_externallinks() {
            let qstring = "CREATE TABLE `externallinks` (
          `el_id` int(10) unsigned NOT NULL AUTO_INCREMENT,
          `el_from` int(8) unsigned NOT NULL DEFAULT '0',
          `el_from_namespace` int(11) NOT NULL DEFAULT '0',
          `el_to` blob NOT NULL,
          `el_index` blob NOT NULL,
          `el_index_60` varbinary(60) NOT NULL,
          PRIMARY KEY (`el_id`),
          KEY `el_from` (`el_from`,`el_to`(40)),
          KEY `el_to` (`el_to`(60),`el_from`),
          KEY `el_index` (`el_index`(60)),
          KEY `el_backlinks_to` (`el_from_namespace`,`el_to`(60),`el_from`),
          KEY `el_index_60` (`el_index_60`,`el_id`),
          KEY `el_from_index_60` (`el_from`,`el_index_60`,`el_id`)
        )";
            creation(Dialect::MySQL)(qstring.as_bytes()).unwrap();
        }

        #[test]
        fn vehicle_load_profiles() {
            let qstring = b"CREATE TABLE `vehicle_load_profiles` (
  `vehicle_load_profile_id` int(11) NOT NULL AUTO_INCREMENT,
  `vehicle_id` int(11) NOT NULL,
  `charge_event_id` int(11) DEFAULT NULL,
  `start_dttm` timestamp NULL DEFAULT NULL,
  `end_dttm` timestamp NULL DEFAULT NULL,
  `is_home` tinyint(1) DEFAULT NULL,
  `energy_delivered` float DEFAULT NULL,
  `energy_added` float DEFAULT NULL,
  `soc_added` float DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `last_updated_at` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  PRIMARY KEY (`vehicle_load_profile_id`),
  KEY `load_profile_vehicle` (`vehicle_id`),
  KEY `vlp_charge_event` (`charge_event_id`),
  CONSTRAINT `load_profile_vehicle` FOREIGN KEY (`vehicle_id`) REFERENCES `vehicles` (`vehicle_id`),
  CONSTRAINT `vlp_charge_event` FOREIGN KEY (`charge_event_id`) REFERENCES `charge_events` (`charge_event_id`)
) ENGINE=InnoDB AUTO_INCREMENT=546971 DEFAULT CHARSET=latin1";
            let res = creation(Dialect::MySQL)(qstring);
            assert!(res.is_ok());
        }
    }

    mod postgres {
        use crate::{ColumnConstraint, Literal, SqlType};

        use super::*;
        use crate::column::Column;
        use crate::table::Table;

        #[test]
        fn double_precision_column() {
            let (rem, res) =
                creation(Dialect::PostgreSQL)(b"create table t(x double precision)").unwrap();
            assert_eq!(str::from_utf8(rem).unwrap(), "");
            assert_eq!(
                res,
                CreateTableStatement {
                    table: "t".into(),
                    fields: vec![ColumnSpecification {
                        column: "t.x".into(),
                        sql_type: SqlType::Double,
                        constraints: vec![],
                        comment: None,
                    }],
                    keys: None,
                    if_not_exists: false,
                    options: vec![],
                }
            );
        }

        #[test]
        fn create_with_non_reserved_identifier() {
            let qstring = "CREATE TABLE groups ( id integer );";
            let res = creation(Dialect::PostgreSQL)(qstring.as_bytes());
            assert_eq!(
                res.unwrap().1,
                CreateTableStatement {
                    table: Table::from("groups"),
                    fields: vec![ColumnSpecification::new(
                        Column::from("groups.id"),
                        SqlType::Int(None)
                    ),],
                    ..Default::default()
                }
            );
        }

        #[test]
        fn create_with_reserved_identifier() {
            let qstring = "CREATE TABLE select ( id integer );";
            let res = creation(Dialect::PostgreSQL)(qstring.as_bytes());
            assert!(res.is_err());
        }

        #[test]
        fn django_create() {
            let qstring = "CREATE TABLE \"django_admin_log\" (
                       \"id\" integer AUTO_INCREMENT NOT NULL PRIMARY KEY,
                       \"action_time\" datetime NOT NULL,
                       \"user_id\" integer NOT NULL,
                       \"content_type_id\" integer,
                       \"object_id\" longtext,
                       \"object_repr\" varchar(200) NOT NULL,
                       \"action_flag\" smallint UNSIGNED NOT NULL,
                       \"change_message\" longtext NOT NULL);";
            let res = creation(Dialect::PostgreSQL)(qstring.as_bytes());
            assert_eq!(
                res.unwrap().1,
                CreateTableStatement {
                    table: Table::from("django_admin_log"),
                    fields: vec![
                        ColumnSpecification::with_constraints(
                            Column::from("django_admin_log.id"),
                            SqlType::Int(None),
                            vec![
                                ColumnConstraint::AutoIncrement,
                                ColumnConstraint::NotNull,
                                ColumnConstraint::PrimaryKey,
                            ],
                        ),
                        ColumnSpecification::with_constraints(
                            Column::from("django_admin_log.action_time"),
                            SqlType::DateTime(None),
                            vec![ColumnConstraint::NotNull],
                        ),
                        ColumnSpecification::with_constraints(
                            Column::from("django_admin_log.user_id"),
                            SqlType::Int(None),
                            vec![ColumnConstraint::NotNull],
                        ),
                        ColumnSpecification::new(
                            Column::from("django_admin_log.content_type_id"),
                            SqlType::Int(None),
                        ),
                        ColumnSpecification::new(
                            Column::from("django_admin_log.object_id"),
                            SqlType::Longtext,
                        ),
                        ColumnSpecification::with_constraints(
                            Column::from("django_admin_log.object_repr"),
                            SqlType::Varchar(200),
                            vec![ColumnConstraint::NotNull],
                        ),
                        ColumnSpecification::with_constraints(
                            Column::from("django_admin_log.action_flag"),
                            SqlType::UnsignedSmallint(None),
                            vec![ColumnConstraint::NotNull],
                        ),
                        ColumnSpecification::with_constraints(
                            Column::from("django_admin_log.change_message"),
                            SqlType::Longtext,
                            vec![ColumnConstraint::NotNull],
                        ),
                    ],
                    keys: Some(vec![TableKey::PrimaryKey {
                        name: None,
                        columns: vec![Column {
                            name: "id".into(),
                            table: Some("django_admin_log".into()),
                            function: None,
                        }]
                    }]),
                    if_not_exists: false,
                    options: vec![],
                }
            );

            let qstring = "CREATE TABLE \"auth_group\" (
                       \"id\" integer AUTO_INCREMENT NOT NULL PRIMARY KEY,
                       \"name\" varchar(80) NOT NULL UNIQUE)";
            let res = creation(Dialect::PostgreSQL)(qstring.as_bytes());
            assert_eq!(
                res.unwrap().1,
                CreateTableStatement {
                    table: Table::from("auth_group"),
                    fields: vec![
                        ColumnSpecification::with_constraints(
                            Column::from("auth_group.id"),
                            SqlType::Int(None),
                            vec![
                                ColumnConstraint::AutoIncrement,
                                ColumnConstraint::NotNull,
                                ColumnConstraint::PrimaryKey,
                            ],
                        ),
                        ColumnSpecification::with_constraints(
                            Column::from("auth_group.name"),
                            SqlType::Varchar(80),
                            vec![ColumnConstraint::NotNull, ColumnConstraint::Unique],
                        ),
                    ],
                    keys: Some(vec![TableKey::PrimaryKey {
                        name: None,
                        columns: vec![Column {
                            name: "id".into(),
                            table: Some("auth_group".into()),
                            function: None,
                        }],
                    }]),
                    if_not_exists: false,
                    options: vec![],
                }
            );
        }

        #[test]
        fn format_create() {
            let qstring = "CREATE TABLE \"auth_group\" (
                       \"id\" integer AUTO_INCREMENT NOT NULL PRIMARY KEY,
                       \"name\" varchar(80) NOT NULL UNIQUE)";
            // TODO(malte): INTEGER isn't quite reflected right here, perhaps
            let expected = "CREATE TABLE `auth_group` (\
                        `id` INT AUTO_INCREMENT NOT NULL, \
                        `name` VARCHAR(80) NOT NULL UNIQUE, PRIMARY KEY (`id`))";
            let res = creation(Dialect::PostgreSQL)(qstring.as_bytes());
            assert_eq!(format!("{}", res.unwrap().1), expected);
        }

        #[test]
        fn simple_create_view() {
            use crate::common::FieldDefinitionExpression;
            use crate::{BinaryOperator, Expression};

            let qstring = "CREATE VIEW v AS SELECT * FROM users WHERE username = 'bob';";

            let res = view_creation(Dialect::PostgreSQL)(qstring.as_bytes());
            assert_eq!(
                res.unwrap().1,
                CreateViewStatement {
                    name: String::from("v"),
                    fields: vec![],
                    definition: Box::new(SelectSpecification::Simple(SelectStatement {
                        tables: vec![Table::from("users")],
                        fields: vec![FieldDefinitionExpression::All],
                        where_clause: Some(Expression::BinaryOp {
                            lhs: Box::new(Expression::Column("username".into())),
                            rhs: Box::new(Expression::Literal(Literal::String("bob".into()))),
                            op: BinaryOperator::Equal,
                        }),
                        ..Default::default()
                    })),
                }
            );
        }

        #[test]
        fn format_create_view() {
            let qstring = "CREATE VIEW \"v\" AS SELECT * FROM \"t\";";
            let expected = "CREATE VIEW `v` AS SELECT * FROM `t`";
            let res = view_creation(Dialect::PostgreSQL)(qstring.as_bytes());
            assert_eq!(format!("{}", res.unwrap().1), expected);
        }

        #[test]
        fn lobsters_indexes() {
            let qstring = "CREATE TABLE \"comments\" (
            \"id\" int unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY,
            \"hat_id\" int,
            fulltext INDEX \"index_comments_on_comment\"  (\"comment\"),
            INDEX \"confidence_idx\"  (\"confidence\"),
            UNIQUE INDEX \"short_id\"  (\"short_id\"),
            INDEX \"story_id_short_id\"  (\"story_id\", \"short_id\"),
            INDEX \"thread_id\"  (\"thread_id\"),
            INDEX \"index_comments_on_user_id\"  (\"user_id\"))
            ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;";
            let res = creation(Dialect::PostgreSQL)(qstring.as_bytes());
            assert_eq!(
                res.unwrap().1,
                CreateTableStatement {
                    table: Table::from("comments"),
                    fields: vec![
                        ColumnSpecification::with_constraints(
                            Column::from("comments.id"),
                            SqlType::UnsignedInt(None),
                            vec![
                                ColumnConstraint::NotNull,
                                ColumnConstraint::AutoIncrement,
                                ColumnConstraint::PrimaryKey,
                            ],
                        ),
                        ColumnSpecification::new(
                            Column::from("comments.hat_id"),
                            SqlType::Int(None),
                        ),
                    ],
                    keys: Some(vec![
                        TableKey::FulltextKey(
                            Some("index_comments_on_comment".into()),
                            vec![Column::from("comments.comment")]
                        ),
                        TableKey::Key {
                            name: "confidence_idx".into(),
                            columns: vec![Column::from("comments.confidence")],
                            index_type: None
                        },
                        TableKey::UniqueKey {
                            name: Some("short_id".into()),
                            columns: vec![Column::from("comments.short_id")],
                            index_type: None,
                        },
                        TableKey::Key {
                            name: "story_id_short_id".into(),
                            columns: vec![
                                Column::from("comments.story_id"),
                                Column::from("comments.short_id")
                            ],
                            index_type: None
                        },
                        TableKey::Key {
                            name: "thread_id".into(),
                            columns: vec![Column::from("comments.thread_id")],
                            index_type: None
                        },
                        TableKey::Key {
                            name: "index_comments_on_user_id".into(),
                            columns: vec![Column::from("comments.user_id")],
                            index_type: None
                        },
                        TableKey::PrimaryKey {
                            name: None,
                            columns: vec![Column {
                                name: "id".into(),
                                table: Some("comments".into()),
                                function: None,
                            }]
                        },
                    ]),
                    if_not_exists: false,
                    options: vec![
                        CreateTableOption::Engine(Some("InnoDB".to_string())),
                        CreateTableOption::Charset("utf8mb4".to_string())
                    ],
                }
            );
        }

        #[test]
        fn mediawiki_create() {
            let qstring =
                "CREATE TABLE user_newtalk (  user_id int(5) NOT NULL default '0',  user_ip \
                       varchar(40) NOT NULL default '') TYPE=MyISAM;";
            let res = creation(Dialect::PostgreSQL)(qstring.as_bytes());
            assert_eq!(
                res.unwrap().1,
                CreateTableStatement {
                    table: Table::from("user_newtalk"),
                    fields: vec![
                        ColumnSpecification::with_constraints(
                            Column::from("user_newtalk.user_id"),
                            SqlType::Int(Some(5)),
                            vec![
                                ColumnConstraint::NotNull,
                                ColumnConstraint::DefaultValue(Literal::String(String::from("0"))),
                            ],
                        ),
                        ColumnSpecification::with_constraints(
                            Column::from("user_newtalk.user_ip"),
                            SqlType::Varchar(40),
                            vec![
                                ColumnConstraint::NotNull,
                                ColumnConstraint::DefaultValue(Literal::String(String::from(""))),
                            ],
                        ),
                    ],
                    options: vec![CreateTableOption::Other],
                    ..Default::default()
                }
            );
        }

        #[test]
        fn mediawiki_create2() {
            let qstring = "CREATE TABLE \"user\" (
                        user_id int unsigned NOT NULL PRIMARY KEY AUTO_INCREMENT,
                        user_name varchar(255) binary NOT NULL default '',
                        user_real_name varchar(255) binary NOT NULL default '',
                        user_password tinyblob NOT NULL,
                        user_newpassword tinyblob NOT NULL,
                        user_newpass_time binary(14),
                        user_email tinytext NOT NULL,
                        user_touched binary(14) NOT NULL default '',
                        user_token binary(32) NOT NULL default '',
                        user_email_authenticated binary(14),
                        user_email_token binary(32),
                        user_email_token_expires binary(14),
                        user_registration binary(14),
                        user_editcount int,
                        user_password_expires varbinary(14) DEFAULT NULL
                       ) ENGINE=, DEFAULT CHARSET=utf8";
            creation(Dialect::PostgreSQL)(qstring.as_bytes()).unwrap();
        }

        #[test]
        fn mediawiki_create3() {
            let qstring = "CREATE TABLE \"interwiki\" (
 iw_prefix varchar(32) NOT NULL,
 iw_url blob NOT NULL,
 iw_api blob NOT NULL,
 iw_wikiid varchar(64) NOT NULL,
 iw_local bool NOT NULL,
 iw_trans tinyint NOT NULL default 0
 ) ENGINE=, DEFAULT CHARSET=utf8";
            creation(Dialect::PostgreSQL)(qstring.as_bytes()).unwrap();
        }

        #[test]
        fn mediawiki_externallinks() {
            let qstring = "CREATE TABLE \"externallinks\" (
          \"el_id\" int(10) unsigned NOT NULL AUTO_INCREMENT,
          \"el_from\" int(8) unsigned NOT NULL DEFAULT '0',
          \"el_from_namespace\" int(11) NOT NULL DEFAULT '0',
          \"el_to\" blob NOT NULL,
          \"el_index\" blob NOT NULL,
          \"el_index_60\" varbinary(60) NOT NULL,
          PRIMARY KEY (\"el_id\"),
          KEY \"el_from\" (\"el_from\",\"el_to\"(40)),
          KEY \"el_to\" (\"el_to\"(60),\"el_from\"),
          KEY \"el_index\" (\"el_index\"(60)), KEY \"el_backlinks_to\" (\"el_from_namespace\",\"el_to\"(60),\"el_from\"),
          KEY \"el_index_60\" (\"el_index_60\",\"el_id\"),
          KEY \"el_from_index_60\" (\"el_from\",\"el_index_60\",\"el_id\")
        )";
            creation(Dialect::PostgreSQL)(qstring.as_bytes()).unwrap();
        }

        #[test]
        fn vehicle_load_profiles() {
            let qstring = b"CREATE TABLE \"vehicle_load_profiles\" (
  \"vehicle_load_profile_id\" int(11) NOT NULL AUTO_INCREMENT,
  \"vehicle_id\" int(11) NOT NULL,
  \"charge_event_id\" int(11) DEFAULT NULL,
  \"start_dttm\" timestamp NULL DEFAULT NULL,
  \"end_dttm\" timestamp NULL DEFAULT NULL,
  \"is_home\" tinyint(1) DEFAULT NULL,
  \"energy_delivered\" float DEFAULT NULL,
  \"energy_added\" float DEFAULT NULL,
  \"soc_added\" float DEFAULT NULL,
  \"created_at\" timestamp NOT NULL DEFAULT current_timestamp(),
  \"last_updated_at\" timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  PRIMARY KEY (\"vehicle_load_profile_id\"),
  KEY \"load_profile_vehicle\" (\"vehicle_id\"),
  KEY \"vlp_charge_event\" (\"charge_event_id\"),
  CONSTRAINT \"load_profile_vehicle\" FOREIGN KEY (\"vehicle_id\") REFERENCES \"vehicles\" (\"vehicle_id\"),
  CONSTRAINT \"vlp_charge_event\" FOREIGN KEY (\"charge_event_id\") REFERENCES \"charge_events\" (\"charge_event_id\")
) ENGINE=InnoDB AUTO_INCREMENT=546971 DEFAULT CHARSET=latin1";
            let res = creation(Dialect::PostgreSQL)(qstring);
            assert!(res.is_ok());
        }
    }

    #[test]
    fn flarum_create_1() {
        let qstring = b"CREATE TABLE `access_tokens` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `token` varchar(40) COLLATE utf8mb4_unicode_ci NOT NULL,
  `user_id` int(10) unsigned NOT NULL,
  `last_activity_at` datetime NOT NULL,
  `created_at` datetime NOT NULL,
  `type` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL,
  `title` varchar(150) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `last_ip_address` varchar(45) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `last_user_agent` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `access_tokens_token_unique` (`token`),
  KEY `access_tokens_user_id_foreign` (`user_id`),
  KEY `access_tokens_type_index` (`type`),
  CONSTRAINT `access_tokens_user_id_foreign` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci";
        let res = test_parse!(creation(Dialect::MySQL), qstring);
        let col = |n: &str| Column {
            name: n.into(),
            table: Some("access_tokens".into()),
            function: None,
        };

        assert_eq!(
            res,
            CreateTableStatement {
                table: "access_tokens".into(),
                fields: vec![
                    ColumnSpecification::with_constraints(
                        col("id"),
                        SqlType::UnsignedInt(Some(10)),
                        vec![ColumnConstraint::NotNull, ColumnConstraint::AutoIncrement,]
                    ),
                    ColumnSpecification::with_constraints(
                        col("token"),
                        SqlType::Varchar(40),
                        vec![
                            ColumnConstraint::Collation("utf8mb4_unicode_ci".into()),
                            ColumnConstraint::NotNull
                        ]
                    ),
                    ColumnSpecification::with_constraints(
                        col("user_id"),
                        SqlType::UnsignedInt(Some(10)),
                        vec![ColumnConstraint::NotNull]
                    ),
                    ColumnSpecification::with_constraints(
                        col("last_activity_at"),
                        SqlType::DateTime(None),
                        vec![ColumnConstraint::NotNull]
                    ),
                    ColumnSpecification::with_constraints(
                        col("created_at"),
                        SqlType::DateTime(None),
                        vec![ColumnConstraint::NotNull],
                    ),
                    ColumnSpecification::with_constraints(
                        col("type"),
                        SqlType::Varchar(100),
                        vec![
                            ColumnConstraint::Collation("utf8mb4_unicode_ci".into()),
                            ColumnConstraint::NotNull,
                        ]
                    ),
                    ColumnSpecification::with_constraints(
                        col("title"),
                        SqlType::Varchar(150),
                        vec![
                            ColumnConstraint::Collation("utf8mb4_unicode_ci".into()),
                            ColumnConstraint::DefaultValue(Literal::Null),
                        ]
                    ),
                    ColumnSpecification::with_constraints(
                        col("last_ip_address"),
                        SqlType::Varchar(45),
                        vec![
                            ColumnConstraint::Collation("utf8mb4_unicode_ci".into()),
                            ColumnConstraint::DefaultValue(Literal::Null),
                        ]
                    ),
                    ColumnSpecification::with_constraints(
                        col("last_user_agent"),
                        SqlType::Varchar(255),
                        vec![
                            ColumnConstraint::Collation("utf8mb4_unicode_ci".into()),
                            ColumnConstraint::DefaultValue(Literal::Null),
                        ]
                    ),
                ],
                keys: Some(vec![
                    TableKey::PrimaryKey {
                        name: None,
                        columns: vec![col("id")]
                    },
                    TableKey::UniqueKey {
                        name: Some("access_tokens_token_unique".into()),
                        columns: vec![col("token")],
                        index_type: None,
                    },
                    TableKey::Key {
                        name: "access_tokens_user_id_foreign".into(),
                        columns: vec![col("user_id")],
                        index_type: None,
                    },
                    TableKey::Key {
                        name: "access_tokens_type_index".into(),
                        columns: vec![col("type")],
                        index_type: None,
                    },
                    TableKey::ForeignKey {
                        name: Some("access_tokens_user_id_foreign".into()),
                        columns: vec![col("user_id")],
                        target_table: "users".into(),
                        target_columns: vec!["id".into()],
                        index_name: None,
                        on_delete: Some(ReferentialAction::Cascade),
                        on_update: Some(ReferentialAction::Cascade),
                    },
                ]),
                if_not_exists: false,
                options: vec![
                    CreateTableOption::Engine(Some("InnoDB".to_string())),
                    CreateTableOption::Charset("utf8mb4".to_string()),
                    CreateTableOption::Collate("utf8mb4_unicode_ci".to_string())
                ],
            }
        )
    }

    #[test]
    fn flarum_create_2() {
        let qstring = b"create table `mentions_posts` (`post_id` int unsigned not null, `mentions_id` int unsigned not null) default character set utf8mb4 collate 'utf8mb4_unicode_ci'";
        let res = test_parse!(creation(Dialect::MySQL), qstring);
        let col = |n: &str| Column {
            name: n.into(),
            table: Some("mentions_posts".into()),
            function: None,
        };

        assert_eq!(
            res,
            CreateTableStatement {
                table: "mentions_posts".into(),
                fields: vec![
                    ColumnSpecification::with_constraints(
                        col("post_id"),
                        SqlType::UnsignedInt(None),
                        vec![ColumnConstraint::NotNull],
                    ),
                    ColumnSpecification::with_constraints(
                        col("mentions_id"),
                        SqlType::UnsignedInt(None),
                        vec![ColumnConstraint::NotNull],
                    ),
                ],
                keys: None,
                if_not_exists: false,
                options: vec![
                    CreateTableOption::Charset("utf8mb4".to_string()),
                    CreateTableOption::Collate("utf8mb4_unicode_ci".to_string())
                ],
            }
        )
    }
}
