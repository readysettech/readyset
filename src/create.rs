use nom::{alphanumeric, digit, multispace};
use std::fmt;
use std::str;
use std::str::FromStr;

use column::{Column, ColumnConstraint, ColumnSpecification};
use common::{
    column_identifier_no_alias, integer_literal, opt_multispace, parse_comment, sql_identifier,
    statement_terminator, string_literal, table_reference, type_identifier, Literal, Real, SqlType,
    TableKey,
};
use compound_select::{compound_selection, CompoundSelectStatement};
use keywords::escape_if_keyword;
use order::{order_type, OrderType};
use select::{nested_selection, SelectStatement};
use table::Table;

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct CreateTableStatement {
    pub table: Table,
    pub fields: Vec<ColumnSpecification>,
    pub keys: Option<Vec<TableKey>>,
}

impl fmt::Display for CreateTableStatement {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CREATE TABLE {} ", escape_if_keyword(&self.table.name))?;
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
        write!(f, ")")
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
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
        write!(f, "CREATE VIEW {} ", escape_if_keyword(&self.name))?;
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

/// MySQL grammar element for index column definition (§13.1.18, index_col_name)
named!(pub index_col_name<&[u8], (Column, Option<u16>, Option<OrderType>)>,
    do_parse!(
        column: column_identifier_no_alias >>
        opt_multispace >>
        len: opt!(delimited!(tag!("("), map_res!(digit, str::from_utf8), tag!(")"))) >>
        order: opt!(order_type) >>
        ((column, len.map(|l| u16::from_str(l).unwrap()), order))
    )
);

/// Helper for list of index columns
named!(pub index_col_list<&[u8], Vec<Column> >,
       many0!(
           do_parse!(
               entry: index_col_name >>
               opt!(
                   complete!(do_parse!(
                       opt_multispace >>
                       tag!(",") >>
                       opt_multispace >>
                       ()
                   ))
               ) >>
               // XXX(malte): ignores length and order
               (entry.0)
           )
       )
);

/// Parse rule for an individual key specification.
named!(pub key_specification<&[u8], TableKey>,
    alt_complete!(
          do_parse!(
              tag_no_case!("fulltext") >>
              multispace >>
              alt_complete!(tag_no_case!("key") | tag_no_case!("index")) >>
              opt_multispace >>
              name: opt!(sql_identifier) >>
              opt_multispace >>
              columns: delimited!(tag!("("), delimited!(opt_multispace, index_col_list, opt_multispace), tag!(")")) >>
              (match name {
                  Some(name) => {
                      let n = String::from(str::from_utf8(name).unwrap());
                      TableKey::FulltextKey(Some(n), columns)
                  },
                  None => TableKey::FulltextKey(None, columns),
              })
          )
        | do_parse!(
              tag_no_case!("primary key") >>
              opt_multispace >>
              columns: delimited!(tag!("("), delimited!(opt_multispace, index_col_list, opt_multispace), tag!(")")) >>
              opt!(complete!(do_parse!(
                          multispace >>
                          tag_no_case!("autoincrement") >>
                          ()
                   ))
              ) >>
              (TableKey::PrimaryKey(columns))
          )
        | do_parse!(
              tag_no_case!("unique") >>
              opt!(preceded!(multispace,
                             alt_complete!(
                                   tag_no_case!("key")
                                 | tag_no_case!("index")
                             )
                   )
              ) >>
              opt_multispace >>
              name: opt!(sql_identifier) >>
              opt_multispace >>
              columns: delimited!(tag!("("), delimited!(opt_multispace, index_col_list, opt_multispace), tag!(")")) >>
              (match name {
                  Some(name) => {
                      let n = String::from(str::from_utf8(name).unwrap());
                      TableKey::UniqueKey(Some(n), columns)
                  },
                  None => TableKey::UniqueKey(None, columns),
              })
          )
        | do_parse!(
              alt_complete!(tag_no_case!("key") | tag_no_case!("index")) >>
              opt_multispace >>
              name: sql_identifier >>
              opt_multispace >>
              columns: delimited!(tag!("("), delimited!(opt_multispace, index_col_list, opt_multispace), tag!(")")) >>
              ({
                  let n = String::from(str::from_utf8(name).unwrap());
                  TableKey::Key(n, columns)
              })
          )
    )
);

/// Parse rule for a comma-separated list.
named!(pub key_specification_list<&[u8], Vec<TableKey>>,
       many1!(
           complete!(do_parse!(
               key: key_specification >>
               opt!(
                   complete!(do_parse!(
                       opt_multispace >>
                       tag!(",") >>
                       opt_multispace >>
                       ()
                   ))
               ) >>
               (key)
           ))
       )
);

/// Parse rule for a comma-separated list.
named!(pub field_specification_list<&[u8], Vec<ColumnSpecification> >,
       many1!(
           complete!(do_parse!(
               identifier: column_identifier_no_alias >>
               fieldtype: opt!(complete!(do_parse!(multispace >>
                                      ti: type_identifier >>
                                      opt_multispace >>
                                      (ti)
                               ))
               ) >>
               constraints: many0!(column_constraint) >>
               comment: opt!(parse_comment) >>
               opt!(
                   complete!(do_parse!(
                       opt_multispace >>
                       tag!(",") >>
                       opt_multispace >>
                       ()
                   ))
               ) >>
               ({
                   let t = match fieldtype {
                       None => SqlType::Text,
                       Some(ref t) => t.clone(),
                   };
                   ColumnSpecification {
                       column: identifier,
                       sql_type: t,
                       constraints: constraints.into_iter().filter_map(|m|m).collect(),
                       comment: comment,
                   }
               })
           ))
       )
);

/// Parse rule for a column definition contraint.
named!(pub column_constraint<&[u8], Option<ColumnConstraint>>,
    alt_complete!(
          do_parse!(
              opt_multispace >>
              tag_no_case!("not null") >>
              opt_multispace >>
              (Some(ColumnConstraint::NotNull))
          )
        | do_parse!(
              opt_multispace >>
              tag_no_case!("null") >>
              opt_multispace >>
              (None)
          )
        | do_parse!(
              opt_multispace >>
              tag_no_case!("auto_increment") >>
              opt_multispace >>
              (Some(ColumnConstraint::AutoIncrement))
          )
        | do_parse!(
              opt_multispace >>
              tag_no_case!("default") >>
              multispace >>
              def: alt_complete!(
                    do_parse!(s: delimited!(tag!("'"), take_until!("'"), tag!("'")) >> (
                        Literal::String(String::from(str::from_utf8(s).unwrap()))
                    ))
                  | do_parse!(i: map_res!(digit, str::from_utf8) >>
                              tag!(".") >>
                              f: map_res!(digit, str::from_utf8) >> (
                              Literal::FixedPoint(Real {
                                  integral: i32::from_str(i).unwrap(),
                                  fractional: i32::from_str(f).unwrap()
                              })
                    ))
                  | do_parse!(d: map_res!(digit, str::from_utf8) >> (
                        Literal::Integer(i64::from_str(d).unwrap())
                    ))
                  | do_parse!(tag!("''") >> (Literal::String(String::from(""))))
                  | do_parse!(tag_no_case!("null") >> (Literal::Null))
                  | do_parse!(tag_no_case!("current_timestamp") >> (Literal::CurrentTimestamp))
              ) >>
              opt_multispace >>
              (Some(ColumnConstraint::DefaultValue(def)))
          )
        | do_parse!(
              opt_multispace >>
              tag_no_case!("primary key") >>
              opt_multispace >>
              (Some(ColumnConstraint::PrimaryKey))
          )
        | do_parse!(
              opt_multispace >>
              tag_no_case!("unique") >>
              opt_multispace >>
              (Some(ColumnConstraint::Unique))
          )
        | do_parse!(
              opt_multispace >>
              tag_no_case!("character set") >>
              multispace >>
              charset: map_res!(sql_identifier, str::from_utf8) >>
              (Some(ColumnConstraint::CharacterSet(charset.to_owned())))
          )
        | do_parse!(
              opt_multispace >>
              tag_no_case!("collate") >>
              multispace >>
              collation: map_res!(sql_identifier, str::from_utf8) >>
              (Some(ColumnConstraint::Collation(collation.to_owned())))
          )
    )
);

/// Parse rule for a SQL CREATE TABLE query.
/// TODO(malte): support types, TEMPORARY tables, IF NOT EXISTS, AS stmt
named!(pub creation<&[u8], CreateTableStatement>,
    complete!(do_parse!(
        tag_no_case!("create") >>
        multispace >>
        tag_no_case!("table") >>
        multispace >>
        table: table_reference >>
        multispace >>
        tag!("(") >>
        opt_multispace >>
        fields: field_specification_list >>
        opt_multispace >>
        keys: opt!(key_specification_list) >>
        opt_multispace >>
        tag!(")") >>
        opt_multispace >>
        // XXX(malte): wrap the two below in a permutation! rule that permits arbitrary ordering
        opt!(
            complete!(
                do_parse!(
                    tag_no_case!("type") >>
                    opt_multispace >>
                    tag!("=") >>
                    opt_multispace >>
                    alphanumeric >>
                    ()
                )
            )
        ) >>
        opt_multispace >>
        opt!(
            complete!(
                do_parse!(
                    tag_no_case!("pack_keys") >>
                    opt_multispace >>
                    tag!("=") >>
                    opt_multispace >>
                    alt_complete!(tag!("0") | tag!("1")) >>
                    ()
                )
            )
        ) >>
        opt_multispace >>
        opt!(
            complete!(
                do_parse!(
                    tag_no_case!("engine") >>
                    opt_multispace >>
                    tag!("=") >>
                    opt_multispace >>
                    opt!(alphanumeric) >>
                    opt!(tag!(",")) >>
                    ()
                )
            )
        ) >>
        opt_multispace >>
        opt!(
            complete!(
                do_parse!(
                    tag_no_case!("auto_increment") >>
                    opt_multispace >>
                    tag!("=") >>
                    opt_multispace >>
                    integer_literal >>
                    ()
                )
            )
        ) >>
        opt_multispace >>
        opt!(
            complete!(
                do_parse!(
                    tag_no_case!("default charset") >>
                    opt_multispace >>
                    tag!("=") >>
                    opt_multispace >>
                    alt_complete!(tag!("utf8mb4") | tag!("utf8") | tag!("latin1")) >>
                    ()
                )
            )
        ) >>
        opt_multispace >>
        opt!(
            complete!(
                do_parse!(
                    tag_no_case!("comment") >>
                    opt_multispace >>
                    tag!("=") >>
                    opt_multispace >>
                    string_literal >>
                    ()
                )
            )
        ) >>
        opt!(
            complete!(
                do_parse!(
                    tag_no_case!("max_rows") >>
                    opt_multispace >>
                    opt!(tag!("=")) >>
                    opt_multispace >>
                    integer_literal >>
                    ()
                )
            )
        ) >>
        opt_multispace >>
        opt!(
            complete!(
                do_parse!(
                    tag_no_case!("avg_row_length") >>
                    opt_multispace >>
                    opt!(tag!("=")) >>
                    opt_multispace >>
                    integer_literal >>
                    ()
                )
            )
        ) >>
        statement_terminator >>
        ({
            // "table AS alias" isn't legal in CREATE statements
            assert!(table.alias.is_none());
            // attach table names to columns:
            let named_fields = fields
                .into_iter()
                .map(|field| {
                    let column = Column {
                        table: Some(table.name.clone()),
                        ..field.column
                    };

                    ColumnSpecification { column, ..field }
                })
                .collect();

            // and to keys:
            let named_keys = keys.and_then(|ks| {
                Some(
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
                                TableKey::PrimaryKey(columns) => {
                                    TableKey::PrimaryKey(attach_names(columns))
                                }
                                TableKey::UniqueKey(name, columns) => {
                                    TableKey::UniqueKey(name, attach_names(columns))
                                }
                                TableKey::FulltextKey(name, columns) => {
                                    TableKey::FulltextKey(name, attach_names(columns))
                                }
                                TableKey::Key(name, columns) => {
                                    TableKey::Key(name, attach_names(columns))
                                }
                            }
                        })
                        .collect(),
                )
            });

            CreateTableStatement {
                table: table,
                fields: named_fields,
                keys: named_keys,
            }
        })
    ))
);

/// Parse rule for a SQL CREATE VIEW query.
named!(pub view_creation<&[u8], CreateViewStatement>,
    complete!(do_parse!(
        tag_no_case!("create") >>
        multispace >>
        tag_no_case!("view") >>
        multispace >>
        name: sql_identifier >>
        multispace >>
        tag_no_case!("as") >>
        multispace >>
        definition: alt_complete!(
              map!(compound_selection, |s| SelectSpecification::Compound(s))
            | map!(nested_selection, |s| SelectSpecification::Simple(s))
        ) >>
        statement_terminator >>
        ({
            CreateViewStatement {
                name: String::from(str::from_utf8(name).unwrap()),
                fields: vec![],  // TODO(malte): support
                definition: Box::new(definition),
            }
        })
    ))
);

#[cfg(test)]
mod tests {
    use super::*;
    use column::Column;
    use table::Table;

    #[test]
    fn sql_types() {
        let type0 = "bigint(20) unsigned";
        let type1 = "varchar(255) binary";

        let res = type_identifier(type0.as_bytes());
        assert_eq!(res.unwrap().1, SqlType::Bigint(20));
        let res = type_identifier(type1.as_bytes());
        assert_eq!(res.unwrap().1, SqlType::Varchar(255));
    }

    #[test]
    fn field_spec() {
        // N.B. trailing comma here because field_specification_list! doesn't handle the eof case
        // because it is never validly the end of a query
        let qstring = "id bigint(20), name varchar(255),";

        let res = field_specification_list(qstring.as_bytes());
        assert_eq!(
            res.unwrap().1,
            vec![
                ColumnSpecification::new(Column::from("id"), SqlType::Bigint(20)),
                ColumnSpecification::new(Column::from("name"), SqlType::Varchar(255)),
            ]
        );
    }

    #[test]
    fn simple_create() {
        let qstring = "CREATE TABLE users (id bigint(20), name varchar(255), email varchar(255));";

        let res = creation(qstring.as_bytes());
        assert_eq!(
            res.unwrap().1,
            CreateTableStatement {
                table: Table::from("users"),
                fields: vec![
                    ColumnSpecification::new(Column::from("users.id"), SqlType::Bigint(20)),
                    ColumnSpecification::new(Column::from("users.name"), SqlType::Varchar(255)),
                    ColumnSpecification::new(Column::from("users.email"), SqlType::Varchar(255)),
                ],
                ..Default::default()
            }
        );
    }

    #[test]
    fn mediawiki_create() {
        let qstring = "CREATE TABLE user_newtalk (  user_id int(5) NOT NULL default '0',  user_ip \
                       varchar(40) NOT NULL default '') TYPE=MyISAM;";
        let res = creation(qstring.as_bytes());
        assert_eq!(
            res.unwrap().1,
            CreateTableStatement {
                table: Table::from("user_newtalk"),
                fields: vec![
                    ColumnSpecification::with_constraints(
                        Column::from("user_newtalk.user_id"),
                        SqlType::Int(5),
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
                ..Default::default()
            }
        );
    }

    #[test]
    fn mediawiki_create2() {
        let qstring = "CREATE TABLE `user` (
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
        creation(qstring.as_bytes()).unwrap();
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
        creation(qstring.as_bytes()).unwrap();
    }

    #[test]
    fn keys() {
        // simple primary key
        let qstring = "CREATE TABLE users (id bigint(20), name varchar(255), email varchar(255), \
                       PRIMARY KEY (id));";

        let res = creation(qstring.as_bytes());
        assert_eq!(
            res.unwrap().1,
            CreateTableStatement {
                table: Table::from("users"),
                fields: vec![
                    ColumnSpecification::new(Column::from("users.id"), SqlType::Bigint(20)),
                    ColumnSpecification::new(Column::from("users.name"), SqlType::Varchar(255)),
                    ColumnSpecification::new(Column::from("users.email"), SqlType::Varchar(255)),
                ],
                keys: Some(vec![TableKey::PrimaryKey(vec![Column::from("users.id")])]),
                ..Default::default()
            }
        );

        // named unique key
        let qstring = "CREATE TABLE users (id bigint(20), name varchar(255), email varchar(255), \
                       UNIQUE KEY id_k (id));";

        let res = creation(qstring.as_bytes());
        assert_eq!(
            res.unwrap().1,
            CreateTableStatement {
                table: Table::from("users"),
                fields: vec![
                    ColumnSpecification::new(Column::from("users.id"), SqlType::Bigint(20)),
                    ColumnSpecification::new(Column::from("users.name"), SqlType::Varchar(255)),
                    ColumnSpecification::new(Column::from("users.email"), SqlType::Varchar(255)),
                ],
                keys: Some(vec![TableKey::UniqueKey(
                    Some(String::from("id_k")),
                    vec![Column::from("users.id")]
                ),]),
                ..Default::default()
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
        let res = creation(qstring.as_bytes());
        assert_eq!(
            res.unwrap().1,
            CreateTableStatement {
                table: Table::from("django_admin_log"),
                fields: vec![
                    ColumnSpecification::with_constraints(
                        Column::from("django_admin_log.id"),
                        SqlType::Int(32),
                        vec![
                            ColumnConstraint::AutoIncrement,
                            ColumnConstraint::NotNull,
                            ColumnConstraint::PrimaryKey,
                        ],
                    ),
                    ColumnSpecification::with_constraints(
                        Column::from("django_admin_log.action_time"),
                        SqlType::DateTime,
                        vec![ColumnConstraint::NotNull],
                    ),
                    ColumnSpecification::with_constraints(
                        Column::from("django_admin_log.user_id"),
                        SqlType::Int(32),
                        vec![ColumnConstraint::NotNull],
                    ),
                    ColumnSpecification::new(
                        Column::from("django_admin_log.content_type_id"),
                        SqlType::Int(32),
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
                        SqlType::Int(32),
                        vec![ColumnConstraint::NotNull],
                    ),
                    ColumnSpecification::with_constraints(
                        Column::from("django_admin_log.change_message"),
                        SqlType::Longtext,
                        vec![ColumnConstraint::NotNull],
                    ),
                ],
                ..Default::default()
            }
        );

        let qstring = "CREATE TABLE `auth_group` (
                       `id` integer AUTO_INCREMENT NOT NULL PRIMARY KEY,
                       `name` varchar(80) NOT NULL UNIQUE)";
        let res = creation(qstring.as_bytes());
        assert_eq!(
            res.unwrap().1,
            CreateTableStatement {
                table: Table::from("auth_group"),
                fields: vec![
                    ColumnSpecification::with_constraints(
                        Column::from("auth_group.id"),
                        SqlType::Int(32),
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
                ..Default::default()
            }
        );
    }

    #[test]
    fn format_create() {
        let qstring = "CREATE TABLE `auth_group` (
                       `id` integer AUTO_INCREMENT NOT NULL PRIMARY KEY,
                       `name` varchar(80) NOT NULL UNIQUE)";
        // TODO(malte): INTEGER isn't quite reflected right here, perhaps
        let expected = "CREATE TABLE auth_group (\
                        id INT(32) AUTO_INCREMENT NOT NULL PRIMARY KEY, \
                        name VARCHAR(80) NOT NULL UNIQUE)";
        let res = creation(qstring.as_bytes());
        assert_eq!(format!("{}", res.unwrap().1), expected);
    }

    #[test]
    fn simple_create_view() {
        use common::{FieldDefinitionExpression, Operator};
        use condition::{ConditionBase, ConditionExpression, ConditionTree};

        let qstring = "CREATE VIEW v AS SELECT * FROM users WHERE username = \"bob\";";

        let res = view_creation(qstring.as_bytes());
        assert_eq!(
            res.unwrap().1,
            CreateViewStatement {
                name: String::from("v"),
                fields: vec![],
                definition: Box::new(SelectSpecification::Simple(SelectStatement {
                    tables: vec![Table::from("users")],
                    fields: vec![FieldDefinitionExpression::All],
                    where_clause: Some(ConditionExpression::ComparisonOp(ConditionTree {
                        left: Box::new(ConditionExpression::Base(ConditionBase::Field(
                            "username".into()
                        ))),
                        right: Box::new(ConditionExpression::Base(ConditionBase::Literal(
                            Literal::String("bob".into())
                        ))),
                        operator: Operator::Equal,
                    })),
                    ..Default::default()
                })),
            }
        );
    }

    #[test]
    fn compound_create_view() {
        use common::FieldDefinitionExpression;
        use compound_select::{CompoundSelectOperator, CompoundSelectStatement};

        let qstring = "CREATE VIEW v AS SELECT * FROM users UNION SELECT * FROM old_users;";

        let res = view_creation(qstring.as_bytes());
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
    fn format_create_view() {
        let qstring = "CREATE VIEW `v` AS SELECT * FROM `t`;";
        let expected = "CREATE VIEW v AS SELECT * FROM t";
        let res = view_creation(qstring.as_bytes());
        assert_eq!(format!("{}", res.unwrap().1), expected);
    }
}
