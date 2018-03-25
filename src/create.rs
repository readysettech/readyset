use nom::{alphanumeric, digit, multispace};
use std::str;
use std::str::FromStr;
use std::fmt;

use common::{column_identifier_no_alias, field_list, integer_literal, opt_multispace,
             sql_identifier, statement_terminator, table_reference, type_identifier, Literal,
             Real, SqlType, TableKey};
use column::{Column, ColumnConstraint, ColumnSpecification};
use keywords::escape_if_keyword;
use table::Table;

#[derive(Clone, Debug, Default, Hash, PartialEq, Serialize, Deserialize)]
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

/// Parse rule for an individual key specification.
named!(pub key_specification<&[u8], TableKey>,
    alt_complete!(
          do_parse!(
              tag_no_case!("fulltext key") >>
              opt_multispace >>
              name: opt!(sql_identifier) >>
              opt_multispace >>
              columns: delimited!(tag!("("), delimited!(opt_multispace, field_list, opt_multispace), tag!(")")) >>
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
              columns: delimited!(tag!("("), delimited!(opt_multispace, field_list, opt_multispace), tag!(")")) >>
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
              columns: delimited!(tag!("("), delimited!(opt_multispace, field_list, opt_multispace), tag!(")")) >>
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
              columns: delimited!(tag!("("), delimited!(opt_multispace, field_list, opt_multispace), tag!(")")) >>
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
                  | do_parse!(d: map_res!(digit, str::from_utf8) >> (
                        Literal::Integer(i64::from_str(d).unwrap())
                    ))
                  | do_parse!(i: map_res!(digit, str::from_utf8) >>
                              tag!(".") >>
                              f: map_res!(digit, str::from_utf8) >> (
                              Literal::FixedPoint(Real {
                                  integral: i32::from_str(i).unwrap(),
                                  fractional: u32::from_str(f).unwrap()
                              })
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
              tag_no_case!("collate") >>
              multispace >>
              collation: map_res!(alphanumeric, str::from_utf8) >>
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
                    tag_no_case!("default charset") >>
                    opt_multispace >>
                    tag!("=") >>
                    opt_multispace >>
                    alt_complete!(tag!("utf8mb4") | tag!("utf8")) >>
                    ()
                )
            )
        ) >>
        opt_multispace >>
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
                keys: Some(vec![
                    TableKey::UniqueKey(Some(String::from("id_k")), vec![Column::from("users.id")]),
                ]),
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
}
