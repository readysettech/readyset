use nom::{alphanumeric, digit, multispace};
use std::str;
use std::str::FromStr;
use std::fmt;

use common::{column_identifier_no_alias, field_list, sql_identifier, statement_terminator,
             table_reference, Literal, SqlType, TableKey};
use column::{ColumnConstraint, ColumnSpecification};
use table::Table;

#[derive(Clone, Debug, Default, Hash, PartialEq, Serialize, Deserialize)]
pub struct CreateTableStatement {
    pub table: Table,
    pub fields: Vec<ColumnSpecification>,
    pub keys: Option<Vec<TableKey>>,
}

impl fmt::Display for CreateTableStatement {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CREATE TABLE {} ", self.table)?;
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

fn len_as_u16(len: &[u8]) -> u16 {
    match str::from_utf8(len) {
        Ok(s) => match u16::from_str(s) {
            Ok(v) => v,
            Err(e) => panic!(e),
        },
        Err(e) => panic!(e),
    }
}

/// A SQL type specifier.
named!(pub type_identifier<&[u8], SqlType>,
    alt_complete!(
          do_parse!(
              tag_no_case!("mediumtext") >>
              (SqlType::Mediumtext)
          )
        | do_parse!(
              tag_no_case!("timestamp") >>
              _len: opt!(delimited!(tag!("("), digit, tag!(")"))) >>
              opt!(multispace) >>
              (SqlType::Timestamp)
          )
         | do_parse!(
               tag_no_case!("varbinary") >>
               len: delimited!(tag!("("), digit, tag!(")")) >>
               opt!(multispace) >>
               (SqlType::Varbinary(len_as_u16(len)))
           )
         | do_parse!(
               tag_no_case!("mediumblob") >>
               (SqlType::Mediumblob)
           )
         | do_parse!(
               tag_no_case!("longblob") >>
               (SqlType::Longblob)
           )
         | do_parse!(
               tag_no_case!("tinyblob") >>
               (SqlType::Tinyblob)
           )
         | do_parse!(
               tag_no_case!("tinytext") >>
               (SqlType::Tinytext)
           )
         | do_parse!(
               tag_no_case!("varchar") >>
               len: delimited!(tag!("("), digit, tag!(")")) >>
               opt!(multispace) >>
               _binary: opt!(tag_no_case!("binary")) >>
               (SqlType::Varchar(len_as_u16(len)))
           )
         | do_parse!(
               tag_no_case!("tinyint") >>
               len: delimited!(tag!("("), digit, tag!(")")) >>
               opt!(multispace) >>
               _signed: opt!(alt_complete!(tag_no_case!("unsigned") | tag_no_case!("signed"))) >>
               (SqlType::Tinyint(len_as_u16(len)))
           )
         | do_parse!(
               tag_no_case!("bigint") >>
               len: delimited!(tag!("("), digit, tag!(")")) >>
               opt!(multispace) >>
               _signed: opt!(alt_complete!(tag_no_case!("unsigned") | tag_no_case!("signed"))) >>
               (SqlType::Bigint(len_as_u16(len)))
           )
         | do_parse!(
               tag_no_case!("double") >>
               opt!(multispace) >>
               _signed: opt!(alt_complete!(tag_no_case!("unsigned") | tag_no_case!("signed"))) >>
               (SqlType::Double)
           )
         | do_parse!(
               tag_no_case!("float") >>
               opt!(multispace) >>
               _signed: opt!(alt_complete!(tag_no_case!("unsigned") | tag_no_case!("signed"))) >>
               (SqlType::Float)
           )
         | do_parse!(
               tag_no_case!("blob") >>
               (SqlType::Blob)
           )
         | do_parse!(
               tag_no_case!("datetime") >>
               (SqlType::DateTime)
           )
         | do_parse!(
               tag_no_case!("date") >>
               (SqlType::Date)
           )
         | do_parse!(
               tag_no_case!("real") >>
               opt!(multispace) >>
               _signed: opt!(alt_complete!(tag_no_case!("unsigned") | tag_no_case!("signed"))) >>
               (SqlType::Real)
           )
         | do_parse!(
               tag_no_case!("text") >>
               (SqlType::Text)
           )
         | do_parse!(
               tag_no_case!("longtext") >>
               (SqlType::Longtext)
           )
         | do_parse!(
               tag_no_case!("char") >>
               len: delimited!(tag!("("), digit, tag!(")")) >>
               opt!(multispace) >>
               _binary: opt!(tag_no_case!("binary")) >>
               (SqlType::Char(len_as_u16(len)))
           )
         | do_parse!(
               alt_complete!(tag_no_case!("integer") | tag_no_case!("int") | tag_no_case!("smallint")) >>
               len: opt!(delimited!(tag!("("), digit, tag!(")"))) >>
               opt!(multispace) >>
               _signed: opt!(alt_complete!(tag_no_case!("unsigned") | tag_no_case!("signed"))) >>
               (SqlType::Int(match len {
                   Some(len) => len_as_u16(len),
                   None => 32 as u16,
               }))
           )
    )
);

/// Parse rule for an individual key specification.
named!(pub key_specification<&[u8], TableKey>,
    alt_complete!(
          do_parse!(
              tag_no_case!("fulltext key") >>
              opt!(multispace) >>
              name: opt!(sql_identifier) >>
              opt!(multispace) >>
              columns: delimited!(tag!("("), field_list, tag!(")")) >>
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
              opt!(multispace) >>
              columns: delimited!(tag!("("), field_list, tag!(")")) >>
              opt!(complete!(do_parse!(
                          multispace >>
                          tag_no_case!("autoincrement") >>
                          ()
                   ))
              ) >>
              (TableKey::PrimaryKey(columns))
          )
        | do_parse!(
              tag_no_case!("unique key") >>
              opt!(multispace) >>
              name: opt!(sql_identifier) >>
              opt!(multispace) >>
              columns: delimited!(tag!("("), field_list, tag!(")")) >>
              (match name {
                  Some(name) => {
                      let n = String::from(str::from_utf8(name).unwrap());
                      TableKey::UniqueKey(Some(n), columns)
                  },
                  None => TableKey::UniqueKey(None, columns),
              })
          )
        | do_parse!(
              tag_no_case!("key") >>
              opt!(multispace) >>
              name: sql_identifier >>
              opt!(multispace) >>
              columns: delimited!(tag!("("), field_list, tag!(")")) >>
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
                       opt!(multispace) >>
                       tag!(",") >>
                       opt!(multispace) >>
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
                                      opt!(multispace) >>
                                      (ti)
                               ))
               ) >>
               constraints: many0!(column_constraint) >>
               opt!(
                   complete!(do_parse!(
                       opt!(multispace) >>
                       tag!(",") >>
                       opt!(multispace) >>
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
                       constraints: constraints,
                   }
               })
           ))
       )
);

/// Parse rule for a column definition contraint.
named!(pub column_constraint<&[u8], ColumnConstraint>,
    alt_complete!(
          do_parse!(
              opt!(multispace) >>
              tag_no_case!("not null") >>
              opt!(multispace) >>
              (ColumnConstraint::NotNull)
          )
        | do_parse!(
              opt!(multispace) >>
              tag_no_case!("auto_increment") >>
              opt!(multispace) >>
              (ColumnConstraint::AutoIncrement)
          )
        | do_parse!(
              opt!(multispace) >>
              tag_no_case!("default") >>
              multispace >>
              def: alt_complete!(
                    do_parse!(s: delimited!(tag!("'"), take_until!("'"), tag!("'")) >> (
                        Literal::String(String::from(str::from_utf8(s).unwrap()))
                    ))
                  | do_parse!(d: map_res!(digit, str::from_utf8) >> (
                      Literal::Integer(i64::from_str(d).unwrap())
                    ))
                  | do_parse!(tag!("''") >> (Literal::String(String::from(""))))
                  | do_parse!(tag_no_case!("null") >> (Literal::Null))
                  | do_parse!(tag_no_case!("current_timestamp") >> (Literal::CurrentTimestamp))
              ) >>
              opt!(multispace) >>
              (ColumnConstraint::DefaultValue(def))
          )
        | do_parse!(
              opt!(multispace) >>
              tag_no_case!("primary key") >>
              opt!(multispace) >>
              (ColumnConstraint::PrimaryKey)
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
        opt!(multispace) >>
        fields: field_specification_list >>
        opt!(multispace) >>
        keys: opt!(key_specification_list) >>
        opt!(multispace) >>
        tag!(")") >>
        opt!(multispace) >>
        // XXX(malte): wrap the two below in a permutation! rule that permits arbitrary ordering
        opt!(
            complete!(
                do_parse!(
                    tag_no_case!("type") >>
                    opt!(multispace) >>
                    tag!("=") >>
                    opt!(multispace) >>
                    alphanumeric >>
                    ()
                )
            )
        ) >>
        opt!(multispace) >>
        opt!(
            complete!(
                do_parse!(
                    tag_no_case!("pack_keys") >>
                    opt!(multispace) >>
                    tag!("=") >>
                    opt!(multispace) >>
                    alt_complete!(tag!("0") | tag!("1")) >>
                    ()
                )
            )
        ) >>
        opt!(multispace) >>
        opt!(
            complete!(
                do_parse!(
                    tag_no_case!("engine") >>
                    opt!(multispace) >>
                    tag!("=") >>
                    opt!(multispace) >>
                    alphanumeric >>
                    ()
                )
            )
        ) >>
        opt!(multispace) >>
        opt!(
            complete!(
                do_parse!(
                    tag_no_case!("default charset") >>
                    opt!(multispace) >>
                    tag!("=") >>
                    opt!(multispace) >>
                    alt_complete!(tag!("utf8")) >>
                    ()
                )
            )
        ) >>
        statement_terminator >>
        ({
            // "table AS alias" isn't legal in CREATE statements
            assert!(table.alias.is_none());

            CreateTableStatement {
                table: table,
                fields: fields,
                keys: keys,
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
                    ColumnSpecification::new(Column::from("id"), SqlType::Bigint(20)),
                    ColumnSpecification::new(Column::from("name"), SqlType::Varchar(255)),
                    ColumnSpecification::new(Column::from("email"), SqlType::Varchar(255)),
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
                        Column::from("user_id"),
                        SqlType::Int(5),
                        vec![
                            ColumnConstraint::NotNull,
                            ColumnConstraint::DefaultValue(Literal::String(String::from("0"))),
                        ],
                    ),
                    ColumnSpecification::with_constraints(
                        Column::from("user_ip"),
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
                    ColumnSpecification::new(Column::from("id"), SqlType::Bigint(20)),
                    ColumnSpecification::new(Column::from("name"), SqlType::Varchar(255)),
                    ColumnSpecification::new(Column::from("email"), SqlType::Varchar(255)),
                ],
                keys: Some(vec![TableKey::PrimaryKey(vec![Column::from("id")])]),
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
                    ColumnSpecification::new(Column::from("id"), SqlType::Bigint(20)),
                    ColumnSpecification::new(Column::from("name"), SqlType::Varchar(255)),
                    ColumnSpecification::new(Column::from("email"), SqlType::Varchar(255)),
                ],
                keys: Some(vec![
                    TableKey::UniqueKey(Some(String::from("id_k")), vec![Column::from("id")]),
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
                        Column::from("id"),
                        SqlType::Int(32),
                        vec![
                            ColumnConstraint::AutoIncrement,
                            ColumnConstraint::NotNull,
                            ColumnConstraint::PrimaryKey,
                        ],
                    ),
                    ColumnSpecification::with_constraints(
                        Column::from("action_time"),
                        SqlType::DateTime,
                        vec![ColumnConstraint::NotNull],
                    ),
                    ColumnSpecification::with_constraints(
                        Column::from("user_id"),
                        SqlType::Int(32),
                        vec![ColumnConstraint::NotNull],
                    ),
                    ColumnSpecification::new(Column::from("content_type_id"), SqlType::Int(32)),
                    ColumnSpecification::new(Column::from("object_id"), SqlType::Longtext),
                    ColumnSpecification::with_constraints(
                        Column::from("object_repr"),
                        SqlType::Varchar(200),
                        vec![ColumnConstraint::NotNull],
                    ),
                    ColumnSpecification::with_constraints(
                        Column::from("action_flag"),
                        SqlType::Int(32),
                        vec![ColumnConstraint::NotNull],
                    ),
                    ColumnSpecification::with_constraints(
                        Column::from("change_message"),
                        SqlType::Longtext,
                        vec![ColumnConstraint::NotNull],
                    ),
                ],
                ..Default::default()
            }
        );
    }

}
