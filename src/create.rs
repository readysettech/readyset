use nom::{alphanumeric, digit, multispace};
use nom::{IResult, Err, ErrorKind, Needed};
use std::str;
use std::str::FromStr;

use common::{column_identifier_no_alias, field_list, sql_identifier, statement_terminator,
             table_reference, SqlType, TableKey};
use column::Column;
use table::Table;

#[derive(Clone, Debug, Default, Hash, PartialEq, Serialize, Deserialize)]
pub struct CreateTableStatement {
    pub table: Table,
    pub fields: Vec<(Column, SqlType)>,
    pub keys: Option<Vec<TableKey>>,
}

fn len_as_u16(len: &[u8]) -> u16 {
    match str::from_utf8(len) {
        Ok(s) => {
            match u16::from_str(s) {
                Ok(v) => v,
                Err(e) => panic!(e),
            }
        }
        Err(e) => panic!(e),
    }
}

/// A SQL type specifier.
named!(pub type_identifier<&[u8], SqlType>,
    alt_complete!(
          chain!(
              caseless_tag!("mediumtext"),
              || { SqlType::Mediumtext }
          )
        | chain!(
              caseless_tag!("timestamp") ~
              _len: opt!(delimited!(tag!("("), digit, tag!(")"))) ~
              multispace?,
              || { SqlType::Timestamp }
          )
        | chain!(
              caseless_tag!("varbinary") ~
              len: delimited!(tag!("("), digit, tag!(")")) ~
              multispace?,
              || { SqlType::Varbinary(len_as_u16(len)) }
          )
        | chain!(
              caseless_tag!("mediumblob"),
              || { SqlType::Mediumblob }
          )
        | chain!(
              caseless_tag!("longblob"),
              || { SqlType::Longblob }
          )
        | chain!(
              caseless_tag!("tinyblob"),
              || { SqlType::Tinyblob }
          )
        | chain!(
              caseless_tag!("tinytext"),
              || { SqlType::Tinytext }
          )
        | chain!(
              caseless_tag!("varchar") ~
              len: delimited!(tag!("("), digit, tag!(")")) ~
              multispace? ~
              _binary: opt!(caseless_tag!("binary")),
              || { SqlType::Varchar(len_as_u16(len)) }
          )
        | chain!(
              caseless_tag!("tinyint") ~
              len: delimited!(tag!("("), digit, tag!(")")) ~
              multispace? ~
              _signed: opt!(alt_complete!(caseless_tag!("unsigned") | caseless_tag!("signed"))),
              || { SqlType::Tinyint(len_as_u16(len)) }
          )
        | chain!(
              caseless_tag!("bigint") ~
              len: delimited!(tag!("("), digit, tag!(")")) ~
              multispace? ~
              _signed: opt!(alt_complete!(caseless_tag!("unsigned") | caseless_tag!("signed"))),
              || { SqlType::Bigint(len_as_u16(len)) }
          )
        | chain!(
              caseless_tag!("double") ~
              multispace? ~
              _signed: opt!(alt_complete!(caseless_tag!("unsigned") | caseless_tag!("signed"))),
              || { SqlType::Double }
          )
        | chain!(
              caseless_tag!("float") ~
              multispace? ~
              _signed: opt!(alt_complete!(caseless_tag!("unsigned") | caseless_tag!("signed"))),
              || { SqlType::Float }
          )
        | chain!(
              caseless_tag!("blob"),
              || { SqlType::Blob }
          )
        | chain!(
              caseless_tag!("date"),
              || { SqlType::Date }
          )
        | chain!(
              caseless_tag!("real") ~
              multispace? ~
              _signed: opt!(alt_complete!(caseless_tag!("unsigned") | caseless_tag!("signed"))),
              || { SqlType::Real }
          )
        | chain!(
              caseless_tag!("text"),
              || { SqlType::Text }
          )
        | chain!(
              caseless_tag!("char") ~
              len: delimited!(tag!("("), digit, tag!(")")) ~
              multispace? ~
              _binary: opt!(caseless_tag!("binary")),
              || { SqlType::Char(len_as_u16(len)) }
          )
        | chain!(
              caseless_tag!("int") ~
              len: opt!(delimited!(tag!("("), digit, tag!(")"))) ~
              multispace? ~
              _signed: opt!(alt_complete!(caseless_tag!("unsigned") | caseless_tag!("signed"))),
              || { SqlType::Int(match len {
                  Some(len) => len_as_u16(len),
                  None => 32 as u16,
              }) }
          )
    )
);

/// Parse rule for an individual key specification.
named!(pub key_specification<&[u8], TableKey>,
    alt_complete!(
          chain!(
              caseless_tag!("fulltext key") ~
              multispace? ~
              name: opt!(sql_identifier) ~
              multispace? ~
              columns: delimited!(tag!("("), field_list, tag!(")")),
              || {
                  match name {
                      Some(name) => {
                          let n = String::from(str::from_utf8(name).unwrap());
                          TableKey::FulltextKey(Some(n), columns)
                      },
                      None => TableKey::FulltextKey(None, columns),
                  }
              }
          )
        | chain!(
              caseless_tag!("primary key") ~
              multispace? ~
              columns: delimited!(tag!("("), field_list, tag!(")")) ~
              opt!(complete!(chain!(
                          multispace ~
                          caseless_tag!("autoincrement"),
                          || { }
                   ))
              ),
              || { TableKey::PrimaryKey(columns) }
          )
        | chain!(
              caseless_tag!("unique key") ~
              multispace? ~
              name: opt!(sql_identifier) ~
              multispace? ~
              columns: delimited!(tag!("("), field_list, tag!(")")),
              || {
                  match name {
                      Some(name) => {
                          let n = String::from(str::from_utf8(name).unwrap());
                          TableKey::UniqueKey(Some(n), columns)
                      },
                      None => TableKey::UniqueKey(None, columns),
                  }
              }
          )
        | chain!(
              caseless_tag!("key") ~
              multispace? ~
              name: sql_identifier ~
              multispace? ~
              columns: delimited!(tag!("("), field_list, tag!(")")),
              || {
                  let n = String::from(str::from_utf8(name).unwrap());
                  TableKey::Key(n, columns)
              }
          )
    )
);

/// Parse rule for a comma-separated list.
named!(pub key_specification_list<&[u8], Vec<TableKey>>,
       many1!(
           complete!(chain!(
               key: key_specification ~
               opt!(
                   complete!(chain!(
                       multispace? ~
                       tag!(",") ~
                       multispace?,
                       || {}
                   ))
               ),
               || { key }
           ))
       )
);

/// Parse rule for a comma-separated list.
named!(pub field_specification_list<&[u8], Vec<(Column, SqlType)> >,
       many1!(
           complete!(chain!(
               fieldname: column_identifier_no_alias ~
               fieldtype: opt!(complete!(chain!(multispace ~
                                      ti: type_identifier ~
                                      multispace?,
                                      || { ti }
                               ))
               ) ~
               // XXX(malte): some of these are mutually exclusive...
               opt!(complete!(chain!(multispace? ~
                           caseless_tag!("not null") ~
                           multispace?,
                           || {}
                    ))
               ) ~
               opt!(complete!(chain!(multispace? ~
                           caseless_tag!("auto_increment") ~
                           multispace?,
                           || {}
                    ))
               ) ~
               opt!(complete!(
                       chain!(
                           multispace? ~
                           caseless_tag!("default") ~
                           multispace ~
                           alt_complete!(
                                 delimited!(tag!("'"), take_until!("'"), tag!("'"))
                               | digit
                               | tag!("''")
                               | caseless_tag!("null")
                               | caseless_tag!("current_timestamp")
                           ) ~
                           multispace?,
                           || {}
                       ))
               ) ~
               opt!(
                   complete!(chain!(
                       multispace? ~
                       tag!(",") ~
                       multispace?,
                       || {}
                   ))
               ),
               || {
                   let t = match fieldtype {
                       None => SqlType::Text,
                       Some(ref t) => t.clone(),
                   };
                   (fieldname, t)
               }
           ))
       )
);

/// Parse rule for a SQL CREATE TABLE query.
/// TODO(malte): support types, TEMPORARY tables, IF NOT EXISTS, AS stmt
named!(pub creation<&[u8], CreateTableStatement>,
    complete!(chain!(
        caseless_tag!("create") ~
        multispace ~
        caseless_tag!("table") ~
        multispace ~
        table: table_reference ~
        multispace ~
        tag!("(") ~
        multispace? ~
        fields: field_specification_list ~
        multispace? ~
        keys: opt!(key_specification_list) ~
        multispace? ~
        tag!(")") ~
        multispace? ~
        // XXX(malte): wrap the two below in a permutation! rule that permits arbitrary ordering
        opt!(
            complete!(
                chain!(
                    caseless_tag!("type") ~
                    multispace? ~
                    tag!("=") ~
                    multispace? ~
                    alphanumeric,
                    || {}
                )
            )
        ) ~
        multispace? ~
        opt!(
            complete!(
                chain!(
                    caseless_tag!("pack_keys") ~
                    multispace? ~
                    tag!("=") ~
                    multispace? ~
                    alt_complete!(tag!("0") | tag!("1")),
                    || {}
                )
            )
        ) ~
        multispace? ~
        opt!(
            complete!(
                chain!(
                    caseless_tag!("engine") ~
                    multispace? ~
                    tag!("=") ~
                    multispace? ~
                    alphanumeric,
                    || {}
                )
            )
        ) ~
        multispace? ~
        opt!(
            complete!(
                chain!(
                    caseless_tag!("default charset") ~
                    multispace? ~
                    tag!("=") ~
                    multispace? ~
                    alt_complete!(tag!("utf8")),
                    || {}
                )
            )
        ) ~
        statement_terminator,
        || {
            // "table AS alias" isn't legal in CREATE statements
            assert!(table.alias.is_none());

            CreateTableStatement {
                table: table,
                fields: fields,
                keys: keys,
            }
        }
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
        assert_eq!(res.unwrap().1,
                   vec![(Column::from("id"), SqlType::Bigint(20)),
                        (Column::from("name"), SqlType::Varchar(255))]);
    }

    #[test]
    fn simple_create() {
        let qstring = "CREATE TABLE users (id bigint(20), name varchar(255), email varchar(255));";

        let res = creation(qstring.as_bytes());
        assert_eq!(res.unwrap().1,
                   CreateTableStatement {
                       table: Table::from("users"),
                       fields: vec![(Column::from("id"), SqlType::Bigint(20)),
                                    (Column::from("name"), SqlType::Varchar(255)),
                                    (Column::from("email"), SqlType::Varchar(255))],
                       ..Default::default()
                   });
    }

    #[test]
    fn mediawiki_create() {
        let qstring = "CREATE TABLE user_newtalk (  user_id int(5) NOT NULL default '0',  user_ip \
                       varchar(40) NOT NULL default '') TYPE=MyISAM;";
        let res = creation(qstring.as_bytes());
        assert_eq!(res.unwrap().1,
                   CreateTableStatement {
                       table: Table::from("user_newtalk"),
                       fields: vec![(Column::from("user_id"), SqlType::Int(5)),
                                    (Column::from("user_ip"), SqlType::Varchar(40))],
                       ..Default::default()
                   });
    }

    #[test]
    fn keys() {
        // simple primary key
        let qstring = "CREATE TABLE users (id bigint(20), name varchar(255), email varchar(255), \
                       PRIMARY KEY (id));";

        let res = creation(qstring.as_bytes());
        assert_eq!(res.unwrap().1,
                   CreateTableStatement {
                       table: Table::from("users"),
                       fields: vec![(Column::from("id"), SqlType::Bigint(20)),
                                    (Column::from("name"), SqlType::Varchar(255)),
                                    (Column::from("email"), SqlType::Varchar(255))],
                       keys: Some(vec![TableKey::PrimaryKey(vec![Column::from("id")])]),
                       ..Default::default()
                   });

        // named unique key
        let qstring = "CREATE TABLE users (id bigint(20), name varchar(255), email varchar(255), \
                       UNIQUE KEY id_k (id));";

        let res = creation(qstring.as_bytes());
        assert_eq!(res.unwrap().1,
                   CreateTableStatement {
                       table: Table::from("users"),
                       fields: vec![(Column::from("id"), SqlType::Bigint(20)),
                                    (Column::from("name"), SqlType::Varchar(255)),
                                    (Column::from("email"), SqlType::Varchar(255))],
                       keys: Some(vec![TableKey::UniqueKey(Some(String::from("id_k")),
                                                           vec![Column::from("id")])]),
                       ..Default::default()
                   });
    }
}
