use nom::character::complete::{digit1, multispace0, multispace1};
use std::fmt;
use std::str;
use std::str::FromStr;

use column::{Column, ColumnConstraint, ColumnSpecification};
use common::{
    column_identifier_no_alias, parse_comment, sql_identifier, statement_terminator,
    table_reference, type_identifier, Literal, Real, SqlType, TableKey,
};
use compound_select::{compound_selection, CompoundSelectStatement};
use create_table_options::table_options;
use keywords::escape_if_keyword;
use nom::branch::alt;
use nom::bytes::complete::{tag, tag_no_case, take_until};
use nom::combinator::{map, opt};
use nom::multi::{many0, many1};
use nom::sequence::{delimited, preceded, terminated, tuple};
use nom::IResult;
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

// MySQL grammar element for index column definition (§13.1.18, index_col_name)
pub fn index_col_name(i: &[u8]) -> IResult<&[u8], (Column, Option<u16>, Option<OrderType>)> {
    let (remaining_input, (column, len_u8, order)) = tuple((
        terminated(column_identifier_no_alias, multispace0),
        opt(delimited(tag("("), digit1, tag("("))),
        opt(order_type),
    ))(i)?;
    let len = len_u8.map(|l| u16::from_str(str::from_utf8(l).unwrap()).unwrap());

    Ok((remaining_input, (column, len, order)))
}

// Helper for list of index columns
pub fn index_col_list(i: &[u8]) -> IResult<&[u8], Vec<Column>> {
    many0(map(
        terminated(
            index_col_name,
            opt(delimited(multispace0, tag(","), multispace0)),
        ),
        // XXX(malte): ignores length and order
        |e| e.0,
    ))(i)
}

// Parse rule for an individual key specification.
pub fn key_specification(i: &[u8]) -> IResult<&[u8], TableKey> {
    alt((full_text_key, primary_key, unique, key_or_index))(i)
}

fn full_text_key(i: &[u8]) -> IResult<&[u8], TableKey> {
    let (remaining_input, (_, _, _, _, name, _, columns)) = tuple((
        tag_no_case("fulltext"),
        multispace1,
        alt((tag_no_case("key"), tag_no_case("index"))),
        multispace1,
        opt(sql_identifier),
        multispace0,
        delimited(
            tag("("),
            delimited(multispace0, index_col_list, multispace0),
            tag(")"),
        ),
    ))(i)?;

    match name {
        Some(name) => {
            let n = String::from_utf8(name.to_vec()).unwrap();
            Ok((remaining_input, TableKey::FulltextKey(Some(n), columns)))
        }
        None => Ok((remaining_input, TableKey::FulltextKey(None, columns))),
    }
}

fn primary_key(i: &[u8]) -> IResult<&[u8], TableKey> {
    let (remaining_input, (_, _, columns, _)) = tuple((
        tag_no_case("primary key"),
        multispace1,
        delimited(
            tag("("),
            delimited(multispace0, index_col_list, multispace0),
            tag(")"),
        ),
        opt(map(
            preceded(multispace1, tag_no_case("auto_increment")),
            |_| (),
        )),
    ))(i)?;

    Ok((remaining_input, TableKey::PrimaryKey(columns)))
}

fn unique(i: &[u8]) -> IResult<&[u8], TableKey> {
    // TODO: add branching to correctly parse whitespace after `unique`
    let (remaining_input, (_, _, _, name, _, columns)) = tuple((
        tag_no_case("unique"),
        opt(preceded(
            multispace1,
            alt((tag_no_case("key"), tag_no_case("index"))),
        )),
        multispace0,
        opt(sql_identifier),
        multispace0,
        delimited(
            tag("("),
            delimited(multispace0, index_col_list, multispace0),
            tag(")"),
        ),
    ))(i)?;

    match name {
        Some(name) => {
            let n = String::from_utf8(name.to_vec()).unwrap();
            Ok((remaining_input, TableKey::UniqueKey(Some(n), columns)))
        }
        None => Ok((remaining_input, TableKey::UniqueKey(None, columns))),
    }
}

fn key_or_index(i: &[u8]) -> IResult<&[u8], TableKey> {
    let (remaining_input, (_, _, name, _, columns)) = tuple((
        alt((tag_no_case("key"), tag_no_case("index"))),
        multispace0,
        sql_identifier,
        multispace0,
        delimited(
            tag("("),
            delimited(multispace0, index_col_list, multispace0),
            tag(")"),
        ),
    ))(i)?;

    let n = String::from_utf8(name.to_vec()).unwrap();
    Ok((remaining_input, TableKey::Key(n, columns)))
}

// Parse rule for a comma-separated list.
pub fn key_specification_list(i: &[u8]) -> IResult<&[u8], Vec<TableKey>> {
    many1(terminated(
        key_specification,
        opt(delimited(multispace0, tag(","), multispace0)),
    ))(i)
}

fn field_specification(i: &[u8]) -> IResult<&[u8], ColumnSpecification> {
    let (remaining_input, (column, field_type, constraints, comment, _)) = tuple((
        column_identifier_no_alias,
        opt(delimited(multispace1, type_identifier, multispace0)),
        many0(column_constraint),
        opt(parse_comment),
        opt(delimited(multispace0, tag(","), multispace0)),
    ))(i)?;

    let sql_type = match field_type {
        None => SqlType::Text,
        Some(ref t) => t.clone(),
    };
    Ok((
        remaining_input,
        ColumnSpecification {
            column,
            sql_type,
            constraints: constraints.into_iter().filter_map(|m| m).collect(),
            comment,
        },
    ))
}

// Parse rule for a comma-separated list.
pub fn field_specification_list(i: &[u8]) -> IResult<&[u8], Vec<ColumnSpecification>> {
    many1(field_specification)(i)
}

// Parse rule for a column definition constraint.
pub fn column_constraint(i: &[u8]) -> IResult<&[u8], Option<ColumnConstraint>> {
    let not_null = map(
        delimited(multispace0, tag_no_case("not null"), multispace0),
        |_| Some(ColumnConstraint::NotNull),
    );
    let null = map(
        delimited(multispace0, tag_no_case("null"), multispace0),
        |_| None,
    );
    let auto_increment = map(
        delimited(multispace0, tag_no_case("auto_increment"), multispace0),
        |_| Some(ColumnConstraint::AutoIncrement),
    );
    let primary_key = map(
        delimited(multispace0, tag_no_case("primary key"), multispace0),
        |_| Some(ColumnConstraint::PrimaryKey),
    );
    let unique = map(
        delimited(multispace0, tag_no_case("unique"), multispace0),
        |_| Some(ColumnConstraint::Unique),
    );
    let character_set = map(
        preceded(
            delimited(multispace0, tag_no_case("character set"), multispace1),
            sql_identifier,
        ),
        |cs| {
            let char_set = str::from_utf8(cs).unwrap().to_owned();
            Some(ColumnConstraint::CharacterSet(char_set))
        },
    );
    let collate = map(
        preceded(
            delimited(multispace0, tag_no_case("collate"), multispace1),
            sql_identifier,
        ),
        |c| {
            let collation = str::from_utf8(c).unwrap().to_owned();
            Some(ColumnConstraint::Collation(collation))
        },
    );

    alt((
        not_null,
        null,
        auto_increment,
        default,
        primary_key,
        unique,
        character_set,
        collate,
    ))(i)
}

fn fixed_point(i: &[u8]) -> IResult<&[u8], Literal> {
    let (remaining_input, (i, _, f)) = tuple((digit1, tag("."), digit1))(i)?;

    Ok((
        remaining_input,
        Literal::FixedPoint(Real {
            integral: i32::from_str(str::from_utf8(i).unwrap()).unwrap(),
            fractional: i32::from_str(str::from_utf8(f).unwrap()).unwrap(),
        }),
    ))
}

fn default(i: &[u8]) -> IResult<&[u8], Option<ColumnConstraint>> {
    let (remaining_input, (_, _, _, def, _)) = tuple((
        multispace0,
        tag_no_case("default"),
        multispace1,
        alt((
            map(
                delimited(tag("'"), take_until("'"), tag("'")),
                |s: &[u8]| Literal::String(String::from_utf8(s.to_vec()).unwrap()),
            ),
            fixed_point,
            map(digit1, |d| {
                let d_i64 = i64::from_str(str::from_utf8(d).unwrap()).unwrap();
                Literal::Integer(d_i64)
            }),
            map(tag("''"), |_| Literal::String(String::from(""))),
            map(tag_no_case("null"), |_| Literal::Null),
            map(tag_no_case("current_timestamp"), |_| {
                Literal::CurrentTimestamp
            }),
        )),
        multispace0,
    ))(i)?;

    Ok((remaining_input, Some(ColumnConstraint::DefaultValue(def))))
}

// Parse rule for a SQL CREATE TABLE query.
// TODO(malte): support types, TEMPORARY tables, IF NOT EXISTS, AS stmt
pub fn creation(i: &[u8]) -> IResult<&[u8], CreateTableStatement> {
    let (remaining_input, (_, _, _, _, table, _, _, _, fields_list, _, keys_list, _, _, _, _, _)) =
        tuple((
            tag_no_case("create"),
            multispace1,
            tag_no_case("table"),
            multispace1,
            table_reference,
            multispace0,
            tag("("),
            multispace0,
            field_specification_list,
            multispace0,
            opt(key_specification_list),
            multispace0,
            tag(")"),
            multispace0,
            table_options,
            statement_terminator,
        ))(i)?;

    // "table AS alias" isn't legal in CREATE statements
    assert!(table.alias.is_none());
    // attach table names to columns:
    let fields = fields_list
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
    let keys = keys_list.and_then(|ks| {
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
                        TableKey::Key(name, columns) => TableKey::Key(name, attach_names(columns)),
                    }
                })
                .collect(),
        )
    });

    Ok((
        remaining_input,
        CreateTableStatement {
            table,
            fields,
            keys,
        },
    ))
}

// Parse rule for a SQL CREATE VIEW query.
pub fn view_creation(i: &[u8]) -> IResult<&[u8], CreateViewStatement> {
    let (remaining_input, (_, _, _, _, name_slice, _, _, _, def, _)) = tuple((
        tag_no_case("create"),
        multispace1,
        tag_no_case("view"),
        multispace1,
        sql_identifier,
        multispace1,
        tag_no_case("as"),
        multispace1,
        alt((
            map(compound_selection, |s| SelectSpecification::Compound(s)),
            map(nested_selection, |s| SelectSpecification::Simple(s)),
        )),
        statement_terminator,
    ))(i)?;

    let name = String::from_utf8(name_slice.to_vec()).unwrap();
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

#[cfg(test)]
mod tests {
    use super::*;
    use column::Column;
    use table::Table;

    #[test]
    fn sql_types() {
        let type0 = "bigint(20)";
        let type1 = "varchar(255) binary";
        let type2 = "bigint(20) unsigned";
        let type3 = "bigint(20) signed";

        let res = type_identifier(type0.as_bytes());
        assert_eq!(res.unwrap().1, SqlType::Bigint(20));
        let res = type_identifier(type1.as_bytes());
        assert_eq!(res.unwrap().1, SqlType::Varchar(255));
        let res = type_identifier(type2.as_bytes());
        assert_eq!(res.unwrap().1, SqlType::UnsignedBigint(20));
        let res = type_identifier(type3.as_bytes());
        assert_eq!(res.unwrap().1, SqlType::Bigint(20));
        let res = type_identifier(type2.as_bytes());
        assert_eq!(res.unwrap().1, SqlType::UnsignedBigint(20));
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
    fn create_without_space_after_tablename() {
        let qstring = "CREATE TABLE t(x integer);";
        let res = creation(qstring.as_bytes());
        assert_eq!(
            res.unwrap().1,
            CreateTableStatement {
                table: Table::from("t"),
                fields: vec![ColumnSpecification::new(
                    Column::from("t.x"),
                    SqlType::Int(32)
                ),],
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
                    vec![Column::from("users.id")],
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
                        SqlType::DateTime(0),
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
                        SqlType::UnsignedInt(32),
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
        let res = creation(qstring.as_bytes());
        assert_eq!(
            res.unwrap().1,
            CreateTableStatement {
                table: Table::from("comments"),
                fields: vec![
                    ColumnSpecification::with_constraints(
                        Column::from("comments.id"),
                        SqlType::UnsignedInt(32),
                        vec![
                            ColumnConstraint::NotNull,
                            ColumnConstraint::AutoIncrement,
                            ColumnConstraint::PrimaryKey,
                        ],
                    ),
                    ColumnSpecification::new(Column::from("comments.hat_id"), SqlType::Int(32),),
                ],
                keys: Some(vec![
                    TableKey::FulltextKey(
                        Some("index_comments_on_comment".into()),
                        vec![Column::from("comments.comment")]
                    ),
                    TableKey::Key(
                        "confidence_idx".into(),
                        vec![Column::from("comments.confidence")]
                    ),
                    TableKey::UniqueKey(
                        Some("short_id".into()),
                        vec![Column::from("comments.short_id")]
                    ),
                    TableKey::Key(
                        "story_id_short_id".into(),
                        vec![
                            Column::from("comments.story_id"),
                            Column::from("comments.short_id")
                        ]
                    ),
                    TableKey::Key("thread_id".into(), vec![Column::from("comments.thread_id")]),
                    TableKey::Key(
                        "index_comments_on_user_id".into(),
                        vec![Column::from("comments.user_id")]
                    ),
                ]),
            }
        );
    }
}
