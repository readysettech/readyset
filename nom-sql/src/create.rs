use std::str;
use std::str::FromStr;

use nom::branch::alt;
use nom::bytes::complete::{is_not, tag, tag_no_case};
use nom::character::complete::digit1;
use nom::combinator::{map, map_res, opt};
use nom::error::{ErrorKind, ParseError};
use nom::multi::{separated_list0, separated_list1};
use nom::sequence::{delimited, preceded, separated_pair, terminated, tuple};
use nom::{Compare, CompareResult};
use nom_locate::LocatedSpan;
use readyset_sql::{ast::*, Dialect};

use crate::column::column_specification;
use crate::common::{
    charset_name, collation_name, column_identifier_no_alias, debug_print, if_not_exists,
    parse_fallible, statement_terminator, until_statement_terminator, ws_sep_comma,
};
use crate::compound_select::nested_compound_selection;
use crate::create_table_options::{
    create_option_equals_pair, create_option_spaced_pair, table_options,
};
use crate::dialect::DialectParser;
use crate::expression::expression;
use crate::order::order_type;
use crate::select::selection;
use crate::table::relation;
use crate::whitespace::{whitespace0, whitespace1};
use crate::{NomSqlError, NomSqlResult};

// MySQL grammar element for index column definition (§13.1.18, index_col_name)
#[allow(clippy::type_complexity)]
pub fn index_col_name(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], (Column, Option<u16>, Option<OrderType>)> {
    move |i| {
        let (remaining_input, (column, len_u8, order)) = tuple((
            terminated(column_identifier_no_alias(dialect), whitespace0),
            opt(delimited(
                tag("("),
                map_res(
                    map_res(digit1, |i: LocatedSpan<&[u8]>| str::from_utf8(&i)),
                    u16::from_str,
                ),
                tag(")"),
            )),
            opt(order_type),
        ))(i)?;

        Ok((remaining_input, (column, len_u8, order)))
    }
}

// Helper for list of index columns
pub fn index_col_list(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], Vec<Column>> {
    move |i| {
        separated_list0(
            ws_sep_comma,
            map(
                index_col_name(dialect),
                // XXX(malte): ignores length and order
                |e| e.0,
            ),
        )(i)
    }
}

// Parse rule for an individual key specification.
pub fn key_specification(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], TableKey> {
    move |i| {
        debug_print("before key_specification", &i);
        let (i, table_key) = alt((
            check_constraint(dialect),
            full_text_key(dialect),
            primary_key(dialect),
            unique(dialect),
            key_or_index(dialect),
            foreign_key(dialect),
        ))(i)?;
        debug_print("after key_specification", &i);
        Ok((i, table_key))
    }
}

fn full_text_key(dialect: Dialect) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], TableKey> {
    move |i| {
        debug_print("before full_text_key", &i);
        let (remaining_input, (_, _, _, _, index_name, _, columns)) = tuple((
            tag_no_case("fulltext"),
            whitespace1,
            alt((tag_no_case("key"), tag_no_case("index"))),
            whitespace1,
            opt(dialect.identifier()),
            whitespace0,
            delimited(
                tag("("),
                delimited(whitespace0, index_col_list(dialect), whitespace0),
                tag(")"),
            ),
        ))(i)?;

        debug_print("after full_text_key", &remaining_input);
        Ok((
            remaining_input,
            TableKey::FulltextKey {
                index_name,
                columns,
            },
        ))
    }
}

fn primary_key(dialect: Dialect) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], TableKey> {
    move |i| {
        debug_print("before primary_key", &i);
        let (i, constraint_name) = constraint_identifier(dialect)(i)?;
        let (i, _) = whitespace0(i)?;
        let (i, (_, index_name, _, columns, _, constraint_timing)) = tuple((
            tag_no_case("primary key"),
            opt(preceded(whitespace1, dialect.identifier())),
            whitespace0,
            delimited(
                tag("("),
                delimited(whitespace0, index_col_list(dialect), whitespace0),
                tag(")"),
            ),
            opt(map(
                preceded(whitespace1, tag_no_case("auto_increment")),
                |_| (),
            )),
            opt(deferrable(dialect, true)),
        ))(i)?;
        let constraint_timing = constraint_timing.unwrap_or(None);
        let (i, _index_type) = opt(using_index)(i)?;

        debug_print("after primary_key", &i);
        Ok((
            i,
            TableKey::PrimaryKey {
                constraint_name,
                constraint_timing,
                index_name,
                columns,
            },
        ))
    }
}

fn referential_action(i: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], ReferentialAction> {
    alt((
        map(tag_no_case("cascade"), |_| ReferentialAction::Cascade),
        map(
            tuple((tag_no_case("set"), whitespace1, tag_no_case("null"))),
            |_| ReferentialAction::SetNull,
        ),
        map(tag_no_case("restrict"), |_| ReferentialAction::Restrict),
        map(
            tuple((tag_no_case("no"), whitespace1, tag_no_case("action"))),
            |_| ReferentialAction::NoAction,
        ),
        map(
            tuple((tag_no_case("set"), whitespace1, tag_no_case("default"))),
            |_| ReferentialAction::SetDefault,
        ),
    ))(i)
}

fn on_delete(i: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], ReferentialAction> {
    let (i, _) = whitespace0(i)?;
    let (i, _) = tag_no_case("on")(i)?;
    let (i, _) = whitespace1(i)?;
    let (i, _) = tag_no_case("delete")(i)?;
    let (i, _) = whitespace1(i)?;
    referential_action(i)
}

fn on_update(i: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], ReferentialAction> {
    let (i, _) = whitespace0(i)?;
    let (i, _) = tag_no_case("on")(i)?;
    let (i, _) = whitespace1(i)?;
    let (i, _) = tag_no_case("update")(i)?;
    let (i, _) = whitespace1(i)?;
    referential_action(i)
}

fn constraint_identifier(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], Option<SqlIdentifier>> {
    // [CONSTRAINT [identifier]]
    move |i| {
        debug_print("before constraint_identifier: ", &i);
        let (i, name) = map(
            opt(preceded(
                tuple((tag_no_case("constraint"), whitespace1)),
                opt(dialect.identifier()),
            )),
            |n| n.flatten(),
        )(i)?;

        debug_print("after constraint_identifier: ", &i);

        Ok((i, name))
    }
}

fn foreign_key(dialect: Dialect) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], TableKey> {
    move |i| {
        debug_print("before foreign_key", &i);
        let (i, constraint_name) = constraint_identifier(dialect)(i)?;

        // FOREIGN KEY identifier
        let (i, _) = whitespace0(i)?;
        let (i, _) = tag_no_case("foreign")(i)?;
        let (i, _) = whitespace0(i)?;
        let (i, _) = tag_no_case("key")(i)?;
        let (i, _) = whitespace1(i)?;

        // (index_name)
        let (i, index_name) = opt(terminated(dialect.identifier(), whitespace1))(i)?;

        // (columns)
        let (i, _) = tag("(")(i)?;
        let (i, _) = whitespace0(i)?;
        let (i, columns) = separated_list0(ws_sep_comma, column_identifier_no_alias(dialect))(i)?;
        let (i, _) = whitespace0(i)?;
        let (i, _) = tag(")")(i)?;

        // REFERENCES
        let (i, _) = whitespace1(i)?;
        let (i, _) = tag_no_case("references")(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, target_table) = relation(dialect)(i)?;

        // (columns)
        let (i, _) = whitespace0(i)?;
        let (i, _) = tag("(")(i)?;
        let (i, target_columns) =
            separated_list0(ws_sep_comma, column_identifier_no_alias(dialect))(i)?;
        let (i, _) = tag(")")(i)?;

        // ON DELETE & ON UPDATE
        let (i, on_modified) = opt(alt((
            move |i| {
                let (i, on_delete) = on_delete(i)?;
                let (i, on_update) = on_update(i)?;
                Ok((i, (Some(on_delete), Some(on_update))))
            },
            move |i| {
                let (i, on_delete) = on_delete(i)?;
                Ok((i, (Some(on_delete), None)))
            },
            move |i| {
                let (i, on_update) = on_update(i)?;
                let (i, on_delete) = on_delete(i)?;
                Ok((i, (Some(on_delete), Some(on_update))))
            },
            move |i| {
                let (i, on_update) = on_update(i)?;
                Ok((i, (None, Some(on_update))))
            },
        )))(i)?;
        let (on_delete, on_update) = match on_modified {
            Some((on_delete, on_update)) => (on_delete, on_update),
            None => (None, None),
        };
        debug_print("after foreign_key", &i);

        Ok((
            i,
            TableKey::ForeignKey {
                constraint_name,
                index_name,
                columns,
                target_table,
                target_columns,
                on_delete,
                on_update,
            },
        ))
    }
}

fn nulls_distinct(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], Option<NullsDistinct>> {
    move |i| {
        if dialect != Dialect::PostgreSQL {
            return Ok((i, None));
        }
        alt((
            move |i| {
                let (i, _) = tag_no_case("nulls")(i)?;
                let (i, _) = whitespace1(i)?;
                let (i, _) = tag_no_case("distinct")(i)?;
                Ok((i, Some(NullsDistinct::Distinct)))
            },
            move |i| {
                let (i, _) = tag_no_case("nulls")(i)?;
                let (i, _) = whitespace1(i)?;
                let (i, _) = tag_no_case("not")(i)?;
                let (i, _) = whitespace1(i)?;
                let (i, _) = tag_no_case("distinct")(i)?;
                Ok((i, Some(NullsDistinct::NotDistinct)))
            },
        ))(i)
    }
}

fn deferrable(
    dialect: Dialect,
    require_whitespace: bool,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], Option<ConstraintTiming>> {
    move |i| {
        if dialect != Dialect::PostgreSQL {
            return Ok((i, None));
        }
        let (i, _) = if require_whitespace {
            whitespace1(i)?
        } else {
            whitespace0(i)?
        };
        alt((
            move |i| {
                let (i, _) = tag_no_case("not")(i)?;
                let (i, _) = whitespace1(i)?;
                let (i, _) = tag_no_case("deferrable")(i)?;
                let (i, _) = whitespace1(i)?;
                let (i, _) = tag_no_case("initially")(i)?;
                let (i, _) = whitespace1(i)?;
                let (i, _) = tag_no_case("immediate")(i)?;
                Ok((i, Some(ConstraintTiming::NotDeferrableInitiallyImmediate)))
            },
            move |i| {
                let (i, _) = tag_no_case("not")(i)?;
                let (i, _) = whitespace1(i)?;
                let (i, _) = tag_no_case("deferrable")(i)?;
                Ok((i, Some(ConstraintTiming::NotDeferrable)))
            },
            move |i| {
                let (i, _) = tag_no_case("deferrable")(i)?;
                let (i, _) = whitespace1(i)?;
                let (i, _) = tag_no_case("initially")(i)?;
                let (i, _) = whitespace1(i)?;
                let (i, _) = tag_no_case("deferred")(i)?;
                Ok((i, Some(ConstraintTiming::DeferrableInitiallyDeferred)))
            },
            move |i| {
                let (i, _) = tag_no_case("deferrable")(i)?;
                let (i, _) = whitespace1(i)?;
                let (i, _) = tag_no_case("initially")(i)?;
                let (i, _) = whitespace1(i)?;
                let (i, _) = tag_no_case("immediate")(i)?;
                Ok((i, Some(ConstraintTiming::DeferrableInitiallyImmediate)))
            },
            move |i| {
                let (i, _) = tag_no_case("deferrable")(i)?;
                Ok((i, Some(ConstraintTiming::Deferrable)))
            },
        ))(i)
    }
}

fn index_type(i: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], IndexType> {
    alt((
        map(tag_no_case("btree"), |_| IndexType::BTree),
        map(tag_no_case("hash"), |_| IndexType::Hash),
    ))(i)
}

fn using_index(i: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], IndexType> {
    let (i, _) = whitespace1(i)?;
    let (i, _) = tag_no_case("using")(i)?;
    let (i, _) = whitespace1(i)?;
    index_type(i)
}

fn unique(dialect: Dialect) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], TableKey> {
    move |i| {
        debug_print("before unique", &i);
        let (i, constraint_name) = constraint_identifier(dialect)(i)?;
        let (i, _) = whitespace0(i)?;
        let (i, _) = tag_no_case("unique")(i)?;
        let (i, _) = if dialect == Dialect::MySQL {
            opt(preceded(
                whitespace1,
                alt((tag_no_case("key"), tag_no_case("index"))),
            ))(i)?
        } else {
            (i, None)
        };
        let (i, index_name) = if dialect == Dialect::MySQL {
            opt(preceded(whitespace1, dialect.identifier()))(i)?
        } else {
            (i, None)
        };
        let (i, nulls_distinct) = opt(preceded(whitespace1, nulls_distinct(dialect)))(i)?;
        let nulls_distinct = nulls_distinct.unwrap_or(None);
        let (i, columns) = preceded(
            whitespace0,
            delimited(
                tag("("),
                delimited(whitespace0, index_col_list(dialect), whitespace0),
                tag(")"),
            ),
        )(i)?;
        let (i, constraint_timing) = opt(deferrable(dialect, false))(i)?;
        let constraint_timing = constraint_timing.unwrap_or(None);
        let (i, index_type) = opt(using_index)(i)?;
        debug_print("after unique", &i);

        Ok((
            i,
            TableKey::UniqueKey {
                constraint_name,
                constraint_timing,
                index_name,
                columns,
                index_type,
                nulls_distinct,
            },
        ))
    }
}

fn key_or_index(dialect: Dialect) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], TableKey> {
    move |i| {
        debug_print("before key_or_index", &i);

        let (i, _) = whitespace0(i)?;
        let (i, _) = alt((tag_no_case("key"), tag_no_case("index")))(i)?;
        let (i, index_name) = opt(preceded(whitespace1, dialect.identifier()))(i)?;
        let (i, _) = whitespace0(i)?;
        let (i, columns) = delimited(
            tag("("),
            delimited(whitespace0, index_col_list(dialect), whitespace0),
            tag(")"),
        )(i)?;
        let (i, index_type) = opt(using_index)(i)?;

        debug_print("after key_or_index", &i);
        Ok((
            i,
            TableKey::Key {
                index_name,
                columns,
                index_type,
            },
        ))
    }
}

fn check_constraint(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], TableKey> {
    move |i| {
        debug_print("before check_constraint", &i);
        let (i, constraint_name) = constraint_identifier(dialect)(i)?;
        let (i, _) = whitespace0(i)?;
        let (i, _) = tag_no_case("check")(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, expr) = delimited(
            terminated(tag("("), whitespace0),
            expression(dialect),
            preceded(whitespace0, tag(")")),
        )(i)?;
        let (i, enforced) = opt(preceded(
            whitespace1,
            terminated(
                map(opt(terminated(tag_no_case("not"), whitespace1)), |n| {
                    n.is_none()
                }),
                tag_no_case("enforced"),
            ),
        ))(i)?;

        let res = Ok((
            i,
            TableKey::CheckConstraint {
                constraint_name,
                expr,
                enforced,
            },
        ));
        debug_print("after check_constraint", &i);
        res
    }
}

// Parse rule for a comma-separated list.
pub fn key_specification_list(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], Vec<TableKey>> {
    move |i| separated_list1(ws_sep_comma, key_specification(dialect))(i)
}

// Parse rule for a comma-separated list of fields.
pub fn field_specification_list(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], Vec<ColumnSpecification>> {
    move |i| separated_list1(ws_sep_comma, column_specification(dialect))(i)
}

fn create_table_body(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], CreateTableBody> {
    move |i| {
        let (i, _) = whitespace0(i)?;
        let (i, fields) = field_specification_list(dialect)(i)?;
        let (i, _) = whitespace0(i)?;
        let (i, keys) = opt(preceded(ws_sep_comma, key_specification_list(dialect)))(i)?;
        let (i, _) = whitespace0(i)?;

        Ok((i, CreateTableBody { fields, keys }))
    }
}

/// Parse rule for a SQL CREATE TABLE query.
pub fn create_table(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], CreateTableStatement> {
    move |i| {
        let (i, _) = tag_no_case("create")(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, _) = tag_no_case("table")(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, if_not_exists) = if_not_exists(i)?;
        let (i, table) = relation(dialect)(i)?;
        let (i, _) = whitespace0(i)?;

        let (i, _) = tag("(")(i)?;
        let (i, body) = parse_fallible(
            create_table_body(dialect),
            map(is_not(")"), |r: LocatedSpan<&[u8]>| *r),
        )(i)?;
        let (i, _) = tag(")")(i)?;
        let (i, _) = whitespace0(i)?;
        let (i, options) = parse_fallible(table_options(dialect), until_statement_terminator)(i)?;
        let (i, _) = statement_terminator(i)?;

        let mut create_table = CreateTableStatement {
            if_not_exists,
            table,
            body,
            options,
        };
        create_table.propagate_default_charset(dialect);
        Ok((i, create_table))
    }
}

fn is_default(i: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], bool> {
    let (i, s) = opt(map(tuple((tag_no_case("default"), whitespace1)), |_| ()))(i)?;
    Ok((i, s.is_some()))
}

fn yes_no_value(i: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], bool> {
    map(
        alt((tag_no_case("y"), tag_no_case("n"))),
        |i: LocatedSpan<&[u8]>| *i == b"y" || *i == b"Y",
    )(i)
}

fn charset_attribute_name(i: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], ()> {
    map(
        separated_pair(tag_no_case("character"), whitespace1, tag_no_case("set")),
        |_| (),
    )(i)
}

fn charset_option(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], CreateDatabaseOption> {
    move |i| {
        map(
            alt((
                create_option_spaced_pair(charset_attribute_name, charset_name(dialect)),
                create_option_equals_pair(charset_attribute_name, charset_name(dialect)),
            )),
            |name| CreateDatabaseOption::CharsetName {
                default: false,
                name,
            },
        )(i)
    }
}

fn collate_option(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], CreateDatabaseOption> {
    move |i| {
        map(
            alt((
                create_option_spaced_pair(tag_no_case("collate"), collation_name(dialect)),
                create_option_equals_pair(tag_no_case("collate"), collation_name(dialect)),
            )),
            |name| CreateDatabaseOption::CollationName {
                default: false,
                name,
            },
        )(i)
    }
}

fn encryption_option(i: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], CreateDatabaseOption> {
    map(
        alt((
            create_option_spaced_pair(tag_no_case("encryption"), yes_no_value),
            create_option_equals_pair(tag_no_case("encryption"), yes_no_value),
        )),
        |encrypted| CreateDatabaseOption::Encryption {
            default: false,
            encrypted,
        },
    )(i)
}

fn database_option(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], CreateDatabaseOption> {
    move |i| {
        let (i, _) = whitespace0(i)?;
        let (i, is_default) = is_default(i)?;
        let (i, option) = alt((
            charset_option(dialect),
            collate_option(dialect),
            encryption_option,
        ))(i)?;
        Ok((
            i,
            match option {
                CreateDatabaseOption::CharsetName { default: _, name } => {
                    CreateDatabaseOption::CharsetName {
                        default: is_default,
                        name,
                    }
                }
                CreateDatabaseOption::CollationName { default: _, name } => {
                    CreateDatabaseOption::CollationName {
                        default: is_default,
                        name,
                    }
                }
                CreateDatabaseOption::Encryption {
                    default: _,
                    encrypted,
                } => CreateDatabaseOption::Encryption {
                    default: is_default,
                    encrypted,
                },
            },
        ))
    }
}

fn database_options(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], Vec<CreateDatabaseOption>> {
    move |i| separated_list0(whitespace1, database_option(dialect))(i)
}

/// Parse rule for a SQL CREATE DATABASE statement.
pub fn create_database(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], CreateDatabaseStatement> {
    move |i| {
        if dialect != Dialect::MySQL {
            return Err(nom::Err::Error(NomSqlError {
                input: i,
                kind: ErrorKind::Fail,
            }));
        }
        let (i, _) = tag_no_case("create")(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, what) = map_res(
            alt((tag_no_case("database"), tag_no_case("schema"))),
            |i: LocatedSpan<&[u8]>| str::from_utf8(&i),
        )(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, if_not_exists) = if_not_exists(i)?;
        let (i, name) = dialect.identifier()(i)?;
        let (i, options) =
            parse_fallible(database_options(dialect), until_statement_terminator)(i)?;
        let (i, _) = statement_terminator(i)?;

        Ok((
            i,
            CreateDatabaseStatement {
                is_schema: what.compare_no_case("schema") == CompareResult::Ok,
                if_not_exists,
                name,
                options,
            },
        ))
    }
}

// Parse the optional CREATE VIEW parameters and discard, ideally we would want to check user
// permissions
pub fn create_view_params(i: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], ()> {
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
                whitespace0,
                tag("="),
                whitespace0,
                alt((
                    tag_no_case("UNDEFINED"),
                    tag_no_case("MERGE"),
                    tag_no_case("TEMPTABLE"),
                )),
                whitespace1,
            ))),
            opt(tuple((
                tag_no_case("DEFINER"),
                whitespace0,
                tag("="),
                whitespace0,
                delimited(tag("`"), is_not("`"), tag("`")),
                tag("@"),
                delimited(tag("`"), is_not("`"), tag("`")),
                whitespace1,
            ))),
            opt(tuple((
                tag_no_case("SQL"),
                whitespace1,
                tag_no_case("SECURITY"),
                whitespace1,
                alt((tag_no_case("DEFINER"), tag_no_case("INVOKER"))),
                whitespace1,
            ))),
        )),
        |_| (),
    )(i)
}

fn or_replace(i: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], ()> {
    let (i, _) = tag_no_case("or")(i)?;
    let (i, _) = whitespace1(i)?;
    let (i, _) = tag_no_case("replace")(i)?;
    Ok((i, ()))
}

// Parse rule for a SQL CREATE VIEW query.
pub fn view_creation(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], CreateViewStatement> {
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
    // CREATE ALGORITHM=UNDEFINED DEFINER=`mysqluser`@`%` SQL SECURITY DEFINER VIEW `myquery2` AS
    // SELECT * FROM employees

    move |i| {
        let (i, _) = tag_no_case("create")(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, or_replace) = map(opt(terminated(or_replace, whitespace1)), |or| or.is_some())(i)?;
        let (i, _) = opt(create_view_params)(i)?;
        let (i, _) = tag_no_case("view")(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, name) = relation(dialect)(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, _) = tag_no_case("as")(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, definition) = parse_fallible(
            map(nested_compound_selection(dialect), Box::new),
            until_statement_terminator,
        )(i)?;
        let (i, _) = statement_terminator(i)?;

        let fields = vec![]; // TODO(malte): support
        Ok((
            i,
            CreateViewStatement {
                name,
                or_replace,
                fields,
                definition,
            },
        ))
    }
}

/// Extract the [`CreateCacheOption`] from a `CREATE CACHE statement.
fn cached_query_options(mut i: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], CreateCacheOptions> {
    // Create an error given the position
    fn error(i: LocatedSpan<&[u8]>) -> nom::Err<NomSqlError<&[u8]>> {
        nom::Err::Failure(NomSqlError::from_error_kind(i, ErrorKind::Permutation))
    }

    // A CREATE CACHE optional argument. Used to avoid string matching
    enum Option {
        Always,
        Concurrently,
    }

    let mut opts = CreateCacheOptions::default();

    // Parse a subset of the options in any order. Ignore errors since all options are optional.
    while let Ok((remaining, opt)) = alt((
        map(tuple((tag_no_case("always"), whitespace1)), |_| {
            Option::Always
        }),
        map(tuple((tag_no_case("concurrently"), whitespace1)), |_| {
            Option::Concurrently
        }),
    ))(i)
    {
        // Error if the same option appears twice.
        match opt {
            Option::Always => {
                if std::mem::replace(&mut opts.always, true) {
                    return Err(error(i));
                }
            }
            Option::Concurrently => {
                if std::mem::replace(&mut opts.concurrently, true) {
                    return Err(error(i));
                }
            }
        }
        i = remaining;
    }
    Ok((i, opts))
}

/// Extract the [`SelectStatement`] or Query ID from a CREATE CACHE statement. Query ID is
/// parsed as a SqlIdentifier
pub fn cached_query_inner(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], CacheInner> {
    move |i| {
        alt((
            map(map(selection(dialect), Box::new), CacheInner::from),
            map(dialect.identifier(), CacheInner::from),
        ))(i)
    }
}

/// Parse a [`CreateCacheStatement`]
pub fn create_cached_query(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], CreateCacheStatement> {
    move |i| {
        let unparsed_create_cache_statement = Some(String::from_utf8_lossy(*i).into());
        let (i, _) = tag_no_case("create")(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, _) = tag_no_case("cache")(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, opts) = cached_query_options(i)?;
        let (i, name) = opt(terminated(relation(dialect), whitespace1))(i)?;
        let (i, _) = tag_no_case("from")(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, inner) =
            parse_fallible(cached_query_inner(dialect), until_statement_terminator)(i)?;
        Ok((
            i,
            CreateCacheStatement {
                name,
                inner,
                unparsed_create_cache_statement,
                always: opts.always,
                concurrently: opts.concurrently,
            },
        ))
    }
}

#[cfg(test)]
mod tests {
    use bit_vec::BitVec;
    use readyset_sql::DialectDisplay;

    use super::*;

    #[test]
    fn field_spec() {
        // N.B. trailing comma here because field_specification_list! doesn't handle the eof case
        // because it is never validly the end of a query
        let qstring = "id bigint(20), name varchar(255),";

        let res = field_specification_list(Dialect::MySQL)(LocatedSpan::new(qstring.as_bytes()));
        assert_eq!(
            res.unwrap().1,
            vec![
                ColumnSpecification::new(Column::from("id"), SqlType::BigInt(Some(20))),
                ColumnSpecification::new(Column::from("name"), SqlType::VarChar(Some(255))),
            ]
        );
    }

    #[test]
    fn simple_create() {
        let qstring = "CREATE TABLE if Not  ExistS users (id bigint(20), name varchar(255), email varchar(255));";

        let res = create_table(Dialect::MySQL)(LocatedSpan::new(qstring.as_bytes()));
        assert_eq!(
            res.unwrap().1,
            CreateTableStatement {
                if_not_exists: true,
                table: Relation::from("users"),
                body: Ok(CreateTableBody {
                    fields: vec![
                        ColumnSpecification::new(Column::from("id"), SqlType::BigInt(Some(20))),
                        ColumnSpecification::new(Column::from("name"), SqlType::VarChar(Some(255))),
                        ColumnSpecification::new(
                            Column::from("email"),
                            SqlType::VarChar(Some(255))
                        ),
                    ],
                    keys: None,
                }),
                options: Ok(vec![])
            }
        );
    }

    #[test]
    fn create_without_space_after_tablename() {
        let qstring = "CREATE TABLE t(x integer);";
        let res = create_table(Dialect::MySQL)(LocatedSpan::new(qstring.as_bytes()));
        assert_eq!(
            res.unwrap().1,
            CreateTableStatement {
                if_not_exists: false,
                table: Relation::from("t"),
                body: Ok(CreateTableBody {
                    fields: vec![ColumnSpecification::new(
                        Column::from("x"),
                        SqlType::Int(None)
                    ),],
                    keys: None,
                }),
                options: Ok(vec![])
            }
        );
    }

    #[test]
    fn create_tablename_with_schema() {
        let qstring = "CREATE TABLE db1.t(x integer);";
        let res = create_table(Dialect::MySQL)(LocatedSpan::new(qstring.as_bytes()));
        assert_eq!(
            res.unwrap().1,
            CreateTableStatement {
                if_not_exists: false,
                table: Relation {
                    schema: Some("db1".into()),
                    name: "t".into(),
                },
                body: Ok(CreateTableBody {
                    fields: vec![ColumnSpecification::new(
                        Column::from("x"),
                        SqlType::Int(None)
                    ),],
                    keys: None,
                }),
                options: Ok(vec![])
            }
        );
    }

    #[test]
    fn keys() {
        // simple primary key
        let qstring = "CREATE TABLE users (id bigint(20), name varchar(255), email varchar(255), \
                       PRIMARY KEY (id));";

        let res = create_table(Dialect::MySQL)(LocatedSpan::new(qstring.as_bytes()));
        assert_eq!(
            res.unwrap().1,
            CreateTableStatement {
                if_not_exists: false,
                table: Relation::from("users"),
                body: Ok(CreateTableBody {
                    fields: vec![
                        ColumnSpecification::new(Column::from("id"), SqlType::BigInt(Some(20))),
                        ColumnSpecification::new(Column::from("name"), SqlType::VarChar(Some(255))),
                        ColumnSpecification::new(
                            Column::from("email"),
                            SqlType::VarChar(Some(255))
                        ),
                    ],
                    keys: Some(vec![TableKey::PrimaryKey {
                        constraint_name: None,
                        constraint_timing: None,
                        index_name: None,
                        columns: vec![Column::from("id")]
                    }]),
                }),
                options: Ok(vec![])
            }
        );

        // named unique key
        let qstring = "CREATE TABLE users (id bigint(20), name varchar(255), email varchar(255), \
                       UNIQUE KEY id_k (id));";

        let res = create_table(Dialect::MySQL)(LocatedSpan::new(qstring.as_bytes()));
        assert_eq!(
            res.unwrap().1,
            CreateTableStatement {
                if_not_exists: false,
                table: Relation::from("users"),
                body: Ok(CreateTableBody {
                    fields: vec![
                        ColumnSpecification::new(Column::from("id"), SqlType::BigInt(Some(20))),
                        ColumnSpecification::new(Column::from("name"), SqlType::VarChar(Some(255))),
                        ColumnSpecification::new(
                            Column::from("email"),
                            SqlType::VarChar(Some(255))
                        ),
                    ],
                    keys: Some(vec![TableKey::UniqueKey {
                        constraint_name: None,
                        constraint_timing: None,
                        index_name: Some("id_k".into()),
                        columns: vec![Column::from("id")],
                        index_type: None,
                        nulls_distinct: None,
                    },]),
                }),
                options: Ok(vec![])
            }
        );
    }

    #[test]
    fn compound_create_view() {
        let qstring = "CREATE VIEW v AS SELECT * FROM users UNION SELECT * FROM old_users;";

        let res = view_creation(Dialect::MySQL)(LocatedSpan::new(qstring.as_bytes()));
        assert_eq!(
            res.unwrap().1,
            CreateViewStatement {
                name: "v".into(),
                or_replace: false,
                fields: vec![],
                definition: Ok(Box::new(SelectSpecification::Compound(
                    CompoundSelectStatement {
                        selects: vec![
                            (
                                None,
                                SelectStatement {
                                    tables: vec![TableExpr::from(Relation::from("users"))],
                                    fields: vec![FieldDefinitionExpr::All],
                                    ..Default::default()
                                },
                            ),
                            (
                                Some(CompoundSelectOperator::UnionDistinct),
                                SelectStatement {
                                    tables: vec![TableExpr::from(Relation::from("old_users"))],
                                    fields: vec![FieldDefinitionExpr::All],
                                    ..Default::default()
                                },
                            ),
                        ],
                        order: None,
                        limit_clause: LimitClause::LimitOffset {
                            limit: None,
                            offset: None
                        },
                    }
                ))),
            }
        );
    }

    #[test]
    fn foreign_key() {
        let qstring = b"CREATE TABLE users (
          id int,
          group_id int,
          primary key (id),
          constraint users_group foreign key (group_id) references `groups` (id)
        ) AUTO_INCREMENT=1000";

        let (rem, res) = create_table(Dialect::MySQL)(LocatedSpan::new(qstring)).unwrap();
        assert!(rem.is_empty());
        assert_eq!(
            res,
            CreateTableStatement {
                if_not_exists: false,
                table: "users".into(),
                body: Ok(CreateTableBody {
                    fields: vec![
                        ColumnSpecification::new("id".into(), SqlType::Int(None),),
                        ColumnSpecification::new("group_id".into(), SqlType::Int(None),),
                    ],
                    keys: Some(vec![
                        TableKey::PrimaryKey {
                            constraint_name: None,
                            constraint_timing: None,
                            index_name: None,
                            columns: vec!["id".into()],
                        },
                        TableKey::ForeignKey {
                            constraint_name: Some("users_group".into()),
                            columns: vec!["group_id".into()],
                            target_table: "groups".into(),
                            target_columns: vec!["id".into()],
                            index_name: None,
                            on_delete: None,
                            on_update: None,
                        }
                    ]),
                }),
                options: Ok(vec![CreateTableOption::AutoIncrement(1000)],)
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

        let (rem, res) = create_table(Dialect::MySQL)(LocatedSpan::new(qstring)).unwrap();
        assert!(rem.is_empty());
        let non_null_col = |n: &str, t: SqlType| {
            ColumnSpecification::with_constraints(n.into(), t, vec![ColumnConstraint::NotNull])
        };

        assert_eq!(
            res,
            CreateTableStatement {
                if_not_exists: false,
                table: "addresses".into(),
                body: Ok(CreateTableBody {
                    fields: vec![
                        ColumnSpecification::with_constraints(
                            "id".into(),
                            SqlType::Int(None),
                            vec![
                                ColumnConstraint::NotNull,
                                ColumnConstraint::AutoIncrement,
                                ColumnConstraint::PrimaryKey,
                            ]
                        ),
                        non_null_col("customer_id", SqlType::Int(None)),
                        non_null_col("street", SqlType::VarChar(Some(255))),
                        non_null_col("city", SqlType::VarChar(Some(255))),
                        non_null_col("state", SqlType::VarChar(Some(255))),
                        non_null_col("zip", SqlType::VarChar(Some(255))),
                        non_null_col(
                            "type",
                            SqlType::from_enum_variants([
                                "SHIPPING".into(),
                                "BILLING".into(),
                                "LIVING".into(),
                            ]),
                        ),
                    ],
                    keys: Some(vec![TableKey::ForeignKey {
                        constraint_name: None,
                        columns: vec!["customer_id".into()],
                        target_table: "customers".into(),
                        target_columns: vec!["id".into()],
                        index_name: None,
                        on_delete: None,
                        on_update: None,
                    },]),
                }),
                options: Ok(vec![CreateTableOption::AutoIncrement(10)],)
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

        let (rem, res) = create_table(Dialect::MySQL)(LocatedSpan::new(qstring)).unwrap();
        assert!(rem.is_empty());

        assert_eq!(
            res,
            CreateTableStatement {
                if_not_exists: false,
                table: "orders".into(),
                body: Ok(CreateTableBody {
                    fields: vec![
                        ColumnSpecification::with_constraints(
                            "order_number".into(),
                            SqlType::Int(None),
                            vec![
                                ColumnConstraint::NotNull,
                                ColumnConstraint::AutoIncrement,
                                ColumnConstraint::PrimaryKey,
                            ]
                        ),
                        ColumnSpecification::with_constraints(
                            "purchaser".into(),
                            SqlType::Int(None),
                            vec![ColumnConstraint::NotNull]
                        ),
                        ColumnSpecification::with_constraints(
                            "product_id".into(),
                            SqlType::Int(None),
                            vec![ColumnConstraint::NotNull]
                        ),
                    ],
                    keys: Some(vec![
                        TableKey::ForeignKey {
                            constraint_name: None,
                            columns: vec!["purchaser".into()],
                            target_table: "customers".into(),
                            target_columns: vec!["id".into()],
                            index_name: Some("order_customer".into()),
                            on_delete: None,
                            on_update: None,
                        },
                        TableKey::ForeignKey {
                            constraint_name: None,
                            columns: vec!["product_id".into()],
                            target_table: "products".into(),
                            target_columns: vec!["id".into()],
                            index_name: Some("ordered_product".into()),
                            on_delete: None,
                            on_update: None,
                        },
                    ]),
                }),
                options: Ok(vec![],)
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

        let (rem, res) = create_table(Dialect::MySQL)(LocatedSpan::new(qstring)).unwrap();
        assert!(rem.is_empty());

        assert_eq!(
            res,
            CreateTableStatement {
                if_not_exists: false,
                table: "customers".into(),
                body: Ok(CreateTableBody {
                    fields: vec![
                        ColumnSpecification::with_constraints(
                            "id".into(),
                            SqlType::Int(None),
                            vec![
                                ColumnConstraint::NotNull,
                                ColumnConstraint::AutoIncrement,
                                ColumnConstraint::PrimaryKey,
                            ]
                        ),
                        ColumnSpecification::with_constraints(
                            "last_name".into(),
                            SqlType::VarChar(Some(255)),
                            vec![ColumnConstraint::NotNull, ColumnConstraint::Unique,]
                        ),
                        ColumnSpecification::with_constraints(
                            "email".into(),
                            SqlType::VarChar(Some(255)),
                            vec![ColumnConstraint::NotNull, ColumnConstraint::Unique,]
                        ),
                    ],
                    keys: None,
                }),
                options: Ok(vec![CreateTableOption::AutoIncrement(1001)],)
            }
        );
        // index type
        let qstring = "CREATE TABLE users (id bigint(20), name varchar(255), email varchar(255), \
                       UNIQUE KEY id_k (id) USING HASH);";

        let res = create_table(Dialect::MySQL)(LocatedSpan::new(qstring.as_bytes()));
        assert_eq!(
            res.unwrap().1,
            CreateTableStatement {
                if_not_exists: false,
                table: Relation::from("users"),
                body: Ok(CreateTableBody {
                    fields: vec![
                        ColumnSpecification::new(Column::from("id"), SqlType::BigInt(Some(20))),
                        ColumnSpecification::new(Column::from("name"), SqlType::VarChar(Some(255))),
                        ColumnSpecification::new(
                            Column::from("email"),
                            SqlType::VarChar(Some(255))
                        ),
                    ],
                    keys: Some(vec![TableKey::UniqueKey {
                        constraint_name: None,
                        constraint_timing: None,
                        index_name: Some("id_k".into()),
                        columns: vec![Column::from("id")],
                        index_type: Some(IndexType::Hash),
                        nulls_distinct: None,
                    }]),
                }),
                options: Ok(vec![]),
            }
        );
    }

    #[test]
    fn key_with_index_type() {
        let res = test_parse!(
            create_table(Dialect::MySQL),
            b"CREATE TABLE users (
                  age INTEGER,
                  KEY age_key (age) USING BTREE
              )"
        );
        assert_eq!(
            res.body.unwrap().keys,
            Some(vec![TableKey::Key {
                index_name: Some("age_key".into()),
                columns: vec!["age".into()],
                index_type: Some(IndexType::BTree),
            }])
        );
    }

    #[test]
    fn check_constraint_no_name() {
        let qs: &[&[u8]] = &[b"CHECK (x > 1)", b"CONSTRAINT CHECK (x > 1)"];
        for q in qs {
            let res = test_parse!(key_specification(Dialect::PostgreSQL), q);
            assert_eq!(
                res,
                TableKey::CheckConstraint {
                    constraint_name: None,
                    expr: Expr::BinaryOp {
                        lhs: Box::new(Expr::Column("x".into())),
                        op: BinaryOperator::Greater,
                        rhs: Box::new(Expr::Literal(1.into())),
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
                constraint_name: Some("foo".into()),
                expr: Expr::BinaryOp {
                    lhs: Box::new(Expr::Column("x".into())),
                    op: BinaryOperator::Greater,
                    rhs: Box::new(Expr::Literal(1.into())),
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
                constraint_name: Some("foo".into()),
                expr: Expr::BinaryOp {
                    lhs: Box::new(Expr::Column("x".into())),
                    op: BinaryOperator::Greater,
                    rhs: Box::new(Expr::Literal(1.into())),
                },
                enforced: Some(false)
            }
        )
    }

    mod mysql {
        use std::vec;

        use super::*;
        use crate::to_nom_result;

        #[test]
        fn if_not_exists() {
            let res = test_parse!(
                create_table(Dialect::MySQL),
                b"CREATE TABLE IF NOT EXISTS t (x int)"
            );
            assert!(res.if_not_exists);
            let rt = res.display(Dialect::MySQL).to_string();
            assert_eq!(rt, "CREATE TABLE IF NOT EXISTS `t` (`x` INT)");
        }

        #[test]
        fn create_view_with_security_params() {
            let qstring = "CREATE ALGORITHM=UNDEFINED DEFINER=`mysqluser`@`%` SQL SECURITY DEFINER VIEW `myquery2` AS SELECT * FROM employees";
            view_creation(Dialect::MySQL)(LocatedSpan::new(qstring.as_bytes())).unwrap();
        }

        #[test]
        fn double_precision_column() {
            let (rem, res) = to_nom_result(create_table(Dialect::MySQL)(LocatedSpan::new(
                b"create table t(x double precision)",
            )))
            .unwrap();
            assert_eq!(str::from_utf8(rem).unwrap(), "");
            assert_eq!(
                res,
                CreateTableStatement {
                    if_not_exists: false,
                    table: "t".into(),
                    body: Ok(CreateTableBody {
                        fields: vec![ColumnSpecification {
                            column: "x".into(),
                            sql_type: SqlType::Double,
                            generated: None,
                            constraints: vec![],
                            comment: None,
                        }],
                        keys: None,
                    }),
                    options: Ok(vec![],)
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
            let res = create_table(Dialect::MySQL)(LocatedSpan::new(qstring.as_bytes()));
            assert_eq!(
                res.unwrap().1,
                CreateTableStatement {
                    if_not_exists: false,
                    table: Relation::from("django_admin_log"),
                    body: Ok(CreateTableBody {
                        fields: vec![
                            ColumnSpecification::with_constraints(
                                Column::from("id"),
                                SqlType::Int(None),
                                vec![
                                    ColumnConstraint::AutoIncrement,
                                    ColumnConstraint::NotNull,
                                    ColumnConstraint::PrimaryKey,
                                ],
                            ),
                            ColumnSpecification::with_constraints(
                                Column::from("action_time"),
                                SqlType::DateTime(None),
                                vec![ColumnConstraint::NotNull],
                            ),
                            ColumnSpecification::with_constraints(
                                Column::from("user_id"),
                                SqlType::Int(None),
                                vec![ColumnConstraint::NotNull],
                            ),
                            ColumnSpecification::new(
                                Column::from("content_type_id"),
                                SqlType::Int(None),
                            ),
                            ColumnSpecification::new(Column::from("object_id"), SqlType::LongText,),
                            ColumnSpecification::with_constraints(
                                Column::from("object_repr"),
                                SqlType::VarChar(Some(200)),
                                vec![ColumnConstraint::NotNull],
                            ),
                            ColumnSpecification::with_constraints(
                                Column::from("action_flag"),
                                SqlType::SmallIntUnsigned(None),
                                vec![ColumnConstraint::NotNull],
                            ),
                            ColumnSpecification::with_constraints(
                                Column::from("change_message"),
                                SqlType::LongText,
                                vec![ColumnConstraint::NotNull],
                            ),
                        ],
                        keys: None,
                    }),
                    options: Ok(vec![],)
                }
            );

            let qstring = "CREATE TABLE `auth_group` (
                       `id` integer AUTO_INCREMENT NOT NULL PRIMARY KEY,
                       `name` varchar(80) NOT NULL UNIQUE)";
            let res = create_table(Dialect::MySQL)(LocatedSpan::new(qstring.as_bytes()));
            assert_eq!(
                res.unwrap().1,
                CreateTableStatement {
                    if_not_exists: false,
                    table: Relation::from("auth_group"),
                    body: Ok(CreateTableBody {
                        fields: vec![
                            ColumnSpecification::with_constraints(
                                Column::from("id"),
                                SqlType::Int(None),
                                vec![
                                    ColumnConstraint::AutoIncrement,
                                    ColumnConstraint::NotNull,
                                    ColumnConstraint::PrimaryKey,
                                ],
                            ),
                            ColumnSpecification::with_constraints(
                                Column::from("name"),
                                SqlType::VarChar(Some(80)),
                                vec![ColumnConstraint::NotNull, ColumnConstraint::Unique],
                            ),
                        ],
                        keys: None,
                    }),
                    options: Ok(vec![],)
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
                        `id` INT AUTO_INCREMENT NOT NULL PRIMARY KEY, \
                        `name` VARCHAR(80) NOT NULL UNIQUE CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci) \
                        ENGINE=InnoDB, AUTO_INCREMENT=495209, DEFAULT CHARSET=utf8mb4, COLLATE=utf8mb4_unicode_ci";
            let res = create_table(Dialect::MySQL)(LocatedSpan::new(qstring.as_bytes()));
            assert_eq!(res.unwrap().1.display(Dialect::MySQL).to_string(), expected);
        }

        #[test]
        fn simple_create_view() {
            let qstring = "CREATE VIEW v AS SELECT * FROM users WHERE username = \"bob\";";

            let res = view_creation(Dialect::MySQL)(LocatedSpan::new(qstring.as_bytes()));
            assert_eq!(
                res.unwrap().1,
                CreateViewStatement {
                    name: "v".into(),
                    or_replace: false,
                    fields: vec![],
                    definition: Ok(Box::new(SelectSpecification::Simple(SelectStatement {
                        tables: vec![TableExpr::from(Relation::from("users"))],
                        fields: vec![FieldDefinitionExpr::All],
                        where_clause: Some(Expr::BinaryOp {
                            lhs: Box::new(Expr::Column("username".into())),
                            rhs: Box::new(Expr::Literal(Literal::String("bob".into()))),
                            op: BinaryOperator::Equal,
                        }),
                        ..Default::default()
                    }))),
                }
            );
        }

        #[test]
        fn format_create_view() {
            let qstring = "CREATE VIEW `v` AS SELECT * FROM `t`;";
            let expected = "CREATE VIEW `v` AS SELECT * FROM `t`";
            let res = view_creation(Dialect::MySQL)(LocatedSpan::new(qstring.as_bytes()));
            assert_eq!(res.unwrap().1.display(Dialect::MySQL).to_string(), expected);
        }

        #[test]
        fn create_cached_query_with_name() {
            let res = test_parse!(
                create_cached_query(Dialect::MySQL),
                b"CREATE CACHE foo FROM SELECT id FROM users WHERE name = ?"
            );
            assert_eq!(res.name, Some("foo".into()));
            let statement = match res.inner {
                Ok(CacheInner::Statement(s)) => s,
                _ => panic!(),
            };
            assert_eq!(
                statement.tables,
                vec![TableExpr::from(Relation::from("users"))]
            );
        }

        #[test]
        fn create_cached_query_without_name() {
            let res = test_parse!(
                create_cached_query(Dialect::MySQL),
                b"CREATE CACHE FROM SELECT id FROM users WHERE name = ?"
            );
            assert_eq!(res.name, None);
            let statement = match res.inner {
                Ok(CacheInner::Statement(s)) => s,
                _ => panic!(),
            };
            assert_eq!(
                statement.tables,
                vec![TableExpr::from(Relation::from("users"))]
            );
        }

        #[test]
        fn create_cached_query_from_id_with_name() {
            let res = test_parse!(
                create_cached_query(Dialect::MySQL),
                b"CREATE CACHE foo FROM q_0123456789ABCDEF"
            );
            assert_eq!(res.name.unwrap(), Relation::from("foo"));
            let id = match res.inner {
                Ok(CacheInner::Id(s)) => s,
                _ => panic!(),
            };
            assert_eq!(id.as_str(), "q_0123456789ABCDEF")
        }

        #[test]
        fn create_cached_query_from_id_without_name() {
            let res = test_parse!(
                create_cached_query(Dialect::MySQL),
                b"CREATE CACHE FROM q_0123456789ABCDEF"
            );
            assert!(res.name.is_none());
            let id = match res.inner {
                Ok(CacheInner::Id(s)) => s,
                _ => panic!(),
            };
            assert_eq!(id.as_str(), "q_0123456789ABCDEF")
        }

        #[test]
        fn create_cached_query_with_optional_args() {
            let q1 = test_parse!(
                create_cached_query(Dialect::MySQL),
                b"CREATE CACHE CONCURRENTLY ALWAYS FROM SELECT id FROM users WHERE name = ?"
            );
            let q2 = test_parse!(
                create_cached_query(Dialect::MySQL),
                b"CREATE CACHE ALWAYS FROM SELECT id FROM users WHERE name = ?"
            );
            let q3 = test_parse!(
                create_cached_query(Dialect::MySQL),
                b"CREATE CACHE CONCURRENTLY FROM SELECT id FROM users WHERE name = ?"
            );
            let q4 = test_parse!(
                create_cached_query(Dialect::MySQL),
                b"CREATE CACHE ALWAYS CONCURRENTLY FROM SELECT id FROM users WHERE name = ?"
            );
            assert!(q1.always);
            assert!(q1.concurrently);
            assert!(q2.always);
            assert!(!q2.concurrently);
            assert!(!q3.always);
            assert!(q3.concurrently);
            assert!(q4.always);
            assert!(q4.concurrently);
            let q = vec![q1, q2, q3, q4];
            for stmt in q {
                assert!(stmt.name.is_none());
                let statement = match stmt.inner {
                    Ok(CacheInner::Statement(s)) => s,
                    _ => panic!(),
                };
                assert_eq!(
                    statement.tables,
                    vec![TableExpr::from(Relation::from("users"))]
                );
            }
        }

        #[test]
        fn display_create_query_cache() {
            let stmt = test_parse!(
                create_cached_query(Dialect::MySQL),
                b"CREATE CACHE CONCURRENTLY ALWAYS foo FROM SELECT id FROM users WHERE name = ?"
            );
            let res = stmt.display(Dialect::MySQL).to_string();
            assert_eq!(
                res,
                "CREATE CACHE CONCURRENTLY ALWAYS `foo` FROM SELECT `id` FROM `users` WHERE (`name` = ?)"
            );
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
            let res = create_table(Dialect::MySQL)(LocatedSpan::new(qstring.as_bytes()));
            assert_eq!(
                res.unwrap().1,
                CreateTableStatement {
                    if_not_exists: false,
                    table: Relation::from("comments"),
                    body: Ok(CreateTableBody {
                        fields: vec![
                            ColumnSpecification::with_constraints(
                                Column::from("id"),
                                SqlType::IntUnsigned(None),
                                vec![
                                    ColumnConstraint::NotNull,
                                    ColumnConstraint::AutoIncrement,
                                    ColumnConstraint::PrimaryKey,
                                ],
                            ),
                            ColumnSpecification::new(Column::from("hat_id"), SqlType::Int(None),),
                        ],
                        keys: Some(vec![
                            TableKey::FulltextKey {
                                index_name: Some("index_comments_on_comment".into()),
                                columns: vec![Column::from("comment")]
                            },
                            TableKey::Key {
                                index_name: Some("confidence_idx".into()),
                                columns: vec![Column::from("confidence")],
                                index_type: None
                            },
                            TableKey::UniqueKey {
                                constraint_name: None,
                                constraint_timing: None,
                                index_name: Some("short_id".into()),
                                columns: vec![Column::from("short_id")],
                                index_type: None,
                                nulls_distinct: None,
                            },
                            TableKey::Key {
                                index_name: Some("story_id_short_id".into()),
                                columns: vec![Column::from("story_id"), Column::from("short_id")],
                                index_type: None
                            },
                            TableKey::Key {
                                index_name: Some("thread_id".into()),
                                columns: vec![Column::from("thread_id")],
                                index_type: None,
                            },
                            TableKey::Key {
                                index_name: Some("index_comments_on_user_id".into()),
                                columns: vec![Column::from("user_id")],
                                index_type: None
                            },
                        ]),
                    }),
                    options: Ok(vec![
                        CreateTableOption::Engine(Some("InnoDB".to_string())),
                        CreateTableOption::Charset(CharsetName::Unquoted("utf8mb4".into())),
                    ])
                }
            );
        }

        #[test]
        fn mediawiki_create() {
            let qstring =
                "CREATE TABLE user_newtalk (  user_id int(5) NOT NULL default '0',  user_ip \
                       varchar(40) NOT NULL default '') TYPE=MyISAM;";
            let res = create_table(Dialect::MySQL)(LocatedSpan::new(qstring.as_bytes()));
            assert_eq!(
                res.unwrap().1,
                CreateTableStatement {
                    if_not_exists: false,
                    table: Relation::from("user_newtalk"),
                    body: Ok(CreateTableBody {
                        fields: vec![
                            ColumnSpecification::with_constraints(
                                Column::from("user_id"),
                                SqlType::Int(Some(5)),
                                vec![
                                    ColumnConstraint::NotNull,
                                    ColumnConstraint::DefaultValue(Expr::Literal(Literal::String(
                                        String::from("0")
                                    ))),
                                ],
                            ),
                            ColumnSpecification::with_constraints(
                                Column::from("user_ip"),
                                SqlType::VarChar(Some(40)),
                                vec![
                                    ColumnConstraint::NotNull,
                                    ColumnConstraint::DefaultValue(Expr::Literal(Literal::String(
                                        String::from("")
                                    ))),
                                ],
                            ),
                        ],
                        keys: None,
                    }),
                    options: Ok(vec![CreateTableOption::Other {
                        key: "TYPE".into(),
                        value: "MyISAM".into()
                    }]),
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
            if let Err(nom::Err::Error(nom::error::Error { input, .. })) = to_nom_result(
                create_table(Dialect::MySQL)(LocatedSpan::new(qstring.as_bytes())),
            ) {
                panic!("{}", std::str::from_utf8(input).unwrap());
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
            create_table(Dialect::MySQL)(LocatedSpan::new(qstring.as_bytes())).unwrap();
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
            create_table(Dialect::MySQL)(LocatedSpan::new(qstring.as_bytes())).unwrap();
        }

        #[test]
        fn employees_employees() {
            test_parse!(
                create_table(Dialect::MySQL),
                b"CREATE TABLE employees (
                    emp_no      INT             NOT NULL,
                    birth_date  DATE            NOT NULL,
                    first_name  VARCHAR(14)     NOT NULL,
                    last_name   VARCHAR(16)     NOT NULL,
                    gender      ENUM ('M','F')  NOT NULL,
                    hire_date   DATE            NOT NULL,
                    PRIMARY KEY (emp_no)
                )"
            );
        }

        #[test]
        fn employees_dept_manager() {
            test_parse!(
                create_table(Dialect::MySQL),
                b"CREATE TABLE dept_manager (
                    dept_no      CHAR(4)         NOT NULL,
                    emp_no       INT             NOT NULL,
                    from_date    DATE            NOT NULL,
                    to_date      DATE            NOT NULL,
                    KEY         (emp_no),
                    KEY         (dept_no),
                    FOREIGN KEY (emp_no)  REFERENCES employees (emp_no)    ,
                    FOREIGN KEY (dept_no) REFERENCES departments (dept_no) ,
                    PRIMARY KEY (emp_no,dept_no)
                )"
            );
        }

        #[test]
        fn employees_dept_empt_latest_date() {
            test_parse!(
                view_creation(Dialect::MySQL),
                b"CREATE OR REPLACE ALGORITHM=UNDEFINED DEFINER=`root`@`%` SQL SECURITY DEFINER \
                  VIEW `dept_emp_latest_date` AS \
                  SELECT emp_no, MAX(from_date) AS from_date, MAX(to_date) AS to_date \
                  FROM dept_emp \
                  GROUP BY emp_no"
            );
        }

        #[test]
        fn create_database_with_options() {
            let queries = vec![
                // No create options, with trailing ws
                ("Create databasE if  not Exists  noria       ",
                "CREATE DATABASE IF NOT EXISTS noria"),

                // No create options, no trailing ws
                ("Create databasE   noria",
                "CREATE DATABASE noria"),

                // Use <SCHEMA> instead of <DATABASE>, no create options, with trailing ws
                ("Create schema   noria       ",
                "CREATE SCHEMA noria"),

                // Use <SCHEMA> instead of <DATABASE>, no create options, no trailing ws
                ("Create scheMA if  not Exists  noria",
                "CREATE SCHEMA IF NOT EXISTS noria"),

                // Use single, eq separated create option, with <default> attribute, no trailing ws
                ("create databasE If  NOT exists  noria    default  character   Set =   utf16",
                "CREATE DATABASE IF NOT EXISTS noria DEFAULT CHARACTER SET = utf16"),

                // Use subset of eq separated create options, no <default> attribute for any, with trailing ws
                ("create schema noria      character   Set =   utf16   Collate utf16_collation   Encryption=  Y     ",
                "CREATE SCHEMA noria CHARACTER SET = utf16 COLLATE = utf16_collation ENCRYPTION = Y"),

                // Use subset of eq and ws separated create options, with <default> attribute for some, with trailing ws
                ("create databasE If  not exists  noria   DEfault  Collate utf16_collation  Character   Set =   utf16      ",
                "CREATE DATABASE IF NOT EXISTS noria DEFAULT COLLATE = utf16_collation CHARACTER SET = utf16"),

                // Use subset of eq and ws separated create options, with <default> attribute for some, with trailing ws
                ("Create schema noria   default Encryption=Y       Collate utf16_collation   ",
                "CREATE SCHEMA noria DEFAULT ENCRYPTION = Y COLLATE = utf16_collation"),

                // Use all available eq and ws separated create options, with <default> attribute for all, with trailing ws
                ("Create databasE if  not exists  noria     default Encryption=N   defaULT Collate utf16_collation   default   character   Set =   utf16    ",
                "CREATE DATABASE IF NOT EXISTS noria DEFAULT ENCRYPTION = N DEFAULT COLLATE = utf16_collation DEFAULT CHARACTER SET = utf16"),

                // Use all available eq and ws separated create options, no <default> attribute for all, no trailing ws
                ("Create databasE noria   Collate utf16_collation   Encryption=N character set =   utf16",
                "CREATE DATABASE noria COLLATE = utf16_collation ENCRYPTION = N CHARACTER SET = utf16"),
            ];

            for (query_str, expected_str) in queries {
                let res = test_parse!(create_database(Dialect::MySQL), query_str.as_bytes());
                assert_eq!(expected_str, res.display(Dialect::MySQL).to_string());
                test_parse_expect_err!(create_database(Dialect::PostgreSQL), query_str.as_bytes());
            }
        }
    }

    mod postgres {
        use super::*;
        use crate::to_nom_result;

        #[test]
        fn if_not_exists() {
            let res = test_parse!(
                create_table(Dialect::PostgreSQL),
                b"CREATE TABLE IF NOT EXISTS t (x int)"
            );
            assert!(res.if_not_exists);
            let rt = res.display(Dialect::PostgreSQL).to_string();
            assert_eq!(rt, "CREATE TABLE IF NOT EXISTS \"t\" (\"x\" INT)");
        }

        #[test]
        fn double_precision_column() {
            let (rem, res) = to_nom_result(create_table(Dialect::PostgreSQL)(LocatedSpan::new(
                b"create table t(x double precision)",
            )))
            .unwrap();
            assert_eq!(str::from_utf8(rem).unwrap(), "");
            assert_eq!(
                res,
                CreateTableStatement {
                    if_not_exists: false,
                    table: "t".into(),
                    body: Ok(CreateTableBody {
                        fields: vec![ColumnSpecification {
                            column: "x".into(),
                            sql_type: SqlType::Double,
                            generated: None,
                            constraints: vec![],
                            comment: None,
                        }],
                        keys: None,
                    }),
                    options: Ok(vec![])
                }
            );
        }

        #[test]
        fn create_with_non_reserved_identifier() {
            let qstring = "CREATE TABLE groups ( id integer );";
            let res = create_table(Dialect::PostgreSQL)(LocatedSpan::new(qstring.as_bytes()));
            assert_eq!(
                res.unwrap().1,
                CreateTableStatement {
                    if_not_exists: false,
                    table: Relation::from("groups"),
                    body: Ok(CreateTableBody {
                        fields: vec![ColumnSpecification::new(
                            Column::from("id"),
                            SqlType::Int(None)
                        ),],
                        keys: None,
                    }),
                    options: Ok(vec![])
                }
            );
        }

        #[test]
        fn create_with_reserved_identifier() {
            let qstring = "CREATE TABLE select ( id integer );";
            let res = create_table(Dialect::PostgreSQL)(LocatedSpan::new(qstring.as_bytes()));
            res.unwrap_err();
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
            let res = create_table(Dialect::PostgreSQL)(LocatedSpan::new(qstring.as_bytes()));
            assert_eq!(
                res.unwrap().1,
                CreateTableStatement {
                    if_not_exists: false,
                    table: Relation::from("django_admin_log"),
                    body: Ok(CreateTableBody {
                        fields: vec![
                            ColumnSpecification::with_constraints(
                                Column::from("id"),
                                SqlType::Int(None),
                                vec![
                                    ColumnConstraint::AutoIncrement,
                                    ColumnConstraint::NotNull,
                                    ColumnConstraint::PrimaryKey,
                                ],
                            ),
                            ColumnSpecification::with_constraints(
                                Column::from("action_time"),
                                SqlType::DateTime(None),
                                vec![ColumnConstraint::NotNull],
                            ),
                            ColumnSpecification::with_constraints(
                                Column::from("user_id"),
                                SqlType::Int(None),
                                vec![ColumnConstraint::NotNull],
                            ),
                            ColumnSpecification::new(
                                Column::from("content_type_id"),
                                SqlType::Int(None),
                            ),
                            ColumnSpecification::new(Column::from("object_id"), SqlType::LongText,),
                            ColumnSpecification::with_constraints(
                                Column::from("object_repr"),
                                SqlType::VarChar(Some(200)),
                                vec![ColumnConstraint::NotNull],
                            ),
                            ColumnSpecification::with_constraints(
                                Column::from("action_flag"),
                                SqlType::SmallIntUnsigned(None),
                                vec![ColumnConstraint::NotNull],
                            ),
                            ColumnSpecification::with_constraints(
                                Column::from("change_message"),
                                SqlType::LongText,
                                vec![ColumnConstraint::NotNull],
                            ),
                        ],
                        keys: None,
                    }),
                    options: Ok(vec![])
                }
            );

            let qstring = "CREATE TABLE \"auth_group\" (
                       \"id\" integer AUTO_INCREMENT NOT NULL PRIMARY KEY,
                       \"name\" varchar(80) NOT NULL UNIQUE)";
            let res = create_table(Dialect::PostgreSQL)(LocatedSpan::new(qstring.as_bytes()));
            assert_eq!(
                res.unwrap().1,
                CreateTableStatement {
                    if_not_exists: false,
                    table: Relation::from("auth_group"),
                    body: Ok(CreateTableBody {
                        fields: vec![
                            ColumnSpecification::with_constraints(
                                Column::from("id"),
                                SqlType::Int(None),
                                vec![
                                    ColumnConstraint::AutoIncrement,
                                    ColumnConstraint::NotNull,
                                    ColumnConstraint::PrimaryKey,
                                ],
                            ),
                            ColumnSpecification::with_constraints(
                                Column::from("name"),
                                SqlType::VarChar(Some(80)),
                                vec![ColumnConstraint::NotNull, ColumnConstraint::Unique],
                            ),
                        ],
                        keys: None,
                    }),
                    options: Ok(vec![]),
                }
            );
        }

        #[test]
        fn format_create() {
            let qstring = "CREATE TABLE \"auth_group\" (
                       \"id\" integer AUTO_INCREMENT NOT NULL PRIMARY KEY,
                       \"name\" varchar(80) NOT NULL UNIQUE)";
            // TODO(malte): INTEGER isn't quite reflected right here, perhaps
            let expected = "CREATE TABLE \"auth_group\" (\
                        \"id\" INT AUTO_INCREMENT NOT NULL PRIMARY KEY, \
                        \"name\" VARCHAR(80) NOT NULL UNIQUE)";
            let res = create_table(Dialect::PostgreSQL)(LocatedSpan::new(qstring.as_bytes()));
            assert_eq!(
                res.unwrap().1.display(Dialect::PostgreSQL).to_string(),
                expected
            );
        }

        #[test]
        fn simple_create_view() {
            let qstring = "CREATE VIEW v AS SELECT * FROM users WHERE username = 'bob';";

            let res = view_creation(Dialect::PostgreSQL)(LocatedSpan::new(qstring.as_bytes()));
            assert_eq!(
                res.unwrap().1,
                CreateViewStatement {
                    name: "v".into(),
                    or_replace: false,
                    fields: vec![],
                    definition: Ok(Box::new(SelectSpecification::Simple(SelectStatement {
                        tables: vec![TableExpr::from(Relation::from("users"))],
                        fields: vec![FieldDefinitionExpr::All],
                        where_clause: Some(Expr::BinaryOp {
                            lhs: Box::new(Expr::Column("username".into())),
                            rhs: Box::new(Expr::Literal(Literal::String("bob".into()))),
                            op: BinaryOperator::Equal,
                        }),
                        ..Default::default()
                    }))),
                }
            );
        }

        #[test]
        fn format_create_view() {
            let qstring = "CREATE VIEW \"v\" AS SELECT * FROM \"t\";";
            let expected = "CREATE VIEW \"v\" AS SELECT * FROM \"t\"";
            let res = view_creation(Dialect::PostgreSQL)(LocatedSpan::new(qstring.as_bytes()));
            assert_eq!(
                res.unwrap().1.display(Dialect::PostgreSQL).to_string(),
                expected
            );
        }

        #[test]
        fn display_create_query_cache() {
            let stmt = test_parse!(
                create_cached_query(Dialect::PostgreSQL),
                b"CREATE CACHE foo FROM SELECT id FROM users WHERE name = ?"
            );
            let res = stmt.display(Dialect::PostgreSQL).to_string();
            assert_eq!(
                res,
                "CREATE CACHE \"foo\" FROM SELECT \"id\" FROM \"users\" WHERE (\"name\" = ?)"
            );
        }

        #[test]
        fn lobsters_indexes() {
            let qstring = "CREATE TABLE \"comments\" (
            \"id\" int unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY,
            \"hat_id\" int,
            fulltext INDEX \"index_comments_on_comment\"  (\"comment\"),
            INDEX \"confidence_idx\"  (\"confidence\"),
            UNIQUE (\"short_id\"),
            INDEX \"story_id_short_id\"  (\"story_id\", \"short_id\"),
            INDEX \"thread_id\"  (\"thread_id\"),
            INDEX \"index_comments_on_user_id\"  (\"user_id\"))
            ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;";
            let res = create_table(Dialect::PostgreSQL)(LocatedSpan::new(qstring.as_bytes()));
            assert_eq!(
                res.unwrap().1,
                CreateTableStatement {
                    if_not_exists: false,
                    table: Relation::from("comments"),
                    body: Ok(CreateTableBody {
                        fields: vec![
                            ColumnSpecification::with_constraints(
                                Column::from("id"),
                                SqlType::IntUnsigned(None),
                                vec![
                                    ColumnConstraint::NotNull,
                                    ColumnConstraint::AutoIncrement,
                                    ColumnConstraint::PrimaryKey,
                                ],
                            ),
                            ColumnSpecification::new(Column::from("hat_id"), SqlType::Int(None),),
                        ],
                        keys: Some(vec![
                            TableKey::FulltextKey {
                                index_name: Some("index_comments_on_comment".into()),
                                columns: vec![Column::from("comment")]
                            },
                            TableKey::Key {
                                index_name: Some("confidence_idx".into()),
                                columns: vec![Column::from("confidence")],
                                index_type: None
                            },
                            TableKey::UniqueKey {
                                constraint_name: None,
                                constraint_timing: None,
                                index_name: None,
                                columns: vec![Column::from("short_id")],
                                index_type: None,
                                nulls_distinct: None,
                            },
                            TableKey::Key {
                                index_name: Some("story_id_short_id".into()),
                                columns: vec![Column::from("story_id"), Column::from("short_id")],
                                index_type: None
                            },
                            TableKey::Key {
                                index_name: Some("thread_id".into()),
                                columns: vec![Column::from("thread_id")],
                                index_type: None
                            },
                            TableKey::Key {
                                index_name: Some("index_comments_on_user_id".into()),
                                columns: vec![Column::from("user_id")],
                                index_type: None
                            },
                        ]),
                    }),
                    options: Ok(vec![
                        CreateTableOption::Engine(Some("InnoDB".to_string())),
                        CreateTableOption::Charset(CharsetName::Unquoted("utf8mb4".into()))
                    ]),
                }
            );
        }

        #[test]
        fn mediawiki_create() {
            let qstring =
                "CREATE TABLE user_newtalk (  user_id int(5) NOT NULL default '0',  user_ip \
                       varchar(40) NOT NULL default '') TYPE=MyISAM;";
            let res = create_table(Dialect::PostgreSQL)(LocatedSpan::new(qstring.as_bytes()));
            assert_eq!(
                res.unwrap().1,
                CreateTableStatement {
                    if_not_exists: false,
                    table: Relation::from("user_newtalk"),
                    body: Ok(CreateTableBody {
                        fields: vec![
                            ColumnSpecification::with_constraints(
                                Column::from("user_id"),
                                SqlType::Int(Some(5)),
                                vec![
                                    ColumnConstraint::NotNull,
                                    ColumnConstraint::DefaultValue(Expr::Literal(Literal::String(
                                        String::from("0")
                                    ))),
                                ],
                            ),
                            ColumnSpecification::with_constraints(
                                Column::from("user_ip"),
                                SqlType::VarChar(Some(40)),
                                vec![
                                    ColumnConstraint::NotNull,
                                    ColumnConstraint::DefaultValue(Expr::Literal(Literal::String(
                                        String::from("")
                                    ))),
                                ],
                            ),
                        ],
                        keys: None,
                    }),
                    options: Ok(vec![CreateTableOption::Other {
                        key: "TYPE".into(),
                        value: "MyISAM".into()
                    }]),
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
            create_table(Dialect::PostgreSQL)(LocatedSpan::new(qstring.as_bytes())).unwrap();
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
            create_table(Dialect::PostgreSQL)(LocatedSpan::new(qstring.as_bytes())).unwrap();
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
            create_table(Dialect::PostgreSQL)(LocatedSpan::new(qstring.as_bytes())).unwrap();
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
        let res = test_parse!(create_table(Dialect::MySQL), qstring);

        assert_eq!(
            res,
            CreateTableStatement {
                if_not_exists: false,
                table: "access_tokens".into(),
                body: Ok(CreateTableBody {
                    fields: vec![
                        ColumnSpecification::with_constraints(
                            "id".into(),
                            SqlType::IntUnsigned(Some(10)),
                            vec![ColumnConstraint::NotNull, ColumnConstraint::AutoIncrement,]
                        ),
                        ColumnSpecification::with_constraints(
                            "token".into(),
                            SqlType::VarChar(Some(40)),
                            vec![
                                ColumnConstraint::Collation("utf8mb4_unicode_ci".into()),
                                ColumnConstraint::NotNull,
                                ColumnConstraint::CharacterSet("utf8mb4".into())
                            ]
                        ),
                        ColumnSpecification::with_constraints(
                            "user_id".into(),
                            SqlType::IntUnsigned(Some(10)),
                            vec![ColumnConstraint::NotNull]
                        ),
                        ColumnSpecification::with_constraints(
                            "last_activity_at".into(),
                            SqlType::DateTime(None),
                            vec![ColumnConstraint::NotNull]
                        ),
                        ColumnSpecification::with_constraints(
                            "created_at".into(),
                            SqlType::DateTime(None),
                            vec![ColumnConstraint::NotNull],
                        ),
                        ColumnSpecification::with_constraints(
                            "type".into(),
                            SqlType::VarChar(Some(100)),
                            vec![
                                ColumnConstraint::Collation("utf8mb4_unicode_ci".into()),
                                ColumnConstraint::NotNull,
                                ColumnConstraint::CharacterSet("utf8mb4".into())
                            ]
                        ),
                        ColumnSpecification::with_constraints(
                            "title".into(),
                            SqlType::VarChar(Some(150)),
                            vec![
                                ColumnConstraint::Collation("utf8mb4_unicode_ci".into()),
                                ColumnConstraint::DefaultValue(Expr::Literal(Literal::Null)),
                                ColumnConstraint::CharacterSet("utf8mb4".into())
                            ]
                        ),
                        ColumnSpecification::with_constraints(
                            "last_ip_address".into(),
                            SqlType::VarChar(Some(45)),
                            vec![
                                ColumnConstraint::Collation("utf8mb4_unicode_ci".into()),
                                ColumnConstraint::DefaultValue(Expr::Literal(Literal::Null)),
                                ColumnConstraint::CharacterSet("utf8mb4".into())
                            ]
                        ),
                        ColumnSpecification::with_constraints(
                            "last_user_agent".into(),
                            SqlType::VarChar(Some(255)),
                            vec![
                                ColumnConstraint::Collation("utf8mb4_unicode_ci".into()),
                                ColumnConstraint::DefaultValue(Expr::Literal(Literal::Null)),
                                ColumnConstraint::CharacterSet("utf8mb4".into())
                            ]
                        ),
                    ],
                    keys: Some(vec![
                        TableKey::PrimaryKey {
                            constraint_name: None,
                            constraint_timing: None,
                            index_name: None,
                            columns: vec!["id".into()]
                        },
                        TableKey::UniqueKey {
                            constraint_name: None,
                            constraint_timing: None,
                            index_name: Some("access_tokens_token_unique".into()),
                            columns: vec!["token".into()],
                            index_type: None,
                            nulls_distinct: None,
                        },
                        TableKey::Key {
                            index_name: Some("access_tokens_user_id_foreign".into()),
                            columns: vec!["user_id".into()],
                            index_type: None,
                        },
                        TableKey::Key {
                            index_name: Some("access_tokens_type_index".into()),
                            columns: vec!["type".into()],
                            index_type: None,
                        },
                        TableKey::ForeignKey {
                            constraint_name: Some("access_tokens_user_id_foreign".into()),
                            columns: vec!["user_id".into()],
                            target_table: "users".into(),
                            target_columns: vec!["id".into()],
                            index_name: None,
                            on_delete: Some(ReferentialAction::Cascade),
                            on_update: Some(ReferentialAction::Cascade),
                        },
                    ]),
                }),
                options: Ok(vec![
                    CreateTableOption::Engine(Some("InnoDB".to_string())),
                    CreateTableOption::Charset(CharsetName::Unquoted("utf8mb4".into())),
                    CreateTableOption::Collate(CollationName::Unquoted(
                        "utf8mb4_unicode_ci".into()
                    ))
                ]),
            }
        )
    }

    #[test]
    fn flarum_create_2() {
        let qstring = b"create table `mentions_posts` (`post_id` int unsigned not null, `mentions_id` int unsigned not null) default character set utf8mb4 collate 'utf8mb4_unicode_ci'";
        let res = test_parse!(create_table(Dialect::MySQL), qstring);

        assert_eq!(
            res,
            CreateTableStatement {
                if_not_exists: false,
                table: "mentions_posts".into(),
                body: Ok(CreateTableBody {
                    fields: vec![
                        ColumnSpecification::with_constraints(
                            "post_id".into(),
                            SqlType::IntUnsigned(None),
                            vec![ColumnConstraint::NotNull],
                        ),
                        ColumnSpecification::with_constraints(
                            "mentions_id".into(),
                            SqlType::IntUnsigned(None),
                            vec![ColumnConstraint::NotNull],
                        ),
                    ],
                    keys: None,
                }),
                options: Ok(vec![
                    CreateTableOption::Charset(CharsetName::Unquoted("utf8mb4".into())),
                    CreateTableOption::Collate(CollationName::Quoted("utf8mb4_unicode_ci".into()))
                ]),
            }
        )
    }

    #[test]
    fn solidus_action_mailbox_inbound_emails() {
        let qstring = b"CREATE TABLE `action_mailbox_inbound_emails` (
            `id` bigint NOT NULL AUTO_INCREMENT, `status` int NOT NULL DEFAULT '0',
            `message_id` varchar(255) NOT NULL,
            `message_checksum` varchar(255) NOT NULL,
            `created_at` datetime(6) NOT NULL,
            `updated_at` datetime(6) NOT NULL,
             PRIMARY KEY (`id`),
             UNIQUE KEY `index_action_mailbox_inbound_emails_uniqueness` (`message_id`,`message_checksum`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3";
        test_parse!(create_table(Dialect::MySQL), qstring);
    }

    #[test]
    fn ar_internal_metadata() {
        let qstring_orig = b"CREATE TABLE `ar_internal_metadata` (
`key` character varying NOT NULL,
`value` character varying,
`created_at` timestamp(6) without time zone NOT NULL,
`updated_at` timestamp(6) without time zone NOT NULL,
PRIMARY KEY (`key`));";
        let res = test_parse!(create_table(Dialect::MySQL), qstring_orig);
        assert_eq!(res.table.name, "ar_internal_metadata");
    }

    #[test]
    fn discourse_uploads() {
        let qstring = b"CREATE TABLE `uploads` (
`id` integer NOT NULL,
`user_id` integer NOT NULL,
`original_filename` character varying NOT NULL,
`filesize` bigint NOT NULL,
`width` integer,
`height` integer,
`url` character varying NOT NULL,
`created_at` timestamp without time zone NOT NULL,
`updated_at` timestamp without time zone NOT NULL,
`sha1` character varying(40),
`origin` character varying(1000),
`retain_hours` integer,
`extension` character varying(10),
`thumbnail_width` integer,
`thumbnail_height` integer,
`etag` character varying,
`secure` boolean NOT NULL,
`access_control_post_id` bigint,
`original_sha1` character varying,
`animated` boolean,
`verification_status` integer NOT NULL,
`security_last_changed_at` timestamp without time zone,
`security_last_changed_reason` character varying,
PRIMARY KEY (`id`));";
        let res = test_parse!(create_table(Dialect::MySQL), qstring);
        assert_eq!(res.table.name, "uploads");
        assert_eq!(res.body.unwrap().fields.len(), 23);
    }

    #[test]
    fn solidus_spree_zones() {
        let qstring = b"CREATE TABLE `spree_zones` (
`id` int NOT NULL AUTO_INCREMENT PRIMARY KEY,
`name` varchar(255), `description` varchar(255),
`default_tax` tinyint(1) DEFAULT FALSE,
`zone_members_count` int DEFAULT 0,
`created_at` datetime(6), `updated_at` datetime(6)) ENGINE=InnoDB;";

        let res = test_parse!(create_table(Dialect::MySQL), qstring);
        assert_eq!(res.table.name, "spree_zones");
    }

    #[test]
    fn bit_literal() {
        let qstring = b"CREATE TABLE `table` (
            `enabled` bit(1) NOT NULL DEFAULT b'0'
          )";

        let res = test_parse!(create_table(Dialect::MySQL), qstring);

        assert_eq!(
            res,
            CreateTableStatement {
                if_not_exists: false,
                table: "table".into(),
                body: Ok(CreateTableBody {
                    fields: vec![ColumnSpecification::with_constraints(
                        "enabled".into(),
                        SqlType::Bit(Some(1)),
                        vec![
                            ColumnConstraint::NotNull,
                            ColumnConstraint::DefaultValue(Expr::Literal(Literal::BitVector(
                                BitVec::from_elem(1, false)
                            ))),
                        ],
                    ),],
                    keys: None,
                }),
                options: Ok(vec![])
            }
        );
    }

    #[test]
    fn on_update_current_timestamp_precision() {
        let qstring = b"CREATE TABLE foo (
          `lastModified` datetime(6) NOT NULL DEFAULT current_timestamp(6) ON UPDATE CURRENT_TIMESTAMP(6)
        );";
        let res = test_parse!(create_table(Dialect::MySQL), qstring);
        assert_eq!(res.table.name, "foo");
        assert_eq!(
            res,
            CreateTableStatement {
                if_not_exists: false,
                table: "foo".into(),
                body: Ok(CreateTableBody {
                    fields: vec![ColumnSpecification::with_constraints(
                        "lastModified".into(),
                        SqlType::DateTime(Some(6)),
                        vec![
                            ColumnConstraint::NotNull,
                            ColumnConstraint::DefaultValue(Expr::Call(FunctionExpr::Call {
                                name: "current_timestamp".into(),
                                arguments: Some(vec![Expr::Literal(Literal::Integer(6,),),]),
                            },),),
                            ColumnConstraint::OnUpdateCurrentTimestamp(Some(Literal::Integer(6)),),
                        ],
                    ),],
                    keys: None,
                }),
                options: Ok(vec![]),
            }
        )
    }

    #[test]
    fn propagate_charset() {
        let qstring = b"CREATE TABLE `t` (
  `id` int,
  `name` varchar(20),
  `name2` varchar(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_as_cs,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci";

        let res = test_parse!(create_table(Dialect::MySQL), qstring);

        assert_eq!(
            res,
            CreateTableStatement {
                if_not_exists: false,
                table: "t".into(),
                body: Ok(CreateTableBody {
                    fields: vec![
                        ColumnSpecification::new("id".into(), SqlType::Int(None)),
                        ColumnSpecification::with_constraints(
                            "name".into(),
                            SqlType::VarChar(Some(20)),
                            vec![
                                ColumnConstraint::CharacterSet("utf8mb4".into()),
                                ColumnConstraint::Collation("utf8mb4_0900_ai_ci".into()),
                            ],
                        ),
                        ColumnSpecification::with_constraints(
                            "name2".into(),
                            SqlType::VarChar(Some(20)),
                            vec![
                                ColumnConstraint::CharacterSet("utf8mb4".into()),
                                ColumnConstraint::Collation("utf8mb4_0900_as_cs".into()),
                            ],
                        ),
                    ],
                    keys: Some(vec![TableKey::PrimaryKey {
                        constraint_name: None,
                        constraint_timing: None,
                        index_name: None,
                        columns: vec!["id".into()]
                    }]),
                }),
                options: Ok(vec![
                    CreateTableOption::Engine(Some("InnoDB".to_string())),
                    CreateTableOption::Charset(CharsetName::Unquoted("utf8mb4".into())),
                    CreateTableOption::Collate(CollationName::Unquoted(
                        "utf8mb4_0900_ai_ci".into()
                    ))
                ]),
            }
        );
    }
}
