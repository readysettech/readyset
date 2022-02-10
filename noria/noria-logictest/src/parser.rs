//! A parser for [sqllogictest][0] test scripts. See the official documentation for information
//! about the format parsed here.
//!
//! [0]: https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki
//!
//! This parser is intended to eventually parse a superset of sqllogictest, with extensions to
//! handle noria-specific features and failure modes

use std::convert::TryInto;
use std::io;
use std::str::FromStr;

use anyhow::{anyhow, bail, Context};
use chrono::NaiveDateTime;
use mysql_time::MysqlTime;
use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::character::complete::{
    alphanumeric1, anychar, char, digit1, line_ending, multispace1, not_line_ending, one_of,
    space0, space1,
};
use nom::combinator::{complete, eof, map, map_opt, map_parser, opt, peek, recognize};
use nom::multi::{count, many0, many1, many_till};
use nom::sequence::{pair, preceded, terminated, tuple};
use nom::IResult;
use noria_data::TIMESTAMP_FORMAT;

use crate::ast::*;

fn comment(i: &[u8]) -> IResult<&[u8], ()> {
    let (i, _) = space0(i)?;
    let (i, _) = char('#')(i)?;
    let (i, _) = not_line_ending(i)?;
    Ok((i, ()))
}

fn skipif(i: &[u8]) -> IResult<&[u8], Conditional> {
    let (i, _) = tag("skipif")(i)?;
    let (i, _) = multispace1(i)?;
    let (i, name) = map(alphanumeric1, String::from_utf8_lossy)(i)?;
    let (i, _) = opt(comment)(i)?;
    Ok((i, Conditional::SkipIf(name.to_string())))
}

fn onlyif(i: &[u8]) -> IResult<&[u8], Conditional> {
    let (i, _) = tag("onlyif")(i)?;
    let (i, _) = multispace1(i)?;
    let (i, name) = map(alphanumeric1, String::from_utf8_lossy)(i)?;
    let (i, _) = opt(comment)(i)?;
    Ok((i, Conditional::OnlyIf(name.to_string())))
}

fn invert_no_upstream(i: &[u8]) -> IResult<&[u8], Conditional> {
    let (i, _) = tag("invert_no_upstream")(i)?;
    let (i, _) = opt(comment)(i)?;

    Ok((i, Conditional::InvertNoUpstream))
}

fn conditional(i: &[u8]) -> IResult<&[u8], Conditional> {
    alt((skipif, onlyif, invert_no_upstream))(i)
}

fn conditionals(i: &[u8]) -> IResult<&[u8], Vec<Conditional>> {
    many0(terminated(conditional, line_ending))(i)
}

fn statement_header(i: &[u8]) -> IResult<&[u8], StatementResult> {
    let (i, _) = tag("statement")(i)?;
    let (i, _) = multispace1(i)?;

    alt((
        map(tag("ok"), |_| StatementResult::Ok),
        map(tag("error"), |_| StatementResult::Error),
    ))(i)
}

fn end_of_statement(i: &[u8]) -> IResult<&[u8], ()> {
    alt((
        map(complete(count(line_ending, 2)), |_| ()),
        map(eof, |_| ()),
    ))(i)
}

fn statement_command(i: &[u8]) -> IResult<&[u8], String> {
    let (i, s) = many_till(anychar, end_of_statement)(i)?;
    Ok((i, s.0.into_iter().collect()))
}

fn statement(i: &[u8]) -> IResult<&[u8], Statement> {
    let (i, conditionals) = conditionals(i)?;
    let (i, result) = statement_header(i)?;
    let (i, _) = line_ending(i)?;
    let (i, command) = statement_command(i)?;

    Ok((
        i,
        Statement {
            result,
            command,
            conditionals,
        },
    ))
}

fn column_type(i: &[u8]) -> IResult<&[u8], Type> {
    alt((
        map(tag("T"), |_| Type::Text),
        map(tag("I"), |_| Type::Integer),
        map(tag("R"), |_| Type::Real),
        map(tag("D"), |_| Type::Date),
        map(tag("M"), |_| Type::Time),
        map(tag("BV"), |_| Type::BitVec),
    ))(i)
}

fn column_types(i: &[u8]) -> IResult<&[u8], Vec<Type>> {
    many1(column_type)(i)
}
fn sort_mode(i: &[u8]) -> IResult<&[u8], SortMode> {
    alt((
        map(tag("nosort"), |_| SortMode::NoSort),
        map(tag("rowsort"), |_| SortMode::RowSort),
        map(tag("valuesort"), |_| SortMode::ValueSort),
    ))(i)
}
fn digest(i: &[u8]) -> IResult<&[u8], md5::Digest> {
    let (i, cs) = count(one_of("1234567890abcdef"), 32)(i)?;
    Ok((
        i,
        md5::Digest(
            hex::decode(cs.into_iter().map(|c| c as u8).collect::<Vec<_>>())
                .unwrap()
                .try_into()
                .unwrap(),
        ),
    ))
}

fn hash_results(i: &[u8]) -> IResult<&[u8], QueryResults> {
    let (i, count) = map_parser(digit1, nom::character::complete::u32)(i)?;
    let (i, _) = space1(i)?;
    let (i, _) = tag("values")(i)?;
    let (i, _) = space1(i)?;
    let (i, _) = tag("hashing")(i)?;
    let (i, _) = space1(i)?;
    let (i, _) = tag("to")(i)?;
    let (i, _) = space1(i)?;
    let (i, d) = digest(i)?;
    let (i, _) = opt(comment)(i)?;

    Ok((
        i,
        QueryResults::Hash {
            count: count as usize,
            digest: d,
        },
    ))
}

fn float(i: &[u8]) -> IResult<&[u8], Value> {
    let (i, v) = map_parser(
        recognize(tuple((opt(tag("-")), digit1, tag("."), digit1))),
        nom::number::complete::double,
    )(i)?;

    Ok((i, Value::from(v)))
}

fn integer(i: &[u8]) -> IResult<&[u8], i64> {
    let (i, sign) = opt(tag("-"))(i)?;
    let (i, num) = map_parser(digit1, nom::character::complete::i64)(i)?;

    Ok((i, if sign.is_some() { -num } else { num }))
}

fn empty_string(i: &[u8]) -> IResult<&[u8], Value> {
    let (i, _) = tag("(empty)")(i)?;
    Ok((i, Value::Text(String::new())))
}

fn value(i: &[u8]) -> IResult<&[u8], Value> {
    alt((
        map(terminated(tag("NULL"), line_ending), |_| Value::Null),
        terminated(empty_string, line_ending),
        terminated(complete(float), line_ending),
        map(terminated(integer, line_ending), Value::Integer),
        terminated(
            map_opt(not_line_ending, |s: &[u8]| {
                Some(Value::Date(
                    NaiveDateTime::parse_from_str(
                        String::from_utf8_lossy(s).as_ref(),
                        TIMESTAMP_FORMAT,
                    )
                    .ok()?,
                ))
            }),
            line_ending,
        ),
        terminated(
            map_opt(not_line_ending, |s: &[u8]| {
                Some(Value::Time(
                    MysqlTime::from_str(String::from_utf8_lossy(s).as_ref()).ok()?,
                ))
            }),
            line_ending,
        ),
        map(
            terminated(
                map_opt(not_line_ending, |s: &[u8]| {
                    if s.is_empty() {
                        None
                    } else {
                        String::from_utf8(s.into()).ok()
                    }
                }),
                line_ending,
            ),
            Value::Text,
        ),
    ))(i)
}

fn positional_param(i: &[u8]) -> IResult<&[u8], Value> {
    let (i, _) = tag("?")(i)?;
    let (i, _) = multispace1(i)?;
    let (i, _) = tag("=")(i)?;
    let (i, _) = multispace1(i)?;
    let (i, value) = value(i)?;

    Ok((i, value))
}

fn positional_params(i: &[u8]) -> IResult<&[u8], QueryParams> {
    map(many1(positional_param), QueryParams::PositionalParams)(i)
}

fn numbered_param(i: &[u8]) -> IResult<&[u8], (u32, Value)> {
    let (i, _) = tag("$")(i)?;
    let (i, digit) = map_parser(digit1, nom::character::complete::u32)(i)?;
    let (i, _) = multispace1(i)?;
    let (i, _) = tag("=")(i)?;
    let (i, _) = multispace1(i)?;
    let (i, value) = value(i)?;

    Ok((i, (digit, value)))
}

fn numbered_params(i: &[u8]) -> IResult<&[u8], QueryParams> {
    let (i, params) = many1(numbered_param)(i)?;
    Ok((i, QueryParams::NumberedParams(params.into_iter().collect())))
}

fn query_params(i: &[u8]) -> IResult<&[u8], QueryParams> {
    let (i, params) = opt(alt((positional_params, numbered_params)))(i)?;
    Ok((i, params.unwrap_or_default()))
}

fn end_of_query_results(i: &[u8]) -> IResult<&[u8], ()> {
    alt((
        map(complete(line_ending), |_| ()),
        map(preceded(opt(many1(line_ending)), eof), |_| ()),
    ))(i)
}

fn query_results(i: &[u8]) -> IResult<&[u8], QueryResults> {
    alt((preceded(line_ending, hash_results), move |i| {
        let (i, _) = line_ending(i)?;
        let (i, (vals, _)) = many_till(complete(value), end_of_query_results)(i)?;
        Ok((i, QueryResults::Results(vals)))
    }))(i)
}

fn end_of_query(i: &[u8]) -> IResult<&[u8], ()> {
    let (i, _) = line_ending(i)?;
    let (i, _) = peek(alt((
        map(tag("----"), |_| ()),
        alt((map(numbered_param, |_| ()), map(positional_param, |_| ()))),
    )))(i)?;

    Ok((i, ()))
}

fn query(i: &[u8]) -> IResult<&[u8], Query> {
    let (i, conditionals) = conditionals(i)?;
    let (i, _) = tag("query")(i)?;
    let (i, column_types) = opt(preceded(space0, column_types))(i)?;
    let (i, sort_mode) = opt(preceded(space0, sort_mode))(i)?;
    let (i, label) = opt(preceded(
        space0,
        map_opt(not_line_ending, |s: &[u8]| {
            String::from_utf8(s.into()).ok().filter(|s| !s.is_empty())
        }),
    ))(i)?;
    let (i, _) = line_ending(i)?;
    let (i, query) = map(many_till(anychar, end_of_query), |(s, _)| {
        s.into_iter().collect::<String>()
    })(i)?;
    let (i, params) = query_params(i)?;
    let (i, _) = tag("----")(i)?;
    let (i, _) = opt(pair(line_ending, comment))(i)?;
    let (i, results) = query_results(i)?;

    Ok((
        i,
        Query {
            label,
            column_types,
            sort_mode,
            conditionals,
            query,
            results,
            params,
        },
    ))
}

fn hash_threshold(i: &[u8]) -> IResult<&[u8], Record> {
    let (i, _) = tag("hash-threshold")(i)?;
    let (i, _) = space1(i)?;
    let (i, threshold) = map_parser(digit1, nom::character::complete::u64)(i)?;
    let (i, _) = line_ending(i)?;
    Ok((i, Record::HashThreshold(threshold as usize)))
}

fn sleep(i: &[u8]) -> IResult<&[u8], Record> {
    let (i, _) = tag("sleep")(i)?;
    let (i, _) = multispace1(i)?;
    let (i, len) = map_parser(digit1, nom::character::complete::u64)(i)?;
    Ok((i, Record::Sleep(len)))
}

fn halt(i: &[u8]) -> IResult<&[u8], Record> {
    let (i, conditionals) = conditionals(i)?;
    let (i, _) = tag("halt")(i)?;
    let (i, _) = opt(comment)(i)?;
    Ok((i, Record::Halt { conditionals }))
}

pub fn record(i: &[u8]) -> IResult<&[u8], Record> {
    alt((
        map(statement, Record::Statement),
        map(query, Record::Query),
        sleep,
        halt,
        map(terminated(tag("graphviz"), line_ending), |_| {
            Record::Graphviz
        }),
        hash_threshold,
    ))(i)
}

pub fn ignore(i: &[u8]) -> IResult<&[u8], ()> {
    map(opt(many1(alt((comment, map(multispace1, |_| ()))))), |_| ())(i)
}

pub fn records(i: &[u8]) -> IResult<&[u8], Vec<Record>> {
    complete(preceded(
        ignore,
        many1(complete(terminated(record, ignore))),
    ))(i)
}

pub fn read_records<R>(mut input: R) -> anyhow::Result<Vec<Record>>
where
    R: io::Read,
{
    // TODO(grfn): stream rather than reading the whole thing
    let mut bytes = Vec::new();
    input
        .read_to_end(&mut bytes)
        .with_context(|| "Failed to read input file")?;
    let (remaining, records) = records(bytes.as_slice()).map_err(|e| match e {
        nom::Err::Incomplete(_) => anyhow!("Parse error: Incomplete"),
        nom::Err::Error(nom::error::Error { input, code })
        | nom::Err::Failure(nom::error::Error { input, code }) => {
            let pos = String::from_utf8_lossy(input);
            anyhow!(
                "Parse error, at {}: {:?}",
                &pos[..std::cmp::min(pos.len(), 16)],
                code
            )
        }
    })?;

    if !remaining.is_empty() {
        bail!(
            "Parse error, at {}: expected end of file",
            &String::from_utf8_lossy(remaining)[..32]
        );
    }
    Ok(records)
}

#[cfg(test)]
mod tests {
    use maplit::hashmap;
    use nom::combinator::complete;
    use pretty_assertions::assert_eq;

    use super::*;

    #[test]
    fn parse_conditional() {
        assert_eq!(
            complete(skipif)(b"skipif mysql").unwrap().1,
            Conditional::SkipIf("mysql".to_string())
        );

        assert_eq!(
            conditional(b"skipif mysql").unwrap().1,
            Conditional::SkipIf("mysql".to_string())
        );

        assert_eq!(
            conditional(b"onlyif mysql").unwrap().1,
            Conditional::OnlyIf("mysql".to_string())
        );

        assert_eq!(
            conditional(b"invert_no_upstream").unwrap().1,
            Conditional::InvertNoUpstream
        );
    }

    #[test]
    fn parse_negative_number_value() {
        assert_eq!(value(b"-1\n").unwrap().1, Value::Integer(-1));
    }

    #[test]
    fn parse_empty_string() {
        assert_eq!(value(b"(empty)\n").unwrap().1, Value::Text(String::new()));
    }

    #[test]
    fn parse_statement_no_conditional() {
        let input = b"statement ok
CREATE TABLE t1(a INTEGER, b INTEGER, c INTEGER, d INTEGER, e INTEGER)";
        assert_eq!(
            complete(statement)(input).unwrap().1,
            Statement {
                conditionals: vec![],
                result: StatementResult::Ok,
                command: "CREATE TABLE t1(a INTEGER, b INTEGER, c INTEGER, d INTEGER, e INTEGER)"
                    .to_string()
            }
        );
    }

    #[test]
    fn parse_statement_conditional() {
        let input = b"skipif mysql
statement ok
CREATE TABLE t1(a INTEGER, b INTEGER, c INTEGER, d INTEGER, e INTEGER)";
        assert_eq!(
            complete(statement)(input).unwrap().1,
            Statement {
                conditionals: vec![Conditional::SkipIf("mysql".to_string())],
                result: StatementResult::Ok,
                command: "CREATE TABLE t1(a INTEGER, b INTEGER, c INTEGER, d INTEGER, e INTEGER)"
                    .to_string()
            }
        );
    }

    #[test]
    fn parse_query_with_hash_result() {
        let input = b"query I nosort
SELECT CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END
FROM t1
ORDER BY 1
----
30 values hashing to 3c13dee48d9356ae19af2515e05e6b54";
        let result = complete(query)(input);
        assert_eq!(
            result.unwrap().1,
            Query {
                column_types: Some(vec![Type::Integer]),
                sort_mode: Some(SortMode::NoSort),
                label: None,
                conditionals: vec![],
                query: "SELECT CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END
FROM t1
ORDER BY 1"
                    .to_string(),
                results: QueryResults::Hash {
                    count: 30,
                    digest: md5::Digest(
                        hex::decode("3c13dee48d9356ae19af2515e05e6b54")
                            .unwrap()
                            .try_into()
                            .unwrap()
                    )
                },
                params: Default::default(),
            }
        )
    }

    #[test]
    fn parse_query_no_column_types() {
        let input = b"query nosort
SELECT CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END
FROM t1
ORDER BY 1
----
30 values hashing to 3c13dee48d9356ae19af2515e05e6b54";
        let result = complete(query)(input);
        assert_eq!(
            result.unwrap().1,
            Query {
                column_types: None,
                sort_mode: Some(SortMode::NoSort),
                label: None,
                conditionals: vec![],
                query: "SELECT CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END
FROM t1
ORDER BY 1"
                    .to_string(),
                results: QueryResults::Hash {
                    count: 30,
                    digest: md5::Digest(
                        hex::decode("3c13dee48d9356ae19af2515e05e6b54")
                            .unwrap()
                            .try_into()
                            .unwrap()
                    )
                },
                params: Default::default(),
            }
        )
    }

    #[test]
    fn parse_query_with_value_result() {
        let input = b"query III nosort
SELECT a,
       c-d,
       d
  FROM t1
 WHERE c>d
   AND a>b
   AND (a>b-2 AND a<b+2)
 ORDER BY 1,2,3
----
131
1
133
182
1
183
";
        let result = complete(query)(input);
        assert_eq!(
            result.unwrap().1,
            Query {
                column_types: Some(vec![Type::Integer, Type::Integer, Type::Integer]),
                sort_mode: Some(SortMode::NoSort),
                label: None,
                conditionals: vec![],
                query: "SELECT a,
       c-d,
       d
  FROM t1
 WHERE c>d
   AND a>b
   AND (a>b-2 AND a<b+2)
 ORDER BY 1,2,3"
                    .to_string(),
                results: QueryResults::Results(vec![
                    131.into(),
                    1.into(),
                    133.into(),
                    182.into(),
                    1.into(),
                    183.into(),
                ]),
                params: Default::default(),
            }
        )
    }

    #[test]
    fn parse_query_with_no_results() {
        let input = b"statement ok
CREATE TABLE t1(x VARCHAR)

query T valuesort
SELECT * FROM t1
----

statement ok
INSERT INTO t1(x) VALUES ('a')

query T valuesort
SELECT * FROM t1
----
a
";
        let result = complete(records)(input);
        assert_eq!(
            result.unwrap().1,
            vec![
                Record::Statement(Statement {
                    result: StatementResult::Ok,
                    command: "CREATE TABLE t1(x VARCHAR)".to_string(),
                    conditionals: vec![],
                },),
                Record::Query(Query {
                    column_types: Some(vec![Type::Text]),
                    sort_mode: Some(SortMode::ValueSort),
                    label: None,
                    conditionals: vec![],
                    query: "SELECT * FROM t1".to_string(),
                    results: QueryResults::Results(vec![]),
                    params: Default::default(),
                }),
                Record::Statement(Statement {
                    result: StatementResult::Ok,
                    command: "INSERT INTO t1(x) VALUES ('a')".to_string(),
                    conditionals: vec![],
                }),
                Record::Query(Query {
                    label: None,
                    column_types: Some(vec![Type::Text]),
                    sort_mode: Some(SortMode::ValueSort),
                    conditionals: vec![],
                    query: "SELECT * FROM t1".to_string(),
                    results: QueryResults::Results(vec![Value::Text("a".to_string())]),
                    params: Default::default(),
                }),
            ]
        )
    }

    #[test]
    fn parse_named_query() {
        let input = b"query I rowsort x0
SELECT CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END
  FROM t1
----
30 values hashing to efdbaa4d180e7867bec1c4d897bd25b9";
        let result = complete(query)(input);
        assert_eq!(
            result.unwrap().1,
            Query {
                column_types: Some(vec![Type::Integer]),
                sort_mode: Some(SortMode::RowSort),
                label: Some("x0".to_string()),
                conditionals: vec![],
                query: "SELECT CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END
  FROM t1"
                    .to_string(),
                results: QueryResults::Hash {
                    count: 30,
                    digest: md5::Digest(
                        hex::decode("efdbaa4d180e7867bec1c4d897bd25b9")
                            .unwrap()
                            .try_into()
                            .unwrap()
                    )
                },
                params: Default::default(),
            }
        );
    }

    #[test]
    fn parse_query_with_positional_params() {
        let input = b"query III nosort
SELECT * FROM t1 WHERE id = ?
? = 1
----
131
1
";
        let result = complete(query)(input);
        assert_eq!(
            result.unwrap().1,
            Query {
                column_types: Some(vec![Type::Integer, Type::Integer, Type::Integer]),
                sort_mode: Some(SortMode::NoSort),
                label: None,
                conditionals: vec![],
                query: "SELECT * FROM t1 WHERE id = ?".to_owned(),
                results: QueryResults::Results(vec![131.into(), 1.into(),]),
                params: QueryParams::PositionalParams(vec![1.into()]),
            }
        )
    }

    #[test]
    fn parse_query_with_numbered_params() {
        let input = b"query III nosort
SELECT * FROM t1 WHERE id = $1
$1 = 1
----
131
1
";
        let result = complete(query)(input);
        assert_eq!(
            result.unwrap().1,
            Query {
                column_types: Some(vec![Type::Integer, Type::Integer, Type::Integer]),
                sort_mode: Some(SortMode::NoSort),
                label: None,
                conditionals: vec![],
                query: "SELECT * FROM t1 WHERE id = $1".to_owned(),
                results: QueryResults::Results(vec![131.into(), 1.into()]),
                params: QueryParams::NumberedParams(hashmap! {1 => 1.into()}),
            }
        )
    }

    #[test]
    fn parse_multiple_records() {
        let input = b"query I rowsort x0
SELECT CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END
  FROM t1
----
123
456

query II rowsort
SELECT * FROM t1
----
123
456
789
";
        let result = complete(records)(input);
        assert_eq!(
            result.unwrap().1,
            vec![
                Record::Query(Query {
                    column_types: Some(vec![Type::Integer]),
                    sort_mode: Some(SortMode::RowSort),
                    label: Some("x0".to_string()),
                    conditionals: vec![],
                    query: "SELECT CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END
  FROM t1"
                        .to_string(),
                    results: QueryResults::Results(vec![123.into(), 456.into()]),
                    params: Default::default(),
                }),
                Record::Query(Query {
                    column_types: Some(vec![Type::Integer, Type::Integer]),
                    sort_mode: Some(SortMode::RowSort),
                    label: None,
                    conditionals: vec![],
                    query: "SELECT * FROM t1".to_string(),
                    results: QueryResults::Results(vec![123.into(), 456.into(), 789.into(),]),
                    params: Default::default(),
                })
            ]
        );
    }

    #[test]
    fn parse_record_with_comment() {
        let input = b"# hi there I'm a comment
query I rowsort x0
SELECT CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END
  FROM t1
----
# comment
30 values hashing to efdbaa4d180e7867bec1c4d897bd25b9 # comment";
        let result = complete(records)(input);
        assert_eq!(
            result.unwrap().1,
            vec![Record::Query(Query {
                column_types: Some(vec![Type::Integer]),
                sort_mode: Some(SortMode::RowSort),
                label: Some("x0".to_string()),
                conditionals: vec![],
                query: "SELECT CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END
  FROM t1"
                    .to_string(),
                results: QueryResults::Hash {
                    count: 30,
                    digest: md5::Digest(
                        hex::decode("efdbaa4d180e7867bec1c4d897bd25b9")
                            .unwrap()
                            .try_into()
                            .unwrap()
                    )
                },
                params: Default::default(),
            })]
        );
    }

    #[test]
    fn float_trailing_zeros() {
        let input = b"0.7500";
        let expected = Value::from(0.75_f64);
        assert_eq!(complete(float)(input).unwrap().1, expected);
    }
}
