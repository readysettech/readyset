//! A parser for [sqllogictest][0] test scripts. See the official documentation for information
//! about the format parsed here.
//!
//! [0]: https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki
//!
//! This parser is intended to eventually parse a superset of sqllogictest, with extensions to
//! handle noria-specific features and failure modes

use std::convert::TryInto;
use std::io;

use crate::ast::*;

use anyhow::{anyhow, bail, Context};
use nom::character::complete::{
    alphanumeric1, anychar, digit1, line_ending, multispace1, not_line_ending,
};
use nom::character::is_space;
use nom::{
    alt, char, complete, count, do_parse, eof, flat_map, many0, many1, many_till, map, map_opt,
    named, one_of, opt, parse_to, preceded, tag, take_while, take_while1, terminated,
};

named!(
    comment<()>,
    complete!(do_parse!(
        take_while!(is_space) >> char!('#') >> not_line_ending >> (())
    ))
);

named!(
    statement_result<StatementResult>,
    alt!(
        tag!("ok") => { |_| StatementResult::Ok } |
        tag!("error") => { |_| StatementResult::Error }
    )
);

named!(
    skipif<Conditional>,
    do_parse!(
        constructor: tag!("skipif")
            >> multispace1
            >> name: flat_map!(alphanumeric1, parse_to!(String))
            >> (Conditional::SkipIf(name))
    )
);

named!(
    onlyif<Conditional>,
    do_parse!(
        constructor: tag!("onlyif")
            >> multispace1
            >> name: flat_map!(alphanumeric1, parse_to!(String))
            >> (Conditional::OnlyIf(name))
    )
);

named!(conditional<Conditional>, alt!(skipif | onlyif));
named!(
    conditionals<Vec<Conditional>>,
    many0!(terminated!(conditional, line_ending))
);

named!(
    statement_header<StatementResult>,
    do_parse!(
        tag!("statement")
            >> multispace1
            >> res: alt!(
                tag!("ok") => { |_| (StatementResult::Ok) } |
                tag!("error") => { |_| (StatementResult::Error) }
            )
            >> (res)
    )
);

named!(
    end_of_statement<()>,
    alt!(
        complete!(count!(line_ending, 2)) => { |_| () } |
        eof!() => { |_| () }
    )
);

named!(
    statement_command<String>,
    map!(many_till!(anychar, end_of_statement), |(s, _)| s
        .into_iter()
        .collect())
);

named!(
    statement<Statement>,
    do_parse!(
        conditionals: conditionals
            >> result: terminated!(statement_header, line_ending)
            >> command: statement_command
            >> (Statement {
                conditionals,
                result,
                command
            })
    )
);

named!(
    column_type<Type>,
    alt!(
        tag!("T") => { |_| Type::Text } |
        tag!("I") => { |_| Type::Integer } |
        tag!("R") => { |_| Type::Real }
    )
);

named!(column_types<Vec<Type>>, many0!(column_type));

named!(
    sort_mode<SortMode>,
    alt!(
        tag!("nosort") => { |_| SortMode::NoSort } |
        tag!("rowsort") => { |_| SortMode::RowSort } |
        tag!("valuesort") => { |_| SortMode::ValueSort }
    )
);

named!(
    digest<md5::Digest>,
    map!(count!(one_of!("1234567890abcdef"), 32), |cs| md5::Digest(
        hex::decode(cs.into_iter().map(|c| c as u8).collect::<Vec<_>>())
            .unwrap()
            .try_into()
            .unwrap()
    ))
);

named!(
    hash_results<QueryResults>,
    do_parse!(
        count: flat_map!(digit1, parse_to!(usize))
            >> take_while1!(is_space)
            >> tag!("values")
            >> take_while1!(is_space)
            >> tag!("hashing")
            >> take_while1!(is_space)
            >> tag!("to")
            >> take_while1!(is_space)
            >> digest: digest
            >> opt!(comment)
            >> (QueryResults::Hash { count, digest })
    )
);

named!(
    float<ResultValue>,
    do_parse!(
        whole: flat_map!(digit1, parse_to!(i64))
            >> tag!(".")
            >> fractional: flat_map!(digit1, parse_to!(u32))
            >> (ResultValue::Real(whole, fractional))
    )
);

named!(
    result_value<ResultValue>,
    preceded!(
        alt!(
            line_ending => { |_| () } |
            eof!() => { |_| () }
        ),
        alt!(
            complete!(float) |
            flat_map!(digit1, parse_to!(i64)) => { |i| ResultValue::Integer(i) } |
            tag!("NULL") => { |_| ResultValue::Null } |
            map_opt!(
                not_line_ending,
                |s: &[u8]| {
                    if s.is_empty() {
                        None
                    } else {
                        String::from_utf8(s.into()).ok()
                    }
                }
            ) => { |s| ResultValue::Text(s) }
        )
    )
);

named!(
    end_of_query_results<()>,
    alt!(
        complete!(count!(line_ending, 2)) => { |_| () } |
        preceded!(opt!(many1!(line_ending)), eof!()) => { |_| () }
    )
);

named!(
    query_results<QueryResults>,
    alt!(
        preceded!(line_ending, hash_results) |
        many_till!(
            complete!(result_value),
            end_of_query_results
        ) => { |(vals, _)| QueryResults::Results(vals) }
    )
);

named!(
    query<Query>,
    do_parse!(
        conditionals: conditionals
            >> tag!("query")
            >> take_while1!(is_space)
            >> column_types: column_types
            >> sort_mode: opt!(preceded!(take_while1!(is_space), sort_mode))
            >> label:
                opt!(preceded!(
                    take_while1!(is_space),
                    map_opt!(not_line_ending, |s: &[u8]| String::from_utf8(s.into())
                        .ok()
                        .filter(|s| !s.is_empty()))
                ))
            >> line_ending
            >> query:
                map!(many_till!(anychar, tag!("\n----")), |(s, _)| s
                    .into_iter()
                    .collect::<String>(
                ))
            >> opt!(preceded!(line_ending, comment))
            >> results: query_results
            >> (Query {
                label,
                column_types,
                sort_mode,
                conditionals,
                query,
                results
            })
    )
);

named!(
    hash_threshold<Record>,
    do_parse!(
        tag!("hash-threshold")
            >> take_while1!(is_space)
            >> threshold: flat_map!(digit1, parse_to!(usize))
            >> line_ending
            >> (Record::HashThreshold(threshold))
    )
);

named!(pub record<Record>, alt!(
    statement => { |stmt| Record::Statement(stmt) } |
    query => { |query| Record::Query(query) } |
    terminated!(tag!("halt"), line_ending) => { |_| Record::Halt } |
    hash_threshold
));

named!(
    ignore<()>,
    map!(
        opt!(many1!(alt!(
            comment |
            multispace1 => { |_| () }
        ))),
        |_| ()
    )
);

named!(pub records<Vec<Record>>, complete!(
    preceded!(
        ignore,
        many1!(complete!(terminated!(
            record,
            ignore
        )))
    )
));

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
        nom::Err::Error((input, kind)) | nom::Err::Failure((input, kind)) => {
            anyhow!(
                "Parse error, at {}: {:?}",
                &String::from_utf8_lossy(input)[..16],
                kind
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
                column_types: vec![Type::Integer],
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
                }
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
183";
        let result = complete(query)(input);
        assert_eq!(
            result.unwrap().1,
            Query {
                column_types: vec![Type::Integer, Type::Integer, Type::Integer],
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
                ])
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
a";
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
                    column_types: vec![Type::Text],
                    sort_mode: Some(SortMode::ValueSort),
                    label: None,
                    conditionals: vec![],
                    query: "SELECT * FROM t1".to_string(),
                    results: QueryResults::Results(vec![])
                }),
                Record::Statement(Statement {
                    result: StatementResult::Ok,
                    command: "INSERT INTO t1(x) VALUES ('a')".to_string(),
                    conditionals: vec![],
                }),
                Record::Query(Query {
                    label: None,
                    column_types: vec![Type::Text],
                    sort_mode: Some(SortMode::ValueSort),
                    conditionals: vec![],
                    query: "SELECT * FROM t1".to_string(),
                    results: QueryResults::Results(vec![ResultValue::Text("a".to_string())]),
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
                column_types: vec![Type::Integer],
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
                }
            }
        );
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
789";
        let result = complete(records)(input);
        assert_eq!(
            result.unwrap().1,
            vec![
                Record::Query(Query {
                    column_types: vec![Type::Integer],
                    sort_mode: Some(SortMode::RowSort),
                    label: Some("x0".to_string()),
                    conditionals: vec![],
                    query: "SELECT CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END
  FROM t1"
                        .to_string(),
                    results: QueryResults::Results(vec![123.into(), 456.into()]),
                }),
                Record::Query(Query {
                    column_types: vec![Type::Integer, Type::Integer],
                    sort_mode: Some(SortMode::RowSort),
                    label: None,
                    conditionals: vec![],
                    query: "SELECT * FROM t1".to_string(),
                    results: QueryResults::Results(vec![123.into(), 456.into(), 789.into(),]),
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
                column_types: vec![Type::Integer],
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
                }
            })]
        );
    }
}
