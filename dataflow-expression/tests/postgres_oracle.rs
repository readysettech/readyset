use std::cell::RefCell;
use std::env;
use std::sync::Arc;

use chrono::{DateTime, FixedOffset};
use dataflow_expression::DateTruncPrecision;
use postgres::{Client, Config, NoTls};
use pretty_assertions::assert_eq;
use proptest::prelude::*;
use readyset_data::DfValue;
use readyset_sql::ast::TimestampField;
use readyset_util::arbitrary::{arbitrary_date_time_timezone, arbitrary_timestamp_naive_date_time};
use test_utils::{tags, upstream};

use crate::common::parse_lower_eval;

fn config() -> Config {
    let mut config = Config::new();
    config
        .host(env::var("PGHOST").as_deref().unwrap_or("localhost"))
        .port(
            env::var("PGPORT")
                .unwrap_or_else(|_| "5432".into())
                .parse()
                .unwrap(),
        )
        .user(env::var("PGUSER").as_deref().unwrap_or("postgres"))
        .password(env::var("PGPASSWORD").unwrap_or_else(|_| "noria".into()));
    config
}

fn postgres_eval(expr: &str, client: &mut Client) -> Result<DfValue, postgres::Error> {
    Ok(client.query_one(&format!("SELECT {expr};"), &[])?.get(0))
}

fn compare_eval(expr: &str, client: &mut Client) {
    let our_result = parse_lower_eval(
        expr,
        readyset_sql::Dialect::PostgreSQL,
        dataflow_expression::Dialect::DEFAULT_POSTGRESQL,
    )
    .unwrap_or_else(|e| panic!("Error evaluating `{expr}`: {e}"));
    let pg_result =
        postgres_eval(expr, client).unwrap_or_else(|e| panic!("Error evaluating `{expr}`: {e}"));
    assert_eq!(
        our_result, pg_result,
        "mismatched results for {expr} (left: us, right: postgres)"
    );
}

#[tags(serial)]
#[upstream(postgres)]
#[test]
fn example_exprs_eval_same_as_postgres() {
    let mut client = config().connect(NoTls).unwrap();

    client.simple_query("DROP TYPE IF EXISTS abc;").unwrap();
    client
        .simple_query("CREATE TYPE abc AS ENUM ('a', 'b', 'c')")
        .unwrap();

    client.simple_query("DROP TYPE IF EXISTS cba;").unwrap();
    client
        .simple_query("CREATE TYPE cba AS ENUM ('c', 'b', 'a')")
        .unwrap();

    for expr in [
        "1 != 2",
        "1 != 1",
        "4 + 5",
        "5 > 4",
        "-1 = -1",
        "-1.0 = -1.0",
        "-1 = -1.0",
        "1 != -1",
        "1.0 != -1.0",
        "'a' like 'A'",
        "'a' ilike 'A'",
        "'a' not like 'a'",
        "'a' not ilike 'b'",
        "'a' like 'A'",
        "'a' not like 'a'",
        "'a' ilike all ('{a,A}')",
        "'a' ilike all ('{a,A,b}')",
        "'a'::abc = 'a'",
        "'a'::abc < all('{b,c}')",
        "'c'::cba < all('{{a,b,a},{b,a,b}}')",
        "1 != all('{2,3}')",
        "1 != all('{1,2,3}')",
        "1 != any('{2,3}')",
        "1 != any('{1,1,1}')",
        "null = null",
        "null != null",
        "1 = null",
        "null != 1",
        "substring('abcdef' for 3)",
        "substring('abcdef', 3)",
        "substring('abcdef', 3, 2)",
        "substring('abcdef', 1, 3)",
        "concat(1,2)",
        "concat('one',2)",
        "concat('one',2)",
        "concat('one',2,'three')",
        "concat('a','b')",
        "concat('a')",
        "split_part('abc~@~def~@~ghi', '~@~', 2)",
        "split_part('a.b.c', '.', 4)",
        r"'foo\bar'",
        r"'foo\%bar'",
        r"'foo\_bar'",
        r"E'foo\bar'",
        r"E'foo\%bar'",
        r"E'foo\_bar'",
        r"'a_b' like 'a\_b'",
        r"'a_b' like 'a\%b'",
        r"'a%b' like 'a\_b'",
        r"'a%b' like 'a\%b'",
        r"'a%b' like 'a\\_b'",
        r"'a%b' like 'a\\%b'",
        r"'a\b' like 'a\\b'",
        r"E'a_b' like E'a\_b'",
        r"E'a_b' like E'a\%b'",
        r"E'a%b' like E'a\_b'",
        r"E'a%b' like E'a\%b'",
        r"E'a%b' like E'a\\_b'",
        r"E'a%b' like E'a\\%b'",
        r"E'a\b' like E'a\\b'",
        r"'a\b' like E'a\\b'",
        r"E'a\b' like E'a\b'",
        r"E'a\b' like 'a\b'",
        r"'a\b' like E'a\\\\b'",
        r"'a\b' like 'a\\b'",
        "CAST(-1 AS INTEGER)",
        "CAST(2 AS INTEGER)",
        "CAST(2.5 AS INTEGER)",
        "CAST(3.5 AS INTEGER)",
        "CAST(-2.5 AS INTEGER)",
        "CAST(-3.5 AS INTEGER)",
        "CAST(2.4 AS INTEGER)",
        "CAST(3.4 AS INTEGER)",
        "CAST(-2.4 AS INTEGER)",
        "CAST(-3.4 AS INTEGER)",
        "CAST(2.6 AS INTEGER)",
        "CAST(3.6 AS INTEGER)",
        "CAST(-2.6 AS INTEGER)",
        "CAST(-3.6 AS INTEGER)",
        "-1::INTEGER",
        "2::INTEGER",
        "2.5::INTEGER",
        "3.5::INTEGER",
        "-2.5::INTEGER",
        "-3.5::INTEGER",
        "2.4::INTEGER",
        "3.4::INTEGER",
        "-2.4::INTEGER",
        "-3.4::INTEGER",
        "2.6::INTEGER",
        "3.6::INTEGER",
        "-2.6::INTEGER",
        "-3.6::INTEGER",
        r#"jsonb_set('{"k": 0}', '{k}', '42')"#,
        r#"jsonb_set('{"a": {"b": 1}}', '{a,b}', '"x"')"#,
        "jsonb_insert('{}', '{k}', '42')",
    ] {
        compare_eval(expr, &mut client);
    }
}

#[tags(serial)]
#[upstream(postgres, 15)]
#[test]
fn example_exprs_eval_same_as_postgres15() {
    let mut client = config().connect(NoTls).unwrap();

    client.simple_query("DROP TYPE IF EXISTS abc;").unwrap();
    client
        .simple_query("CREATE TYPE abc AS ENUM ('a', 'b', 'c')")
        .unwrap();

    client.simple_query("DROP TYPE IF EXISTS cba;").unwrap();
    client
        .simple_query("CREATE TYPE cba AS ENUM ('c', 'b', 'a')")
        .unwrap();

    for expr in [
        "split_part('a.b.c', '.', -1)",
        "split_part('a.b.c', '.', -4)",
    ] {
        compare_eval(expr, &mut client);
    }
}

// Normalize the UTC offset to hours and minutes (dropping any seconds).
// This is currently necessary as `chrono::FixedOffset` does not correctly
// parse seconds in all circumstances (https://github.com/chronotope/chrono/pull/1083).
fn normalize_offset(datetime: DateTime<FixedOffset>) -> DateTime<FixedOffset> {
    let offset = datetime.timezone().local_minus_utc();
    let normalized_offset = (offset / 60) * 60;
    let new_offset = FixedOffset::east_opt(normalized_offset).unwrap();
    datetime.with_timezone(&new_offset)
}

/// Test the `date_trunc` built-in function. Passes a simple timestamp,
/// with no timezone information, and does not pass the optional timezone
/// for normalized conversions.
#[upstream(postgres)]
#[test]
fn date_trunc_timestamp_no_opt_tz() {
    let client = RefCell::new(config().connect(NoTls).unwrap());

    proptest!(| (precision: DateTruncPrecision, datetime in arbitrary_timestamp_naive_date_time()) | {
        let mut client = client.borrow_mut();
        let expr = format!("date_trunc('{precision}', '{datetime}'::timestamp)");
        compare_eval(expr.as_str(), &mut client);
    });
}

/// Test the `date_trunc` built-in function. Passes a timestamp with
/// timezone information, and does not pass the optional timezone for
/// normalized conversions.
#[upstream(postgres)]
#[test]
fn date_trunc_timestamptz_no_opt_tz() {
    let client = RefCell::new(config().connect(NoTls).unwrap());

    proptest!(| (precision: DateTruncPrecision, datetime in arbitrary_date_time_timezone()) | {
       let dt = normalize_offset(datetime);
       let mut client = client.borrow_mut();
       let expr = format!("date_trunc('{precision}', '{dt}'::timestamptz)");
       compare_eval(expr.as_str(), &mut client);
    });
}

/// Test the `date_trunc` built-in function. Passes a timestamp with
/// timezone information but casts to a timestamp, and does not pass
/// the optional timezone for normalized conversions.
#[upstream(postgres)]
#[test]
fn date_trunc_timestamptz_downcast_no_opt_tz() {
    let client = RefCell::new(config().connect(NoTls).unwrap());

    proptest!(| (precision: DateTruncPrecision, datetime in arbitrary_date_time_timezone()) | {
       let dt = normalize_offset(datetime);
       let mut client = client.borrow_mut();
       let expr = format!("date_trunc('{precision}', '{dt}'::timestamp)");
       compare_eval(expr.as_str(), &mut client);
    });
}

/// Test the `date_trunc` built-in function. Passes a timestamp without
/// timezone information but casts to a timestamptz, and does not pass
/// the optional timezone for normalized conversions.
#[upstream(postgres)]
#[test]
fn date_trunc_timestamp_upcast_no_opt_tz() {
    let client = RefCell::new(config().connect(NoTls).unwrap());

    proptest!(| (precision: DateTruncPrecision, datetime in arbitrary_timestamp_naive_date_time()) | {
       let mut client = client.borrow_mut();
       let expr = format!("date_trunc('{precision}', '{datetime}'::timestamptz)");
       compare_eval(expr.as_str(), &mut client);
    });
}

mod extract {
    use bytes::BytesMut;
    use pretty_assertions::assert_eq;
    use readyset_util::arbitrary::{
        arbitrary_date_time_with_nanos, arbitrary_naive_date, arbitrary_naive_date_time_with_nanos,
        arbitrary_naive_time,
    };
    use readyset_util::fmt::FastEncode;
    use regex::Regex;

    use super::*;

    fn compare_eval_numeric(expr: &str, client: &mut Client, re: &Regex) {
        fn truncate_dfvalue(value: &DfValue) -> DfValue {
            match value {
                DfValue::Numeric(dec) => DfValue::Numeric(Arc::new(dec.round_dp(20))),
                _ => panic!(),
            }
        }

        fn extract_message_body(our_err: &str, pg_err: &str) -> Option<(String, String)> {
            if let Some(pg_err_pos) = pg_err.find("ERROR: ") {
                if let Some(our_err_pos) = our_err.find("ERROR: ") {
                    return Some((
                        our_err[our_err_pos..our_err.len()].to_string(),
                        pg_err[pg_err_pos..pg_err.len()].to_string(),
                    ));
                }
            }
            None
        }

        let our_result = parse_lower_eval(
            expr,
            readyset_sql::Dialect::PostgreSQL,
            dataflow_expression::Dialect::DEFAULT_POSTGRESQL,
        );

        match (postgres_eval(expr, client), our_result) {
            (Ok(pg_result), Ok(our_result)) => {
                assert_eq!(
                    truncate_dfvalue(&our_result),
                    truncate_dfvalue(&pg_result),
                    "mismatched results for {expr} (left: us, right: postgres)"
                );
            }
            (Err(pg_err), Err(our_err)) => {
                let pg_err_str = readyset_errors::postgres_err(&pg_err);

                let mut asserted = false;
                if re.is_match(&pg_err_str) {
                    if let Some((our_err, pg_err)) =
                        extract_message_body(&our_err.to_string(), &pg_err_str)
                    {
                        assert_eq!(
                            our_err, pg_err,
                            "mismatched error message for {expr} (left: us, right: postgres)"
                        );
                        asserted = true;
                    }
                }
                if !asserted {
                    assert_eq!(
                        our_err.to_string(),
                        pg_err_str,
                        "mismatched error message for {expr} (left: us, right: postgres)"
                    );
                }
            }
            (Ok(_), Err(our_err)) => {
                panic!("We failed with error \"{our_err}\", but Postgres succeeded");
            }
            (Err(pg_err), Ok(_)) => {
                panic!("Postgres failed with error \"{pg_err}\", but we succeeded");
            }
        }
    }

    /*
     * Builds Regex for parsing error message resulting from invalid call to EXTRACT function.
     * Note: this error message pattern is compliant with Postgres 15.
     */
    fn build_regex_for_invalid_extract_call(type_name: &str) -> Regex {
        if type_name.eq_ignore_ascii_case("timestamptz") {
            // There are no "invalid calls" for type TIMESTAMPTZ
            Regex::new("").unwrap()
        } else {
            let mut regex_text: String =
                format!("ERROR:\\s*unit \"[a-z_]*\" not supported for type {type_name}");
            if type_name.eq_ignore_ascii_case("time") || type_name.eq_ignore_ascii_case("timestamp")
            {
                regex_text.push_str(" without time zone");
            }
            Regex::new(&regex_text).unwrap()
        }
    }

    #[upstream(postgres, 15)]
    #[test]
    fn timestamptz() {
        let client = RefCell::new(config().connect(NoTls).unwrap());
        let re = build_regex_for_invalid_extract_call("timestamptz");

        proptest!(| (field: TimestampField, datetime in arbitrary_date_time_with_nanos()) | {
            let mut client = client.borrow_mut();
            let mut bytes = BytesMut::new();
            datetime.put(&mut bytes);
            let dt_string = String::from_utf8(bytes.to_vec()).unwrap();

            let expr = format!("extract({field} FROM '{dt_string}'::timestamptz)");
            compare_eval_numeric(expr.as_str(), &mut client, &re);
        });
    }

    #[upstream(postgres, 15)]
    #[test]
    fn timestamp() {
        let client = RefCell::new(config().connect(NoTls).unwrap());
        let re = build_regex_for_invalid_extract_call("timestamp");

        proptest!(| (field: TimestampField, datetime in arbitrary_naive_date_time_with_nanos()) | {
            let mut client = client.borrow_mut();
            let mut bytes = BytesMut::new();
            datetime.put(&mut bytes);
            let ts_string = String::from_utf8(bytes.to_vec()).unwrap();

            let expr = format!("extract({field} FROM '{ts_string}'::timestamp)");
            compare_eval_numeric(expr.as_str(), &mut client, &re);
        });
    }

    #[upstream(postgres, 15)]
    #[test]
    fn date() {
        let client = RefCell::new(config().connect(NoTls).unwrap());
        let re = build_regex_for_invalid_extract_call("date");

        proptest!(| (field: TimestampField, date in arbitrary_naive_date()) | {
            let mut client = client.borrow_mut();
            let mut bytes = BytesMut::new();
            date.put(&mut bytes);
            let date_string = String::from_utf8(bytes.to_vec()).unwrap();

            let expr = format!("extract({field} FROM '{date_string}'::date)");
            compare_eval_numeric(expr.as_str(), &mut client, &re);
        });
    }

    fn test_extract_time_from(type_name: &str) {
        let client = RefCell::new(config().connect(NoTls).unwrap());
        let re = build_regex_for_invalid_extract_call("date");

        proptest!(| (field: TimestampField, date in arbitrary_naive_date()) | {
            match field {
                TimestampField::Hour
                | TimestampField::Minute
                | TimestampField::Second
                | TimestampField::Milliseconds
                | TimestampField::Microseconds => {
                    let mut client = client.borrow_mut();
                    let mut bytes = BytesMut::new();
                    date.put(&mut bytes);
                    let date_string = String::from_utf8(bytes.to_vec()).unwrap();

                    let expr = format!("extract({field} FROM '{date_string}'::{type_name})");
                    compare_eval_numeric(expr.as_str(), &mut client, &re);
                }
                _ => ()
            }
        });
    }

    #[upstream(postgres, 15)]
    #[test]
    fn time_from_date() {
        test_extract_time_from("date");
    }

    #[upstream(postgres, 15)]
    #[test]
    fn time_from_timestamp_with_date_only() {
        test_extract_time_from("timestamp");
    }

    #[upstream(postgres, 15)]
    #[test]
    fn time_from_timestamptz_with_date_only() {
        test_extract_time_from("timestamptz");
    }

    #[upstream(postgres, 15)]
    #[test]
    fn timeee() {
        let client = RefCell::new(config().connect(NoTls).unwrap());
        let re = build_regex_for_invalid_extract_call("time");

        proptest!(| (field: TimestampField, time in arbitrary_naive_time()) | {
            let mut client = client.borrow_mut();

            let expr = format!("extract({field} FROM '{time}'::time)");
            compare_eval_numeric(expr.as_str(), &mut client, &re);
        });
    }

    #[upstream(postgres, 15)]
    #[test]
    fn ethan() {
        use chrono::{NaiveDate, NaiveDateTime, NaiveTime, TimeZone};

        let mut client = config().connect(NoTls).unwrap();
        let re = Regex::new("").unwrap();

        // 0001-01-01 00:00:00-00:00:01
        let time = FixedOffset::east_opt(1)
            .unwrap()
            .from_local_datetime(&NaiveDateTime::new(
                NaiveDate::from_ymd_opt(-1, 1, 1).unwrap(),
                NaiveTime::from_hms_opt(0, 0, 1).unwrap(),
            ))
            .single()
            .unwrap();
        let mut bytes = BytesMut::new();
        time.put(&mut bytes);

        let time_string = String::from_utf8(bytes.to_vec()).unwrap();
        let field = TimestampField::Julian;
        let expr = format!("extract({field} FROM '{time_string}'::timestamptz)");
        compare_eval_numeric(expr.as_str(), &mut client, &re);
    }
}

/// Checks the comparison of two columns against PostgreSQL, for each pair of types and each
/// comparison operator and its negation. Readyset must fail to lower a pair of types that
/// PostgreSQL refuses to compare.
mod type_comparison {
    use dataflow_expression::{Expr, LowerContext};
    use readyset_data::{Collation, DfType};
    use readyset_errors::{internal, ReadySetResult};
    use readyset_sql::ast::{Column, Relation, SqlType, SqlTypeArbitraryOptions};
    use readyset_sql::postgres_literals::{
        arbitrary_postgres_literal, postgres_edge_literals, postgres_representative_types,
        postgres_setup_sql, POSTGRES_TEST_ENUM, POSTGRES_TEST_ENUM_LABELS,
    };
    use readyset_sql::{Dialect as SqlDialect, DialectDisplay};
    use readyset_sql_parsing::{parse_expr_with_config, ParsingPreset};

    use super::*;

    const OPS: [&str; 8] = [
        "=",
        "<>",
        "<",
        "<=",
        ">",
        ">=",
        "IS DISTINCT FROM",
        "IS NOT DISTINCT FROM",
    ];

    fn connect() -> Client {
        let mut client = config().connect(NoTls).unwrap();
        client
            .simple_query(&format!("SET TIME ZONE 'UTC'; {}", postgres_setup_sql()))
            .unwrap();
        client
    }

    fn type_name(ty: &SqlType) -> String {
        ty.display(SqlDialect::PostgreSQL).to_string()
    }

    /// The type that Readyset gives a replicated column of `ty`. The test enum resolves to its
    /// labels, and a string type takes the database's collation.
    fn df_type(client: &mut Client, ty: &SqlType) -> ReadySetResult<DfType> {
        let dialect = readyset_data::Dialect::DEFAULT_POSTGRESQL;
        let resolve = |relation: Relation| {
            (relation.schema.is_none() && relation.name.as_str() == POSTGRES_TEST_ENUM).then(|| {
                DfType::from_enum_variants(
                    POSTGRES_TEST_ENUM_LABELS
                        .iter()
                        .map(|l| l.to_string())
                        .collect::<Vec<_>>(),
                    None,
                )
            })
        };
        let datcollate: String = client
            .query_one(
                "SELECT datcollate FROM pg_database WHERE datname = current_database()",
                &[],
            )
            .unwrap()
            .get(0);
        let collation = Collation::get_or_default(dialect, &datcollate);
        DfType::from_sql_type(ty, dialect, resolve, Some(collation))
    }

    fn is_number(ty: &SqlType) -> bool {
        use SqlType::*;
        matches!(
            ty,
            SmallInt(_)
                | Int2
                | Int(_)
                | Int4
                | Serial
                | BigInt(_)
                | Int8
                | BigSerial
                | Real
                | Double
                | Float
                | Numeric(_)
                | Decimal(..)
        )
    }

    #[derive(Debug, Clone)]
    struct Columns(DfType, DfType);

    impl LowerContext for Columns {
        fn resolve_column(&self, col: Column) -> ReadySetResult<(usize, DfType)> {
            match col.table.as_ref().map(|t| t.name.as_str()) {
                Some("l") => Ok((0, self.0.clone())),
                Some("r") => Ok((1, self.1.clone())),
                _ => internal!("unexpected column {col:?}"),
            }
        }

        fn resolve_type(&self, _ty: Relation) -> Option<DfType> {
            None
        }
    }

    /// One side of a comparison: a column type and values of that type, each with its literal.
    struct Operand {
        sql: SqlType,
        df: DfType,
        values: Vec<(String, DfValue)>,
    }

    /// Reads `literals` of type `ty` from PostgreSQL, except those that PostgreSQL rejects, such as
    /// a NUMERIC infinity before PostgreSQL 14. `None` if no value remains.
    fn operand(
        client: &mut Client,
        ty: &SqlType,
        literals: &[String],
        findings: &mut Vec<String>,
    ) -> Option<Operand> {
        let df = df_type(client, ty)
            .map_err(|e| findings.push(format!("unrepresentable type {}: {e}", type_name(ty))))
            .ok()?;
        let mut values = Vec::new();
        for lit in literals {
            let row = match client.query_one(&format!("SELECT {lit}"), &[]) {
                Ok(row) => row,
                Err(e) if e.as_db_error().is_some() => continue,
                Err(e) => panic!("PostgreSQL failed on {lit}: {e}"),
            };
            // Convert what `FromSql` reads to what a column stores: an enum label to its variant,
            // and a string to its column's collation.
            let value = row.try_get::<_, DfValue>(0).map(|v| match &df {
                DfType::Enum { .. } => v.coerce_to(&df, &DfType::Unknown).unwrap(),
                DfType::Array(element) if matches!(**element, DfType::Enum { .. }) => {
                    v.coerce_to(&df, &DfType::Unknown).unwrap()
                }
                DfType::Text(collation)
                | DfType::VarChar(_, collation)
                | DfType::Char(_, collation) => match <&str>::try_from(&v) {
                    Ok(text) => DfValue::from_str_and_collation(text, *collation),
                    Err(_) => v,
                },
                _ => v,
            });
            match value {
                Ok(value) => values.push((lit.clone(), value)),
                Err(e) => findings.push(format!("unrepresentable {lit}: {e}")),
            }
        }
        (!values.is_empty()).then(|| Operand {
            sql: ty.clone(),
            df,
            values,
        })
    }

    /// Each operator and its negation, over `l.v` and `r.v`, or over rows that carry them.
    fn predicates(rows: bool) -> Vec<String> {
        let operands = |op: &str| {
            if rows {
                format!("ROW(l.v, 1) {op} ROW(r.v, 1)")
            } else {
                format!("l.v {op} r.v")
            }
        };
        OPS.iter()
            .map(|op| operands(op))
            .chain(OPS.iter().map(|op| format!("NOT ({})", operands(op))))
            .collect()
    }

    /// Compares every value of `left` with every value of `right`, under each operator and its
    /// negation, and records every result that differs from PostgreSQL's. With `rows` set, each
    /// comparison is between rows that carry the two values.
    fn compare(
        client: &mut Client,
        left: &Operand,
        right: &Operand,
        rows: bool,
        findings: &mut Vec<String>,
    ) {
        let pair = if rows {
            format!(
                "ROW({}) vs ROW({})",
                type_name(&left.sql),
                type_name(&right.sql)
            )
        } else {
            format!("{} vs {}", type_name(&left.sql), type_name(&right.sql))
        };
        let ctx = Columns(left.df.clone(), right.df.clone());
        let mut checked = Vec::new();
        for pred in predicates(rows) {
            let ast = parse_expr_with_config(
                ParsingPreset::BothPreferSqlparser,
                readyset_sql::Dialect::PostgreSQL,
                &pred,
            )
            .unwrap();
            let expr = Expr::lower(ast, dataflow_expression::Dialect::DEFAULT_POSTGRESQL, &ctx);
            let probe = format!(
                "SELECT {pred} FROM (VALUES ({})) AS l(v), (VALUES ({})) AS r(v)",
                left.values[0].0, right.values[0].0
            );
            match (client.prepare(&probe), expr) {
                (Ok(_), Ok(expr)) => checked.push((pred, expr)),
                (Ok(_), Err(e)) => findings.push(format!("refused {pair}: {pred}: {e}")),
                (Err(_), Ok(_)) => {
                    findings.push(format!("accepted {pair}: {pred}, which PostgreSQL rejects"))
                }
                (Err(_), Err(_)) => {}
            }
        }
        if checked.is_empty() {
            return;
        }

        let values = |vals: &[(String, DfValue)]| {
            vals.iter()
                .enumerate()
                .map(|(i, (lit, _))| format!("({i}, {lit})"))
                .collect::<Vec<_>>()
                .join(", ")
        };
        let preds = checked.iter().map(|(p, _)| p.as_str()).collect::<Vec<_>>();
        let query = format!(
            "SELECT l.i, r.i, {} FROM (VALUES {}) AS l(i, v) CROSS JOIN (VALUES {}) AS r(i, v)",
            preds.join(", "),
            values(&left.values),
            values(&right.values),
        );
        let rows = match client.query(&query, &[]) {
            Ok(rows) => rows,
            // PostgreSQL fails to evaluate a comparison, as with a NUMERIC outside a float's range.
            Err(e) if e.as_db_error().is_some() => return,
            Err(e) => panic!("PostgreSQL failed `{query}`: {e}"),
        };
        for row in rows {
            let l = &left.values[row.get::<_, i32>(0) as usize];
            let r = &right.values[row.get::<_, i32>(1) as usize];
            let record = [l.1.clone(), r.1.clone()];
            for (i, (pred, expr)) in checked.iter().enumerate() {
                let expected: Option<bool> = row.get(i + 2);
                let ours = expr
                    .eval::<DfValue>(&record)
                    .map(|v| (!v.is_none()).then(|| v.is_truthy()));
                if ours.as_ref().ok() != Some(&expected) {
                    findings.push(format!(
                        "wrong {pair}: {pred} with l.v = {}, r.v = {}: postgres {expected:?}, \
                         readyset {ours:?}",
                        l.0, r.0
                    ));
                }
            }
        }
    }

    fn assert_no_findings(findings: &[String]) {
        assert!(
            findings.is_empty(),
            "{} findings:\n{}",
            findings.len(),
            findings.join("\n")
        );
    }

    fn edge_values_compare_as_in_postgres(keep: fn(&SqlType) -> bool) {
        let mut client = connect();
        let mut findings = Vec::new();
        let types = postgres_representative_types()
            .iter()
            .filter(|ty| keep(ty))
            .filter_map(|ty| operand(&mut client, ty, &postgres_edge_literals(ty), &mut findings))
            .collect::<Vec<_>>();
        for left in &types {
            for right in &types {
                compare(&mut client, left, right, false, &mut findings);
            }
        }
        assert_no_findings(&findings);
    }

    fn arbitrary_values_compare_as_in_postgres(keep: fn(&SqlType) -> bool, rows: bool) {
        let typed_literal = any_with::<SqlType>(SqlTypeArbitraryOptions {
            dialect: SqlDialect::PostgreSQL,
            ..Default::default()
        })
        .prop_filter("kept", keep)
        .prop_flat_map(|ty| {
            let literal =
                arbitrary_postgres_literal(&ty).unwrap_or_else(|| panic!("no literals for {ty:?}"));
            (Just(ty), literal)
        });
        let client = RefCell::new(connect());
        proptest!(|((lt, l) in typed_literal.clone(), (rt, r) in typed_literal)| {
            let mut client = client.borrow_mut();
            let mut findings = Vec::new();
            let left = operand(&mut client, &lt, &[l], &mut findings);
            let right = operand(&mut client, &rt, &[r], &mut findings);
            if let (Some(left), Some(right)) = (left, right) {
                compare(&mut client, &left, &right, rows, &mut findings);
            }
            // Acceptance depends only on the pair of types, which the edge-value tests cover.
            findings.retain(|f| !f.starts_with("accepted"));
            prop_assert!(findings.is_empty(), "{}", findings.join("\n"));
        });
    }

    #[upstream(postgres, all)]
    #[test]
    fn number_edge_values() {
        edge_values_compare_as_in_postgres(is_number);
    }

    #[upstream(postgres, all)]
    #[test]
    fn arbitrary_number_values() {
        arbitrary_values_compare_as_in_postgres(is_number, false);
    }

    #[upstream(postgres, all)]
    #[test]
    fn arbitrary_number_row_values() {
        arbitrary_values_compare_as_in_postgres(is_number, true);
    }

    /// Compares rows that carry one value of each number type, position by position.
    fn number_rows_compare_as_in_postgres() {
        let mut client = connect();
        let mut findings = Vec::new();
        let types = postgres_representative_types()
            .iter()
            .filter(|ty| is_number(ty))
            .filter_map(|ty| operand(&mut client, ty, &postgres_edge_literals(ty), &mut findings))
            .collect::<Vec<_>>();
        for left in &types {
            for right in &types {
                compare(&mut client, left, right, true, &mut findings);
            }
        }
        assert_no_findings(&findings);
    }

    #[upstream(postgres, all)]
    #[test]
    fn number_row_values() {
        number_rows_compare_as_in_postgres();
    }

    #[upstream(postgres, all)]
    #[ignore = "REA-7274: Readyset disagrees with PostgreSQL on some type comparisons"]
    #[test]
    fn edge_values() {
        edge_values_compare_as_in_postgres(|_| true);
    }

    #[upstream(postgres, all)]
    #[ignore = "REA-7274: Readyset disagrees with PostgreSQL on some type comparisons"]
    #[test]
    fn arbitrary_values() {
        arbitrary_values_compare_as_in_postgres(|_| true, false);
    }
}
