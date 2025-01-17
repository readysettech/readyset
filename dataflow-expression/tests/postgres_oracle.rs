use std::cell::RefCell;
use std::env;
use std::sync::Arc;

use chrono::{DateTime, FixedOffset};
use dataflow_expression::DateTruncPrecision;
use nom_sql::TimestampField;
use postgres::{Client, Config, NoTls};
use proptest::prelude::*;
use readyset_data::DfValue;
use readyset_util::arbitrary::{arbitrary_date_time_timezone, arbitrary_timestamp_naive_date_time};

use self::common::parse_lower_eval;

mod common;

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

fn postgres_eval(expr: &str, client: &mut Client) -> Result<DfValue, anyhow::Error> {
    Ok(client.query_one(&format!("SELECT {expr};"), &[])?.get(0))
}

fn compare_eval(expr: &str, client: &mut Client) {
    let our_result = parse_lower_eval(
        expr,
        nom_sql::Dialect::PostgreSQL,
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
        "split_part('a.b.c', '.', -1)",
        "split_part('a.b.c', '.', -4)",
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
#[test]
fn date_trunc_timestamp_no_opt_tz() {
    let client = RefCell::new(config().connect(NoTls).unwrap());

    proptest!(| (precision: DateTruncPrecision, datetime in arbitrary_timestamp_naive_date_time()) | {
        let mut client = client.borrow_mut();
        let expr = format!("date_trunc('{}', '{}'::timestamp)", precision, datetime);
        compare_eval(expr.as_str(), &mut client);
    });
}

/// Test the `date_trunc` built-in function. Passes a timestamp with
/// timezone information, and does not pass the optional timezone for
/// normalized conversions.
#[test]
fn date_trunc_timestamptz_no_opt_tz() {
    let client = RefCell::new(config().connect(NoTls).unwrap());

    proptest!(| (precision: DateTruncPrecision, datetime in arbitrary_date_time_timezone()) | {
       let dt = normalize_offset(datetime);
       let mut client = client.borrow_mut();
       let expr = format!("date_trunc('{}', '{}'::timestamptz)", precision, dt);
       compare_eval(expr.as_str(), &mut client);
    });
}

/// Test the `date_trunc` built-in function. Passes a timestamp with
/// timezone information but casts to a timestamp, and does not pass
/// the optional timezone for normalized conversions.
#[test]
fn date_trunc_timestamptz_downcast_no_opt_tz() {
    let client = RefCell::new(config().connect(NoTls).unwrap());

    proptest!(| (precision: DateTruncPrecision, datetime in arbitrary_date_time_timezone()) | {
       let dt = normalize_offset(datetime);
       let mut client = client.borrow_mut();
       let expr = format!("date_trunc('{}', '{}'::timestamp)", precision, dt);
       compare_eval(expr.as_str(), &mut client);
    });
}

/// Test the `date_trunc` built-in function. Passes a timestamp without
/// timezone information but casts to a timestamptz, and does not pass
/// the optional timezone for normalized conversions.
#[test]
fn date_trunc_timestamp_upcast_no_opt_tz() {
    let client = RefCell::new(config().connect(NoTls).unwrap());

    proptest!(| (precision: DateTruncPrecision, datetime in arbitrary_timestamp_naive_date_time()) | {
       let mut client = client.borrow_mut();
       let expr = format!("date_trunc('{}', '{}'::timestamptz)", precision, datetime);
       compare_eval(expr.as_str(), &mut client);
    });
}

mod extract {
    use bytes::BytesMut;
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
            nom_sql::Dialect::PostgreSQL,
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
                let mut asserted = false;
                if re.is_match(&pg_err.to_string()) {
                    if let Some((our_err, pg_err)) =
                        extract_message_body(&our_err.to_string(), &pg_err.to_string())
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
                        pg_err.to_string(),
                        "mismatched error message for {expr} (left: us, right: postgres)"
                    );
                }
            }
            (Ok(_), Err(our_err)) => {
                panic!(
                    "We failed with error \"{}\", but Postgres succeeded",
                    our_err
                );
            }
            (Err(pg_err), Ok(_)) => {
                panic!(
                    "Postgres failed with error \"{}\", but we succeeded",
                    pg_err
                );
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
            let mut regex_text: String = format!(
                "ERROR:\\s*unit \"[a-z_]*\" not supported for type {}",
                type_name
            );
            if type_name.eq_ignore_ascii_case("time") || type_name.eq_ignore_ascii_case("timestamp")
            {
                regex_text.push_str(" without time zone");
            }
            Regex::new(&regex_text).unwrap()
        }
    }

    #[test]
    fn timestamptz() {
        let client = RefCell::new(config().connect(NoTls).unwrap());
        let re = build_regex_for_invalid_extract_call("timestamptz");

        proptest!(| (field: TimestampField, datetime in arbitrary_date_time_with_nanos()) | {
            let mut client = client.borrow_mut();
            let mut bytes = BytesMut::new();
            datetime.put(&mut bytes);
            let dt_string = String::from_utf8(bytes.to_vec()).unwrap();

            let expr = format!("extract({} FROM '{}'::timestamptz)", field, dt_string);
            compare_eval_numeric(expr.as_str(), &mut client, &re);
        });
    }

    #[test]
    fn timestamp() {
        let client = RefCell::new(config().connect(NoTls).unwrap());
        let re = build_regex_for_invalid_extract_call("timestamp");

        proptest!(| (field: TimestampField, datetime in arbitrary_naive_date_time_with_nanos()) | {
            let mut client = client.borrow_mut();
            let mut bytes = BytesMut::new();
            datetime.put(&mut bytes);
            let ts_string = String::from_utf8(bytes.to_vec()).unwrap();

            let expr = format!("extract({} FROM '{}'::timestamp)", field, ts_string);
            compare_eval_numeric(expr.as_str(), &mut client, &re);
        });
    }

    #[test]
    fn date() {
        let client = RefCell::new(config().connect(NoTls).unwrap());
        let re = build_regex_for_invalid_extract_call("date");

        proptest!(| (field: TimestampField, date in arbitrary_naive_date()) | {
            let mut client = client.borrow_mut();
            let mut bytes = BytesMut::new();
            date.put(&mut bytes);
            let date_string = String::from_utf8(bytes.to_vec()).unwrap();

            let expr = format!("extract({} FROM '{}'::date)", field, date_string);
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

                    let expr = format!("extract({} FROM '{}'::{})", field, date_string, type_name);
                    compare_eval_numeric(expr.as_str(), &mut client, &re);
                }
                _ => ()
            }
        });
    }

    #[test]
    fn time_from_date() {
        test_extract_time_from("date");
    }

    #[test]
    fn time_from_timestamp_with_date_only() {
        test_extract_time_from("timestamp");
    }

    #[test]
    fn time_from_timestamptz_with_date_only() {
        test_extract_time_from("timestamptz");
    }

    #[test]
    fn timeee() {
        let client = RefCell::new(config().connect(NoTls).unwrap());
        let re = build_regex_for_invalid_extract_call("time");

        proptest!(| (field: TimestampField, time in arbitrary_naive_time()) | {
            let mut client = client.borrow_mut();

            let expr = format!("extract({} FROM '{}'::time)", field, time);
            compare_eval_numeric(expr.as_str(), &mut client, &re);
        });
    }

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
        let expr = format!("extract({} FROM '{}'::timestamptz)", field, time_string);
        compare_eval_numeric(expr.as_str(), &mut client, &re);
    }
}
