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
        arbitrary_date_time, arbitrary_naive_date, arbitrary_naive_time,
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

        let our_result = parse_lower_eval(
            expr,
            nom_sql::Dialect::PostgreSQL,
            dataflow_expression::Dialect::DEFAULT_POSTGRESQL,
        );

        match postgres_eval(expr, client) {
            Ok(pg_result) => {
                assert_eq!(
                    truncate_dfvalue(&our_result.unwrap()),
                    truncate_dfvalue(&pg_result),
                    "mismatched results for {expr} (left: us, right: postgres)"
                );
            }
            Err(e) if re.is_match(&e.to_string()) => {
                assert!(our_result
                    .unwrap_err()
                    .to_string()
                    .contains("Cannot extract field"));
            }
            Err(e) => panic!("Error evaluating `{expr}`: {e}"),
        }
    }

    #[test]
    fn timestamptz() {
        let client = RefCell::new(config().connect(NoTls).unwrap());
        // TODO ethan rethink this regex thing
        let re = Regex::new("").unwrap();
        use chrono::Datelike;

        proptest!(ProptestConfig::with_cases(10_000), | (field: TimestampField, datetime in arbitrary_date_time()) | {
            let mut client = client.borrow_mut();
            let mut bytes = BytesMut::new();
            datetime.put(&mut bytes);
            let dt_string = String::from_utf8(bytes.to_vec()).unwrap();
            dbg!(&dt_string);

            let expr = format!("extract({} FROM '{}'::timestamptz)", field, dt_string);
            compare_eval_numeric(expr.as_str(), &mut client, &re);
        });
    }

    #[test]
    fn timestamp() {
        let client = RefCell::new(config().connect(NoTls).unwrap());
        let re = Regex::new("").unwrap();

        proptest!(ProptestConfig::with_cases(10_000), | (field: TimestampField, datetime in arbitrary_timestamp_naive_date_time()) | {
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
        let re = Regex::new("ERROR: date units \"[a-z_]*\" not supported").unwrap();

        proptest!(ProptestConfig::with_cases(10_000), | (field: TimestampField, date in arbitrary_naive_date()) | {
            let mut client = client.borrow_mut();
            let mut bytes = BytesMut::new();
            date.put(&mut bytes);
            let date_string = String::from_utf8(bytes.to_vec()).unwrap();

            let expr = format!("extract({} FROM '{}'::date)", field, date_string);
            compare_eval_numeric(expr.as_str(), &mut client, &re);
        });
    }

    #[test]
    fn timeee() {
        let client = RefCell::new(config().connect(NoTls).unwrap());
        let re = Regex::new("ERROR: \"time\" units \"[a-z_]*\" not recognized").unwrap();

        proptest!(ProptestConfig::with_cases(10_000), | (field: TimestampField, time in arbitrary_naive_time()) | {
            let mut client = client.borrow_mut();

            let expr = format!("extract({} FROM '{}'::time)", field, time);
            compare_eval_numeric(expr.as_str(), &mut client, &re);
        });
    }

    #[test]
    fn ethan() {
        use chrono::{NaiveDate, NaiveDateTime, NaiveTime, TimeZone};

        let mut client = config().connect(NoTls).unwrap();
        let re = Regex::new("ERROR: \"time\" units \"[a-z_]*\" not recognized").unwrap();

        // 0001-01-01 00:00:00-00:00:01
        let time = FixedOffset::east_opt(1)
            .unwrap()
            .from_local_datetime(&NaiveDateTime::new(
                NaiveDate::from_ymd_opt(-1, 1, 1).unwrap(),
                NaiveTime::from_hms_opt(0, 0, 1).unwrap(),
            ))
            .single()
            .unwrap();
        dbg!(&time);
        dbg!(&time.naive_local());
        dbg!(&time.naive_utc());
        let mut bytes = BytesMut::new();
        time.put(&mut bytes);

        let time_string = String::from_utf8(bytes.to_vec()).unwrap();
        let field = TimestampField::Julian;
        let expr = format!("extract({} FROM '{}'::timestamptz)", field, time_string);
        compare_eval_numeric(expr.as_str(), &mut client, &re);
    }
}
