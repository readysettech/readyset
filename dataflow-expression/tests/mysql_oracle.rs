use std::env;

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Opts, OptsBuilder, Row};
use pretty_assertions::assert_eq;
use readyset_data::{DfType, DfValue};
use readyset_sql_parsing::parse_sql_type;
use readyset_util::retry_with_exponential_backoff;
use test_utils::tags;

use self::common::parse_lower_eval;

mod common;

fn opts() -> Opts {
    OptsBuilder::default()
        .ip_or_hostname(env::var("MYSQL_HOST").unwrap_or_else(|_| "127.0.0.1".into()))
        .tcp_port(
            env::var("MYSQL_TCP_PORT")
                .unwrap_or_else(|_| "3306".into())
                .parse()
                .unwrap(),
        )
        .user(Some(
            env::var("MYSQL_USER").unwrap_or_else(|_| "root".into()),
        ))
        .pass(Some(
            env::var("MYSQL_PASSWORD").unwrap_or_else(|_| "noria".into()),
        ))
        .db_name(Some(
            env::var("MYSQL_DATABASE").unwrap_or_else(|_| "noria".into()),
        ))
        .prefer_socket(false)
        .into()
}

async fn connect() -> Conn {
    retry_with_exponential_backoff!(
        { Conn::new(opts()).await },
        retries: 10,
        delay: 100,
        backoff: 2,
    )
    .unwrap()
}

async fn mysql_eval(expr: &str, conn: &mut Conn) -> DfValue {
    // We currently don't properly support the default mysql case-insensitive collation, so make
    // sure we change the collation of the database to be case-sensitive and respect padding
    conn.query_drop("SET NAMES 'utf8mb4' COLLATE 'utf8mb4_bin'")
        .await
        .unwrap();

    // Because mysql is much more weakly typed than postgres, it returns results for queries using
    // values of different types than the actual results of expressions; eg, `1 = 1` returns
    // `Value::Bytes("1")` instead of `Value::Int(1)`. To work around comparison issues, we first
    // determine the type mysql has inferred for the expression (for which the seemingly simplest
    // way is to create a temporary table with that expression as its only column, then look at the
    // type of that column) and then use that to coerce the result value from actually evaluating
    // the expression
    conn.query_drop("DROP TABLE IF EXISTS t").await.unwrap();
    conn.query_drop(format!("CREATE TEMPORARY TABLE t AS SELECT {expr} AS r"))
        .await
        .unwrap();
    let col: Row = conn
        .query_first("SHOW COLUMNS FROM t;")
        .await
        .unwrap()
        .unwrap();
    let raw_type: String = col.get("Type").unwrap();
    let sql_type = parse_sql_type(readyset_sql::Dialect::MySQL, raw_type).unwrap();
    let target_type = DfType::from_sql_type(
        &sql_type,
        dataflow_expression::Dialect::DEFAULT_MYSQL,
        |_| None,
        None,
    )
    .unwrap();

    let res: DfValue = conn
        .query_first::<mysql_async::Value, _>(format!("SELECT {expr}"))
        .await
        .unwrap()
        .unwrap()
        .try_into()
        .unwrap();

    res.coerce_to(&target_type, &DfType::Unknown).unwrap()
}

async fn compare_eval(expr: &str, conn: &mut Conn) {
    let mysql_result = mysql_eval(expr, conn).await;
    let our_result = parse_lower_eval(
        expr,
        readyset_sql::Dialect::MySQL,
        dataflow_expression::Dialect::DEFAULT_MYSQL,
    )
    .unwrap_or_else(|e| panic!("Error evaluating `{expr}`: {e}"));
    assert_eq!(
        our_result, mysql_result,
        "mismatched results for {expr} (left: us, right: mysql)"
    );
}

#[tokio::test]
#[tags(serial, mysql_upstream)]
async fn example_exprs_eval_same_as_mysql() {
    let mut conn = connect().await;

    for expr in [
        "1 != 2",
        "1 != 1",
        "-1 = -1",
        "-1.0 = -1.0",
        "-1 = -1.0",
        "1 = --1",
        "1 != -1",
        "1.0 = --1.0",
        "1.0 != -1.0",
        "4 + 5",
        "4 + '5'",
        "5 > 4",
        "'5' > 4",
        "5 > '4'",
        "'a' like 'A'",
        "'a' not like 'a'",
        "1 like 2",
        "1 = '1'",
        "'1' = 1",
        "'5.0' = 5",
        "5 + '5.0'",
        "'5.0' + 5",
        "'7' + 5",
        "5 < '5.0'",
        "'5.0' > 5",
        "'2004-01-01 12:00:00' = convert_tz('2004-01-01 12:00:00','GMT','MET')",
        "'asdfadsf' = 'asdfadsf'",
        "'12345' > '00012345'",
        "timediff('2004-01-01 12:30:00', '2004-01-01 12:00:00') = '00:30:00'",
        "convert_tz('2004-01-01 12:00:00','GMT','MET')",
        "convert_tz('asdfadsf','asdf','MET')",
        "convert_tz('asdfadsf','asdf',null)",
        // "convert_tz('2004-01-01 12:00:00','+00:00','+10:00')", TODO(ENG-2761)
        "dayofweek('2022-03-24')",
        "dayofweek('2022-03-24 12:00:00')",
        "dayofweek(null)",
        "month('2022-03-24')",
        "month(null)",
        "not -(1)",
        "null = null",
        "null != null",
        "1 = null",
        "null != 1",
        "substring('abcdef', 3)",
        "substring('abcdef', 3, 2)",
        "substring('abcdef', 1, 3)",
        "substring('abcdef', '1', '3')",
        "concat(1,2)",
        "concat('one',2)",
        "concat('one',2)",
        "concat('one',2,'three')",
        "concat('a','b')",
        "concat('a')",
        "concat_ws(',', 'First name', 'Second name', 'Last Name')",
        "concat_ws(',', '', 'b', 'c')",
        "concat_ws(',', 'First name', null, 'Last Name')",
        "concat_ws(',', null, 'First name', 'Last Name')",
        r"'foo\bar'",
        r"'foo\%bar'",
        r"'foo\_bar'",
        r"'a_b' like 'a\_b'",
        r"'a_b' like 'a\%b'",
        r"'a%b' like 'a\_b'",
        r"'a%b' like 'a\%b'",
        r"'a%b' like 'a\\_b'",
        r"'a%b' like 'a\\%b'",
        r"'a\b' like 'a\\b'",
        "CAST(-1.0 AS SIGNED)",
        "CAST(CAST(-1 AS DECIMAL) AS SIGNED)",
        "CAST(-1 AS SIGNED)",
        "CAST(2 AS SIGNED)",
        "CAST(2.5 AS SIGNED)",
        "CAST(3.5 AS SIGNED)",
        "CAST(-2.5 AS SIGNED)",
        "CAST(-3.5 AS SIGNED)",
        "CAST(2.4 AS SIGNED)",
        "CAST(3.4 AS SIGNED)",
        "CAST(-2.4 AS SIGNED)",
        "CAST(-3.4 AS SIGNED)",
        "CAST(2.6 AS SIGNED)",
        "CAST(3.6 AS SIGNED)",
        "CAST(-2.6 AS SIGNED)",
        "CAST(-3.6 AS SIGNED)",
        "CAST(2 AS UNSIGNED)",
        "CAST(2.5 AS UNSIGNED)",
        "CAST(3.5 AS UNSIGNED)",
        "CAST(2.4 AS UNSIGNED)",
        "CAST(3.4 AS UNSIGNED)",
        "CAST(2.6 AS UNSIGNED)",
        "CAST(3.6 AS UNSIGNED)",
    ] {
        compare_eval(expr, &mut conn).await;
    }
}

#[tokio::test]
#[tags(serial, mysql8_upstream)]
async fn example_exprs_eval_same_as_mysql8() {
    let mut conn = connect().await;

    for expr in [
        "json_overlaps(null, null)",
        "json_overlaps(null, '[]')",
        "json_overlaps('[]', null)",
        "json_overlaps('[]', '[]')",
        "json_overlaps('true', 'true')",
        "json_overlaps('[42]', '[0, 42, 0]')",
    ] {
        compare_eval(expr, &mut conn).await;
    }
}
