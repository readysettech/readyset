use std::panic::AssertUnwindSafe;

use mysql_async::{Conn, Row, prelude::Queryable};
use readyset_client_test_helpers::{
    TestBuilder,
    mysql_helpers::{self, MySQLAdapter},
    psql_helpers,
};
use readyset_sql::{Dialect, DialectDisplay, ast::SqlType};
use readyset_sql_parsing::ParsingPreset;
use readyset_util::eventually;
use test_utils::tags;

async fn test_aggregation_type_inner_postgres(
    expr: &str,
    column_type: SqlType,
    values: &[&str],
    is_window: bool,
) {
    readyset_tracing::init_test_logging();
    let mut builder = TestBuilder::default();
    if is_window {
        builder = builder.parsing_preset(ParsingPreset::OnlySqlparser);
    }
    let (rs_opts, _handle, shutdown_tx) = builder.build::<psql_helpers::PostgreSQLAdapter>().await;
    let rs_conn = psql_helpers::connect(rs_opts).await;

    let mut upstream_config = psql_helpers::upstream_config();
    upstream_config.dbname("noria");
    let upstream_conn = psql_helpers::connect(upstream_config).await;

    upstream_conn
        .execute(
            &format!(
                "CREATE TABLE t (x {})",
                column_type.display(Dialect::PostgreSQL)
            ),
            &[],
        )
        .await
        .unwrap();

    if !values.is_empty() {
        let values_clause = values
            .iter()
            .map(|v| format!("({v})"))
            .collect::<Vec<_>>()
            .join(", ");
        upstream_conn
            .execute(&format!("INSERT INTO t (x) VALUES {values_clause}"), &[])
            .await
            .unwrap();
    }

    let window_clause = if is_window { " OVER ()" } else { "" };
    let order_clause = if is_window { " ORDER BY x" } else { "" };
    let query = format!("SELECT {expr}{window_clause} FROM t{order_clause}");

    let upstream_rows = upstream_conn.query(&query, &[]).await.unwrap();
    let upstream_type = upstream_rows
        .first()
        .map(|row| row.columns()[0].type_().clone());
    let upstream_values: Vec<Vec<u8>> = upstream_rows
        .iter()
        .map(|row| row.body().buffer().to_vec())
        .collect();

    eventually!(run_test: {
        let query = query.clone();
        let rs_rows = rs_conn.query(&query, &[]).await;
        AssertUnwindSafe(|| { rs_rows })
    }, then_assert: |result| {
        let rs_rows = result().unwrap();
        assert_eq!(upstream_values.len(), rs_rows.len());

        match (upstream_type.as_ref(), rs_rows.first()) {
            (Some(upstream_type), Some(rs_row)) => {
                assert_eq!(upstream_type, rs_row.columns()[0].type_());
            }
            (None, None) => {}
            _ => panic!("Result type mismatch between upstream and Readyset"),
        }

        let rs_values: Vec<Vec<u8>> = rs_rows
            .iter()
            .map(|row| row.body().buffer().to_vec())
            .collect();
        assert_eq!(upstream_values, rs_values);
    });

    shutdown_tx.shutdown().await;
}

async fn test_aggregation_type_inner_mysql(
    expr: &str,
    column_type: SqlType,
    values: &[&str],
    is_window: bool,
) {
    readyset_tracing::init_test_logging();
    let mut builder = TestBuilder::default();
    if is_window {
        builder = builder.parsing_preset(ParsingPreset::OnlySqlparser);
    }
    let (rs_opts, _handle, shutdown_tx) = builder.build::<MySQLAdapter>().await;
    let mut rs_conn = mysql_async::Conn::new(rs_opts).await.unwrap();

    let upstream_opts = mysql_helpers::upstream_config().db_name(Some("noria"));
    let mut upstream_conn = Conn::new(upstream_opts).await.unwrap();

    upstream_conn
        .query_drop(format!(
            "CREATE TABLE t (x {})",
            column_type.display(Dialect::MySQL)
        ))
        .await
        .unwrap();

    if !values.is_empty() {
        let values_clause = values
            .iter()
            .map(|v| format!("({v})"))
            .collect::<Vec<_>>()
            .join(", ");
        upstream_conn
            .query_drop(format!("INSERT INTO t (x) VALUES {values_clause}"))
            .await
            .unwrap();
    }

    let window_clause = if is_window { " OVER ()" } else { "" };
    let order_clause = if is_window { " ORDER BY x" } else { "" };
    let query = format!("SELECT {expr}{window_clause} FROM t{order_clause}");

    let upstream_rows: Vec<Row> = upstream_conn.query(query.clone()).await.unwrap();
    let upstream_type = upstream_rows
        .first()
        .map(|row| row.columns_ref()[0].column_type());
    let upstream_values: Vec<mysql_async::Value> =
        upstream_rows.iter().map(|row| row[0].clone()).collect();

    eventually!(run_test: {
        let query = query.clone();
        let rs_rows = rs_conn.query(query).await;
        AssertUnwindSafe(|| { rs_rows })
    }, then_assert: |result| {
        let rs_rows: Vec<Row> = result().unwrap();
        assert_eq!(upstream_values.len(), rs_rows.len());

        match (upstream_type, rs_rows.first()) {
            (Some(upstream_type), Some(rs_row)) => {
                assert_eq!(upstream_type, rs_row.columns_ref()[0].column_type());
            }
            (None, None) => {}
            _ => panic!("Result type mismatch between upstream and Readyset"),
        }

        let rs_values: Vec<mysql_async::Value> =
            rs_rows.iter().map(|row| row[0].clone()).collect();
        assert_eq!(upstream_values, rs_values);
    });

    shutdown_tx.shutdown().await;
}
macro_rules! test_aggregation_type {
    ($upstream:ident, $name:ident, $expr:expr, $coltype:expr) => {
        paste::paste! {
            #[tokio::test]
            #[tags(serial, slow, [<$upstream _upstream>])]
            async fn [<$name _ $upstream>]() {
                [<test_aggregation_type_inner_ $upstream>](
                    $expr,
                    $coltype,
                    &[],
                    false
                )
                .await;
            }
        }
    };
}

macro_rules! test_window_aggregation_type {
    (postgres, $name:ident, $expr:expr, $coltype:expr, [$($value:expr),+ $(,)?]) => {
        paste::paste! {
            #[tokio::test]
            #[tags(serial, slow, postgres_upstream)]
            async fn [<$name _window_postgres>]() {
                test_aggregation_type_inner_postgres(
                    $expr,
                    $coltype,
                    &[$($value),+],
                    true,
                )
                .await;
            }
        }
    };
    (mysql, $name:ident, $expr:expr, $coltype:expr, [$($value:expr),+ $(,)?]) => {
        paste::paste! {
            #[tokio::test]
            #[tags(serial, slow, mysql8_upstream)]
            async fn [<$name _window_mysql>]() {
                test_aggregation_type_inner_mysql(
                    $expr,
                    $coltype,
                    &[$($value),+],
                    true,
                )
                .await;
            }
        }
    };
}

test_aggregation_type!(postgres, avg_bigint, "avg(x)", SqlType::BigInt(None));
test_aggregation_type!(postgres, avg_float, "avg(x)", SqlType::Float);
test_aggregation_type!(postgres, avg_double, "avg(x)", SqlType::Double);
test_aggregation_type!(postgres, avg_numeric, "avg(x)", SqlType::Numeric(None));
test_aggregation_type!(
    postgres,
    avg_numeric_with_precision,
    "avg(x)",
    SqlType::Numeric(Some((10, None)))
);
test_aggregation_type!(
    postgres,
    avg_numeric_with_precision_and_scale,
    "avg(x)",
    SqlType::Numeric(Some((43, Some(16))))
);
test_aggregation_type!(postgres, avg_int2, "avg(x)", SqlType::Int2);
test_aggregation_type!(postgres, avg_int8, "avg(x)", SqlType::Int8);
test_aggregation_type!(postgres, avg_int, "avg(x)", SqlType::Int(None));

test_aggregation_type!(postgres, sum_float, "sum(x)", SqlType::Float);
test_aggregation_type!(postgres, sum_double, "sum(x)", SqlType::Double);
test_aggregation_type!(postgres, sum_numeric, "sum(x)", SqlType::Numeric(None));
test_aggregation_type!(
    postgres,
    sum_numeric_with_precision,
    "sum(x)",
    SqlType::Numeric(Some((10, None)))
);
test_aggregation_type!(
    postgres,
    sum_numeric_with_precision_and_scale,
    "sum(x)",
    SqlType::Numeric(Some((43, Some(16))))
);
test_aggregation_type!(postgres, sum_int2, "sum(x)", SqlType::Int2);
test_aggregation_type!(postgres, sum_int, "sum(x)", SqlType::Int(None));
test_aggregation_type!(postgres, sum_bigint, "sum(x)", SqlType::BigInt(None));
test_aggregation_type!(postgres, sum_int8, "sum(x)", SqlType::Int8);

test_aggregation_type!(postgres, count_bigint, "count(x)", SqlType::BigInt(None));
test_aggregation_type!(postgres, count_text, "count(x)", SqlType::Text);
test_aggregation_type!(postgres, count_float, "count(x)", SqlType::Float);

test_aggregation_type!(mysql, avg_float, "avg(x)", SqlType::Float);
test_aggregation_type!(mysql, avg_double, "avg(x)", SqlType::Double);
test_aggregation_type!(mysql, avg_numeric, "avg(x)", SqlType::Numeric(None));
test_aggregation_type!(mysql, avg_decimal, "avg(x)", SqlType::Decimal(43, 16));
test_aggregation_type!(mysql, avg_int, "avg(x)", SqlType::Int(None));
test_aggregation_type!(mysql, avg_bigint, "avg(x)", SqlType::BigInt(None));

test_aggregation_type!(mysql, sum_float, "sum(x)", SqlType::Float);
test_aggregation_type!(mysql, sum_double, "sum(x)", SqlType::Double);
test_aggregation_type!(mysql, sum_numeric, "sum(x)", SqlType::Numeric(None));
test_aggregation_type!(mysql, sum_decimal, "sum(x)", SqlType::Decimal(43, 16));
test_aggregation_type!(mysql, sum_int, "sum(x)", SqlType::Int(None));
test_aggregation_type!(mysql, sum_bigint, "sum(x)", SqlType::BigInt(None));

test_aggregation_type!(mysql, count_bigint, "count(x)", SqlType::BigInt(None));
test_aggregation_type!(mysql, count_text, "count(x)", SqlType::Text);
test_aggregation_type!(mysql, count_float, "count(x)", SqlType::Float);

test_window_aggregation_type!(
    postgres,
    sum_bigint,
    "sum(x)",
    SqlType::BigInt(None),
    ["5188155168561903705"]
);
test_window_aggregation_type!(
    mysql,
    sum_bigint,
    "sum(x)",
    SqlType::BigInt(None),
    ["5188155168561903705"]
);
