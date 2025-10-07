use mysql_async::{Conn, Row, prelude::Queryable};
use readyset_client_test_helpers::{
    TestBuilder,
    mysql_helpers::{self, MySQLAdapter},
    psql_helpers,
};
use readyset_sql::{Dialect, DialectDisplay, ast::SqlType};
use readyset_util::eventually;
use test_utils::tags;

async fn test_aggregation_type_inner_postgres(expr: &str, column_type: SqlType) {
    readyset_tracing::init_test_logging();
    let (rs_opts, _handle, shutdown_tx) = TestBuilder::default()
        .build::<psql_helpers::PostgreSQLAdapter>()
        .await;
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

    let upstream_row = upstream_conn
        .query_one(&format!("SELECT {expr} FROM t"), &[])
        .await
        .unwrap();
    let upstream_type = upstream_row.columns()[0].type_();
    let upstream_value: Vec<u8> = upstream_row.body().buffer().to_vec();

    eventually!(run_test: {
        let rs_row: tokio_postgres::Row = rs_conn
            .query_one(&format!("SELECT {expr} FROM t"), &[])
            .await
            .unwrap();
        (rs_row.columns()[0].type_().clone(), rs_row.body().buffer().to_vec())
    }, then_assert: |result| {
        let (rs_type, rs_value): (tokio_postgres::types::Type, Vec<u8>) = result;
        assert_eq!(*upstream_type, rs_type);
        assert_eq!(upstream_value, rs_value);
    });

    shutdown_tx.shutdown().await;
}

async fn test_aggregation_type_inner_mysql(expr: &str, column_type: SqlType) {
    readyset_tracing::init_test_logging();
    let (rs_opts, _handle, shutdown_tx) = TestBuilder::default().build::<MySQLAdapter>().await;
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

    let upstream_row: Row = upstream_conn
        .query_first(format!("SELECT {expr} FROM t"))
        .await
        .unwrap()
        .unwrap();
    let upstream_type = upstream_row.columns_ref()[0].column_type();
    let upstream_value: mysql_async::Value = upstream_row.get(0).unwrap();

    eventually!(run_test: {
        let rs_row: Row = rs_conn
            .query_first(format!("SELECT {expr} FROM t"))
            .await
            .unwrap()
            .unwrap();
        (rs_row.columns_ref()[0].column_type(), rs_row.get(0).unwrap())
    }, then_assert: |result| {
        let (rs_type, rs_value): (mysql_async::consts::ColumnType, mysql_async::Value) = result;
        assert_eq!(upstream_type, rs_type);
        assert_eq!(upstream_value, rs_value);
    });

    shutdown_tx.shutdown().await;
}

macro_rules! test_aggregation_type {
    ($upstream:ident, $name:ident, $expr:expr, $coltype:expr) => {
        paste::paste! {
            #[tokio::test]
            #[tags(serial, slow, [<$upstream _upstream>])]
            async fn [<$name _ $upstream>]() {
                [<test_aggregation_type_inner_ $upstream>]($expr, $coltype).await;
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
