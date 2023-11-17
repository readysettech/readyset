use std::env;

use data_generator::{random_value_of_type, unique_value_of_type, value_of_type};
use mysql_async::prelude::Queryable;
use mysql_async::Value;
use nom_sql::{Dialect, DialectDisplay, SqlType};
use proptest::prop_assume;
use rand::rngs::mock::StepRng;
use serial_test::serial;
use test_strategy::proptest;
use test_utils::slow;

async fn mysql_connection() -> mysql_async::Conn {
    mysql_async::Conn::new(
        mysql_async::OptsBuilder::default()
            .ip_or_hostname(env::var("MYSQL_HOST").unwrap_or_else(|_| "127.0.0.1".to_owned()))
            .user(Some(
                env::var("MYSQL_USER").unwrap_or_else(|_| "root".to_owned()),
            ))
            .pass(Some(
                env::var("MYSQL_PASSWORD").unwrap_or_else(|_| "noria".to_owned()),
            ))
            .db_name(Some(
                env::var("MYSQL_DATABASE").unwrap_or_else(|_| "mysql".to_owned()),
            )),
    )
    .await
    .unwrap()
}

#[proptest]
#[serial]
#[slow]
fn value_of_type_always_valid(
    #[any(generate_arrays = false, dialect = Some(Dialect::MySQL))] ty: SqlType,
) {
    let val = value_of_type(&ty);
    eprintln!("value: {val:?}");
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async move {
        let mut conn = mysql_connection().await;
        conn.query_drop("DROP TABLE IF EXISTS t").await.unwrap();
        conn.query_drop(format!("CREATE TABLE t (x {})", ty.display(Dialect::MySQL)))
            .await
            .unwrap();
        conn.exec_drop(
            "INSERT INTO t (x) VALUES (?)",
            vec![Value::try_from(val).unwrap()],
        )
        .await
    })
    .unwrap();
}

#[proptest]
#[serial]
#[slow]
fn unique_value_of_type_always_valid(
    #[any(generate_arrays = false, dialect = Some(Dialect::MySQL))] ty: SqlType,
    #[strategy(0..=255u32)] idx: u32,
) {
    prop_assume!(!matches!(ty, SqlType::Bool));

    let val = unique_value_of_type(&ty, idx);
    eprintln!("value: {val:?}");
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async move {
        let mut conn = mysql_connection().await;
        conn.query_drop("DROP TABLE IF EXISTS t").await.unwrap();
        conn.query_drop(format!("CREATE TABLE t (x {})", ty.display(Dialect::MySQL)))
            .await
            .unwrap();
        conn.exec_drop(
            "INSERT INTO t (x) VALUES (?)",
            vec![Value::try_from(val).unwrap()],
        )
        .await
    })
    .unwrap();
}

#[proptest]
#[serial]
#[slow]
fn random_value_of_type_always_valid(
    #[any(generate_arrays = false, dialect = Some(Dialect::MySQL))] ty: SqlType,
    initial: u64,
    increment: u64,
) {
    prop_assume!(!matches!(ty, SqlType::Bool));

    let val = random_value_of_type(&ty, StepRng::new(initial, increment));
    eprintln!("value: {val:?}");
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async move {
        let mut conn = mysql_connection().await;
        conn.query_drop("DROP TABLE IF EXISTS t").await.unwrap();
        conn.query_drop(format!("CREATE TABLE t (x {})", ty.display(Dialect::MySQL)))
            .await
            .unwrap();
        conn.exec_drop(
            "INSERT INTO t (x) VALUES (?)",
            vec![Value::try_from(val).unwrap()],
        )
        .await
    })
    .unwrap();
}
