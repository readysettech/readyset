//! Tests for reading+writing noria-data types to+from postgres
//!
//! All tests in this module assume a postgresql database running with the configuration provided by
//! `docker-compose.yml` and `docker-compose.override.exmaple`:
//!
//! ```console
//! $ cp docker-compose.override.yml.example docker-compose.override.yml
//! $ docker-compose up -d
//! ```

use std::env;

use ndarray::{ArrayD, IxDyn};
use readyset_data::{Array, DfValue};
use serial_test::serial;
use tokio_postgres::NoTls;

fn config() -> tokio_postgres::Config {
    let mut config = tokio_postgres::Config::new();
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

mod arrays {
    use super::*;

    async fn round_trip(type_name: &str, array: Array) {
        let (client, conn) = config().connect(NoTls).await.unwrap();
        tokio::spawn(conn);

        client
            .execute("DROP TABLE IF EXISTS array_test;", &[])
            .await
            .unwrap();

        client
            .execute(
                format!("CREATE TABLE array_test (v {type_name});").as_str(),
                &[],
            )
            .await
            .unwrap();

        client
            .execute("INSERT INTO array_test (v) values ($1)", &[&array])
            .await
            .unwrap();

        let res = client
            .query_one("SELECT v FROM array_test", &[])
            .await
            .unwrap()
            .get::<_, Array>(0);

        assert_eq!(res, array);
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn empty_array_to_from_sql() {
        round_trip("text[]", Array::from(vec![])).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn one_d_array_to_from_sql() {
        round_trip(
            "int[]",
            Array::from(vec![DfValue::from(1), DfValue::from(2)]),
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn two_d_array_to_from_sql() {
        round_trip(
            "int[][]",
            Array::from(
                ArrayD::from_shape_vec(
                    IxDyn(&[2, 2]),
                    vec![
                        DfValue::from(1),
                        DfValue::from(2),
                        DfValue::from(3),
                        DfValue::from(4),
                    ],
                )
                .unwrap(),
            ),
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn two_d_array_with_alternate_lower_bounds_to_from_sql() {
        round_trip(
            "int[][]",
            Array::from_lower_bounds_and_contents(
                vec![-5, 10],
                ArrayD::from_shape_vec(
                    IxDyn(&[2, 2]),
                    vec![
                        DfValue::from(1),
                        DfValue::from(2),
                        DfValue::from(3),
                        DfValue::from(4),
                    ],
                )
                .unwrap(),
            )
            .unwrap(),
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn two_d_string_array_to_from_sql() {
        round_trip(
            "text[][]",
            Array::from(
                ArrayD::from_shape_vec(
                    IxDyn(&[2, 2]),
                    vec![
                        DfValue::from("a"),
                        DfValue::from("b"),
                        DfValue::from("c"),
                        DfValue::from("d"),
                    ],
                )
                .unwrap(),
            ),
        )
        .await;
    }
}
