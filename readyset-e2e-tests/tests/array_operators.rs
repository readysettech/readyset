use std::panic::AssertUnwindSafe;

use readyset_client_test_helpers::{TestBuilder, psql_helpers};
use readyset_sql_parsing::ParsingPreset;
use readyset_util::eventually;
use test_utils::tags;

/// Creates the shared test table and inserts rows via the upstream connection.
async fn setup_array_table(upstream_conn: &tokio_postgres::Client) {
    upstream_conn
        .execute("CREATE TABLE t (id int, arr int[])", &[])
        .await
        .unwrap();

    upstream_conn
        .execute(
            "INSERT INTO t VALUES
                (1, ARRAY[1,2]),
                (2, ARRAY[1,2,3]),
                (3, ARRAY[10]),
                (4, ARRAY[1,3]),
                (5, '{}')",
            &[],
        )
        .await
        .unwrap();
}

/// Tests array comparison operators: =, <>, <, >, <=, >=
#[tokio::test]
#[tags(serial, slow, postgres_upstream)]
async fn array_comparison_postgres() {
    readyset_tracing::init_test_logging();
    let (rs_opts, _handle, shutdown_tx) = TestBuilder::default()
        .parsing_preset(ParsingPreset::OnlySqlparser)
        .build::<psql_helpers::PostgreSQLAdapter>()
        .await;
    let rs_conn = psql_helpers::connect(rs_opts).await;

    let mut upstream_config = psql_helpers::upstream_config();
    upstream_config.dbname("noria");
    let upstream_conn = psql_helpers::connect(upstream_config).await;

    setup_array_table(&upstream_conn).await;

    let queries: Vec<(&str, &str)> = vec![
        (
            "SELECT id FROM t WHERE arr = ARRAY[1, 2]",
            "equality with ARRAY literal",
        ),
        (
            "SELECT id FROM t WHERE arr = '{1,2}'::int[]",
            "equality with cast string literal",
        ),
        (
            "SELECT id FROM t WHERE arr <> ARRAY[1, 2]",
            "inequality",
        ),
        (
            "SELECT id FROM t WHERE arr > ARRAY[1, 2]",
            "greater-than",
        ),
        (
            "SELECT id FROM t WHERE arr >= ARRAY[1, 2]",
            "greater-than-or-equal",
        ),
        (
            "SELECT id FROM t WHERE arr < ARRAY[1, 2, 3]",
            "less-than",
        ),
        (
            "SELECT id FROM t WHERE arr <= ARRAY[1, 2, 3]",
            "less-than-or-equal",
        ),
        (
            "SELECT id FROM t WHERE arr < ARRAY[1]",
            "empty array is less than any non-empty array",
        ),
        (
            "SELECT id FROM t WHERE arr > ARRAY[1, 2, 3]",
            "greater-than with longer array",
        ),
    ];

    for (query, description) in &queries {
        let upstream_rows = upstream_conn.query(*query, &[]).await.unwrap();
        let mut expected: Vec<i32> = upstream_rows.iter().map(|r| r.get(0)).collect();
        expected.sort();

        eventually!(run_test: {
            let rs_rows = rs_conn.query(*query, &[]).await;
            AssertUnwindSafe(|| { rs_rows })
        }, then_assert: |result| {
            let rs_rows = result().unwrap();
            let mut actual: Vec<i32> = rs_rows.iter().map(|r| r.get(0)).collect();
            actual.sort();
            assert_eq!(actual, expected, "{description}");
        });
    }

    shutdown_tx.shutdown().await;
}
