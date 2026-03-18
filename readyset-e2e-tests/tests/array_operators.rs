use std::panic::AssertUnwindSafe;

use readyset_client_test_helpers::{TestBuilder, psql_helpers};
use readyset_sql_parsing::ParsingPreset;
use readyset_util::eventually;
use test_utils::{tags, upstream};

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
#[tags(serial, slow)]
#[upstream(postgres13, postgres15)]
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
        ("SELECT id FROM t WHERE arr <> ARRAY[1, 2]", "inequality"),
        ("SELECT id FROM t WHERE arr > ARRAY[1, 2]", "greater-than"),
        (
            "SELECT id FROM t WHERE arr >= ARRAY[1, 2]",
            "greater-than-or-equal",
        ),
        ("SELECT id FROM t WHERE arr < ARRAY[1, 2, 3]", "less-than"),
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

/// Tests array containment operators: @> and <@
#[tokio::test]
#[tags(serial, slow)]
#[upstream(postgres13, postgres15)]
async fn array_containment_postgres() {
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
            "SELECT id FROM t WHERE arr @> ARRAY[1, 2]",
            "@> contains [1,2]",
        ),
        (
            "SELECT id FROM t WHERE arr @> ARRAY[10, 20]",
            "@> contains [10,20] (empty result)",
        ),
        (
            "SELECT id FROM t WHERE arr @> '{}'::int[]",
            "@> contains empty array (all rows)",
        ),
        (
            "SELECT id FROM t WHERE arr @> ARRAY[3, 1]",
            "@> set-based containment [3,1]",
        ),
        (
            "SELECT id FROM t WHERE arr <@ ARRAY[1, 2, 3]",
            "<@ contained by [1,2,3]",
        ),
        (
            "SELECT id FROM t WHERE arr <@ ARRAY[10]",
            "<@ contained by [10]",
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

/// Tests array overlap operator: &&
#[tokio::test]
#[tags(serial, slow)]
#[upstream(postgres13, postgres15)]
async fn array_overlap_postgres() {
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
            "SELECT id FROM t WHERE arr && ARRAY[2, 3]",
            "&& overlap with matching elements",
        ),
        (
            "SELECT id FROM t WHERE arr && ARRAY[99]",
            "&& overlap with no matching elements",
        ),
        (
            "SELECT id FROM t WHERE arr && '{}'::int[]",
            "&& overlap with empty array",
        ),
        (
            "SELECT id FROM t WHERE arr && ARRAY[1]",
            "&& overlap with multiple matches",
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

/// REA-6243: array_agg(int4_col + literal) must produce int4[], not int8[].
/// Without integer literal type narrowing, the literal gets BigInt, promoting the
/// expression to BigInt. The array then has 8-byte elements but the column type
/// declares int4[], causing wire protocol deserialization failure.
#[tokio::test]
#[tags(serial, slow)]
#[upstream(postgres13, postgres15)]
async fn array_agg_int_literal_type_postgres() {
    readyset_tracing::init_test_logging();
    let (rs_opts, _handle, shutdown_tx) = TestBuilder::default()
        .parsing_preset(ParsingPreset::OnlySqlparser)
        .build::<psql_helpers::PostgreSQLAdapter>()
        .await;
    let rs_conn = psql_helpers::connect(rs_opts).await;

    let mut upstream_config = psql_helpers::upstream_config();
    upstream_config.dbname("noria");
    let upstream_conn = psql_helpers::connect(upstream_config).await;

    upstream_conn
        .execute(
            "CREATE TABLE agg_nums (id int, grp int, val int)",
            &[],
        )
        .await
        .unwrap();

    upstream_conn
        .execute(
            "INSERT INTO agg_nums VALUES (1, 1, 10), (2, 1, 20), (3, 1, 30)",
            &[],
        )
        .await
        .unwrap();

    let query = "SELECT array_agg(val + 5) FROM agg_nums WHERE grp = $1";

    let upstream_rows = upstream_conn.query(query, &[&1i32]).await.unwrap();
    let expected: Vec<i32> = upstream_rows[0].get::<_, Vec<i32>>(0);

    eventually!(run_test: {
        let rs_rows = rs_conn.query(query, &[&1i32]).await;
        AssertUnwindSafe(|| { rs_rows })
    }, then_assert: |result| {
        let rs_rows = result().unwrap();
        let actual: Vec<i32> = rs_rows[0].get::<_, Vec<i32>>(0);
        assert_eq!(actual, expected, "array_agg(int + literal) type mismatch");
    });

    shutdown_tx.shutdown().await;
}

/// Tests string concatenation with the `||` operator
#[tokio::test]
#[tags(serial, slow)]
#[upstream(postgres13, postgres15)]
async fn string_concat_postgres() {
    readyset_tracing::init_test_logging();
    let (rs_opts, _handle, shutdown_tx) = TestBuilder::default()
        .parsing_preset(ParsingPreset::OnlySqlparser)
        .build::<psql_helpers::PostgreSQLAdapter>()
        .await;
    let rs_conn = psql_helpers::connect(rs_opts).await;

    let mut upstream_config = psql_helpers::upstream_config();
    upstream_config.dbname("noria");
    let upstream_conn = psql_helpers::connect(upstream_config).await;

    upstream_conn
        .execute(
            "CREATE TABLE str_t (id int, col1 text, col2 varchar(50))",
            &[],
        )
        .await
        .unwrap();

    upstream_conn
        .execute(
            "INSERT INTO str_t VALUES
                (1, 'hello', ' world'),
                (2, 'foo', 'bar'),
                (3, '', 'empty')",
            &[],
        )
        .await
        .unwrap();

    // Test projection: SELECT col1 || col2 FROM str_t
    {
        let query = "SELECT col1 || col2 FROM str_t ORDER BY id";
        let upstream_rows = upstream_conn.query(query, &[]).await.unwrap();
        let expected: Vec<String> = upstream_rows.iter().map(|r| r.get(0)).collect();

        eventually!(run_test: {
            let rs_rows = rs_conn.query(query, &[]).await;
            AssertUnwindSafe(|| { rs_rows })
        }, then_assert: |result| {
            let rs_rows = result().unwrap();
            let actual: Vec<String> = rs_rows.iter().map(|r| r.get(0)).collect();
            assert_eq!(actual, expected, "string concat in projection");
        });
    }

    // Test WHERE clause: SELECT id FROM str_t WHERE col1 || col2 = 'hello world'
    {
        let query = "SELECT id FROM str_t WHERE col1 || col2 = 'hello world'";
        let upstream_rows = upstream_conn.query(query, &[]).await.unwrap();
        let mut expected: Vec<i32> = upstream_rows.iter().map(|r| r.get(0)).collect();
        expected.sort();

        eventually!(run_test: {
            let rs_rows = rs_conn.query(query, &[]).await;
            AssertUnwindSafe(|| { rs_rows })
        }, then_assert: |result| {
            let rs_rows = result().unwrap();
            let mut actual: Vec<i32> = rs_rows.iter().map(|r| r.get(0)).collect();
            actual.sort();
            assert_eq!(actual, expected, "string concat in WHERE clause");
        });
    }

    shutdown_tx.shutdown().await;
}
