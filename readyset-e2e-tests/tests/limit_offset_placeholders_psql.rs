//! Checks that `LIMIT $1 OFFSET $2` gives the same rows, or the same error, through Readyset as on
//! Postgres, for integers at both ends of the range, zero, negatives and NULL.

use readyset_adapter::backend::MigrationMode;
use readyset_client_metrics::QueryDestination;
use readyset_client_test_helpers::psql_helpers::{self, PostgreSQLAdapter, last_query_info};
use readyset_client_test_helpers::{Adapter, TestBuilder, derive_test_name, sleep};
use test_utils::{tags, upstream};
use tokio_postgres::Client;

const SQL: &str = "SELECT v FROM t ORDER BY id LIMIT $1 OFFSET $2";

/// Runs the query and returns its rows, or the SQLSTATE if it fails.
async fn read(
    client: &Client,
    limit: Option<i64>,
    offset: Option<i64>,
) -> Result<Vec<i32>, String> {
    match client.query(SQL, &[&limit, &offset]).await {
        Ok(rows) => Ok(rows.iter().map(|row| row.get(0)).collect()),
        Err(err) => Err(err.code().expect("a database error").code().to_string()),
    }
}

#[tokio::test]
#[tags(serial)]
#[upstream(postgres)]
async fn a_count_reads_as_postgres_reads_it() {
    readyset_tracing::init_test_logging();
    let db = derive_test_name();
    PostgreSQLAdapter::recreate_database(&db).await;
    let mut config = psql_helpers::upstream_config();
    config.dbname(&db);
    let upstream = psql_helpers::connect(config).await;
    upstream
        .batch_execute(
            "CREATE TABLE t (id int PRIMARY KEY, v int); \
             INSERT INTO t VALUES (1, 10), (2, 20), (3, 30), (4, 40);",
        )
        .await
        .unwrap();
    let (opts, _handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .migration_mode(MigrationMode::OutOfBand)
        .fallback(true)
        .replicate_db(&db)
        .build::<PostgreSQLAdapter>()
        .await;
    let readyset = psql_helpers::connect(opts).await;
    readyset
        .simple_query(&format!("CREATE CACHE FROM {SQL}"))
        .await
        .unwrap();
    sleep().await;

    for (limit, offset) in [
        (Some(2), Some(1)),
        (None, Some(1)),
        (Some(2), None),
        (Some(-1), Some(0)),
        (Some(2), Some(-1)),
        (Some(i64::MIN), Some(0)),
        (Some(i64::MAX), Some(1)),
        (Some(2), Some(i64::MAX)),
        (Some(0), Some(0)),
    ] {
        assert_eq!(
            read(&readyset, limit, offset).await,
            read(&upstream, limit, offset).await,
            "LIMIT {limit:?} OFFSET {offset:?}"
        );
    }
    assert_eq!(read(&readyset, Some(2), Some(1)).await, Ok(vec![20, 30]));
    assert!(matches!(
        last_query_info(&readyset).await.destination,
        QueryDestination::Readyset(_)
    ));

    shutdown_tx.shutdown().await;
}
