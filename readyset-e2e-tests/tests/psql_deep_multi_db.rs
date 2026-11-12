use std::mem;

use readyset_client_test_helpers::psql_helpers::{self, PostgreSQLAdapter};
use readyset_client_test_helpers::{Adapter, TestBuilder, derive_test_name};
use readyset_server::Handle;
use test_utils::{tags, upstream};

/// Create table `t` in `db` on the upstream.
async fn create_table(db: &str) {
    let mut config = psql_helpers::upstream_config();
    config.dbname(db);
    let upstream = psql_helpers::connect(config).await;
    upstream
        .simple_query("CREATE TABLE t (a INT PRIMARY KEY, b INT); INSERT INTO t VALUES (1, 1)")
        .await
        .unwrap();
}

/// Recreate the test's two databases with table `t` in each, and return their names.
async fn create_databases() -> (String, String) {
    let url_db = derive_test_name();
    let other_db = format!("{url_db}_other");
    for db in [&url_db, &other_db] {
        PostgreSQLAdapter::recreate_database(db).await;
        create_table(db).await;
    }
    (url_db, other_db)
}

/// The catalogs of the controller's replication channels.
async fn channel_catalogs(handle: &mut Handle) -> Vec<Option<String>> {
    let status = handle.status().await.unwrap();
    status.channels.into_iter().map(|c| c.catalog).collect()
}

/// The databases with a Readyset replication slot, out of `databases`.
async fn databases_with_slots(databases: &[&str]) -> Vec<String> {
    let upstream = psql_helpers::connect(psql_helpers::upstream_config()).await;
    let mut found: Vec<String> = upstream
        .query(
            "SELECT database::text FROM pg_replication_slots \
             WHERE slot_name LIKE 'readyset%' AND database = ANY($1)",
            &[&databases],
        )
        .await
        .unwrap()
        .iter()
        .map(|row| row.get(0))
        .collect();
    found.sort();
    found
}

/// `--replication-databases` replicates each database with its own slot, and the stored list
/// outlives the flag.
#[tokio::test]
#[tags(serial)]
#[upstream(postgres)]
async fn replicates_each_database() {
    readyset_tracing::init_test_logging();

    let (url_db, other_db) = create_databases().await;

    let (_rs_opts, mut handle, mut shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .replicate_db(&url_db)
        .fallback(true)
        .replication_catalogs(vec![other_db.clone()])
        .build::<PostgreSQLAdapter>()
        .await;

    let expected = vec![None, Some(other_db.clone())];
    assert_eq!(channel_catalogs(&mut handle).await, expected);
    let mut databases = [url_db.as_str(), other_db.as_str()];
    databases.sort();
    assert_eq!(databases_with_slots(&databases).await, databases);

    // The stored list wins over the flag.
    let builder = shutdown_tx.builder_mut();
    *builder = mem::take(builder).replication_catalogs(Vec::new());
    let (_rs_opts, mut handle, shutdown_tx) = shutdown_tx.restart(handle).await;
    assert_eq!(channel_catalogs(&mut handle).await, expected);

    shutdown_tx.shutdown().await;
}
