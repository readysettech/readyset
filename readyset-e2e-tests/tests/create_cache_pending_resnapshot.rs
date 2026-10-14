//! A schema change that needs a resnapshot removes the caches over the changed table, and the
//! leader refuses new caches while the resnapshot runs. Once the replicator has decided to
//! resnapshot, a CREATE CACHE either fails with the leader not ready, or names a cache that still
//! exists after the resnapshot.
#![cfg(feature = "failure_injection")]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use fail::FailScenario;
use readyset_client_test_helpers::psql_helpers::{self, PostgreSQLAdapter};
use readyset_client_test_helpers::{Adapter, TestBuilder, wait_for_schema_generation_change};
use readyset_server::Handle;
use readyset_util::{eventually, failpoints};
use schema_catalog::SchemaGeneration;
use test_utils::{tags, upstream};

const CACHE: &str = "pending_resnapshot";

/// Flags the replicator's decision to resnapshot, which it takes before its wait.
fn observe_resnapshot_decision() -> Arc<AtomicBool> {
    let decided = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&decided);
    fail::cfg_callback(failpoints::POSTGRES_PARTIAL_RESNAPSHOT, move || {
        flag.store(true, Ordering::SeqCst)
    })
    .unwrap();
    decided
}

/// Returns inside the replicator's wait before the resnapshot. The controller learns of a
/// pending resnapshot through a channel, so this gives it a moment, well inside the wait.
async fn enter_wait_before_resnapshot(handle: &mut Handle, decided: &AtomicBool) {
    eventually!(attempts: 400, sleep: Duration::from_millis(25), {
        decided.load(Ordering::SeqCst)
    });
    let _ = tokio::time::timeout(Duration::from_millis(500), async {
        while handle.leader_ready().await.unwrap() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
}

/// Waits for the resnapshot to end, then requires the cache a successful CREATE CACHE named to
/// exist, and a refused one to have been refused for the pending resnapshot.
async fn assert_create_cache_kept_or_refused(
    handle: &mut Handle,
    created: Result<(), String>,
    generation_before: SchemaGeneration,
) {
    wait_for_schema_generation_change(handle, generation_before).await;
    match created {
        Ok(()) => {
            let views = handle.views().await.unwrap();
            assert!(
                views.keys().any(|view| view.name == CACHE),
                "CREATE CACHE succeeded during the wait before a resnapshot, and the resnapshot \
                 removed the cache; views: {views:?}"
            );
        }
        Err(error) => assert!(
            error.contains("leader is not ready"),
            "CREATE CACHE failed for a reason other than the pending resnapshot: {error}"
        ),
    }
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(postgres)]
async fn create_cache_while_a_resnapshot_is_pending_is_kept_or_refused_postgres() {
    readyset_tracing::init_test_logging();
    let db = "create_cache_pending_resnapshot";
    PostgreSQLAdapter::recreate_database(db).await;
    let mut upstream_config = psql_helpers::upstream_config();
    upstream_config.dbname(db);
    let upstream = psql_helpers::connect(upstream_config).await;
    upstream
        .simple_query("CREATE TABLE t (id INT PRIMARY KEY, v INT); INSERT INTO t VALUES (1, 10);")
        .await
        .unwrap();

    let _scenario = FailScenario::setup();
    let (rs_opts, mut handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .replicate_db(db)
        .fallback(true)
        .build::<PostgreSQLAdapter>()
        .await;
    let mut rs_config = rs_opts.clone();
    rs_config.dbname(db);
    let rs = psql_helpers::connect(rs_config).await;
    eventually!(rs.simple_query("CREATE CACHE FROM SELECT v FROM t").await.is_ok());
    let generation_before = handle.schema_catalog().await.unwrap().generation;

    let decided = observe_resnapshot_decision();
    upstream
        .simple_query("ALTER TABLE t ADD COLUMN c INT")
        .await
        .unwrap();
    enter_wait_before_resnapshot(&mut handle, &decided).await;
    let created = rs
        .simple_query(&format!("CREATE CACHE {CACHE} FROM SELECT id, v FROM t WHERE id = $1"))
        .await
        .map(drop)
        .map_err(|error| format!("{error:?}"));
    assert_create_cache_kept_or_refused(&mut handle, created, generation_before).await;

    shutdown_tx.shutdown().await;
}
