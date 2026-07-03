use std::assert_matches;

use mysql_async::prelude::Queryable;
use mysql_async::Row;
use tokio::test;

use readyset_adapter::backend::{BackendBuilder, MigrationMode};
use readyset_client_metrics::QueryDestination;
use readyset_client_test_helpers::{
    Adapter, TestBuilder, derive_test_name,
    mysql_helpers::{self, MySQLAdapter, last_query_info},
    psql_helpers::{self, PostgreSQLAdapter},
};
use readyset_server::CacheMode;
use readyset_sql_passes::shallow::ShallowCacheEligibility;
use readyset_tracing::init_test_logging;
use readyset_util::eventually;
use test_utils::{tags, upstream};

#[test]
#[tags(serial)]
#[upstream(mysql)]
async fn cache_mode_shallow() {
    init_test_logging();

    let test_name = derive_test_name();
    mysql_helpers::recreate_database(&test_name).await;

    let upstream_opts = mysql_helpers::upstream_config().db_name(Some(&test_name));
    let mut upstream = mysql_async::Conn::new(upstream_opts).await.unwrap();

    upstream
        .query_drop("CREATE TABLE foo (a INT)")
        .await
        .unwrap();
    upstream
        .query_drop("INSERT INTO foo VALUES (42)")
        .await
        .unwrap();

    let backend = BackendBuilder::default()
        .require_authentication(false)
        .cache_mode(CacheMode::Shallow);
    let (readyset_opts, _readyset_handle, shutdown_tx) = TestBuilder::new(backend)
        .recreate_database(false)
        .fallback(true)
        .build::<MySQLAdapter>()
        .await;
    let mut readyset = mysql_async::Conn::new(readyset_opts).await.unwrap();
    readyset
        .query_drop(format!("USE {test_name}"))
        .await
        .unwrap();

    readyset
        .query_drop("CREATE CACHE FROM SELECT a FROM foo")
        .await
        .unwrap();

    readyset
        .query_drop("SELECT a FROM foo")
        .await
        .unwrap();
    assert_eq!(
        last_query_info(&mut readyset).await.destination,
        QueryDestination::Upstream
    );

    readyset
        .query_drop("SELECT a FROM foo")
        .await
        .unwrap();
    assert_eq!(
        last_query_info(&mut readyset).await.destination,
        QueryDestination::ReadysetShallow
    );

    shutdown_tx.shutdown().await;
}

#[test]
#[tags(serial)]
#[upstream(mysql)]
async fn cache_mode_deep() {
    init_test_logging();

    let test_name = derive_test_name();
    mysql_helpers::recreate_database(&test_name).await;

    let upstream_opts = mysql_helpers::upstream_config().db_name(Some(&test_name));
    let mut upstream = mysql_async::Conn::new(upstream_opts).await.unwrap();

    upstream
        .query_drop("CREATE TABLE foo (a INT)")
        .await
        .unwrap();
    upstream
        .query_drop("INSERT INTO foo VALUES (42)")
        .await
        .unwrap();

    let backend = BackendBuilder::default()
        .require_authentication(false)
        .cache_mode(CacheMode::Deep);
    let (readyset_opts, _readyset_handle, shutdown_tx) = TestBuilder::new(backend)
        .replicate_db(&test_name)
        .recreate_database(false)
        .fallback(true)
        .build::<MySQLAdapter>()
        .await;
    let mut readyset = mysql_async::Conn::new(readyset_opts).await.unwrap();
    readyset
        .query_drop(format!("USE {test_name}"))
        .await
        .unwrap();

    eventually! {
        readyset
            .query_drop("CREATE CACHE FROM SELECT a FROM foo")
            .await
            .is_ok()
    };

    readyset
        .query_drop("SELECT a FROM foo")
        .await
        .unwrap();
    assert_matches!(
        last_query_info(&mut readyset).await.destination,
        QueryDestination::Readyset(..)
    );

    shutdown_tx.shutdown().await;
}

#[test]
#[tags(serial)]
#[upstream(mysql)]
async fn cache_mode_deep_then_shallow() {
    init_test_logging();

    let test_name = derive_test_name();
    mysql_helpers::recreate_database(&test_name).await;

    let upstream_opts = mysql_helpers::upstream_config().db_name(Some(&test_name));
    let mut upstream = mysql_async::Conn::new(upstream_opts).await.unwrap();

    upstream
        .query_drop("CREATE TABLE foo (a INT)")
        .await
        .unwrap();
    upstream
        .query_drop("INSERT INTO foo VALUES (42)")
        .await
        .unwrap();

    let backend = BackendBuilder::default()
        .require_authentication(false)
        .cache_mode(CacheMode::DeepThenShallow);
    let (readyset_opts, _readyset_handle, shutdown_tx) = TestBuilder::new(backend)
        .replicate_db(&test_name)
        .recreate_database(false)
        .fallback(true)
        .build::<MySQLAdapter>()
        .await;
    let mut readyset = mysql_async::Conn::new(readyset_opts).await.unwrap();
    readyset
        .query_drop(format!("USE {test_name}"))
        .await
        .unwrap();

    eventually! {
        readyset
            .query_drop("CREATE CACHE FROM SELECT a, RAND() FROM foo")
            .await
            .is_ok()
    };

    readyset
        .query_drop("SELECT a, RAND() FROM foo")
        .await
        .unwrap();
    assert_eq!(
        last_query_info(&mut readyset).await.destination,
        QueryDestination::Upstream
    );

    readyset
        .query_drop("SELECT a, RAND() FROM foo")
        .await
        .unwrap();
    assert_eq!(
        last_query_info(&mut readyset).await.destination,
        QueryDestination::ReadysetShallow
    );

    shutdown_tx.shutdown().await;
}

// In-request-path + cache_mode=shallow should auto-create a shallow cache on
// first SELECT, without an explicit CREATE CACHE statement or hint.
#[test]
#[tags(serial)]
#[upstream(mysql)]
async fn cache_mode_shallow_auto_create_in_request_path() {
    init_test_logging();

    let test_name = derive_test_name();
    mysql_helpers::recreate_database(&test_name).await;

    let upstream_opts = mysql_helpers::upstream_config().db_name(Some(&test_name));
    let mut upstream = mysql_async::Conn::new(upstream_opts).await.unwrap();

    upstream
        .query_drop("CREATE TABLE foo (a INT)")
        .await
        .unwrap();
    upstream
        .query_drop("INSERT INTO foo VALUES (42)")
        .await
        .unwrap();

    let backend = BackendBuilder::default()
        .require_authentication(false)
        .cache_mode(CacheMode::Shallow);
    let (readyset_opts, _readyset_handle, shutdown_tx) = TestBuilder::new(backend)
        .migration_mode(MigrationMode::InRequestPath)
        .recreate_database(false)
        .fallback(true)
        .build::<MySQLAdapter>()
        .await;
    let mut readyset = mysql_async::Conn::new(readyset_opts).await.unwrap();
    readyset
        .query_drop(format!("USE {test_name}"))
        .await
        .unwrap();

    // No CREATE CACHE, no hint — the adapter should auto-create a shallow
    // cache because we are in MigrationMode::InRequestPath with
    // CacheMode::Shallow. First execution is a miss and populates the
    // cache via upstream.
    readyset
        .query_drop("SELECT a FROM foo")
        .await
        .unwrap();
    assert_eq!(
        last_query_info(&mut readyset).await.destination,
        QueryDestination::Upstream
    );

    // Second execution should be served from the auto-created shallow cache.
    readyset
        .query_drop("SELECT a FROM foo")
        .await
        .unwrap();
    assert_eq!(
        last_query_info(&mut readyset).await.destination,
        QueryDestination::ReadysetShallow
    );

    // Auto-created caches use adaptive refresh.
    let adaptive: Vec<bool> = readyset
        .query("SELECT adaptive FROM readyset.shallow_caches")
        .await
        .unwrap();
    assert_eq!(adaptive, vec![true]);

    shutdown_tx.shutdown().await;
}

// In-request-path shallow auto-create must SKIP queries that are not safe to
// reuse. A plain SELECT auto-caches, but queries calling non-deterministic
// (RAND, NOW) or side-effecting (SLEEP) functions stay on the upstream path no
// matter how many times they are seen.
#[test]
#[tags(serial)]
#[upstream(mysql)]
async fn cache_mode_shallow_auto_create_skips_ineligible() {
    init_test_logging();

    let test_name = derive_test_name();
    mysql_helpers::recreate_database(&test_name).await;

    let upstream_opts = mysql_helpers::upstream_config().db_name(Some(&test_name));
    let mut upstream = mysql_async::Conn::new(upstream_opts).await.unwrap();
    upstream
        .query_drop("CREATE TABLE foo (a INT)")
        .await
        .unwrap();
    upstream
        .query_drop("INSERT INTO foo VALUES (42)")
        .await
        .unwrap();

    let backend = BackendBuilder::default()
        .require_authentication(false)
        .cache_mode(CacheMode::Shallow);
    let (readyset_opts, _readyset_handle, shutdown_tx) = TestBuilder::new(backend)
        .migration_mode(MigrationMode::InRequestPath)
        .recreate_database(false)
        .fallback(true)
        .build::<MySQLAdapter>()
        .await;
    let mut readyset = mysql_async::Conn::new(readyset_opts).await.unwrap();
    readyset
        .query_drop(format!("USE {test_name}"))
        .await
        .unwrap();

    // Control: a plain SELECT is eligible, so the second execution is served
    // from an auto-created shallow cache.
    readyset.query_drop("SELECT a FROM foo").await.unwrap();
    readyset.query_drop("SELECT a FROM foo").await.unwrap();
    assert_eq!(
        last_query_info(&mut readyset).await.destination,
        QueryDestination::ReadysetShallow,
        "a plain SELECT should auto-create a shallow cache"
    );

    // Ineligible queries are never auto-cached: every execution proxies to
    // upstream, even after the query has been seen before.
    for q in ["SELECT RAND()", "SELECT NOW()", "SELECT SLEEP(0)"] {
        for _ in 0..3 {
            readyset.query_drop(q).await.unwrap();
            assert_eq!(
                last_query_info(&mut readyset).await.destination,
                QueryDestination::Upstream,
                "ineligible query should never be auto-cached: {q}"
            );
        }
    }

    shutdown_tx.shutdown().await;
}

// PostgreSQL counterpart: random()/now() (non-deterministic) and pg_sleep()
// (side-effecting) must not be auto-cached in-request-path.
#[test]
#[tags(serial)]
#[upstream(postgres)]
async fn cache_mode_shallow_auto_create_skips_ineligible_pg() {
    init_test_logging();

    let backend = BackendBuilder::default()
        .require_authentication(false)
        .cache_mode(CacheMode::Shallow);
    // The adapter proxies to (and the harness recreates) the `noria` database,
    // so seed the table there through Readyset rather than side-channelling into
    // a per-test database the fallback connection never sees.
    let (rs_opts, _readyset_handle, shutdown_tx) = TestBuilder::new(backend)
        .migration_mode(MigrationMode::InRequestPath)
        .fallback_without_replication("noria")
        .build::<PostgreSQLAdapter>()
        .await;
    let rs = psql_helpers::connect(rs_opts).await;

    rs.simple_query("CREATE TABLE foo (a INT)").await.unwrap();
    rs.simple_query("INSERT INTO foo VALUES (42)").await.unwrap();

    // Control: a plain SELECT auto-caches on the second execution.
    rs.simple_query("SELECT a FROM foo").await.unwrap();
    rs.simple_query("SELECT a FROM foo").await.unwrap();
    assert_eq!(
        psql_helpers::last_query_info(&rs).await.destination,
        QueryDestination::ReadysetShallow,
        "a plain SELECT should auto-create a shallow cache"
    );

    // Ineligible queries always proxy to upstream.
    for q in ["SELECT random()", "SELECT now()", "SELECT pg_sleep(0)"] {
        for _ in 0..3 {
            rs.simple_query(q).await.unwrap();
            assert_eq!(
                psql_helpers::last_query_info(&rs).await.destination,
                QueryDestination::Upstream,
                "ineligible query should never be auto-cached: {q}"
            );
        }
    }

    shutdown_tx.shutdown().await;
}

#[test]
#[tags(serial)]
#[upstream(mysql)]
async fn create_cache_returns_deep_type() {
    init_test_logging();

    let test_name = derive_test_name();
    mysql_helpers::recreate_database(&test_name).await;

    let upstream_opts = mysql_helpers::upstream_config().db_name(Some(&test_name));
    let mut upstream = mysql_async::Conn::new(upstream_opts).await.unwrap();

    upstream
        .query_drop("CREATE TABLE foo (a INT)")
        .await
        .unwrap();

    let backend = BackendBuilder::default()
        .require_authentication(false)
        .cache_mode(CacheMode::Deep);
    let (readyset_opts, _readyset_handle, shutdown_tx) = TestBuilder::new(backend)
        .replicate_db(&test_name)
        .recreate_database(false)
        .fallback(true)
        .build::<MySQLAdapter>()
        .await;
    let mut readyset = mysql_async::Conn::new(readyset_opts).await.unwrap();
    readyset
        .query_drop(format!("USE {test_name}"))
        .await
        .unwrap();

    let row: Row = eventually! {
        run_test: {
            readyset
                .query_first("CREATE CACHE my_deep FROM SELECT a FROM foo WHERE a = ?")
                .await
                .unwrap()
        },
        then_assert: |result| {
            assert!(result.is_some(), "expected a result row");
            result.unwrap()
        }
    };

    let query_id: String = row.get("query_id").unwrap();
    let cache_name: String = row.get("name").unwrap();
    let query: String = row.get("query").unwrap();
    let cache_type: String = row.get("cache_type").unwrap();

    assert!(query_id.starts_with("q_"), "query id should start with q_");
    assert_eq!(cache_name, "my_deep");
    assert!(!query.is_empty());
    assert_eq!(cache_type, "deep");

    let row: Row = readyset
        .query_first("SELECT query_id, name, always FROM readyset.deep_caches WHERE name = 'my_deep'")
        .await
        .unwrap()
        .expect("expected one row in readyset.deep_caches");
    let vrel_query_id: String = row.get("query_id").unwrap();
    let vrel_name: String = row.get("name").unwrap();
    let vrel_always: bool = row.get("always").unwrap();
    assert_eq!(vrel_query_id, query_id);
    assert_eq!(vrel_name, "my_deep");
    assert!(!vrel_always);

    shutdown_tx.shutdown().await;
}

#[test]
#[tags(serial)]
#[upstream(mysql)]
async fn create_cache_returns_shallow_type() {
    init_test_logging();

    let test_name = derive_test_name();
    mysql_helpers::recreate_database(&test_name).await;

    let upstream_opts = mysql_helpers::upstream_config().db_name(Some(&test_name));
    let mut upstream = mysql_async::Conn::new(upstream_opts).await.unwrap();

    upstream
        .query_drop("CREATE TABLE foo (a INT)")
        .await
        .unwrap();

    let backend = BackendBuilder::default()
        .require_authentication(false)
        .cache_mode(CacheMode::Shallow);
    let (readyset_opts, _readyset_handle, shutdown_tx) = TestBuilder::new(backend)
        .recreate_database(false)
        .fallback(true)
        .build::<MySQLAdapter>()
        .await;
    let mut readyset = mysql_async::Conn::new(readyset_opts).await.unwrap();
    readyset
        .query_drop(format!("USE {test_name}"))
        .await
        .unwrap();

    let row: Row = readyset
        .query_first("CREATE CACHE my_shallow FROM SELECT a FROM foo")
        .await
        .unwrap()
        .expect("expected a result row");

    let query_id: String = row.get("query_id").unwrap();
    let cache_name: String = row.get("name").unwrap();
    let query: String = row.get("query").unwrap();
    let cache_type: String = row.get("cache_type").unwrap();

    assert!(query_id.starts_with("q_"), "query id should start with q_");
    assert_eq!(cache_name, "my_shallow");
    assert!(!query.is_empty());
    assert_eq!(cache_type, "shallow");

    shutdown_tx.shutdown().await;
}

#[test]
#[tags(serial)]
#[upstream(mysql)]
async fn create_cache_returns_shallow_type_on_fallback() {
    init_test_logging();

    let test_name = derive_test_name();
    mysql_helpers::recreate_database(&test_name).await;

    let upstream_opts = mysql_helpers::upstream_config().db_name(Some(&test_name));
    let mut upstream = mysql_async::Conn::new(upstream_opts).await.unwrap();

    upstream
        .query_drop("CREATE TABLE foo (a INT)")
        .await
        .unwrap();

    let backend = BackendBuilder::default()
        .require_authentication(false)
        .cache_mode(CacheMode::DeepThenShallow);
    let (readyset_opts, _readyset_handle, shutdown_tx) = TestBuilder::new(backend)
        .replicate_db(&test_name)
        .recreate_database(false)
        .fallback(true)
        .build::<MySQLAdapter>()
        .await;
    let mut readyset = mysql_async::Conn::new(readyset_opts).await.unwrap();
    readyset
        .query_drop(format!("USE {test_name}"))
        .await
        .unwrap();

    // RAND() is not supported by deep caching, so this will fall back to shallow.
    let row: Row = eventually! {
        run_test: {
            readyset
                .query_first("CREATE CACHE FROM SELECT a, RAND() FROM foo")
                .await
                .unwrap()
        },
        then_assert: |result| {
            assert!(result.is_some(), "expected a result row");
            result.unwrap()
        }
    };

    let cache_type: String = row.get("cache_type").unwrap();
    assert_eq!(cache_type, "shallow");

    shutdown_tx.shutdown().await;
}

// With `--shallow-cache-allow-nondeterministic`, a non-deterministic call such
// as NOW() becomes eligible and auto-caches, while a side-effecting call such as
// SLEEP() keeps proxying to upstream: the opt-in opens exactly one category, not
// a blanket allow. This is the front-door counterpart to the per-flag unit
// mapping test, proving a flag actually changes caching behavior.
#[test]
#[tags(serial)]
#[upstream(mysql)]
async fn cache_mode_shallow_allow_nondeterministic_makes_eligible() {
    init_test_logging();

    let test_name = derive_test_name();
    mysql_helpers::recreate_database(&test_name).await;

    let upstream_opts = mysql_helpers::upstream_config().db_name(Some(&test_name));
    let mut upstream = mysql_async::Conn::new(upstream_opts).await.unwrap();
    upstream
        .query_drop("CREATE TABLE foo (a INT)")
        .await
        .unwrap();
    upstream
        .query_drop("INSERT INTO foo VALUES (42)")
        .await
        .unwrap();

    let backend = BackendBuilder::default()
        .require_authentication(false)
        .cache_mode(CacheMode::Shallow)
        .shallow_cache_eligibility(ShallowCacheEligibility {
            allow_nondeterministic: true,
            ..Default::default()
        });
    let (readyset_opts, _readyset_handle, shutdown_tx) = TestBuilder::new(backend)
        .migration_mode(MigrationMode::InRequestPath)
        .recreate_database(false)
        .fallback(true)
        .build::<MySQLAdapter>()
        .await;
    let mut readyset = mysql_async::Conn::new(readyset_opts).await.unwrap();
    readyset
        .query_drop(format!("USE {test_name}"))
        .await
        .unwrap();

    // NOW() is non-deterministic; the opt-in makes it eligible, so the second
    // execution is served from an auto-created shallow cache.
    readyset.query_drop("SELECT NOW()").await.unwrap();
    readyset.query_drop("SELECT NOW()").await.unwrap();
    assert_eq!(
        last_query_info(&mut readyset).await.destination,
        QueryDestination::ReadysetShallow,
        "NOW() should auto-cache once non-deterministic calls are allowed"
    );

    // SLEEP() is side-effecting/blocking, which the non-determinism opt-in does
    // not cover, so it keeps proxying to upstream.
    for _ in 0..3 {
        readyset.query_drop("SELECT SLEEP(0)").await.unwrap();
        assert_eq!(
            last_query_info(&mut readyset).await.destination,
            QueryDestination::Upstream,
            "SLEEP() must stay ineligible: allow-nondeterministic is not a blanket opt-in"
        );
    }

    shutdown_tx.shutdown().await;
}

// PostgreSQL counterpart: allow-nondeterministic makes now() eligible, while a
// non-immutable builtin outside the curated non-deterministic list, such as
// pg_sleep(), stays ineligible (default-deny still applies to it).
#[test]
#[tags(serial)]
#[upstream(postgres)]
async fn cache_mode_shallow_allow_nondeterministic_makes_eligible_pg() {
    init_test_logging();

    let test_name = derive_test_name();
    PostgreSQLAdapter::recreate_database(&test_name).await;

    let mut cfg = psql_helpers::upstream_config();
    cfg.dbname(&test_name);
    let upstream = psql_helpers::connect(cfg).await;
    upstream
        .simple_query("CREATE TABLE foo (a INT)")
        .await
        .unwrap();
    upstream
        .simple_query("INSERT INTO foo VALUES (42)")
        .await
        .unwrap();

    let backend = BackendBuilder::default()
        .require_authentication(false)
        .cache_mode(CacheMode::Shallow)
        .shallow_cache_eligibility(ShallowCacheEligibility {
            allow_nondeterministic: true,
            ..Default::default()
        });
    let (rs_opts, _readyset_handle, shutdown_tx) = TestBuilder::new(backend)
        .migration_mode(MigrationMode::InRequestPath)
        .recreate_database(false)
        .fallback(true)
        .build::<PostgreSQLAdapter>()
        .await;
    let mut cfg_rs = rs_opts.clone();
    cfg_rs.dbname(&test_name);
    let rs = psql_helpers::connect(cfg_rs).await;

    // now() is in the curated non-deterministic list, so the opt-in makes it
    // eligible and it auto-caches on the second execution.
    rs.simple_query("SELECT now()").await.unwrap();
    rs.simple_query("SELECT now()").await.unwrap();
    assert_eq!(
        psql_helpers::last_query_info(&rs).await.destination,
        QueryDestination::ReadysetShallow,
        "now() should auto-cache once non-deterministic calls are allowed"
    );

    // pg_sleep() is a non-immutable builtin outside the curated list, so it is
    // denied regardless of the non-determinism opt-in.
    for _ in 0..3 {
        rs.simple_query("SELECT pg_sleep(0)").await.unwrap();
        assert_eq!(
            psql_helpers::last_query_info(&rs).await.destination,
            QueryDestination::Upstream,
            "pg_sleep() must stay ineligible under allow-nondeterministic"
        );
    }

    shutdown_tx.shutdown().await;
}
