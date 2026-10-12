use std::assert_matches;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::Duration;

use readyset_adapter::BackendBuilder;
use readyset_adapter::backend::{AllowedUsers, QueryInfo};
use readyset_client::consensus::AllowedUsersMap;
use readyset_client_metrics::QueryDestination;
use readyset_client_test_helpers::psql_helpers::{self, PostgreSQLAdapter};
use readyset_client_test_helpers::{Adapter, TestBuilder, derive_test_name};
use readyset_server::CacheMode;
use readyset_util::eventually;
use test_utils::{tags, upstream};
use tokio_postgres::{Client, SimpleQueryMessage};

const QUERY: &str = "SELECT b FROM t WHERE a = 1";

fn first_value(messages: &[SimpleQueryMessage]) -> &str {
    messages
        .iter()
        .find_map(|m| match m {
            SimpleQueryMessage::Row(row) => row.get(0),
            _ => None,
        })
        .expect("query returned no rows")
}

/// Run [`QUERY`] through Readyset, returning its value and how Readyset served it.
async fn query_b(conn: &Client) -> (String, QueryInfo) {
    let rows = conn.simple_query(QUERY).await.unwrap();
    let value = first_value(&rows).to_owned();
    (value, psql_helpers::last_query_info(conn).await)
}

/// Create table `t` in `db` on the upstream with a single row `(1, b)`, returning the upstream
/// connection.
async fn create_table(db: &str, b: i32) -> Client {
    let mut config = psql_helpers::upstream_config();
    config.dbname(db);
    let upstream = psql_helpers::connect(config).await;
    upstream
        .simple_query(&format!("CREATE TABLE t (a INT, b INT); INSERT INTO t VALUES (1, {b})"))
        .await
        .unwrap();
    upstream
}

/// Shallow caches are separate per database. The same query from sessions on two databases fills,
/// serves, and refreshes from its own database.
#[tokio::test]
#[tags(serial)]
#[upstream(postgres)]
async fn shallow_caches_are_per_database() {
    readyset_tracing::init_test_logging();

    let url_db = derive_test_name();
    let other_db = format!("{url_db}_other");
    PostgreSQLAdapter::recreate_database(&url_db).await;
    PostgreSQLAdapter::recreate_database(&other_db).await;
    let url_upstream = create_table(&url_db, 100).await;
    let other_upstream = create_table(&other_db, 200).await;

    let backend = BackendBuilder::new().require_authentication(false);
    let (mut rs_opts, _handle, shutdown_tx) = TestBuilder::new(backend)
        .cache_mode(CacheMode::Shallow)
        .recreate_database(false)
        .replicate(false)
        .fallback_without_replication(&url_db)
        .build::<PostgreSQLAdapter>()
        .await;
    let url_conn = psql_helpers::connect(rs_opts.clone()).await;
    rs_opts.dbname(&other_db);
    let other_conn = psql_helpers::connect(rs_opts).await;

    let create = format!("CREATE SHALLOW CACHE POLICY TTL 60 SECONDS REFRESH 1 SECONDS FROM {QUERY}");
    url_conn.simple_query(&create).await.unwrap();
    other_conn.simple_query(&create).await.unwrap();
    let caches = url_conn.simple_query("SHOW CACHES").await.unwrap();
    let count = caches
        .iter()
        .filter(|m| matches!(m, SimpleQueryMessage::Row(_)))
        .count();
    assert_eq!(count, 2, "each database has its own cache");

    for (conn, b) in [(&url_conn, "100"), (&other_conn, "200")] {
        let (value, info) = query_b(conn).await;
        assert_eq!(value, b);
        assert_matches!(info.destination, QueryDestination::ReadysetThenUpstream(_), "{info:?}");
        let (value, info) = query_b(conn).await;
        assert_eq!(value, b);
        assert_matches!(info.destination, QueryDestination::ReadysetShallow(_), "{info:?}");
    }

    // Refreshes run on the refresh pool, which must access the correct database.
    url_upstream
        .simple_query("UPDATE t SET b = 101")
        .await
        .unwrap();
    other_upstream
        .simple_query("UPDATE t SET b = 201")
        .await
        .unwrap();
    for (conn, b) in [(&url_conn, "101"), (&other_conn, "201")] {
        eventually!(run_test: {
            let result = query_b(conn).await;
            AssertUnwindSafe(move || result)
        }, then_assert: |result| {
            let (value, info) = result();
            assert_eq!(value, b);
            assert_matches!(info.destination, QueryDestination::ReadysetShallow(_), "{info:?}");
        });
    }

    shutdown_tx.shutdown().await;
}

/// In shallow cache mode, a session connects to the database it names, not the upstream URL's.
#[tokio::test]
#[tags(serial)]
#[upstream(postgres)]
async fn shallow_mode_connects_to_session_database() {
    readyset_tracing::init_test_logging();

    let url_db = derive_test_name();
    let session_db = "psql_db_selection_session";
    PostgreSQLAdapter::recreate_database(session_db).await;

    let backend = BackendBuilder::new().require_authentication(false);
    let (mut rs_opts, _handle, shutdown_tx) = TestBuilder::new(backend)
        .cache_mode(CacheMode::Shallow)
        .replicate(false)
        .fallback_without_replication(&url_db)
        .build::<PostgreSQLAdapter>()
        .await;

    let url_conn = psql_helpers::connect(rs_opts.clone()).await;
    let rows = url_conn.simple_query("SELECT current_database()").await.unwrap();
    assert_eq!(first_value(&rows), url_db);

    rs_opts.dbname(session_db);
    let session_conn = psql_helpers::connect(rs_opts).await;
    let rows = session_conn
        .simple_query("SELECT current_database()")
        .await
        .unwrap();
    assert_eq!(first_value(&rows), session_db);

    shutdown_tx.shutdown().await;
}

/// A session that specifies no database connects to the database with the user's name, as it does
/// on the upstream.
#[tokio::test]
#[tags(serial)]
#[upstream(postgres)]
async fn session_database_defaults_to_user_name() {
    readyset_tracing::init_test_logging();

    let url_db = derive_test_name();
    let backend = BackendBuilder::new().require_authentication(false);
    let (rs_opts, _handle, shutdown_tx) = TestBuilder::new(backend)
        .cache_mode(CacheMode::Shallow)
        .replicate(false)
        .fallback_without_replication(&url_db)
        .build::<PostgreSQLAdapter>()
        .await;

    let upstream_config = psql_helpers::upstream_config();
    let user = upstream_config.get_user().expect("upstream user");
    let upstream = psql_helpers::connect(upstream_config.clone()).await;
    let rows = upstream.simple_query("SELECT current_database()").await.unwrap();
    assert_eq!(first_value(&rows), user);

    let mut rs_config = tokio_postgres::Config::new();
    rs_config.host("127.0.0.1").port(rs_opts.get_ports()[0]).user(user);
    let conn = psql_helpers::connect(rs_config).await;
    let rows = conn.simple_query("SELECT current_database()").await.unwrap();
    assert_eq!(first_value(&rows), user);

    shutdown_tx.shutdown().await;
}

/// Outside shallow cache mode, a session may connect only to the upstream URL's database.
#[tokio::test]
#[tags(serial)]
#[upstream(postgres)]
async fn deep_mode_rejects_other_database() {
    readyset_tracing::init_test_logging();

    let backend = BackendBuilder::new().require_authentication(false);
    let (mut rs_opts, _handle, shutdown_tx) = TestBuilder::new(backend)
        .fallback(true)
        .build::<PostgreSQLAdapter>()
        .await;

    rs_opts.dbname("postgres");
    let Err(err) = rs_opts.connect(tokio_postgres::NoTls).await else {
        panic!("connecting to another database should fail");
    };
    let message = err.as_db_error().map(|e| e.message());
    assert!(
        message.is_some_and(|m| m.contains("Database postgres unavailable outside shallow cache mode")),
        "unexpected error: {err:?}"
    );

    shutdown_tx.shutdown().await;
}

/// Readyset reads the RLS catalog of the upstream URL's database only, so with RLS enabled it
/// refuses to cache a query on another database.
#[tokio::test]
#[tags(serial)]
#[upstream(postgres)]
async fn rls_refuses_caches_on_other_database() {
    readyset_tracing::init_test_logging();

    let url_db = derive_test_name();
    let other_db = format!("{url_db}_other");
    PostgreSQLAdapter::recreate_database(&url_db).await;
    PostgreSQLAdapter::recreate_database(&other_db).await;
    create_table(&other_db, 200).await;

    let backend = BackendBuilder::new().require_authentication(false);
    let (mut rs_opts, _handle, shutdown_tx) = TestBuilder::new(backend)
        .cache_mode(CacheMode::Shallow)
        .recreate_database(false)
        .replicate_db(&url_db)
        .fallback(true)
        .rls(true)
        .build::<PostgreSQLAdapter>()
        .await;
    rs_opts.dbname(&other_db);
    let conn = psql_helpers::connect(rs_opts).await;

    let Err(err) = conn
        .simple_query(&format!("CREATE SHALLOW CACHE FROM {QUERY}"))
        .await
    else {
        panic!("creating a cache on another database should fail");
    };
    let message = err.as_db_error().map(|e| e.message());
    assert!(
        message.is_some_and(|m| m.contains("rls_uncacheable[code=other_database")),
        "unexpected error: {err:?}"
    );
    let (value, info) = query_b(&conn).await;
    assert_eq!(value, "200");
    assert_eq!(info.destination, QueryDestination::Upstream, "{info:?}");

    shutdown_tx.shutdown().await;
}

/// Readyset probes cache ACLs on the upstream URL's database only, so while authentication is
/// required a session on another database is never served from a cache.
#[tokio::test]
#[tags(serial)]
#[upstream(postgres)]
async fn acl_never_serves_caches_on_other_database() {
    readyset_tracing::init_test_logging();

    let url_db = derive_test_name();
    let other_db = format!("{url_db}_other");
    PostgreSQLAdapter::recreate_database(&url_db).await;
    PostgreSQLAdapter::recreate_database(&other_db).await;
    create_table(&url_db, 100).await;
    create_table(&other_db, 200).await;

    let upstream = psql_helpers::upstream_config();
    let user = upstream.get_user().expect("upstream user");
    let password = upstream.get_password().expect("upstream password");
    let password_str = std::str::from_utf8(password).expect("UTF-8 password");
    let users: AllowedUsersMap = [(user.to_owned(), password_str.into())]
        .into_iter()
        .collect();
    let backend = BackendBuilder::new()
        .require_authentication(true)
        .users(Arc::new(AllowedUsers::new(users, None)));
    let (mut rs_opts, _handle, shutdown_tx) = TestBuilder::new(backend)
        .cache_mode(CacheMode::Shallow)
        .recreate_database(false)
        .replicate_db(&url_db)
        .fallback(true)
        .cache_acl_refresh_interval(Duration::from_millis(250))
        .build::<PostgreSQLAdapter>()
        .await;
    rs_opts.user(user).password(password);
    let url_conn = psql_helpers::connect(rs_opts.clone()).await;
    rs_opts.dbname(&other_db);
    let other_conn = psql_helpers::connect(rs_opts).await;

    let create = format!("CREATE SHALLOW CACHE FROM {QUERY}");
    url_conn.simple_query(&create).await.unwrap();
    other_conn.simple_query(&create).await.unwrap();

    // Serving the URL's database from its cache shows the ACL worker has probed.
    eventually!(run_test: {
        let result = query_b(&url_conn).await;
        AssertUnwindSafe(move || result)
    }, then_assert: |result| {
        let (value, info) = result();
        assert_eq!(value, "100");
        assert_matches!(info.destination, QueryDestination::ReadysetShallow(_), "{info:?}");
    });
    for _ in 0..3 {
        let (value, info) = query_b(&other_conn).await;
        assert_eq!(value, "200");
        assert_eq!(info.destination, QueryDestination::Upstream, "{info:?}");
    }

    shutdown_tx.shutdown().await;
}
