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
use readyset_sql_parsing::ParsingPreset;
use readyset_util::eventually;
use test_utils::{tags, upstream};
use tokio_postgres::{Client, SimpleQueryMessage, SimpleQueryRow};

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

fn rows(messages: &[SimpleQueryMessage]) -> impl Iterator<Item = &SimpleQueryRow> {
    messages.iter().filter_map(|m| match m {
        SimpleQueryMessage::Row(row) => Some(row),
        _ => None,
    })
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
        .parsing_preset(ParsingPreset::OnlySqlparser)
        .recreate_database(false)
        .replicate(false)
        .fallback_without_replication(&url_db)
        .build::<PostgreSQLAdapter>()
        .await;
    let url_conn = psql_helpers::connect(rs_opts.clone()).await;
    rs_opts.dbname(&other_db);
    let other_conn = psql_helpers::connect(rs_opts).await;

    let caches = [(&url_conn, "url_cache"), (&other_conn, "other_cache")];
    for (conn, name) in caches {
        let create = format!(
            "CREATE SHALLOW CACHE POLICY TTL 60 SECONDS REFRESH 1 SECONDS {name} FROM {QUERY}"
        );
        conn.simple_query(&create).await.unwrap();
    }

    let shown = url_conn.simple_query("SHOW CACHES").await.unwrap();
    let mut shown: Vec<_> = rows(&shown)
        .map(|row| (row.get("name").unwrap(), row.get("database").unwrap()))
        .collect();
    shown.sort();
    assert_eq!(shown, [("other_cache", other_db.as_str()), ("url_cache", url_db.as_str())]);

    for (conn, name) in caches {
        let explained = conn.simple_query("EXPLAIN CACHES").await.unwrap();
        let explained: Vec<_> = rows(&explained).map(|row| row.get(0).unwrap()).collect();
        assert_eq!(explained.len(), 1, "{explained:?}");
        assert!(explained[0].contains(name), "{explained:?}");

        let explained = conn.simple_query("EXPLAIN ALL CACHES").await.unwrap();
        let explained: Vec<_> = rows(&explained)
            .map(|row| (row.get("database").unwrap(), row.get("query").unwrap()))
            .collect();
        assert_eq!(explained.len(), 2, "{explained:?}");
        for (database, name) in [(url_db.as_str(), "url_cache"), (other_db.as_str(), "other_cache")] {
            assert!(
                explained.iter().any(|(d, q)| *d == database && q.contains(name)),
                "{explained:?}"
            );
        }
    }

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

    // The user name differs from the database that connections without one use.
    let user = derive_test_name();
    PostgreSQLAdapter::recreate_database(&user).await;

    let backend = BackendBuilder::new().require_authentication(false);
    let (rs_opts, _handle, shutdown_tx) = TestBuilder::new(backend)
        .cache_mode(CacheMode::Shallow)
        .replicate(false)
        .fallback_without_database()
        .build::<PostgreSQLAdapter>()
        .await;

    let mut rs_config = tokio_postgres::Config::new();
    rs_config.host("127.0.0.1").port(rs_opts.get_ports()[0]).user(&user);
    let conn = psql_helpers::connect(rs_config).await;
    let rows = conn.simple_query("SELECT current_database()").await.unwrap();
    assert_eq!(first_value(&rows), user);

    shutdown_tx.shutdown().await;
}

/// In shallow cache mode, the upstream URL doesn't need a database.
#[tokio::test]
#[tags(serial)]
#[upstream(postgres)]
async fn shallow_mode_upstream_url_without_database() {
    readyset_tracing::init_test_logging();

    let db = derive_test_name();
    PostgreSQLAdapter::recreate_database(&db).await;

    let backend = BackendBuilder::new().require_authentication(false);
    let (mut rs_opts, _handle, shutdown_tx) = TestBuilder::new(backend)
        .cache_mode(CacheMode::Shallow)
        .replicate(false)
        .fallback_without_database()
        .build::<PostgreSQLAdapter>()
        .await;

    for db in ["postgres", db.as_str()] {
        rs_opts.dbname(db);
        let conn = psql_helpers::connect(rs_opts.clone()).await;
        let rows = conn.simple_query("SELECT current_database()").await.unwrap();
        assert_eq!(first_value(&rows), db);
    }

    shutdown_tx.shutdown().await;
}

/// CHANGE UPSTREAM rejects a URL that adds or removes the database of the current URL.
#[tokio::test]
#[tags(serial)]
#[upstream(postgres)]
async fn change_upstream_rejects_adding_or_removing_database() {
    readyset_tracing::init_test_logging();

    let db = derive_test_name();
    PostgreSQLAdapter::recreate_database(&db).await;

    for (from, to) in [("", db.as_str()), (db.as_str(), "")] {
        let backend = BackendBuilder::new().require_authentication(false);
        let (rs_opts, _handle, shutdown_tx) = TestBuilder::new(backend)
            .cache_mode(CacheMode::Shallow)
            .recreate_database(false)
            .replicate(false)
            .fallback_without_replication(from)
            .build::<PostgreSQLAdapter>()
            .await;
        let conn = psql_helpers::connect(rs_opts).await;

        let url = PostgreSQLAdapter::upstream_url(to);
        let err = conn
            .simple_query(&format!("ALTER READYSET CHANGE UPSTREAM TO '{url}'"))
            .await
            .expect_err("changing whether upstream URL has database should fail");
        let message = err.as_db_error().map(|e| e.message());
        assert!(
            message.is_some_and(|m| m.contains("Cannot add or remove database in upstream URL")),
            "unexpected error: {err:?}"
        );

        shutdown_tx.shutdown().await;
    }
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

/// Readyset probes cache ACLs on each cache's database. A user who can't connect to a database
/// is never served its caches.
#[tokio::test]
#[tags(serial)]
#[upstream(postgres)]
async fn acl_probes_each_database() {
    readyset_tracing::init_test_logging();

    let url_db = derive_test_name();
    let other_db = format!("{url_db}_other");
    let limited = format!("{url_db}_limited");
    PostgreSQLAdapter::recreate_database(&url_db).await;
    PostgreSQLAdapter::recreate_database(&other_db).await;
    let url_upstream = create_table(&url_db, 100).await;
    let other_upstream = create_table(&other_db, 200).await;
    url_upstream
        .simple_query(&format!(
            "DROP ROLE IF EXISTS {limited};
             CREATE ROLE {limited} LOGIN PASSWORD 'pass';
             GRANT SELECT ON t TO {limited};
             REVOKE CONNECT ON DATABASE {other_db} FROM PUBLIC"
        ))
        .await
        .unwrap();
    other_upstream
        .simple_query(&format!("GRANT SELECT ON t TO {limited}"))
        .await
        .unwrap();

    let upstream = psql_helpers::upstream_config();
    let user = upstream.get_user().expect("upstream user");
    let password = upstream.get_password().expect("upstream password");
    let password_str = std::str::from_utf8(password).expect("UTF-8 password");
    let users: AllowedUsersMap = [
        (user.to_owned(), password_str.into()),
        (limited.clone(), "pass".into()),
    ]
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
    let other_conn = psql_helpers::connect(rs_opts.clone()).await;
    rs_opts.user(&limited).password("pass");
    let limited_other_conn = psql_helpers::connect(rs_opts.clone()).await;
    rs_opts.dbname(&url_db);
    let limited_url_conn = psql_helpers::connect(rs_opts).await;

    let create = format!("CREATE SHALLOW CACHE FROM {QUERY}");
    url_conn.simple_query(&create).await.unwrap();
    other_conn.simple_query(&create).await.unwrap();

    for (conn, b) in [(&url_conn, "100"), (&other_conn, "200"), (&limited_url_conn, "100")] {
        eventually!(run_test: {
            let result = query_b(conn).await;
            AssertUnwindSafe(move || result)
        }, then_assert: |result| {
            let (value, info) = result();
            assert_eq!(value, b);
            assert_matches!(info.destination, QueryDestination::ReadysetShallow(_), "{info:?}");
        });
    }
    // The ACL denies the limited user the cache on the other database. Its session there has no
    // upstream connection, because the user can't connect to that database, so the read fails.
    for _ in 0..3 {
        limited_other_conn.simple_query(QUERY).await.unwrap_err();
    }

    shutdown_tx.shutdown().await;
}
