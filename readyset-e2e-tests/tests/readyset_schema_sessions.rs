//! Sessions bound to the Readyset schema, with and without a reachable upstream.

use std::sync::Arc;

use mysql_async::prelude::Queryable;
use tokio::sync::RwLock;
use tokio_postgres::{Client, Config, SimpleQueryMessage};

use database_utils::UpstreamConfig;
use readyset_adapter::BackendBuilder;
use readyset_client_test_helpers::mysql_helpers::MySQLAdapter;
use readyset_client_test_helpers::psql_helpers::{self, PostgreSQLAdapter};
use readyset_client_test_helpers::{Adapter, TestBuilder, derive_test_name};
use readyset_tracing::init_test_logging;
use test_utils::{tags, upstream};

/// Upstream URLs that always refuse connections.
const UNREACHABLE_MYSQL_UPSTREAM: &str = "mysql://root:noria@127.0.0.1:1/noria";
const UNREACHABLE_PSQL_UPSTREAM: &str = "postgresql://postgres:noria@127.0.0.1:1/noria";

fn with_db(rs_opts: &mysql_async::Opts, db: &str) -> mysql_async::Opts {
    mysql_async::OptsBuilder::from_opts(rs_opts.clone())
        .db_name(Some(db))
        .into()
}

async fn connect_with_db(rs_opts: &Config, db: &str) -> Client {
    let mut config = rs_opts.clone();
    config.dbname(db);
    psql_helpers::connect(config).await
}

fn single_value(messages: &[SimpleQueryMessage]) -> String {
    messages
        .iter()
        .find_map(|message| match message {
            SimpleQueryMessage::Row(row) => Some(row.get(0).unwrap().to_string()),
            _ => None,
        })
        .unwrap()
}

async fn assert_readyset_schema_session_mysql(conn: &mut mysql_async::Conn, other_db: &str) {
    let database: Option<String> = conn.query_first("SELECT database()").await.unwrap();
    assert_eq!(database.as_deref(), Some("readyset"));

    conn.query_drop("SELECT @@version_comment LIMIT 1")
        .await
        .unwrap();

    conn.query_drop("SET NAMES utf8mb4").await.unwrap();
    conn.query_drop("BEGIN").await.unwrap();
    conn.query_drop("COMMIT").await.unwrap();

    conn.query_drop("SELECT user FROM users").await.unwrap();
    conn.query_drop("SHOW READYSET VERSION").await.unwrap();
    conn.query_drop("SHOW READYSET STATUS").await.unwrap();

    conn.query_drop(format!("USE {other_db}")).await.unwrap();
    conn.query_drop("USE readyset").await.unwrap();
    let database: Option<String> = conn.query_first("SELECT database()").await.unwrap();
    assert_eq!(database.as_deref(), Some("readyset"));
}

async fn assert_readyset_schema_session_psql(conn: &Client) {
    let messages = conn.simple_query("SELECT database()").await.unwrap();
    assert_eq!(single_value(&messages), "readyset");

    conn.simple_query("BEGIN").await.unwrap();
    conn.simple_query("COMMIT").await.unwrap();

    conn.simple_query(r#"SELECT "user" FROM users"#)
        .await
        .unwrap();
    conn.simple_query("SHOW READYSET VERSION").await.unwrap();
    conn.simple_query("SHOW READYSET STATUS").await.unwrap();

    conn.simple_query("SET search_path TO public").await.unwrap();
    conn.simple_query("SET search_path TO readyset")
        .await
        .unwrap();
    let messages = conn.simple_query("SELECT database()").await.unwrap();
    assert_eq!(single_value(&messages), "readyset");
}

#[tokio::test]
#[tags(serial)]
#[upstream(mysql)]
async fn connect_with_readyset_schema_database_mysql() {
    init_test_logging();
    let test_name = derive_test_name();
    MySQLAdapter::recreate_database(&test_name).await;

    let (rs_opts, _handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .replicate_db(&test_name)
        .fallback(true)
        .build::<MySQLAdapter>()
        .await;

    let mut conn = mysql_async::Conn::new(with_db(&rs_opts, "readyset"))
        .await
        .unwrap();
    assert_readyset_schema_session_mysql(&mut conn, &test_name).await;

    let mut normal = mysql_async::Conn::new(rs_opts).await.unwrap();
    normal
        .query_drop("CREATE TABLE rs_schema_sessions (a INT)")
        .await
        .unwrap();
    normal
        .query_drop("SELECT a FROM rs_schema_sessions")
        .await
        .unwrap();

    shutdown_tx.shutdown().await;
}

#[tokio::test]
#[tags(serial)]
#[upstream(postgres)]
async fn connect_with_readyset_schema_database_psql() {
    init_test_logging();
    let test_name = derive_test_name();
    PostgreSQLAdapter::recreate_database(&test_name).await;

    let (rs_opts, _handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .replicate_db(&test_name)
        .fallback(true)
        .build::<PostgreSQLAdapter>()
        .await;

    let conn = connect_with_db(&rs_opts, "readyset").await;
    assert_readyset_schema_session_psql(&conn).await;

    let normal = connect_with_db(&rs_opts, &test_name).await;
    normal
        .simple_query("CREATE TABLE rs_schema_sessions (a INT)")
        .await
        .unwrap();
    normal
        .simple_query("SELECT a FROM rs_schema_sessions")
        .await
        .unwrap();

    shutdown_tx.shutdown().await;
}

#[tokio::test]
#[tags(serial)]
#[upstream(mysql)]
async fn use_readyset_runs_readyset_commands_mysql() {
    init_test_logging();
    let test_name = derive_test_name();
    MySQLAdapter::recreate_database(&test_name).await;

    let (rs_opts, _handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .replicate_db(&test_name)
        .fallback(true)
        .build::<MySQLAdapter>()
        .await;

    let mut conn = mysql_async::Conn::new(rs_opts).await.unwrap();
    conn.query_drop("CREATE TABLE use_readyset_test (a INT)")
        .await
        .unwrap();

    conn.query_drop("USE readyset").await.unwrap();
    assert_readyset_schema_session_mysql(&mut conn, &test_name).await;

    conn.query_drop(format!("USE {test_name}")).await.unwrap();
    conn.query_drop("SELECT a FROM use_readyset_test")
        .await
        .unwrap();

    shutdown_tx.shutdown().await;
}

#[tokio::test]
#[tags(serial)]
#[upstream(postgres)]
async fn set_search_path_runs_readyset_commands_psql() {
    init_test_logging();
    let test_name = derive_test_name();
    PostgreSQLAdapter::recreate_database(&test_name).await;

    let (rs_opts, _handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .replicate_db(&test_name)
        .fallback(true)
        .build::<PostgreSQLAdapter>()
        .await;

    let conn = connect_with_db(&rs_opts, &test_name).await;
    conn.simple_query("CREATE TABLE search_path_test (a INT)")
        .await
        .unwrap();

    conn.simple_query("SET search_path TO readyset")
        .await
        .unwrap();
    assert_readyset_schema_session_psql(&conn).await;

    conn.simple_query("SET search_path TO public").await.unwrap();
    conn.simple_query("SELECT a FROM search_path_test")
        .await
        .unwrap();

    shutdown_tx.shutdown().await;
}

#[tokio::test]
#[tags(serial)]
#[upstream(mysql)]
async fn readyset_schema_session_with_unreachable_upstream_mysql() {
    init_test_logging();
    let test_name = derive_test_name();
    MySQLAdapter::recreate_database(&test_name).await;

    let backend_builder = BackendBuilder::default()
        .require_authentication(false)
        .upstream_config(Some(Arc::new(RwLock::new(UpstreamConfig::from_url(
            UNREACHABLE_MYSQL_UPSTREAM,
        )))));
    let (rs_opts, _handle, shutdown_tx) = TestBuilder::new(backend_builder)
        .recreate_database(false)
        .replicate_db(&test_name)
        .fallback(true)
        .build::<MySQLAdapter>()
        .await;

    let mut normal = mysql_async::Conn::new(rs_opts.clone()).await.unwrap();
    assert!(normal.query_drop("SELECT 42").await.is_err());
    normal.query_drop("USE readyset").await.unwrap();
    let database: Option<String> = normal.query_first("SELECT database()").await.unwrap();
    assert_eq!(database.as_deref(), Some("readyset"));

    let mut conn = mysql_async::Conn::new(with_db(&rs_opts, "readyset"))
        .await
        .unwrap();
    assert_readyset_schema_session_mysql(&mut conn, &test_name).await;
    conn.query_drop("ALTER READYSET STOP REPLICATION")
        .await
        .unwrap();
    conn.query_drop("ALTER READYSET START REPLICATION")
        .await
        .unwrap();

    shutdown_tx.shutdown().await;
}

#[tokio::test]
#[tags(serial)]
#[upstream(postgres)]
async fn readyset_schema_session_with_unreachable_upstream_psql() {
    init_test_logging();
    let test_name = derive_test_name();
    PostgreSQLAdapter::recreate_database(&test_name).await;

    let backend_builder = BackendBuilder::default()
        .require_authentication(false)
        .upstream_config(Some(Arc::new(RwLock::new(UpstreamConfig::from_url(
            UNREACHABLE_PSQL_UPSTREAM,
        )))));
    let (rs_opts, _handle, shutdown_tx) = TestBuilder::new(backend_builder)
        .recreate_database(false)
        .replicate_db(&test_name)
        .fallback(true)
        .build::<PostgreSQLAdapter>()
        .await;

    let normal = connect_with_db(&rs_opts, &test_name).await;
    assert!(normal.simple_query("SELECT 42").await.is_err());

    let conn = connect_with_db(&rs_opts, "readyset").await;
    assert_readyset_schema_session_psql(&conn).await;
    conn.simple_query("ALTER READYSET STOP REPLICATION")
        .await
        .unwrap();
    conn.simple_query("ALTER READYSET START REPLICATION")
        .await
        .unwrap();

    shutdown_tx.shutdown().await;
}
