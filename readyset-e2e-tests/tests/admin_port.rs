use std::collections::HashMap;
use std::sync::Arc;

use mysql_async::prelude::Queryable;
use tokio::sync::RwLock;
use tokio_postgres::{NoTls, SimpleQueryMessage};

use database_utils::UpstreamConfig;
use readyset_adapter::BackendBuilder;
use readyset_adapter::backend::AllowedUsers;
use readyset_client::consensus::UserCredentials;
use readyset_client_test_helpers::mysql_helpers::MySQLAdapter;
use readyset_client_test_helpers::psql_helpers::{self, PostgreSQLAdapter};
use readyset_client_test_helpers::{Adapter, TestBuilder, derive_test_name};
use readyset_sql_parsing::ParsingPreset;
use readyset_tracing::init_test_logging;
use test_utils::{tags, upstream};

const ADMIN_USER: &str = "admin";
const ADMIN_PASSWORD: &str = "admin";

const UNREACHABLE_MYSQL_UPSTREAM: &str = "mysql://root:noria@127.0.0.1:1/noria";
const UNREACHABLE_PSQL_UPSTREAM: &str = "postgresql://postgres:noria@127.0.0.1:1/noria";

fn admin_backend_builder(upstream_url: &str) -> BackendBuilder {
    BackendBuilder::new()
        .admin(true)
        .require_authentication(true)
        .users(Arc::new(AllowedUsers::new(
            HashMap::from([(
                ADMIN_USER.to_owned(),
                UserCredentials::new(ADMIN_PASSWORD.to_owned()),
            )]),
            None,
        )))
        .upstream_config(Some(Arc::new(RwLock::new(UpstreamConfig::from_url(
            upstream_url,
        )))))
}

fn admin_mysql_opts(rs_opts: &mysql_async::Opts, db: Option<&str>) -> mysql_async::Opts {
    mysql_async::OptsBuilder::from_opts(rs_opts.clone())
        .user(Some(ADMIN_USER))
        .pass(Some(ADMIN_PASSWORD))
        .db_name(db)
        .into()
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

#[tokio::test]
#[tags(serial)]
#[upstream(mysql)]
async fn admin_port_session_mysql() {
    init_test_logging();
    let test_name = derive_test_name();
    MySQLAdapter::recreate_database(&test_name).await;

    let (rs_opts, _handle, shutdown_tx) =
        TestBuilder::new(admin_backend_builder(UNREACHABLE_MYSQL_UPSTREAM))
            .recreate_database(false)
            .replicate_db(&test_name)
            .fallback(true)
            .build::<MySQLAdapter>()
            .await;

    // Only the admin credentials and only the Readyset schema (or no database) are accepted at
    // connect time.
    let bad_password: mysql_async::Opts =
        mysql_async::OptsBuilder::from_opts(admin_mysql_opts(&rs_opts, None))
            .pass(Some("wrong"))
            .into();
    assert!(mysql_async::Conn::new(bad_password).await.is_err());
    assert!(
        mysql_async::Conn::new(admin_mysql_opts(&rs_opts, Some(&test_name)))
            .await
            .is_err()
    );

    let mut conn = mysql_async::Conn::new(admin_mysql_opts(&rs_opts, None))
        .await
        .unwrap();
    let database: Option<String> = conn.query_first("SELECT database()").await.unwrap();
    assert_eq!(database.as_deref(), Some("readyset"));

    for stmt in [
        "SHOW READYSET STATUS",
        "DROP ALL CACHES",
        "ALTER READYSET STOP REPLICATION",
    ] {
        conn.query_drop(stmt)
            .await
            .unwrap_or_else(|e| panic!("{stmt}: {e}"));
    }
    let user: Option<String> = conn.query_first("SELECT user FROM users").await.unwrap();
    assert_eq!(user.as_deref(), Some(ADMIN_USER));

    assert!(conn.query_drop(format!("USE {test_name}")).await.is_err());
    let database: Option<String> = conn.query_first("SELECT database()").await.unwrap();
    assert_eq!(database.as_deref(), Some("readyset"));
    conn.query_drop("USE readyset").await.unwrap();

    let err = conn
        .query_drop("CREATE CACHE FROM SELECT a FROM t")
        .await
        .unwrap_err();
    assert!(err.to_string().contains("Readyset schema"), "{err}");

    assert!(conn.query_drop("SELECT a FROM missing_table").await.is_err());
    conn.query_drop("SHOW READYSET VERSION").await.unwrap();

    shutdown_tx.shutdown().await;
}

#[tokio::test]
#[tags(serial)]
#[upstream(postgres)]
async fn admin_port_session_psql() {
    init_test_logging();
    let test_name = derive_test_name();
    PostgreSQLAdapter::recreate_database(&test_name).await;

    let (rs_opts, _handle, shutdown_tx) =
        TestBuilder::new(admin_backend_builder(UNREACHABLE_PSQL_UPSTREAM))
            .recreate_database(false)
            .replicate_db(&test_name)
            .fallback(true)
            .build::<PostgreSQLAdapter>()
            .await;

    let config = |user: &str, password: &str, dbname: &str| {
        let mut config = rs_opts.clone();
        config.user(user).password(password).dbname(dbname);
        config
    };

    assert!(
        config(ADMIN_USER, ADMIN_PASSWORD, &test_name)
            .connect(NoTls)
            .await
            .is_err()
    );
    assert!(
        config(ADMIN_USER, "wrong", "readyset")
            .connect(NoTls)
            .await
            .is_err()
    );
    let conn = psql_helpers::connect(config(ADMIN_USER, ADMIN_PASSWORD, "readyset")).await;

    let messages = conn.simple_query("SELECT database()").await.unwrap();
    assert_eq!(single_value(&messages), "readyset");
    conn.simple_query("SHOW READYSET STATUS").await.unwrap();
    let messages = conn
        .simple_query(r#"SELECT "user" FROM users"#)
        .await
        .unwrap();
    assert_eq!(single_value(&messages), ADMIN_USER);

    assert!(conn.simple_query("SET search_path TO public").await.is_err());
    let messages = conn.simple_query("SELECT database()").await.unwrap();
    assert_eq!(single_value(&messages), "readyset");

    let err = conn
        .simple_query("CREATE CACHE FROM SELECT a FROM t")
        .await
        .unwrap_err();
    let msg = err
        .as_db_error()
        .map(|db_err| db_err.message().to_owned())
        .unwrap_or_else(|| err.to_string());
    assert!(msg.contains("Readyset schema"), "{msg}");

    shutdown_tx.shutdown().await;
}

#[tokio::test]
#[tags(serial)]
#[upstream(mysql)]
async fn shutdown_denied_off_admin_port_mysql() {
    init_test_logging();
    let test_name = derive_test_name();
    MySQLAdapter::recreate_database(&test_name).await;

    // The statement exists in sqlparser only, so parse as production does.
    let (rs_opts, _handle, shutdown_tx) = TestBuilder::default()
        .parsing_preset(ParsingPreset::for_prod())
        .recreate_database(false)
        .replicate_db(&test_name)
        .fallback(true)
        .build::<MySQLAdapter>()
        .await;

    let mut conn = mysql_async::Conn::new(rs_opts).await.unwrap();
    for stmt in ["ALTER READYSET SHUTDOWN", "ALTER READYSET SHUTDOWN RESET"] {
        let err = conn.query_drop(stmt).await.unwrap_err();
        assert!(err.to_string().contains("admin port"), "{stmt}: {err}");
    }

    shutdown_tx.shutdown().await;
}
