use std::mem;
use std::time::Duration;

use readyset_client_test_helpers::psql_helpers::{self, PostgreSQLAdapter};
use readyset_client_test_helpers::{Adapter, TestBuilder, derive_test_name};
use readyset_server::Handle;
use readyset_sql_parsing::ParsingPreset;
use readyset_util::eventually;
use test_utils::{tags, upstream};
use tokio_postgres::{Client, Config, SimpleQueryMessage};

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

/// Connect to `db` on Readyset.
async fn connect(rs_opts: &Config, db: &str) -> Client {
    let mut config = rs_opts.clone();
    config.dbname(db);
    psql_helpers::connect(config).await
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

/// The value of the variable `name` in `SHOW READYSET STATUS`.
async fn status_value(conn: &Client, name: &str) -> Option<String> {
    conn.simple_query("SHOW READYSET STATUS")
        .await
        .unwrap()
        .into_iter()
        .find_map(|msg| match msg {
            SimpleQueryMessage::Row(row) if row.get(0) == Some(name) => {
                row.get(1).map(String::from)
            }
            _ => None,
        })
}

/// The replication status of `channel` in `SHOW READYSET STATUS`.
async fn channel_status(conn: &Client, channel: &str) -> String {
    status_value(conn, &format!("Replication Status (channel '{channel}')"))
        .await
        .unwrap_or_else(|| panic!("no replication status for channel '{channel}'"))
}

/// Whether the Readyset replication slot in `db` is active.
async fn slot_active(db: &str) -> bool {
    let upstream = psql_helpers::connect(psql_helpers::upstream_config()).await;
    let row = upstream
        .query_one(
            "SELECT active FROM pg_replication_slots \
             WHERE slot_name LIKE 'readyset%' AND database = $1",
            &[&db],
        )
        .await
        .unwrap();
    row.get(0)
}

/// The upstream's current WAL write position.
async fn wal_position() -> String {
    let upstream = psql_helpers::connect(psql_helpers::upstream_config()).await;
    let row = upstream
        .query_one("SELECT pg_current_wal_lsn()::text", &[])
        .await
        .unwrap();
    row.get(0)
}

/// Wait until the Readyset replication slot in `db` is `active`.
async fn wait_for_slot(db: &str, active: bool) {
    eventually!(attempts: 60, sleep: Duration::from_millis(500), run_test: {
        slot_active(db).await
    }, then_assert: |is_active| {
        assert_eq!(is_active, active, "slot in {db}");
    });
}

/// `FOR CHANNEL` stops, starts and positions one database's replication channel, and the
/// statements without it act on all channels.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(postgres)]
async fn stops_and_starts_each_channel() {
    readyset_tracing::init_test_logging();

    let (url_db, other_db) = create_databases().await;

    let (rs_opts, _handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .replicate_db(&url_db)
        .fallback(true)
        .replication_catalogs(vec![other_db.clone()])
        .replication_lag_interval(1)
        // Only sqlparser parses `FOR CHANNEL`.
        .parsing_preset(ParsingPreset::for_prod())
        .build::<PostgreSQLAdapter>()
        .await;
    let rs = connect(&rs_opts, &url_db).await;

    eventually!(run_test: { status_value(&rs, "Status").await }, then_assert: |status| {
        assert_eq!(status.as_deref(), Some("Online"));
    });
    assert_eq!(channel_status(&rs, "").await, "Running");
    assert_eq!(channel_status(&rs, &other_db).await, "Running");

    // The lag view has one row per running channel.
    eventually!(run_test: {
        let mut channels: Vec<String> = rs
            .simple_query("SELECT channel FROM readyset.replication_status")
            .await
            .unwrap()
            .into_iter()
            .filter_map(|msg| match msg {
                SimpleQueryMessage::Row(row) => row.get(0).map(String::from),
                _ => None,
            })
            .collect();
        channels.sort();
        channels
    }, then_assert: |channels| {
        assert_eq!(channels, vec![String::new(), other_db.clone()]);
    });

    let other = format!("FOR CHANNEL '{other_db}'");
    rs.simple_query(&format!("ALTER READYSET STOP REPLICATION {other}"))
        .await
        .unwrap();
    assert_eq!(channel_status(&rs, &other_db).await, "Stopped");
    assert_eq!(channel_status(&rs, "").await, "Running");
    wait_for_slot(&other_db, false).await;
    wait_for_slot(&url_db, true).await;

    for statement in [
        format!("ALTER READYSET STOP REPLICATION {other}"),
        "ALTER READYSET START REPLICATION FOR CHANNEL ''".into(),
        "ALTER READYSET STOP REPLICATION FOR CHANNEL 'nope'".into(),
        format!("ALTER READYSET SET REPLICATION POSITION '0/1' FOR CHANNEL '{url_db}'"),
    ] {
        assert!(rs.simple_query(&statement).await.is_err(), "{statement}");
    }

    let position = wal_position().await;
    rs.simple_query(&format!(
        "ALTER READYSET SET REPLICATION POSITION '{position}' {other}"
    ))
    .await
    .unwrap();
    for bound in ["Minimum", "Maximum"] {
        let name = format!("{bound} Replication Offset (channel '{other_db}')");
        let offset = status_value(&rs, &name).await;
        assert_eq!(offset, Some(format!("({position}, {position})")), "{name}");
    }

    rs.simple_query(&format!("ALTER READYSET START REPLICATION {other}"))
        .await
        .unwrap();
    assert_eq!(channel_status(&rs, &other_db).await, "Running");
    wait_for_slot(&other_db, true).await;

    // The URL database's channel is '', and its name also refers to it.
    for channel in ["", url_db.as_str()] {
        rs.simple_query(&format!(
            "ALTER READYSET STOP REPLICATION FOR CHANNEL '{channel}'"
        ))
        .await
        .unwrap();
        assert_eq!(channel_status(&rs, "").await, "Stopped");
        assert_eq!(channel_status(&rs, &other_db).await, "Running");
        rs.simple_query(&format!(
            "ALTER READYSET START REPLICATION FOR CHANNEL '{channel}'"
        ))
        .await
        .unwrap();
        assert_eq!(channel_status(&rs, "").await, "Running");
    }

    // Without a channel, the statements act on all channels.
    rs.simple_query("ALTER READYSET STOP REPLICATION")
        .await
        .unwrap();
    assert_eq!(channel_status(&rs, "").await, "Stopped");
    assert_eq!(channel_status(&rs, &other_db).await, "Stopped");
    // With more than one channel, SET REPLICATION POSITION needs a channel.
    let err = rs
        .simple_query(&format!(
            "ALTER READYSET SET REPLICATION POSITION '{position}'"
        ))
        .await
        .unwrap_err();
    let message = err.as_db_error().unwrap().message();
    assert!(message.contains("requires FOR CHANNEL"), "{message}");

    // A replicator error has its channel in the variable name.
    let upstream = psql_helpers::connect(psql_helpers::upstream_config()).await;
    let allow_connections = format!("ALTER DATABASE {url_db} ALLOW_CONNECTIONS");
    upstream
        .simple_query(&format!("{allow_connections} false"))
        .await
        .unwrap();
    rs.simple_query("ALTER READYSET START REPLICATION")
        .await
        .unwrap();
    eventually!(run_test: {
        status_value(&rs, "Last replicator error (channel '')").await
    }, then_assert: |error| {
        assert!(error.is_some());
    });
    upstream
        .simple_query(&format!("{allow_connections} true"))
        .await
        .unwrap();
    assert_eq!(channel_status(&rs, "").await, "Running");
    assert_eq!(channel_status(&rs, &other_db).await, "Running");
    wait_for_slot(&url_db, true).await;
    wait_for_slot(&other_db, true).await;

    shutdown_tx.shutdown().await;
}
