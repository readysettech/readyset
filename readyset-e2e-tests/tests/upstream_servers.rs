//! Weighted upstream server lists: replication failover, list edits, and the
//! `readyset.upstream_servers` virtual relation.
//!
//! CI has one MySQL server, so a dead server is a port on 127.0.0.1 that
//! nothing listens on.

use std::collections::HashMap;
use std::sync::Arc;

use mysql_async::Row;
use mysql_async::prelude::Queryable;
use readyset_client::consensus::{Authority, LocalAuthority, LocalAuthorityStore};
use readyset_client_test_helpers::TestBuilder;
use readyset_client_test_helpers::mysql_helpers::{self, MySQLAdapter};
use readyset_util::eventually;
use test_utils::{tags, upstream};

async fn readyset_status(conn: &mut mysql_async::Conn) -> String {
    let rows: Vec<Row> = conn.query("SHOW READYSET STATUS").await.unwrap();
    rows.iter()
        .find(|r| r.get::<String, _>(0).unwrap() == "Status")
        .expect("Status row not found")
        .get::<String, _>(1)
        .unwrap()
}

/// A `host:port` nothing listens on.
fn dead_server() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    format!("127.0.0.1:{port}")
}

fn url_for(server: &str, db: &str) -> String {
    format!("mysql://root:noria@{server}/{db}")
}

fn live_server() -> String {
    let url: database_utils::DatabaseURL = MySQLAdapter::url_with_db("x").parse().unwrap();
    format!("{}:{}", url.host().unwrap(), url.port())
}

/// The live server under a second `host:port`, so a list can name it as a new server.
async fn live_server_alias() -> String {
    let live = live_server();
    let addr = tokio::net::lookup_host(&live)
        .await
        .unwrap()
        .find(std::net::SocketAddr::is_ipv4)
        .expect("live server has an IPv4 address");
    let alias = addr.to_string();
    if alias == live {
        format!("localhost:{}", addr.port())
    } else {
        alias
    }
}

/// One row of `readyset.upstream_servers`.
#[derive(Debug)]
struct ServerRow {
    purpose: String,
    active_cdc: Option<bool>,
    health: Option<String>,
    weight: u32,
}

/// The rows of `readyset.upstream_servers`, keyed by `host:port`.
async fn upstream_servers(conn: &mut mysql_async::Conn) -> HashMap<String, ServerRow> {
    let rows: Vec<Row> = conn
        .query(
            "SELECT host, port, weight, purpose, active_cdc, health \
             FROM readyset.upstream_servers",
        )
        .await
        .unwrap();
    rows.into_iter()
        .map(|r| {
            let host: String = r.get(0).unwrap();
            let port: u32 = r.get(1).unwrap();
            (
                format!("{host}:{port}"),
                ServerRow {
                    weight: r.get(2).unwrap(),
                    purpose: r.get(3).unwrap(),
                    active_cdc: r.get(4).unwrap(),
                    health: r.get(5).unwrap(),
                },
            )
        })
        .collect()
}

async fn wait_online(conn: &mut mysql_async::Conn) {
    eventually!(attempts: 60, sleep: std::time::Duration::from_millis(500), run_test: {
        readyset_status(conn).await
    }, then_assert: |status| {
        assert_eq!(status, "Online");
    });
}

async fn seed_table(db_name: &str) -> mysql_async::Conn {
    mysql_helpers::recreate_database(db_name).await;
    let upstream_opts = mysql_helpers::upstream_config().db_name(Some(db_name));
    let mut upstream = mysql_async::Conn::new(upstream_opts).await.unwrap();
    upstream
        .query_drop("CREATE TABLE t1 (id INT PRIMARY KEY, val INT)")
        .await
        .unwrap();
    upstream
        .query_drop("INSERT INTO t1 VALUES (1, 10)")
        .await
        .unwrap();
    upstream
}

async fn assert_replicates(
    upstream: &mut mysql_async::Conn,
    rs: &mut mysql_async::Conn,
    id: i32,
    val: i32,
) {
    upstream
        .query_drop(format!("INSERT INTO t1 VALUES ({id}, {val})"))
        .await
        .unwrap();
    eventually!(attempts: 120, sleep: std::time::Duration::from_millis(500), run_test: {
        rs.exec_first::<(i32,), _, _>("SELECT val FROM t1 WHERE id = ?", (id,)).await.unwrap()
    }, then_assert: |row| {
        assert_eq!(row, Some((val,)));
    });
}

/// Replication and sessions skip a dead highest-weight server, and the vrel
/// reports the server replication uses.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(mysql, modern, gtid)]
async fn replication_fails_over_past_a_dead_server() {
    readyset_tracing::init_test_logging();
    let db_name = "upstream_servers_failover";
    let mut upstream = seed_table(db_name).await;
    let dead = dead_server();
    let live = live_server();
    let list = format!(
        "{}|90,{}|50",
        url_for(&dead, db_name),
        url_for(&live, db_name)
    );

    let (rs_opts, _handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .replicate_url(&list)
        .fallback(true)
        .build::<MySQLAdapter>()
        .await;
    let mut rs = mysql_async::Conn::new(rs_opts.clone()).await.unwrap();
    wait_online(&mut rs).await;

    rs.query_drop("CREATE CACHE FROM SELECT val FROM t1 WHERE id = ?")
        .await
        .unwrap();
    assert_replicates(&mut upstream, &mut rs, 2, 20).await;

    let (n,): (i64,) = rs
        .query_first("SELECT COUNT(*) FROM t1 WHERE val > 0 AND id <> 12345")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(n, 2);

    let servers = upstream_servers(&mut rs).await;
    assert_eq!(servers.len(), 2, "{servers:?}");
    let dead_row = &servers[&dead];
    let live_row = &servers[&live];
    assert_eq!(dead_row.weight, 90);
    assert_eq!(dead_row.purpose, "cdc, proxy");
    assert_eq!(dead_row.active_cdc, Some(false));
    assert_eq!(live_row.active_cdc, Some(true));
    assert_eq!(live_row.health.as_deref(), Some("up"));

    shutdown_tx.shutdown().await;
}

/// CHANGE UPSTREAM refuses bad credentials, and accepts an unreachable server
/// as down.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(mysql, modern, gtid)]
async fn change_upstream_validates_new_servers() {
    readyset_tracing::init_test_logging();
    let db_name = "upstream_servers_change_upstream";
    let _upstream = seed_table(db_name).await;
    let live = live_server();
    let dead = dead_server();

    let (rs_opts, _handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .replicate_db(db_name)
        .fallback(true)
        .build::<MySQLAdapter>()
        .await;
    let mut rs = mysql_async::Conn::new(rs_opts.clone()).await.unwrap();
    wait_online(&mut rs).await;

    let bad = format!("mysql://bad_user:bad_pass@{live}/{db_name}");
    let err = rs
        .query_drop(format!("ALTER READYSET CHANGE UPSTREAM TO '{bad}'"))
        .await
        .expect_err("bad credentials must be rejected");
    assert!(
        err.to_string().contains("credentials rejected"),
        "unexpected error: {err}"
    );
    let servers = upstream_servers(&mut rs).await;
    assert_eq!(servers.len(), 1, "list untouched: {servers:?}");

    let list = format!(
        "{}|60,{}|40",
        url_for(&live, db_name),
        url_for(&dead, db_name)
    );
    let outcomes: Vec<(String, String)> = rs
        .query(format!("ALTER READYSET CHANGE UPSTREAM TO '{list}'"))
        .await
        .unwrap();
    let outcomes: HashMap<String, String> = outcomes.into_iter().collect();
    assert_eq!(outcomes[&live], "unchanged", "{outcomes:?}");
    assert!(
        outcomes[&dead].starts_with("unreachable, marked down"),
        "{outcomes:?}"
    );

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    let servers = upstream_servers(&mut rs).await;
    assert_eq!(servers.len(), 2, "{servers:?}");
    assert_eq!(servers[&dead].health.as_deref(), Some("down"));
    assert_eq!(servers[&dead].purpose, "proxy");
    assert_eq!(servers[&live].purpose, "cdc, proxy");

    // Servers already in the list are not checked again, so use a new dead server.
    let dead2 = dead_server();
    let err = rs
        .query_drop(format!(
            "ALTER READYSET CHANGE UPSTREAM TO '{}'",
            url_for(&dead2, db_name)
        ))
        .await
        .expect_err("a list with no reachable server must be rejected");
    assert!(
        err.to_string().contains("servers are all unreachable"),
        "unexpected error: {err}"
    );

    shutdown_tx.shutdown().await;
}

/// CHANGE CDC keeps the active server when it stays in the list, and rows
/// keep flowing.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(mysql, modern, gtid)]
async fn change_cdc_while_running_keeps_the_active_server() {
    readyset_tracing::init_test_logging();
    let db_name = "upstream_servers_change_cdc";
    let mut upstream = seed_table(db_name).await;
    let live = live_server();
    let dead = dead_server();

    let (rs_opts, _handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .replicate_db(db_name)
        .fallback(true)
        .load_stored_upstream_servers(true)
        .build::<MySQLAdapter>()
        .await;
    let mut rs = mysql_async::Conn::new(rs_opts.clone()).await.unwrap();
    wait_online(&mut rs).await;
    rs.query_drop("CREATE CACHE FROM SELECT val FROM t1 WHERE id = ?")
        .await
        .unwrap();

    let list = format!(
        "{}|60,{}|10",
        url_for(&live, db_name),
        url_for(&dead, db_name)
    );
    rs.query_drop(format!("ALTER READYSET CHANGE CDC TO '{list}'"))
        .await
        .unwrap();

    assert_replicates(&mut upstream, &mut rs, 2, 20).await;
    eventually!(attempts: 20, sleep: std::time::Duration::from_millis(500), run_test: {
        upstream_servers(&mut rs).await
    }, then_assert: |servers| {
        assert_eq!(servers[&live].active_cdc, Some(true), "{servers:?}");
        assert_eq!(servers[&dead].active_cdc, Some(false), "{servers:?}");
        assert_eq!(servers[&dead].purpose, "cdc");
    });

    shutdown_tx.shutdown().await;
}

/// The vrel reports an active CDC server only while replication is on one:
/// STOP REPLICATION clears it, and START REPLICATION picks it back up.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(mysql, modern, gtid)]
async fn stopping_replication_clears_the_active_cdc_server() {
    readyset_tracing::init_test_logging();
    let db_name = "upstream_servers_stop_cdc";
    let _upstream = seed_table(db_name).await;
    let live = live_server();

    let (rs_opts, _handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .replicate_db(db_name)
        .fallback(true)
        .build::<MySQLAdapter>()
        .await;
    let mut rs = mysql_async::Conn::new(rs_opts.clone()).await.unwrap();
    wait_online(&mut rs).await;
    assert_eq!(upstream_servers(&mut rs).await[&live].active_cdc, Some(true));

    rs.query_drop("ALTER READYSET STOP REPLICATION").await.unwrap();
    eventually!(attempts: 20, sleep: std::time::Duration::from_millis(500), run_test: {
        upstream_servers(&mut rs).await
    }, then_assert: |servers| {
        assert_eq!(servers[&live].active_cdc, Some(false), "{servers:?}");
    });

    rs.query_drop("ALTER READYSET START REPLICATION").await.unwrap();
    eventually!(attempts: 40, sleep: std::time::Duration::from_millis(500), run_test: {
        upstream_servers(&mut rs).await
    }, then_assert: |servers| {
        assert_eq!(servers[&live].active_cdc, Some(true), "{servers:?}");
    });

    shutdown_tx.shutdown().await;
}

/// With `--cdc-db-url` unset, CHANGE CDC starts from an empty CDC list.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(mysql, modern, gtid)]
async fn change_cdc_without_a_configured_cdc_url() {
    readyset_tracing::init_test_logging();
    let db_name = "upstream_servers_cdc_unset";
    let mut upstream = seed_table(db_name).await;
    let live = live_server();
    let dead = dead_server();

    let (rs_opts, _handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .replicate_db(db_name)
        .cdc_db_url(false)
        .fallback(true)
        .load_stored_upstream_servers(true)
        .build::<MySQLAdapter>()
        .await;
    let mut rs = mysql_async::Conn::new(rs_opts.clone()).await.unwrap();
    wait_online(&mut rs).await;
    rs.query_drop("CREATE CACHE FROM SELECT val FROM t1 WHERE id = ?")
        .await
        .unwrap();

    let list = format!(
        "{}|60,{}|10",
        url_for(&live, db_name),
        url_for(&dead, db_name)
    );
    rs.query_drop(format!("ALTER READYSET CHANGE CDC TO '{list}'"))
        .await
        .unwrap();

    assert_replicates(&mut upstream, &mut rs, 2, 20).await;
    eventually!(attempts: 20, sleep: std::time::Duration::from_millis(500), run_test: {
        upstream_servers(&mut rs).await
    }, then_assert: |servers| {
        assert_eq!(servers.len(), 2, "{servers:?}");
        assert_eq!(servers[&live].purpose, "cdc, proxy", "{servers:?}");
        assert_eq!(servers[&live].active_cdc, Some(true), "{servers:?}");
        assert_eq!(servers[&dead].purpose, "cdc", "{servers:?}");
    });

    shutdown_tx.shutdown().await;
}

/// With `--cdc-db-url` unset, CHANGE UPSTREAM also moves replication off a
/// server that left the list.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(mysql, modern, gtid)]
async fn change_upstream_without_a_configured_cdc_url_moves_replication() {
    readyset_tracing::init_test_logging();
    let db_name = "upstream_servers_upstream_moves_cdc";
    let mut upstream = seed_table(db_name).await;
    let alias = live_server_alias().await;

    let (rs_opts, _handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .replicate_db(db_name)
        .cdc_db_url(false)
        .fallback(true)
        .load_stored_upstream_servers(true)
        .build::<MySQLAdapter>()
        .await;
    let mut rs = mysql_async::Conn::new(rs_opts.clone()).await.unwrap();
    wait_online(&mut rs).await;
    rs.query_drop("CREATE CACHE FROM SELECT val FROM t1 WHERE id = ?")
        .await
        .unwrap();

    rs.query_drop(format!(
        "ALTER READYSET CHANGE UPSTREAM TO '{}'",
        url_for(&alias, db_name)
    ))
    .await
    .unwrap();
    // The session's server left the list, so the session must reconnect.
    drop(rs);
    let mut rs = mysql_async::Conn::new(rs_opts.clone()).await.unwrap();

    assert_replicates(&mut upstream, &mut rs, 2, 20).await;
    eventually!(attempts: 20, sleep: std::time::Duration::from_millis(500), run_test: {
        upstream_servers(&mut rs).await
    }, then_assert: |servers| {
        assert_eq!(servers.len(), 1, "{servers:?}");
        assert_eq!(servers[&alias].purpose, "cdc, proxy", "{servers:?}");
        assert_eq!(servers[&alias].active_cdc, Some(true), "{servers:?}");
    });

    shutdown_tx.shutdown().await;
}

/// A CDC server that accepts logins but refuses the binlog request uses up its
/// retries, and replication fails over to the next server.
#[cfg(feature = "failure_injection")]
#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(mysql, modern, gtid)]
async fn replication_fails_over_past_a_server_that_cannot_stream() {
    use readyset_util::failpoints;

    readyset_tracing::init_test_logging();
    let db_name = "upstream_servers_cannot_stream";
    let mut upstream = seed_table(db_name).await;
    let live = live_server();
    let refusing = live_server_alias().await;
    let _fail_scenario = fail::FailScenario::setup();
    fail::cfg(
        failpoints::MYSQL_REQUEST_BINLOG,
        &format!("return({refusing})"),
    )
    .unwrap();

    let list = format!(
        "{}|90,{}|10",
        url_for(&refusing, db_name),
        url_for(&live, db_name)
    );

    let (rs_opts, _handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .replicate_url(&list)
        .fallback(true)
        .build::<MySQLAdapter>()
        .await;
    let mut rs = mysql_async::Conn::new(rs_opts.clone()).await.unwrap();
    wait_online(&mut rs).await;
    rs.query_drop("CREATE CACHE FROM SELECT val FROM t1 WHERE id = ?")
        .await
        .unwrap();

    assert_replicates(&mut upstream, &mut rs, 2, 20).await;
    eventually!(attempts: 20, sleep: std::time::Duration::from_millis(500), run_test: {
        upstream_servers(&mut rs).await
    }, then_assert: |servers| {
        assert_eq!(servers[&live].active_cdc, Some(true), "{servers:?}");
        assert_eq!(servers[&refusing].active_cdc, Some(false), "{servers:?}");
    });

    shutdown_tx.shutdown().await;
}

/// The lists live in the Authority: an edit survives a restart, and the
/// command line is ignored once the Authority holds a list.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(mysql, modern, gtid)]
async fn list_edits_persist_across_restarts() {
    readyset_tracing::init_test_logging();
    let db_name = "upstream_servers_persist";
    let _upstream = seed_table(db_name).await;
    let live = live_server();
    let dead = dead_server();
    let authority = Arc::new(Authority::from(LocalAuthority::new_with_store(Arc::new(
        LocalAuthorityStore::new(),
    ))));

    let (rs_opts, handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .replicate_db(db_name)
        .fallback(true)
        .authority(Arc::clone(&authority))
        .load_stored_upstream_servers(true)
        .build::<MySQLAdapter>()
        .await;
    let mut rs = mysql_async::Conn::new(rs_opts.clone()).await.unwrap();
    wait_online(&mut rs).await;

    let list = format!(
        "{}|60,{}|40",
        url_for(&live, db_name),
        url_for(&dead, db_name)
    );
    rs.query_drop(format!("ALTER READYSET CHANGE UPSTREAM TO '{list}'"))
        .await
        .unwrap();

    drop(rs);
    shutdown_tx.shutdown().await;
    drop(handle);

    // The old worker may still hold the default address while it shuts down.
    let (rs_opts, _handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .replicate_db(db_name)
        .fallback(true)
        .authority(authority)
        .external_addr("127.0.0.1:6034".parse().unwrap())
        .load_stored_upstream_servers(true)
        .build::<MySQLAdapter>()
        .await;
    let mut rs = mysql_async::Conn::new(rs_opts.clone()).await.unwrap();
    wait_online(&mut rs).await;
    eventually!(attempts: 20, sleep: std::time::Duration::from_millis(500), run_test: {
        upstream_servers(&mut rs).await
    }, then_assert: |servers| {
        assert_eq!(servers.len(), 2, "{servers:?}");
        assert_eq!(servers[&dead].weight, 40);
    });

    shutdown_tx.shutdown().await;
}

/// SHOW CONNECTIONS names the upstream server each session's connection uses.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(mysql, modern)]
async fn show_connections_reports_the_picked_upstream() {
    readyset_tracing::init_test_logging();
    let db_name = "upstream_servers_show_connections";
    let _upstream = seed_table(db_name).await;
    let live = live_server();

    let (rs_opts, _handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .replicate_db(db_name)
        .fallback(true)
        .build::<MySQLAdapter>()
        .await;
    let mut rs = mysql_async::Conn::new(rs_opts.clone()).await.unwrap();
    wait_online(&mut rs).await;

    let rows: Vec<(String, String, Option<String>)> = rs.query("SHOW CONNECTIONS").await.unwrap();
    assert!(
        rows.iter()
            .any(|(_, _, upstream)| upstream.as_deref() == Some(live.as_str())),
        "{rows:?}"
    );

    shutdown_tx.shutdown().await;
}
