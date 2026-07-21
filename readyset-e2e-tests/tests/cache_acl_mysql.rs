//! End-to-end coverage for the cache ACL (REA-6708), MySQL.
//!
//! Mirrors the Postgres suite where the protocols agree, and additionally
//! exercises the MySQL-specific prober shape: one upstream connection cycled
//! through users with COM_CHANGE_USER, a failed switch marking the row
//! Denied, and the Phase 1 SET ROLE rule (a session cannot silently assume a
//! role, so verdicts stay keyed by the authenticated username).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use mysql_async::Conn;
use mysql_async::prelude::Queryable;
use readyset_adapter::BackendBuilder;
use readyset_adapter::backend::AllowedUsers;
use readyset_client::CacheMode;
use readyset_client_metrics::QueryDestination;
use readyset_client_test_helpers::TestBuilder;
use readyset_sql_parsing::ParsingPreset;
use readyset_server::Handle;
use readyset_client_test_helpers::mysql_helpers::{self, MySQLAdapter, last_query_info};
use readyset_tracing::init_test_logging;
use readyset_util::eventually;
use readyset_util::shutdown::ShutdownSender;
use test_utils::{tags, upstream};

const ACL_INTERVAL: Duration = Duration::from_secs(2);
const DB: &str = "noria";

/// Provision a table `alice` and `bob` may read and start Readyset with
/// authentication on and a short ACL freshness interval.
async fn setup() -> (mysql_async::Opts, Handle, ShutdownSender, Conn) {
    let mut users = HashMap::new();
    users.insert("acl_alice".to_string(), "pass".to_string());
    users.insert("acl_bob".to_string(), "pass".to_string());
    let (rs_opts, handle, shutdown_tx) = TestBuilder::new(
        BackendBuilder::new()
            .require_authentication(true)
            .cache_mode(CacheMode::Shallow)
            .users(Arc::new(AllowedUsers::new(users, None))),
    )
    .fallback(true)
    // Match the production parsing preset, mirroring the Postgres suite.
    .parsing_preset(ParsingPreset::for_prod())
    .cache_acl_refresh_interval(ACL_INTERVAL)
    .build::<MySQLAdapter>()
    .await;

    // Upstream state is provisioned after the harness is up: building it
    // recreates the database, which would drop the table.
    let upstream_opts = mysql_helpers::upstream_config().db_name(Some(DB));
    let mut upstream = Conn::new(upstream_opts).await.unwrap();
    for stmt in [
        "DROP USER IF EXISTS 'acl_alice'@'%'",
        "DROP USER IF EXISTS 'acl_bob'@'%'",
        "CREATE USER 'acl_alice'@'%' IDENTIFIED BY 'pass'",
        "CREATE USER 'acl_bob'@'%' IDENTIFIED BY 'pass'",
        "DROP TABLE IF EXISTS acl_t",
        "CREATE TABLE acl_t (id INT NOT NULL PRIMARY KEY, val TEXT)",
        "INSERT INTO acl_t VALUES (1, 'one'), (2, 'two')",
        // Table-level grants: MySQL applies db-level privilege changes to a
        // live session only on its next USE, which would mask the revocation
        // from bob's already-open proxy connection.
        &format!("GRANT SELECT ON {DB}.acl_t TO 'acl_alice'@'%'"),
        &format!("GRANT SELECT ON {DB}.acl_t TO 'acl_bob'@'%'"),
    ] {
        upstream.query_drop(stmt).await.unwrap();
    }

    (rs_opts, handle, shutdown_tx, upstream)
}

async fn connect_as(rs_opts: &mysql_async::Opts, user: &str) -> Conn {
    let opts = mysql_async::OptsBuilder::from_opts(rs_opts.clone())
        .user(Some(user))
        .pass(Some("pass"))
        .db_name(Some(DB));
    Conn::new(opts).await.unwrap()
}

async fn destination(conn: &mut Conn, query: &str) -> QueryDestination {
    conn.query_drop(query).await.unwrap();
    last_query_info(conn).await.destination
}

/// Whether the read was served from the shallow cache, whichever cache it named.
fn is_shallow(destination: &QueryDestination) -> bool {
    matches!(destination, QueryDestination::ReadysetShallow(_))
}

/// Both users converge to Allowed via the background probes -- the creator
/// included -- which cycle one prober connection through both users with
/// COM_CHANGE_USER.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(mysql)]
async fn acl_two_users_converge_mysql() {
    init_test_logging();
    let (rs_opts, _handle, shutdown_tx, _upstream) = setup().await;

    let mut alice = connect_as(&rs_opts, "acl_alice").await;
    alice
        .query_drop("CREATE SHALLOW CACHE FROM SELECT val FROM acl_t WHERE id = ?")
        .await
        .unwrap();

    let query = "SELECT val FROM acl_t WHERE id = 1";
    eventually!(
        attempts: 40,
        sleep: Duration::from_millis(250),
        message: "alice was never served from the cache she created",
        { is_shallow(&destination(&mut alice, query).await) }
    );

    // Bob starts Unknown -- served through the proxy, which re-authorizes
    // him -- until the creation-time column probe lands his Allowed. The
    // probe races this test, so only the convergence is asserted.
    let mut bob = connect_as(&rs_opts, "acl_bob").await;
    eventually!(
        attempts: 40,
        sleep: Duration::from_millis(250),
        message: "bob was never served from the shallow cache",
        { is_shallow(&destination(&mut bob, query).await) }
    );

    shutdown_tx.shutdown().await;
}

/// A REVOKE followed by ALTER READYSET FLUSH PRIVILEGES stops serving the
/// revoked user: their reads proxy to upstream, which rejects them, their row
/// resolves to denied rather than staying unknown, and the granted user keeps
/// their hit rate.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(mysql)]
async fn acl_revoke_then_flush_privileges_denies_mysql() {
    init_test_logging();
    let (rs_opts, _handle, shutdown_tx, mut upstream) = setup().await;

    let mut alice = connect_as(&rs_opts, "acl_alice").await;
    alice
        .query_drop("CREATE SHALLOW CACHE FROM SELECT val FROM acl_t WHERE id = ?")
        .await
        .unwrap();
    let query = "SELECT val FROM acl_t WHERE id = 1";
    alice.query_drop(query).await.unwrap();

    let mut bob = connect_as(&rs_opts, "acl_bob").await;
    eventually!(
        attempts: 40,
        sleep: Duration::from_millis(250),
        message: "bob never converged to Allowed",
        { is_shallow(&destination(&mut bob, query).await) }
    );

    upstream
        .query_drop(format!("REVOKE SELECT ON {DB}.acl_t FROM 'acl_bob'@'%'"))
        .await
        .unwrap();
    alice
        .query_drop("ALTER READYSET FLUSH PRIVILEGES")
        .await
        .unwrap();

    eventually!(
        attempts: 40,
        sleep: Duration::from_millis(250),
        message: "bob kept being served after his grant was revoked",
        { bob.query_drop(query).await.is_err() }
    );
    // The revoked grant was bob's only privilege on the database, so his probe
    // session can no longer enter it. That refusal has to resolve the row rather
    // than read as a transient fault leaving it unknown and retrying forever.
    eventually!(
        attempts: 40,
        sleep: Duration::from_millis(250),
        message: "bob's row never converged to denied after his grant was revoked",
        {
            let rows: Vec<(String, String)> = alice
                .query("SELECT user, verdict FROM readyset.cache_grants")
                .await
                .unwrap();
            rows.iter()
                .any(|(user, verdict)| user == "acl_bob" && verdict == "denied")
        }
    );
    eventually!(
        attempts: 40,
        sleep: Duration::from_millis(250),
        message: "alice lost her cache access after bob's revocation",
        { is_shallow(&destination(&mut alice, query).await) }
    );

    shutdown_tx.shutdown().await;
}

/// Phase 1 SET ROLE rule: a MySQL session cannot silently assume a role. The
/// statement is rejected at the adapter, so the upstream session's identity
/// never diverges from the username the verdicts are keyed by.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(mysql)]
async fn acl_set_role_rejected_mysql() {
    init_test_logging();
    let (rs_opts, _handle, shutdown_tx, _upstream) = setup().await;

    let mut alice = connect_as(&rs_opts, "acl_alice").await;
    alice
        .query_drop("CREATE SHALLOW CACHE FROM SELECT val FROM acl_t WHERE id = ?")
        .await
        .unwrap();
    let query = "SELECT val FROM acl_t WHERE id = 1";
    eventually!(
        attempts: 40,
        sleep: Duration::from_millis(250),
        message: "alice was never served from the cache she created",
        { is_shallow(&destination(&mut alice, query).await) }
    );

    assert!(alice.query_drop("SET ROLE ALL").await.is_err());

    // The session keeps serving as its authenticated user.
    assert!(is_shallow(&destination(&mut alice, query).await));

    shutdown_tx.shutdown().await;
}

/// A user dropped upstream cannot open a probe session: the failed
/// COM_CHANGE_USER marks their row Denied and their traffic is served
/// through the proxy path only.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(mysql)]
async fn acl_dropped_upstream_account_goes_denied_mysql() {
    init_test_logging();
    let (rs_opts, _handle, shutdown_tx, mut upstream) = setup().await;

    let mut alice = connect_as(&rs_opts, "acl_alice").await;
    alice
        .query_drop("CREATE SHALLOW CACHE FROM SELECT val FROM acl_t WHERE id = ?")
        .await
        .unwrap();
    let query = "SELECT val FROM acl_t WHERE id = 1";
    alice.query_drop(query).await.unwrap();

    let mut bob = connect_as(&rs_opts, "acl_bob").await;
    eventually!(
        attempts: 40,
        sleep: Duration::from_millis(250),
        message: "bob never converged to Allowed",
        { is_shallow(&destination(&mut bob, query).await) }
    );

    // Drop bob's upstream account. The prober can no longer open a session
    // as him, which is the A7 signal: his row converges to denied, visible
    // in readyset.cache_grants. (His own reads now fail upstream outright
    // -- the drop also stripped his table grant -- so the vrel is the
    // observable for the row, and alice's hit rate is the control.)
    upstream
        .query_drop("DROP USER 'acl_bob'@'%'")
        .await
        .unwrap();
    alice
        .query_drop("ALTER READYSET FLUSH PRIVILEGES")
        .await
        .unwrap();

    eventually!(
        attempts: 40,
        sleep: Duration::from_millis(250),
        message: "a dropped account's row never converged to denied",
        {
            let rows: Vec<(String, String)> = alice
                .query("SELECT user, verdict FROM readyset.cache_grants")
                .await
                .unwrap();
            rows.iter()
                .any(|(user, verdict)| user == "acl_bob" && verdict == "denied")
        }
    );
    assert!(is_shallow(&destination(&mut alice, query).await));

    shutdown_tx.shutdown().await;
}
