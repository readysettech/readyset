//! End-to-end coverage for the cache ACL (REA-6708), Postgres.
//!
//! The verdict matrix gates every shallow serve by the session's effective
//! identity: `Allowed` serves from cache, anything else routes the query to
//! upstream, which re-authorizes it. Verdicts converge in the background --
//! creator-first column probes at cache creation, per-user grant-fingerprint
//! checks each interval, and `ALTER READYSET FLUSH PRIVILEGES` on demand --
//! so these tests assert convergence with `eventually!`, never sleeps.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::test;
use tokio_postgres::{Client, SimpleQueryMessage};

use readyset_adapter::BackendBuilder;
use readyset_adapter::backend::AllowedUsers;
use readyset_client::CacheMode;
use readyset_client_metrics::QueryDestination;
use readyset_client_test_helpers::psql_helpers::{self, PostgreSQLAdapter, last_query_info};
use readyset_client_test_helpers::{Adapter, TestBuilder, derive_test_name};
use readyset_sql_parsing::ParsingPreset;
use readyset_server::Handle;
use readyset_tracing::init_test_logging;
use readyset_util::eventually;
use readyset_util::shutdown::ShutdownSender;
use test_utils::{tags, upstream};

const ACL_INTERVAL: Duration = Duration::from_secs(2);

/// A freshness interval long enough that no periodic pass runs for the
/// duration of a test, so what the test observes is what the creation-time
/// path established on its own.
const NO_PERIODIC_PASS: Duration = Duration::from_secs(600);

/// Per-test identities. Postgres roles are cluster-wide while the test
/// databases are per-test and containers are reused, so generic names would
/// collide with other tests' grants; deriving them from the test name keeps
/// every role's objects inside this test's own (recreated) database.
struct Roles {
    alice: String,
    bob: String,
    limited: String,
}

/// Room for the longest suffix below within Postgres's 63-byte identifier limit. A role
/// created under a name past that limit is stored truncated, and a client connecting under
/// the full name never matches it.
const MAX_ROLE_STEM_LEN: usize = 63 - "_limited".len();

impl Roles {
    fn for_test(test_name: &str) -> Self {
        let stem: String = test_name.chars().take(MAX_ROLE_STEM_LEN).collect();
        Self {
            alice: format!("{stem}_alice"),
            bob: format!("{stem}_bob"),
            limited: format!("{stem}_limited"),
        }
    }
}

/// Provision a table both `alice` and `bob` may read, plus a `limited` role
/// granted to alice, and start Readyset with authentication on and a short
/// ACL freshness interval.
async fn setup(
    test_name: &str,
) -> (tokio_postgres::Config, Handle, ShutdownSender, Client, Roles) {
    PostgreSQLAdapter::recreate_database(test_name).await;
    let roles = Roles::for_test(test_name);
    let Roles { alice, bob, limited } = &roles;

    let mut cfg = psql_helpers::upstream_config();
    cfg.dbname(test_name);
    let upstream = psql_helpers::connect(cfg).await;
    upstream
        .simple_query(&format!(
            "DROP ROLE IF EXISTS {alice};
             DROP ROLE IF EXISTS {bob};
             DROP ROLE IF EXISTS {limited};
             CREATE ROLE {alice} LOGIN PASSWORD 'pass';
             CREATE ROLE {bob} LOGIN PASSWORD 'pass';
             CREATE ROLE {limited} NOLOGIN;
             GRANT {limited} TO {alice};
             CREATE TABLE t (id int PRIMARY KEY, val text);
             INSERT INTO t VALUES (1, 'one'), (2, 'two');
             GRANT SELECT ON t TO {alice};
             GRANT SELECT ON t TO {bob};
             GRANT SELECT ON t TO {limited};"
        ))
        .await
        .unwrap();

    let (rs_opts, handle, shutdown_tx) =
        start_readyset(test_name, &[alice, bob], ACL_INTERVAL).await;

    (rs_opts, handle, shutdown_tx, upstream, roles)
}

/// Start Readyset against `test_name` with authentication on, shallow caching,
/// `users` as the allowed logins, and `interval` between ACL freshness passes.
async fn start_readyset(
    test_name: &str,
    users: &[&String],
    interval: Duration,
) -> (tokio_postgres::Config, Handle, ShutdownSender) {
    let users: HashMap<String, String> = users
        .iter()
        .map(|user| ((*user).clone(), "pass".to_string()))
        .collect();
    TestBuilder::new(
        BackendBuilder::new()
            .require_authentication(true)
            .cache_mode(CacheMode::Shallow)
            .users(Arc::new(AllowedUsers::new(users, None))),
    )
    .fallback(true)
    .replicate_db(test_name)
    .recreate_database(false)
    // The prod preset is the only one that parses SET ROLE into the form the
    // session mirror consumes, mirroring rls.rs.
    .parsing_preset(ParsingPreset::for_prod())
    .cache_acl_refresh_interval(interval)
    .build::<PostgreSQLAdapter>()
    .await
}

/// Connect as `user` to whichever server `cfg` points at -- the adapter's
/// `rs_opts`, or [`psql_helpers::upstream_config`] to reach upstream directly.
async fn connect_as(cfg: &tokio_postgres::Config, test_name: &str, user: &str) -> Client {
    let mut cfg = cfg.clone();
    cfg.dbname(test_name);
    cfg.user(user).password(b"pass");
    psql_helpers::connect(cfg).await
}

async fn destination(conn: &Client, query: &str) -> QueryDestination {
    conn.query(query, &[]).await.unwrap();
    last_query_info(conn).await.destination
}

/// Whether the read was served from the shallow cache, whichever cache it named.
fn is_shallow(destination: &QueryDestination) -> bool {
    matches!(destination, QueryDestination::ReadysetShallow(_))
}

/// Every user starts Unknown, routes to upstream, and starts being served
/// once the background probe lands their Allowed -- the creator included,
/// who is probed first but is no more trusted for having issued the CREATE.
#[test]
#[tags(serial, slow)]
#[upstream(postgres)]
async fn acl_unknown_user_converges_to_allowed() {
    init_test_logging();
    let test_name = derive_test_name();
    let (rs_opts, _handle, shutdown_tx, _upstream, roles) = setup(&test_name).await;

    let alice = connect_as(&rs_opts, &test_name, &roles.alice).await;
    alice
        .simple_query("CREATE SHALLOW CACHE FROM SELECT val FROM t WHERE id = 1")
        .await
        .unwrap();

    let query = "SELECT val FROM t WHERE id = 1";
    // The creator's own reads proxy until her probe lands, then fill and serve.
    eventually!(
        attempts: 40,
        sleep: Duration::from_millis(250),
        message: "alice was never served from the cache she created",
        { is_shallow(&destination(&alice, query).await) }
    );

    // Bob starts Unknown -- served through the proxy, which re-authorizes
    // him -- until the creation-time column probe lands his Allowed. The
    // probe races this test, so only the convergence is asserted.
    let bob = connect_as(&rs_opts, &test_name, &roles.bob).await;
    eventually!(
        attempts: 40,
        sleep: Duration::from_millis(250),
        message: "bob was never served from the shallow cache",
        { is_shallow(&destination(&bob, query).await) }
    );

    shutdown_tx.shutdown().await;
}

/// A REVOKE followed by ALTER READYSET FLUSH PRIVILEGES stops serving the
/// revoked user within one pass: their reads route to upstream, which rejects
/// them, while the still-granted user keeps their hit rate.
#[test]
#[tags(serial, slow)]
#[upstream(postgres)]
async fn acl_revoke_then_flush_privileges_denies() {
    init_test_logging();
    let test_name = derive_test_name();
    let (rs_opts, _handle, shutdown_tx, upstream, roles) = setup(&test_name).await;

    let alice = connect_as(&rs_opts, &test_name, &roles.alice).await;
    alice
        .simple_query("CREATE SHALLOW CACHE FROM SELECT val FROM t WHERE id = 1")
        .await
        .unwrap();
    let query = "SELECT val FROM t WHERE id = 1";
    alice.query(query, &[]).await.unwrap();

    let bob = connect_as(&rs_opts, &test_name, &roles.bob).await;
    eventually!(
        attempts: 40,
        sleep: Duration::from_millis(250),
        message: "bob never converged to Allowed",
        { is_shallow(&destination(&bob, query).await) }
    );

    upstream
        .simple_query(&format!("REVOKE SELECT ON t FROM {}", roles.bob))
        .await
        .unwrap();
    alice
        .simple_query("ALTER READYSET FLUSH PRIVILEGES")
        .await
        .unwrap();

    // The fingerprint flip re-probes bob's row to Denied; his reads then
    // proxy to upstream, which rejects them with permission denied.
    eventually!(
        attempts: 40,
        sleep: Duration::from_millis(250),
        message: "bob kept being served after his grant was revoked",
        { bob.query(query, &[]).await.is_err() }
    );
    // Alice keeps being served; her row may be mid-re-probe (the cache-set
    // change flips every fingerprint once), so allow it to settle.
    eventually!(
        attempts: 40,
        sleep: Duration::from_millis(250),
        message: "alice lost her cache access after bob's revocation",
        { is_shallow(&destination(&alice, query).await) }
    );

    shutdown_tx.shutdown().await;
}

/// The verdict overrides TrxCachePolicy::Always: a pin to the cache cannot
/// pin a user who lost access, even mid-transaction on the extended protocol.
#[test]
#[tags(serial, slow)]
#[upstream(postgres)]
async fn acl_verdict_overrides_trx_cache_policy_always() {
    init_test_logging();
    let test_name = derive_test_name();
    let (rs_opts, _handle, shutdown_tx, upstream, roles) = setup(&test_name).await;

    let alice = connect_as(&rs_opts, &test_name, &roles.alice).await;
    alice
        .simple_query("CREATE SHALLOW CACHE ALWAYS FROM SELECT val FROM t WHERE id = 1")
        .await
        .unwrap();
    let query = "SELECT val FROM t WHERE id = 1";
    alice.query(query, &[]).await.unwrap();

    let bob = connect_as(&rs_opts, &test_name, &roles.bob).await;
    eventually!(
        attempts: 40,
        sleep: Duration::from_millis(250),
        message: "bob never converged to Allowed",
        { is_shallow(&destination(&bob, query).await) }
    );

    // ALWAYS pins the cache through transactions for an allowed user.
    bob.simple_query("BEGIN").await.unwrap();
    assert!(is_shallow(&destination(&bob, query).await));
    bob.simple_query("COMMIT").await.unwrap();

    upstream
        .simple_query(&format!("REVOKE SELECT ON t FROM {}", roles.bob))
        .await
        .unwrap();
    alice
        .simple_query("ALTER READYSET FLUSH PRIVILEGES")
        .await
        .unwrap();

    // Once Denied lands, the ALWAYS pin no longer serves bob anywhere --
    // including inside a transaction; upstream rejects the proxied read.
    eventually!(
        attempts: 40,
        sleep: Duration::from_millis(250),
        message: "the ALWAYS pin kept serving a revoked user",
        {
            bob.simple_query("BEGIN").await.unwrap();
            let denied = bob.query(query, &[]).await.is_err();
            bob.simple_query("ROLLBACK").await.unwrap();
            denied
        }
    );

    shutdown_tx.shutdown().await;
}

/// A session is judged by its effective identity: after SET ROLE it serves by
/// the assumed role's row (resolved on demand), a rejected SET ROLE leaves
/// the login identity in force, and RESET ROLE restores it.
#[test]
#[tags(serial, slow)]
#[upstream(postgres)]
async fn acl_set_role_uses_effective_identity() {
    init_test_logging();
    let test_name = derive_test_name();
    let (rs_opts, _handle, shutdown_tx, _upstream, roles) = setup(&test_name).await;

    let alice = connect_as(&rs_opts, &test_name, &roles.alice).await;
    alice
        .simple_query("CREATE SHALLOW CACHE FROM SELECT val FROM t WHERE id = 1")
        .await
        .unwrap();
    let query = "SELECT val FROM t WHERE id = 1";
    eventually!(
        attempts: 40,
        sleep: Duration::from_millis(250),
        message: "alice was never served from the cache she created",
        { is_shallow(&destination(&alice, query).await) }
    );

    // A role upstream refuses never changes the session's identity: alice
    // keeps serving as alice.
    assert!(alice.simple_query("SET ROLE nonexistent_role").await.is_err());
    assert!(is_shallow(&destination(&alice, query).await));

    // The full pass discovers `limited` as assumable by alice and resolves
    // its row proactively, before any session assumes it.
    eventually!(
        attempts: 40,
        sleep: Duration::from_millis(250),
        message: "the assumable role's row was never discovered and resolved",
        {
            alice
                .simple_query("SELECT \"user\", verdict FROM readyset.cache_grants")
                .await
                .unwrap()
                .into_iter()
                .filter_map(|msg| match msg {
                    SimpleQueryMessage::Row(row) => Some((
                        row.get(0).unwrap_or_default().to_string(),
                        row.get(1).unwrap_or_default().to_string(),
                    )),
                    _ => None,
                })
                .any(|(user, verdict)| user == roles.limited && verdict == "allowed")
        }
    );

    // An accepted SET ROLE switches the row the session is judged by; the
    // discovered role row serves it.
    alice
        .simple_query(&format!("SET ROLE {}", roles.limited))
        .await
        .unwrap();
    eventually!(
        attempts: 40,
        sleep: Duration::from_millis(250),
        message: "the assumed role's row never resolved to Allowed",
        { is_shallow(&destination(&alice, query).await) }
    );

    alice.simple_query("RESET ROLE").await.unwrap();
    assert!(is_shallow(&destination(&alice, query).await));

    shutdown_tx.shutdown().await;
}

/// A prepared statement is re-judged on every execute, in both directions: a
/// handle prepared while the verdict is still Unknown starts serving once the
/// probe lands Allowed, and stops again after a revocation. A verdict must
/// never decide the prepare result itself, or the first half latches the
/// handle off-cache for its whole life.
#[test]
#[tags(serial, slow)]
#[upstream(postgres)]
async fn acl_prepared_statement_sees_revocation() {
    init_test_logging();
    let test_name = derive_test_name();
    let (rs_opts, _handle, shutdown_tx, upstream, roles) = setup(&test_name).await;

    let alice = connect_as(&rs_opts, &test_name, &roles.alice).await;
    alice
        .simple_query("CREATE SHALLOW CACHE FROM SELECT val FROM t WHERE id = $1")
        .await
        .unwrap();
    alice.query("SELECT val FROM t WHERE id = $1", &[&1i32]).await.unwrap();

    let bob = connect_as(&rs_opts, &test_name, &roles.bob).await;
    let stmt = bob.prepare("SELECT val FROM t WHERE id = $1").await.unwrap();
    eventually!(
        attempts: 40,
        sleep: Duration::from_millis(250),
        message: "bob's prepared statement never served from the cache",
        {
            bob.query(&stmt, &[&1i32]).await.unwrap();
            is_shallow(&last_query_info(&bob).await.destination)
        }
    );

    upstream
        .simple_query(&format!("REVOKE SELECT ON t FROM {}", roles.bob))
        .await
        .unwrap();
    alice
        .simple_query("ALTER READYSET FLUSH PRIVILEGES")
        .await
        .unwrap();

    // The same statement handle stops serving once Denied lands.
    eventually!(
        attempts: 40,
        sleep: Duration::from_millis(250),
        message: "a prepared statement kept serving a revoked user",
        { bob.query(&stmt, &[&1i32]).await.is_err() }
    );

    shutdown_tx.shutdown().await;
}

/// EXPLAIN LAST STATEMENT surfaces why a statement was routed off-cache.
/// A user whose probe session cannot open (NOLOGIN) is denied per A7 while
/// their live connection keeps working, so their reads proxy successfully
/// and report the ACL as the reason.
#[test]
#[tags(serial, slow)]
#[upstream(postgres)]
async fn acl_proxy_reason_surfaces_denial() {
    init_test_logging();
    let test_name = derive_test_name();
    let (rs_opts, _handle, shutdown_tx, upstream, roles) = setup(&test_name).await;

    let alice = connect_as(&rs_opts, &test_name, &roles.alice).await;
    alice
        .simple_query("CREATE SHALLOW CACHE FROM SELECT val FROM t WHERE id = 1")
        .await
        .unwrap();
    let query = "SELECT val FROM t WHERE id = 1";
    alice.query(query, &[]).await.unwrap();

    let bob = connect_as(&rs_opts, &test_name, &roles.bob).await;
    eventually!(
        attempts: 40,
        sleep: Duration::from_millis(250),
        message: "bob never converged to Allowed",
        { is_shallow(&destination(&bob, query).await) }
    );

    // Block new sessions as bob without touching his grants: his open client
    // connection keeps working, but the prober cannot open a session, which
    // denies his row (A7) and routes him through the proxy path only.
    upstream
        .simple_query(&format!("ALTER ROLE {} NOLOGIN", roles.bob))
        .await
        .unwrap();
    alice
        .simple_query("ALTER READYSET FLUSH PRIVILEGES")
        .await
        .unwrap();

    eventually!(
        attempts: 40,
        sleep: Duration::from_millis(250),
        message: "bob's off-cache routing never reported the ACL as the reason",
        {
            bob.query(query, &[]).await.unwrap();
            let info = last_query_info(&bob).await;
            info.destination == QueryDestination::Upstream
                && info.reason == "cache_acl_denied"
        }
    );

    // Alice is served from the cache with no off-cache reason.
    eventually!(
        attempts: 40,
        sleep: Duration::from_millis(250),
        message: "alice was never served from the cache with no off-cache reason",
        {
            alice.query(query, &[]).await.unwrap();
            let info = last_query_info(&alice).await;
            is_shallow(&info.destination) && info.reason == "ok"
        }
    );

    shutdown_tx.shutdown().await;
}

/// readyset.cache_grants exposes the matrix: stored verdicts with a probe
/// timestamp, derived unknown pairs with none, and rows disappear when their
/// user is dropped from the adapter.
#[test]
#[tags(serial, slow)]
#[upstream(postgres)]
async fn acl_cache_grants_relation() {
    init_test_logging();
    let test_name = derive_test_name();
    let (rs_opts, _handle, shutdown_tx, _upstream, roles) = setup(&test_name).await;

    let alice = connect_as(&rs_opts, &test_name, &roles.alice).await;
    alice
        .simple_query("CREATE SHALLOW CACHE FROM SELECT val FROM t WHERE id = 1")
        .await
        .unwrap();

    // (user, verdict, has probe timestamp) triples from the vrel. Virtual
    // relations answer on the simple/text protocol.
    async fn grants(conn: &Client) -> Vec<(String, String, bool)> {
        conn.simple_query("SELECT \"user\", cache, verdict, probed_at FROM readyset.cache_grants")
            .await
            .unwrap()
            .into_iter()
            .filter_map(|msg| match msg {
                SimpleQueryMessage::Row(row) => Some((
                    row.get(0).unwrap_or_default().to_string(),
                    row.get(2).unwrap_or_default().to_string(),
                    row.get(3).is_some(),
                )),
                _ => None,
            })
            .collect()
    }

    // Both users' pairs surface as rows straight away -- unknown with no
    // timestamp until probed, allowed with one after.
    let rows = grants(&alice).await;
    assert!(rows.iter().any(|(user, _, _)| user == &roles.alice));
    assert!(rows.iter().any(|(user, _, _)| user == &roles.bob));

    for user in [&roles.alice, &roles.bob] {
        let message = format!("{user}'s verdict never resolved in readyset.cache_grants");
        eventually!(
            attempts: 40,
            sleep: Duration::from_millis(250),
            message: &message,
            {
                grants(&alice).await.iter().any(|(candidate, verdict, probed)| {
                    candidate == user && verdict == "allowed" && *probed
                })
            }
        );
    }

    alice
        .simple_query(&format!("ALTER READYSET DROP USER '{}'", roles.bob))
        .await
        .unwrap();
    eventually!(
        attempts: 40,
        sleep: Duration::from_millis(250),
        message: "a dropped user's rows lingered in readyset.cache_grants",
        { !grants(&alice).await.iter().any(|(user, _, _)| user == &roles.bob) }
    );

    shutdown_tx.shutdown().await;
}

/// Provision a table only `alice` may read, plus a `limited` role both alice
/// and bob are members of and which holds no grant on it. The freshness
/// interval is long enough that no periodic pass runs.
async fn setup_unprivileged_role(
    test_name: &str,
) -> (tokio_postgres::Config, Handle, ShutdownSender, Roles) {
    PostgreSQLAdapter::recreate_database(test_name).await;
    let roles = Roles::for_test(test_name);
    let Roles { alice, bob, limited } = &roles;

    let mut cfg = psql_helpers::upstream_config();
    cfg.dbname(test_name);
    let upstream = psql_helpers::connect(cfg).await;
    upstream
        .simple_query(&format!(
            "DROP ROLE IF EXISTS {alice};
             DROP ROLE IF EXISTS {bob};
             DROP ROLE IF EXISTS {limited};
             CREATE ROLE {alice} LOGIN PASSWORD 'pass';
             CREATE ROLE {bob} LOGIN PASSWORD 'pass';
             CREATE ROLE {limited} NOLOGIN;
             GRANT {limited} TO {alice}, {bob};
             CREATE TABLE secrets (id int PRIMARY KEY, val text);
             INSERT INTO secrets VALUES (1, 'classified');
             GRANT SELECT ON secrets TO {alice};"
        ))
        .await
        .unwrap();

    let (rs_opts, handle, shutdown_tx) =
        start_readyset(test_name, &[alice, bob], NO_PERIODIC_PASS).await;

    (rs_opts, handle, shutdown_tx, roles)
}

/// The stored verdict for `user` in `readyset.cache_grants`, or `None` for a
/// pair with no resolved cell.
async fn verdict_for(conn: &Client, user: &str) -> Option<String> {
    conn.simple_query("SELECT \"user\", verdict FROM readyset.cache_grants")
        .await
        .unwrap()
        .into_iter()
        .filter_map(|msg| match msg {
            SimpleQueryMessage::Row(row) => Some((
                row.get(0).unwrap_or_default().to_string(),
                row.get(1).unwrap_or_default().to_string(),
            )),
            _ => None,
        })
        .find(|(candidate, _)| candidate == user)
        .map(|(_, verdict)| verdict)
}

/// Creating a cache is not evidence that the creator may read from it, so a
/// session may not turn `CREATE SHALLOW CACHE` into a grant.
///
/// bob is a member of `limited`, which holds no grant on `secrets`, so
/// upstream refuses him both as himself and under the role. Assuming the role
/// and creating the cache must not change that: alice's own entitled reads
/// fill the cache, and bob must still be refused. A shared `search_path` puts
/// both sessions on one shallow partition, which is what makes the entry alice
/// fills the same one bob would read.
#[test]
#[tags(serial, slow)]
#[upstream(postgres)]
async fn acl_role_creator_cannot_self_grant() {
    init_test_logging();
    let test_name = derive_test_name();
    let (rs_opts, _handle, shutdown_tx, roles) = setup_unprivileged_role(&test_name).await;
    let query = "SELECT val FROM secrets WHERE id = 1";

    // Ground truth, straight from upstream: bob is refused both ways.
    let bob_upstream = connect_as(&psql_helpers::upstream_config(), &test_name, &roles.bob).await;
    assert!(bob_upstream.query(query, &[]).await.is_err());
    bob_upstream
        .simple_query(&format!("SET ROLE {}", roles.limited))
        .await
        .unwrap();
    assert!(bob_upstream.query(query, &[]).await.is_err());

    let alice = connect_as(&rs_opts, &test_name, &roles.alice).await;
    alice.simple_query("SET search_path TO public").await.unwrap();

    // bob assumes the role and creates the cache. Creation only prepares the
    // statement upstream, which Postgres answers without checking table
    // privileges, so this succeeds and proves nothing about `limited`.
    let bob = connect_as(&rs_opts, &test_name, &roles.bob).await;
    bob.simple_query("SET search_path TO public").await.unwrap();
    bob.simple_query(&format!("SET ROLE {}", roles.limited))
        .await
        .unwrap();
    bob.simple_query(&format!("CREATE SHALLOW CACHE FROM {query}"))
        .await
        .unwrap();

    // No verdict is `allowed` until a probe says so, least of all one the
    // creating session wrote about itself.
    assert_ne!(
        verdict_for(&alice, &roles.limited).await.as_deref(),
        Some("allowed"),
        "creating a cache under `limited` wrote it an unprobed allowed verdict"
    );

    // Ordinary traffic: alice reads what she is entitled to read, which fills
    // the cache and starts serving her once her own probe lands.
    eventually!(
        attempts: 40,
        sleep: Duration::from_millis(250),
        message: "alice was never served her own query from the shallow cache",
        { is_shallow(&destination(&alice, query).await) }
    );

    // bob collects: the entry is populated, and the role is the only thing
    // standing between him and it.
    let leaked = bob
        .query(query, &[])
        .await
        .ok()
        .and_then(|rows| rows.first().map(|row| row.get::<_, String>(0)));
    assert_eq!(
        leaked, None,
        "bob was served the row under `limited`, which may not read `secrets`"
    );

    // The role is positively known to be denied, not merely unresolved.
    eventually!(
        attempts: 40,
        sleep: Duration::from_millis(250),
        message: "`limited` never resolved to a denied verdict",
        { verdict_for(&alice, &roles.limited).await.as_deref() == Some("denied") }
    );

    shutdown_tx.shutdown().await;
}
