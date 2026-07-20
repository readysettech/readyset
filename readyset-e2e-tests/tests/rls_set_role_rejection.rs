//! End-to-end test for the `SET ROLE` trust invariant on PostgreSQL.
//!
//! Readyset does not evaluate role membership itself: a `SET ROLE` is forwarded
//! on the client's own upstream connection, where the engine rejects it unless
//! the connected user was granted the role. The session mirror that RLS scoping
//! (and, in future, the cache ACL) keys reads on must therefore advance *only
//! after upstream accepts the statement*. If the mirror advances on a rejected
//! `SET ROLE`, a client can assume a role upstream refused it and be served that
//! role's cached partition -- an authorization bypass.
//!
//! This exercises the leak with a `BYPASSRLS` role: a low-privilege login user,
//! a member of `authenticated` but not of the `BYPASSRLS` `service_role`, tries
//! to `SET ROLE service_role`. Upstream rejects it. A correct adapter keeps the
//! victim scoped to its own row; a buggy one flips the mirror to the bypass role
//! and serves the shared bypass partition (every tenant's rows). Both the
//! simple/text and extended/binary protocols advance the mirror through
//! different code paths (`handle_set` at dispatch vs. `prepare_set`), so each is
//! asserted.

use std::collections::HashMap;
use std::time::Duration;

use readyset_adapter::BackendBuilder;
use readyset_adapter::backend::UnsupportedSetMode;
use readyset_client::CacheMode;
use readyset_client_metrics::QueryDestination;
use readyset_client_test_helpers::{
    Adapter, TestBuilder, derive_test_name,
    psql_helpers::{self, PostgreSQLAdapter},
};
use readyset_server::Handle;
use readyset_sql_parsing::ParsingPreset;
use readyset_tracing::init_test_logging;
use readyset_util::shutdown::ShutdownSender;
use test_utils::{tags, upstream};
use tokio::test;
use tokio_postgres::{Client, SimpleQueryMessage};

/// The victim's login role: granted `authenticated`, deliberately *not* granted
/// the `BYPASSRLS` `service_role`.
const VICTIM: &str = "acl_setrole_victim";
const VICTIM_PW: &str = "vpass";

/// Postgres wire protocol a `SET ROLE` and read run on. `Simple` is the text
/// protocol (`simple_query`); `Extended` is Parse/Bind/Execute. The mirror
/// advances through different adapter code on each, so both are covered.
#[derive(Clone, Copy, Debug)]
enum Protocol {
    Simple,
    Extended,
}

/// The `owner_id` (column 1 of `SELECT *`) of every row of a simple-query result.
fn owner_ids(rows: &[SimpleQueryMessage]) -> Vec<String> {
    rows.iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(row) => Some(row.get(1).expect("owner_id column").to_string()),
            _ => None,
        })
        .collect()
}

/// The `owner_id` of every row of an extended-protocol result.
fn owner_ids_ext(rows: &[tokio_postgres::Row]) -> Vec<String> {
    rows.iter().map(|r| r.get::<_, String>("owner_id")).collect()
}

/// Read `SELECT * FROM api.todos` on `proto`, returning each row's `owner_id`
/// sorted so multi-row results compare deterministically.
async fn read_todos(client: &Client, proto: Protocol) -> Vec<String> {
    let mut ids = match proto {
        Protocol::Simple => owner_ids(
            &client
                .simple_query("SELECT * FROM api.todos")
                .await
                .expect("read (simple)"),
        ),
        Protocol::Extended => owner_ids_ext(
            &client
                .query("SELECT * FROM api.todos", &[])
                .await
                .expect("read (extended)"),
        ),
    };
    ids.sort();
    ids
}

/// Establish the `request.jwt.claims` `sub` on `proto`.
async fn set_sub_claim(client: &Client, proto: Protocol, sub: &str) {
    let claims = format!(r#"{{"sub":"{sub}"}}"#);
    match proto {
        Protocol::Simple => {
            client
                .simple_query(&format!("SET request.jwt.claims = '{claims}'"))
                .await
                .expect("set sub claim (simple)");
        }
        Protocol::Extended => {
            client
                .query("SELECT set_config('request.jwt.claims', $1, false)", &[&claims])
                .await
                .expect("set sub claim (extended)");
        }
    }
}

/// Issue `SET ROLE <role>` on `proto`, returning the driver result so the caller
/// can assert acceptance or rejection.
async fn try_set_role(
    client: &Client,
    proto: Protocol,
    role: &str,
) -> Result<(), tokio_postgres::Error> {
    let sql = format!("SET ROLE {role}");
    match proto {
        Protocol::Simple => client.simple_query(&sql).await.map(|_| ()),
        Protocol::Extended => client.execute(&sql, &[]).await.map(|_| ()),
    }
}

/// Assert the routing of the most recently executed query.
async fn assert_dest(client: &Client, expected: QueryDestination, ctx: &str) {
    assert_eq!(
        psql_helpers::last_query_info(client).await.destination,
        expected,
        "{ctx}",
    );
}

/// Create the RLS-active `api.todos` table (scoped to the `sub` JWT claim), the
/// `authenticated`/`service_role` roles, and the low-privilege `VICTIM` login
/// role -- granted `authenticated`, not `service_role`. Run directly against the
/// upstream as the superuser.
async fn setup_upstream(test_name: &str) {
    let mut cfg = psql_helpers::upstream_config();
    cfg.dbname(test_name);
    let upstream = psql_helpers::connect(cfg).await;

    let victim_setup = format!(
        "DROP ROLE IF EXISTS {VICTIM}; \
         CREATE ROLE {VICTIM} LOGIN PASSWORD '{VICTIM_PW}'; \
         GRANT authenticated TO {VICTIM}"
    );
    for stmt in [
        "CREATE SCHEMA api",
        // Cluster-wide roles may already exist from a prior run; create only when
        // absent. `service_role` carries BYPASSRLS (the trusted service identity);
        // `authenticated` is the ordinary non-bypass app role.
        "DO $$ BEGIN \
           IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'authenticated') THEN \
             CREATE ROLE authenticated NOLOGIN; \
           END IF; \
           IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'service_role') THEN \
             CREATE ROLE service_role NOLOGIN; \
           END IF; \
         END $$",
        "ALTER ROLE service_role WITH BYPASSRLS NOLOGIN",
        victim_setup.as_str(),
        "CREATE TABLE api.todos (id int PRIMARY KEY, owner_id text, title text, done boolean)",
        "ALTER TABLE api.todos ENABLE ROW LEVEL SECURITY",
        "GRANT USAGE ON SCHEMA api TO PUBLIC",
        "GRANT SELECT ON api.todos TO PUBLIC",
        "CREATE POLICY owner ON api.todos FOR SELECT TO public \
         USING (owner_id = ((current_setting('request.jwt.claims'::text, true))::json ->> 'sub'::text))",
        "INSERT INTO api.todos VALUES \
         (1, 'alice', 'alice buys milk', false), \
         (2, 'alice', 'alice walks dog', false), \
         (3, 'bob', 'bob writes report', false)",
    ] {
        upstream
            .simple_query(stmt)
            .await
            .unwrap_or_else(|e| panic!("upstream setup `{stmt}` failed: {e}"));
    }
}

/// Bring up an RLS + authentication adapter replicating `test_name`, then connect
/// the superuser (`admin`, able to assume the bypass role) and the low-privilege
/// `victim`. Authentication must be on so each client's upstream connection
/// authenticates as them -- that is what makes upstream reject the victim's
/// `SET ROLE service_role`.
async fn connect_auth_rls(test_name: &str) -> (Client, Client, Handle, ShutdownSender) {
    let admin_user = std::env::var("PGUSER").unwrap_or_else(|_| "postgres".into());
    let admin_pw = std::env::var("PGPASSWORD").unwrap_or_else(|_| "noria".into());

    let mut users = HashMap::new();
    users.insert(admin_user.clone(), admin_pw.clone());
    users.insert(VICTIM.to_string(), VICTIM_PW.to_string());

    let (rs_opts, handle, shutdown_tx) = TestBuilder::new(
        BackendBuilder::new()
            .require_authentication(true)
            .cache_mode(CacheMode::Shallow)
            .users(users),
    )
    .recreate_database(false)
    .replicate_db(test_name)
    .fallback(true)
    .rls(true)
    .rls_poll_interval(Duration::from_secs(1))
    .unsupported_set_mode(UnsupportedSetMode::Proxy)
    .parsing_preset(ParsingPreset::for_prod())
    .build::<PostgreSQLAdapter>()
    .await;

    let mut admin_cfg = rs_opts.clone();
    admin_cfg.dbname(test_name);
    admin_cfg.user(&admin_user).password(admin_pw.as_bytes());
    let admin = psql_helpers::connect(admin_cfg).await;

    let mut victim_cfg = rs_opts.clone();
    victim_cfg.dbname(test_name);
    victim_cfg.user(VICTIM).password(VICTIM_PW.as_bytes());
    let victim = psql_helpers::connect(victim_cfg).await;

    (admin, victim, handle, shutdown_tx)
}

/// A `SET ROLE` upstream rejects must not advance the session mirror: the victim
/// stays scoped to its own row and is never served the `BYPASSRLS` partition.
async fn run_rejected_set_role_no_leak(proto: Protocol) {
    init_test_logging();

    let test_name = derive_test_name();
    PostgreSQLAdapter::recreate_database(&test_name).await;
    setup_upstream(&test_name).await;

    let (admin, victim, _handle, shutdown_tx) = connect_auth_rls(&test_name).await;

    // Prime the shared BYPASSRLS partition with every tenant's rows, using a
    // session that may legitimately assume the bypass role. This is the partition
    // a buggy mirror would wrongly serve to the victim. Fill it on `proto` so the
    // victim's attack read (same protocol) can hit it deterministically. The
    // 5-minute TTL keeps it resident for the whole test.
    admin
        .simple_query("SET ROLE service_role")
        .await
        .expect("admin assumes bypass role");
    admin
        .simple_query("CREATE SHALLOW CACHE POLICY TTL 300 SECONDS FROM SELECT * FROM api.todos")
        .await
        .expect("create cache");
    let all = vec!["alice", "alice", "bob"];
    assert_eq!(read_todos(&admin, proto).await, all, "bypass role sees all rows");
    assert_dest(&admin, QueryDestination::Upstream, "bypass partition fills").await;
    assert_eq!(read_todos(&admin, proto).await, all);
    assert_dest(&admin, QueryDestination::ReadysetShallow, "bypass partition resident").await;

    // The victim (member of `authenticated` only) establishes its own scoped
    // partition and confirms it hits.
    try_set_role(&victim, proto, "authenticated")
        .await
        .expect("victim may assume authenticated");
    set_sub_claim(&victim, proto, "bob").await;
    assert_eq!(read_todos(&victim, proto).await, vec!["bob"], "victim scoped to bob");
    assert_dest(&victim, QueryDestination::Upstream, "victim partition fills").await;
    assert_eq!(read_todos(&victim, proto).await, vec!["bob"]);
    assert_dest(&victim, QueryDestination::ReadysetShallow, "victim partition hits").await;

    // The attack: the victim asks to assume the BYPASSRLS role. Its own upstream
    // connection is a non-member, so upstream must reject the statement.
    let rejected = try_set_role(&victim, proto, "service_role").await;
    assert!(
        rejected.is_err(),
        "upstream must reject SET ROLE to a role the victim is not a member of",
    );

    // The invariant: a rejected SET ROLE must not change the session's effective
    // identity, so the victim must still be `authenticated` + bob and keep
    // hitting *its own* partition. Pre-fix the mirror advances to `service_role`
    // regardless of the rejection, moving the session's cache identity: the read
    // is knocked off bob's partition (a miss that proxies upstream), and had the
    // assumed role's partition been populated with broader rows -- the primed
    // BYPASSRLS partition here -- the victim would have been served rows it must
    // not see. The routing check catches the identity move directly; the rows
    // check guards against the leak.
    let rows = read_todos(&victim, proto).await;
    assert_eq!(
        rows,
        vec!["bob"],
        "SECURITY: a rejected SET ROLE must not expose another partition's rows; \
         the victim must still see only its own row, not {rows:?}",
    );
    assert_dest(
        &victim,
        QueryDestination::ReadysetShallow,
        "a rejected SET ROLE must leave the session on its own cached partition, \
         not move it off (the observable sign the mirror wrongly advanced)",
    )
    .await;

    shutdown_tx.shutdown().await;
}

#[test]
#[tags(serial)]
#[upstream(postgres)]
async fn pg_rejected_set_role_no_leak_simple() {
    run_rejected_set_role_no_leak(Protocol::Simple).await;
}

#[test]
#[tags(serial)]
#[upstream(postgres)]
async fn pg_rejected_set_role_no_leak_extended() {
    run_rejected_set_role_no_leak(Protocol::Extended).await;
}
