//! End-to-end tests for RLS-aware shallow caching on PostgreSQL.
//!
//! Validates the PostgREST-shaped scenario: a shallow cache over an
//! RLS-active table partitions per session security context, keyed on the
//! `SET ROLE` + `request.jwt.claims` the client establishes. A repeated read
//! on the same context hits the cache; a different `sub` claim is a distinct
//! partition that never serves the prior tenant's cached rows.
//!
//! These cover both the simple/text protocol (`simple_query` -- `SET ROLE` and
//! the namespaced `SET request.jwt.claims = ...` form, as a `psql` session
//! uses) and the extended protocol (`query`/`execute` with bound params --
//! parameterized `set_config`, as PostgREST uses) end to end. A parameterized
//! cache additionally proves the no-leak invariant under a primary-key probe: a
//! tenant looking up another tenant's row by id sees zero rows, never the
//! other's cached row.

use std::time::Duration;

use readyset_adapter::backend::UnsupportedSetMode;
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

/// The `owner_id` (column 1 of `SELECT *`) of every row in a simple-query
/// result, in result order.
fn owner_ids(rows: &[SimpleQueryMessage]) -> Vec<String> {
    rows.iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(row) => {
                Some(row.get(1).expect("owner_id column").to_string())
            }
            _ => None,
        })
        .collect()
}

/// The `owner_id` of every row in an extended-protocol (`query`) result.
fn owner_ids_ext(rows: &[tokio_postgres::Row]) -> Vec<String> {
    rows.iter().map(|r| r.get::<_, String>("owner_id")).collect()
}

/// Postgres wire protocol a step runs on. `Simple` is the text protocol
/// (`simple_query`, as psql sends); `Extended` is Parse/Bind/Execute
/// (`query`/`execute`, as most drivers and PostgREST send). The context-mutation
/// and read paths differ between the two, so each functional case is asserted on
/// both.
#[derive(Clone, Copy, Debug)]
enum Protocol {
    Simple,
    Extended,
}

/// Bring up an RLS-enabled Readyset adapter replicating `test_name` and connect
/// a client. Hold the returned `Handle` for the adapter's life and call
/// `shutdown_tx.shutdown()` when done.
async fn connect_rls(test_name: &str) -> (Client, Handle, ShutdownSender) {
    let (rs_opts, handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .replicate_db(test_name)
        .fallback(true)
        .rls(true)
        // Match the `--auto-cache` RLS deployment: Shallow cache mode, where
        // InRequestPath auto-migration creates RLS-aware shallow scoped caches.
        .cache_mode(readyset_client::CacheMode::Shallow)
        // Poll the catalog every second so policy-change invalidation is observed
        // within a test's timeout (default is 60s).
        .rls_poll_interval(Duration::from_secs(1))
        // Match the adapter binary's default: an unsupported SET proxies the
        // session upstream rather than erroring.
        .unsupported_set_mode(UnsupportedSetMode::Proxy)
        .parsing_preset(ParsingPreset::for_prod())
        .build::<PostgreSQLAdapter>()
        .await;
    let mut rs_cfg = rs_opts.clone();
    rs_cfg.dbname(test_name);
    (psql_helpers::connect(rs_cfg).await, handle, shutdown_tx)
}

/// Establish the `request.jwt.claims` `sub` on `proto`: a namespaced `SET` on
/// the simple protocol, a parameterized `set_config` on the extended protocol.
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
                .query(
                    "SELECT set_config('request.jwt.claims', $1, false)",
                    &[&claims],
                )
                .await
                .expect("set sub claim (extended)");
        }
    }
}

/// Establish the `request.jwt.claims` `sub` transaction-locally via
/// `set_config(..., true)` on `proto` (the `is_local = true` form).
async fn set_sub_claim_local(client: &Client, proto: Protocol, sub: &str) {
    let claims = format!(r#"{{"sub":"{sub}"}}"#);
    match proto {
        Protocol::Simple => {
            client
                .simple_query(&format!(
                    "SELECT set_config('request.jwt.claims', '{claims}', true)"
                ))
                .await
                .expect("set local claim (simple)");
        }
        Protocol::Extended => {
            client
                .query(
                    "SELECT set_config('request.jwt.claims', $1, true)",
                    &[&claims],
                )
                .await
                .expect("set local claim (extended)");
        }
    }
}

/// Run a context-mutation utility statement (`RESET`, `RESET ALL`, `DISCARD
/// ALL`, `SET SESSION AUTHORIZATION`, ...) on `proto`.
async fn run_stmt(client: &Client, proto: Protocol, sql: &str) {
    match proto {
        Protocol::Simple => {
            client
                .simple_query(sql)
                .await
                .unwrap_or_else(|e| panic!("`{sql}` (simple) failed: {e}"));
        }
        Protocol::Extended => {
            client
                .execute(sql, &[])
                .await
                .unwrap_or_else(|e| panic!("`{sql}` (extended) failed: {e}"));
        }
    }
}

/// Like [`read_owner_ids`] but returns the upstream error instead of panicking,
/// for polling a cache into a ready/observed state.
async fn try_read_owner_ids(
    client: &Client,
    proto: Protocol,
    sql: &str,
) -> Result<Vec<String>, tokio_postgres::Error> {
    let mut ids = match proto {
        Protocol::Simple => owner_ids(&client.simple_query(sql).await?),
        Protocol::Extended => owner_ids_ext(&client.query(sql, &[]).await?),
    };
    ids.sort();
    Ok(ids)
}

/// Run `sql` (which must project an `owner_id` column at index 1) on `proto` and
/// return the `owner_id`s sorted, so multi-row results compare deterministically
/// without an ORDER BY in the cached query.
async fn read_owner_ids(client: &Client, proto: Protocol, sql: &str) -> Vec<String> {
    let mut ids = match proto {
        Protocol::Simple => owner_ids(&client.simple_query(sql).await.expect("read (simple)")),
        Protocol::Extended => owner_ids_ext(&client.query(sql, &[]).await.expect("read (extended)")),
    };
    ids.sort();
    ids
}

/// Read `SELECT * FROM api.todos` on `proto`, returning each row's `owner_id`.
async fn read_todos(client: &Client, proto: Protocol) -> Vec<String> {
    read_owner_ids(client, proto, "SELECT * FROM api.todos").await
}

/// Run a single-value count query (`SELECT count(*) ...`) on `proto`.
async fn read_count(client: &Client, proto: Protocol, sql: &str) -> i64 {
    match proto {
        Protocol::Simple => client
            .simple_query(sql)
            .await
            .expect("count (simple)")
            .iter()
            .find_map(|m| match m {
                SimpleQueryMessage::Row(r) => Some(r.get(0).expect("count value").parse().unwrap()),
                _ => None,
            })
            .expect("count row"),
        Protocol::Extended => client.query(sql, &[]).await.expect("count (extended)")[0].get(0),
    }
}

/// Set the full `request.jwt.claims` JSON on `proto` (session scope).
async fn set_claims(client: &Client, proto: Protocol, claims: &str) {
    match proto {
        Protocol::Simple => {
            client
                .simple_query(&format!("SET request.jwt.claims = '{claims}'"))
                .await
                .expect("set claims (simple)");
        }
        Protocol::Extended => {
            client
                .query("SELECT set_config('request.jwt.claims', $1, false)", &[&claims])
                .await
                .expect("set claims (extended)");
        }
    }
}

/// Read a single todo by primary key on `proto`, returning matching `owner_id`s
/// (empty when the row is invisible to the session's security context).
async fn read_todo_by_id(client: &Client, proto: Protocol, id: i32) -> Vec<String> {
    match proto {
        Protocol::Simple => owner_ids(
            &client
                .simple_query(&format!("SELECT * FROM api.todos WHERE id = {id}"))
                .await
                .expect("read todo by id (simple)"),
        ),
        Protocol::Extended => owner_ids_ext(
            &client
                .query("SELECT * FROM api.todos WHERE id = $1", &[&id])
                .await
                .expect("read todo by id (extended)"),
        ),
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

/// Prove Readyset serves the cached result of `sql` for the current security
/// context: a first read fills the partition (or it is already resident), and the
/// second read must return `expected` *from the shallow cache* -- not an upstream
/// passthrough. This is the assertion that matters for a cache e2e test; checking
/// rows from a single (upstream-served) read only proves upstream is correct. Not
/// for empty results on the extended protocol, where an empty result is not
/// served from cache.
async fn assert_owner_ids_cached(
    client: &Client,
    proto: Protocol,
    sql: &str,
    expected: &[&str],
    ctx: &str,
) {
    read_owner_ids(client, proto, sql).await;
    let rows = read_owner_ids(client, proto, sql).await;
    let got: Vec<&str> = rows.iter().map(String::as_str).collect();
    assert_eq!(got, expected, "{ctx}: cached rows");
    assert_dest(client, QueryDestination::ReadysetShallow, ctx).await;
}

/// Like [`assert_owner_ids_cached`] for the default `SELECT * FROM api.todos`.
async fn assert_served_from_cache(client: &Client, proto: Protocol, expected: &[&str], ctx: &str) {
    assert_owner_ids_cached(client, proto, "SELECT * FROM api.todos", expected, ctx).await;
}

/// Prove the cached count of `sql` is served from the shallow cache.
async fn assert_count_cached(client: &Client, proto: Protocol, sql: &str, expected: i64, ctx: &str) {
    read_count(client, proto, sql).await;
    let n = read_count(client, proto, sql).await;
    assert_eq!(n, expected, "{ctx}: cached count");
    assert_dest(client, QueryDestination::ReadysetShallow, ctx).await;
}

/// Prove the session fails closed: two identical reads both route `Upstream`. A
/// single `Upstream` read could be a first-time cache miss; a trusted session
/// would serve the second from cache, so a *second* `Upstream` confirms the
/// session is genuinely off-cache (e.g. marked untrusted).
async fn assert_fails_closed(client: &Client, proto: Protocol, ctx: &str) {
    read_todos(client, proto).await;
    assert_dest(client, QueryDestination::Upstream, &format!("{ctx} (first read)")).await;
    read_todos(client, proto).await;
    assert_dest(
        client,
        QueryDestination::Upstream,
        &format!("{ctx} (repeat still upstream, not a miss)"),
    )
    .await;
}

/// Create the RLS-active `api.todos` table (PostgREST-style shape, scoped to
/// the `sub` JWT claim) on the upstream and seed alice/bob rows. `authenticated`
/// does not bypass RLS, so the adapter keys the cache by the resolved claim.
async fn setup_todos_upstream(test_name: &str) {
    let mut cfg = psql_helpers::upstream_config();
    cfg.dbname(test_name);
    let upstream = psql_helpers::connect(cfg).await;
    for stmt in [
        "CREATE SCHEMA api",
        // Roles are cluster-wide and may already exist (e.g. a PostgREST-style
        // upstream), so create each only when absent. `service_role` carries
        // BYPASSRLS (the trusted service identity); `anon` is the unauthenticated
        // role; `authenticated` is the ordinary non-bypass app role.
        "DO $$ BEGIN \
           IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'authenticated') THEN \
             CREATE ROLE authenticated NOLOGIN; \
           END IF; \
           IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'anon') THEN \
             CREATE ROLE anon NOLOGIN; \
           END IF; \
           IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'service_role') THEN \
             CREATE ROLE service_role NOLOGIN; \
           END IF; \
         END $$",
        // Ensure the attribute even if the role pre-existed from a prior run.
        "ALTER ROLE service_role WITH BYPASSRLS NOLOGIN",
        "CREATE TABLE api.todos (id int PRIMARY KEY, owner_id text, title text, done boolean)",
        "ALTER TABLE api.todos ENABLE ROW LEVEL SECURITY",
        "GRANT USAGE ON SCHEMA api TO PUBLIC",
        "GRANT SELECT ON api.todos TO PUBLIC",
        // Policy applies to every non-bypass role (public), keyed on the `sub`
        // claim: an unset claim resolves to NULL and scopes the read to no rows.
        "CREATE POLICY owner ON api.todos FOR SELECT TO public \
         USING (owner_id = ((current_setting('request.jwt.claims'::text, true))::json ->> 'sub'::text))",
        "INSERT INTO api.todos VALUES \
         (1, 'alice', 'alice buys milk', false), \
         (2, 'alice', 'alice walks dog', false), \
         (3, 'bob', 'bob writes report', false)",
        // Plain (non-RLS) lookup table, one row per owner, for the RLS-JOIN-plain
        // case (Q3): the join is filtered by api.todos' policy.
        "CREATE TABLE api.projects (owner_id text PRIMARY KEY, plan text)",
        "GRANT SELECT ON api.projects TO PUBLIC",
        "INSERT INTO api.projects VALUES ('alice', 'pro'), ('bob', 'free')",
        // Second RLS table keyed on a DIFFERENT claim (`tenant`), for the
        // two-RLS-table join (Q4): the join's partition is keyed by the union of
        // both tables' GUCs (`sub` and `tenant`).
        "CREATE TABLE api.orders (id int PRIMARY KEY, tenant text, amount int)",
        "ALTER TABLE api.orders ENABLE ROW LEVEL SECURITY",
        "GRANT SELECT ON api.orders TO PUBLIC",
        "CREATE POLICY tenant_isolation ON api.orders FOR SELECT TO public \
         USING (tenant = ((current_setting('request.jwt.claims'::text, true))::json ->> 'tenant'::text))",
        "INSERT INTO api.orders VALUES \
         (1, 'acme', 100), (2, 'acme', 200), (3, 'globex', 300)",
        // A table that starts WITHOUT RLS, for the enable-RLS-later case: a cache
        // over it is Plain until RLS is turned on at runtime.
        "CREATE TABLE api.docs (id int PRIMARY KEY, owner_id text, body text)",
        "GRANT SELECT ON api.docs TO PUBLIC",
        "INSERT INTO api.docs VALUES (1, 'alice', 'a doc'), (2, 'bob', 'b doc')",
        // Search-path decoy: a same-named `documents` in two schemas. The RLS-
        // active `private.documents` is what a `search_path = private, public`
        // session reads; the non-RLS `public.documents` is the decoy that a
        // public-defaulting analyzer would mis-resolve to (building a leaking
        // Plain cache).
        "CREATE TABLE public.documents (id int PRIMARY KEY, owner_id text, body text)",
        "GRANT SELECT ON public.documents TO PUBLIC",
        "INSERT INTO public.documents VALUES (1, 'decoy', 'public decoy')",
        "CREATE SCHEMA private",
        "GRANT USAGE ON SCHEMA private TO PUBLIC",
        "CREATE TABLE private.documents (id int PRIMARY KEY, owner_id text, body text)",
        "ALTER TABLE private.documents ENABLE ROW LEVEL SECURITY",
        "GRANT SELECT ON private.documents TO PUBLIC",
        "CREATE POLICY doc_owner ON private.documents FOR SELECT TO public \
         USING (owner_id = ((current_setting('request.jwt.claims'::text, true))::json ->> 'sub'::text))",
        "INSERT INTO private.documents VALUES \
         (1, 'bob', 'bob private'), \
         (2, 'alice', 'alice private one'), \
         (3, 'alice', 'alice private two')",
    ] {
        upstream
            .simple_query(stmt)
            .await
            .unwrap_or_else(|e| panic!("upstream setup `{stmt}` failed: {e}"));
    }
}

/// A shallow cache over an RLS-active table partitions by the session's
/// security context: the same `sub` claim hits the cache, a different `sub`
/// is an isolated partition, and returning to the first claim still serves
/// only that tenant's rows.
#[test]
#[tags(serial)]
#[upstream(postgres)]
async fn pg_rls_shallow_cache_partitions_by_session() {
    init_test_logging();

    let test_name = derive_test_name();
    PostgreSQLAdapter::recreate_database(&test_name).await;
    setup_todos_upstream(&test_name).await;

    // Bootstrap RLS from the upstream catalog and use the prod parser preset
    // (the only one that parses `SET ROLE` and namespaced `SET`, matching the
    // adapter binary).
    let (rs, _handle, shutdown_tx) = connect_rls(&test_name).await;

    // Establish the bob security context (simple protocol, as psql does).
    rs.simple_query("SET ROLE authenticated")
        .await
        .expect("set role");
    rs.simple_query("SET request.jwt.claims = '{\"sub\":\"bob\"}'")
        .await
        .expect("set bob claims");

    // First read: creates the scoped shallow cache and fills bob's partition
    // from upstream, where RLS scopes the result to bob. The 5-minute TTL keeps
    // the filled partition resident for the whole test, so the later
    // "served from cache" assertions can't flake on eviction.
    let rows = rs
        .simple_query("SELECT /*rs+ CREATE SHALLOW CACHE POLICY TTL 300 SECONDS */ * FROM api.todos")
        .await
        .expect("bob first read");
    assert_eq!(owner_ids(&rows), vec!["bob"], "upstream RLS scopes to bob");
    assert_eq!(
        psql_helpers::last_query_info(&rs).await.destination,
        QueryDestination::Upstream,
    );

    // Second read on the same context: served from the shallow cache.
    let rows = rs
        .simple_query("SELECT * FROM api.todos")
        .await
        .expect("bob second read");
    assert_eq!(owner_ids(&rows), vec!["bob"]);
    assert_eq!(
        psql_helpers::last_query_info(&rs).await.destination,
        QueryDestination::ReadysetShallow,
        "repeated read in the same context must hit the shallow cache",
    );

    // Switch to the alice context: a distinct partition. The first read must
    // miss -- it must NOT serve bob's cached row -- and fill alice's rows.
    rs.simple_query("SET request.jwt.claims = '{\"sub\":\"alice\"}'")
        .await
        .expect("set alice claims");
    let rows = rs
        .simple_query("SELECT * FROM api.todos")
        .await
        .expect("alice first read");
    assert_eq!(
        owner_ids(&rows),
        vec!["alice", "alice"],
        "alice sees her own rows, not bob's cached row",
    );
    assert_eq!(
        psql_helpers::last_query_info(&rs).await.destination,
        QueryDestination::Upstream,
        "a new security context is a distinct partition (cache miss)",
    );

    // Repeat as alice: served from alice's partition.
    let rows = rs
        .simple_query("SELECT * FROM api.todos")
        .await
        .expect("alice second read");
    assert_eq!(owner_ids(&rows), vec!["alice", "alice"]);
    assert_eq!(
        psql_helpers::last_query_info(&rs).await.destination,
        QueryDestination::ReadysetShallow,
    );

    // Back to bob: bob's partition is intact and still hits, uncontaminated by
    // alice's fill.
    rs.simple_query("SET request.jwt.claims = '{\"sub\":\"bob\"}'")
        .await
        .expect("reset bob claims");
    let rows = rs
        .simple_query("SELECT * FROM api.todos")
        .await
        .expect("bob third read");
    assert_eq!(owner_ids(&rows), vec!["bob"]);
    assert_eq!(
        psql_helpers::last_query_info(&rs).await.destination,
        QueryDestination::ReadysetShallow,
    );

    shutdown_tx.shutdown().await;
}

/// The production PostgREST path: the security context is established with a
/// parameterized `set_config('request.jwt.claims', $1, ...)` over the extended
/// protocol (Parse/Bind/Execute), not a simple-protocol `SET`. The scoped
/// shallow cache must partition and hit the same way, and a different bound
/// claim must be an isolated partition.
#[test]
#[tags(serial)]
#[upstream(postgres)]
async fn pg_rls_shallow_cache_extended_protocol_set_config() {
    init_test_logging();

    let test_name = derive_test_name();
    PostgreSQLAdapter::recreate_database(&test_name).await;
    setup_todos_upstream(&test_name).await;

    let (rs, _handle, shutdown_tx) = connect_rls(&test_name).await;

    // The 5-minute TTL keeps each filled partition resident for the test, so the
    // "served from cache" assertions can't flake on eviction.
    rs.simple_query("CREATE SHALLOW CACHE POLICY TTL 300 SECONDS FROM SELECT * FROM api.todos")
        .await
        .expect("create cache");

    // Establish the role via simple protocol, then the JWT claim via the
    // extended protocol -- a parameterized `set_config` Parse/Bind/Execute,
    // the form PostgREST emits. The latter is the path under test.
    rs.simple_query("SET ROLE authenticated")
        .await
        .expect("set role");
    rs.query(
        "SELECT set_config('request.jwt.claims', $1, false)",
        &[&r#"{"sub":"bob"}"#],
    )
    .await
    .expect("set bob claims");

    // First read (extended): fills bob's partition from upstream.
    let rows = rs
        .query("SELECT * FROM api.todos", &[])
        .await
        .expect("bob first read");
    assert_eq!(owner_ids_ext(&rows), vec!["bob"]);
    assert_eq!(
        psql_helpers::last_query_info(&rs).await.destination,
        QueryDestination::Upstream,
    );

    // Second read on the same context: served from the shallow cache.
    let rows = rs
        .query("SELECT * FROM api.todos", &[])
        .await
        .expect("bob second read");
    assert_eq!(owner_ids_ext(&rows), vec!["bob"]);
    assert_eq!(
        psql_helpers::last_query_info(&rs).await.destination,
        QueryDestination::ReadysetShallow,
        "repeated read with the bound claim must hit the shallow cache",
    );

    // Rebind a different claim: a distinct partition (miss, alice's rows).
    rs.query(
        "SELECT set_config('request.jwt.claims', $1, false)",
        &[&r#"{"sub":"alice"}"#],
    )
    .await
    .expect("set alice claims");
    let rows = rs
        .query("SELECT * FROM api.todos", &[])
        .await
        .expect("alice first read");
    assert_eq!(
        owner_ids_ext(&rows),
        vec!["alice", "alice"],
        "rebinding the claim must isolate alice from bob's cached partition",
    );
    assert_eq!(
        psql_helpers::last_query_info(&rs).await.destination,
        QueryDestination::Upstream,
    );

    // Repeat as alice: served from alice's partition (cached retrieval).
    let rows = rs
        .query("SELECT * FROM api.todos", &[])
        .await
        .expect("alice second read");
    assert_eq!(owner_ids_ext(&rows), vec!["alice", "alice"]);
    assert_eq!(
        psql_helpers::last_query_info(&rs).await.destination,
        QueryDestination::ReadysetShallow,
        "repeated read in alice's context must hit the shallow cache",
    );

    // Back to bob: bob's partition is intact and still served from cache,
    // uncontaminated by alice's fill.
    rs.query(
        "SELECT set_config('request.jwt.claims', $1, false)",
        &[&r#"{"sub":"bob"}"#],
    )
    .await
    .expect("reset bob claims");
    let rows = rs
        .query("SELECT * FROM api.todos", &[])
        .await
        .expect("bob third read");
    assert_eq!(owner_ids_ext(&rows), vec!["bob"]);
    assert_eq!(
        psql_helpers::last_query_info(&rs).await.destination,
        QueryDestination::ReadysetShallow,
        "bob's partition is intact and served from cache",
    );

    shutdown_tx.shutdown().await;
}

/// A parameterized shallow cache partitions on the bound parameter *and* the
/// security context: a tenant probing another tenant's primary key sees zero
/// rows (never the other's cached row), and the same key under each tenant's own
/// context returns that tenant's row from that tenant's partition. The no-leak
/// invariant is the point; it holds on both protocols (Q2).
async fn run_cross_tenant_pk(proto: Protocol) {
    init_test_logging();

    let test_name = derive_test_name();
    PostgreSQLAdapter::recreate_database(&test_name).await;
    setup_todos_upstream(&test_name).await;
    let (rs, _handle, shutdown_tx) = connect_rls(&test_name).await;

    rs.simple_query("SET ROLE authenticated")
        .await
        .expect("set role");
    rs.simple_query(
        "CREATE SHALLOW CACHE POLICY TTL 300 SECONDS FROM SELECT * FROM api.todos WHERE id = $1",
    )
        .await
        .expect("create parameterized cache");

    // id 3 is bob's row; ids 1 and 2 are alice's.
    set_sub_claim(&rs, proto, "bob").await;
    assert_eq!(
        read_todo_by_id(&rs, proto, 3).await,
        vec!["bob"],
        "bob sees his own row by id",
    );
    assert_dest(&rs, QueryDestination::Upstream, "bob id=3 fills").await;
    assert_eq!(read_todo_by_id(&rs, proto, 3).await, vec!["bob"]);
    assert_dest(&rs, QueryDestination::ReadysetShallow, "bob id=3 hits").await;

    // Cross-tenant probe: bob asks for alice's row by primary key -> zero rows.
    // The no-leak invariant: bob's partition never serves alice's row.
    let empty: Vec<String> = vec![];
    assert_eq!(
        read_todo_by_id(&rs, proto, 1).await,
        empty,
        "bob cannot see alice's row by id",
    );
    assert_dest(&rs, QueryDestination::Upstream, "bob id=1 fills empty").await;

    // The same key under alice's context returns alice's row from alice's
    // partition, not bob's cached empty result -- and repeats hit her partition.
    set_sub_claim(&rs, proto, "alice").await;
    assert_eq!(
        read_todo_by_id(&rs, proto, 1).await,
        vec!["alice"],
        "alice sees her own row by id, not bob's cached empty result",
    );
    assert_dest(&rs, QueryDestination::Upstream, "alice id=1 fills").await;
    assert_eq!(read_todo_by_id(&rs, proto, 1).await, vec!["alice"]);
    assert_dest(&rs, QueryDestination::ReadysetShallow, "alice id=1 hits").await;

    shutdown_tx.shutdown().await;
}

#[test]
#[tags(serial)]
#[upstream(postgres)]
async fn pg_rls_shallow_cache_cross_tenant_pk_simple() {
    run_cross_tenant_pk(Protocol::Simple).await;
}

#[test]
#[tags(serial)]
#[upstream(postgres)]
async fn pg_rls_shallow_cache_cross_tenant_pk_extended() {
    run_cross_tenant_pk(Protocol::Extended).await;
}

/// A `BYPASSRLS` role (`service_role`) is not subject to policy: it sees every
/// row regardless of claims, and the result is cacheable and served from the
/// shared bypass partition on repeat (F12).
async fn run_bypass_role(proto: Protocol) {
    init_test_logging();

    let test_name = derive_test_name();
    PostgreSQLAdapter::recreate_database(&test_name).await;
    setup_todos_upstream(&test_name).await;
    let (rs, _handle, shutdown_tx) = connect_rls(&test_name).await;

    rs.simple_query("SET ROLE service_role")
        .await
        .expect("set role");
    rs.simple_query("CREATE SHALLOW CACHE POLICY TTL 300 SECONDS FROM SELECT * FROM api.todos")
        .await
        .expect("create cache");

    let all = vec!["alice", "alice", "bob"];
    assert_eq!(read_todos(&rs, proto).await, all, "bypass role sees all rows");
    assert_dest(&rs, QueryDestination::Upstream, "bypass fills").await;
    assert_eq!(read_todos(&rs, proto).await, all);
    assert_dest(&rs, QueryDestination::ReadysetShallow, "bypass hits").await;

    shutdown_tx.shutdown().await;
}

#[test]
#[tags(serial)]
#[upstream(postgres)]
async fn pg_rls_shallow_cache_bypass_role_simple() {
    run_bypass_role(Protocol::Simple).await;
}

#[test]
#[tags(serial)]
#[upstream(postgres)]
async fn pg_rls_shallow_cache_bypass_role_extended() {
    run_bypass_role(Protocol::Extended).await;
}

/// The unauthenticated `anon` role has no claim, so the policy resolves the
/// `sub` to NULL and scopes the read to zero rows -- a distinct partition that
/// never serves an authenticated tenant's rows (F15).
async fn run_anon_no_claims(proto: Protocol) {
    init_test_logging();

    let test_name = derive_test_name();
    PostgreSQLAdapter::recreate_database(&test_name).await;
    setup_todos_upstream(&test_name).await;
    let (rs, _handle, shutdown_tx) = connect_rls(&test_name).await;

    rs.simple_query("CREATE SHALLOW CACHE POLICY TTL 300 SECONDS FROM SELECT * FROM api.todos")
        .await
        .expect("create cache");

    // anon: no claim -> NULL sub -> no rows.
    rs.simple_query("SET ROLE anon").await.expect("set anon");
    let empty: Vec<String> = vec![];
    assert_eq!(read_todos(&rs, proto).await, empty, "anon sees no rows");
    assert_dest(&rs, QueryDestination::Upstream, "anon fills empty").await;

    // An authenticated tenant is a distinct partition and sees its own rows,
    // never served anon's (or anyone else's) result.
    rs.simple_query("SET ROLE authenticated")
        .await
        .expect("set authenticated");
    set_sub_claim(&rs, proto, "bob").await;
    assert_eq!(
        read_todos(&rs, proto).await,
        vec!["bob"],
        "authenticated bob is isolated from anon",
    );
    assert_dest(&rs, QueryDestination::Upstream, "bob fills").await;
    assert_eq!(read_todos(&rs, proto).await, vec!["bob"]);
    assert_dest(&rs, QueryDestination::ReadysetShallow, "bob hits").await;

    shutdown_tx.shutdown().await;
}

#[test]
#[tags(serial)]
#[upstream(postgres)]
async fn pg_rls_shallow_cache_anon_no_claims_simple() {
    run_anon_no_claims(Protocol::Simple).await;
}

#[test]
#[tags(serial)]
#[upstream(postgres)]
async fn pg_rls_shallow_cache_anon_no_claims_extended() {
    run_anon_no_claims(Protocol::Extended).await;
}

/// `set_config(..., false)` on the simple/text protocol mirrors the claim into
/// the session just like the namespaced `SET` form, so the scoped cache
/// partitions and hits (F4 / RLS-28).
#[test]
#[tags(serial)]
#[upstream(postgres)]
async fn pg_rls_shallow_cache_set_config_simple() {
    init_test_logging();

    let test_name = derive_test_name();
    PostgreSQLAdapter::recreate_database(&test_name).await;
    setup_todos_upstream(&test_name).await;
    let (rs, _handle, shutdown_tx) = connect_rls(&test_name).await;

    rs.simple_query("SET ROLE authenticated")
        .await
        .expect("set role");
    rs.simple_query("CREATE SHALLOW CACHE POLICY TTL 300 SECONDS FROM SELECT * FROM api.todos")
        .await
        .expect("create cache");
    // set_config issued over the simple protocol (not the extended `query`).
    rs.simple_query("SELECT set_config('request.jwt.claims', '{\"sub\":\"bob\"}', false)")
        .await
        .expect("set_config (simple)");

    assert_eq!(read_todos(&rs, Protocol::Simple).await, vec!["bob"]);
    assert_dest(&rs, QueryDestination::Upstream, "set_config simple fills").await;
    assert_eq!(read_todos(&rs, Protocol::Simple).await, vec!["bob"]);
    assert_dest(&rs, QueryDestination::ReadysetShallow, "set_config simple hits").await;

    shutdown_tx.shutdown().await;
}

/// A namespaced `SET request.jwt.claims = '...'` issued over the extended
/// protocol (Parse/Bind/Execute) mirrors the claim, so the scoped cache
/// partitions and hits (F3, the extended counterpart of the simple `SET`).
#[test]
#[tags(serial)]
#[upstream(postgres)]
async fn pg_rls_shallow_cache_set_namespaced_extended() {
    init_test_logging();

    let test_name = derive_test_name();
    PostgreSQLAdapter::recreate_database(&test_name).await;
    setup_todos_upstream(&test_name).await;
    let (rs, _handle, shutdown_tx) = connect_rls(&test_name).await;

    rs.simple_query("SET ROLE authenticated")
        .await
        .expect("set role");
    rs.simple_query("CREATE SHALLOW CACHE POLICY TTL 300 SECONDS FROM SELECT * FROM api.todos")
        .await
        .expect("create cache");
    // Namespaced SET over the extended protocol.
    rs.execute("SET request.jwt.claims = '{\"sub\":\"bob\"}'", &[])
        .await
        .expect("SET namespaced (extended)");

    assert_eq!(read_todos(&rs, Protocol::Extended).await, vec!["bob"]);
    assert_dest(&rs, QueryDestination::Upstream, "extended SET fills").await;
    assert_eq!(read_todos(&rs, Protocol::Extended).await, vec!["bob"]);
    assert_dest(&rs, QueryDestination::ReadysetShallow, "extended SET hits").await;

    shutdown_tx.shutdown().await;
}

/// `RESET ALL` resets run-time GUCs but, per Postgres semantics, does NOT revert
/// the role set by `SET ROLE` (verified: `current_user` persists). It clears the
/// keyed claim to an empty string, so the partition re-bases to the role's
/// claim-less context. With the JSON-cast test policy an empty claim errors
/// (`''::json`) -- upstream-faithful but untestable here -- so this is
/// `#[ignore]`'d pending a claim-tolerant policy (F8).
async fn run_reset_all(proto: Protocol) {
    init_test_logging();

    let test_name = derive_test_name();
    PostgreSQLAdapter::recreate_database(&test_name).await;
    setup_todos_upstream(&test_name).await;
    let (rs, _handle, shutdown_tx) = connect_rls(&test_name).await;

    rs.simple_query("SET ROLE authenticated")
        .await
        .expect("set role");
    rs.simple_query("CREATE SHALLOW CACHE POLICY TTL 300 SECONDS FROM SELECT * FROM api.todos")
        .await
        .expect("create cache");
    set_sub_claim(&rs, proto, "bob").await;
    assert_eq!(read_todos(&rs, proto).await, vec!["bob"], "bob scoped");
    assert_dest(&rs, QueryDestination::Upstream, "bob fills").await;

    // RESET ALL keeps the role (authenticated) and clears the claim to empty, so
    // the policy yields no rows -- a re-based, claim-less partition.
    run_stmt(&rs, proto, "RESET ALL").await;
    let empty: Vec<String> = vec![];
    assert_eq!(
        read_todos(&rs, proto).await,
        empty,
        "RESET ALL clears the claim -> no rows",
    );
    assert_dest(
        &rs,
        QueryDestination::Upstream,
        "re-based partition fills after RESET ALL",
    )
    .await;

    shutdown_tx.shutdown().await;
}

#[test]
#[tags(serial)]
#[upstream(postgres)]
#[ignore = "Postgres RESET ALL keeps the role and clears the claim to ''; the \
            JSON-cast test policy errors on ''::json (upstream-faithful). Needs \
            a claim-tolerant policy"]
async fn pg_rls_shallow_cache_reset_all_simple() {
    run_reset_all(Protocol::Simple).await;
}

#[test]
#[tags(serial)]
#[upstream(postgres)]
#[ignore = "same empty-claim policy limitation as the simple case; the extended \
            path now proxies+mirrors RESET ALL identically"]
async fn pg_rls_shallow_cache_reset_all_extended() {
    run_reset_all(Protocol::Extended).await;
}

/// `RESET ROLE` reverts the effective role to the superuser login (leaving GUCs
/// intact), which bypasses RLS: the read re-bases to the bypass partition rather
/// than the prior role's scoped result (F2).
async fn run_reset_role(proto: Protocol) {
    init_test_logging();

    let test_name = derive_test_name();
    PostgreSQLAdapter::recreate_database(&test_name).await;
    setup_todos_upstream(&test_name).await;
    let (rs, _handle, shutdown_tx) = connect_rls(&test_name).await;

    rs.simple_query("SET ROLE authenticated")
        .await
        .expect("set role");
    rs.simple_query("CREATE SHALLOW CACHE POLICY TTL 300 SECONDS FROM SELECT * FROM api.todos")
        .await
        .expect("create cache");
    set_sub_claim(&rs, proto, "bob").await;
    assert_served_from_cache(&rs, proto, &["bob"], "bob scoped before RESET ROLE").await;

    run_stmt(&rs, proto, "RESET ROLE").await;
    assert_served_from_cache(
        &rs,
        proto,
        &["alice", "alice", "bob"],
        "RESET ROLE re-bases to the bypass login partition",
    )
    .await;

    shutdown_tx.shutdown().await;
}

#[test]
#[tags(serial)]
#[upstream(postgres)]
async fn pg_rls_shallow_cache_reset_role_simple() {
    run_reset_role(Protocol::Simple).await;
}

#[test]
#[tags(serial)]
#[upstream(postgres)]
async fn pg_rls_shallow_cache_reset_role_extended() {
    run_reset_role(Protocol::Extended).await;
}

/// `DISCARD ALL` performs a full session reset (role + GUCs + trust state), as on
/// connection return-to-pool: the role reverts to the superuser login (bypass)
/// and the read re-bases to the bypass partition (F9).
async fn run_discard_all(proto: Protocol) {
    init_test_logging();

    let test_name = derive_test_name();
    PostgreSQLAdapter::recreate_database(&test_name).await;
    setup_todos_upstream(&test_name).await;
    let (rs, _handle, shutdown_tx) = connect_rls(&test_name).await;

    rs.simple_query("SET ROLE authenticated")
        .await
        .expect("set role");
    rs.simple_query("CREATE SHALLOW CACHE POLICY TTL 300 SECONDS FROM SELECT * FROM api.todos")
        .await
        .expect("create cache");
    set_sub_claim(&rs, proto, "bob").await;
    assert_served_from_cache(&rs, proto, &["bob"], "bob scoped before DISCARD ALL").await;

    run_stmt(&rs, proto, "DISCARD ALL").await;
    assert_served_from_cache(
        &rs,
        proto,
        &["alice", "alice", "bob"],
        "DISCARD ALL re-bases to the bypass login partition",
    )
    .await;

    shutdown_tx.shutdown().await;
}

#[test]
#[tags(serial)]
#[upstream(postgres)]
async fn pg_rls_shallow_cache_discard_all_simple() {
    run_discard_all(Protocol::Simple).await;
}

#[test]
#[tags(serial)]
#[upstream(postgres)]
async fn pg_rls_shallow_cache_discard_all_extended() {
    run_discard_all(Protocol::Extended).await;
}

/// `RESET request.jwt.claims` clears the keyed claim while the role stays
/// `authenticated`, so the policy resolves the claim to NULL and scopes the read
/// to zero rows -- a distinct partition that does not serve the prior tenant's
/// rows (F7). Extended-protocol `RESET` does not clear the mirror yet, so only
/// the simple path is asserted.
async fn run_reset_claim(proto: Protocol) {
    init_test_logging();

    let test_name = derive_test_name();
    PostgreSQLAdapter::recreate_database(&test_name).await;
    setup_todos_upstream(&test_name).await;
    let (rs, _handle, shutdown_tx) = connect_rls(&test_name).await;

    rs.simple_query("SET ROLE authenticated")
        .await
        .expect("set role");
    rs.simple_query("CREATE SHALLOW CACHE POLICY TTL 300 SECONDS FROM SELECT * FROM api.todos")
        .await
        .expect("create cache");
    set_sub_claim(&rs, proto, "bob").await;
    assert_eq!(read_todos(&rs, proto).await, vec!["bob"], "bob scoped");
    assert_dest(&rs, QueryDestination::Upstream, "bob fills").await;

    // Clear only the claim; role stays authenticated -> NULL claim -> no rows.
    run_stmt(&rs, proto, "RESET request.jwt.claims").await;
    let empty: Vec<String> = vec![];
    assert_eq!(
        read_todos(&rs, proto).await,
        empty,
        "cleared claim sees no rows, not bob's",
    );
    assert_dest(&rs, QueryDestination::Upstream, "unset partition fills").await;

    shutdown_tx.shutdown().await;
}

#[test]
#[tags(serial)]
#[upstream(postgres)]
#[ignore = "RESET <guc> leaves an empty-string value that the JSON-cast policy \
            rejects (''::json); needs a claim-tolerant policy"]
async fn pg_rls_shallow_cache_reset_claim_simple() {
    run_reset_claim(Protocol::Simple).await;
}

#[test]
#[tags(serial)]
#[upstream(postgres)]
#[ignore = "same empty-claim policy limitation as the simple case (''::json); \
            the extended path now proxies+mirrors RESET identically"]
async fn pg_rls_shallow_cache_reset_claim_extended() {
    run_reset_claim(Protocol::Extended).await;
}

/// `SET SESSION AUTHORIZATION <user>` is a first-class, mirrorable identity
/// change: after upstream accepts it, the adapter resolves the new role + bypass
/// and updates the session mirror (`session_user` is part of the partition key),
/// so reads serve a correctly-scoped cached partition rather than failing closed.
/// `authenticated` does not bypass RLS, so the claim still scopes the rows.
#[test]
#[tags(serial)]
#[upstream(postgres)]
async fn pg_rls_shallow_cache_session_authorization_mirrors_identity() {
    init_test_logging();

    let test_name = derive_test_name();
    PostgreSQLAdapter::recreate_database(&test_name).await;
    setup_todos_upstream(&test_name).await;
    let (rs, _handle, shutdown_tx) = connect_rls(&test_name).await;

    rs.simple_query("SET ROLE authenticated")
        .await
        .expect("set role");
    rs.simple_query("CREATE SHALLOW CACHE POLICY TTL 300 SECONDS FROM SELECT * FROM api.todos")
        .await
        .expect("create cache");
    set_sub_claim(&rs, Protocol::Simple, "bob").await;
    assert_served_from_cache(&rs, Protocol::Simple, &["bob"], "bob served from cache").await;

    // SET SESSION AUTHORIZATION is mirrored after upstream accepts it: the
    // session stays trusted under the new identity rather than wedging off-cache.
    rs.simple_query("SET SESSION AUTHORIZATION authenticated")
        .await
        .expect("set session authorization");

    // Reads serve a scoped partition for the mirrored identity (still scoped to
    // bob's claim, since `authenticated` does not bypass RLS).
    assert_served_from_cache(
        &rs,
        Protocol::Simple,
        &["bob"],
        "scoped partition after SET SESSION AUTHORIZATION",
    )
    .await;

    shutdown_tx.shutdown().await;
}

/// `SET SESSION AUTHORIZATION DEFAULT` and `RESET SESSION AUTHORIZATION` both
/// parse (modeled as the `DEFAULT` case) and restore the startup identity. Since
/// that identity is mirrorable, the adapter resolves it (here the login role,
/// which bypasses RLS) and serves the corresponding partition from cache -- it no
/// longer wedges the session off-cache.
#[test]
#[tags(serial)]
#[upstream(postgres)]
async fn pg_rls_shallow_cache_reset_session_authorization() {
    init_test_logging();

    let test_name = derive_test_name();
    PostgreSQLAdapter::recreate_database(&test_name).await;
    setup_todos_upstream(&test_name).await;
    let (rs, _handle, shutdown_tx) = connect_rls(&test_name).await;

    rs.simple_query("SET ROLE authenticated")
        .await
        .expect("set role");
    rs.simple_query("CREATE SHALLOW CACHE POLICY TTL 300 SECONDS FROM SELECT * FROM api.todos")
        .await
        .expect("create cache");
    set_sub_claim(&rs, Protocol::Simple, "bob").await;
    assert_served_from_cache(&rs, Protocol::Simple, &["bob"], "bob served from cache").await;

    // SET SESSION AUTHORIZATION DEFAULT restores the startup identity (the login
    // role, which bypasses RLS), mirrored after upstream accepts it: reads serve
    // the bypass partition (all rows) from cache, not fail closed.
    rs.simple_query("SET SESSION AUTHORIZATION DEFAULT")
        .await
        .expect("set session authorization default");
    assert_served_from_cache(
        &rs,
        Protocol::Simple,
        &["alice", "alice", "bob"],
        "bypass partition after SET SESSION AUTHORIZATION DEFAULT",
    )
    .await;

    // RESET SESSION AUTHORIZATION: same modeled AST, same restored identity.
    rs.simple_query("RESET SESSION AUTHORIZATION")
        .await
        .expect("reset session authorization");
    assert_served_from_cache(
        &rs,
        Protocol::Simple,
        &["alice", "alice", "bob"],
        "bypass partition after RESET SESSION AUTHORIZATION",
    )
    .await;

    shutdown_tx.shutdown().await;
}

/// `SET SESSION AUTHORIZATION` through the extended protocol
/// (Parse/Bind/Execute, as drivers send it) is mirrored at the
/// prepared-execute tail exactly as the simple path mirrors it at dispatch.
/// The final identity restore flips the session to the login role
/// (BYPASSRLS), so reads after it must serve the bypass partition (all
/// rows): serving `bob`'s cached partition there would mean the mirror
/// missed an upstream identity change and kept keying reads on a stale
/// security context.
#[test]
#[tags(serial)]
#[upstream(postgres)]
async fn pg_rls_shallow_cache_session_authorization_extended() {
    init_test_logging();

    let test_name = derive_test_name();
    PostgreSQLAdapter::recreate_database(&test_name).await;
    setup_todos_upstream(&test_name).await;
    let (rs, _handle, shutdown_tx) = connect_rls(&test_name).await;

    run_stmt(
        &rs,
        Protocol::Extended,
        "SET SESSION AUTHORIZATION authenticated",
    )
    .await;
    rs.simple_query("CREATE SHALLOW CACHE POLICY TTL 300 SECONDS FROM SELECT * FROM api.todos")
        .await
        .expect("create cache");
    set_sub_claim(&rs, Protocol::Extended, "bob").await;
    assert_served_from_cache(
        &rs,
        Protocol::Extended,
        &["bob"],
        "scoped partition under prepared SET SESSION AUTHORIZATION",
    )
    .await;

    run_stmt(&rs, Protocol::Extended, "SET SESSION AUTHORIZATION DEFAULT").await;
    assert_served_from_cache(
        &rs,
        Protocol::Extended,
        &["alice", "alice", "bob"],
        "bypass partition after prepared SET SESSION AUTHORIZATION DEFAULT",
    )
    .await;

    shutdown_tx.shutdown().await;
}

/// `SET LOCAL SESSION AUTHORIZATION` reverts at the transaction boundary, which
/// the session mirror cannot model for `session_user`, so it fails closed
/// (transaction-scoped) until the transaction ends rather than serving a stale
/// partition. The `UNTIL WRITE` cache would otherwise serve reads inside the
/// transaction, so a fail-closed read here isolates the trust gate; COMMIT
/// clears the transaction-scoped gap and caching resumes.
#[test]
#[tags(serial)]
#[upstream(postgres)]
async fn pg_rls_shallow_cache_local_session_authorization_fails_closed() {
    init_test_logging();

    let test_name = derive_test_name();
    PostgreSQLAdapter::recreate_database(&test_name).await;
    setup_todos_upstream(&test_name).await;
    let (rs, _handle, shutdown_tx) = connect_rls(&test_name).await;

    rs.simple_query(
        "CREATE SHALLOW CACHE WITH (POLICY TTL 300 SECONDS, UNTIL WRITE) \
         FROM SELECT * FROM api.todos",
    )
    .await
    .expect("create cache");

    rs.simple_query("BEGIN").await.expect("begin");
    run_stmt(&rs, Protocol::Simple, "SET LOCAL ROLE authenticated").await;
    set_sub_claim_local(&rs, Protocol::Simple, "bob").await;
    assert_served_from_cache(&rs, Protocol::Simple, &["bob"], "bob scoped inside the transaction")
        .await;

    // Transaction-local identity change: marks the session transaction-untrusted,
    // so reads fail closed despite the UNTIL WRITE cache. The repeat read
    // confirms it is not a one-off miss.
    run_stmt(
        &rs,
        Protocol::Simple,
        "SET LOCAL SESSION AUTHORIZATION authenticated",
    )
    .await;
    assert_fails_closed(
        &rs,
        Protocol::Simple,
        "transaction-untrusted after SET LOCAL SESSION AUTHORIZATION",
    )
    .await;

    // COMMIT clears the transaction-scoped gap and reverts the trx-local context
    // to the bypass login: caching resumes (all rows).
    rs.simple_query("COMMIT").await.expect("commit");
    assert_served_from_cache(
        &rs,
        Protocol::Simple,
        &["alice", "alice", "bob"],
        "trust restored after COMMIT",
    )
    .await;

    shutdown_tx.shutdown().await;
}

/// A transaction-local claim (`set_config(..., true)`) scopes reads inside the
/// transaction and is fully discarded at COMMIT and at ROLLBACK: afterward the
/// connection reverts to its (bypass) login role, so the local tenant's scoped
/// view never persists (T1, T2 / F6 + F14).
async fn run_transaction_local_claim(proto: Protocol) {
    init_test_logging();

    let test_name = derive_test_name();
    PostgreSQLAdapter::recreate_database(&test_name).await;
    setup_todos_upstream(&test_name).await;
    let (rs, _handle, shutdown_tx) = connect_rls(&test_name).await;

    // `UNTIL WRITE` lets the cache serve reads inside a (read-only) transaction.
    rs.simple_query(
        "CREATE SHALLOW CACHE WITH (POLICY TTL 300 SECONDS, UNTIL WRITE) \
         FROM SELECT * FROM api.todos",
    )
    .await
    .expect("create cache");

    // T1: the local context scopes reads inside the transaction (served from the
    // tenant's cached partition), and reverts after COMMIT -- outside the
    // transaction the superuser login bypasses RLS and the cache serves all rows.
    rs.simple_query("BEGIN").await.expect("begin");
    run_stmt(&rs, proto, "SET LOCAL ROLE authenticated").await;
    set_sub_claim_local(&rs, proto, "bob").await;
    assert_served_from_cache(&rs, proto, &["bob"], "bob scoped inside the transaction").await;
    rs.simple_query("COMMIT").await.expect("commit");
    assert_served_from_cache(
        &rs,
        proto,
        &["alice", "alice", "bob"],
        "reverted to bypass login after COMMIT",
    )
    .await;

    // T2: the local context is discarded on ROLLBACK, back to the bypass login.
    rs.simple_query("BEGIN").await.expect("begin");
    run_stmt(&rs, proto, "SET LOCAL ROLE authenticated").await;
    set_sub_claim_local(&rs, proto, "alice").await;
    assert_served_from_cache(&rs, proto, &["alice", "alice"], "alice scoped inside the transaction")
        .await;
    rs.simple_query("ROLLBACK").await.expect("rollback");
    assert_served_from_cache(
        &rs,
        proto,
        &["alice", "alice", "bob"],
        "discarded after ROLLBACK (back to bypass login)",
    )
    .await;

    shutdown_tx.shutdown().await;
}

#[test]
#[tags(serial)]
#[upstream(postgres)]
async fn pg_rls_shallow_cache_transaction_local_claim_simple() {
    run_transaction_local_claim(Protocol::Simple).await;
}

#[test]
#[tags(serial)]
#[upstream(postgres)]
async fn pg_rls_shallow_cache_transaction_local_claim_extended() {
    run_transaction_local_claim(Protocol::Extended).await;
}

/// The canonical PostgREST request shape: a transaction that sets the role and
/// JWT claim transaction-locally, reads, and commits. Two tenants running the
/// identical flow are isolated, and the context does not survive COMMIT
/// (T4 + F14 + F6).
async fn run_golden_production_pattern(proto: Protocol) {
    init_test_logging();

    let test_name = derive_test_name();
    PostgreSQLAdapter::recreate_database(&test_name).await;
    setup_todos_upstream(&test_name).await;
    let (rs, _handle, shutdown_tx) = connect_rls(&test_name).await;

    // `UNTIL WRITE` lets the cache serve reads inside a (read-only) transaction;
    // without it the default `Never` policy proxies every in-transaction read
    // upstream, so PostgREST's transactional reads would never hit the cache.
    rs.simple_query(
        "CREATE SHALLOW CACHE WITH (POLICY TTL 300 SECONDS, UNTIL WRITE) \
         FROM SELECT * FROM api.todos",
    )
    .await
    .expect("create cache");

    // One PostgREST-shaped request: a transaction that sets role + claim locally,
    // reads, and commits. Returns the rows and where the read was served.
    async fn request(
        rs: &Client,
        proto: Protocol,
        sub: &str,
    ) -> (Vec<String>, QueryDestination) {
        rs.simple_query("BEGIN").await.expect("begin");
        run_stmt(rs, proto, "SET LOCAL ROLE authenticated").await;
        set_sub_claim_local(rs, proto, sub).await;
        let rows = read_todos(rs, proto).await;
        let dest = psql_helpers::last_query_info(rs).await.destination;
        rs.simple_query("COMMIT").await.expect("commit");
        (rows, dest)
    }

    for (sub, expected) in [("bob", vec!["bob"]), ("alice", vec!["alice", "alice"])] {
        // First request fills the tenant's partition.
        let (rows, dest) = request(&rs, proto, sub).await;
        assert_eq!(rows, expected, "first request scopes to {sub}");
        assert_eq!(dest, QueryDestination::Upstream, "first request fills from upstream");

        // Second identical request must be served from the shallow cache.
        let (rows, dest) = request(&rs, proto, sub).await;
        assert_eq!(rows, expected, "second request returns {sub}'s cached rows");
        assert_eq!(
            dest,
            QueryDestination::ReadysetShallow,
            "second identical request must hit the shallow cache, not re-proxy upstream",
        );
    }

    shutdown_tx.shutdown().await;
}

#[test]
#[tags(serial)]
#[upstream(postgres)]
async fn pg_rls_shallow_cache_golden_production_pattern_simple() {
    run_golden_production_pattern(Protocol::Simple).await;
}

#[test]
#[tags(serial)]
#[upstream(postgres)]
async fn pg_rls_shallow_cache_golden_production_pattern_extended() {
    run_golden_production_pattern(Protocol::Extended).await;
}

/// A multi-statement simple-query message mirrors each statement in order: a
/// `SET` of the claim followed by a read in one message. The follow-up reads
/// then prove the cache serves the partition the batch's `SET` established --
/// i.e. the batch's mutation was mirrored, not just executed upstream (T6).
/// Simple protocol only -- multi-statement batches are a simple-protocol feature.
#[test]
#[tags(serial)]
#[upstream(postgres)]
async fn pg_rls_shallow_cache_multistatement_batch_simple() {
    init_test_logging();

    let test_name = derive_test_name();
    PostgreSQLAdapter::recreate_database(&test_name).await;
    setup_todos_upstream(&test_name).await;
    let (rs, _handle, shutdown_tx) = connect_rls(&test_name).await;

    rs.simple_query("SET ROLE authenticated")
        .await
        .expect("set role");
    rs.simple_query("CREATE SHALLOW CACHE POLICY TTL 300 SECONDS FROM SELECT * FROM api.todos")
        .await
        .expect("create cache");

    // One simple message carrying a session-level claim SET and a read. If the
    // batch's SET were not mirrored, the follow-up reads below would not scope to
    // bob.
    rs.simple_query("SET request.jwt.claims = '{\"sub\":\"bob\"}'; SELECT * FROM api.todos")
        .await
        .expect("multi-statement batch");

    // The claim the batch set persists; prove the cache serves bob's partition.
    assert_served_from_cache(
        &rs,
        Protocol::Simple,
        &["bob"],
        "cache serves the partition the batch's SET established",
    )
    .await;

    shutdown_tx.shutdown().await;
}

/// A join of an RLS-active table and a plain table is scoped by the RLS table's
/// policy: the plain side is filtered to rows that join to the tenant's visible
/// todos. The cached join partitions by the `sub` claim and is served from the
/// shallow cache, isolated per tenant (Q3).
async fn run_join_rls_plain(proto: Protocol) {
    init_test_logging();

    let test_name = derive_test_name();
    PostgreSQLAdapter::recreate_database(&test_name).await;
    setup_todos_upstream(&test_name).await;
    let (rs, _handle, shutdown_tx) = connect_rls(&test_name).await;

    rs.simple_query("SET ROLE authenticated")
        .await
        .expect("set role");
    let join = "SELECT t.id, t.owner_id FROM api.todos t \
                JOIN api.projects p ON t.owner_id = p.owner_id";
    rs.simple_query(&format!("CREATE SHALLOW CACHE POLICY TTL 300 SECONDS FROM {join}"))
        .await
        .expect("create cache");

    set_sub_claim(&rs, proto, "bob").await;
    assert_owner_ids_cached(&rs, proto, join, &["bob"], "bob's join scoped by RLS").await;

    set_sub_claim(&rs, proto, "alice").await;
    assert_owner_ids_cached(&rs, proto, join, &["alice", "alice"], "alice's join is isolated").await;

    set_sub_claim(&rs, proto, "bob").await;
    assert_owner_ids_cached(&rs, proto, join, &["bob"], "bob's join partition intact").await;

    shutdown_tx.shutdown().await;
}

#[test]
#[tags(serial)]
#[upstream(postgres)]
async fn pg_rls_shallow_cache_join_rls_plain_simple() {
    run_join_rls_plain(Protocol::Simple).await;
}

#[test]
#[tags(serial)]
#[upstream(postgres)]
async fn pg_rls_shallow_cache_join_rls_plain_extended() {
    run_join_rls_plain(Protocol::Extended).await;
}

/// A join of two RLS-active tables is scoped by the *union* of both policies'
/// keyed GUCs (`sub` for `todos`, `tenant` for `orders`): changing either claim
/// is a distinct cached partition, so the cache never conflates contexts that
/// differ in only one of the two inputs (Q4).
async fn run_join_two_rls(proto: Protocol) {
    init_test_logging();

    let test_name = derive_test_name();
    PostgreSQLAdapter::recreate_database(&test_name).await;
    setup_todos_upstream(&test_name).await;
    let (rs, _handle, shutdown_tx) = connect_rls(&test_name).await;

    rs.simple_query("SET ROLE authenticated")
        .await
        .expect("set role");
    // Cross join: visible(todos) x visible(orders). bob:1 / alice:2 todos;
    // acme:2 / globex:1 orders.
    let join = "SELECT count(*) FROM api.todos t, api.orders o";
    rs.simple_query(&format!("CREATE SHALLOW CACHE POLICY TTL 300 SECONDS FROM {join}"))
        .await
        .expect("create cache");

    set_claims(&rs, proto, r#"{"sub":"bob","tenant":"acme"}"#).await;
    assert_count_cached(&rs, proto, join, 2, "bob+acme (1 todo x 2 orders)").await;

    // Same sub, different tenant -> distinct partition (orders side changes).
    set_claims(&rs, proto, r#"{"sub":"bob","tenant":"globex"}"#).await;
    assert_count_cached(&rs, proto, join, 1, "bob+globex: tenant is part of the key").await;

    // Different sub, same tenant -> distinct partition (todos side changes).
    set_claims(&rs, proto, r#"{"sub":"alice","tenant":"acme"}"#).await;
    assert_count_cached(&rs, proto, join, 4, "alice+acme: sub is part of the key").await;

    shutdown_tx.shutdown().await;
}

#[test]
#[tags(serial)]
#[upstream(postgres)]
async fn pg_rls_shallow_cache_join_two_rls_simple() {
    run_join_two_rls(Protocol::Simple).await;
}

#[test]
#[tags(serial)]
#[upstream(postgres)]
async fn pg_rls_shallow_cache_join_two_rls_extended() {
    run_join_two_rls(Protocol::Extended).await;
}

/// An aggregate over an RLS-active table counts only the partition's visible
/// rows: the per-tenant count is served from the shallow cache and never reveals
/// another tenant's cardinality (Q7).
async fn run_aggregate_count(proto: Protocol) {
    init_test_logging();

    let test_name = derive_test_name();
    PostgreSQLAdapter::recreate_database(&test_name).await;
    setup_todos_upstream(&test_name).await;
    let (rs, _handle, shutdown_tx) = connect_rls(&test_name).await;

    rs.simple_query("SET ROLE authenticated")
        .await
        .expect("set role");
    let count = "SELECT count(*) FROM api.todos";
    rs.simple_query(&format!("CREATE SHALLOW CACHE POLICY TTL 300 SECONDS FROM {count}"))
        .await
        .expect("create cache");

    set_sub_claim(&rs, proto, "bob").await;
    assert_count_cached(&rs, proto, count, 1, "bob counts only his row").await;

    set_sub_claim(&rs, proto, "alice").await;
    assert_count_cached(&rs, proto, count, 2, "alice counts only her rows").await;

    set_sub_claim(&rs, proto, "bob").await;
    assert_count_cached(&rs, proto, count, 1, "bob's count partition intact").await;

    shutdown_tx.shutdown().await;
}

#[test]
#[tags(serial)]
#[upstream(postgres)]
async fn pg_rls_shallow_cache_aggregate_count_simple() {
    run_aggregate_count(Protocol::Simple).await;
}

#[test]
#[tags(serial)]
#[upstream(postgres)]
async fn pg_rls_shallow_cache_aggregate_count_extended() {
    run_aggregate_count(Protocol::Extended).await;
}

/// A policy change must re-key the cache, not keep serving a partition pinned to
/// the create-time inputs. The policy's keyed claim is switched from `sub` to
/// `tenant` on the upstream; a session with crossed claims (`sub`=bob,
/// `tenant`=alice) sees bob's row before the change and alice's rows after the
/// poller picks it up -- proving the cache re-keyed to the new GUC rather than
/// serving the stale `sub`-keyed partition. The harness polls the catalog every
/// second so the change is observed within the test's timeout.
async fn run_policy_change_invalidation(proto: Protocol) {
    init_test_logging();

    let test_name = derive_test_name();
    PostgreSQLAdapter::recreate_database(&test_name).await;
    setup_todos_upstream(&test_name).await;
    let (rs, _handle, shutdown_tx) = connect_rls(&test_name).await;

    // A separate upstream connection to mutate the policy mid-test.
    let mut upstream_cfg = psql_helpers::upstream_config();
    upstream_cfg.dbname(&test_name);
    let upstream = psql_helpers::connect(upstream_cfg).await;

    rs.simple_query("SET ROLE authenticated")
        .await
        .expect("set role");
    // Crossed claims: a `sub`-keyed policy resolves to bob; a `tenant`-keyed one
    // resolves to alice.
    set_claims(&rs, proto, r#"{"sub":"bob","tenant":"alice"}"#).await;
    rs.simple_query("CREATE SHALLOW CACHE POLICY TTL 300 SECONDS FROM SELECT * FROM api.todos")
        .await
        .expect("create cache");

    // Before: the policy keys on `sub`, so the session sees bob's row from cache.
    assert_served_from_cache(&rs, proto, &["bob"], "before change: keyed on sub").await;

    // Switch the keyed claim from `sub` to `tenant` on the upstream.
    upstream
        .simple_query(
            "ALTER POLICY owner ON api.todos USING \
             (owner_id = ((current_setting('request.jwt.claims'::text, true))::json ->> 'tenant'::text))",
        )
        .await
        .expect("alter policy");

    // The poller detects the changed policy expression, bumps the generation, and
    // re-keys the cache to `tenant`. Poll until the same session reflects it:
    // tenant=alice now scopes to alice's rows (never the stale bob partition).
    let mut detected = false;
    for _ in 0..40 {
        if read_todos(&rs, proto).await == ["alice", "alice"] {
            detected = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert!(
        detected,
        "poller did not re-key the cache after the policy change (still serving stale partition)",
    );

    // After re-keying, the new (tenant-keyed) partition is served from cache.
    assert_served_from_cache(&rs, proto, &["alice", "alice"], "after change: re-keyed to tenant")
        .await;

    shutdown_tx.shutdown().await;
}

#[test]
#[tags(serial)]
#[upstream(postgres)]
async fn pg_rls_shallow_cache_policy_change_invalidation_simple() {
    run_policy_change_invalidation(Protocol::Simple).await;
}

#[test]
#[tags(serial)]
#[upstream(postgres)]
async fn pg_rls_shallow_cache_policy_change_invalidation_extended() {
    run_policy_change_invalidation(Protocol::Extended).await;
}

/// A cache created over a table with NO RLS is `Plain` -- it serves every row to
/// every session. When RLS is later enabled on that table, the poller must DROP
/// the Plain cache AND reset the query's migration status (a Plain cache over an
/// RLS-active table would serve all rows to all tenants -- the worst-case
/// bleed). The reset makes the next read re-migrate: in `--auto-cache` (Shallow)
/// mode InRequestPath re-analyzes the now-RLS-active table and re-creates a
/// scoped shallow cache, so bob sees only his row -- never the stale all-rows
/// result (RLS-6).
async fn run_enable_rls_drops_plain_cache(proto: Protocol) {
    init_test_logging();

    let test_name = derive_test_name();
    PostgreSQLAdapter::recreate_database(&test_name).await;
    setup_todos_upstream(&test_name).await;
    let (rs, _handle, shutdown_tx) = connect_rls(&test_name).await;

    // A separate upstream connection to enable RLS mid-test.
    let mut upstream_cfg = psql_helpers::upstream_config();
    upstream_cfg.dbname(&test_name);
    let upstream = psql_helpers::connect(upstream_cfg).await;

    let docs = "SELECT * FROM api.docs";
    rs.simple_query("SET ROLE authenticated")
        .await
        .expect("set role");
    set_sub_claim(&rs, proto, "bob").await;
    rs.simple_query(&format!("CREATE SHALLOW CACHE POLICY TTL 300 SECONDS FROM {docs}"))
        .await
        .expect("create cache");

    // No RLS yet: the Plain cache serves every row, from Readyset (a repeat read
    // hits ReadysetShallow, proving it is cache-served, not an upstream
    // passthrough).
    assert_owner_ids_cached(&rs, proto, docs, &["alice", "bob"], "plain cache before RLS").await;

    // Enable RLS and add a sub-keyed policy on the upstream.
    upstream
        .simple_query("ALTER TABLE api.docs ENABLE ROW LEVEL SECURITY")
        .await
        .expect("enable rls");
    upstream
        .simple_query(
            "CREATE POLICY docs_owner ON api.docs FOR SELECT TO public USING \
             (owner_id = ((current_setting('request.jwt.claims'::text, true))::json ->> 'sub'::text))",
        )
        .await
        .expect("create policy");

    // The poller observes the RLS flag and drops the Plain cache. Poll until the
    // read is scoped to bob -- never erroring, never the stale all-rows result.
    let mut scoped = false;
    for _ in 0..40 {
        if try_read_owner_ids(&rs, proto, docs).await.ok() == Some(vec!["bob".to_string()]) {
            scoped = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert!(
        scoped,
        "after enabling RLS the read must re-migrate to a scoped cache (bob's row), not serve the stale all-rows result",
    );

    // The re-created scoped cache isolates per tenant.
    set_sub_claim(&rs, proto, "alice").await;
    assert_eq!(
        read_owner_ids(&rs, proto, docs).await,
        vec!["alice"],
        "after RLS, alice is scoped to her own row",
    );

    shutdown_tx.shutdown().await;
}

/// An unqualified relation must be analyzed against the session `search_path`,
/// not a hard-coded `public`. With `search_path = private, public`, a same-named
/// non-RLS `public.documents` decoy must NOT shadow the RLS-active
/// `private.documents`: `SELECT * FROM documents` reads `private.documents`
/// upstream, so it must be analyzed as RLS-active and cached *scoped*. A
/// public-defaulting analyzer would see the non-RLS decoy, build a Plain
/// (unpartitioned) cache, and leak one tenant's rows to every other tenant.
async fn run_search_path_resolution(proto: Protocol) {
    init_test_logging();

    let test_name = derive_test_name();
    PostgreSQLAdapter::recreate_database(&test_name).await;
    setup_todos_upstream(&test_name).await;
    let (rs, _handle, shutdown_tx) = connect_rls(&test_name).await;

    // `private` first: `documents` resolves to the RLS-active `private.documents`.
    rs.simple_query("SET search_path = private, public")
        .await
        .expect("set search_path");
    rs.simple_query("SET ROLE authenticated")
        .await
        .expect("set role");
    let docs = "SELECT * FROM documents";
    rs.simple_query(&format!("CREATE SHALLOW CACHE POLICY TTL 300 SECONDS FROM {docs}"))
        .await
        .expect("create cache");

    // bob sees only his private row, served from a scoped cache.
    set_sub_claim(&rs, proto, "bob").await;
    assert_owner_ids_cached(&rs, proto, docs, &["bob"], "bob scoped via search_path").await;

    // alice is a distinct partition -- never bob's rows. If `documents` were
    // mis-analyzed as the non-RLS public decoy, the cache would be Plain and
    // alice would see bob's cached row (the leak this guards against).
    set_sub_claim(&rs, proto, "alice").await;
    assert_owner_ids_cached(
        &rs,
        proto,
        docs,
        &["alice", "alice"],
        "alice isolated -- search_path-resolved RLS table is cached scoped, not Plain",
    )
    .await;

    shutdown_tx.shutdown().await;
}

#[test]
#[tags(serial)]
#[upstream(postgres)]
async fn pg_rls_shallow_cache_search_path_resolution_simple() {
    run_search_path_resolution(Protocol::Simple).await;
}

#[test]
#[tags(serial)]
#[upstream(postgres)]
async fn pg_rls_shallow_cache_search_path_resolution_extended() {
    run_search_path_resolution(Protocol::Extended).await;
}

#[test]
#[tags(serial)]
#[upstream(postgres)]
async fn pg_rls_shallow_cache_enable_rls_drops_plain_cache_simple() {
    run_enable_rls_drops_plain_cache(Protocol::Simple).await;
}

#[test]
#[tags(serial)]
#[upstream(postgres)]
async fn pg_rls_shallow_cache_enable_rls_drops_plain_cache_extended() {
    run_enable_rls_drops_plain_cache(Protocol::Extended).await;
}

