use std::time::Duration;

use mysql_async::Conn;
use mysql_async::params::Params;
use mysql_async::prelude::{FromRow, Queryable};
use readyset_adapter::backend::{MigrationMode, QueryInfo};
use readyset_adapter::query_status_cache::{MigrationStyle, QueryStatusCache};
use readyset_client_metrics::QueryDestination;
use readyset_client_test_helpers::mysql_helpers::{self, MySQLAdapter};
use readyset_client_test_helpers::{TestBuilder, TestShutdownSender, sleep, wait_for_schema_generation_change};
use readyset_server::Handle;
use readyset_sql_parsing::ParsingPreset;
use readyset_util::eventually;
use test_utils::{tags, upstream};

/// Run `query` until the adapter reports it served by `expected`, then hand back its rows.
///
/// A read taken right after its cache is created is served upstream until the cache is ready, and
/// both sides return the same rows, so where the read went is what the poll waits on. The rows
/// are the caller's to assert on once it lands.
async fn eventually_readyset<T>(
    rs_conn: &mut Conn,
    query: impl AsRef<str>,
    expected: QueryDestination,
) -> Vec<T>
where
    T: FromRow + Send + 'static,
{
    let query = query.as_ref();
    let mut last = None;
    for _ in 0..40 {
        let rows: Vec<T> = rs_conn.query(query).await.unwrap();
        match last_target(rs_conn).await {
            (destination, _) if destination == expected => return rows,
            info => last = Some(info),
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let (destination, reason) = last.expect("at least one attempt");
    panic!("`{query}` went to {destination:?}, expected {expected:?}: {reason}");
}

/// As [`eventually_readyset`], for a read whose values the client binds.
async fn eventually_readyset_exec<T, P>(
    rs_conn: &mut Conn,
    query: impl AsRef<str>,
    params: P,
    expected: QueryDestination,
) -> Vec<T>
where
    T: FromRow + Send + 'static,
    P: Into<Params> + Send + Clone,
{
    let query = query.as_ref();
    let mut last = None;
    for _ in 0..40 {
        let rows: Vec<T> = rs_conn.exec(query, params.clone()).await.unwrap();
        match last_target(rs_conn).await {
            (destination, _) if destination == expected => return rows,
            info => last = Some(info),
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let (destination, reason) = last.expect("at least one attempt");
    panic!("`{query}` went to {destination:?}, expected {expected:?}: {reason}");
}

/// The destination the adapter reports for the previous statement, with its reason.
async fn last_target(rs_conn: &mut Conn) -> (QueryDestination, String) {
    let info: QueryInfo = rs_conn
        .query_first("EXPLAIN LAST STATEMENT")
        .await
        .unwrap()
        .unwrap();
    (info.destination, info.reason)
}

/// Fire an `EXPLAIN LAST STATEMENT` and assert where the previous query went.
async fn assert_last_target_was(rs_conn: &mut Conn, expected: QueryDestination) {
    let destination: QueryInfo = rs_conn
        .query_first("EXPLAIN LAST STATEMENT")
        .await
        .unwrap()
        .unwrap();
    let msg = destination.reason;
    assert_eq!(destination.destination, expected, "{msg}");
}

/// Bring up an adapter over a fresh database, with `autoparameterize` deciding whether caches
/// keep the literals their author wrote inline. Out-of-band migration keeps a plain SELECT from
/// creating a cache of its own, so what routes a read is only ever an explicit CREATE CACHE.
async fn adapter(db_name: &str, schema: &str) -> (Conn, Conn, Handle, TestShutdownSender<MySQLAdapter>) {
    adapter_with_preset(db_name, schema, ParsingPreset::for_tests()).await
}

/// As `adapter`, with the parser named. `AUTOPARAM ON` reaches only the sqlparser parser, so a
/// statement writing it parses with that parser alone.
async fn adapter_with_preset(
    db_name: &str,
    schema: &str,
    preset: ParsingPreset,
) -> (Conn, Conn, Handle, TestShutdownSender<MySQLAdapter>) {
    readyset_tracing::init_test_logging();
    mysql_helpers::recreate_database(db_name).await;

    let (rs_opts, handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .migration_mode(MigrationMode::OutOfBand)
        .parsing_preset(preset)
        .fallback(true)
        .replicate_db(db_name)
        .build::<MySQLAdapter>()
        .await;

    let upstream_opts = mysql_helpers::upstream_config().db_name(Some(db_name));
    let mut upstream_conn = mysql_async::Conn::new(upstream_opts).await.unwrap();
    let rs_conn = mysql_async::Conn::new(rs_opts).await.unwrap();

    upstream_conn.query_drop(schema).await.unwrap();
    sleep().await;

    (rs_conn, upstream_conn, handle, shutdown_tx)
}

const T: &str = "CREATE TABLE t (id int, status varchar(16), v int); \
                 INSERT INTO t (id, status, v) VALUES \
                 (1, 'active', 10), (1, 'archived', 20), (2, 'active', 30), (1, 'pending', 40);";

/// The behaviour the mode exists for. A cache written with one position left to a placeholder
/// and another spelled out is reached by an ad-hoc read that spells the same literal out: both
/// autoparameterize to one shape, and the slots filed under that shape put the literal back.
///
/// A read spelling out a different value there is a different query, and goes upstream.
#[tokio::test]
#[tags(serial)]
#[upstream(mysql)]
async fn a_literal_read_reaches_a_cache_that_kept_its_literal() {
    let (mut rs_conn, _upstream, _handle, shutdown_tx) =
        adapter("autoparam_mode_literal_read", T).await;

    rs_conn
        .query_drop(
            "CREATE CACHE kept WITH (AUTOPARAM OFF) FROM SELECT v FROM t WHERE id = ? AND status = 'active'",
        )
        .await
        .unwrap();

    // Ad-hoc, spelling out the literal the cache kept: served by that cache.

    let result: Vec<i32> = eventually_readyset(
        &mut rs_conn,
        "SELECT v FROM t WHERE id = 1 AND status = 'active'",
        QueryDestination::Readyset(Some("kept".into())),
    )
    .await;
    assert_eq!(result, vec![10]);

    // The parameterized position still serves every value of itself.

    let result: Vec<i32> = eventually_readyset(
        &mut rs_conn,
        "SELECT v FROM t WHERE id = 2 AND status = 'active'",
        QueryDestination::Readyset(Some("kept".into())),
    )
    .await;
    assert_eq!(result, vec![30]);

    // A different value where the cache kept a literal is a different query: upstream, correct.

    let result: Vec<i32> = eventually_readyset(
        &mut rs_conn,
        "SELECT v FROM t WHERE id = 1 AND status = 'archived'",
        QueryDestination::Upstream,
    )
    .await;
    assert_eq!(result, vec![20]);

    shutdown_tx.shutdown().await;
}

/// A cache keeping its literals inline is registered wherever a deep cache is built, not only
/// where the mode is exactly `deep`. Under `deep-then-shallow` a plain `CREATE CACHE` still
/// resolves deep, so the read has to reach it without waiting for a restart to re-register it.
#[tokio::test]
#[tags(serial)]
#[upstream(mysql)]
async fn a_kept_literal_cache_is_reachable_under_deep_then_shallow() {
    readyset_tracing::init_test_logging();
    let db_name = "autoparam_mode_deep_then_shallow";
    mysql_helpers::recreate_database(db_name).await;

    let (rs_opts, _handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .migration_mode(MigrationMode::OutOfBand)
        .cache_mode(readyset_client::CacheMode::DeepThenShallow)
        .fallback(true)
        .replicate_db(db_name)
        .build::<MySQLAdapter>()
        .await;

    let upstream_opts = mysql_helpers::upstream_config().db_name(Some(db_name));
    let mut upstream_conn = mysql_async::Conn::new(upstream_opts).await.unwrap();
    let mut rs_conn = mysql_async::Conn::new(rs_opts).await.unwrap();
    upstream_conn.query_drop(T).await.unwrap();
    sleep().await;

    rs_conn
        .query_drop(
            "CREATE CACHE kept WITH (AUTOPARAM OFF) \
             FROM SELECT v FROM t WHERE id = ? AND status = 'active'",
        )
        .await
        .unwrap();

    let rows: Vec<i32> = eventually_readyset(
        &mut rs_conn,
        "SELECT v FROM t WHERE id = 1 AND status = 'active'",
        QueryDestination::Readyset(Some("kept".into())),
    )
    .await;
    assert_eq!(rows, vec![10]);

    // The literal the cache kept is part of its shape, so another value is another query.

    let rows: Vec<i32> = eventually_readyset(
        &mut rs_conn,
        "SELECT v FROM t WHERE id = 1 AND status = 'archived'",
        QueryDestination::Upstream,
    )
    .await;
    assert_eq!(rows, vec![20]);

    shutdown_tx.shutdown().await;
}

/// A literal `IN` is the shape the mode exists for: parameterizing it would explode into one
/// lookup key per element, so the cache keeps it inline and the server has to lower it as an
/// ordinary filter.
#[tokio::test]
#[tags(serial)]
#[upstream(mysql)]
async fn a_kept_in_list_is_lowered_as_a_filter() {
    let (mut rs_conn, _upstream, _handle, shutdown_tx) = adapter("autoparam_mode_kept_in", T).await;

    rs_conn
        .query_drop(
            "CREATE CACHE kept_in WITH (AUTOPARAM OFF) FROM \
             SELECT v FROM t WHERE id = ? AND status IN ('active', 'archived')",
        )
        .await
        .unwrap();
    sleep().await;

    let mut result: Vec<i32> = rs_conn
        .query("SELECT v FROM t WHERE id = 1 AND status IN ('active', 'archived')")
        .await
        .unwrap();
    result.sort();
    // `pending` shares this `id`, so the kept list is what excludes it: a dropped predicate
    // would return it too.
    assert_eq!(result, vec![10, 20]);
    assert_last_target_was(
        &mut rs_conn,
        QueryDestination::Readyset(Some("kept_in".into())),
    )
    .await;

    // A different list is a different query, whatever its length.
    for other in [
        "SELECT v FROM t WHERE id = 1 AND status IN ('active')",
        "SELECT v FROM t WHERE id = 1 AND status IN ('active', 'archived', 'pending')",
    ] {
        let _: Vec<i32> = rs_conn.query(other).await.unwrap();
        assert_last_target_was(&mut rs_conn, QueryDestination::Upstream).await;
    }

    shutdown_tx.shutdown().await;
}

/// Auto-creation never mints a cache that keeps a literal inline. Three reads differing only in
/// that literal share one parameterized cache, where one per literal would multiply caches by the
/// column's cardinality.
#[tokio::test]
#[tags(serial)]
#[upstream(mysql)]
async fn auto_creation_mints_one_cache_for_every_literal() {
    readyset_tracing::init_test_logging();
    let db_name = "autoparam_mode_no_mint";
    mysql_helpers::recreate_database(db_name).await;

    let (rs_opts, _handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .migration_mode(MigrationMode::InRequestPath)
        .fallback(true)
        .replicate_db(db_name)
        .build::<MySQLAdapter>()
        .await;

    let upstream_opts = mysql_helpers::upstream_config().db_name(Some(db_name));
    let mut upstream_conn = mysql_async::Conn::new(upstream_opts).await.unwrap();
    let mut rs_conn = mysql_async::Conn::new(rs_opts).await.unwrap();
    upstream_conn.query_drop(T).await.unwrap();
    sleep().await;

    rs_conn
        .query_drop(
            "CREATE CACHE kept WITH (AUTOPARAM OFF) FROM SELECT v FROM t WHERE id = ? AND status = 'active'",
        )
        .await
        .unwrap();
    sleep().await;

    let before: Vec<mysql_async::Row> = rs_conn.query("SHOW CACHES").await.unwrap();

    for status in ["archived", "pending", "deleted"] {
        let _: Vec<i32> = rs_conn
            .query(format!(
                "SELECT v FROM t WHERE id = 1 AND status = '{status}'"
            ))
            .await
            .unwrap();
    }
    sleep().await;

    let after: Vec<mysql_async::Row> = rs_conn.query("SHOW CACHES").await.unwrap();
    assert_eq!(
        after.len(),
        before.len() + 1,
        "three literals should share one parameterized cache: {} -> {}",
        before.len(),
        after.len()
    );

    shutdown_tx.shutdown().await;
}

/// One shape holds every cache over it, since two caches differing only in a literal share it.
/// A read reaches whichever of them holds the literals it carries.
#[tokio::test]
#[tags(serial)]
#[upstream(mysql)]
async fn one_shape_holds_several_caches() {
    let (mut rs_conn, _upstream, _handle, shutdown_tx) =
        adapter("autoparam_mode_one_shape", T).await;

    rs_conn
        .query_drop("CREATE CACHE active WITH (AUTOPARAM OFF) FROM SELECT v FROM t WHERE id = ? AND status = 'active'")
        .await
        .unwrap();
    rs_conn
        .query_drop("CREATE CACHE archived WITH (AUTOPARAM OFF) FROM SELECT v FROM t WHERE id = ? AND status = 'archived'")
        .await
        .unwrap();
    // A cache keeping the other position stands alongside them, which is what a read carrying
    // its literals is matched against.
    rs_conn
        .query_drop("CREATE CACHE by_id WITH (AUTOPARAM OFF) FROM SELECT v FROM t WHERE id = 1 AND status = ?")
        .await
        .unwrap();

    // Each read reaches the one cache holding the literals it carries.

    let result: Vec<i32> = eventually_readyset(
        &mut rs_conn,
        "SELECT v FROM t WHERE id = 2 AND status = 'active'",
        QueryDestination::Readyset(Some("active".into())),
    )
    .await;
    assert_eq!(result, vec![30]);

    let result: Vec<i32> = rs_conn
        .query("SELECT v FROM t WHERE id = 1 AND status = 'archived'")
        .await
        .unwrap();
    assert_eq!(result, vec![20]);
    assert_last_target_was(
        &mut rs_conn,
        QueryDestination::Readyset(Some("archived".into())),
    )
    .await;

    // Dropping one gives up only its own claim: the others still serve.
    rs_conn.query_drop("DROP CACHE archived").await.unwrap();

    let result: Vec<i32> = eventually_readyset(
        &mut rs_conn,
        "SELECT v FROM t WHERE id = 2 AND status = 'active'",
        QueryDestination::Readyset(Some("active".into())),
    )
    .await;
    assert_eq!(result, vec![30]);

    // And the read whose literals only the dropped cache held goes upstream.

    let result: Vec<i32> = eventually_readyset(
        &mut rs_conn,
        "SELECT v FROM t WHERE id = 2 AND status = 'archived'",
        QueryDestination::Upstream,
    )
    .await;
    assert!(result.is_empty());

    shutdown_tx.shutdown().await;
}

/// A cache naming no option parameterizes every literal in a position that supports it, so one
/// cache serves every value of each.
#[tokio::test]
#[tags(serial)]
#[upstream(mysql)]
async fn a_cache_naming_no_option_parameterizes_every_literal() {
    let (mut rs_conn, _upstream, _handle, shutdown_tx) = adapter("autoparam_mode_default", T).await;

    rs_conn
        .query_drop("CREATE CACHE every FROM SELECT v FROM t WHERE id = ? AND status = 'active'")
        .await
        .unwrap();

    // The literal the author spelled out became a parameter, so the cache serves other values
    // of it too.
    for (status, expected) in [("active", vec![10]), ("archived", vec![20])] {
        let result: Vec<i32> = eventually_readyset(
            &mut rs_conn,
            format!("SELECT v FROM t WHERE id = 1 AND status = '{status}'"),
            QueryDestination::Readyset(Some("every".into())),
        )
        .await;
        assert_eq!(result, expected, "status = {status}");
    }

    shutdown_tx.shutdown().await;
}

/// A cache whose limit clause the adapter applies has it stripped before the shape is taken, so
/// the cache's view holds unbounded rows and a read of that shape has to be bounded by its own
/// limit rather than the one its cache was created with.
///
/// This holds where the server builds no TopK node, which is this harness's configuration. With
/// TopK on, a literal limit stays in the shape instead; the tests over [`topk_adapter`] cover
/// that side.
#[tokio::test]
#[tags(serial)]
#[upstream(mysql)]
async fn a_read_keeps_its_own_limit() {
    let (mut rs_conn, _upstream, _handle, shutdown_tx) =
        adapter("autoparam_mode_pagination", T).await;

    rs_conn
        .query_drop("CREATE CACHE paged WITH (AUTOPARAM OFF) FROM SELECT v FROM t WHERE status = 'active' LIMIT 1")
        .await
        .unwrap();

    let one: Vec<i32> = eventually_readyset(
        &mut rs_conn,
        "SELECT v FROM t WHERE status = 'active' LIMIT 1",
        QueryDestination::Readyset(Some("paged".into())),
    )
    .await;
    assert_eq!(one.len(), 1);

    // Two rows carry 'active'. The read asking for both has to get both.

    let mut both: Vec<i32> = eventually_readyset(
        &mut rs_conn,
        "SELECT v FROM t WHERE status = 'active' LIMIT 10",
        QueryDestination::Readyset(Some("paged".into())),
    )
    .await;
    both.sort();
    assert_eq!(both, vec![10, 30], "the read's own LIMIT has to bound it");

    shutdown_tx.shutdown().await;
}

/// `AUTOPARAM ON` is accepted and parameterizes, which is what an absent clause does too. It is
/// kept so a statement naming it round-trips through the DDL the authority persists.
#[tokio::test]
#[tags(serial)]
#[upstream(mysql)]
async fn a_cache_naming_autoparam_on_parameterizes() {
    let (mut rs_conn, _upstream, _handle, shutdown_tx) =
        adapter_with_preset("autoparam_percache_on", T, ParsingPreset::OnlySqlparser).await;

    rs_conn
        .query_drop(
            "CREATE CACHE opened WITH (AUTOPARAM ON) \
             FROM SELECT v FROM t WHERE id = 1 AND status = 'active'",
        )
        .await
        .unwrap();

    // Both literals became parameters, so a read naming other values reaches the same cache.

    let result: Vec<i32> = eventually_readyset(
        &mut rs_conn,
        "SELECT v FROM t WHERE id = 2 AND status = 'active'",
        QueryDestination::Readyset(Some("opened".into())),
    )
    .await;
    assert_eq!(result, vec![30]);

    let result: Vec<i32> = eventually_readyset(
        &mut rs_conn,
        "SELECT v FROM t WHERE id = 1 AND status = 'archived'",
        QueryDestination::Readyset(Some("opened".into())),
    )
    .await;
    assert_eq!(result, vec![20]);

    shutdown_tx.shutdown().await;
}

/// A cache keeping its literals inline and one parameterizing the same shape serve alongside each
/// other: neither hides the other.
#[tokio::test]
#[tags(serial)]
#[upstream(mysql)]
async fn a_kept_literal_does_not_hide_a_parameterized_cache() {
    let (mut rs_conn, _upstream, _handle, shutdown_tx) =
        adapter("autoparam_percache_mixed", T).await;

    rs_conn
        .query_drop(
            "CREATE CACHE kept WITH (AUTOPARAM OFF) \
             FROM SELECT v FROM t WHERE id = ? AND status = 'active'",
        )
        .await
        .unwrap();
    rs_conn
        .query_drop("CREATE CACHE both FROM SELECT v FROM t WHERE id = ? AND status = ?")
        .await
        .unwrap();

    // The literal this read spells out is the one `kept` holds inline.

    let result: Vec<i32> = eventually_readyset(
        &mut rs_conn,
        "SELECT v FROM t WHERE id = 1 AND status = 'active'",
        QueryDestination::Readyset(Some("kept".into())),
    )
    .await;
    assert_eq!(result, vec![10]);

    // No cache kept this one, so the parameterized cache serves it.

    let result: Vec<i32> = eventually_readyset(
        &mut rs_conn,
        "SELECT v FROM t WHERE id = 1 AND status = 'archived'",
        QueryDestination::Readyset(Some("both".into())),
    )
    .await;
    assert_eq!(result, vec![20]);

    shutdown_tx.shutdown().await;
}

/// A scoped exclusion cannot be routed to, since a read has no way to name the scope its own
/// literals came from, so the statement is refused rather than built as something else.
#[tokio::test]
#[tags(serial)]
#[upstream(mysql)]
async fn an_exclusion_scope_is_refused() {
    let (mut rs_conn, _upstream, _handle, shutdown_tx) =
        adapter("autoparam_percache_exclude", T).await;

    let err = rs_conn
        .query_drop(
            "CREATE CACHE scoped WITH (AUTOPARAM (EXCLUDE_JOINS)) \
             FROM SELECT v FROM t WHERE id = ? AND status = 'active'",
        )
        .await
        .expect_err("an exclusion scope is not supported");
    assert!(
        err.to_string().to_lowercase().contains("exclusion"),
        "unexpected error: {err}"
    );

    shutdown_tx.shutdown().await;
}

/// `SHOW CACHES` names the option every deep cache was created with, so which caches keep their
/// author's literals inline is visible without reading each query text for an inline literal.
#[tokio::test]
#[tags(serial)]
#[upstream(mysql)]
async fn show_caches_names_a_cache_that_keeps_its_literals() {
    let (mut rs_conn, _upstream, _handle, shutdown_tx) = adapter("autoparam_show_caches", T).await;

    rs_conn
        .query_drop(
            "CREATE CACHE kept WITH (AUTOPARAM OFF) \
             FROM SELECT v FROM t WHERE id = ? AND status = 'active'",
        )
        .await
        .unwrap();
    rs_conn
        .query_drop("CREATE CACHE plain FROM SELECT v FROM t WHERE id = ? AND status = ?")
        .await
        .unwrap();
    sleep().await;

    let rows: Vec<(String, String, String, String, String)> =
        rs_conn.query("SHOW CACHES").await.unwrap();
    let properties = |name: &str| {
        rows.iter()
            .find(|(_, n, ..)| n == name)
            .map(|(_, _, _, p, _)| p.clone())
            .unwrap_or_else(|| panic!("{name} is not listed"))
    };
    assert!(
        properties("kept").contains("autoparam off"),
        "expected the option to be listed, got {:?}",
        properties("kept")
    );
    assert!(
        properties("plain").contains("autoparam on"),
        "a cache that parameterizes says so too, got {:?}",
        properties("plain")
    );

    shutdown_tx.shutdown().await;
}

/// `EXPLAIN CACHES` writes the option back into each reconstructed statement, so replaying its
/// output recreates a cache that keeps its literals as one that keeps them.
#[tokio::test]
#[tags(serial)]
#[upstream(mysql)]
async fn explain_caches_names_a_cache_that_keeps_its_literals() {
    let (mut rs_conn, _upstream, _handle, shutdown_tx) =
        adapter("autoparam_explain_caches", T).await;

    rs_conn
        .query_drop(
            "CREATE CACHE kept WITH (AUTOPARAM OFF) \
             FROM SELECT v FROM t WHERE id = ? AND status = 'active'",
        )
        .await
        .unwrap();
    rs_conn
        .query_drop("CREATE CACHE plain FROM SELECT v FROM t WHERE id = ? AND status = ?")
        .await
        .unwrap();
    sleep().await;

    let rows: Vec<String> = rs_conn.query("EXPLAIN CACHES").await.unwrap();
    let statement = |name: &str| {
        rows.iter()
            .find(|s| s.contains(&format!("`{name}`")))
            .unwrap_or_else(|| panic!("{name} is not listed in {rows:?}"))
    };
    assert!(
        statement("kept").contains("WITH (AUTOPARAM OFF)"),
        "expected the option to be listed, got {:?}",
        statement("kept")
    );
    assert!(
        !statement("plain").contains("AUTOPARAM"),
        "a cache that parameterizes lists no option, got {:?}",
        statement("plain")
    );

    shutdown_tx.shutdown().await;
}

/// A query reaches the status cache already rewritten, so naming one by id leaves no literals for
/// `AUTOPARAM OFF` to keep. The statement is refused rather than built as an ordinary cache.
#[tokio::test]
#[tags(serial)]
#[upstream(mysql)]
async fn keeping_literals_from_a_query_id_is_refused() {
    readyset_tracing::init_test_logging();
    let db_name = "autoparam_from_id";
    mysql_helpers::recreate_database(db_name).await;
    let (rs_opts, _handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .migration_mode(MigrationMode::OutOfBand)
        .migration_style(MigrationStyle::Explicit)
        .fallback(true)
        .replicate_db(db_name)
        .build::<MySQLAdapter>()
        .await;
    let upstream_opts = mysql_helpers::upstream_config().db_name(Some(db_name));
    let mut upstream_conn = mysql_async::Conn::new(upstream_opts).await.unwrap();
    let mut rs_conn = mysql_async::Conn::new(rs_opts).await.unwrap();
    upstream_conn.query_drop(T).await.unwrap();
    sleep().await;

    // Proxy a query so it is recorded with an id of its own.
    let _: Vec<i32> = rs_conn
        .query("SELECT id FROM t WHERE v = 999")
        .await
        .unwrap_or_default();
    sleep().await;
    let proxied: Vec<(String, String, String, String)> =
        rs_conn.query("SHOW PROXIED QUERIES").await.unwrap();
    let query_id = proxied
        .first()
        .map(|(id, ..)| id.clone())
        .expect("the proxied query is recorded with an id");

    let err = rs_conn
        .query_drop(format!(
            "CREATE CACHE from_id WITH (AUTOPARAM OFF) FROM {query_id}"
        ))
        .await
        .expect_err("a query id keeps no literals to act on");
    assert!(
        err.to_string().to_lowercase().contains("autoparam"),
        "unexpected error: {err}"
    );

    // The same id without the option still caches, so only what cannot be honoured is refused.
    rs_conn
        .query_drop(format!("CREATE CACHE plain_id FROM {query_id}"))
        .await
        .expect("a query id caches as usual");

    shutdown_tx.shutdown().await;
}

/// A statement keeping its literals inline is refused where a read could not reach the cache it
/// would build.
///
/// The shape a read hashes to comes out of the rewrite's structural passes, and those run only
/// for a statement holding no placeholder. A statement holding one alongside a join or a subquery
/// takes a shape no read arrives at, so the cache would serve nothing and the statement is
/// refused instead. A single-table statement is unaffected: there the passes reshape nothing, so
/// the two shapes coincide and a read reaches the cache.
#[tokio::test]
#[tags(serial)]
#[upstream(mysql)]
async fn a_placeholder_in_a_nested_statement_is_refused() {
    let (mut rs_conn, _upstream, _handle, shutdown_tx) = adapter("autoparam_mode_nested", T).await;

    let refused = rs_conn
        .query_drop(
            "CREATE CACHE gated WITH (AUTOPARAM OFF) \
             FROM SELECT d.v FROM (SELECT id, status, v FROM t) AS d \
             WHERE d.id = ? AND d.status = 'active'",
        )
        .await;
    let err = refused.expect_err("a nested statement with a placeholder has to be refused");
    assert!(
        err.to_string().contains("AUTOPARAM with a placeholder"),
        "unexpected error: {err}"
    );

    // The same option over a single-table statement still builds a cache a read reaches.
    rs_conn
        .query_drop(
            "CREATE CACHE flat WITH (AUTOPARAM OFF) \
             FROM SELECT v FROM t WHERE id = ? AND status = 'active'",
        )
        .await
        .unwrap();

    let got: Vec<i32> = eventually_readyset(
        &mut rs_conn,
        "SELECT v FROM t WHERE id = 1 AND status = 'active'",
        QueryDestination::Readyset(Some("flat".into())),
    )
    .await;
    assert_eq!(got, vec![10]);

    shutdown_tx.shutdown().await;
}

/// A cache that keeps its literals inline is reached by an ad-hoc read, which carries its
/// literals as text.
///
/// A prepared statement spelling out the literal the cache kept reaches it, with its bound value
/// keying the lookup, the same way the ad-hoc form does.
#[tokio::test]
#[tags(serial)]
#[upstream(mysql)]
async fn a_prepared_read_reaches_a_per_cache_off_cache() {
    let (mut rs_conn, _upstream, _handle, shutdown_tx) =
        adapter("autoparam_mode_prepared_pc", T).await;

    rs_conn
        .query_drop(
            "CREATE CACHE kept WITH (AUTOPARAM OFF) \
             FROM SELECT v FROM t WHERE id = ? AND status = 'active'",
        )
        .await
        .unwrap();

    // The ad-hoc form carries its literals as text and reaches the cache directly.

    let rows: Vec<i32> = eventually_readyset(
        &mut rs_conn,
        "SELECT v FROM t WHERE id = 1 AND status = 'active'",
        QueryDestination::Readyset(Some("kept".into())),
    )
    .await;
    assert_eq!(rows, vec![10]);

    let rows: Vec<i32> = rs_conn
        .exec("SELECT v FROM t WHERE id = ? AND status = 'active'", (1,))
        .await
        .unwrap();
    assert_eq!(rows, vec![10]);
    assert_last_target_was(
        &mut rs_conn,
        QueryDestination::Readyset(Some("kept".into())),
    )
    .await;

    shutdown_tx.shutdown().await;
}

/// The cache's parameters take their values from two places at once: a position this statement
/// spells out is filled from its literal, and one it binds is filled by the client. They have to
/// interleave in the cache's own parameter order, which only shows when the two differ.
#[tokio::test]
#[tags(serial)]
#[upstream(mysql)]
async fn a_prepared_read_interleaves_its_literals_with_its_bound_values() {
    let (mut rs_conn, _upstream, _handle, shutdown_tx) =
        adapter("autoparam_mode_prepared_mix", T).await;

    // Two parameterized positions and one kept inline, so a read can spell out either parameter
    // and bind the other.
    rs_conn
        .query_drop(
            "CREATE CACHE mixed WITH (AUTOPARAM OFF) \
             FROM SELECT v FROM t WHERE id = ? AND v = ? AND status = 'active'",
        )
        .await
        .unwrap();

    let served = QueryDestination::Readyset(Some("mixed".into()));

    // `id` spelled out, `v` bound: the literal fills the cache's first parameter and the client's
    // value fills the second.

    let rows: Vec<i32> = eventually_readyset_exec(
        &mut rs_conn,
        "SELECT v FROM t WHERE id = 1 AND v = ? AND status = 'active'",
        (10,),
        served.clone(),
    )
    .await;
    assert_eq!(rows, vec![10]);

    // A value matching no row still reaches the cache, which is what says the key was built in
    // the right order rather than landing on another row by luck.

    let rows: Vec<i32> = eventually_readyset_exec(
        &mut rs_conn,
        "SELECT v FROM t WHERE id = 1 AND v = ? AND status = 'active'",
        (30,),
        served.clone(),
    )
    .await;
    assert!(rows.is_empty());

    // The other way round: `v` spelled out, `id` bound. The same cache, the opposite interleave.

    let rows: Vec<i32> = eventually_readyset_exec(
        &mut rs_conn,
        "SELECT v FROM t WHERE id = ? AND v = 10 AND status = 'active'",
        (1,),
        served,
    )
    .await;
    assert_eq!(rows, vec![10]);

    shutdown_tx.shutdown().await;
}

/// A statement binding the position the cache kept inline is matched by the value it binds, at
/// execute: the value the cache kept reaches it, another goes upstream.
#[tokio::test]
#[tags(serial)]
#[upstream(mysql)]
async fn a_prepared_read_binding_a_kept_position_is_matched_by_its_value() {
    let (mut rs_conn, _upstream, _handle, shutdown_tx) =
        adapter("autoparam_mode_prepared_bind", T).await;

    rs_conn
        .query_drop(
            "CREATE CACHE kept WITH (AUTOPARAM OFF) \
             FROM SELECT v FROM t WHERE id = ? AND status = 'active'",
        )
        .await
        .unwrap();
    sleep().await;

    let read = "SELECT v FROM t WHERE id = ? AND status = ?";
    let rows: Vec<i32> = rs_conn.exec(read, (1, "active")).await.unwrap();
    assert_eq!(rows, vec![10]);
    assert_last_target_was(
        &mut rs_conn,
        QueryDestination::Readyset(Some("kept".into())),
    )
    .await;

    let rows: Vec<i32> = rs_conn.exec(read, (1, "archived")).await.unwrap();
    assert_eq!(rows, vec![20]);
    assert_last_target_was(&mut rs_conn, QueryDestination::Upstream).await;

    shutdown_tx.shutdown().await;
}

/// Caching a few hot values of a key the application binds: one prepared statement lands on a
/// different cache per execute, and upstream for a value no cache kept.
#[tokio::test]
#[tags(serial)]
#[upstream(mysql)]
async fn one_prepared_statement_reaches_a_different_cache_per_bound_value() {
    let (mut rs_conn, _upstream, _handle, shutdown_tx) =
        adapter("autoparam_mode_prepared_values", T).await;

    for (name, status) in [("act", "active"), ("arc", "archived")] {
        rs_conn
            .query_drop(format!(
                "CREATE CACHE {name} WITH (AUTOPARAM OFF) \
                 FROM SELECT v FROM t WHERE status = '{status}' ORDER BY v"
            ))
            .await
            .unwrap();
    }
    sleep().await;

    let read = "SELECT v FROM t WHERE status = ? ORDER BY v";
    let rows: Vec<i32> = rs_conn.exec(read, ("active",)).await.unwrap();
    assert_eq!(rows, vec![10, 30]);
    assert_last_target_was(&mut rs_conn, QueryDestination::Readyset(Some("act".into()))).await;

    let rows: Vec<i32> = rs_conn.exec(read, ("archived",)).await.unwrap();
    assert_eq!(rows, vec![20]);
    assert_last_target_was(&mut rs_conn, QueryDestination::Readyset(Some("arc".into()))).await;

    let rows: Vec<i32> = rs_conn.exec(read, ("pending",)).await.unwrap();
    assert_eq!(rows, vec![40]);
    assert_last_target_was(&mut rs_conn, QueryDestination::Upstream).await;

    shutdown_tx.shutdown().await;
}

/// Rows enough that every limit in the TopK tests is a real bound: three per status, ordered by
/// `v`, and a `pending` row so a read that matches no cache still proves the upstream answered.
/// A prepared statement whose cache is dropped stops being served by it, and picks it up again
/// when an identical cache is created.
#[tokio::test]
#[tags(serial)]
#[upstream(mysql)]
async fn a_prepared_read_follows_its_cache_being_dropped_and_recreated() {
    let (mut rs_conn, _upstream, _handle, shutdown_tx) =
        adapter("autoparam_mode_prepared_drop", T).await;

    let create = "CREATE CACHE kept WITH (AUTOPARAM OFF) \
                  FROM SELECT v FROM t WHERE id = ? AND status = 'active'";
    let read = "SELECT v FROM t WHERE id = ? AND status = 'active'";
    let served = QueryDestination::Readyset(Some("kept".into()));

    rs_conn.query_drop(create).await.unwrap();
    sleep().await;
    let rows: Vec<i32> = rs_conn.exec(read, (1,)).await.unwrap();
    assert_eq!(rows, vec![10]);
    assert_last_target_was(&mut rs_conn, served.clone()).await;

    // Dropping it has to take the statement upstream rather than leave it reading a gone view.
    rs_conn.query_drop("DROP CACHE kept").await.unwrap();
    sleep().await;
    let rows: Vec<i32> = rs_conn.exec(read, (1,)).await.unwrap();
    assert_eq!(rows, vec![10], "still correct rows once the cache is gone");
    assert_last_target_was(&mut rs_conn, QueryDestination::Upstream).await;

    // And creating it again has to be picked up.
    rs_conn.query_drop(create).await.unwrap();
    sleep().await;
    let rows: Vec<i32> = rs_conn.exec(read, (1,)).await.unwrap();
    assert_eq!(rows, vec![10]);
    assert_last_target_was(&mut rs_conn, served).await;

    shutdown_tx.shutdown().await;
}

/// Matching happens at execute, so a statement prepared before its cache existed reaches the
/// cache from the next execute on, and stops when the cache is dropped.
#[tokio::test]
#[tags(serial)]
#[upstream(mysql)]
async fn a_prepared_read_reaches_a_cache_created_after_it_prepared() {
    let (mut rs_conn, _upstream, _handle, shutdown_tx) =
        adapter("autoparam_mode_prepared_after", T).await;

    let read = "SELECT v FROM t WHERE id = ? AND status = 'active'";

    // Prepared and executed while no cache exists, so the statement settles on proxying.
    let rows: Vec<i32> = rs_conn.exec(read, (1,)).await.unwrap();
    assert_eq!(rows, vec![10]);
    assert_last_target_was(&mut rs_conn, QueryDestination::Upstream).await;

    rs_conn
        .query_drop(
            "CREATE CACHE kept WITH (AUTOPARAM OFF) \
             FROM SELECT v FROM t WHERE id = ? AND status = 'active'",
        )
        .await
        .unwrap();
    sleep().await;

    // Same connection, same statement: the cache it now belongs to serves it.
    let rows: Vec<i32> = rs_conn.exec(read, (1,)).await.unwrap();
    assert_eq!(rows, vec![10]);
    assert_last_target_was(
        &mut rs_conn,
        QueryDestination::Readyset(Some("kept".into())),
    )
    .await;

    rs_conn.query_drop("DROP CACHE kept").await.unwrap();
    sleep().await;

    let rows: Vec<i32> = rs_conn.exec(read, (1,)).await.unwrap();
    assert_eq!(rows, vec![10]);
    assert_last_target_was(&mut rs_conn, QueryDestination::Upstream).await;

    shutdown_tx.shutdown().await;
}

/// A cache that keeps its literals inline has to come back from a restart and serve traffic
/// again. The server replays the cache DDL and re-derives the same form, since it is a function
/// of the statement alone; the adapter has to recover which shape holds which cache so a read
/// still reaches it.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(mysql, modern)]
async fn a_kept_literal_cache_survives_a_restart() {
    readyset_tracing::init_test_logging();
    let db_name = "autoparam_mode_restart";
    mysql_helpers::recreate_database(db_name).await;

    let upstream_opts = mysql_helpers::upstream_config().db_name(Some(db_name));
    let mut upstream_conn = mysql_async::Conn::new(upstream_opts).await.unwrap();
    upstream_conn.query_drop(T).await.unwrap();

    let (rs_opts, handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .migration_mode(MigrationMode::OutOfBand)
        .migration_style(MigrationStyle::Explicit)
        .replicate_db(db_name)
        .fallback(true)
        .build::<MySQLAdapter>()
        .await;

    let mut rs_conn = mysql_async::Conn::new(rs_opts).await.unwrap();
    eventually! {
        let rows: Vec<mysql_async::Row> = rs_conn.query("SHOW READYSET STATUS").await.unwrap();
        rows.iter().any(|r| r.get::<String, _>(1).as_deref() == Some("Online"))
    }

    rs_conn
        .query_drop("CREATE CACHE kept WITH (AUTOPARAM OFF) FROM SELECT v FROM t WHERE id = ? AND status = 'active'")
        .await
        .unwrap();

    let result: Vec<i32> = eventually_readyset(
        &mut rs_conn,
        "SELECT v FROM t WHERE id = 1 AND status = 'active'",
        QueryDestination::Readyset(Some("kept".into())),
    )
    .await;
    assert_eq!(result, vec![10]);

    drop(rs_conn);

    let (rs_opts, _handle, shutdown_tx) = shutdown_tx.restart(handle).await;

    let mut rs_conn = mysql_async::Conn::new(rs_opts).await.unwrap();
    eventually! {
        let caches: Vec<mysql_async::Row> = rs_conn.query("SHOW CACHES").await.unwrap();
        !caches.is_empty()
    }

    // Present is not enough: the read has to reach it. The views synchronizer has to promote
    // the recovered form's status first, once it sees the server's view for it.
    eventually!(run_test: {
        let rows: Vec<i32> = rs_conn
            .query("SELECT v FROM t WHERE id = 1 AND status = 'active'")
            .await
            .unwrap();
        let info: QueryInfo = rs_conn
            .query_first("EXPLAIN LAST STATEMENT")
            .await
            .unwrap()
            .unwrap();
        (rows, info.destination)
    }, then_assert: |(rows, destination)| {
        assert_eq!(rows, vec![10]);
        assert_eq!(destination, QueryDestination::Readyset(Some("kept".into())));
    });
    shutdown_tx.shutdown().await;
}

/// A cache the server drops leaves nothing for a read to reach, so it falls to the upstream. The
/// entry may be filed again -- the rewrite reads no column -- but the cache's own status is gone,
/// which is what keeps the read off it.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(mysql)]
async fn a_dropped_cache_takes_its_reads_upstream() {
    readyset_tracing::init_test_logging();
    let db_name = "autoparam_mode_dropped_cache";
    mysql_helpers::recreate_database(db_name).await;

    let query_status_cache: &'static QueryStatusCache = Box::leak(Box::new(
        QueryStatusCache::new().style(MigrationStyle::Explicit),
    ));
    let (rs_opts, mut handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .migration_mode(MigrationMode::OutOfBand)
        .migration_style(MigrationStyle::Explicit)
        .query_status_cache(query_status_cache)
        .fallback(true)
        .replicate_db(db_name)
        .build::<MySQLAdapter>()
        .await;
    let upstream_opts = mysql_helpers::upstream_config().db_name(Some(db_name));
    let mut upstream_conn = mysql_async::Conn::new(upstream_opts).await.unwrap();
    let mut rs_conn = mysql_async::Conn::new(rs_opts).await.unwrap();
    upstream_conn.query_drop(T).await.unwrap();
    sleep().await;

    rs_conn
        .query_drop(
            "CREATE CACHE gone WITH (AUTOPARAM OFF) \
             FROM SELECT v FROM t WHERE id = ? AND status = 'active'",
        )
        .await
        .unwrap();
    let read = "SELECT v FROM t WHERE id = 1 AND status = 'active'";
    let rows: Vec<i32> = eventually_readyset(
        &mut rs_conn,
        read,
        QueryDestination::Readyset(Some("gone".into())),
    )
    .await;
    assert_eq!(rows, vec![10]);
    assert!(query_status_cache.may_have_inline_literal_caches());

    let generation = handle.schema_catalog().await.unwrap().generation;
    upstream_conn
        .query_drop("ALTER TABLE t DROP COLUMN status")
        .await
        .unwrap();
    wait_for_schema_generation_change(&mut handle, generation).await;

    // The entry goes with the column and nothing files it again.
    // The server dropped the cache with the column.
    eventually!(run_test: {
        let caches: Vec<mysql_async::Row> = rs_conn.query("SHOW CACHES").await.unwrap();
        caches.len()
    }, then_assert: |len| assert_eq!(len, 0));

    // So the read is answered by the upstream, as it would be for any dropped cache.
    let read_after = "SELECT v FROM t WHERE id = 1";
    eventually!(run_test: {
        let rows: Vec<i32> = rs_conn.query(read_after).await.unwrap();
        (rows, last_target(&mut rs_conn).await.0)
    }, then_assert: |(rows, destination)| {
        assert_eq!(rows, vec![10, 20, 40]);
        assert_eq!(destination, QueryDestination::Upstream);
    });

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(mysql)]
async fn a_kept_literal_cache_survives_a_schema_change_that_keeps_it() {
    readyset_tracing::init_test_logging();
    let db_name = "autoparam_mode_schema_change";
    mysql_helpers::recreate_database(db_name).await;

    let query_status_cache: &'static QueryStatusCache = Box::leak(Box::new(
        QueryStatusCache::new().style(MigrationStyle::Explicit),
    ));
    let (rs_opts, mut handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .migration_mode(MigrationMode::OutOfBand)
        .migration_style(MigrationStyle::Explicit)
        .query_status_cache(query_status_cache)
        .fallback(true)
        .replicate_db(db_name)
        .build::<MySQLAdapter>()
        .await;
    let upstream_opts = mysql_helpers::upstream_config().db_name(Some(db_name));
    let mut upstream_conn = mysql_async::Conn::new(upstream_opts).await.unwrap();
    let mut rs_conn = mysql_async::Conn::new(rs_opts).await.unwrap();
    upstream_conn.query_drop(T).await.unwrap();
    sleep().await;

    rs_conn
        .query_drop(
            "CREATE CACHE kept WITH (AUTOPARAM OFF) \
             FROM SELECT v FROM t WHERE id = ? AND status = 'active'",
        )
        .await
        .unwrap();
    let read = "SELECT v FROM t WHERE id = 1 AND status = 'active'";
    let served = QueryDestination::Readyset(Some("kept".into()));
    let rows: Vec<i32> = eventually_readyset(&mut rs_conn, read, served.clone()).await;
    assert_eq!(rows, vec![10]);

    let filed = query_status_cache.inline_literal_caches_generation();
    let generation = handle.schema_catalog().await.unwrap().generation;
    upstream_conn
        .query_drop("ALTER TABLE t ADD INDEX status_idx (status)")
        .await
        .unwrap();
    wait_for_schema_generation_change(&mut handle, generation).await;
    eventually! {
        query_status_cache.inline_literal_caches_generation() > filed
    }
    eventually!(
        message: "the kept cache should be filed again after the schema change".to_string(),
        { query_status_cache.may_have_inline_literal_caches() }
    );

    // The server kept the cache, so the read has to reach it again.
    let caches: Vec<mysql_async::Row> = rs_conn.query("SHOW CACHES").await.unwrap();
    assert_eq!(caches.len(), 1, "an added index drops no cache");
    eventually!(run_test: {
        let rows: Vec<i32> = rs_conn.query(read).await.unwrap();
        (rows, last_target(&mut rs_conn).await.0)
    }, then_assert: |(rows, destination)| {
        assert_eq!(rows, vec![10]);
        assert_eq!(destination, served);
    });

    shutdown_tx.shutdown().await;
}

/// Rows enough that every limit in the TopK tests is a real bound: three per status, ordered by
/// `v`, and a `pending` row so a read that matches no cache still proves the upstream answered.
const TK: &str = "CREATE TABLE t (id int, status varchar(16), v int); \
                  INSERT INTO t (id, status, v) VALUES \
                  (1, 'active', 10), (2, 'active', 30), (3, 'active', 50), \
                  (4, 'archived', 20), (5, 'archived', 40), (6, 'archived', 60), \
                  (7, 'pending', 70);";

/// Like [`adapter`], but with the server building TopK nodes, the production default. A literal
/// `LIMIT` under an `ORDER BY` then stays in the shape a read hashes to, while a placeholder
/// `LIMIT` is stripped from it, so the two spellings of one query take different shapes.
async fn topk_adapter(db_name: &str, schema: &str) -> (Conn, Conn, Handle, TestShutdownSender<MySQLAdapter>) {
    readyset_tracing::init_test_logging();
    mysql_helpers::recreate_database(db_name).await;

    let (rs_opts, handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .migration_mode(MigrationMode::OutOfBand)
        .parsing_preset(ParsingPreset::OnlySqlparser)
        .set_topk(true)
        .fallback(true)
        .replicate_db(db_name)
        .build::<MySQLAdapter>()
        .await;

    let upstream_opts = mysql_helpers::upstream_config().db_name(Some(db_name));
    let mut upstream_conn = mysql_async::Conn::new(upstream_opts).await.unwrap();
    let rs_conn = mysql_async::Conn::new(rs_opts).await.unwrap();

    upstream_conn.query_drop(schema).await.unwrap();
    sleep().await;

    (rs_conn, upstream_conn, handle, shutdown_tx)
}

/// A cache that kept a literal `LIMIT` under an `ORDER BY` is a TopK cache: the limit is part of
/// the shape it is filed under, so only a read spelling out that limit reaches it. A read with
/// another limit tries the stripped shape too, and no cache is filed there.
#[tokio::test]
#[tags(serial)]
#[upstream(mysql)]
async fn a_topk_read_reaches_the_cache_that_kept_its_limit() {
    let (mut rs_conn, _upstream, _handle, shutdown_tx) =
        topk_adapter("autoparam_topk_kept_limit", TK).await;

    rs_conn
        .query_drop(
            "CREATE CACHE lit WITH (AUTOPARAM OFF) \
             FROM SELECT v FROM t WHERE status = 'active' ORDER BY v LIMIT 2",
        )
        .await
        .unwrap();

    let rows: Vec<i32> = eventually_readyset(
        &mut rs_conn,
        "SELECT v FROM t WHERE status = 'active' ORDER BY v LIMIT 2",
        QueryDestination::Readyset(Some("lit".into())),
    )
    .await;
    assert_eq!(rows, vec![10, 30]);

    let rows: Vec<i32> = eventually_readyset(
        &mut rs_conn,
        "SELECT v FROM t WHERE status = 'active' ORDER BY v LIMIT 1",
        QueryDestination::Upstream,
    )
    .await;
    assert_eq!(rows, vec![10]);

    shutdown_tx.shutdown().await;
}

/// A cache created with a placeholder `LIMIT` is filed under the shape that strips the limit,
/// while a read spelling one out under an `ORDER BY` first takes the shape that keeps it. The
/// read reaches the cache on a second attempt without the limit -- the same second attempt the
/// router makes for an ordinary cache -- and its own limit bounds the rows.
#[tokio::test]
#[tags(serial)]
#[upstream(mysql)]
async fn a_literal_limit_read_reaches_a_cache_with_a_placeholder_limit() {
    let (mut rs_conn, _upstream, _handle, shutdown_tx) =
        topk_adapter("autoparam_topk_placeholder_limit", TK).await;

    rs_conn
        .query_drop(
            "CREATE CACHE par WITH (AUTOPARAM OFF) \
             FROM SELECT v FROM t WHERE status = 'archived' ORDER BY v LIMIT ?",
        )
        .await
        .unwrap();

    let served = QueryDestination::Readyset(Some("par".into()));

    let rows: Vec<i32> = eventually_readyset(
        &mut rs_conn,
        "SELECT v FROM t WHERE status = 'archived' ORDER BY v LIMIT 1",
        served.clone(),
    )
    .await;
    assert_eq!(rows, vec![20]);

    // Another limit is the same query to this cache: the read's own limit bounds it.

    let rows: Vec<i32> = eventually_readyset(
        &mut rs_conn,
        "SELECT v FROM t WHERE status = 'archived' ORDER BY v LIMIT 2",
        served,
    )
    .await;
    assert_eq!(rows, vec![20, 40]);

    // The second attempt lands on the shape the cache is filed under, but the literal it kept
    // still decides: a different value there is a different query.

    let rows: Vec<i32> = eventually_readyset(
        &mut rs_conn,
        "SELECT v FROM t WHERE status = 'pending' ORDER BY v LIMIT 1",
        QueryDestination::Upstream,
    )
    .await;
    assert_eq!(rows, vec![70]);

    // Dropping the cache takes the second attempt's target with it.
    rs_conn.query_drop("DROP CACHE par").await.unwrap();

    let rows: Vec<i32> = eventually_readyset(
        &mut rs_conn,
        "SELECT v FROM t WHERE status = 'archived' ORDER BY v LIMIT 1",
        QueryDestination::Upstream,
    )
    .await;
    assert_eq!(rows, vec![20]);

    shutdown_tx.shutdown().await;
}

/// One query text can stand behind two caches at once: one that kept its literal limit and one
/// created with a placeholder. The read whose limit the kept cache holds reaches it on the first
/// attempt, so the server does the bounding; every other limit falls through to the placeholder
/// cache on the second. Dropping the kept cache leaves its read the second attempt.
#[tokio::test]
#[tags(serial)]
#[upstream(mysql)]
async fn a_read_prefers_the_cache_that_kept_its_exact_limit() {
    let (mut rs_conn, _upstream, _handle, shutdown_tx) =
        topk_adapter("autoparam_topk_two_caches", TK).await;

    rs_conn
        .query_drop(
            "CREATE CACHE lit2 WITH (AUTOPARAM OFF) \
             FROM SELECT v FROM t WHERE status = 'archived' ORDER BY v LIMIT 2",
        )
        .await
        .unwrap();
    rs_conn
        .query_drop(
            "CREATE CACHE par WITH (AUTOPARAM OFF) \
             FROM SELECT v FROM t WHERE status = 'archived' ORDER BY v LIMIT ?",
        )
        .await
        .unwrap();

    let rows: Vec<i32> = eventually_readyset(
        &mut rs_conn,
        "SELECT v FROM t WHERE status = 'archived' ORDER BY v LIMIT 2",
        QueryDestination::Readyset(Some("lit2".into())),
    )
    .await;
    assert_eq!(rows, vec![20, 40]);

    let rows: Vec<i32> = eventually_readyset(
        &mut rs_conn,
        "SELECT v FROM t WHERE status = 'archived' ORDER BY v LIMIT 1",
        QueryDestination::Readyset(Some("par".into())),
    )
    .await;
    assert_eq!(rows, vec![20]);

    rs_conn.query_drop("DROP CACHE lit2").await.unwrap();

    let rows: Vec<i32> = eventually_readyset(
        &mut rs_conn,
        "SELECT v FROM t WHERE status = 'archived' ORDER BY v LIMIT 2",
        QueryDestination::Readyset(Some("par".into())),
    )
    .await;
    assert_eq!(rows, vec![20, 40]);

    shutdown_tx.shutdown().await;
}

/// The second attempt runs exactly where the router retries an ordinary cache: behind an
/// `ORDER BY`. An order-less literal limit keeps its one shape, so a cache filed under the
/// stripped form is not reached by it.
#[tokio::test]
#[tags(serial)]
#[upstream(mysql)]
async fn an_orderless_limit_read_takes_one_shape() {
    let (mut rs_conn, _upstream, _handle, shutdown_tx) =
        topk_adapter("autoparam_topk_orderless", TK).await;

    rs_conn
        .query_drop(
            "CREATE CACHE par_nord WITH (AUTOPARAM OFF) \
             FROM SELECT v FROM t WHERE status = 'archived' LIMIT ?",
        )
        .await
        .unwrap();

    let rows: Vec<i32> = eventually_readyset(
        &mut rs_conn,
        "SELECT v FROM t WHERE status = 'archived' LIMIT 1",
        QueryDestination::Upstream,
    )
    .await;
    assert_eq!(rows.len(), 1);
    assert!(
        [20, 40, 60].contains(&rows[0]),
        "an archived row, from upstream: {rows:?}"
    );

    shutdown_tx.shutdown().await;
}

/// The prepared form of the TopK reads: a statement spelling its limit out under an `ORDER BY`
/// reaches the cache that kept that limit, and another limit is another query.
#[tokio::test]
#[tags(serial)]
#[upstream(mysql)]
async fn a_prepared_topk_read_reaches_the_cache_that_kept_its_limit() {
    let (mut rs_conn, _upstream, _handle, shutdown_tx) =
        topk_adapter("autoparam_topk_prepared_kept", TK).await;

    rs_conn
        .query_drop(
            "CREATE CACHE lit WITH (AUTOPARAM OFF) \
             FROM SELECT v FROM t WHERE status = 'active' ORDER BY v LIMIT 2",
        )
        .await
        .unwrap();
    sleep().await;

    let rows: Vec<i32> = rs_conn
        .exec(
            "SELECT v FROM t WHERE status = 'active' ORDER BY v LIMIT 2",
            (),
        )
        .await
        .unwrap();
    assert_eq!(rows, vec![10, 30]);
    assert_last_target_was(&mut rs_conn, QueryDestination::Readyset(Some("lit".into()))).await;

    let rows: Vec<i32> = rs_conn
        .exec(
            "SELECT v FROM t WHERE status = 'active' ORDER BY v LIMIT 1",
            (),
        )
        .await
        .unwrap();
    assert_eq!(rows, vec![10]);
    assert_last_target_was(&mut rs_conn, QueryDestination::Upstream).await;

    shutdown_tx.shutdown().await;
}

/// A prepared statement is matched under the same two shapes an ad-hoc read tries. One binding
/// its limit needs only the first: its own rewrite strips the placeholder, which is the shape the
/// cache is filed under. One spelling the limit out keeps it in its first shape and finds the
/// cache under the second, with its own limit bounding the rows.
#[tokio::test]
#[tags(serial)]
#[upstream(mysql)]
async fn a_prepared_literal_limit_read_reaches_a_cache_with_a_placeholder_limit() {
    let (mut rs_conn, _upstream, _handle, shutdown_tx) =
        topk_adapter("autoparam_topk_prepared_par", TK).await;

    rs_conn
        .query_drop(
            "CREATE CACHE par WITH (AUTOPARAM OFF) \
             FROM SELECT v FROM t WHERE status = 'archived' ORDER BY v LIMIT ?",
        )
        .await
        .unwrap();
    sleep().await;

    let served = QueryDestination::Readyset(Some("par".into()));

    let rows: Vec<i32> = rs_conn
        .exec(
            "SELECT v FROM t WHERE status = 'archived' ORDER BY v LIMIT ?",
            (1,),
        )
        .await
        .unwrap();
    assert_eq!(rows, vec![20]);
    assert_last_target_was(&mut rs_conn, served.clone()).await;

    let rows: Vec<i32> = rs_conn
        .exec(
            "SELECT v FROM t WHERE status = 'archived' ORDER BY v LIMIT 2",
            (),
        )
        .await
        .unwrap();
    assert_eq!(rows, vec![20, 40]);
    assert_last_target_was(&mut rs_conn, served).await;

    // A literal the cache did not keep matches nothing on either attempt.
    let rows: Vec<i32> = rs_conn
        .exec(
            "SELECT v FROM t WHERE status = 'pending' ORDER BY v LIMIT 1",
            (),
        )
        .await
        .unwrap();
    assert_eq!(rows, vec![70]);
    assert_last_target_was(&mut rs_conn, QueryDestination::Upstream).await;

    shutdown_tx.shutdown().await;
}

/// A statement spelling its limit out, prepared while no cache exists, settles on proxying. A
/// placeholder-limit cache created afterwards is filed under the shape that strips the limit,
/// which the execute has to try after the statement's own. Dropping the cache lets the
/// statement go again.
#[tokio::test]
#[tags(serial)]
#[upstream(mysql)]
async fn a_prepared_literal_limit_read_picks_up_a_placeholder_limit_cache() {
    let (mut rs_conn, _upstream, _handle, shutdown_tx) =
        topk_adapter("autoparam_topk_prepared_pickup", TK).await;

    let read = "SELECT v FROM t WHERE status = 'archived' ORDER BY v LIMIT 2";

    let rows: Vec<i32> = rs_conn.exec(read, ()).await.unwrap();
    assert_eq!(rows, vec![20, 40]);
    assert_last_target_was(&mut rs_conn, QueryDestination::Upstream).await;

    rs_conn
        .query_drop(
            "CREATE CACHE par WITH (AUTOPARAM OFF) \
             FROM SELECT v FROM t WHERE status = 'archived' ORDER BY v LIMIT ?",
        )
        .await
        .unwrap();
    sleep().await;

    // Same connection, same statement: the cache it now belongs to serves it.
    let rows: Vec<i32> = rs_conn.exec(read, ()).await.unwrap();
    assert_eq!(rows, vec![20, 40]);
    assert_last_target_was(&mut rs_conn, QueryDestination::Readyset(Some("par".into()))).await;

    rs_conn.query_drop("DROP CACHE par").await.unwrap();
    sleep().await;
    let rows: Vec<i32> = rs_conn.exec(read, ()).await.unwrap();
    assert_eq!(rows, vec![20, 40]);
    assert_last_target_was(&mut rs_conn, QueryDestination::Upstream).await;

    shutdown_tx.shutdown().await;
}
