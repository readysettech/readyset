//! End-to-end collation correctness tests for the MySQL path.
//!
//! The oracle is always the live upstream. Every operation runs on both a direct upstream
//! connection and the Readyset connection and the results are diffed, so no per-collation
//! expectations are hardcoded.

use std::assert_matches;
use std::panic::AssertUnwindSafe;
use std::time::Duration;

use mysql_async::prelude::{FromRow, Queryable};
use pretty_assertions::assert_eq;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use readyset_adapter::backend::MigrationMode;
use readyset_client_metrics::QueryDestination;
use readyset_client_test_helpers::{
    TestBuilder,
    mysql_helpers::{self, MySQLAdapter, last_query_info},
};
use readyset_server::Handle;
use readyset_sql_parsing::ParsingPreset;
use readyset_util::eventually;
use readyset_util::shutdown::ShutdownSender;
use test_utils::{tags, upstream};

/// Case pairs plus a distinct high value so ORDER BY and MIN/MAX have a spread.
const CASE_ROWS: &str = "(1, 'abc'), (2, 'ABC'), (8, 'zzz')";
/// Accent and case variants of the same word.
const ACCENT_ROWS: &str = "(4, 'resume'), (5, 'Resume'), (6, 'résumé'), (7, 'RÉSUMÉ')";
/// The same prefix with and without a trailing space.
const PAD_ROWS: &str = "(1, 'abc'), (3, 'abc ')";
/// Case and accent rows together, giving every LIKE pattern a potential match.
const LIKE_ROWS: &str =
    "(1, 'abc'), (2, 'ABC'), (8, 'zzz'), (4, 'resume'), (5, 'Resume'), (6, 'résumé'), (7, 'RÉSUMÉ')";

/// A replicating adapter plus a direct upstream connection over the shared `coll_t` fixture.
struct Harness {
    db_name: String,
    upstream: mysql_async::Conn,
    rs: mysql_async::Conn,
    _handle: Handle,
    shutdown_tx: ShutdownSender,
}

/// Builds the `coll_t` fixture and a replicating adapter with fallback and out-of-band
/// migrations. With `streaming` the adapter starts before the DDL so the table and data flow
/// through the binlog; otherwise they are snapshotted.
async fn setup(test_name: &str, column_def: &str, rows: &str, streaming: bool) -> Harness {
    setup_with_parsing_preset(
        test_name,
        column_def,
        rows,
        streaming,
        ParsingPreset::for_tests(),
    )
    .await
}

async fn setup_with_parsing_preset(
    test_name: &str,
    column_def: &str,
    rows: &str,
    streaming: bool,
    parsing_preset: ParsingPreset,
) -> Harness {
    readyset_tracing::init_test_logging();
    let db_name = format!("collation_{test_name}");
    mysql_helpers::recreate_database(&db_name).await;

    let upstream_opts = mysql_helpers::upstream_config().db_name(Some(&db_name));
    let mut upstream = mysql_async::Conn::new(upstream_opts).await.unwrap();

    let create = format!("CREATE TABLE coll_t (id INT NOT NULL PRIMARY KEY, t {column_def})");
    let insert = format!("INSERT INTO coll_t (id, t) VALUES {rows}");

    if !streaming {
        upstream.query_drop(&create).await.unwrap();
        upstream.query_drop(&insert).await.unwrap();
    }

    let (rs_opts, handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .replicate_db(&db_name)
        .fallback(true)
        .migration_mode(MigrationMode::OutOfBand)
        .parsing_preset(parsing_preset)
        .build::<MySQLAdapter>()
        .await;
    let mut rs = mysql_async::Conn::new(rs_opts).await.unwrap();

    if streaming {
        upstream.query_drop(&create).await.unwrap();
        upstream.query_drop(&insert).await.unwrap();
    }

    // With fallback an uncached count would proxy upstream and prove nothing, so gate on a
    // cached count instead. CREATE CACHE succeeds once the table has replicated, and the
    // cached count reaches the upstream's once the rows have.
    let expected: usize = upstream
        .query_first("SELECT COUNT(*) FROM coll_t")
        .await
        .unwrap()
        .unwrap();
    eventually!(attempts: 30, sleep: Duration::from_secs(1), {
        rs.query_drop("CREATE CACHE FROM SELECT COUNT(*) AS c FROM coll_t")
            .await
            .is_ok()
    });
    eventually!(attempts: 30, sleep: Duration::from_secs(1), {
        matches!(
            rs.query_first::<usize, _>("SELECT COUNT(*) AS c FROM coll_t").await,
            Ok(Some(count)) if count == expected
        )
    });

    Harness {
        db_name,
        upstream,
        rs,
        _handle: handle,
        shutdown_tx,
    }
}

impl Harness {
    async fn create_cache(&mut self, query: &str) {
        self.rs
            .query_drop(format!("CREATE CACHE FROM {query}"))
            .await
            .unwrap_or_else(|e| panic!("CREATE CACHE FROM {query} failed: {e}"));
    }

    /// Asserts the last statement on the Readyset connection was served from a noria cache.
    /// Runs EXPLAIN LAST STATEMENT, so nothing else may execute on the Readyset connection
    /// between the statement under test and this call.
    async fn assert_cached(&mut self, ctx: &str) {
        let destination = last_query_info(&mut self.rs).await.destination;
        assert_matches!(
            destination,
            QueryDestination::Readyset(_),
            "{ctx}: expected the query to be served from the cache"
        );
    }

    /// Runs the query on both connections, asserting the Readyset side was served from a
    /// noria cache, and returns the upstream and Readyset rows.
    async fn query_both<T>(&mut self, query: &str, ctx: &str) -> (Vec<T>, Vec<T>)
    where
        T: FromRow + Send + 'static,
    {
        let my: Vec<T> = self.upstream.query(query).await.unwrap();
        let rs: Vec<T> = self.rs.query(query).await.unwrap();
        self.assert_cached(&format!("{ctx}: {query}")).await;
        (my, rs)
    }

    /// Runs the query on both connections and asserts the sorted row sets match.
    async fn compare_sorted(&mut self, query: &str, ctx: &str) {
        let (mut my, mut rs) = self.query_both::<(i64, Option<String>)>(query, ctx).await;
        my.sort();
        rs.sort();
        assert_eq!(
            my, rs,
            "mysql (left) differed from readyset (right); {ctx}: {query}"
        );
    }

    /// Runs the ordered query on both connections and asserts the rows match in order.
    async fn compare_ordered(&mut self, query: &str, ctx: &str) {
        let (my, rs) = self.query_both::<(i64, Option<String>)>(query, ctx).await;
        assert_eq!(
            my, rs,
            "mysql (left) differed from readyset (right); {ctx}: {query}"
        );
    }

    /// Compares GROUP BY group sizes only. Which case or accent variant represents each group
    /// is nondeterministic under a case-insensitive collation, so the grouped value itself is
    /// not compared.
    async fn compare_group_counts(&mut self, ctx: &str) {
        const QUERY: &str = "SELECT t, COUNT(*) AS c FROM coll_t GROUP BY t";
        self.create_cache(QUERY).await;
        let (my, rs) = self.query_both::<(Option<String>, i64)>(QUERY, ctx).await;
        let counts = |rows: Vec<(Option<String>, i64)>| {
            let mut counts: Vec<i64> = rows.into_iter().map(|(_, c)| c).collect();
            counts.sort();
            counts
        };
        assert_eq!(
            counts(my),
            counts(rs),
            "mysql (left) differed from readyset (right); {ctx}: {QUERY}"
        );
    }

    async fn compare_count_distinct(&mut self, ctx: &str) {
        const QUERY: &str = "SELECT COUNT(DISTINCT t) AS c FROM coll_t";
        self.create_cache(QUERY).await;
        let (my, rs) = self.query_both::<i64>(QUERY, ctx).await;
        assert_eq!(
            my, rs,
            "mysql (left) differed from readyset (right); {ctx}: {QUERY}"
        );
    }

    /// MIN/MAX can tie across values that compare equal, so instead of comparing strings the
    /// upstream judges whether Readyset's result is collation-equal to its own. The subqueries
    /// anchor the comparison to the column so its collation wins coercibility.
    async fn compare_min_max(&mut self, ctx: &str) {
        const QUERY: &str = "SELECT MIN(t) AS mn, MAX(t) AS mx FROM coll_t";
        self.create_cache(QUERY).await;
        let (rs_min, rs_max): (Option<String>, Option<String>) =
            self.rs.query_first(QUERY).await.unwrap().unwrap();
        self.assert_cached(&format!("{ctx}: {QUERY}")).await;
        let (min_eq, max_eq): (i64, i64) = self
            .upstream
            .exec_first(
                "SELECT (SELECT MIN(t) FROM coll_t) <=> ?, (SELECT MAX(t) FROM coll_t) <=> ?",
                (rs_min.as_deref(), rs_max.as_deref()),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            (min_eq, max_eq),
            (1, 1),
            "{ctx}: readyset MIN/MAX ({rs_min:?}, {rs_max:?}) not collation-equal to upstream's"
        );
    }

    async fn teardown(mut self) {
        self.shutdown_tx.shutdown().await;
        self.upstream
            .query_drop(format!("DROP DATABASE {}", self.db_name))
            .await
            .unwrap();
    }
}

/// Point lookups, ordering, grouping, distinct counting, and MIN/MAX over the case rows.
async fn compare_case_ops(h: &mut Harness, ctx: &str) {
    h.create_cache("SELECT id, t FROM coll_t WHERE t = ?").await;
    for key in ["abc", "ABC", "zzz"] {
        h.compare_sorted(&format!("SELECT id, t FROM coll_t WHERE t = '{key}'"), ctx)
            .await;
    }
    h.create_cache("SELECT id, t FROM coll_t ORDER BY t, id")
        .await;
    h.compare_ordered("SELECT id, t FROM coll_t ORDER BY t, id", ctx)
        .await;
    h.compare_group_counts(ctx).await;
    h.compare_count_distinct(ctx).await;
    h.compare_min_max(ctx).await;
}

/// Point lookups, grouping, and distinct counting over the accent rows.
async fn compare_accent_ops(h: &mut Harness, ctx: &str) {
    h.create_cache("SELECT id, t FROM coll_t WHERE t = ?").await;
    for key in ["resume", "RÉSUMÉ"] {
        h.compare_sorted(&format!("SELECT id, t FROM coll_t WHERE t = '{key}'"), ctx)
            .await;
    }
    h.compare_group_counts(ctx).await;
    h.compare_count_distinct(ctx).await;
}

/// Point lookups and grouping over rows differing only in a trailing space.
async fn compare_padding_ops(h: &mut Harness, ctx: &str) {
    h.create_cache("SELECT id, t FROM coll_t WHERE t = ?").await;
    for key in ["abc", "abc ", "abc  "] {
        h.compare_sorted(&format!("SELECT id, t FROM coll_t WHERE t = '{key}'"), ctx)
            .await;
    }
    h.compare_group_counts(ctx).await;
}

/// LIKE prefix matches over the case and accent rows. A LIKE placeholder is not a supported
/// parameter position, so each pattern is cached as a literal.
async fn compare_like_ops(h: &mut Harness, ctx: &str) {
    for pattern in ["abc%", "ABC%", "résum%", "RESUM%"] {
        let query = format!("SELECT id, t FROM coll_t WHERE t LIKE '{pattern}'");
        h.create_cache(&query).await;
        h.compare_sorted(&query, ctx).await;
    }
}

macro_rules! collation_test {
    ($testname:ident, $ops:ident, $column_def:expr_2021, $rows:expr_2021, $streaming:expr_2021,
     [$($gate:tt)*] $(, $extra:meta)*) => {
        #[tokio::test(flavor = "multi_thread")]
        #[tags(serial, slow)]
        #[upstream($($gate)*)]
        $(#[$extra])*
        async fn $testname() {
            let mut h = setup(stringify!($testname), $column_def, $rows, $streaming).await;
            $ops(&mut h, stringify!($testname)).await;
            h.teardown().await;
        }
    };
}

/// The comparison-semantics matrix entry for one collation: a snapshot variant (DDL before the
/// adapter starts) and a streaming variant (DDL through the binlog).
macro_rules! collation_case_tests {
    ($name:ident, $column_def:expr_2021, [$($gate:tt)*] $(, $extra:meta)*) => {
        paste::paste! {
            collation_test!(
                [<test_ $name _case_snapshot>], compare_case_ops, $column_def, CASE_ROWS, false,
                [$($gate)*] $(, $extra)*
            );
            collation_test!(
                [<test_ $name _case_streaming>], compare_case_ops, $column_def, CASE_ROWS, true,
                [$($gate)*] $(, $extra)*
            );
        }
    };
}

collation_case_tests!(
    utf8mb4_0900_ai_ci,
    "VARCHAR(32) COLLATE 'utf8mb4_0900_ai_ci'",
    [mysql, modern]
);
collation_case_tests!(
    utf8mb4_0900_as_cs,
    "VARCHAR(32) COLLATE 'utf8mb4_0900_as_cs'",
    [mysql, modern]
);
collation_case_tests!(utf8mb4_bin, "VARCHAR(32) COLLATE 'utf8mb4_bin'", [mysql]);
collation_case_tests!(
    utf8mb4_general_ci,
    "VARCHAR(32) COLLATE 'utf8mb4_general_ci'",
    [mysql]
);
collation_case_tests!(
    utf8mb4_unicode_ci,
    "VARCHAR(32) COLLATE 'utf8mb4_unicode_ci'",
    [mysql]
);
collation_case_tests!(
    latin1_swedish_ci,
    "VARCHAR(32) COLLATE 'latin1_swedish_ci'",
    [mysql]
);
collation_case_tests!(
    latin1_general_ci,
    "VARCHAR(32) COLLATE 'latin1_general_ci'",
    [mysql]
);
collation_case_tests!(binary, "VARBINARY(32)", [mysql]);

collation_test!(
    test_utf8mb4_0900_ai_ci_accents,
    compare_accent_ops,
    "VARCHAR(32) COLLATE 'utf8mb4_0900_ai_ci'",
    ACCENT_ROWS,
    false,
    [mysql, modern]
);
collation_test!(
    test_utf8mb4_0900_as_cs_accents,
    compare_accent_ops,
    "VARCHAR(32) COLLATE 'utf8mb4_0900_as_cs'",
    ACCENT_ROWS,
    false,
    [mysql, modern]
);
collation_test!(
    test_utf8mb4_bin_accents,
    compare_accent_ops,
    "VARCHAR(32) COLLATE 'utf8mb4_bin'",
    ACCENT_ROWS,
    false,
    [mysql]
);
collation_test!(
    test_latin1_swedish_ci_accents,
    compare_accent_ops,
    "VARCHAR(32) COLLATE 'latin1_swedish_ci'",
    ACCENT_ROWS,
    false,
    [mysql]
);
collation_test!(
    test_utf8mb4_general_ci_accents,
    compare_accent_ops,
    "VARCHAR(32) COLLATE 'utf8mb4_general_ci'",
    ACCENT_ROWS,
    false,
    [mysql]
);
collation_test!(
    test_utf8mb4_unicode_ci_accents,
    compare_accent_ops,
    "VARCHAR(32) COLLATE 'utf8mb4_unicode_ci'",
    ACCENT_ROWS,
    false,
    [mysql]
);
// latin1_general_ci falls through the suffix heuristic (readyset-data/src/collation.rs) to
// Utf8Ci, but both are case-insensitive and accent-sensitive, so these operations agree.
collation_test!(
    test_latin1_general_ci_accents,
    compare_accent_ops,
    "VARCHAR(32) COLLATE 'latin1_general_ci'",
    ACCENT_ROWS,
    false,
    [mysql]
);

collation_test!(
    test_utf8mb4_0900_ai_ci_padding,
    compare_padding_ops,
    "VARCHAR(32) COLLATE 'utf8mb4_0900_ai_ci'",
    PAD_ROWS,
    false,
    [mysql, modern]
);
collation_test!(
    test_utf8mb4_0900_as_cs_padding,
    compare_padding_ops,
    "VARCHAR(32) COLLATE 'utf8mb4_0900_as_cs'",
    PAD_ROWS,
    false,
    [mysql, modern]
);
// Don't test utf8mb4_general_ci on 5.7.  In MySQL 5.7, SHOW CREATE TABLE omits the COLLATE
// clause when the collation is the charset default (which utf8mb4_general_ci is). The
// snapshot would then fall back to a non-padded collation, and thereafter lookups of padded
// values would fail.
collation_test!(
    test_utf8mb4_general_ci_padding,
    compare_padding_ops,
    "VARCHAR(32) COLLATE 'utf8mb4_general_ci'",
    PAD_ROWS,
    false,
    [mysql, modern]
);
collation_test!(
    test_utf8mb4_unicode_ci_padding,
    compare_padding_ops,
    "VARCHAR(32) COLLATE 'utf8mb4_unicode_ci'",
    PAD_ROWS,
    false,
    [mysql]
);
// MySQL 5.7's SHOW CREATE TABLE omits the charset and collation entirely when they match the
// table default, as latin1_swedish_ci does there. The snapshot then falls back to a collation
// that does not pad, so padded lookups miss rows on 5.7.
collation_test!(
    test_latin1_swedish_ci_padding,
    compare_padding_ops,
    "VARCHAR(32) COLLATE 'latin1_swedish_ci'",
    PAD_ROWS,
    false,
    [mysql, modern]
);
collation_test!(
    test_binary_padding,
    compare_padding_ops,
    "VARBINARY(32)",
    PAD_ROWS,
    false,
    [mysql]
);

collation_test!(
    test_utf8mb4_0900_as_cs_like,
    compare_like_ops,
    "VARCHAR(32) COLLATE 'utf8mb4_0900_as_cs'",
    LIKE_ROWS,
    false,
    [mysql, modern]
);
collation_test!(
    test_utf8mb4_bin_like,
    compare_like_ops,
    "VARCHAR(32) COLLATE 'utf8mb4_bin'",
    LIKE_ROWS,
    false,
    [mysql]
);
collation_test!(
    test_binary_like,
    compare_like_ops,
    "VARBINARY(32)",
    LIKE_ROWS,
    false,
    [mysql]
);

/// A COLLATE expression in the filter is not supported by the query graph, so the query cannot
/// be cached and proxies to the upstream, whose rows it returns. Uses the production parsing
/// preset because nom-sql cannot parse COLLATE expressions and the test preset panics when the
/// parsers diverge.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(mysql)]
async fn test_collate_expr_falls_back_to_upstream() {
    let mut h = setup_with_parsing_preset(
        "collate_expr_fallback",
        "VARCHAR(32) COLLATE 'utf8mb4_general_ci'",
        CASE_ROWS,
        false,
        ParsingPreset::for_prod(),
    )
    .await;

    const QUERY: &str = "SELECT id, t FROM coll_t WHERE t = 'abc' COLLATE utf8mb4_bin";
    let created = h.rs.query_drop(format!("CREATE CACHE FROM {QUERY}")).await;
    assert!(
        created.is_err(),
        "COLLATE expressions gained cache support; extend the comparison matrix to cover them"
    );

    let mut my: Vec<(i64, Option<String>)> = h.upstream.query(QUERY).await.unwrap();
    my.sort();
    let mut rs: Vec<(i64, Option<String>)> = h.rs.query(QUERY).await.unwrap();
    let destination = last_query_info(&mut h.rs).await.destination;
    rs.sort();
    assert_matches!(destination, QueryDestination::Upstream);
    assert_eq!(my, rs);

    h.teardown().await;
}

/// The collation ids of each column in the result-set metadata of the given query.
async fn column_charsets(conn: &mut mysql_async::Conn, query: &str) -> Vec<u16> {
    let result = conn.query_iter(query).await.unwrap();
    let charsets = result
        .columns_ref()
        .iter()
        .map(|c| c.character_set())
        .collect();
    drop(result);
    charsets
}

const COLL_META_DDL: &str = "CREATE TABLE coll_meta (
    id INT PRIMARY KEY,
    lat VARCHAR(32) CHARACTER SET latin1,
    bin_c VARCHAR(32) COLLATE utf8mb4_bin,
    txt VARCHAR(32),
    vb VARBINARY(16)
)";
const COLL_META_QUERY: &str = "SELECT id, lat, bin_c, txt, vb FROM coll_meta WHERE id = 1";

/// Result-set metadata reports each column's collation id. Proxied results mirror the
/// upstream's ids directly, and shallow fills and hits replay the upstream's wire metadata, so
/// all three must agree with a direct upstream connection.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(mysql)]
async fn test_column_def_collation_ids_proxied_and_shallow_match_upstream() {
    readyset_tracing::init_test_logging();
    let (opts, _handle, shutdown_tx) = TestBuilder::default()
        .fallback(true)
        .migration_mode(MigrationMode::OutOfBand)
        .build::<MySQLAdapter>()
        .await;
    let mut conn = mysql_async::Conn::new(opts.clone()).await.unwrap();
    conn.query_drop(COLL_META_DDL).await.unwrap();
    conn.query_drop("INSERT INTO coll_meta VALUES (1, 'a', 'b', 'c', 'd')")
        .await
        .unwrap();

    let upstream_opts = mysql_helpers::upstream_config().db_name(opts.db_name());
    let mut upstream_conn = mysql_async::Conn::new(upstream_opts).await.unwrap();
    let expected = column_charsets(&mut upstream_conn, COLL_META_QUERY).await;

    let proxied = column_charsets(&mut conn, COLL_META_QUERY).await;
    assert_matches!(
        last_query_info(&mut conn).await.destination,
        QueryDestination::Upstream
    );
    assert_eq!(proxied, expected);

    conn.query_drop(
        "CREATE SHALLOW CACHE FROM SELECT id, lat, bin_c, txt, vb FROM coll_meta WHERE id = ?",
    )
    .await
    .unwrap();

    // The fill passes through the upstream's metadata.
    let fill = column_charsets(&mut conn, COLL_META_QUERY).await;
    assert_matches!(
        last_query_info(&mut conn).await.destination,
        QueryDestination::ReadysetThenUpstream(..)
    );
    assert_eq!(fill, expected);

    // The hit replays the stored metadata.
    eventually!(run_test: {
        let charsets = column_charsets(&mut conn, COLL_META_QUERY).await;
        let info = last_query_info(&mut conn).await;
        AssertUnwindSafe(move || (info, charsets))
    }, then_assert: |result| {
        let (info, charsets) = result();
        assert_matches!(info.destination, QueryDestination::ReadysetShallow(..));
        assert_eq!(charsets, expected);
    });

    shutdown_tx.shutdown().await;
}

/// After SET NAMES with a COLLATE clause, a native MySQL session reports result-set metadata
/// in the named collation. The adapter must mirror that for proxied results, including for a
/// non-utf8mb4 charset, where the forwarded statement restores the upstream's
/// character_set_client to utf8mb4.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(mysql)]
async fn test_column_def_collation_ids_proxied_after_set_names_collate() {
    readyset_tracing::init_test_logging();
    let (opts, _handle, shutdown_tx) = TestBuilder::default()
        .fallback(true)
        .migration_mode(MigrationMode::OutOfBand)
        .build::<MySQLAdapter>()
        .await;
    let mut conn = mysql_async::Conn::new(opts.clone()).await.unwrap();
    conn.query_drop(COLL_META_DDL).await.unwrap();
    conn.query_drop("INSERT INTO coll_meta VALUES (1, 'a', 'b', 'c', 'd')")
        .await
        .unwrap();

    for set_names in [
        "SET NAMES utf8mb4 COLLATE utf8mb4_bin",
        "SET NAMES latin1 COLLATE latin1_bin",
    ] {
        let mut conn = mysql_async::Conn::new(opts.clone()).await.unwrap();
        conn.query_drop(set_names).await.unwrap();

        let upstream_opts = mysql_helpers::upstream_config().db_name(opts.db_name());
        let mut upstream_conn = mysql_async::Conn::new(upstream_opts).await.unwrap();
        upstream_conn.query_drop(set_names).await.unwrap();
        let expected = column_charsets(&mut upstream_conn, COLL_META_QUERY).await;

        let proxied = column_charsets(&mut conn, COLL_META_QUERY).await;
        assert_matches!(
            last_query_info(&mut conn).await.destination,
            QueryDestination::Upstream
        );
        assert_eq!(proxied, expected, "{set_names}");
    }

    shutdown_tx.shutdown().await;
}

/// A SET NAMES the upstream rejects must leave the session unchanged. The collation name
/// parses locally but does not exist upstream, so the forwarded statement fails and the
/// session must keep matching a native session that established latin1 and never attempted
/// the rejected statement.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(mysql)]
async fn test_set_names_rejected_upstream_leaves_encodings_unchanged() {
    readyset_tracing::init_test_logging();
    let (opts, _handle, shutdown_tx) = TestBuilder::default()
        .fallback(true)
        .migration_mode(MigrationMode::OutOfBand)
        .build::<MySQLAdapter>()
        .await;
    let mut conn = mysql_async::Conn::new(opts.clone()).await.unwrap();

    let upstream_opts = mysql_helpers::upstream_config().db_name(opts.db_name());
    let mut upstream_conn = mysql_async::Conn::new(upstream_opts).await.unwrap();
    upstream_conn.query_drop("SET NAMES latin1").await.unwrap();

    conn.query_drop("SET NAMES latin1").await.unwrap();
    conn.query_drop("SET NAMES utf8mb4 COLLATE utf8mb4_nonexistent")
        .await
        .unwrap_err();

    // 0xE1 is latin1 'a acute' and is not valid UTF-8, so this statement only decodes in a
    // session still at latin1.
    let probe: &[u8] = b"SELECT '\xE1' AS s";
    let expected: Option<Vec<u8>> = upstream_conn.query_first(probe).await.unwrap();
    let proxied: Option<Vec<u8>> = conn.query_first(probe).await.unwrap();
    assert_matches!(
        last_query_info(&mut conn).await.destination,
        QueryDestination::Upstream
    );
    assert_eq!(proxied, expected);

    shutdown_tx.shutdown().await;
}

/// Noria-cached result metadata must report the columns' collation ids the way the upstream
/// does for the same session. Both sessions SET NAMES to a non-default collation so the ids
/// cannot coincide by accident with a default mysql_async session's collation.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(mysql)]
async fn test_column_def_collation_ids_cached_match_upstream() {
    readyset_tracing::init_test_logging();
    let (opts, _handle, shutdown_tx) = TestBuilder::default()
        .fallback(true)
        .migration_mode(MigrationMode::OutOfBand)
        .build::<MySQLAdapter>()
        .await;
    let mut conn = mysql_async::Conn::new(opts.clone()).await.unwrap();
    conn.query_drop(COLL_META_DDL).await.unwrap();
    conn.query_drop("INSERT INTO coll_meta VALUES (1, 'a', 'b', 'c', 'd')")
        .await
        .unwrap();
    conn.query_drop("SET NAMES utf8mb4 COLLATE utf8mb4_bin")
        .await
        .unwrap();

    let upstream_opts = mysql_helpers::upstream_config().db_name(opts.db_name());
    let mut upstream_conn = mysql_async::Conn::new(upstream_opts).await.unwrap();
    upstream_conn
        .query_drop("SET NAMES utf8mb4 COLLATE utf8mb4_bin")
        .await
        .unwrap();
    let expected = column_charsets(&mut upstream_conn, COLL_META_QUERY).await;

    eventually! {
        conn.query_drop(
            "CREATE CACHE FROM SELECT id, lat, bin_c, txt, vb FROM coll_meta WHERE id = ?",
        )
        .await
        .is_ok()
    }

    eventually!(run_test: {
        let charsets = column_charsets(&mut conn, COLL_META_QUERY).await;
        let info = last_query_info(&mut conn).await;
        AssertUnwindSafe(move || (info, charsets))
    }, then_assert: |result| {
        let (info, charsets) = result();
        assert_matches!(info.destination, QueryDestination::Readyset(..));
        assert_eq!(charsets, expected);
    });

    shutdown_tx.shutdown().await;
}

/// SET NAMES with a COLLATE clause is forwarded so the upstream session reports the requested
/// collation_connection, and a cached point lookup on a case-insensitive column keeps matching
/// the upstream because the column's collation wins coercibility on both sides.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(mysql)]
async fn test_set_names_collate_forwarded_upstream() {
    readyset_tracing::init_test_logging();
    let (opts, _handle, shutdown_tx) = TestBuilder::default()
        .fallback(true)
        .migration_mode(MigrationMode::OutOfBand)
        .build::<MySQLAdapter>()
        .await;
    let mut conn = mysql_async::Conn::new(opts.clone()).await.unwrap();
    conn.query_drop("CREATE TABLE coll_names (id INT PRIMARY KEY, t VARCHAR(32))")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO coll_names (id, t) VALUES (1, 'abc'), (2, 'ABC')")
        .await
        .unwrap();

    conn.query_drop("SET NAMES utf8mb4 COLLATE utf8mb4_bin")
        .await
        .unwrap();
    let collation: String = conn
        .query_first("SELECT @@collation_connection")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(collation, "utf8mb4_bin");

    let upstream_opts = mysql_helpers::upstream_config().db_name(opts.db_name());
    let mut upstream_conn = mysql_async::Conn::new(upstream_opts).await.unwrap();
    upstream_conn
        .query_drop("SET NAMES utf8mb4 COLLATE utf8mb4_bin")
        .await
        .unwrap();

    eventually! {
        conn.query_drop("CREATE CACHE FROM SELECT id, t FROM coll_names WHERE t = ?")
            .await
            .is_ok()
    }

    const QUERY: &str = "SELECT id, t FROM coll_names WHERE t = 'ABC'";
    let mut expected: Vec<(i64, String)> = upstream_conn.query(QUERY).await.unwrap();
    expected.sort();
    eventually!(run_test: {
        let mut rows: Vec<(i64, String)> = conn.query(QUERY).await.unwrap();
        rows.sort();
        let info = last_query_info(&mut conn).await;
        AssertUnwindSafe(move || (info, rows))
    }, then_assert: |result| {
        let (info, rows) = result();
        assert_matches!(info.destination, QueryDestination::Readyset(..));
        assert_eq!(rows, expected);
    });

    shutdown_tx.shutdown().await;
}

/// SET collation_connection is forwarded verbatim to the upstream and the session keeps
/// serving from caches. This documents intended current behavior; no local collation
/// semantics are attached to the setting (TODO(mvzink) in readyset-mysql/src/query_handler.rs).
#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(mysql)]
async fn test_set_collation_connection_proxied_and_still_cached() {
    readyset_tracing::init_test_logging();
    let (opts, _handle, shutdown_tx) = TestBuilder::default()
        .fallback(true)
        .migration_mode(MigrationMode::OutOfBand)
        .build::<MySQLAdapter>()
        .await;
    let mut conn = mysql_async::Conn::new(opts.clone()).await.unwrap();
    conn.query_drop("CREATE TABLE coll_set (id INT PRIMARY KEY, t VARCHAR(32))")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO coll_set (id, t) VALUES (1, 'abc')")
        .await
        .unwrap();

    eventually! {
        conn.query_drop("CREATE CACHE FROM SELECT id, t FROM coll_set WHERE id = ?")
            .await
            .is_ok()
    }

    const QUERY: &str = "SELECT id, t FROM coll_set WHERE id = 1";
    eventually!(run_test: {
        let rows: Vec<(i64, String)> = conn.query(QUERY).await.unwrap();
        let info = last_query_info(&mut conn).await;
        AssertUnwindSafe(move || (info, rows))
    }, then_assert: |result| {
        let (info, rows) = result();
        assert_matches!(info.destination, QueryDestination::Readyset(..));
        assert_eq!(rows, vec![(1, "abc".to_string())]);
    });

    conn.query_drop("SET collation_connection = 'utf8mb4_bin'")
        .await
        .unwrap();
    let collation: String = conn
        .query_first("SELECT @@collation_connection")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(collation, "utf8mb4_bin");

    let rows: Vec<(i64, String)> = conn.query(QUERY).await.unwrap();
    assert_matches!(
        last_query_info(&mut conn).await.destination,
        QueryDestination::Readyset(..)
    );
    assert_eq!(rows, vec![(1, "abc".to_string())]);

    shutdown_tx.shutdown().await;
}

/// A minimal raw MySQL client for advertising a handshake collation byte mysql_async cannot
/// send (it always negotiates utf8mb4). Trimmed from the copy in encoding_mysql.rs; a third
/// copy should trigger extraction into readyset-client-test-helpers.
struct RawConn {
    stream: TcpStream,
}

impl RawConn {
    async fn read_packet(&mut self) -> (u8, Vec<u8>) {
        let mut header = [0u8; 4];
        self.stream.read_exact(&mut header).await.unwrap();
        let len = u32::from_le_bytes([header[0], header[1], header[2], 0]) as usize;
        let mut payload = vec![0u8; len];
        self.stream.read_exact(&mut payload).await.unwrap();
        (header[3], payload)
    }

    async fn write_packet(&mut self, seq: u8, payload: &[u8]) {
        let mut buf = Vec::with_capacity(4 + payload.len());
        buf.extend_from_slice(&(payload.len() as u32).to_le_bytes()[..3]);
        buf.push(seq);
        buf.extend_from_slice(payload);
        self.stream.write_all(&buf).await.unwrap();
    }

    /// Connect and complete a handshake advertising the given charset (a collation id).
    async fn connect_with_charset(opts: &mysql_async::Opts, charset: u8) -> Self {
        const CLIENT_CONNECT_WITH_DB: u32 = 0x0000_0008;
        const CLIENT_PROTOCOL_41: u32 = 0x0000_0200;
        const CLIENT_SECURE_CONNECTION: u32 = 0x0000_8000;
        const CLIENT_PLUGIN_AUTH: u32 = 0x0008_0000;

        let stream = TcpStream::connect((opts.ip_or_hostname(), opts.tcp_port()))
            .await
            .unwrap();
        let mut conn = RawConn { stream };
        let (seq, _server_handshake) = conn.read_packet().await;

        let mut capabilities = CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | CLIENT_PLUGIN_AUTH;
        if opts.db_name().is_some() {
            capabilities |= CLIENT_CONNECT_WITH_DB;
        }
        let mut payload = Vec::new();
        payload.extend_from_slice(&capabilities.to_le_bytes());
        payload.extend_from_slice(&(16u32 << 20).to_le_bytes()); // max packet size
        payload.push(charset);
        payload.extend_from_slice(&[0u8; 23]);
        payload.extend_from_slice(opts.user().unwrap_or("root").as_bytes());
        payload.push(0);
        // Authentication is disabled in the test harness; any non-empty scramble is accepted.
        payload.push(20);
        payload.extend_from_slice(&[1u8; 20]);
        if let Some(db) = opts.db_name() {
            payload.extend_from_slice(db.as_bytes());
            payload.push(0);
        }
        payload.extend_from_slice(b"mysql_native_password\0");
        conn.write_packet(seq + 1, &payload).await;

        let (_, response) = conn.read_packet().await;
        assert_eq!(
            response.first(),
            Some(&0x00),
            "handshake should succeed: {response:?}"
        );
        conn
    }

    /// Send a COM_QUERY with the given raw statement bytes and return the raw payloads of any
    /// result row packets (empty for an OK response).
    async fn query_raw(&mut self, statement: &[u8]) -> Vec<Vec<u8>> {
        let mut payload = Vec::with_capacity(statement.len() + 1);
        payload.push(0x03); // COM_QUERY
        payload.extend_from_slice(statement);
        self.write_packet(0, &payload).await;

        let (_, first) = self.read_packet().await;
        match first.first() {
            Some(0x00) => return Vec::new(),
            Some(0xFF) => panic!("query failed: {}", String::from_utf8_lossy(&first[9..])),
            _ => {}
        }
        // A result set: `first` holds the column count; column definitions follow, terminated
        // by EOF, then row packets, terminated by EOF.
        for _ in 0..first[0] {
            self.read_packet().await;
        }
        let (_, eof) = self.read_packet().await;
        assert_eq!(eof.first(), Some(&0xFE), "expected EOF after column defs");
        let mut rows = Vec::new();
        loop {
            let (_, packet) = self.read_packet().await;
            if packet.first() == Some(&0xFE) && packet.len() < 9 {
                return rows;
            }
            rows.push(packet);
        }
    }
}

/// Extract the value of a single-column text-protocol row, assuming a value under 251 bytes.
fn single_text_column(row: &[u8]) -> &[u8] {
    let len = row[0] as usize;
    assert_eq!(row.len(), len + 1, "expected a one-column row: {row:?}");
    &row[1..]
}

/// Extract the first column of a text-protocol row, assuming a value under 251 bytes.
fn first_text_column(row: &[u8]) -> &[u8] {
    let len = row[0] as usize;
    &row[1..1 + len]
}

/// The utf8mb4_bin collation id.
const UTF8MB4_BIN_COLLATION: u8 = 46;

/// A handshake advertising utf8mb4_bin must reach the upstream session, so a proxied
/// SELECT @@collation_connection reports it, matching a direct MySQL connection.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(mysql)]
async fn test_handshake_collation_byte_upstream_fidelity() {
    readyset_tracing::init_test_logging();
    let (opts, _handle, shutdown_tx) = TestBuilder::default()
        .fallback(true)
        .migration_mode(MigrationMode::OutOfBand)
        .build::<MySQLAdapter>()
        .await;

    let mut raw = RawConn::connect_with_charset(&opts, UTF8MB4_BIN_COLLATION).await;
    let rows = raw.query_raw(b"SELECT @@collation_connection").await;
    assert_eq!(rows.len(), 1);
    assert_eq!(single_text_column(&rows[0]), b"utf8mb4_bin");

    shutdown_tx.shutdown().await;
}

/// The utf8mb3_general_ci collation id.
const UTF8MB3_GENERAL_CI_COLLATION: u8 = 33;

/// A handshake collation byte of utf8mb3_general_ci converts supplementary characters in
/// results to '?' on both the cached and proxied paths, matching a direct MySQL connection.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(mysql)]
async fn test_handshake_utf8mb3_converts_results_on_both_paths() {
    readyset_tracing::init_test_logging();
    let (opts, _handle, shutdown_tx) = TestBuilder::default()
        .fallback(true)
        .migration_mode(MigrationMode::OutOfBand)
        .build::<MySQLAdapter>()
        .await;
    let mut conn = mysql_async::Conn::new(opts.clone()).await.unwrap();
    conn.query_drop(
        "CREATE TABLE mb3_handshake (id INT PRIMARY KEY, t VARCHAR(32) CHARACTER SET utf8mb4)",
    )
        .await
        .unwrap();
    // U+1F600 GRINNING FACE, a supplementary character utf8mb3 cannot represent.
    conn.query_drop("INSERT INTO mb3_handshake (id, t) VALUES (1, 'a\u{1F600}b')")
        .await
        .unwrap();

    // A direct connection with utf8mb3 results gets the supplementary character replaced
    // by '?'.
    let upstream_opts = mysql_helpers::upstream_config().db_name(opts.db_name());
    let mut upstream_conn = mysql_async::Conn::new(upstream_opts).await.unwrap();
    upstream_conn
        .query_drop("SET character_set_results = utf8mb3")
        .await
        .unwrap();
    let expected: Vec<u8> = upstream_conn
        .query_first("SELECT t FROM mb3_handshake WHERE id = 1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(expected, b"a?b");

    eventually! {
        conn.query_drop("CREATE CACHE FROM SELECT t FROM mb3_handshake WHERE id = ?")
            .await
            .is_ok()
    }

    let mut raw = RawConn::connect_with_charset(&opts, UTF8MB3_GENERAL_CI_COLLATION).await;

    // An uncached query proxies upstream, where the session results charset follows the
    // handshake.
    let rows = raw.query_raw(b"SELECT t FROM mb3_handshake").await;
    assert_eq!(rows.len(), 1);
    assert_eq!(single_text_column(&rows[0]), expected);

    // The cached query converts the same way once it serves from Readyset.
    const CACHED_QUERY: &[u8] = b"SELECT t FROM mb3_handshake WHERE id = 1";
    eventually!(run_test: {
        let rows = raw.query_raw(CACHED_QUERY).await;
        let explain = raw.query_raw(b"EXPLAIN LAST STATEMENT").await;
        let destination = first_text_column(&explain[0]).to_vec();
        AssertUnwindSafe(move || (destination, rows))
    }, then_assert: |result| {
        let (destination, rows) = result();
        let destination = QueryDestination::try_from(String::from_utf8(destination).unwrap())
            .expect("a parseable query destination");
        assert_matches!(destination, QueryDestination::Readyset(..));
        assert_eq!(rows.len(), 1);
        assert_eq!(single_text_column(&rows[0]), expected);
    });

    shutdown_tx.shutdown().await;
}

/// SET NAMES utf8mb3 converts supplementary characters in cached results to '?', matching a
/// direct MySQL connection.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(mysql)]
async fn test_set_names_utf8mb3_converts_cached_results() {
    readyset_tracing::init_test_logging();
    let (opts, _handle, shutdown_tx) = TestBuilder::default()
        .fallback(true)
        .migration_mode(MigrationMode::OutOfBand)
        .build::<MySQLAdapter>()
        .await;
    let mut conn = mysql_async::Conn::new(opts.clone()).await.unwrap();
    conn.query_drop(
        "CREATE TABLE mb3_names (id INT PRIMARY KEY, t VARCHAR(32) CHARACTER SET utf8mb4)",
    )
        .await
        .unwrap();
    // U+1F600 GRINNING FACE, inserted while the session is still utf8mb4.
    conn.query_drop("INSERT INTO mb3_names (id, t) VALUES (1, 'a\u{1F600}b')")
        .await
        .unwrap();

    let upstream_opts = mysql_helpers::upstream_config().db_name(opts.db_name());
    let mut upstream_conn = mysql_async::Conn::new(upstream_opts).await.unwrap();
    upstream_conn.query_drop("SET NAMES utf8mb3").await.unwrap();
    let expected: Vec<u8> = upstream_conn
        .query_first("SELECT t FROM mb3_names WHERE id = 1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(expected, b"a?b");

    conn.query_drop("SET NAMES utf8mb3").await.unwrap();

    eventually! {
        conn.query_drop("CREATE CACHE FROM SELECT t FROM mb3_names WHERE id = ?")
            .await
            .is_ok()
    }

    eventually!(run_test: {
        let rows: Vec<Vec<u8>> = conn.query("SELECT t FROM mb3_names WHERE id = 1").await.unwrap();
        let info = last_query_info(&mut conn).await;
        AssertUnwindSafe(move || (info, rows))
    }, then_assert: |result| {
        let (info, rows) = result();
        assert_matches!(info.destination, QueryDestination::Readyset(..));
        assert_eq!(rows, vec![expected.clone()]);
    });

    shutdown_tx.shutdown().await;
}

/// Builds an adapter with fallback, the `coll_shallow` fixture with the given column
/// collation, and a shallow point-lookup cache on the text column.
async fn setup_shallow(
    collation: &str,
) -> (
    mysql_async::Conn,
    mysql_async::Conn,
    Handle,
    ShutdownSender,
) {
    readyset_tracing::init_test_logging();
    let (opts, handle, shutdown_tx) = TestBuilder::default()
        .fallback(true)
        .build::<MySQLAdapter>()
        .await;
    let mut conn = mysql_async::Conn::new(opts.clone()).await.unwrap();
    conn.query_drop(format!(
        "CREATE TABLE coll_shallow (id INT PRIMARY KEY, t VARCHAR(32) COLLATE {collation})"
    ))
    .await
    .unwrap();
    conn.query_drop("INSERT INTO coll_shallow (id, t) VALUES (1, 'abc'), (2, 'ABC')")
        .await
        .unwrap();
    conn.query_drop("CREATE SHALLOW CACHE FROM SELECT id, t FROM coll_shallow WHERE t = ?")
        .await
        .unwrap();
    let upstream_opts = mysql_helpers::upstream_config().db_name(opts.db_name());
    let upstream_conn = mysql_async::Conn::new(upstream_opts).await.unwrap();
    (conn, upstream_conn, handle, shutdown_tx)
}

/// Text-protocol literals on a case-sensitive column must key distinct shallow entries.
/// After `WHERE t = 'abc'` fills, `WHERE t = 'ABC'` must return row 2 as the upstream does.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(mysql, modern)]
async fn test_shallow_case_distinct_literals_not_shared_on_cs_column() {
    let (mut conn, mut upstream_conn, _handle, shutdown_tx) =
        setup_shallow("utf8mb4_0900_as_cs").await;

    // Fill and confirm the 'abc' entry.
    let rows: Vec<(i64, String)> = conn
        .query("SELECT id, t FROM coll_shallow WHERE t = 'abc'")
        .await
        .unwrap();
    assert_eq!(rows, vec![(1, "abc".to_string())]);
    assert_matches!(
        last_query_info(&mut conn).await.destination,
        QueryDestination::ReadysetThenUpstream(..)
    );
    eventually!(run_test: {
        let rows: Vec<(i64, String)> = conn
            .query("SELECT id, t FROM coll_shallow WHERE t = 'abc'")
            .await
            .unwrap();
        let info = last_query_info(&mut conn).await;
        AssertUnwindSafe(move || (info, rows))
    }, then_assert: |result| {
        let (info, rows) = result();
        assert_matches!(info.destination, QueryDestination::ReadysetShallow(..));
        assert_eq!(rows, vec![(1, "abc".to_string())]);
    });

    // On the case-sensitive column the upstream returns only row 2. Whether Readyset serves
    // this as a miss or from a collation-aware entry is immaterial; the rows must match.
    let mut expected: Vec<(i64, String)> = upstream_conn
        .query("SELECT id, t FROM coll_shallow WHERE t = 'ABC'")
        .await
        .unwrap();
    expected.sort();
    assert_eq!(expected, vec![(2, "ABC".to_string())]);
    let mut rows: Vec<(i64, String)> = conn
        .query("SELECT id, t FROM coll_shallow WHERE t = 'ABC'")
        .await
        .unwrap();
    rows.sort();
    assert_eq!(rows, expected);

    shutdown_tx.shutdown().await;
}

/// Binary-protocol parameters keep shallow keys byte-distinct on a case-sensitive column, so
/// each parameter fills and then hits its own entry with the correct rows.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(mysql, modern)]
async fn test_shallow_prepared_params_correct_on_cs_column() {
    let (mut conn, _upstream_conn, _handle, shutdown_tx) =
        setup_shallow("utf8mb4_0900_as_cs").await;

    const QUERY: &str = "SELECT id, t FROM coll_shallow WHERE t = ?";
    let cases = [("abc", 1i64), ("ABC", 2i64)];

    for (param, id) in cases {
        let rows: Vec<(i64, String)> = conn.exec(QUERY, (param,)).await.unwrap();
        assert_eq!(rows, vec![(id, param.to_string())]);
        assert_matches!(
            last_query_info(&mut conn).await.destination,
            QueryDestination::ReadysetThenUpstream(..)
        );
    }

    for (param, id) in cases {
        eventually!(run_test: {
            let rows: Vec<(i64, String)> = conn.exec(QUERY, (param,)).await.unwrap();
            let info = last_query_info(&mut conn).await;
            AssertUnwindSafe(move || (info, rows))
        }, then_assert: |result| {
            let (info, rows) = result();
            assert_matches!(info.destination, QueryDestination::ReadysetShallow(..));
            assert_eq!(rows, vec![(id, param.to_string())]);
        });
    }

    shutdown_tx.shutdown().await;
}

/// A text-protocol fill and a binary-protocol execute with the same literal key disjoint
/// shallow entries today, because ByteArray parameters hash differently from
/// auto-parameterized text values. The execute misses and refills, and the rows must match the
/// upstream either way. A future key normalization could legitimately turn the miss into a
/// ReadysetShallow hit; the rows assertion is the part that must keep holding.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(mysql)]
async fn test_shallow_text_vs_prepared_key_disjoint() {
    let (mut conn, mut upstream_conn, _handle, shutdown_tx) =
        setup_shallow("utf8mb4_general_ci").await;

    // Fill and confirm the text-protocol entry.
    conn.query_drop("SELECT id, t FROM coll_shallow WHERE t = 'abc'")
        .await
        .unwrap();
    assert_matches!(
        last_query_info(&mut conn).await.destination,
        QueryDestination::ReadysetThenUpstream(..)
    );
    eventually!(run_test: {
        conn.query_drop("SELECT id, t FROM coll_shallow WHERE t = 'abc'")
            .await
            .unwrap();
        let info = last_query_info(&mut conn).await;
        AssertUnwindSafe(move || info)
    }, then_assert: |result| {
        let info = result();
        assert_matches!(info.destination, QueryDestination::ReadysetShallow(..));
    });

    // The binary-protocol execute keys its own entry, so it misses and refills.
    const QUERY: &str = "SELECT id, t FROM coll_shallow WHERE t = ?";
    let mut expected: Vec<(i64, String)> = upstream_conn.exec(QUERY, ("abc",)).await.unwrap();
    expected.sort();
    let mut rows: Vec<(i64, String)> = conn.exec(QUERY, ("abc",)).await.unwrap();
    rows.sort();
    assert_matches!(
        last_query_info(&mut conn).await.destination,
        QueryDestination::ReadysetThenUpstream(..)
    );
    assert_eq!(rows, expected);

    shutdown_tx.shutdown().await;
}
