use std::time::{Duration, Instant};

use antithesis_sdk::prelude::*;
use anyhow::Result;
use clap::{Args, Parser, Subcommand};
use failpoint_client::FailpointClient;
use mysql_async::prelude::*;
use mysql_async::{Conn, Pool};
use rand::RngExt as _;
use readyset_tracing::init_test_logging;
use readyset_util::{failpoints, scheduler_potentially_yield};
use serde_json::json;
use tracing::{debug, error, info, warn};

const TABLES: &[&str] = &["stress_a", "stress_b", "stress_c"];
const MAX_RETRY_SECS: u64 = 10;
const RETRY_SLEEP_MS: u64 = 50;

/// DDL operations with their relative weights for random selection.
#[derive(Debug, Clone, Copy)]
enum DdlOp {
    CreateTable,
    AlterAddColumn,
    AlterDropColumn,
    DropTable,
    AlterAddUnsupportedColumn,
    AlterDropUnsupportedColumn,
}

impl DdlOp {
    const WEIGHTED: &[(u32, DdlOp)] = &[
        (25, DdlOp::CreateTable),
        (25, DdlOp::AlterAddColumn),
        (25, DdlOp::AlterDropColumn),
        (15, DdlOp::DropTable),
        (5, DdlOp::AlterAddUnsupportedColumn),
        (5, DdlOp::AlterDropUnsupportedColumn),
    ];

    fn random(rng: &mut impl rand::Rng) -> Self {
        const TOTAL: u32 = 25 + 25 + 25 + 15 + 5 + 5;
        let roll: u32 = rng.random_range(0..TOTAL);
        let mut cumulative = 0;
        for &(weight, op) in Self::WEIGHTED {
            cumulative += weight;
            if roll < cumulative {
                return op;
            }
        }
        // Fallback — mathematically unreachable when TOTAL matches WEIGHTED sum
        *Self::WEIGHTED
            .last()
            .map(|(_, op)| op)
            .expect("WEIGHTED must not be empty")
    }

    fn name(self) -> &'static str {
        match self {
            DdlOp::CreateTable => "CREATE TABLE",
            DdlOp::AlterAddColumn => "ALTER TABLE ADD COLUMN",
            DdlOp::AlterDropColumn => "ALTER TABLE DROP COLUMN",
            DdlOp::DropTable => "DROP TABLE",
            DdlOp::AlterAddUnsupportedColumn => "ALTER TABLE ADD UNSUPPORTED COLUMN",
            DdlOp::AlterDropUnsupportedColumn => "ALTER TABLE DROP UNSUPPORTED COLUMN",
        }
    }
}

#[derive(Parser)]
#[command(name = "ddl-stress")]
struct Opts {
    #[command(flatten)]
    mysql: MysqlOpts,

    #[command(flatten)]
    readyset: ReadysetOpts,

    /// Duration in seconds (0 = unlimited)
    #[arg(long, env = "DDL_STRESS_DURATION", default_value_t = 300)]
    duration_secs: u64,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// One-time setup: create initial tables on upstream MySQL
    Setup,
    /// DDL workload against upstream MySQL
    Ddl,
    /// Query workload against Readyset
    Query,
    /// Periodically activate SSE failpoints on Readyset to test recovery
    Chaos,
    /// Binlog conversion errors deny one table without stopping replication
    CharsetDenial,
}

#[derive(Args, Clone)]
struct MysqlOpts {
    #[arg(long, env = "MYSQL_HOST", default_value = "mysql")]
    mysql_host: String,
    #[arg(long, env = "MYSQL_PORT", default_value_t = 3306)]
    mysql_port: u16,
    #[arg(long, env = "MYSQL_USER", default_value = "root")]
    mysql_user: String,
    #[arg(long, env = "MYSQL_PWD", default_value = "noria")]
    mysql_password: String,
    #[arg(long, env = "MYSQL_DB", default_value = "noria")]
    mysql_db: String,
}

#[derive(Args, Clone)]
struct ReadysetOpts {
    #[arg(long, env = "READYSET_HOST", default_value = "readyset")]
    readyset_host: String,
    #[arg(long, env = "READYSET_PORT", default_value_t = 3307)]
    readyset_port: u16,
    #[arg(long, env = "READYSET_HTTP_PORT", default_value_t = 6033)]
    readyset_http_port: u16,
}

impl MysqlOpts {
    fn to_mysql_opts(&self) -> mysql_async::Opts {
        mysql_async::OptsBuilder::default()
            .ip_or_hostname(&self.mysql_host)
            .tcp_port(self.mysql_port)
            .user(Some(&self.mysql_user))
            .pass(Some(&self.mysql_password))
            .db_name(Some(&self.mysql_db))
            .prefer_socket(false)
            .into()
    }

    fn to_readyset_opts(&self, rs: &ReadysetOpts) -> mysql_async::Opts {
        mysql_async::OptsBuilder::default()
            .ip_or_hostname(&rs.readyset_host)
            .tcp_port(rs.readyset_port)
            .user(Some(&self.mysql_user))
            .pass(Some(&self.mysql_password))
            .db_name(Some(&self.mysql_db))
            .prefer_socket(false)
            .into()
    }
}

fn duration_expired(duration_secs: u64, start: Instant) -> bool {
    duration_secs > 0 && start.elapsed() >= Duration::from_secs(duration_secs)
}

fn main() -> Result<()> {
    antithesis_init();
    init_test_logging();
    let opts = Opts::parse();
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        match opts.command {
            Command::Setup => run_setup(&opts.mysql).await,
            Command::Ddl => run_ddl(&opts.mysql, opts.duration_secs).await,
            Command::Query => run_query(&opts.mysql, &opts.readyset, opts.duration_secs).await,
            Command::Chaos => run_chaos(&opts.readyset).await,
            Command::CharsetDenial => {
                run_charset_denial(&opts.mysql, &opts.readyset, opts.duration_secs).await
            }
        }
    })
}

async fn run_setup(mysql: &MysqlOpts) -> Result<()> {
    info!(
        host = %mysql.mysql_host,
        port = mysql.mysql_port,
        db = %mysql.mysql_db,
        "Connecting to MySQL"
    );
    let pool = Pool::new(mysql.to_mysql_opts());
    let mut conn = pool.get_conn().await?;
    info!("Connected to MySQL");

    assert_reachable!("Connected to upstream MySQL during setup", &json!({}));

    let mut all_ok = true;
    for &table in TABLES {
        let sql =
            format!("CREATE TABLE IF NOT EXISTS `{table}` (id INT PRIMARY KEY, val TEXT, num INT)");
        info!(table, "Creating table");
        if let Err(e) = conn.query_drop(&sql).await {
            warn!(%e, table, "Failed to create table");
            all_ok = false;
        }
    }

    assert_always!(all_ok, "Initial table creation succeeds", &json!({}));
    info!(all_ok, "Initial table setup complete");

    drop(conn);
    pool.disconnect().await?;
    Ok(())
}

async fn run_ddl(mysql: &MysqlOpts, duration_secs: u64) -> Result<()> {
    info!(
        host = %mysql.mysql_host,
        port = mysql.mysql_port,
        db = %mysql.mysql_db,
        duration_secs,
        "Starting DDL driver"
    );
    let pool = Pool::new(mysql.to_mysql_opts());
    let start = Instant::now();
    let mut rng = rand::rng();
    let mut iteration: u64 = 0;

    while !duration_expired(duration_secs, start) {
        // Optionally yield to avoid busy-spinning, but go as fast as possible
        scheduler_potentially_yield!();

        iteration += 1;
        let mut conn = match pool.get_conn().await {
            Ok(c) => c,
            Err(e) => {
                warn!(%e, "Failed to acquire MySQL connection");
                continue;
            }
        };

        let table = TABLES[rng.random_range(0..TABLES.len())];
        let op = DdlOp::random(&mut rng);
        let op_name = op.name();

        debug!(iteration, op_name, table, "Executing DDL");

        let result = match op {
            DdlOp::CreateTable => {
                let sql = format!(
                    "CREATE TABLE IF NOT EXISTS `{table}` (id INT PRIMARY KEY, val TEXT, num INT)"
                );
                let r = conn.query_drop(&sql).await;
                if r.is_ok() {
                    assert_reachable!("Executed CREATE TABLE", &json!({}));
                }
                r
            }
            DdlOp::AlterAddColumn => {
                let col_suffix: u32 = rng.random_range(0..20);
                let sql = format!("ALTER TABLE `{table}` ADD COLUMN `extra_{col_suffix}` INT");
                let r = conn.query_drop(&sql).await;
                if r.is_ok() {
                    assert_reachable!("Executed ALTER TABLE ADD COLUMN", &json!({}));
                }
                r
            }
            DdlOp::AlterDropColumn => {
                let col = find_extra_column(&mut conn, &mysql.mysql_db, table).await;
                if let Some(col_name) = col {
                    let sql = format!("ALTER TABLE `{table}` DROP COLUMN `{col_name}`");
                    let r = conn.query_drop(&sql).await;
                    if r.is_ok() {
                        assert_reachable!("Executed ALTER TABLE DROP COLUMN", &json!({}));
                    }
                    r
                } else {
                    debug!(table, "No extra_ columns to drop");
                    Ok(())
                }
            }
            DdlOp::DropTable => {
                let sql = format!("DROP TABLE IF EXISTS `{table}`");
                let r = conn.query_drop(&sql).await;
                if r.is_ok() {
                    assert_reachable!("Executed DROP TABLE", &json!({}));
                }
                r
            }
            DdlOp::AlterAddUnsupportedColumn => {
                let col_suffix: u32 = rng.random_range(0..5);
                let sql = format!(
                    "ALTER TABLE `{table}` ADD COLUMN `unsupported_{col_suffix}` LINESTRING"
                );
                let r = conn.query_drop(&sql).await;
                if r.is_ok() {
                    assert_reachable!(
                        "Executed ALTER TABLE ADD unsupported type column",
                        &json!({"table": table})
                    );
                }
                r
            }
            DdlOp::AlterDropUnsupportedColumn => {
                let col = find_unsupported_column(&mut conn, &mysql.mysql_db, table).await;
                if let Some(col_name) = col {
                    let sql = format!("ALTER TABLE `{table}` DROP COLUMN `{col_name}`");
                    let r = conn.query_drop(&sql).await;
                    if r.is_ok() {
                        assert_reachable!(
                            "Executed ALTER TABLE DROP unsupported type column",
                            &json!({"table": table})
                        );
                    }
                    r
                } else {
                    debug!(table, "No unsupported_ columns to drop");
                    Ok(())
                }
            }
        };

        match result {
            Ok(()) => {
                info!(iteration, op_name, table, "DDL succeeded");
                assert_reachable!("Successfully executed DDL operation", &json!({}));
            }
            Err(e) => {
                info!(%e, iteration, op_name, table, "DDL failed (expected)");
            }
        }
    }

    assert_sometimes!(
        iteration > 0,
        "DDL driver completed at least one iteration",
        &json!({"iterations": iteration})
    );

    info!(
        iterations = iteration,
        elapsed_secs = start.elapsed().as_secs(),
        "DDL driver finished"
    );
    pool.disconnect().await?;
    Ok(())
}

async fn find_extra_column(conn: &mut Conn, db: &str, table: &str) -> Option<String> {
    let sql = format!(
        "SELECT COLUMN_NAME FROM information_schema.COLUMNS \
         WHERE TABLE_SCHEMA = '{db}' AND TABLE_NAME = '{table}' \
         AND COLUMN_NAME LIKE 'extra_%' ORDER BY RAND() LIMIT 1"
    );
    match conn.query_first::<String, _>(&sql).await {
        Ok(col) => col,
        Err(e) => {
            info!(%e, table, "Failed to query information_schema");
            None
        }
    }
}

async fn find_unsupported_column(conn: &mut Conn, db: &str, table: &str) -> Option<String> {
    let sql = format!(
        "SELECT COLUMN_NAME FROM information_schema.COLUMNS \
         WHERE TABLE_SCHEMA = '{db}' AND TABLE_NAME = '{table}' \
         AND COLUMN_NAME LIKE 'unsupported_%' ORDER BY RAND() LIMIT 1"
    );
    match conn.query_first::<String, _>(&sql).await {
        Ok(col) => col,
        Err(e) => {
            info!(%e, table, "Failed to query information_schema");
            None
        }
    }
}

async fn run_query(mysql: &MysqlOpts, readyset: &ReadysetOpts, duration_secs: u64) -> Result<()> {
    info!(
        host = %readyset.readyset_host,
        port = readyset.readyset_port,
        db = %mysql.mysql_db,
        duration_secs,
        "Starting query driver"
    );
    let pool = Pool::new(mysql.to_readyset_opts(readyset));

    // Verify we're connected to Readyset, not upstream MySQL
    info!("Verifying Readyset connectivity");
    let mut conn = pool.get_conn().await?;
    match conn.query_drop("SHOW READYSET VERSION").await {
        Ok(()) => info!("Connected to Readyset"),
        Err(e) => {
            anyhow::bail!(
                "Failed to verify Readyset connectivity (is {}:{} actually Readyset?): {e}",
                readyset.readyset_host,
                readyset.readyset_port,
            );
        }
    }
    drop(conn);

    let start = Instant::now();
    let mut rng = rand::rng();
    let mut cycle: u64 = 0;

    while !duration_expired(duration_secs, start) {
        cycle += 1;

        // Phase 1: Trigger cache creation via a mix of explicit CREATE CACHE
        // and SELECT queries (which create caches implicitly via inRequestPath).
        info!(cycle, "Creating caches");
        for &table in TABLES {
            if duration_expired(duration_secs, start) {
                break;
            }

            // Alternate between explicit CREATE CACHE and implicit cache
            // creation via SELECT to exercise both code paths.
            let use_explicit = rng.random_bool(0.5);
            let sql = if use_explicit {
                format!("CREATE CACHE cache_{table} FROM SELECT * FROM {table} WHERE id = 1")
            } else {
                format!("SELECT * FROM {table} WHERE id = 1")
            };
            let method = if use_explicit {
                "CREATE CACHE"
            } else {
                "SELECT (implicit)"
            };

            match retry_on_schema_mismatch(&pool, &sql).await {
                Ok(()) => {
                    info!(cycle, table, method, "Cache creation succeeded");
                    assert_reachable!("Successfully created cache", &json!({}));
                    assert_reachable!("Connected to Readyset", &json!({}));
                }
                Err(e) => {
                    warn!(%e, cycle, table, method, "Cache creation failed");
                }
            }
        }

        // Phase 1b: EXPLAIN consistency check — verify that if EXPLAIN says a
        // query is supported, CREATE CACHE actually succeeds. A stale EXPLAIN
        // result (reporting "yes" when the table now has unsupported columns)
        // is the exact bug REA-6108 fixes.
        let mut explain_conn = match pool.get_conn().await {
            Ok(c) => c,
            Err(e) => {
                warn!(%e, "Failed to acquire Readyset connection for EXPLAIN check");
                continue;
            }
        };
        // Drop all caches first so EXPLAIN can return "yes" (supported but not
        // yet cached) instead of "cached". Without this, Phase 1's cache
        // creation makes every EXPLAIN return "cached", so the consistency
        // check never actually tests anything.
        let _ = explain_conn.query_drop("DROP ALL CACHES").await;

        for &table in TABLES {
            if duration_expired(duration_secs, start) {
                break;
            }

            let explain_sql =
                format!("EXPLAIN CREATE CACHE FROM SELECT * FROM {table} WHERE id = 1");
            let explain_result: Result<Option<(String, String, String)>, _> =
                explain_conn.query_first(&explain_sql).await;

            match explain_result {
                Ok(Some((_query_id, readyset_supported, _query_text))) => {
                    let supported_lower = readyset_supported.to_lowercase();
                    // Only follow up on "yes" — the status where staleness
                    // actually matters. "cached" means a cache already exists
                    // (CREATE CACHE would fail with "cache already exists",
                    // not staleness). "pending" means the cache is in-flight
                    // and could hit transient states unrelated to staleness.
                    if supported_lower == "yes" {
                        assert_reachable!(
                            "EXPLAIN returned 'yes' during consistency check",
                            &json!({"table": table, "status": readyset_supported})
                        );

                        // Follow up: if EXPLAIN says supported, CREATE CACHE
                        // should not fail with "unsupported type".
                        // Use explain_conn directly instead of retry_on_schema_mismatch,
                        // because is_expected_error() treats "unsupported type" as
                        // expected and returns Ok — which would mask the exact error
                        // this consistency check is trying to detect.
                        let create_sql =
                            format!("CREATE CACHE FROM SELECT * FROM {table} WHERE id = 1");
                        match explain_conn.query_drop(&create_sql).await {
                            Ok(()) => {
                                assert_reachable!(
                                    "EXPLAIN 'yes' confirmed by successful CREATE CACHE",
                                    &json!({"table": table, "status": readyset_supported})
                                );
                            }
                            Err(create_err) => {
                                let create_msg = create_err.to_string().to_lowercase();
                                // Only flag unsupported-type errors as stale EXPLAIN.
                                // Other failures (table dropped, etc.) are concurrent
                                // DDL races, not catalog staleness.
                                let is_stale = create_msg.contains("unsupported type");

                                if is_stale {
                                    // Re-run EXPLAIN to distinguish genuine staleness
                                    // from a TOCTOU race with concurrent DDL. If the
                                    // second EXPLAIN still says "yes", the catalog is
                                    // genuinely stale. If it now says unsupported, the
                                    // schema changed between our first EXPLAIN and the
                                    // CREATE CACHE — a legitimate race, not a bug.
                                    let recheck: Result<Option<(String, String, String)>, _> =
                                        explain_conn.query_first(&explain_sql).await;
                                    let still_stale = match recheck {
                                        Ok(Some((_, status, _))) => {
                                            let s = status.to_lowercase();
                                            s == "yes"
                                        }
                                        // If the recheck fails or returns no rows,
                                        // the table was likely dropped — not staleness.
                                        _ => false,
                                    };

                                    if still_stale {
                                        error!(
                                            %create_err,
                                            table,
                                            explain_status = %readyset_supported,
                                            "Stale EXPLAIN detected"
                                        );
                                        assert_unreachable!(
                                            "EXPLAIN CREATE CACHE must not report stale 'yes'",
                                            &json!({
                                                "table": table,
                                                "explain_status": readyset_supported,
                                                "create_error": create_err.to_string()
                                            })
                                        );
                                    }
                                }
                            }
                        }
                    } else {
                        // EXPLAIN said unsupported, cached, pending, etc. —
                        // confirms the DDL paths are observable.
                        assert_reachable!(
                            "EXPLAIN returned non-yes status after DDL",
                            &json!({"table": table, "status": readyset_supported})
                        );
                    }
                }
                Ok(None) => {
                    debug!(table, "EXPLAIN returned no rows (table may not exist)");
                }
                Err(e) => {
                    // Table may have been dropped by DDL driver — expected.
                    debug!(%e, table, "EXPLAIN query failed (expected during stress)");
                }
            }
        }

        // Phase 2: Drop all caches
        info!(cycle, "Dropping caches");
        for &table in TABLES {
            if duration_expired(duration_secs, start) {
                break;
            }
            let sql = format!("DROP CACHE cache_{table}");
            match retry_on_schema_mismatch(&pool, &sql).await {
                Ok(()) => {
                    info!(cycle, table, "Cache dropped");
                    assert_reachable!("Successfully dropped cache", &json!({}));
                }
                Err(e) => {
                    // DROP CACHE for a nonexistent cache is expected when the
                    // table was dropped by DDL or the implicit SELECT path was
                    // used (no named cache to drop).
                    info!(%e, cycle, table, "Drop cache failed (expected)");
                }
            }
        }

        info!(cycle, "Cycle complete");
    }

    assert_sometimes!(
        cycle > 0,
        "Query driver completed at least one cycle",
        &json!({"cycles": cycle})
    );

    info!(
        cycles = cycle,
        elapsed_secs = start.elapsed().as_secs(),
        "Query driver finished"
    );
    pool.disconnect().await?;
    Ok(())
}

async fn retry_on_schema_mismatch(pool: &Pool, sql: &str) -> Result<()> {
    let mut mismatch_start: Option<Instant> = None;
    let mut saw_mismatch = false;
    loop {
        let mut conn = pool.get_conn().await?;

        match conn.query_drop(sql).await {
            Ok(()) => {
                if saw_mismatch {
                    let elapsed_ms = mismatch_start.unwrap().elapsed().as_millis();
                    info!(elapsed_ms, sql, "Schema mismatch resolved after retries");
                    assert_reachable!(
                        "Schema mismatch resolved within retry window",
                        &json!({"elapsed_ms": elapsed_ms, "sql": sql})
                    );
                }
                return Ok(());
            }
            Err(e) => {
                let msg = e.to_string();
                let is_schema_mismatch = msg.contains("Schema generation mismatch");
                let is_leader_not_ready = msg.contains("The leader is not ready");

                if is_schema_mismatch || is_leader_not_ready {
                    if is_leader_not_ready {
                        // Don't count time spent waiting for the leader toward
                        // the schema mismatch retry window. Re-snapshots can
                        // take arbitrarily long, and the first mismatch after
                        // the leader comes back is expected (we sent a stale
                        // generation before we could know about the DDL change
                        // that triggered the re-snapshot).
                        mismatch_start = None;
                    }
                    if is_schema_mismatch {
                        saw_mismatch = true;
                        mismatch_start.get_or_insert_with(Instant::now);
                        assert_reachable!("Encountered schema generation mismatch", &json!({}));
                    }
                    if mismatch_start
                        .is_some_and(|s| s.elapsed() >= Duration::from_secs(MAX_RETRY_SECS))
                    {
                        let elapsed_ms = mismatch_start.unwrap().elapsed().as_millis();
                        assert_unreachable!(
                            "Schema mismatch retry timed out",
                            &json!({"elapsed_ms": elapsed_ms, "sql": sql})
                        );
                        anyhow::bail!(
                            "Schema generation mismatch not resolved within {MAX_RETRY_SECS}s for: {sql}"
                        );
                    }
                    debug!(
                        elapsed_ms = mismatch_start.map(|s| s.elapsed().as_millis()),
                        sql,
                        is_schema_mismatch,
                        is_leader_not_ready,
                        "Retryable error, retrying in {RETRY_SLEEP_MS}ms"
                    );
                    tokio::time::sleep(Duration::from_millis(RETRY_SLEEP_MS)).await;
                } else if is_expected_error(&msg) {
                    info!(%e, sql, "SQL error (expected, non-mismatch)");
                    return Ok(());
                } else {
                    warn!(%e, sql, "SQL error (unexpected, non-mismatch)");
                    assert_unreachable!(
                        "Unexpected SQL error in retry_on_schema_mismatch",
                        &json!({"sql": sql, "error": msg})
                    );
                    anyhow::bail!("Unexpected SQL error for {sql}: {msg}");
                }
            }
        }
    }
}

/// Multibyte charsets with no conversion table. A streamed binlog write to a column in any
/// of these fails row conversion in the MySQL replicator.
const DENIAL_CHARSETS: &[&str] = &[
    "gbk", "sjis", "big5", "ujis", "euckr", "cp932", "eucjpms", "gb18030",
];
const DENIAL_TABLE: &str = "charset_denial_multibyte";
const DENIAL_SIBLING: &str = "charset_denial_sibling";
/// Polling budget for observing the denial or a replicated write. Under fault injection the
/// budget can expire without the observation; that ends the pass without any assertion.
const DENIAL_POLL_ATTEMPTS: u32 = 100;
const DENIAL_POLL_SLEEP_MS: u64 = 200;
/// The status and description SHOW READYSET ALL TABLES reports for a table removed through
/// the replicator's per-table denial path. Snapshot or DDL failures under fault injection
/// also mark tables not replicated but carry different descriptions, so matching on the
/// pair distinguishes a denial from fault-induced degradation.
const DENIAL_STATUS: &str = "Not replicated";
const DENIAL_DESCRIPTION: &str = "Table has been dropped.";

/// Exercises the binlog-conversion-error-isolates-table property. Each pass writes through
/// the upstream to a table whose column charset the replicator cannot convert and to a
/// utf8mb4 sibling, waits for SHOW READYSET ALL TABLES to report the denial, then checks
/// that the denial is scoped to the one table, that reads of the denied table match the
/// upstream, and that the sibling's writes still arrive through replication.
async fn run_charset_denial(
    mysql: &MysqlOpts,
    readyset: &ReadysetOpts,
    duration_secs: u64,
) -> Result<()> {
    info!(
        host = %readyset.readyset_host,
        port = readyset.readyset_port,
        db = %mysql.mysql_db,
        duration_secs,
        "Starting charset denial driver"
    );
    let upstream_pool = Pool::new(mysql.to_mysql_opts());
    let rs_pool = Pool::new(mysql.to_readyset_opts(readyset));
    let start = Instant::now();
    let mut rng = rand::rng();
    let mut pass: u64 = 0;
    let mut denials_observed: u64 = 0;

    while !duration_expired(duration_secs, start) {
        scheduler_potentially_yield!();
        pass += 1;

        // Menu axis: the unsupported charset for this pass's table, and the streamed batch
        // size (single row plus small batches; the conversion error fires per row event, so
        // larger batches add nothing).
        let charset = DENIAL_CHARSETS[rng.random_range(0..DENIAL_CHARSETS.len())];
        let batch = [1usize, 2, 16][rng.random_range(0..3)];

        let mut upstream = match upstream_pool.get_conn().await {
            Ok(c) => c,
            Err(e) => {
                warn!(%e, "Failed to acquire upstream MySQL connection");
                continue;
            }
        };

        // The denial persists in the replicator's in-memory table filter until Readyset
        // restarts, so fresh denials come from fault-injected restarts followed by a
        // re-snapshot. Occasionally drop the multibyte table so the upstream definition
        // varies its charset for whichever re-snapshot happens next.
        if rng.random_bool(0.3)
            && let Err(e) = upstream
                .query_drop(format!("DROP TABLE IF EXISTS `{DENIAL_TABLE}`"))
                .await
        {
            info!(%e, "Dropping multibyte table failed");
        }
        for sql in [
            format!(
                "CREATE TABLE IF NOT EXISTS `{DENIAL_SIBLING}` \
                 (id INT PRIMARY KEY, t VARCHAR(32)) CHARACTER SET utf8mb4"
            ),
            format!(
                "CREATE TABLE IF NOT EXISTS `{DENIAL_TABLE}` \
                 (id INT PRIMARY KEY, t VARCHAR(32) CHARACTER SET {charset})"
            ),
        ] {
            if let Err(e) = upstream.query_drop(&sql).await {
                info!(%e, sql, "Charset denial setup DDL failed");
            }
        }

        // Streamed writes through the upstream. REPLACE tolerates id collisions across
        // passes. Values are pure ASCII; the conversion error fires regardless of content
        // because the charset has no decode at all.
        let base: u32 = rng.random_range(0..1_000_000);
        for i in 0..batch {
            let id = base.wrapping_add(i as u32);
            for table in [DENIAL_TABLE, DENIAL_SIBLING] {
                if let Err(e) = upstream
                    .query_drop(format!(
                        "REPLACE INTO `{table}` (id, t) VALUES ({id}, 'row-{id}')"
                    ))
                    .await
                {
                    info!(%e, table, id, "Streamed write failed");
                }
            }
        }

        // Wait for the denial to become visible through the adapter. Matching both the
        // status and the denial-path description avoids counting fault-induced snapshot or
        // DDL failures as denials. The workload's own DROP TABLE also produces this
        // signature transiently until the following CREATE is processed, so require it on
        // two consecutive polls; a real denial is stable until the replicator restarts.
        let mut denied = false;
        let mut sibling_state: Option<(String, String)> = None;
        let mut seen_once = false;
        for _ in 0..DENIAL_POLL_ATTEMPTS {
            if let Some((multibyte, sibling)) = table_statuses(&rs_pool).await
                && multibyte.as_ref().is_some_and(|(status, description)| {
                    status == DENIAL_STATUS && description == DENIAL_DESCRIPTION
                })
            {
                if seen_once {
                    denied = true;
                    sibling_state = sibling;
                    break;
                }
                seen_once = true;
            } else {
                seen_once = false;
            }
            tokio::time::sleep(Duration::from_millis(DENIAL_POLL_SLEEP_MS)).await;
        }

        if !denied {
            // Under faults the denial may not become observable within the budget. Not a
            // bug, just an unexercised pass.
            info!(pass, charset, "Denial not observed this pass");
            continue;
        }
        denials_observed += 1;

        info!(
            pass,
            charset,
            ?sibling_state,
            "Observed charset table denial"
        );
        assert_reachable!(
            "Charset denial scenario saw table become not replicated",
            &json!({"charset": charset, "pass": pass})
        );

        // Isolation: at the moment the denial is visible, the sibling must not have been
        // denied through the same path. A sibling marked not replicated with a different
        // description was degraded by fault injection (e.g. a snapshot failure), which is
        // outside this property.
        if let Some((status, description)) = &sibling_state {
            let sibling_denied = status == DENIAL_STATUS && description == DENIAL_DESCRIPTION;
            if status == DENIAL_STATUS && !sibling_denied {
                info!(
                    status,
                    description, "Sibling not replicated for a non-denial reason; skipping"
                );
            } else {
                assert_always_or_unreachable!(
                    !sibling_denied,
                    "Sibling table remains replicated after charset denial",
                    &json!({
                        "sibling_status": status,
                        "sibling_description": description,
                        "charset": charset,
                    })
                );
            }
        }

        // Fallback correctness: reads of the denied table through Readyset must serve the
        // upstream's rows, never the stale pre-denial snapshot. This driver is the only
        // writer to these tables and runs serially, so back-to-back reads can't race a
        // concurrent write.
        let read_sql = format!("SELECT id, t FROM `{DENIAL_TABLE}` ORDER BY id");
        let rs_rows: Option<Vec<(u32, String)>> = match rs_pool.get_conn().await {
            Ok(mut c) => c.query(&read_sql).await.ok(),
            Err(_) => None,
        };
        let upstream_rows: Option<Vec<(u32, String)>> = upstream.query(&read_sql).await.ok();
        if let (Some(rs_rows), Some(upstream_rows)) = (rs_rows, upstream_rows) {
            assert_always_or_unreachable!(
                rs_rows == upstream_rows,
                "Denied charset table reads match upstream",
                &json!({
                    "charset": charset,
                    "readyset_rows": rs_rows.len(),
                    "upstream_rows": upstream_rows.len(),
                })
            );
        }

        // Liveness: a fresh sibling write still arrives through replication. Require the
        // read to be served by Readyset itself, since a proxied read would see the row
        // whether or not replication made progress. The value carries the pass number so a
        // row left by an id collision with an earlier pass doesn't count.
        let live_id = base.wrapping_add(1_000_003);
        let live_value = format!("live-{pass}-{live_id}");
        if let Err(e) = upstream
            .query_drop(format!(
                "REPLACE INTO `{DENIAL_SIBLING}` (id, t) VALUES ({live_id}, '{live_value}')"
            ))
            .await
        {
            info!(%e, live_id, "Post-denial sibling write failed");
            continue;
        }
        let mut sibling_visible = false;
        for _ in 0..DENIAL_POLL_ATTEMPTS {
            if sibling_row_served_by_readyset(&rs_pool, live_id, &live_value).await {
                sibling_visible = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(DENIAL_POLL_SLEEP_MS)).await;
        }
        assert_sometimes!(
            sibling_visible,
            "Sibling write visible through Readyset after charset denial",
            &json!({"charset": charset, "live_id": live_id})
        );
        info!(
            pass,
            charset, sibling_visible, "Charset denial pass complete"
        );
    }

    assert_reachable!(
        "Charset denial driver completed",
        &json!({"passes": pass, "denials_observed": denials_observed})
    );

    info!(
        passes = pass,
        denials_observed,
        elapsed_secs = start.elapsed().as_secs(),
        "Charset denial driver finished"
    );
    upstream_pool.disconnect().await?;
    rs_pool.disconnect().await?;
    Ok(())
}

/// The SHOW READYSET ALL TABLES (status, description) of the two charset denial tables, as
/// (multibyte, sibling). None means the statement itself failed; an inner None means the
/// table is absent from the output.
#[allow(clippy::type_complexity)]
async fn table_statuses(
    rs_pool: &Pool,
) -> Option<(Option<(String, String)>, Option<(String, String)>)> {
    let mut conn = match rs_pool.get_conn().await {
        Ok(c) => c,
        Err(e) => {
            info!(%e, "Failed to acquire Readyset connection for table statuses");
            return None;
        }
    };
    let rows: Vec<(String, String, String)> = match conn.query("SHOW READYSET ALL TABLES").await {
        Ok(rows) => rows,
        Err(e) => {
            info!(%e, "SHOW READYSET ALL TABLES failed");
            return None;
        }
    };
    let state_of = |name: &str| {
        rows.iter()
            .find(|(table, _, _)| table.contains(name))
            .map(|(_, status, description)| (status.clone(), description.clone()))
    };
    Some((state_of(DENIAL_TABLE), state_of(DENIAL_SIBLING)))
}

/// Whether the sibling row is visible through Readyset with the expected value and the read
/// was served by Readyset rather than proxied upstream.
async fn sibling_row_served_by_readyset(rs_pool: &Pool, id: u32, value: &str) -> bool {
    let mut conn = match rs_pool.get_conn().await {
        Ok(c) => c,
        Err(_) => return false,
    };
    let row: Option<(u32, String)> = match conn
        .query_first(format!(
            "SELECT id, t FROM `{DENIAL_SIBLING}` WHERE id = {id}"
        ))
        .await
    {
        Ok(row) => row,
        Err(_) => return false,
    };
    if row.is_none_or(|(_, t)| t != value) {
        return false;
    }
    match conn
        .query_first::<mysql_async::Row, _>("EXPLAIN LAST STATEMENT")
        .await
    {
        Ok(Some(info)) => {
            let destination: Option<String> = info.get("Query_destination");
            destination.is_some_and(|d| d.starts_with("readyset") && !d.contains("upstream"))
        }
        _ => false,
    }
}

/// Failpoint definitions used by the chaos driver: (failpoint name, action string).
const CHAOS_FAILPOINTS: &[(&str, &str)] = &[
    (failpoints::CONTROLLER_EVENTS_SSE_DISCONNECT, "1*return"),
    (
        failpoints::CONTROLLER_EVENTS_SSE_CONNECT_DELAY,
        "1*return(1500)",
    ),
    (
        failpoints::CONTROLLER_EVENTS_SSE_SEND_DELAY,
        "1*return(1000)",
    ),
    (
        failpoints::SCHEMA_CATALOG_SYNCHRONIZER_DELAY,
        "1*sleep(2000)",
    ),
];

async fn run_chaos(readyset: &ReadysetOpts) -> Result<()> {
    let base_url = format!(
        "http://{}:{}",
        readyset.readyset_host, readyset.readyset_http_port
    );

    let client = FailpointClient::new(&base_url);
    let mut rng = rand::rng();
    let idx = rng.random_range(0..CHAOS_FAILPOINTS.len());
    let (name, action) = CHAOS_FAILPOINTS[idx];

    info!(failpoint = name, action, "Activating failpoint");
    match client.set(name, action).await {
        Ok(()) => {
            info!(failpoint = name, "Failpoint activated successfully");
            assert_reachable!(
                "Chaos driver activated failpoint",
                &json!({"failpoint": name})
            );
        }
        Err(e) => {
            error!(%e, failpoint = name, "Failed to activate failpoint");
            assert_unreachable!(
                "Failpoint HTTP request failed",
                &json!({"failpoint": name, "error": e.to_string()})
            );
            anyhow::bail!("Failed to activate failpoint {name}: {e}");
        }
    }

    Ok(())
}

/// Returns true for errors that are expected during normal DDL stress test operation,
/// such as tables being dropped concurrently by the DDL driver.
fn is_expected_error(msg: &str) -> bool {
    let lower = msg.to_lowercase();
    lower.contains("doesn't exist")
        || lower.contains("does not exist")
        || lower.contains("unknown table")
        || lower.contains("not being replicated")
        || lower.contains("no cache named")
        || lower.contains("no query found")
        || lower.contains("table already exists")
        || lower.contains("cache already exists")
        || lower.contains("could not find table")
        || lower.contains("unsupported type")
}
