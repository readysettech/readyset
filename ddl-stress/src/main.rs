use std::time::{Duration, Instant};

use antithesis_sdk::prelude::*;
use anyhow::Result;
use clap::{Args, Parser, Subcommand};
use failpoint_client::FailpointClient;
use mysql_async::prelude::*;
use mysql_async::{Conn, Pool};
use rand::Rng as _;
use readyset_tracing::init_test_logging;
use readyset_util::failpoints;
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
        // Yield briefly to avoid busy-spinning, but go as fast as possible
        tokio::task::yield_now().await;

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
        || lower.contains("not replicated")
        || lower.contains("no cache named")
        || lower.contains("no query found")
        || lower.contains("table already exists")
        || lower.contains("cache already exists")
        || lower.contains("could not find table")
        || lower.contains("unsupported type")
}
