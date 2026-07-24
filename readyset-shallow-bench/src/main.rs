//! Standalone benchmark comparing shallow-cache configurations on a skewed product-catalog
//! workload, printing staleness and latency stats (and per-key time-series traces) to stdout.
//!
//! Runs an in-process Readyset adapter (no replication; shallow caches proxy to upstream)
//! against an external MySQL configured via `MYSQL_HOST`/`MYSQL_TCP_PORT`/`MYSQL_USER`/
//! `MYSQL_PASSWORD`, like the e2e tests.

use std::collections::{HashMap, HashSet, VecDeque};
use std::ops::RangeInclusive;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use clap::Parser;
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Opts};
use rand::RngExt;
use readyset_client_metrics::QueryDestination;
use readyset_client_test_helpers::{
    TestBuilder,
    mysql_helpers::{self, MySQLAdapter, last_query_info},
};
use readyset_util::timestamp::current_timestamp_ms;
use readyset_util::{eventually, retry_with_exponential_backoff};
use tokio::task::JoinHandle;
use tokio::time::{sleep, sleep_until};

/// Configured refresh period for the refreshing scenarios, milliseconds.
const CONFIGURED_PERIOD_MS: u64 = 2000;
/// Read-traffic share of hot/warm/cold tiers, percent.
const TIER_WEIGHTS: [u32; 3] = [70, 20, 10];
/// Share of a long-tail tier's reads that target its working set, percent.
const WORKING_SET_PERCENT: u32 = 80;
/// Trace sampling interval.
const TRACE_INTERVAL: Duration = Duration::from_millis(500);
/// How far back per-key write times are retained; must comfortably exceed the oldest
/// version the cache can still serve (the TTL).
const WRITE_LOG_HORIZON_MS: u64 = 30_000;

#[derive(Parser, Debug)]
#[command(about = "Benchmark shallow-cache configurations on a skewed product-catalog workload")]
struct Args {
    /// Total products in the catalog
    #[arg(long, default_value_t = 1_000_000)]
    products: u64,
    /// Products updated every second (trending items)
    #[arg(long, default_value_t = 1_000)]
    hot: u64,
    /// Products updated every ten seconds (seasonal items)
    #[arg(long, default_value_t = 10_000)]
    warm: u64,
    /// Parallel client reader tasks against Readyset
    #[arg(long, default_value_t = 4)]
    readers: u64,
    /// Measurement window per scenario, seconds
    #[arg(long, default_value_t = 60)]
    duration_secs: u64,
    /// Cold-tier keys that receive most of the cold read traffic (the popular long tail);
    /// these are also pre-warmed into the cache
    #[arg(long, default_value_t = 10_000)]
    cold_working_set: u64,
    /// Working-set size for the load-cap scenario; every key in it churns at 1s
    #[arg(long, default_value_t = 8_000)]
    cap_keys: u64,
    /// Adaptive refresh load cap for the load-cap scenario, percent over baseline
    #[arg(long, default_value_t = 100)]
    cap_extra_load_percent: u64,
    /// Scenarios to run
    #[arg(long, value_delimiter = ',', default_values_t = vec![1, 2, 3, 4, 5, 6])]
    scenarios: Vec<u32>,
    /// Skip recreating and reseeding the products table
    #[arg(long)]
    no_seed: bool,
    /// Upstream database name
    #[arg(long, default_value = "shallow_bench")]
    database: String,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Tier {
    Hot,
    Warm,
    Cold,
}

impl Tier {
    fn name(self) -> &'static str {
        match self {
            Tier::Hot => "hot",
            Tier::Warm => "warm",
            Tier::Cold => "cold",
        }
    }
}

#[derive(Clone, Debug)]
struct TierSpec {
    tier: Tier,
    range: RangeInclusive<u64>,
    /// Relative share of read traffic.
    weight: u32,
    /// When set, most reads target the first `working_set` keys of the range and the rest
    /// spread over the whole range (long-tail tiers).
    working_set: Option<u64>,
}

#[derive(Clone, Debug)]
struct Tiers(Vec<TierSpec>);

impl Tiers {
    fn standard(args: &Args) -> Self {
        let cold_lo = args.hot + args.warm + 1;
        let cold_len = args.products - args.hot - args.warm;
        Tiers(vec![
            TierSpec {
                tier: Tier::Hot,
                range: 1..=args.hot,
                weight: TIER_WEIGHTS[0],
                working_set: None,
            },
            TierSpec {
                tier: Tier::Warm,
                range: args.hot + 1..=args.hot + args.warm,
                weight: TIER_WEIGHTS[1],
                working_set: None,
            },
            TierSpec {
                tier: Tier::Cold,
                range: cold_lo..=args.products,
                weight: TIER_WEIGHTS[2],
                working_set: Some(cold_len.min(args.cold_working_set)),
            },
        ])
    }

    fn all_hot(keys: u64) -> Self {
        Tiers(vec![TierSpec {
            tier: Tier::Hot,
            range: 1..=keys,
            weight: 1,
            working_set: None,
        }])
    }

    fn pick(&self) -> (Tier, u64) {
        let mut rng = rand::rng();
        let total: u32 = self.0.iter().map(|s| s.weight).sum();
        let mut roll = rng.random_range(0..total);
        for spec in &self.0 {
            if roll >= spec.weight {
                roll -= spec.weight;
                continue;
            }
            let id = match spec.working_set {
                Some(ws) if rng.random_range(0..100) < WORKING_SET_PERCENT => {
                    rng.random_range(*spec.range.start()..*spec.range.start() + ws)
                }
                _ => rng.random_range(spec.range.clone()),
            };
            return (spec.tier, id);
        }
        unreachable!("weighted pick fell through")
    }
}

/// A tier's write pattern: every key in `range` is updated once per `cadence`.
#[derive(Clone, Debug)]
struct Churn {
    range: RangeInclusive<u64>,
    cadence: Duration,
}

fn point_select(id: u64) -> String {
    format!("SELECT name, price_cents, inventory, updated_ms FROM products WHERE id = {id}")
}

fn category_select(category: u64) -> String {
    format!(
        "SELECT id, name, price_cents FROM products WHERE category_id = {category} \
         ORDER BY RAND()"
    )
}

async fn upstream_conn(db: &str) -> Conn {
    retry_with_exponential_backoff!(
        { Conn::new(mysql_helpers::upstream_config().db_name(Some(db))).await },
        retries: 5,
        delay: 100,
        backoff: 2,
    )
    .expect("connecting to upstream MySQL failed")
}

async fn readyset_conn(opts: &Opts) -> Conn {
    retry_with_exponential_backoff!(
        { Conn::new(opts.clone()).await },
        retries: 5,
        delay: 100,
        backoff: 2,
    )
    .expect("connecting to Readyset failed")
}

/// The refresh pool alone can open 100 upstream connections, and writers, samplers, and
/// each adapter client connection add more; a default `max_connections` (151) run dies with
/// connection resets mid-benchmark. Warn up front instead.
async fn check_max_connections() {
    let mut conn = upstream_conn("mysql").await;
    let row: Option<(String, String)> = conn
        .query_first("SHOW VARIABLES LIKE 'max_connections'")
        .await
        .expect("reading max_connections failed");
    let max: u64 = row
        .map(|(_, v)| v.parse().expect("unparseable max_connections"))
        .expect("max_connections variable missing");
    if max < 300 {
        println!(
            "warning: MySQL max_connections is {max}; this benchmark can need well over \
             150 connections and may see connection resets. Consider raising it, e.g. \
             SET GLOBAL max_connections = 500"
        );
    }
}

async fn seed(db: &str, products: u64) {
    println!("seeding {products} products into database {db}");
    mysql_helpers::recreate_database(db).await;
    let mut conn = upstream_conn(db).await;
    conn.query_drop(
        "CREATE TABLE products (
             id INT PRIMARY KEY,
             name VARCHAR(64),
             category_id INT,
             price_cents INT,
             inventory INT,
             updated_ms BIGINT,
             KEY (category_id)
         )",
    )
    .await
    .expect("creating products table failed");
    let now = current_timestamp_ms();
    let mut lo = 1;
    while lo <= products {
        let hi = (lo + 9_999).min(products);
        let rows = (lo..=hi)
            .map(|id| {
                let price = (id * 7919) % 99_901 + 99;
                format!("({id}, 'product {id}', {}, {price}, 1000, {now})", id / 10)
            })
            .collect::<Vec<_>>()
            .join(", ");
        conn.query_drop(format!("INSERT INTO products VALUES {rows}"))
            .await
            .expect("seeding products failed");
        if hi % 100_000 == 0 {
            println!("  seeded {hi} rows");
        }
        lo = hi + 1;
    }
}

/// Upstream write times per key, recorded by the churn writers as each update lands, so
/// staleness measurement can find the first write that superseded a served row version.
/// Writes older than `WRITE_LOG_HORIZON_MS` are pruned as new ones arrive.
#[derive(Clone, Debug, Default)]
struct WriteLog(Arc<Mutex<HashMap<u64, VecDeque<u64>>>>);

impl WriteLog {
    fn record(&self, id: u64, ts: u64) {
        let mut map = self.0.lock().expect("write log lock poisoned");
        let log = map.entry(id).or_default();
        log.push_back(ts);
        while log.front().is_some_and(|&t| t + WRITE_LOG_HORIZON_MS < ts) {
            log.pop_front();
        }
    }

    /// The earliest recorded write strictly newer than `version_ms`, or `None` when that
    /// version is still current.
    fn first_after(&self, id: u64, version_ms: u64) -> Option<u64> {
        let map = self.0.lock().expect("write log lock poisoned");
        map.get(&id)?.iter().copied().find(|&t| t > version_ms)
    }
}

/// Update each key in each churn range once per its cadence, from a pool of sharded writer
/// connections, spreading a range's updates evenly across its cadence window. Returns the
/// writer handles; each yields its update count when joined after `stop` is set.
fn spawn_writers(
    db: &str,
    churn: &[Churn],
    write_log: &WriteLog,
    stop: &Arc<AtomicBool>,
) -> Vec<JoinHandle<u64>> {
    let mut handles = Vec::new();
    for c in churn {
        let keys = c.range.clone().count() as u64;
        if keys == 0 {
            continue;
        }
        let updates_per_sec = keys * 1000 / c.cadence.as_millis().max(1) as u64;
        // Sized so each connection only needs to sustain ~200 single-row updates a second,
        // which stays achievable when the upstream is also absorbing refresh load.
        let conns = updates_per_sec.div_ceil(200).clamp(1, 50);
        for shard in 0..conns {
            let shard_keys: Vec<u64> = c.range.clone().filter(|id| id % conns == shard).collect();
            if shard_keys.is_empty() {
                continue;
            }
            let db = db.to_owned();
            let cadence = c.cadence;
            let write_log = write_log.clone();
            let stop = stop.clone();
            handles.push(tokio::spawn(churn_writer(
                db, shard_keys, cadence, write_log, stop,
            )));
        }
    }
    handles
}

async fn churn_writer(
    db: String,
    keys: Vec<u64>,
    cadence: Duration,
    write_log: WriteLog,
    stop: Arc<AtomicBool>,
) -> u64 {
    let mut conn = upstream_conn(&db).await;
    let interval = cadence / keys.len() as u32;
    let mut updates = 0u64;
    'outer: loop {
        let pass_start = tokio::time::Instant::now();
        for (i, id) in keys.iter().enumerate() {
            if stop.load(Ordering::Relaxed) {
                break 'outer;
            }
            let now = current_timestamp_ms();
            // Bump the price on a random tenth of updates so queries that don't select
            // inventory still see periodic content changes on every key. A fixed every-Nth
            // rule aliases with the shard size and can starve some keys entirely.
            let price = if rand::rng().random_range(0..10) == 0 {
                ", price_cents = price_cents + 1"
            } else {
                ""
            };
            conn.query_drop(format!(
                "UPDATE products SET inventory = inventory - 1{price}, updated_ms = {now} \
                 WHERE id = {id}"
            ))
            .await
            .expect("churn update failed");
            write_log.record(*id, now);
            updates += 1;
            sleep_until(pass_start + interval * (i as u32 + 1)).await;
        }
    }
    updates
}

/// Simulated product-page traffic: read random products with the tier mix over `conn`,
/// recording `(tier, served_by_shallow_cache, latency_us)` per read. `classify` consults
/// EXPLAIN LAST STATEMENT after each read; pass false for connections straight to the
/// upstream.
async fn latency_reader(
    mut conn: Conn,
    classify: bool,
    tiers: Tiers,
    stop: Arc<AtomicBool>,
) -> Vec<(Tier, bool, u64)> {
    let mut samples = Vec::new();
    while !stop.load(Ordering::Relaxed) {
        let (tier, id) = tiers.pick();
        let start = Instant::now();
        conn.query_drop(point_select(id))
            .await
            .expect("timed read failed");
        let us = start.elapsed().as_micros() as u64;
        let shallow = classify
            && last_query_info(&mut conn).await.destination == QueryDestination::ReadysetShallow;
        samples.push((tier, shallow, us));
    }
    samples
}

/// Measure served staleness: read a product through Readyset and, when the row came from the
/// shallow cache and a churn write has superseded the served version, record how long ago
/// the first such write landed; 0 when the served version is still current. Waits out
/// `delay` first, giving the churn and the cache time to reach steady state.
async fn staleness_sampler(
    opts: Opts,
    tiers: Tiers,
    write_log: WriteLog,
    delay: Duration,
    stop: Arc<AtomicBool>,
) -> Vec<(Tier, u64)> {
    let mut rs = readyset_conn(&opts).await;
    let mut samples = Vec::new();
    sleep(delay).await;
    while !stop.load(Ordering::Relaxed) {
        let (tier, id) = tiers.pick();
        let cached: Option<(String, i64, i64, u64)> = rs
            .query_first(point_select(id))
            .await
            .expect("staleness read through Readyset failed");
        let served_at = current_timestamp_ms();
        let Some((_, _, _, cached_ms)) = cached else {
            continue;
        };
        if last_query_info(&mut rs).await.destination != QueryDestination::ReadysetShallow {
            continue;
        }
        let staleness = write_log
            .first_after(id, cached_ms)
            .map_or(0, |stale_since| served_at.saturating_sub(stale_since));
        samples.push((tier, staleness));
    }
    samples
}

#[derive(Clone, Copy, Debug, Default)]
struct CacheCounters {
    hits: u64,
    misses: u64,
    refreshes: u64,
    /// Refresh write-backs never returned by a hit before the entry was replaced or evicted.
    wasted: u64,
    /// Refreshes the pipeline could not execute; nonzero means the refresh pool saturated
    /// and staleness numbers are suspect.
    dropped: u64,
}

async fn cache_counters(rs: &mut Conn) -> CacheCounters {
    let (hits, misses, refreshes): (Option<u64>, Option<u64>, Option<u64>) = rs
        .query_first("SELECT SUM(hits), SUM(misses), SUM(refreshes) FROM readyset.shallow_caches")
        .await
        .expect("listing shallow caches failed")
        .expect("aggregate query returned no row");
    let (wasted, dropped): (Option<u64>, Option<u64>) = rs
        .query_first(
            "SELECT SUM(wasted_refreshes), SUM(dropped_refreshes) \
             FROM readyset.shallow_cache_refresh_stats",
        )
        .await
        .expect("reading refresh stats failed")
        .expect("aggregate query returned no row");
    CacheCounters {
        hits: hits.unwrap_or_default(),
        misses: misses.unwrap_or_default(),
        refreshes: refreshes.unwrap_or_default(),
        wasted: wasted.unwrap_or_default(),
        dropped: dropped.unwrap_or_default(),
    }
}

async fn cache_query_id(rs: &mut Conn) -> String {
    let (query_id,): (String,) = rs
        .query_first("SELECT query_id FROM readyset.shallow_caches")
        .await
        .expect("listing shallow caches failed")
        .expect("no shallow cache found");
    query_id
}

/// Per-entry refresh periods for the cache with the given query id. `None` for
/// non-adaptive caches.
async fn entry_periods(rs: &mut Conn, query_id: &str) -> HashMap<String, Option<u64>> {
    rs.query::<(String, String, Option<u64>), _>(
        "SELECT query_id, entry_id, refresh_period_ms FROM readyset.shallow_cache_entries",
    )
    .await
    .expect("listing shallow cache entries failed")
    .into_iter()
    .filter(|(qid, _, _)| qid == query_id)
    .map(|(_, entry_id, period)| (entry_id, period))
    .collect()
}

/// `(load_actual_ppm, load_baseline_ppm, over_cap)` for the single cache.
async fn load_stats(rs: &mut Conn) -> (u64, u64, bool) {
    let rows: Vec<(Option<u64>, Option<u64>, Option<bool>)> = rs
        .query(
            "SELECT load_actual_ppm, load_baseline_ppm, over_cap \
             FROM readyset.shallow_cache_refresh_stats",
        )
        .await
        .expect("reading refresh stats failed");
    let (actual, baseline, over) = rows.first().copied().expect("no refresh stats row");
    (
        actual.unwrap_or_default(),
        baseline.unwrap_or_default(),
        over.unwrap_or_default(),
    )
}

/// Warm one key via `read_query` and return its opaque entry id, telling it apart from
/// entries warmed earlier by diffing against `seen`.
async fn traced_entry(
    rs: &mut Conn,
    query_id: &str,
    read_query: &str,
    seen: &mut HashSet<String>,
) -> String {
    rs.query_drop(read_query).await.expect("warm read failed");
    let expected = seen.len() + 1;
    eventually!(
        attempts: 40,
        sleep: Duration::from_millis(250),
        message: format!("entry for traced query did not appear: {read_query}"),
        { entry_periods(rs, query_id).await.len() == expected }
    );
    let entry = entry_periods(rs, query_id)
        .await
        .into_keys()
        .find(|e| !seen.contains(e))
        .expect("traced entry missing");
    seen.insert(entry.clone());
    entry
}

/// Populate cache entries for the given key ranges by reading each key once.
async fn warm_ranges(opts: &Opts, ranges: &[RangeInclusive<u64>]) {
    const CONNS: u64 = 4;
    let mut handles = Vec::new();
    for shard in 0..CONNS {
        let ranges = ranges.to_vec();
        let opts = opts.clone();
        handles.push(tokio::spawn(async move {
            let mut conn = readyset_conn(&opts).await;
            for range in ranges {
                for id in range.filter(|id| id % CONNS == shard) {
                    conn.query_drop(point_select(id))
                        .await
                        .expect("warm read failed");
                }
            }
        }));
    }
    for handle in handles {
        handle.await.expect("warm task panicked");
    }
}

/// A key whose staleness and refresh period are logged over time.
struct TracedKey {
    label: &'static str,
    id: u64,
    entry_id: String,
}

/// Log `trace,<elapsed_ms>,<label>,<staleness_ms>,<refresh_period_ms>` for each traced key
/// every 500ms, plus `trace,<elapsed_ms>,load,<actual_ppm>,<baseline_ppm>,<over_cap>` when
/// requested. Staleness is the time since the first churn write that superseded the served
/// row version, 0 when that version is still current or the read wasn't served by the cache.
async fn trace_point_keys(
    opts: Opts,
    query_id: String,
    traced: Vec<TracedKey>,
    write_log: WriteLog,
    show_load: bool,
    stop: Arc<AtomicBool>,
) {
    let mut rs = readyset_conn(&opts).await;
    let start = Instant::now();
    println!("trace columns: trace,elapsed_ms,label,staleness_ms,refresh_period_ms");
    if show_load {
        println!("trace columns: trace,elapsed_ms,load,actual_ppm,baseline_ppm,over_cap");
    }
    while !stop.load(Ordering::Relaxed) {
        let periods = entry_periods(&mut rs, &query_id).await;
        let elapsed = start.elapsed().as_millis();
        for key in &traced {
            let cached: Option<(String, i64, i64, u64)> = rs
                .query_first(point_select(key.id))
                .await
                .expect("trace read through Readyset failed");
            let served_at = current_timestamp_ms();
            let mut staleness = 0;
            if let Some((_, _, _, cached_ms)) = cached
                && last_query_info(&mut rs).await.destination == QueryDestination::ReadysetShallow
                && let Some(stale_since) = write_log.first_after(key.id, cached_ms)
            {
                staleness = served_at.saturating_sub(stale_since);
            }
            let period = periods
                .get(&key.entry_id)
                .copied()
                .flatten()
                .map(|p| p.to_string())
                .unwrap_or_default();
            println!("trace,{elapsed},{},{staleness},{period}", key.label);
        }
        if show_load {
            let (actual, baseline, over) = load_stats(&mut rs).await;
            println!("trace,{elapsed},load,{actual},{baseline},{over}");
        }
        sleep(TRACE_INTERVAL).await;
    }
}

struct WorkloadStats {
    staleness: Vec<(Tier, u64)>,
    readyset_latency: Vec<(Tier, bool, u64)>,
    upstream_latency: Vec<(Tier, u64)>,
}

/// Run latency readers against Readyset and the upstream, and the staleness sampler, for
/// `duration`, then set `stop` and collect everything.
async fn measure(
    opts: &Opts,
    db: &str,
    tiers: &Tiers,
    write_log: &WriteLog,
    n_readers: u64,
    duration: Duration,
    stop: Arc<AtomicBool>,
) -> WorkloadStats {
    let mut readers = Vec::new();
    for _ in 0..n_readers {
        let conn = readyset_conn(opts).await;
        readers.push(tokio::spawn(latency_reader(
            conn,
            true,
            tiers.clone(),
            stop.clone(),
        )));
    }
    let upstream_reader = tokio::spawn(latency_reader(
        upstream_conn(db).await,
        false,
        tiers.clone(),
        stop.clone(),
    ));
    let sampler = tokio::spawn(staleness_sampler(
        opts.clone(),
        tiers.clone(),
        write_log.clone(),
        // Long enough to cover a full warm-tier churn pass before sampling begins.
        (duration / 4).min(Duration::from_secs(12)),
        stop.clone(),
    ));
    sleep(duration).await;
    stop.store(true, Ordering::Relaxed);

    let mut stats = WorkloadStats {
        staleness: sampler.await.expect("staleness sampler panicked"),
        readyset_latency: Vec::new(),
        upstream_latency: upstream_reader
            .await
            .expect("upstream reader panicked")
            .into_iter()
            .map(|(tier, _, us)| (tier, us))
            .collect(),
    };
    for reader in readers {
        stats
            .readyset_latency
            .extend(reader.await.expect("latency reader panicked"));
    }
    stats
}

struct Summary {
    n: usize,
    mean: u64,
    p50: u64,
    p95: u64,
    p99: u64,
    max: u64,
}

fn summarize(mut samples: Vec<u64>) -> Option<Summary> {
    if samples.is_empty() {
        return None;
    }
    samples.sort_unstable();
    let pct = |p: usize| samples[(samples.len() * p / 100).min(samples.len() - 1)];
    Some(Summary {
        n: samples.len(),
        mean: samples.iter().sum::<u64>() / samples.len() as u64,
        p50: pct(50),
        p95: pct(95),
        p99: pct(99),
        max: *samples.last().expect("nonempty"),
    })
}

fn print_dist(label: &str, samples: Vec<u64>) {
    match summarize(samples) {
        Some(s) => println!(
            "  {label:<24} samples={:<9} mean={:<8} p50={:<8} p95={:<8} p99={:<8} max={}",
            s.n, s.mean, s.p50, s.p95, s.p99, s.max
        ),
        None => println!("  {label:<24} samples=0"),
    }
}

fn print_stats(
    stats: &WorkloadStats,
    before: CacheCounters,
    after: CacheCounters,
    window: Duration,
    writes: u64,
    write_target: u64,
) {
    println!("readyset read latency (us):");
    for tier in [Tier::Hot, Tier::Warm, Tier::Cold] {
        for (shallow, kind) in [(true, "cache hit"), (false, "miss/proxied")] {
            let samples: Vec<u64> = stats
                .readyset_latency
                .iter()
                .filter(|(t, s, _)| *t == tier && *s == shallow)
                .map(|(_, _, us)| *us)
                .collect();
            print_dist(&format!("{} {kind}", tier.name()), samples);
        }
    }

    println!("upstream read latency (us):");
    for tier in [Tier::Hot, Tier::Warm, Tier::Cold] {
        let samples: Vec<u64> = stats
            .upstream_latency
            .iter()
            .filter(|(t, _)| *t == tier)
            .map(|(_, us)| *us)
            .collect();
        print_dist(tier.name(), samples);
    }

    println!(
        "cache counters during the {:.1?} window: hits {} misses {} refreshes {} \
         (wasted {} dropped {})",
        window,
        after.hits - before.hits,
        after.misses - before.misses,
        after.refreshes - before.refreshes,
        after.wasted - before.wasted,
        after.dropped - before.dropped,
    );
    println!("upstream updates during the window: {writes} (cadence target ~{write_target})");
}

fn print_staleness(stats: &WorkloadStats) {
    println!("sampled staleness of shallow-served rows (ms):");
    for tier in [Tier::Hot, Tier::Warm, Tier::Cold] {
        let samples: Vec<u64> = stats
            .staleness
            .iter()
            .filter(|(t, _)| *t == tier)
            .map(|(_, s)| *s)
            .collect();
        print_dist(tier.name(), samples);
    }
    print_dist("overall", stats.staleness.iter().map(|(_, s)| *s).collect());
}

fn standard_churn(args: &Args) -> Vec<Churn> {
    vec![
        Churn {
            range: 1..=args.hot,
            cadence: Duration::from_secs(1),
        },
        Churn {
            range: args.hot + 1..=args.hot + args.warm,
            cadence: Duration::from_secs(10),
        },
    ]
}

/// Scenarios 1-5: a point-lookup cache over the product page query.
async fn point_scenario(num: u32, args: &Args) {
    let (title, policy, adaptive) = match num {
        1 => (
            "baseline TTL (refresh never fires before expiry; classic sawtooth)",
            "POLICY TTL 10 SECONDS REFRESH 9999 MILLISECONDS",
            false,
        ),
        2 => (
            "on-access refresh",
            "POLICY TTL 10 SECONDS REFRESH 2 SECONDS",
            false,
        ),
        3 => (
            "scheduled refresh",
            "POLICY TTL 10 SECONDS REFRESH EVERY 2 SECONDS",
            false,
        ),
        4 => (
            "adaptive refresh",
            "POLICY TTL 10 SECONDS REFRESH EVERY 2 SECONDS",
            true,
        ),
        5 => (
            "adaptive refresh load cap (every key hot)",
            "POLICY TTL 10 SECONDS REFRESH EVERY 2 SECONDS",
            true,
        ),
        _ => unreachable!(),
    };
    // Scenario 5 exercises the cap; everywhere else it is kept out of the way.
    let cap = if num == 5 {
        args.cap_extra_load_percent
    } else {
        10_000
    };
    let (tiers, churn) = if num == 5 {
        (
            Tiers::all_hot(args.cap_keys),
            vec![Churn {
                range: 1..=args.cap_keys,
                cadence: Duration::from_secs(1),
            }],
        )
    } else {
        (Tiers::standard(args), standard_churn(args))
    };

    println!("\n=== scenario {num}: {title} ===");
    let ddl = format!(
        "CREATE SHALLOW CACHE WITH ({policy}{}) \
         FROM SELECT name, price_cents, inventory, updated_ms FROM products WHERE id = ?",
        if adaptive { ", ADAPTIVE" } else { "" }
    );
    println!("{ddl}");
    if num == 5 {
        println!(
            "working set: {} keys, all updated every 1s; load cap {}% over baseline",
            args.cap_keys, cap
        );
    }

    let (opts, _readyset_handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .replicate(false)
        .fallback_without_replication(&args.database)
        .shallow_adaptive_max_extra_load_percent(cap)
        .build::<MySQLAdapter>()
        .await;
    let mut rs = readyset_conn(&opts).await;
    rs.query_drop(&ddl)
        .await
        .expect("CREATE SHALLOW CACHE failed");
    let query_id = cache_query_id(&mut rs).await;

    // Warm the traced keys first, one at a time, to learn their entry ids.
    let traced_ids: Vec<(&'static str, u64)> = match num {
        4 => vec![("hot", 1), ("cold", args.products)],
        5 => vec![("hot", 1)],
        _ => vec![],
    };
    let mut seen = HashSet::new();
    let mut traced = Vec::new();
    for (label, id) in traced_ids {
        let entry_id = traced_entry(&mut rs, &query_id, &point_select(id), &mut seen).await;
        traced.push(TracedKey {
            label,
            id,
            entry_id,
        });
    }

    let stop = Arc::new(AtomicBool::new(false));
    let write_log = WriteLog::default();
    // Start the churn before warming so cached content reflects steady-state writes rather
    // than pre-scenario timestamps.
    let writers = spawn_writers(&args.database, &churn, &write_log, &stop);
    // Start tracing before the bulk warm so the traced entries stay accessed (and alive)
    // throughout; the trace shows the warm phase as a flat prefix.
    let trace = (!traced.is_empty()).then(|| {
        tokio::spawn(trace_point_keys(
            opts.clone(),
            query_id.clone(),
            traced,
            write_log.clone(),
            num == 5,
            stop.clone(),
        ))
    });

    let warm_list: Vec<RangeInclusive<u64>> = if num == 5 {
        vec![1..=args.cap_keys]
    } else {
        // Hot and warm tiers plus the cold working set; the rest of the long tail
        // populates organically.
        vec![
            1..=args.hot + args.warm,
            args.hot + args.warm + 1
                ..=(args.hot + args.warm + args.cold_working_set).min(args.products),
        ]
    };
    println!("warming cache entries");
    warm_ranges(&opts, &warm_list).await;

    let before = cache_counters(&mut rs).await;
    let start = Instant::now();
    let stats = measure(
        &opts,
        &args.database,
        &tiers,
        &write_log,
        args.readers,
        Duration::from_secs(args.duration_secs),
        stop.clone(),
    )
    .await;
    let window = start.elapsed();
    let after = cache_counters(&mut rs).await;
    let mut writes = 0;
    for writer in writers {
        writes += writer.await.expect("churn writer panicked");
    }
    if let Some(trace) = trace {
        trace.await.expect("trace task panicked");
    }
    // Writers also run during the warm phase, so the target based on the measurement
    // window alone slightly understates; close enough to spot a shortfall.
    let write_target = churn
        .iter()
        .map(|c| {
            c.range.clone().count() as u64 * window.as_millis() as u64
                / c.cadence.as_millis().max(1) as u64
        })
        .sum();
    print_stats(&stats, before, after, window, writes, write_target);
    print_staleness(&stats);

    shutdown_tx.shutdown().await;
}

/// Scenario 6: a category-listing cache whose ORDER BY RAND() shuffles row order on each
/// refresh; the order-insensitive result hash keeps a static category's refresh period from
/// collapsing.
async fn order_insensitive_scenario(args: &Args) {
    let hot_category = 2;
    let cold_category = args.products / 10 - 1;
    println!("\n=== scenario 6: order-insensitive equality (ORDER BY RAND()) ===");
    let ddl = "CREATE SHALLOW CACHE WITH (POLICY TTL 10 SECONDS REFRESH EVERY 2 SECONDS, \
         ADAPTIVE) FROM SELECT id, name, price_cents FROM products WHERE category_id = ? \
         ORDER BY RAND()";
    println!("{ddl}");
    println!(
        "cold category {cold_category} never changes; hot category {hot_category} sees \
         price changes"
    );

    let (opts, _readyset_handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .replicate(false)
        .fallback_without_replication(&args.database)
        .shallow_adaptive_max_extra_load_percent(10_000)
        .build::<MySQLAdapter>()
        .await;
    let mut rs = readyset_conn(&opts).await;
    rs.query_drop(ddl)
        .await
        .expect("CREATE SHALLOW CACHE failed");
    let query_id = cache_query_id(&mut rs).await;

    let churn = standard_churn(args);
    let stop = Arc::new(AtomicBool::new(false));
    let write_log = WriteLog::default();
    let writers = spawn_writers(&args.database, &churn, &write_log, &stop);

    let mut seen = HashSet::new();
    let mut traced = Vec::new();
    for (label, category) in [("cold", cold_category), ("hot", hot_category)] {
        let entry_id =
            traced_entry(&mut rs, &query_id, &category_select(category), &mut seen).await;
        traced.push((label, category, entry_id));
    }

    let before = cache_counters(&mut rs).await;

    // Read both categories every tick: the reads keep the entries alive (scheduled
    // refreshes don't count as accesses) and the vrel supplies each period.
    let start = Instant::now();
    println!("trace columns: trace,elapsed_ms,label,,refresh_period_ms");
    let mut samples: Vec<(&'static str, Option<u64>)> = Vec::new();
    while start.elapsed() < Duration::from_secs(args.duration_secs) {
        let periods = entry_periods(&mut rs, &query_id).await;
        let elapsed = start.elapsed().as_millis();
        for (label, category, entry_id) in &traced {
            rs.query_drop(category_select(*category))
                .await
                .expect("category read failed");
            let period = periods.get(entry_id).copied().flatten();
            println!(
                "trace,{elapsed},{label},,{}",
                period.map(|p| p.to_string()).unwrap_or_default()
            );
            samples.push((label, period));
        }
        sleep(TRACE_INTERVAL).await;
    }
    stop.store(true, Ordering::Relaxed);
    for writer in writers {
        writer.await.expect("churn writer panicked");
    }
    let after = cache_counters(&mut rs).await;

    let held = samples
        .iter()
        .filter(|(l, p)| *l == "cold" && *p == Some(CONFIGURED_PERIOD_MS))
        .count();
    let cold_total = samples.iter().filter(|(l, _)| *l == "cold").count();
    let hot_min = samples
        .iter()
        .filter_map(|(l, p)| (*l == "hot").then_some(*p).flatten())
        .min();
    println!(
        "cold category held the configured {CONFIGURED_PERIOD_MS}ms period in \
         {held}/{cold_total} samples despite {} refreshes",
        after.refreshes - before.refreshes,
    );
    match hot_min {
        Some(min) => println!("hot category period shrank to {min}ms on real changes"),
        None => println!("hot category period was never observed"),
    }

    shutdown_tx.shutdown().await;
}

async fn run_scenario(num: u32, args: &Args) {
    match num {
        1..=5 => point_scenario(num, args).await,
        6 => order_insensitive_scenario(args).await,
        _ => println!("unknown scenario {num}, skipping"),
    }
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    assert!(
        args.hot >= 30 && args.warm > 0 && args.hot + args.warm < args.products,
        "need hot >= 30, warm > 0, and hot + warm < products"
    );
    assert!(
        args.cap_keys <= args.products,
        "cap-keys must not exceed products"
    );
    check_max_connections().await;
    if !args.no_seed {
        seed(&args.database, args.products).await;
    }
    for &num in &args.scenarios {
        run_scenario(num, &args).await;
    }
}
