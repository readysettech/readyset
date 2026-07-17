//! Medium-scale end-to-end tests for adaptive shallow-cache refresh: per-key period
//! differentiation under mixed churn, and the aggregate refresh load cap.

use std::collections::{HashMap, HashSet};
use std::ops::RangeInclusive;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use mysql_async::prelude::Queryable;
use rand::RngExt;
use readyset_client_metrics::QueryDestination;
use readyset_client_test_helpers::{
    derive_test_name,
    mysql_helpers::{self, last_query_info, MySQLAdapter},
    TestBuilder,
};
use readyset_tracing::init_test_logging;
use readyset_util::eventually;
use readyset_util::timestamp::current_timestamp_ms;
use test_utils::{tags, upstream};
use tokio::time::sleep;

const CONFIGURED_PERIOD_MS: u64 = 1000;

/// Total keys in the adaptive cache.
const N_KEYS: u64 = 1000;
/// Keys churned fast (updated every 150ms); their periods pin near the floor.
const N_FAST: u64 = 100;
/// Keys churned slowly (updated every 900ms); their periods walk toward roughly half the
/// update cadence. The remaining keys stay static.
const N_SLOW: u64 = 100;
/// Parallel client reader tasks in the sampling workloads.
const READERS: u64 = 4;

fn cache_ddl(table: &str, adaptive: bool) -> String {
    let adaptive = if adaptive { ", ADAPTIVE" } else { "" };
    // Entries expire TTL after their last client access (scheduled refreshes don't count as
    // accesses), so the TTL must comfortably exceed the test duration. Most keys are only ever
    // read once, during warm-up.
    format!(
        "CREATE SHALLOW CACHE \
         WITH (POLICY TTL 600 SECONDS REFRESH EVERY 1 SECONDS{adaptive}) \
         FROM SELECT val, modified_ms FROM {table} WHERE id = ?"
    )
}

/// Create a counters table with keys `1..=n`, recording each row's last-modified wall-clock
/// time. Timestamps are written by the test process so staleness measurements share one clock.
async fn create_counters(upstream: &mut mysql_async::Conn, table: &str, n: u64) {
    upstream
        .query_drop(format!(
            "CREATE TABLE {table} (id INT PRIMARY KEY, val INT, modified_ms BIGINT)"
        ))
        .await
        .unwrap();
    let now = current_timestamp_ms();
    let rows = (1..=n)
        .map(|id| format!("({id}, 0, {now})"))
        .collect::<Vec<_>>()
        .join(", ");
    upstream
        .query_drop(format!("INSERT INTO {table} VALUES {rows}"))
        .await
        .unwrap();
}

#[derive(Clone, Copy, Debug, Default)]
struct CacheCounters {
    hits: u64,
    misses: u64,
    refreshes: u64,
}

impl CacheCounters {
    fn reads(&self) -> u64 {
        self.hits + self.misses
    }
}

/// Cumulative read and refresh counters, by cache query id.
async fn cache_counters(readyset: &mut mysql_async::Conn) -> HashMap<String, CacheCounters> {
    readyset
        .query::<(String, Option<u64>, Option<u64>, Option<u64>), _>(
            "SELECT query_id, hits, misses, refreshes FROM readyset.shallow_caches",
        )
        .await
        .unwrap()
        .into_iter()
        .map(|(qid, hits, misses, refreshes)| {
            let counters = CacheCounters {
                hits: hits.unwrap_or(0),
                misses: misses.unwrap_or(0),
                refreshes: refreshes.unwrap_or(0),
            };
            (qid, counters)
        })
        .collect()
}

/// Per-entry refresh periods for the cache with the given query id. The period is `None` for
/// non-adaptive caches.
async fn entry_periods(
    readyset: &mut mysql_async::Conn,
    query_id: &str,
) -> HashMap<String, Option<u64>> {
    readyset
        .query::<(String, String, Option<u64>), _>(
            "SELECT query_id, entry_id, refresh_period_ms FROM readyset.shallow_cache_entries",
        )
        .await
        .unwrap()
        .into_iter()
        .filter(|(qid, _, _)| qid == query_id)
        .map(|(_, entry_id, period)| (entry_id, period))
        .collect()
}

/// Warm the keys in `ids` and return their opaque entry ids, telling them apart from entries
/// warmed earlier by diffing against `seen`. The diff is reliable because in-flight entries are
/// not listed and nothing is evicted at this scale.
async fn warm_class(
    readyset: &mut mysql_async::Conn,
    table: &str,
    query_id: &str,
    ids: RangeInclusive<u64>,
    seen: &mut HashSet<String>,
) -> HashSet<String> {
    for id in ids.clone() {
        readyset
            .query_drop(format!(
                "SELECT val, modified_ms FROM {table} WHERE id = {id}"
            ))
            .await
            .unwrap();
    }
    let expected = seen.len() + ids.clone().count();
    eventually!(
        attempts: 40,
        sleep: Duration::from_millis(250),
        message: format!("entries for keys {ids:?} did not all appear"),
        { entry_periods(readyset, query_id).await.len() == expected }
    );
    let class: HashSet<String> = entry_periods(readyset, query_id)
        .await
        .into_keys()
        .filter(|e| !seen.contains(e))
        .collect();
    seen.extend(class.iter().cloned());
    class
}

/// Bump `val` and `modified_ms` for keys in `[lo, hi]` of each table every `tick` until `stop`
/// is set.
fn spawn_churn(
    opts: mysql_async::OptsBuilder,
    tables: &[&str],
    lo: u64,
    hi: u64,
    tick: Duration,
    stop: Arc<AtomicBool>,
) -> tokio::task::JoinHandle<()> {
    let tables: Vec<String> = tables.iter().map(|t| t.to_string()).collect();
    tokio::spawn(async move {
        let mut conn = mysql_async::Conn::new(opts).await.unwrap();
        while !stop.load(Ordering::Relaxed) {
            let now = current_timestamp_ms();
            for table in &tables {
                conn.query_drop(format!(
                    "UPDATE {table} SET val = val + 1, modified_ms = {now} \
                     WHERE id BETWEEN {lo} AND {hi}"
                ))
                .await
                .unwrap();
            }
            sleep(tick).await;
        }
    })
}

/// Read random fast keys through both mixed-churn caches for 10 seconds, recording the
/// staleness of every read actually served from the shallow cache. Each key is read through
/// both caches back to back so the paired samples see the same upstream churn state. Returns
/// the (adaptive, fixed) staleness samples in milliseconds.
async fn sample_staleness(opts: mysql_async::Opts, db: String) -> (Vec<u64>, Vec<u64>) {
    let mut conn = mysql_async::Conn::new(opts).await.unwrap();
    conn.query_drop(format!("USE {db}")).await.unwrap();
    let mut adaptive_staleness = Vec::new();
    let mut fixed_staleness = Vec::new();
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(10) {
        let id = rand::rng().random_range(1..=N_FAST);
        for (table, samples) in [
            ("counters_adaptive", &mut adaptive_staleness),
            ("counters_fixed", &mut fixed_staleness),
        ] {
            let (_val, modified_ms): (i64, u64) = conn
                .query_first(format!(
                    "SELECT val, modified_ms FROM {table} WHERE id = {id}"
                ))
                .await
                .unwrap()
                .unwrap();
            let staleness = current_timestamp_ms().saturating_sub(modified_ms);
            if last_query_info(&mut conn).await.destination == QueryDestination::ReadysetShallow {
                samples.push(staleness);
            }
        }
    }
    (adaptive_staleness, fixed_staleness)
}

/// Read random keys `1..=hi` through the cap test's cache until `stop` is set, recording the
/// staleness of every read actually served from the shallow cache.
async fn sample_counters(
    opts: mysql_async::Opts,
    db: String,
    hi: u64,
    stop: Arc<AtomicBool>,
) -> Vec<u64> {
    let mut conn = mysql_async::Conn::new(opts).await.unwrap();
    conn.query_drop(format!("USE {db}")).await.unwrap();
    let mut staleness_samples = Vec::new();
    while !stop.load(Ordering::Relaxed) {
        let id = rand::rng().random_range(1..=hi);
        let (_val, modified_ms): (i64, u64) = conn
            .query_first(format!(
                "SELECT val, modified_ms FROM counters WHERE id = {id}"
            ))
            .await
            .unwrap()
            .unwrap();
        let staleness = current_timestamp_ms().saturating_sub(modified_ms);
        if last_query_info(&mut conn).await.destination == QueryDestination::ReadysetShallow {
            staleness_samples.push(staleness);
        }
    }
    staleness_samples
}

/// (mean, p50, p95, max) over staleness samples.
fn staleness_summary(samples: &[u64]) -> (u64, u64, u64, u64) {
    assert!(!samples.is_empty(), "expected samples!");
    let mut v = samples.to_vec();
    v.sort_unstable();
    let mean = v.iter().sum::<u64>() / v.len() as u64;
    (mean, v[v.len() / 2], v[v.len() * 95 / 100], *v.last().unwrap())
}

fn print_staleness_stats(
    adaptive: &[u64],
    fixed: &[u64],
    adaptive_counters: CacheCounters,
    fixed_counters: CacheCounters,
    adaptive_refreshes: u64,
    fixed_refreshes: u64,
    window: Duration,
) {
    let (a_mean, a_p50, a_p95, a_max) = staleness_summary(adaptive);
    let (f_mean, f_p50, f_p95, f_max) = staleness_summary(fixed);
    println!(
        "staleness of served rows (ms), {}/{} adaptive/fixed samples:",
        adaptive.len(),
        fixed.len()
    );
    println!("  adaptive: mean {a_mean} p50 {a_p50} p95 {a_p95} max {a_max}");
    println!("  fixed:    mean {f_mean} p50 {f_p50} p95 {f_p95} max {f_max}");
    if let Some(reduction) = (f_mean.saturating_sub(a_mean) * 100).checked_div(f_mean) {
        println!("  mean staleness reduction: {reduction}%");
    }
    println!("sampling window runtime: {window:.1?}");
    println!(
        "refreshes during the sampling window ({N_KEYS} entries each): \
         adaptive {adaptive_refreshes}, fixed {fixed_refreshes}"
    );
    println!(
        "total client reads: adaptive {}, fixed {}",
        adaptive_counters.reads(),
        fixed_counters.reads()
    );
}

/// Under mixed churn, an adaptive cache's per-key refresh periods differentiate by change rate
/// and settle back to the configured period once the churn stops. Along the way, samples the
/// staleness of rows served by this cache and by an identical non-adaptive cache, and prints a
/// comparison; run with `--no-capture` to see it.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(mysql)]
async fn adaptive_mixed_churn_differentiates_periods() {
    init_test_logging();

    let test_name = derive_test_name();
    mysql_helpers::recreate_database(&test_name).await;

    let upstream_opts = mysql_helpers::upstream_config().db_name(Some(&test_name));
    let mut upstream = mysql_async::Conn::new(upstream_opts.clone()).await.unwrap();

    // Two identical tables churned in lockstep: one behind the adaptive cache, one behind the
    // non-adaptive control cache used for the staleness and refresh-count comparison.
    create_counters(&mut upstream, "counters_adaptive", N_KEYS).await;
    create_counters(&mut upstream, "counters_fixed", N_KEYS).await;

    let (readyset_opts, _readyset_handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .fallback(true)
        // Keep the load cap out of the way; this test is about per-key adaptation.
        .shallow_adaptive_max_extra_load_percent(10_000)
        .build::<MySQLAdapter>()
        .await;
    let mut readyset = mysql_async::Conn::new(readyset_opts.clone()).await.unwrap();
    readyset
        .query_drop(format!("USE {test_name}"))
        .await
        .unwrap();

    readyset
        .query_drop(cache_ddl("counters_adaptive", true))
        .await
        .unwrap();
    readyset
        .query_drop(cache_ddl("counters_fixed", false))
        .await
        .unwrap();
    let caches: Vec<(String, bool)> = readyset
        .query("SELECT query_id, adaptive FROM readyset.shallow_caches")
        .await
        .unwrap();
    assert_eq!(caches.len(), 2);
    let adaptive_id = caches.iter().find(|(_, a)| *a).unwrap().0.clone();
    let fixed_id = caches.iter().find(|(_, a)| !*a).unwrap().0.clone();

    // Warm the adaptive cache one churn class at a time to learn which entry ids belong to
    // which class.
    let mut seen = HashSet::new();
    let fast_entries = warm_class(
        &mut readyset,
        "counters_adaptive",
        &adaptive_id,
        1..=N_FAST,
        &mut seen,
    )
    .await;
    let slow_entries = warm_class(
        &mut readyset,
        "counters_adaptive",
        &adaptive_id,
        N_FAST + 1..=N_FAST + N_SLOW,
        &mut seen,
    )
    .await;
    let static_entries = warm_class(
        &mut readyset,
        "counters_adaptive",
        &adaptive_id,
        N_FAST + N_SLOW + 1..=N_KEYS,
        &mut seen,
    )
    .await;
    warm_class(
        &mut readyset,
        "counters_fixed",
        &fixed_id,
        1..=N_KEYS,
        &mut HashSet::new(),
    )
    .await;

    // Every entry starts at the configured period.
    let periods = entry_periods(&mut readyset, &adaptive_id).await;
    assert_eq!(periods.len(), N_KEYS as usize);
    assert!(
        periods
            .values()
            .all(|p| *p == Some(CONFIGURED_PERIOD_MS)),
        "all entries should start at the configured period"
    );

    let stop = Arc::new(AtomicBool::new(false));
    // Fast keys: every refresh sees a change until the period nears the floor.
    let fast_churn = spawn_churn(
        upstream_opts.clone(),
        &["counters_adaptive", "counters_fixed"],
        1,
        N_FAST,
        Duration::from_millis(150),
        stop.clone(),
    );
    // Slow keys: periods walk toward roughly half the update cadence.
    let slow_churn = spawn_churn(
        upstream_opts.clone(),
        &["counters_adaptive", "counters_fixed"],
        N_FAST + 1,
        N_FAST + N_SLOW,
        Duration::from_millis(900),
        stop.clone(),
    );

    // While the churn runs, the per-key periods differentiate by change rate. Static keys hold
    // the configured period exactly: an unchanged refresh can only grow the period, and growth
    // clamps there.
    fn differentiated(
        periods: &HashMap<String, Option<u64>>,
        fast: &HashSet<String>,
        slow: &HashSet<String>,
        static_: &HashSet<String>,
    ) -> bool {
        let period = |e: &String| periods.get(e).copied().flatten();
        let median = |class: &HashSet<String>| {
            let mut v: Vec<u64> = class.iter().filter_map(&period).collect();
            v.sort_unstable();
            v.get(v.len() / 2).copied()
        };
        fast.iter().all(|e| period(e).is_some_and(|p| p <= 400))
            && slow
                .iter()
                .all(|e| period(e).is_some_and(|p| p < CONFIGURED_PERIOD_MS))
            && static_
                .iter()
                .all(|e| period(e) == Some(CONFIGURED_PERIOD_MS))
            && median(fast) < median(slow)
    }
    eventually!(
        attempts: 60,
        sleep: Duration::from_millis(500),
        message: "per-key periods did not differentiate by change rate",
        {
            let periods = entry_periods(&mut readyset, &adaptive_id).await;
            differentiated(&periods, &fast_entries, &slow_entries, &static_entries)
        }
    );

    // Run a sustained read workload against both caches for 10 seconds while the churn
    // continues: parallel readers on their own connections sampling random fast keys.
    let counters_before = cache_counters(&mut readyset).await;
    let sampling_start = Instant::now();
    let readers: Vec<_> = (0..READERS)
        .map(|_| tokio::spawn(sample_staleness(readyset_opts.clone(), test_name.clone())))
        .collect();
    let mut adaptive_staleness = Vec::new();
    let mut fixed_staleness = Vec::new();
    for reader in readers {
        let (adaptive, fixed) = reader.await.unwrap();
        adaptive_staleness.extend(adaptive);
        fixed_staleness.extend(fixed);
    }
    let sampling_elapsed = sampling_start.elapsed();
    let counters_after = cache_counters(&mut readyset).await;
    print_staleness_stats(
        &adaptive_staleness,
        &fixed_staleness,
        counters_after[&adaptive_id],
        counters_after[&fixed_id],
        counters_after[&adaptive_id].refreshes - counters_before[&adaptive_id].refreshes,
        counters_after[&fixed_id].refreshes - counters_before[&fixed_id].refreshes,
        sampling_elapsed,
    );

    stop.store(true, Ordering::Relaxed);
    fast_churn.await.unwrap();
    slow_churn.await.unwrap();

    // Served data converges on the final upstream values once the churn stops.
    let (expected,): (i64,) = upstream
        .query_first("SELECT val FROM counters_adaptive WHERE id = 1")
        .await
        .unwrap()
        .unwrap();
    eventually!({
        let (val, _): (i64, u64) = readyset
            .query_first("SELECT val, modified_ms FROM counters_adaptive WHERE id = 1")
            .await
            .unwrap()
            .unwrap();
        val == expected
    });

    // With the churn stopped, every key stretches back out to the configured period.
    eventually!(
        attempts: 60,
        sleep: Duration::from_millis(500),
        message: "periods did not settle back to the configured period",
        {
            entry_periods(&mut readyset, &adaptive_id)
                .await
                .values()
                .all(|p| *p == Some(CONFIGURED_PERIOD_MS))
        }
    );

    shutdown_tx.shutdown().await;
}

/// With most keys churning, the adaptive load cap engages: the over-cap flag trips, aggregate
/// refresh load stays bounded near the cap, and periods don't collapse to the floor wholesale.
/// A parallel read workload runs against the churned keys throughout, and a summary of served
/// staleness, refreshes, and reads is printed; run with `--no-capture` to see it.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(mysql)]
async fn adaptive_load_cap_bounds_refresh_load() {
    init_test_logging();

    /// Keys churned fast; the rest stay static.
    const N_CHURNED: u64 = 800;

    let test_name = derive_test_name();
    mysql_helpers::recreate_database(&test_name).await;

    let upstream_opts = mysql_helpers::upstream_config().db_name(Some(&test_name));
    let mut upstream = mysql_async::Conn::new(upstream_opts.clone()).await.unwrap();
    create_counters(&mut upstream, "counters", N_KEYS).await;

    // Cap the extra refresh load at 50% over baseline. With 800 of 1000 keys churning, the cap
    // engages once the churning keys shrink to about 60% of the configured period, only a few
    // nudges in and far from the 10% floor.
    const MAX_EXTRA_LOAD_PERCENT: u64 = 50;
    let (readyset_opts, _readyset_handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .fallback(true)
        .shallow_adaptive_max_extra_load_percent(MAX_EXTRA_LOAD_PERCENT)
        .build::<MySQLAdapter>()
        .await;
    let mut readyset = mysql_async::Conn::new(readyset_opts.clone()).await.unwrap();
    readyset
        .query_drop(format!("USE {test_name}"))
        .await
        .unwrap();
    readyset
        .query_drop(cache_ddl("counters", true))
        .await
        .unwrap();
    let (query_id,): (String,) = readyset
        .query_first("SELECT query_id FROM readyset.shallow_caches")
        .await
        .unwrap()
        .unwrap();

    let mut seen = HashSet::new();
    warm_class(&mut readyset, "counters", &query_id, 1..=N_KEYS, &mut seen).await;

    // Churn most keys while parallel readers hit random churned keys, mimicking a busy cache
    // whose hot working set keeps changing.
    let counters_before = cache_counters(&mut readyset).await;
    let stop = Arc::new(AtomicBool::new(false));
    let window_start = Instant::now();
    let churn = spawn_churn(
        upstream_opts.clone(),
        &["counters"],
        1,
        N_CHURNED,
        Duration::from_millis(150),
        stop.clone(),
    );
    let readers: Vec<_> = (0..READERS)
        .map(|_| {
            tokio::spawn(sample_counters(
                readyset_opts.clone(),
                test_name.clone(),
                N_CHURNED,
                stop.clone(),
            ))
        })
        .collect();

    async fn load_stats(readyset: &mut mysql_async::Conn) -> (u64, u64, bool) {
        let rows: Vec<(Option<u64>, Option<u64>, Option<bool>)> = readyset
            .query(
                "SELECT load_actual_ppm, load_baseline_ppm, over_cap \
                 FROM readyset.shallow_cache_refresh_stats",
            )
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        (rows[0].0.unwrap(), rows[0].1.unwrap(), rows[0].2.unwrap())
    }

    // The over-cap flag flickers at equilibrium (over the cap, periods grow, load dips back
    // under), so poll for it.
    eventually!(attempts: 60, sleep: Duration::from_millis(250), {
        load_stats(&mut readyset).await.2
    });

    // While the churn continues, aggregate load stays bounded rather than climbing toward the
    // level a floor-pinned cache would sustain (about 8x baseline here). The equilibrium
    // oscillates coarsely above the 1.5x cap: a single nudge next to the floor doubles or
    // halves that key's contribution, and contributions are weighted by real execution time,
    // so instantaneous load can overshoot the cap by a factor of a few.
    let mut load_samples = Vec::new();
    for _ in 0..10 {
        let (actual, baseline, _) = load_stats(&mut readyset).await;
        assert!(baseline > 0, "baseline load should be nonzero");
        assert!(
            actual <= baseline * 4,
            "load should stay bounded near the cap: actual {actual} baseline {baseline}"
        );
        load_samples.push((actual, baseline));
        sleep(Duration::from_millis(300)).await;
    }

    // The cap bounds the aggregate, not each key: keys that refresh first shrink first, so
    // some keys can reach the floor while the rest hold higher periods. A floor-pinned key
    // spends about 10x its baseline share, so the capped budget only sustains a fraction of
    // the churned keys there.
    let periods = entry_periods(&mut readyset, &query_id).await;
    let at_floor = periods
        .values()
        .filter(|p| p.unwrap() <= CONFIGURED_PERIOD_MS / 10)
        .count();
    assert!(
        at_floor <= (N_CHURNED / 4) as usize,
        "the cap should keep most keys above the floor period: {at_floor} at the floor"
    );

    stop.store(true, Ordering::Relaxed);
    churn.await.unwrap();
    let mut staleness_samples = Vec::new();
    for reader in readers {
        staleness_samples.extend(reader.await.unwrap());
    }
    let window_elapsed = window_start.elapsed();
    let counters_after = cache_counters(&mut readyset).await;

    let (mean, p50, p95, max) = staleness_summary(&staleness_samples);
    println!(
        "staleness of served rows (ms), {} samples: mean {mean} p50 {p50} p95 {p95} max {max}",
        staleness_samples.len()
    );
    println!("workload window runtime: {window_elapsed:.1?}");
    println!(
        "refreshes during the window ({N_KEYS} entries): {}",
        counters_after[&query_id].refreshes - counters_before[&query_id].refreshes
    );
    println!("total client reads: {}", counters_after[&query_id].reads());
    let cap_percents: Vec<u64> = load_samples
        .iter()
        .map(|(actual, baseline)| {
            let cap = baseline + baseline * MAX_EXTRA_LOAD_PERCENT / 100;
            actual * 100 / cap
        })
        .collect();
    println!(
        "load relative to the cap over {} samples: min {}% mean {}% max {}%",
        cap_percents.len(),
        cap_percents.iter().min().unwrap(),
        cap_percents.iter().sum::<u64>() / cap_percents.len() as u64,
        cap_percents.iter().max().unwrap()
    );

    shutdown_tx.shutdown().await;
}
