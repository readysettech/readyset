use std::hash::Hash;

use metrics_exporter_prometheus::Distribution;
use readyset_client::query::QueryId;
use readyset_data::DfType;
use readyset_metrics::metrics_handle;
use readyset_shallow::{CacheEntryInfo, CacheInfo, CacheManager};
use readyset_sql::ast::TrxCachePolicy;
use readyset_sql::{DialectDisplay, ast::Relation};
use readyset_util::SizeOf;

use crate::bind_vrel;
use crate::virtual_relation::{VrelContext, VrelRead, VrelRows};

/// Trait for accessing shallow cache state in virtual relations.
pub trait ShallowInfo: Send + Sync {
    fn list_caches(&self, query_id: Option<QueryId>, name: Option<&Relation>) -> Vec<CacheInfo>;
    fn list_entries(&self, query_id: Option<QueryId>, limit: Option<usize>) -> Vec<CacheEntryInfo>;
}

impl<K, V> ShallowInfo for CacheManager<K, V>
where
    K: Clone + Hash + Eq + Send + Sync + SizeOf + 'static,
    V: Send + Sync + SizeOf + 'static,
{
    fn list_caches(&self, query_id: Option<QueryId>, name: Option<&Relation>) -> Vec<CacheInfo> {
        self.list_caches(query_id, name)
    }

    fn list_entries(&self, query_id: Option<QueryId>, limit: Option<usize>) -> Vec<CacheEntryInfo> {
        self.list_entries(query_id, limit)
    }
}

const SHALLOW_CACHES_SCHEMA: &[(&str, DfType)] = &[
    ("query_id", DfType::DEFAULT_TEXT),
    ("name", DfType::DEFAULT_TEXT),
    ("query", DfType::DEFAULT_TEXT),
    ("ttl_ms", DfType::UnsignedBigInt),
    ("refresh_ms", DfType::UnsignedBigInt),
    ("coalesce_ms", DfType::UnsignedBigInt),
    ("always", DfType::Bool),
    ("until_write", DfType::Bool),
    ("schedule", DfType::Bool),
    ("adaptive", DfType::Bool),
    ("hits", DfType::UnsignedBigInt),
    ("misses", DfType::UnsignedBigInt),
    ("refreshes", DfType::UnsignedBigInt),
    ("wasted_refreshes", DfType::UnsignedBigInt),
    ("coalesces", DfType::UnsignedBigInt),
];

fn shallow_caches_read(ctx: &VrelContext) -> VrelRead {
    let dialect = ctx.dialect;
    let caches = ctx.shallow.list_caches(None, None);
    Box::pin(async move {
        let [hits, misses, refreshes, wasted, coalesces] = metrics_handle()
            .map(|h| {
                h.counters_by_label(
                    [
                        metric::SHALLOW_HIT,
                        metric::SHALLOW_MISS,
                        metric::SHALLOW_REFRESH,
                        metric::SHALLOW_REFRESH_WASTED,
                        metric::SHALLOW_COALESCE_SUCCESS,
                    ],
                    "query_id",
                    [],
                )
            })
            .unwrap_or_default();

        let rows: VrelRows = Box::new(caches.into_iter().map(move |cache| {
            let query_id = cache.query_id.to_string();
            vec![
                query_id.clone().into(),
                cache.name.map(|n| n.display_unquoted().to_string()).into(),
                cache.query.display(dialect).to_string().into(),
                cache.ttl_ms.into(),
                cache.refresh_ms.into(),
                cache.coalesce_ms.into(),
                matches!(cache.trx_cache_policy, TrxCachePolicy::Always).into(),
                matches!(cache.trx_cache_policy, TrxCachePolicy::UntilWrite).into(),
                cache.schedule.into(),
                cache.adaptive.into(),
                hits.get(&query_id).into(),
                misses.get(&query_id).into(),
                refreshes.get(&query_id).into(),
                wasted.get(&query_id).into(),
                coalesces.get(&query_id).into(),
            ]
        }));
        Ok(rows)
    })
}
bind_vrel!(shallow_caches, SHALLOW_CACHES_SCHEMA, shallow_caches_read);

const SHALLOW_CACHE_REFRESH_STATS_SCHEMA: &[(&str, DfType)] = &[
    ("query_id", DfType::DEFAULT_TEXT),
    ("load_actual_ppm", DfType::UnsignedBigInt),
    ("load_baseline_ppm", DfType::UnsignedBigInt),
    ("over_cap", DfType::Bool),
    ("scheduler_queue_len", DfType::UnsignedBigInt),
];

fn shallow_cache_refresh_stats_read(ctx: &VrelContext) -> VrelRead {
    let caches = ctx.shallow.list_caches(None, None);
    Box::pin(async move {
        let rows: VrelRows = Box::new(caches.into_iter().map(move |cache| {
            vec![
                cache.query_id.to_string().into(),
                cache.load_actual_ppm.into(),
                cache.load_baseline_ppm.into(),
                cache.over_cap.into(),
                cache.scheduler_queue_len.into(),
            ]
        }));
        Ok(rows)
    })
}
bind_vrel!(
    shallow_cache_refresh_stats,
    SHALLOW_CACHE_REFRESH_STATS_SCHEMA,
    shallow_cache_refresh_stats_read
);

fn summary_stats(dist: &Distribution) -> (u64, u64) {
    match dist {
        Distribution::Summary(summary, _quantiles, sum) => (summary.count() as u64, *sum as u64),
        _ => (0, 0),
    }
}

const SHALLOW_CACHE_COALESCE_STATS_SCHEMA: &[(&str, DfType)] = &[
    ("query_id", DfType::DEFAULT_TEXT),
    ("coalesce_timeouts", DfType::UnsignedBigInt),
    ("coalesce_aborts", DfType::UnsignedBigInt),
    ("total_us", DfType::UnsignedBigInt),
    ("avg_us", DfType::UnsignedBigInt),
    ("upstream_saved_total_us", DfType::UnsignedBigInt),
    ("upstream_saved_avg_us", DfType::UnsignedBigInt),
    ("timeout_total_us", DfType::UnsignedBigInt),
    ("timeout_avg_us", DfType::UnsignedBigInt),
    ("abort_total_us", DfType::UnsignedBigInt),
    ("abort_avg_us", DfType::UnsignedBigInt),
];

fn shallow_cache_coalesce_stats_read(ctx: &VrelContext) -> VrelRead {
    let caches = ctx.shallow.list_caches(None, None);
    Box::pin(async move {
        let Some(handle) = metrics_handle() else {
            return Ok(Box::new(std::iter::empty()) as VrelRows);
        };
        let [timeouts, aborts] = handle.counters_by_label(
            [
                metric::SHALLOW_COALESCE_TIMEOUT,
                metric::SHALLOW_COALESCE_ABORT,
            ],
            "query_id",
            [],
        );
        let [waits, timeout_waits, abort_waits] = handle.distributions_by_label(
            [
                metric::SHALLOW_COALESCE_SUCCESS_WAIT,
                metric::SHALLOW_COALESCE_TIMEOUT_WAIT,
                metric::SHALLOW_COALESCE_ABORT_WAIT,
            ],
            "query_id",
            [],
        );
        let [upstream_times] = handle.distributions_by_label(
            [metric::QUERY_LOG_EXECUTION_TIME],
            "query_id",
            [("database_type", "upstream")],
        );

        let avg = |total: u64, count: u64| total.checked_div(count).unwrap_or(0);

        let rows: VrelRows = Box::new(caches.into_iter().map(move |cache| {
            let query_id = cache.query_id.to_string();

            let (coalesce_count, wait_total_us) = summary_stats(waits.get(&query_id));
            let (timeout_count, timeout_total_us) = summary_stats(timeout_waits.get(&query_id));
            let (abort_count, abort_total_us) = summary_stats(abort_waits.get(&query_id));
            let (upstream_count, upstream_total_us) = summary_stats(upstream_times.get(&query_id));

            let wait_avg_us = avg(wait_total_us, coalesce_count);
            let avg_upstream_us = avg(upstream_total_us, upstream_count);

            let upstream_saved_total_us = coalesce_count
                .saturating_mul(avg_upstream_us)
                .saturating_sub(wait_total_us);
            let upstream_saved_avg_us = avg_upstream_us.saturating_sub(wait_avg_us);

            vec![
                query_id.clone().into(),
                timeouts.get(&query_id).into(),
                aborts.get(&query_id).into(),
                wait_total_us.into(),
                wait_avg_us.into(),
                upstream_saved_total_us.into(),
                upstream_saved_avg_us.into(),
                timeout_total_us.into(),
                avg(timeout_total_us, timeout_count).into(),
                abort_total_us.into(),
                avg(abort_total_us, abort_count).into(),
            ]
        }));
        Ok(rows)
    })
}
bind_vrel!(
    shallow_cache_coalesce_stats,
    SHALLOW_CACHE_COALESCE_STATS_SCHEMA,
    shallow_cache_coalesce_stats_read
);

const SHALLOW_CACHE_ENTRIES_SCHEMA: &[(&str, DfType)] = &[
    ("query_id", DfType::DEFAULT_TEXT),
    ("entry_id", DfType::DEFAULT_TEXT),
    ("last_accessed_ms", DfType::UnsignedBigInt),
    ("last_refreshed_ms", DfType::UnsignedBigInt),
    ("refresh_time_ms", DfType::UnsignedBigInt),
    ("refresh_period_ms", DfType::UnsignedBigInt),
    ("bytes", DfType::UnsignedBigInt),
    ("served", DfType::Bool),
];

fn shallow_cache_entries_read(ctx: &VrelContext) -> VrelRead {
    let entries = ctx.shallow.list_entries(None, None);
    Box::pin(async move {
        let rows: VrelRows = Box::new(entries.into_iter().map(move |entry| {
            vec![
                entry.query_id.to_string().into(),
                format!("{:016x}", entry.entry_id).into(),
                entry.last_accessed_ms.into(),
                entry.last_refreshed_ms.into(),
                entry.refresh_time_ms.into(),
                entry.refresh_period_ms.into(),
                (entry.bytes as u64).into(),
                entry.served.into(),
            ]
        }));
        Ok(rows)
    })
}
bind_vrel!(
    shallow_cache_entries,
    SHALLOW_CACHE_ENTRIES_SCHEMA,
    shallow_cache_entries_read
);
