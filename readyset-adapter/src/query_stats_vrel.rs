use metrics_exporter_prometheus::Distribution;
use quanta::Instant;

use readyset_client_metrics::recorded;
use readyset_data::{DfType, DfValue};
use readyset_metrics::metrics_handle;
use readyset_schema::bind_vrel;
use readyset_schema::virtual_relation::{VrelContext, VrelRead, VrelRows};
use readyset_sql::DialectDisplay;
use readyset_util::scheduler_potentially_yield;

use crate::query_status_cache::QueryStatusCache;

fn extract_summary(dist: &Distribution) -> (f64, f64, f64, f64, f64, f64, f64, f64) {
    if let Distribution::Summary(summary, _quantiles, sum) = dist {
        let snapshot = summary.snapshot(Instant::now());
        (
            *sum,
            snapshot.quantile(0.0).unwrap_or(0.0),
            snapshot.quantile(1.0).unwrap_or(0.0),
            snapshot.quantile(0.5).unwrap_or(0.0),
            snapshot.quantile(0.9).unwrap_or(0.0),
            snapshot.quantile(0.95).unwrap_or(0.0),
            snapshot.quantile(0.99).unwrap_or(0.0),
            snapshot.quantile(0.999).unwrap_or(0.0),
        )
    } else {
        Default::default()
    }
}

fn query_stats_read(ctx: &VrelContext, database_type: &'static str) -> VrelRead {
    let dialect = ctx.dialect;
    let query_status_cache = ctx.query_status_cache.downcast_ref::<QueryStatusCache>();

    Box::pin(async move {
        let Some(handle) = metrics_handle() else {
            return Ok(Box::new(std::iter::empty()) as VrelRows);
        };
        let database_type = [("database_type", database_type)];
        let [counts] = handle.counters_by_label(
            [recorded::QUERY_LOG_EXECUTION_COUNT],
            "query_id",
            database_type,
        );
        let [exec_times] = handle.distributions_by_label(
            [recorded::QUERY_LOG_EXECUTION_TIME],
            "query_id",
            database_type,
        );
        let [last_exec] = handle.gauges_by_label(
            [recorded::QUERY_LOG_LAST_EXECUTION_EPOCH_S],
            "query_id",
            database_type,
        );

        let mut entries: Vec<Vec<DfValue>> = Vec::new();
        for (query_id, count) in counts.iter() {
            scheduler_potentially_yield!();
            let query = query_status_cache
                .and_then(|qs| Some(qs.query(query_id)?.display(dialect).to_string()));
            let last_exec_s = last_exec.get(query_id);
            let (sum, min, max, p50, p90, p95, p99, p999) =
                extract_summary(exec_times.get(query_id));
            let avg = if count > 0 { sum / count as f64 } else { 0.0 };

            entries.push(vec![
                DfValue::from(query_id),
                query.map(DfValue::from).unwrap_or(DfValue::None),
                DfValue::UnsignedInt(count),
                DfValue::Double(sum),
                DfValue::Double(avg),
                DfValue::Double(min),
                DfValue::Double(max),
                DfValue::Double(p50),
                DfValue::Double(p90),
                DfValue::Double(p95),
                DfValue::Double(p99),
                DfValue::Double(p999),
                DfValue::UnsignedInt(last_exec_s as u64),
            ]);
        }

        let rows: VrelRows = Box::new(entries.into_iter());
        Ok(rows)
    })
}

const QUERY_STATS_SCHEMA: &[(&str, DfType)] = &[
    ("query_id", DfType::DEFAULT_TEXT),
    ("query", DfType::DEFAULT_TEXT),
    ("total_count", DfType::UnsignedBigInt),
    ("total_us", DfType::Double),
    ("avg_us", DfType::Double),
    ("min_us", DfType::Double),
    ("max_us", DfType::Double),
    ("p50_us", DfType::Double),
    ("p90_us", DfType::Double),
    ("p95_us", DfType::Double),
    ("p99_us", DfType::Double),
    ("p999_us", DfType::Double),
    ("last_executed_s", DfType::UnsignedBigInt),
];

fn cached_query_stats_read(ctx: &VrelContext) -> VrelRead {
    query_stats_read(ctx, "readyset")
}
bind_vrel!(
    cached_query_stats,
    QUERY_STATS_SCHEMA,
    cached_query_stats_read
);

fn upstream_query_stats_read(ctx: &VrelContext) -> VrelRead {
    query_stats_read(ctx, "upstream")
}
bind_vrel!(
    upstream_query_stats,
    QUERY_STATS_SCHEMA,
    upstream_query_stats_read
);
