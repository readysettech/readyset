use metrics_exporter_prometheus::{Distribution, LabelSet};
use quanta::Instant;

use readyset_client_metrics::recorded;
use readyset_data::{DfType, DfValue};
use readyset_schema::bind_vrel;
use readyset_schema::virtual_relation::{VrelContext, VrelRead, VrelRows};
use readyset_server::metrics::get_global_recorder;
use readyset_sql::DialectDisplay;

use crate::query_status_cache::QueryStatusCache;

fn label_value(labels: &LabelSet, key: &str) -> Option<String> {
    let prefix = format!("{key}=\"");
    for tag in labels.to_strings() {
        if let Some(rest) = tag.strip_prefix(&prefix) {
            return Some(rest.trim_end_matches('"').to_string());
        }
    }
    None
}

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
        let handle = match get_global_recorder() {
            Some(r) => r.handle(),
            None => {
                let rows: VrelRows = Box::new(std::iter::empty());
                return Ok(rows);
            }
        };

        let counts = handle.counters(Some(|k: &str| k == recorded::QUERY_LOG_EXECUTION_COUNT));
        let exec_times =
            handle.distributions(Some(|k: &str| k == recorded::QUERY_LOG_EXECUTION_TIME));
        let last_exec_times = handle.gauges(Some(|k: &str| {
            k == recorded::QUERY_LOG_LAST_EXECUTION_EPOCH_S
        }));

        let counts = counts.get(recorded::QUERY_LOG_EXECUTION_COUNT);
        let exec_times = exec_times.get(recorded::QUERY_LOG_EXECUTION_TIME);
        let last_exec_times = last_exec_times.get(recorded::QUERY_LOG_LAST_EXECUTION_EPOCH_S);

        let entries: Vec<_> = counts
            .into_iter()
            .flat_map(|m| m.iter())
            .filter(|(labels, _)| {
                label_value(labels, "database_type").as_deref() == Some(database_type)
            })
            .filter_map(|(labels, count)| {
                let query_id = label_value(labels, "query_id")?;
                let query = query_status_cache.and_then(|qs| {
                    let q = qs.query(&query_id)?;
                    Some(q.display(dialect).to_string())
                });
                let last_exec_s = last_exec_times
                    .and_then(|g| g.get(labels))
                    .copied()
                    .unwrap_or(0.0);
                let (sum, min, max, p50, p90, p95, p99, p999) = exec_times
                    .and_then(|d| d.get(labels))
                    .map(extract_summary)
                    .unwrap_or_default();
                let avg = if *count > 0 { sum / *count as f64 } else { 0.0 };

                Some(vec![
                    DfValue::from(query_id),
                    query.map(DfValue::from).unwrap_or(DfValue::None),
                    DfValue::UnsignedInt(*count),
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
                ])
            })
            .collect();

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
