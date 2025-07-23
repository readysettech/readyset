use criterion::{black_box, criterion_group, criterion_main, Bencher, Criterion};
use pprof::criterion::{Output, PProfProfiler};
use readyset::query_logger::QueryLogger;
use readyset_client::query::QueryId;
use readyset_client_metrics::{EventType, QueryExecutionEvent, QueryLogMode, SqlQueryType};
use readyset_client_metrics::{QueryIdWrapper, ReadysetExecutionEvent};
use readyset_server::PrometheusBuilder;
use readyset_sql::ast::SqlQuery;
use readyset_sql::Dialect;
use readyset_sql_passes::adapter_rewrites::AdapterRewriteParams;
use std::sync::Arc;
use std::time::Duration;

const DIALECT: Dialect = Dialect::PostgreSQL;

fn create_event(
    with_query: bool,
    with_query_id: bool,
    with_parse_duration: bool,
    with_upstream_duration: bool,
    with_readyset_event: bool,
) -> QueryExecutionEvent {
    let schema_search_path = vec!["my_schema".into()];
    let (query, query_id) = if with_query {
        let stmt = "select max(t2.l) from t1 join t2 on t1.id = t2.id where t1.id in (1, 2, 3) group by t1.id";
        let sql_stmt = readyset_sql_parsing::parse_query(DIALECT, stmt).unwrap();
        let query = match sql_stmt.clone() {
            SqlQuery::Select(s) => s,
            _ => panic!("Error parsing query"),
        };
        let query_id = if with_query_id {
            QueryIdWrapper::Calculated(QueryId::from_select(&query, &schema_search_path))
        } else {
            QueryIdWrapper::Uncalculated(schema_search_path)
        };

        (Some(Arc::new(sql_stmt)), query_id)
    } else {
        (None, QueryIdWrapper::None)
    };

    let readyset_event = if with_readyset_event {
        Some(ReadysetExecutionEvent::CacheRead {
            cache_name: "my_cache".into(),
            num_keys: 100,
            cache_misses: 10,
            duration: Duration::from_millis(100),
        })
    } else {
        None
    };

    QueryExecutionEvent {
        event: EventType::Query,
        sql_type: SqlQueryType::Read,
        query,
        query_id,
        parse_duration: if with_parse_duration {
            Some(Duration::from_millis(100))
        } else {
            None
        },
        upstream_duration: if with_upstream_duration {
            Some(Duration::from_millis(100))
        } else {
            None
        },
        readyset_event,
        noria_error: None,
        destination: None,
    }
}

fn event_logging_bench(c: &mut Criterion, mode: QueryLogMode) {
    let adapter_rewrite_params = AdapterRewriteParams::new(DIALECT);

    let mut logger = QueryLogger::new(mode, DIALECT, adapter_rewrite_params);

    let test_cases = vec![
        ("no-query", false, false, false, false, false),
        ("no-query-id", true, false, false, false, false),
        ("cache-read-adhoc", true, true, true, false, true),
        ("cache-read-prepared", true, true, false, false, true),
        ("cache-attempt-then-upstream", true, true, true, true, true),
        ("proxy-upstream", true, true, true, true, false),
    ];

    let mut group = c.benchmark_group(format!("query_logger_-{mode:?}"));
    for (
        name,
        with_query,
        with_query_id,
        with_parse_duration,
        with_upstream_duration,
        with_readyset_event,
    ) in test_cases
    {
        group.bench_with_input(
            name,
            &create_event(
                with_query,
                with_query_id,
                with_parse_duration,
                with_upstream_duration,
                with_readyset_event,
            ),
            |b: &mut Bencher, event| {
                b.iter(|| {
                    logger.handle_event(event);
                    black_box(())
                });
            },
        );
    }
}

fn query_logger_bench(c: &mut Criterion) {
    // tokio runtime is needed for the readyset Prometheus recorder.
    let rt = tokio::runtime::Runtime::new().expect("failed to create runtime");
    let _guard = rt.enter();

    let recorder = PrometheusBuilder::new().build_recorder();
    metrics::set_global_recorder(recorder).expect("failed to install Prometheus recorder");

    event_logging_bench(c, QueryLogMode::Enabled);
    event_logging_bench(c, QueryLogMode::Verbose);
}

// Instead of running with the Standard Criterion timer profile, plug-in the
// flamegraph output of the pprof-rs profiler.[0]
//
// [0] https://bheisler.github.io/criterion.rs/book/user_guide/profiling.html
fn flamegraphs_profiler() -> Criterion {
    Criterion::default().with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)))
}

criterion_group!(
    name = benches;
    config = flamegraphs_profiler();
    targets = query_logger_bench,
);
criterion_main!(benches);
