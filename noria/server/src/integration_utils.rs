use std::env;
use std::fmt::Display;
use std::sync::Arc;
use std::time::Duration;

use dataflow::{DurabilityMode, PersistenceParameters};
use noria::consensus::{Authority, LocalAuthority, LocalAuthorityStore};
use noria::metrics::client::MetricsClient;
use noria::metrics::{DumpedMetric, DumpedMetricValue, MetricsDump};
use noria_errors::{ReadySetError, ReadySetResult};

use crate::metrics::{
    get_global_recorder, install_global_recorder, CompositeMetricsRecorder, MetricsRecorder,
    NoriaMetricsRecorder,
};
use crate::{Builder, Handle, ReuseConfigType};

// Settle time must be longer than the leader state check interval
// when using a local authority.
pub const DEFAULT_SETTLE_TIME_MS: u64 = 1500;
pub const DEFAULT_SHARDING: usize = 2;

/// PersistenceParameters with a log_name on the form of `prefix` + timestamp,
/// avoiding collisions between separate test runs (in case an earlier panic causes clean-up to
/// fail).
pub fn get_persistence_params(prefix: &str) -> PersistenceParameters {
    PersistenceParameters {
        mode: DurabilityMode::DeleteOnExit,
        db_filename_prefix: String::from(prefix),
        ..Default::default()
    }
}

/// Builds a local worker.
pub async fn start_simple(prefix: &str) -> Handle {
    build(prefix, Some(DEFAULT_SHARDING), None).await
}

#[allow(dead_code)]
/// Builds a local worker without sharding.
pub async fn start_simple_unsharded(prefix: &str) -> Handle {
    build(prefix, None, None).await
}

pub async fn start_simple_reuse_unsharded(prefix: &str) -> Handle {
    let authority_store = Arc::new(LocalAuthorityStore::new());
    let authority = Arc::new(Authority::from(LocalAuthority::new_with_store(
        authority_store,
    )));
    let mut builder = Builder::for_tests();
    builder.set_reuse(Some(ReuseConfigType::Finkelstein));
    builder.set_persistence(get_persistence_params(prefix));
    builder.set_allow_topk(true);
    builder.set_allow_paginate(true);
    builder.set_sharding(None);
    builder.start_local_custom(authority.clone()).await.unwrap()
}

/// Builds a custom local worker with log prefix `prefix`,
/// with optional sharding and eviction.
pub async fn build(
    prefix: &str,
    sharding: Option<usize>,
    eviction: Option<(usize, Duration)>,
) -> Handle {
    let authority_store = Arc::new(LocalAuthorityStore::new());
    build_custom(
        prefix,
        sharding,
        true,
        Arc::new(Authority::from(LocalAuthority::new_with_store(
            authority_store,
        ))),
        None,
        false,
        eviction,
    )
    .await
}

/// Builds a custom local worker.
pub async fn build_custom(
    prefix: &str,
    sharding: Option<usize>,
    controller: bool,
    authority: Arc<Authority>,
    region: Option<String>,
    reader_only: bool,
    eviction: Option<(usize, Duration)>,
) -> Handle {
    readyset_tracing::init_test_logging();
    let mut builder = Builder::for_tests();
    builder.set_sharding(sharding);
    builder.set_persistence(get_persistence_params(prefix));
    // don't return unsupported errors for topk in queries
    builder.set_allow_topk(true);
    builder.set_allow_paginate(true);

    if reader_only {
        builder.as_reader_only();
    }

    if region.is_some() {
        builder.set_region(region.unwrap());
    }

    if let Some((limit, period)) = eviction {
        builder.set_aggressively_update_state_sizes(true);
        builder.set_memory_limit(limit, period);
    }

    if controller {
        builder.start_local_custom(authority.clone()).await.unwrap()
    } else {
        builder.start(authority.clone()).await.unwrap()
    }
}

pub fn get_settle_time() -> Duration {
    let settle_time: u64 = match env::var("SETTLE_TIME") {
        Ok(value) => value.parse().unwrap(),
        Err(_) => DEFAULT_SETTLE_TIME_MS,
    };

    Duration::from_millis(settle_time)
}

/// Sleeps for either DEFAULT_SETTLE_TIME_MS milliseconds, or
/// for the value given through the SETTLE_TIME environment variable.
pub async fn sleep() {
    tokio::time::sleep(get_settle_time()).await;
}

/// Initializes the metrics recorder if it has not been initialized yet. This
/// must be called before the server is started, otherwise it will fail to
/// register its metrics with the correct recorder, and none will be recorded
pub fn register_metric_recorder() {
    unsafe {
        if get_global_recorder().is_none() {
            let rec = CompositeMetricsRecorder::with_recorders(vec![MetricsRecorder::Noria(
                NoriaMetricsRecorder::new(),
            )]);
            install_global_recorder(rec).unwrap();
        }
    }
}

/// Creates the metrics client for a given local deployment.
pub async fn initialize_metrics(handle: &mut Handle) -> MetricsClient {
    let mut metrics_client = MetricsClient::new(handle.c.clone().unwrap()).unwrap();
    let res = metrics_client.reset_metrics().await;
    assert!(res.is_ok());

    metrics_client
}

/// Get the counter value for `metric` from the current process. If tests
/// are run in the same process this may include values from across several
/// tests.
pub fn get_counter(metric: &str, metrics_dump: &MetricsDump) -> f64 {
    let dumped_metric: &DumpedMetric = &metrics_dump.metrics.get(metric).unwrap()[0];

    if let DumpedMetricValue::Counter(v) = dumped_metric.value {
        v
    } else {
        panic!("{} is not a counter", metric);
    }
}

pub fn assert_table_not_found<T, S>(err: ReadySetResult<T>, table_name: S)
where
    S: Into<String> + Display,
{
    let table_name: String = table_name.into();
    match err {
        Err(ReadySetError::TableNotFound(name))
        | Err(ReadySetError::RpcFailed {
            source: box ReadySetError::TableNotFound(name),
            ..
        }) => assert_eq!(*name, table_name),
        _ => panic!("Expected table not found error for table {}", table_name),
    }
}

pub fn assert_view_not_found<T, S>(err: ReadySetResult<T>, view_name: S)
where
    S: Into<String> + Display,
{
    let view_name: String = view_name.into();
    match err {
        Err(ReadySetError::ViewNotFound(name))
        | Err(ReadySetError::ViewNotFoundInWorkers { name, .. })
        | Err(ReadySetError::RpcFailed {
            source: box ReadySetError::ViewNotFound(name),
            ..
        })
        | Err(ReadySetError::RpcFailed {
            source: box ReadySetError::ViewNotFoundInWorkers { name, .. },
            ..
        }) => {
            assert_eq!(*name, view_name)
        }
        _ => panic!("Expected view not found error for view {}", view_name),
    }
}

/// Retrieves the value of column of a row, by passing the column name and
/// the type.
#[macro_export(local_inner_macros)]
macro_rules! get_col {
    ($row:expr, $field:expr, $into_type:ty) => {
        $row.get($field)
            .and_then(|dt| <$into_type>::try_from(dt).ok())
            .unwrap()
    };
    ($row:expr, $field:expr) => {
        $row.get($field).unwrap()
    };
}
