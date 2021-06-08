use std::env;
use std::sync::Arc;
use std::time::Duration;

use dataflow::{DurabilityMode, PersistenceParameters};
use noria::consensus::LocalAuthority;
use noria::{
    metrics::client::MetricsClient,
    metrics::{DumpedMetric, DumpedMetricValue, MetricsDump},
};

use crate::metrics::{
    get_global_recorder_opt, install_global_recorder, BufferedRecorder, CompositeMetricsRecorder,
    MetricsRecorder, NoriaMetricsRecorder,
};
use crate::{Builder, Handle};

pub const DEFAULT_SETTLE_TIME_MS: u64 = 200;
pub const DEFAULT_SHARDING: usize = 2;

/// PersistenceParameters with a log_name on the form of `prefix` + timestamp,
/// avoiding collisions between separate test runs (in case an earlier panic causes clean-up to
/// fail).
pub fn get_persistence_params(prefix: &str) -> PersistenceParameters {
    let mut params = PersistenceParameters::default();
    params.mode = DurabilityMode::DeleteOnExit;
    params.log_prefix = String::from(prefix);
    params
}

/// Builds a local worker.
pub async fn start_simple(prefix: &str) -> Handle<LocalAuthority> {
    build(prefix, Some(DEFAULT_SHARDING), false).await
}

#[allow(dead_code)]
/// Builds a lock worker without sharding.
pub async fn start_simple_unsharded(prefix: &str) -> Handle<LocalAuthority> {
    build(prefix, None, false).await
}

#[allow(dead_code)]
/// Builds a local worker with DEFAULT_SHARDING shards and
/// logging.
pub async fn start_simple_logging(prefix: &str) -> Handle<LocalAuthority> {
    build(prefix, Some(DEFAULT_SHARDING), true).await
}

/// Builds a custom local worker with log prefix `prefix`,
/// with optional sharding and logging.
pub async fn build(prefix: &str, sharding: Option<usize>, log: bool) -> Handle<LocalAuthority> {
    build_custom(
        prefix,
        sharding,
        log,
        true,
        Arc::new(LocalAuthority::new()),
        None,
        false,
    )
    .await
}

/// Builds a custom local worker.
pub async fn build_custom(
    prefix: &str,
    sharding: Option<usize>,
    log: bool,
    controller: bool,
    authority: Arc<LocalAuthority>,
    region: Option<String>,
    reader_only: bool,
) -> Handle<LocalAuthority> {
    use crate::logger_pls;
    let mut builder = Builder::default();
    if log {
        builder.log_with(logger_pls());
    }
    builder.set_sharding(sharding);
    builder.set_persistence(get_persistence_params(prefix));

    if reader_only {
        builder.as_reader_only();
    }

    if region.is_some() {
        builder.set_region(region.unwrap());
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

/// Creates the metrics client for a given local deployment and initializes
/// the metrics recorder if it has not been initialized yet. Initializing the
/// metrics clears all previously recorded metrics. As such if tests are run
/// in parallel that depends on metrics, this may cause flaky metrics results.
pub async fn initialize_metrics(
    handle: &mut Handle<LocalAuthority>,
) -> MetricsClient<LocalAuthority> {
    unsafe {
        if get_global_recorder_opt().is_none() {
            let rec = CompositeMetricsRecorder::new();
            rec.add(MetricsRecorder::Noria(NoriaMetricsRecorder::new()));
            let bufrec = BufferedRecorder::new(rec, 1024);
            install_global_recorder(bufrec).unwrap();
        }
    }

    let mut metrics_client = MetricsClient::new(handle.c.clone().unwrap()).unwrap();
    let res = metrics_client.reset_metrics().await;
    assert!(!res.is_err());

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
