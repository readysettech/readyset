//! Data types representing metrics dumped from a running ReadySet instance

use std::borrow::Borrow;
use std::collections::HashMap;
use std::hash::Hash;

pub use metrics::Key;
use metrics_util::Histogram;
use serde::{Deserialize, Serialize};

/// A client for accessing readyset metrics for a deployment.
pub mod client;

/// Documents the set of metrics that are currently being recorded within
/// a ReadySet instance.
pub mod recorded {
    /// Counter: The number of times the adapter has started up. In standalone mode, this metric
    /// can be used to count the number of system startups.
    pub const READYSET_ADAPTER_STARTUPS: &str = "readyset_adapter_startups";

    /// Counter: The number of times the server has started up. In standalone mode, this metric
    /// should track [`READYSET_ADAPTER_STARTUPS`] exactly.
    pub const READYSET_SERVER_STARTUPS: &str = "readyset_server_startups";

    /// Counter: The number of lookup misses that occurred during replay
    /// requests. Recorded at the domain on every lookup miss during a
    /// replay request.
    ///
    /// | Tag | Description |
    /// | --- | ----------- |
    /// | cache_name | The name of the cache associated with this replay.
    pub const DOMAIN_REPLAY_MISSES: &str = "readyset_domain.replay_misses";

    /// Histogram: The time in microseconds that a domain spends
    /// handling and forwarding a Message or Input packet. Recorded at
    /// the domain following handling each Message and Input packet.
    pub const DOMAIN_FORWARD_TIME: &str = "readyset_forward_time_us";

    /// Counter: The total time the domain spends handling and forwarding
    /// a Message or Input packet. Recorded at the domain following handling
    /// each Message and Input packet.
    pub const DOMAIN_TOTAL_FORWARD_TIME: &str = "readyset_total_forward_time_us";

    /// Histogram: The time in microseconds that a domain spends
    /// handling a ReplayPiece packet. Recorded at the domain following
    /// ReplayPiece packet handling.
    ///
    /// | Tag | Description |
    /// | --- | ----------- |
    /// | cache_name | The name of the cache associated with this replay.
    pub const DOMAIN_REPLAY_TIME: &str = "readyset_domain.handle_replay_time";

    /// Counter: The total time in microseconds that a domain spends
    /// handling a ReplayPiece packet. Recorded at the domain following
    /// ReplayPiece packet handling.
    ///
    /// | Tag | Description |
    /// | --- | ----------- |
    /// | cache_name | The name of the cache associated with this replay.
    pub const DOMAIN_TOTAL_REPLAY_TIME: &str = "readyset_domain.total_handle_replay_time";

    /// Histogram: The time in microseconds spent handling a reader replay
    /// request. Recorded at the domain following RequestReaderReplay
    /// packet handling.
    ///
    ///
    /// | Tag | Description |
    /// | --- | ----------- |
    /// | cache_name | The name of the cache associated with this replay.
    pub const DOMAIN_READER_REPLAY_REQUEST_TIME: &str =
        "readyset_domain.reader_replay_request_time_us";

    /// Counter: The total time in microseconds spent handling a reader replay
    /// request. Recorded at the domain following RequestReaderReplay
    /// packet handling.
    ///
    ///
    /// | Tag | Description |
    /// | --- | ----------- |
    /// | cache_name | The name of the cache associated with this replay.
    pub const DOMAIN_READER_TOTAL_REPLAY_REQUEST_TIME: &str =
        "readyset_domain.reader_total_replay_request_time_us";

    /// Histogram: The time in microseconds that a domain spends
    /// handling a RequestPartialReplay packet. Recorded at the domain
    /// following RequestPartialReplay packet handling.
    ///
    /// | Tag | Description |
    /// | --- | ----------- |
    /// | cache_name | The name of the cache associated with this replay.
    pub const DOMAIN_SEED_REPLAY_TIME: &str = "readyset_domain.seed_replay_time_us";

    /// Counter: The total time in microseconds that a domain spends
    /// handling a RequestPartialReplay packet. Recorded at the domain
    /// following RequestPartialReplay packet handling.
    ///
    /// | Tag | Description |
    /// | --- | ----------- |
    /// | cache_name | The name of the cache associated with this replay.
    pub const DOMAIN_TOTAL_SEED_REPLAY_TIME: &str = "readyset_domain.total_seed_replay_time_us";

    /// Histogram: The time in microseconds that a domain spawning a state
    /// chunker at a node during the processing of a StartReplay packet.
    /// Recorded at the domain when the state chunker thread is finished
    /// executing.
    pub const DOMAIN_CHUNKED_REPLAY_TIME: &str = "readyset_domain.chunked_replay_time_us";

    /// Counter: The total time in microseconds that a domain spawning a state
    /// chunker at a node during the processing of a StartReplay packet.
    /// Recorded at the domain when the state chunker thread is finished
    /// executing.
    pub const DOMAIN_TOTAL_CHUNKED_REPLAY_TIME: &str =
        "readyset_domain.total_chunked_replay_time_us";

    /// Histogram: The time in microseconds that a domain spends
    /// handling a StartReplay packet. Recorded at the domain
    /// following StartReplay packet handling.
    pub const DOMAIN_CHUNKED_REPLAY_START_TIME: &str =
        "readyset_domain.chunked_replay_start_time_us";

    /// Counter: The total time in microseconds that a domain spends
    /// handling a StartReplay packet. Recorded at the domain
    /// following StartReplay packet handling.
    pub const DOMAIN_TOTAL_CHUNKED_REPLAY_START_TIME: &str =
        "readyset_domain.total_chunked_replay_start_time_us";

    /// Histogram: The time in microseconds that a domain spends
    /// handling a Finish packet for a replay. Recorded at the domain
    /// following Finish packet handling.
    ///
    /// | Tag | Description |
    /// | --- | ----------- |
    /// | cache_name | The name of the cache associated with this replay.
    pub const DOMAIN_FINISH_REPLAY_TIME: &str = "readyset_domain.finish_replay_time_us";

    /// Counter: The total time in microseconds that a domain spends
    /// handling a Finish packet for a replay. Recorded at the domain
    /// following Finish packet handling.
    ///
    /// | Tag | Description |
    /// | --- | ----------- |
    /// | cache_name | The name of the cache associated with this replay.
    pub const DOMAIN_TOTAL_FINISH_REPLAY_TIME: &str = "readyset_domain.total_finish_replay_time_us";

    /// Histogram: The amount of time spent handling an eviction
    /// request.
    pub const EVICTION_TIME: &str = "readyset_eviction_time_us";

    /// Histogram: The time in microseconds that the controller spent committing
    /// a migration to the soup graph. Recorded at the controller at the end of
    /// the `commit` call.
    pub const CONTROLLER_MIGRATION_TIME: &str = "readyset_controller.migration_time_us";

    /// Gauge: Migration in progress indicator. Set to 1 when a migration
    /// is in progress, 0 otherwise.
    pub const CONTROLLER_MIGRATION_IN_PROGRESS: &str = "readyset_controller.migration_in_progress";

    /// Counter: The number of evicitons performed at a worker. Incremented each
    /// time `do_eviction` is called at the worker.
    pub const EVICTION_WORKER_EVICTIONS_REQUESTED: &str =
        "readyset_eviction_worker.evictions_requested";

    /// Gauge: The amount of memory allocated in the heap of the full server process
    pub const EVICTION_WORKER_HEAP_ALLOCATED_BYTES: &str =
        "readyset_eviction_worker.heap_allocated_bytes";

    /// Histogram: The amount of time that the eviction worker spends making an eviction
    /// decision and sending packets.
    pub const EVICTION_WORKER_EVICTION_TIME: &str = "readyset_eviction_worker.eviction_time_us";

    /// Gauge: The sum of the amount of bytes used to store a node's reader state
    /// within a domain.
    ///
    /// | Tag | Description |
    /// | --- | ----------- |
    /// | name | The name of the reader node |
    pub const READER_STATE_SIZE_BYTES: &str = "readyset_reader_state_size_bytes";

    /// Gauge: The sum of the amount of bytes used to store a node's base tables
    /// on disk.
    ///
    /// | Tag | Description |
    /// | --- | ----------- |
    /// | table_name | The name of the base table. |
    pub const ESTIMATED_BASE_TABLE_SIZE_BYTES: &str = "readyset_base_tables_estimated_size_bytes";

    /// Counter: The number of HTTP requests received at the readyset-server, for either the
    /// controller or worker.
    pub const SERVER_EXTERNAL_REQUESTS: &str = "readyset_server.external_requests";

    /// Counter: The number of worker HTTP requests received by the readyset-server.
    pub const SERVER_WORKER_REQUESTS: &str = "readyset_server.worker_requests";

    /// Counter: The number of controller HTTP requests received by the readyset-server.
    pub const SERVER_CONTROLLER_REQUESTS: &str = "readyset_server.controller_requests";

    /// Counter: The number of lookup requests to a base table nodes state.
    ///
    /// | Tag | Description |
    /// | --- | ----------- |
    /// | table_name | The name of the base table. |
    /// | cache_name | The name of the cache associated with this replay.
    pub const BASE_TABLE_LOOKUP_REQUESTS: &str = "readyset_base_table.lookup_requests";

    /// Counter: The number of packets dropped by an egress node.
    pub const EGRESS_NODE_DROPPED_PACKETS: &str = "readyset_egress.dropped_packets";

    /// Counter: The number of packets sent by an egress node.
    pub const EGRESS_NODE_SENT_PACKETS: &str = "readyset_egress.sent_packets";

    /// Counter: The number of eviction packets received.
    pub const EVICTION_REQUESTS: &str = "readyset_eviction_requests";

    /// Histogram: The total number of bytes evicted.
    pub const EVICTION_FREED_MEMORY: &str = "readyset_eviction_freed_memory";

    /// Counter: The number of times a query was served entirely from reader cache.
    pub const SERVER_VIEW_QUERY_HIT: &str = "readyset_server.view_query_result_hit";

    /// Counter: The number of times a query required at least a partial replay.
    pub const SERVER_VIEW_QUERY_MISS: &str = "readyset_server.view_query_result_miss";

    /// Histogram: The amount of time in microseconds spent waiting for an upquery during a read
    /// request.
    pub const SERVER_VIEW_UPQUERY_DURATION: &str = "readyset_server.view_query_upquery_duration_us";

    /// Counter: The number of times a dataflow node type is added to the
    /// dataflow graph. Recorded at the time the new graph is committed.
    ///
    /// | Tag | Description |
    /// | --- | ----------- |
    /// | ntype | The dataflow node type. |
    pub const NODE_ADDED: &str = "readyset_node_added";

    /// Counter: The number of times a dataflow packet has been propagated
    /// for each domain.
    ///
    /// | Tag | Description |
    /// | --- | ----------- |
    /// | packet_type | The type of packet |
    pub const DOMAIN_PACKET_SENT: &str = "readyset_domain.packet_sent";

    /// Gauge: The number of dataflow packets queued for each domain.
    ///
    /// | Tag | Description |
    /// | --- | ----------- |
    /// | packet_type | The type of packet |
    pub const DOMAIN_PACKETS_QUEUED: &str = "readyset_domain.packets_queued";

    /// Histogram: The amount of time in microseconds an operator node spends handling a call to
    /// `Ingredient::on_input`.
    ///
    /// | Tag | Description |
    /// | --- | ----------- |
    /// | ntype | The operator node type. |
    pub const NODE_ON_INPUT_DURATION: &str = "readyset_domain.node_on_input_duration_us";

    /// Counter: The number of times `Ingredient::on_input` has been invoked for a node.
    ///
    /// | Tag | Description |
    /// | --- | ----------- |
    /// | ntype | The operator node type. |
    pub const NODE_ON_INPUT_INVOCATIONS: &str = "readyset_domain.node_on_input_invocations";

    /// Histogram: The time a snapshot takes to be performed.
    pub const REPLICATOR_SNAPSHOT_DURATION: &str = "readyset_replicator.snapshot_duration_us";

    /// How the replicator handled a snapshot.
    pub enum SnapshotStatusTag {
        /// A snapshot was started by the replicator.
        Started,
        /// A snapshot succeeded at the replicator.
        Successful,
        /// A snapshot failed at the replicator.
        Failed,
    }
    impl SnapshotStatusTag {
        /// Returns the enum tag as a &str for use in metrics labels.
        pub fn value(&self) -> &str {
            match self {
                SnapshotStatusTag::Started => "started",
                SnapshotStatusTag::Successful => "successful",
                SnapshotStatusTag::Failed => "failed",
            }
        }
    }

    /// Counter: Number of snapshots started at this node. Incremented by 1 when a
    /// snapshot begins.
    ///
    /// | Tag | Description |
    /// | --- | ----------- |
    /// | status | SnapshotStatusTag |
    pub const REPLICATOR_SNAPSHOT_STATUS: &str = "readyset_replicator.snapshot_status";

    /// Gauge: The number of tables currently snapshotting
    pub const REPLICATOR_TABLES_SNAPSHOTTING: &str = "readyset_replicator.tables_snapshotting";

    /// Counter: Number of failures encountered when following the replication
    /// log.
    pub const REPLICATOR_FAILURE: &str = "readyset_replicator.update_failure";

    /// Counter: Number of tables that failed to replicate and are ignored
    pub const TABLE_FAILED_TO_REPLICATE: &str = "readyset_replicator.table_failed";

    /// Counter: Number of replication actions performed successfully.
    pub const REPLICATOR_SUCCESS: &str = "readyset_replicator.update_success";

    /// Gauge: Indicates whether a server is the leader. Set to 1 when the
    /// server is leader, 0 for follower.
    pub const CONTROLLER_IS_LEADER: &str = "readyset_controller.is_leader";

    /// Counter: The total amount of time spent servicing controller RPCs.
    ///
    /// | Tag | Description |
    /// | --- | ----------- |
    /// | path | The http path associated with the rpc request. |
    pub const CONTROLLER_RPC_OVERALL_TIME: &str = "readyset_controller.rpc_overall_time";

    /// Histogram: The distribution of time spent servicing controller RPCs
    /// for each request.
    ///
    /// | Tag | Description |
    /// | --- | ----------- |
    /// | path | The http path associated with the rpc request. |
    pub const CONTROLLER_RPC_REQUEST_TIME: &str = "readyset_controller.rpc_request_time";

    /// Gauge: The number of queries sent to the `/view_names` controller RPC.
    pub const CONTROLLER_RPC_VIEW_NAMES_NUM_QUERIES: &str =
        "readyset_controller.rpc_view_names_num_queries";

    /// Histgoram: Write propagation time from binlog to reader node. For each
    /// input packet, this is recorded for each reader node that the packet
    /// propagates to. If the packet does not reach the reader because it hits a
    /// hole, the write propagation time is not recorded.
    pub const PACKET_WRITE_PROPAGATION_TIME: &str = "readyset_packet.write_propagation_time_us";

    /// Histogram: The time it takes to clone the dataflow state graph.
    pub const DATAFLOW_STATE_CLONE_TIME: &str = "readyset_dataflow_state.clone_time";

    /// Gauge: The size of the dataflow state, serialized and compressed, measured when it is
    /// written to the authority. This metric may be recorded even if the state does not
    /// get written to the authority (due to a failure). It is only recorded when the Consul
    /// authority is in use
    pub const DATAFLOW_STATE_SERIALIZED: &str = "readyset_dataflow_state.serialized_size";

    /// Gauge: A stub gague used to report the version information for the adapter.
    /// Labels are used to convey the version information.
    pub const READYSET_ADAPTER_VERSION: &str = "readyset_adapter_version";

    /// Gauge: A stub gague used to report the version information for the server.
    /// Labels are used to convey the version information.
    pub const READYSET_SERVER_VERSION: &str = "readyset_server_version";

    /// Gauge: The size of the dash map that holds query status of each query
    /// that have been processed by readyset adapter.
    pub const QUERY_STATUS_CACHE_SIZE: &str = "readyset_query_status_cache.id_to_status.size";

    /// Gauge: The size of the LRUCache that holds full query & query status for a fixed number
    /// of queries that have been processed by readyset adapter.
    pub const QUERY_STATUS_CACHE_PERSISTENT_CACHE_SIZE: &str =
        "readyset_query_status_cache.persistent_cache.statuses.size";
}

/// A dumped metric's kind.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum DumpedMetricValue {
    /// Counters that can be incremented or decremented
    Counter(f64),

    /// Gauges whose values can be explicitly set
    Gauge(f64),

    /// Histograms that track the number of samples that fall within
    /// predefined buckets.
    Histogram(Vec<(f64, u64)>),
}

/// A dumped metric's value.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct DumpedMetric {
    /// Labels associated with this metric value.
    pub labels: HashMap<String, String>,
    /// The actual value.
    pub value: DumpedMetricValue,
}

/// A dump of metrics that implements `Serialize`.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct MetricsDump {
    /// The actual metrics.
    pub metrics: HashMap<String, Vec<DumpedMetric>>,
}

fn convert_key(k: Key) -> (String, HashMap<String, String>) {
    let (name_parts, labels) = k.into_parts();
    let name = name_parts.as_str().to_string();
    let labels = labels
        .into_iter()
        .map(|l| {
            let (k, v) = l.into_parts();
            (k.into_owned(), v.into_owned())
        })
        .collect();
    (name, labels)
}

impl MetricsDump {
    /// Build a [`MetricsDump`] from a map containing values for counters, and another map
    /// containing values for gauges
    #[allow(clippy::mutable_key_type)] // for Key in the hashmap keys
    pub fn from_metrics(
        counters: Vec<(Key, u64)>,
        gauges: Vec<(Key, f64)>,
        histograms: Vec<(Key, Histogram)>,
    ) -> Self {
        let mut ret = HashMap::new();
        for (key, val) in counters.into_iter() {
            let (name, labels) = convert_key(key);
            let ent = ret.entry(name).or_insert_with(Vec::new);
            ent.push(DumpedMetric {
                labels,
                // It's going to be serialized to JSON anyway, so who cares
                value: DumpedMetricValue::Counter(val as f64),
            });
        }
        for (key, val) in gauges.into_iter() {
            let (name, labels) = convert_key(key);
            let ent = ret.entry(name).or_insert_with(Vec::new);
            ent.push(DumpedMetric {
                labels,
                value: DumpedMetricValue::Gauge(val),
            });
        }
        for (key, val) in histograms.into_iter() {
            let (name, labels) = convert_key(key);
            let ent = ret.entry(name).or_insert_with(Vec::new);
            ent.push(DumpedMetric {
                labels,
                value: DumpedMetricValue::Histogram(val.buckets()),
            });
        }

        Self { metrics: ret }
    }

    /// Return the sum of all the reported values for the given metric, if present
    pub fn total<K>(&self, metric: &K) -> Option<f64>
    where
        String: Borrow<K>,
        K: Hash + Eq + ?Sized,
    {
        Some(
            self.metrics
                .get(metric)?
                .iter()
                .map(|m| {
                    match &m.value {
                        DumpedMetricValue::Counter(v) | DumpedMetricValue::Gauge(v) => *v,
                        // Return the sum of counts for a histogram.
                        DumpedMetricValue::Histogram(v) => {
                            v.iter().map(|v| v.1).sum::<u64>() as f64
                        }
                    }
                })
                .sum(),
        )
    }

    /// Return an iterator over all the metric keys in this [`MetricsDump`]
    pub fn keys(&self) -> impl Iterator<Item = &String> {
        self.metrics.keys()
    }

    /// Returns the first `DumpedMetricValue` found for a specific metric
    /// that includes `labels` as a subset of the metrics labels.
    ///
    /// None is returned if the metric is not found or there is no metric
    /// that includes `labels` as a subset.
    pub fn metric_with_labels<K>(
        &self,
        metric: &K,
        labels: &[(&K, &str)],
    ) -> Option<DumpedMetricValue>
    where
        String: Borrow<K>,
        K: Hash + Eq + ?Sized,
    {
        match self.metrics.get(metric) {
            None => {
                return None;
            }
            Some(dm) => {
                for m in dm {
                    if labels.iter().all(|l| match m.labels.get(l.0) {
                        None => false,
                        Some(metric_label) => l.1 == metric_label,
                    }) {
                        return Some(m.value.clone());
                    }
                }
            }
        }

        None
    }

    /// Returns the set of DumpedMetric's for a specific metric that have
    /// `labels` as a subset of their labels.
    ///
    /// An empty vec is returned if the metric is not found or there is no metric
    /// that includes `labels` as a subset.
    pub fn all_metrics_with_labels<K>(&self, metric: &K, labels: &[(&K, &str)]) -> Vec<DumpedMetric>
    where
        String: Borrow<K>,
        K: Hash + Eq + ?Sized,
    {
        let mut dumped_metrics = Vec::new();
        if let Some(dm) = self.metrics.get(metric) {
            for m in dm {
                if labels.iter().all(|l| {
                    m.labels
                        .get(l.0)
                        .filter(|metric_label| l.1 == *metric_label)
                        .is_some()
                }) {
                    dumped_metrics.push(m.clone());
                }
            }
        }

        dumped_metrics
    }
}

impl DumpedMetricValue {
    /// Get the encapsulated floating point value for the metric
    /// if it is not of the Histrogram type
    pub fn value(&self) -> Option<f64> {
        match self {
            DumpedMetricValue::Counter(v) => Some(*v),
            DumpedMetricValue::Gauge(v) => Some(*v),
            DumpedMetricValue::Histogram(_) => None,
        }
    }
}

/// Checks a metrics dump for a specified metric with a set of labels.
/// If the metric exists, returns Some(DumpedMetricValue), otherwise returns
/// None.
///
/// The first two parameters are MetricsDump, MetricsName. After which,
/// any number of labels can be specified. Labels are specified with the
/// syntax: label1 => label_value.
///
/// Example usage:
///     get_metric!(
///         metrics_dump,
///         recorded::DOMAIN_NODE_ADDED,
///         "ntype" => "Reader"
///     );
#[macro_export]
macro_rules! get_metric {
    (
        $metrics_dump:expr,
        $metrics_name:expr
        $(, $label:expr => $value:expr)*
    ) => {
        {
            let labels = vec![$(($label, $value),)*];
            $metrics_dump.metric_with_labels($metrics_name, labels.as_slice())
        }
    };
}

/// Checks a metrics dump for a specified metric with a set of labels.
/// If the metric exists, returns a vector of all metrics that match
/// the specified metrics that include the set of labels as a subset of
/// their own labels.
///
/// The first two parameters are MetricsDump, MetricsName. After which,
/// any number of labels can be specified. Labels are specified with the
/// syntax: label1 => label_value.
///
/// Example usage:
///     get_all_metrics!(
///         metrics_dump,
///         recorded::DOMAIN_NODE_ADDED,
///         "ntype" => "Reader"
///     );
#[macro_export]
macro_rules! get_all_metrics {
    (
        $metrics_dump:expr,
        $metrics_name:expr
        $(, $label:expr => $value:expr)*
    ) => {
        {
            #[allow(unused_mut)]
            let mut labels = Vec::new();
            $(
                labels.push(($label, $value));
            )*
            $metrics_dump.all_metrics_with_labels($metrics_name, labels.as_slice())
        }
    };
}

#[cfg(test)]
mod test {
    use super::*;

    // Tests the syntax of the get_metric macro.
    #[test]
    fn test_metric_macro() {
        let md = MetricsDump {
            metrics: HashMap::new(),
        };
        let metrics_name = recorded::SERVER_CONTROLLER_REQUESTS;
        assert_eq!(
            get_metric!(md, metrics_name, "test1" => "test2", "test3" => "test4"),
            None
        );
        assert_eq!(get_metric!(md, metrics_name, "test1" => "test2"), None);

        assert_eq!(get_metric!(md, metrics_name), None);
    }
}
