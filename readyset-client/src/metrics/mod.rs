//! Data types representing metrics dumped from a running ReadySet instance

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
    ///
    /// | Tag | Description |
    /// | --- | ----------- |
    /// | packet_type | The type of the packet, either "input" or "message". |
    pub const DOMAIN_FORWARD_TIME: &str = "readyset_forward_time_us";

    /// Counter: The total time the domain spends handling and forwarding
    /// a Message or Input packet. Recorded at the domain following handling
    /// each Message and Input packet.
    ///
    /// | Tag | Description |
    /// | --- | ----------- |
    /// | packet_type | The type of the packet, either "input" or "message". |
    pub const DOMAIN_TOTAL_FORWARD_TIME: &str = "readyset_total_forward_time_us";

    /// Histogram: The time in microseconds that a domain spends
    /// handling a ReplayPiece packet. Recorded at the domain following
    /// ReplayPiece packet handling.
    ///
    /// | Tag | Description |
    /// | --- | ----------- |
    /// | cache_name | The name of the cache associated with this replay.
    pub const DOMAIN_REPLAY_TIME: &str = "readyset_domain.handle_replay_time_us";

    /// Counter: The total time in microseconds that a domain spends
    /// handling a ReplayPiece packet. Recorded at the domain following
    /// ReplayPiece packet handling.
    ///
    /// | Tag | Description |
    /// | --- | ----------- |
    /// | cache_name | The name of the cache associated with this replay.
    pub const DOMAIN_TOTAL_REPLAY_TIME: &str = "readyset_domain.total_handle_replay_time_us";

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
    /// a migration to the dataflow graph. Recorded at the controller at the end of
    /// the `commit` call.
    pub const CONTROLLER_MIGRATION_TIME: &str = "readyset_controller.migration_time_us";

    /// Gauge: Migration in progress indicator. Set to 1 when a migration
    /// is in progress, 0 otherwise.
    pub const CONTROLLER_MIGRATION_IN_PROGRESS: &str = "readyset_controller.migration_in_progress";

    /// Counter: The number of times the eviction task has run.
    pub const EVICTION_WORKER_EVICTION_TICKS: &str = "readyset_eviction_worker.eviction_ticks";

    /// Counter: The number of times the eviction task has checked whether to evict.
    pub const EVICTION_WORKER_EVICTION_CHECKS: &str = "readyset_eviction_worker.eviction_checks";

    /// Counter: The number of times the eviction task has tried to free memory.
    pub const EVICTION_WORKER_EVICTION_RUNS: &str = "readyset_eviction_worker.eviction_runs";

    /// Counter: The number of evictions performed at a worker. Incremented each
    /// time `do_eviction` is called at the worker.
    pub const EVICTION_WORKER_EVICTIONS_REQUESTED: &str =
        "readyset_eviction_worker.evictions_requested";

    /// Gauge: The amount of memory allocated in the heap of the full server process
    pub const EVICTION_WORKER_HEAP_ALLOCATED_BYTES: &str =
        "readyset_eviction_worker.heap_allocated_bytes";

    /// Histogram: The amount of time im microseconds that the eviction worker spends
    /// making an eviction decision and sending packets.
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

    pub const SHALLOW_HIT: &str = "readyset_shallow.shallow_result_hit";
    pub const SHALLOW_MISS: &str = "readyset_shallow.shallow_result_miss";
    pub const SHALLOW_REFRESH: &str = "readyset_shallow.shallow_result_refresh";
    pub const SHALLOW_EVICT_MEMORY: &str = "readyset_shallow.shallow_evict_memory";
    pub const SHALLOW_REFRESH_QUEUE_EXCEEDED: &str =
        "readyset_shallow.shallow_refresh_queue_exceeded";

    /// Histogram: The amount of time in microseconds spent executing the upstream query during a
    /// shallow cache refresh. Only recorded for successful queries.
    ///
    /// | Tag | Description |
    /// | --- | ----------- |
    /// | query_id | The query ID for the cached query being refreshed. |
    pub const SHALLOW_REFRESH_QUERY_TIME: &str = "readyset_shallow.shallow_refresh_query_time_us";

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

    /// Histogram: The amount of time in microseconds spent processing data during
    /// `Ingredient::on_input_raw`.
    ///
    /// | Tag | Description |
    /// | --- | ----------- |
    /// | ntype | The operator node type. |
    pub const NODE_ON_INPUT_RAW_DURATION: &str = "readyset_domain.node_on_input_raw_duration_us";

    /// Counter: The number of times `Ingredient::on_input_raw` has been invoked for a node.
    ///
    /// | Tag | Description |
    /// | --- | ----------- |
    /// | ntype | The operator node type. |
    pub const NODE_ON_INPUT_RAW_INVOCATIONS: &str = "readyset_domain.node_on_input_raw_invocations";

    /// Histogram: The amount of time in microseconds a snapshot takes to be performed.
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

    /// Counter: The total amount of time in microseconds spent servicing controller RPCs.
    ///
    /// | Tag | Description |
    /// | --- | ----------- |
    /// | path | The http path associated with the rpc request. |
    pub const CONTROLLER_RPC_OVERALL_TIME: &str = "readyset_controller.rpc_overall_time_us";

    /// Histogram: The distribution of time in microseconds spent servicing controller RPCs
    /// for each request.
    ///
    /// | Tag | Description |
    /// | --- | ----------- |
    /// | path | The http path associated with the rpc request. |
    pub const CONTROLLER_RPC_REQUEST_TIME: &str = "readyset_controller.rpc_request_time_us";

    /// Gauge: The number of queries sent to the `/views_info` controller RPC.
    pub const CONTROLLER_RPC_VIEWS_INFO_NUM_QUERIES: &str =
        "readyset_controller.rpc_views_info_num_queries";

    /// Histgoram: Write propagation time in microseconds from binlog to reader node.
    /// For each input packet, this is recorded for each reader node that the packet
    /// propagates to. If the packet does not reach the reader because it hits a
    /// hole, the write propagation time is not recorded.
    pub const PACKET_WRITE_PROPAGATION_TIME: &str = "readyset_packet.write_propagation_time_us";

    /// Histogram: The time in microseconds it takes to clone the dataflow state graph.
    pub const DATAFLOW_STATE_CLONE_TIME: &str = "readyset_dataflow_state.clone_time_us";

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

    /// Gauge: The number of queries in the query sampler queue.
    ///
    /// | Tag | Description |
    /// | --- | ----------- |
    /// | queue_len | The number of queries in the queue |
    pub const QUERY_SAMPLER_QUEUE_LEN: &str = "readyset_query_sampler_queue_len";

    /// Counter: The number of queries sampled by the query sampler.
    ///
    /// | Tag | Description |
    /// | --- | ----------- |
    /// | queries_sampled | The number of queries sampled |
    pub const QUERY_SAMPLER_QUERIES_SAMPLED: &str = "readyset_query_sampler_queries_sampled";

    /// Counter: The number of queries mismatched by the query sampler.
    ///
    /// | Tag | Description |
    /// | --- | ----------- |
    /// | queries_mismatched | The number of queries mismatched |
    pub const QUERY_SAMPLER_QUERIES_MISMATCHED: &str = "readyset_query_sampler_queries_mismatched";

    /// Gauge: The number of entries in the retry queue.
    ///
    /// | Tag | Description |
    /// | --- | ----------- |
    /// | retry_queue_len | The number of entries in the retry queue |
    pub const QUERY_SAMPLER_RETRY_QUEUE_LEN: &str = "readyset_query_sampler_retry_queue_len";

    /// Counter: The number of times the retry queue is full.
    ///
    /// | Tag | Description |
    /// | --- | ----------- |
    /// | retry_queue_full | The number of times the retry queue is full |
    pub const QUERY_SAMPLER_RETRY_QUEUE_FULL: &str = "readyset_query_sampler_retry_queue_full";

    /// Counter: The number of times the sampler is reconnecting to the upstream database.
    ///
    /// | Tag | Description |
    /// | --- | ----------- |
    /// | reconnects | The number of times the sampler is reconnecting to the upstream database |
    pub const QUERY_SAMPLER_RECONNECTS: &str = "readyset_query_sampler_reconnects";

    /// Counter: The number of times the sampler has hit the max QPS.
    ///
    /// | Tag | Description |
    /// | --- | ----------- |
    /// | max_qps | The number of times the sampler has hit the max QPS |
    pub const QUERY_SAMPLER_MAX_QPS_HIT: &str = "readyset_query_sampler_max_qps_hit";

    /// Counter: Incremented each time schema generation advances after a successful DDL migration.
    pub const SCHEMA_CATALOG_GENERATION_INCREMENTED: &str =
        "readyset_schema_catalog_generation_incremented";

    /// Counter: Incremented each time a schema catalog update is broadcast via SSE.
    pub const SCHEMA_CATALOG_UPDATE_SENT: &str = "readyset_schema_catalog_update_sent";

    /// Counter: Incremented when serializing a schema catalog update fails.
    pub const SCHEMA_CATALOG_UPDATE_SERIALIZATION_FAILED: &str =
        "readyset_schema_catalog_update_serialization_failed";

    /// Counter: Incremented when a CreateCache request has a stale schema generation.
    pub const SCHEMA_GENERATION_MISMATCH: &str = "readyset_schema_generation_mismatch";

    /// Counter: Incremented each time the SSE client connects to the controller successfully.
    pub const CONTROLLER_EVENTS_CONNECTED: &str = "readyset_controller_events_connected";

    /// Counter: Incremented each time the SSE stream closes or errors.
    pub const CONTROLLER_EVENTS_DISCONNECTED: &str = "readyset_controller_events_disconnected";

    /// Counter: Incremented each time the schema catalog broadcast receiver detects lag,
    /// causing the update stream to terminate and the synchronizer to reconnect.
    pub const SCHEMA_CATALOG_BROADCAST_LAGGED: &str = "readyset_schema_catalog_broadcast_lagged";

    /// Counter: Incremented by the number of events skipped when broadcast lag is detected.
    pub const SCHEMA_CATALOG_BROADCAST_SKIPPED: &str = "readyset_schema_catalog_broadcast_skipped";
}
