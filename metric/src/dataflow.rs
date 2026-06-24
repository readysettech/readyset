/// Counter: The number of lookup misses that occurred during replay requests. Recorded at the
/// domain on every lookup miss during a replay request.
///
/// | Tag | Description |
/// | --- | ----------- |
/// | cache_name | The name of the cache associated with this replay.
pub const DOMAIN_REPLAY_MISSES: &str = "readyset_domain.replay_misses";

/// Histogram: The time in microseconds that a domain spends handling and forwarding a Message or
/// Input packet. Recorded at the domain following handling each Message and Input packet.
///
/// | Tag | Description |
/// | --- | ----------- |
/// | packet_type | The type of the packet, either "input" or "message". |
pub const DOMAIN_FORWARD_TIME: &str = "readyset_forward_time_us";

/// Counter: The total time the domain spends handling and forwarding a Message or Input packet.
/// Recorded at the domain following handling each Message and Input packet.
///
/// | Tag | Description |
/// | --- | ----------- |
/// | packet_type | The type of the packet, either "input" or "message". |
pub const DOMAIN_TOTAL_FORWARD_TIME: &str = "readyset_total_forward_time_us";

/// Histogram: The time in microseconds that a domain spends handling a ReplayPiece packet. Recorded
/// at the domain following ReplayPiece packet handling.
///
/// | Tag | Description |
/// | --- | ----------- |
/// | cache_name | The name of the cache associated with this replay.
pub const DOMAIN_REPLAY_TIME: &str = "readyset_domain.handle_replay_time_us";

/// Counter: The total time in microseconds that a domain spends handling a ReplayPiece packet.
/// Recorded at the domain following ReplayPiece packet handling.
///
/// | Tag | Description |
/// | --- | ----------- |
/// | cache_name | The name of the cache associated with this replay.
pub const DOMAIN_TOTAL_REPLAY_TIME: &str = "readyset_domain.total_handle_replay_time_us";

/// Histogram: The time in microseconds spent handling a reader replay request. Recorded at the
/// domain following RequestReaderReplay packet handling.
///
///
/// | Tag | Description |
/// | --- | ----------- |
/// | cache_name | The name of the cache associated with this replay.
pub const DOMAIN_READER_REPLAY_REQUEST_TIME: &str = "readyset_domain.reader_replay_request_time_us";

/// Counter: The total time in microseconds spent handling a reader replay request. Recorded at the
/// domain following RequestReaderReplay packet handling.
///
///
/// | Tag | Description |
/// | --- | ----------- |
/// | cache_name | The name of the cache associated with this replay.
pub const DOMAIN_READER_TOTAL_REPLAY_REQUEST_TIME: &str =
    "readyset_domain.reader_total_replay_request_time_us";

/// Histogram: The time in microseconds that a domain spends handling a RequestPartialReplay packet.
/// Recorded at the domain following RequestPartialReplay packet handling.
///
/// | Tag | Description |
/// | --- | ----------- |
/// | cache_name | The name of the cache associated with this replay.
pub const DOMAIN_SEED_REPLAY_TIME: &str = "readyset_domain.seed_replay_time_us";

/// Counter: The total time in microseconds that a domain spends handling a RequestPartialReplay
/// packet. Recorded at the domain following RequestPartialReplay packet handling.
///
/// | Tag | Description |
/// | --- | ----------- |
/// | cache_name | The name of the cache associated with this replay.
pub const DOMAIN_TOTAL_SEED_REPLAY_TIME: &str = "readyset_domain.total_seed_replay_time_us";

/// Histogram: The time in microseconds that a domain spawning a state chunker at a node during the
/// processing of a StartReplay packet. Recorded at the domain when the state chunker thread is
/// finished executing.
pub const DOMAIN_CHUNKED_REPLAY_TIME: &str = "readyset_domain.chunked_replay_time_us";

/// Counter: The total time in microseconds that a domain spawning a state chunker at a node during
/// the processing of a StartReplay packet. Recorded at the domain when the state chunker thread is
/// finished executing.
pub const DOMAIN_TOTAL_CHUNKED_REPLAY_TIME: &str = "readyset_domain.total_chunked_replay_time_us";

/// Histogram: The time in microseconds that a domain spends handling a StartReplay packet. Recorded
/// at the domain following StartReplay packet handling.
pub const DOMAIN_CHUNKED_REPLAY_START_TIME: &str = "readyset_domain.chunked_replay_start_time_us";

/// Counter: The total time in microseconds that a domain spends handling a StartReplay packet.
/// Recorded at the domain following StartReplay packet handling.
pub const DOMAIN_TOTAL_CHUNKED_REPLAY_START_TIME: &str =
    "readyset_domain.total_chunked_replay_start_time_us";

/// Histogram: The time in microseconds that a domain spends handling a Finish packet for a replay.
/// Recorded at the domain following Finish packet handling.
///
/// | Tag | Description |
/// | --- | ----------- |
/// | cache_name | The name of the cache associated with this replay.
pub const DOMAIN_FINISH_REPLAY_TIME: &str = "readyset_domain.finish_replay_time_us";

/// Counter: The total time in microseconds that a domain spends handling a Finish packet for a
/// replay. Recorded at the domain following Finish packet handling.
///
/// | Tag | Description |
/// | --- | ----------- |
/// | cache_name | The name of the cache associated with this replay.
pub const DOMAIN_TOTAL_FINISH_REPLAY_TIME: &str = "readyset_domain.total_finish_replay_time_us";

/// Histogram: The amount of time spent handling an eviction request.
pub const EVICTION_TIME: &str = "readyset_eviction_time_us";

/// Counter: The number of times the eviction task has run.
pub const EVICTION_WORKER_EVICTION_TICKS: &str = "readyset_eviction_worker.eviction_ticks";

/// Counter: The number of times the eviction task has checked whether to evict.
pub const EVICTION_WORKER_EVICTION_CHECKS: &str = "readyset_eviction_worker.eviction_checks";

/// Counter: The number of times the eviction task has tried to free memory.
pub const EVICTION_WORKER_EVICTION_RUNS: &str = "readyset_eviction_worker.eviction_runs";

/// Counter: The number of evictions performed at a worker. Incremented each time `do_eviction` is
/// called at the worker.
pub const EVICTION_WORKER_EVICTIONS_REQUESTED: &str =
    "readyset_eviction_worker.evictions_requested";

/// Gauge: The amount of memory allocated in the heap of the full server process
pub const EVICTION_WORKER_HEAP_ALLOCATED_BYTES: &str =
    "readyset_eviction_worker.heap_allocated_bytes";

/// Histogram: The amount of time im microseconds that the eviction worker spends making an eviction
/// decision and sending packets.
pub const EVICTION_WORKER_EVICTION_TIME: &str = "readyset_eviction_worker.eviction_time_us";

/// Counter: The number of barrier credits returned to the worker's `BarrierManager` (incremented
/// once per credit returned, regardless of credit amount).
pub const BARRIER_CREDITS_RETURNED: &str = "readyset_eviction_worker.barrier_credits_returned";

/// Gauge: The sum of the amount of bytes used to store a node's reader state within a domain.
///
/// | Tag | Description |
/// | --- | ----------- |
/// | name | The name of the reader node |
pub const READER_STATE_SIZE_BYTES: &str = "readyset_reader_state_size_bytes";

/// Gauge: The sum of the amount of bytes used to store a node's base tables on disk.
///
/// | Tag | Description |
/// | --- | ----------- |
/// | table_name | The name of the base table. |
pub const ESTIMATED_BASE_TABLE_SIZE_BYTES: &str = "readyset_base_tables_estimated_size_bytes";

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

/// Counter: The number of reads that bailed out with `UpqueryTimeout` because the upquery did not
/// complete within the configured budget. Tracks the rate at which the adapter falls through to
/// upstream because the cache could not satisfy a request in time.
pub const SERVER_VIEW_UPQUERY_TIMEOUT: &str = "readyset_server.view_query_upquery_timeout";

/// Counter: The number of times a dataflow packet has been propagated for each domain.
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

/// Counter: The number of times a TopK operator has triggered a backfill from its parent because a
/// group dropped below k rows after deletions. High values indicate the buffer zone may be
/// undersized.
///
/// | Tag | Description |
/// | --- | ----------- |
/// | cache_name | The `CREATE CACHE` name of the cache this TopK serves (the `name` column
///   in `SHOW CACHES`); `unknown` if the operator has no recorded cache name. |
pub const TOPK_BACKFILL_REQUESTS: &str = "readyset_domain.topk_backfill_requests";

/// Histgoram: Write propagation time in microseconds from binlog to reader node. For each input
/// packet, this is recorded for each reader node that the packet propagates to. If the packet does
/// not reach the reader because it hits a hole, the write propagation time is not recorded.
pub const PACKET_WRITE_PROPAGATION_TIME: &str = "readyset_packet.write_propagation_time_us";

/// Histogram: The time in microseconds it takes to clone the dataflow state graph.
pub const DATAFLOW_STATE_CLONE_TIME: &str = "readyset_dataflow_state.clone_time_us";

/// Gauge: The size of the dataflow state, serialized and compressed, measured when it is written to
/// the authority. This metric may be recorded even if the state does not get written to the
/// authority (due to a failure). It is only recorded when the Consul authority is in use
pub const DATAFLOW_STATE_SERIALIZED: &str = "readyset_dataflow_state.serialized_size";

/// Histogram: The number of unique rows stored in the HashSet-based DISTINCT dedup during
/// post-lookup processing. Recorded once per query that uses `PostLookupDistinct::HashBased`.
/// Monitor this to detect queries with high-cardinality DISTINCT that may use excessive memory.
pub const POST_LOOKUP_DISTINCT_HASH_SET_SIZE: &str = "readyset_post_lookup_distinct_hash_set_size";
