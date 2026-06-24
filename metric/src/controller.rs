/// Histogram: The time in microseconds that the controller spent committing a migration to the
/// dataflow graph. Recorded at the controller at the end of the `commit` call.
pub const CONTROLLER_MIGRATION_TIME: &str = "readyset_controller.migration_time_us";

/// Counter: Number of orphaned domains reclaimed by the controller.
pub const CONTROLLER_RECLAIMED_DOMAINS: &str = "readyset_controller.reclaimed_domains";

/// Gauge: Migration in progress indicator. Set to 1 when a migration is in progress, 0 otherwise.
pub const CONTROLLER_MIGRATION_IN_PROGRESS: &str = "readyset_controller.migration_in_progress";

/// Counter: The number of HTTP requests received at the readyset-server, for either the controller
/// or worker.
pub const SERVER_EXTERNAL_REQUESTS: &str = "readyset_server.external_requests";

/// Counter: The number of controller HTTP requests received by the readyset-server.
pub const SERVER_CONTROLLER_REQUESTS: &str = "readyset_server.controller_requests";

/// Counter: The number of times a dataflow node type is added to the dataflow graph. Recorded at
/// the time the new graph is committed.
///
/// | Tag | Description |
/// | --- | ----------- |
/// | ntype | The dataflow node type. |
pub const NODE_ADDED: &str = "readyset_node_added";

/// Gauge: Indicates whether a server is the leader. Set to 1 when the server is leader, 0 for
/// follower.
pub const CONTROLLER_IS_LEADER: &str = "readyset_controller.is_leader";

/// Counter: The total amount of time in microseconds spent servicing controller RPCs.
///
/// | Tag | Description |
/// | --- | ----------- |
/// | path | The http path associated with the rpc request. |
pub const CONTROLLER_RPC_OVERALL_TIME: &str = "readyset_controller.rpc_overall_time_us";

/// Histogram: The distribution of time in microseconds spent servicing controller RPCs for each
/// request.
///
/// | Tag | Description |
/// | --- | ----------- |
/// | path | The http path associated with the rpc request. |
pub const CONTROLLER_RPC_REQUEST_TIME: &str = "readyset_controller.rpc_request_time_us";

/// Gauge: The number of queries sent to the `/views_info` controller RPC.
pub const CONTROLLER_RPC_VIEWS_INFO_NUM_QUERIES: &str =
    "readyset_controller.rpc_views_info_num_queries";
