/// Counter: The number of times the adapter has started up. In standalone mode, this metric can be
/// used to count the number of system startups.
pub const READYSET_ADAPTER_STARTUPS: &str = "readyset_adapter_startups";

/// Counter: The number of times the server has started up. In standalone mode, this metric should
/// track [`READYSET_ADAPTER_STARTUPS`] exactly.
pub const READYSET_SERVER_STARTUPS: &str = "readyset_server_startups";

/// Gauge: A stub gague used to report the version information for the adapter. Labels are used to
/// convey the version information.
pub const READYSET_ADAPTER_VERSION: &str = "readyset_adapter_version";

/// Gauge: A stub gague used to report the version information for the server. Labels are used to
/// convey the version information.
pub const READYSET_SERVER_VERSION: &str = "readyset_server_version";

/// Gauge: The last seen size in bytes of a /metrics payload.
pub const METRICS_PAYLOAD_SIZE_BYTES: &str = "readyset_metrics_payload_size_bytes";
