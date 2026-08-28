/// Counter: The number of HTTP requests received at the noria-client.
pub const ADAPTER_EXTERNAL_REQUESTS: &str = "readyset_noria_client_external_requests";

/// Gauge: The number of currently connected SQL clients
pub const CONNECTED_CLIENTS: &str = "readyset_noria_client_connected_clients";

/// Counter: The number of connections opened by clients
pub const CLIENT_CONNECTIONS_OPENED: &str = "readyset_noria_client_conns_opened";

/// Counter: The number of connections closed by clients
pub const CLIENT_CONNECTIONS_CLOSED: &str = "readyset_noria_client_conns_closed";

/// Gauge: The number of open connections to the upstream database, on behalf of client connections.
pub const CLIENT_UPSTREAM_CONNECTIONS: &str = "readyset_client_upstream_connections";

/// Gauge: The number of open connections this adapter holds to each upstream server, labeled by
/// `server` (host:port).
pub const UPSTREAM_SERVER_CONNECTIONS: &str = "readyset_upstream_server_connections";

/// Gauge: 1 while the adapter believes the upstream `server` is up, 0 while it is marked down.
pub const UPSTREAM_SERVER_UP: &str = "readyset_upstream_server_up";

/// Counter: Failed connection attempts to an upstream `server`, labeled by the failure `reason`.
pub const UPSTREAM_CONNECT_FAILURES: &str = "readyset_upstream_connect_failures_total";
