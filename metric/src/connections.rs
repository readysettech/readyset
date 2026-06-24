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
