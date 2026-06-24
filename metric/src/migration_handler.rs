/// Counter: The number of successful queries (dry runs/real) processed by the migration handler.
pub const MIGRATION_HANDLER_SUCCESSES: &str = "readyset_migration_handler_successes";

/// Counter: The number of failed queries (dry runs/real) processed by the migration handler.
pub const MIGRATION_HANDLER_FAILURES: &str = "readyset_migration_handler_failures";

/// Counter: The number of queries the migration handler has set to allowed. Incremented on each
/// loop of the migration handler. TODO(justin): In the future it would be good to support gauges
/// for the counts of each query status in the query status cache. Requires optimization of locking.
pub const MIGRATION_HANDLER_ALLOWED: &str = "readyset_migration_handler_allowed";
