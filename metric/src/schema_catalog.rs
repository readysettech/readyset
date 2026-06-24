//! Documents the set of metrics that are currently being recorded within the schema-catalog crate.

/// Counter: Incremented when the schema catalog content changes without the generation advancing.
pub const SCHEMA_CATALOG_UNEXPECTED_GENERATION: &str =
    "readyset_schema_catalog_unexpected_generation";

/// Counter: Incremented each time a new catalog snapshot is written to the handle.
pub const SCHEMA_CATALOG_UPDATE_APPLIED: &str = "readyset_schema_catalog_update_applied";

/// Gauge: Set to the generation number of the catalog just applied.
pub const SCHEMA_CATALOG_CURRENT_GENERATION: &str = "readyset_schema_catalog_current_generation";

/// Counter: Incremented when a `SchemaCatalogUpdate` fails to decode.
pub const SCHEMA_CATALOG_DECODE_FAILED: &str = "readyset_schema_catalog_decode_failed";

/// Counter: Incremented when the update stream ends and we re-subscribe.
pub const SCHEMA_CATALOG_STREAM_RECONNECTED: &str = "readyset_schema_catalog_stream_reconnected";

/// Counter: Incremented when a schema change triggers targeted invalidation of queries referencing
/// specific changed tables.
pub const SCHEMA_CATALOG_INVALIDATION_TARGETED: &str =
    "readyset_schema_catalog_invalidation_targeted";

/// Counter: Incremented when a schema change triggers full invalidation of all cached query state
/// (e.g., custom type changes or first catalog update).
pub const SCHEMA_CATALOG_INVALIDATION_FULL: &str = "readyset_schema_catalog_invalidation_full";

/// Histogram: Duration (in seconds) of the schema catalog diff operation.
pub const SCHEMA_CATALOG_DIFF_DURATION: &str = "readyset_schema_catalog_diff_duration_seconds";

/// Histogram: Duration (in seconds) of the schema change invalidation operation.
pub const SCHEMA_CATALOG_INVALIDATION_DURATION: &str =
    "readyset_schema_catalog_invalidation_duration_seconds";

/// Histogram: Wall-clock time (in seconds) between when a new catalog is sent to the invalidation
/// sidecar and when invalidation completes. Tracks how long the QSC may contain stale entries after
/// a schema update is applied to the cache.
pub const SCHEMA_CATALOG_INVALIDATION_STALENESS: &str =
    "readyset_schema_catalog_invalidation_staleness_seconds";

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

/// Counter: Incremented each time the schema catalog broadcast receiver detects lag, causing the
/// update stream to terminate and the synchronizer to reconnect.
pub const SCHEMA_CATALOG_BROADCAST_LAGGED: &str = "readyset_schema_catalog_broadcast_lagged";

/// Counter: Incremented by the number of events skipped when broadcast lag is detected.
pub const SCHEMA_CATALOG_BROADCAST_SKIPPED: &str = "readyset_schema_catalog_broadcast_skipped";
