//! Documents the set of metrics that are currently being recorded within
//! the schema-catalog crate.

/// Counter: The number of schema catalog updates received with a generation that does not follow
/// the expected monotonic sequence (i.e., the new generation does not directly succeed the current
/// one).
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
