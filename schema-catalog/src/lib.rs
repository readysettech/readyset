use base64::Engine;
use base64::engine::general_purpose::STANDARD as B64;
use readyset_errors::{ReadySetResult, internal_err};
use readyset_sql::ast::{CreateTableBody, NonReplicatedRelation, Relation, SqlIdentifier};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

// Only the synchronizer in the `handle` module can update the catalog (through the handle); we
// reexport the public types here so that the `handle` module can be private.
mod handle;
pub use handle::{SchemaCatalogHandle, SchemaCatalogProvider, SchemaCatalogSynchronizer};

/// A serializable catalog of schema information that can be shared between the Readyset adapter and
/// server. This contains the core schema state needed for query rewriting and schema resolution.
///
/// The `SchemaCatalog` encapsulates only the schema information needed for the adapter to be able
/// to perform schema-aware rewrites without communicating with the server. What ends up in here is
/// thus generally decided by what we do in [`readyset_sql_passes::adapter_rewrites`].
#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SchemaCatalog {
    /// Base table schemas, mapping relation names to their CREATE TABLE definitions.
    pub base_schemas: HashMap<Relation, CreateTableBody>,
    /// Views that have been declared but not yet compiled into the dataflow graph.
    pub uncompiled_views: Vec<Relation>,
    /// Custom types (enums, composite types, etc.), mapping schema names to a set of type names.
    pub custom_types: HashMap<SqlIdentifier, HashSet<SqlIdentifier>>,
    /// Map from names of views and tables to (ordered) lists of column names.
    pub view_schemas: HashMap<Relation, Vec<SqlIdentifier>>,
    /// Set of relations that exist in the upstream database but are not being replicated and
    /// therefore can't be queried by deep caches.
    pub non_replicated_relations: HashSet<NonReplicatedRelation>,
}

impl SchemaCatalog {
    pub fn new() -> Self {
        Self {
            ..Default::default()
        }
    }
}

/// Wrapper for schema catalog updates sent over the controller event stream.
///
/// The catalog is serialized via `bincode` and base64-encoded to remain JSON-safe for SSE.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SchemaCatalogUpdate {
    pub catalog_b64: String,
}

impl TryFrom<&SchemaCatalog> for SchemaCatalogUpdate {
    type Error = readyset_errors::ReadySetError;

    fn try_from(catalog: &SchemaCatalog) -> ReadySetResult<Self> {
        let bytes = bincode::serialize(catalog)
            .map_err(|e| internal_err!("Failed to serialize catalog: {e}"))?;
        Ok(Self {
            catalog_b64: B64.encode(bytes),
        })
    }
}

impl TryFrom<SchemaCatalogUpdate> for SchemaCatalog {
    type Error = readyset_errors::ReadySetError;

    fn try_from(update: SchemaCatalogUpdate) -> ReadySetResult<Self> {
        let bytes = B64
            .decode(update.catalog_b64)
            .map_err(|e| internal_err!("Failed to decode catalog: {e}"))?;
        bincode::deserialize(&bytes)
            .map_err(|e| internal_err!("Failed to deserialize catalog: {e}"))
    }
}
