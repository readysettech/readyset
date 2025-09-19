use readyset_sql::ast::{CreateTableBody, NonReplicatedRelation, Relation, SqlIdentifier};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

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
