use base64::Engine;
use base64::engine::general_purpose::STANDARD as B64;
use readyset_errors::{ReadySetResult, internal_err};
use readyset_sql::ast::{CreateTableBody, NonReplicatedRelation, Relation, SqlIdentifier};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

/// Describes what changed between two schema catalog versions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaChanges {
    /// Specific relations changed; invalidate queries referencing these.
    Relations(Vec<Relation>),
    /// Non-relation-specific changes (e.g., custom types) or first catalog update; invalidate all
    /// cached query state.
    All,
}

/// Trait for components that need to react to schema catalog changes. Implemented by
/// QueryStatusCache in readyset-adapter.
pub trait SchemaChangeHandler: Send + Sync {
    /// Invalidate cached state for queries referencing any of the given tables.
    ///
    /// Note: targeted invalidation is best-effort when schema qualifications are mixed between
    /// the cached queries and the `tables` list (e.g., a cached query references `"foo"` but
    /// the catalog reports `"public"."foo"`). In such cases, some stale entries may not be
    /// invalidated until the next full invalidation. See TODO REA-5970.
    fn invalidate_for_tables(&self, tables: &[Relation]);

    /// Invalidate all cached query state. Called when changes can't be mapped to specific tables
    /// (e.g., custom type changes) or on first catalog update.
    fn invalidate_all(&self);
}

mod generation;
pub mod metrics;

pub use generation::SchemaGeneration;

// Only the synchronizer in the `handle` module can update the catalog (through the handle); we
// reexport the public types here so that the `handle` module can be private.
mod handle;
pub use handle::{SchemaCatalogHandle, SchemaCatalogProvider, SchemaCatalogSynchronizer};

mod rewrite_context;

pub use rewrite_context::RewriteContext;

/// A serializable catalog of schema information that can be shared between the Readyset adapter and
/// server. This contains the core schema state needed for query rewriting and schema resolution.
///
/// The `SchemaCatalog` encapsulates only the schema information needed for the adapter to be able
/// to perform schema-aware rewrites without communicating with the server. What ends up in here is
/// thus generally decided by what we do in [`readyset_sql_passes::adapter_rewrites`].
#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SchemaCatalog {
    /// Monotonically increasing generation number that changes whenever the schema is modified.
    /// Used to detect when cache creation attempts are using stale schema information.
    ///
    /// # Semantics
    /// - Starts at [`SchemaGeneration::INITIAL`] (1) and increments after each successful DDL
    ///   migration
    /// - CreateCache-only migrations do NOT increment the generation
    /// - The generation from this catalog should be attached to cache creation requests
    ///   so the controller can validate the adapter's schema view matches its own
    pub generation: SchemaGeneration,
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

    /// Computes what changed between `self` (old) and `new` (incoming).
    ///
    /// Returns [`SchemaChanges::All`] for custom type changes (can't map type changes to specific
    /// queries). Returns [`SchemaChanges::Relations`] with the union of all changed relations from
    /// base_schemas, view_schemas, and non_replicated_relations.
    pub fn diff(&self, new: &SchemaCatalog) -> SchemaChanges {
        // Custom type changes can't be mapped to specific queries
        if self.custom_types != new.custom_types {
            return SchemaChanges::All;
        }

        let mut changed = HashSet::new();

        // base_schemas: collect symmetric difference of keys + keys with different values
        for (relation, body) in &self.base_schemas {
            match new.base_schemas.get(relation) {
                Some(new_body) if new_body != body => {
                    changed.insert(relation.clone());
                }
                None => {
                    changed.insert(relation.clone());
                }
                _ => {}
            }
        }
        for relation in new.base_schemas.keys() {
            if !self.base_schemas.contains_key(relation) {
                changed.insert(relation.clone());
            }
        }

        // view_schemas: same — symmetric diff + value diff
        for (relation, columns) in &self.view_schemas {
            match new.view_schemas.get(relation) {
                Some(new_columns) if new_columns != columns => {
                    changed.insert(relation.clone());
                }
                None => {
                    changed.insert(relation.clone());
                }
                _ => {}
            }
        }
        for relation in new.view_schemas.keys() {
            if !self.view_schemas.contains_key(relation) {
                changed.insert(relation.clone());
            }
        }

        // uncompiled_views: symmetric diff
        let old_uncompiled: HashSet<&Relation> = self.uncompiled_views.iter().collect();
        let new_uncompiled: HashSet<&Relation> = new.uncompiled_views.iter().collect();
        for view in old_uncompiled.symmetric_difference(&new_uncompiled) {
            changed.insert((*view).clone());
        }

        // non_replicated_relations: extract Relation from symmetric difference
        for nrr in &self.non_replicated_relations {
            if !new.non_replicated_relations.contains(nrr) {
                changed.insert(nrr.name.clone());
            }
        }
        for nrr in &new.non_replicated_relations {
            if !self.non_replicated_relations.contains(nrr) {
                changed.insert(nrr.name.clone());
            }
        }

        SchemaChanges::Relations(changed.into_iter().collect())
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
        let bytes = bincode::serialize(catalog).map_err(|e| {
            internal_err!("Failed to serialize SchemaCatalog for SchemaCatalogUpdate: {e}")
        })?;
        Ok(Self {
            catalog_b64: B64.encode(bytes),
        })
    }
}

impl TryFrom<SchemaCatalogUpdate> for SchemaCatalog {
    type Error = readyset_errors::ReadySetError;

    fn try_from(update: SchemaCatalogUpdate) -> ReadySetResult<Self> {
        let bytes = B64.decode(update.catalog_b64).map_err(|e| {
            internal_err!("Failed to base64-decode SchemaCatalog from SchemaCatalogUpdate: {e}")
        })?;
        bincode::deserialize(&bytes).map_err(|e| {
            internal_err!("Failed to deserialize SchemaCatalog from SchemaCatalogUpdate: {e}")
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_body(col_name: &str) -> CreateTableBody {
        use readyset_sql::ast::*;

        CreateTableBody {
            fields: vec![ColumnSpecification {
                column: Column {
                    name: col_name.into(),
                    table: None,
                },
                sql_type: SqlType::Int(None),
                generated: None,
                constraints: vec![],
                comment: None,
            }],
            keys: None,
        }
    }

    #[test]
    fn diff_identical_catalogs() {
        let a = SchemaCatalog::new();
        let b = SchemaCatalog::new();
        assert_eq!(a.diff(&b), SchemaChanges::Relations(vec![]));
    }

    #[test]
    fn diff_table_added() {
        let old = SchemaCatalog::new();
        let mut new = SchemaCatalog::new();
        new.base_schemas
            .insert(Relation::from("foo"), make_body("x"));

        match old.diff(&new) {
            SchemaChanges::Relations(tables) => {
                assert_eq!(tables.len(), 1);
                assert_eq!(tables[0], Relation::from("foo"));
            }
            other => panic!("Expected Relations, got {other:?}"),
        }
    }

    #[test]
    fn diff_table_removed() {
        let mut old = SchemaCatalog::new();
        old.base_schemas
            .insert(Relation::from("foo"), make_body("x"));
        let new = SchemaCatalog::new();

        match old.diff(&new) {
            SchemaChanges::Relations(tables) => {
                assert_eq!(tables.len(), 1);
                assert_eq!(tables[0], Relation::from("foo"));
            }
            other => panic!("Expected Relations, got {other:?}"),
        }
    }

    #[test]
    fn diff_table_altered() {
        let mut old = SchemaCatalog::new();
        old.base_schemas
            .insert(Relation::from("foo"), make_body("x"));
        let mut new = SchemaCatalog::new();
        new.base_schemas
            .insert(Relation::from("foo"), make_body("y"));

        match old.diff(&new) {
            SchemaChanges::Relations(tables) => {
                assert_eq!(tables.len(), 1);
                assert_eq!(tables[0], Relation::from("foo"));
            }
            other => panic!("Expected Relations, got {other:?}"),
        }
    }

    #[test]
    fn diff_table_unchanged() {
        let mut old = SchemaCatalog::new();
        old.base_schemas
            .insert(Relation::from("foo"), make_body("x"));
        let mut new = SchemaCatalog::new();
        new.base_schemas
            .insert(Relation::from("foo"), make_body("x"));

        assert_eq!(old.diff(&new), SchemaChanges::Relations(vec![]));
    }

    #[test]
    fn diff_view_added() {
        let old = SchemaCatalog::new();
        let mut new = SchemaCatalog::new();
        new.view_schemas
            .insert(Relation::from("v1"), vec!["col".into()]);

        match old.diff(&new) {
            SchemaChanges::Relations(tables) => {
                assert_eq!(tables.len(), 1);
                assert_eq!(tables[0], Relation::from("v1"));
            }
            other => panic!("Expected Relations, got {other:?}"),
        }
    }

    #[test]
    fn diff_view_removed() {
        let mut old = SchemaCatalog::new();
        old.view_schemas
            .insert(Relation::from("v1"), vec!["col".into()]);
        let new = SchemaCatalog::new();

        match old.diff(&new) {
            SchemaChanges::Relations(tables) => {
                assert_eq!(tables.len(), 1);
                assert_eq!(tables[0], Relation::from("v1"));
            }
            other => panic!("Expected Relations, got {other:?}"),
        }
    }

    #[test]
    fn diff_view_changed() {
        let mut old = SchemaCatalog::new();
        old.view_schemas
            .insert(Relation::from("v1"), vec!["col_a".into()]);
        let mut new = SchemaCatalog::new();
        new.view_schemas
            .insert(Relation::from("v1"), vec!["col_a".into(), "col_b".into()]);

        match old.diff(&new) {
            SchemaChanges::Relations(tables) => {
                assert_eq!(tables.len(), 1);
                assert_eq!(tables[0], Relation::from("v1"));
            }
            other => panic!("Expected Relations, got {other:?}"),
        }
    }

    #[test]
    fn diff_non_replicated_relation_added() {
        let old = SchemaCatalog::new();
        let mut new = SchemaCatalog::new();
        new.non_replicated_relations
            .insert(NonReplicatedRelation::new(Relation::from("nr1")));

        match old.diff(&new) {
            SchemaChanges::Relations(tables) => {
                assert_eq!(tables.len(), 1);
                assert_eq!(tables[0], Relation::from("nr1"));
            }
            other => panic!("Expected Relations, got {other:?}"),
        }
    }

    #[test]
    fn diff_non_replicated_relation_removed() {
        let mut old = SchemaCatalog::new();
        old.non_replicated_relations
            .insert(NonReplicatedRelation::new(Relation::from("nr1")));
        let new = SchemaCatalog::new();

        match old.diff(&new) {
            SchemaChanges::Relations(tables) => {
                assert_eq!(tables.len(), 1);
                assert_eq!(tables[0], Relation::from("nr1"));
            }
            other => panic!("Expected Relations, got {other:?}"),
        }
    }

    #[test]
    fn diff_uncompiled_view_added() {
        let old = SchemaCatalog::new();
        let mut new = SchemaCatalog::new();
        new.uncompiled_views.push(Relation::from("v_pending"));

        match old.diff(&new) {
            SchemaChanges::Relations(tables) => {
                assert_eq!(tables.len(), 1);
                assert_eq!(tables[0], Relation::from("v_pending"));
            }
            other => panic!("Expected Relations, got {other:?}"),
        }
    }

    #[test]
    fn diff_uncompiled_view_removed() {
        let mut old = SchemaCatalog::new();
        old.uncompiled_views.push(Relation::from("v_pending"));
        let new = SchemaCatalog::new();

        match old.diff(&new) {
            SchemaChanges::Relations(tables) => {
                assert_eq!(tables.len(), 1);
                assert_eq!(tables[0], Relation::from("v_pending"));
            }
            other => panic!("Expected Relations, got {other:?}"),
        }
    }

    #[test]
    fn diff_custom_type_changed_returns_all() {
        let mut old = SchemaCatalog::new();
        old.custom_types
            .insert("public".into(), HashSet::from(["my_enum".into()]));
        let mut new = SchemaCatalog::new();
        new.custom_types.insert(
            "public".into(),
            HashSet::from(["my_enum".into(), "my_enum2".into()]),
        );

        assert_eq!(old.diff(&new), SchemaChanges::All);
    }

    #[test]
    fn diff_mixed_changes() {
        let mut old = SchemaCatalog::new();
        old.base_schemas
            .insert(Relation::from("t1"), make_body("x"));
        old.base_schemas
            .insert(Relation::from("t2"), make_body("a"));

        let mut new = SchemaCatalog::new();
        // t1 altered
        new.base_schemas
            .insert(Relation::from("t1"), make_body("y"));
        // t2 unchanged
        new.base_schemas
            .insert(Relation::from("t2"), make_body("a"));
        // t3 added
        new.base_schemas
            .insert(Relation::from("t3"), make_body("z"));

        match old.diff(&new) {
            SchemaChanges::Relations(mut tables) => {
                tables.sort_by(|a, b| a.name.cmp(&b.name));
                assert_eq!(tables.len(), 2);
                assert_eq!(tables[0], Relation::from("t1"));
                assert_eq!(tables[1], Relation::from("t3"));
            }
            other => panic!("Expected Relations, got {other:?}"),
        }
    }
}
