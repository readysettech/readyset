use std::collections::{HashMap, HashSet};
use std::str;
use std::vec::Vec;

use ::mir::visualize::GraphViz;
use ::mir::DfNodeIndex;
use ::serde::{Deserialize, Serialize};
use nom_sql::{
    CompoundSelectOperator, CompoundSelectStatement, CreateTableBody, CreateTableOption,
    DialectDisplay, FieldDefinitionExpr, NonReplicatedRelation, NotReplicatedReason, Relation,
    SelectSpecification, SelectStatement, SqlIdentifier, SqlType, TableExpr,
};
use petgraph::graph::NodeIndex;
use readyset_client::query::QueryId;
use readyset_client::recipe::changelist::{AlterTypeChange, Change, PostgresTableMetadata};
use readyset_client::recipe::ChangeList;
use readyset_data::dialect::SqlEngine;
use readyset_data::{DfType, Dialect, PgEnumMetadata};
use readyset_errors::{
    internal, internal_err, invalid_query_err, invariant, unsupported, ReadySetError,
    ReadySetResult,
};
use readyset_sql_passes::alias_removal::TableAliasRewrite;
use readyset_sql_passes::{AliasRemoval, DetectUnsupportedPlaceholders, Rewrite, RewriteContext};
use readyset_util::redacted::Sensitive;
use tracing::{debug, error, info, trace, warn};
use vec1::Vec1;

use self::mir::{LeafBehavior, NodeIndex as MirNodeIndex, SqlToMirConverter};
use self::query_graph::to_query_graph;
pub(crate) use self::recipe::{ExprId, Recipe, Schema};
use self::registry::ExprRegistry;
use crate::controller::mir_to_flow::{mir_node_to_flow_parts, mir_query_to_flow_parts};
pub(crate) use crate::controller::sql::registry::RecipeExpr;
use crate::controller::Migration;
use crate::sql::mir::MirRemovalResult;
use crate::ReuseConfigType;

pub(crate) mod mir;
mod query_graph;
mod query_signature;
mod recipe;
mod registry;

/// Configuration for converting SQL to dataflow
#[derive(Clone, Debug, Serialize, Deserialize)]
#[non_exhaustive]
pub(crate) struct Config {
    pub(crate) reuse_type: Option<ReuseConfigType>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            reuse_type: Some(ReuseConfigType::Finkelstein),
        }
    }
}

/// Information about a SQL `VIEW` that has been registered with ReadySet, but has yet to be
/// compiled to Dataflow because no query has selected FROM them yet. Used as the value in
/// [`SqlIncorporator::uncompiled_views`].
#[derive(Clone, Debug, Serialize, Deserialize)]
struct UncompiledView {
    /// The name of the view.
    ///
    /// This exists here in addition to as the key in `SqlIncorporator.uncompiled_views` because it
    /// makes it easier to make sure view names are always schema-qualified
    name: Relation,

    /// The definition of the view itself
    definition: SelectSpecification,

    /// The schema search path that the view was created with
    schema_search_path: Vec<SqlIdentifier>,
}

/// Schema for a SQL base node
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct BaseSchema {
    /// The body of the original `CREATE TABLE` statement for the table
    pub statement: CreateTableBody,
    /// Metadata about the table from PostgreSQL, if any
    pub pg_meta: Option<PostgresTableMetadata>,
}

/// Long-lived struct that holds information about the SQL queries (tables, views, and caches) that
/// have been incorporated into the dataflow graph.
///
/// The incorporator shares the lifetime of the dataflow graph it is associated with.
///
/// The entrypoints for adding queries to the `SqlIncorporator` are:
///
/// * [`add_table`][Self::add_table], to add a new `TABLE`
/// * [`add_view`][Self::add_view], to add a new `VIEW`
/// * [`add_query`][Self::add_query], to add a new cached query
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
// crate viz for tests
pub(crate) struct SqlIncorporator {
    mir_converter: SqlToMirConverter,
    leaf_addresses: HashMap<Relation, NodeIndex>,

    /// Stores VIEWs and CACHE queries.
    named_queries: HashMap<Relation, u64>,
    num_queries: usize,

    /// Views which have been added to ReadySet, but which have not been compiled to dataflow yet
    /// because no query has selected FROM them yet. Represented as a map from the
    /// (schema-qualified!) name of the view to the *post-rewrite* SelectStatement for the view.
    uncompiled_views: HashMap<Relation, UncompiledView>,

    base_schemas: HashMap<Relation, BaseSchema>,
    view_schemas: HashMap<Relation, Vec<SqlIdentifier>>,

    /// User-defined custom types, indexed by (schema-qualified) name.
    ///
    /// Internally, we just represent custom types as named aliases for a [`DfType`].
    custom_types: HashMap<Relation, DfType>,

    /// Map from postgresql `oid` for custom types to the names of those custom types.
    ///
    /// # Invariants
    ///
    /// All values in this map will also be keys in `self.custom_types`.
    custom_types_by_oid: HashMap<u32, Relation>,

    pub(crate) config: Config,

    /// A structure which keeps track of all the raw AST expressions in the schema, and their
    /// dependencies on each other
    registry: ExprRegistry,

    /// Whether or to treat failed writes to base tables as no-ops
    permissive_writes: bool,
}

impl SqlIncorporator {
    /// Creates a new `SqlIncorporator` for an empty flow graph.
    pub(super) fn new() -> Self {
        Default::default()
    }

    /// Set the MIR configuration for future migrations
    pub(crate) fn set_mir_config(&mut self, mir_config: mir::Config) {
        self.mir_converter.set_config(mir_config);
    }

    /// Set the permissive write behavior for base tables
    pub(crate) fn set_permissive_writes(&mut self, permissive_writes: bool) {
        self.permissive_writes = permissive_writes;
    }

    pub(crate) fn mir_config(&self) -> &mir::Config {
        self.mir_converter.config()
    }

    /// Disable node reuse for future migrations.
    #[allow(unused)]
    pub(crate) fn disable_reuse(&mut self) {
        self.config.reuse_type = None;
    }

    /// Disable node reuse for future migrations.
    #[allow(unused)]
    pub(crate) fn enable_reuse(&mut self, reuse_type: ReuseConfigType) {
        self.config.reuse_type = Some(reuse_type);
    }

    /// Rewrite the given SQL statement to normalize, validate, and desugar it, based on the stored
    /// relations in `self`.
    ///
    /// Can optionally provide a mutable reference to a list of names of non-existent tables which,
    /// if created, should invalidate the query
    // TODO(aspen): This should really be happening as part of the `add_<whatever>` methods (it was,
    // before this was made pub(crate)) but since the recipe expression registry stores expressions
    // we need it happening earlier. We should move it back to its rightful place once we can get
    // rid of that.
    pub(crate) fn rewrite<S>(
        &self,
        stmt: S,
        search_path: &[SqlIdentifier],
        dialect: Dialect,
        invalidating_tables: Option<&mut Vec<Relation>>,
    ) -> ReadySetResult<S>
    where
        S: Rewrite,
    {
        stmt.rewrite(&mut RewriteContext {
            view_schemas: &self.view_schemas,
            base_schemas: self
                .base_schemas
                .iter()
                .map(|(k, BaseSchema { statement, .. })| (k, statement))
                .collect(),
            uncompiled_views: &self.uncompiled_views.keys().collect::<Vec<_>>(),
            non_replicated_relations: &self.mir_converter.non_replicated_relations,
            custom_types: &self
                .custom_types
                .keys()
                .filter_map(|t| Some((t.schema.as_ref()?, &t.name)))
                .fold(HashMap::new(), |mut acc, (schema, name)| {
                    acc.entry(schema).or_default().insert(name);
                    acc
                }),
            search_path,
            dialect,
            invalidating_tables,
        })
    }

    pub(crate) fn apply_changelist(
        &mut self,
        changelist: ChangeList,
        mig: &mut Migration<'_>,
    ) -> ReadySetResult<()> {
        debug!(
            num_queries = self.registry.len(),
            named_queries = self.registry.num_aliases(),
        );

        let ChangeList {
            changes,
            schema_search_path,
            dialect,
        } = changelist;

        for change in changes {
            match change {
                Change::CreateTable {
                    statement: mut cts,
                    pg_meta,
                } => {
                    cts = self.rewrite(cts, &schema_search_path, dialect, None)?;
                    let body = match cts.body {
                        Ok(body) => body,
                        Err(unparsed) => unsupported!(
                            "CREATE TABLE {} body failed to parse: {}",
                            cts.table.display_unquoted(),
                            Sensitive(&unparsed)
                        ),
                    };
                    if dialect.engine() == SqlEngine::MySQL {
                        let options = match cts.options {
                            Ok(options) => options,
                            Err(unparsed) => unsupported!(
                                "CREATE TABLE {} options failed to parse: {}",
                                cts.table.display_unquoted(),
                                Sensitive(&unparsed)
                            ),
                        };
                        if let Some(CreateTableOption::Engine(Some(storage_engine))) = options
                            .iter()
                            .find(|option| matches!(option, CreateTableOption::Engine(Some(_))))
                        {
                            if !dialect.storage_engine_is_supported(storage_engine) {
                                unsupported!(
                                    "CREATE TABLE {} specifies unsupported storage engine: {}",
                                    cts.table.display_unquoted(),
                                    storage_engine
                                )
                            }
                        }
                    }
                    if body.fields.is_empty() {
                        return Err(ReadySetError::TableError {
                            table: cts.table.clone(),
                            source: Box::new(ReadySetError::Unsupported(
                                "tables must have at least one column".to_string(),
                            )),
                        });
                    }
                    match self.registry.get(&cts.table) {
                        Some(RecipeExpr::Table {
                            body: current_body, ..
                        }) => {
                            // Table already exists, so check if it has been changed.
                            if current_body != &body {
                                // Table has changed. Drop and recreate.
                                trace!(
                                    table = %cts.table.display_unquoted(),
                                    "table exists and has changed. Dropping and recreating..."
                                );
                                self.drop_and_recreate_table(
                                    &cts.table.clone(),
                                    body,
                                    pg_meta,
                                    mig,
                                )?;
                                continue;
                            }
                            trace!(
                                name = %cts.table.name,
                                "table exists, but hasn't changed. Ignoring..."
                            );
                        }
                        Some(RecipeExpr::View { .. }) => {
                            return Err(ReadySetError::ViewAlreadyExists(
                                cts.table.name.clone().into(),
                            ))
                        }
                        _ => {
                            self.invalidate_queries_for_added_relation(&cts.table, mig)?;
                            self.add_table(cts.table.clone(), body.clone(), pg_meta.clone(), mig)?;
                            self.registry.add_query(RecipeExpr::Table {
                                name: cts.table,
                                body,
                                pg_meta,
                            })?;
                        }
                    }
                }

                Change::AddNonReplicatedRelation(NonReplicatedRelation {
                    name,
                    reason: NotReplicatedReason::OtherError(desc),
                }) => {
                    debug!(name = %name.display_unquoted(), desc = %desc, "Adding non-replicated relation with OtherError");
                    self.add_non_replicated_relation(NonReplicatedRelation {
                        name,
                        reason: NotReplicatedReason::OtherError(desc),
                    });
                }
                Change::AddNonReplicatedRelation(NonReplicatedRelation {
                    name,
                    reason: NotReplicatedReason::UnsupportedType(desc),
                }) => {
                    debug!(name = %name.display_unquoted(), desc = %desc, "Adding non-replicated relation with UnsupportedType");
                    self.add_non_replicated_relation(NonReplicatedRelation {
                        name,
                        reason: NotReplicatedReason::UnsupportedType(desc),
                    });
                }
                Change::AddNonReplicatedRelation(NonReplicatedRelation {
                    name,
                    reason: NotReplicatedReason::Partitioned,
                }) => {
                    debug!(name = %name.display_unquoted(), "Adding non-replicated relation with Partitioned");
                    self.add_non_replicated_relation(NonReplicatedRelation {
                        name,
                        reason: NotReplicatedReason::Partitioned,
                    });
                }
                Change::AddNonReplicatedRelation(NonReplicatedRelation {
                    name,
                    reason: NotReplicatedReason::TableDropped,
                }) => {
                    debug!(name = %name.display_unquoted(), "Adding non-replicated relation with TableDropped");
                    self.add_non_replicated_relation(NonReplicatedRelation {
                        name,
                        reason: NotReplicatedReason::TableDropped,
                    });
                }
                Change::AddNonReplicatedRelation(NonReplicatedRelation {
                    name,
                    reason: NotReplicatedReason::Configuration,
                }) => {
                    debug!(name = %name.display_unquoted(), "Adding non-replicated relation with Configuration");
                    self.add_non_replicated_relation(NonReplicatedRelation {
                        name,
                        reason: NotReplicatedReason::Configuration,
                    });
                }
                Change::AddNonReplicatedRelation(NonReplicatedRelation {
                    name,
                    reason: NotReplicatedReason::Default,
                }) => {
                    debug!(name = %name.display_unquoted(), "Adding non-replicated relation with Default");
                    self.add_non_replicated_relation(NonReplicatedRelation {
                        name,
                        reason: NotReplicatedReason::Default,
                    });
                }
                Change::CreateView(mut stmt) => {
                    if let Some(first_schema) = schema_search_path.first() {
                        if stmt.name.schema.is_none() {
                            stmt.name.schema = Some(first_schema.clone())
                        }
                    }

                    let definition = match stmt.definition {
                        Ok(definition) => *definition,
                        Err(unparsed) => unsupported!(
                            "CREATE VIEW {} body failed to parse: {}",
                            stmt.name.display_unquoted(),
                            Sensitive(&unparsed)
                        ),
                    };

                    self.invalidate_queries_for_added_relation(&stmt.name, mig)?;
                    self.add_view(stmt.name, definition, schema_search_path.clone())?;
                }
                Change::CreateCache(cc) => {
                    self.add_query(cc.name, *cc.statement, cc.always, &schema_search_path, mig)?;
                }
                Change::AlterTable(_) => {
                    // The only ALTER TABLE changes that can end up here (currently) are ones that
                    // aren't relevant to ReadySet, so we can just ignore them.
                }
                Change::CreateType { mut name, ty } => {
                    if let Some(first_schema) = schema_search_path.first() {
                        if name.schema.is_none() {
                            name.schema = Some(first_schema.clone())
                        }
                    }
                    let needs_resnapshot = if let Some(existing_ty) = self.get_custom_type(&name) {
                        invariant!(self.registry.contains_custom_type(&name));
                        // Type already exists, so check if it has been changed in a way that
                        // requires dropping and recreating dependent tables
                        match (existing_ty, &ty) {
                            (
                                DfType::Enum {
                                    variants: original_variants,
                                    ..
                                },
                                DfType::Enum {
                                    variants: new_variants,
                                    ..
                                },
                            ) => {
                                new_variants.len() > original_variants.len()
                                    && new_variants[..original_variants.len()]
                                        != **original_variants
                            }
                            _ => true,
                        }
                    } else {
                        false
                    };

                    self.registry.add_custom_type(name.clone());
                    self.add_custom_type(name.clone(), ty);
                    if needs_resnapshot {
                        debug!(
                            name = %name.display_unquoted(),
                            "Replacing existing custom type"
                        );
                        for expr in self
                            .registry
                            .expressions_referencing_custom_type(&name)
                            .cloned()
                            .collect::<Vec<_>>()
                        {
                            match expr {
                                RecipeExpr::Table {
                                    name,
                                    body,
                                    pg_meta,
                                } => {
                                    self.drop_and_recreate_table(&name, body, pg_meta, mig)?;
                                }
                                RecipeExpr::View { name, .. } | RecipeExpr::Cache { name, .. } => {
                                    self.remove_expression(&name, mig)?;
                                }
                            }
                        }
                    }
                }
                Change::Drop {
                    mut name,
                    if_exists,
                } => {
                    if name.schema.is_none() {
                        if let Some(first_schema) = schema_search_path.first() {
                            name.schema = Some(first_schema.clone());
                        }
                    }

                    let removed = if self
                        .remove_non_replicated_relation(&NonReplicatedRelation::new(name.clone()))
                    {
                        true
                    } else if self.registry.remove_custom_type(&name) {
                        for expr in self
                            .registry
                            .expressions_referencing_custom_type(&name)
                            .cloned()
                            .collect::<Vec<_>>()
                        {
                            match expr {
                                // Technically postgres doesn't allow removing custom types
                                // before removing tables and views referencing those custom types -
                                // but we might as well be more
                                // permissive here
                                RecipeExpr::Table { name, .. }
                                | RecipeExpr::View { name, .. }
                                | RecipeExpr::Cache { name, .. } => {
                                    self.remove_expression(&name, mig)?;
                                }
                            }
                        }

                        self.drop_custom_type(&name).is_some()
                    } else {
                        self.remove_expression(&name, mig)?.is_some()
                    };

                    if !removed && !if_exists {
                        error!(
                            name = %name.display_unquoted(),
                            "attempted to drop relation, but relation does not exist"
                        );
                        internal!(
                            "attempted to drop relation, but relation {} does not exist",
                            name.display_unquoted()
                        );
                    }
                }
                Change::AlterType { oid, name, change } => {
                    let (ty, _old_name) =
                        self.alter_custom_type(oid, &name, change).map_err(|e| {
                            e.context(format!(
                                "while altering custom type {}",
                                name.display_unquoted()
                            ))
                        })?;
                    let ty = ty.clone();

                    let mut table_names = vec![];
                    let mut queries_to_remove = vec![];
                    for expr in self
                        .registry
                        .expressions_referencing_custom_type(&name)
                        // TODO: this cloned+collect can go away once this happens inside
                        // SqlIncorporator
                        .cloned()
                        .collect::<Vec<_>>()
                    {
                        match expr {
                            RecipeExpr::Table {
                                name: table_name,
                                body,
                                pg_meta: _,
                            } => {
                                for field in body.fields.iter() {
                                    if matches!(&field.sql_type, SqlType::Other(t) if t == &name) {
                                        self.set_base_column_type(
                                            &table_name,
                                            &field.column,
                                            ty.clone(),
                                            mig,
                                        )?;
                                    }
                                }

                                table_names.push(table_name);
                            }

                            RecipeExpr::View { name, .. } | RecipeExpr::Cache { name, .. } => {
                                queries_to_remove.push(name.clone());
                            }
                        }
                    }

                    for table_name in table_names {
                        self.remove_dependent_queries(&table_name, mig)?;
                    }

                    for name in queries_to_remove {
                        self.remove_expression(&name, mig)?;
                    }
                }
            }
        }

        Ok(())
    }

    fn invalidate_queries_for_added_relation(
        &mut self,
        relation: &Relation,
        mig: &mut Migration<'_>,
    ) -> ReadySetResult<()> {
        let invalidate_queries = self
            .registry
            .queries_to_invalidate_for_table(relation)
            .cloned()
            .collect::<Vec<_>>();

        for invalidate_query in invalidate_queries {
            info!(
                table = %relation.display_unquoted(),
                query = %invalidate_query.display_unquoted(),
                "Created relation invalidates previously-created query due to schema resolution; \
                 dropping query"
            );
            self.remove_expression(&invalidate_query, mig)?;
        }

        Ok(())
    }

    /// Add a new table, specified by the given `CREATE TABLE` statement, to the graph, using the
    /// given `mig` to track changes.
    pub(crate) fn add_table(
        &mut self,
        name: Relation,
        body: CreateTableBody,
        pg_meta: Option<PostgresTableMetadata>,
        mig: &mut Migration<'_>,
    ) -> ReadySetResult<()> {
        let (name, dataflow_idx) = self.add_base_via_mir(name, body, pg_meta, mig)?;
        self.remove_non_replicated_relation(&NonReplicatedRelation::new(name.clone()));
        self.leaf_addresses.insert(name, dataflow_idx);
        Ok(())
    }

    /// Add a new SQL VIEW, specified by the given `CREATE VIEW` statement, to the db
    fn add_view(
        &mut self,
        name: Relation,
        definition: SelectSpecification,
        schema_search_path: Vec<SqlIdentifier>,
    ) -> ReadySetResult<()> {
        trace!(name = %name.display_unquoted(), "Adding uncompiled view");
        self.remove_non_replicated_relation(&NonReplicatedRelation::new(name.clone()));
        self.uncompiled_views.insert(
            name.clone(),
            UncompiledView {
                name,
                definition,
                schema_search_path,
            },
        );
        Ok(())
    }

    /// Add a new query to the graph, using the given `mig` to track changes.
    ///
    /// If `name` is provided, will use that as the name for the query to add, otherwise a unique
    /// name will be generated from the query. In either case, returns the name of the added query.
    pub(crate) fn add_query(
        &mut self,
        name: Option<Relation>,
        mut stmt: SelectStatement,
        always: bool,
        schema_search_path: &[SqlIdentifier],
        mig: &mut Migration<'_>,
    ) -> ReadySetResult<Relation> {
        let name = name.unwrap_or_else(|| format!("q_{}", self.num_queries).into());
        let query_id = QueryId::from_select(&stmt, schema_search_path);

        let mut invalidating_tables = vec![];
        let detect_placeholders_config =
            readyset_sql_passes::detect_unsupported_placeholders::Config {
                allow_mixed_comparisons: self.mir_converter.config.allow_mixed_comparisons,
            };
        let mir_query = match stmt
            .detect_unsupported_placeholders(detect_placeholders_config)
            .and_then(|_| {
                self.select_query_to_mir(
                    name.clone(),
                    &mut stmt,
                    schema_search_path,
                    Some(&mut invalidating_tables),
                    LeafBehavior::Leaf,
                    mig,
                )
            }) {
            // Placeholders were supported and we successfully added a query
            Ok(mir_query) => Ok(Some(mir_query)),
            // Placeholder positions were not supported in the query, see if we can reuse an
            // existing cached query.
            Err(err @ ReadySetError::UnsupportedPlaceholders { .. }) => {
                // We must rewrite before looking for a query we can reuse.
                let rewritten_stmt = self.rewrite(
                    stmt.clone(),
                    schema_search_path,
                    mig.dialect,
                    Some(&mut invalidating_tables),
                )?;
                let caches = self.registry.caches_for_query(rewritten_stmt)?;
                if caches.is_empty() {
                    // Can't reuse anything. Return the error.
                    return Err(err);
                } else {
                    #[allow(clippy::unwrap_used)]
                    // we checked that caches is not empty
                    self.registry
                        .add_reused_caches(name.clone(), Vec1::try_from(caches).unwrap());
                    warn!(%err, "Migration failed");
                    info!(
                        "Found compatible view for {}. View may not satisfy all possible \
                         placeholder values",
                        name.display_unquoted()
                    );
                    // We have nothing to add to the graph so we return None. However, we can't exit
                    // here, because we need to add the query to registry.
                    Ok(None)
                }
            }
            // We did not detect unsupported placeholders, but we failed to migrate the query.
            // Unlikely that we could reuse a cache, so we don't attempt to do so.
            Err(err) => Err(err),
        }?;

        let aliased = !self.registry.add_query(RecipeExpr::Cache {
            name: name.clone(),
            statement: stmt,
            always,
            query_id,
        })?;
        self.registry
            .insert_invalidating_tables(name.clone(), invalidating_tables)?;

        if aliased {
            return Ok(name);
        }

        // We don't add a leaf if we're reusing a query
        if let Some(mir_query) = mir_query {
            let leaf = self.mir_to_dataflow(name.clone(), mir_query, mig)?;
            self.leaf_addresses.insert(name.clone(), leaf);
        }

        Ok(name)
    }

    /// Add a new user-defined custom type (represented internally as a named alias for a
    /// [`DfType`]). Will return an error if a type already exists with the same name
    pub(crate) fn add_custom_type(&mut self, name: Relation, ty: DfType) {
        if let DfType::Enum {
            metadata: Some(PgEnumMetadata { oid, .. }),
            ..
        } = ty
        {
            self.custom_types_by_oid.insert(oid, name.clone());
        }
        self.custom_types.insert(name, ty);
    }

    /// Alter the definition of the given custom type according to the given `change`, and ensuring
    /// that the type with the given `oid` has the given `name`.
    ///
    /// Returns the updated type, and the old name of the type if it was renamed.
    pub(crate) fn alter_custom_type(
        &mut self,
        oid: u32,
        name: &Relation,
        change: AlterTypeChange,
    ) -> ReadySetResult<(&DfType, Option<Relation>)> {
        let old_name = if !self.custom_types.contains_key(name) {
            let Some(old_name) = self.custom_types_by_oid.remove(&oid) else {
                return Err(invalid_query_err!(
                    "Could not find custom type with oid {oid}"
                ));
            };
            self.custom_types_by_oid.insert(oid, name.clone());
            let ty = self
                .custom_types
                .remove(&old_name)
                .expect("custom_types_by_oid must point at types in custom_types");
            self.custom_types.insert(name.clone(), ty);
            self.registry.rename_custom_type(&old_name, name);
            trace!(
                old_name = %old_name.display_unquoted(),
                new_name = %name.display_unquoted(),
                %oid,
                "Renaming custom type"
            );
            Some(old_name)
        } else {
            None
        };

        let ty = self
            .custom_types
            .get_mut(name)
            .expect("just ensured the key was present");

        match change {
            AlterTypeChange::SetVariants { new_variants, .. } => {
                let metadata = match ty {
                    DfType::Enum { variants, metadata } => {
                        if new_variants.len() > variants.len()
                            && new_variants[..variants.len()] != **variants
                        {
                            return Err(invalid_query_err!(
                                "Cannot drop variants or add new variants unless they're at the \
                                 end"
                            ));
                        }
                        if old_name.is_some() {
                            if let Some(metadata) = metadata {
                                metadata.name = name.name.clone();
                                if let Some(schema) = &name.schema {
                                    metadata.schema = schema.clone()
                                }
                            }
                        }
                        metadata.take()
                    }
                    _ => {
                        return Err(invalid_query_err!(
                            "Custom type {} is not an enum",
                            name.display_unquoted()
                        ))
                    }
                };

                *ty = DfType::from_enum_variants(new_variants, metadata);
            }
        }

        Ok((ty, old_name))
    }

    pub(crate) fn drop_custom_type(&mut self, name: &Relation) -> Option<DfType> {
        self.custom_types.remove(name)
    }

    pub(crate) fn get_custom_type(&self, name: &Relation) -> Option<&DfType> {
        self.custom_types.get(name)
    }

    /// Return a set of all relations (tables or views) which are known to exist in the upstream
    /// database that we are replicating from, but are not being replicated to ReadySet
    pub(crate) fn non_replicated_relations(&self) -> &HashSet<NonReplicatedRelation> {
        &self.mir_converter.non_replicated_relations
    }

    /// Record that a relation (a table or view) with the given `name` exists in the upstream
    /// database, but is not being replicated
    pub(crate) fn add_non_replicated_relation(&mut self, name: NonReplicatedRelation) {
        self.mir_converter.non_replicated_relations.insert(name);
    }

    /// Remove the given `name` from the set of tables that are known to exist in the upstream
    /// database, but are not being replicated. Returns whether the table was in the set.
    pub(crate) fn remove_non_replicated_relation(&mut self, name: &NonReplicatedRelation) -> bool {
        self.mir_converter.non_replicated_relations.remove(name)
    }

    pub(super) fn set_base_column_type(
        &mut self,
        table: &Relation,
        column: &nom_sql::Column,
        new_ty: DfType,
        mig: &mut Migration<'_>,
    ) -> ReadySetResult<()> {
        let not_found_err = || self.mir_converter.table_not_found_err(table);

        let addr = self.leaf_addresses.get(table).ok_or_else(not_found_err)?;
        let idx = self
            .get_base_schema(table)
            .ok_or_else(not_found_err)?
            .statement
            .fields
            .iter()
            .position(|f| f.column == *column)
            .ok_or_else(|| ReadySetError::NoSuchColumn(column.name.clone().into()))?;
        mig.set_column_type(*addr, idx, new_ty)?;

        Ok(())
    }

    /// Remove the expression with the given name or alias, from the recipe.
    /// Returns the node indices that were removed due to the removal of the expression.
    /// Returns `Ok(None)` if the expression was not found.
    ///
    /// # Errors
    /// This method will return an error whenever there's an inconsistence between the
    /// [`ExprRegistry`] and the [`SqlIncorporator`], i.e, an expression exists in one but not
    /// in the other.
    pub(super) fn remove_expression(
        &mut self,
        name_or_alias: &Relation,
        mig: &mut Migration<'_>,
    ) -> ReadySetResult<Option<HashSet<DfNodeIndex>>> {
        let expression = match self.registry.remove_expression(name_or_alias) {
            Some(expression) => expression,
            None => {
                if self.uncompiled_views.remove(name_or_alias).is_some() {
                    return Ok(Some(Default::default()));
                }

                return Ok(None);
            }
        };
        let name = expression.name();
        let removal_result = match expression {
            RecipeExpr::Table { .. } => {
                // a base may have many dependent queries, including ones that also lost
                // nodes; the code handling `removed_leaves` therefore needs to take care
                // not to remove bases while they still have children, or to try removing
                // them twice.
                match self.remove_base(name, mig) {
                    Ok(ni) => ni,
                    Err(e) => {
                        error!(
                            err = %e,
                            name = %name.display_unquoted(),
                            "failed to remove base whose address could not be resolved",
                        );

                        return Err(e.context(format!(
                            "failed to remove base {} whose address could not be resolved",
                            name.display_unquoted()
                        )));
                    }
                }
            }
            _ => self.remove_query(name, mig)?,
        };
        Ok(Some(removal_result.dataflow_nodes_to_remove))
    }

    fn drop_and_recreate_table(
        &mut self,
        table: &Relation,
        body: CreateTableBody,
        pg_meta: Option<PostgresTableMetadata>,
        mig: &mut Migration,
    ) -> ReadySetResult<()> {
        let removed_node_indices = self.remove_expression(table, mig)?;
        if removed_node_indices.is_none() {
            error!(
                table = %table.display_unquoted(),
                "attempted to issue ALTER TABLE, but table does not exist"
            );
            return Err(ReadySetError::TableNotFound {
                name: table.name.clone().into(),
                schema: table.schema.clone().map(Into::into),
            });
        };
        self.add_table(table.clone(), body.clone(), pg_meta.clone(), mig)?;
        self.registry.add_query(RecipeExpr::Table {
            name: table.clone(),
            body,
            pg_meta,
        })?;
        Ok(())
    }

    pub(super) fn get_base_schema<'a>(&'a self, table: &Relation) -> Option<&'a BaseSchema> {
        self.base_schemas.get(table)
    }

    pub(super) fn get_view_schema<'a>(&'a self, name: &Relation) -> Option<&'a [SqlIdentifier]> {
        self.view_schemas.get(name).as_ref().map(|v| v.as_slice())
    }

    /// Retrieves the flow node associated with a given query's leaf view.
    pub(super) fn get_query_address(&self, name: &Relation) -> Option<NodeIndex> {
        match self.leaf_addresses.get(name) {
            None => self
                .mir_converter
                .get_flow_node_address(name)
                .map(|na| na.address()),
            Some(na) => Some(*na),
        }
    }

    fn add_base_via_mir(
        &mut self,
        name: Relation,
        statement: CreateTableBody,
        pg_meta: Option<PostgresTableMetadata>,
        mig: &mut Migration<'_>,
    ) -> ReadySetResult<(Relation, NodeIndex)> {
        // first, compute the MIR representation of the SQL query
        let mir = self
            .mir_converter
            .named_base_to_mir(name.clone(), &statement)?;

        trace!(base_node_mir = ?mir);

        // no optimization, because standalone base nodes can't be optimized
        let dataflow_node =
            mir_node_to_flow_parts(mir.graph, mir.mir_node, &self.custom_types, mig)?
                .ok_or_else(|| internal_err!("Base MIR nodes must have a Dataflow node assigned"))?
                .address();

        self.base_schemas
            .insert(name.clone(), BaseSchema { statement, pg_meta });

        let fields = mir.fields;
        self.register_query(name.clone(), fields);

        Ok((name, dataflow_node))
    }

    fn add_compound_query(
        &mut self,
        query_name: Relation,
        query: &mut CompoundSelectStatement,
        search_path: &[SqlIdentifier],
        mut invalidating_tables: Option<&mut Vec<Relation>>,
        leaf_behavior: LeafBehavior,
        mig: &mut Migration<'_>,
    ) -> ReadySetResult<MirNodeIndex> {
        let mut subqueries = Vec::with_capacity(query.selects.len());
        for (_, stmt) in &mut query.selects {
            let mut tables = invalidating_tables.is_some().then(Vec::new);
            subqueries.push(self.select_query_to_mir(
                query_name.clone(),
                stmt,
                search_path,
                tables.as_mut(),
                LeafBehavior::Anonymous,
                mig,
            )?);
            if let Some(ts) = tables {
                if let Some(its) = invalidating_tables.as_mut() {
                    its.extend(ts);
                }
            }
        }

        self.mir_converter.compound_query_to_mir(
            &query_name,
            subqueries,
            CompoundSelectOperator::Union,
            &query.order,
            &query.limit_clause,
            leaf_behavior,
        )
    }

    /// Add a new SelectStatement to the given migration, returning the index of the leaf MIR node
    /// that was added
    fn select_query_to_mir(
        &mut self,
        query_name: Relation,
        stmt: &mut SelectStatement,
        search_path: &[SqlIdentifier],
        mut invalidating_tables: Option<&mut Vec<Relation>>,
        leaf_behavior: LeafBehavior,
        mig: &mut Migration<'_>,
    ) -> ReadySetResult<MirNodeIndex> {
        let on_err = |e| ReadySetError::SelectQueryCreationFailed {
            qname: query_name.display_unquoted().to_string(),
            source: Box::new(e),
        };

        // Keep attempting to compile the query to MIR, but every time we run into a TableNotFound
        // error for a view we've yet to migrate, migrate that view then retry
        //
        // TODO(aspen): This is a janky and inefficient way of making this happen. Someday, when
        // SqlIncorporator and SqlToMirConverter are merged into one, this ought to not be
        // necesssary - instead, we can just migrate the view as part of the `get_relation` method
        let mut tables = vec![];
        loop {
            let compile_res = self.select_query_to_mir_inner(
                &query_name,
                stmt,
                search_path,
                invalidating_tables.is_some().then_some(&mut tables),
                leaf_behavior,
                mig,
            );
            match compile_res {
                Ok(mir_leaf) => {
                    if let Some(its) = invalidating_tables.as_mut() {
                        its.extend(tables);
                    }
                    return Ok(mir_leaf);
                }
                Err(e) => {
                    if let Some(view) = e
                        .table_not_found_cause()
                        .map(|(name, schema)| Relation {
                            schema: schema.map(Into::into),
                            name: name.into(),
                        })
                        .and_then(|rel| self.uncompiled_views.remove(&rel))
                    {
                        trace!(
                            name = %view.name.display_unquoted(),
                            "Query referenced uncompiled view; compiling"
                        );
                        if let Err(e) = self.compile_uncompiled_view(view.clone(), mig) {
                            trace!(%e, "Compiling uncompiled view failed");
                            // The view *might* have failed to migrate for a transient reason - put
                            // it back in the map of uncompiled views so we can try again later if
                            // the user asks us to
                            self.uncompiled_views.insert(view.name.clone(), view);
                            return Err(on_err(e));
                        }
                    } else {
                        return Err(on_err(e));
                    }
                }
            }
        }
    }

    /// Compile a select statement to MIR within the context of the given migration, but *not*
    /// handling errors caused by referencing uncompiled views.
    ///
    /// Do not call this method directly - call `select_query_to_mir` instead.
    fn select_query_to_mir_inner(
        &mut self,
        query_name: &Relation,
        stmt: &mut SelectStatement,
        search_path: &[SqlIdentifier],
        invalidating_tables: Option<&mut Vec<Relation>>,
        leaf_behavior: LeafBehavior,
        mig: &mut Migration<'_>,
    ) -> ReadySetResult<MirNodeIndex> {
        // FIXME(REA-2168): Use correct dialect.
        trace!(stmt = %stmt.display(nom_sql::Dialect::MySQL), "Adding select query");
        *stmt = self.rewrite(stmt.clone(), search_path, mig.dialect, invalidating_tables)?;

        self.num_queries += 1;

        // Remove all table aliases from the query. Create named views in cases where the alias must
        // be replaced with a view rather than the table itself in order to prevent ambiguity. (This
        // may occur when a single table is referenced using more than one alias).
        let table_alias_rewrites = stmt.rewrite_table_aliases(&query_name.name);
        let mut anon_queries = HashMap::new();
        for r in table_alias_rewrites {
            match r {
                TableAliasRewrite::View {
                    to_view, for_table, ..
                } => {
                    let mut query = SelectStatement {
                        tables: vec![TableExpr::from(for_table)],
                        fields: vec![FieldDefinitionExpr::All],
                        ..Default::default()
                    };
                    let subquery_leaf = self.select_query_to_mir(
                        query_name.clone(),
                        &mut query,
                        &[], /* Don't need a schema search path since we're only resolving
                              * one (already qualified) table */
                        None,
                        LeafBehavior::Anonymous,
                        mig,
                    )?;
                    anon_queries.insert(to_view, subquery_leaf);
                }
                TableAliasRewrite::Cte {
                    to_view,
                    mut for_statement,
                    ..
                } => {
                    let subquery_leaf = self.select_query_to_mir(
                        query_name.clone(),
                        for_statement.as_mut(),
                        search_path,
                        None,
                        LeafBehavior::Anonymous,
                        mig,
                    )?;
                    anon_queries.insert(to_view, subquery_leaf);
                }
                TableAliasRewrite::Table { .. } => {}
            }
        }

        // FIXME(REA-2168): Use correct dialect.
        trace!(rewritten_query = %stmt.display(nom_sql::Dialect::MySQL));

        let query_graph = to_query_graph(stmt.clone())?;

        self.mir_converter.named_query_to_mir(
            query_name,
            &query_graph,
            &anon_queries,
            leaf_behavior,
        )
    }

    /// Compile the given uncompiled view all the way to dataflow
    fn compile_uncompiled_view(
        &mut self,
        uncompiled_view: UncompiledView,
        mig: &mut Migration<'_>,
    ) -> ReadySetResult<()> {
        let UncompiledView {
            name,
            mut definition,
            schema_search_path,
        } = uncompiled_view;

        let mir_leaf = match &mut definition {
            SelectSpecification::Compound(stmt) => self.add_compound_query(
                name.clone(),
                stmt,
                &schema_search_path,
                None,
                LeafBehavior::NamedWithoutLeaf,
                mig,
            )?,
            SelectSpecification::Simple(stmt) => self.select_query_to_mir(
                name.clone(),
                stmt,
                &schema_search_path,
                None,
                LeafBehavior::NamedWithoutLeaf,
                mig,
            )?,
        };

        if !self.registry.add_query(RecipeExpr::View {
            name: name.clone(),
            definition,
        })? {
            // The expression is already present, and we successfully added
            // a new alias for it.
            return Ok(());
        }

        self.mir_to_dataflow(name, mir_leaf, mig)?;

        Ok(())
    }

    fn mir_to_dataflow(
        &mut self,
        query_name: Relation,
        mir_leaf: MirNodeIndex,
        mig: &mut Migration<'_>,
    ) -> ReadySetResult<NodeIndex> {
        let on_err = |e| ReadySetError::SelectQueryCreationFailed {
            qname: query_name.display_unquoted().to_string(),
            source: Box::new(e),
        };
        let mir_query = self
            .mir_converter
            .make_mir_query(query_name.clone(), mir_leaf);

        trace!(pre_opt_mir = %mir_query.to_graphviz());
        let mut opt_mir = mir_query.rewrite().map_err(on_err)?;
        trace!(post_opt_mir = %opt_mir.to_graphviz());

        let df_leaf =
            mir_query_to_flow_parts(&mut opt_mir, &self.custom_types, mig).map_err(on_err)?;
        let fields = opt_mir.fields();

        self.register_query(query_name, fields);

        Ok(df_leaf.address())
    }

    pub(super) fn remove_query(
        &mut self,
        query_name: &Relation,
        mig: &mut Migration<'_>,
    ) -> ReadySetResult<MirRemovalResult> {
        trace!(query_name = %query_name.display_unquoted(), "removing query");
        if self.uncompiled_views.remove(query_name).is_some() {
            trace!(query_name = %query_name.display_unquoted(), "Removed uncompiled view");
            return Ok(Default::default());
        }
        let mut mir_removal_result = self.mir_converter.remove_query(query_name)?;
        self.process_removal(&mut mir_removal_result, mig);
        self.view_schemas.remove(query_name);
        Ok(mir_removal_result)
    }

    pub(super) fn remove_base(
        &mut self,
        table_name: &Relation,
        mig: &mut Migration<'_>,
    ) -> ReadySetResult<MirRemovalResult> {
        trace!(table_name = %table_name.display_unquoted(), "removing base table");
        let mut mir_removal_result = self.mir_converter.remove_base(table_name)?;
        self.process_removal(&mut mir_removal_result, mig);
        Ok(mir_removal_result)
    }

    pub(super) fn remove_dependent_queries(
        &mut self,
        table_name: &Relation,
        mig: &mut Migration<'_>,
    ) -> ReadySetResult<MirRemovalResult> {
        trace!(table_name = %table_name.display_unquoted(), "removing base table dependencies");
        let mut mir_removal_result = self.mir_converter.remove_dependent_queries(table_name)?;
        self.process_removal(&mut mir_removal_result, mig);
        Ok(mir_removal_result)
    }

    fn process_removal(&mut self, removal_result: &mut MirRemovalResult, mig: &mut Migration<'_>) {
        for query in removal_result.relations_removed.iter() {
            self.leaf_addresses.remove(query);
            self.registry.remove_expression(query);
            self.view_schemas.remove(query);
        }
        // Sadly, we don't use `DfNodeIndex` for migrations/df state, so we need to map them
        // to `NodeIndex`.
        // TODO(fran): Replace all occurrences of Dataflow node indices for `DfNodeIndex`.
        mig.changes.drop_nodes(
            &removal_result
                .dataflow_nodes_to_remove
                .iter()
                .map(|df_node_idx| df_node_idx.address())
                .collect(),
        );

        // Look for and remove ingress, egress and reader nodes, which are not present in MIR.
        let next_for = |ni: DfNodeIndex| {
            mig.dataflow_state
                .ingredients
                .neighbors_directed(ni.address(), petgraph::EdgeDirection::Outgoing)
                .filter(|ni| !mig.dataflow_state.ingredients[*ni].is_dropped())
                .map(|ni| DfNodeIndex::new(ni))
        };
        let mut removed = Vec::new();
        for node in removal_result.dataflow_nodes_to_remove.iter() {
            let mut stack = next_for(*node).collect::<Vec<_>>();
            while let Some(node) = stack.pop() {
                removed.push(node);
                mig.changes.drop_node(node.address());
                stack.extend(next_for(node));
            }
        }
        removal_result.dataflow_nodes_to_remove.extend(removed);
    }

    fn register_query(&mut self, query_name: Relation, fields: Vec<SqlIdentifier>) {
        debug!(query_name = %query_name.display_unquoted(), "registering query");
        self.view_schemas.insert(query_name, fields);
    }
}
