use std::collections::{HashMap, HashSet};
use std::str;
use std::vec::Vec;

use ::mir::visualize::GraphViz;
use ::mir::DfNodeIndex;
use ::serde::{Deserialize, Serialize};
use nom_sql::{
    CompoundSelectOperator, CompoundSelectStatement, CreateTableBody, FieldDefinitionExpr,
    Relation, SelectSpecification, SelectStatement, SqlIdentifier, TableExpr,
};
use petgraph::graph::NodeIndex;
use readyset_client::recipe::changelist::AlterTypeChange;
use readyset_data::{DfType, Dialect, PgEnumMetadata};
use readyset_errors::{internal_err, invalid_err, ReadySetError, ReadySetResult};
use readyset_sql_passes::alias_removal::TableAliasRewrite;
use readyset_sql_passes::{AliasRemoval, Rewrite, RewriteContext};
use readyset_tracing::{debug, trace};

use self::mir::{NodeIndex as MirNodeIndex, SqlToMirConverter};
use self::query_graph::to_query_graph;
use crate::controller::mir_to_flow::{mir_node_to_flow_parts, mir_query_to_flow_parts};
use crate::controller::Migration;
use crate::sql::mir::MirRemovalResult;
use crate::ReuseConfigType;

pub(crate) mod mir;
mod query_graph;
mod query_signature;

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

/// Long-lived struct that holds information about the SQL queries that have been incorporated into
/// the dataflow graph `graph`.
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

    base_schemas: HashMap<Relation, CreateTableBody>,
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
    // TODO(grfn): This should really be happening as part of the `add_<whatever>` methods (it was,
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
            base_schemas: &self.base_schemas,
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

    /// Add a new table, specified by the given `CREATE TABLE` statement, to the graph, using the
    /// given `mig` to track changes.
    pub(crate) fn add_table(
        &mut self,
        name: Relation,
        body: CreateTableBody,
        mig: &mut Migration<'_>,
    ) -> ReadySetResult<()> {
        let (name, dataflow_idx) = self.add_base_via_mir(name, body, mig)?;
        self.remove_non_replicated_relation(&name);
        self.leaf_addresses.insert(name, dataflow_idx);
        Ok(())
    }

    /// Add a new SQL VIEW, specified by the given `CREATE VIEW` statement, to the graph, using the
    /// given `mig` to track changes.
    pub(crate) fn add_view(
        &mut self,
        name: Relation,
        definition: SelectSpecification,
        mig: &mut Migration<'_>,
    ) -> ReadySetResult<()> {
        let mir_leaf = match definition {
            SelectSpecification::Compound(query) => {
                self.add_compound_query(name.clone(), query, /* is_leaf = */ true, mig)?
            }
            SelectSpecification::Simple(query) => {
                self.add_select_query(name.clone(), query, /* is_leaf = */ true, mig)?
            }
        };
        self.mir_to_dataflow(name.clone(), mir_leaf, mig)?;
        self.remove_non_replicated_relation(&name);
        Ok(())
    }

    /// Add a new query to the graph, using the given `mig` to track changes.
    ///
    /// If `name` is provided, will use that as the name for the query to add, otherwise a unique
    /// name will be generated from the query. In either case, returns the name of the added query.
    pub(crate) fn add_query(
        &mut self,
        name: Option<Relation>,
        stmt: SelectStatement,
        mig: &mut Migration<'_>,
    ) -> ReadySetResult<Relation> {
        let name = name.unwrap_or_else(|| format!("q_{}", self.num_queries).into());
        let mir_query =
            self.add_select_query(name.clone(), stmt, /* is_leaf = */ true, mig)?;

        let leaf = self.mir_to_dataflow(name.clone(), mir_query, mig)?;
        self.leaf_addresses.insert(name.clone(), leaf);

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
                return Err(invalid_err!("Could not find custom type with oid {oid}"));
            };
            self.custom_types_by_oid.insert(oid, name.clone());
            let ty = self
                .custom_types
                .remove(&old_name)
                .expect("custom_types_by_oid must point at types in custom_types");
            self.custom_types.insert(name.clone(), ty);
            trace!(%old_name, new_name = %name, %oid, "Renaming custom type");
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
                            return Err(invalid_err!(
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
                    _ => return Err(invalid_err!("Custom type {name} is not an enum")),
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
    pub(crate) fn non_replicated_relations(&self) -> &HashSet<Relation> {
        &self.mir_converter.non_replicated_relations
    }

    /// Record that a relation (a table or view) with the given `name` exists in the upstream
    /// database, but is not being replicated
    pub(crate) fn add_non_replicated_relation(&mut self, name: Relation) {
        self.mir_converter.non_replicated_relations.insert(name);
    }

    /// Remove the given `name` from the set of tables that are known to exist in the upstream
    /// database, but are not being replicated. Returns whether the table was in the set.
    pub(crate) fn remove_non_replicated_relation(&mut self, name: &Relation) -> bool {
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
            .fields
            .iter()
            .position(|f| f.column == *column)
            .ok_or_else(|| ReadySetError::NoSuchColumn(column.name.clone().into()))?;
        mig.set_column_type(*addr, idx, new_ty)?;

        Ok(())
    }

    pub(super) fn get_base_schema(&self, table: &Relation) -> Option<CreateTableBody> {
        self.base_schemas.get(table).cloned()
    }

    pub(super) fn get_view_schema(&self, name: &Relation) -> Option<Vec<String>> {
        self.view_schemas
            .get(name)
            .map(|s| s.iter().map(SqlIdentifier::to_string).collect())
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

    pub(super) fn is_leaf_address(&self, ni: NodeIndex) -> bool {
        self.leaf_addresses.values().any(|nn| *nn == ni)
    }

    pub(super) fn get_leaf_name(&self, ni: NodeIndex) -> Option<&Relation> {
        self.leaf_addresses
            .iter()
            .find(|(_, idx)| **idx == ni)
            .map(|(name, _)| name)
    }

    fn add_base_via_mir(
        &mut self,
        name: Relation,
        body: CreateTableBody,
        mig: &mut Migration<'_>,
    ) -> ReadySetResult<(Relation, NodeIndex)> {
        // first, compute the MIR representation of the SQL query
        let mir = self.mir_converter.named_base_to_mir(name.clone(), &body)?;

        trace!(base_node_mir = ?mir);

        // no optimization, because standalone base nodes can't be optimized
        let dataflow_node =
            mir_node_to_flow_parts(mir.graph, mir.mir_node, &self.custom_types, mig)?
                .ok_or_else(|| internal_err!("Base MIR nodes must have a Dataflow node assigned"))?
                .address();

        self.base_schemas.insert(name.clone(), body);

        let fields = mir.fields;
        self.register_query(name.clone(), fields);

        Ok((name, dataflow_node))
    }

    fn add_compound_query(
        &mut self,
        query_name: Relation,
        query: CompoundSelectStatement,
        is_leaf: bool,
        mig: &mut Migration<'_>,
    ) -> Result<MirNodeIndex, ReadySetError> {
        let mut subqueries = Vec::new();
        for (_, stmt) in query.selects.into_iter() {
            let subquery_leaf = self.add_select_query(query_name.clone(), stmt, false, mig)?;
            subqueries.push(subquery_leaf);
        }

        let mir_leaf = self.mir_converter.compound_query_to_mir(
            &query_name,
            subqueries,
            CompoundSelectOperator::Union,
            &query.order,
            &query.limit,
            &query.offset,
            is_leaf,
        )?;

        Ok(mir_leaf)
    }

    /// Add a new SelectStatement to the given migration, returning the index of the leaf MIR node
    /// that was added
    fn add_select_query(
        &mut self,
        query_name: Relation,
        mut stmt: SelectStatement,
        is_leaf: bool,
        mig: &mut Migration<'_>,
    ) -> Result<MirNodeIndex, ReadySetError> {
        let on_err = |e| ReadySetError::SelectQueryCreationFailed {
            qname: query_name.to_string(),
            source: Box::new(e),
        };

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
                    let query = SelectStatement {
                        tables: vec![TableExpr::from(for_table)],
                        fields: vec![FieldDefinitionExpr::All],
                        ..Default::default()
                    };
                    let subquery_leaf = self.add_select_query(
                        query_name.clone(),
                        self.rewrite(
                            query,
                            &[], /* Don't need a schema search path since we're only resolving
                                  * one (already qualified) table */
                            mig.dialect,
                            None,
                        )
                        .map_err(on_err)?,
                        false,
                        mig,
                    )?;
                    anon_queries.insert(to_view, subquery_leaf);
                }
                TableAliasRewrite::Cte {
                    to_view,
                    for_statement,
                    ..
                } => {
                    let subquery_leaf = self
                        .add_select_query(query_name.clone(), *for_statement, false, mig)
                        .map_err(on_err)?;
                    anon_queries.insert(to_view, subquery_leaf);
                }
                TableAliasRewrite::Table { .. } => {}
            }
        }

        trace!(rewritten_query = %stmt);

        let query_graph = to_query_graph(stmt).map_err(on_err)?;
        let mir_leaf = self
            .mir_converter
            .named_query_to_mir(&query_name, &query_graph, anon_queries, is_leaf)
            .map_err(on_err)?;

        Ok(mir_leaf)
    }

    fn mir_to_dataflow(
        &mut self,
        query_name: Relation,
        mir_leaf: MirNodeIndex,
        mig: &mut Migration<'_>,
    ) -> ReadySetResult<NodeIndex> {
        let on_err = |e| ReadySetError::SelectQueryCreationFailed {
            qname: query_name.to_string(),
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
        let mut mir_removal_result = self.mir_converter.remove_query(query_name)?;
        self.process_removal(&mut mir_removal_result, mig);
        Ok(mir_removal_result)
    }

    pub(super) fn remove_base(
        &mut self,
        table_name: &Relation,
        mig: &mut Migration<'_>,
    ) -> ReadySetResult<MirRemovalResult> {
        let mut mir_removal_result = self.mir_converter.remove_base(table_name)?;
        self.process_removal(&mut mir_removal_result, mig);
        Ok(mir_removal_result)
    }

    fn process_removal(&mut self, removal_result: &mut MirRemovalResult, mig: &mut Migration<'_>) {
        for query in removal_result.relations_removed.iter() {
            self.leaf_addresses.remove(query);
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
        debug!(%query_name, "registering query");
        self.view_schemas.insert(query_name, fields);
    }
}
