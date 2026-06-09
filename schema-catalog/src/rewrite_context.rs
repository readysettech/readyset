use std::sync::Arc;

use readyset_data::Dialect;
use readyset_sql::ast::{CreateTableBody, NonReplicatedRelation, Relation, SqlIdentifier};
use readyset_sql_passes::{
    BaseSchemasContext, CanQuery, ImpliedTablesContext, ResolveSchemasContext,
    RewriteDialectContext, StarExpansionContext, adapter_rewrites::AdapterRewriteContext,
};

use crate::{SchemaCatalog, SchemaGeneration};

pub struct RewriteContext {
    dialect: Dialect,
    schema_catalog: Arc<SchemaCatalog>,
    search_path: Vec<SqlIdentifier>,
}

impl RewriteContext {
    pub fn new(
        dialect: Dialect,
        schema_catalog: Arc<SchemaCatalog>,
        search_path: Vec<SqlIdentifier>,
    ) -> Self {
        Self {
            dialect,
            schema_catalog,
            search_path,
        }
    }

    pub fn set_search_path(&mut self, search_path: Vec<SqlIdentifier>) {
        self.search_path = search_path;
    }

    pub fn search_path(&self) -> &[SqlIdentifier] {
        &self.search_path
    }

    fn is_non_replicated_relation(&self, relation: &Relation) -> bool {
        // `non_replicated_relations` may contain either schema-qualified or unqualified relations
        // depending on where it originated. Be permissive and match by name when schema
        // qualification differs.
        if self
            .schema_catalog
            .non_replicated_relations
            .contains(&NonReplicatedRelation::new(relation.clone()))
        {
            return true;
        }

        if relation.schema.is_some() {
            let mut unqualified = relation.clone();
            unqualified.schema = None;
            if self
                .schema_catalog
                .non_replicated_relations
                .contains(&NonReplicatedRelation::new(unqualified))
            {
                return true;
            }
        }

        self.schema_catalog
            .non_replicated_relations
            .iter()
            .any(|nr| {
                nr.name.name == relation.name
                    && (nr.name.schema.is_none() || nr.name.schema == relation.schema)
            })
    }
}

impl RewriteContext {
    pub fn schema_generation(&self) -> SchemaGeneration {
        self.schema_catalog.generation
    }
}
impl AdapterRewriteContext for RewriteContext {}

impl BaseSchemasContext for RewriteContext {
    fn base_schemas(&self) -> Box<dyn Iterator<Item = (&Relation, &CreateTableBody)> + '_> {
        Box::new(self.schema_catalog.base_schemas.iter())
    }

    fn base_schema(&self, relation: &Relation) -> Option<&CreateTableBody> {
        self.schema_catalog.base_schemas.get(relation)
    }
}

impl ResolveSchemasContext for RewriteContext {
    fn add_invalidating_table(&self, _table: Relation) {
        // noop
    }

    fn can_query_table(&self, schema: &SqlIdentifier, table: &SqlIdentifier) -> Option<CanQuery> {
        let relation = Relation {
            schema: Some(schema.clone()),
            name: table.clone(),
        };

        if self.schema_catalog.view_schemas.contains_key(&relation)
            || self.schema_catalog.base_schemas.contains_key(&relation)
            || self.schema_catalog.uncompiled_views.contains_key(&relation)
        {
            Some(CanQuery::Yes)
        } else if self.is_non_replicated_relation(&relation) {
            Some(CanQuery::No)
        } else {
            None
        }
    }

    fn search_path(&self) -> &[SqlIdentifier] {
        self.search_path()
    }

    fn schema_contains_custom_type(
        &self,
        schema: &SqlIdentifier,
        custom_type: &SqlIdentifier,
    ) -> bool {
        self.schema_catalog
            .custom_types
            .get(schema)
            .is_some_and(|tys| tys.contains(custom_type))
    }
}

impl StarExpansionContext for RewriteContext {
    fn schema_for_relation(
        &self,
        relation: &Relation,
    ) -> Option<impl IntoIterator<Item = SqlIdentifier>> {
        self.schema_catalog
            .view_schemas
            .get(relation)
            .cloned()
            .map(|cols| {
                // Filter out invisible columns for base tables during SELECT * expansion.
                // Views cannot have invisible columns, so only base tables need filtering.
                if let Some(body) = self.schema_catalog.base_schemas.get(relation) {
                    cols.into_iter()
                        .zip(body.fields.iter())
                        .filter(|(_, spec)| !spec.invisible)
                        .map(|(name, _)| name)
                        .collect::<Vec<_>>()
                } else {
                    cols
                }
            })
            .or_else(|| self.schema_catalog.uncompiled_views.get(relation).cloned())
    }

    fn is_relation_non_replicated(&self, relation: &Relation) -> bool {
        self.schema_catalog
            .non_replicated_relations
            .contains(&NonReplicatedRelation::new(relation.clone()))
    }
}

impl RewriteDialectContext for RewriteContext {
    fn dialect(&self) -> Dialect {
        self.dialect
    }
}

impl ImpliedTablesContext for RewriteContext {
    fn all_schemas(&self) -> impl IntoIterator<Item = (Relation, Vec<SqlIdentifier>)> {
        // Both compiled views/base tables and uncompiled views need to be
        // visible to `expand_implied_tables`; otherwise unqualified column
        // references whose owning relation is an uncompiled VIEW go
        // unresolved and trip `validate_pipeline_invariants`'s "Unresolved
        // column" internal error.  When a relation appears in both buckets
        // (e.g. transient inconsistency during compile), `view_schemas`
        // wins via `entry(..).or_insert_with(..)`.
        let mut combined = self.schema_catalog.view_schemas.clone();
        for (rel, cols) in &self.schema_catalog.uncompiled_views {
            combined.entry(rel.clone()).or_insert_with(|| cols.clone());
        }
        combined
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use super::*;

    fn ctx_with(
        view_schemas: HashMap<Relation, Vec<SqlIdentifier>>,
        uncompiled_views: HashMap<Relation, Vec<SqlIdentifier>>,
    ) -> RewriteContext {
        let catalog = SchemaCatalog {
            generation: SchemaGeneration::INITIAL,
            base_schemas: HashMap::new(),
            uncompiled_views,
            custom_types: HashMap::new(),
            view_schemas,
            non_replicated_relations: HashSet::new(),
        };
        RewriteContext::new(Dialect::DEFAULT_POSTGRESQL, Arc::new(catalog), vec![])
    }

    /// Uncompiled views must be visible to `all_schemas` so column resolution
    /// can match unqualified columns against them.  Pre-fix the production
    /// impl returned only `view_schemas`, dropping uncompiled views and
    /// causing the "Unresolved column" internal error.
    #[test]
    fn all_schemas_includes_uncompiled_views() {
        let view_schemas = HashMap::from([(
            Relation::from("supplier"),
            vec!["s_suppkey".into(), "s_name".into()],
        )]);
        let uncompiled_views = HashMap::from([(
            Relation::from("revenues"),
            vec!["supplier_no".into(), "total_revenue".into()],
        )]);
        let ctx = ctx_with(view_schemas, uncompiled_views);
        let combined: HashMap<Relation, Vec<SqlIdentifier>> =
            ctx.all_schemas().into_iter().collect();
        assert_eq!(combined.len(), 2);
        assert_eq!(
            combined[&Relation::from("revenues")],
            vec![
                SqlIdentifier::from("supplier_no"),
                SqlIdentifier::from("total_revenue")
            ]
        );
    }

    /// On key collision, `view_schemas` wins.  The same relation appearing
    /// in both buckets is a transient state during compile; the compiled
    /// schema is the authoritative one.
    #[test]
    fn all_schemas_prefers_view_schemas_on_collision() {
        let view_schemas =
            HashMap::from([(Relation::from("revenues"), vec!["compiled_col".into()])]);
        let uncompiled_views =
            HashMap::from([(Relation::from("revenues"), vec!["uncompiled_col".into()])]);
        let ctx = ctx_with(view_schemas, uncompiled_views);
        let combined: HashMap<Relation, Vec<SqlIdentifier>> =
            ctx.all_schemas().into_iter().collect();
        assert_eq!(combined.len(), 1);
        assert_eq!(
            combined[&Relation::from("revenues")],
            vec![SqlIdentifier::from("compiled_col")]
        );
    }

    #[test]
    fn expand_implied_tables_resolves_uncompiled_view_columns_q15_shape() {
        use readyset_sql::{Dialect as SqlDialect, DialectDisplay};
        use readyset_sql_parsing::{ParsingPreset, parse_query_with_config};
        use readyset_sql_passes::ImpliedTableExpansion;

        let view_schemas = HashMap::from([(
            Relation::from("supplier"),
            vec![
                "s_suppkey".into(),
                "s_name".into(),
                "s_address".into(),
                "s_phone".into(),
            ],
        )]);
        let uncompiled_views = HashMap::from([(
            Relation::from("revenues"),
            vec!["supplier_no".into(), "total_revenue".into()],
        )]);
        let ctx = ctx_with(view_schemas, uncompiled_views);

        let mut q = parse_query_with_config(
            ParsingPreset::OnlySqlparser,
            SqlDialect::PostgreSQL,
            "SELECT s_suppkey, s_name, total_revenue \
             FROM supplier, revenues \
             WHERE s_suppkey = supplier_no \
             ORDER BY s_suppkey",
        )
        .expect("Q15-shape query parses");

        q.expand_implied_tables(&ctx)
            .expect("expand_implied_tables succeeds when uncompiled views are visible");

        let expected = parse_query_with_config(
            ParsingPreset::OnlySqlparser,
            SqlDialect::PostgreSQL,
            "SELECT \"supplier\".\"s_suppkey\", \"supplier\".\"s_name\", \"revenues\".\"total_revenue\" \
             FROM \"supplier\", \"revenues\" \
             WHERE \"supplier\".\"s_suppkey\" = \"revenues\".\"supplier_no\" \
             ORDER BY \"supplier\".\"s_suppkey\"",
        )
        .expect("expected query parses");

        assert_eq!(
            q,
            expected,
            "\n  left: {}\n right: {}",
            q.display(SqlDialect::PostgreSQL),
            expected.display(SqlDialect::PostgreSQL)
        );
    }
}
