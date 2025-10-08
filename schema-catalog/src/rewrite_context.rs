use std::sync::Arc;

use readyset_data::Dialect;
use readyset_sql::ast::{NonReplicatedRelation, Relation, SqlIdentifier};
use readyset_sql_passes::{
    CanQuery, ResolveSchemasContext, RewriteDialectContext, StarExpansionContext,
    adapter_rewrites::AdapterRewriteContext,
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
            .or_else(|| self.schema_catalog.uncompiled_views.get(relation))
            .cloned()
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
