use std::collections::HashMap;

use readyset_sql::ast::{CreateTableBody, Relation};

use crate::BaseSchemasContext;

/// Schema context for tests that pin a pass to looking up only the relations it reads: walking
/// the whole catalog panics.
pub(crate) struct PerRelationBaseSchemas(pub(crate) HashMap<Relation, CreateTableBody>);

impl BaseSchemasContext for PerRelationBaseSchemas {
    fn base_schemas(&self) -> Box<dyn Iterator<Item = (&Relation, &CreateTableBody)> + '_> {
        panic!("walked the whole catalog")
    }

    fn base_schema(&self, relation: &Relation) -> Option<&CreateTableBody> {
        self.0.get(relation)
    }
}
