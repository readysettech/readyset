use std::sync::Arc;

use readyset_data::Dialect;
use readyset_sql::ast::SqlIdentifier;
use readyset_sql_passes::adapter_rewrites::AdapterRewriteContext;

use crate::SchemaCatalog;

pub struct RewriteContext {
    _dialect: Dialect,
    _schema_catalog: Arc<SchemaCatalog>,
    search_path: Vec<SqlIdentifier>,
}

impl RewriteContext {
    pub fn new(
        _dialect: Dialect,
        _schema_catalog: Arc<SchemaCatalog>,
        search_path: Vec<SqlIdentifier>,
    ) -> Self {
        Self {
            _dialect,
            _schema_catalog,
            search_path,
        }
    }

    pub fn set_search_path(&mut self, search_path: Vec<SqlIdentifier>) {
        self.search_path = search_path;
    }

    pub fn search_path(&self) -> &[SqlIdentifier] {
        &self.search_path
    }
}

impl AdapterRewriteContext for RewriteContext {}
