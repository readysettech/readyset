use std::borrow::Cow;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, FieldRef};
use datafusion::catalog::{
    CatalogProvider, CatalogProviderList, MemoryCatalogProvider, MemoryCatalogProviderList,
    MemorySchemaProvider,
};
use datafusion::common::DFSchema;
use datafusion::config::ConfigOptions;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::{SessionState, SessionStateBuilder};
use datafusion::functions::utils::make_scalar_function;
use datafusion::logical_expr::{ScalarUDF, Volatility};
use datafusion::prelude::{SQLOptions, SessionConfig, SessionContext, create_udf};

use readyset_errors::ReadySetResult;
use readyset_sql::Dialect;

pub mod mysql;
pub mod psql;

const CATALOG: &str = "readyset";

pub struct ReadysetSchema {
    name: String,
    session_init: SessionState,
    sql_opts: SQLOptions,
}

impl ReadysetSchema {
    pub fn init(name: &str, dialect: Dialect) -> ReadySetResult<Arc<Self>> {
        let runtime = Arc::new(RuntimeEnv::default());
        let catalog = Arc::new(MemoryCatalogProvider::new());
        let schema = Arc::new(MemorySchemaProvider::new());
        catalog.register_schema(name, schema)?;
        let catalog_list = MemoryCatalogProviderList::new();
        catalog_list.register_catalog(CATALOG.into(), catalog);

        let config = {
            let mut config = ConfigOptions::new();
            config.catalog.create_default_catalog_and_schema = false;
            config.catalog.default_catalog = CATALOG.into();
            config.catalog.default_schema = name.into();
            config.catalog.information_schema = true;
            config.sql_parser.dialect = match dialect {
                Dialect::MySQL => datafusion::config::Dialect::MySQL,
                Dialect::PostgreSQL => datafusion::config::Dialect::PostgreSQL,
            };
            SessionConfig::from(config)
        };
        let session_init = SessionStateBuilder::new()
            .with_default_features()
            .with_runtime_env(Arc::clone(&runtime))
            .with_config(config)
            .with_catalog_list(Arc::new(catalog_list))
            .with_scalar_functions(Self::init_functions(name))
            .build();

        let sql_opts = SQLOptions::new().with_allow_ddl(false);

        Ok(Arc::new(Self {
            name: name.into(),
            session_init,
            sql_opts,
        }))
    }

    /// Create some additional functions we want available in the Readyset schema.
    fn init_functions(schema_name: &str) -> Vec<Arc<ScalarUDF>> {
        let database = schema_name.to_string();
        vec![Arc::new(create_udf(
            "database",
            vec![],
            DataType::Utf8,
            Volatility::Immutable,
            Arc::new(make_scalar_function(
                move |_| Ok(Arc::new(StringArray::from(vec![database.as_ref()]))),
                vec![],
            )),
        ))]
    }

    /// The name of the Readyset schema.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get a session for running SQL against the Readyset schema.
    pub fn session(&self) -> ReadysetSchemaSession {
        let session = SessionContext::new_with_state(self.session_init.clone());
        let sql_opts = self.sql_opts;
        ReadysetSchemaSession { session, sql_opts }
    }
}

/// Represents a single session to the Readyset schema.
pub struct ReadysetSchemaSession {
    session: SessionContext,
    sql_opts: SQLOptions,
}

impl ReadysetSchemaSession {
    pub async fn query(&self, query: &str) -> ReadySetResult<ReadysetSchemaResult> {
        let df = self.session.sql_with_options(query, self.sql_opts).await?;
        let schema = df.schema().clone();
        let results = df.collect().await?;
        Ok(ReadysetSchemaResult::Select { schema, results })
    }
}

/// An iterator over a set of Readyset schema query results.
pub struct ReadysetSchemaResultIter<'a> {
    results: Cow<'a, [RecordBatch]>,
    batch_idx: usize,
    row_idx: usize,
}

impl<'a> ReadysetSchemaResultIter<'a> {
    pub fn next_row(&mut self) -> Option<(impl Iterator<Item = &ArrayRef>, usize)> {
        loop {
            if self.batch_idx >= self.results.len() {
                return None;
            }
            let batch = &self.results[self.batch_idx];
            if self.row_idx < batch.num_rows() {
                let row = self.row_idx;
                self.row_idx += 1;
                return Some((batch.columns().iter(), row));
            }
            self.batch_idx += 1;
            self.row_idx = 0;
        }
    }
}

/// A result produced by a query against the Readyset schema.
#[derive(Debug)]
pub enum ReadysetSchemaResult {
    Select {
        schema: DFSchema,
        results: Vec<RecordBatch>,
    },
}

impl ReadysetSchemaResult {
    pub fn schema(&self) -> &[FieldRef] {
        match self {
            ReadysetSchemaResult::Select { schema, .. } => schema.fields(),
        }
    }

    pub fn borrowed_iter(&self) -> ReadysetSchemaResultIter<'_> {
        match self {
            ReadysetSchemaResult::Select { results, .. } => ReadysetSchemaResultIter {
                results: Cow::Borrowed(results.as_slice()),
                batch_idx: 0,
                row_idx: 0,
            },
        }
    }

    pub fn owned_iter(self) -> ReadysetSchemaResultIter<'static> {
        match self {
            ReadysetSchemaResult::Select { results, .. } => ReadysetSchemaResultIter {
                results: Cow::Owned(results),
                batch_idx: 0,
                row_idx: 0,
            },
        }
    }
}
