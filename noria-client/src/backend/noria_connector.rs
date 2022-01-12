use noria::results::Results;
use noria::{
    consistency::Timestamp, internal::LocalNodeIndex, ControllerHandle, ReadySetError,
    ReadySetResult, SchemaType, Table, TableOperation, View, ViewQuery, ViewQueryFilter,
    ViewQueryOperator,
};
use noria_data::DataType;

use nom_sql::{
    self, BinaryOperator, ColumnConstraint, DeleteStatement, InsertStatement, SelectStatement,
    SqlQuery, UpdateStatement,
};
use vec1::vec1;

use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::convert::{TryFrom, TryInto};
use std::sync::atomic;
use std::sync::{Arc, RwLock};

use crate::rewrite::{self, ProcessedQueryParams};
use crate::utils;

use crate::backend::SelectSchema;
use itertools::Itertools;
use noria::ColumnSchema;
use noria_errors::ReadySetError::PreparedStatementMissing;
use noria_errors::{internal, internal_err, invariant_eq, table_err, unsupported, unsupported_err};
use std::fmt;
use tracing::{error, info, trace};

type StatementID = u32;

#[derive(Clone)]
pub(crate) enum PreparedStatement {
    Select(PreparedSelectStatement),
    Insert(nom_sql::InsertStatement),
    Update(nom_sql::UpdateStatement),
    Delete(DeleteStatement),
}

#[derive(Clone)]
pub(crate) struct PreparedSelectStatement {
    name: String,
    statement: Box<nom_sql::SelectStatement>,
    key_column_indices: Vec<usize>,
    processed_query_params: ProcessedQueryParams,
    /// Parameter columns ignored by noria server
    /// The adapter assumes that all LIMIT/OFFSET parameters are ignored by the
    /// server. (If the server cannot ignore them, it will fail to install the query).
    ignored_columns: Vec<ColumnSchema>,
}

impl fmt::Debug for PreparedStatement {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match self {
            PreparedStatement::Select(PreparedSelectStatement {
                name, statement, ..
            }) => write!(f, "{}: {}", name, statement),
            PreparedStatement::Insert(s) => write!(f, "{}", s),
            PreparedStatement::Update(s) => write!(f, "{}", s),
            PreparedStatement::Delete(s) => write!(f, "{}", s),
        }
    }
}

/// Wrapper around a NoriaBackendInner which may not have been successfully
/// created. When this is the case, this wrapper allows returning an error
/// from any call that requires NoriaBackendInner through an error
/// returned by `get_mut`.
#[derive(Clone)]
pub struct NoriaBackend {
    inner: Option<NoriaBackendInner>,
}

impl NoriaBackend {
    async fn get_mut(&mut self) -> ReadySetResult<&mut NoriaBackendInner> {
        // TODO(ENG-707): Support retrying to create a backend in the future.
        self.inner
            .as_mut()
            .ok_or_else(|| internal_err("Failed to create a Noria backend."))
    }
}

pub struct NoriaBackendInner {
    noria: ControllerHandle,
    inputs: BTreeMap<String, Table>,
    outputs: BTreeMap<String, View>,
}

impl Clone for NoriaBackendInner {
    fn clone(&self) -> Self {
        Self {
            noria: self.noria.clone(),
            inputs: self.inputs.clone(),
            outputs: self.outputs.clone(),
        }
    }
}

macro_rules! noria_await {
    ($self:expr, $fut:expr) => {{
        let noria = &mut $self.noria;

        futures_util::future::poll_fn(|cx| noria.poll_ready(cx)).await?;
        $fut.await
    }};
}

impl NoriaBackendInner {
    async fn new(ch: ControllerHandle) -> ReadySetResult<Self> {
        Ok(NoriaBackendInner {
            inputs: BTreeMap::new(),
            outputs: BTreeMap::new(),
            noria: ch,
        })
    }

    async fn get_noria_table(&mut self, table: &str) -> ReadySetResult<&mut Table> {
        if !self.inputs.contains_key(table) {
            let t = noria_await!(self, self.noria.table(table))?;
            self.inputs.insert(table.to_owned(), t);
        }
        Ok(self.inputs.get_mut(table).unwrap())
    }

    /// If `ignore_cache` is passed, the view cache, `outputs` will be ignored and a
    /// view will be retrieve from noria.
    async fn get_noria_view(
        &mut self,
        view: &str,
        region: Option<&str>,
        invalidate_cache: bool,
    ) -> ReadySetResult<&mut View> {
        if invalidate_cache {
            self.outputs.remove(view);
        }
        if !self.outputs.contains_key(view) {
            let vh = match region {
                None => noria_await!(self, self.noria.view(view))?,
                Some(r) => noria_await!(self, self.noria.view_from_region(view, r))?,
            };
            self.outputs.insert(view.to_owned(), vh);
        }
        Ok(self.outputs.get_mut(view).unwrap())
    }
}

#[derive(Debug)]
pub enum PrepareResult {
    Select {
        statement_id: u32,
        params: Vec<ColumnSchema>,
        schema: Vec<ColumnSchema>,
    },
    Insert {
        statement_id: u32,
        params: Vec<ColumnSchema>,
        schema: Vec<ColumnSchema>,
    },
    Update {
        statement_id: u32,
        params: Vec<ColumnSchema>,
    },
    Delete {
        statement_id: u32,
        params: Vec<ColumnSchema>,
    },
}

/// A single row in the variable table associated with [`QueryResult::MetaVariables`].
#[derive(Debug)]
pub struct MetaVariable {
    /// The variable name.
    pub name: String,
    /// The value associated with the variable.
    pub value: String,
}

#[derive(Debug)]
pub enum QueryResult<'a> {
    Empty,
    Insert {
        num_rows_inserted: u64,
        first_inserted_id: u64,
    },
    Select {
        data: Vec<Results>,
        select_schema: SelectSchema<'a>,
    },
    Update {
        num_rows_updated: u64,
        last_inserted_id: u64,
    },
    Delete {
        num_rows_deleted: u64,
    },
    /// A metadata string returned as a response to eg an EXPLAIN query
    Meta {
        /// The label for the metadata, used as a column header when writing results
        label: String,
        /// The actual value
        value: String,
    },
    /// A table of variables returned as a response to a SHOW READYSET STATUS query.
    MetaVariables(Vec<MetaVariable>),
}

impl<'a> QueryResult<'a> {
    #[inline]
    pub fn into_owned(self) -> QueryResult<'static> {
        match self {
            QueryResult::Select {
                data,
                select_schema,
            } => QueryResult::Select {
                data,
                select_schema: select_schema.into_owned(),
            },
            // Have to manually pass each variant to convince rustc that the
            // returned type is really owned
            QueryResult::Empty => QueryResult::Empty,
            QueryResult::Insert {
                num_rows_inserted,
                first_inserted_id,
            } => QueryResult::Insert {
                num_rows_inserted,
                first_inserted_id,
            },
            QueryResult::Update {
                num_rows_updated,
                last_inserted_id,
            } => QueryResult::Update {
                num_rows_updated,
                last_inserted_id,
            },
            QueryResult::Delete { num_rows_deleted } => QueryResult::Delete { num_rows_deleted },
            QueryResult::Meta { label, value } => QueryResult::Meta { label, value },
            QueryResult::MetaVariables(vec) => QueryResult::MetaVariables(vec),
        }
    }
}

pub struct NoriaConnector {
    inner: NoriaBackend,
    auto_increments: Arc<RwLock<HashMap<String, atomic::AtomicUsize>>>,
    /// global cache of view endpoints and prepared statements
    cached: Arc<RwLock<HashMap<SelectStatement, String>>>,
    /// thread-local version of `cached` (consulted first)
    tl_cached: HashMap<SelectStatement, String>,
    prepared_statement_cache: HashMap<StatementID, PreparedStatement>,
    /// The region to pass to noria for replica selection.
    region: Option<String>,

    /// Set of views that have failed on previous requests. Separate from the backend
    /// to allow returning references to schemas from views all the way to msql-srv,
    /// but on subsequent requests, do not use a failed view.
    failed_views: HashSet<String>,
}

/// Removes limit and offset params passed in with an execute function. These are not sent to the
/// server.
fn pop_limit_offset_params<'param>(
    mut params: &'param [DataType],
    ignored_columns: &[ColumnSchema],
) -> (
    Option<&'param DataType>,
    Option<&'param DataType>,
    &'param [DataType],
) {
    let mut offset = None;
    let mut row_count = None;

    if ignored_columns
        .iter()
        .any(|col| matches!(col.spec.column.name.as_str(), "__offset"))
    {
        offset = params.split_last().map(|(last, rest)| {
            params = rest;
            last
        });
    }
    if ignored_columns
        .iter()
        .any(|col| matches!(col.spec.column.name.as_str(), "__row_count"))
    {
        row_count = params.split_last().map(|(last, rest)| {
            params = rest;
            last
        });
    }

    (offset, row_count, params)
}

/// Used when we can determine that the params for 'OFFSET ?' or 'LIMIT ?' passed in
/// with an execute statement will result in an empty resultset
async fn short_circuit_empty_resultset(getter: &mut View) -> ReadySetResult<QueryResult<'_>> {
    let getter_schema = getter
        .schema()
        .ok_or_else(|| internal_err("No schema for view"))?;
    Ok(QueryResult::Select {
        data: vec![],
        select_schema: SelectSchema {
            use_bogo: false,
            schema: Cow::Borrowed(getter_schema.schema(SchemaType::ReturnedSchema)),
            columns: Cow::Borrowed(getter.columns()),
        },
    })
}

impl Clone for NoriaConnector {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            auto_increments: self.auto_increments.clone(),
            cached: self.cached.clone(),
            tl_cached: self.tl_cached.clone(),
            prepared_statement_cache: self.prepared_statement_cache.clone(),
            region: self.region.clone(),
            failed_views: self.failed_views.clone(),
        }
    }
}

impl NoriaConnector {
    pub async fn new(
        ch: ControllerHandle,
        auto_increments: Arc<RwLock<HashMap<String, atomic::AtomicUsize>>>,
        query_cache: Arc<RwLock<HashMap<SelectStatement, String>>>,
        region: Option<String>,
    ) -> Self {
        let backend = NoriaBackendInner::new(ch).await;
        if let Err(e) = &backend {
            error!(%e, "Error creating a noria backend");
        }

        NoriaConnector {
            inner: NoriaBackend {
                inner: backend.ok(),
            },
            auto_increments,
            cached: query_cache,
            tl_cached: HashMap::new(),
            prepared_statement_cache: HashMap::new(),
            region,
            failed_views: HashSet::new(),
        }
    }

    pub(crate) async fn graphviz(
        &mut self,
        simplified: bool,
    ) -> ReadySetResult<QueryResult<'static>> {
        let noria = &mut self.inner.get_mut().await?.noria;

        let (label, graphviz) = if simplified {
            ("SIMPLIFIED GRAPHVIZ", noria.simple_graphviz().await?)
        } else {
            ("GRAPHVIZ", noria.graphviz().await?)
        };

        Ok(QueryResult::Meta {
            label: label.to_owned(),
            value: graphviz,
        })
    }

    pub(crate) async fn verbose_outputs(&mut self) -> ReadySetResult<QueryResult<'_>> {
        let noria = &mut self.inner.get_mut().await?.noria;
        let outputs = noria.verbose_outputs().await?;
        //TODO(DAN): this is ridiculous, update Meta instead
        let select_schema = SelectSchema {
            use_bogo: false,
            schema: Cow::Owned(vec![
                ColumnSchema {
                    spec: nom_sql::ColumnSpecification {
                        column: nom_sql::Column {
                            name: "name".to_string(),
                            table: None,
                            function: None,
                        },
                        sql_type: nom_sql::SqlType::Text,
                        constraints: vec![],
                        comment: None,
                    },
                    base: None,
                },
                ColumnSchema {
                    spec: nom_sql::ColumnSpecification {
                        column: nom_sql::Column {
                            name: "query".to_string(),
                            table: None,
                            function: None,
                        },
                        sql_type: nom_sql::SqlType::Text,
                        constraints: vec![],
                        comment: None,
                    },
                    base: None,
                },
            ]),

            columns: Cow::Owned(vec!["name".to_string(), "query".to_string()]),
        };
        let data = outputs
            .into_iter()
            .map(|(n, q)| vec![DataType::from(n), DataType::from(q.to_string())])
            .collect::<Vec<_>>();
        let data = vec![Results::new(
            data,
            Arc::new(["name".to_string(), "query".to_string()]),
        )];
        Ok(QueryResult::Select {
            data,
            select_schema,
        })
    }

    // TODO(andrew): Allow client to map table names to NodeIndexes without having to query Noria
    // repeatedly. Eventually, this will be responsibility of the TimestampService.
    pub async fn node_index_of(&mut self, table_name: &str) -> ReadySetResult<LocalNodeIndex> {
        let table_handle = self.inner.get_mut().await?.noria.table(table_name).await?;
        Ok(table_handle.node)
    }

    pub async fn handle_insert(
        &mut self,
        q: &nom_sql::InsertStatement,
    ) -> ReadySetResult<QueryResult<'_>> {
        let table = &q.table.name;

        // create a mutator if we don't have one for this table already
        trace!(%table, "query::insert::access mutator");
        let putter = self.inner.get_mut().await?.get_noria_table(table).await?;
        trace!("query::insert::extract schema");
        let schema = putter
            .schema()
            .ok_or_else(|| internal_err(format!("no schema for table '{}'", table)))?;

        // set column names (insert schema) if not set
        let q = match q.fields {
            Some(_) => Cow::Borrowed(q),
            None => {
                let mut query = q.clone();
                query.fields = Some(schema.fields.iter().map(|cs| cs.column.clone()).collect());
                Cow::Owned(query)
            }
        };

        let data: Vec<Vec<DataType>> = q
            .data
            .iter()
            .map(|row| {
                row.iter()
                    .map(DataType::try_from)
                    .collect::<Result<Vec<_>, _>>()
            })
            .collect::<Result<Vec<_>, _>>()?;

        self.do_insert(&q, data).await
    }

    pub async fn prepare_insert(
        &mut self,
        mut q: nom_sql::InsertStatement,
        statement_id: u32,
    ) -> ReadySetResult<PrepareResult> {
        trace!(table = %q.table.name, "insert::access mutator");
        let mutator = self
            .inner
            .get_mut()
            .await?
            .get_noria_table(&q.table.name)
            .await?;
        trace!("insert::extract schema");
        let schema = mutator
            .schema()
            .ok_or_else(|| {
                internal_err(format!("Could not find schema for table {}", q.table.name))
            })?
            .fields
            .iter()
            .map(|cs| ColumnSchema::from_base(cs.clone(), q.table.name.clone()))
            .collect::<Vec<_>>();

        if q.fields.is_none() {
            q.fields = Some(
                mutator
                    .schema()
                    .as_ref()
                    .unwrap()
                    .fields
                    .iter()
                    .map(|cs| cs.column.clone())
                    .collect(),
            );
        }

        let params: Vec<_> = {
            // extract parameter columns -- easy here, since they must all be in the same table
            let param_cols = utils::insert_statement_parameter_columns(&q);
            param_cols
                .into_iter()
                .map(|c| {
                    schema
                        .iter()
                        .cloned()
                        .find(|mc| c.name == mc.spec.column.name)
                        .ok_or_else(|| {
                            internal_err(format!("column '{}' missing in mutator schema", c))
                        })
                })
                .collect::<ReadySetResult<Vec<_>>>()?
        };

        // nothing more to do for an insert
        // register a new prepared statement
        trace!(id = statement_id, "insert::registered");
        self.prepared_statement_cache
            .insert(statement_id, PreparedStatement::Insert(q));
        Ok(PrepareResult::Insert {
            statement_id,
            params,
            schema,
        })
    }

    pub(crate) async fn execute_prepared_insert(
        &mut self,
        q_id: u32,
        params: &[DataType],
    ) -> ReadySetResult<QueryResult<'_>> {
        let prep: PreparedStatement = self
            .prepared_statement_cache
            .get(&q_id)
            .ok_or(PreparedStatementMissing { statement_id: q_id })?
            .clone();
        trace!("delegate");
        match prep {
            PreparedStatement::Insert(ref q) => {
                let table = &q.table.name;
                let putter = self.inner.get_mut().await?.get_noria_table(table).await?;
                trace!("insert::extract schema");
                let schema = putter
                    .schema()
                    .ok_or_else(|| internal_err(format!("no schema for table '{}'", table)))?;
                // unwrap: safe because we always pass in Some(params) so don't hit None path of coerce_params
                let coerced_params =
                    utils::coerce_params(Some(params), &SqlQuery::Insert(q.clone()), schema)
                        .unwrap()
                        .unwrap();
                return self.do_insert(q, vec![coerced_params]).await;
            }
            _ => {
                internal!(
                    "Execute_prepared_insert is being called for a non insert prepared statement."
                );
            }
        };
    }

    pub(crate) async fn handle_delete(
        &mut self,
        q: &nom_sql::DeleteStatement,
    ) -> ReadySetResult<QueryResult<'_>> {
        let cond = q
            .where_clause
            .as_ref()
            .ok_or_else(|| unsupported_err("only supports DELETEs with WHERE-clauses"))?;

        // create a mutator if we don't have one for this table already
        trace!(table = %q.table.name, "delete::access mutator");
        let mutator = self
            .inner
            .get_mut()
            .await?
            .get_noria_table(&q.table.name)
            .await?;

        trace!("delete::extract schema");
        let pkey = if let Some(cts) = mutator.schema() {
            utils::get_primary_key(cts)
                .into_iter()
                .map(|(_, c)| c)
                .collect::<Vec<_>>()
        } else {
            unsupported!("cannot delete from view");
        };

        trace!("delete::flatten conditionals");
        match utils::flatten_conditional(cond, &pkey)? {
            None => Ok(QueryResult::Delete {
                num_rows_deleted: 0_u64,
            }),
            Some(ref flattened) if flattened.is_empty() => {
                unsupported!("DELETE only supports WHERE-clauses on primary keys")
            }
            Some(flattened) => {
                let count = flattened.len() as u64;
                trace!("delete::execute");
                for key in flattened {
                    if let Err(e) = mutator.delete(key).await {
                        error!(error = %e, "failed");
                        return Err(e);
                    };
                }
                trace!("delete::done");
                Ok(QueryResult::Delete {
                    num_rows_deleted: count,
                })
            }
        }
    }

    pub(crate) async fn handle_update(
        &mut self,
        q: &nom_sql::UpdateStatement,
    ) -> ReadySetResult<QueryResult<'_>> {
        self.do_update(Cow::Borrowed(q), None).await
    }

    pub(crate) async fn prepare_update(
        &mut self,
        q: nom_sql::UpdateStatement,
        statement_id: u32,
    ) -> ReadySetResult<PrepareResult> {
        // ensure that we have schemas and endpoints for the query
        trace!(table = %q.table.name, "update::access mutator");
        let mutator = self
            .inner
            .get_mut()
            .await?
            .get_noria_table(&q.table.name)
            .await?;
        trace!("update::extract schema");
        let table_schema = mutator.schema().ok_or_else(|| {
            internal_err(format!("Could not find schema for table {}", q.table.name))
        })?;

        // extract parameter columns
        let params = utils::update_statement_parameter_columns(&q)
            .into_iter()
            .map(|c| {
                table_schema
                    .fields
                    .iter()
                    // We know that only one table is mentioned, so no need to match on both table
                    // and name - just check name here
                    .find(|f| f.column.name == c.name)
                    .cloned()
                    .map(|cs| ColumnSchema::from_base(cs, q.table.name.clone()))
                    .ok_or_else(|| internal_err(format!("Unknown column {}", c)))
            })
            .collect::<Result<Vec<_>, _>>()?;

        trace!(id = statement_id, "update::registered");
        self.prepared_statement_cache
            .insert(statement_id, PreparedStatement::Update(q));
        Ok(PrepareResult::Update {
            statement_id,
            params,
        })
    }

    pub(crate) async fn execute_prepared_update(
        &mut self,
        q_id: u32,
        params: &[DataType],
    ) -> ReadySetResult<QueryResult<'_>> {
        let prep: PreparedStatement = self
            .prepared_statement_cache
            .get(&q_id)
            .ok_or(PreparedStatementMissing { statement_id: q_id })?
            .clone();

        trace!("delegate");
        match prep {
            PreparedStatement::Update(q) => {
                return self.do_update(Cow::Owned(q), Some(params)).await
            }
            _ => internal!(),
        };
    }

    pub(crate) async fn prepare_delete(
        &mut self,
        q: DeleteStatement,
        statement_id: u32,
    ) -> ReadySetResult<PrepareResult> {
        // ensure that we have schemas and endpoints for the query
        trace!(table = %q.table.name, "delete::access mutator");
        let mutator = self
            .inner
            .get_mut()
            .await?
            .get_noria_table(&q.table.name)
            .await?;
        trace!("delete::extract schema");
        let table_schema = mutator.schema().ok_or_else(|| {
            internal_err(format!("Could not find schema for table {}", q.table.name))
        })?;

        // extract parameter columns
        let params = utils::delete_statement_parameter_columns(&q)
            .into_iter()
            .map(|c| {
                table_schema
                    .fields
                    .iter()
                    // We know that only one table is mentioned, so no need to match on both table
                    // and name - just check name here
                    .find(|f| f.column.name == c.name)
                    .cloned()
                    .map(|cs| ColumnSchema::from_base(cs, q.table.name.clone()))
                    .ok_or_else(|| internal_err(format!("Unknown column {}", c)))
            })
            .collect::<Result<Vec<_>, _>>()?;

        trace!(id = statement_id, "delete::registered");
        self.prepared_statement_cache
            .insert(statement_id, PreparedStatement::Delete(q));
        Ok(PrepareResult::Delete {
            statement_id,
            params,
        })
    }

    pub(crate) async fn execute_prepared_delete(
        &mut self,
        q_id: u32,
        params: &[DataType],
    ) -> ReadySetResult<QueryResult<'_>> {
        let prep: PreparedStatement = self
            .prepared_statement_cache
            .get(&q_id)
            .ok_or(PreparedStatementMissing { statement_id: q_id })?
            .clone();

        trace!("delegate");
        match prep {
            PreparedStatement::Delete(q) => {
                return self.do_delete(Cow::Owned(q), Some(params)).await
            }
            _ => internal!(),
        };
    }

    pub(crate) async fn handle_create_table(
        &mut self,
        q: &nom_sql::CreateTableStatement,
    ) -> ReadySetResult<QueryResult<'_>> {
        // TODO(malte): we should perhaps check our usual caches here, rather than just blindly
        // doing a migration on Noria ever time. On the other hand, CREATE TABLE is rare...
        info!(table = %q.table.name, "table::create");
        noria_await!(
            self.inner.get_mut().await?,
            self.inner
                .get_mut()
                .await?
                .noria
                .extend_recipe(&format!("{};", q))
        )?;
        trace!("table::created");
        Ok(QueryResult::Empty)
    }
}

fn generate_query_name(statement: &nom_sql::SelectStatement) -> String {
    format!("q_{:x}", utils::hash_select_query(statement))
}

impl NoriaConnector {
    /// This function handles CREATE CACHED QUERY statements. When explicit-migrations is enabled,
    /// this function is the only way to create a view in noria.
    pub async fn handle_create_cached_query(
        &mut self,
        name: Option<&str>,
        statement: &nom_sql::SelectStatement,
    ) -> ReadySetResult<()> {
        let name = name.map_or_else(|| Cow::Owned(generate_query_name(statement)), Cow::Borrowed);
        noria_await!(
            self.inner.get_mut().await?,
            self.inner
                .get_mut()
                .await?
                .noria
                .extend_recipe(&format!("QUERY {}: {}", name, statement))
        )?;

        // If the query is already in there with a different name, we don't need to make a new name
        // for it, as *lookups* only need one of the names for the query, and when we drop it we'll
        // be hitting noria anyway
        self.tl_cached
            .entry(statement.clone())
            .or_insert_with(|| name.clone().into_owned());
        tokio::task::block_in_place(|| {
            self.cached
                .write()
                .unwrap()
                .entry(statement.clone())
                .or_insert_with(|| name.clone().into_owned());
        });

        Ok(())
    }

    async fn get_view(
        &mut self,
        q: &nom_sql::SelectStatement,
        prepared: bool,
        create_if_not_exist: bool,
    ) -> ReadySetResult<String> {
        let qname = match self.tl_cached.get(q) {
            None => {
                // check global cache
                let qname_opt = {
                    let gc = tokio::task::block_in_place(|| self.cached.read().unwrap());
                    gc.get(q).cloned()
                };
                let qname = match qname_opt {
                    Some(qname) => qname,
                    None => {
                        let qname = generate_query_name(q);

                        // add the query to Noria
                        if create_if_not_exist {
                            if prepared {
                                info!(query = %q, name = %qname, "adding parameterized query");
                            } else {
                                info!(query = %q, name = %qname, "adding ad-hoc query");
                            }

                            if let Err(e) = noria_await!(
                                self.inner.get_mut().await?,
                                self.inner
                                    .get_mut()
                                    .await?
                                    .noria
                                    .extend_recipe(&format!("QUERY {}: {};", qname, q))
                            ) {
                                error!(error = %e, "add query failed");
                                return Err(e);
                            }
                        } else if let Err(e) = noria_await!(
                            self.inner.get_mut().await?,
                            self.inner.get_mut().await?.noria.view(&qname)
                        ) {
                            error!(error = %e, "getting view from noria failed");
                            return Err(e);
                        }

                        let mut gc = tokio::task::block_in_place(|| self.cached.write().unwrap());
                        gc.insert(q.clone(), qname.clone());
                        qname
                    }
                };

                self.tl_cached.insert(q.clone(), qname.clone());

                qname
            }
            Some(qname) => qname.to_owned(),
        };
        Ok(qname)
    }

    /// Make a request to Noria to drop the query with the given name, and remove it from all
    /// internal state.
    pub async fn drop_view(&mut self, name: &str) -> ReadySetResult<()> {
        noria_await!(
            self.inner.get_mut().await?,
            self.inner.get_mut().await?.noria.remove_query(name)
        )?;
        // dropping queries is rare and we don't have a huge number of queries usually, so fine to
        // just linear scan
        self.tl_cached.retain(|_, v| v != name);
        tokio::task::block_in_place(|| {
            self.cached.write().unwrap().retain(|_, v| v != name);
        });
        Ok(())
    }

    pub fn select_statement_from_name(&self, name: &str) -> Option<SelectStatement> {
        self.tl_cached
            .iter()
            .find(|(_, n)| &n[..] == name)
            .map(|(v, _)| v.clone())
            .or_else(|| {
                tokio::task::block_in_place(|| {
                    self.cached
                        .read()
                        .unwrap()
                        .iter()
                        .find(|(_, n)| &n[..] == name)
                        .map(|(v, _)| v.clone())
                })
            })
    }

    async fn do_insert(
        &mut self,
        q: &InsertStatement,
        data: Vec<Vec<DataType>>,
    ) -> ReadySetResult<QueryResult<'_>> {
        let table = &q.table.name;

        // create a mutator if we don't have one for this table already
        trace!(%table, "insert::access mutator");
        let putter = self.inner.get_mut().await?.get_noria_table(table).await?;
        trace!("insert::extract schema");
        let schema = putter
            .schema()
            .ok_or_else(|| internal_err(format!("no schema for table '{}'", table)))?;

        let columns_specified: Vec<_> = q
            .fields
            .as_ref()
            .unwrap()
            .iter()
            .cloned()
            .map(|mut c| {
                c.table = Some(q.table.name.clone());
                c
            })
            .collect();

        // handle auto increment
        trace!("insert::auto-increment");
        let auto_increment_columns: Vec<_> = schema
            .fields
            .iter()
            .filter(|c| c.constraints.contains(&ColumnConstraint::AutoIncrement))
            .collect();
        if auto_increment_columns.len() > 1 {
            // can only have zero or one AUTO_INCREMENT columns
            return Err(table_err(table, ReadySetError::MultipleAutoIncrement));
        }

        let ai = &mut self.auto_increments;
        tokio::task::block_in_place(|| {
            let ai_lock = ai.read().unwrap();
            if ai_lock.get(table).is_none() {
                drop(ai_lock);
                ai.write()
                    .unwrap()
                    .entry(table.to_owned())
                    .or_insert_with(|| atomic::AtomicUsize::new(0));
            }
        });
        let mut buf = vec![vec![DataType::None; schema.fields.len()]; data.len()];
        let mut first_inserted_id = None;
        tokio::task::block_in_place(|| -> ReadySetResult<_> {
            let ai_lock = ai.read().unwrap();
            let last_insert_id = &ai_lock[table];

            // handle default values
            trace!("insert::default values");
            let mut default_value_columns: Vec<_> = schema
                .fields
                .iter()
                .filter_map(|c| {
                    for cc in &c.constraints {
                        if let ColumnConstraint::DefaultValue(ref v) = *cc {
                            return Some((c.column.clone(), v.clone()));
                        }
                    }
                    None
                })
                .collect();

            trace!("insert::construct ops");

            for (ri, row) in data.iter().enumerate() {
                if let Some(col) = auto_increment_columns.get(0) {
                    let idx = schema
                        .fields
                        .iter()
                        .position(|f| f == *col)
                        .ok_or_else(|| {
                            table_err(table, ReadySetError::NoSuchColumn(col.column.name.clone()))
                        })?;
                    // query can specify an explicit AUTO_INCREMENT value
                    if !columns_specified.contains(&col.column) {
                        let id = last_insert_id.fetch_add(1, atomic::Ordering::SeqCst) as i64 + 1;
                        if first_inserted_id.is_none() {
                            first_inserted_id = Some(id);
                        }
                        buf[ri][idx] = DataType::from(id);
                    }
                }

                for (c, v) in default_value_columns.drain(..) {
                    let idx = schema
                        .fields
                        .iter()
                        .position(|f| f.column == c)
                        .ok_or_else(|| {
                            table_err(table, ReadySetError::NoSuchColumn(c.name.clone()))
                        })?;
                    // only use default value if query doesn't specify one
                    if !columns_specified.contains(&c) {
                        buf[ri][idx] = v.try_into()?;
                    }
                }

                for (ci, c) in columns_specified.iter().enumerate() {
                    let (idx, field) = schema
                        .fields
                        .iter()
                        .find_position(|f| f.column == *c)
                        .ok_or_else(|| {
                            table_err(
                                &schema.table.name,
                                ReadySetError::NoSuchColumn(c.name.clone()),
                            )
                        })?;
                    let value = row
                        .get(ci)
                        .ok_or_else(|| {
                            internal_err(
                                "Row returned from noria-server had the wrong number of columns",
                            )
                        })?
                        .coerce_to(&field.sql_type)?;
                    buf[ri][idx] = value.into_owned();
                }
            }
            Ok(())
        })?;

        let result = if let Some(ref update_fields) = q.on_duplicate {
            trace!("insert::complex");
            invariant_eq!(buf.len(), 1);

            let updates = {
                // fake out an update query
                let mut uq = UpdateStatement {
                    table: nom_sql::Table::from(table.as_str()),
                    fields: update_fields.clone(),
                    where_clause: None,
                };
                utils::extract_update_params_and_fields(
                    &mut uq,
                    &mut None::<std::iter::Empty<DataType>>,
                    schema,
                )?
            };

            // TODO(malte): why can't I consume buf here?
            let r = putter.insert_or_update(buf[0].clone(), updates).await;
            trace!("insert::complex::complete");
            r
        } else {
            trace!("insert::simple");
            let buf: Vec<_> = buf.into_iter().map(TableOperation::Insert).collect();
            let r = putter.perform_all(buf).await;
            trace!("insert::simple::complete");
            r
        };
        result?;
        Ok(QueryResult::Insert {
            num_rows_inserted: data.len() as u64,
            first_inserted_id: first_inserted_id.unwrap_or(0) as u64,
        })
    }

    async fn do_update(
        &mut self,
        q: Cow<'_, UpdateStatement>,
        params: Option<&[DataType]>,
    ) -> ReadySetResult<QueryResult<'_>> {
        trace!(table = %q.table.name, "update::access mutator");
        let mutator = self
            .inner
            .get_mut()
            .await?
            .get_noria_table(&q.table.name)
            .await?;

        let q = q.into_owned();
        let (key, updates) = {
            trace!("update::extract schema");
            let schema = if let Some(cts) = mutator.schema() {
                cts
            } else {
                // no update on views
                unsupported!();
            };
            let coerced_params =
                utils::coerce_params(params, &SqlQuery::Update(q.clone()), schema)?;
            utils::extract_update(q, coerced_params.map(|p| p.into_iter()), schema)?
        };

        trace!("update::update");
        mutator.update(key, updates).await?;
        trace!("update::complete");
        // TODO: return meaningful fields for (num_rows_updated, last_inserted_id) rather than hardcoded (1,0)
        Ok(QueryResult::Update {
            num_rows_updated: 1,
            last_inserted_id: 0,
        })
    }

    async fn do_delete(
        &mut self,
        q: Cow<'_, DeleteStatement>,
        params: Option<&[DataType]>,
    ) -> ReadySetResult<QueryResult<'_>> {
        trace!(table = %q.table.name, "delete::access mutator");
        let mutator = self
            .inner
            .get_mut()
            .await?
            .get_noria_table(&q.table.name)
            .await?;

        let q = q.into_owned();
        let key = {
            trace!("delete::extract schema");
            let schema = if let Some(cts) = mutator.schema() {
                cts
            } else {
                // no delete on views
                unsupported!();
            };
            let coerced_params =
                utils::coerce_params(params, &SqlQuery::Delete(q.clone()), schema)?;
            utils::extract_delete(q, coerced_params.map(|p| p.into_iter()), schema)?
        };

        trace!("delete::delete");
        mutator.delete(key).await?;
        trace!("delete::complete");
        // TODO: return meaningful fields for (num_rows_deleted, last_inserted_id) rather than hardcoded (1,0)
        Ok(QueryResult::Delete {
            num_rows_deleted: 1,
        })
    }

    pub(crate) async fn handle_select(
        &mut self,
        // TODO(mc):  Take a reference here; requires getting rewrite::process_query() to Cow
        mut query: nom_sql::SelectStatement,
        ticket: Option<Timestamp>,
        create_if_not_exist: bool,
    ) -> ReadySetResult<QueryResult<'_>> {
        let processed = rewrite::process_query(&mut query)?;

        trace!("query::select::access view");
        let qname = self.get_view(&query, false, create_if_not_exist).await?;

        // we need the schema for the result writer
        trace!(%qname, "query::select::extract schema");

        let view_failed = self.failed_views.take(&qname).is_some();
        let getter = self
            .inner
            .get_mut()
            .await?
            .get_noria_view(&qname, self.region.as_deref(), view_failed)
            .await?;

        // Currently there is exactly one key map entry per user parameter (ignoring limit/offset).
        // TODO: update to ignore elements based on missing entries
        let key_column_indices = getter
            .key_map()
            .iter()
            .map(|(_, key_index)| *key_index)
            .collect::<Vec<_>>();

        let keys = processed.make_keys(&[])?;

        trace!(%qname, "query::select::do");
        let res = do_read(getter, &query, keys, &key_column_indices, ticket).await;
        if res.is_err() {
            self.failed_views.insert(qname.to_owned());
        }

        res
    }

    pub(crate) async fn prepare_select(
        &mut self,
        mut statement: nom_sql::SelectStatement,
        statement_id: u32,
        create_if_not_exist: bool,
    ) -> ReadySetResult<PrepareResult> {
        // extract parameter columns *for the client*
        // note that we have to do this *before* processing the query, otherwise the
        // client will be confused about the number of parameters it's supposed to
        // give.
        let client_param_columns: Vec<_> = utils::select_statement_parameter_columns(&statement)
            .into_iter()
            .cloned()
            .collect();

        trace!("select::collapse where-in clauses");
        let processed_query_params = rewrite::process_query(&mut statement)?;

        let limit_columns: Vec<_> = utils::get_limit_parameters(&statement)
            .into_iter()
            .map(|c| ColumnSchema {
                spec: nom_sql::ColumnSpecification {
                    column: c,
                    sql_type: nom_sql::SqlType::UnsignedBigint(None),
                    constraints: vec![],
                    comment: None,
                },
                base: None,
            })
            .collect();

        // check if we already have this query prepared
        trace!("select::access view");
        let qname = self.get_view(&statement, true, create_if_not_exist).await?;

        // extract result schema
        trace!(qname = %qname, "select::extract schema");
        let view_failed = self.failed_views.take(&qname).is_some();
        let getter = self
            .inner
            .get_mut()
            .await?
            .get_noria_view(&qname, self.region.as_deref(), view_failed)
            .await?;

        let getter_schema = getter
            .schema()
            .ok_or_else(|| internal_err(format!("no schema for view '{}'", qname)))?;

        let mut params: Vec<_> = getter_schema
            .to_cols(&client_param_columns, SchemaType::ProjectedSchema)?
            .into_iter()
            .map(|cs| {
                let mut cs = cs.clone();
                cs.spec.column.table = Some(qname.clone());
                cs
            })
            .collect();

        // Currently there is exactly one key map entry per user parameter (ignoring limit/offset).
        // TODO: update to ignore elements based on missing entries
        let key_column_indices = getter
            .key_map()
            .iter()
            .map(|(_, key_index)| *key_index)
            .collect::<Vec<_>>();

        trace!(id = statement_id, "select::registered");
        let ps = PreparedSelectStatement {
            name: qname,
            statement: Box::new(statement),
            key_column_indices,
            processed_query_params,
            ignored_columns: limit_columns.clone(),
        };
        self.prepared_statement_cache
            .insert(statement_id, PreparedStatement::Select(ps));

        params.extend(limit_columns);
        Ok(PrepareResult::Select {
            statement_id,
            params,
            schema: getter_schema.schema(SchemaType::ReturnedSchema).to_vec(),
        })
    }

    pub(crate) async fn execute_prepared_select(
        &mut self,
        q_id: u32,
        params: &[DataType],
        ticket: Option<Timestamp>,
    ) -> ReadySetResult<QueryResult<'_>> {
        let NoriaConnector {
            prepared_statement_cache,
            region,
            failed_views,
            ..
        } = self;

        let PreparedSelectStatement {
            name,
            statement,
            key_column_indices,
            processed_query_params,
            ignored_columns,
        } = {
            match prepared_statement_cache.get(&q_id) {
                Some(PreparedStatement::Select(ps)) => ps,
                Some(_) => internal!(),
                None => return Err(PreparedStatementMissing { statement_id: q_id }),
            }
        };

        trace!("apply where-in rewrites");
        // ignore LIMIT and OFFSET params (and return empty resultset according to value)
        let (offset, limit, params) = pop_limit_offset_params(params, ignored_columns);

        let res = {
            let view_failed = failed_views.take(name).is_some();
            let getter = self
                .inner
                .get_mut()
                .await?
                .get_noria_view(name, region.as_deref(), view_failed)
                .await?;

            // TODO(DAN): These should have been passed as UnsignedBigInt
            if (offset.is_some() && !matches!(offset, Some(DataType::BigInt(0))))
                || matches!(limit, Some(DataType::BigInt(0)))
            {
                short_circuit_empty_resultset(getter).await
            } else {
                do_read(
                    getter,
                    statement,
                    processed_query_params.make_keys(params)?,
                    key_column_indices,
                    ticket,
                )
                .await
            }
        };

        if res.is_err() {
            failed_views.insert(name.to_owned());
        }

        res
    }

    pub(crate) async fn handle_create_view(
        &mut self,
        q: &nom_sql::CreateViewStatement,
    ) -> ReadySetResult<QueryResult<'_>> {
        // TODO(malte): we should perhaps check our usual caches here, rather than just blindly
        // doing a migration on Noria every time. On the other hand, CREATE VIEW is rare...

        info!(%q.definition, %q.name, "view::create");

        noria_await!(
            self.inner.get_mut().await?,
            self.inner
                .get_mut()
                .await?
                .noria
                .extend_recipe(&format!("VIEW {}: {};", q.name, q.definition))
        )?;
        Ok(QueryResult::Empty)
    }
}

/// Run the supplied [`SelectStatement`] on the supplied [`View`]
/// Assumption: the [`View`] was created for that specific [`SelectStatement`]
#[allow(clippy::needless_lifetimes)] // clippy erroneously thinks the timelife can be elided
async fn do_read<'a>(
    getter: &'a mut View,
    q: &nom_sql::SelectStatement,
    mut keys: Vec<Cow<'_, [DataType]>>,
    key_column_indices: &[usize],
    ticket: Option<Timestamp>,
) -> ReadySetResult<QueryResult<'a>> {
    trace!("select::access view");
    let getter_schema = getter
        .schema()
        .ok_or_else(|| internal_err("No schema for view"))?;
    let projected_schema = getter_schema.schema(SchemaType::ProjectedSchema);
    let mut key_types = getter_schema.col_types(key_column_indices, SchemaType::ProjectedSchema)?;
    trace!("select::lookup");
    let bogo = vec![vec1![DataType::from(0i32)].into()];
    let mut binops = utils::get_select_statement_binops(q);
    let mut filter_op_idx = None;
    let filter = binops
        .iter()
        .enumerate()
        .find_map(|(i, (col, binop))| {
            ViewQueryOperator::try_from(*binop)
                .ok()
                .map(|op| (i, col, op))
        })
        .map(|(idx, col, operator)| -> ReadySetResult<_> {
            let key = keys.drain(0..1).next().ok_or(ReadySetError::EmptyKey)?;
            if !keys.is_empty() {
                unsupported!(
                    "LIKE/ILIKE not currently supported for more than one lookup key at a time"
                );
            }
            let column = projected_schema
                .iter()
                .position(|x| x.spec.column.name == col.name)
                .ok_or_else(|| ReadySetError::NoSuchColumn(col.name.clone()))?;
            let value = String::try_from(
                key[idx]
                    .coerce_to(key_types.remove(idx))
                    .unwrap()
                    .into_owned(),
            )?;
            if !key.is_empty() {
                // the LIKE/ILIKE isn't our only key, add the rest back to `keys`
                keys.push(key);
            }

            filter_op_idx = Some(idx);

            Ok(ViewQueryFilter {
                column,
                operator,
                value,
            })
        })
        .transpose()?;

    if let Some(filter_op_idx) = filter_op_idx {
        // if we're using a column for a post-lookup filter, remove it from our list of binops
        // so we can use the remaining list for our keys
        binops.remove(filter_op_idx);
    }

    let use_bogo = keys.is_empty();
    let keys = if use_bogo {
        bogo
    } else {
        let mut binops = binops.into_iter().map(|(_, b)| b).unique();
        let binop_to_use = binops.next().unwrap_or(BinaryOperator::Equal);
        if let Some(other) = binops.next() {
            unsupported!(
                "attempted to execute statement with conflicting binary operators {:?} and {:?}",
                binop_to_use,
                other
            );
        }

        keys.drain(..)
            .map(|key| {
                let k = key
                    .iter()
                    .zip(&key_types)
                    .map(|(val, col_type)| val.coerce_to(col_type).map(Cow::into_owned))
                    .collect::<ReadySetResult<Vec<DataType>>>()?;

                (k, binop_to_use)
                    .try_into()
                    .map_err(|_| ReadySetError::EmptyKey)
            })
            .collect::<ReadySetResult<Vec<_>>>()?
    };

    let vq = ViewQuery {
        key_comparisons: keys,
        block: true,
        filter,
        // TODO(andrew): Add a timestamp to views when RYW consistency
        // is specified.
        timestamp: ticket,
    };

    let data = getter.raw_lookup(vq).await?;
    trace!("select::complete");

    Ok(QueryResult::Select {
        data,
        select_schema: SelectSchema {
            use_bogo,
            schema: Cow::Borrowed(getter.schema().unwrap().schema(SchemaType::ReturnedSchema)), // Safe because we already unwrapped above
            columns: Cow::Borrowed(getter.columns()),
        },
    })
}
