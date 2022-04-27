use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::convert::{TryFrom, TryInto};
use std::fmt;
use std::ops::Bound;
use std::sync::{atomic, Arc, RwLock};

use dataflow_expression::Expression as DataflowExpression;
use itertools::Itertools;
use launchpad::redacted::Sensitive;
use nom_sql::{
    self, BinaryOperator, ColumnConstraint, DeleteStatement, InsertStatement, Literal,
    SelectStatement, SqlIdentifier, SqlQuery, UpdateStatement,
};
use noria::consistency::Timestamp;
use noria::internal::LocalNodeIndex;
use noria::recipe::changelist::{Change, ChangeList};
use noria::results::Results;
use noria::{
    ColumnSchema, ControllerHandle, KeyColumnIdx, KeyComparison, ReadQuery, ReadySetError,
    ReadySetResult, SchemaType, Table, TableOperation, View, ViewPlaceholder, ViewQuery,
    ViewSchema,
};
use noria_data::DataType;
use noria_errors::ReadySetError::PreparedStatementMissing;
use noria_errors::{internal, internal_err, invariant_eq, table_err, unsupported, unsupported_err};
use noria_server::worker::readers::ReadRequestHandler;
use readyset_tracing::instrument_child;
use tracing::{error, info, instrument, trace};
use vec1::vec1;

use crate::backend::SelectSchema;
use crate::rewrite::{self, ProcessedQueryParams};
use crate::utils;

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
    processed_query_params: ProcessedQueryParams,
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

#[derive(Debug, Clone)]
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

impl PrepareResult {
    /// Get the noria statement id for this statement
    pub fn statement_id(&self) -> u32 {
        match self {
            PrepareResult::Select { statement_id, .. }
            | PrepareResult::Insert { statement_id, .. }
            | PrepareResult::Delete { statement_id, .. }
            | PrepareResult::Update { statement_id, .. } => *statement_id,
        }
    }
}

/// A single row in the variable table associated with [`QueryResult::MetaVariables`].
#[derive(Debug)]
pub struct MetaVariable {
    /// The variable name.
    pub name: SqlIdentifier,
    /// The value associated with the variable.
    pub value: String,
}

impl<N: Into<SqlIdentifier>, V: Into<String>> From<(N, V)> for MetaVariable {
    fn from((name, value): (N, V)) -> Self {
        MetaVariable {
            name: name.into(),
            value: value.into(),
        }
    }
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
    /// A metadata table returned as a response to eg an EXPLAIN query. Unlike
    /// [`QueryResult::MetaVariables`] it will format the output as a table with a single row,
    /// where the columns names correspond to the [`MetaVariable`] names.
    Meta(Vec<MetaVariable>),
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
            QueryResult::Meta(meta) => QueryResult::Meta(meta),
            QueryResult::MetaVariables(vec) => QueryResult::MetaVariables(vec),
        }
    }
}

#[derive(Clone)]
pub struct ViewCache {
    /// Global cache of view endpoints and prepared statements.
    global: Arc<RwLock<HashMap<SelectStatement, String>>>,
    /// Thread-local version of global cache (consulted first).
    local: HashMap<SelectStatement, String>,
}

impl ViewCache {
    /// Construct a new ViewCache with a passed in global view cache.
    pub fn new(global_cache: Arc<RwLock<HashMap<SelectStatement, String>>>) -> ViewCache {
        ViewCache {
            global: global_cache,
            local: HashMap::new(),
        }
    }

    /// Registers a statement with the provided name into both the local and global view caches.
    pub fn register_statement(&mut self, name: &str, statement: &nom_sql::SelectStatement) {
        self.local
            .entry(statement.clone())
            .or_insert_with(|| name.to_string());
        tokio::task::block_in_place(|| {
            self.global
                .write()
                .unwrap()
                .entry(statement.clone())
                .or_insert_with(|| name.to_string());
        });
    }

    /// Retrieves the name for the provided statement if it's in the cache. We first check local
    /// cache, and if it's not there we check global cache. If it's in global but not local, we
    /// backfill local cache before returning the result.
    pub fn statement_name(&mut self, statement: &nom_sql::SelectStatement) -> Option<String> {
        let maybe_name = if let Some(name) = self.local.get(statement) {
            return Some(name.to_string());
        } else {
            // Didn't find it in local, so let's check global.
            let gc = tokio::task::block_in_place(|| self.global.read().unwrap());
            gc.get(statement).cloned()
        };

        maybe_name.map(|n| {
            // Backfill into local before we return.
            self.local.insert(statement.clone(), n.clone());
            n
        })
    }

    /// Removes the statement with the given name from both the global and local caches.
    pub fn remove_statement(&mut self, name: &str) {
        self.local.retain(|_, v| v != name);
        tokio::task::block_in_place(|| {
            self.global.write().unwrap().retain(|_, v| v != name);
        });
    }

    /// Returns the select statement based on a provided name if it exists in either the local or
    /// global caches.
    pub fn select_statement_from_name(&self, name: &str) -> Option<SelectStatement> {
        self.local
            .iter()
            .find(|(_, n)| &n[..] == name)
            .map(|(v, _)| v.clone())
            .or_else(|| {
                tokio::task::block_in_place(|| {
                    self.global
                        .read()
                        .unwrap()
                        .iter()
                        .find(|(_, n)| &n[..] == name)
                        .map(|(v, _)| v.clone())
                })
            })
    }
}

/// If the query has parametrized OFFSET or LIMIT, get the values for those params from the given
/// slice of parameters, returning a tuple of `limit, offset`
fn limit_offset_params<'param>(
    params: &'param [DataType],
    query: &SelectStatement,
) -> (Option<&'param DataType>, Option<&'param DataType>) {
    let limit = if let Some(limit) = &query.limit {
        limit
    } else {
        return (None, None);
    };

    let offset = if matches!(limit.offset, Some(Literal::Placeholder(_))) {
        params.last()
    } else {
        None
    };

    let limit = if matches!(limit.limit, Literal::Placeholder(_)) {
        if offset.is_some() {
            params.get(params.len() - 2)
        } else {
            params.last()
        }
    } else {
        None
    };

    (limit, offset)
}

pub struct NoriaConnector {
    inner: NoriaBackend,
    auto_increments: Arc<RwLock<HashMap<String, atomic::AtomicUsize>>>,
    /// Global and thread-local cache of view endpoints and prepared statements.
    view_cache: ViewCache,

    prepared_statement_cache: HashMap<StatementID, PreparedStatement>,
    /// The region to pass to noria for replica selection.
    region: Option<String>,

    /// Set of views that have failed on previous requests. Separate from the backend
    /// to allow returning references to schemas from views all the way to mysql-srv,
    /// but on subsequent requests, do not use a failed view.
    failed_views: HashSet<String>,

    /// How to handle issuing reads against Noria. See [`ReadBehavior`].
    read_behavior: ReadBehavior,

    /// A read request handler that may be used to service reads from readers
    /// on the same server.
    read_request_handler: Option<ReadRequestHandler>,
}

/// The read behavior used when executing a read against Noria.
#[derive(Clone, Copy)]
pub enum ReadBehavior {
    /// If Noria is unable to immediately service the read due to a cache miss, block on the
    /// response.
    Blocking,
    /// If Noria is unable to immediately service the read, return an error.
    NonBlocking,
}

impl ReadBehavior {
    fn is_blocking(&self) -> bool {
        matches!(self, Self::Blocking)
    }
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
            view_cache: self.view_cache.clone(),
            prepared_statement_cache: self.prepared_statement_cache.clone(),
            region: self.region.clone(),
            failed_views: self.failed_views.clone(),
            read_behavior: self.read_behavior,
            read_request_handler: self.read_request_handler.clone(),
        }
    }
}

impl NoriaConnector {
    pub async fn new(
        ch: ControllerHandle,
        auto_increments: Arc<RwLock<HashMap<String, atomic::AtomicUsize>>>,
        query_cache: Arc<RwLock<HashMap<SelectStatement, String>>>,
        region: Option<String>,
        read_behavior: ReadBehavior,
    ) -> Self {
        NoriaConnector::new_with_local_reads(
            ch,
            auto_increments,
            query_cache,
            region,
            read_behavior,
            None,
        )
        .await
    }

    pub async fn new_with_local_reads(
        ch: ControllerHandle,
        auto_increments: Arc<RwLock<HashMap<String, atomic::AtomicUsize>>>,
        query_cache: Arc<RwLock<HashMap<SelectStatement, String>>>,
        region: Option<String>,
        read_behavior: ReadBehavior,
        read_request_handler: Option<ReadRequestHandler>,
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
            view_cache: ViewCache::new(query_cache),
            prepared_statement_cache: HashMap::new(),
            region,
            failed_views: HashSet::new(),
            read_behavior,
            read_request_handler,
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

        Ok(QueryResult::Meta(vec![(label, graphviz).into()]))
    }

    pub(crate) async fn verbose_outputs(&mut self) -> ReadySetResult<QueryResult<'static>> {
        let noria = &mut self.inner.get_mut().await?.noria;
        let outputs = noria.verbose_outputs().await?;
        //TODO(DAN): this is ridiculous, update Meta instead
        let select_schema = SelectSchema {
            use_bogo: false,
            schema: Cow::Owned(vec![
                ColumnSchema {
                    spec: nom_sql::ColumnSpecification {
                        column: nom_sql::Column {
                            name: "name".into(),
                            table: None,
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
                            name: "query".into(),
                            table: None,
                        },
                        sql_type: nom_sql::SqlType::Text,
                        constraints: vec![],
                        comment: None,
                    },
                    base: None,
                },
            ]),

            columns: Cow::Owned(vec!["name".into(), "query".into()]),
        };
        let data = outputs
            .into_iter()
            .map(|(n, q)| vec![DataType::from(n), DataType::from(q.to_string())])
            .collect::<Vec<_>>();
        let data = vec![Results::new(
            data,
            Arc::new(["name".into(), "query".into()]),
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
                // unwrap: safe because we always pass in Some(params) so don't hit None path of
                // coerce_params
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

    /// Calls the `extend_recipe` endpoint on Noria with the given
    /// query.
    // TODO(fran): Instead of serialize using `Display`, we should implement `Serialize`
    //   and `Deserialize` for each table operation struct, and send that instead; otherwise
    //   we are always recalculating the same stuff.
    //   Additionally (optional, open to discussion), we should use custom structs that have
    //   already purged the data into a structure more useful to use, instead of using nom-sql
    //   structs directly as domain objects.
    pub(crate) async fn handle_table_operation<C>(
        &mut self,
        changelist: C,
    ) -> ReadySetResult<QueryResult<'_>>
    where
        C: Into<ChangeList>,
    {
        // TODO(malte): we should perhaps check our usual caches here, rather than just blindly
        // doing a migration on Noria ever time. On the other hand, CREATE TABLE is rare...
        noria_await!(
            self.inner.get_mut().await?,
            self.inner
                .get_mut()
                .await?
                .noria
                .extend_recipe(changelist.into())
        )?;
        Ok(QueryResult::Empty)
    }

    pub(crate) async fn readyset_status(&mut self) -> ReadySetResult<QueryResult<'static>> {
        let status = noria_await!(
            self.inner.get_mut().await?,
            self.inner.get_mut().await?.noria.status()
        )?;

        // Converts from ReadySetStatus -> Vec<(String, String)> -> QueryResult
        Ok(QueryResult::MetaVariables(
            <Vec<(String, String)>>::from(status)
                .into_iter()
                .map(MetaVariable::from)
                .collect(),
        ))
    }
}

impl NoriaConnector {
    /// This function handles CREATE CACHE statements. When explicit-migrations is enabled,
    /// this function is the only way to create a view in noria.
    pub async fn handle_create_cached_query(
        &mut self,
        name: Option<&str>,
        statement: &nom_sql::SelectStatement,
    ) -> ReadySetResult<()> {
        let name: SqlIdentifier = name
            .map(|s| s.into())
            .unwrap_or_else(|| utils::generate_query_name(statement).into());
        let changelist = ChangeList {
            changes: vec![Change::create_cache(name.clone(), statement.clone())],
        };

        noria_await!(
            self.inner.get_mut().await?,
            self.inner.get_mut().await?.noria.extend_recipe(changelist)
        )?;

        // If the query is already in there with a different name, we don't need to make a new name
        // for it, as *lookups* only need one of the names for the query, and when we drop it we'll
        // be hitting noria anyway
        self.view_cache.register_statement(name.as_ref(), statement);

        Ok(())
    }

    async fn get_view(
        &mut self,
        q: &nom_sql::SelectStatement,
        prepared: bool,
        create_if_not_exist: bool,
    ) -> ReadySetResult<String> {
        match self.view_cache.statement_name(q) {
            None => {
                let qname = utils::generate_query_name(q);

                // add the query to Noria
                if create_if_not_exist {
                    if prepared {
                        info!(query = %Sensitive(q), name = %qname, "adding parameterized query");
                    } else {
                        info!(query = %Sensitive(q), name = %qname, "adding ad-hoc query");
                    }

                    let changelist = ChangeList {
                        changes: vec![Change::create_cache(qname.clone(), q.clone())],
                    };

                    if let Err(e) = noria_await!(
                        self.inner.get_mut().await?,
                        self.inner.get_mut().await?.noria.extend_recipe(changelist)
                    ) {
                        error!(error = %e, "add query failed");
                        return Err(e);
                    }
                } else if let Err(e) = noria_await!(
                    self.inner.get_mut().await?,
                    self.inner.get_mut().await?.noria.view(qname.as_str())
                ) {
                    error!(error = %e, "getting view from noria failed");
                    return Err(e);
                }
                self.view_cache.register_statement(&qname, q);

                Ok(qname)
            }
            Some(name) => Ok(name),
        }
    }

    /// Make a request to Noria to drop the query with the given name, and remove it from all
    /// internal state.
    pub async fn drop_view(&mut self, name: &str) -> ReadySetResult<()> {
        noria_await!(
            self.inner.get_mut().await?,
            self.inner.get_mut().await?.noria.remove_query(name)
        )?;
        self.view_cache.remove_statement(name);
        Ok(())
    }

    pub fn select_statement_from_name(&self, name: &str) -> Option<SelectStatement> {
        self.view_cache.select_statement_from_name(name)
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
            return Err(table_err(
                table.to_string(),
                ReadySetError::MultipleAutoIncrement,
            ));
        }

        let ai = &mut self.auto_increments;
        tokio::task::block_in_place(|| {
            let ai_lock = ai.read().unwrap();
            if ai_lock.get(table.as_str()).is_none() {
                drop(ai_lock);
                ai.write()
                    .unwrap()
                    .entry(table.to_string())
                    .or_insert_with(|| atomic::AtomicUsize::new(0));
            }
        });
        let mut buf = vec![vec![DataType::None; schema.fields.len()]; data.len()];
        let mut first_inserted_id = None;
        tokio::task::block_in_place(|| -> ReadySetResult<_> {
            let ai_lock = ai.read().unwrap();
            let last_insert_id = &ai_lock[table.as_str()];

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
                            table_err(
                                table,
                                ReadySetError::NoSuchColumn(col.column.name.to_string()),
                            )
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
                            table_err(table, ReadySetError::NoSuchColumn(c.name.to_string()))
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
                                ReadySetError::NoSuchColumn(c.name.to_string()),
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
                    buf[ri][idx] = value;
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
        // TODO: return meaningful fields for (num_rows_updated, last_inserted_id) rather than
        // hardcoded (1,0)
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
        // TODO: return meaningful fields for (num_rows_deleted, last_inserted_id) rather than
        // hardcoded (1,0)
        Ok(QueryResult::Delete {
            num_rows_deleted: 1,
        })
    }

    #[instrument_child(level = "info", fields(create_if_not_exist))]
    pub(crate) async fn handle_select(
        &mut self,
        // TODO(mc):  Take a reference here; requires getting rewrite::process_query() to Cow
        mut query: nom_sql::SelectStatement,
        ticket: Option<Timestamp>,
        create_if_not_exist: bool,
        event: &mut noria_client_metrics::QueryExecutionEvent,
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

        let keys = processed.make_keys(&[])?;

        trace!(%qname, "query::select::do");
        let res = do_read(
            getter,
            &query,
            keys,
            ticket,
            self.read_behavior,
            self.read_request_handler.as_mut(),
            event,
        )
        .await;
        if let Err(e) = res.as_ref() {
            if e.is_networking_related() {
                self.failed_views.insert(qname.to_owned());
            }
        }

        res
    }

    #[instrument(level = "info", skip(self, statement))]
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

        trace!("select::collapse where-in clauses");
        let processed_query_params = rewrite::process_query(&mut statement)?;

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
                cs.spec.column.table = Some(qname.as_str().into());
                cs
            })
            .collect();

        trace!(id = statement_id, "select::registered");
        let ps = PreparedSelectStatement {
            name: qname.to_string(),
            statement: Box::new(statement),
            processed_query_params,
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

    #[instrument(level = "debug", skip(self, params, event))]
    pub(crate) async fn execute_prepared_select(
        &mut self,
        q_id: u32,
        params: &[DataType],
        ticket: Option<Timestamp>,
        event: &mut noria_client_metrics::QueryExecutionEvent,
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
            processed_query_params,
        } = {
            match prepared_statement_cache.get(&q_id) {
                Some(PreparedStatement::Select(ps)) => ps,
                Some(_) => internal!(),
                None => return Err(PreparedStatementMissing { statement_id: q_id }),
            }
        };

        let (limit, offset) = limit_offset_params(params, statement);

        let res = {
            let view_failed = failed_views.take(name).is_some();
            let getter = self
                .inner
                .get_mut()
                .await?
                .get_noria_view(name, region.as_deref(), view_failed)
                .await?;

            if (matches!(limit, Some(DataType::Int(1)))
                && offset.is_some()
                && !matches!(offset, Some(DataType::Int(0))))
                || matches!(limit, Some(DataType::Int(0)))
            {
                short_circuit_empty_resultset(getter).await
            } else {
                do_read(
                    getter,
                    statement,
                    processed_query_params.make_keys(params)?,
                    ticket,
                    self.read_behavior,
                    self.read_request_handler.as_mut(),
                    event,
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

        info!(view = %Sensitive(&q.definition), name = %q.name, "view::create");

        let changelist = ChangeList {
            changes: vec![Change::CreateView(q.clone())],
        };

        noria_await!(
            self.inner.get_mut().await?,
            self.inner.get_mut().await?.noria.extend_recipe(changelist)
        )?;
        Ok(QueryResult::Empty)
    }
}

/// Build a [`ViewQuery`] for performing a lookup of the given `q` with the given `raw_keys`,
/// provided `getter_schema` and `key_map` from the [`View`] itself.
fn build_view_query(
    getter_schema: &ViewSchema,
    key_map: &[(ViewPlaceholder, KeyColumnIdx)],
    q: &nom_sql::SelectStatement,
    mut raw_keys: Vec<Cow<'_, [DataType]>>,
    ticket: Option<Timestamp>,
    read_behavior: ReadBehavior,
) -> ReadySetResult<ViewQuery> {
    let projected_schema = getter_schema.schema(SchemaType::ProjectedSchema);

    let mut key_types = getter_schema.col_types(
        key_map.iter().map(|(_, key_column_idx)| *key_column_idx),
        SchemaType::ProjectedSchema,
    )?;

    trace!("select::lookup");
    let bogo = vec![vec1![DataType::from(0i32)].into()];
    let mut binops = utils::get_select_statement_binops(q);
    let mut filter_op_idx = None;
    let mut filters = binops
        .iter()
        .enumerate()
        .filter(|(_, (_, binop))| matches!(binop, BinaryOperator::Like | BinaryOperator::ILike))
        .map(|(idx, (col, op))| -> ReadySetResult<_> {
            let key = raw_keys.drain(0..1).next().ok_or(ReadySetError::EmptyKey)?;
            if !raw_keys.is_empty() {
                unsupported!(
                    "LIKE/ILIKE not currently supported for more than one lookup key at a time"
                );
            }
            let column = projected_schema
                .iter()
                .position(|x| x.spec.column.name == col.name)
                .ok_or_else(|| ReadySetError::NoSuchColumn(col.name.to_string()))?;
            let value = key[idx].coerce_to(key_types.remove(idx))?;
            if !key.is_empty() {
                // the LIKE/ILIKE isn't our only key, add the rest back to `keys`
                raw_keys.push(key);
            }

            filter_op_idx = Some(idx);

            Ok(DataflowExpression::Op {
                left: Box::new(DataflowExpression::Column(column)),
                op: *op,
                right: Box::new(DataflowExpression::Literal(value)),
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    if let Some(filter_op_idx) = filter_op_idx {
        // if we're using a column for a post-lookup filter, remove it from our list of binops
        // so we can use the remaining list for our keys
        binops.remove(filter_op_idx);
    }

    let keys = if raw_keys.is_empty() {
        bogo
    } else {
        let mut unique_binops = binops.iter().map(|(_, b)| *b).unique();
        let binop_to_use = unique_binops.next().unwrap_or(BinaryOperator::Equal);
        let mixed_binops = unique_binops.next().is_some();

        let key_types = key_map
            .iter()
            .zip(key_types)
            .map(|((_, key_column_idx), key_type)| (*key_column_idx, key_type))
            .collect::<HashMap<_, _>>();

        raw_keys
            .into_iter()
            .map(|key| {
                let mut k = vec![];
                let mut bounds: Option<(Vec<DataType>, Vec<DataType>)> = if mixed_binops {
                    Some((vec![], vec![]))
                } else {
                    None
                };
                for (view_placeholder, key_column_idx) in key_map {
                    match view_placeholder {
                        ViewPlaceholder::Generated => continue,
                        ViewPlaceholder::OneToOne(idx) => {
                            let key_type = if let Some(key_type) = key_types.get(key_column_idx) {
                                key_type
                            } else {
                                // if the key isn't in the list of key types, that means it was
                                // removed during post-read filter construction (for LIKE/ILIKE)
                                // above, so just skip it in the lookup key.
                                continue;
                            };
                            // parameter numbering is 1-based, but vecs are 0-based, so subtract 1
                            let value = key[*idx - 1].coerce_to(key_type)?;

                            let make_op = |op| DataflowExpression::Op {
                                left: Box::new(DataflowExpression::Column(*key_column_idx)),
                                op,
                                right: Box::new(DataflowExpression::Literal(value.clone())),
                            };

                            if let Some((lower_bound, upper_bound)) = &mut bounds {
                                let binop = binops[*idx - 1].1;
                                match binop {
                                    BinaryOperator::Like
                                    | BinaryOperator::NotLike
                                    | BinaryOperator::ILike
                                    | BinaryOperator::NotILike => {
                                        internal!("Already should have matched on LIKE above")
                                    }

                                    BinaryOperator::Equal => {
                                        lower_bound.push(value.clone());
                                        upper_bound.push(value);
                                    }
                                    BinaryOperator::GreaterOrEqual => {
                                        filters.push(make_op(BinaryOperator::GreaterOrEqual));
                                        lower_bound.push(value);
                                        upper_bound.push(DataType::Max);
                                    }
                                    BinaryOperator::LessOrEqual => {
                                        filters.push(make_op(BinaryOperator::LessOrEqual));
                                        lower_bound.push(DataType::None); // NULL is the minimum DataType
                                        upper_bound.push(value);
                                    }
                                    BinaryOperator::Greater => {
                                        filters.push(make_op(BinaryOperator::Greater));
                                        lower_bound.push(value);
                                        upper_bound.push(DataType::Max);
                                    }
                                    BinaryOperator::Less => {
                                        filters.push(make_op(BinaryOperator::Less));
                                        lower_bound.push(DataType::None); // NULL is the minimum DataType
                                        upper_bound.push(value);
                                    }
                                    op => unsupported!(
                                        "Unsupported binary operator in query: `{}`",
                                        op
                                    ),
                                }
                            } else {
                                if !k.is_empty() && binop_to_use != BinaryOperator::Equal {
                                    filters.push(make_op(binop_to_use));
                                }
                                k.push(value);
                            }
                        }
                        ViewPlaceholder::Between(lower_idx, upper_idx) => {
                            let key_type = key_types[key_column_idx];
                            // parameter numbering is 1-based, but vecs are 0-based, so subtract 1
                            let lower_value = key[*lower_idx - 1].coerce_to(key_type)?;
                            let upper_value = key[*upper_idx - 1].coerce_to(key_type)?;
                            let (lower_key, upper_key) =
                                bounds.get_or_insert_with(Default::default);
                            lower_key.push(lower_value);
                            upper_key.push(upper_value);
                        }
                        ViewPlaceholder::PageNumber {
                            offset_placeholder,
                            limit,
                        } => {
                            // parameter numbering is 1-based, but vecs are 0-based, so subtract 1
                            // offset parameters should always be a Bigint
                            let offset: u64 = key[*offset_placeholder - 1]
                                .coerce_to(&nom_sql::SqlType::Bigint(None))?
                                .try_into()?;
                            if offset % *limit != 0 {
                                unsupported!(
                                    "OFFSET must currently be an integer multiple of LIMIT"
                                );
                            }
                            let page_number = offset / *limit;
                            k.push(page_number.into());
                        }
                    };
                }

                if let Some((lower, upper)) = bounds {
                    debug_assert!(k.is_empty());
                    Ok(KeyComparison::Range((
                        Bound::Included(lower.try_into()?),
                        Bound::Included(upper.try_into()?),
                    )))
                } else {
                    (k, binop_to_use)
                        .try_into()
                        .map_err(|_| ReadySetError::EmptyKey)
                }
            })
            .collect::<ReadySetResult<Vec<_>>>()?
    };

    trace!(?keys, ?filters, "Built view query");

    Ok(ViewQuery {
        key_comparisons: keys,
        block: read_behavior.is_blocking(),
        filter: filters
            .into_iter()
            .reduce(|expr1, expr2| DataflowExpression::Op {
                left: Box::new(expr1),
                op: BinaryOperator::And,
                right: Box::new(expr2),
            }),
        timestamp: ticket,
    })
}

/// Run the supplied [`SelectStatement`] on the supplied [`View`]
/// Assumption: the [`View`] was created for that specific [`SelectStatement`]
#[allow(clippy::needless_lifetimes)] // clippy erroneously thinks the timelife can be elided
async fn do_read<'a>(
    getter: &'a mut View,
    q: &nom_sql::SelectStatement,
    raw_keys: Vec<Cow<'_, [DataType]>>,
    ticket: Option<Timestamp>,
    read_behavior: ReadBehavior,
    read_request_handler: Option<&'a mut ReadRequestHandler>,
    _event: &mut noria_client_metrics::QueryExecutionEvent,
) -> ReadySetResult<QueryResult<'a>> {
    let use_bogo = raw_keys.is_empty();
    let vq = build_view_query(
        getter
            .schema()
            .ok_or_else(|| internal_err("No schema for view"))?,
        getter.key_map(),
        q,
        raw_keys,
        ticket,
        read_behavior,
    )?;

    let data = if let Some(rh) = read_request_handler {
        let request = noria::Tagged::from(ReadQuery::Normal {
            target: (*getter.node(), getter.name(), 0),
            query: vq.clone(),
        });

        // Query the local reader if it is a read query, otherwise default to the traditional
        // View API.
        let tag = request.tag;
        if let ReadQuery::Normal { target, query } = request.v {
            // Issue a normal read query returning the raw unserialized results.
            let result = rh.handle_normal_read_query(tag, target, query, true).await;
            result?
                .v
                .into_normal()
                .ok_or_else(|| internal_err("Unexpected response type from reader service"))?
                .map(|z| {
                    z.map_results(|rows, _| {
                        // `rows` is Unserialized as we pass `raw_result` = true.
                        #[allow(clippy::unwrap_used)]
                        Results::new(rows.into_unserialized().unwrap(), getter.column_slice())
                    })
                })?
                .into_results()
                .ok_or(ReadySetError::ReaderMissingKey)?
        } else {
            getter
                .raw_lookup(vq)
                .await?
                .into_results()
                .ok_or(ReadySetError::ReaderMissingKey)?
        }
    } else {
        getter
            .raw_lookup(vq)
            .await?
            .into_results()
            .ok_or(ReadySetError::ReaderMissingKey)?
    };
    trace!("select::complete");

    Ok(QueryResult::Select {
        data,
        select_schema: SelectSchema {
            use_bogo,
            schema: Cow::Borrowed(getter.schema().unwrap().schema(SchemaType::ReturnedSchema)), /* Safe because we already unwrapped above */
            columns: Cow::Borrowed(getter.columns()),
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_view_cache() {
        let global = Arc::new(RwLock::new(HashMap::new()));
        let mut view_cache = ViewCache::new(global);

        let name = "test_statement_name";
        let statement_str = "SELECT a_col FROM t1";
        let statement = if let SqlQuery::Select(s) =
            nom_sql::parse_query(nom_sql::Dialect::MySQL, statement_str).unwrap()
        {
            s
        } else {
            unreachable!();
        };

        view_cache.register_statement(name, &statement);
        let retrieved_statement = view_cache.select_statement_from_name(name);
        assert_eq!(Some(statement), retrieved_statement);

        view_cache.remove_statement(name);
        let retrieved_statement = view_cache.select_statement_from_name(name);
        assert_eq!(None, retrieved_statement);
    }

    mod build_view_query {
        use lazy_static::lazy_static;
        use nom_sql::{parse_query, Column, ColumnSpecification, Dialect, SqlType};
        use noria::ColumnBase;

        use super::*;

        lazy_static! {
            static ref SCHEMA: ViewSchema = ViewSchema::new(
                vec![
                    ColumnSchema {
                        spec: ColumnSpecification {
                            column: Column {
                                name: "x".into(),
                                table: Some("t".into()),
                            },
                            sql_type: SqlType::Int(None),
                            constraints: vec![],
                            comment: None,
                        },
                        base: Some(ColumnBase {
                            table: "t".into(),
                            column: "x".into(),
                        }),
                    },
                    ColumnSchema {
                        spec: ColumnSpecification {
                            column: Column {
                                name: "y".into(),
                                table: Some("t".into()),
                            },
                            sql_type: SqlType::Text,
                            constraints: vec![],
                            comment: None,
                        },
                        base: Some(ColumnBase {
                            table: "t".into(),
                            column: "y".into(),
                        }),
                    }
                ],
                vec![
                    ColumnSchema {
                        spec: ColumnSpecification {
                            column: Column {
                                name: "x".into(),
                                table: Some("t".into()),
                            },
                            sql_type: SqlType::Int(None),
                            constraints: vec![],
                            comment: None,
                        },
                        base: Some(ColumnBase {
                            table: "t".into(),
                            column: "x".into(),
                        }),
                    },
                    ColumnSchema {
                        spec: ColumnSpecification {
                            column: Column {
                                name: "y".into(),
                                table: Some("t".into()),
                            },
                            sql_type: SqlType::Text,
                            constraints: vec![],
                            comment: None,
                        },
                        base: Some(ColumnBase {
                            table: "t".into(),
                            column: "y".into(),
                        }),
                    }
                ],
            );
        }

        fn parse_select_statement(src: &str) -> SelectStatement {
            match parse_query(Dialect::MySQL, src).unwrap() {
                SqlQuery::Select(stmt) => stmt,
                _ => panic!("Unexpected query type"),
            }
        }

        #[test]
        fn simple_point_lookup() {
            let query = build_view_query(
                &*SCHEMA,
                &[(ViewPlaceholder::OneToOne(1), 0)],
                &parse_select_statement("SELECT t.x FROM t WHERE t.x = $1"),
                vec![vec![DataType::from(1)].into()],
                None,
                ReadBehavior::Blocking,
            )
            .unwrap();

            assert!(query.filter.is_none());
            assert_eq!(
                query.key_comparisons,
                vec![KeyComparison::from(vec1![DataType::from(1)])]
            );
        }

        #[test]
        fn single_between() {
            let query = build_view_query(
                &*SCHEMA,
                &[(ViewPlaceholder::Between(1, 2), 0)],
                &parse_select_statement("SELECT t.x FROM t WHERE t.x BETWEEN $1 AND $2"),
                vec![vec![DataType::from(1), DataType::from(2)].into()],
                None,
                ReadBehavior::Blocking,
            )
            .unwrap();

            assert!(query.filter.is_none());
            assert_eq!(
                query.key_comparisons,
                vec![KeyComparison::from_range(
                    &(vec1![DataType::from(1)]..=vec1![DataType::from(2)])
                )]
            );
        }

        #[test]
        fn ilike_and_equality() {
            let query = build_view_query(
                &*SCHEMA,
                &[
                    (ViewPlaceholder::OneToOne(1), 0),
                    (ViewPlaceholder::OneToOne(2), 1),
                ],
                &parse_select_statement("SELECT t.x FROM t WHERE t.x = $1 AND t.y ILIKE $2"),
                vec![vec![DataType::from(1), DataType::from("%a%")].into()],
                None,
                ReadBehavior::Blocking,
            )
            .unwrap();

            assert_eq!(
                query.filter,
                Some(DataflowExpression::Op {
                    left: Box::new(DataflowExpression::Column(1)),
                    op: BinaryOperator::ILike,
                    right: Box::new(DataflowExpression::Literal(DataType::from("%a%")))
                })
            );
        }

        #[test]
        fn mixed_equal_and_inclusive() {
            let query = build_view_query(
                &*SCHEMA,
                &[
                    (ViewPlaceholder::OneToOne(2), 1),
                    (ViewPlaceholder::OneToOne(1), 0),
                ],
                &parse_select_statement("SELECT t.x FROM t WHERE t.x >= $1 AND t.y = $2"),
                vec![vec![DataType::from(1), DataType::from("a")].into()],
                None,
                ReadBehavior::Blocking,
            )
            .unwrap();

            assert_eq!(
                query.key_comparisons,
                vec![KeyComparison::from_range(
                    &(vec1![DataType::from("a"), DataType::from(1)]
                        ..=vec1![DataType::from("a"), DataType::Max])
                )]
            );
        }

        #[test]
        fn mixed_equal_and_between() {
            let query = build_view_query(
                &*SCHEMA,
                &[
                    (ViewPlaceholder::OneToOne(3), 1),
                    (ViewPlaceholder::Between(1, 2), 0),
                ],
                &parse_select_statement(
                    "SELECT t.x FROM t WHERE t.x BETWEEN $1 AND $2 AND t.y = $3",
                ),
                vec![vec![DataType::from(1), DataType::from(2), DataType::from("a")].into()],
                None,
                ReadBehavior::Blocking,
            )
            .unwrap();

            assert!(query.filter.is_none());
            assert_eq!(
                query.key_comparisons,
                vec![KeyComparison::from_range(
                    &(vec1![DataType::from("a"), DataType::from(1)]
                        ..=vec1![DataType::from("a"), DataType::from(2)])
                )]
            );
        }

        #[test]
        fn mixed_equal_and_exclusive() {
            let query = build_view_query(
                &*SCHEMA,
                &[
                    (ViewPlaceholder::OneToOne(2), 1),
                    (ViewPlaceholder::OneToOne(1), 0),
                ],
                &parse_select_statement("SELECT t.x FROM t WHERE t.x > $1 AND t.y = $2"),
                vec![vec![DataType::from(1), DataType::from("a")].into()],
                None,
                ReadBehavior::Blocking,
            )
            .unwrap();

            assert_eq!(
                query.filter,
                Some(DataflowExpression::Op {
                    left: Box::new(DataflowExpression::Column(0)),
                    op: BinaryOperator::Greater,
                    right: Box::new(DataflowExpression::Literal(1.into()))
                })
            );
            assert_eq!(
                query.key_comparisons,
                vec![KeyComparison::from_range(
                    &(vec1![DataType::from("a"), DataType::from(1)]
                        ..=vec1![DataType::from("a"), DataType::Max])
                )]
            );
        }

        #[test]
        fn compound_range() {
            let query = build_view_query(
                &*SCHEMA,
                &[
                    (ViewPlaceholder::OneToOne(1), 0),
                    (ViewPlaceholder::OneToOne(2), 1),
                ],
                &parse_select_statement("SELECT t.x FROM t WHERE t.x > $1 AND t.y > $2"),
                vec![vec![DataType::from(1), DataType::from("a")].into()],
                None,
                ReadBehavior::Blocking,
            )
            .unwrap();

            assert_eq!(
                query.filter,
                Some(DataflowExpression::Op {
                    left: Box::new(DataflowExpression::Column(1)),
                    op: BinaryOperator::Greater,
                    right: Box::new(DataflowExpression::Literal("a".into()))
                })
            );

            assert_eq!(
                query.key_comparisons,
                vec![KeyComparison::Range((
                    Bound::Excluded(vec1![DataType::from(1), DataType::from("a")]),
                    Bound::Unbounded
                ))]
            );
        }

        #[test]
        fn paginated_with_key() {
            let query = build_view_query(
                &*SCHEMA,
                &[
                    (ViewPlaceholder::OneToOne(1), 0),
                    (
                        ViewPlaceholder::PageNumber {
                            offset_placeholder: 2,
                            limit: 3,
                        },
                        1,
                    ),
                ],
                &parse_select_statement(
                    "SELECT t.x FROM t WHERE t.x = $1 ORDER BY t.y ASC LIMIT 3 OFFSET $2",
                ),
                vec![vec![DataType::from(1), DataType::from(3)].into()],
                None,
                ReadBehavior::Blocking,
            )
            .unwrap();

            assert_eq!(query.filter, None);

            assert_eq!(
                query.key_comparisons,
                vec![vec1![
                    DataType::from(1),
                    DataType::from(1) // page 2
                ]
                .into()]
            );
        }
    }
}
