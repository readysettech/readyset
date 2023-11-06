use std::borrow::Cow;
use std::cmp::Reverse;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::convert::{TryFrom, TryInto};
use std::sync::{atomic, Arc};

use itertools::Itertools;
use nom_sql::{
    self, ColumnConstraint, DeleteStatement, DialectDisplay, Expr, InsertStatement, Relation,
    SqlIdentifier, SqlQuery, UnaryOperator, UpdateStatement,
};
use readyset_client::consensus::{Authority, AuthorityControl};
use readyset_client::consistency::Timestamp;
use readyset_client::internal::LocalNodeIndex;
use readyset_client::recipe::changelist::{Change, ChangeList, IntoChanges};
use readyset_client::results::{ResultIterator, Results};
use readyset_client::{
    ColumnSchema, GraphvizOptions, ReadQuery, ReaderAddress, ReaderHandle, ReadySetHandle,
    SchemaType, Table, TableOperation, View, ViewCreateRequest, ViewQuery,
};
use readyset_data::{DfType, DfValue, Dialect, TimestampTz};
use readyset_errors::{
    internal_err, invariant_eq, table_err, unsupported, unsupported_err, ReadySetError,
    ReadySetResult,
};
use readyset_server::worker::readers::{CallResult, ReadRequestHandler};
use readyset_util::redacted::Sensitive;
use readyset_util::shared_cache::{self, LocalCache};
use tokio::sync::RwLock;
use tracing::{error, info, instrument, trace, warn};

use crate::backend::SelectSchema;
use crate::rewrite::{self, ProcessedQueryParams};
use crate::utils;

#[derive(Clone, Debug)]
pub struct PreparedSelectStatement {
    name: Relation,
    processed_query_params: ProcessedQueryParams,
}

/// Wrapper around a NoriaBackendInner which may not have been successfully
/// created. When this is the case, this wrapper allows returning an error
/// from any call that requires NoriaBackendInner through an error
/// returned by `get_mut`.
pub struct NoriaBackend {
    inner: Option<NoriaBackendInner>,
}

impl NoriaBackend {
    fn get_mut(&mut self) -> ReadySetResult<&mut NoriaBackendInner> {
        // TODO(ENG-707): Support retrying to create a backend in the future.
        self.inner
            .as_mut()
            .ok_or_else(|| internal_err!("Failed to create a Noria backend."))
    }
}

pub struct NoriaBackendInner {
    noria: ReadySetHandle,
    tables: BTreeMap<Relation, Table>,
    views: LocalCache<Relation, View>,
    /// The server can handle (non-parameterized) LIMITs and (parameterized) OFFSETs in the
    /// dataflow graph
    server_supports_pagination: bool,
}

macro_rules! noria_await {
    ($self:expr, $fut:expr) => {{
        let noria = &mut $self.noria;

        futures_util::future::poll_fn(|cx| noria.poll_ready(cx)).await?;
        $fut.await
    }};
}

impl NoriaBackendInner {
    async fn new(
        ch: ReadySetHandle,
        views: LocalCache<Relation, View>,
        server_supports_pagination: bool,
    ) -> Self {
        NoriaBackendInner {
            tables: BTreeMap::new(),
            views,
            noria: ch,
            server_supports_pagination,
        }
    }

    async fn get_noria_table(&mut self, table: &Relation) -> ReadySetResult<&mut Table> {
        if !self.tables.contains_key(table) {
            let t = noria_await!(self, self.noria.table(table.clone()))?;
            self.tables.insert(table.to_owned(), t);
        }
        Ok(self.tables.get_mut(table).unwrap())
    }

    /// If `invalidate_cache` is passed, `self.views` will be ignored and a view will be retrieved
    /// from noria.
    async fn get_noria_view<'a>(
        &'a mut self,
        view: &Relation,
        invalidate_cache: bool,
    ) -> ReadySetResult<&'a mut View> {
        if invalidate_cache {
            self.views.remove(view).await;
        }
        self.views
            .get_mut_or_try_insert_with(view, shared_cache::InsertMode::Shared, async {
                noria_await!(self, self.noria.view(view.clone()))
            })
            .await
    }
}

#[derive(Debug, Clone)]
pub struct SelectPrepareResultInner {
    pub params: Vec<ColumnSchema>,
    pub schema: Vec<ColumnSchema>,
}

/// Types for a prepared select statement against ReadySet
#[derive(Debug, Clone)]
pub enum PreparedSelectTypes {
    /// Statement can be executed against ReadySet but we do not know the schema because it does
    /// not exist in dataflow. This variant is not useful without an upstream connection.
    ///
    /// This variant will be returned when the statement we are preparing reuses the cache of
    /// another query. We cannot return a prepared statement response to the client using this
    /// variant by itself, because we cannot determine the correct parameter and returned column
    /// metadata (since the query itself is not cached in dataflow). Instead, we must form the
    /// prepared statement response by retrieving the correct metadata from the upstream
    /// prepared statement response.
    NoSchema,
    /// The statement is cached in dataflow and we have the schema.
    Schema(SelectPrepareResultInner),
}

#[derive(Debug, Clone)]
// Due to differences in data type sizes, the large_enum_variant Clippy warning was being emitted
// for this type, but only when compiling for aarch64 targets.
#[cfg_attr(target_arch = "aarch64", allow(clippy::large_enum_variant))]
pub enum PrepareResult {
    Select {
        types: PreparedSelectTypes,
        statement: PreparedSelectStatement,
    },
    Insert {
        params: Vec<ColumnSchema>,
        schema: Vec<ColumnSchema>,
        statement: InsertStatement,
    },
    Update {
        params: Vec<ColumnSchema>,
        statement: UpdateStatement,
    },
    Delete {
        params: Vec<ColumnSchema>,
        statement: DeleteStatement,
    },
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
#[allow(clippy::large_enum_variant)]
pub enum QueryResult<'a> {
    Empty,
    Insert {
        num_rows_inserted: u64,
        first_inserted_id: u64,
    },
    Select {
        rows: ResultIterator,
        schema: SelectSchema<'a>,
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
    /// A table of variables returned as a response to a SHOW READYSET STATUS query.
    /// The first MetaVariable serves as the column headers
    MetaWithHeader(Vec<MetaVariable>),
}

impl<'a> QueryResult<'a> {
    pub fn from_owned(schema: SelectSchema<'a>, data: Vec<Results>) -> Self {
        QueryResult::Select {
            schema,
            rows: ResultIterator::owned(data),
        }
    }

    pub fn empty(schema: SelectSchema<'a>) -> Self {
        QueryResult::Select {
            schema,
            rows: ResultIterator::owned(vec![]),
        }
    }

    pub fn from_iter(schema: SelectSchema<'a>, rows: ResultIterator) -> Self {
        QueryResult::Select { schema, rows }
    }

    #[inline]
    pub fn into_owned(self) -> QueryResult<'static> {
        match self {
            QueryResult::Select { schema, rows } => QueryResult::Select {
                schema: schema.into_owned(),
                rows,
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
            QueryResult::MetaWithHeader(vec) => QueryResult::MetaWithHeader(vec),
        }
    }
}

pub struct NoriaConnector {
    inner: NoriaBackend,
    auto_increments: Arc<RwLock<HashMap<Relation, atomic::AtomicUsize>>>,
    /// Global and thread-local cache of view endpoints and prepared statements.
    view_name_cache: LocalCache<ViewCreateRequest, Relation>,

    /// Set of views that have failed on previous requests. Separate from the backend
    /// to allow returning references to schemas from views all the way to mysql-srv,
    /// but on subsequent requests, do not use a failed view.
    failed_views: HashSet<Relation>,

    /// How to handle issuing reads against ReadySet. See [`ReadBehavior`].
    read_behavior: ReadBehavior,

    /// A read request handler that may be used to service reads from readers
    /// on the same server.
    read_request_handler: request_handler::LocalReadHandler,

    /// SQL Dialect to pass to ReadySet as part of all migration requests
    dialect: Dialect,

    /// Dialect to use to parse and format all SQL strings
    parse_dialect: nom_sql::Dialect,

    /// Currently configured search path for schemas.
    ///
    /// Note that the terminology used here is maximally general - while only PostgreSQL truly
    /// supports a multi-element schema search path, the concept of "currently connected database"
    /// in MySQL can be thought of as a schema search path that only has one element.
    schema_search_path: Vec<SqlIdentifier>,
}

mod request_handler {
    use readyset_server::worker::readers::ReadRequestHandler;

    /// Since [`ReadRequestHandler`] contains some fields that aren't [`Sync`], this is a workaround
    /// wrapper to make it safely [`Sync`], by ensuring that all accesses to the underlying
    /// [`ReadRequestHandler`] are exclusive. This effectively makes sure that no references are
    /// shared between threads ever. This is implemented in a submodule so that the private fields
    /// are not accidentally accessed without an exclusive reference.
    #[derive(Clone)]
    #[repr(transparent)]
    pub(super) struct LocalReadHandler(Option<ReadRequestHandler>);

    impl LocalReadHandler {
        pub(super) fn new(handler: Option<ReadRequestHandler>) -> Self {
            LocalReadHandler(handler)
        }

        #[inline]
        pub(super) fn as_mut(&mut self) -> Option<&mut ReadRequestHandler> {
            self.0.as_mut()
        }
    }

    /// SAFETY: since all accesses to the inner field are exclusive, no references are ever shared
    /// between threads.
    unsafe impl Sync for LocalReadHandler {}
}

/// The read behavior used when executing a read against ReadySet.
#[derive(Clone, Copy)]
pub enum ReadBehavior {
    /// If ReadySet is unable to immediately service the read due to a cache miss, block on the
    /// response.
    Blocking,
    /// If ReadySet is unable to immediately service the read, return an error.
    NonBlocking,
}

impl ReadBehavior {
    fn is_blocking(&self) -> bool {
        matches!(self, Self::Blocking)
    }
}

/// Provides the necessary context to execute a select statement against noria, either for a
/// prepared or an ad-hoc query
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub(crate) enum ExecuteSelectContext<'ctx> {
    Prepared {
        ps: &'ctx PreparedSelectStatement,
        params: &'ctx [DfValue],
    },
    AdHoc {
        statement: nom_sql::SelectStatement,
        create_if_missing: bool,
        processed_query_params: ProcessedQueryParams,
    },
}

impl NoriaConnector {
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        ch: ReadySetHandle,
        auto_increments: Arc<RwLock<HashMap<Relation, atomic::AtomicUsize>>>,
        view_name_cache: LocalCache<ViewCreateRequest, Relation>,
        view_cache: LocalCache<Relation, View>,
        read_behavior: ReadBehavior,
        dialect: Dialect,
        parse_dialect: nom_sql::Dialect,
        schema_search_path: Vec<SqlIdentifier>,
        server_supports_pagination: bool,
    ) -> Self {
        NoriaConnector::new_with_local_reads(
            ch,
            auto_increments,
            view_name_cache,
            view_cache,
            read_behavior,
            None,
            dialect,
            parse_dialect,
            schema_search_path,
            server_supports_pagination,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn new_with_local_reads(
        ch: ReadySetHandle,
        auto_increments: Arc<RwLock<HashMap<Relation, atomic::AtomicUsize>>>,
        view_name_cache: LocalCache<ViewCreateRequest, Relation>,
        view_cache: LocalCache<Relation, View>,
        read_behavior: ReadBehavior,
        read_request_handler: Option<ReadRequestHandler>,
        dialect: Dialect,
        parse_dialect: nom_sql::Dialect,
        schema_search_path: Vec<SqlIdentifier>,
        server_supports_pagination: bool,
    ) -> Self {
        let backend = NoriaBackendInner::new(ch, view_cache, server_supports_pagination).await;

        NoriaConnector {
            inner: NoriaBackend {
                inner: Some(backend),
            },
            auto_increments,
            view_name_cache,
            failed_views: HashSet::new(),
            read_behavior,
            read_request_handler: request_handler::LocalReadHandler::new(read_request_handler),
            dialect,
            parse_dialect,
            schema_search_path,
        }
    }

    pub(crate) async fn graphviz(
        &mut self,
        simplified: bool,
        for_query: Option<Relation>,
    ) -> ReadySetResult<QueryResult<'static>> {
        let label = if simplified {
            "SIMPLIFIED GRAPHVIZ"
        } else {
            "GRAPHVIZ"
        };

        let graphviz = self
            .inner
            .get_mut()?
            .noria
            .graphviz(GraphvizOptions {
                detailed: !simplified,
                for_query,
            })
            .await?;

        Ok(QueryResult::Meta(vec![(label, graphviz).into()]))
    }

    pub(crate) async fn explain_domains(&mut self) -> ReadySetResult<QueryResult<'static>> {
        let domains = self.inner.get_mut()?.noria.domains().await?;
        let schema = SelectSchema {
            schema: Cow::Owned(vec![
                ColumnSchema {
                    column: nom_sql::Column {
                        name: "domain".into(),
                        table: None,
                    },
                    column_type: DfType::DEFAULT_TEXT,
                    base: None,
                },
                ColumnSchema {
                    column: nom_sql::Column {
                        name: "worker".into(),
                        table: None,
                    },
                    column_type: DfType::DEFAULT_TEXT,
                    base: None,
                },
            ]),
            columns: Cow::Owned(vec!["domain".into(), "worker".into()]),
        };

        let mut data = domains
            .into_iter()
            .flat_map(|(di, shards)| {
                shards
                    .into_iter()
                    .enumerate()
                    .flat_map(move |(shard, replicas)| {
                        replicas
                            .into_iter()
                            .enumerate()
                            .map(move |(replica, worker)| {
                                vec![
                                    DfValue::from(format!("{di}.{shard}.{replica}")),
                                    DfValue::from(
                                        worker.map(|w| w.to_string()).unwrap_or_default(),
                                    ),
                                ]
                            })
                    })
            })
            .collect::<Vec<_>>();

        data.sort_by(|r1, r2| r1[1].cmp(&r2[1]));

        Ok(QueryResult::from_owned(schema, vec![Results::new(data)]))
    }

    pub(crate) async fn explain_materializations(
        &mut self,
    ) -> ReadySetResult<QueryResult<'static>> {
        let mut materializations = self.inner.get_mut()?.noria.materialization_info().await?;
        materializations.sort_unstable_by_key(|mi| Reverse(mi.size.bytes));
        let cols = [
            ("node_index", DfType::Int),
            ("node_name", DfType::DEFAULT_TEXT),
            ("node_description", DfType::DEFAULT_TEXT),
            ("keys", DfType::DEFAULT_TEXT),
            ("size_bytes", DfType::BigInt),
            ("partial", DfType::Bool),
            ("indexes", DfType::Array(Box::new(DfType::DEFAULT_TEXT))),
        ];
        let schema = SelectSchema {
            columns: cols.iter().map(|(n, _)| n.into()).collect(),
            schema: cols
                .iter()
                .map(|(name, column_type)| ColumnSchema {
                    column: nom_sql::Column {
                        name: name.into(),
                        table: None,
                    },
                    column_type: column_type.clone(),
                    base: None,
                })
                .collect(),
        };

        let data = materializations
            .into_iter()
            .map(|mi| {
                vec![
                    mi.node_index.index().into(),
                    mi.node_name.display_unquoted().to_string().into(),
                    mi.node_description.into(),
                    mi.size.key_count.to_string().into(),
                    mi.size.bytes.0.into(),
                    mi.partial.into(),
                    mi.indexes
                        .into_iter()
                        .map(|idx| {
                            format!("{:?}[{}]", idx.index_type, idx.columns.iter().join(", "))
                                .into()
                        })
                        .collect::<Vec<DfValue>>()
                        .into(),
                ]
            })
            .collect();

        Ok(QueryResult::from_owned(schema, vec![Results::new(data)]))
    }

    pub(crate) async fn list_create_cache_stmts(&mut self) -> ReadySetResult<Vec<String>> {
        let noria = &mut self.inner.get_mut()?.noria;
        Ok(noria
            .verbose_views()
            .await?
            .into_iter()
            .map(|stmt| stmt.display(self.parse_dialect).to_string())
            .collect())
    }

    pub(crate) fn server_supports_pagination(&self) -> bool {
        self.inner
            .inner
            .as_ref()
            .map(|v| v.server_supports_pagination)
            .unwrap_or(false)
    }

    // TODO(andrew): Allow client to map table names to NodeIndexes without having to query ReadySet
    // repeatedly. Eventually, this will be responsibility of the TimestampService.
    pub async fn node_index_of(&mut self, table_name: &str) -> ReadySetResult<LocalNodeIndex> {
        let table_handle = self.inner.get_mut()?.noria.table(table_name).await?;
        Ok(table_handle.node)
    }

    pub async fn handle_insert(
        &mut self,
        q: &nom_sql::InsertStatement,
    ) -> ReadySetResult<QueryResult<'_>> {
        let table = &q.table;

        // create a mutator if we don't have one for this table already
        trace!(table = %table.display_unquoted(), "query::insert::access mutator");
        let putter = self.inner.get_mut()?.get_noria_table(table).await?;
        trace!("query::insert::extract schema");
        let schema = putter
            .schema()
            .ok_or_else(|| internal_err!("no schema for table {}", table.display_unquoted()))?;

        // set column names (insert schema) if not set
        let q = match q.fields {
            Some(_) => Cow::Borrowed(q),
            None => {
                let mut query = q.clone();
                query.fields = Some(schema.fields.iter().map(|cs| cs.column.clone()).collect());
                Cow::Owned(query)
            }
        };

        let data: Vec<Vec<DfValue>> = q
            .data
            .iter()
            .map(|row| {
                row.iter()
                    .map(|expr| match expr {
                        Expr::Literal(lit) => DfValue::try_from(lit),
                        // Ad-hoc handle unary negation (for logictests, to allow them to insert
                        // negative values)
                        Expr::UnaryOp {
                            op: UnaryOperator::Neg,
                            rhs: box Expr::Literal(lit),
                        } => {
                            let val = DfValue::try_from(lit)?;
                            &val * &(-1).into()
                        }
                        _ => unsupported!("Only literal values are supported in expressions"),
                    })
                    .collect::<Result<Vec<_>, _>>()
            })
            .collect::<Result<Vec<_>, _>>()?;

        self.do_insert(&q, data).await
    }

    pub async fn prepare_insert(
        &mut self,
        mut statement: nom_sql::InsertStatement,
    ) -> ReadySetResult<PrepareResult> {
        trace!(table = %statement.table.name, "insert::access mutator");
        let mutator = self
            .inner
            .get_mut()?
            .get_noria_table(&statement.table)
            .await?;
        trace!("insert::extract schema");
        let schema = mutator
            .schema()
            .ok_or_else(|| {
                internal_err!("Could not find schema for table {}", statement.table.name)
            })?
            .fields
            .iter()
            .map(|cs| ColumnSchema::from_base(cs.clone(), statement.table.clone(), self.dialect))
            .collect::<Result<Vec<_>, _>>()?;

        if statement.fields.is_none() {
            statement.fields = Some(
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
            let param_cols = utils::insert_statement_parameter_columns(&statement);
            param_cols
                .into_iter()
                .map(|c| {
                    schema
                        .iter()
                        .cloned()
                        .find(|mc| c.name == mc.column.name)
                        .ok_or_else(|| {
                            internal_err!(
                                "column {} missing in mutator schema",
                                c.display_unquoted()
                            )
                        })
                })
                .collect::<ReadySetResult<Vec<_>>>()?
        };

        Ok(PrepareResult::Insert {
            params,
            schema,
            statement,
        })
    }

    pub(crate) async fn execute_prepared_insert(
        &mut self,
        q: &InsertStatement,
        params: &[DfValue],
    ) -> ReadySetResult<QueryResult<'_>> {
        let table = &q.table;
        let putter = self.inner.get_mut()?.get_noria_table(table).await?;
        trace!("insert::extract schema");
        let schema = putter
            .schema()
            .ok_or_else(|| internal_err!("no schema for table {}", table.display_unquoted()))?;
        let rows = utils::extract_insert(q, params, schema, self.dialect)?;
        self.do_insert(q, rows).await
    }

    pub(crate) async fn handle_delete(
        &mut self,
        q: &nom_sql::DeleteStatement,
    ) -> ReadySetResult<QueryResult<'_>> {
        let cond = q
            .where_clause
            .as_ref()
            .ok_or_else(|| unsupported_err!("only supports DELETEs with WHERE-clauses"))?;

        // create a mutator if we don't have one for this table already
        trace!(table = %q.table.name, "delete::access mutator");
        let mutator = self.inner.get_mut()?.get_noria_table(&q.table).await?;

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

    pub(crate) async fn handle_update<'a>(
        &'a mut self,
        q: &nom_sql::UpdateStatement,
    ) -> ReadySetResult<QueryResult<'a>> {
        self.do_update(Cow::Borrowed(q), None).await
    }

    pub(crate) async fn prepare_update(
        &mut self,
        statement: UpdateStatement,
    ) -> ReadySetResult<PrepareResult> {
        // ensure that we have schemas and endpoints for the query
        trace!(table = %statement.table.name, "update::access mutator");
        let mutator = self
            .inner
            .get_mut()?
            .get_noria_table(&statement.table)
            .await?;
        trace!("update::extract schema");
        let table_schema = mutator.schema().ok_or_else(|| {
            internal_err!("Could not find schema for table {}", statement.table.name)
        })?;

        // extract parameter columns
        let params = utils::update_statement_parameter_columns(&statement)
            .into_iter()
            .map(|c| {
                table_schema
                    .fields
                    .iter()
                    // We know that only one table is mentioned, so no need to match on both table
                    // and name - just check name here
                    .find(|f| f.column.name == c.name)
                    .cloned()
                    .map(|cs| ColumnSchema::from_base(cs, statement.table.clone(), self.dialect))
                    .transpose()?
                    .ok_or_else(|| internal_err!("Unknown column {}", c.display_unquoted()))
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(PrepareResult::Update { params, statement })
    }

    pub(crate) async fn execute_prepared_update(
        &mut self,
        q: &UpdateStatement,
        params: &[DfValue],
    ) -> ReadySetResult<QueryResult<'_>> {
        self.do_update(Cow::Owned(q.clone()), Some(params)).await
    }

    pub(crate) async fn prepare_delete(
        &mut self,
        statement: DeleteStatement,
    ) -> ReadySetResult<PrepareResult> {
        // ensure that we have schemas and endpoints for the query
        trace!(table = %statement.table.name, "delete::access mutator");
        let mutator = self
            .inner
            .get_mut()?
            .get_noria_table(&statement.table)
            .await?;
        trace!("delete::extract schema");
        let table_schema = mutator.schema().ok_or_else(|| {
            internal_err!("Could not find schema for table {}", statement.table.name)
        })?;

        // extract parameter columns
        let params = utils::delete_statement_parameter_columns(&statement)
            .into_iter()
            .map(|c| {
                table_schema
                    .fields
                    .iter()
                    // We know that only one table is mentioned, so no need to match on both table
                    // and name - just check name here
                    .find(|f| f.column.name == c.name)
                    .cloned()
                    .map(|cs| ColumnSchema::from_base(cs, statement.table.clone(), self.dialect))
                    .transpose()?
                    .ok_or_else(|| internal_err!("Unknown column {}", c.display_unquoted()))
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(PrepareResult::Delete { params, statement })
    }

    pub(crate) async fn execute_prepared_delete(
        &mut self,
        q: &DeleteStatement,
        params: &[DfValue],
    ) -> ReadySetResult<QueryResult<'_>> {
        self.do_delete(Cow::Owned(q.clone()), Some(params)).await
    }

    /// Calls the `extend_recipe` endpoint on ReadySet with the given query.
    pub(crate) async fn handle_table_operation<C>(
        &mut self,
        changes: C,
    ) -> ReadySetResult<QueryResult<'_>>
    where
        C: IntoChanges,
    {
        // TODO(malte): we should perhaps check our usual caches here, rather than just blindly
        // doing a migration on ReadySet ever time. On the other hand, CREATE TABLE is rare...
        noria_await!(
            self.inner.get_mut()?,
            self.inner.get_mut()?.noria.extend_recipe(
                ChangeList::from_changes(changes, self.dialect)
                    .with_schema_search_path(self.schema_search_path.clone())
            )
        )?;
        Ok(QueryResult::Empty)
    }

    /// Returns status provided by the Controller and persisted in the Authority. Also appends
    /// additional_meta provided by the caller to the status.
    pub(crate) async fn readyset_status(
        &mut self,
        authority: &Authority,
        mut additional_meta: Vec<(String, String)>,
    ) -> ReadySetResult<QueryResult<'static>> {
        let mut status =
            match noria_await!(self.inner.get_mut()?, self.inner.get_mut()?.noria.status()) {
                Ok(s) => <Vec<(String, String)>>::from(s),
                Err(_) => vec![(
                    "ReadySet Controller Status".to_string(),
                    "Unavailable".to_string(),
                )],
            };

        // Helper function for formatting
        fn time_or_null(time_ms: Option<u64>) -> String {
            if let Some(t) = time_ms {
                TimestampTz::from_unix_ms(t).to_string()
            } else {
                "NULL".to_string()
            }
        }

        if let Ok(Some(stats)) = authority.persistent_stats().await {
            status.push((
                "Last started Controller".to_string(),
                time_or_null(stats.last_controller_startup),
            ));
            status.push((
                "Last completed snapshot".to_string(),
                time_or_null(stats.last_completed_snapshot),
            ));
            status.push((
                "Last started replication".to_string(),
                time_or_null(stats.last_started_replication),
            ));
            if let Some(err) = stats.last_replicator_error {
                status.push(("Last replicator error".to_string(), err))
            }
        }

        additional_meta.append(&mut status);

        Ok(QueryResult::MetaVariables(
            additional_meta
                .into_iter()
                .map(MetaVariable::from)
                .collect(),
        ))
    }

    /// Query the status of a pending migration identified by the given `migration_id`. Once the
    /// function returns a result (completed or an error), calling again with the same id will lead
    /// to undefined behavior.
    pub(crate) async fn migration_status(
        &mut self,
        id: u64,
    ) -> ReadySetResult<QueryResult<'static>> {
        let status = noria_await!(
            self.inner.get_mut()?,
            self.inner.get_mut()?.noria.migration_status(id)
        )?
        .to_string();
        Ok(QueryResult::Meta(vec![(
            "Migration Status".to_string(),
            status,
        )
            .into()]))
    }

    pub(crate) async fn table_statuses(&mut self) -> ReadySetResult<QueryResult<'static>> {
        let statuses = noria_await!(
            self.inner.get_mut()?,
            self.inner.get_mut()?.noria.table_statuses()
        )?;

        let schema = SelectSchema {
            schema: Cow::Owned(
                ["table", "status", "description"]
                    .iter()
                    .map(|name| ColumnSchema {
                        column: nom_sql::Column {
                            name: name.into(),
                            table: None,
                        },
                        column_type: DfType::DEFAULT_TEXT,
                        base: None,
                    })
                    .collect(),
            ),
            columns: Cow::Owned(vec![
                "table".into(),
                "replication status".into(),
                "replication status description".into(),
            ]),
        };
        let data = statuses
            .into_iter()
            .map(|(tbl, status)| {
                let replication_status_str = status.replication_status.to_string();
                let replication_split = replication_status_str
                    .splitn(2, ": ")
                    .collect::<Vec<&str>>();
                let (replication_status, description) =
                    if replication_split[0].starts_with("Not Replicated") {
                        (
                            replication_split[0].to_string(),
                            replication_split.get(1).unwrap_or(&"").to_string(),
                        )
                    } else {
                        (status.replication_status.to_string(), "".to_string())
                    };
                vec![
                    tbl.display(self.parse_dialect).to_string().into(),
                    replication_status.into(),
                    description.into(),
                ]
            })
            .collect::<Vec<_>>();

        Ok(QueryResult::from_owned(schema, vec![Results::new(data)]))
    }

    /// Set the schema search path
    pub fn set_schema_search_path(&mut self, search_path: Vec<SqlIdentifier>) {
        self.schema_search_path = search_path;
    }

    /// Returns a reference to the currently configured schema search path
    pub fn schema_search_path(&self) -> &[SqlIdentifier] {
        self.schema_search_path.as_ref()
    }
}

impl NoriaConnector {
    /// This function handles CREATE CACHE statements. When explicit-migrations is enabled,
    /// this function is the only way to create a view in noria.
    ///
    /// Returns Ok(Some(id)) if CREATE CACHE CONCURRENTLY is issued, where id is a unique identifier
    /// that can be used to query the status of the migration. Otherwise, returns Ok(None) on
    /// success and Err(_) on failure.
    pub async fn handle_create_cached_query(
        &mut self,
        name: Option<&Relation>,
        statement: &nom_sql::SelectStatement,
        override_schema_search_path: Option<Vec<SqlIdentifier>>,
        always: bool,
        concurrently: bool,
    ) -> ReadySetResult<Option<u64>> {
        let name = name.cloned().unwrap_or_else(|| {
            utils::generate_query_name(statement, self.schema_search_path()).into()
        });
        let schema_search_path =
            override_schema_search_path.unwrap_or_else(|| self.schema_search_path.clone());
        let changelist = ChangeList::from_change(
            Change::create_cache(name.clone(), statement.clone(), always),
            self.dialect,
        )
        .with_schema_search_path(schema_search_path.clone());

        if concurrently {
            let id = noria_await!(
                self.inner.get_mut()?,
                self.inner.get_mut()?.noria.extend_recipe_async(changelist)
            )?;
            Ok(Some(id))
        } else {
            noria_await!(
                self.inner.get_mut()?,
                self.inner.get_mut()?.noria.extend_recipe(changelist)
            )?;

            // If the query is already in there with a different name, we don't need to make a new
            // name for it, as *lookups* only need one of the names for the query, and
            // when we drop it we'll be hitting noria anyway
            self.view_name_cache
                .insert(
                    ViewCreateRequest::new(statement.clone(), schema_search_path),
                    name.clone(),
                )
                .await;

            Ok(None)
        }
    }

    pub(crate) async fn get_view_name(
        &mut self,
        q: &nom_sql::SelectStatement,
        is_prepared: bool,
        create_if_not_exist: bool,
        override_schema_search_path: Option<Vec<SqlIdentifier>>,
    ) -> ReadySetResult<Relation> {
        let search_path =
            override_schema_search_path.unwrap_or_else(|| self.schema_search_path().to_vec());
        let view_request = ViewCreateRequest::new(q.clone(), search_path.clone());
        self.view_name_cache
            .get_mut_or_try_insert_with(&view_request, shared_cache::InsertMode::Shared, async {
                let qname: Relation = utils::generate_query_name(q, &search_path).into();

                // add the query to ReadySet
                if create_if_not_exist {
                    if is_prepared {
                        info!(
                            query = %Sensitive(&q.display(self.parse_dialect)),
                            name = %qname.display_unquoted(),
                            "adding parameterized query"
                        );
                    } else {
                        info!(
                            query = %Sensitive(&q.display(self.parse_dialect)),
                            name = %qname.display_unquoted(),
                            "adding ad-hoc query"
                        );
                    }

                    // We currently don't fully support async migrations, and piping through the
                    // full 'create cache' string for _every_ query we see would be overly
                    // expensive, so we fall back to constructing a 'create cache' statement from a
                    // displayed version of the the SelectStatement we already parsed in this case
                    let changelist = ChangeList::from_change(
                        Change::create_cache(qname.clone(), q.clone(), false),
                        self.dialect,
                    )
                    .with_schema_search_path(search_path);

                    if let Err(error) = noria_await!(
                        self.inner.get_mut()?,
                        self.inner.get_mut()?.noria.extend_recipe(changelist)
                    ) {
                        if error.caused_by_table_not_replicated() {
                            warn!(%error, "add query failed");
                        } else {
                            error!(%error, "add query failed");
                        }

                        return Err(error);
                    }
                } else {
                    match noria_await!(
                        self.inner.get_mut()?,
                        self.inner.get_mut()?.noria.view(qname.clone())
                    ) {
                        Ok(view) => {
                            // We should not have an entry, but if we do it's safe to overwrite
                            // since we got this information from the controller.
                            self.inner
                                .get_mut()?
                                .views
                                .insert(qname.clone(), view)
                                .await;
                        }
                        Err(e) => {
                            return Err(e);
                        }
                    }
                }
                Ok(qname)
            })
            .await
            .cloned()
    }

    /// Make a request to ReadySet to drop the query with the given name, and remove it from all
    /// internal state.
    pub async fn drop_view(&mut self, name: &Relation) -> ReadySetResult<u64> {
        let result = noria_await!(
            self.inner.get_mut()?,
            self.inner.get_mut()?.noria.remove_query(name)
        )?;
        self.view_name_cache.remove_val(name).await;
        Ok(result)
    }

    /// Make a request to ReadySet to drop all cached queries, and empty all internal state
    pub async fn drop_all_caches(&mut self) -> ReadySetResult<()> {
        noria_await!(
            self.inner.get_mut()?,
            self.inner.get_mut()?.noria.remove_all_queries()
        )?;
        self.view_name_cache.clear().await;
        Ok(())
    }

    pub async fn view_create_request_from_name(
        &self,
        name: &Relation,
    ) -> Option<ViewCreateRequest> {
        self.view_name_cache.key_for_val(name).await
    }

    async fn do_insert(
        &mut self,
        q: &InsertStatement,
        data: Vec<Vec<DfValue>>,
    ) -> ReadySetResult<QueryResult<'_>> {
        let table = &q.table;

        // create a mutator if we don't have one for this table already
        trace!(table = %table.display_unquoted(), "insert::access mutator");
        let putter = self.inner.get_mut()?.get_noria_table(table).await?;
        trace!("insert::extract schema");
        let schema = putter
            .schema()
            .ok_or_else(|| internal_err!("no schema for table {}", table.display_unquoted()))?;

        let columns_specified: Vec<_> = q
            .fields
            .as_ref()
            .unwrap()
            .iter()
            .cloned()
            .map(|mut c| {
                c.table = Some(q.table.clone());
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
                table.clone(),
                ReadySetError::MultipleAutoIncrement,
            ));
        }

        let ai = &mut self.auto_increments;
        let ai_lock = ai.read().await;
        if ai_lock.get(table).is_none() {
            drop(ai_lock);
            ai.write()
                .await
                .entry(table.clone())
                .or_insert_with(|| atomic::AtomicUsize::new(0));
        }
        let mut buf = vec![vec![DfValue::None; schema.fields.len()]; data.len()];
        let mut first_inserted_id = None;
        let ai_lock = ai.read().await;
        let last_insert_id = &ai_lock[table];

        // handle default values
        trace!("insert::default values");
        let mut default_value_columns = vec![];
        for c in &schema.fields {
            for cc in &c.constraints {
                if let ColumnConstraint::DefaultValue(ref def) = *cc {
                    match def {
                        Expr::Literal(v) => {
                            default_value_columns.push((c.column.clone(), v.clone()))
                        }
                        _ => {
                            unsupported!("Only literal values are supported in default values")
                        }
                    }
                }
            }
        }

        trace!("insert::construct ops");

        for (ri, row) in data.iter().enumerate() {
            if let Some(col) = auto_increment_columns.get(0) {
                let idx = schema
                    .fields
                    .iter()
                    .position(|f| f == *col)
                    .ok_or_else(|| {
                        table_err(
                            table.clone(),
                            ReadySetError::NoSuchColumn(col.column.name.to_string()),
                        )
                    })?;
                // query can specify an explicit AUTO_INCREMENT value
                if !columns_specified.contains(&col.column) {
                    let id = last_insert_id.fetch_add(1, atomic::Ordering::SeqCst) as i64 + 1;
                    if first_inserted_id.is_none() {
                        first_inserted_id = Some(id);
                    }
                    buf[ri][idx] = DfValue::from(id);
                }
            }

            for (c, v) in default_value_columns.drain(..) {
                let idx = schema
                    .fields
                    .iter()
                    .position(|f| f.column == c)
                    .ok_or_else(|| {
                        table_err(
                            table.clone(),
                            ReadySetError::NoSuchColumn(c.name.to_string()),
                        )
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
                            putter.table_name().clone(),
                            ReadySetError::NoSuchColumn(c.name.to_string()),
                        )
                    })?;

                let target_type = DfType::from_sql_type(&field.sql_type, self.dialect, |_| None)?;

                let value = row
                    .get(ci)
                    .ok_or_else(|| {
                        internal_err!(
                            "Row returned from readyset-server had the wrong number of columns",
                        )
                    })?
                    .coerce_to(&target_type, &DfType::Unknown)?; // No from_ty, we're inserting literals
                buf[ri][idx] = value;
            }
        }

        let result = if let Some(ref update_fields) = q.on_duplicate {
            trace!("insert::complex");
            invariant_eq!(buf.len(), 1);

            let updates = {
                // fake out an update query
                let mut uq = UpdateStatement {
                    table: table.clone(),
                    fields: update_fields.clone(),
                    where_clause: None,
                };
                utils::extract_update_params_and_fields(
                    &mut uq,
                    &mut None::<std::iter::Empty<DfValue>>,
                    schema,
                    self.dialect,
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
        params: Option<&[DfValue]>,
    ) -> ReadySetResult<QueryResult<'_>> {
        trace!(table = %q.table.name, "update::access mutator");
        let mutator = self.inner.get_mut()?.get_noria_table(&q.table).await?;

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
                utils::coerce_params(params, &SqlQuery::Update(q.clone()), schema, self.dialect)?;
            utils::extract_update(
                q,
                coerced_params.map(|p| p.into_iter()),
                schema,
                self.dialect,
            )?
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

    async fn do_delete<'a>(
        &'a mut self,
        q: Cow<'_, DeleteStatement>,
        params: Option<&[DfValue]>,
    ) -> ReadySetResult<QueryResult<'a>> {
        trace!(table = %q.table.name, "delete::access mutator");
        let mutator = self.inner.get_mut()?.get_noria_table(&q.table).await?;

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
                utils::coerce_params(params, &SqlQuery::Delete(q.clone()), schema, self.dialect)?;
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

    #[instrument(level = "info", skip(self, statement))]
    pub(crate) async fn prepare_select(
        &mut self,
        mut statement: nom_sql::SelectStatement,
        create_if_not_exist: bool,
        override_schema_search_path: Option<Vec<SqlIdentifier>>,
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
            .map(|column| ColumnSchema {
                column,
                column_type: DfType::UnsignedBigInt,
                base: None,
            })
            .collect();

        trace!("select::collapse where-in clauses");
        let processed_query_params =
            rewrite::process_query(&mut statement, self.server_supports_pagination())?;

        // check if we already have this query prepared
        trace!("select::access view");
        let qname = self
            .get_view_name(
                &statement,
                true,
                create_if_not_exist,
                override_schema_search_path,
            )
            .await?;

        let view_failed = self.failed_views.take(&qname).is_some();
        let getter = self
            .inner
            .get_mut()?
            .get_noria_view(&qname, view_failed)
            .await?;

        // extract result schema
        let getter_schema = match getter {
            View::MultipleReused(_) => None,
            View::Single(view) => {
                let schema = view.schema();
                if schema.is_none() {
                    warn!(view = %qname.display_unquoted(), "no schema for view");
                }
                schema
            }
        };

        let statement = PreparedSelectStatement {
            name: qname.clone(),
            processed_query_params,
        };

        let types = if let Some(getter_schema) = getter_schema {
            let mut params: Vec<_> = getter_schema
                .to_cols(&client_param_columns, SchemaType::ProjectedSchema)?
                .into_iter()
                .map(|cs| {
                    let mut cs = cs.clone();
                    cs.column.table = Some(qname.clone());
                    cs
                })
                .collect();

            params.extend(limit_columns);

            PreparedSelectTypes::Schema(SelectPrepareResultInner {
                params,
                schema: getter_schema.schema(SchemaType::ReturnedSchema).to_vec(),
            })
        } else {
            PreparedSelectTypes::NoSchema
        };

        Ok(PrepareResult::Select { types, statement })
    }

    #[instrument(level = "debug", skip(self, event))]
    pub(crate) async fn execute_select(
        &mut self,
        ctx: ExecuteSelectContext<'_>,
        ticket: Option<Timestamp>,
        event: &mut readyset_client_metrics::QueryExecutionEvent,
    ) -> ReadySetResult<QueryResult<'_>> {
        let (qname, processed_query_params, params) = match ctx {
            ExecuteSelectContext::Prepared {
                ps:
                    PreparedSelectStatement {
                        name,
                        processed_query_params,
                    },
                params,
            } => (
                Cow::Borrowed(name),
                Cow::Borrowed(processed_query_params),
                params,
            ),
            ExecuteSelectContext::AdHoc {
                statement,
                create_if_missing,
                processed_query_params,
            } => {
                let name = self
                    .get_view_name(&statement, false, create_if_missing, None)
                    .await?;
                (
                    Cow::Owned(name),
                    Cow::Owned(processed_query_params),
                    &[][..],
                )
            }
        };

        let view_failed = self.failed_views.take(qname.as_ref()).is_some();
        let getter = self
            .inner
            .get_mut()?
            .get_noria_view(&qname, view_failed)
            .await?;

        let res = do_read(
            getter,
            processed_query_params.as_ref(),
            params,
            ticket,
            self.read_behavior,
            self.read_request_handler.as_mut(),
            event,
            self.dialect,
        )
        .await;

        if let Err(e) = res.as_ref() {
            if e.is_networking_related() || e.caused_by_view_destroyed() {
                self.failed_views.insert(qname.into_owned());
            }
        }

        res
    }

    pub(crate) async fn handle_create_view<'a>(
        &'a mut self,
        q: &nom_sql::CreateViewStatement,
    ) -> ReadySetResult<QueryResult<'a>> {
        // TODO(malte): we should perhaps check our usual caches here, rather than just blindly
        // doing a migration on ReadySet every time. On the other hand, CREATE VIEW is rare...

        let changelist = ChangeList::from_change(Change::CreateView(q.clone()), self.dialect)
            .with_schema_search_path(self.schema_search_path.clone());

        noria_await!(
            self.inner.get_mut()?,
            self.inner.get_mut()?.noria.extend_recipe(changelist)
        )?;
        Ok(QueryResult::Empty)
    }

    /// Requests a view for the query from the controller. Invalidates the current entry in the view
    /// cache, regardless of whether the view is marked as failed. Optionally creates a new
    /// cache for the query.
    pub async fn update_view_cache(
        &mut self,
        statement: &nom_sql::SelectStatement,
        override_schema_search_path: Option<Vec<SqlIdentifier>>,
        create_if_not_exists: bool,
        is_prepared: bool,
    ) -> ReadySetResult<()> {
        let qname = self
            .get_view_name(
                statement,
                is_prepared,
                create_if_not_exists,
                override_schema_search_path,
            )
            .await?;

        // Remove the view from failed_views if present, and request the view from the controller.
        self.failed_views.remove(&qname);
        self.inner.get_mut()?.get_noria_view(&qname, true).await?;
        Ok(())
    }
}

/// Creates keys from processed query params, gets the select statement binops, and calls
/// View::build_view_query.
fn build_view_query<'a>(
    getter: &'a mut View,
    processed_query_params: &ProcessedQueryParams,
    params: &[DfValue],
    ticket: Option<Timestamp>,
    read_behavior: ReadBehavior,
    dialect: Dialect,
) -> ReadySetResult<Option<(&'a mut ReaderHandle, ViewQuery)>> {
    let (limit, offset) = processed_query_params.limit_offset_params(params)?;
    let raw_keys = processed_query_params.make_keys(params)?;

    getter.build_view_query(
        raw_keys,
        limit,
        offset,
        ticket,
        read_behavior.is_blocking(),
        dialect,
    )
}

/// Run the supplied [`SelectStatement`] on the supplied [`View`]
/// Assumption: the [`View`] was created for that specific [`SelectStatement`]
#[allow(clippy::needless_lifetimes)] // clippy erroneously thinks the timelife can be elided
#[allow(clippy::too_many_arguments)]
async fn do_read<'a>(
    getter: &'a mut View,
    processed_query_params: &ProcessedQueryParams,
    params: &[DfValue],
    ticket: Option<Timestamp>,
    read_behavior: ReadBehavior,
    read_request_handler: Option<&'a mut ReadRequestHandler>,
    event: &mut readyset_client_metrics::QueryExecutionEvent,
    dialect: Dialect,
) -> ReadySetResult<QueryResult<'a>> {
    let (reader_handle, vq) = match build_view_query(
        getter,
        processed_query_params,
        params,
        ticket,
        read_behavior,
        dialect,
    )? {
        Some(res) => res,
        None => return Err(ReadySetError::NoCacheForQuery),
    };

    event.num_keys = Some(vq.key_comparisons.len() as _);

    let data = if let Some(rh) = read_request_handler {
        let request = readyset_client::Tagged::from(ReadQuery::Normal {
            target: ReaderAddress {
                node: *reader_handle.node(),
                name: reader_handle.name().clone(),
                shard: 0,
            },
            query: vq.clone(),
        });

        // Query the local reader if it is a read query, otherwise default to the traditional
        // View API.
        let tag = request.tag;
        if let ReadQuery::Normal { target, query } = request.v {
            // Issue a normal read query returning the raw unserialized results.
            let result = match rh.handle_normal_read_query(tag, target, query, true) {
                CallResult::Immediate(result) => result?,
                CallResult::Async(chan) => chan.await?,
            };

            result
                .v
                .into_normal()
                .ok_or_else(|| internal_err!("Unexpected response type from reader service"))??
                .into_results()
                .ok_or(ReadySetError::ReaderMissingKey)?
                .pop()
                .ok_or_else(|| internal_err!("Expected a single result set for local reader"))?
                .into_unserialized()
                .expect("Requested raw result")
        } else {
            reader_handle.raw_lookup(vq).await?
        }
    } else {
        reader_handle.raw_lookup(vq).await?
    };

    event.cache_misses = data.total_stats().map(|s| s.cache_misses);

    trace!("select::complete");

    Ok(QueryResult::from_iter(
        SelectSchema {
            schema: Cow::Borrowed(
                reader_handle
                    .schema()
                    .unwrap()
                    .schema(SchemaType::ReturnedSchema),
            ), /* Safe because we already unwrapped above */
            columns: Cow::Borrowed(reader_handle.columns()),
        },
        data,
    ))
}
