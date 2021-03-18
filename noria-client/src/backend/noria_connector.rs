use noria::{
    consistency::Timestamp, ControllerHandle, DataType, Table, TableOperation, View, ViewQuery,
    ViewQueryFilter, ViewQueryOperator, ZookeeperAuthority,
};

use msql_srv::{self, *};
use nom_sql::{
    self, BinaryOperator, ColumnConstraint, InsertStatement, Literal, SelectStatement,
    UpdateStatement,
};
use vec1::vec1;

use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::convert::{TryFrom, TryInto};
use std::sync::atomic;
use std::sync::{Arc, RwLock};

use crate::convert::ToDataType;
use crate::rewrite;
use crate::schema::{self, schema_for_column, Schema};
use crate::utils;

use crate::backend::error::Error;
use crate::backend::error::Error::{
    MissingPreparedStatement, NoriaRecipeError, NoriaWriteError, UnimplementedError,
    UnsupportedError,
};
use crate::backend::SelectSchema;
use itertools::Itertools;
use noria::results::Results;
use std::fmt;

type StatementID = u32;

#[derive(Clone)]
pub enum PreparedStatement {
    Select {
        name: String,
        statement: nom_sql::SelectStatement,
        schema: Vec<msql_srv::Column>,
        key_column_indices: Vec<usize>,
        rewritten_columns: Option<(usize, usize)>,
    },
    Insert(nom_sql::InsertStatement),
    Update(nom_sql::UpdateStatement),
}

impl fmt::Debug for PreparedStatement {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match self {
            PreparedStatement::Select {
                name, statement, ..
            } => write!(f, "{}: {}", name, statement),
            PreparedStatement::Insert(s) => write!(f, "{}", s),
            PreparedStatement::Update(s) => write!(f, "{}", s),
        }
    }
}

pub struct NoriaBackendInner {
    noria: ControllerHandle<ZookeeperAuthority>,
    inputs: BTreeMap<String, Table>,
    outputs: BTreeMap<String, View>,
}

macro_rules! noria_await {
    ($self:expr, $fut:expr) => {{
        let noria = &mut $self.noria;

        futures_util::future::poll_fn(|cx| noria.poll_ready(cx)).await?;
        $fut.await
    }};
}

impl NoriaBackendInner {
    async fn new(mut ch: ControllerHandle<ZookeeperAuthority>) -> Self {
        ch.ready().await.unwrap();
        let inputs = ch.inputs().await.expect("couldn't get inputs from Noria");
        let mut i = BTreeMap::new();
        for (n, _) in inputs {
            ch.ready().await.unwrap();
            let t = ch.table(&n).await.unwrap();
            i.insert(n, t);
        }
        ch.ready().await.unwrap();
        let outputs = ch.outputs().await.expect("couldn't get outputs from Noria");
        let mut o = BTreeMap::new();
        for (n, _) in outputs {
            ch.ready().await.unwrap();
            let t = ch.view(&n).await.unwrap();
            o.insert(n, t);
        }
        NoriaBackendInner {
            inputs: i,
            outputs: o,
            noria: ch,
        }
    }

    async fn ensure_mutator<'a, 'b>(&'a mut self, table: &'b str) -> &'a mut Table {
        self.get_or_make_mutator(table)
            .await
            .expect(&format!("no table named '{}'!", table))
    }

    async fn ensure_getter<'a, 'b>(&'a mut self, view: &'b str) -> &'a mut View {
        self.get_or_make_getter(view)
            .await
            .expect(&format!("no view named '{}'!", view))
    }

    async fn get_or_make_mutator<'a, 'b>(
        &'a mut self,
        table: &'b str,
    ) -> Result<&'a mut Table, anyhow::Error> {
        if !self.inputs.contains_key(table) {
            let t = noria_await!(self, self.noria.table(table))?;
            self.inputs.insert(table.to_owned(), t);
        }
        Ok(self.inputs.get_mut(table).unwrap())
    }

    async fn get_or_make_getter<'a, 'b>(
        &'a mut self,
        view: &'b str,
    ) -> Result<&'a mut View, anyhow::Error> {
        if !self.outputs.contains_key(view) {
            let vh = noria_await!(self, self.noria.view(view))?;
            self.outputs.insert(view.to_owned(), vh);
        }
        Ok(self.outputs.get_mut(view).unwrap())
    }
}

pub struct NoriaConnector {
    inner: NoriaBackendInner,
    auto_increments: Arc<RwLock<HashMap<String, atomic::AtomicUsize>>>,
    /// global cache of view endpoints and prepared statements
    cached: Arc<RwLock<HashMap<SelectStatement, String>>>,
    /// thread-local version of `cached` (consulted first)
    tl_cached: HashMap<SelectStatement, String>,
    prepared_statement_cache: HashMap<StatementID, PreparedStatement>,
}

impl NoriaConnector {
    pub async fn new(
        ch: ControllerHandle<ZookeeperAuthority>,
        auto_increments: Arc<RwLock<HashMap<String, atomic::AtomicUsize>>>,
        query_cache: Arc<RwLock<HashMap<SelectStatement, String>>>,
    ) -> Self {
        NoriaConnector {
            inner: NoriaBackendInner::new(ch).await,
            auto_increments,
            cached: query_cache,
            tl_cached: HashMap::new(),
            prepared_statement_cache: HashMap::new(),
        }
    }

    pub async fn handle_insert(
        &mut self,
        mut q: nom_sql::InsertStatement,
    ) -> std::result::Result<(u64, u64), Error> {
        let table = &q.table.name;

        // create a mutator if we don't have one for this table already
        trace!(%table, "query::insert::access mutator");
        let putter = self.inner.ensure_mutator(table).await;
        trace!("query::insert::extract schema");
        let schema = putter
            .schema()
            .expect(&format!("no schema for table '{}'", table));

        // set column names (insert schema) if not set
        if q.fields.is_none() {
            q.fields = Some(schema.fields.iter().map(|cs| cs.column.clone()).collect());
        }

        let data: Vec<Vec<DataType>> = q
            .data
            .iter()
            .map(|row| row.iter().map(DataType::from).collect())
            .collect();

        self.do_insert(&q, data).await
    }

    pub async fn prepare_insert(
        &mut self,
        mut sql_q: nom_sql::SqlQuery,
        statement_id: u32,
    ) -> std::result::Result<(u32, Vec<msql_srv::Column>, Vec<Column>), Error> {
        let q = if let nom_sql::SqlQuery::Insert(ref q) = sql_q {
            q
        } else {
            unreachable!()
        };

        trace!(table = %q.table.name, "insert::access mutator");
        let mutator = self.inner.ensure_mutator(&q.table.name).await;
        trace!("insert::extract schema");
        let schema = schema::convert_schema(&Schema::Table(mutator.schema().unwrap().clone()));

        match sql_q {
            // set column names (insert schema) if not set
            nom_sql::SqlQuery::Insert(ref mut q) => {
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
            }
            _ => (),
        }

        let params: Vec<_> = {
            // extract parameter columns -- easy here, since they must all be in the same table
            let param_cols = utils::get_parameter_columns(&sql_q);
            param_cols
                .into_iter()
                .map(|c| {
                    //let mut cc = c.clone();
                    //cc.table = Some(q.table.name.clone());
                    //schema_for_column(table_schemas, &cc)
                    schema
                        .iter()
                        .cloned()
                        .find(|mc| c.name == mc.column)
                        .expect(&format!("column '{}' missing in mutator schema", c))
                })
                .collect()
        };

        // nothing more to do for an insert
        // register a new prepared statement
        let q = if let nom_sql::SqlQuery::Insert(q) = sql_q {
            q
        } else {
            unreachable!()
        };
        trace!(id = statement_id, "insert::registered");
        self.prepared_statement_cache
            .insert(statement_id, PreparedStatement::Insert(q));
        Ok((statement_id, params, schema))
    }

    pub(crate) async fn execute_prepared_insert(
        &mut self,
        q_id: u32,
        params: Vec<DataType>,
    ) -> std::result::Result<(u64, u64), Error> {
        let prep: PreparedStatement = self
            .prepared_statement_cache
            .get(&q_id)
            .ok_or(MissingPreparedStatement)?
            .clone();
        trace!("delegate");
        match prep {
            PreparedStatement::Insert(ref q) => {
                return self.do_insert(&q, vec![params]).await;
            }
            _ => {
                unreachable!(
                    "Execute_prepared_insert is being called for a non insert prepared statement."
                );
            }
        };
    }

    pub(crate) async fn handle_delete(
        &mut self,
        q: nom_sql::DeleteStatement,
    ) -> std::result::Result<u64, Error> {
        let cond = q
            .where_clause
            .expect("only supports DELETEs with WHERE-clauses");

        // create a mutator if we don't have one for this table already
        trace!(table = %q.table.name, "delete::access mutator");
        let mutator = self.inner.ensure_mutator(&q.table.name).await;

        trace!("delete::extract schema");
        let pkey = if let Some(cts) = mutator.schema() {
            utils::get_primary_key(cts)
                .into_iter()
                .map(|(_, c)| c)
                .collect()
        } else {
            unimplemented!("cannot delete from view");
        };

        trace!("delete::flatten conditionals");
        match utils::flatten_conditional(&cond, &pkey) {
            None => Ok(0 as u64),
            Some(ref flattened) if flattened.is_empty() => Err(UnimplementedError(
                "DELETE only supports WHERE-clauses on primary keys"
                    .parse()
                    .unwrap(),
            )),
            Some(flattened) => {
                let count = flattened.len() as u64;
                trace!("delete::execute");
                for key in flattened {
                    if let Err(e) = mutator.delete(key).await {
                        error!(error = %e, "failed");
                        return Err(NoriaWriteError(e));
                    };
                }
                trace!("delete::done");
                Ok(count)
            }
        }
    }

    pub(crate) async fn handle_update(
        &mut self,
        q: nom_sql::UpdateStatement,
    ) -> std::result::Result<(u64, u64), Error> {
        self.do_update(Cow::Owned(q), None).await
    }

    pub(crate) async fn prepare_update(
        &mut self,
        sql_q: nom_sql::SqlQuery,
        statement_id: u32,
    ) -> std::result::Result<(u64, Vec<Column>), Error> {
        // ensure that we have schemas and endpoints for the query
        let q = if let nom_sql::SqlQuery::Update(ref q) = sql_q {
            q
        } else {
            unreachable!()
        };

        trace!(table = %q.table.name, "update::access mutator");
        let mutator = self.inner.ensure_mutator(&q.table.name).await;
        trace!("update::extract schema");
        let schema = Schema::Table(mutator.schema().unwrap().clone());

        // extract parameter columns
        let params: Vec<msql_srv::Column> = {
            utils::get_parameter_columns(&sql_q)
                .into_iter()
                .map(|c| schema_for_column(&schema, c))
                .collect()
        };

        // must have an update query
        let q = if let nom_sql::SqlQuery::Update(q) = sql_q {
            q
        } else {
            unreachable!();
        };

        trace!(id = statement_id, "update::registered");
        self.prepared_statement_cache
            .insert(statement_id, PreparedStatement::Update(q));
        Ok((statement_id as u64, params))
    }

    pub(crate) async fn execute_prepared_update(
        &mut self,
        q_id: u32,
        params: Vec<DataType>,
    ) -> std::result::Result<(u64, u64), Error> {
        let prep: PreparedStatement = self
            .prepared_statement_cache
            .get(&q_id)
            .ok_or(MissingPreparedStatement)?
            .clone();

        trace!("delegate");
        match prep {
            PreparedStatement::Update(q) => {
                return self.do_update(Cow::Owned(q), Some(params)).await
            }
            _ => unreachable!(),
        };
    }

    pub(crate) async fn handle_create_table(
        &mut self,
        q: nom_sql::CreateTableStatement,
    ) -> std::result::Result<(), Error> {
        // TODO(malte): we should perhaps check our usual caches here, rather than just blindly
        // doing a migration on Noria ever time. On the other hand, CREATE TABLE is rare...
        info!(table = %q.table.name, "table::create");
        noria_await!(
            self.inner,
            self.inner.noria.extend_recipe(&format!("{};", q))
        )?;
        trace!("table::created");
        Ok(())
    }
}

impl NoriaConnector {
    async fn get_or_create_view(
        &mut self,
        q: &nom_sql::SelectStatement,
        prepared: bool,
    ) -> std::result::Result<String, Error> {
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
                        let qh = utils::hash_select_query(q);
                        let qname = format!("q_{:x}", qh);

                        // add the query to Noria
                        if prepared {
                            info!(query = %q, name = %qname, "adding parameterized query");
                        } else {
                            info!(query = %q, name = %qname, "adding ad-hoc query");
                        }
                        if let Err(e) = noria_await!(
                            self.inner,
                            self.inner
                                .noria
                                .extend_recipe(&format!("QUERY {}: {};", qname, q))
                        ) {
                            error!(error = %e, "add query failed");
                            return Err(NoriaRecipeError(e));
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

    async fn do_insert(
        &mut self,
        q: &InsertStatement,
        data: Vec<Vec<DataType>>,
    ) -> std::result::Result<(u64, u64), Error> {
        let table = &q.table.name;

        // create a mutator if we don't have one for this table already
        trace!(%table, "insert::access mutator");
        let putter = self.inner.ensure_mutator(table).await;
        trace!("insert::extract schema");
        let schema = putter
            .schema()
            .expect(&format!("no schema for table '{}'", table));

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
        // can only have zero or one AUTO_INCREMENT columns
        assert!(auto_increment_columns.len() <= 1);

        let ai = &mut self.auto_increments;
        tokio::task::block_in_place(|| {
            let ai_lock = ai.read().unwrap();
            if ai_lock.get(table).is_none() {
                drop(ai_lock);
                ai.write()
                    .unwrap()
                    .entry(table.to_owned())
                    .or_insert(atomic::AtomicUsize::new(0));
            }
        });
        let mut buf = vec![vec![DataType::None; schema.fields.len()]; data.len()];
        let mut first_inserted_id = None;
        tokio::task::block_in_place(|| {
            let ai_lock = ai.read().unwrap();
            let last_insert_id = &ai_lock[table];

            // handle default values
            trace!("insert::default values");
            let mut default_value_columns: Vec<_> = schema
                .fields
                .iter()
                .filter_map(|ref c| {
                    for cc in &c.constraints {
                        if let ColumnConstraint::DefaultValue(ref v) = *cc {
                            return Some((c.column.clone(), v.clone()));
                        }
                    }
                    None
                })
                .collect();

            trace!("insert::construct ops");

            for (ri, ref row) in data.iter().enumerate() {
                if let Some(col) = auto_increment_columns.iter().next() {
                    let idx = schema
                        .fields
                        .iter()
                        .position(|f| f == *col)
                        .expect(&format!("no column named '{}'", col.column.name));
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
                        .expect(&format!("no column named '{}'", c.name));
                    // only use default value if query doesn't specify one
                    if !columns_specified.contains(&c) {
                        buf[ri][idx] = v.into();
                    }
                }

                for (ci, c) in columns_specified.iter().enumerate() {
                    let (idx, field) = schema
                        .fields
                        .iter()
                        .find_position(|f| f.column == *c)
                        .expect(&format!("no column '{:?}' in table '{}'", c, schema.table));
                    // TODO(grfn): Convert this unwrap() to an actual user error once we have proper
                    // error return values (PR#50)
                    let value = row.get(ci).unwrap().coerce_to(&field.sql_type).unwrap();
                    buf[ri][idx] = value.into_owned();
                }
            }
        });

        let result = if let Some(ref update_fields) = q.on_duplicate {
            trace!("insert::complex");
            assert_eq!(buf.len(), 1);

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
                )
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
        match result {
            Ok(_) => Ok((data.len() as u64, first_inserted_id.unwrap_or(0) as u64)),
            Err(e) => Err(NoriaWriteError(e)),
        }
    }

    async fn do_read(
        &mut self,
        qname: &str,
        q: &nom_sql::SelectStatement,
        mut keys: Vec<Vec<DataType>>,
        schema: &Vec<Column>,
        key_column_indices: &[usize],
        ticket: Option<Timestamp>,
    ) -> std::result::Result<(Vec<Results>, SelectSchema), Error> {
        // create a getter if we don't have one for this query already
        // TODO(malte): may need to make one anyway if the query has changed w.r.t. an
        // earlier one of the same name
        trace!("select::access view");
        let getter = self.inner.ensure_getter(&qname).await;
        let getter_schema = getter.schema().expect("No schema for view");
        let mut key_types = key_column_indices
            .iter()
            .map(|i| &getter_schema[*i].sql_type)
            .collect::<Vec<_>>();
        trace!("select::lookup");
        let bogo = vec![vec1![DataType::from(0i32)].into()];
        let cols = Vec::from(getter.columns());
        let mut binops = utils::get_select_statement_binops(q);
        let mut filter_op_idx = None;
        let filter = binops
            .iter()
            .enumerate()
            .filter_map(|(i, (col, binop))| {
                ViewQueryOperator::try_from(*binop)
                    .ok()
                    .map(|op| (i, col, op))
            })
            .next()
            .map(|(idx, col, operator)| {
                let mut key = keys.drain(0..1).next().expect("Expected at least one key");
                assert!(
                    keys.is_empty(),
                    "LIKE/ILIKE not currently supported for more than one lookup key at a time"
                );
                let column = schema
                    .iter()
                    .position(|x| x.column == col.name)
                    .expect("Filter column not in schema!");
                let value = String::from(
                    &key.remove(idx)
                        .coerce_to(&key_types.remove(idx))
                        .unwrap()
                        .into_owned(),
                );
                if !key.is_empty() {
                    // the LIKE/ILIKE isn't our only key, add the rest back to `keys`
                    keys.push(key);
                }

                filter_op_idx = Some(idx);

                ViewQueryFilter {
                    column,
                    operator,
                    value,
                }
            });

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
                let e = format!("attempted to execute statement with conflicting binary operators {:?} and {:?}", binop_to_use, other);
                return Err(UnsupportedError(e));
            }

            keys.drain(..)
                .map(|mut key| {
                    let k = key
                        .drain(..)
                        .zip(&key_types)
                        .map(|(val, col_type)| {
                            val.coerce_to(col_type)
                                .map(Cow::into_owned)
                                // TODO(grfn): Drop this unwrap once we have real error return
                                // values here (#50)
                                .unwrap()
                        })
                        .collect::<Vec<DataType>>();

                    (k, binop_to_use)
                        .try_into()
                        .expect("Input key cannot be empty")
                })
                .collect()
        };

        let order_by = q.order.as_ref().map(|oc| {
            // TODO(eta): support this. It isn't necessarily hard, just a pain.
            assert_eq!(
                oc.columns.len(),
                1,
                "ORDER BY expressions with more than one column are not supported yet"
            );
            // TODO(eta): figure out whether this error is actually possible
            let col_idx = schema
                .iter()
                .position(|x| x.column == oc.columns[0].0.name)
                .expect("ORDER BY column not in schema!");
            (
                col_idx,
                oc.columns[0].1 == nom_sql::OrderType::OrderDescending,
            )
        });

        let limit = q.limit.as_ref().map(|lc| {
            assert_eq!(lc.offset, 0, "OFFSET is not supported yet");
            // FIXME(eta): this cast is ugly!
            lc.limit as usize
        });

        let vq = ViewQuery {
            key_comparisons: keys,
            block: true,
            order_by,
            limit,
            filter,
            // TODO(andrew): Add a timestamp to views when RYW consistency
            // is specified.
            timestamp: ticket,
        };

        let data = getter.raw_lookup(vq).await?;
        trace!("select::complete");
        let schema = schema.to_vec();
        Ok((
            data,
            SelectSchema {
                use_bogo,
                schema,
                columns: cols,
            },
        ))
    }

    async fn do_update(
        &mut self,
        q: Cow<'_, UpdateStatement>,
        params: Option<Vec<DataType>>,
    ) -> std::result::Result<(u64, u64), Error> {
        trace!(table = %q.table.name, "update::access mutator");
        let mutator = self.inner.ensure_mutator(&q.table.name).await;

        let q = q.into_owned();
        let (key, updates) = {
            trace!("update::extract schema");
            let schema = if let Some(cts) = mutator.schema() {
                cts
            } else {
                // no update on views
                unimplemented!();
            };
            utils::extract_update(q, params.map(|p| p.into_iter()), schema)
        };

        trace!("update::update");
        mutator.update(key, updates).await?;
        trace!("update::complete");
        // TODO: return meaningful fields for (num_rows_updated, last_inserted_id) rather than hardcoded (1,0)
        Ok((1, 0))
    }

    pub(crate) async fn handle_select(
        &mut self,
        q: nom_sql::SelectStatement,
        use_params: Vec<Literal>,
        ticket: Option<Timestamp>,
    ) -> std::result::Result<(Vec<Results>, SelectSchema), Error> {
        trace!("query::select::access view");
        let qname = self.get_or_create_view(&q, false).await?;

        let keys: Vec<_> = use_params
            .into_iter()
            .map(|l| vec1![l.to_datatype()].into())
            .collect();

        // we need the schema for the result writer
        trace!(%qname, "query::select::extract schema");
        let getter_schema = self
            .inner
            .ensure_getter(&qname)
            .await
            .schema()
            .expect(&format!("no schema for view '{}'", qname));

        let schema = schema::convert_schema(&Schema::View(
            getter_schema
                .iter()
                .cloned()
                .filter(|c| c.column.name != "bogokey")
                .collect(),
        ));

        let key_column_indices = utils::select_statement_parameter_columns(&q)
            .into_iter()
            .map(|col| {
                getter_schema
                    .iter()
                    // TODO(grfn): Looking up columns in the resulting view by the name of the
                    // column in the input query is a little iffy - ideally, the getter itself would
                    // be able to tell us the types of the columns and we could skip all of this
                    // nonsense.
                    // https://app.clubhouse.io/readysettech/story/203/add-a-key-types-method-to-view
                    .position(|getter_col| getter_col.column.name == *col.name)
                    .unwrap()
            })
            .collect::<Vec<_>>();

        trace!(%qname, "query::select::do");
        self.do_read(&qname, &q, keys, &schema, &key_column_indices, ticket)
            .await
    }

    pub(crate) async fn prepare_select(
        &mut self,
        mut sql_q: nom_sql::SqlQuery,
        statement_id: u32,
    ) -> std::result::Result<(u32, Vec<msql_srv::Column>, Vec<Column>), Error> {
        // extract parameter columns
        // note that we have to do this *before* collapsing WHERE IN, otherwise the
        // client will be confused about the number of parameters it's supposed to
        // give.
        let param_columns: Vec<nom_sql::Column> = utils::get_parameter_columns(&sql_q)
            .into_iter()
            .cloned()
            .collect();

        trace!("select::collapse where-in clauses");
        let rewritten = rewrite::collapse_where_in(&mut sql_q, false);
        let q = if let nom_sql::SqlQuery::Select(q) = sql_q {
            q
        } else {
            unreachable!();
        };

        // check if we already have this query prepared
        trace!("select::access view");
        let qname = self.get_or_create_view(&q, true).await?;

        // extract result schema
        trace!(qname = %qname, "select::extract schema");
        let getter_schema = self
            .inner
            .ensure_getter(&qname)
            .await
            .schema()
            .expect(&format!("no schema for view '{}'", qname));

        let schema = Schema::View(
            getter_schema
                .iter()
                .cloned()
                .filter(|c| c.column.name != "bogokey")
                .collect(),
        );

        let key_column_indices = param_columns
            .iter()
            .map(|col| {
                getter_schema
                    .iter()
                    // TODO: https://app.clubhouse.io/readysettech/story/203/add-a-key-types-method-to-view
                    .position(|getter_col| getter_col.column.name == *col.name)
                    .unwrap()
            })
            .collect::<Vec<_>>();

        // now convert params to msql_srv types; we have to do this here because we don't have
        // access to the schema yet when we extract them above.
        let params: Vec<msql_srv::Column> = param_columns
            .into_iter()
            .map(|mut c| {
                c.table = Some(qname.clone());
                schema_for_column(&schema, &c)
            })
            .collect();
        let schema = schema::convert_schema(&schema);
        let select_schema = schema.clone();
        trace!(id = statement_id, "select::registered");
        let ps = PreparedStatement::Select {
            name: qname,
            statement: q,
            schema: select_schema,
            key_column_indices,
            rewritten_columns: rewritten.map(|(a, b)| (a, b.len())),
        };
        self.prepared_statement_cache.insert(statement_id, ps);
        Ok((statement_id, params, schema))
    }

    pub(crate) async fn execute_prepared_select(
        &mut self,
        q_id: u32,
        params: Vec<DataType>,
        ticket: Option<Timestamp>,
    ) -> std::result::Result<(Vec<Results>, SelectSchema), Error> {
        let prep: PreparedStatement = {
            match self.prepared_statement_cache.get(&q_id) {
                Some(e) => e.clone(),
                None => {
                    return Err(MissingPreparedStatement);
                }
            }
        };

        match &prep {
            PreparedStatement::Select {
                name,
                statement: q,
                schema,
                key_column_indices,
                rewritten_columns: rewritten,
            } => {
                trace!("apply where-in rewrites");
                let keys = match rewritten {
                    Some((first_rewritten, nrewritten)) => {
                        // this is a little tricky
                        // the user is giving us some params [a, b, c, d]
                        // for the query WHERE x = ? AND y IN (?, ?) AND z = ?
                        // that we rewrote to WHERE x = ? AND y = ? AND z = ?
                        // so we need to turn that into the keys:
                        // [[a, b, d], [a, c, d]]
                        assert_ne!(
                            params.len(),
                            0,
                            "empty parameters passed to a rewritten query"
                        );
                        (0..*nrewritten)
                            .map(|poffset| {
                                params
                                    .iter()
                                    .take(*first_rewritten)
                                    .chain(params.iter().skip(first_rewritten + poffset).take(1))
                                    .chain(params.iter().skip(first_rewritten + nrewritten))
                                    .cloned()
                                    .collect()
                            })
                            .collect()
                    }
                    None => {
                        if params.len() > 0 {
                            vec![params]
                        } else {
                            vec![]
                        }
                    }
                };

                return self
                    .do_read(name, q, keys, schema, key_column_indices, ticket)
                    .await;
            }
            _ => {
                unreachable!()
            }
        };
    }

    pub(crate) async fn handle_create_view(
        &mut self,
        q: nom_sql::CreateViewStatement,
    ) -> std::result::Result<(), Error> {
        // TODO(malte): we should perhaps check our usual caches here, rather than just blindly
        // doing a migration on Noria every time. On the other hand, CREATE VIEW is rare...

        info!(%q.definition, %q.name, "view::create");

        noria_await!(
            self.inner,
            self.inner
                .noria
                .extend_recipe(&format!("VIEW {}: {};", q.name, q.definition))
        )?;

        Ok(())
    }
}
