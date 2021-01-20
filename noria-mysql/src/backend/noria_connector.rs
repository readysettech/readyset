use noria::{
    ControllerHandle, DataType, Table, TableOperation, View, ViewQuery, ZookeeperAuthority,
};

use anyhow;
use futures_executor::block_on as block_on_buffer;
use msql_srv::{self, *};
use nom_sql::{
    self, BinaryOperator, ColumnConstraint, InsertStatement, Literal, SelectStatement,
    UpdateStatement,
};
use vec1::vec1;

use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::convert::TryInto;
use std::io;
use std::sync::atomic;
use std::sync::{Arc, RwLock};

use crate::convert::ToDataType;
use crate::rewrite;
use crate::schema::{self, schema_for_column, Schema};
use crate::utils;

use crate::backend::reader::Reader;
use crate::backend::writer::Writer;
use crate::backend::PreparedStatement;
use itertools::Itertools;

pub struct NoriaBackendInner {
    executor: tokio::runtime::Handle,
    noria: ControllerHandle<ZookeeperAuthority>,
    inputs: BTreeMap<String, Table>,
    outputs: BTreeMap<String, View>,
}

macro_rules! block_on {
    ($self:expr, $fut:expr) => {{
        let noria = &mut $self.noria;
        futures_executor::block_on(futures_util::future::poll_fn(|cx| noria.poll_ready(cx)))
            .unwrap();
        futures_executor::block_on($self.executor.spawn($fut)).unwrap()
    }};
}

impl NoriaBackendInner {
    async fn new(ex: tokio::runtime::Handle, mut ch: ControllerHandle<ZookeeperAuthority>) -> Self {
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
            executor: ex,
            inputs: i,
            outputs: o,
            noria: ch,
        }
    }

    fn ensure_mutator<'a, 'b>(&'a mut self, table: &'b str) -> &'a mut Table {
        self.get_or_make_mutator(table)
            .expect(&format!("no table named '{}'!", table))
    }

    fn ensure_getter<'a, 'b>(&'a mut self, view: &'b str) -> &'a mut View {
        self.get_or_make_getter(view)
            .expect(&format!("no view named '{}'!", view))
    }

    fn get_or_make_mutator<'a, 'b>(
        &'a mut self,
        table: &'b str,
    ) -> Result<&'a mut Table, anyhow::Error> {
        if !self.inputs.contains_key(table) {
            let t = block_on!(self, self.noria.table(table))?;
            self.inputs.insert(table.to_owned(), t);
        }
        Ok(self.inputs.get_mut(table).unwrap())
    }

    fn get_or_make_getter<'a, 'b>(
        &'a mut self,
        view: &'b str,
    ) -> Result<&'a mut View, anyhow::Error> {
        if !self.outputs.contains_key(view) {
            let vh = block_on!(self, self.noria.view(view))?;
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
}

impl NoriaConnector {
    pub async fn new(
        ex: tokio::runtime::Handle,
        ch: ControllerHandle<ZookeeperAuthority>,
        auto_increments: Arc<RwLock<HashMap<String, atomic::AtomicUsize>>>,
        query_cache: Arc<RwLock<HashMap<SelectStatement, String>>>,
    ) -> Self {
        NoriaConnector {
            inner: NoriaBackendInner::new(ex, ch).await,
            auto_increments,
            cached: query_cache,
            tl_cached: HashMap::new(),
        }
    }
}

impl<W: io::Write> Writer<W> for NoriaConnector {
    fn handle_create_table(
        &mut self,
        q: nom_sql::CreateTableStatement,
        results: QueryResultWriter<W>,
    ) -> io::Result<()> {
        // TODO(malte): we should perhaps check our usual caches here, rather than just blindly
        // doing a migration on Noria ever time. On the other hand, CREATE TABLE is rare...

        info!(table = %q.table.name, "table::create");
        match block_on!(
            self.inner,
            self.inner.noria.extend_recipe(&format!("{};", q))
        ) {
            Ok(_) => {
                // no rows to return
                // TODO(malte): potentially eagerly cache the mutator for this table
                trace!("table::created");
                results.completed(0, 0)
            }
            Err(e) => Err(io::Error::new(io::ErrorKind::Other, e)),
        }
    }

    fn handle_create_view(
        &mut self,
        q: nom_sql::CreateViewStatement,
        results: QueryResultWriter<W>,
    ) -> io::Result<()> {
        // TODO(malte): we should perhaps check our usual caches here, rather than just blindly
        // doing a migration on Noria every time. On the other hand, CREATE VIEW is rare...

        info!(%q.definition, %q.name, "view::create");
        match block_on!(
            self.inner,
            self.inner
                .noria
                .extend_recipe(&format!("VIEW {}: {};", q.name, q.definition))
        ) {
            Ok(_) => {
                // no rows to return
                trace!("view::created");
                results.completed(0, 0)
            }
            Err(e) => Err(io::Error::new(io::ErrorKind::Other, e)),
        }
    }

    fn handle_delete(
        &mut self,
        q: nom_sql::DeleteStatement,
        results: QueryResultWriter<W>,
    ) -> io::Result<()> {
        let cond = q
            .where_clause
            .expect("only supports DELETEs with WHERE-clauses");

        // create a mutator if we don't have one for this table already
        trace!(table = %q.table.name, "delete::access mutator");
        let mutator = self.inner.ensure_mutator(&q.table.name);

        trace!("delete::extract schema");
        let pkey = if let Some(cts) = mutator.schema() {
            utils::get_primary_key(cts)
                .into_iter()
                .map(|(_, c)| c)
                .collect()
        } else {
            // cannot delete from view
            unimplemented!();
        };

        trace!("delete::flatten conditionals");
        match utils::flatten_conditional(&cond, &pkey) {
            None => results.completed(0, 0),
            Some(ref flattened) if flattened.len() == 0 => {
                panic!("DELETE only supports WHERE-clauses on primary keys");
            }
            Some(flattened) => {
                let count = flattened.len() as u64;
                trace!("delete::execute");
                for key in flattened {
                    match block_on_buffer(mutator.delete(key)) {
                        Ok(_) => {}
                        Err(e) => {
                            error!(error = %e, "failed");
                            return results.error(
                                msql_srv::ErrorKind::ER_UNKNOWN_ERROR,
                                format!("{:?}", e).as_bytes(),
                            );
                        }
                    };
                }

                trace!("delete::done");
                results.completed(count, 0)
            }
        }
    }

    fn handle_insert(
        &mut self,
        mut q: nom_sql::InsertStatement,
        results: QueryResultWriter<W>,
    ) -> io::Result<()> {
        let table = &q.table.name;

        // create a mutator if we don't have one for this table already
        trace!(%table, "query::insert::access mutator");
        let putter = self.inner.ensure_mutator(table);
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
            .map(|row| row.iter().map(|v| DataType::from(v)).collect())
            .collect();

        self.do_insert(&q, data, results)
    }

    fn handle_set(
        &mut self,
        q: nom_sql::SetStatement,
        results: QueryResultWriter<W>,
    ) -> io::Result<()> {
        trace!(%q.variable, "set");
        // ignore
        // todo https://app.clubhouse.io/readysettech/story/107/currently-ignoring-set-in-noria-mysql-adaptor
        results.completed(0, 0)
    }

    fn handle_update(
        &mut self,
        q: nom_sql::UpdateStatement,
        results: QueryResultWriter<W>,
    ) -> io::Result<()> {
        self.do_update(Cow::Owned(q), None, results)
    }

    fn prepare_insert(
        &mut self,
        mut sql_q: nom_sql::SqlQuery,
        info: StatementMetaWriter<W>,
        statement_id: u32,
    ) -> io::Result<PreparedStatement> {
        let q = if let nom_sql::SqlQuery::Insert(ref q) = sql_q {
            q
        } else {
            unreachable!()
        };

        trace!(table = %q.table.name, "insert::access mutator");
        let mutator = self.inner.ensure_mutator(&q.table.name);
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
        info.reply(statement_id, params.as_slice(), schema.as_slice())?;

        // register a new prepared statement
        let q = if let nom_sql::SqlQuery::Insert(q) = sql_q {
            q
        } else {
            unreachable!()
        };
        trace!(id = statement_id, "insert::registered");
        Ok(PreparedStatement::Insert(q))
    }

    fn execute_update(
        &mut self,
        q: &UpdateStatement,
        params: ParamParser,
        results: QueryResultWriter<W>,
    ) -> io::Result<()> {
        self.do_update(Cow::Borrowed(q), Some(params), results)
    }

    fn prepare_update(
        &mut self,
        sql_q: nom_sql::SqlQuery,
        info: StatementMetaWriter<W>,
        statement_id: u32,
    ) -> io::Result<PreparedStatement> {
        // ensure that we have schemas and endpoints for the query
        let q = if let nom_sql::SqlQuery::Update(ref q) = sql_q {
            q
        } else {
            unreachable!()
        };

        trace!(table = %q.table.name, "update::access mutator");
        let mutator = self.inner.ensure_mutator(&q.table.name);
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

        info.reply(statement_id, &params[..], &[]);
        Ok(PreparedStatement::Update(q))
    }

    fn execute_insert(
        &mut self,
        q: &InsertStatement,
        data: Vec<DataType>,
        results: QueryResultWriter<W>,
    ) -> io::Result<()> {
        assert_eq!(q.fields.as_ref().unwrap().len(), data.len());
        self.do_insert(&q, vec![data], results)
    }
}

impl<W: io::Write> Reader<W> for NoriaConnector {
    fn handle_select(
        &mut self,
        q: nom_sql::SelectStatement,
        use_params: Vec<Literal>,
        results: QueryResultWriter<W>,
    ) -> io::Result<()> {
        trace!("query::select::access view");
        let qname = match self.get_or_create_view(&q, false) {
            Ok(qn) => qn,
            Err(e) => {
                error!(error = ?e, "failed to parse");
                if e.kind() == io::ErrorKind::Other {
                    // maybe ER_SYNTAX_ERROR ?
                    // would be good to narrow these down
                    // TODO: why are the actual error contents never printed?
                    return results.error(
                        msql_srv::ErrorKind::ER_UNKNOWN_ERROR,
                        e.to_string().as_bytes(),
                    );
                } else {
                    return Err(e);
                }
            }
        };

        let keys: Vec<_> = use_params
            .into_iter()
            .map(|l| vec1![l.to_datatype()].into())
            .collect();

        // we need the schema for the result writer
        trace!(%qname, "query::select::extract schema");
        let schema = schema::convert_schema(&Schema::View(
            self.inner
                .ensure_getter(&qname)
                .schema()
                .expect(&format!("no schema for view '{}'", qname))
                .iter()
                .cloned()
                .filter(|c| c.column.name != "bogokey")
                .collect(),
        ));

        trace!(%qname, "query::select::do");
        self.do_read(&qname, &q, keys, schema.as_slice(), results)
    }

    fn prepare_select(
        &mut self,
        mut sql_q: nom_sql::SqlQuery,
        info: StatementMetaWriter<W>,
        statement_id: u32,
    ) -> io::Result<PreparedStatement> {
        // extract parameter columns
        // note that we have to do this *before* collapsing WHERE IN, otherwise the
        // client will be confused about the number of parameters it's supposed to
        // give.
        let params: Vec<nom_sql::Column> = utils::get_parameter_columns(&sql_q)
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
        let qname = self.get_or_create_view(&q, true)?;

        // extract result schema
        trace!(qname = %qname, "select::extract schema");
        let schema = Schema::View(
            self.inner
                .ensure_getter(&qname)
                .schema()
                .expect(&format!("no schema for view '{}'", qname))
                .iter()
                .cloned()
                .filter(|c| c.column.name != "bogokey")
                .collect(),
        );

        // now convert params to msql_srv types; we have to do this here because we don't have
        // access to the schema yet when we extract them above.
        let params: Vec<msql_srv::Column> = params
            .into_iter()
            .map(|mut c| {
                c.table = Some(qname.clone());
                schema_for_column(&schema, &c)
            })
            .collect();
        let schema = schema::convert_schema(&schema);

        info.reply(statement_id, params.as_slice(), schema.as_slice())?;
        trace!(id = statement_id, "select::registered");
        Ok(PreparedStatement::Select(
            qname,
            q,
            schema,
            rewritten.map(|(a, b)| (a, b.len())),
        ))
    }

    fn execute_select(
        &mut self,
        qname: &str,
        q: &nom_sql::SelectStatement,
        keys: Vec<Vec<DataType>>,
        schema: &[msql_srv::Column],
        results: QueryResultWriter<W>,
    ) -> io::Result<()> {
        self.do_read(qname, q, keys, schema, results)
    }
}
impl NoriaConnector {
    fn get_or_create_view(
        &mut self,
        q: &nom_sql::SelectStatement,
        prepared: bool,
    ) -> io::Result<String> {
        let qname = match self.tl_cached.get(q) {
            None => {
                // check global cache
                let gc = self.cached.read().unwrap();
                let qname = match gc.get(q) {
                    Some(qname) => qname.clone(),
                    None => {
                        drop(gc);
                        let mut gc = self.cached.write().unwrap();
                        if let Some(qname) = gc.get(q) {
                            qname.clone()
                        } else {
                            let qh = utils::hash_select_query(q);
                            let qname = format!("q_{:x}", qh);

                            // add the query to Noria
                            if prepared {
                                info!(query = %q, name = %qname, "adding parameterized query");
                            } else {
                                info!(query = %q, name = %qname, "adding ad-hoc query");
                            }
                            // HACK(eta): unless a TopK operator would be made, null out the ORDER
                            // BY clause in order to get around the issue PR #11 was trying to fix
                            let q_noria = if q.order.is_some() && !q.limit.is_some() {
                                let mut q_hack = q.clone();
                                q_hack.order = None;
                                q_hack
                            } else {
                                q.clone()
                            };
                            if let Err(e) = block_on!(
                                self.inner,
                                self.inner
                                    .noria
                                    .extend_recipe(&format!("QUERY {}: {};", qname, q_noria))
                            ) {
                                error!(error = %e, "add query failed");
                                return Err(io::Error::new(io::ErrorKind::Other, e));
                            }

                            gc.insert(q.clone(), qname.clone());
                            qname
                        }
                    }
                };

                self.tl_cached.insert(q.clone(), qname.clone());

                qname
            }
            Some(qname) => qname.to_owned(),
        };
        Ok(qname)
    }

    fn do_insert<W: io::Write>(
        &mut self,
        q: &InsertStatement,
        data: Vec<Vec<DataType>>,
        results: QueryResultWriter<W>,
    ) -> io::Result<()> {
        let table = &q.table.name;

        // create a mutator if we don't have one for this table already
        trace!(%table, "insert::access mutator");
        let putter = self.inner.ensure_mutator(table);
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

        {
            let ai_lock = self.auto_increments.read().unwrap();
            if ai_lock.get(table).is_none() {
                drop(ai_lock);
                self.auto_increments
                    .write()
                    .unwrap()
                    .entry(table.to_owned())
                    .or_insert(atomic::AtomicUsize::new(0));
            }
        };
        let ai_lock = self.auto_increments.read().unwrap();
        let last_insert_id = &ai_lock[table];

        // handle default values
        trace!("insert::default values");
        let mut default_value_columns: Vec<_> = schema
            .fields
            .iter()
            .filter_map(|ref c| {
                for cc in &c.constraints {
                    match *cc {
                        ColumnConstraint::DefaultValue(ref v) => {
                            return Some((c.column.clone(), v.clone()))
                        }
                        _ => (),
                    }
                }
                None
            })
            .collect();

        trace!("insert::construct ops");
        let mut buf = vec![vec![DataType::None; schema.fields.len()]; data.len()];

        let mut first_inserted_id = None;
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
                let idx = schema
                    .fields
                    .iter()
                    .position(|f| f.column == *c)
                    .expect(&format!("no column '{:?}' in table '{}'", c, schema.table));
                buf[ri][idx] = row.get(ci).unwrap().clone();
            }
        }

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
                utils::extract_update_params_and_fields(&mut uq, &mut None, schema)
            };

            // TODO(malte): why can't I consume buf here?
            let r = block_on_buffer(putter.insert_or_update(buf[0].clone(), updates));
            trace!("insert::complex::complete");
            r
        } else {
            trace!("insert::simple");
            let buf: Vec<_> = buf
                .into_iter()
                .map(|r| TableOperation::Insert(r.into()))
                .collect();
            let r = block_on_buffer(putter.perform_all(buf));
            trace!("insert::simple::complete");
            r
        };

        match result {
            Ok(_) => results.completed(data.len() as u64, first_inserted_id.unwrap_or(0) as u64),
            Err(e) => {
                error!(error = %e, "failed");
                results.error(
                    msql_srv::ErrorKind::ER_UNKNOWN_ERROR,
                    format!("{:?}", e).as_bytes(),
                )
            }
        }
    }

    fn do_read<W: io::Write>(
        &mut self,
        qname: &str,
        q: &nom_sql::SelectStatement,
        mut keys: Vec<Vec<DataType>>,
        schema: &[msql_srv::Column],
        results: QueryResultWriter<W>,
    ) -> io::Result<()> {
        // create a getter if we don't have one for this query already
        // TODO(malte): may need to make one anyway if the query has changed w.r.t. an
        // earlier one of the same name
        trace!("select::access view");
        let getter = self.inner.ensure_getter(&qname);

        let write_column = |rw: &mut RowWriter<W>, c: &DataType, cs: &msql_srv::Column| {
            let written = match *c {
                DataType::None => rw.write_col(None::<i32>),
                // NOTE(malte): the code repetition here is unfortunate, but it's hard to factor
                // this out into a helper since i has a different time depending on the DataType
                // variant.
                DataType::Int(i) => {
                    if cs.colflags.contains(ColumnFlags::UNSIGNED_FLAG) {
                        rw.write_col(i as usize)
                    } else {
                        rw.write_col(i as isize)
                    }
                }
                DataType::BigInt(i) => {
                    if cs.colflags.contains(ColumnFlags::UNSIGNED_FLAG) {
                        rw.write_col(i as usize)
                    } else {
                        rw.write_col(i as isize)
                    }
                }
                DataType::UnsignedInt(i) => {
                    if cs.colflags.contains(ColumnFlags::UNSIGNED_FLAG) {
                        rw.write_col(i as usize)
                    } else {
                        rw.write_col(i as isize)
                    }
                }
                DataType::UnsignedBigInt(i) => {
                    if cs.colflags.contains(ColumnFlags::UNSIGNED_FLAG) {
                        rw.write_col(i as usize)
                    } else {
                        rw.write_col(i as isize)
                    }
                }
                DataType::Text(ref t) => rw.write_col(t.to_str().unwrap()),
                ref dt @ DataType::TinyText(_) => rw.write_col(dt.to_string()),
                ref dt @ DataType::Real(_, _) => match cs.coltype {
                    msql_srv::ColumnType::MYSQL_TYPE_DECIMAL => {
                        let f = dt.to_string();
                        rw.write_col(f)
                    }
                    msql_srv::ColumnType::MYSQL_TYPE_DOUBLE => {
                        let f: f64 = dt.into();
                        rw.write_col(f)
                    }
                    _ => unreachable!(),
                },
                DataType::Timestamp(ts) => rw.write_col(ts),
            };
            match written {
                Ok(_) => (),
                Err(e) => panic!("failed to write column: {:?}", e),
            }
        };

        trace!("select::lookup");
        let cols = Vec::from(getter.columns());
        let bogo = vec![vec1![DataType::from(0i32)].into()];
        let use_bogo = keys.is_empty();
        let keys = if use_bogo {
            bogo
        } else {
            let mut binops = utils::get_select_statement_binops(q)
                .into_iter()
                .map(|(_, b)| b)
                .unique();
            let binop_to_use = binops.next().unwrap_or(BinaryOperator::Equal);
            if let Some(other) = binops.next() {
                panic!("attempted to execute statement with conflicting binary operators {:?} and {:?}", binop_to_use, other);
            }

            keys.drain(..)
                .map(|k_op| {
                    (k_op, binop_to_use)
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
        };

        // if first lookup fails, there's no reason to try the others
        match block_on_buffer(getter.raw_lookup(vq)) {
            Ok(d) => {
                trace!("select::complete");
                let mut rw = results.start(schema).unwrap();
                for resultsets in d {
                    for r in resultsets {
                        let mut r: Vec<_> = r.into();
                        if use_bogo {
                            // drop bogokey
                            r.pop();
                        }

                        for c in schema {
                            let coli =
                                cols.iter().position(|f| f == &c.column).unwrap_or_else(|| {
                                    panic!(
                                        "tried to emit column {:?} not in getter for {:?} with schema {:?}",
                                        c.column, qname, cols
                                    );
                                });
                            write_column(&mut rw, &r[coli], c);
                        }
                        rw.end_row()?;
                    }
                }
                rw.finish()
            }
            Err(e) => {
                error!(error = %e, "failed");
                results.error(
                    msql_srv::ErrorKind::ER_UNKNOWN_ERROR,
                    "Noria returned an error".as_bytes(),
                )
            }
        }
    }

    fn do_update<W: io::Write>(
        &mut self,
        q: Cow<UpdateStatement>,
        params: Option<ParamParser>,
        results: QueryResultWriter<W>,
    ) -> io::Result<()> {
        trace!(table = %q.table.name, "update::access mutator");
        let mutator = self.inner.ensure_mutator(&q.table.name);

        let q = q.into_owned();
        let (key, updates) = {
            trace!("update::extract schema");
            let schema = if let Some(cts) = mutator.schema() {
                cts
            } else {
                // no update on views
                unimplemented!();
            };
            utils::extract_update(q, params, schema)
        };

        trace!("update::update");
        match block_on_buffer(mutator.update(key, updates)) {
            Ok(..) => {
                trace!("update::complete");
                results.completed(1 /* TODO */, 0)
            }
            Err(e) => {
                error!(error = %e, "failed");
                results.error(
                    msql_srv::ErrorKind::ER_UNKNOWN_ERROR,
                    format!("{:?}", e).as_bytes(),
                )
            }
        }
    }
}
