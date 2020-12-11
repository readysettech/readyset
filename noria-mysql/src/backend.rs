use noria::{ControllerHandle, DataType, Table, TableOperation, View, ZookeeperAuthority};

use anyhow;
use futures_executor::block_on as block_on_buffer;
use msql_srv::{self, *};
use nom_sql::{
    self, ColumnConstraint, InsertStatement, Literal, SelectStatement, SqlQuery, UpdateStatement,
};
use vec1::vec1;

use std::borrow::Cow;
use std::fmt;
use std::io;
use std::sync::atomic;
use std::sync::{Arc, RwLock};
use std::time;
use std::{
    collections::{BTreeMap, HashMap},
    convert::TryInto,
};
use tracing::Level;

use crate::convert::ToDataType;
use crate::referred_tables::ReferredTables;
use crate::rewrite;
use crate::schema::{self, schema_for_column, Schema};
use crate::utils;

#[derive(Clone)]
enum PreparedStatement {
    /// Query name, Query, result schema, optional parameter rewrite map
    Select(
        String,
        nom_sql::SelectStatement,
        Vec<msql_srv::Column>,
        Option<(usize, usize)>,
    ),
    Insert(nom_sql::InsertStatement),
    Update(nom_sql::UpdateStatement),
}

impl fmt::Debug for PreparedStatement {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match *self {
            PreparedStatement::Select(ref qname, ref s, _, _) => write!(f, "{}: {}", qname, s),
            PreparedStatement::Insert(ref s) => write!(f, "{}", s),
            PreparedStatement::Update(ref s) => write!(f, "{}", s),
        }
    }
}

struct NoriaBackendInner {
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
}

impl NoriaBackendInner {
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

pub struct NoriaBackend {
    inner: NoriaBackendInner,
    auto_increments: Arc<RwLock<HashMap<String, atomic::AtomicUsize>>>,

    prepared: HashMap<u32, PreparedStatement>,
    prepared_count: u32,

    /// global cache of view endpoints and prepared statements
    cached: Arc<RwLock<HashMap<SelectStatement, String>>>,
    /// thread-local version of `cached` (consulted first)
    tl_cached: HashMap<SelectStatement, String>,

    parsed: HashMap<String, (SqlQuery, Vec<nom_sql::Literal>)>,

    sanitize: bool,
    slowlog: bool,
    static_responses: bool,
    permissive: bool,
}

impl NoriaBackend {
    pub async fn new(
        ex: tokio::runtime::Handle,
        ch: ControllerHandle<ZookeeperAuthority>,
        auto_increments: Arc<RwLock<HashMap<String, atomic::AtomicUsize>>>,
        query_cache: Arc<RwLock<HashMap<SelectStatement, String>>>,
        slowlog: bool,
        static_responses: bool,
        sanitize: bool,
        permissive: bool,
    ) -> Self {
        NoriaBackend {
            inner: NoriaBackendInner::new(ex, ch).await,

            auto_increments,

            prepared: HashMap::new(),
            prepared_count: 0,

            cached: query_cache,
            tl_cached: HashMap::new(),

            parsed: HashMap::new(),

            sanitize,
            slowlog,
            static_responses,
            permissive,
        }
    }

    fn fetch_endpoints(&mut self, need: Vec<nom_sql::Table>) -> Result<(), anyhow::Error> {
        for t in need {
            //  1. check inner.inputs/inner.outputs
            if self.inner.inputs.contains_key(&t.name) {
                // already have mutator, nothing more to do
                continue;
            } else if self.inner.outputs.contains_key(&t.name) {
                // already have getter, nothing more to do
                continue;
            } else {
                //  2. If nothing, RPC for iew
                match self.inner.get_or_make_getter(&t.name) {
                    // TODO(malte): the error handling here is lame, as it doesn't differentiate
                    // between "no such view/table" errors and transport or network errors.
                    Ok(_) => continue,
                    Err(_) => {
                        //  3. If nothing, RPC for table
                        if self.inner.get_or_make_mutator(&t.name).is_err() {
                            //  4. If still nothing, panic
                            bail!("no view or table named '{}'", t.name);
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn handle_create_table<W: io::Write>(
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

    fn handle_create_view<W: io::Write>(
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

    fn handle_delete<W: io::Write>(
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

    fn handle_insert<W: io::Write>(
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

    fn handle_select<W: io::Write>(
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
        self.do_read(&qname, keys, schema.as_slice(), results)
    }

    fn handle_set<W: io::Write>(
        &mut self,
        q: nom_sql::SetStatement,
        results: QueryResultWriter<W>,
    ) -> io::Result<()> {
        trace!(%q.variable, "set");
        // ignore
        results.completed(0, 0)
    }

    fn handle_update<W: io::Write>(
        &mut self,
        q: nom_sql::UpdateStatement,
        results: QueryResultWriter<W>,
    ) -> io::Result<()> {
        self.do_update(Cow::Owned(q), None, results)
    }

    fn prepare_insert<W: io::Write>(
        &mut self,
        mut sql_q: nom_sql::SqlQuery,
        info: StatementMetaWriter<W>,
    ) -> io::Result<()> {
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

        self.prepared_count += 1;

        // nothing more to do for an insert
        info.reply(self.prepared_count, params.as_slice(), schema.as_slice())?;

        // register a new prepared statement
        let q = if let nom_sql::SqlQuery::Insert(q) = sql_q {
            q
        } else {
            unreachable!()
        };
        self.prepared
            .insert(self.prepared_count, PreparedStatement::Insert(q));

        trace!(id = self.prepared_count, "insert::registered");

        Ok(())
    }

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
                            if let Err(e) = block_on!(
                                self.inner,
                                self.inner
                                    .noria
                                    .extend_recipe(&format!("QUERY {}: {};", qname, q))
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

    fn prepare_select<W: io::Write>(
        &mut self,
        mut sql_q: nom_sql::SqlQuery,
        info: StatementMetaWriter<W>,
    ) -> io::Result<()> {
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
                .to_vec(),
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

        self.prepared_count += 1;
        info.reply(self.prepared_count, params.as_slice(), schema.as_slice())?;

        // register a new prepared statement
        self.prepared.insert(
            self.prepared_count,
            PreparedStatement::Select(qname, q, schema, rewritten.map(|(a, b)| (a, b.len()))),
        );

        trace!(id = self.prepared_count, "select::registered");

        Ok(())
    }

    fn prepare_update<W: io::Write>(
        &mut self,
        sql_q: nom_sql::SqlQuery,
        info: StatementMetaWriter<W>,
    ) -> io::Result<()> {
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

        // register a new prepared statement
        self.prepared_count += 1;
        self.prepared
            .insert(self.prepared_count, PreparedStatement::Update(q));

        trace!(id = self.prepared_count, "update::registered");

        info.reply(self.prepared_count, &params[..], &[])
    }

    fn execute_insert<W: io::Write>(
        &mut self,
        q: &InsertStatement,
        data: Vec<DataType>,
        results: QueryResultWriter<W>,
    ) -> io::Result<()> {
        assert_eq!(q.fields.as_ref().unwrap().len(), data.len());
        self.do_insert(&q, vec![data], results)
    }

    fn execute_select<W: io::Write>(
        &mut self,
        qname: &str,
        keys: Vec<Vec<DataType>>,
        schema: &[msql_srv::Column],
        results: QueryResultWriter<W>,
    ) -> io::Result<()> {
        self.do_read(qname, keys, schema, results)
    }

    fn execute_update<W: io::Write>(
        &mut self,
        q: &UpdateStatement,
        params: ParamParser,
        results: QueryResultWriter<W>,
    ) -> io::Result<()> {
        self.do_update(Cow::Borrowed(q), Some(params), results)
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
            keys.drain(..)
                .map(|k| k.try_into().expect("Input key cannot be empty"))
                .collect()
        };

        // if first lookup fails, there's no reason to try the others
        match block_on_buffer(getter.multi_lookup(keys, true)) {
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

impl<W: io::Write> MysqlShim<W> for &mut NoriaBackend {
    type Error = io::Error;

    fn on_prepare(&mut self, query: &str, info: StatementMetaWriter<W>) -> io::Result<()> {
        let span = span!(Level::DEBUG, "prepare", query);
        let _g = span.enter();

        trace!("sanitize");
        let query = if self.sanitize {
            utils::sanitize_query(query)
        } else {
            query.to_owned()
        };

        trace!("parse");
        let sql_q = match self.parsed.get(&query) {
            None => match nom_sql::parse_query(&query) {
                Ok(sql_q) => {
                    // ensure that we have schemas and endpoints for the query
                    let endpoints_needed = sql_q.referred_tables();
                    self.fetch_endpoints(endpoints_needed).unwrap();

                    self.parsed
                        .insert(query.to_owned(), (sql_q.clone(), vec![]));

                    sql_q
                }
                Err(e) => {
                    // if nom-sql rejects the query, there is no chance Noria will like it
                    error!(%query, "query can't be parsed: \"{}\"", query);
                    return info.error(msql_srv::ErrorKind::ER_PARSE_ERROR, e.as_bytes());
                }
            },
            Some((q, _)) => q.clone(),
        };

        trace!("delegate");
        match sql_q {
            nom_sql::SqlQuery::Select(_) => self.prepare_select(sql_q, info),
            nom_sql::SqlQuery::Insert(_) => self.prepare_insert(sql_q, info),
            nom_sql::SqlQuery::Update(_) => self.prepare_update(sql_q, info),
            _ => {
                // Noria only supports prepared SELECT statements at the moment
                error!(%query, "unsupported query for prepared statement");
                return info.error(
                    msql_srv::ErrorKind::ER_NOT_SUPPORTED_YET,
                    "unsupported query".as_bytes(),
                );
            }
        }
    }

    fn on_execute(
        &mut self,
        id: u32,
        params: ParamParser,
        results: QueryResultWriter<W>,
    ) -> io::Result<()> {
        let span = span!(Level::TRACE, "execute", id);
        let _g = span.enter();

        let start = time::Instant::now();

        // TODO(malte): unfortunate clone here, but we can't call execute_select(&mut self) if we
        // have self.prepared borrowed
        let prep: PreparedStatement = {
            match self.prepared.get(&id) {
                Some(e) => e.clone(),
                None => {
                    return results.error(
                        msql_srv::ErrorKind::ER_UNKNOWN_ERROR,
                        "non-existent statement".as_bytes(),
                    )
                }
            }
        };

        trace!("delegate");
        let res = match prep {
            PreparedStatement::Select(ref qname, ref _q, ref schema, ref rewritten) => {
                trace!("apply where-in rewrites");
                let key = match rewritten {
                    Some((first_rewritten, nrewritten)) => {
                        // this is a little tricky
                        // the user is giving us some params [a, b, c, d]
                        // for the query WHERE x = ? AND y IN (?, ?) AND z = ?
                        // that we rewrote to WHERE x = ? AND y = ? AND z = ?
                        // so we need to turn that into the keys:
                        // [[a, b, d], [a, c, d]]
                        let params: Vec<_> = params
                            .into_iter()
                            .map(|pv| pv.value.to_datatype())
                            .collect::<Vec<_>>();
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
                    None => vec![params
                        .into_iter()
                        .map(|pv| pv.value.to_datatype())
                        .collect::<Vec<_>>()],
                };

                self.execute_select(&qname, key, schema, results)
            }
            PreparedStatement::Insert(ref q) => {
                let values: Vec<DataType> = params
                    .into_iter()
                    .map(|pv| pv.value.to_datatype())
                    .collect();

                self.execute_insert(&q, values, results)
            }
            PreparedStatement::Update(ref q) => self.execute_update(&q, params, results),
        };

        if self.slowlog {
            let took = start.elapsed();
            if took.as_secs() > 0 || took.subsec_nanos() > 5_000_000 {
                let query: &dyn std::fmt::Display = match prep {
                    PreparedStatement::Select(_, ref q, ..) => q,
                    PreparedStatement::Insert(ref q) => q,
                    PreparedStatement::Update(ref q) => q,
                };
                warn!(
                    %query,
                    time = ?took,
                    "slow query",
                );
            }
        }

        res
    }

    fn on_close(&mut self, _: u32) {}

    fn on_query(&mut self, query: &str, results: QueryResultWriter<W>) -> io::Result<()> {
        let span = span!(Level::TRACE, "query", query);
        let _g = span.enter();

        let start = time::Instant::now();
        let query = if self.sanitize {
            let q = utils::sanitize_query(query);
            trace!(%q, "sanitized");
            q
        } else {
            query.to_owned()
        };

        let query_lc = query.to_lowercase();

        if query_lc.starts_with("begin")
            || query_lc.starts_with("start transaction")
            || query_lc.starts_with("commit")
        {
            trace!("ignoring transaction bits");
            return results.completed(0, 0);
        }

        if query_lc.starts_with("show databases")
            || query_lc.starts_with("rollback")
            || query_lc.starts_with("alter table")
            || query_lc.starts_with("create index")
            || query_lc.starts_with("create unique index")
            || query_lc.starts_with("create fulltext index")
        {
            warn!("unsupported query; returning empty results");
            return results.completed(0, 0);
        }

        if query_lc.starts_with("show tables") {
            let cols = [Column {
                table: String::from(""),
                column: String::from("Tables"),
                coltype: ColumnType::MYSQL_TYPE_STRING,
                colflags: ColumnFlags::empty(),
            }];
            // TODO(malte): we could find out which tables exist via RPC to Soup and return them
            let writer = results.start(&cols)?;
            trace!("mocking show tables");
            return writer.finish();
        }

        if self.static_responses {
            for &(ref pattern, ref columns) in &*utils::HARD_CODED_REPLIES {
                if pattern.is_match(&query) {
                    trace!("sending static response");
                    let cols: Vec<_> = columns
                        .iter()
                        .map(|c| Column {
                            table: String::from(""),
                            column: String::from(c.0),
                            coltype: ColumnType::MYSQL_TYPE_STRING,
                            colflags: ColumnFlags::empty(),
                        })
                        .collect();
                    let mut writer = results.start(&cols[..])?;
                    for &(_, ref r) in columns {
                        writer.write_col(String::from(*r))?;
                    }
                    return writer.end_row();
                }
            }
        }

        trace!("analyzing query");
        let (q, use_params) = match self.parsed.get(&query) {
            None => {
                trace!("parsing query");
                match nom_sql::parse_query(&query) {
                    Ok(mut q) => {
                        trace!("checking endpoint availability");

                        // for all tables and views mentioned in a query we're seeing for the first
                        // time, fetch the schemas if we don't have them already.
                        // Note that this implicitly also creates mutators and getters for the
                        // relevant tables/views.
                        match q {
                            SqlQuery::CreateTable(_) | SqlQuery::DropTable(_) => {
                                // don't fetch endpoints, since table or view does not exist yet
                                // TODO(malte): properly DROP TABLE IF EXISTS
                            }
                            _ => {
                                let endpoints_needed = q.referred_tables();
                                self.fetch_endpoints(endpoints_needed).unwrap();
                            }
                        }

                        trace!("collapsing where-in clauses");
                        let mut use_params = Vec::new();
                        if let Some((_, p)) = rewrite::collapse_where_in(&mut q, true) {
                            use_params = p;
                        }

                        self.parsed
                            .insert(query.to_owned(), (q.clone(), use_params.clone()));

                        (q, use_params)
                    }
                    Err(e) => {
                        // if nom-sql rejects the query, there is no chance Noria will like it
                        error!(%query, "query can't be parsed: \"{}\"", query);
                        if self.permissive {
                            warn!("permissive flag enabled, so returning success despite query parse failure");
                            return results.completed(0, 0);
                        } else {
                            return results
                                .error(msql_srv::ErrorKind::ER_PARSE_ERROR, e.as_bytes());
                        }
                    }
                }
            }
            Some((q, use_params)) => (q.clone(), use_params.clone()),
        };

        trace!("delegate");
        let res = match q {
            nom_sql::SqlQuery::CreateTable(q) => self.handle_create_table(q, results),
            nom_sql::SqlQuery::CreateView(q) => self.handle_create_view(q, results),
            nom_sql::SqlQuery::Insert(q) => self.handle_insert(q, results),
            nom_sql::SqlQuery::Select(q) => self.handle_select(q, use_params, results),
            nom_sql::SqlQuery::Set(q) => self.handle_set(q, results),
            nom_sql::SqlQuery::Update(q) => self.handle_update(q, results),
            nom_sql::SqlQuery::Delete(q) => self.handle_delete(q, results),
            nom_sql::SqlQuery::DropTable(_) => {
                warn!("ignoring drop table");
                return results.completed(0, 0);
            }
            _ => {
                error!("unsupported query");
                return results.error(
                    msql_srv::ErrorKind::ER_NOT_SUPPORTED_YET,
                    "unsupported query".as_bytes(),
                );
            }
        };

        if self.slowlog {
            let took = start.elapsed();
            if took.as_secs() > 0 || took.subsec_nanos() > 5_000_000 {
                warn!(
                    %query,
                    time = ?took,
                    "slow query",
                );
            }
        }

        res
    }
}
