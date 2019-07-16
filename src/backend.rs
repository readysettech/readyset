use noria::{
    DataType, SyncControllerHandle, SyncTable, SyncView, TableOperation, ZookeeperAuthority,
};

use failure;
use msql_srv::{self, *};
use nom_sql::{
    self, ColumnConstraint, InsertStatement, Literal, SelectStatement, SqlQuery, UpdateStatement,
};

use slog;
use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::io;
use std::sync::atomic;
use std::sync::{Arc, RwLock};
use std::time;

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

struct NoriaBackendInner<E> {
    noria: SyncControllerHandle<ZookeeperAuthority, E>,
    inputs: BTreeMap<String, SyncTable>,
    outputs: BTreeMap<String, SyncView>,
}

impl<E> NoriaBackendInner<E>
where
    E: tokio::executor::Executor,
{
    fn new(mut ch: SyncControllerHandle<ZookeeperAuthority, E>) -> Self {
        NoriaBackendInner {
            inputs: ch
                .inputs()
                .expect("couldn't get inputs from Noria")
                .into_iter()
                .map(|(n, _)| (n.clone(), ch.table(&n).unwrap().into_sync()))
                .collect::<BTreeMap<String, SyncTable>>(),
            outputs: ch
                .outputs()
                .expect("couldn't get outputs from Noria")
                .into_iter()
                .map(|(n, _)| (n.clone(), ch.view(&n).unwrap().into_sync()))
                .collect::<BTreeMap<String, SyncView>>(),
            noria: ch,
        }
    }

    fn ensure_mutator<'a, 'b>(&'a mut self, table: &'b str) -> &'a mut SyncTable {
        self.get_or_make_mutator(table)
            .expect(&format!("no table named '{}'!", table))
    }

    fn ensure_getter<'a, 'b>(&'a mut self, view: &'b str) -> &'a mut SyncView {
        self.get_or_make_getter(view)
            .expect(&format!("no view named '{}'!", view))
    }

    fn get_or_make_mutator<'a, 'b>(
        &'a mut self,
        table: &'b str,
    ) -> Result<&'a mut SyncTable, failure::Error> {
        let noria = &mut self.noria;
        if !self.inputs.contains_key(table) {
            let t = noria.table(table)?;
            self.inputs.insert(table.to_owned(), t.into_sync());
        }
        Ok(self.inputs.get_mut(table).unwrap())
    }

    fn get_or_make_getter<'a, 'b>(
        &'a mut self,
        view: &'b str,
    ) -> Result<&'a mut SyncView, failure::Error> {
        let noria = &mut self.noria;
        if !self.outputs.contains_key(view) {
            let vh = noria.view(view)?;
            self.outputs.insert(view.to_owned(), vh.into_sync());
        }
        Ok(self.outputs.get_mut(view).unwrap())
    }
}

pub struct NoriaBackend<E> {
    inner: NoriaBackendInner<E>,
    log: slog::Logger,
    ops: Arc<atomic::AtomicUsize>,
    trace_every: Option<usize>,

    auto_increments: Arc<RwLock<HashMap<String, atomic::AtomicUsize>>>,

    prepared: HashMap<u32, PreparedStatement>,
    prepared_count: u32,

    /// global cache of view endpoints and prepared statements
    cached: Arc<RwLock<HashMap<SelectStatement, String>>>,
    /// thread-local version of `cached` (consulted first)
    tl_cached: HashMap<SelectStatement, String>,

    primed: Arc<atomic::AtomicBool>,
    reset: bool,

    parsed: HashMap<String, (SqlQuery, Vec<nom_sql::Literal>)>,

    sanitize: bool,
    slowlog: bool,
    static_responses: bool,
}

impl<E> NoriaBackend<E>
where
    E: tokio::executor::Executor,
{
    pub fn new(
        ch: SyncControllerHandle<ZookeeperAuthority, E>,
        auto_increments: Arc<RwLock<HashMap<String, atomic::AtomicUsize>>>,
        query_cache: Arc<RwLock<HashMap<SelectStatement, String>>>,
        (ops, trace_every): (Arc<atomic::AtomicUsize>, Option<usize>),
        primed: Arc<atomic::AtomicBool>,
        slowlog: bool,
        static_responses: bool,
        sanitize: bool,
        log: slog::Logger,
    ) -> Self {
        NoriaBackend {
            inner: NoriaBackendInner::new(ch),
            log: log,
            ops,
            trace_every,

            auto_increments: auto_increments,

            prepared: HashMap::new(),
            prepared_count: 0,

            cached: query_cache,
            tl_cached: HashMap::new(),

            parsed: HashMap::new(),

            primed,
            reset: false,

            sanitize,
            slowlog,
            static_responses,
        }
    }

    fn fetch_endpoints(&mut self, need: Vec<nom_sql::Table>) -> Result<(), failure::Error> {
        for t in need {
            //  1. check inner.inputs/inner.outputs
            if self.inner.inputs.contains_key(&t.name) {
                // already have mutator, nothing more to do
                continue;
            } else if self.inner.outputs.contains_key(&t.name) {
                // already have getter, nothing more to do
                continue;
            } else {
                //  2. If nothing, RPC for view
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
        match self.inner.noria.extend_recipe(&format!("{};", q)) {
            Ok(_) => {
                // no rows to return
                // TODO(malte): potentially eagerly cache the mutator for this table
                results.completed(0, 0)
            }
            Err(e) => Err(io::Error::new(io::ErrorKind::Other, e.compat())),
        }
    }

    fn handle_create_view<W: io::Write>(
        &mut self,
        q: nom_sql::CreateViewStatement,
        results: QueryResultWriter<W>,
    ) -> io::Result<()> {
        // TODO(malte): we should perhaps check our usual caches here, rather than just blindly
        // doing a migration on Noria every time. On the other hand, CREATE VIEW is rare...

        info!(
            self.log,
            "Adding view \"{}\" to Noria as {}", q.definition, q.name
        );
        match self
            .inner
            .noria
            .extend_recipe(&format!("VIEW {}: {};", q.name, q.definition))
        {
            Ok(_) => {
                // no rows to return
                results.completed(0, 0)
            }
            Err(e) => Err(io::Error::new(io::ErrorKind::Other, e.compat())),
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
        let mutator = self.inner.ensure_mutator(&q.table.name);

        let pkey = if let Some(cts) = mutator.schema() {
            utils::get_primary_key(cts)
                .into_iter()
                .map(|(_, c)| c)
                .collect()
        } else {
            // cannot delete from view
            unimplemented!();
        };

        match utils::flatten_conditional(&cond, &pkey) {
            None => results.completed(0, 0),
            Some(ref flattened) if flattened.len() == 0 => {
                panic!("DELETE only supports WHERE-clauses on primary keys");
            }
            Some(flattened) => {
                let count = flattened.len() as u64;
                for key in flattened {
                    match mutator.delete(key) {
                        Ok(_) => {}
                        Err(e) => {
                            error!(self.log, "delete error: {:?}", e);
                            return results.error(
                                msql_srv::ErrorKind::ER_UNKNOWN_ERROR,
                                format!("{:?}", e).as_bytes(),
                            );
                        }
                    };
                }

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
        let putter = self.inner.ensure_mutator(table);
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
        let qname = match self.get_or_create_view(&q, false) {
            Ok(qn) => qn,
            Err(e) => {
                if e.kind() == io::ErrorKind::Other {
                    // maybe ER_SYNTAX_ERROR ?
                    // would be good to narrow these down
                    // TODO: why are the actual error contents never printed?
                    use std::error::Error;
                    return results.error(
                        msql_srv::ErrorKind::ER_UNKNOWN_ERROR,
                        e.description().as_bytes(),
                    );
                } else {
                    return Err(e);
                }
            }
        };
        let keys: Vec<_> = use_params
            .into_iter()
            .map(|l| vec![l.to_datatype()])
            .collect();
        // we need the schema for the result writer
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
        self.do_read(&qname, keys, schema.as_slice(), results)
    }

    fn handle_set<W: io::Write>(
        &mut self,
        q: nom_sql::SetStatement,
        results: QueryResultWriter<W>,
    ) -> io::Result<()> {
        if q.variable == "@primed" {
            self.primed.store(true, atomic::Ordering::SeqCst);
        }
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
        let mutator = self.inner.ensure_mutator(&q.table.name);
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
                                info!(
                                    self.log,
                                    "Adding parameterized query \"{}\" to Noria as {}", q, qname
                                );
                            } else {
                                info!(
                                    self.log,
                                    "Adding ad-hoc query \"{}\" to Noria as {}", q, qname
                                );
                            }
                            if let Err(e) = self
                                .inner
                                .noria
                                .extend_recipe(&format!("QUERY {}: {};", qname, q))
                            {
                                error!(self.log, "{:?}", e);
                                return Err(io::Error::new(io::ErrorKind::Other, e.compat()));
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

        let rewritten = rewrite::collapse_where_in(&mut sql_q, false);
        let q = if let nom_sql::SqlQuery::Select(q) = sql_q {
            q
        } else {
            unreachable!();
        };

        // check if we already have this query prepared
        let qname = self.get_or_create_view(&q, true)?;

        // extract result schema
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
        let mutator = self.inner.ensure_mutator(&q.table.name);
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
        let putter = self.inner.ensure_mutator(table);
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

        let ops = &self.ops;
        if self
            .trace_every
            .map(|te| ops.fetch_add(1, atomic::Ordering::AcqRel) % te == 0)
            .unwrap_or(false)
        {
            noria::trace_my_next_op();
        }

        let result = if let Some(ref update_fields) = q.on_duplicate {
            trace!(self.log, "inserting-or-updating {:?}", buf);
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
            putter.insert_or_update(buf[0].clone(), updates)
        } else {
            trace!(self.log, "inserting {:?}", buf);
            let buf: Vec<_> = buf
                .into_iter()
                .map(|r| TableOperation::Insert(r.into()))
                .collect();
            putter.perform_all(buf)
        };

        match result {
            Ok(_) => results.completed(data.len() as u64, first_inserted_id.unwrap_or(0) as u64),
            Err(e) => {
                error!(self.log, "put error: {:?}", e);
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
        keys: Vec<Vec<DataType>>,
        schema: &[msql_srv::Column],
        results: QueryResultWriter<W>,
    ) -> io::Result<()> {
        // create a getter if we don't have one for this query already
        // TODO(malte): may need to make one anyway if the query has changed w.r.t. an
        // earlier one of the same name
        let getter = self.inner.ensure_getter(&qname);

        let write_column = |rw: &mut RowWriter<W>, c: &DataType, cs: &msql_srv::Column| {
            let written = match *c {
                DataType::None => rw.write_col(None::<i32>),
                DataType::Int(i) => rw.write_col(i as isize),
                DataType::BigInt(i) => rw.write_col(i as isize),
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

        let cols = Vec::from(getter.columns());
        let bogo = vec![vec![DataType::from(0 as i32)]];
        let is_bogo = keys.is_empty() || keys.iter().all(|k| k.is_empty());
        let keys = if is_bogo { bogo } else { keys };

        let ops = &self.ops;
        if self
            .trace_every
            .map(|te| ops.fetch_add(1, atomic::Ordering::AcqRel) % te == 0)
            .unwrap_or(false)
        {
            noria::trace_my_next_op();
        }

        // if first lookup fails, there's no reason to try the others
        match getter.multi_lookup(keys, true) {
            Ok(d) => {
                let mut rw = results.start(schema).unwrap();
                for resultsets in d {
                    for mut r in resultsets {
                        if is_bogo {
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
                error!(self.log, "error executing SELECT: {:?}", e);
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
        let mutator = self.inner.ensure_mutator(&q.table.name);

        let q = q.into_owned();
        let (key, updates) = {
            let schema = if let Some(cts) = mutator.schema() {
                cts
            } else {
                // no update on views
                unimplemented!();
            };
            utils::extract_update(q, params, schema)
        };

        let ops = &self.ops;
        if self
            .trace_every
            .map(|te| ops.fetch_add(1, atomic::Ordering::AcqRel) % te == 0)
            .unwrap_or(false)
        {
            noria::trace_my_next_op();
        }

        match mutator.update(key, updates) {
            Ok(..) => results.completed(1 /* TODO */, 0),
            Err(e) => {
                error!(self.log, "update: {:?}", e);
                results.error(
                    msql_srv::ErrorKind::ER_UNKNOWN_ERROR,
                    format!("{:?}", e).as_bytes(),
                )
            }
        }
    }
}

impl<W: io::Write, E> MysqlShim<W> for &mut NoriaBackend<E>
where
    E: tokio::executor::Executor,
{
    type Error = io::Error;

    fn on_prepare(&mut self, query: &str, info: StatementMetaWriter<W>) -> io::Result<()> {
        trace!(self.log, "prepare: {}", query);

        if !self.reset && self.primed.load(atomic::Ordering::Acquire) {
            self.reset = true;
        }

        let query = if self.sanitize {
            utils::sanitize_query(query)
        } else {
            query.to_owned()
        };

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
                    error!(self.log, "query can't be parsed: \"{}\"", query);
                    return info.error(msql_srv::ErrorKind::ER_PARSE_ERROR, e.as_bytes());
                }
            },
            Some((q, _)) => q.clone(),
        };

        match sql_q {
            nom_sql::SqlQuery::Select(_) => self.prepare_select(sql_q, info),
            nom_sql::SqlQuery::Insert(_) => self.prepare_insert(sql_q, info),
            nom_sql::SqlQuery::Update(_) => self.prepare_update(sql_q, info),
            _ => {
                // Noria only supports prepared SELECT statements at the moment
                error!(
                    self.log,
                    "unsupported query for prepared statement: {}", query
                );
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
        if !self.reset && self.primed.load(atomic::Ordering::Acquire) {
            self.reset = true;
        }

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

        let res = match prep {
            PreparedStatement::Select(ref qname, ref _q, ref schema, ref rewritten) => {
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
                warn!(
                    self.log,
                    "{:?} took {}ms",
                    prep,
                    took.as_secs() * 1_000 + took.subsec_nanos() as u64 / 1_000_000
                );
            }
        }

        res
    }

    fn on_close(&mut self, _: u32) {}

    fn on_query(&mut self, query: &str, results: QueryResultWriter<W>) -> io::Result<()> {
        trace!(self.log, "query: {}", query);

        if !self.reset && self.primed.load(atomic::Ordering::Acquire) {
            self.reset = true;
        }

        let start = time::Instant::now();
        let query = if self.sanitize {
            utils::sanitize_query(query)
        } else {
            query.to_owned()
        };

        let query_lc = query.to_lowercase();

        if query_lc.starts_with("begin")
            || query_lc.starts_with("start transaction")
            || query_lc.starts_with("commit")
        {
            return results.completed(0, 0);
        }

        if query_lc.starts_with("show databases")
            || query_lc.starts_with("rollback")
            || query_lc.starts_with("alter table")
            || query_lc.starts_with("create index")
            || query_lc.starts_with("create unique index")
            || query_lc.starts_with("create fulltext index")
        {
            warn!(
                self.log,
                "ignoring unsupported query \"{}\" and returning empty results", query
            );
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
            println!(" -> Ok({} rows)", 0);
            return writer.finish();
        }

        if self.static_responses {
            for &(ref pattern, ref columns) in &*utils::HARD_CODED_REPLIES {
                if pattern.is_match(&query) {
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

        let (q, use_params) = match self.parsed.get(&query) {
            None => {
                match nom_sql::parse_query(&query) {
                    Ok(mut q) => {
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

                        let mut use_params = Vec::new();
                        if let Some((_, p)) = rewrite::collapse_where_in(&mut q, true) {
                            use_params = p;
                        }

                        self.parsed
                            .insert(query.to_owned(), (q.clone(), use_params.clone()));

                        (q, use_params)
                    }
                    Err(_e) => {
                        // if nom-sql rejects the query, there is no chance Noria will like it
                        error!(self.log, "query can't be parsed: \"{}\"", query);
                        return results.completed(0, 0);
                        //return results.error(msql_srv::ErrorKind::ER_PARSE_ERROR, e.as_bytes());
                    }
                }
            }
            Some((q, use_params)) => (q.clone(), use_params.clone()),
        };

        let res = match q {
            nom_sql::SqlQuery::CreateTable(q) => self.handle_create_table(q, results),
            nom_sql::SqlQuery::CreateView(q) => self.handle_create_view(q, results),
            nom_sql::SqlQuery::Insert(q) => self.handle_insert(q, results),
            nom_sql::SqlQuery::Select(q) => self.handle_select(q, use_params, results),
            nom_sql::SqlQuery::Set(q) => self.handle_set(q, results),
            nom_sql::SqlQuery::Update(q) => self.handle_update(q, results),
            nom_sql::SqlQuery::Delete(q) => self.handle_delete(q, results),
            nom_sql::SqlQuery::DropTable(q) => {
                warn!(self.log, "Ignoring DROP TABLE query: \"{}\"", q);
                return results.completed(0, 0);
            }
            _ => {
                error!(self.log, "Unsupported query: {}", query);
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
                    self.log,
                    "{} took {}ms",
                    query,
                    took.as_secs() * 1_000 + took.subsec_nanos() as u64 / 1_000_000
                );
            }
        }

        res
    }
}
