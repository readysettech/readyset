use distributary::{ControllerHandle, DataType, Table, View, ZookeeperAuthority};

use msql_srv::{self, *};
use nom_sql::{
    self, ColumnConstraint, InsertStatement, Literal, SelectSpecification, SelectStatement,
    SqlQuery, UpdateStatement,
};

use slog;
use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::io;
use std::sync::atomic;
use std::sync::{self, Arc, RwLock};
use std::time;

use convert::ToDataType;
use rewrite;
use schema::{schema_for_column, schema_for_insert, schema_for_select, Schema};
use utils;

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

struct SoupBackendInner {
    soup: ControllerHandle<ZookeeperAuthority>,
    inputs: BTreeMap<String, Table>,
    outputs: BTreeMap<String, View>,
}

impl SoupBackendInner {
    fn new(zk_addr: &str, deployment: &str, log: &slog::Logger) -> Self {
        let mut zk_auth = ZookeeperAuthority::new(&format!("{}/{}", zk_addr, deployment)).unwrap();
        zk_auth.log_with(log.clone());

        debug!(log, "Connecting to Soup...",);
        let mut ch = ControllerHandle::new(zk_auth).unwrap();

        let soup = SoupBackendInner {
            inputs: ch
                .inputs()
                .expect("couldn't get inputs from Soup")
                .into_iter()
                .map(|(n, _)| (n.clone(), ch.table(&n).unwrap()))
                .collect::<BTreeMap<String, Table>>(),
            outputs: ch
                .outputs()
                .expect("couldn't get outputs from Soup")
                .into_iter()
                .map(|(n, _)| (n.clone(), ch.view(&n).unwrap()))
                .collect::<BTreeMap<String, View>>(),
            soup: ch,
        };

        debug!(log, "Connected!");

        soup
    }

    fn get_or_make_mutator<'a, 'b>(&'a mut self, table: &'b str) -> &'a mut Table {
        let soup = &mut self.soup;
        self.inputs.entry(table.to_owned()).or_insert_with(|| {
            soup.table(table)
                .expect(&format!("no table named {}", table))
        })
    }

    fn get_or_make_getter<'a, 'b>(&'a mut self, view: &'b str) -> &'a mut View {
        let soup = &mut self.soup;
        self.outputs
            .entry(view.to_owned())
            .or_insert_with(|| soup.view(view).expect(&format!("no view named '{}'", view)))
    }
}

pub struct SoupBackend {
    inner: SoupBackendInner,
    log: slog::Logger,

    table_schemas: Arc<RwLock<HashMap<String, Schema>>>,
    auto_increments: Arc<RwLock<HashMap<String, atomic::AtomicUsize>>>,

    query_count: Arc<atomic::AtomicUsize>,

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
}

impl SoupBackend {
    pub fn new(
        zk_addr: &str,
        deployment: &str,
        schemas: Arc<RwLock<HashMap<String, Schema>>>,
        auto_increments: Arc<RwLock<HashMap<String, atomic::AtomicUsize>>>,
        query_cache: Arc<RwLock<HashMap<SelectStatement, String>>>,
        query_counter: Arc<atomic::AtomicUsize>,
        slowlog: bool,
        static_responses: bool,
        sanitize: bool,
        log: slog::Logger,
    ) -> Self {
        SoupBackend {
            inner: SoupBackendInner::new(zk_addr, deployment, &log),
            log: log,

            table_schemas: schemas,
            auto_increments: auto_increments,

            query_count: query_counter,

            prepared: HashMap::new(),
            prepared_count: 0,

            cached: query_cache,
            tl_cached: HashMap::new(),

            parsed: HashMap::new(),

            sanitize,
            slowlog,
            static_responses,
        }
    }

    fn handle_create_table<W: io::Write>(
        &mut self,
        q: nom_sql::CreateTableStatement,
        results: QueryResultWriter<W>,
    ) -> io::Result<()> {
        // TODO(malte): we should perhaps check our usual caches here, rather than just blindly
        // doing a migration on Soup ever time. On the other hand, CREATE TABLE is rare...
        match self.inner.soup.extend_recipe(&format!("{};", q)) {
            Ok(_) => {
                let mut ts_lock = self.table_schemas.write().unwrap();
                ts_lock.insert(q.table.name.clone(), Schema::Table(q));
                // no rows to return
                // TODO(malte): potentially eagerly cache the mutator for this table
                results.completed(0, 0)
            }
            Err(e) => Err(io::Error::new(io::ErrorKind::Other, e.compat())),
        }
    }

    fn handle_create_view<W: io::Write>(
        &mut self,
        mut q: nom_sql::CreateViewStatement,
        results: QueryResultWriter<W>,
    ) -> io::Result<()> {
        // TODO(malte): we should perhaps check our usual caches here, rather than just blindly
        // doing a migration on Soup ever time. On the other hand, CREATE VIEW is rare...

        // NOTE(jon): expand stars here so that we don't have to recursively walk them when given a
        // view.* at query-time.
        {
            let ts_lock = self.table_schemas.read().unwrap();
            let table_schemas = &(*ts_lock);
            match *q.definition {
                SelectSpecification::Simple(ref mut q) => rewrite::expand_stars(q, table_schemas),
                SelectSpecification::Compound(ref mut qs) => {
                    for &mut (_, ref mut q) in &mut qs.selects {
                        rewrite::expand_stars(q, table_schemas);
                    }
                }
            };
        }

        info!(
            self.log,
            "Adding view \"{}\" to Soup as {}", q.definition, q.name
        );
        match self
            .inner
            .soup
            .extend_recipe(&format!("{}: {};", q.name, q.definition))
        {
            Ok(_) => {
                let mut ts_lock = self.table_schemas.write().unwrap();
                ts_lock.insert(q.name.clone(), Schema::View(q));
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

        let ts = self.table_schemas.read().unwrap();
        let pkey = if let Some(Schema::Table(ref cts)) = ts.get(&q.table.name) {
            utils::get_primary_key(cts)
                .into_iter()
                .map(|(_, c)| c)
                .collect()
        } else {
            // cannot delete from view
            unimplemented!();
        };

        // create a mutator if we don't have one for this table already
        let mutator = self.inner.get_or_make_mutator(&q.table.name);

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
        q: nom_sql::InsertStatement,
        results: QueryResultWriter<W>,
    ) -> io::Result<()> {
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
        let qname = self.get_or_create_view(&q, false)?;
        let keys: Vec<_> = use_params
            .into_iter()
            .map(|l| vec![l.to_datatype()])
            .collect();
        // we need the schema for the result writer
        let schema = {
            let ts_lock = self.table_schemas.read().unwrap();
            schema_for_select(&(*ts_lock), &q)
        };
        self.do_read(&qname, keys, schema.as_slice(), results)
    }

    fn handle_set<W: io::Write>(
        &mut self,
        _q: nom_sql::SetStatement,
        results: QueryResultWriter<W>,
    ) -> io::Result<()> {
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
        let ts_lock = self.table_schemas.read().unwrap();
        let table_schemas = &(*ts_lock);

        match sql_q {
            // set column names (insert schema) if not set
            nom_sql::SqlQuery::Insert(ref mut q) => if q.fields.is_none() {
                match table_schemas[&q.table.name] {
                    Schema::Table(ref ts) => {
                        q.fields = Some(ts.fields.iter().map(|cs| cs.column.clone()).collect());
                    }
                    _ => unreachable!(),
                }
            },
            _ => (),
        }

        let params: Vec<_> = {
            let q = if let nom_sql::SqlQuery::Insert(ref q) = sql_q {
                q
            } else {
                unreachable!()
            };

            // extract parameter columns
            let param_cols = utils::get_parameter_columns(&sql_q);
            param_cols
                .into_iter()
                .map(|c| {
                    let mut cc = c.clone();
                    cc.table = Some(q.table.name.clone());
                    schema_for_column(table_schemas, &cc)
                })
                .collect()
        };

        let q = if let nom_sql::SqlQuery::Insert(q) = sql_q {
            q
        } else {
            unreachable!()
        };

        // extract result schema
        let schema = schema_for_insert(table_schemas, &q);

        self.prepared_count += 1;

        // nothing more to do for an insert
        // TODO(malte): proactively get mutator?
        info.reply(self.prepared_count, params.as_slice(), schema.as_slice())?;

        // register a new prepared statement
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
                            let qc = self
                                .query_count
                                .fetch_add(1, sync::atomic::Ordering::SeqCst);
                            let qname = format!("q_{}", qc);

                            // add the query to Soup
                            if prepared {
                                info!(
                                    self.log,
                                    "Adding parameterized query \"{}\" to Soup as {}", q, qname
                                );
                            } else {
                                info!(
                                    self.log,
                                    "Adding ad-hoc query \"{}\" to Soup as {}", q, qname
                                );
                            }
                            if let Err(e) = self
                                .inner
                                .soup
                                .extend_recipe(&format!("QUERY {}: {};", qname, q))
                            {
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
        let (params, schema) = {
            let ts_lock = self.table_schemas.read().unwrap();
            let table_schemas = &(*ts_lock);

            // extract parameter columns
            // note that we have to do this *before* collapsing WHERE IN, otherwise the
            // client will be confused about the number of parameters it's supposed to
            // give.
            let params: Vec<msql_srv::Column> = utils::get_parameter_columns(&sql_q)
                .into_iter()
                .map(|c| schema_for_column(table_schemas, c))
                .collect();

            let q = if let nom_sql::SqlQuery::Select(ref q) = sql_q {
                q
            } else {
                unreachable!();
            };

            // extract result schema
            let schema = schema_for_select(table_schemas, &q);
            (params, schema)
        };

        let rewritten = rewrite::collapse_where_in(&mut sql_q, false);
        let q = if let nom_sql::SqlQuery::Select(q) = sql_q {
            q
        } else {
            unreachable!();
        };

        // check if we already have this query prepared
        let qname = self.get_or_create_view(&q, true)?;

        self.prepared_count += 1;

        // TODO(malte): proactively get getter, to avoid &mut ref on exec?
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
        // extract parameter columns
        let params: Vec<msql_srv::Column> = {
            let ts = self.table_schemas.read().unwrap();
            utils::get_parameter_columns(&sql_q)
                .into_iter()
                .map(|c| schema_for_column(&*ts, c))
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
        let columns_specified = &q.fields.as_ref().unwrap();

        // create a mutator if we don't have one for this table already
        let putter = self.inner.get_or_make_mutator(table);
        let schema: Vec<String> = putter.columns().to_vec();

        // handle auto increment
        let ts_lock = self.table_schemas.read().unwrap();
        let table_schema = if let Some(Schema::Table(ref cts)) = ts_lock.get(table) {
            cts
        } else {
            // cannot insert into a view!
            unimplemented!();
        };
        let auto_increment_columns: Vec<_> = table_schema
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
        let mut default_value_columns: Vec<_> = table_schema
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

        let mut buf = vec![vec![DataType::None; schema.len()]; data.len()];

        let mut first_inserted_id = None;
        for (ri, ref row) in data.iter().enumerate() {
            if let Some(col) = auto_increment_columns.iter().next() {
                let idx = schema
                    .iter()
                    .position(|f| *f == col.column.name)
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
                    .iter()
                    .position(|f| *f == c.name)
                    .expect(&format!("no column named '{}'", c.name));
                // only use default value if query doesn't specify one
                if !columns_specified.contains(&c) {
                    buf[ri][idx] = v.into();
                }
            }

            for (ci, c) in columns_specified.iter().enumerate() {
                let idx = schema
                    .iter()
                    .position(|f| *f == c.name)
                    .expect(&format!("no column named '{}'", c.name));
                buf[ri][idx] = row.get(ci).unwrap().clone();
            }
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
                utils::extract_update_params_and_fields(&mut uq, &mut None, table_schema)
            };

            // TODO(malte): why can't I consume buf here?
            putter.insert_or_update(buf[0].clone(), updates)
        } else {
            trace!(self.log, "inserting {:?}", buf);
            putter.batch_insert(buf)
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
        let getter = self.inner.get_or_make_getter(&qname);

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
        let is_bogo = keys.is_empty();
        let keys = if is_bogo { bogo } else { keys };

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
                            let coli = cols.iter().position(|f| f == &c.column).expect(&format!(
                                "tried to emit column {:?} not in getter for {:?} with schema {:?}",
                                c.column, qname, cols
                            ));
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
                    "Soup returned an error".as_bytes(),
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
        let mutator = self.inner.get_or_make_mutator(&q.table.name);

        let q = q.into_owned();
        let (key, updates) = {
            let ts = self.table_schemas.read().unwrap();
            let schema = if let Some(Schema::Table(ref cts)) = ts.get(&q.table.name) {
                cts
            } else {
                // no update on views
                unimplemented!();
            };
            utils::extract_update(q, params, schema)
        };

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

impl<W: io::Write> MysqlShim<W> for SoupBackend {
    fn on_prepare(&mut self, query: &str, info: StatementMetaWriter<W>) -> io::Result<()> {
        trace!(self.log, "prepare: {}", query);

        let query = if self.sanitize {
            utils::sanitize_query(query)
        } else {
            query.to_owned()
        };

        let sql_q = match self.parsed.get(&query) {
            None => match nom_sql::parse_query(&query) {
                Ok(mut sql_q) => {
                    if let SqlQuery::Select(ref mut q) = sql_q {
                        let ts_lock = self.table_schemas.read().unwrap();
                        let table_schemas = &(*ts_lock);
                        rewrite::expand_stars(q, table_schemas)
                    }

                    self.parsed
                        .insert(query.to_owned(), (sql_q.clone(), vec![]));

                    sql_q
                }
                Err(e) => {
                    // if nom-sql rejects the query, there is no chance Soup will like it
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
                // Soup only supports prepared SELECT statements at the moment
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
                    None => vec![
                        params
                            .into_iter()
                            .map(|pv| pv.value.to_datatype())
                            .collect::<Vec<_>>(),
                    ],
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
            // TODO(malte): we actually know what tables exist via self.table_schemas, so
            //              return them here
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
                        if let SqlQuery::Select(ref mut q) = q {
                            let ts_lock = self.table_schemas.read().unwrap();
                            let table_schemas = &(*ts_lock);
                            rewrite::expand_stars(q, table_schemas);
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
                        // if nom-sql rejects the query, there is no chance Soup will like it
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
