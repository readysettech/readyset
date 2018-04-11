use distributary::{ControllerHandle, DataType, Mutator, RemoteGetter, ZookeeperAuthority};

use msql_srv::{self, *};
use nom_sql::{self, ColumnConstraint, ConditionBase, ConditionExpression, ConditionTree,
              CreateTableStatement, FieldExpression, InsertStatement, Literal, Operator,
              SelectStatement, SqlQuery};

use slog;
use std::collections::{BTreeMap, HashMap};
use std::io;
use std::sync::{self, Arc, Mutex};

use convert::ToDataType;
use utils::{self, QueryID};
use rewrite;
use schema::{schema_for_column, schema_for_insert, schema_for_select};

/// Differentiate between different kinds of cached queries.
pub enum Cached {
    /// Prepared statement ID
    Prepared(String, u32),
    /// Name of ad-hoc query view
    AdHoc(String),
}

struct SoupBackendInner {
    soup: ControllerHandle<ZookeeperAuthority>,
    inputs: BTreeMap<String, Mutator>,
    outputs: BTreeMap<String, RemoteGetter>,
}

impl SoupBackendInner {
    fn new(zk_addr: &str, deployment: &str, log: &slog::Logger) -> Self {
        let mut zk_auth = ZookeeperAuthority::new(&format!("{}/{}", zk_addr, deployment));
        zk_auth.log_with(log.clone());

        debug!(log, "Connecting to Soup...",);
        let mut ch = ControllerHandle::new(zk_auth);

        let soup = SoupBackendInner {
            inputs: ch.inputs()
                .into_iter()
                .map(|(n, _)| (n.clone(), ch.get_mutator(&n).unwrap()))
                .collect::<BTreeMap<String, Mutator>>(),
            outputs: ch.outputs()
                .into_iter()
                .map(|(n, _)| (n.clone(), ch.get_getter(&n).unwrap()))
                .collect::<BTreeMap<String, RemoteGetter>>(),
            soup: ch,
        };

        debug!(log, "Connected!");

        soup
    }

    fn get_or_make_mutator<'a, 'b>(&'a mut self, table: &'b str) -> &'a mut Mutator {
        let soup = &mut self.soup;
        self.inputs.entry(table.to_owned()).or_insert_with(|| {
            soup.get_mutator(table)
                .expect(&format!("no table named {}", table))
        })
    }

    fn get_or_make_getter<'a, 'b>(&'a mut self, view: &'b str) -> &'a mut RemoteGetter {
        let soup = &mut self.soup;
        self.outputs.entry(view.to_owned()).or_insert_with(|| {
            soup.get_getter(view)
                .expect(&format!("no view named '{}'", view))
        })
    }
}

pub struct SoupBackend {
    inner: SoupBackendInner,
    log: slog::Logger,

    table_schemas: Arc<Mutex<HashMap<String, CreateTableStatement>>>,
    auto_increments: Arc<Mutex<HashMap<String, u64>>>,

    query_count: Arc<sync::atomic::AtomicUsize>,

    prepared: HashMap<u32, (String, nom_sql::SqlQuery)>,
    prepared_count: u32,

    /// global cache of view endpoints and prepared statements
    cached: Arc<Mutex<HashMap<QueryID, Cached>>>,
    /// thread-local version of `cached` (consulted first)
    tl_cached: HashMap<QueryID, Cached>,
}

impl SoupBackend {
    pub fn new(
        zk_addr: &str,
        deployment: &str,
        schemas: Arc<Mutex<HashMap<String, CreateTableStatement>>>,
        auto_increments: Arc<Mutex<HashMap<String, u64>>>,
        query_cache: Arc<Mutex<HashMap<QueryID, Cached>>>,
        query_counter: Arc<sync::atomic::AtomicUsize>,
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
        }
    }

    fn handle_create_table<W: io::Write>(
        &mut self,
        q: nom_sql::CreateTableStatement,
        results: QueryResultWriter<W>,
    ) -> io::Result<()> {
        match self.inner.soup.extend_recipe(format!("{};", q)) {
            Ok(_) => {
                let mut ts_lock = self.table_schemas.lock().unwrap();
                ts_lock.insert(q.table.name.clone(), q);
                // no rows to return
                // TODO(malte): potentially eagerly cache the mutator for this table
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
        let cond = q.where_clause
            .expect("only supports DELETEs with WHERE-clauses");

        let ts = self.table_schemas.lock().unwrap();
        let pkey = utils::get_primary_key(&ts[&q.table.name])
            .into_iter()
            .map(|(_, c)| c)
            .collect();

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
        let data: Vec<Vec<DataType>> = q.data
            .iter()
            .map(|row| row.iter().map(|v| DataType::from(v)).collect())
            .collect();

        self.do_insert(&q.table.name, &q.fields, data, results)
    }

    fn handle_select<W: io::Write>(
        &mut self,
        q: nom_sql::SelectStatement,
        results: QueryResultWriter<W>,
    ) -> io::Result<()> {
        // TODO(malte): ewww clone
        let qhash = utils::hash_query(&SqlQuery::Select(q.clone()));

        let (qname, locally_cached) = match self.tl_cached.get(&qhash) {
            None => {
                // consult global cache
                let mut gc = self.cached.lock().unwrap();
                let (qname, cached) = match gc.get(&qhash) {
                    None => {
                        let qc = self.query_count
                            .fetch_add(1, sync::atomic::Ordering::SeqCst);
                        let qname = format!("q_{}", qc);
                        // first do a migration to add the query if it doesn't exist already
                        info!(self.log, "Adding ad-hoc query \"{}\" to Soup", q);
                        match self.inner
                            .soup
                            .extend_recipe(format!("QUERY {}: {};", qname, q))
                        {
                            Ok(_) => {
                                // and also in the global cache
                                (qname, false)
                            }
                            Err(e) => return Err(io::Error::new(io::ErrorKind::Other, e)),
                        }
                    }
                    Some(&Cached::AdHoc(ref qname)) => (qname.clone(), false),
                    Some(&Cached::Prepared(_, _)) => unreachable!(),
                };
                if !cached {
                    gc.insert(qhash, Cached::AdHoc(qname.clone()));
                }
                (qname, false)
            }
            Some(&Cached::AdHoc(ref qname)) => (qname.clone(), true),
            Some(&Cached::Prepared(_, _)) => unreachable!(),
        };

        if !locally_cached {
            // remember this query for the future, so we avoid unnecessary migrations
            self.tl_cached.insert(qhash, Cached::AdHoc(qname.clone()));
        }

        self.do_read(&qname, &q, None, results)
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
        mut q: nom_sql::UpdateStatement,
        results: QueryResultWriter<W>,
    ) -> io::Result<()> {
        let cond = q.where_clause
            .expect("only supports UPDATEs with WHERE-clauses");

        let ts = self.table_schemas.lock().unwrap();
        let CreateTableStatement { ref fields, .. } = ts[&q.table.name];
        let pkey = utils::get_primary_key(&ts[&q.table.name]);
        let key_indices: Vec<_> = pkey.iter().map(|&(i, _)| i).collect();
        let key_values: Vec<_> = pkey.iter().map(|&(_, c)| c).collect();

        // Updating rows happens in three steps:
        // 1. Read from Soup by key to get full rows
        // 2. Rewrite the column values specified in SET part of UPDATE clause
        // 3. Write results to Soup, deleting old rows, then putting new ones
        //
        // To accomplish the first step we need to buld a getter that retrieves
        // all the columns in the table:
        let where_clause = Some(ConditionExpression::ComparisonOp(ConditionTree {
            operator: Operator::Equal,
            left: box ConditionExpression::Base(ConditionBase::Field(key_values[0].clone())),
            right: box ConditionExpression::Base(ConditionBase::Literal(Literal::Placeholder)),
        }));

        let select_q = SelectStatement {
            tables: vec![q.table.clone()],
            fields: fields
                .into_iter()
                .map(|cs| FieldExpression::Col(cs.column.clone()))
                .collect(),
            distinct: false,
            join: vec![],
            where_clause,
            group_by: None,
            order: None,
            limit: None,
        };

        // update_columns maps column indices to the value they should be updated to:
        let mut update_columns = HashMap::new();
        for (i, field) in fields.into_iter().enumerate() {
            for &mut (ref mut update_column, ref value) in q.fields.iter_mut() {
                // we must ensure that all columns have their table set because the schema we're
                // comparing against sets it; the parser does not itself guarantee this.
                if update_column.table.is_none() {
                    update_column.table = Some(q.table.name.clone());
                }
                if update_column == &field.column {
                    update_columns.insert(i, DataType::from(value));
                }
            }
        }

        // create a mutator if we don't have one for this table already
        match utils::flatten_conditional(&cond, &key_values) {
            None => results.completed(0, 0),
            Some(ref flattened) if flattened.len() == 0 => {
                panic!("UPDATE only supports WHERE-clauses on primary keys");
            }
            Some(flattened) => {
                let qhash = utils::hash_query(&SqlQuery::Select(select_q.clone()));

                // read rows, adding query if necessary
                let lookup_results = {
                    let getter = match self.tl_cached.get(&qhash) {
                        None => {
                            // not in thread-local cache, check global cache
                            let mut gc = self.cached.lock().unwrap();
                            let (qname, cached) =
                                if let Some(&Cached::AdHoc(ref qname)) = gc.get(&qhash) {
                                    (qname.clone(), true)
                                } else {
                                    let qc = self.query_count
                                        .fetch_add(1, sync::atomic::Ordering::SeqCst);
                                    let qname = format!("q_{}", qc);

                                    (qname, false)
                                };

                            if !cached {
                                info!(
                                    self.log,
                                    "Adding ad-hoc query \"{}\" from update to Soup", select_q
                                );
                                match self.inner
                                    .soup
                                    .extend_recipe(format!("QUERY {}: {};", qname, select_q))
                                {
                                    Ok(_) => {
                                        gc.insert(qhash, Cached::Prepared(qname.clone(), 0));
                                        self.inner.get_or_make_getter(&qname)
                                    }
                                    Err(e) => {
                                        error!(self.log, "update: {:?}", e);
                                        return results.error(
                                            msql_srv::ErrorKind::ER_UNKNOWN_ERROR,
                                            format!("{:?}", e).as_bytes(),
                                        );
                                    }
                                }
                            } else {
                                self.inner.get_or_make_getter(&qname)
                            }
                        }
                        Some(&Cached::AdHoc(ref qname)) => self.inner.get_or_make_getter(&qname),
                        _ => unreachable!(),
                    };

                    match getter.multi_lookup(flattened, true) {
                        Ok(r) => r,
                        Err(e) => {
                            error!(self.log, "update: {:?}", e);
                            return results.error(
                                msql_srv::ErrorKind::ER_UNKNOWN_ERROR,
                                format!("{:?}", e).as_bytes(),
                            );
                        }
                    }
                };

                // multi_lookup gives us a vector of results for each key, i.e.:
                // [
                //   [[1, "bob"], [1, "anne"]],
                //   [[2, "cat"], [1, "dog"]],
                // ]
                //
                // We want to turn that into a flat list of records, and update each record with
                // the new values given in the query:
                let changed_rows: Vec<(Vec<DataType>, Vec<DataType>)> = lookup_results
                    .into_iter()
                    .enumerate()
                    .map(|(_i, result)| {
                        result
                            .into_iter()
                            .filter_map(|row| {
                                let new_row = row.clone()
                                    .into_iter()
                                    .enumerate()
                                    .map(|(i, column)| {
                                        update_columns
                                            .get(&i)
                                            .and_then(|v| Some(v.clone()))
                                            .unwrap_or(column)
                                    })
                                    .collect::<Vec<DataType>>();

                                // Filter out rows with no actual content changes:
                                if row == new_row {
                                    None
                                } else {
                                    // Keep both the old and new row here in case we mutated the
                                    // primary key (if that's the case we'll need to delete by the
                                    // old key, _not_ the new one):
                                    Some((row, new_row))
                                }
                            })
                            .collect::<Vec<(Vec<DataType>, Vec<DataType>)>>()
                    })
                    .flat_map(|r| r)
                    .collect();

                if changed_rows.len() == 0 {
                    // This might happen if there's no actual changes in content.
                    return results.completed(0, 0);
                }

                // Then we want to delete the old rows:
                {
                    let mutator = self.inner.get_or_make_mutator(&q.table.name);
                    let mut new_rows = vec![];
                    for (old_row, new_row) in changed_rows.into_iter() {
                        new_rows.push(new_row);
                        let key: Vec<_> = key_indices.iter().map(|i| old_row[*i].clone()).collect();
                        match mutator.delete(key) {
                            Ok(..) => {}
                            Err(e) => {
                                error!(self.log, "update: {:?}", e);
                                return results.error(
                                    msql_srv::ErrorKind::ER_UNKNOWN_ERROR,
                                    format!("{:?}", e).as_bytes(),
                                );
                            }
                        };
                    }

                    // And finally, insert the updated rows:
                    let count = new_rows.len() as u64;
                    match mutator.multi_put(new_rows) {
                        Ok(..) => results.completed(count, 0),
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
        }
    }

    fn execute_insert<W: io::Write>(
        &mut self,
        q: &InsertStatement,
        data: Vec<DataType>,
        results: QueryResultWriter<W>,
    ) -> io::Result<()> {
        assert_eq!(q.fields.len(), data.len());
        self.do_insert(&q.table.name, &q.fields, vec![data], results)
    }

    fn execute_select<W: io::Write>(
        &mut self,
        qname: &str,
        q: &SelectStatement,
        key: Vec<DataType>,
        results: QueryResultWriter<W>,
    ) -> io::Result<()> {
        self.do_read(qname, q, Some(key), results)
    }

    fn do_insert<W: io::Write>(
        &mut self,
        table: &str,
        columns_specified: &Vec<nom_sql::Column>,
        data: Vec<Vec<DataType>>,
        results: QueryResultWriter<W>,
    ) -> io::Result<()> {
        // create a mutator if we don't have one for this table already
        let putter = self.inner.get_or_make_mutator(table);
        let schema: Vec<String> = putter.columns().to_vec();

        // handle auto increment
        let ts_lock = self.table_schemas.lock().unwrap();
        let auto_increment_columns: Vec<_> = ts_lock[table]
            .fields
            .iter()
            .filter(|c| c.constraints.contains(&ColumnConstraint::AutoIncrement))
            .collect();
        // can only have zero or one AUTO_INCREMENT columns
        assert!(auto_increment_columns.len() <= 1);
        let mut ai_lock = self.auto_increments.lock().unwrap();
        let auto_increment: &mut u64 = &mut (*ai_lock).entry(table.to_owned()).or_insert(0);
        let last_insert_id = *auto_increment + 1;

        // handle default values
        let mut default_value_columns: Vec<_> = ts_lock[table]
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

        for (ri, ref row) in data.iter().enumerate() {
            if let Some(col) = auto_increment_columns.iter().next() {
                let idx = schema
                    .iter()
                    .position(|f| *f == col.column.name)
                    .expect(&format!("no column named '{}'", col.column.name));
                *auto_increment += 1;
                // query can specify an explicit AUTO_INCREMENT value
                if !columns_specified.contains(&col.column) {
                    buf[ri][idx] = DataType::from(*auto_increment as i64);
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

        trace!(self.log, "inserting {:?}", buf);
        match putter.multi_put(buf) {
            Ok(_) => results.completed(data.len() as u64, last_insert_id),
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
        q: &SelectStatement,
        key: Option<Vec<DataType>>,
        results: QueryResultWriter<W>,
    ) -> io::Result<()> {
        // we need the schema for the result writer
        // TODO(malte): cache?
        let schema = {
            let ts_lock = self.table_schemas.lock().unwrap();
            schema_for_select(&(*ts_lock), q)
        };

        // create a getter if we don't have one for this query already
        // TODO(malte): may need to make one anyway if the query has changed w.r.t. an
        // earlier one of the same name
        let getter = self.inner.get_or_make_getter(&qname);

        // if there is no key specified, we "execute" the query via a bogokey lookup
        let (key, is_bogo) = match key {
            None => (vec![DataType::from(0 as i32)], true),
            Some(k) => (k, false),
        };

        let write_column = |rw: &mut RowWriter<W>, c: DataType, cs: &msql_srv::Column| {
            let written = match c {
                DataType::None => rw.write_col(None::<i32>),
                DataType::Int(i) => rw.write_col(i as isize),
                DataType::BigInt(i) => rw.write_col(i as isize),
                DataType::Text(t) => rw.write_col(t.to_str().unwrap()),
                dt @ DataType::TinyText(_) => rw.write_col(dt.to_string()),
                dt @ DataType::Real(_, _) => match cs.coltype {
                    msql_srv::ColumnType::MYSQL_TYPE_DECIMAL => {
                        let f: &str = &dt.to_string();
                        rw.write_col(f)
                    }
                    msql_srv::ColumnType::MYSQL_TYPE_DOUBLE => {
                        let f: f64 = (&dt).into();
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

        match getter.lookup(&key, true) {
            Ok(d) => {
                let num_rows = d.len();
                if num_rows > 0 {
                    let mut rw = results.start(schema.as_slice()).unwrap();
                    for mut r in d {
                        if is_bogo {
                            // drop bogokey
                            r.pop();
                        }
                        for (i, c) in r.into_iter().enumerate() {
                            // XXX TODO XXX
                            // Need to drop parameter columns
                            if i < schema.len() {
                                write_column(&mut rw, c, &schema[i]);
                            }
                        }
                        rw.end_row()?;
                    }
                    rw.finish()
                } else {
                    results.completed(0, 0)
                }
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
}

impl<W: io::Write> MysqlShim<W> for SoupBackend {
    fn on_prepare(&mut self, query: &str, info: StatementMetaWriter<W>) -> io::Result<()> {
        trace!(self.log, "prepare: {}", query);

        let query = utils::sanitize_query(query);

        let ts_lock = self.table_schemas.lock().unwrap();
        let table_schemas = &(*ts_lock);

        match nom_sql::parse_query(&query) {
            Ok(sql_q) => {
                let sql_q = rewrite::expand_stars(sql_q, table_schemas);
                match sql_q {
                    nom_sql::SqlQuery::Select(ref q) => {
                        // extract parameter columns
                        let params: Vec<msql_srv::Column> = utils::get_parameter_columns(&sql_q)
                            .into_iter()
                            .map(|c| schema_for_column(table_schemas, c))
                            .collect();

                        // extract result schema
                        let schema = schema_for_select(table_schemas, &q);

                        // check if we already have this query prepared
                        let qhash = utils::hash_query(&SqlQuery::Select(q.clone()));

                        match self.tl_cached.get(&qhash) {
                            None => {
                                // check global cache
                                let mut gc = self.cached.lock().unwrap();
                                let (qname, cached) =
                                    if let Some(&Cached::Prepared(ref qname, _)) = gc.get(&qhash) {
                                        (qname.clone(), true)
                                    } else {
                                        let qc = self.query_count
                                            .fetch_add(1, sync::atomic::Ordering::SeqCst);
                                        let qname = format!("q_{}", qc);

                                        (qname, false)
                                    };

                                if !cached {
                                    // add the query to Soup
                                    info!(
                                        self.log,
                                        "Adding parameterized query \"{}\" to Soup", query
                                    );
                                    match self.inner
                                        .soup
                                        .extend_recipe(format!("QUERY {}: {};", qname, q))
                                    {
                                        Ok(_) => {
                                            // add to global cache
                                            gc.insert(qhash, Cached::Prepared(qname.clone(), 0));
                                        }
                                        Err(e) => {
                                            return Err(io::Error::new(io::ErrorKind::Other, e))
                                        }
                                    }
                                }
                                // register a new prepared statement
                                self.prepared_count += 1;
                                self.prepared
                                    .insert(self.prepared_count, (qname.clone(), sql_q.clone()));
                                self.tl_cached
                                    .insert(qhash, Cached::Prepared(qname, self.prepared_count));
                                // TODO(malte): proactively get getter, to avoid &mut ref on exec?
                                info.reply(
                                    self.prepared_count,
                                    params.as_slice(),
                                    schema.as_slice(),
                                )
                            }
                            Some(&Cached::Prepared(_, id)) => {
                                // reuse existing prepared statement in this thread
                                info.reply(id, params.as_slice(), schema.as_slice())
                            }
                            Some(&Cached::AdHoc(_)) => unreachable!(),
                        }
                    }
                    nom_sql::SqlQuery::Insert(ref q) => {
                        // extract parameter columns
                        let params: Vec<msql_srv::Column> = utils::get_parameter_columns(&sql_q)
                            .into_iter()
                            .map(|c| {
                                let mut cc = c.clone();
                                cc.table = Some(q.table.name.clone());
                                schema_for_column(table_schemas, &cc)
                            })
                            .collect();

                        // extract result schema
                        let schema = schema_for_insert(table_schemas, &q);

                        // register a new prepared statement
                        self.prepared_count += 1;
                        self.prepared.insert(
                            self.prepared_count,
                            (
                                format!("{}_{}", q.table.name.to_owned(), self.prepared_count),
                                sql_q.clone(),
                            ),
                        );

                        // nothing more to do for an insert
                        // TODO(malte): proactively get mutator?
                        info.reply(self.prepared_count, params.as_slice(), schema.as_slice())
                    }
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
            Err(e) => {
                // if nom-sql rejects the query, there is no chance Soup will like it
                error!(self.log, "query can't be parsed: \"{}\"", query);
                info.error(msql_srv::ErrorKind::ER_PARSE_ERROR, e.as_bytes())
            }
        }
    }

    fn on_execute(
        &mut self,
        id: u32,
        params: ParamParser,
        results: QueryResultWriter<W>,
    ) -> io::Result<()> {
        // TODO(malte): unfortunate clone here, but we can't call execute_select(&mut self) if we
        // have self.prepared borrowed
        if let Some((qname, q)) = self.prepared.get(&id).map(|e| e.clone()) {
            match q {
                nom_sql::SqlQuery::Select(ref q) => {
                    let key: Vec<_> = params
                        .into_iter()
                        .map(|pv| pv.value.to_datatype())
                        .collect();
                    self.execute_select(&qname, &q, key, results)
                }
                nom_sql::SqlQuery::Insert(ref q) => {
                    let values: Vec<DataType> = params
                        .into_iter()
                        .map(|pv| pv.value.to_datatype())
                        .collect();
                    self.execute_insert(&q, values, results)
                }
                _ => unimplemented!(),
            }
        } else {
            results.error(
                msql_srv::ErrorKind::ER_UNKNOWN_ERROR,
                "non-existent statement".as_bytes(),
            )
        }
    }

    fn on_close(&mut self, _: u32) {}

    fn on_query(&mut self, query: &str, results: QueryResultWriter<W>) -> io::Result<()> {
        trace!(self.log, "query: {}", query);

        let query = utils::sanitize_query(query);

        if query.to_lowercase().contains("show databases")
            || query.to_lowercase().starts_with("begin")
            || query.to_lowercase().starts_with("start transaction")
            || query.to_lowercase().starts_with("rollback")
            || query.to_lowercase().starts_with("alter table")
            || query.to_lowercase().starts_with("commit")
            || query.to_lowercase().starts_with("create index")
            || query.to_lowercase().starts_with("create unique index")
            || query.to_lowercase().starts_with("create fulltext index")
        {
            warn!(
                self.log,
                "ignoring unsupported query \"{}\" and returning empty results", query
            );
            return results.completed(0, 0);
        }

        if query.to_lowercase().contains("show tables") {
            let cols = [
                Column {
                    table: String::from(""),
                    column: String::from("Tables"),
                    coltype: ColumnType::MYSQL_TYPE_STRING,
                    colflags: ColumnFlags::empty(),
                },
            ];
            // TODO(malte): we actually know what tables exist via self.table_schemas, so
            //              return them here
            let writer = results.start(&cols)?;
            println!(" -> Ok({} rows)", 0);
            return writer.finish();
        }

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

        match nom_sql::parse_query(&query) {
            Ok(q) => {
                let q = {
                    let ts_lock = self.table_schemas.lock().unwrap();
                    let table_schemas = &(*ts_lock);
                    rewrite::expand_stars(q, table_schemas)
                };

                match q {
                    nom_sql::SqlQuery::CreateTable(q) => self.handle_create_table(q, results),
                    nom_sql::SqlQuery::Insert(q) => self.handle_insert(q, results),
                    nom_sql::SqlQuery::Select(q) => self.handle_select(q, results),
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
                }
            }
            Err(_e) => {
                // if nom-sql rejects the query, there is no chance Soup will like it
                error!(self.log, "query can't be parsed: \"{}\"", query);
                results.completed(0, 0)
                //return results.error(msql_srv::ErrorKind::ER_PARSE_ERROR, e.as_bytes());
            }
        }
    }
}
