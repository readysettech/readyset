use distributary::{ControllerHandle, DataType, Mutator, RemoteGetter, ZookeeperAuthority};

use msql_srv::{self, *};
use nom_sql::{self, ColumnConstraint, ColumnSpecification, ConditionBase, ConditionExpression,
              ConditionTree, FieldExpression, Literal, Operator, SelectStatement, SqlType};
use slog;
use std::io;
use std::collections::{BTreeMap, HashMap};
use std::sync::{self, Arc, Mutex};

use utils;

pub struct SoupBackend {
    soup: ControllerHandle<ZookeeperAuthority>,
    log: slog::Logger,

    inputs: BTreeMap<String, Mutator>,
    outputs: BTreeMap<String, RemoteGetter>,

    table_schemas: Arc<Mutex<HashMap<String, Vec<ColumnSpecification>>>>,
    auto_increments: Arc<Mutex<HashMap<String, u64>>>,

    query_count: Arc<sync::atomic::AtomicUsize>,
}

impl SoupBackend {
    pub fn new(
        zk_addr: &str,
        deployment_id: &str,
        schemas: Arc<Mutex<HashMap<String, Vec<ColumnSpecification>>>>,
        auto_increments: Arc<Mutex<HashMap<String, u64>>>,
        query_counter: Arc<sync::atomic::AtomicUsize>,
        log: slog::Logger,
    ) -> Self {
        let mut zk_auth = ZookeeperAuthority::new(&format!("{}/{}", zk_addr, deployment_id));
        zk_auth.log_with(log.clone());

        debug!(log, "Connecting to Soup...",);
        let mut ch = ControllerHandle::new(zk_auth);

        let inputs = ch.inputs()
            .into_iter()
            .map(|(n, _)| (n.clone(), ch.get_mutator(&n).unwrap()))
            .collect::<BTreeMap<String, Mutator>>();
        let outputs = ch.outputs()
            .into_iter()
            .map(|(n, _)| (n.clone(), ch.get_getter(&n).unwrap()))
            .collect::<BTreeMap<String, RemoteGetter>>();

        debug!(log, "Connected!");

        SoupBackend {
            soup: ch,
            log: log,

            inputs: inputs,
            outputs: outputs,

            table_schemas: schemas,
            auto_increments: auto_increments,

            query_count: query_counter,
        }
    }

    fn schema_for_column(&self, c: &nom_sql::Column) -> msql_srv::Column {
        let table = c.table.as_ref().unwrap();
        let ts_lock = self.table_schemas.lock().unwrap();
        let col_schema = &(*ts_lock)[table]
            .iter()
            .find(|cc| cc.column.name == c.name)
            .expect(&format!("column {} not found", c.name));
        assert_eq!(col_schema.column.name, c.name);

        msql_srv::Column {
            table: table.clone(),
            column: c.name.clone(),
            coltype: match col_schema.sql_type {
                SqlType::Longtext => msql_srv::ColumnType::MYSQL_TYPE_BLOB,
                SqlType::Text => msql_srv::ColumnType::MYSQL_TYPE_STRING,
                SqlType::Varchar(_) => msql_srv::ColumnType::MYSQL_TYPE_VAR_STRING,
                SqlType::Int(_) => msql_srv::ColumnType::MYSQL_TYPE_LONG,
                SqlType::DateTime => msql_srv::ColumnType::MYSQL_TYPE_DATETIME,
                SqlType::Bool => msql_srv::ColumnType::MYSQL_TYPE_TINY,
                _ => unimplemented!(),
            },
            colflags: {
                let mut flags = msql_srv::ColumnFlags::empty();
                for c in &col_schema.constraints {
                    match *c {
                        ColumnConstraint::AutoIncrement => {
                            flags |= msql_srv::ColumnFlags::AUTO_INCREMENT_FLAG;
                        }
                        ColumnConstraint::NotNull => {
                            flags |= msql_srv::ColumnFlags::NOT_NULL_FLAG;
                        }
                        ColumnConstraint::PrimaryKey => {
                            flags |= msql_srv::ColumnFlags::PRI_KEY_FLAG;
                        }
                        ColumnConstraint::Unique => {
                            flags |= msql_srv::ColumnFlags::UNIQUE_KEY_FLAG;
                        }
                        _ => (),
                    }
                }
                flags
            },
        }
    }

    fn handle_create_table<W: io::Write>(
        &mut self,
        q: nom_sql::CreateTableStatement,
        results: QueryResultWriter<W>,
    ) -> io::Result<()> {
        match self.soup.extend_recipe(format!("{};", q)) {
            Ok(_) => {
                let mut ts_lock = self.table_schemas.lock().unwrap();
                // TODO(ekmartin): handle q.keys as well:
                // CREATE TABLE t (id int, PRIMARY KEY (id))
                // will result in t not having any constraints,
                // but instead have a PrimaryKey under q.keys.
                ts_lock.insert(q.table.name.clone(), q.fields);
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

        {
            let ts = self.table_schemas.lock().unwrap();
            let pkey: Vec<_> = ts.get(&q.table.name)
                .unwrap()
                .into_iter()
                .filter(|cs| cs.constraints.contains(&ColumnConstraint::PrimaryKey))
                .map(|cs| &cs.column)
                .collect();

            assert!(
                utils::ensure_pkey_condition(&cond, &pkey),
                "DELETE with non-primary-key WHERE-clause is not supported"
            );
        }

        // create a mutator if we don't have one for this table already
        let mutator = self.inputs
            .entry(q.table.name.clone())
            .or_insert(self.soup.get_mutator(&q.table.name).unwrap());

        match utils::flatten_conditional(&cond) {
            None => results.completed(0, 0),
            Some(flattened) => {
                let mut deleted = 0;
                for key in flattened {
                    match mutator.delete(vec![key]) {
                        Ok(_) => deleted += 1,
                        Err(e) => {
                            error!(self.log, "delete error: {:?}", e);
                            return results.error(
                                msql_srv::ErrorKind::ER_UNKNOWN_ERROR,
                                format!("{:?}", e).as_bytes(),
                            );
                        }
                    };
                }

                results.completed(deleted, 0)
            }
        }
    }

    fn handle_insert<W: io::Write>(
        &mut self,
        q: nom_sql::InsertStatement,
        results: QueryResultWriter<W>,
    ) -> io::Result<()> {
        let table = q.table.name.clone();

        // create a mutator if we don't have one for this table already
        let putter = self.inputs
            .entry(table.clone())
            .or_insert(self.soup.get_mutator(&table).unwrap());

        let schema: Vec<String> = putter.columns().to_vec();
        let mut data: Vec<Vec<DataType>> =
            vec![vec![DataType::from(0 as i32); schema.len()]; q.data.len()];

        let ts_lock = self.table_schemas.lock().unwrap();
        let auto_increment_columns: Vec<_> = ts_lock[&table]
            .iter()
            .filter(|c| c.constraints.contains(&ColumnConstraint::AutoIncrement))
            .collect();

        // can only have zero or one AUTO_INCREMENT columns
        assert!(auto_increment_columns.len() <= 1);

        let mut ai_lock = self.auto_increments.lock().unwrap();
        let auto_increment: &mut u64 = &mut (*ai_lock).entry(table.clone()).or_insert(0);
        let last_insert_id = *auto_increment + 1;

        for (ri, ref row) in q.data.iter().enumerate() {
            if let Some(col) = auto_increment_columns.iter().next() {
                let idx = schema
                    .iter()
                    .position(|f| *f == col.column.name)
                    .expect(&format!("no column named '{}'", col.column.name));
                *auto_increment += 1;
                data[ri][idx] = DataType::from(*auto_increment as i64);
            }

            for (ci, c) in q.fields.iter().enumerate() {
                let idx = schema
                    .iter()
                    .position(|f| *f == c.name)
                    .expect(&format!("no column named '{}'", c.name));
                data[ri][idx] = DataType::from(row.get(ci).unwrap());
            }
        }

        match putter.multi_put(data) {
            Ok(_) => {
                // XXX(malte): last_insert_id needs to be set correctly
                // Could we have put more than one row?
                results.completed(q.data.len() as u64, last_insert_id)
            }
            Err(e) => {
                error!(self.log, "put error: {:?}", e);
                results.error(
                    msql_srv::ErrorKind::ER_UNKNOWN_ERROR,
                    format!("{:?}", e).as_bytes(),
                )
            }
        }
    }

    fn handle_select<W: io::Write>(
        &mut self,
        q: nom_sql::SelectStatement,
        results: QueryResultWriter<W>,
    ) -> io::Result<()> {
        let qc = self.query_count
            .fetch_add(1, sync::atomic::Ordering::SeqCst);
        let qname = format!("q_{}", qc);

        // first do a migration to add the query if it doesn't exist already
        match self.soup.extend_recipe(format!("QUERY {}: {};", qname, q)) {
            Ok(_) => {
                let mut schema: Vec<msql_srv::Column> = Vec::new();
                for fe in q.fields {
                    match fe {
                        nom_sql::FieldExpression::Col(c) => {
                            schema.push(self.schema_for_column(&c));
                        }
                        nom_sql::FieldExpression::Literal(le) => schema.push(msql_srv::Column {
                            table: "".to_owned(),
                            column: match le.alias {
                                Some(a) => a,
                                None => le.value.to_string(),
                            },
                            coltype: match le.value {
                                Literal::Integer(_) => msql_srv::ColumnType::MYSQL_TYPE_LONG,
                                Literal::String(_) => msql_srv::ColumnType::MYSQL_TYPE_VAR_STRING,
                                _ => unimplemented!(),
                            },
                            colflags: msql_srv::ColumnFlags::empty(),
                        }),
                        _ => unimplemented!(),
                    }
                }

                // create a getter if we don't have one for this query already
                // TODO(malte): may need to make one anyway if the query has changed w.r.t. an
                // earlier one of the same name?
                let getter = self.outputs.entry(qname.clone()).or_insert(
                    self.soup
                        .get_getter(&qname)
                        .expect(&format!("no view named '{}'", qname)),
                );

                // now "execute" the query via a bogokey lookup
                match getter.lookup(&DataType::from(0 as i32), true) {
                    Ok(d) => {
                        let num_rows = d.len();
                        if num_rows > 0 {
                            let mut rw = results.start(schema.as_slice()).unwrap();
                            for mut r in d {
                                // drop bogokey
                                r.pop();
                                for c in r {
                                    match c {
                                        DataType::Int(i) => rw.write_col(i as i32)?,
                                        DataType::BigInt(i) => rw.write_col(i as i64)?,
                                        DataType::Text(t) => rw.write_col(t.to_str().unwrap())?,
                                        dt @ DataType::TinyText(_) => rw.write_col(dt.to_string())?,
                                        _ => unimplemented!(),
                                    }
                                }
                                rw.end_row()?;
                            }
                            rw.finish()
                        } else {
                            results.completed(0, 0)
                        }
                    }
                    Err(_) => {
                        error!(self.log, "error executing SELECT");
                        results.error(
                            msql_srv::ErrorKind::ER_UNKNOWN_ERROR,
                            "Soup returned an error".as_bytes(),
                        )
                    }
                }
            }
            Err(e) => Err(io::Error::new(io::ErrorKind::Other, e)),
        }
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
        let cond = q.where_clause
            .expect("only supports DELETEs with WHERE-clauses");

        let ts = self.table_schemas.lock().unwrap();
        let fields = ts.get(&q.table.name).unwrap();
        let pkey: Vec<_> = fields
            .into_iter()
            .filter(|cs| cs.constraints.contains(&ColumnConstraint::PrimaryKey))
            .map(|cs| &cs.column)
            .collect();

        assert!(
            utils::ensure_pkey_condition(&cond, &pkey),
            "DELETE with non-primary-key WHERE-clause is not supported"
        );

        // Updating rows happens in three steps:
        // 1. Read from Soup by key to get full rows
        // 2. Rewrite the column values specified in SET part of UPDATE clause
        // 3. Write results to Soup, deleting old rows, then putting new ones
        //
        // To accomplish the first step we need to buld a getter that retrieves
        // all the columns in the table:
        let where_clause = Some(ConditionExpression::ComparisonOp(ConditionTree {
            operator: Operator::Equal,
            left: box ConditionExpression::Base(ConditionBase::Field(pkey[0].clone())),
            right: box ConditionExpression::Base(ConditionBase::Placeholder),
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

        // Note that this only works for single column primary keys for now.
        let key_index = fields
            .iter()
            .position(|ref cs| cs.constraints.contains(&ColumnConstraint::PrimaryKey))
            .unwrap(); // We already know we have a PK at this point.

        // update_columns maps column indices to the value they should be updated to:
        let mut update_columns = HashMap::new();
        for (i, field) in fields.into_iter().enumerate() {
            for &(ref update_column, ref value) in q.fields.iter() {
                if update_column == &field.column {
                    update_columns.insert(i, DataType::from(value));
                }
            }
        }

        // create a mutator if we don't have one for this table already
        let mutator = self.inputs
            .entry(q.table.name.clone())
            .or_insert(self.soup.get_mutator(&q.table.name).unwrap());

        match utils::flatten_conditional(&cond) {
            None => results.completed(0, 0),
            Some(flattened) => {
                let qc = self.query_count
                    .fetch_add(1, sync::atomic::Ordering::SeqCst);
                let qname = format!("q_{}", qc);
                // TODO(ekmartin): does this create an extra set of nodes?
                // if so we should cache the getter
                let getter = match self.soup
                    .extend_recipe(format!("QUERY {}: {};", qname, select_q))
                {
                    Ok(_) => self.outputs.entry(qname.clone()).or_insert(
                        self.soup
                            .get_getter(&qname)
                            .expect(&format!("no view named '{}'", qname)),
                    ),
                    Err(e) => {
                        error!(self.log, "update: {:?}", e);
                        return results.error(
                            msql_srv::ErrorKind::ER_UNKNOWN_ERROR,
                            format!("{:?}", e).as_bytes(),
                        );
                    }
                };

                let keys = flattened.into_iter().collect();
                let lookup_results = match getter.multi_lookup(keys, true) {
                    Ok(r) => r,
                    Err(e) => {
                        error!(self.log, "update: {:?}", e);
                        return results.error(
                            msql_srv::ErrorKind::ER_UNKNOWN_ERROR,
                            format!("{:?}", e).as_bytes(),
                        );
                    }
                };

                // multi_lookup gives us a vector of results for each key, i.e.:
                // [
                //   [[1, "bob"], [1, "anne"]],
                //   [[2, "cat"], [1, "dog"]],
                // ]
                // We want to turn that into a flat list of records, and update each record with
                // the new values given in the query:
                let updated_rows: Vec<Vec<DataType>> = lookup_results
                    .into_iter()
                    .map(|result| {
                        result
                            .into_iter()
                            .filter_map(|row| {
                                let mut has_changed = false;
                                let new_row = row.into_iter()
                                    .enumerate()
                                    .map(|(i, column)| {
                                        update_columns
                                            .get(&i)
                                            .and_then(|v| {
                                                if v != &column {
                                                    has_changed = true;
                                                    Some(v.clone())
                                                } else {
                                                    None
                                                }
                                            })
                                            .unwrap_or(column)
                                    })
                                    .collect::<Vec<DataType>>();

                                // Filter out rows with no actual content changes:
                                if has_changed {
                                    Some(new_row)
                                } else {
                                    None
                                }
                            })
                            .collect::<Vec<Vec<DataType>>>()
                    })
                    .flat_map(|r| r)
                    .collect();

                if updated_rows.len() == 0 {
                    // This might happen if there's no actual changes in content.
                    return results.completed(0, 0);
                }

                // Then we want to delete the old rows:
                for row in updated_rows.iter() {
                    let key = vec![row[key_index].clone()];
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
                let count = updated_rows.len() as u64;
                match mutator.multi_put(updated_rows) {
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

impl<W: io::Write> MysqlShim<W> for SoupBackend {
    fn on_prepare(&mut self, query: &str, info: StatementMetaWriter<W>) -> io::Result<()> {
        error!(self.log, "prepare: {}", query);
        info.reply(42, &[], &[])
    }

    fn on_execute(
        &mut self,
        id: u32,
        _: ParamParser,
        results: QueryResultWriter<W>,
    ) -> io::Result<()> {
        error!(self.log, "exec: {}", id);
        results.completed(0, 0)
    }

    fn on_close(&mut self, _: u32) {}

    fn on_query(&mut self, query: &str, results: QueryResultWriter<W>) -> io::Result<()> {
        debug!(self.log, "query: {}", query);

        let query = utils::sanitize_query(query);

        if query.to_lowercase().contains("show databases")
            || query.to_lowercase().starts_with("begin")
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
            Ok(q) => match q {
                nom_sql::SqlQuery::CreateTable(q) => self.handle_create_table(q, results),
                nom_sql::SqlQuery::Insert(q) => self.handle_insert(q, results),
                nom_sql::SqlQuery::Select(q) => self.handle_select(q, results),
                nom_sql::SqlQuery::Set(q) => self.handle_set(q, results),
                nom_sql::SqlQuery::Update(q) => self.handle_update(q, results),
                nom_sql::SqlQuery::Delete(q) => self.handle_delete(q, results),
                _ => {
                    error!(self.log, "Unsupported query: {}", query);
                    return results.error(
                        msql_srv::ErrorKind::ER_NOT_SUPPORTED_YET,
                        "unsupported query".as_bytes(),
                    );
                }
            },
            Err(_e) => {
                // if nom-sql rejects the query, there is no chance Soup will like it
                error!(self.log, "query can't be parsed: \"{}\"", query);
                results.completed(0, 0)
                //return results.error(msql_srv::ErrorKind::ER_PARSE_ERROR, e.as_bytes());
            }
        }
    }
}
