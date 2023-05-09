#![feature(trace_macros)]

//! This test suite implements the [Vertical Testing Design Doc][doc].
//!
//! [doc]: https://docs.google.com/document/d/1rTDzd4Z5jSUDqGmIu2C7R06f2HkNWxEll33-rF4WC-c
//!
//! Note that this test suite is ignored by default, and conditionally de-ignored with the
//! `vertical_tests` feature to prevent it running in normal builds (since it's slow and may find
//! new bugs); to run it locally run:
//!
//! ```notrust
//! cargo test -p readyset-mysql --features vertical_tests --test vertical
//! ```
//!
//! This test suite will connect to a local mysql database, which can be set up with all the correct
//! configuration using the `docker-compose.yml` and `docker-compose.override.example.yml` in the
//! root of the repository. To run that mysql database, run:
//!
//! ```notrust
//! $ cp docker-compose.override.example.yml docker-compose.yml
//! $ docker-compose up -d mysql
//! ```
//!
//! Note that this test suite requires the *exact* configuration specified in that docker-compose
//! configuration, including the port, username, and password.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::time::Duration;
use std::{cmp, env};

use async_trait::async_trait;
use itertools::Itertools;
use mysql_async::prelude::Queryable;
use mysql_async::Conn;
use mysql_common::value::Value;
use proptest::prelude::*;
use proptest::sample::select;
use readyset_client_test_helpers::mysql_helpers::MySQLAdapter;
use readyset_client_test_helpers::TestBuilder;
use readyset_data::DfValue;
use readyset_server::Handle;
use readyset_util::shutdown::ShutdownSender;
use stateful_proptest::{ModelState, StatefulProptestConfig};

#[derive(Clone, Debug, PartialEq, Eq)]
enum Operation {
    Query {
        key: Vec<DfValue>,
    },
    Insert {
        table: &'static str,
        row: Vec<DfValue>,
    },
    Update {
        table: &'static str,
        old_row: Vec<DfValue>,
        new_row: Vec<DfValue>,
    },
    Delete {
        table: &'static str,
        row: Vec<DfValue>,
    },
    /* TODO: coming soon
    Evict {
        /// *seed* for the node index to evict from.
        ///
        /// Note that we don't know how many nodes a query will have until after we install it in
        /// ReadySet, so the actual node index will be this modulo the number of non-base-table
        /// nodes
        node_seed: usize,
        key: Vec<DfValue>,
    },
    */
}

#[derive(Debug, Clone)]
pub enum ColumnStrategy {
    Value(BoxedStrategy<DfValue>),
    ForeignKey {
        table: &'static str,
        foreign_column: usize,
    },
}

#[derive(Debug, Clone)]
pub struct RowStrategy(Vec<ColumnStrategy>);

impl RowStrategy {
    fn no_foreign_keys(self) -> Option<Vec<BoxedStrategy<DfValue>>> {
        self.0
            .into_iter()
            .map(|cs| match cs {
                ColumnStrategy::Value(strat) => Some(strat),
                ColumnStrategy::ForeignKey { .. } => None,
            })
            .collect()
    }

    fn has_foreign_keys(&self) -> bool {
        self.0
            .iter()
            .any(|cs| matches!(cs, ColumnStrategy::ForeignKey { .. }))
    }

    fn fill_foreign_keys<F, S>(self, foreign_key_strategy: F) -> Option<Vec<BoxedStrategy<DfValue>>>
    where
        F: Fn(&'static str, usize) -> Option<S>,
        S: Strategy<Value = DfValue> + Sized + 'static,
    {
        self.0
            .into_iter()
            .map(move |cs| match cs {
                ColumnStrategy::Value(strat) => Some(strat),
                ColumnStrategy::ForeignKey {
                    table,
                    foreign_column,
                } => foreign_key_strategy(table, foreign_column).map(|s| s.boxed()),
            })
            .collect()
    }
}

#[derive(Clone, Debug, Default)]
struct DataflowModelState<T>
where
    T: TestDef + 'static,
{
    rows: HashMap<&'static str, Vec<Vec<DfValue>>>,
    _test_def: PhantomData<T>,
}

struct RunContext {
    mysql: Conn,
    readyset: Conn,
    shutdown_tx: Option<ShutdownSender>, // Needs to be Option so we can move it out of the struct
    _handle: Handle,
}

impl<'a, T> DataflowModelState<T>
where
    T: TestDef,
{
    /// Return an iterator over query lookup keys composed of all possible combination of lookup
    /// keys from existing rows in tables.
    fn existing_keys(&'a self) -> impl Iterator<Item = Vec<DfValue>> + 'a {
        // The reason we use [`multi_cartesian_product`] is that we're trying to combine different
        // rows from different tables to create (potentially) new keys. Keys created in this
        // way may or may not actually return any results when queried on, but by trying to
        // specifically query using keys based off of actual rows from existing tables, we
        // increase our chances of getting interesting query results when using those keys.
        self.rows
            .iter()
            .map(|(tbl, rows)| rows.iter().map(|r| (tbl, r)).collect::<Vec<_>>())
            .multi_cartesian_product()
            .filter_map(|vals| {
                T::key_columns()
                    .iter()
                    .map(|(tbl, idx)| Some(vals.iter().find(|(t, _)| tbl == *t)?.1[*idx].clone()))
                    .collect::<Option<Vec<_>>>()
            })
    }

    /// Return a proptest [`Strategy`] for generating new keys for the query
    fn key_strategy(&self) -> impl Strategy<Value = Vec<DfValue>> {
        T::key_columns()
            .into_iter()
            .map(move |(t, idx)| {
                T::row_strategies()[t]
                    .clone()
                    .no_foreign_keys()
                    .expect("foreign key can't be a key_column")
                    .prop_map(move |mut r| r.remove(idx))
                    .boxed()
            })
            .chain(T::extra_key_strategies())
            .collect::<Vec<_>>()
    }
}

#[async_trait(?Send)]
impl<T> ModelState for DataflowModelState<T>
where
    T: TestDef,
{
    type Operation = Operation;
    type RunContext = RunContext;
    type OperationStrategy = BoxedStrategy<Operation>;

    fn op_generators(&self) -> Vec<Self::OperationStrategy> {
        let mut res = vec![];

        // We can always insert a row into a table or query a randomly generated key, so include
        // those two strategies no matter what. Later in this function we may conditionally add more
        // strategies.
        let no_fk_strategies = T::row_strategies()
            .iter()
            .filter_map(|(k, v)| Some((*k, v.clone().no_foreign_keys()?)))
            .collect::<Vec<_>>();

        let insert_strategy = select(no_fk_strategies)
            .prop_flat_map(|(table, row_strat)| {
                row_strat.prop_map(move |row| Operation::Insert { table, row })
            })
            .boxed();

        res.push(insert_strategy);

        let random_key_query_strat = self
            .key_strategy()
            .prop_map(|key| Operation::Query { key })
            .boxed();

        res.push(random_key_query_strat);

        let existing_keys = self.existing_keys().collect::<Vec<_>>();

        if !existing_keys.is_empty() {
            // If there are any existing keys we can use to generate queries on those keys, do so:
            let extra_key_strategies = T::extra_key_strategies();
            let existing_key_query_strat = select(existing_keys)
                .prop_flat_map(move |key| {
                    key.into_iter()
                        .map(|val| Just(val).boxed())
                        .chain(extra_key_strategies.clone())
                        .collect::<Vec<_>>()
                })
                .prop_map(|key| Operation::Query { key })
                .boxed();

            res.push(existing_key_query_strat);
        }

        let rows_is_empty = self.rows.values().all(|rows| rows.is_empty());
        if !rows_is_empty {
            let rows = self
                .rows
                .iter()
                .flat_map(|(table, rows)| rows.iter().map(|r| (*table, r.clone())))
                .collect::<Vec<_>>();
            let rows2 = rows.clone(); // Cloned for move into closure
            let fill_foreign_keys = move |row_strategy: RowStrategy| {
                row_strategy.fill_foreign_keys(|table, col| {
                    let vals = rows2
                        .iter()
                        .filter(|(t, _)| table == *t)
                        .map(|(_, r)| r[col].clone())
                        .collect::<Vec<_>>();
                    if vals.is_empty() {
                        None
                    } else {
                        Some(select(vals))
                    }
                })
            };

            let fk_tables = T::row_strategies()
                .iter()
                .filter(|(_, v)| v.has_foreign_keys())
                .map(|(t, v)| (*t, v.clone()))
                .collect::<Vec<_>>();
            let fill_foreign_keys2 = fill_foreign_keys.clone(); // Cloned for move into closure
            if !fk_tables.is_empty() {
                let mk_fk_insert = select(fk_tables)
                    .prop_filter_map("No previously-generated rows", move |(table, row_strat)| {
                        Some(
                            fill_foreign_keys2(row_strat)?
                                .prop_map(move |row| Operation::Insert { table, row }),
                        )
                    })
                    .prop_flat_map(|s| s)
                    .boxed();
                res.push(mk_fk_insert);
            }

            let mk_update = select(rows.clone())
                .prop_flat_map(move |(table, old_row)| {
                    (fill_foreign_keys(T::row_strategies()[table].clone())
                        .unwrap()
                        .into_iter()
                        .zip(old_row.clone())
                        .map(|(new_val, old_val)| prop_oneof![Just(old_val), new_val])
                        .collect::<Vec<_>>())
                    .prop_filter_map("No-op update", move |new_row| {
                        (*old_row != new_row).then(|| Operation::Update {
                            table,
                            old_row: old_row.clone(),
                            new_row,
                        })
                    })
                })
                .boxed();

            res.push(mk_update);

            let mk_delete = select(rows)
                .prop_map(move |(table, row)| Operation::Delete { table, row })
                .boxed();

            res.push(mk_delete);
        }

        res
    }

    fn preconditions_met(&self, _op: &Self::Operation) -> bool {
        true // TODO
    }

    fn next_state(&mut self, op: &Self::Operation) {
        match op {
            Operation::Insert { table, row } => {
                let table_rows = self.rows.entry(table).or_default();
                table_rows.push(row.clone())
            }
            Operation::Update {
                table,
                old_row,
                new_row,
            } => {
                let table_rows = self.rows.entry(table).or_default();
                let old_state_row = table_rows.iter_mut().find(|r| *r == old_row).unwrap();
                *old_state_row = new_row.clone();
            }
            Operation::Delete { table, row } => {
                let table_rows = self.rows.entry(table).or_default();
                let row_pos = table_rows.iter().position(|r| r == row).unwrap();
                table_rows.swap_remove(row_pos);
            }
            Operation::Query { .. } => (),
        }
    }

    async fn init_test_run(&self) -> Self::RunContext {
        let tables = T::test_tables();

        readyset_tracing::init_test_logging();
        readyset_client_test_helpers::mysql_helpers::recreate_database("vertical").await;
        let mut mysql = mysql_async::Conn::new(
            mysql_async::OptsBuilder::default()
                .user(Some("root"))
                .pass(Some("noria"))
                .ip_or_hostname(env::var("MYSQL_HOST").unwrap_or_else(|_| "127.0.0.1".into()))
                .tcp_port(
                    env::var("MYSQL_TCP_PORT")
                        .unwrap_or_else(|_| "3306".into())
                        .parse()
                        .unwrap(),
                )
                .db_name(Some("vertical")),
        )
        .await
        .unwrap();

        mysql
            .query_drop(
                "ALTER DATABASE vertical DEFAULT CHARACTER SET utf8mb4 DEFAULT COLLATE utf8mb4_0900_bin",
            )
            .await
            .unwrap();

        let (opts, handle, shutdown_tx) = TestBuilder::default().build::<MySQLAdapter>().await;
        let mut readyset = mysql_async::Conn::new(opts).await.unwrap();

        for table in tables.values() {
            mysql.query_drop(table.create_statement).await.unwrap();
            readyset.query_drop(table.create_statement).await.unwrap();
        }

        RunContext {
            mysql,
            readyset,
            _handle: handle,
            shutdown_tx: Some(shutdown_tx),
        }
    }

    async fn run_op(&self, op: &Self::Operation, ctxt: &mut Self::RunContext) {
        let query = T::test_query();
        let tables = T::test_tables();

        let RunContext {
            mysql, readyset, ..
        } = ctxt;

        let mysql_res = op.run(mysql, query, &tables).await.unwrap();

        /* TODO: figure out a way to reintroduce this logic, if needed:
        // skip tests where mysql returns an error for the operations
        prop_assume!(
            !mysql_res.is_err(),
            "MySQL returned an error: {}",
            mysql_res.err().unwrap()
        );
        */

        let readyset_res = op.run(readyset, query, &tables).await.unwrap();
        assert_eq!(mysql_res, readyset_res);
    }

    async fn check_postconditions(&self, _ctxt: &mut Self::RunContext) {
        // No universal postconditions defined for this test
    }

    async fn clean_up_test_run(&self, ctxt: &mut RunContext) {
        ctxt.shutdown_tx.take().unwrap().shutdown().await
    }
}

#[derive(Debug)]
struct Table {
    name: &'static str,
    create_statement: &'static str,
    primary_key: usize,
    columns: Vec<&'static str>,
}

impl Table {
    fn primary_key_column(&self) -> &'static str {
        self.columns[self.primary_key]
    }
}

/// The result of running a single [`Operation`] against either MySQL or ReadySet.
#[derive(Debug)]
pub enum OperationResult {
    Err(mysql_async::Error),
    Rows(Vec<mysql_async::Row>),
    NoResults,
}

impl OperationResult {
    /// Returns `true` if the operation result is [`Err`].
    ///
    /// [`Err`]: OperationResult::Err
    pub fn is_err(&self) -> bool {
        matches!(self, Self::Err(..))
    }

    pub fn err(&self) -> Option<&mysql_async::Error> {
        match self {
            Self::Err(v) => Some(v),
            _ => None,
        }
    }
}

fn compare_rows(r1: &mysql_async::Row, r2: &mysql::Row) -> Ordering {
    let l = cmp::min(r1.len(), r2.len());
    for i in 0..l {
        match r1.as_ref(i).unwrap().partial_cmp(r2.as_ref(i).unwrap()) {
            Some(Ordering::Equal) => {}
            Some(non_equal) => return non_equal,
            None => {}
        }
    }
    r1.len().cmp(&r2.len())
}

impl PartialEq for OperationResult {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            // TODO: is it worth trying to check error equality here? probably not
            (Self::Err(_), Self::Err(_)) => true,
            (Self::Rows(rs1), Self::Rows(rs2)) => {
                rs1.len() == rs2.len() && {
                    let mut rs1 = rs1.iter().collect::<Vec<_>>();
                    rs1.sort_by(|r1, r2| compare_rows(r1, r2));
                    let mut rs2 = rs2.iter().collect::<Vec<_>>();
                    rs2.sort_by(|r1, r2| compare_rows(r1, r2));
                    rs1.iter().zip(rs2).all(|(r1, r2)| {
                        r1.len() == r2.len()
                            && (0..r1.len()).all(|i| r1.as_ref(i).unwrap() == r2.as_ref(i).unwrap())
                    })
                }
            }
            (Self::NoResults, Self::NoResults) => true,
            _ => false,
        }
    }
}

impl From<mysql_async::Result<Vec<mysql::Row>>> for OperationResult {
    fn from(res: mysql_async::Result<Vec<mysql::Row>>) -> Self {
        match res {
            Ok(rows) => Self::Rows(rows),
            Err(e) => Self::Err(e),
        }
    }
}

impl From<mysql_async::Result<()>> for OperationResult {
    fn from(res: mysql_async::Result<()>) -> Self {
        match res {
            Ok(()) => Self::NoResults,
            Err(e) => Self::Err(e),
        }
    }
}

impl Operation {
    async fn run(
        &self,
        conn: &mut mysql_async::Conn,
        query: &str,
        tables: &HashMap<&str, Table>,
    ) -> Result<OperationResult, TestCaseError> {
        fn to_values(dts: &[DfValue]) -> Result<Vec<Value>, TestCaseError> {
            dts.iter()
                .map(|dt| {
                    Value::try_from(dt.clone()).map_err(|e| {
                        TestCaseError::reject(format!(
                            "DfValue conversion to mysql Value failed: {}",
                            e
                        ))
                    })
                })
                .collect()
        }

        match self {
            Operation::Query { key } => Ok(conn
                .exec::<mysql_async::Row, _, _>(query, mysql::Params::Positional(to_values(key)?))
                .await
                .into()),
            Operation::Insert { table, row } => Ok(conn
                .exec_drop(
                    format!(
                        "INSERT INTO {} VALUES ({})",
                        table,
                        (0..row.len()).map(|_| "?").join(",")
                    ),
                    to_values(row)?,
                )
                .await
                .into()),
            Operation::Update {
                table: table_name,
                old_row,
                new_row,
            } => {
                let table = &tables[table_name];
                let updates = table
                    .columns
                    .iter()
                    .zip(old_row)
                    .zip(new_row)
                    .filter_map(|((col_name, old_val), new_val)| {
                        (old_val != new_val).then_some((col_name, new_val))
                    })
                    .collect::<Vec<_>>();
                let set_clause = updates
                    .iter()
                    .map(|(col_name, _)| format!("{} = ?", col_name))
                    .join(",");
                let mut params = updates
                    .into_iter()
                    .map(|(_, val)| Value::try_from(val.clone()).unwrap())
                    .collect::<Vec<_>>();
                params.push(old_row[table.primary_key].clone().try_into().unwrap());
                Ok(conn
                    .exec_drop(
                        format!(
                            "UPDATE {} SET {} WHERE {} = ?",
                            table.name,
                            set_clause,
                            table.primary_key_column()
                        ),
                        params,
                    )
                    .await
                    .into())
            }
            Operation::Delete {
                table: table_name,
                row,
            } => {
                let table = &tables[table_name];
                Ok(conn
                    .exec_drop(
                        format!(
                            "DELETE FROM {} WHERE {} = ?",
                            table.name,
                            table.primary_key_column()
                        ),
                        (Value::try_from(row[table.primary_key].clone()).unwrap(),),
                    )
                    .await
                    .into())
            }
        }
    }
}

/*
macro_rules! vertical_tests {
    ($(#[$meta:meta])* $name:ident($($params: tt)*); $($rest: tt)*) => {
        vertical_tests!(@test $(#[$meta])* $name($($params)*));
        vertical_tests!($($rest)*);
    };

    // define the test itself
    (@test $(#[$meta:meta])* $name:ident($query: expr; $options: expr; $($tables: tt)*)) => {
        paste! {
        fn [<$name _generate_ops>]() -> impl Strategy<Value = Operations> {
            let size_range = 1..100; // TODO make configurable
            let row_strategies = vertical_tests!(@row_strategies $($tables)*);
            let key_columns = vertical_tests!(@key_columns $($tables)*);

            let params = OperationsParams {
                size_range,
                key_columns,
                row_strategies,
                options: $options,
            };

            Operations::arbitrary(params)
        }

        #[proptest]
        #[serial_test::serial]
        #[cfg_attr(not(feature = "vertical_tests"), ignore)]
        $(#[$meta])*
        fn $name(
            #[strategy([<$name _generate_ops>]())]
            operations: Operations
        ) {
            let tables = vertical_tests!(@tables $($tables)*);
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(operations.run($query, &tables))?;
        }
        }
    };
    (@test $(#[$meta:meta])* $name:ident($query: expr; $($tables: tt)*)) => {
        vertical_tests!(@test $(#[$meta])* $name($query;Default::default();$($tables)*));
    };

    // collect together all of the key columns from the tables into a single array
    (@key_columns $($table_name: expr => (
        $create_table: expr,
        schema: [$($schema:tt)*],
        primary_key: $pk_index: expr,
        key_columns: [$($kc: expr),* $(,)?]
        $(,)?
    )),* $(,)?) => {
        vec![$($(($table_name, $kc),)*)*]
    };

    // Build up the hashmap of Tables
    (@tables $($table_name: expr => (
        $create_table: expr,
        schema: [$($col_name: ident : $col_strat: expr),* $(,)?],
        primary_key: $pk_index: expr,
        key_columns: [$($kc: expr),* $(,)?]
        $(,)?
    )),* $(,)?) => {
        HashMap::from([
            $(($table_name, Table {
                name: $table_name,
                create_statement: $create_table,
                primary_key: $pk_index,
                columns: vec![$(stringify!($col_name),)*],
            }),)*
        ])
    };

    // Build up the hashmap of row_strategies
    (@row_strategies $($table_name: expr => (
        $create_table: expr,
        schema: [$($schema:tt)*],
        primary_key: $pk_index: expr,
        key_columns: [$($kc: expr),* $(,)?]
        $(,)?
    )),* $(,)?) => {
        HashMap::from([
            $(($table_name, vertical_tests!(@row_strategy $($schema)*)),)*
        ])
    };

    (@row_strategy $($col_name: ident : $col_type: tt $(($($type_args: tt)*))?),* $(,)?) => {{
        RowStrategy(vec![
            $(vertical_tests!(@column_strategy $col_type $(($($type_args)*))*), )*
        ])
    }};

    (@column_strategy foreign_key($table_name: expr, $foreign_column: expr)) => {
        ColumnStrategy::ForeignKey {
            table: $table_name,
            foreign_column: $foreign_column,
        }
    };

    (@column_strategy nullable($schema_type: ty)) => {
        ColumnStrategy::Value(any::<Option<$schema_type>>().prop_map(|ov| match ov {
            Some(v) => DfValue::from(v),
            None => DfValue::None,
        }).boxed())
    };

    (@column_strategy value($value: expr)) => {
        ColumnStrategy::Value(Just(DfValue::try_from($value).unwrap()).boxed())
    };

    (@column_strategy $schema_type: ty) => {
        ColumnStrategy::Value(any::<$schema_type>().prop_map_into::<DfValue>().boxed())
    };

    () => {};
}
*/

trait TestDef: Clone + Debug + Default {
    fn test_query() -> &'static str;
    fn test_tables() -> HashMap<&'static str, Table>;

    fn key_columns() -> Vec<(&'static str, usize)>;
    fn row_strategies() -> HashMap<&'static str, RowStrategy>;
    fn extra_key_strategies() -> Vec<BoxedStrategy<DfValue>>;
}

#[derive(Clone, Debug, Default)]
struct SimplePointLookupTestDef {}
impl TestDef for SimplePointLookupTestDef {
    fn test_query() -> &'static str {
        "SELECT id, name FROM users WHERE id = ?"
    }

    fn test_tables() -> HashMap<&'static str, Table> {
        HashMap::from([(
            "users",
            Table {
                name: "users",
                create_statement: "CREATE TABLE users (id INT, name TEXT, PRIMARY KEY (id))",
                primary_key: 0,
                columns: vec!["id", "name"],
            },
        )])
    }

    fn key_columns() -> Vec<(&'static str, usize)> {
        vec![("users", 0)]
    }

    fn row_strategies() -> HashMap<&'static str, RowStrategy> {
        HashMap::from([("users", {
            RowStrategy(vec![
                (ColumnStrategy::Value(any::<i32>().prop_map_into::<DfValue>().boxed())),
                (ColumnStrategy::Value(any::<String>().prop_map_into::<DfValue>().boxed())),
            ])
        })])
    }

    fn extra_key_strategies() -> Vec<BoxedStrategy<DfValue>> {
        vec![]
    }
}

#[test]
#[serial_test::serial]
#[cfg_attr(not(feature = "vertical_tests"), ignore)]
fn simple_point_lookup() {
    let config = StatefulProptestConfig {
        min_ops: 1,
        max_ops: 100,
        test_case_timeout: Duration::from_secs(60),
    };

    stateful_proptest::test::<DataflowModelState<SimplePointLookupTestDef>>(config);
}

/*
vertical_tests! {
    simple_point_lookup(
        "SELECT id, name FROM users WHERE id = ?";
        "users" => (
            "CREATE TABLE users (id INT, name TEXT, PRIMARY KEY (id))",
            schema: [id: i32, name: String],
            primary_key: 0,
            key_columns: [0],
        )
    );

    aggregate_with_filter(
        "SELECT count(*) FROM users_groups WHERE group_id = ? AND joined_at IS NOT NULL";
        "users_groups" => (
            "CREATE TABLE users_groups (
                id int NOT NULL,
                user_id int NOT NULL,
                group_id int NOT NULL,
                joined_at int DEFAULT NULL,
                PRIMARY KEY (id)
            )",
            schema: [id: i32, user_id: i32, group_id: i32, joined_at: nullable(i32)],
            primary_key: 0,
            key_columns: [2],
        )
    );

    partial_inner_join(
        "SELECT posts.id, posts.title, users.name
         FROM posts
         JOIN users ON posts.author_id = users.id
         WHERE users.id = ?";
        "posts" => (
            "CREATE TABLE posts (id INT, title TEXT, author_id INT, PRIMARY KEY (id))",
            schema: [id: i32, title: String, author_id: foreign_key("users", 0)],
            primary_key: 0,
            key_columns: [],
        ),
        "users" => (
            "CREATE TABLE users (id INT, name TEXT, PRIMARY KEY (id))",
            schema: [id: i32, name: String],
            primary_key: 0,
            key_columns: [0],
        )
    );

    partial_union_inner_join(
        "SELECT posts.id, posts.title, users.name
         FROM posts
         JOIN users ON posts.author_id = users.id
         WHERE (users.name = \"a\" OR posts.title = \"a\")
           AND users.id = ?";
        "posts" => (
            "CREATE TABLE posts (id INT, title TEXT, author_id INT, PRIMARY KEY (id))",
            schema: [id: i32, title: value("a"), author_id: i32],
            primary_key: 0,
            key_columns: [],
        ),
        "users" => (
            "CREATE TABLE users (id INT, name TEXT, PRIMARY KEY (id))",
            schema: [id: i32, name: value("a")],
            primary_key: 0,
            key_columns: [0],
        )
    );

    partial_topk(
        "SELECT id, title FROM posts WHERE author_id = ? ORDER BY score DESC LIMIT 3";
        "posts" => (
            "CREATE TABLE posts (id INT, title TEXT, score INT, author_id INT, PRIMARY KEY (id))",
            schema: [id: i32, title: String, score: i32, author_id: foreign_key("users", 0)],
            primary_key: 0,
            key_columns: []
        ),
        "users" => (
            "CREATE TABLE users (id INT, name TEXT, PRIMARY KEY (id))",
            schema: [id: i32, name: String],
            primary_key: 0,
            key_columns: [0],
        )
    );

    simple_range(
        "SELECT id, name, score FROM posts WHERE score > ?";
        "posts" => (
            "CREATE TABLE posts (id INT, name TEXT, score INT, PRIMARY KEY (id))",
            schema: [id: i32, name: String, score: i32],
            primary_key: 0,
            key_columns: [2],
        )
    );

    paginate_grouped(
        "SELECT id FROM posts WHERE author_id = ? ORDER BY score DESC LIMIT 3 OFFSET ?";
        TestOptions {
            extra_key_strategies: vec![
                (0u32..=15u32).prop_map(|i| DfValue::from(i * 3)).boxed()
            ]
        };
        "posts" => (
            "CREATE TABLE posts (id INT, author_id INT, score INT, PRIMARY KEY (id))",
            schema: [id: i32, author_id: i32, score: i32],
            primary_key: 0,
            key_columns: [2],
        )
    );

    compound_range(
        "SELECT id, name, score, age FROM posts WHERE score > ? and age > ?";
        "posts" => (
            "CREATE TABLE posts (id INT, name TEXT, score INT, age INT, PRIMARY KEY (id))",
            schema: [id: i32, name: String, score: i32, age: i32],
            primary_key: 0,
            key_columns: [2, 3],
        )
    );

    mixed_range(
        "SELECT id, name, score FROM posts WHERE name = ? AND score > ?";
        "posts" => (
            "CREATE TABLE posts (id INT, name TEXT, score INT, PRIMARY KEY (id))",
            schema: [id: i32, name: String, score: i32],
            primary_key: 0,
            key_columns: [1, 2],
        )
    );

    between(
        "SELECT id, name, score FROM posts WHERE score BETWEEN ? AND ?";
        "posts" => (
            "CREATE TABLE posts (id INT, name TEXT, score INT, PRIMARY KEY (id))",
            schema: [id: i32, name: String, score: i32],
            primary_key: 0,
            key_columns: [2, 2],
        )
    );
}
*/
