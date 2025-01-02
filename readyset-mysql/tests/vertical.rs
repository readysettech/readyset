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

use std::cell::RefCell;
use std::cmp::Ordering;
// We use BTreeSet and BTreeMap instead of HashSet and HashMap to avoid non-deterministic
// ordering when generating test cases, which is important when re-running failing seeds:
use std::collections::{BTreeMap, BTreeSet};
use std::convert::TryFrom;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::panic::AssertUnwindSafe;
use std::time::Duration;
use std::{cmp, env};

use async_trait::async_trait;
use itertools::Itertools;
use mysql_async::prelude::Queryable;
use mysql_async::Conn;
use mysql_common::value::Value;
use paste::paste;
use proptest::prelude::*;
use proptest::sample::select;
use proptest_stateful::{
    proptest_config_with_local_failure_persistence, ModelState, ProptestStatefulConfig,
};
use readyset_client::SingleKeyEviction;
use readyset_client_test_helpers::mysql_helpers::MySQLAdapter;
use readyset_client_test_helpers::TestBuilder;
use readyset_data::DfValue;
use readyset_server::Handle;
use readyset_util::eventually;
use readyset_util::shutdown::ShutdownSender;
use test_utils::serial;

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
    /// This operation triggers an eviction of a single key in ReadySet, using `inner` as the
    /// payload for the /evict_single RPC.
    ///
    /// The payload is initialized to `None`, which triggers a random eviction the first time this
    /// operation is run. `inner` is then updated with the `SingleKeyResult` returned by the
    /// /evict_single RPC, so that if this operation is run again, we can trigger the same eviction
    /// again. This behavior is necessary to ensure consistent results when attempting to reproduce
    /// a failing test case.
    Evict {
        inner: RefCell<Option<SingleKeyEviction>>,
    },
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

    fn fill_foreign_keys<F, S>(self, foreign_key_strategy: F) -> Vec<BoxedStrategy<DfValue>>
    where
        F: Fn(&'static str, usize) -> S,
        S: Strategy<Value = DfValue> + Sized + 'static,
    {
        self.0
            .into_iter()
            .map(move |cs| match cs {
                ColumnStrategy::Value(strat) => strat.boxed(),
                ColumnStrategy::ForeignKey {
                    table,
                    foreign_column,
                } => foreign_key_strategy(table, foreign_column).boxed(),
            })
            .collect()
    }

    fn foreign_tables(&self) -> BTreeSet<&'static str> {
        self.0
            .iter()
            .filter_map(|column_strat| match column_strat {
                ColumnStrategy::ForeignKey { table, .. } => Some(*table),
                _ => None,
            })
            .collect()
    }
}

#[derive(Clone, Debug, Default)]
struct DataflowModelState<T>
where
    T: TestDef + 'static,
{
    rows: BTreeMap<&'static str, Vec<Vec<DfValue>>>,
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

    /// Return a [`BTreeMap`] with a key for each test table where the values are other tables that
    /// the key table contains foreign keys for.
    ///
    /// Useful as a helper for other code that does things like decide whether we will be able to
    /// fill in all the foreign keys on a generated row or not.
    fn table_dependencies(&self) -> BTreeMap<&'static str, BTreeSet<&'static str>> {
        T::row_strategies()
            .into_iter()
            .map(|(table, row_strat)| (table, row_strat.foreign_tables()))
            .collect()
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
        // those two strategies no matter what. We can also always request an eviction, though it
        // may be a no-op if we have not inserted anything.
        // Later in this function we may conditionally add more strategies.
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

        let evict_strategy = Just(Operation::Evict {
            inner: RefCell::new(None),
        })
        .boxed();

        res.push(evict_strategy);

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
                    select(vals)
                })
            };

            // Generate the set of all test tables except for ones where we cannot generate valid
            // rows due to foreign keys that have no valid values due to an empty foreign table:
            let fk_tables: Vec<&'static str> = self
                .table_dependencies()
                .iter()
                .filter_map(|(table, deps)| {
                    if deps.iter().all(|dep| {
                        self.rows
                            .get(dep)
                            .is_some_and(|dep_rows| !dep_rows.is_empty())
                    }) {
                        Some(*table)
                    } else {
                        None
                    }
                })
                .collect();

            if !fk_tables.is_empty() {
                let fill_foreign_keys = fill_foreign_keys.clone(); // Cloned for move into closure
                let fill_foreign_keys2 = fill_foreign_keys.clone(); // Cloned for move into closure

                let insertable_tables = T::row_strategies()
                    .into_iter()
                    .filter(|(table, _)| fk_tables.contains(table))
                    .collect::<Vec<_>>();

                let mk_fk_insert = select(insertable_tables)
                    .prop_map(move |(table, row_strat)| {
                        fill_foreign_keys(row_strat)
                            .prop_map(move |row| Operation::Insert { table, row })
                    })
                    .prop_flat_map(|s| s)
                    .boxed();
                res.push(mk_fk_insert);

                let updateable_rows = rows
                    .iter()
                    .filter(|(table, _)| fk_tables.contains(table))
                    .cloned()
                    .collect::<Vec<_>>();
                if !updateable_rows.is_empty() {
                    let mk_update = select(updateable_rows)
                        .prop_flat_map(move |(table, old_row)| {
                            (fill_foreign_keys2(T::row_strategies()[table].clone())
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
                }
            }

            let mk_delete = select(rows)
                .prop_map(move |(table, row)| Operation::Delete { table, row })
                .boxed();

            res.push(mk_delete);
        }

        res
    }

    fn preconditions_met(&self, op: &Self::Operation) -> bool {
        match op {
            Operation::Update {
                table,
                old_row: row,
                ..
            }
            | Operation::Delete { table, row } => self
                .rows
                .get(table)
                .is_some_and(|table_rows| table_rows.contains(row)),
            // Can always insert any row or query any key or request an eviction and we shouldn't
            // error out
            Operation::Insert { .. } | Operation::Query { .. } | Operation::Evict { .. } => true,
        }
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
                old_state_row.clone_from(new_row);
            }
            Operation::Delete { table, row } => {
                let table_rows = self.rows.entry(table).or_default();
                let row_pos = table_rows.iter().position(|r| r == row).unwrap();
                table_rows.swap_remove(row_pos);
            }
            Operation::Query { .. } | Operation::Evict { .. } => (),
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

        if matches!(op, Operation::Evict { .. }) {
            // `Operation::Evict` is ReadySet only
            op.run(readyset, query, &tables).await.unwrap();
        } else if matches!(op, Operation::Query { .. }) {
            eventually!(attempts: 5, sleep: Duration::from_millis(20), run_test: {
                let mysql_res = op.run(mysql, query, &tables).await.unwrap();
                let readyset_res = op.run(readyset, query, &tables).await.unwrap();
                AssertUnwindSafe(move || (mysql_res, readyset_res))
            }, then_assert: |results| {
                let (mysql_res, readyset_res) = results();
                assert_eq!(mysql_res, readyset_res);
            });
        } else {
            let mysql_res = op.run(mysql, query, &tables).await.unwrap();
            let readyset_res = op.run(readyset, query, &tables).await.unwrap();
            assert_eq!(mysql_res, readyset_res);
        }
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
        tables: &BTreeMap<&str, Table>,
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
            Operation::Evict { inner } => {
                // Call the /evict_random Controller RPC. We don't care about the result.
                let host = conn.opts().ip_or_hostname();
                let client = reqwest::Client::new();
                let body = bincode::serialize::<Option<SingleKeyEviction>>(&*inner.borrow())?;
                let res = client
                    .post(format!("http://{host}:6033/evict_single"))
                    .body(body)
                    .send()
                    .await?;
                if inner.borrow().is_none() {
                    let eviction =
                        bincode::deserialize::<Option<SingleKeyEviction>>(&res.bytes().await?)?;
                    inner.replace(eviction);
                }
                Ok(OperationResult::NoResults)
            }
        }
    }
}

macro_rules! vertical_tests {
    ($(#[$meta:meta])* $name:ident($($params: tt)*); $($rest: tt)*) => {
        vertical_tests!(@test $(#[$meta])* $name($($params)*));
        vertical_tests!($($rest)*);
    };

    // define the test itself
    (@test $(#[$meta:meta])* $name:ident($query: expr; $extra_key_strategies: expr; $($tables: tt)*)) => {
        paste! {
            #[derive(Clone, Debug, Default)]
            struct [<$name:camel TestDef>] {}
            impl TestDef for [<$name:camel TestDef>] {
                fn test_query() -> &'static str {
                    $query
                }

                fn test_tables() -> BTreeMap<&'static str, Table> {
                    vertical_tests!(@tables $($tables)*)
                }

                fn key_columns() -> Vec<(&'static str, usize)> {
                    vertical_tests!(@key_columns $($tables)*)
                }

                fn row_strategies() -> BTreeMap<&'static str, RowStrategy> {
                    vertical_tests!(@row_strategies $($tables)*)
                }

                fn extra_key_strategies() -> Vec<BoxedStrategy<DfValue>> {
                    $extra_key_strategies
                }
            }

            #[serial(mysql)]
            #[test]
            #[cfg_attr(not(feature = "vertical_tests"), ignore)]
            fn $name() {
                let config = ProptestStatefulConfig {
                    min_ops: 1,
                    max_ops: 100,
                    test_case_timeout: Duration::from_secs(60),
                    proptest_config: proptest_config_with_local_failure_persistence!(),
                };

                proptest_stateful::test::<DataflowModelState<[<$name:camel TestDef>]>>(config);
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

    // Build up the map of Tables
    (@tables $($table_name: expr => (
        $create_table: expr,
        schema: [$($col_name: ident : $col_strat: expr),* $(,)?],
        primary_key: $pk_index: expr,
        key_columns: [$($kc: expr),* $(,)?]
        $(,)?
    )),* $(,)?) => {
        BTreeMap::from([
            $(($table_name, Table {
                name: $table_name,
                create_statement: $create_table,
                primary_key: $pk_index,
                columns: vec![$(stringify!($col_name),)*],
            }),)*
        ])
    };

    // Build up the map of row_strategies
    (@row_strategies $($table_name: expr => (
        $create_table: expr,
        schema: [$($schema:tt)*],
        primary_key: $pk_index: expr,
        key_columns: [$($kc: expr),* $(,)?]
        $(,)?
    )),* $(,)?) => {
        BTreeMap::from([
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

trait TestDef: Clone + Debug + Default {
    fn test_query() -> &'static str;
    fn test_tables() -> BTreeMap<&'static str, Table>;

    fn key_columns() -> Vec<(&'static str, usize)>;
    fn row_strategies() -> BTreeMap<&'static str, RowStrategy>;
    fn extra_key_strategies() -> Vec<BoxedStrategy<DfValue>>;
}

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
        vec![
            (0u32..=15u32).prop_map(|i| DfValue::from(i * 3)).boxed()
        ];
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

    successive_disjunction_diamonds(
        // The idea here is to create a graph with multiple successive union diamonds, like this:
        //   B
        //  / \
        // σ   σ
        //  \ /
        //   ⊎
        //  / \
        // σ   σ
        //  \ /
        //   ⊎
        "SELECT id, name FROM posts WHERE
         (score1 < 16384 OR score1 > 16384) AND
         (score2 > 16384 OR score2 < 16384) AND
         name = ?";
        "posts" => (
            "CREATE TABLE posts
             (id INT, name TEXT, score1 SMALLINT, score2 SMALLINT, PRIMARY KEY (id))",
            schema: [id: i32, name: String, score1: i16, score2: i16],
            primary_key: 0,
            key_columns: [1],
        )
    );

    // Join on a different key than the one in the WHERE clause. This is mainly meant to exercise
    // the eviction code by re-creating the kind of corner case described in section 4.5.2 of the
    // Noria thesis under the "Incongruent Joins" heading:
    partial_incongruent_join(
        "SELECT posts.id, posts.title, users.name
         FROM posts
         JOIN users ON posts.author_id = users.id
         WHERE posts.id = ?";
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

    partial_left_outer_join(
        "SELECT posts.id, posts.title, users.name
         FROM posts
         LEFT OUTER JOIN users ON posts.author_id = users.id
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
}
