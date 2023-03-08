//! This test suite implements the [Replicator Vertical Testing Doc][doc].
//!
//! [doc]: https://docs.google.com/document/d/1GRYV7okEzz2T-KuF06M5Y4EkyRv7euMSb1viroT9JTk
//!
//! Note that this test suite is ignored by default, and conditionally de-ignored with the
//! `ddl_vertical_tests` feature to prevent it running in normal builds (since it's slow and may
//! find new bugs); to run it locally run:
//!
//! ```notrust
//! cargo test -p replicators --features ddl_vertical_tests --test ddl_vertical
//! ```
//!
//! This test suite will connect to a local Postgres database, which can be set up with all the
//! correct configuration using the `docker-compose.yml` and `docker-compose.override.example.yml`
//! in the root of the repository. To run that Postgres database, run:
//!
//! ```notrust
//! $ cp docker-compose.override.example.yml docker-compose.yml
//! $ docker-compose up -d postgres
//! ```
//!
//! Note that this test suite requires the *exact* configuration specified in that docker-compose
//! configuration, including the port, username, and password.

use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Formatter, Result};
use std::iter::once;
use std::panic::AssertUnwindSafe;

use itertools::Itertools;
use nom_sql::SqlType;
use proptest::prelude::*;
use proptest::strategy::{BoxedStrategy, Just, NewTree, Strategy, ValueTree};
use proptest::test_runner::TestRunner;
use proptest::{collection, proptest, sample};
use rand::distributions::{Distribution, Uniform};
use rand::Rng;
use readyset_client_test_helpers::psql_helpers::{self, PostgreSQLAdapter};
use readyset_client_test_helpers::TestBuilder;
use readyset_data::{DfValue, TimestampTz};
use readyset_util::eventually;
use tokio_postgres::{Client, Config, NoTls, Row};

const SQL_NAME_REGEX: &str = "[a-zA-Z_][a-zA-Z0-9_]*";
const MIN_OPS: usize = 7;
const MAX_OPS: usize = 13;

/// This struct is used to generate arbitrary column specifications, both for creating tables, and
/// potentially for altering them by adding columns and such.
#[derive(Clone)]
struct ColumnSpec {
    name: String,
    sql_type: SqlType,
    gen: BoxedStrategy<DfValue>,
}

// The debug output for the generators can be really verbose and is usually not helpful, so we
// custom derive Debug to skip that part:
impl Debug for ColumnSpec {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        f.debug_struct("ColumnSpec")
            .field("name", &self.name)
            .field("sql_type", &self.sql_type)
            .finish()
    }
}

impl Arbitrary for ColumnSpec {
    type Parameters = ();
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with((): Self::Parameters) -> Self::Strategy {
        (
            SQL_NAME_REGEX.prop_filter("Can't generate additional columns named \"id\"", |s| {
                s.to_lowercase() != "id"
            }),
            sample::select(vec![
                (
                    SqlType::Int(None),
                    any::<i32>().prop_map(DfValue::from).boxed(),
                ),
                (
                    SqlType::Real,
                    any::<f32>()
                        // unwrap is fine because the f32 Arbitrary impl only yields finite values
                        .prop_map(|f| DfValue::try_from(f).unwrap())
                        .boxed(),
                ),
                (
                    SqlType::VarChar(None),
                    any::<String>().prop_map(DfValue::from).boxed(),
                ),
                (
                    SqlType::TimestampTz,
                    any::<TimestampTz>().prop_map(DfValue::TimestampTz).boxed(),
                ),
            ]),
        )
            .prop_map(|(name, (sql_type, gen))| ColumnSpec {
                name,
                sql_type,
                gen,
            })
            .boxed()
    }
}

/// Each Operation represents one step to take in a given test run.
#[derive(Clone, Debug)]
enum Operation {
    /// Create a new table with the given name and columns
    CreateTable(String, Vec<ColumnSpec>),
    /// Drop the table with the given name
    DropTable(String),
    /// Write a random row to a random table with the given primary key
    /// (The [`SqlTypes`] values are just to check preconditions so that we don't try to write a
    /// row to a table whose column types don't match the row we originally generated.)
    WriteRow {
        table: String,
        pkey: i32,
        col_vals: Vec<DfValue>,
        col_types: Vec<SqlType>,
    },
    /// Delete rows to a given table that match a given key
    DeleteRow(String, i32),
    /// Adds a new column to an existing table
    AddColumn(String, ColumnSpec),
    /// Removes a column from an existing table
    DropColumn(String, String),
    /// Alters a column to a different name
    AlterColumnName {
        table: String,
        col_name: String,
        new_name: String,
    },
    /// Creates a simple view that does a SELECT * on a given table
    CreateSimpleView { name: String, table_source: String },
    /// Drops the view with the given name
    DropView(String),
}

impl Operation {
    /// Checks preconditions for this operation given a current test model state.
    ///
    /// These are primarily needed for shrinking, so that we can make sure that we don't do things
    /// like remove a CreateTable when a later WriteRow operation depends on the corresponding
    /// table.
    ///
    /// We also check preconditions during runtime, and throw out any test cases where the
    /// preconditions aren't satisfied. This should be rare, though, because [`TestModel::gen_op`]
    /// should usually only generate cases where the preconditions are already satisified. It's
    /// possible there are weird corner cases though (such as multiple random strings happening to
    /// generate the same string value for two different table names) where preconditions could
    /// save us from a false positive test failure.
    fn preconditions(&self, state: &TestModel) -> bool {
        match self {
            Self::CreateTable(name, _cols) => !state.tables.contains_key(name),
            Self::DropTable(name) => state.tables.contains_key(name),
            Self::WriteRow {
                table,
                pkey,
                col_vals: _,
                col_types,
            } => {
                // Make sure that the table doesn't already contain a row with this key, and also
                // make sure that the column types in the table also match up with the types in the
                // row that we're trying to write:
                state
                    .pkeys
                    .get(table)
                    .map_or(false, |table_keys| !table_keys.contains(pkey))
                    && state.tables.get(table).map_or(false, |table_cols| {
                        table_cols
                            .iter()
                            .zip(col_types)
                            .all(|(cs, row_type)| cs.sql_type == *row_type)
                    })
            }
            Self::DeleteRow(table, key) => state
                .pkeys
                .get(table)
                .map_or(false, |table_keys| table_keys.contains(key)),
            Self::AddColumn(table, column_spec) => state
                .tables
                .get(table)
                .map_or(false, |t| t.iter().all(|cs| cs.name != *column_spec.name)),
            Self::DropColumn(table, col_name) => state
                .tables
                .get(table)
                .map_or(false, |t| t.iter().any(|cs| cs.name == *col_name)),
            Self::AlterColumnName {
                table,
                col_name,
                new_name,
            } => state.tables.get(table).map_or(false, |t| {
                t.iter().any(|cs| cs.name == *col_name) && t.iter().all(|cs| cs.name != *new_name)
            }),
            Self::CreateSimpleView { name, table_source } => {
                state.tables.contains_key(table_source)
                    && !state.tables.contains_key(name)
                    && !state.views.contains_key(name)
            }
            Self::DropView(name) => state.views.contains_key(name),
        }
    }
}

// Generators for Operation:

fn gen_column_specs() -> impl Strategy<Value = Vec<ColumnSpec>> {
    collection::vec(any::<ColumnSpec>(), 1..4)
        .prop_filter("duplicate column names not allowed", |specs| {
            specs.iter().map(|cs| &cs.name).all_unique()
        })
}

prop_compose! {
    fn gen_create_table()(name in SQL_NAME_REGEX, cols in gen_column_specs()) -> Operation {
        Operation::CreateTable(name, cols)
    }
}

prop_compose! {
    fn gen_drop_table(tables: Vec<String>)(t in sample::select(tables)) -> Operation {
        Operation::DropTable(t)
    }
}

prop_compose! {
    fn gen_write_row(tables: HashMap<String, Vec<ColumnSpec>>, pkeys: HashMap<String, Vec<i32>>)
                    (t in sample::select(tables.keys().cloned().collect::<Vec<_>>()))
                    (col_vals in tables[&t].iter().map(|cs| cs.gen.clone()).collect::<Vec<_>>(),
                     col_types in Just(tables[&t].iter().map(|cs| cs.sql_type.clone()).collect()),
                     table in Just(t))
                    -> Operation {
        let table_keys = &pkeys[&table];
        // Find the first unused key:
        let pkey = (0..).find(|k| !table_keys.contains(k)).unwrap();
        Operation::WriteRow { table, pkey, col_vals, col_types }
    }
}

prop_compose! {
    fn gen_add_col_unfiltered(tables: Vec<String>)
                             (t in sample::select(tables), col in any::<ColumnSpec>())
                             -> Operation {
        Operation::AddColumn(t, col)
    }
}

fn gen_add_col(tables: HashMap<String, Vec<ColumnSpec>>) -> impl Strategy<Value = Operation> {
    gen_add_col_unfiltered(tables.keys().cloned().collect()).prop_filter(
        "Can't add a new column with a duplicate name",
        move |op| match op {
            Operation::AddColumn(table, new_cs) => {
                new_cs.name != "id"
                    && !tables[table]
                        .iter()
                        .any(|table_cs| new_cs.name.eq_ignore_ascii_case(&table_cs.name))
            }
            _ => unreachable!(),
        },
    )
}

fn gen_non_id_col_name() -> impl Strategy<Value = String> {
    SQL_NAME_REGEX.prop_filter("Can't generate additional columns named \"id\"", |s| {
        s.to_lowercase() != "id"
    })
}

prop_compose! {
    fn gen_rename_col(tables: HashMap<String, Vec<ColumnSpec>>, tables_with_cols: Vec<String>)
                     (table in sample::select(tables_with_cols))
                     (col_name in sample::select(
                         tables[&table]
                             .iter()
                             .map(|cs| cs.name.clone())
                             .collect::<Vec<_>>()),
                      new_name in gen_non_id_col_name(),
                      table in Just(table))
                     -> Operation {
        Operation::AlterColumnName { table, col_name, new_name }
    }
}

prop_compose! {
    fn gen_drop_col(tables: HashMap<String, Vec<ColumnSpec>>, tables_with_cols: Vec<String>)
                   (table in sample::select(tables_with_cols))
                   (col_name in sample::select(
                       tables[&table]
                           .iter()
                           .map(|cs| cs.name.clone())
                           .collect::<Vec<_>>()),
                    table in Just(table))
                 -> Operation {
        Operation::DropColumn(table, col_name)
    }
}

prop_compose! {
    fn gen_delete_row(non_empty_tables: Vec<String>, pkeys: HashMap<String, Vec<i32>>)
                     (table in sample::select(non_empty_tables))
                     (key in sample::select(pkeys[&table].clone()),
                      table in Just(table))
                     -> Operation {
        Operation::DeleteRow(table, key)
    }
}

prop_compose! {
    fn gen_create_simple_view(tables: Vec<String>)
                             (name in SQL_NAME_REGEX,
                              table_source in sample::select(tables))
                             -> Operation {
        Operation::CreateSimpleView { name, table_source }
    }
}

prop_compose! {
    fn gen_drop_view(views: Vec<String>)(name in sample::select(views)) -> Operation {
        Operation::DropView(name)
    }
}

/// A model of the current test state, used to help generate operations in a way that we expect to
/// succeed, as well as to assist in shrinking, and to determine postconditions to check during
/// test runtime.
///
/// Initially we assume an empty database, but as operations are generated, tracking their expected
/// results helps inform which operations we are able to test further along in the test case. For
/// example, when writing a row, we must pick a table to write the row to, so we must look at the
/// model state to see what tables have been previously created. We don't actually run a test case
/// until all the steps have been generated, so [`TestModel`] allows us to simulate the expected
/// state of the system for a given test case without having to actually run any of the steps
/// against the system under test.
#[derive(Clone, Debug, Default)]
struct TestModel {
    tables: HashMap<String, Vec<ColumnSpec>>,
    deleted_tables: HashSet<String>,
    pkeys: HashMap<String, Vec<i32>>, // Primary keys in use for each table
    // Map of view name to view definition (right now just a table that we do a SELECT * from)
    views: HashMap<String, String>,
}

impl TestModel {
    /// Each invocation of this function returns a [`BoxedStrategy`] for generating an
    /// [`Operation`] *given the current state of the test model*. With a brand new model, the only
    /// possible operation is [`Operation::CreateTable`], but as tables are created and rows are
    /// written, other operations become possible.
    ///
    /// Note that there is some redundancy between the logic in this function and the logic in
    /// [`Operation::preconditions`](enum.Operation.html#method.preconditions). This is necessary
    /// because `gen_op` is used for the initial test generation, but the preconditions are used
    /// during shrinking. (Technically, we do also check and filter on preconditions at the start
    /// of each test, but it's best to depend on that check as little as possible since test
    /// filters like that can lead to slow and lopsided test generation.)
    fn gen_op(&self, runner: &mut TestRunner) -> impl Strategy<Value = Operation> {
        // We can always create more tables, so start with just the one operation generator:
        let mut possible_ops = vec![gen_create_table().boxed()];

        // If we have at least one table, we can do any of:
        //  * delete a table
        //  * write a row
        //  * add a column
        //  * create a simple view
        if !self.tables.is_empty() {
            let drop_strategy = gen_drop_table(self.tables.keys().cloned().collect()).boxed();
            let write_strategy = gen_write_row(self.tables.clone(), self.pkeys.clone()).boxed();
            let add_col_strat = gen_add_col(self.tables.clone()).boxed();
            let create_simple_view_strat =
                gen_create_simple_view(self.tables.keys().cloned().collect()).boxed();

            possible_ops.extend(
                [
                    drop_strategy,
                    write_strategy,
                    add_col_strat,
                    create_simple_view_strat,
                ]
                .into_iter(),
            );
        }

        // If we have at least one view in existence, we can drop one:
        if !self.views.is_empty() {
            let drop_view_strategy = gen_drop_view(self.views.keys().cloned().collect()).boxed();
            possible_ops.push(drop_view_strategy);
        }

        // If we have a table with at least one (non-pkey) column, we can rename or drop a column:
        let tables_with_cols: Vec<String> = self
            .tables
            .iter()
            .filter_map(|(table, columns)| {
                if columns.is_empty() {
                    None
                } else {
                    Some(table)
                }
            })
            .cloned()
            .collect();
        if !tables_with_cols.is_empty() {
            // This is cloned so that we can move it into the closures for rename_col_strat:
            let rename_col_strat =
                gen_rename_col(self.tables.clone(), tables_with_cols.clone()).boxed();
            possible_ops.push(rename_col_strat);

            let _drop_col_strategy = gen_drop_col(self.tables.clone(), tables_with_cols).boxed();
            // Commented out for now because this triggers ENG-2548
            // possible_ops.push(drop_col_strategy);
        }
        // If we have at least one row written to a table, we can generate delete ops:
        let non_empty_tables: Vec<String> = self
            .pkeys
            .iter()
            .filter_map(|(table, pkeys)| {
                if !pkeys.is_empty() {
                    Some(table.clone())
                } else {
                    None
                }
            })
            .collect();
        if !non_empty_tables.is_empty() {
            let delete_strategy = gen_delete_row(non_empty_tables, self.pkeys.clone()).boxed();
            possible_ops.push(delete_strategy);
        }

        // Once generated, we do not shrink individual Operations; all shrinking is done via
        // removing individual operations from the test sequence. Thus, rather than returning a
        // Strategy to generate any of the possible ops from `possible_ops`, it's simpler to
        // just randomly pick one and return it directly. It's important to use the RNG from
        // `runner` though to ensure test runs are deterministic based on the seed from proptest.
        possible_ops.swap_remove(runner.rng().gen_range(0..possible_ops.len()))
    }

    /// This method is used to update `self` based on the expected results of executing a single
    /// [`Operation`]. It is used during test generation, but notably, we repeat the same sequence
    /// of state updates at runtime since we depend on the current state of the model to check
    /// postconditions when we're actually executing a given test case.
    ///
    /// In theory we could choose to only run through the sequence of `next_state` calls once at
    /// test generation time, and save each intermediate state for use at runtime, but that would
    /// be more complex to implement and it's questionable whether it would actually be more
    /// efficient. Additionally, if we ever want to add symbolic placeholders to the state (for
    /// representing return values of operations at runtime) then the runtime state will actually
    /// differ from the generation-time state, and running through the sequence of `next_state`
    /// calls two separate times will become strictly necessary as a result.
    fn next_state(&mut self, op: &Operation) {
        match op {
            Operation::CreateTable(name, cols) => {
                self.tables.insert(name.clone(), cols.clone());
                self.pkeys.insert(name.clone(), vec![]);
            }
            Operation::DropTable(name) => {
                self.tables.remove(name);
                self.deleted_tables.insert(name.clone());
                self.pkeys.remove(name);
                self.views
                    .retain(|_view_name, table_source| name != table_source);
            }
            Operation::WriteRow { table, pkey, .. } => {
                self.pkeys.get_mut(table).unwrap().push(*pkey);
            }
            Operation::AddColumn(table, col_spec) => {
                let col_specs = self.tables.get_mut(table).unwrap();
                col_specs.push(col_spec.clone());
            }
            Operation::DropColumn(table, col_name) => {
                let col_specs = self.tables.get_mut(table).unwrap();
                col_specs.retain(|cs| cs.name != *col_name);
            }
            Operation::AlterColumnName {
                table,
                col_name,
                new_name,
            } => {
                let col_specs = self.tables.get_mut(table).unwrap();
                let mut spec = col_specs
                    .iter_mut()
                    .find(|cs| cs.name == *col_name)
                    .unwrap();
                spec.name = new_name.clone();
            }
            Operation::DeleteRow(..) => (),
            Operation::CreateSimpleView { name, table_source } => {
                self.views.insert(name.clone(), table_source.clone());
            }
            Operation::DropView(name) => {
                self.views.remove(name);
            }
        }
    }
}

/// Tests whether a given sequence of [`Operation`] elements maintains all of the required
/// preconditions as each step is executed according to [`TestModel`].
fn preconditions_hold(ops: &[Operation]) -> bool {
    let mut candidate_state = TestModel::default();
    for op in ops {
        if !op.preconditions(&candidate_state) {
            return false;
        } else {
            candidate_state.next_state(op);
        }
    }
    true
}

/// This is used to store and shrink a list of [`Operation`] values by implementing the
/// [`ValueTree`] trait.
///
/// Note that shrinking is entirely based around removing elements from the sequence of operations;
/// the individual operations themselves are not shrunk, as doing so would likely cause
/// preconditions to be violated later in the sequence of operations (unless we re-generated the
/// rest of the sequence, but this would likely result in a test case that no longer fails).
///
/// In practice, this is not a terrible limitation, as having a minimal sequence of operations
/// tends to make it relatively easy to understand the cause of a failing case, even if the inputs
/// to those operations (like table names and column specs) are not necessarily minimal themselves.
struct TestTree {
    /// List of all operations initially included in this test case
    ops: Vec<Operation>,
    /// List of which operations are still included vs. which have been shrunk out
    currently_included: Vec<bool>,
    /// The last operation we removed via shrinking (which may or may not end up needing to be
    /// added back in, depending on whether the test continues to fail with that operation removed)
    last_shrank_idx: usize,
}

impl ValueTree for TestTree {
    type Value = Vec<Operation>;

    /// Simply returns a [`Vec`] of all [`Operation`] values for this test case that have not been
    /// shrunk out.
    fn current(&self) -> Self::Value {
        self.ops
            .iter()
            .zip(self.currently_included.iter())
            .filter_map(|(v, include)| if *include { Some(v) } else { None })
            .cloned()
            .collect()
    }

    /// Attempts to remove one [`Operation`] for shrinking purposes, starting at the end of the
    /// list and working backward for each invocation of this method. Operations are only removed
    /// if it is possible to do so without violating a precondition.
    ///
    /// There are some limitations in the current approach that I'd like to address at some point:
    ///  * It is inefficient to only remove one element at a time.
    ///    * At a minimum, we should be able to immediately remove all elements that are past the
    ///      point at which the test failed.
    ///    * Beyond that it is also likely that we can randomly try removing multiple elements at
    ///      once since most minimal failing cases are much smaller than the original failing cases
    ///      that produced them. The math around this is trickier though, as the ideal number of
    ///      elements to try removing at any given time depends on how big we expect a typical
    ///      minimal failing test case to be.
    ///  * Removing only one element at a time may prevent us from fully shrinking down to the
    ///    minimal failing case
    ///    * For example, if we create a table A, then drop A, then create A again with different
    ///      columns, then the third step may be the only one necessary to reproduce the failure,
    ///      but we will not be able to remove the first two steps. Removing the first step alone
    ///      would violate a precondition for the second step (since the second stop is to drop A),
    ///      and removing the second step alone would violate a precondition for the third step
    ///      (since we cannot create A again unless we've dropped the previous table named A). In
    ///      order to shrink this down completely, we must remove both of the first two steps in one
    ///      step, which the current shrinking algorithm cannot do.
    ///
    /// However, removing only one element at a time is much simpler from an implementation
    /// standpoint, so that is all that I've implemented for now. Designing and implementing a
    /// better shrinking algorithm could definitely be useful in other tests in the future, though.
    fn simplify(&mut self) -> bool {
        // More efficient to iterate backward, since we only need to go over the list once
        // (preconditions are based only on the state changes caused by previous ops, so if we're
        // moving backward through the list then the only time we'll be unable to remove an op
        // based on a precondition is if we've previously had to add back in a later op that
        // depends on it due to failing to remove the later op via shrinking; in that case we'll
        // never be able to remove the current op either, so there's no need to check it more than
        // once).
        let mut next_idx_to_try = self.last_shrank_idx;
        while next_idx_to_try > 0 {
            next_idx_to_try -= 1;

            // try removing next idx to try, and see if preconditions still hold
            self.currently_included[next_idx_to_try] = false;
            let candidate_ops = self.current();
            // if they still hold, leave it as removed, and we're done
            if preconditions_hold(&candidate_ops) {
                self.last_shrank_idx = next_idx_to_try;
                return true;
            }
            // if they don't still hold, add it back and try another run through the loop
            self.currently_included[next_idx_to_try] = true;
        }
        // We got all the way through and didn't remove anything
        self.last_shrank_idx = 0; // To make sure future calls to simplify also return false
        false
    }

    /// Just undoes the last call to [`simplify`].
    fn complicate(&mut self) -> bool {
        if self.currently_included[self.last_shrank_idx] {
            // Second time being called in a row, can't complicate any further
            false
        } else {
            self.currently_included[self.last_shrank_idx] = true;
            true
        }
    }
}

impl Strategy for TestModel {
    type Tree = TestTree;
    type Value = Vec<Operation>;

    /// Generates a new test case, consisting of a sequence of [`Operation`] values that conform to
    /// the preconditions defined for [`TestModel`].
    fn new_tree(&self, runner: &mut TestRunner) -> NewTree<Self> {
        let mut symbolic_state = TestModel::default();

        let size = Uniform::new_inclusive(MIN_OPS, MAX_OPS).sample(runner.rng());
        let ops = (0..size)
            .map(|_| {
                let next_op = symbolic_state
                    .gen_op(runner)
                    .new_tree(runner)
                    .unwrap()
                    .current();
                symbolic_state.next_state(&next_op);
                next_op
            })
            .collect();

        // initially we include everything, then exclude different steps during shrinking:
        let currently_included = vec![true; size];
        Ok(TestTree {
            ops,
            currently_included,
            last_shrank_idx: size, // 1 past the end of the ops vec
        })
    }
}

/// Spawns a new connection (either to ReadySet or directly to Postgres).
async fn connect(config: Config) -> Client {
    let (client, connection) = config.connect(NoTls).await.unwrap();
    tokio::spawn(connection);
    client
}

/// The "oracle" database is the name of the PostgreSQL DB that we're using as an oracle to verify
/// that the ReadySet behavior matches the native Postgres behavior.
const ORACLE_DB_NAME: &str = "vertical_ddl_oracle";

/// Gets the [`Config`] used to connect to the PostgreSQL oracle DB.
fn oracle_db_config() -> Config {
    let mut upstream_config = psql_helpers::upstream_config();
    upstream_config.dbname(ORACLE_DB_NAME);
    upstream_config
}

/// Drops and recreates the oracle database prior to each test run.
async fn recreate_oracle_db() {
    let mut config = oracle_db_config();
    let (client, connection) = config.dbname("postgres").connect(NoTls).await.unwrap();
    tokio::spawn(connection);

    let drop_query = format!("DROP DATABASE IF EXISTS {ORACLE_DB_NAME}");
    let create_query = format!("CREATE DATABASE {ORACLE_DB_NAME}");

    client.simple_query(&drop_query).await.unwrap();
    client.simple_query(&create_query).await.unwrap();
}

/// Converts a [`Vec`] of [`Row`] values to a nested [`Vec`] of [`DfValue`] values. Currently just
/// used as a convenient way to compare two query results, since you can't directly compare
/// [`Row`]s with each other.
fn rows_to_dfvalue_vec(rows: Vec<Row>) -> Vec<Vec<DfValue>> {
    rows.iter()
        .map(|row| {
            (0..row.len())
                .map(|idx| row.get::<usize, DfValue>(idx))
                .collect()
        })
        .collect()
}

/// Run a single test case by:
///  * Setting up a test instance of ReadySet that connects to an upstream instance of Postgres
///  * Wiping and recreating a fresh copy of the oracle database directly in Postgres, and setting
///    up a connection
///  * Running each step in `ops`, and checking afterward that:
///    * The contents of the tables tracked by our model match across both ReadySet and Postgres
///    * Any deleted tables appear as deleted in both ReadySet and Postgres
async fn run(ops: Vec<Operation>) {
    readyset_tracing::init_test_logging();

    let (opts, _handle, shutdown_tx) = TestBuilder::default()
        .fallback(true)
        .build::<PostgreSQLAdapter>()
        .await;
    let rs_conn = connect(opts).await;

    recreate_oracle_db().await;
    let pg_conn = connect(oracle_db_config()).await;

    let mut runtime_state = TestModel::default();

    for op in ops {
        println!("Running op: {op:?}");
        match &op {
            Operation::CreateTable(table_name, cols) => {
                let non_pkey_cols = cols
                    .iter()
                    .map(|ColumnSpec { name, sql_type, .. }| format!("\"{name}\" {sql_type}"));
                let col_defs: Vec<String> = once("id INT PRIMARY KEY".to_string())
                    .chain(non_pkey_cols)
                    .collect();
                let col_defs = col_defs.join(", ");
                let query = format!("CREATE TABLE \"{table_name}\" ({col_defs})");
                rs_conn.simple_query(&query).await.unwrap();
                pg_conn.simple_query(&query).await.unwrap();

                let create_cache =
                    format!("CREATE CACHE ALWAYS FROM SELECT * FROM \"{table_name}\"");
                eventually!(run_test: {
                    let result = rs_conn.simple_query(&create_cache).await;
                    AssertUnwindSafe(move || result)
                }, then_assert: |result| {
                    result().unwrap()
                });
            }
            Operation::DropTable(name) => {
                let query = format!("DROP TABLE \"{name}\" CASCADE");
                rs_conn.simple_query(&query).await.unwrap();
                pg_conn.simple_query(&query).await.unwrap();
            }
            Operation::WriteRow {
                table,
                pkey,
                col_vals,
                ..
            } => {
                let pkey = DfValue::from(*pkey);
                let params: Vec<&DfValue> = once(&pkey).chain(col_vals.iter()).collect();
                let placeholders: Vec<_> = (1..=params.len()).map(|n| format!("${n}")).collect();
                let placeholders = placeholders.join(", ");
                let query = format!("INSERT INTO \"{table}\" VALUES ({placeholders})");
                rs_conn.query_raw(&query, &params).await.unwrap();
                pg_conn.query_raw(&query, &params).await.unwrap();
            }
            Operation::AddColumn(table_name, col_spec) => {
                let query = format!(
                    "ALTER TABLE \"{}\" ADD COLUMN \"{}\" {}",
                    table_name, col_spec.name, col_spec.sql_type
                );
                rs_conn.simple_query(&query).await.unwrap();
                pg_conn.simple_query(&query).await.unwrap();
            }
            Operation::DropColumn(table_name, col_name) => {
                let query = format!(
                    "ALTER TABLE \"{}\" DROP COLUMN \"{}\"",
                    table_name, col_name
                );
                rs_conn.simple_query(&query).await.unwrap();
                pg_conn.simple_query(&query).await.unwrap();
            }
            Operation::AlterColumnName {
                table,
                col_name,
                new_name,
            } => {
                let query = format!(
                    "ALTER TABLE \"{}\" RENAME COLUMN \"{}\" TO \"{}\"",
                    table, col_name, new_name
                );
                rs_conn.simple_query(&query).await.unwrap();
                pg_conn.simple_query(&query).await.unwrap();
            }
            Operation::DeleteRow(table_name, key) => {
                let query = format!("DELETE FROM \"{table_name}\" WHERE id = ({key})");
                rs_conn.simple_query(&query).await.unwrap();
                pg_conn.simple_query(&query).await.unwrap();
            }
            Operation::CreateSimpleView { name, table_source } => {
                let query = format!("CREATE VIEW \"{name}\" AS SELECT * FROM \"{table_source}\"");
                rs_conn.simple_query(&query).await.unwrap();
                pg_conn.simple_query(&query).await.unwrap();
                let create_cache = format!("CREATE CACHE ALWAYS FROM SELECT * FROM \"{name}\"");
                eventually!(run_test: {
                    let result = rs_conn.simple_query(&create_cache).await;
                    AssertUnwindSafe(move || result)
                }, then_assert: |result| {
                    result().unwrap()
                });
            }
            Operation::DropView(name) => {
                let query = format!("DROP VIEW \"{name}\"");
                rs_conn.simple_query(&query).await.unwrap();
                pg_conn.simple_query(&query).await.unwrap();
            }
        }

        // Note that if we were verifying results of any operations directly, it would be better to
        // wait until checking postconditions before we compute the next state; this would
        // complicate the postcondition code a bit, but would allow us to check that the op result
        // makes sense given the state *prior* to the operation being run. However, right now we
        // just check that the database contents match, so it's simpler to update the expected
        // state right away and then check the table contents based on that.
        runtime_state.next_state(&op);

        // After each op, check that all table and view contents match
        for relation in runtime_state
            .tables
            .keys()
            .chain(runtime_state.views.keys())
        {
            eventually!(run_test: {
                let rs_rows = rs_conn
                    .query(&format!("SELECT * FROM \"{relation}\""), &[])
                    .await
                    .unwrap();
                let pg_rows = pg_conn
                    .query(&format!("SELECT * FROM \"{relation}\""), &[])
                    .await
                    .unwrap();
                // Previously, we would run all the result handling in the run_test block, but
                // doing it in the then_assert block lets us work around a tokio-postgres client
                // crash caused by ENG-2548 by retrying until ReadySet stops sending us bad
                // packets.
                AssertUnwindSafe(move || (rs_rows, pg_rows))
            }, then_assert: |results| {
                let (rs_rows, pg_rows) = results();

                let mut rs_results = rows_to_dfvalue_vec(rs_rows);
                let mut pg_results = rows_to_dfvalue_vec(pg_rows);

                rs_results.sort_unstable();
                pg_results.sort_unstable();

                assert_eq!(pg_results, rs_results);
            });
        }
        // Also make sure all deleted tables were actually deleted:
        for table in &runtime_state.deleted_tables {
            rs_conn
                .query(&format!("DROP TABLE \"{table}\""), &[])
                .await
                .unwrap_err();
        }
    }

    shutdown_tx.shutdown().await;
}

proptest! {
    #![proptest_config(ProptestConfig {
        max_shrink_iters: MAX_OPS as u32 * 2,
        .. ProptestConfig::default()
    })]
    #[test]
    #[cfg_attr(not(feature = "ddl_vertical_tests"), ignore)]
    fn run_cases(steps in TestModel::default()) {
        prop_assume!(preconditions_hold(&steps));
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(run(steps));
    }

    #[test]
    #[ignore]
    fn print_cases(steps in TestModel::default()) {
        // This is mostly just useful for debugging test generation, hence it being marked ignored.
        for op in steps {
            println!("{:?}", op);
        }
        println!("----------");
    }
}
