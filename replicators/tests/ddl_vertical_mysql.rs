//! This test suite implements the [Replicator Vertical Testing Doc][doc].
//!
//! [doc]: https://docs.google.com/document/d/1GRYV7okEzz2T-KuF06M5Y4EkyRv7euMSb1viroT9JTk
//!
//! Note that this test suite is ignored by default, and conditionally de-ignored with the
//! `ddl_vertical_tests` feature to prevent it running in normal builds (since it's slow and may
//! find new bugs); to run it locally run:
//!
//! ```notrust
//! cargo test -p replicators --features ddl_vertical_tests --test ddl_vertical_mysql
//! ```
//!
//! This test suite will connect to a local MySQL database, which can be set up with all the
//! correct configuration using the `docker-compose.yml` and `docker-compose.override.example.yml`
//! in the build directory of the repository. To run that MySQL database, run:
//!
//! ```notrust
//! $ cp docker-compose.override.example.yml docker-compose.yml
//! $ docker-compose up -d MySQL
//! ```
//!
//! Note that this test suite requires the *exact* configuration specified in that docker-compose
//! configuration, including the port, username, and password.

use std::cell::RefCell;
use std::collections::{BTreeMap, HashSet};
use std::fmt::{Debug, Formatter, Result};
use std::iter::once;
use std::panic::AssertUnwindSafe;
use std::sync::LazyLock;
use std::time::Duration;

use async_trait::async_trait;
use itertools::Itertools;
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, OptsBuilder, Params, Row};
use proptest::prelude::*;
use proptest::strategy::{BoxedStrategy, Just, Strategy};
use proptest::{collection, sample};
use proptest_stateful::{
    proptest_config_with_local_failure_persistence, ModelState, ProptestStatefulConfig,
};
use readyset_client::SingleKeyEviction;
use readyset_client_test_helpers::mysql_helpers::MySQLAdapter;
use readyset_client_test_helpers::{mysql_helpers, TestBuilder};
use readyset_data::DfValue;
use readyset_server::Handle;
use readyset_sql::ast::SqlType;
use readyset_sql::DialectDisplay;
use readyset_util::arbitrary::arbitrary_timestamp_naive_date_time;
use readyset_util::eventually;
use readyset_util::shutdown::ShutdownSender;

static CLIENT: LazyLock<reqwest::Client> = LazyLock::new(reqwest::Client::new);

// Disabling update case while REA-4432 is being worked on
//const SQL_NAME_REGEX: &str = "[a-zA-Z_][a-zA-Z0-9_]*";
const SQL_NAME_REGEX: &str = "[a-z_][a-z0-9_]+";

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
    type Parameters = BTreeMap<String, Vec<String>>;
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
        let name_gen = SQL_NAME_REGEX
            .prop_filter("Can't generate additional columns named id", |s| {
                s.to_lowercase() != "id"
            });
        let col_types = vec![
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
                SqlType::Text,
                any::<String>().prop_map(DfValue::from).boxed(),
            ),
            (
                SqlType::Blob,
                any::<Vec<u8>>().prop_map(DfValue::from).boxed(),
            ),
            (
                SqlType::DateTime(None),
                arbitrary_timestamp_naive_date_time()
                    .prop_map(DfValue::from)
                    .boxed(),
            ),
            (
                SqlType::Timestamp,
                arbitrary_timestamp_naive_date_time()
                    .prop_map(DfValue::from)
                    .boxed(),
            ),
        ];
        (name_gen, sample::select(col_types))
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
    /// Creates a view that does a SELECT * on a JOIN of two tables
    CreateJoinView {
        name: String,
        table_a: String,
        table_b: String,
    },
    /// Drops the view with the given name
    DropView(String),
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
    /// Adds a new key to an existing table
    AddKey { table: String, column: String },
}

// Generators for Operation:

fn gen_column_specs() -> impl Strategy<Value = Vec<ColumnSpec>> {
    collection::vec(any_with::<ColumnSpec>(Default::default()), 1..4)
        .prop_filter("duplicate column names not allowed", |specs| {
            specs.iter().map(|cs| &cs.name).all_unique()
        })
}

prop_compose! {
    fn gen_create_table()
                       (name in SQL_NAME_REGEX, cols in gen_column_specs())
                       -> Operation {
        Operation::CreateTable(name, cols)
    }
}

prop_compose! {
    fn gen_drop_table(tables: Vec<String>)(t in sample::select(tables)) -> Operation {
        Operation::DropTable(t)
    }
}

prop_compose! {
    fn gen_write_row(tables: BTreeMap<String, Vec<ColumnSpec>>, pkeys: BTreeMap<String, Vec<i32>>)
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
    fn gen_add_key(tables: BTreeMap<String, Vec<ColumnSpec>>)
    (t in sample::select(tables.keys().cloned().collect::<Vec<_>>()))
    (table in Just(t.clone()),
     col_name in sample::select(tables[&t].iter().map(|cs| cs.name.clone()).collect::<Vec<_>>()))
    -> Operation {
        Operation::AddKey { table, column: col_name }
    }
}

prop_compose! {
    fn gen_add_col_unfiltered(tables: Vec<String>)
                             (t in sample::select(tables), col in any::<ColumnSpec>())
                             -> Operation {
        Operation::AddColumn(t, col)
    }
}

fn gen_add_col(tables: BTreeMap<String, Vec<ColumnSpec>>) -> impl Strategy<Value = Operation> {
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
    SQL_NAME_REGEX.prop_filter("Can't generate additional columns named id", |s| {
        s.to_lowercase() != "id"
    })
}

prop_compose! {
    fn gen_rename_col(tables: BTreeMap<String, Vec<ColumnSpec>>, tables_with_cols: Vec<String>)
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
    fn gen_drop_col(tables: BTreeMap<String, Vec<ColumnSpec>>, tables_with_cols: Vec<String>)
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
    fn gen_delete_row(non_empty_tables: Vec<String>, pkeys: BTreeMap<String, Vec<i32>>)
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
    fn gen_create_join_view(tables: Vec<String>)
                           (name in SQL_NAME_REGEX,
                            source_tables in sample::subsequence(tables, 2..=2))
                           -> Operation {
        let table_a = source_tables[0].clone();
        let table_b = source_tables[1].clone();
        Operation::CreateJoinView { name, table_a, table_b }
    }
}

prop_compose! {
    fn gen_drop_view(views: Vec<String>)(name in sample::select(views)) -> Operation {
        Operation::DropView(name)
    }
}

/// A definition for a test view. Currently one of:
///  - Simple (SELECT * FROM table)
///  - Join (SELECT * FROM table_a JOIN table_b ON table_a.id = table_b.id)
#[derive(Clone, Debug)]
enum TestViewDef {
    Simple(String),
    Join { table_a: String, table_b: String },
}

struct DDLTestRunContext {
    rs_host: String,
    rs_conn: Conn,
    mysql_conn: Conn,
    shutdown_tx: Option<ShutdownSender>, // Needs to be Option so we can move it out of the struct
    _handle: Handle,
}

/// A model of the current test state, used to help generate operations in a way that we expect to
/// succeed, as well as to assist in shrinking, and to determine postconditions to check during
/// test runtime.
///
/// Initially we assume an empty database, but as operations are generated, tracking their expected
/// results helps inform which operations we are able to test further along in the test case. For
/// example, when writing a row, we must pick a table to write the row to, so we must look at the
/// model state to see what tables have been previously created. We don't actually run a test case
/// until all the steps have been generated, so [`DDLModelState`] allows us to simulate the
/// expected state of the system for a given test case without having to actually run any of the
/// steps against the system under test.
#[derive(Clone, Debug, Default)]
struct DDLModelState {
    // We use BTreeMap instead of HashMap so that the `keys()` method gives us a deterministic
    // ordering, which allows us to reliably regenerate the same test case for a given seed.
    tables: BTreeMap<String, Vec<ColumnSpec>>,
    deleted_tables: HashSet<String>,
    pkeys: BTreeMap<String, Vec<i32>>, // Primary keys in use for each table
    // Map of view name to view definition
    views: BTreeMap<String, TestViewDef>,
    deleted_views: HashSet<String>,
}

#[async_trait(?Send)]
impl ModelState for DDLModelState {
    type Operation = Operation;
    type RunContext = DDLTestRunContext;
    type OperationStrategy = BoxedStrategy<Operation>;

    /// Each invocation of this function returns a [`Vec`] of [`Strategy`]s for generating
    /// [`Operation`]s *given the current state of the test model*. With a brand new model, the only
    /// possible operation is [`Operation::CreateTable`], but as
    /// tables/types are created and rows are written, other operations become possible.
    ///
    /// Note that there is some redundancy between the logic in this function and the logic in
    /// [`Operation::preconditions`](enum.Operation.html#method.preconditions). This is necessary
    /// because `op_generators` is used for the initial test generation, but the preconditions are
    /// used during shrinking. (Technically, we do also check and filter on preconditions at the
    /// start of each test, but it's best to depend on that check as little as possible since
    /// test filters like that can lead to slow and lopsided test generation.)
    fn op_generators(&self) -> Vec<Self::OperationStrategy> {
        let create_table_strat = gen_create_table().boxed();
        // We can also always try to issue an eviction:
        let evict_strategy = Just(Operation::Evict {
            inner: RefCell::new(None),
        })
        .boxed();

        let mut possible_ops = vec![create_table_strat, evict_strategy];

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
            let add_key_strat = gen_add_key(self.tables.clone()).boxed();

            possible_ops.extend([
                drop_strategy,
                write_strategy,
                add_col_strat,
                create_simple_view_strat,
                add_key_strat,
            ]);
        }

        // If we have at least two tables, we can create a join view:
        if self.tables.len() > 1 {
            let create_join_view_strat =
                gen_create_join_view(self.tables.keys().cloned().collect()).boxed();
            possible_ops.push(create_join_view_strat);
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
            // Commented out for now because this triggers REA-2216
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
        possible_ops
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
                self.deleted_tables.remove(name);
                // Also remove the name from deleted_views if it exists, since we should no longer
                // expect "SELECT * FROM name" to return an error and can stop checking that
                // postcondition:
                self.deleted_views.remove(name);
            }
            Operation::DropTable(name) => {
                self.tables.remove(name);
                self.deleted_tables.insert(name.clone());
                self.pkeys.remove(name);
                self.views.retain(|_view_name, view_def| match view_def {
                    TestViewDef::Simple(table_source) => name != table_source,
                    TestViewDef::Join { table_a, table_b } => name != table_a && name != table_b,
                });
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
                let spec = col_specs
                    .iter_mut()
                    .find(|cs| cs.name == *col_name)
                    .unwrap();
                spec.name.clone_from(new_name);
                // MySQL does not update the column name in views when the column is renamed in the
                // table. We need to drop all views pointing to the table.
                self.views.retain(|_view_name, view_def| match view_def {
                    TestViewDef::Simple(table_source) => table != table_source,
                    TestViewDef::Join { table_a, table_b } => table != table_a && table != table_b,
                });
            }
            Operation::DeleteRow(..) => (),
            Operation::CreateSimpleView { name, table_source } => {
                self.views
                    .insert(name.clone(), TestViewDef::Simple(table_source.clone()));
                self.deleted_views.remove(name);
                // Also remove the name from deleted_tables if it exists, since we should no longer
                // expect "SELECT * FROM name" to return an error and can stop checking that
                // postcondition:
                self.deleted_tables.remove(name);
            }
            Operation::CreateJoinView {
                name,
                table_a,
                table_b,
            } => {
                let table_a = table_a.clone();
                let table_b = table_b.clone();
                let view_def = TestViewDef::Join { table_a, table_b };
                self.views.insert(name.clone(), view_def);
                self.deleted_views.remove(name);
                // See comment in CreateSimpleView clause above for why this is needed:
                self.deleted_tables.remove(name);
            }
            Operation::DropView(name) => {
                self.views.remove(name);
                self.deleted_views.insert(name.clone());
            }
            Operation::Evict { .. } => (),
            Operation::AddKey { .. } => (),
        }
    }

    /// Checks preconditions for an [`Operation`] given a current test model state.
    ///
    /// These are primarily needed for shrinking, so that we can make sure that we don't do things
    /// like remove a [`Operation::CreateTable`] when a later [`Operation::WriteRow`] operation
    /// depends on the corresponding table.
    ///
    /// We also check preconditions during runtime, and throw out any test cases where the
    /// preconditions aren't satisfied. This should be rare, though, because
    /// [`DDLModelState::op_generators`] should *usually* only generate cases where the
    /// preconditions are already satisfied. It's possible there are weird corner cases though
    /// (such as multiple random strings happening to generate the same string value for two
    /// different table names) where preconditions could save us from a false positive test
    /// failure.
    fn preconditions_met(&self, op: &Self::Operation) -> bool {
        match op {
            Operation::CreateTable(name, _) => !self.name_in_use(name),
            Operation::DropTable(name) => self.tables.contains_key(name),
            Operation::WriteRow {
                table,
                pkey,
                col_vals: _,
                col_types,
            } => {
                // Make sure that the table doesn't already contain a row with this key, and also
                // make sure that the column types in the table also match up with the types in the
                // row that we're trying to write:
                self.pkeys
                    .get(table)
                    .is_some_and(|table_keys| !table_keys.contains(pkey))
                    && self.tables.get(table).is_some_and(|table_cols| {
                        // Must compare lengths before zipping and comparing individual types
                        // because zip will drop elements if the Vec lengths don't match up:
                        table_cols.len() == col_types.len()
                            // Make sure all types in the WriteRow match the table cols:
                            && table_cols
                                .iter()
                                .zip(col_types)
                                .all(|(cs, row_type)| cs.sql_type == *row_type)
                    })
            }
            Operation::DeleteRow(table, key) => self
                .pkeys
                .get(table)
                .is_some_and(|table_keys| table_keys.contains(key)),
            Operation::AddColumn(table, column_spec) => self
                .tables
                .get(table)
                .is_some_and(|t| t.iter().all(|cs| cs.name != *column_spec.name)),
            Operation::AddKey { table, column } => self
                .tables
                .get(table)
                .is_some_and(|t| t.iter().any(|cs| cs.name == *column)),
            Operation::DropColumn(table, col_name) => self
                .tables
                .get(table)
                .is_some_and(|t| t.iter().any(|cs| cs.name == *col_name)),
            Operation::AlterColumnName {
                table,
                col_name,
                new_name,
            } => self.tables.get(table).is_some_and(|t| {
                t.iter().any(|cs| cs.name == *col_name) && t.iter().all(|cs| cs.name != *new_name)
            }),
            Operation::CreateSimpleView { name, table_source } => {
                !self.name_in_use(name) && self.tables.contains_key(table_source)
            }
            Operation::CreateJoinView {
                name,
                table_a,
                table_b,
            } => {
                self.tables.contains_key(table_a)
                    && self.tables.contains_key(table_b)
                    && !self.name_in_use(name)
            }
            Operation::DropView(name) => self.views.contains_key(name),
            // Even if the key is shrunk out, evicting it is a no-op, so we don't need to worry
            // about preconditions at all for evictions:
            Operation::Evict { .. } => true,
        }
    }

    /// Get ready to run a single test case by:
    ///  * Setting up a test instance of ReadySet that connects to an upstream instance of MySQL
    ///  * Wiping and recreating a fresh copy of the oracle database directly in MySQL, and setting
    ///    up a connection
    async fn init_test_run(&self) -> Self::RunContext {
        readyset_tracing::init_test_logging();

        let (opts, handle, shutdown_tx) = TestBuilder::default()
            .fallback(true)
            .build::<MySQLAdapter>()
            .await;
        // We need the raw hostname for eviction operations later:
        let rs_host = opts.ip_or_hostname().to_string();
        let rs_conn = connect(OptsBuilder::from_opts(opts)).await;

        recreate_oracle_db().await;
        let mysql_conn = connect(oracle_db_config()).await;

        DDLTestRunContext {
            rs_host,
            rs_conn,
            mysql_conn,
            _handle: handle,
            shutdown_tx: Some(shutdown_tx),
        }
    }

    /// Run the code to test a single operation:
    ///  * Running each step in `ops`, and checking afterward that:
    ///    * The contents of the tables tracked by our model match across both ReadySet and MySQL
    ///    * Any deleted tables appear as deleted in both ReadySet and MySQL
    async fn run_op(&self, op: &Self::Operation, ctxt: &mut Self::RunContext) {
        let DDLTestRunContext {
            rs_conn,
            mysql_conn,
            ..
        } = ctxt;

        match op {
            Operation::CreateTable(table_name, cols) => {
                let non_pkey_cols = cols.iter().map(|ColumnSpec { name, sql_type, .. }| {
                    format!(
                        "`{name}` {}",
                        sql_type.display(readyset_sql::Dialect::MySQL)
                    )
                });
                let col_defs: Vec<String> = once("id INT PRIMARY KEY".to_string())
                    .chain(non_pkey_cols)
                    .collect();
                let col_defs = col_defs.join(", ");
                let query = format!("CREATE TABLE `{table_name}` ({col_defs})");
                println!("Creating table: {query}");
                rs_conn.query_drop(&query).await.unwrap();
                mysql_conn.query_drop(&query).await.unwrap();

                let create_cache = format!("CREATE CACHE ALWAYS FROM SELECT * FROM `{table_name}`");
                eventually!(run_test: {
                    let result = rs_conn.query_drop(&create_cache).await;
                    AssertUnwindSafe(move || result)
                }, then_assert: |result| {
                    result().unwrap()
                });
            }
            Operation::DropTable(name) => {
                let query = format!("DROP TABLE `{name}` CASCADE");
                rs_conn.query_drop(&query).await.unwrap();
                mysql_conn.query_drop(&query).await.unwrap();
            }
            Operation::WriteRow {
                table,
                pkey,
                col_vals,
                ..
            } => {
                let pkey = DfValue::from(*pkey);
                let params: Vec<&DfValue> = once(&pkey).chain(col_vals.iter()).collect();
                let params: Vec<mysql_async::Value> = params
                    .iter()
                    .map(|v| match v {
                        DfValue::Int(_) => mysql_async::Value::Int(v.to_string().parse().unwrap()),
                        DfValue::Float(_) => {
                            mysql_async::Value::Float(v.to_string().parse().unwrap())
                        }
                        _ => mysql_async::Value::Bytes(v.to_string().as_bytes().to_vec()),
                    })
                    .collect();
                let placeholders: Vec<_> = (1..=params.len()).map(|_| "?".to_string()).collect();
                let placeholders = placeholders.join(", ");
                let query = format!("INSERT INTO `{table}` VALUES ({placeholders})");
                rs_conn.exec_drop(&query, &params).await.unwrap();
                mysql_conn.exec_drop(&query, &params).await.unwrap();
            }
            Operation::AddColumn(table_name, col_spec) => {
                let query = format!(
                    "ALTER TABLE `{}` ADD COLUMN `{}` {}",
                    table_name,
                    col_spec.name,
                    col_spec.sql_type.display(readyset_sql::Dialect::MySQL)
                );
                rs_conn.query_drop(&query).await.unwrap();
                mysql_conn.query_drop(&query).await.unwrap();
            }
            Operation::AddKey { table, column } => {
                let col_type = &self.tables[table]
                    .iter()
                    .find(|cs| cs.name == *column)
                    .unwrap()
                    .sql_type;
                let key_len = match col_type {
                    SqlType::Blob | SqlType::Text => "(10)",
                    _ => "",
                };
                let query = format!("ALTER TABLE `{table}` ADD KEY (`{column}`{key_len})");
                println!("Adding key: {query}");
                rs_conn.query_drop(&query).await.unwrap();
                mysql_conn.query_drop(&query).await.unwrap();
            }
            Operation::DropColumn(table_name, col_name) => {
                let query = format!("ALTER TABLE `{table_name}` DROP COLUMN `{col_name}`");
                rs_conn.query_drop(&query).await.unwrap();
                mysql_conn.query_drop(&query).await.unwrap();
            }
            Operation::AlterColumnName {
                table,
                col_name,
                new_name,
            } => {
                let query =
                    format!("ALTER TABLE `{table}` RENAME COLUMN `{col_name}` TO `{new_name}`");
                for (view, def) in self.views.iter() {
                    match def {
                        TestViewDef::Simple(table_source) => {
                            if table == table_source {
                                let drop_view = format!("DROP VIEW `{view}`");
                                rs_conn.query_drop(&drop_view).await.unwrap();
                                mysql_conn.query_drop(&drop_view).await.unwrap();
                            }
                        }
                        TestViewDef::Join { table_a, table_b } => {
                            if table == table_a || table == table_b {
                                let drop_view = format!("DROP VIEW `{view}`");
                                rs_conn.query_drop(&drop_view).await.unwrap();
                                mysql_conn.query_drop(&drop_view).await.unwrap();
                            }
                        }
                    }
                }
                rs_conn.query_drop(&query).await.unwrap();
                mysql_conn.query_drop(&query).await.unwrap();
            }
            Operation::DeleteRow(table_name, key) => {
                let query = format!("DELETE FROM `{table_name}` WHERE id = ({key})");
                rs_conn.query_drop(&query).await.unwrap();
                mysql_conn.query_drop(&query).await.unwrap();
            }
            Operation::CreateSimpleView { name, table_source } => {
                let query = format!("CREATE VIEW `{name}` AS SELECT * FROM `{table_source}`");
                rs_conn.query_drop(&query).await.unwrap();
                mysql_conn.query_drop(&query).await.unwrap();
                let create_cache = format!("CREATE CACHE ALWAYS FROM SELECT * FROM `{name}`");
                eventually!(run_test: {
                    let result = rs_conn.query_drop(&create_cache).await;
                    AssertUnwindSafe(move || result)
                }, then_assert: |result| {
                    result().unwrap()
                });
            }
            Operation::CreateJoinView {
                name,
                table_a,
                table_b,
            } => {
                // Must give a unique alias to each column in the source tables to avoid issues
                // with duplicate column names in the resulting view
                let select_list: Vec<String> = self.tables[table_a]
                    .iter()
                    .map(|cs| (table_a, &cs.name))
                    .chain(self.tables[table_b].iter().map(|cs| (table_b, &cs.name)))
                    .enumerate()
                    .map(|(i, (tab, col))| format!("`{tab}`.`{col}` AS `c{i}`"))
                    .collect();
                let select_list = select_list.join(", ");
                let view_def = format!(
                    "SELECT {select_list} FROM `{table_a}` JOIN `{table_b}` ON `{table_a}`.`id` = `{table_b}`.`id`"
                );
                let query = format!("CREATE VIEW `{name}` AS {view_def}");
                rs_conn.query_drop(&query).await.unwrap();
                mysql_conn.query_drop(&query).await.unwrap();
                let create_cache = format!("CREATE CACHE ALWAYS FROM SELECT * FROM `{name}`");
                eventually!(run_test: {
                    let result = rs_conn.query_drop(&create_cache).await;
                    AssertUnwindSafe(move || result)
                }, then_assert: |result| {
                    result().unwrap()
                });
            }
            Operation::DropView(name) => {
                let query = format!("DROP VIEW `{name}`");
                rs_conn.query_drop(&query).await.unwrap();
                mysql_conn.query_drop(&query).await.unwrap();
            }
            Operation::Evict { inner } => {
                let body =
                    bincode::serialize::<Option<SingleKeyEviction>>(&*inner.borrow()).unwrap();
                let res = CLIENT
                    .post(format!("http://{}:6033/evict_single", ctxt.rs_host))
                    .body(body)
                    .send()
                    .await
                    .unwrap();
                if inner.borrow().is_none() {
                    let eviction = bincode::deserialize::<Option<SingleKeyEviction>>(
                        &res.bytes().await.unwrap(),
                    )
                    .unwrap();
                    inner.replace(eviction);
                }
            }
        }
    }

    async fn check_postconditions(&self, ctxt: &mut Self::RunContext) {
        let DDLTestRunContext {
            rs_conn,
            mysql_conn,
            ..
        } = ctxt;

        // After each op, check that all table and view contents match
        for relation in self.tables.keys().chain(self.views.keys()) {
            eventually!(run_test: {
                println!("Checking contents of relation: {}", relation);
                let rs_rows = rs_conn
                    .exec(format!("SELECT * FROM `{relation}`"), Params::Empty)
                    .await
                    .unwrap();
                let mysql_rows = mysql_conn
                    .exec(format!("SELECT * FROM `{relation}`"), Params::Empty)
                    .await
                    .unwrap();
                // Previously, we would run all the result handling in the run_test block, but
                // doing it in the then_assert block lets us work around a tokio-MySQL client
                // crash caused by ENG-2548 by retrying until ReadySet stops sending us bad
                // packets.
                AssertUnwindSafe(move || (rs_rows, mysql_rows))
            }, then_assert: |results| {
                let (rs_rows, mysql_rows) = results();

                let mut rs_results = rows_to_dfvalue_vec(rs_rows);
                let mut mysql_results = rows_to_dfvalue_vec(mysql_rows);

                rs_results.sort_unstable();
                mysql_results.sort_unstable();

                assert_eq!(mysql_results, rs_results);
            });
        }
        // Also make sure all deleted tables were actually deleted:
        for table in &self.deleted_tables {
            rs_conn
                .query_drop(format!("DROP TABLE `{table}`"))
                .await
                .unwrap_err();
        }
        // And then do the same for views:
        for view in &self.deleted_views {
            rs_conn
                .query_drop(format!("DROP VIEW `{view}`"))
                .await
                .unwrap_err();
        }
    }

    async fn clean_up_test_run(&self, ctxt: &mut Self::RunContext) {
        ctxt.shutdown_tx.take().unwrap().shutdown().await
    }
}

impl DDLModelState {
    /// Returns whether the given name is in use in the model state as any of a table, a view, or
    /// an enum type.
    ///
    /// This is useful for checking preconditions, because MySQL does not let you reuse the same
    /// name for any of these three things (e.g. you're not allowed to name a view and a type the
    /// same thing).
    fn name_in_use(&self, name: &String) -> bool {
        self.tables.contains_key(name) || self.views.contains_key(name)
    }
}

/// Spawns a new connection (either to ReadySet or directly to MySQL).
async fn connect(config: OptsBuilder) -> Conn {
    mysql_async::Conn::new(config).await.unwrap()
}

/// The "oracle" database is the name of the MySQL DB that we're using as an oracle to verify
/// that the ReadySet behavior matches the native MySQL behavior.
const ORACLE_DB_NAME: &str = "vertical_ddl_oracle";

/// Gets the [`OptsBuilder`] used to connect to the MySQL oracle DB.
fn oracle_db_config() -> OptsBuilder {
    mysql_helpers::upstream_config().db_name(Some(ORACLE_DB_NAME))
}

/// Drops and recreates the oracle database prior to each test run.
async fn recreate_oracle_db() {
    let config = mysql_helpers::upstream_config();
    let mut client = connect(config).await;

    let drop_query = format!("DROP DATABASE IF EXISTS {ORACLE_DB_NAME}");
    let create_query = format!("CREATE DATABASE {ORACLE_DB_NAME}");

    client.query_drop(&drop_query).await.unwrap();
    client.query_drop(&create_query).await.unwrap();
}

/// Although both are of the exact same type, there is a conflict between reexported versions
fn value_to_value(val: &mysql_async::Value) -> mysql_common::value::Value {
    match val {
        mysql_async::Value::NULL => mysql_common::value::Value::NULL,
        mysql_async::Value::Bytes(b) => mysql_common::value::Value::Bytes(b.clone()),
        mysql_async::Value::Int(i) => mysql_common::value::Value::Int(*i),
        mysql_async::Value::UInt(u) => mysql_common::value::Value::UInt(*u),
        mysql_async::Value::Float(f) => mysql_common::value::Value::Float(*f),
        mysql_async::Value::Double(d) => mysql_common::value::Value::Double(*d),
        mysql_async::Value::Date(y, m, d, hh, mm, ss, us) => {
            mysql_common::value::Value::Date(*y, *m, *d, *hh, *mm, *ss, *us)
        }
        mysql_async::Value::Time(is_neg, d, hh, mm, ss, us) => {
            mysql_common::value::Value::Time(*is_neg, *d, *hh, *mm, *ss, *us)
        }
    }
}

/// Converts a [`Vec`] of [`Row`] values to a nested [`Vec`] of [`DfValue`] values. Currently just
/// used as a convenient way to compare two query results, since you can't directly compare
/// [`Row`]s with each other.
fn rows_to_dfvalue_vec(rows: Vec<Row>) -> Vec<Vec<DfValue>> {
    rows.iter()
        .map(|row| {
            let mut noria_row = Vec::with_capacity(row.len());
            for idx in 0..row.len() {
                let val = value_to_value(row.as_ref(idx).unwrap());
                noria_row.push(readyset_data::DfValue::try_from(val).unwrap());
            }
            noria_row
        })
        .collect()
}

#[test]
#[cfg_attr(not(feature = "vertical_tests"), ignore)]
fn run_cases() {
    let config = ProptestStatefulConfig {
        min_ops: 10,
        max_ops: 20,
        test_case_timeout: Duration::from_secs(60),
        proptest_config: proptest_config_with_local_failure_persistence!(),
    };

    proptest_stateful::test::<DDLModelState>(config);
}
