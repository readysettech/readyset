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

use std::cell::Cell;
use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Display, Formatter, Result};
use std::iter::once;
use std::panic::AssertUnwindSafe;
use std::rc::Rc;
use std::time::Duration;

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
const TEST_CASE_TIMEOUT: Duration = Duration::from_secs(60);

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
    type Parameters = HashMap<String, Vec<String>>;
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with(enum_types: Self::Parameters) -> Self::Strategy {
        let name_gen = SQL_NAME_REGEX
            .prop_filter("Can't generate additional columns named \"id\"", |s| {
                s.to_lowercase() != "id"
            });
        let mut col_types = vec![
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
        ];
        let enum_col_types: Vec<_> = enum_types
            .into_iter()
            .map(|(name, values)| {
                (
                    // We use SqlType::Other instead of SqlType::Enum because we want to refer
                    // directly to the named enum type when we use this SqlType value to create a
                    // column definition in the corresponding CREATE TABLE statement:
                    SqlType::Other(name.into()),
                    sample::select(values).prop_map(DfValue::from).boxed(),
                )
            })
            .collect();
        col_types.extend_from_slice(&enum_col_types);
        (name_gen, sample::select(col_types))
            .prop_map(|(name, (sql_type, gen))| ColumnSpec {
                name,
                sql_type,
                gen,
            })
            .boxed()
    }
}

/// Used for the [`Operation::InsertEnumValue`] variant to specify where to add the new enum value.
#[derive(test_strategy::Arbitrary, Clone, Debug)]
enum EnumPos {
    Before,
    After,
}

impl Display for EnumPos {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        match self {
            EnumPos::Before => f.write_str("BEFORE"),
            EnumPos::After => f.write_str("AFTER"),
        }
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
    /// Creates an ENUM type with the given name and values
    CreateEnum(String, Vec<String>),
    /// Drops an ENUM type with the given name
    DropEnum(String),
    /// Adds a value to the end of an existing ENUM type with the given names
    AppendEnumValue {
        type_name: String,
        value_name: String,
    },
    InsertEnumValue {
        type_name: String,
        value_name: String,
        position: EnumPos,
        next_to_value: String,
    },
    /// Renames an existing ENUM type value
    RenameEnumValue {
        type_name: String,
        value_name: String,
        new_name: String,
    },
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
            Self::CreateTable(name, cols) => {
                !state.tables.contains_key(name)
                    && cols.iter().all(|cs| match cs {
                        ColumnSpec {
                            sql_type: SqlType::Other(type_name),
                            ..
                        } => state.enum_types.contains_key(type_name.name.as_str()),
                        _ => true,
                    })
            }
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
            Self::CreateJoinView {
                name,
                table_a,
                table_b,
            } => {
                state.tables.contains_key(table_a)
                    && state.tables.contains_key(table_b)
                    && !state.tables.contains_key(name)
                    && !state.views.contains_key(name)
            }
            Self::DropView(name) => state.views.contains_key(name),
            Self::CreateEnum(name, _values) => !state.enum_types.contains_key(name),
            Self::DropEnum(name) => tables_using_type(&state.tables, name).next().is_none(),
            Self::AppendEnumValue {
                type_name,
                value_name,
            } => state
                .enum_types
                .get(type_name)
                .map_or(false, |t| !t.contains(value_name)),
            Self::InsertEnumValue {
                type_name,
                value_name,
                next_to_value,
                ..
            } => state.enum_types.get(type_name).map_or(false, |t| {
                t.contains(next_to_value) && !t.contains(value_name)
            }),
            Self::RenameEnumValue {
                type_name,
                value_name,
                new_name,
            } => state
                .enum_types
                .get(type_name)
                .map_or(false, |t| t.contains(value_name) && !t.contains(new_name)),
        }
    }
}

/// Returns an iterator that yields names of tables that use the given type.
fn tables_using_type<'a>(
    tables: &'a HashMap<String, Vec<ColumnSpec>>,
    type_name: &'a str,
) -> impl Iterator<Item = &'a str> {
    tables.iter().filter_map(move |(name, columns)| {
        if columns.iter().any(|cs| cs.name == type_name) {
            Some(name.as_str())
        } else {
            None
        }
    })
}

// Generators for Operation:

fn gen_column_specs(
    enum_types: HashMap<String, Vec<String>>,
) -> impl Strategy<Value = Vec<ColumnSpec>> {
    collection::vec(any_with::<ColumnSpec>(enum_types), 1..4)
        .prop_filter("duplicate column names not allowed", |specs| {
            specs.iter().map(|cs| &cs.name).all_unique()
        })
}

prop_compose! {
    fn gen_enum_values()(values in collection::hash_set(SQL_NAME_REGEX, 1..4)) -> Vec<String> {
        // Note that order is deterministic based on the HashSet iterator implementation. If we
        // want the order to be random, we will have to be careful to make the random order
        // deterministic on the test case being run, which probably means we add a generator for a
        // random seed then use that to shuffle the resulting Vec below, since that way the
        // generated seed will be based on the seed for the given test case.
        values.into_iter().collect()
    }
}

prop_compose! {
    fn gen_create_table(enum_types: HashMap<String, Vec<String>>)
                       (name in SQL_NAME_REGEX, cols in gen_column_specs(enum_types))
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

prop_compose! {
    fn gen_create_enum()(name in SQL_NAME_REGEX, values in gen_enum_values()) -> Operation {
        Operation::CreateEnum(name, values)
    }
}

prop_compose! {
    fn gen_drop_enum(enum_types: Vec<String>)(name in sample::select(enum_types)) -> Operation {
        Operation::DropEnum(name)
    }
}

fn gen_append_enum_value(
    enum_types: HashMap<String, Vec<String>>,
) -> impl Strategy<Value = Operation> {
    gen_append_enum_value_inner(enum_types.keys().cloned().collect()).prop_filter(
        "Can't add duplicate value to existing ENUM type",
        move |op| match op {
            Operation::AppendEnumValue {
                type_name,
                value_name,
            } => !enum_types[type_name].contains(value_name),
            _ => unreachable!(),
        },
    )
}

prop_compose! {
    fn gen_append_enum_value_inner(enum_type_names: Vec<String>)
                               (type_name in sample::select(enum_type_names),
                                value_name in SQL_NAME_REGEX)
                               -> Operation {
        Operation::AppendEnumValue { type_name, value_name }
    }
}

prop_compose! {
    fn gen_insert_enum_value(enum_types: HashMap<String, Vec<String>>)
                            (et in sample::select(enum_types.keys().cloned().collect::<Vec<_>>()))
                            (next_to_value in sample::select(enum_types[&et].clone()),
                             type_name in Just(et),
                             position in any::<EnumPos>(),
                             value_name in SQL_NAME_REGEX)
                            -> Operation {
        Operation::InsertEnumValue { type_name, value_name, position, next_to_value }
    }
}

prop_compose! {
    fn gen_rename_enum_value(enum_types: HashMap<String, Vec<String>>)
                            (et in sample::select(enum_types.keys().cloned().collect::<Vec<_>>()))
                            (value_name in sample::select(enum_types[&et].clone()),
                             type_name in Just(et),
                             new_name in SQL_NAME_REGEX)
                            -> Operation {
        Operation::RenameEnumValue { type_name, value_name, new_name }
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
    // Map of view name to view definition
    views: HashMap<String, TestViewDef>,
    deleted_views: HashSet<String>,
    // Map of custom ENUM type names to type definitions (represented by a Vec of ENUM elements)
    enum_types: HashMap<String, Vec<String>>,

    // Just cloned into [`TestTree`] and used for shrinking, not part of the model used for testing
    // itself:
    next_step_idx: Rc<Cell<usize>>,
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
        // We can always create more tables or enum types, so start with those two generators:
        let create_table_strat = gen_create_table(self.enum_types.clone()).boxed();
        let create_enum_strat = gen_create_enum().boxed();
        let mut possible_ops = vec![create_table_strat, create_enum_strat];

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

        // If we have at least one enum type created, we can add a value or rename a value
        if !self.enum_types.is_empty() {
            let append_enum_value_strat = gen_append_enum_value(self.enum_types.clone()).boxed();
            possible_ops.push(append_enum_value_strat);

            let insert_enum_value_strat = gen_insert_enum_value(self.enum_types.clone()).boxed();
            possible_ops.push(insert_enum_value_strat);

            let _rename_enum_value_strat = gen_rename_enum_value(self.enum_types.clone()).boxed();
            // TODO uncomment after ENG-2823 is fixed
            //possible_ops.push(rename_enum_value_strat);
        }

        // If we have at least one enum type created, and no table is using it, we can drop an enum
        let unused_enums: Vec<String> = self
            .enum_types
            .keys()
            .filter_map(|name| {
                if !self.tables.values().any(|columns| {
                    columns
                        .iter()
                        .any(|cs| cs.sql_type == SqlType::Other(name.into()))
                }) {
                    Some(name.clone())
                } else {
                    None
                }
            })
            .collect();
        if !unused_enums.is_empty() {
            let drop_enum_strat = gen_drop_enum(unused_enums).boxed();
            possible_ops.push(drop_enum_strat);
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
                let mut spec = col_specs
                    .iter_mut()
                    .find(|cs| cs.name == *col_name)
                    .unwrap();
                spec.name = new_name.clone();
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
            Operation::CreateEnum(name, values) => {
                self.enum_types.insert(name.clone(), values.clone());
            }
            Operation::DropEnum(name) => {
                self.enum_types.remove(name);
            }
            Operation::AppendEnumValue {
                type_name,
                value_name,
            } => {
                self.enum_types
                    .get_mut(type_name)
                    .unwrap()
                    .push(value_name.clone());
            }
            Operation::InsertEnumValue {
                type_name,
                value_name,
                position,
                next_to_value,
            } => {
                let type_values = self.enum_types.get_mut(type_name).unwrap();
                let next_to_idx = type_values.iter().position(|v| v == next_to_value).unwrap();
                let insert_idx = match position {
                    EnumPos::Before => next_to_idx,
                    EnumPos::After => next_to_idx + 1,
                };
                type_values.insert(insert_idx, value_name.clone());
            }
            Operation::RenameEnumValue {
                type_name,
                value_name,
                new_name,
            } => {
                let val_ref = self
                    .enum_types
                    .get_mut(type_name)
                    .unwrap()
                    .iter_mut()
                    .find(|v| *v == value_name)
                    .unwrap();
                *val_ref = new_name.clone();
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
    /// The index of the next step we're planning to run. If the test fails, the last successful
    /// step will thus be `next_step_idx - 1`.
    next_step_idx: Rc<Cell<usize>>,
    /// Whether we've already used next_step_idx to trim out steps after the failure.
    have_trimmed_after_failure: bool,
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
    ///    * It is likely that we can randomly try removing multiple elements at once, since most
    ///    minimal failing cases are much smaller than the original failing cases that produced
    ///    them. The math around this is a bit tricky though, as the ideal number of elements to try
    ///    removing at any given time depends on how big we expect a typical minimal failing test
    ///    case to be.
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
        if !self.have_trimmed_after_failure {
            self.have_trimmed_after_failure = true; // Only run this block once per test case

            let next_step_idx = self.next_step_idx.get();

            // next_step_idx is the failing step, and we want to exclude steps *beyond* the failing
            // step, hence adding 1 to the start of the range:
            let steps_after_failure = next_step_idx + 1..self.ops.len();
            for i in steps_after_failure {
                self.currently_included[i] = false;
            }
            // Setting last_shrank_idx to next_step_idx causes us to skip trying to shrink the step
            // that failed, which should be good since the step that triggers the failure is
            // presumably needed to reproduce the failure:
            self.last_shrank_idx = next_step_idx;
        }

        self.try_removing_op()
    }

    /// Undoes the last call to [`simplify`], and attempts to remove another element instead.
    fn complicate(&mut self) -> bool {
        self.currently_included[self.last_shrank_idx] = true;
        self.try_removing_op()
    }
}

impl TestTree {
    /// Used by both [`simplify`] and [`complicate`] to attempt the removal of another operation
    /// during shrinking. If we are able to find another operation we can remove without violating
    /// any preconditions, this returns `true`; otherwise, this function returns `false`.
    fn try_removing_op(&mut self) -> bool {
        // More efficient to iterate backward, since we only need to go over the list once
        // (preconditions are based only on the state changes caused by previous ops, so if we're
        // moving backward through the list then the only time we'll be unable to remove an op
        // based on a precondition is if we've previously had to add back in a later op that
        // depends on it due to failing to remove the later op via shrinking; in that case we'll
        // never be able to remove the current op either, so there's no need to check it more than
        // once).
        while self.last_shrank_idx > 0 {
            self.last_shrank_idx -= 1;

            // try removing next idx to try, and see if preconditions still hold
            self.currently_included[self.last_shrank_idx] = false;
            let candidate_ops = self.current();
            // if they still hold, leave it as removed, and we're done
            if preconditions_hold(&candidate_ops) {
                return true;
            }
            // if they don't still hold, add it back and try another run through the loop
            self.currently_included[self.last_shrank_idx] = true;
        }
        // We got all the way through and didn't remove anything
        false
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
            next_step_idx: self.next_step_idx.clone(),
            have_trimmed_after_failure: false,
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
async fn run(ops: Vec<Operation>, next_step_idx: Rc<Cell<usize>>) {
    readyset_tracing::init_test_logging();

    let (opts, _handle, shutdown_tx) = TestBuilder::default()
        .fallback(true)
        .build::<PostgreSQLAdapter>()
        .await;
    let rs_conn = connect(opts).await;

    recreate_oracle_db().await;
    let pg_conn = connect(oracle_db_config()).await;

    let mut runtime_state = TestModel::default();

    for (idx, op) in ops.into_iter().enumerate() {
        println!("Running op {idx}: {op:?}");
        match &op {
            Operation::CreateTable(table_name, cols) => {
                let non_pkey_cols = cols.iter().map(|ColumnSpec { name, sql_type, .. }| {
                    format!(
                        "\"{name}\" {}",
                        sql_type.display(nom_sql::Dialect::PostgreSQL)
                    )
                });
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
                    table_name,
                    col_spec.name,
                    col_spec.sql_type.display(nom_sql::Dialect::PostgreSQL)
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
            Operation::CreateJoinView {
                name,
                table_a,
                table_b,
            } => {
                // Must give a unique alias to each column in the source tables to avoid issues
                // with duplicate column names in the resulting view
                let select_list: Vec<String> = runtime_state.tables[table_a]
                    .iter()
                    .chain(runtime_state.tables[table_b].iter())
                    .enumerate()
                    .map(|(i, cs)| format!("\"{}\" AS c{}", cs.name, i))
                    .collect();
                let select_list = select_list.join(", ");
                let view_def = format!(
                    "SELECT {} FROM \"{}\" JOIN \"{}\" ON \"{}\".id = \"{}\".id",
                    select_list, table_a, table_b, table_a, table_b
                );
                let query = format!("CREATE VIEW \"{}\" AS {}", name, view_def);
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
            Operation::CreateEnum(name, elements) => {
                let elements: Vec<String> = elements.iter().map(|e| format!("'{e}'")).collect();
                let element_list = elements.join(", ");
                // Quote the type name to prevent clashes with builtin types or reserved keywords
                let query = format!("CREATE TYPE \"{}\" AS ENUM ({})", name, element_list);
                rs_conn.simple_query(&query).await.unwrap();
                pg_conn.simple_query(&query).await.unwrap();
            }
            Operation::DropEnum(name) => {
                let query = format!("DROP TYPE \"{name}\"");
                rs_conn.simple_query(&query).await.unwrap();
                pg_conn.simple_query(&query).await.unwrap();
            }
            Operation::AppendEnumValue {
                type_name,
                value_name,
            } => {
                let query = format!("ALTER TYPE \"{type_name}\" ADD VALUE '{value_name}'");
                rs_conn.simple_query(&query).await.unwrap();
                pg_conn.simple_query(&query).await.unwrap();
                recreate_caches_using_type(type_name, &runtime_state.tables, &rs_conn).await;
            }
            Operation::InsertEnumValue {
                type_name,
                value_name,
                position,
                next_to_value,
            } => {
                let query = format!(
                    "ALTER TYPE \"{}\" ADD VALUE '{}' {} '{}'",
                    type_name, value_name, position, next_to_value
                );
                rs_conn.simple_query(&query).await.unwrap();
                pg_conn.simple_query(&query).await.unwrap();
                recreate_caches_using_type(type_name, &runtime_state.tables, &rs_conn).await;
            }
            Operation::RenameEnumValue {
                type_name,
                value_name,
                new_name,
            } => {
                let query = format!(
                    "ALTER TYPE \"{}\" RENAME VALUE '{}' TO '{}'",
                    type_name, value_name, new_name
                );
                rs_conn.simple_query(&query).await.unwrap();
                pg_conn.simple_query(&query).await.unwrap();
                recreate_caches_using_type(type_name, &runtime_state.tables, &rs_conn).await;
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
        // And then do the same for views:
        for view in &runtime_state.deleted_views {
            rs_conn
                .query(&format!("DROP VIEW \"{view}\""), &[])
                .await
                .unwrap_err();
        }

        // This must always happen last in this loop so that we don't increment this if something
        // triggers a test failure earlier in the iteration:
        next_step_idx.set(next_step_idx.get() + 1);
    }

    shutdown_tx.shutdown().await;
}

async fn recreate_caches_using_type(
    type_name: &str,
    tables: &HashMap<String, Vec<ColumnSpec>>,
    rs_conn: &Client,
) {
    for table in tables_using_type(tables, type_name) {
        let create_cache = format!("CREATE CACHE ALWAYS FROM SELECT * FROM \"{table}\"");

        eventually!(run_test: {
            let result = rs_conn.simple_query(&create_cache).await;
            AssertUnwindSafe(move || result)
        }, then_assert: |result| {
            result().unwrap()
        });
    }
}

#[test]
#[cfg_attr(not(feature = "ddl_vertical_tests"), ignore)]
fn run_cases() {
    let config = ProptestConfig {
        max_shrink_iters: MAX_OPS as u32 * 2,
        ..ProptestConfig::default()
    };

    // Used for optimizing shrinking by letting us immediately discard any steps after the failing
    // step. To enable this, we need to be able to mutate the value from the test itself, and then
    // access it from within the [`TestModel`], which is not easily doable within the constraints
    // of the proptest API, since the test passed to [`TestRunner`] is [`Fn`] (not [`FnMut`]).
    // Hence the need for interior mutability.
    let next_step_idx = Rc::new(Cell::new(0));

    let model = TestModel {
        next_step_idx: next_step_idx.clone(),
        ..TestModel::default()
    };
    proptest!(config, |(steps in model)| {
        prop_assume!(preconditions_hold(&steps));
        next_step_idx.set(0);
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(
            async {
                tokio::time::timeout(TEST_CASE_TIMEOUT, run(steps, next_step_idx.clone())).await
            }
        ).unwrap();
    });
}

proptest! {
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
