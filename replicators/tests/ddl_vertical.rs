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
use std::fmt::{Debug, Display, Formatter, Result};
use std::iter::once;
use std::panic::AssertUnwindSafe;
use std::time::Duration;

use async_trait::async_trait;
use itertools::Itertools;
use nom_sql::SqlType;
use proptest::prelude::*;
use proptest::strategy::{BoxedStrategy, Just, Strategy};
use proptest::{collection, sample};
use readyset_client_test_helpers::psql_helpers::{self, PostgreSQLAdapter};
use readyset_client_test_helpers::TestBuilder;
use readyset_data::{DfValue, TimestampTz};
use readyset_server::Handle;
use readyset_util::eventually;
use readyset_util::shutdown::ShutdownSender;
use stateful_proptest::{ModelState, StatefulProptestConfig};
use tokio_postgres::{Client, Config, NoTls, Row};

const SQL_NAME_REGEX: &str = "[a-zA-Z_][a-zA-Z0-9_]*";

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

struct DDLTestRunContext {
    rs_conn: Client,
    pg_conn: Client,
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
    tables: HashMap<String, Vec<ColumnSpec>>,
    deleted_tables: HashSet<String>,
    pkeys: HashMap<String, Vec<i32>>, // Primary keys in use for each table
    // Map of view name to view definition
    views: HashMap<String, TestViewDef>,
    deleted_views: HashSet<String>,
    // Map of custom ENUM type names to type definitions (represented by a Vec of ENUM elements)
    enum_types: HashMap<String, Vec<String>>,
}

#[async_trait(?Send)]
impl ModelState for DDLModelState {
    type Operation = Operation;
    type RunContext = DDLTestRunContext;
    type OperationStrategy = BoxedStrategy<Operation>;

    /// Each invocation of this function returns a [`Vec`] of [`Strategy`]s for generating
    /// [`Operation`]s *given the current state of the test model*. With a brand new model, the only
    /// possible operations are [`Operation::CreateTable`] and [`Operation::CreateEnum`], but as
    /// tables/types are created and rows are written, other operations become possible.
    ///
    /// Note that there is some redundancy between the logic in this function and the logic in
    /// [`Operation::preconditions`](enum.Operation.html#method.preconditions). This is necessary
    /// because `gen_op` is used for the initial test generation, but the preconditions are used
    /// during shrinking. (Technically, we do also check and filter on preconditions at the start
    /// of each test, but it's best to depend on that check as little as possible since test
    /// filters like that can lead to slow and lopsided test generation.)
    fn op_generators(&self) -> Vec<Self::OperationStrategy> {
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

    /// Checks preconditions for an [`Operation`] given a current test model state.
    ///
    /// These are primarily needed for shrinking, so that we can make sure that we don't do things
    /// like remove a [`Operation::CreateTable`] when a later [`Operation::WriteRow`] operation
    /// depends on the corresponding table.
    ///
    /// We also check preconditions during runtime, and throw out any test cases where the
    /// preconditions aren't satisfied. This should be rare, though, because
    /// [`DDLModelState::op_generators`] should *usually* only generate cases where the
    /// preconditions are already satisified. It's possible there are weird corner cases though
    /// (such as multiple random strings happening to generate the same string value for two
    /// different table names) where preconditions could save us from a false positive test
    /// failure.
    fn preconditions_met(&self, op: &Self::Operation) -> bool {
        match op {
            Operation::CreateTable(name, cols) => {
                !self.tables.contains_key(name)
                    && cols.iter().all(|cs| match cs {
                        ColumnSpec {
                            sql_type: SqlType::Other(type_name),
                            ..
                        } => self.enum_types.contains_key(type_name.name.as_str()),
                        _ => true,
                    })
            }
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
                    .map_or(false, |table_keys| !table_keys.contains(pkey))
                    && self.tables.get(table).map_or(false, |table_cols| {
                        table_cols
                            .iter()
                            .zip(col_types)
                            .all(|(cs, row_type)| cs.sql_type == *row_type)
                    })
            }
            Operation::DeleteRow(table, key) => self
                .pkeys
                .get(table)
                .map_or(false, |table_keys| table_keys.contains(key)),
            Operation::AddColumn(table, column_spec) => self
                .tables
                .get(table)
                .map_or(false, |t| t.iter().all(|cs| cs.name != *column_spec.name)),
            Operation::DropColumn(table, col_name) => self
                .tables
                .get(table)
                .map_or(false, |t| t.iter().any(|cs| cs.name == *col_name)),
            Operation::AlterColumnName {
                table,
                col_name,
                new_name,
            } => self.tables.get(table).map_or(false, |t| {
                t.iter().any(|cs| cs.name == *col_name) && t.iter().all(|cs| cs.name != *new_name)
            }),
            Operation::CreateSimpleView { name, table_source } => {
                self.tables.contains_key(table_source)
                    && !self.tables.contains_key(name)
                    && !self.views.contains_key(name)
            }
            Operation::CreateJoinView {
                name,
                table_a,
                table_b,
            } => {
                self.tables.contains_key(table_a)
                    && self.tables.contains_key(table_b)
                    && !self.tables.contains_key(name)
                    && !self.views.contains_key(name)
            }
            Operation::DropView(name) => self.views.contains_key(name),
            Operation::CreateEnum(name, _values) => !self.enum_types.contains_key(name),
            Operation::DropEnum(name) => tables_using_type(&self.tables, name).next().is_none(),
            Operation::AppendEnumValue {
                type_name,
                value_name,
            } => self
                .enum_types
                .get(type_name)
                .map_or(false, |t| !t.contains(value_name)),
            Operation::InsertEnumValue {
                type_name,
                value_name,
                next_to_value,
                ..
            } => self.enum_types.get(type_name).map_or(false, |t| {
                t.contains(next_to_value) && !t.contains(value_name)
            }),
            Operation::RenameEnumValue {
                type_name,
                value_name,
                new_name,
            } => self
                .enum_types
                .get(type_name)
                .map_or(false, |t| t.contains(value_name) && !t.contains(new_name)),
        }
    }

    /// Get ready to run a single test case by:
    ///  * Setting up a test instance of ReadySet that connects to an upstream instance of Postgres
    ///  * Wiping and recreating a fresh copy of the oracle database directly in Postgres, and
    ///    setting up a connection
    async fn init_test_run(&self) -> Self::RunContext {
        readyset_tracing::init_test_logging();

        let (opts, handle, shutdown_tx) = TestBuilder::default()
            .fallback(true)
            .build::<PostgreSQLAdapter>()
            .await;
        let rs_conn = connect(opts).await;

        recreate_oracle_db().await;
        let pg_conn = connect(oracle_db_config()).await;

        DDLTestRunContext {
            rs_conn,
            pg_conn,
            _handle: handle,
            shutdown_tx: Some(shutdown_tx),
        }
    }

    /// Run the code to test a single operation:
    ///  * Running each step in `ops`, and checking afterward that:
    ///    * The contents of the tables tracked by our model match across both ReadySet and Postgres
    ///    * Any deleted tables appear as deleted in both ReadySet and Postgres
    async fn run_op(&self, op: &Self::Operation, ctxt: &mut Self::RunContext) {
        let DDLTestRunContext {
            rs_conn, pg_conn, ..
        } = ctxt;

        match op {
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
                let select_list: Vec<String> = self.tables[table_a]
                    .iter()
                    .chain(self.tables[table_b].iter())
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
                recreate_caches_using_type(type_name, &self.tables, rs_conn).await;
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
                recreate_caches_using_type(type_name, &self.tables, rs_conn).await;
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
                recreate_caches_using_type(type_name, &self.tables, rs_conn).await;
            }
        }
    }

    async fn check_postconditions(&self, ctxt: &mut Self::RunContext) {
        let DDLTestRunContext {
            rs_conn, pg_conn, ..
        } = ctxt;

        // After each op, check that all table and view contents match
        for relation in self.tables.keys().chain(self.views.keys()) {
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
        for table in &self.deleted_tables {
            rs_conn
                .query(&format!("DROP TABLE \"{table}\""), &[])
                .await
                .unwrap_err();
        }
        // And then do the same for views:
        for view in &self.deleted_views {
            rs_conn
                .query(&format!("DROP VIEW \"{view}\""), &[])
                .await
                .unwrap_err();
        }
    }

    async fn clean_up_test_run(&self, ctxt: &mut Self::RunContext) {
        ctxt.shutdown_tx.take().unwrap().shutdown().await
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
    let _config = StatefulProptestConfig {
        min_ops: 7,
        max_ops: 13,
        test_case_timeout: Duration::from_secs(60),
    };

    // TODO uncomment once the rest of the module is converted to use stateful-proptest
    //stateful_proptest::test::<DDLModelState>(stateful_config);
}
