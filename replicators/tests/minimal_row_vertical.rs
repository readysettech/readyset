//! This test suite implements a MySQL Minimal Row Vertical test.
//!
//! It focuses on the following operations:
//! * Create Table
//! * Insert Row (with partial column specification)
//! * Update Row (with partial column specification)
//! * Delete Row
//!
//! It also supports column constraints like:
//!  * NOT NULL
//!  * DEFAULT values
//!  * ON UPDATE CURRENT TIMESTAMP
//!
//! Note that this test suite is ignored by default, and conditionally de-ignored with the
//! `vertical_tests` feature to prevent it running in normal builds (since it's slow and may
//! find new bugs); to run it locally run:
//!
//! ```notrust
//! cargo test -p replicators --features vertical_tests --test minimal_row_vertical
//! ```
//!
//! This test suite will connect to a local MySQL database, will create tables and run DML on this tables
//! after each operation, it will check the results from MySQL and Readyset.

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};
use std::fmt::{Debug, Formatter, Result};
use std::iter::once;
use std::panic::AssertUnwindSafe;
use std::time::Duration;

use async_trait::async_trait;
use itertools::Itertools;
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, OptsBuilder, Row};
use mysql_time::MySqlTime;
use proptest::prelude::*;
use proptest::strategy::{BoxedStrategy, Just, Strategy};
use proptest::{collection, sample};
use proptest_stateful::{
    proptest_config_with_local_failure_persistence, ModelState, ProptestStatefulConfig,
};
use rand::seq::SliceRandom;
use readyset_client_test_helpers::mysql_helpers::MySQLAdapter;
use readyset_client_test_helpers::{mysql_helpers, TestBuilder};
use readyset_data::DfValue;
use readyset_server::Handle;
use readyset_sql::ast::{EnumVariants, SqlType};
use readyset_sql::DialectDisplay;
use readyset_util::arbitrary::{
    arbitrary_duration_without_microseconds_in_range, arbitrary_mysql_date,
    arbitrary_timestamp_naive_date_time,
};
use readyset_util::eventually;
use readyset_util::shutdown::ShutdownSender;

/// This struct is used to generate arbitrary column specifications, both for creating tables.
#[derive(Clone)]
struct ColumnSpec {
    name: String,
    sql_type: SqlType,
    gen: BoxedStrategy<DfValue>,
    is_nullable: bool,
    default_value: Option<DfValue>,
    default_current_timestamp: bool,
    on_update_current_timestamp: bool,
}

// We need to implement Eq, PartialEq,  for ColumnSpec so that we can use it in a BTreeSet
impl Eq for ColumnSpec {}
impl PartialEq for ColumnSpec {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl PartialOrd for ColumnSpec {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ColumnSpec {
    fn cmp(&self, other: &Self) -> Ordering {
        self.name.cmp(&other.name)
    }
}

// The debug output for the generators can be really verbose and is usually not helpful, so we
// custom derive Debug to skip that part:
impl Debug for ColumnSpec {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        f.debug_struct("ColumnSpec")
            .field("name", &self.name)
            .field("sql_type", &self.sql_type)
            .field("is_nullable", &self.is_nullable)
            .field("has_default", &self.default_value.is_some())
            .field("default_current_timestamp", &self.default_current_timestamp)
            .field(
                "on_update_current_timestamp",
                &self.on_update_current_timestamp,
            )
            .finish()
    }
}

impl Arbitrary for ColumnSpec {
    type Parameters = BTreeMap<String, Vec<String>>;
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
        // Randomly decide if the column is nullable
        let is_nullable = any::<bool>();

        // Randomly decide if the column has a default value
        let has_default = any::<bool>();

        // Randomly decide if the column has a default current timestamp
        let default_current_timestamp = any::<bool>();

        // Randomly decide if the column has an on update current timestamp
        let on_update_current_timestamp = any::<bool>();

        let base_types = vec![
            (
                SqlType::TinyInt(None),
                any::<i8>().prop_map(DfValue::from).boxed(),
            ),
            (
                SqlType::MediumInt(None),
                any::<i16>().prop_map(DfValue::from).boxed(),
            ),
            (
                SqlType::Int(None),
                any::<i32>().prop_map(DfValue::from).boxed(),
            ),
            (
                SqlType::BigInt(None),
                any::<i64>().prop_map(DfValue::from).boxed(),
            ),
            (
                SqlType::TinyIntUnsigned(None),
                any::<u8>().prop_map(DfValue::from).boxed(),
            ),
            (
                SqlType::MediumIntUnsigned(None),
                any::<u16>().prop_map(DfValue::from).boxed(),
            ),
            (
                SqlType::IntUnsigned(None),
                any::<u32>().prop_map(DfValue::from).boxed(),
            ),
            (
                SqlType::BigIntUnsigned(None),
                any::<u64>().prop_map(DfValue::from).boxed(),
            ),
            // Test CHAR types, it's important to distinguish CHAR as it will catch padding issues
            (SqlType::Text, "[A-Z]{1,10}".prop_map(DfValue::from).boxed()),
            (
                SqlType::Char(Some(10)),
                "[A-Za-z]{1,10}"
                    .prop_map(|s| DfValue::from(s.as_bytes().to_vec()))
                    .boxed(),
            ),
            (
                SqlType::VarChar(Some(10)),
                "[A-Z]{1,10}"
                    .prop_map(|s| DfValue::from(s.as_bytes().to_vec()))
                    .boxed(),
            ),
            (
                SqlType::Blob,
                "[A-Z]{1,10}"
                    .prop_map(|s| DfValue::from(s.as_bytes().to_vec()))
                    .boxed(),
            ),
            (
                SqlType::DateTime(None),
                arbitrary_timestamp_naive_date_time()
                    .prop_map(DfValue::from)
                    .boxed(),
            ),
            (
                SqlType::Date,
                arbitrary_mysql_date().prop_map(DfValue::from).boxed(),
            ),
            (
                SqlType::Time,
                arbitrary_duration_without_microseconds_in_range(-255i32..=255)
                    .prop_map(MySqlTime::new)
                    .prop_map(DfValue::Time)
                    .boxed(),
            ),
            (SqlType::Bool, any::<bool>().prop_map(DfValue::from).boxed()),
            (
                SqlType::Timestamp,
                arbitrary_timestamp_naive_date_time()
                    .prop_map(DfValue::from)
                    .boxed(),
            ),
            /*(
                // Disabled due to REA-5632
                SqlType::Real,
                any::<f32>()
                    .prop_filter("only finite values", |f| f.is_finite())
                    .prop_map(|f| {
                        // Scale the value to be between 1 and 10 with 5 decimal places
                        let scaled = 1.0 + (f.abs() % 9.0);
                        let rounded = (scaled * 100000.0).round() / 100000.0;
                        DfValue::try_from(rounded).unwrap()
                    })
                    .boxed(),
            ),*/
        ];

        let base_strategy = Just(base_types)
            .prop_map(|types| {
                let (sql_type, gen) = types.into_iter().next().unwrap();
                (sql_type, gen)
            })
            .boxed();

        let variants = any_with::<EnumVariants>(("[A-Za-z]{1,15}", (1..=10).into()))
            .prop_filter("All variants should be unique", |v| {
                v.iter().map(|cs| cs.to_lowercase()).all_unique()
            })
            .boxed();
        let enum_strategy = variants
            .prop_flat_map(|v| {
                let len = v.len();
                let sql_type = SqlType::Enum(v.clone());
                let value_strategy = any::<usize>()
                    .prop_map(move |idx| DfValue::from(v[idx % len].clone()))
                    .boxed();
                Just((sql_type, value_strategy))
            })
            .boxed();

        let col_type_strategy = prop_oneof![base_strategy, enum_strategy];

        (
            "", // name gets populated later with a proper column index, col_0, col_1, etc.
            col_type_strategy,
            is_nullable,
            has_default,
            default_current_timestamp,
            on_update_current_timestamp,
        )
            .prop_flat_map(
                |(
                    name,
                    (sql_type, gen),
                    is_nullable,
                    has_default,
                    default_current_timestamp,
                    on_update_current_timestamp,
                )| {
                    // If the column has a default value and is not BLOB, TEXT, or JSON column (they don't support default in MySQL)
                    // generate one based on the column type
                    let default_value = if has_default
                        && !matches!(sql_type, SqlType::Blob)
                        && !matches!(sql_type, SqlType::Text)
                        && !matches!(sql_type, SqlType::Json)
                        && !matches!(sql_type, SqlType::Jsonb)
                    {
                        gen.clone().prop_map(Some).boxed()
                    } else {
                        Just(None).boxed()
                    };

                    (
                        Just(name),
                        Just(sql_type),
                        Just(gen),
                        Just(is_nullable),
                        default_value,
                        Just(default_current_timestamp),
                        Just(on_update_current_timestamp),
                    )
                },
            )
            .prop_map(
                |(
                    name,
                    sql_type,
                    gen,
                    is_nullable,
                    default_value,
                    default_current_timestamp,
                    on_update_current_timestamp,
                )| ColumnSpec {
                    name,
                    sql_type,
                    gen,
                    is_nullable,
                    default_value,
                    default_current_timestamp,
                    on_update_current_timestamp,
                },
            )
            .boxed()
    }
}

/// Each Operation represents one step to take in a given test run.
#[derive(Clone, Debug)]
enum Operation {
    /// Create a new table with the given name and columns
    CreateTable(Vec<ColumnSpec>, bool),
    /// Insert a row into a table, specifying only some columns
    InsertRow {
        pkey: Option<i32>,
        col_names: Vec<String>,
        col_vals: Vec<DfValue>,
    },
    /// Update a row in a table
    UpdateRow {
        pkey: Option<i32>,
        col_names: Vec<String>,
        col_vals: Vec<DfValue>,
    },
    /// Delete a row from a table
    DeleteRow(Option<i32>),
}

// Generators for Operation:

fn gen_column_specs() -> impl Strategy<Value = Vec<ColumnSpec>> {
    // Create a strategy that generates a vector of column specs with sequential IDs
    collection::vec(any_with::<ColumnSpec>(Default::default()), 4..12)
        .prop_map(|mut specs| {
            // Reset the column counter for each new table
            for (i, spec) in specs.iter_mut().enumerate() {
                // Set the name to col_ID format
                spec.name = format!("col_{}", i);
            }
            specs
        })
        .prop_filter("duplicate column names not allowed", |specs| {
            specs.iter().map(|cs| &cs.name).all_unique()
        })
}

fn gen_create_table() -> impl Strategy<Value = Operation> {
    let cols = gen_column_specs().prop_map(|specs| specs);
    // randomly decide if the table has a primary key
    let has_primary_key = any::<bool>();
    (cols, has_primary_key)
        .prop_map(move |(cols, has_primary_key)| Operation::CreateTable(cols, has_primary_key))
        .boxed()
}

fn gen_insert_row(
    table_columns: Vec<ColumnSpec>,
    has_pk: bool,
    pkeys: Vec<i32>,
) -> impl Strategy<Value = Operation> {
    proptest::collection::btree_set(
        sample::select(table_columns.clone()),
        1..=table_columns.len(),
    )
    .prop_flat_map(|cols| {
        cols.iter()
            .map(|col| (col.gen.clone(), Just(col.name.clone())))
            .collect::<Vec<_>>()
    })
    .prop_map(move |col_vals| {
        let (col_vals, col_names) = col_vals.into_iter().unzip::<_, _, Vec<_>, Vec<_>>();
        let pkey = if has_pk {
            Some((0..).find(|k: &i32| !pkeys.contains(k)).unwrap())
        } else {
            None
        };
        Operation::InsertRow {
            pkey,
            col_names,
            col_vals,
        }
    })
    .boxed()
}

fn gen_update_row(
    table_columns: Vec<ColumnSpec>,
    has_pk: bool,
    pkeys: Vec<i32>,
) -> impl Strategy<Value = Operation> {
    proptest::collection::btree_set(
        sample::select(table_columns.clone()),
        1..=table_columns.len(),
    )
    .prop_flat_map(|cols| {
        cols.iter()
            .map(|col| (col.gen.clone(), Just(col.name.clone())))
            .collect::<Vec<_>>()
    })
    .prop_map(move |col_vals| {
        let (col_vals, col_names) = col_vals.into_iter().unzip::<_, _, Vec<_>, Vec<_>>();
        let pkey = if has_pk {
            Some((0..).find(|k: &i32| pkeys.contains(k)).unwrap())
        } else {
            None
        };

        Operation::UpdateRow {
            pkey,
            col_names,
            col_vals,
        }
    })
    .boxed()
}

fn gen_delete_row(pkeys: Vec<i32>) -> impl Strategy<Value = Operation> {
    if pkeys.is_empty() {
        Just(Operation::DeleteRow(None)).boxed()
    } else {
        sample::select(pkeys)
            .prop_map(|pkey| Operation::DeleteRow(Some(pkey)))
            .boxed()
    }
}

const TABLE_NAME: &str = "table_0";

/// A model of the current test state, used to help generate operations in a way that we expect to
/// succeed, as well as to assist in shrinking, and to determine postconditions to check during
/// test runtime.
#[derive(Clone, Debug, Default)]
struct DDLModelState {
    table_exists: bool,
    table_columns: Vec<ColumnSpec>,
    has_pk: bool,
    pkeys: Vec<i32>,
    table_records: u32,
}

struct DDLTestRunContext {
    rs_conn: Conn,
    mysql_conn: Conn,
    shutdown_tx: Option<ShutdownSender>, // Needs to be Option so we can move it out of the struct
    _handle: Handle,
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
    fn op_generators(&self) -> Vec<Self::OperationStrategy> {
        println!(
            "op_generators called with table_exists: {}",
            self.table_exists
        );
        let mut possible_ops = vec![];
        let mut possible_ops_string = vec![];
        // If we have at least one table, we can do any of:
        //  * insert a row
        //  * update a row
        //  * delete a row
        if self.table_exists {
            let insert_strategy =
                gen_insert_row(self.table_columns.clone(), self.has_pk, self.pkeys.clone()).boxed();
            possible_ops.push(insert_strategy);
            possible_ops_string.push("insert".to_string());
            if self.table_records > 0 {
                let update_strategy =
                    gen_update_row(self.table_columns.clone(), self.has_pk, self.pkeys.clone())
                        .boxed();
                let delete_strategy = gen_delete_row(self.pkeys.clone()).boxed();
                possible_ops.extend([update_strategy, delete_strategy]);
                possible_ops_string.extend(["update".to_string(), "delete".to_string()]);
            }
        } else {
            let create_table_strat = gen_create_table().boxed();
            possible_ops.push(create_table_strat);
            possible_ops_string.push("create_table".to_string());
        }
        println!("possible_ops: {:?}", possible_ops_string);
        possible_ops
    }

    /// This method is used to update `self` based on the expected results of executing a single
    /// [`Operation`].
    fn next_state(&mut self, op: &Operation) {
        match op {
            Operation::CreateTable(cols, has_primary_key) => {
                self.table_columns = cols.clone();
                self.has_pk = *has_primary_key;
                self.pkeys = vec![];
                self.table_records = 0;
                self.table_exists = true;
            }
            Operation::InsertRow { pkey, .. } => {
                if self.has_pk {
                    self.pkeys.push(pkey.unwrap());
                }
                self.table_records += 1;
            }
            Operation::UpdateRow { .. } => (),
            Operation::DeleteRow(key) => {
                if let Some(key) = key {
                    self.pkeys.retain(|k| k != key);
                }
                self.table_records -= 1;
            }
        }
    }

    /// Checks preconditions for an [`Operation`] given a current test model state.
    fn preconditions_met(&self, op: &Self::Operation) -> bool {
        println!("preconditions_met table_exists: {}", self.table_exists);
        println!("preconditions_met op: {:?}", op);
        match op {
            Operation::CreateTable { .. } => true,
            Operation::InsertRow { pkey, .. } => {
                // Make sure that the table doesn't already contain a row with this key
                if let Some(pkey) = pkey {
                    !self.pkeys.contains(pkey) && self.table_exists
                } else {
                    self.table_exists
                }
            }
            Operation::UpdateRow { pkey, .. } | Operation::DeleteRow(pkey) => {
                // Make sure that the table contains a row with this key
                if let Some(pkey) = pkey {
                    self.pkeys.contains(pkey) && self.table_exists
                } else {
                    self.table_records > 0 && self.table_exists
                }
            }
        }
    }

    /// Get ready to run a single test case by:
    ///  * Setting up a test instance of ReadySet that connects to an upstream instance of MySQL
    async fn init_test_run(&self) -> Self::RunContext {
        readyset_tracing::init_test_logging();

        let (opts, handle, shutdown_tx) = TestBuilder::default()
            .replicate(true)
            .durability_mode(readyset_server::DurabilityMode::DeleteOnExit)
            .fallback(true)
            .build::<MySQLAdapter>()
            .await;
        let mut mysql_conn =
            connect(mysql_helpers::upstream_config().db_name(opts.db_name())).await;
        let rs_conn = connect(OptsBuilder::from_opts(opts)).await;
        mysql_conn.query_drop("SET sql_mode=''").await.unwrap();
        mysql_conn
            .query_drop("SET binlog_row_image = 'MINIMAL'")
            .await
            .unwrap();
        DDLTestRunContext {
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
            Operation::CreateTable(cols, has_primary_key) => {
                let non_pkey_cols = cols.iter().map(
                    |ColumnSpec {
                         name,
                         sql_type,
                         is_nullable,
                         default_value,
                         default_current_timestamp,
                         on_update_current_timestamp,
                         ..
                     }| {
                        let mut col_def = format!(
                            "`{name}` {}",
                            sql_type.display(readyset_sql::Dialect::MySQL)
                        );

                        // add collation if this is a character type
                        if sql_type.is_any_text() {
                            let collations = [
                                "utf8mb4_0900_ai_ci",
                                "",
                                "utf8mb4_general_ci",
                                "utf8mb4_0900_as_cs",
                                // "binary", // Disabled due to REA-5707
                            ];
                            let random_collation =
                                collations.choose(&mut rand::thread_rng()).unwrap();
                            if random_collation != &"" {
                                col_def.push_str(&format!(" COLLATE {}", random_collation));
                            }
                        }

                        // Add NOT NULL constraint if specified
                        if !is_nullable {
                            col_def.push_str(" NOT NULL");
                        }

                        // Add DEFAULT value if specified
                        if let Some(default) = default_value {
                            let mut def_value = default.clone();
                            // convert from bytesarray to string
                            if let DfValue::ByteArray(bytes) = def_value {
                                def_value =
                                    DfValue::from(String::from_utf8(bytes.to_vec()).unwrap());
                            }

                            // check if the dfvalue is of string type and if so, wrap it in single quotes
                            if def_value.is_string()
                                || def_value.is_datetime()
                                || def_value.is_time()
                            {
                                col_def.push_str(&format!(" DEFAULT '{}'", def_value));
                            } else {
                                col_def.push_str(&format!(" DEFAULT {}", def_value));
                            }
                        } else if *default_current_timestamp
                            && (matches!(sql_type, SqlType::Timestamp)
                                || matches!(sql_type, SqlType::DateTime(_)))
                        {
                            col_def.push_str(" DEFAULT CURRENT_TIMESTAMP");
                        }

                        if *on_update_current_timestamp
                            && (matches!(sql_type, SqlType::Timestamp)
                                || matches!(sql_type, SqlType::DateTime(_)))
                        {
                            col_def.push_str(" ON UPDATE CURRENT_TIMESTAMP");
                        }

                        col_def
                    },
                );

                let col_defs: String = if *has_primary_key {
                    once("id INT PRIMARY KEY".to_string())
                        .chain(non_pkey_cols)
                        .collect::<Vec<_>>()
                        .join(", ")
                } else {
                    non_pkey_cols.collect::<Vec<_>>().join(", ")
                };
                let query = format!("CREATE TABLE `{TABLE_NAME}` ({col_defs})");
                mysql_conn.query_drop(&query).await.unwrap_or_else(|e| {
                    println!("Error creating table: {e}");
                    panic!("Error creating table: {query}");
                });

                let create_cache = format!("CREATE CACHE ALWAYS FROM SELECT * FROM `{TABLE_NAME}`");
                eventually!(run_test: {
                    let result = rs_conn.query_drop(&create_cache).await;
                    AssertUnwindSafe(move || result)
                }, then_assert: |result| {
                    result().unwrap()
                });
            }
            Operation::InsertRow {
                pkey,
                col_names,
                col_vals,
            } => {
                // Create a map of column names to values
                let mut col_map: HashMap<String, DfValue> = HashMap::new();
                for (name, val) in col_names.iter().zip(col_vals.iter()) {
                    col_map.insert(name.clone(), val.clone());
                }

                // Get all columns for this table
                let all_cols = self.table_columns.clone();

                // Build the column list and value list for the INSERT statement
                let (mut insert_cols, mut insert_vals) = if self.has_pk {
                    (vec!["id"], vec![DfValue::from(*pkey)])
                } else {
                    (vec![], vec![])
                };

                // Add the specified columns
                for col in all_cols.iter() {
                    if col_names.contains(&col.name) {
                        insert_cols.push(&col.name);
                        insert_vals.push(col_map.get(&col.name).unwrap().clone());
                    }
                }

                // Convert to MySQL values
                let params: Vec<mysql_async::Value> = insert_vals
                    .iter()
                    .map(|v| match v {
                        DfValue::Int(_) => mysql_async::Value::Int(v.to_string().parse().unwrap()),
                        DfValue::Float(_) => {
                            mysql_async::Value::Float(v.to_string().parse().unwrap())
                        }
                        _ => mysql_async::Value::Bytes(v.to_string().as_bytes().to_vec()),
                    })
                    .collect();

                // Build the INSERT statement
                let col_list = insert_cols
                    .iter()
                    .map(|c| format!("`{}`", c))
                    .collect::<Vec<_>>()
                    .join(", ");
                let placeholders: Vec<_> = (1..=params.len()).map(|_| "?".to_string()).collect();
                let placeholders = placeholders.join(", ");
                let query =
                    format!("INSERT INTO `{TABLE_NAME}` ({col_list}) VALUES ({placeholders})");

                mysql_conn.exec_drop(&query, &params).await.unwrap();
            }
            Operation::UpdateRow {
                pkey,
                col_names,
                col_vals,
            } => {
                // Create a map of column names to values
                let mut col_map: HashMap<String, DfValue> = HashMap::new();
                for (name, val) in col_names.iter().zip(col_vals.iter()) {
                    col_map.insert(name.clone(), val.clone());
                }

                // Build the SET clause for the UPDATE statement
                let set_clauses: Vec<String> = col_names
                    .iter()
                    .map(|name| format!("`{}` = ?", name))
                    .collect();
                let set_clauses = set_clauses.join(", ");

                // Convert to MySQL values
                let params: Vec<mysql_async::Value> = col_vals
                    .iter()
                    .map(|v| match v {
                        DfValue::Int(_) => mysql_async::Value::Int(v.to_string().parse().unwrap()),
                        DfValue::Float(_) => {
                            mysql_async::Value::Float(v.to_string().parse().unwrap())
                        }
                        _ => mysql_async::Value::Bytes(v.to_string().as_bytes().to_vec()),
                    })
                    .collect();

                // Add the WHERE clause parameter
                let mut all_params = params;

                // Build the UPDATE statement
                let query = if let Some(pkey) = pkey {
                    all_params.push(mysql_async::Value::Int(*pkey as i64));
                    format!("UPDATE `{TABLE_NAME}` SET {set_clauses} WHERE id = ?")
                } else {
                    format!("UPDATE `{TABLE_NAME}` SET {set_clauses} LIMIT 1")
                };

                mysql_conn.exec_drop(&query, &all_params).await.unwrap();
            }
            Operation::DeleteRow(key) => {
                let query = if let Some(key) = key {
                    format!("DELETE FROM `{TABLE_NAME}` WHERE id = {key}")
                } else {
                    format!("DELETE FROM `{TABLE_NAME}` LIMIT 1")
                };
                mysql_conn.query_drop(&query).await.unwrap();
            }
        }
    }

    async fn check_postconditions(&self, ctxt: &mut Self::RunContext) {
        let DDLTestRunContext {
            rs_conn,
            mysql_conn,
            ..
        } = ctxt;

        // After each op, check that all table contents match

        eventually!(attempts: 10, run_test: {
            let rs_rows = rs_conn
                .exec(format!("SELECT * FROM `{TABLE_NAME}`"), ())
                .await
                .unwrap();
            let rs_executed_at: (String, String) = rs_conn.query_first("EXPLAIN LAST STATEMENT".to_string()).await.unwrap().unwrap();
            let mysql_rows = mysql_conn
                .exec(format!("SELECT * FROM `{TABLE_NAME}`"), ())
                .await
                .unwrap();
            // Previously, we would run all the result handling in the run_test block, but
            // doing it in the then_assert block lets us work around a tokio-MySQL client
            // crash caused by ENG-2548 by retrying until ReadySet stops sending us bad
            // packets.
            AssertUnwindSafe(move || (rs_rows, mysql_rows, rs_executed_at))
        }, then_assert: |results| {
            let (rs_rows, mysql_rows, rs_executed_at) = results();

            let mut rs_results = rows_to_dfvalue_vec(rs_rows);
            let mut mysql_results = rows_to_dfvalue_vec(mysql_rows);

            rs_results.sort_unstable();
            mysql_results.sort_unstable();

            pretty_assertions::assert_eq!(mysql_results, rs_results, "MySQL(left) and ReadySet(right) results do not match for table: {TABLE_NAME}");
            pretty_assertions::assert_eq!(rs_executed_at.0, "readyset", "ReadySet did not execute the last statement for table: {TABLE_NAME}");
        });
    }

    async fn clean_up_test_run(&self, ctxt: &mut Self::RunContext) {
        println!(
            "****************clean_up_test_run table_exists: {}",
            self.table_exists
        );
        ctxt.shutdown_tx.take().unwrap().shutdown().await
    }
}

/// Spawns a new connection (either to ReadySet or directly to MySQL).
async fn connect(config: OptsBuilder) -> Conn {
    mysql_async::Conn::new(config).await.unwrap()
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
    std::env::set_var("RUST_BACKTRACE", "0");
    let config = ProptestStatefulConfig {
        min_ops: 10,
        max_ops: 20,
        test_case_timeout: Duration::from_secs(60),
        proptest_config: proptest_config_with_local_failure_persistence!(),
    };

    proptest_stateful::test::<DDLModelState>(config);
}
