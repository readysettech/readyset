//! This test suite implements the [Vertical Testing Design Doc][doc]
//!
//! [doc]: https://docs.google.com/document/d/1rTDzd4Z5jSUDqGmIu2C7R06f2HkNWxEll33-rF4WC-c
use std::collections::HashMap;
use std::convert::TryFrom;
use std::env;
use std::iter;
use std::mem;
use std::ops::Range;

use itertools::Itertools;
use mysql::prelude::Queryable;
use mysql_common::value::Value;
use proptest::prelude::*;
use proptest::sample::select;
use proptest::test_runner::TestCaseResult;
use test_strategy::proptest;

use noria::DataType;

mod common;

#[derive(Clone, Debug, PartialEq, Eq)]
enum Operation<const K: usize> {
    Query { key: [DataType; K] },
    Insert { table: String, row: Vec<DataType> },
    /* TODO: coming soon
    Update {
        table: String,
        key: DataType,
        old_row: Vec<DataType>,
        new_row: Vec<DataType>,
    },
    Delete {
        table: String,
        key: DataType,
    },
    Evict {
        /// *seed* for the node index to evict from.
        ///
        /// Note that we don't know how many nodes a query will have until after we install it in
        /// noria, so the actual node index will be this modulo the number of non-base-table nodes
        node_seed: usize,
        key: [DataType; K],
    },
    */
}

pub struct OperationParameters<'a, const K: usize> {
    already_generated: &'a [Operation<K>],

    /// table name, index in table
    key_columns: [(&'a str, usize); K],

    row_strategies: HashMap<&'static str, Vec<BoxedStrategy<DataType>>>,
}

impl<'a, const K: usize> OperationParameters<'a, K> {
    /// Return an iterator over all the query lookup keys that match rows previously inserted into
    /// the table
    fn existing_keys(&'a self) -> impl Iterator<Item = [DataType; K]> + 'a {
        let mut rows: HashMap<&'a str, Vec<&'a Vec<DataType>>> = HashMap::new();
        for op in self.already_generated {
            if let Operation::Insert { table, row } = op {
                rows.entry(table).or_default().push(row);
            }
        }

        rows.into_iter()
            .map(|(tbl, rows)| rows.into_iter().map(|r| (tbl, r)).collect::<Vec<_>>())
            .multi_cartesian_product()
            .map(move |vals| {
                self.key_columns
                    .map(|(tbl, idx)| vals.iter().find(|(t, _)| tbl == *t).unwrap().1[idx].clone())
            })
    }

    /// Return a proptest [`Strategy`] for generating new keys for the query
    fn key_strategy(&'a self) -> impl Strategy<Value = [DataType; K]> + 'static
    where
        [BoxedStrategy<DataType>; K]: Strategy<Value = [DataType; K]>,
    {
        self.key_columns.map(move |(t, idx)| {
            self.row_strategies[t]
                .clone()
                .prop_map(move |mut r| r.remove(idx))
                .boxed()
        })
    }
}

impl<const K: usize> Operation<K>
where
    [DataType; K]: Arbitrary,
    [BoxedStrategy<DataType>; K]: Strategy<Value = [DataType; K]>,
{
    /// Return a proptest [`Strategy`] for generating the *first* [`Operation`] in the sequence (eg
    /// not dependent on previous operations)
    fn first_arbitrary(
        key_columns: [(&str, usize); K],
        row_strategies: HashMap<&'static str, Vec<BoxedStrategy<DataType>>>,
    ) -> impl Strategy<Value = Self> {
        Self::arbitrary(OperationParameters {
            already_generated: &[],
            key_columns,
            row_strategies,
        })
    }

    /// Return a proptest [`Strategy`] for generating all but the first [`Operation`], based on the
    /// previously generated operations
    fn arbitrary(params: OperationParameters<K>) -> impl Strategy<Value = Self> + 'static
    where
        [BoxedStrategy<DataType>; K]: Strategy<Value = [DataType; K]>,
    {
        use Operation::*;

        let row_strategies = params.row_strategies.clone();
        let non_key_ops = prop_oneof![
            params.key_strategy().prop_map(|key| Query { key }),
            select(row_strategies.into_iter().collect::<Vec<_>>()).prop_flat_map(
                |(table, row_strat)| row_strat.prop_map(move |row| Insert {
                    table: table.to_string(),
                    row,
                })
            )
        ];

        let keys = params.existing_keys().collect::<Vec<_>>();
        if keys.is_empty() {
            non_key_ops.boxed()
        } else {
            prop_oneof![non_key_ops, select(keys).prop_map(|key| Query { key })].boxed()
        }
    }
}

/// The result of running a single [`Operation`] against either mysql or noria.
#[derive(Debug)]
pub enum OperationResult {
    Err(mysql::Error),
    Rows(Vec<mysql::Row>),
    NoResults,
}

impl OperationResult {
    /// Returns `true` if the operation result is [`Err`].
    ///
    /// [`Err`]: OperationResult::Err
    pub fn is_err(&self) -> bool {
        matches!(self, Self::Err(..))
    }

    pub fn err(&self) -> Option<&mysql::Error> {
        match self {
            Self::Err(v) => Some(v),
            _ => None,
        }
    }
}

impl PartialEq for OperationResult {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            // TODO: is it worth trying to check error equality here? probably not
            (Self::Err(_), Self::Err(_)) => true,
            (Self::Rows(rs1), Self::Rows(rs2)) => {
                rs1.len() == rs2.len()
                    && rs1.iter().zip(rs2).all(|(r1, r2)| {
                        r1.len() == r2.len()
                            && (0..r1.len()).all(|i| r1.as_ref(i).unwrap() == r2.as_ref(i).unwrap())
                    })
            }
            (Self::NoResults, Self::NoResults) => true,
            _ => false,
        }
    }
}

impl From<mysql::Result<Vec<mysql::Row>>> for OperationResult {
    fn from(res: mysql::Result<Vec<mysql::Row>>) -> Self {
        match res {
            Ok(rows) => Self::Rows(rows),
            Err(e) => Self::Err(e),
        }
    }
}

impl From<mysql::Result<()>> for OperationResult {
    fn from(res: mysql::Result<()>) -> Self {
        match res {
            Ok(()) => Self::NoResults,
            Err(e) => Self::Err(e),
        }
    }
}

impl<const K: usize> Operation<K> {
    fn run(
        &self,
        conn: &mut mysql::Conn,
        query: &'static str,
    ) -> Result<OperationResult, TestCaseError> {
        fn to_values(dts: &[DataType]) -> Result<Vec<Value>, TestCaseError> {
            dts.iter()
                .map(|dt| {
                    Value::try_from(dt.clone()).map_err(|e| {
                        TestCaseError::reject(format!(
                            "DataType conversion to mysql Value failed: {}",
                            e
                        ))
                    })
                })
                .collect()
        }

        match self {
            Operation::Query { key } => Ok(conn
                .exec::<mysql::Row, _, _>(query, mysql::Params::Positional(to_values(key)?))
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
                .into()),
        }
    }
}

struct OperationsParams<const K: usize> {
    size_range: Range<usize>,
    key_columns: [(&'static str, usize); K],
    row_strategies: HashMap<&'static str, Vec<BoxedStrategy<DataType>>>,
}

#[derive(Default, Debug, Clone)]
struct Operations<const K: usize>(Vec<Operation<K>>);

impl<const K: usize> Operations<K>
where
    [DataType; K]: Arbitrary,
    [BoxedStrategy<DataType>; K]: Strategy<Value = [DataType; K]>,
{
    fn arbitrary(mut params: OperationsParams<K>) -> impl Strategy<Value = Self> {
        let key_columns = params.key_columns;
        let row_strategies = mem::take(&mut params.row_strategies);
        params.size_range.prop_flat_map(move |len| {
            if len == 0 {
                return Just(Default::default()).boxed();
            }

            let mut res = Operation::first_arbitrary(key_columns, row_strategies.clone())
                .prop_map(|op| vec![op])
                .boxed();
            for _ in 0..len {
                let row_strategies = row_strategies.clone();
                res = res
                    .prop_flat_map(move |ops| {
                        let op_params = OperationParameters {
                            already_generated: &ops,
                            key_columns,
                            row_strategies: row_strategies.clone(),
                        };

                        Operation::arbitrary(op_params).prop_map(move |op| {
                            ops.clone().into_iter().chain(iter::once(op)).collect()
                        })
                    })
                    .boxed();
            }
            res.prop_map(Operations).boxed()
        })
    }
}

impl<const K: usize> Operations<K> {
    fn run(self, query: &'static str, tables: Vec<&'static str>) -> TestCaseResult {
        common::recreate_database("vertical");
        let mut mysql = mysql::Conn::new(
            mysql::OptsBuilder::default()
                .user(Some("root"))
                .pass(Some("noria"))
                .ip_or_hostname(Some(
                    env::var("MYSQL_HOST").unwrap_or_else(|_| "127.0.0.1".into()),
                ))
                .tcp_port(
                    env::var("MYSQL_TCP_PORT")
                        .unwrap_or_else(|_| "3306".into())
                        .parse()
                        .unwrap(),
                )
                .db_name(Some("vertical")),
        )
        .unwrap();
        let mut noria = mysql::Conn::new(common::setup(true)).unwrap();

        for table in tables {
            mysql.query_drop(table).unwrap();
            noria.query_drop(table).unwrap();
        }

        for op in self.0 {
            let mysql_res = op.run(&mut mysql, query)?;
            // skip tests where mysql returns an error for the operations
            prop_assume!(
                !mysql_res.is_err(),
                "MySQL returned an error: {}",
                mysql_res.err().unwrap()
            );

            let noria_res = op.run(&mut noria, query)?;
            assert_eq!(mysql_res, noria_res);
        }

        Ok(())
    }
}

macro_rules! replace_expr {
    ($_t:tt $sub:expr) => {
        $sub
    };
}

macro_rules! vertical_tests {
    ($name:ident($($params: tt)*); $($rest: tt)*) => {
        vertical_tests!(@test $name($($params)*));
        vertical_tests!($($rest)*);
    };

    // define the test itself
    (@test $(#[$meta:meta])* $name:ident($query: expr; $($tables: tt)*)) => {
        fn generate_ops() -> impl Strategy<Value = Operations<{vertical_tests!(@key_len $($tables)*)}>> {
            let size_range = 1..100; // TODO make configurable
            let mut row_strategies = HashMap::new();
            vertical_tests!(@row_strategies row_strategies, $($tables)*);
            let key_columns = vertical_tests!(@key_columns $($tables)*);

            let params = OperationsParams {
                size_range,
                key_columns,
                row_strategies,
            };

            Operations::arbitrary(params)
        }

        #[proptest]
        #[serial_test::serial]
        $(#[$meta])*
        fn $name(
            #[strategy(generate_ops())]
            operations: Operations<{vertical_tests!(@key_len $($tables)*)}>
        ) {
            let tables = vertical_tests!(@tables $($tables)*);
            operations.run($query, tables)?;
        }
    };

    // make the const literal for the K type parameter by summing up the lengths of the
    // `key_columns` in the tables
    (@key_len $(,)?) => {0usize};
    (@key_len $table_name: expr => (
        $create_table: expr,
        schema: [$($schema: tt)*],
        primary_key: $pk_index: expr,
        key_columns: []
        $(,)?
    ) $($tables: tt)*) => {vertical_tests!(@key_len $($tables)*)};
    (@key_len $table_name: expr => (
        $create_table: expr,
        schema: [$($schema: tt)*],
        primary_key: $pk_index: expr,
        key_columns: [$($kc: expr),* $(,)?]
        $(,)?
    ) $($tables: tt)*) => {
        0usize $(+ replace_expr!($kc 1usize))* + vertical_tests!(@key_len $($tables)*)
    };

    // collect together all of the key columns from the tables into a single array
    (@key_columns $($table_name: expr => (
        $create_table: expr,
        schema: [$($schema_type: ty),* $(,)?],
        primary_key: $pk_index: expr,
        key_columns: [$($kc: expr),* $(,)?]
        $(,)?
    )),* $(,)?) => {
        [$($(($table_name, $kc),)*)*]
    };

    // make a vec of create_table strings
    (@tables $($table_name: expr => (
        $create_table: expr,
        schema: [$($schema_type: ty),* $(,)?],
        primary_key: $pk_index: expr,
        key_columns: [$($kc: expr),* $(,)?]
        $(,)?
    )),* $(,)?) => {
        vec![$($create_table,)*]
    };

    // Build up the hashmap of row_strategies, storing the result in the variable named `$out`
    (@row_strategies $out: expr, $(,)?) => {};
    (@row_strategies $out: expr, $table_name: expr => (
        $create_table: expr,
        schema: [$($schema_type: ty),* $(,)?],
        primary_key: $pk_index: expr,
        key_columns: [$($kc: expr),* $(,)?] $(,)?
    ) $(, $tables: tt)*) => {
        let row_strategy = vec![
            $(any::<$schema_type>().prop_map_into::<DataType>().boxed(), )*
        ];
        $out.insert($table_name, row_strategy);

        vertical_tests!(@row_strategies $out, $($tables)*);
    };

    () => {};
}

vertical_tests! {
    simple_point_lookup(
        "SELECT id, name FROM users WHERE id = ?";
        "users" => (
            "CREATE TABLE users (id INT, name TEXT, PRIMARY KEY (id))",
            schema: [i32, String],
            primary_key: 0,
            key_columns: [0],
        )
    );
}
