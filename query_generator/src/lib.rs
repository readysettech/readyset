#![warn(clippy::dbg_macro)]
//! A deterministic, exhaustive, parametric generator for SQL queries, and associated DDL.
//!
//! The intent of this library is to be used to automatically and *deterministically* generate an
//! exhaustive set of SQL queries, to be used as seed data to run *comparative* benchmarks of
//! various operators in Noria. Notably, this means a few things are explicitly *not* in scope for
//! this library:
//!
//! - The queries we generate are intended primarily for benchmarking, not for correctness testing.
//!   For example, we don't attempt to generate interesting seed data to exercise edge cases in
//!   operators.
//! - Everything this library does *must* be deterministic, so we can provide a consistent and
//!   reproducible environment for running comparative benchmarks. This means no random generation
//!   of permutations of query operators, and no random seed data.
//!
//! Alongside the library component of this crate is a command-line interface with a runtime for
//! running benchmarks on generated queries against noria and collecting metrics - see the
//! documentation for `main.rs` for more information.
//!
//! # Examples
//!
//! Generating a simple query, with a single query parameter and a single inner join:
//!
//! ```rust
//! use query_generator::{GeneratorState, QueryOperation};
//! use nom_sql::JoinOperator;
//!
//! let mut gen = GeneratorState::default();
//! let query = gen.generate_query(&[
//!   QueryOperation::SingleParameter,
//!   QueryOperation::Join(JoinOperator::InnerJoin),
//! ]);
//! let query_str = format!("{}", query.statement);
//! assert_eq!(query_str, "SELECT table_1.column_3, table_2.column_2 \
//! FROM table_1 \
//! INNER JOIN table_2 ON table_1.column_2 = table_2.column_1 \
//! WHERE table_1.column_1 = ?");
//! ```
//!
//! # Architecture
//!
//! - There's a [`QueryOperation`] enum which enumerates, in some sense, the individual "operations"
//!   that can be performed as part of a SQL query
//! - Each [`QueryOperation`] knows how to [add itself to a SQL query][0]
//! - To support that, there's a [`GeneratorState`] struct, to which mutable references get passed
//!   around, which knows how to summon up [new tables][1] and [columns][2] for use in queries
//! - We can then [calculate all permutations of all the possible QueryOperations][3] (up to a
//!   certain depth), and use those to generate queries.
//!
//! [0]: QueryOperation::add_to_query
//! [1]: GeneratorState::fresh_table_mut
//! [2]: TableSpec::fresh_column
//! [3]: QueryOperation::permute

#![feature(duration_zero)]

use anyhow::anyhow;
use chrono::{NaiveDate, NaiveTime};
use derive_more::{Display, From, Into};
use itertools::Itertools;
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use std::borrow::Borrow;
use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::iter;
use std::str::FromStr;
use strum::IntoEnumIterator;
use strum_macros::EnumIter;

use nom_sql::{
    BinaryOperator, Column, ColumnSpecification, ConditionBase, ConditionExpression, ConditionTree,
    CreateTableStatement, FieldDefinitionExpression, FieldValueExpression, FunctionArgument,
    FunctionExpression, ItemPlaceholder, JoinClause, JoinConstraint, JoinOperator, JoinRightSide,
    Literal, LiteralExpression, SelectStatement, SqlType, Table,
};
use noria::DataType;

/// Generate a constant value with the given [`SqlType`]
///
/// The following SqlTypes do not have a representation as a [`DataType`] and will panic if passed:
///
/// - [`SqlType::Date`]
/// - [`SqlType::Enum`]
/// - [`SqlType::Bool`]
fn value_of_type(typ: &SqlType) -> DataType {
    match typ {
        SqlType::Char(_)
        | SqlType::Varchar(_)
        | SqlType::Blob
        | SqlType::Longblob
        | SqlType::Mediumblob
        | SqlType::Tinyblob
        | SqlType::Tinytext
        | SqlType::Mediumtext
        | SqlType::Longtext
        | SqlType::Text
        | SqlType::Binary(_)
        | SqlType::Varbinary(_) => "a".into(),
        SqlType::Int(_) => 1i32.into(),
        SqlType::Bigint(_) => 1i64.into(),
        SqlType::UnsignedInt(_) => 1u32.into(),
        SqlType::UnsignedBigint(_) => 1u64.into(),
        SqlType::Tinyint(_) => 1i8.into(),
        SqlType::UnsignedTinyint(_) => 1u8.into(),
        SqlType::Smallint(_) => 1i16.into(),
        SqlType::UnsignedSmallint(_) => 1u16.into(),
        SqlType::Double | SqlType::Float | SqlType::Real | SqlType::Decimal(_, _) => 1.5.into(),
        SqlType::DateTime(_) | SqlType::Timestamp => {
            NaiveDate::from_ymd(2020, 1, 1).and_hms(12, 30, 45).into()
        }
        SqlType::Time => NaiveTime::from_hms(12, 30, 45).into(),
        SqlType::Date => unimplemented!(),
        SqlType::Enum(_) => unimplemented!(),
        SqlType::Bool => unimplemented!(),
    }
}

/// Generate a unique value with the given [`SqlType`] from a monotonically increasing counter,
/// `idx`.
///
/// This is an injective function (from `(idx, typ)` to the resultant [`DataType`]).
///
/// The following SqlTypes do not have a representation as a [`DataType`] and will panic if passed:
///
/// - [`SqlType::Date`]
/// - [`SqlType::Enum`]
/// - [`SqlType::Bool`]
fn unique_value_of_type(typ: &SqlType, idx: u8) -> DataType {
    match typ {
        SqlType::Char(_)
        | SqlType::Varchar(_)
        | SqlType::Blob
        | SqlType::Longblob
        | SqlType::Mediumblob
        | SqlType::Tinyblob
        | SqlType::Tinytext
        | SqlType::Mediumtext
        | SqlType::Longtext
        | SqlType::Text
        | SqlType::Binary(_)
        | SqlType::Varbinary(_) => format!("{}", idx).into(),
        SqlType::Int(_) => (2i32 + idx as i32).into(),
        SqlType::Bigint(_) => (2i64 + idx as i64).into(),
        SqlType::UnsignedInt(_) => (2u32 + idx as u32).into(),
        SqlType::UnsignedBigint(_) => (2u64 + idx as u64).into(),
        SqlType::Tinyint(_) => (2i8 + idx as i8).into(),
        SqlType::UnsignedTinyint(_) => (2u8 + idx).into(),
        SqlType::Smallint(_) => (2i16 + idx as i16).into(),
        SqlType::UnsignedSmallint(_) => (1u16 + idx as u16).into(),
        SqlType::Double | SqlType::Float | SqlType::Real | SqlType::Decimal(_, _) => {
            (1.5 + idx as f64).into()
        }
        SqlType::DateTime(_) | SqlType::Timestamp => NaiveDate::from_ymd(2020, 1, 1)
            .and_hms(12, idx as _, 30)
            .into(),
        SqlType::Date => unimplemented!(),
        SqlType::Enum(_) => unimplemented!(),
        SqlType::Bool => unimplemented!(),
        SqlType::Time => NaiveTime::from_hms(12, idx as _, 30).into(),
    }
}

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Hash, From, Into, Display, Clone)]
#[repr(transparent)]
pub struct TableName(String);

impl From<TableName> for Table {
    fn from(name: TableName) -> Self {
        Table {
            name: name.into(),
            alias: None,
            schema: None,
        }
    }
}

impl<'a> From<&'a TableName> for &'a str {
    fn from(tn: &'a TableName) -> Self {
        &tn.0
    }
}

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Hash, From, Into, Display, Clone)]
#[repr(transparent)]
pub struct ColumnName(String);

impl From<ColumnName> for Column {
    fn from(name: ColumnName) -> Self {
        Self {
            name: name.into(),
            alias: None,
            table: None,
            function: None,
        }
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct TableSpec {
    pub name: TableName,
    pub columns: HashMap<ColumnName, SqlType>,
    column_name_counter: u32,

    /// Values per column that should be present in that column at least some of the time.
    ///
    /// This is used to ensure that queries that filter on constant values get at least some results
    expected_values: HashMap<ColumnName, HashSet<DataType>>,
}

impl From<TableSpec> for CreateTableStatement {
    fn from(spec: TableSpec) -> Self {
        CreateTableStatement {
            table: spec.name.into(),
            fields: spec
                .columns
                .into_iter()
                .map(|(col_name, col_type)| ColumnSpecification {
                    column: col_name.into(),
                    sql_type: col_type,
                    constraints: vec![],
                    comment: None,
                })
                .collect(),
            keys: None,
        }
    }
}

impl TableSpec {
    pub fn new(name: TableName) -> Self {
        Self {
            name,
            columns: Default::default(),
            column_name_counter: 0,
            expected_values: Default::default(),
        }
    }

    /// Generate a new, unique column in this table (of an unspecified type) and return its name
    pub fn fresh_column(&mut self) -> ColumnName {
        self.fresh_column_with_type(SqlType::Int(32))
    }

    /// Generate a new, unique column in this table with the specified type and return its name
    pub fn fresh_column_with_type(&mut self, col_type: SqlType) -> ColumnName {
        self.column_name_counter += 1;
        let column_name = ColumnName(format!("column_{}", self.column_name_counter));
        self.columns.insert(column_name.clone(), col_type);
        column_name
    }

    /// Returns the name of *some* column in this table, potentially generating a new column if
    /// necessary
    pub fn some_column_name(&mut self) -> ColumnName {
        self.columns
            .keys()
            .next()
            .cloned()
            .unwrap_or_else(|| self.fresh_column())
    }

    /// Record that the column given by `column_name` should contain `value` at least some of the
    /// time.
    ///
    /// This can be used, for example, to ensure that queries that filter comparing against a
    /// constant value return at least some results
    pub fn expect_value(&mut self, column_name: ColumnName, value: DataType) {
        assert!(self.columns.contains_key(&column_name));
        self.expected_values
            .entry(column_name)
            .or_default()
            .insert(value);
    }

    /// Generate `num_rows` rows of data for this table
    pub fn generate_data(&self, num_rows: usize) -> Vec<HashMap<&ColumnName, DataType>> {
        (0..num_rows)
            .map(|n| {
                self.columns
                    .iter()
                    .map(|(col_name, col_type)| {
                        (
                            col_name,
                            // if we have expected values, yield them half the time
                            if n % 2 == 0 {
                                self.expected_values
                                    .get(col_name)
                                    .map(|vals| {
                                        // yield an even distribution of the expected values
                                        vals.iter().nth((n / 2) % vals.len()).unwrap().clone()
                                    })
                                    .unwrap_or_else(|| value_of_type(col_type))
                            } else {
                                value_of_type(col_type)
                            },
                        )
                    })
                    .collect()
            })
            .collect()
    }
}

#[derive(Debug, Default)]
pub struct GeneratorState {
    tables: HashMap<TableName, TableSpec>,
    table_name_counter: u32,
    alias_counter: u32,
}

impl GeneratorState {
    /// Create a new, unique, empty table, and return a mutable reference to that table
    pub fn fresh_table_mut(&mut self) -> &mut TableSpec {
        self.table_name_counter += 1;
        let table_name = TableName(format!("table_{}", self.table_name_counter));
        self.tables
            .entry(table_name)
            .or_insert_with_key(|tn| TableSpec::new(tn.clone()))
    }

    /// Returns a mutable reference to the table with the given name, if it exists
    pub fn table_mut<'a, TN>(&'a mut self, name: &TN) -> Option<&'a mut TableSpec>
    where
        TableName: Borrow<TN>,
        TN: Eq + Hash,
    {
        self.tables.get_mut(name)
    }

    /// Returns an iterator over all the names of tables created for queries by this generator state
    pub fn table_names(&self) -> impl Iterator<Item = &TableName> {
        self.tables.keys()
    }

    /// Return a mutable reference to *some* table in the schema - the implication being that the
    /// caller doesn't care which table
    pub fn some_table_mut(&mut self) -> &mut TableSpec {
        if self.tables.is_empty() {
            self.fresh_table_mut()
        } else {
            self.tables.values_mut().next().unwrap()
        }
    }

    pub fn new_query(&mut self) -> QueryState<'_> {
        QueryState::new(self)
    }

    /// Generate a new query using the given list of [`QueryOperation`]s
    pub fn generate_query<'gen, 'a, I>(&'gen mut self, operations: I) -> Query<'gen>
    where
        I: IntoIterator<Item = &'a QueryOperation>,
    {
        let mut query = SelectStatement::default();
        let mut state = self.new_query();
        for op in operations {
            op.add_to_query(&mut state, &mut query);
        }

        if query.tables.is_empty() {
            let table = state.tables.iter().next().unwrap();
            query.tables.push(table.clone().into());
        }

        if query.fields.is_empty() {
            query.fields.push(FieldDefinitionExpression::All);
        }

        Query::new(state, query)
    }

    /// Generate a list of queries given by permutations of all query operations up to length
    /// `max_depth`
    pub fn generate_queries(
        &mut self,
        max_depth: usize,
    ) -> impl Iterator<Item = SelectStatement> + '_ {
        QueryOperation::permute(max_depth).map(move |ops| self.generate_query(ops).statement)
    }

    /// Return an iterator over `CreateTableStatement`s for all the tables in the schema
    pub fn into_ddl(self) -> impl Iterator<Item = CreateTableStatement> {
        self.tables.into_iter().map(|(_, tbl)| tbl.into())
    }

    /// Return an iterator over clones of `CreateTableStatement`s for all the tables in the schema
    pub fn ddl(&self) -> impl Iterator<Item = CreateTableStatement> + '_ {
        self.tables.iter().map(|(_, tbl)| tbl.clone().into())
    }

    /// Generate `num_rows` rows of data for the table given by `table_name`
    ///
    /// # Panics
    ///
    /// Panics if `table_name` is not a known table
    pub fn generate_data_for_table(
        &self,
        table_name: &TableName,
        num_rows: usize,
    ) -> Vec<HashMap<&ColumnName, DataType>> {
        self.tables[table_name].generate_data(num_rows)
    }
}

pub struct QueryState<'a> {
    gen: &'a mut GeneratorState,
    tables: HashSet<TableName>,
    parameters: Vec<(TableName, ColumnName)>,
    unique_parameters: HashMap<TableName, Vec<(ColumnName, DataType)>>,
    alias_counter: u32,
    datatype_counter: u8,
}

impl<'a> QueryState<'a> {
    pub fn new(gen: &'a mut GeneratorState) -> Self {
        Self {
            gen,
            tables: HashSet::new(),
            unique_parameters: HashMap::new(),
            parameters: Vec::new(),
            alias_counter: 0,
            datatype_counter: 0,
        }
    }

    /// Generate a new, unique column alias for the query
    pub fn fresh_alias(&mut self) -> String {
        self.alias_counter += 1;
        format!("alias_{}", self.alias_counter)
    }

    /// Return a mutable reference to *some* table in the schema - the implication being that the
    /// caller doesn't care which table
    pub fn some_table_mut(&mut self) -> &mut TableSpec {
        let table = self.gen.some_table_mut();
        self.tables.insert(table.name.clone());
        table
    }

    /// Create a new, unique, empty table, and return a mutable reference to that table
    pub fn fresh_table_mut(&mut self) -> &mut TableSpec {
        let table = self.gen.fresh_table_mut();
        self.tables.insert(table.name.clone());
        table
    }

    /// Generate `rows_per_table` rows of data for all the tables referenced in the query for this
    /// QueryState.
    ///
    /// If `make_unique` is true and `make_unique_key` was previously called, the returned rows
    /// are modified to match the key returned by `make_unique_key`.
    pub fn generate_data(
        &self,
        rows_per_table: usize,
        make_unique: bool,
    ) -> HashMap<&TableName, Vec<HashMap<&ColumnName, DataType>>> {
        self.tables
            .iter()
            .map(|table_name| {
                let mut rows = self.gen.generate_data_for_table(table_name, rows_per_table);
                if make_unique {
                    if let Some(column_data) = self.unique_parameters.get(table_name) {
                        for row in &mut rows {
                            for (column, data) in column_data {
                                row.insert(&column, data.clone());
                            }
                        }
                    }
                }
                (table_name, rows)
            })
            .collect()
    }

    /// Record a new (positional) parameter for the query, comparing against the given column of the
    /// given table
    pub fn add_parameter(&mut self, table_name: TableName, column_name: ColumnName) {
        self.parameters.push((table_name, column_name))
    }

    /// Make a new, unique key for all the parameters in the query.
    ///
    /// To get data that matches this key, call `generate_data()` after calling this function.
    pub fn make_unique_key(&mut self) -> Vec<DataType> {
        let mut ret = Vec::with_capacity(self.parameters.len());
        for (table_name, column_name) in self.parameters.iter() {
            let val = unique_value_of_type(
                &self.gen.tables[table_name].columns[column_name],
                self.datatype_counter,
            );
            self.unique_parameters
                .entry(table_name.clone())
                .or_insert_with(|| vec![])
                .push((column_name.clone(), val.clone()));
            self.datatype_counter += 1;
            ret.push(val);
        }
        ret
    }

    /// Returns a lookup key for the parameters in the query that will return results
    pub fn key(&self) -> Vec<DataType> {
        self.parameters
            .iter()
            .map(|(table_name, column_name)| {
                value_of_type(&self.gen.tables[table_name].columns[column_name])
            })
            .collect()
    }
}

pub struct Query<'gen> {
    pub state: QueryState<'gen>,
    pub statement: SelectStatement,
}

impl<'gen> Query<'gen> {
    pub fn new(state: QueryState<'gen>, statement: SelectStatement) -> Self {
        Self { state, statement }
    }

    /// Converts the DDL for this query into a Noria recipe
    pub fn ddl_recipe(&self) -> String {
        self.state
            .tables
            .iter()
            .map(|table_name| {
                let stmt = CreateTableStatement::from(self.state.gen.tables[table_name].clone());
                format!("{};", stmt)
            })
            .join("\n")
    }

    /// Converts this query into a Noria recipe, including both the DDL and the query itself, using
    /// the given name for the query
    pub fn to_recipe(&self, query_name: &str) -> String {
        format!(
            "{}\nQUERY {}: {};",
            self.ddl_recipe(),
            query_name,
            self.statement
        )
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Copy, EnumIter, Serialize, Deserialize)]
pub enum AggregateType {
    Count,
    Sum,
    Avg,
    GroupConcat,
    Max,
    Min,
}

#[derive(Debug, Eq, PartialEq, Clone, Copy, EnumIter, Serialize, Deserialize)]
pub enum FilterRHS {
    Constant,
    Column,
}

#[derive(Debug, Eq, PartialEq, Clone, Copy, EnumIter, Serialize, Deserialize)]
pub enum LogicalOp {
    And,
    Or,
}

impl From<LogicalOp> for BinaryOperator {
    fn from(op: LogicalOp) -> Self {
        match op {
            LogicalOp::And => BinaryOperator::And,
            LogicalOp::Or => BinaryOperator::Or,
        }
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub struct Filter {
    pub operator: BinaryOperator,
    pub rhs: FilterRHS,
    pub extend_where_with: LogicalOp,
}

/// Operations that can be performed as part of a SQL query
///
/// Members of this enum represent some sense of an individual operation that can be performed on an
/// arbitrary SQL query. Each operation knows how to add itself to a given SQL query (via
/// [`add_to_query`](QueryOperation::add_to_query)) with the aid of a mutable reference to a
/// [`GeneratorState`].
///
/// Note that not every operation that Noria supports is currently included in this enum - planned
/// for the future are:
///
/// - arithmetic projections
/// - topk
/// - union
/// - order by
/// - ilike
///
/// each of which should be relatively straightforward to add here.
#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub enum QueryOperation {
    ColumnAggregate(AggregateType),
    Filter(Filter),
    Distinct,
    Join(JoinOperator),
    ProjectLiteral,
    SingleParameter,
    MultipleParameters,
}

const COMPARISON_OPS: &[BinaryOperator] = &[
    BinaryOperator::Equal,
    BinaryOperator::NotEqual,
    BinaryOperator::Greater,
    BinaryOperator::GreaterOrEqual,
    BinaryOperator::Less,
    BinaryOperator::LessOrEqual,
];

const JOIN_OPERATORS: &[JoinOperator] = &[
    JoinOperator::LeftJoin,
    JoinOperator::LeftOuterJoin,
    JoinOperator::InnerJoin,
];

lazy_static! {
    static ref ALL_FILTERS: Vec<Filter> = {
        COMPARISON_OPS
            .iter()
            .cartesian_product(FilterRHS::iter())
            .cartesian_product(LogicalOp::iter())
            .map(|((operator, rhs), extend_where_with)| Filter {
                operator: *operator,
                rhs,
                extend_where_with,
            })
            .collect()
    };

    /// A list of all possible [`QueryOperation`]s
    pub static ref ALL_OPERATIONS: Vec<QueryOperation> = {
        AggregateType::iter()
            .map(QueryOperation::ColumnAggregate)
            .chain(ALL_FILTERS.iter().cloned().map(QueryOperation::Filter))
            .chain(iter::once(QueryOperation::Distinct))
            .chain(JOIN_OPERATORS.iter().cloned().map(QueryOperation::Join))
            .chain(iter::once(QueryOperation::ProjectLiteral))
            .chain(iter::once(QueryOperation::SingleParameter))
            .collect()
    };
}

fn extend_where(query: &mut SelectStatement, op: LogicalOp, cond: ConditionExpression) {
    query.where_clause = Some(match query.where_clause.take() {
        Some(existing_cond) => ConditionExpression::LogicalOp(ConditionTree {
            operator: op.into(),
            left: Box::new(existing_cond),
            right: Box::new(cond),
        }),
        None => cond,
    })
}

fn and_where(query: &mut SelectStatement, cond: ConditionExpression) {
    extend_where(query, LogicalOp::And, cond)
}

impl QueryOperation {
    /// Add this query operation to `query`, recording information about new tables and columns in
    /// `state`.
    fn add_to_query<'state>(&self, state: &mut QueryState<'state>, query: &mut SelectStatement) {
        match self {
            QueryOperation::ColumnAggregate(agg) => {
                use AggregateType::*;

                let alias = state.fresh_alias();
                let tbl = state.some_table_mut();
                let col = tbl.fresh_column_with_type(match agg {
                    GroupConcat => SqlType::Text,
                    _ => SqlType::Int(32),
                });
                let arg = FunctionArgument::Column(Column {
                    name: col.into(),
                    alias: None,
                    table: Some(tbl.name.clone().into()),
                    function: None,
                });
                let func = match agg {
                    Count => FunctionExpression::Count(arg, false),
                    Sum => FunctionExpression::Sum(arg, false),
                    Avg => FunctionExpression::Avg(arg, false),
                    GroupConcat => FunctionExpression::GroupConcat(arg, ", ".to_owned()),
                    Max => FunctionExpression::Max(arg),
                    Min => FunctionExpression::Min(arg),
                };

                query.fields.push(FieldDefinitionExpression::Col(Column {
                    name: alias.clone(),
                    alias: Some(alias),
                    table: None,
                    function: Some(Box::new(func)),
                }))
            }

            QueryOperation::Filter(filter) => {
                let tbl = state.some_table_mut();
                let col = tbl.fresh_column_with_type(SqlType::Int(1));
                let right = Box::new(match filter.rhs {
                    FilterRHS::Constant => {
                        tbl.expect_value(col.clone(), 1i32.into());
                        ConditionExpression::Base(ConditionBase::Literal(Literal::Integer(1)))
                    }
                    FilterRHS::Column => {
                        let col = tbl.fresh_column();
                        ConditionExpression::Base(ConditionBase::Field(Column {
                            table: Some(tbl.name.clone().into()),
                            ..col.into()
                        }))
                    }
                });

                let cond = ConditionExpression::ComparisonOp(ConditionTree {
                    operator: filter.operator,
                    left: Box::new(ConditionExpression::Base(ConditionBase::Field(Column {
                        table: Some(tbl.name.clone().into()),
                        ..col.clone().into()
                    }))),
                    right,
                });

                query.fields.push(FieldDefinitionExpression::Col(Column {
                    table: Some(tbl.name.clone().into()),
                    ..col.into()
                }));

                extend_where(query, filter.extend_where_with, cond);
            }

            QueryOperation::Distinct => {
                query.distinct = true;
            }

            QueryOperation::Join(operator) => {
                let left_table = state.some_table_mut();
                let left_table_name = left_table.name.clone();
                let left_join_key = left_table.fresh_column_with_type(SqlType::Int(32));
                let left_projected = left_table.fresh_column();

                if query.tables.is_empty() {
                    query.tables.push(left_table_name.clone().into());
                }

                let right_table = state.fresh_table_mut();
                let right_table_name = right_table.name.clone();
                let right_join_key = right_table.fresh_column_with_type(SqlType::Int(32));
                let right_projected = right_table.fresh_column();

                query.join.push(JoinClause {
                    operator: *operator,
                    right: JoinRightSide::Table(right_table.name.clone().into()),
                    constraint: JoinConstraint::On(ConditionExpression::ComparisonOp(
                        ConditionTree {
                            operator: BinaryOperator::Equal,
                            left: Box::new(ConditionExpression::Base(ConditionBase::Field(
                                Column {
                                    table: Some(left_table_name.clone().into()),
                                    ..left_join_key.into()
                                },
                            ))),
                            right: Box::new(ConditionExpression::Base(ConditionBase::Field(
                                Column {
                                    table: Some(right_table_name.clone().into()),
                                    ..right_join_key.into()
                                },
                            ))),
                        },
                    )),
                });

                query.fields.push(FieldDefinitionExpression::Col(Column {
                    table: Some(left_table_name.into()),
                    ..left_projected.into()
                }));
                query.fields.push(FieldDefinitionExpression::Col(Column {
                    table: Some(right_table_name.into()),
                    ..right_projected.into()
                }));
            }

            QueryOperation::ProjectLiteral => {
                query.fields.push(FieldDefinitionExpression::Value(
                    FieldValueExpression::Literal(LiteralExpression {
                        value: Literal::Integer(1),
                        alias: None,
                    }),
                ));
            }

            QueryOperation::SingleParameter => {
                let table = state.some_table_mut();
                let col = table.fresh_column();
                and_where(
                    query,
                    ConditionExpression::ComparisonOp(ConditionTree {
                        operator: BinaryOperator::Equal,
                        left: Box::new(ConditionExpression::Base(ConditionBase::Field(Column {
                            table: Some(table.name.clone().into()),
                            ..col.clone().into()
                        }))),
                        right: Box::new(ConditionExpression::Base(ConditionBase::Literal(
                            Literal::Placeholder(ItemPlaceholder::QuestionMark),
                        ))),
                    }),
                );
                let table_name = table.name.clone();
                state.add_parameter(table_name, col);
            }

            QueryOperation::MultipleParameters => {
                QueryOperation::SingleParameter.add_to_query(state, query);
                QueryOperation::SingleParameter.add_to_query(state, query);
            }
        }
    }

    /// Returns an iterator over all permuations of length 1..`max_depth` [`QueryOperation`]s.
    pub fn permute(max_depth: usize) -> impl Iterator<Item = Vec<&'static QueryOperation>> {
        (1..=max_depth).flat_map(|depth| ALL_OPERATIONS.iter().combinations(depth))
    }
}

/// Representation of a subset of permutations of query operations
#[repr(transparent)]
pub struct Operations(pub Vec<Vec<QueryOperation>>);

impl FromStr for Operations {
    type Err = anyhow::Error;

    /// Parse a specification for a subset of permutations of query operations from a human-supplied
    /// string.
    ///
    /// The supported syntax is a comma-separated list of specifications for query operations, and
    /// the result will be a list of all permutations of the corresponding query operations.
    ///
    /// The supported specifications are:
    ///
    /// | Specification                           | Meaning                           |
    /// |-----------------------------------------|-----------------------------------|
    /// | aggregates                              | All [`AggregateType`]s            |
    /// | filters                                 | All constant-valued [`Filter`]s   |
    /// | distinct                                | `SELECT DISTINCT`                 |
    /// | joins                                   | Joins, with all [`JoinOperator`]s |
    /// | single_parameter / single_param / param | A single query parameter          |
    /// | project_literal                         | A projected literal value         |
    /// | multiple_parameters / params            | Multiple query parameters         |
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        use QueryOperation::*;

        Ok(Self(
            s.split(',')
                .map(|s| -> anyhow::Result<Vec<_>> {
                    match s {
                        "aggregates" => Ok(AggregateType::iter().map(ColumnAggregate).collect()),
                        "filters" => Ok(ALL_FILTERS.iter().cloned().map(Filter).collect()),
                        "distinct" => Ok(vec![Distinct]),
                        "joins" => Ok(JOIN_OPERATORS.iter().cloned().map(Join).collect()),
                        "single_parameter" | "single_param" | "param" => Ok(vec![SingleParameter]),
                        "project_literal" => Ok(vec![ProjectLiteral]),
                        "multiple_parameters" | "params" => Ok(vec![MultipleParameters]),
                        s => Err(anyhow!("unknown query operation: {}", s)),
                    }
                })
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .multi_cartesian_product()
                .collect(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn generate_query<'a, I>(ops: I) -> SelectStatement
    where
        I: IntoIterator<Item = &'a QueryOperation>,
    {
        let mut gen = GeneratorState::default();
        gen.generate_query(ops).statement
    }

    #[test]
    fn parse_operations() {
        let src = "aggregates,joins";
        let Operations(res) = Operations::from_str(src).unwrap();
        assert_eq!(res.len(), 18);
        assert!(res.contains(&vec![
            QueryOperation::ColumnAggregate(AggregateType::Count),
            QueryOperation::Join(JoinOperator::LeftJoin)
        ]))
    }

    #[test]
    fn single_join() {
        let query = generate_query(&[QueryOperation::Join(JoinOperator::LeftJoin)]);
        eprintln!("query: {}", query);
        assert_eq!(query.tables.len(), 1);
        assert_eq!(query.join.len(), 1);
        let join = query.join.first().unwrap();
        match &join.constraint {
            JoinConstraint::On(ConditionExpression::ComparisonOp(ConditionTree {
                operator,
                left,
                right,
            })) => {
                assert_eq!(operator, &BinaryOperator::Equal);
                match (left.as_ref(), right.as_ref()) {
                    (
                        ConditionExpression::Base(ConditionBase::Field(left_field)),
                        ConditionExpression::Base(ConditionBase::Field(right_field)),
                    ) => {
                        assert_eq!(
                            left_field.table.as_ref(),
                            Some(&query.tables.first().unwrap().name)
                        );
                        assert_eq!(
                            right_field.table.as_ref(),
                            Some(match &join.right {
                                JoinRightSide::Table(table) => &table.name,
                                _ => unreachable!(),
                            })
                        );
                    }
                    _ => unreachable!(),
                }
            }
            constraint => unreachable!("Unexpected constraint: {:?}", constraint),
        }
    }
}
