#![feature(or_insert_with_key)]

use chrono::NaiveDate;
use derive_more::{Display, From, Into};
use itertools::Itertools;
use lazy_static::lazy_static;
use noria::{DataType, KeyComparison};
use serde::{Deserialize, Serialize};
use std::borrow::Borrow;
use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::iter;
use strum::IntoEnumIterator;
use strum_macros::EnumIter;

use nom_sql::{
    BinaryOperator, Column, ColumnSpecification, ConditionBase, ConditionExpression, ConditionTree,
    CreateTableStatement, FieldDefinitionExpression, FieldValueExpression, FunctionArgument,
    FunctionExpression, ItemPlaceholder, JoinClause, JoinConstraint, JoinOperator, JoinRightSide,
    Literal, LiteralExpression, SelectStatement, SqlType, Table,
};

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
        SqlType::Date => unimplemented!(),
        SqlType::Enum(_) => unimplemented!(),
        SqlType::Bool => unimplemented!(),
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

    pub fn fresh_column(&mut self) -> ColumnName {
        self.fresh_column_with_type(SqlType::Int(32))
    }

    pub fn fresh_column_with_type(&mut self, col_type: SqlType) -> ColumnName {
        self.column_name_counter += 1;
        let column_name = ColumnName(format!("column_{}", self.column_name_counter));
        self.columns.insert(column_name.clone(), col_type);
        column_name
    }

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
    pub fn fresh_table_mut(&mut self) -> &mut TableSpec {
        self.table_name_counter += 1;
        let table_name = TableName(format!("table_{}", self.table_name_counter));
        self.tables
            .entry(table_name)
            .or_insert_with_key(|tn| TableSpec::new(tn.clone()))
    }

    pub fn table_mut<'a, TN>(&'a mut self, name: &TN) -> &'a mut TableSpec
    where
        TableName: Borrow<TN>,
        TN: Eq + Hash,
    {
        self.tables.get_mut(name).unwrap()
    }

    pub fn table_names(&self) -> impl Iterator<Item = &TableName> {
        self.tables.keys()
    }

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

    pub fn generate_query<'a, I>(&mut self, operations: I) -> SelectStatement
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
        query
    }

    pub fn generate_queries(
        &mut self,
        max_depth: usize,
    ) -> impl Iterator<Item = SelectStatement> + '_ {
        QueryOperation::permute(max_depth).map(move |ops| self.generate_query(ops))
    }

    pub fn into_ddl(self) -> impl Iterator<Item = CreateTableStatement> {
        self.tables.into_iter().map(|(_, tbl)| tbl.into())
    }

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
    alias_counter: u32,
}

impl<'a> QueryState<'a> {
    pub fn new(gen: &'a mut GeneratorState) -> Self {
        Self {
            gen,
            tables: HashSet::new(),
            parameters: Vec::new(),
            alias_counter: 0,
        }
    }

    pub fn fresh_alias(&mut self) -> String {
        self.alias_counter += 1;
        format!("alias_{}", self.alias_counter)
    }

    pub fn some_table_mut(&mut self) -> &mut TableSpec {
        let table = self.gen.some_table_mut();
        self.tables.insert(table.name.clone());
        table
    }

    pub fn fresh_table_mut(&mut self) -> &mut TableSpec {
        let table = self.gen.fresh_table_mut();
        self.tables.insert(table.name.clone());
        table
    }

    /// Generate `rows_per_table` rows of data for all the tables referenced in the query for this
    /// QueryState
    pub fn generate_data(
        &self,
        rows_per_table: usize,
    ) -> HashMap<&TableName, Vec<HashMap<&ColumnName, DataType>>> {
        self.tables
            .iter()
            .map(|table_name| {
                let rows = self.gen.generate_data_for_table(table_name, rows_per_table);
                (table_name, rows)
            })
            .collect()
    }

    /// Record a new (positional) parameter for the query, comparing against the given column of the
    /// given table
    pub fn add_parameter(&mut self, table_name: TableName, column_name: ColumnName) {
        self.parameters.push((table_name, column_name))
    }

    /// Returns a lookup key for the parameters in the query that will return results
    pub fn key(&self) -> KeyComparison {
        self.parameters
            .iter()
            .map(|(table_name, column_name)| {
                value_of_type(&self.gen.tables[table_name].columns[column_name])
            })
            .collect()
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
    operator: BinaryOperator,
    rhs: FilterRHS,
    extend_where_with: LogicalOp,
}

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
    static ref ALL_OPERATIONS: Vec<QueryOperation> = {
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

                query
                    .fields
                    .push(FieldDefinitionExpression::Col(col.into()));

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
                let col = table.some_column_name();
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

    pub fn permute(max_depth: usize) -> impl Iterator<Item = Vec<&'static QueryOperation>> {
        (1..=max_depth).flat_map(|depth| ALL_OPERATIONS.iter().combinations(depth))
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
        gen.generate_query(ops)
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
