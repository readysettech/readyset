use std::cmp::Ordering;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::convert::TryFrom;
use std::hash::{Hash, Hasher};
use std::vec::Vec;
use std::{iter, mem};

use common::{DfValue, IndexType};
use nom_sql::analysis::visit_mut::{walk_expr, VisitorMut};
use nom_sql::analysis::ReferredColumns;
use nom_sql::{
    BinaryOperator, Column, DialectDisplay, Expr, FieldDefinitionExpr, FieldReference,
    FunctionExpr, InValue, ItemPlaceholder, JoinConstraint, JoinOperator, JoinRightSide,
    LimitClause, Literal, OrderBy, OrderType, Relation, SelectStatement, SqlIdentifier, TableExpr,
    TableExprInner,
};
use readyset_client::{PlaceholderIdx, ViewPlaceholder};
use readyset_errors::{
    internal, invalid_query, invalid_query_err, invariant, invariant_eq, no_table_for_col,
    unsupported, unsupported_err, ReadySetResult,
};
use readyset_sql_passes::{is_aggregate, is_correlated, is_predicate, map_aggregates, LogicalOp};
use serde::{Deserialize, Serialize};

use super::mir::{self, PAGE_NUMBER_COL};

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct LiteralColumn {
    pub name: SqlIdentifier,
    pub table: Option<Relation>,
    pub value: Literal,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct ExprColumn {
    pub name: SqlIdentifier,
    pub table: Option<Relation>,
    pub expression: Expr,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum OutputColumn {
    Data {
        alias: SqlIdentifier,
        column: Column,
    },
    Literal(LiteralColumn),
    Expr(ExprColumn),
}

impl OutputColumn {
    /// Returns the final projected name for this [`OutputColumn`]
    pub fn name(&self) -> &str {
        match self {
            OutputColumn::Data { alias, .. } => alias,
            OutputColumn::Literal(LiteralColumn { name, .. }) => name,
            OutputColumn::Expr(ExprColumn { name, .. }) => name,
        }
    }

    /// Convert this [`OutputColumn`] into an [`Expr`], consuming it
    pub fn into_expr(self) -> Expr {
        match self {
            OutputColumn::Data { column, .. } => Expr::Column(column),
            OutputColumn::Literal(LiteralColumn { value, .. }) => Expr::Literal(value),
            OutputColumn::Expr(ExprColumn { expression, .. }) => expression,
        }
    }
}

impl Ord for OutputColumn {
    fn cmp(&self, other: &OutputColumn) -> Ordering {
        match *self {
            OutputColumn::Expr(ExprColumn {
                ref name,
                ref table,
                ..
            })
            | OutputColumn::Data {
                column:
                    Column {
                        ref name,
                        ref table,
                        ..
                    },
                ..
            }
            | OutputColumn::Literal(LiteralColumn {
                ref name,
                ref table,
                ..
            }) => match *other {
                OutputColumn::Expr(ExprColumn {
                    name: ref other_name,
                    table: ref other_table,
                    ..
                })
                | OutputColumn::Data {
                    column:
                        Column {
                            name: ref other_name,
                            table: ref other_table,
                            ..
                        },
                    ..
                }
                | OutputColumn::Literal(LiteralColumn {
                    name: ref other_name,
                    table: ref other_table,
                    ..
                }) => {
                    if table.is_some() && other_table.is_some() {
                        match table.cmp(other_table) {
                            Ordering::Equal => name.cmp(other_name),
                            x => x,
                        }
                    } else {
                        name.cmp(other_name)
                    }
                }
            },
        }
    }
}

impl PartialOrd for OutputColumn {
    fn partial_cmp(&self, other: &OutputColumn) -> Option<Ordering> {
        match *self {
            OutputColumn::Expr(ExprColumn {
                ref name,
                ref table,
                ..
            })
            | OutputColumn::Data {
                column:
                    Column {
                        ref name,
                        ref table,
                        ..
                    },
                ..
            }
            | OutputColumn::Literal(LiteralColumn {
                ref name,
                ref table,
                ..
            }) => match *other {
                OutputColumn::Expr(ExprColumn {
                    name: ref other_name,
                    table: ref other_table,
                    ..
                })
                | OutputColumn::Data {
                    column:
                        Column {
                            name: ref other_name,
                            table: ref other_table,
                            ..
                        },
                    ..
                }
                | OutputColumn::Literal(LiteralColumn {
                    name: ref other_name,
                    table: ref other_table,
                    ..
                }) => {
                    if table.is_some() && other_table.is_some() {
                        match table.cmp(other_table) {
                            Ordering::Equal => Some(name.cmp(other_name)),
                            x => Some(x),
                        }
                    } else if table.is_none() && other_table.is_none() {
                        Some(name.cmp(other_name))
                    } else {
                        None
                    }
                }
            },
        }
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct JoinRef {
    pub src: Relation,
    pub dst: Relation,
}

/// An equality predicate on two columns, used as the key for a join
#[derive(Clone, Debug, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct JoinPredicate {
    pub left: Column,
    pub right: Column,
}

/// An individual column on which a query is parameterized
#[derive(Clone, Debug, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct Parameter {
    pub col: Column,
    pub op: nom_sql::BinaryOperator,
    pub placeholder_idx: Option<PlaceholderIdx>,
}

#[derive(Clone, Debug, Hash, PartialEq, Serialize, Deserialize)]
pub struct QueryGraphNode {
    pub relation: Relation,
    pub predicates: Vec<Expr>,
    pub columns: Vec<Column>,
    pub parameters: Vec<Parameter>,
    /// If this query graph relation refers to a subquery, the graph of that subquery and the AST
    /// for the query itself
    pub subgraph: Option<Box<QueryGraph>>,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub enum QueryGraphEdge {
    Join {
        on: Vec<JoinPredicate>,
    },
    LeftJoin {
        on: Vec<JoinPredicate>,
        /// Predicates which are local to the left-hand side of the left join.
        left_local_preds: Vec<Expr>,
        /// Predicates which are local to the right-hand side of the left join.
        right_local_preds: Vec<Expr>,
        /// Global predicates mentioned in the ON clause of the join
        global_preds: Vec<Expr>,
        /// Parameters mentioned in the ON clause of the join
        params: Vec<Parameter>,
    },
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct Pagination {
    pub order: Option<Vec<(Expr, OrderType)>>,
    pub limit: usize,
    pub offset: Option<ViewPlaceholder>,
}

/// Description of the lookup key for a view
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ViewKey {
    /// The list of key columns for the view, and for each column a description of how that column
    /// maps back to a placeholder in the original query, if at all
    pub columns: Vec<(mir::Column, ViewPlaceholder)>,

    /// The selected index type for the view
    pub index_type: IndexType,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
// NOTE: Keep in mind this struct has a custom Hash impl - when changing it, remember to update that
// as well!
// TODO(aspen): impl Arbitrary for this struct so we can make a proptest for that
pub struct QueryGraph {
    /// Relations mentioned in the query.
    pub relations: HashMap<Relation, QueryGraphNode>,
    /// Joins in the query.
    #[serde(with = "serde_with::rust::hashmap_as_tuple_list")]
    pub edges: HashMap<(Relation, Relation), QueryGraphEdge>,
    /// Whether the query has a `DISTINCT` in the `SELECT` clause
    pub distinct: bool,
    /// Aggregates in the query, represented as a map from the aggregate function to the alias for
    /// that aggregate function
    ///
    /// If a single aggregate is projected as multiple aliases, only one will appear in this map,
    /// but both will appear in `self.columns` as [`OutputColumn::Data`] referencing that alias
    pub aggregates: HashMap<FunctionExpr, SqlIdentifier>,
    /// Set of expressions that appear in the GROUP BY clause
    pub group_by: HashSet<Expr>,
    /// Final set of projected columns in this query; may include literals in addition to the
    /// columns reflected in individual relations' `QueryGraphNode` structures.
    pub columns: Vec<OutputColumn>,
    /// Fields projected by the original query
    // TODO: Sort out the difference between this and `columns`
    pub fields: Vec<FieldDefinitionExpr>,
    /// Row of default values to return on an empty result set, for example if we're aggregating
    /// and no rows are found
    pub default_row: Option<Vec<DfValue>>,
    /// Establishes an order for join predicates. Each join predicate can be identified by
    /// its (src, dst) pair
    pub join_order: Vec<JoinRef>,
    /// Global predicates (not associated with a particular relation)
    pub global_predicates: Vec<Expr>,
    /// HAVING predicates (like global predicates, but applied after aggregate functions)
    pub having_predicates: Vec<Expr>,
    /// The list of columns and directionsj that the query is ordering by, if any
    pub order: Option<Vec<(Column, OrderType)>>,
    /// The pagination (order, limit, offset) for the query, if any
    pub pagination: Option<Pagination>,
    /// True if the query is correlated (is a subquery that refers to columns in an outer query)
    pub is_correlated: bool,
}

impl QueryGraph {
    /// Returns the set of columns on which this query is parametrized. They can come from
    /// multiple tables involved in the query.
    /// Does not include limit or offset parameters on which this query may be parametrized.
    pub fn parameters(&self) -> Vec<&Parameter> {
        self.relations
            .values()
            .flat_map(|qgn| qgn.parameters.iter())
            .collect()
    }

    /// Construct a representation of the lookup key of a view for this query graph, based on the
    /// parameters in this query and the page number if this query is parametrized on an offset key.
    pub(crate) fn view_key(&self, config: &mir::Config) -> ReadySetResult<ViewKey> {
        let offset = self.pagination.as_ref().and_then(|p| p.offset);
        if self.parameters().is_empty() {
            if let Some(offset) = offset {
                Ok(ViewKey {
                    columns: vec![(mir::Column::named(PAGE_NUMBER_COL.clone()), offset)],
                    index_type: IndexType::HashMap,
                })
            } else {
                Ok(ViewKey {
                    columns: vec![],
                    index_type: IndexType::HashMap,
                })
            }
        } else {
            let mut parameters = self.parameters();

            // Sort the parameters to put equal comparisons first, to take advantage of
            // lexicographic key ordering for queries that mix equality and range comparisons
            parameters.sort_by(|param1, param2| {
                match (param1.op, param2.op) {
                    // All equal operators go first
                    (BinaryOperator::Equal, _) => Ordering::Less,
                    (_, BinaryOperator::Equal) => Ordering::Greater,
                    // sort_by is stable, so if we return Equal we just leave things
                    // in the same order
                    (_, _) => Ordering::Equal,
                }
                // then sort by column, so that later when we iterate parameters with the same
                // column but different comparisons are adjacent, allowing us to detect
                // BETWEEN-style comparisons
                .then_with(|| param1.col.cmp(&param2.col))
            });

            /// Given the current IndexType and the next operator in the query, resolve the
            /// IndexType for the query
            fn resolve_index_type(
                current_index_type: Option<IndexType>,
                operator: BinaryOperator,
                config: &mir::Config,
            ) -> ReadySetResult<Option<IndexType>> {
                let new_index_type = Some(IndexType::for_operator(operator).ok_or_else(|| {
                    unsupported_err!("Unsupported binary operator `{}`", operator)
                })?);
                if !config.allow_mixed_comparisons
                    && current_index_type.is_some()
                    && current_index_type != new_index_type
                {
                    unsupported!("Conflicting binary operators in query");
                } else {
                    // TODO: resolve index type rather than overwriting the old value
                    Ok(new_index_type)
                }
            }

            /// Combine a `ViewPlaceholder` generated from a column-placeholder comparison and a new
            /// `Parameter`.
            ///
            /// This function should only be called with a `ViewPlaceholder` and `Parameter` that
            /// refer to the same column.
            fn combine_comparisons(
                current_view_placeholder: &ViewPlaceholder,
                new_param: &Parameter,
            ) -> ReadySetResult<ViewPlaceholder> {
                match (current_view_placeholder, new_param) {
                    (
                        ViewPlaceholder::OneToOne(idx, BinaryOperator::GreaterOrEqual),
                        Parameter {
                            op: BinaryOperator::LessOrEqual,
                            placeholder_idx: Some(ref placeholder_idx),
                            ..
                        },
                    ) => Ok(ViewPlaceholder::Between(*idx, *placeholder_idx)),
                    (
                        ViewPlaceholder::OneToOne(idx, BinaryOperator::LessOrEqual),
                        Parameter {
                            op: BinaryOperator::GreaterOrEqual,
                            placeholder_idx: Some(ref placeholder_idx),
                            ..
                        },
                    ) => Ok(ViewPlaceholder::Between(*placeholder_idx, *idx)),
                    _ => unsupported!("Conflicting binary operators in query"),
                }
            }

            let (index_type, mut columns) = parameters.into_iter().try_fold(
                (None, vec![]),
                |(index_type, mut columns), param| -> ReadySetResult<_> {
                    let index_type = resolve_index_type(index_type, param.op, config)?;
                    match columns.last_mut() {
                        // If the last two columns match and have different operators
                        Some((col, placeholder))
                            if *col == param.col
                                && matches!(placeholder, ViewPlaceholder::OneToOne(_, ref op) if *op != param.op) =>
                        {
                            *placeholder = combine_comparisons(placeholder, param)?;
                            Ok((index_type, columns))
                        }
                        // Otherwise, add a new ViewPlaceholder and continue
                        _ => {
                            columns.push((
                                mir::Column::from(param.col.clone()),
                                param
                                    .placeholder_idx
                                    .map(|idx| ViewPlaceholder::OneToOne(idx, param.op))
                                    .unwrap_or(ViewPlaceholder::Generated),
                            ));
                            Ok((index_type, columns))
                        }
                    }
                },
            )?;

            if let Some(offset) = offset {
                if index_type == Some(IndexType::BTreeMap) {
                    unsupported!("ReadySet does not support Pagination and range queries")
                } else {
                    columns.push((mir::Column::named(PAGE_NUMBER_COL.clone()), offset));
                }
            }

            #[allow(clippy::expect_used)]
            Ok(ViewKey {
                columns,
                index_type: index_type.expect("Checked self.parameters() isn't empty above"),
            })
        }
    }
}

#[allow(clippy::derived_hash_with_manual_eq)]
impl Hash for QueryGraph {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // sorted iteration over relations, edges to ensure consistent hash
        let mut rels: Vec<(&Relation, &QueryGraphNode)> = self.relations.iter().collect();
        rels.sort_by(|a, b| a.0.cmp(b.0));
        rels.hash(state);
        let mut edges: Vec<(&(Relation, Relation), &QueryGraphEdge)> = self.edges.iter().collect();
        edges.sort_by(|(a, _), (b, _)| match a.0.cmp(&b.0) {
            Ordering::Equal => a.1.cmp(&b.1),
            x => x,
        });
        edges.hash(state);

        self.distinct.hash(state);

        let mut group_by = self.group_by.iter().collect::<Vec<_>>();
        group_by.sort();
        group_by.hash(state);

        let mut aggregates = self.aggregates.iter().collect::<Vec<_>>();
        aggregates.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
        aggregates.hash(state);

        // these fields are Vecs, so already ordered
        self.columns.hash(state);
        self.fields.hash(state);
        self.default_row.hash(state);
        self.join_order.hash(state);
        self.global_predicates.hash(state);
        self.having_predicates.hash(state);
        self.order.hash(state);
        self.pagination.hash(state);
        self.is_correlated.hash(state);
    }
}

/// Splits top level conjunctions into multiple predicates
fn split_conjunctions<'a, T>(ces: T) -> Vec<Expr>
where
    T: Iterator<Item = &'a Expr>,
{
    let mut new_ces = Vec::new();
    for ce in ces {
        match ce {
            Expr::BinaryOp {
                op: BinaryOperator::And,
                lhs,
                rhs,
            } => {
                new_ces.extend(split_conjunctions(iter::once(&**lhs)));
                new_ces.extend(split_conjunctions(iter::once(&**rhs)));
            }
            _ => {
                new_ces.push(ce.clone());
            }
        }
    }

    new_ces
}

// 1. Extract any predicates with placeholder parameters. We push these down to the edge nodes,
//    since we cannot instantiate the parameters inside the data flow graph (except for
//    non-materialized nodes).
// 2. Extract local predicates
// 3. Collect remaining predicates as global predicates
fn classify_conditionals(
    ce: &Expr,
    local: &mut HashMap<Relation, Vec<Expr>>,
    global: &mut Vec<Expr>,
    params: &mut Vec<Parameter>,
) -> ReadySetResult<()> {
    // Handling OR and AND expressions requires some care as there are some corner cases.
    //    a) we don't support OR expressions with predicates with placeholder parameters,
    //       because these expressions are meaningless in the Soup context.
    //    b) we don't support OR expressions with join predicates because they are weird and
    //       too hard.
    //    c) we don't support OR expressions between different tables (e.g table1.x = 1 OR
    //       table2.y= 42). this is a global predicate according to finkelstein algorithm
    //       and we don't support these yet.

    match ce {
        Expr::BinaryOp { op, lhs, rhs } => {
            if let Ok(op) = LogicalOp::try_from(*op) {
                // first, we recurse on both sides, collected the result of nested predicate
                // analysis in separate collections. What do do with these depends
                // on whether we're an AND or an OR clause:
                //  1) AND can be split into separate local predicates one one or more tables
                //  2) OR predictes must be preserved in their entirety, and we only use the nested
                //     local predicates discovered to decide if the OR is over one table (so it can
                //     remain a local predicate) or over several (so it must be a global predicate)
                let mut new_params = Vec::new();
                let mut new_local = HashMap::new();
                let mut new_global = Vec::new();

                classify_conditionals(
                    lhs.as_ref(),
                    &mut new_local,
                    &mut new_global,
                    &mut new_params,
                )?;
                classify_conditionals(
                    rhs.as_ref(),
                    &mut new_local,
                    &mut new_global,
                    &mut new_params,
                )?;

                match op {
                    LogicalOp::And => {
                        //
                        for (t, ces) in new_local {
                            // conjunction, check if either side had a local predicate
                            invariant!(
                                ces.len() <= 2,
                                "can only combine two or fewer ConditionExpr's"
                            );
                            if ces.len() == 2 {
                                #[allow(clippy::unwrap_used)] // checked ces.len() first
                                let new_ce = Expr::BinaryOp {
                                    op: BinaryOperator::And,
                                    lhs: Box::new(ces.first().unwrap().clone()),
                                    rhs: Box::new(ces.last().unwrap().clone()),
                                };

                                let e = local.entry(t).or_default();
                                e.push(new_ce);
                            } else {
                                let e = local.entry(t).or_default();
                                e.extend(ces);
                            }
                        }

                        // one side of the AND might be a global predicate, so we need to keep
                        // new_global around
                        global.extend(new_global);
                    }
                    LogicalOp::Or => {
                        if !new_params.is_empty() {
                            unsupported!(
                                "can't handle OR expressions between query parameter predicates"
                            );
                        }
                        if new_local.keys().len() == 1 && new_global.is_empty() {
                            // OR over a single table => local predicate
                            // just checked that new_local has one entry
                            #[allow(clippy::unwrap_used)]
                            let (t, ces) = new_local.into_iter().next().unwrap();
                            if ces.len() != 2 {
                                unsupported!("should combine only 2 ConditionExpr's");
                            }
                            #[allow(clippy::unwrap_used)] // ces.len() == 2
                            let new_ce = Expr::BinaryOp {
                                lhs: Box::new(ces.first().unwrap().clone()),
                                op: BinaryOperator::Or,
                                rhs: Box::new(ces.last().unwrap().clone()),
                            };

                            let e = local.entry(t).or_default();
                            e.push(new_ce);
                        } else {
                            // OR between different tables => global predicate
                            global.push(ce.clone())
                        }
                    }
                }

                params.extend(new_params);
            } else if is_predicate(op) {
                // atomic selection predicate
                if let Expr::Literal(Literal::Placeholder(ref placeholder)) = **rhs {
                    if let Expr::Column(ref lf) = **lhs {
                        let idx = match placeholder {
                            ItemPlaceholder::DollarNumber(idx) => Some(*idx as usize),
                            _ => None,
                        };
                        params.push(Parameter {
                            col: lf.clone(),
                            op: *op,
                            placeholder_idx: idx,
                        });
                    }
                } else if let Expr::Column(Column {
                    table: Some(table), ..
                }) = &**lhs
                {
                    local.entry(table.clone()).or_default().push(ce.clone());
                } else {
                    // comparisons between computed columns and literals are global
                    // predicates
                    global.push(ce.clone());
                }
            } else {
                unsupported!("Arithmetic not supported here")
            }
        }
        Expr::In {
            lhs,
            rhs: InValue::List(rhs),
            ..
        } => {
            let tables = lhs
                .referred_columns()
                .chain(rhs.iter().flat_map(|expr| expr.referred_columns()))
                .flat_map(|col| &col.table)
                .collect::<HashSet<_>>();
            let num_tables = tables.len();
            match tables.into_iter().next() {
                // TODO(aspen): This limitation probably isn't too hard to lift
                None => {
                    unsupported!("Filter conditions must currently mention at least one column")
                }
                Some(table) if num_tables == 1 => {
                    // only one table mentioned, so local
                    local.entry(table.clone()).or_default().push(ce.clone())
                }
                _ => {
                    // more than 1 table mentioned, so must be a global predicate
                    global.push(ce.clone())
                }
            }
        }
        Expr::Exists(_) => {
            // TODO(aspen): Look into the query for correlated references to see if it's actually a
            // local predicate in disguise
            global.push(ce.clone())
        }
        Expr::Between { .. } => {
            internal!("Between should have been removed earlier")
        }
        Expr::In {
            rhs: InValue::Subquery(..),
            ..
        }
        | Expr::Call(_)
        | Expr::Literal(_)
        | Expr::UnaryOp { .. }
        | Expr::OpAny { .. }
        | Expr::OpSome { .. }
        | Expr::OpAll { .. }
        | Expr::CaseWhen { .. }
        | Expr::Column(_)
        | Expr::NestedSelect(_)
        | Expr::Cast { .. }
        | Expr::Array(_)
        | Expr::Row { .. }
        | Expr::Variable(_) => global.push(ce.clone()),
    }
    Ok(())
}

/// Convert the given `Expr`, which should be a set of AND-ed together direct
/// comparison predicates, into a list of predicate expressions
fn collect_join_predicates(
    cond: Expr,
    join_preds: &mut Vec<JoinPredicate>,
    extra_preds: &mut Vec<Expr>,
) {
    match cond {
        Expr::BinaryOp {
            op: BinaryOperator::Equal,
            lhs: box Expr::Column(left),
            rhs: box Expr::Column(right),
        } => {
            join_preds.push(JoinPredicate { left, right });
        }
        Expr::BinaryOp {
            lhs,
            op: BinaryOperator::And,
            rhs,
        } => {
            collect_join_predicates(*lhs, join_preds, extra_preds);
            collect_join_predicates(*rhs, join_preds, extra_preds);
        }
        _ => {
            extra_preds.push(cond);
        }
    }
}

/// Processes the provided HAVING expression by extracting aggregates, splitting predicates, and
/// replacing aggregates in predicates with column references.
///
/// Note that `aggregates` is an out parameter; the return value of the function is the modified
/// predicate Expr values, and the extracted aggregates are saved separately in the `aggregates`
/// map.
fn extract_having_aggregates(
    having_expr: &Expr,
    aggregates: &mut HashMap<FunctionExpr, SqlIdentifier>,
) -> Vec<Expr> {
    let mut having_predicates = split_conjunctions(iter::once(having_expr));

    #[derive(Default)]
    struct AggregateFinder {
        result: Vec<(FunctionExpr, SqlIdentifier)>,
    }

    impl<'ast> VisitorMut<'ast> for AggregateFinder {
        type Error = !;

        fn visit_expr(&mut self, expr: &'ast mut Expr) -> Result<(), Self::Error> {
            if matches!(expr, Expr::Call(fun) if is_aggregate(fun)) {
                // FIXME(REA-2168): Use correct dialect.
                let name: SqlIdentifier = expr.display(nom_sql::Dialect::MySQL).to_string().into();
                let col_expr = Expr::Column(nom_sql::Column {
                    name: name.clone(),
                    table: None,
                });
                let agg_expr = mem::replace(expr, col_expr);
                let Expr::Call(fun) = agg_expr else {
                    unreachable!("Checked matches above")
                };
                self.result.push((fun, name));
                Ok(())
            } else {
                walk_expr(self, expr)
            }
        }

        fn visit_select_statement(
            &mut self,
            _: &'ast mut SelectStatement,
        ) -> Result<(), Self::Error> {
            // Don't walk into subqueries
            Ok(())
        }
    }

    let mut af = AggregateFinder::default();
    for pred in having_predicates.iter_mut() {
        let _ = af.visit_expr(pred);
    }
    aggregates.extend(af.result);

    having_predicates
}

/// Convert limit and offset fields to an optional constant numeric limit and optional placeholder
/// for the offset
pub(crate) fn extract_limit_offset(
    limit_clause: &LimitClause,
) -> ReadySetResult<Option<(usize, Option<ViewPlaceholder>)>> {
    if limit_clause.limit().is_none() && limit_clause.offset().is_some() {
        unsupported!("ReadySet does not support OFFSET without LIMIT");
    }

    let limit = if let Some(limit) = limit_clause.limit() {
        limit
    } else {
        return Ok(None);
    };

    let limit = match limit {
        Literal::UnsignedInteger(val) => *val,
        Literal::Integer(val) => u64::try_from(*val)
            .map_err(|_| unsupported_err!("LIMIT field cannot have a negative value"))?,
        Literal::Placeholder(_) => {
            unsupported!("ReadySet does not support parametrized LIMIT fields")
        }
        _ => unsupported!("Invalid LIMIT statement"),
    };

    let offset = limit_clause
        .offset()
        .as_ref()
        // For now, remove offset if it is a literal 0
        .filter(|offset| !matches!(offset, Literal::Integer(0)))
        .map(|offset| -> ReadySetResult<ViewPlaceholder> {
            match offset {
                Literal::Placeholder(ItemPlaceholder::DollarNumber(idx)) => {
                    Ok(ViewPlaceholder::PageNumber {
                        offset_placeholder: *idx as _,
                        limit,
                    })
                }
                _ => unsupported!("Numeric OFFSETs must be parametrized"),
            }
        })
        .transpose()?;

    Ok(Some((limit as _, offset)))
}

fn table_expr_name(table_expr: &TableExpr) -> ReadySetResult<Relation> {
    match &table_expr.inner {
        TableExprInner::Table(t) => Ok(t.clone()),
        TableExprInner::Subquery(_) => Ok(table_expr
            .alias
            .as_ref()
            .ok_or_else(|| invalid_query_err!("All subqueries must have an alias"))?
            .clone()
            .into()),
    }
}

fn default_row_for_select(st: &SelectStatement) -> Option<Vec<DfValue>> {
    // If this is an aggregated query AND it does not contain a GROUP BY clause,
    // set default values based on the aggregation (or lack thereof) on each
    // individual field
    if !st.contains_aggregate_select() || st.group_by.is_some() {
        return None;
    }
    Some(
        st.fields
            .iter()
            .map(|f| match f {
                FieldDefinitionExpr::Expr {
                    expr: Expr::Call(func),
                    ..
                } => match func {
                    FunctionExpr::Avg { .. } => DfValue::None,
                    FunctionExpr::Count { .. } => DfValue::Int(0),
                    FunctionExpr::CountStar => DfValue::Int(0),
                    FunctionExpr::Sum { .. } => DfValue::None,
                    FunctionExpr::Max(..) => DfValue::None,
                    FunctionExpr::Min(..) => DfValue::None,
                    FunctionExpr::GroupConcat { .. } => DfValue::None,
                    FunctionExpr::Call { .. } | FunctionExpr::Substring { .. } => DfValue::None,
                },
                _ => DfValue::None,
            })
            .collect(),
    )
}

#[allow(clippy::cognitive_complexity)]
pub fn to_query_graph(stmt: SelectStatement) -> ReadySetResult<QueryGraph> {
    // a handy closure for making new relation nodes
    let new_node = |rel: Relation,
                    preds: Vec<Expr>,
                    fields: &Vec<FieldDefinitionExpr>|
     -> ReadySetResult<QueryGraphNode> {
        Ok(QueryGraphNode {
            relation: rel.clone(),
            predicates: preds,
            columns: fields
                .iter()
                .map(|field| {
                    Ok(match field {
                        // unreachable because SQL rewrite passes will have expanded these
                        // already
                        FieldDefinitionExpr::All => {
                            internal!("* should have been expanded already")
                        }
                        FieldDefinitionExpr::AllInTable(_) => {
                            internal!("<table>.* should have been expanded already")
                        }
                        FieldDefinitionExpr::Expr {
                            expr: Expr::Column(c),
                            ..
                        } => c.table.as_ref().and_then(|t| {
                            if rel == *t {
                                Some(c.clone())
                            } else {
                                None
                            }
                        }),
                        FieldDefinitionExpr::Expr { .. } => {
                            // No need to do anything for expressions here, since they aren't
                            // associated with a relation (and thus have
                            // no QGN) XXX(malte): don't drop
                            // aggregation columns
                            None
                        }
                    })
                })
                // FIXME(eta): error handling overhead
                .collect::<ReadySetResult<Vec<_>>>()?
                .into_iter()
                .flatten()
                .collect(),
            parameters: Vec::new(),
            subgraph: None,
        })
    };

    let default_row = default_row_for_select(&stmt);
    let is_correlated = is_correlated(&stmt);

    // Used later on to determine whether to classify predicates as "join predicates" or not
    let mut inner_join_rels = HashSet::new();

    // 1. Add any relations mentioned in the query to the query graph.
    // This is needed so that we don't end up with an empty query graph when there are no
    // conditionals, but rather with a one-node query graph that has no predicates.

    let mut relations = HashMap::new();
    let mut add_table_expr = |table_expr: &TableExpr| -> ReadySetResult<Relation> {
        match &table_expr.inner {
            TableExprInner::Table(t) => {
                if relations
                    .insert(t.clone(), new_node(t.clone(), vec![], &stmt.fields)?)
                    .is_some()
                {
                    invalid_query!(
                        "Table name {} specified more than once",
                        t.display_unquoted()
                    );
                };
                Ok(t.clone())
            }
            TableExprInner::Subquery(sq) => {
                let rel = Relation::from(
                    table_expr
                        .alias
                        .as_ref()
                        .ok_or_else(|| invalid_query_err!("All subqueries must have an alias"))?
                        .clone(),
                );
                if let Entry::Vacant(e) = relations.entry(rel.clone()) {
                    let mut node = new_node(rel.clone(), vec![], &stmt.fields)?;
                    node.subgraph = Some(Box::new(to_query_graph((**sq).clone())?));
                    e.insert(node);
                } else {
                    invalid_query!(
                        "Table name {} specified more than once",
                        rel.display_unquoted()
                    );
                }

                Ok(rel)
            }
        }
    };

    for table_expr in stmt.tables.iter() {
        let rel = add_table_expr(table_expr)?;
        inner_join_rels.insert(rel);
    }

    for jc in &stmt.join {
        match &jc.right {
            JoinRightSide::Table(table_expr) => {
                let rel = add_table_expr(table_expr)?;
                if jc.operator.is_inner_join() {
                    inner_join_rels.insert(rel);
                }
            }
            JoinRightSide::Tables(_) => unsupported!("JoinRightSide::Tables not yet implemented"),
        };
    }

    // 2. Add edges for each pair of joined relations. Note that we must keep track of the join
    //    predicates here already, but more may be added when processing the WHERE clause lateron.

    let mut edges = HashMap::new();

    // 2a. Explicit joins

    let mut local_predicates = HashMap::new();
    let mut global_predicates = Vec::new();
    let mut query_parameters = Vec::new();

    // The table specified in the query is available for USING joins.
    let prev_table =
        table_expr_name(stmt.tables.last().ok_or_else(|| {
            unsupported_err!("SELECT statements with no tables are unsupported")
        })?)?;

    for jc in stmt.join {
        let rhs_relation = match jc.right {
            JoinRightSide::Table(te) => table_expr_name(&te)?,
            JoinRightSide::Tables(_) => unsupported!("JoinRightSide::Tables not yet implemented"),
        };
        // will be defined by join constraint
        let left_table;
        let right_table;

        let (on, extra_preds) = match jc.constraint {
            JoinConstraint::On(cond) => {
                use nom_sql::analysis::ReferredTables;

                // find all distinct tables mentioned in the condition
                // conditions for now.
                let mut tables_mentioned: Vec<Relation> =
                    cond.referred_tables().into_iter().collect();

                let mut join_preds = vec![];
                let mut extra_preds = vec![];
                collect_join_predicates(cond, &mut join_preds, &mut extra_preds);

                if tables_mentioned.len() == 2 {
                    // tables can appear in any order in the join predicate, but
                    // we cannot just rely on that order, since it may lead us to
                    // flip LEFT JOINs by accident (yes, this happened)
                    #[allow(clippy::indexing_slicing)] // len() == 2
                    if tables_mentioned[1] != rhs_relation {
                        // tables are in the wrong order in join predicate, swap
                        tables_mentioned.swap(0, 1);
                        invariant_eq!(tables_mentioned[1], rhs_relation);
                    }
                    left_table = tables_mentioned.remove(0);
                    right_table = tables_mentioned.remove(0);
                } else if tables_mentioned.len() == 1 {
                    // just one table mentioned --> this is a self-join
                    left_table = tables_mentioned.remove(0);
                    right_table = left_table.clone();
                } else {
                    unsupported!("more than 2 tables mentioned in join condition!");
                };

                for pred in join_preds.iter_mut() {
                    // the condition tree might specify tables in opposite order to
                    // their join order in the query; if so, flip them
                    // TODO(malte): this only deals with simple, flat join
                    // conditions for now.
                    if *pred.left.table.as_ref().ok_or_else(|| no_table_for_col())? == right_table
                        && *pred
                            .right
                            .table
                            .as_ref()
                            .ok_or_else(|| no_table_for_col())?
                            == left_table
                    {
                        mem::swap(&mut pred.left, &mut pred.right);
                    }
                }

                (join_preds, extra_preds)
            }
            JoinConstraint::Using(cols) => {
                invariant_eq!(cols.len(), 1);
                #[allow(clippy::unwrap_used)] // cols.len() == 1
                let col = cols.first().unwrap();

                left_table = prev_table.clone();
                right_table = rhs_relation;

                (
                    vec![JoinPredicate {
                        left: Column {
                            table: Some(left_table.clone()),
                            name: col.name.clone(),
                        },
                        right: Column {
                            table: Some(right_table.clone()),
                            name: col.name.clone(),
                        },
                    }],
                    vec![],
                )
            }
            JoinConstraint::Empty => {
                left_table = prev_table.clone();
                right_table = rhs_relation;
                // An empty predicate indicates a cartesian product is expected
                (vec![], vec![])
            }
        };

        // add edge for join
        if let std::collections::hash_map::Entry::Vacant(e) =
            edges.entry((left_table.clone(), right_table.clone()))
        {
            e.insert(match jc.operator {
                JoinOperator::LeftJoin | JoinOperator::LeftOuterJoin => {
                    let mut local_preds = HashMap::new();
                    let mut global_preds = vec![];
                    let mut params = vec![];
                    for pred in &extra_preds {
                        classify_conditionals(
                            pred,
                            &mut local_preds,
                            &mut global_preds,
                            &mut params,
                        )?;
                    }

                    let left_local_preds = local_preds.remove(&left_table).unwrap_or_default();
                    let right_local_preds = local_preds.remove(&right_table).unwrap_or_default();

                    // Anything that isn't local to the left or the right is actually a global
                    // predicate in disguise
                    global_predicates.extend(local_preds.into_values().flatten());

                    QueryGraphEdge::LeftJoin {
                        on,
                        left_local_preds,
                        right_local_preds,
                        global_preds,
                        params,
                    }
                }
                JoinOperator::Join | JoinOperator::InnerJoin => {
                    for pred in &extra_preds {
                        classify_conditionals(
                            pred,
                            &mut local_predicates,
                            &mut global_predicates,
                            &mut query_parameters,
                        )?;
                    }
                    QueryGraphEdge::Join { on }
                }
                _ => unsupported!("join operator not supported"),
            });
        }
    }

    if let Some(ref cond) = stmt.where_clause {
        // Let's classify the predicates we have in the query
        classify_conditionals(
            cond,
            &mut local_predicates,
            &mut global_predicates,
            &mut query_parameters,
        )?;
    }

    for (_, ces) in local_predicates.iter_mut() {
        *ces = split_conjunctions(ces.iter());
    }

    // 1. Add local predicates for each node that has them
    for (name, preds) in local_predicates {
        if let Some(rel) = relations.get_mut(&name) {
            rel.predicates.extend(preds);
        } else {
            // This predicate is not on any relation in the query - it might be a correlated
            // predicate (referencing an outer query), so just add it as a global predicate so
            // we can try to decorrelate it later
            global_predicates.extend(preds);
        }
    }

    // 2. Add any columns that are query parameters, and which therefore must appear in the leaf
    //    node for this query. Such columns will be carried all the way through the operators
    //    implementing the query (unlike in a traditional query plan, where the predicates on
    //    parameters might be evaluated sooner).
    for param in query_parameters.into_iter() {
        if let Some(table) = &param.col.table {
            let rel = relations.get_mut(table).ok_or_else(|| {
                invalid_query_err!(
                    "Column {} references non-existent table {}",
                    param.col.name,
                    table.display_unquoted()
                )
            })?;
            if !rel.columns.contains(&param.col) {
                rel.columns.push(param.col.clone());
            }
            // the parameter column is included in the projected columns of the output, but
            // we also separately register it as a parameter so that we can set keys
            // correctly on the leaf view
            rel.parameters.push(param.clone());
        }
    }

    // Add HAVING predicates and aggregates. Note that unlike below for selected columns, we don't
    // add any found aggregate functions in the HAVING clause to qg.columns, since we don't want to
    // necessarily return these in the query results.
    let mut aggregates = HashMap::new();
    let having_predicates = if let Some(having_expr) = stmt.having.as_ref() {
        extract_having_aggregates(having_expr, &mut aggregates)
    } else {
        vec![]
    };

    let mut columns = Vec::with_capacity(stmt.fields.len());
    for field in stmt.fields.iter() {
        match field {
            FieldDefinitionExpr::All | FieldDefinitionExpr::AllInTable(_) => {
                internal!("Stars should have been expanded by now!")
            }
            FieldDefinitionExpr::Expr { expr, alias } => {
                let name: SqlIdentifier = alias
                    .clone()
                    // FIXME(REA-2168): Use correct dialect.
                    .unwrap_or_else(|| expr.display(nom_sql::Dialect::MySQL).to_string().into());
                match expr {
                    Expr::Literal(l) => columns.push(OutputColumn::Literal(LiteralColumn {
                        name,
                        table: None,
                        value: l.clone(),
                    })),
                    Expr::Column(c) => {
                        columns.push(OutputColumn::Data {
                            alias: alias.clone().unwrap_or_else(|| c.name.clone()),
                            column: c.clone(),
                        });
                    }
                    Expr::Call(function) if is_aggregate(function) => {
                        let agg_name = aggregates
                            .entry(function.clone())
                            .or_insert_with(|| name.clone())
                            .clone();
                        // Aggregates end up in qg.columns as OutputColumn::Data not because they're
                        // columns in parent tables, but because by the time we're projecting the
                        // result set columns in a query the values for the aggregates will have
                        // *already been projected* (because we make aggregate nodes for all the
                        // aggregates in `qg.aggregates`). If they were `OutputColumn::Expr` here
                        // we'd try to *evaluate them* as project expressions, which obviously we
                        // can't do.
                        columns.push(OutputColumn::Data {
                            alias: alias.clone().unwrap_or(name),
                            column: Column {
                                name: agg_name,
                                table: None,
                            },
                        })
                    }
                    _ => {
                        let mut expr = expr.clone();
                        let aggs = map_aggregates(&mut expr);
                        aggregates.extend(aggs);

                        columns.push(OutputColumn::Expr(ExprColumn {
                            name,
                            table: None,
                            expression: expr,
                        }))
                    }
                };
            }
        }
    }

    let group_by = if let Some(group_by_clause) = &stmt.group_by {
        group_by_clause
            .fields
            .iter()
            .map(|f| match f {
                FieldReference::Numeric(_) => {
                    internal!("Numeric field references should have been removed")
                }
                FieldReference::Expr(e) => Ok(e.clone()),
            })
            .collect::<ReadySetResult<HashSet<_>>>()?
    } else {
        Default::default()
    };

    if let Some(ref order) = stmt.order {
        // For each column in the `ORDER BY` clause, check if it needs to be projected
        order
            .order_by
            .iter()
            .for_each(|OrderBy { field, .. }| match field {
                FieldReference::Expr(Expr::Column(Column { table: None, .. })) => {
                    // This is a reference to a projected column, otherwise the table value
                    // would be assigned in the `rewrite_selection` pass
                }
                FieldReference::Expr(Expr::Column(col @ Column { table: Some(_), .. })) => {
                    // This is a reference to a column in a table, we need to project it if it is
                    // not yet projected in order to be able to execute `ORDER
                    // BY` post lookup.
                    if !columns
                        .iter()
                        .any(|e| matches!(e, OutputColumn::Data {  column, .. } if column == col))
                    {
                        // The projected column does not already contains that column, so add it
                        columns.push(OutputColumn::Data {
                            alias: col.name.clone(),
                            column: col.clone(),
                        })
                    }
                }
                FieldReference::Expr(Expr::Call(func)) if is_aggregate(func) => {
                    // This is an aggregate expression that we need to add to the list of
                    // aggregates. We *don't* add it to the list of projected columns here, since
                    // we don't necessarily need it projected in the result set of the query, and
                    // the pull_columns pass will make sure the topk/paginate node gets the
                    // aggregate result column
                    aggregates.entry(func.clone()).or_insert_with(|| {
                        // FIXME(REA-2168): Use correct dialect.
                        func.display(nom_sql::Dialect::MySQL).to_string().into()
                    });
                }
                FieldReference::Expr(expr) => {
                    // This is an expression that we need to add to the list of projected columns
                    columns.push(OutputColumn::Expr(ExprColumn {
                        // FIXME(REA-2168): Use correct dialect.
                        name: expr.display(nom_sql::Dialect::MySQL).to_string().into(),
                        table: None,
                        expression: expr.clone(),
                    }));
                }
                // Numeric field references have already been projected, by definition
                FieldReference::Numeric(_) => {}
            })
    }

    let order = stmt
        .order
        .as_ref()
        .map(|order| {
            order
                .order_by
                .iter()
                .cloned()
                .map(
                    |OrderBy {
                         field,
                         order_type,
                         null_order,
                     }| {
                        let order_type = order_type.unwrap_or(OrderType::OrderAscending);
                        if let Some(null_order) = null_order {
                            if !null_order.is_default_for(order_type) {
                                unsupported!("Non-default NULLS FIRST/LAST is not yet supported");
                            }
                        }

                        Ok((
                            match field {
                                FieldReference::Expr(Expr::Column(col)) => col,
                                FieldReference::Expr(expr) => Column {
                                    // FIXME(REA-2168): Use correct dialect.
                                    name: expr.display(nom_sql::Dialect::MySQL).to_string().into(),
                                    table: None,
                                },
                                FieldReference::Numeric(_) => {
                                    internal!("Numeric field references should have been removed")
                                }
                            },
                            order_type,
                        ))
                    },
                )
                .collect::<ReadySetResult<_>>()
        })
        .transpose()?;

    // Extract pagination parameters
    let pagination = extract_limit_offset(&stmt.limit_clause)?
        .map(|(limit, offset)| -> ReadySetResult<Pagination> {
            Ok(Pagination {
                order: stmt
                    .order
                    .as_ref()
                    .map(|o| {
                        o.order_by
                            .iter()
                            .cloned()
                            .map(
                                |OrderBy {
                                     field,
                                     order_type,
                                     null_order,
                                 }| {
                                    let order_type =
                                        order_type.unwrap_or(OrderType::OrderAscending);
                                    if let Some(null_order) = null_order {
                                        if !null_order.is_default_for(order_type) {
                                            unsupported!(
                                                "Non-default NULLS FIRST/LAST is not yet supported"
                                            );
                                        }
                                    }
                                    Ok((
                                        match field {
                                            FieldReference::Numeric(_) => {
                                                internal!(
                                                "Numeric field references should have been removed"
                                            )
                                            }
                                            FieldReference::Expr(expr) => expr,
                                        },
                                        order_type,
                                    ))
                                },
                            )
                            .collect::<ReadySetResult<_>>()
                    })
                    .transpose()?,
                limit,
                offset,
            })
        })
        .transpose()?;

    // create initial join order
    let join_order = {
        let mut sorted_edges: Vec<(&(Relation, Relation), &QueryGraphEdge)> =
            edges.iter().collect();
        // Sort the edges to ensure deterministic join order.
        sorted_edges.sort_by(|&(a, _), &(b, _)| b.0.cmp(&a.0).then_with(|| a.1.cmp(&b.1)));

        sorted_edges
            .iter()
            .map(|((src, dst), _)| JoinRef {
                src: src.clone(),
                dst: dst.clone(),
            })
            .collect()
    };

    Ok(QueryGraph {
        distinct: stmt.distinct,
        relations,
        edges,
        aggregates,
        group_by,
        columns,
        fields: stmt.fields.clone(),
        default_row,
        join_order,
        global_predicates,
        having_predicates,
        pagination,
        order,
        is_correlated,
    })
}

#[allow(clippy::unwrap_used)]
#[allow(clippy::panic)]
#[cfg(test)]
mod tests {
    use assert_unordered::assert_eq_unordered;
    use nom_sql::{parse_query, parse_select_statement, Dialect, FunctionExpr, SqlQuery};

    use super::*;

    fn make_query_graph(sql: &str) -> QueryGraph {
        let query = match parse_query(Dialect::MySQL, sql).unwrap() {
            SqlQuery::Select(stmt) => stmt,
            q => panic!(
                "Unexpected query type; expected SelectStatement but got {:?}",
                q
            ),
        };

        to_query_graph(query).unwrap()
    }

    #[test]
    fn aggregates() {
        let qg = make_query_graph("SELECT max(t1.x) FROM t1 JOIN t2 ON t1.id = t2.id");
        assert_eq!(
            qg.relations.keys().cloned().collect::<HashSet<_>>(),
            HashSet::from(["t1".into(), "t2".into()])
        );
        assert_eq!(
            qg.aggregates,
            HashMap::from([(
                FunctionExpr::Max(Box::new(Expr::Column("t1.x".into()))),
                "max(`t1`.`x`)".into()
            )])
        );
    }

    #[test]
    fn aggregates_with_alias() {
        let qg = make_query_graph("SELECT max(t1.x) AS max_x FROM t1 JOIN t2 ON t1.id = t2.id");
        assert_eq!(
            qg.relations.keys().cloned().collect::<HashSet<_>>(),
            HashSet::from(["t1".into(), "t2".into()])
        );
        assert_eq!(
            qg.aggregates,
            HashMap::from([(
                FunctionExpr::Max(Box::new(Expr::Column("t1.x".into()))),
                "max_x".into()
            )])
        );
    }

    #[test]
    fn same_aggregate_with_two_aliases() {
        let qg = make_query_graph(
            "SELECT max(t1.x) AS max_x_1, max(t1.x) as max_x_2 FROM t1 JOIN t2 ON t1.id = t2.id",
        );
        assert_eq!(
            qg.relations.keys().cloned().collect::<HashSet<_>>(),
            HashSet::from(["t1".into(), "t2".into()])
        );

        let alias = qg
            .aggregates
            .get(&FunctionExpr::Max(Box::new(Expr::Column("t1.x".into()))))
            .unwrap();
        let agg_column = Column::from(alias.as_str());
        assert_eq!(
            qg.columns,
            vec![
                OutputColumn::Data {
                    alias: "max_x_1".into(),
                    column: agg_column.clone()
                },
                OutputColumn::Data {
                    alias: "max_x_2".into(),
                    column: agg_column
                },
            ]
        );
    }

    #[test]
    fn having_predicates_and_aggregates() {
        let qg = make_query_graph("select t.x from t having t.x > 2;");
        assert_eq!(
            qg.having_predicates,
            vec![Expr::BinaryOp {
                lhs: Box::new(Expr::Column("t.x".into())),
                op: BinaryOperator::Greater,
                rhs: Box::new(Expr::Literal(Literal::Integer(2)))
            }]
        );
        assert_eq!(qg.aggregates, HashMap::new());

        let qg = make_query_graph(
            "select t.x, sum(t.y) from t group by t.x having min(t.y) > 1 and sum(t.y) > 3;",
        );
        assert_eq_unordered!(
            qg.having_predicates,
            vec![
                Expr::BinaryOp {
                    lhs: Box::new(Expr::Column(Column {
                        name: "min(`t`.`y`)".into(),
                        table: None
                    })),
                    op: BinaryOperator::Greater,
                    rhs: Box::new(Expr::Literal(Literal::Integer(1)))
                },
                Expr::BinaryOp {
                    lhs: Box::new(Expr::Column(Column {
                        name: "sum(`t`.`y`)".into(),
                        table: None
                    })),
                    op: BinaryOperator::Greater,
                    rhs: Box::new(Expr::Literal(Literal::Integer(3)))
                },
            ]
        );

        let expected_aggs = [
            (
                FunctionExpr::Min(Box::new(Expr::Column(Column {
                    name: "y".into(),
                    table: Some("t".into()),
                }))),
                "min(`t`.`y`)".into(),
            ),
            (
                FunctionExpr::Sum {
                    expr: Box::new(Expr::Column(Column {
                        name: "y".into(),
                        table: Some("t".into()),
                    })),
                    distinct: false,
                },
                "sum(`t`.`y`)".into(),
            ),
        ];
        assert_eq!(qg.aggregates, HashMap::from(expected_aggs));
    }

    #[test]
    fn with_subquery() {
        let qg = make_query_graph(
            "SELECT t.x, sq.y FROM t JOIN (SELECT t2.id, t2.z FROM t2) sq ON t.id = sq.id",
        );

        assert_eq!(
            qg.relations.keys().cloned().collect::<HashSet<_>>(),
            HashSet::from(["t".into(), "sq".into()])
        );

        let subquery_rel = qg.relations.get(&Relation::from("sq")).unwrap();
        assert!(subquery_rel.subgraph.is_some());
    }

    #[test]
    fn duplicate_subquery_name() {
        let query = parse_select_statement(
            Dialect::MySQL,
            "select * from (select * from t) q1, (select * from t) q1;",
        )
        .unwrap();
        to_query_graph(query).unwrap_err();
    }

    #[test]
    fn order_by_aggregate() {
        let qg = make_query_graph(
            "SELECT t.a, t.b, sum(t.c) FROM t GROUP BY t.a, t.b ORDER BY sum(t.c);",
        );

        assert_eq!(
            qg.columns,
            vec![
                OutputColumn::Data {
                    alias: "a".into(),
                    column: Column::from("t.a")
                },
                OutputColumn::Data {
                    alias: "b".into(),
                    column: Column::from("t.b")
                },
                OutputColumn::Data {
                    alias: "sum(`t`.`c`)".into(),
                    column: Column {
                        name: "sum(`t`.`c`)".into(),
                        table: None
                    }
                }
            ]
        );

        assert_eq!(
            qg.aggregates,
            HashMap::from([(
                FunctionExpr::Sum {
                    expr: Box::new(Expr::Column("t.c".into())),
                    distinct: false,
                },
                "sum(`t`.`c`)".into()
            )])
        );
    }

    #[test]
    fn constant_filter() {
        let qg = make_query_graph("SELECT x FROM t WHERE x = $1 AND 1");
        assert_eq!(qg.global_predicates, vec![Expr::Literal(1.into())])
    }

    #[test]
    fn local_pred_in_join_condition() {
        let qg = make_query_graph("SELECT t1.x FROM t1 JOIN t2 ON t1.x = t2.x AND t2.y = 4");
        assert_eq!(
            qg.relations.get(&"t2".into()).unwrap().predicates,
            vec![Expr::BinaryOp {
                lhs: Box::new(Expr::Column("t2.y".into())),
                op: BinaryOperator::Equal,
                rhs: Box::new(Expr::Literal(4.into()))
            }]
        )
    }

    #[test]
    fn global_pred_in_join_condition() {
        let qg = make_query_graph("SELECT t1.x FROM t1 JOIN t2 ON t1.x = t2.x AND t2.is_thing");
        assert_eq!(
            qg.global_predicates,
            vec![Expr::Column("t2.is_thing".into())]
        )
    }

    #[test]
    fn local_pred_in_left_join() {
        let qg = make_query_graph(
            "SELECT t1.x FROM t1 LEFT JOIN t2 ON t2.x = 4 AND t1.y = 7 AND t1.z = t2.z",
        );

        let join = qg.edges.get(&("t1".into(), "t2".into())).unwrap();

        match join {
            QueryGraphEdge::LeftJoin {
                on,
                left_local_preds,
                right_local_preds,
                global_preds,
                params,
            } => {
                assert_eq!(
                    *on,
                    vec![JoinPredicate {
                        left: Column::from("t1.z"),
                        right: Column::from("t2.z")
                    }]
                );
                assert_eq!(
                    *left_local_preds,
                    vec![Expr::BinaryOp {
                        lhs: Box::new(Expr::Column("t1.y".into())),
                        op: BinaryOperator::Equal,
                        rhs: Box::new(Expr::Literal(7.into()))
                    }]
                );
                assert_eq!(
                    *right_local_preds,
                    vec![Expr::BinaryOp {
                        lhs: Box::new(Expr::Column("t2.x".into())),
                        op: BinaryOperator::Equal,
                        rhs: Box::new(Expr::Literal(4.into()))
                    }]
                );
                assert_eq!(*global_preds, vec![]);
                assert_eq!(*params, vec![]);
            }
            QueryGraphEdge::Join { .. } => panic!("Expected left join, got {join:?}"),
        }
    }

    mod view_key {
        use super::*;

        #[test]
        fn one_to_one_equal_key() {
            let qg = make_query_graph("SELECT t.x FROM t WHERE t.x = $1");
            let key = qg.view_key(&Default::default()).unwrap();

            assert_eq!(key.index_type, IndexType::HashMap);
            assert_eq!(
                key.columns,
                vec![(
                    mir::Column::new(Some("t"), "x"),
                    ViewPlaceholder::OneToOne(1, BinaryOperator::Equal)
                )]
            )
        }

        #[test]
        fn double_equality_same_column() {
            let qg = make_query_graph("SELECT t.x FROM t WHERE t.x = $1 AND t.x = $2");
            let key = qg.view_key(&Default::default()).unwrap();

            assert_eq!(key.index_type, IndexType::HashMap);

            assert_eq!(
                key.columns,
                vec![
                    (
                        mir::Column::new(Some("t"), "x"),
                        ViewPlaceholder::OneToOne(2, BinaryOperator::Equal)
                    ),
                    (
                        mir::Column::new(Some("t"), "x"),
                        ViewPlaceholder::OneToOne(1, BinaryOperator::Equal)
                    )
                ]
            )
        }

        #[test]
        fn double_range_same_column() {
            let qg = make_query_graph("SELECT t.x FROM t WHERE t.x > $1 AND t.x > $2");
            let key = qg.view_key(&Default::default()).unwrap();

            assert_eq!(key.index_type, IndexType::BTreeMap);

            assert_eq!(
                key.columns,
                vec![
                    (
                        mir::Column::new(Some("t"), "x"),
                        ViewPlaceholder::OneToOne(1, BinaryOperator::Greater)
                    ),
                    (
                        mir::Column::new(Some("t"), "x"),
                        ViewPlaceholder::OneToOne(2, BinaryOperator::Greater)
                    )
                ]
            )
        }

        #[test]
        fn compound_keys() {
            let qg =
                make_query_graph("SELECT Cats.id FROM Cats WHERE Cats.name = $1 AND Cats.id = $2");
            let key = qg.view_key(&Default::default()).unwrap();

            assert_eq!(key.index_type, IndexType::HashMap);
            assert_eq!(
                key.columns,
                vec![
                    (
                        mir::Column::new(Some("Cats"), "id"),
                        ViewPlaceholder::OneToOne(2, BinaryOperator::Equal)
                    ),
                    (
                        mir::Column::new(Some("Cats"), "name"),
                        ViewPlaceholder::OneToOne(1, BinaryOperator::Equal)
                    ),
                ]
            );
        }

        #[test]
        fn one_to_one_range_key() {
            let qg = make_query_graph("SELECT t.x FROM t WHERE t.x > $1");
            let key = qg.view_key(&Default::default()).unwrap();

            assert_eq!(key.index_type, IndexType::BTreeMap);
            assert_eq!(
                key.columns,
                vec![(
                    mir::Column::new(Some("t"), "x"),
                    ViewPlaceholder::OneToOne(1, BinaryOperator::Greater)
                )]
            )
        }

        #[test]
        fn between_keys() {
            let qg = make_query_graph("SELECT t.x FROM t WHERE t.x >= $1 AND t.x <= $2");
            let key = qg.view_key(&Default::default()).unwrap();

            assert_eq!(key.index_type, IndexType::BTreeMap);
            assert_eq!(
                key.columns,
                vec![(
                    mir::Column::new(Some("t"), "x"),
                    ViewPlaceholder::Between(1, 2)
                )]
            );
        }

        #[test]
        fn between_keys_reversed() {
            let qg = make_query_graph("SELECT t.x FROM t WHERE t.x <= $1 AND t.x >= $2");
            let key = qg.view_key(&Default::default()).unwrap();

            assert_eq!(key.index_type, IndexType::BTreeMap);
            assert_eq!(
                key.columns,
                vec![(
                    mir::Column::new(Some("t"), "x"),
                    ViewPlaceholder::Between(2, 1)
                )]
            );
        }

        #[test]
        fn mixed_inclusive_and_equal() {
            let qg = make_query_graph("SELECT t.x FROM t WHERE t.x >= $1 AND t.y = $2");
            let key = qg
                .view_key(&mir::Config {
                    allow_mixed_comparisons: true,
                    ..Default::default()
                })
                .unwrap();

            assert_eq!(key.index_type, IndexType::BTreeMap);
            assert_eq!(
                key.columns,
                vec![
                    (
                        mir::Column::new(Some("t"), "y"),
                        ViewPlaceholder::OneToOne(2, BinaryOperator::Equal)
                    ),
                    (
                        mir::Column::new(Some("t"), "x"),
                        ViewPlaceholder::OneToOne(1, BinaryOperator::GreaterOrEqual)
                    ),
                ]
            );
        }

        #[test]
        fn mixed_opposite_ranges() {
            let qg =
                make_query_graph("SELECT t.x FROM t WHERE t.x > $1 AND t.y <= $2 AND t.z = $3");
            let key = qg
                .view_key(&mir::Config {
                    allow_mixed_comparisons: true,
                    ..Default::default()
                })
                .unwrap();

            assert_eq!(key.index_type, IndexType::BTreeMap);
            assert_eq!(
                key.columns,
                vec![
                    (
                        mir::Column::new(Some("t"), "z"),
                        ViewPlaceholder::OneToOne(3, BinaryOperator::Equal)
                    ),
                    (
                        mir::Column::new(Some("t"), "x"),
                        ViewPlaceholder::OneToOne(1, BinaryOperator::Greater)
                    ),
                    (
                        mir::Column::new(Some("t"), "y"),
                        ViewPlaceholder::OneToOne(2, BinaryOperator::LessOrEqual)
                    ),
                ]
            );
        }

        #[test]
        fn mixed_equal_and_between() {
            let qg =
                make_query_graph("SELECT t.x FROM t WHERE t.x >= $1 AND t.x <= $2 AND t.y = $3");
            let key = qg
                .view_key(&mir::Config {
                    allow_mixed_comparisons: true,
                    ..Default::default()
                })
                .unwrap();

            assert_eq!(key.index_type, IndexType::BTreeMap);
            assert_eq!(
                key.columns,
                vec![
                    (
                        mir::Column::new(Some("t"), "y"),
                        ViewPlaceholder::OneToOne(3, BinaryOperator::Equal)
                    ),
                    (
                        mir::Column::new(Some("t"), "x"),
                        ViewPlaceholder::Between(1, 2)
                    )
                ]
            );
        }

        #[test]
        fn mixed_equal_range_and_between() {
            let qg = make_query_graph(
                "SELECT t.x FROM t WHERE t.x >= $1 AND t.x <= $2 AND t.y < $3 AND t.z = $4",
            );
            let key = qg
                .view_key(&mir::Config {
                    allow_mixed_comparisons: true,
                    ..Default::default()
                })
                .unwrap();

            assert_eq!(key.index_type, IndexType::BTreeMap);
            assert_eq!(
                key.columns,
                vec![
                    (
                        mir::Column::new(Some("t"), "z"),
                        ViewPlaceholder::OneToOne(4, BinaryOperator::Equal)
                    ),
                    (
                        mir::Column::new(Some("t"), "x"),
                        ViewPlaceholder::Between(1, 2)
                    ),
                    (
                        mir::Column::new(Some("t"), "y"),
                        ViewPlaceholder::OneToOne(3, BinaryOperator::Less)
                    )
                ]
            );
        }

        #[test]
        fn paginated() {
            let qg = make_query_graph(
                "SELECT t.x FROM t WHERE t.x = $1 ORDER BY t.y ASC LIMIT 3 OFFSET $2",
            );
            let key = qg
                .view_key(&mir::Config {
                    allow_paginate: true,
                    ..Default::default()
                })
                .unwrap();

            assert_eq!(key.index_type, IndexType::HashMap);
            assert_eq!(
                key.columns,
                vec![
                    (
                        mir::Column::new(Some("t"), "x"),
                        ViewPlaceholder::OneToOne(1, BinaryOperator::Equal)
                    ),
                    (
                        mir::Column::named("__page_number"),
                        ViewPlaceholder::PageNumber {
                            offset_placeholder: 2,
                            limit: 3,
                        }
                    )
                ]
            );
        }
    }
}
