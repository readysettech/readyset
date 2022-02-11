use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::convert::TryFrom;
use std::hash::{Hash, Hasher};
use std::mem;
use std::vec::Vec;

use common::IndexType;
use nom_sql::analysis::ReferredColumns;
use nom_sql::{
    BinaryOperator, Column, Expression, FieldDefinitionExpression, FunctionExpression, InValue,
    ItemPlaceholder, JoinConstraint, JoinOperator, JoinRightSide, LimitClause, Literal, OrderType,
    SelectStatement, SqlIdentifier, Table, UnaryOperator,
};
use noria::{PlaceholderIdx, ViewPlaceholder};
use noria_errors::{
    internal, internal_err, invariant, invariant_eq, unsupported, unsupported_err, ReadySetResult,
};
use noria_sql_passes::{is_aggregate, is_predicate, map_aggregates, LogicalOp};
use serde::{Deserialize, Serialize};

use super::mir::{self, PAGE_NUMBER_COL};

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct LiteralColumn {
    pub name: SqlIdentifier,
    pub table: Option<SqlIdentifier>,
    pub value: Literal,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct ExpressionColumn {
    pub name: SqlIdentifier,
    pub table: Option<SqlIdentifier>,
    pub expression: Expression,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum OutputColumn {
    Data {
        alias: SqlIdentifier,
        column: Column,
    },
    Literal(LiteralColumn),
    Expression(ExpressionColumn),
}

impl Ord for OutputColumn {
    fn cmp(&self, other: &OutputColumn) -> Ordering {
        match *self {
            OutputColumn::Expression(ExpressionColumn {
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
                OutputColumn::Expression(ExpressionColumn {
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
            OutputColumn::Expression(ExpressionColumn {
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
                OutputColumn::Expression(ExpressionColumn {
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

#[derive(Clone, Debug, Hash, PartialEq, Serialize, Deserialize)]
pub struct JoinRef {
    pub src: SqlIdentifier,
    pub dst: SqlIdentifier,
}

/// An equality predicate on two expressions, used as the key for a join
#[derive(Clone, Debug, Hash, PartialEq, Serialize, Deserialize)]
pub struct JoinPredicate {
    pub left: Expression,
    pub right: Expression,
}

/// An individual column on which a query is parameterized
#[derive(Clone, Debug, Hash, PartialEq, Serialize, Deserialize)]
pub struct Parameter {
    pub col: Column,
    pub op: nom_sql::BinaryOperator,
    pub placeholder_idx: Option<PlaceholderIdx>,
}

#[derive(Clone, Debug, Hash, PartialEq, Serialize, Deserialize)]
pub struct QueryGraphNode {
    pub rel_name: SqlIdentifier,
    pub predicates: Vec<Expression>,
    pub columns: Vec<Column>,
    pub parameters: Vec<Parameter>,
    /// If this query graph relation refers to a subquery, the graph of that subquery and the AST
    /// for the query itself
    // TODO(grfn): Just pass around the SelectStatement as a field of the QueryGraph
    pub subgraph: Option<(Box<QueryGraph>, SelectStatement)>,
}

#[derive(Clone, Debug, Hash, PartialEq, Serialize, Deserialize)]
pub enum QueryGraphEdge {
    Join { on: Vec<JoinPredicate> },
    LeftJoin { on: Vec<JoinPredicate> },
}

#[derive(Clone, Debug, Hash, PartialEq, Serialize, Deserialize)]
pub struct Pagination {
    pub order: Option<Vec<(Expression, OrderType)>>,
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

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
// NOTE: Keep in mind this struct has a custom Hash impl - when changing it, remember to update that
// as well!
// TODO(grfn): impl Arbitrary for this struct so we can make a proptest for that
pub struct QueryGraph {
    /// Relations mentioned in the query.
    pub relations: HashMap<SqlIdentifier, QueryGraphNode>,
    /// Joins in the query.
    #[serde(with = "serde_with::rust::hashmap_as_tuple_list")]
    pub edges: HashMap<(SqlIdentifier, SqlIdentifier), QueryGraphEdge>,
    /// Aggregates in the query, represented as a list of (expression, alias) pairs
    pub aggregates: Vec<(FunctionExpression, SqlIdentifier)>,
    /// Set of columns that appear in the GROUP BY clause
    pub group_by: HashSet<Column>,
    /// Final set of projected columns in this query; may include literals in addition to the
    /// columns reflected in individual relations' `QueryGraphNode` structures.
    pub columns: Vec<OutputColumn>,
    /// Establishes an order for join predicates. Each join predicate can be identified by
    /// its (src, dst) pair
    pub join_order: Vec<JoinRef>,
    /// Global predicates (not associated with a particular relation)
    pub global_predicates: Vec<Expression>,
    /// The pagination (order, limit, offset) for the query, if any
    pub pagination: Option<Pagination>,
}

impl QueryGraph {
    fn new() -> Self {
        Default::default()
    }

    /// Returns the set of columns on which this query is parametrized. They can come from
    /// multiple tables involved in the query.
    /// Does not include limit or offset parameters on which this query may be parametrized.
    pub fn parameters(&self) -> Vec<&Parameter> {
        self.relations
            .values()
            .flat_map(|qgn| qgn.parameters.iter())
            .collect()
    }

    pub fn exact_hash(&self) -> u64 {
        use std::collections::hash_map::DefaultHasher;

        let mut s = DefaultHasher::new();
        self.hash(&mut s);
        s.finish()
    }

    /// Construct a representation of the lookup key of a view for this query graph, based on the
    /// parameters in this query and the page number if this query is parametrized on an offset key.
    pub(crate) fn view_key(&self, config: &mir::Config) -> ReadySetResult<ViewKey> {
        let offset = self.pagination.as_ref().and_then(|p| p.offset);
        if self.parameters().is_empty() {
            if let Some(offset) = offset {
                Ok(ViewKey {
                    columns: vec![(mir::Column::new(None, PAGE_NUMBER_COL.clone()), offset)],
                    index_type: IndexType::HashMap,
                })
            } else {
                Ok(ViewKey {
                    columns: vec![(
                        mir::Column::new(None, "bogokey"),
                        ViewPlaceholder::Generated,
                    )],
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

            let mut index_type = None;
            let mut columns: Vec<(mir::Column, ViewPlaceholder)> = vec![];
            let mut last_op = None;
            for param in parameters {
                let new_index_type = Some(IndexType::for_operator(param.op).ok_or_else(|| {
                    unsupported_err(format!("Unsupported binary operator `{}`", param.op))
                })?);

                if !config.allow_mixed_comparisons
                    && index_type.is_some()
                    && new_index_type != index_type
                {
                    unsupported!("Conflicting binary operators in query");
                } else {
                    index_type = new_index_type;
                }

                if let (Some((last_col, placeholder)), Some(last_op)) =
                    (columns.last_mut(), last_op)
                {
                    if *last_col == param.col {
                        match (last_op, param.op) {
                            (op1, op2) if op1 == op2 => {}
                            (BinaryOperator::GreaterOrEqual, BinaryOperator::LessOrEqual) => {
                                match (*placeholder, param.placeholder_idx) {
                                    (ViewPlaceholder::OneToOne(lower_idx), Some(upper_idx)) => {
                                        *placeholder =
                                            ViewPlaceholder::Between(lower_idx, upper_idx);
                                        continue;
                                    }
                                    _ => unsupported!("Conflicting binary operators in query"),
                                }
                            }
                            (BinaryOperator::LessOrEqual, BinaryOperator::GreaterOrEqual) => {
                                match (param.placeholder_idx, *placeholder) {
                                    (Some(lower_idx), ViewPlaceholder::OneToOne(upper_idx)) => {
                                        *placeholder =
                                            ViewPlaceholder::Between(lower_idx, upper_idx);
                                        continue;
                                    }
                                    _ => unsupported!("Conflicting binary operators in query"),
                                }
                            }
                            _ => unsupported!("Conflicting binary operators in query"),
                        }
                    }
                }

                columns.push((
                    mir::Column::from(param.col.clone()),
                    param.placeholder_idx.into(),
                ));

                last_op = Some(param.op);
            }

            if let Some(offset) = offset {
                if index_type == Some(IndexType::BTreeMap) {
                    unsupported!("ReadySet does not support Pagination and range queries")
                } else {
                    columns.push((mir::Column::new(None, PAGE_NUMBER_COL.clone()), offset));
                }
            }

            Ok(ViewKey {
                columns,
                index_type: index_type.expect("Checked self.parameters() isn't empty above"),
            })
        }
    }
}

#[allow(clippy::derive_hash_xor_eq)]
impl Hash for QueryGraph {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // sorted iteration over relations, edges to ensure consistent hash
        let mut rels: Vec<(&SqlIdentifier, &QueryGraphNode)> = self.relations.iter().collect();
        rels.sort_by(|a, b| a.0.cmp(b.0));
        rels.hash(state);
        let mut edges: Vec<(&(SqlIdentifier, SqlIdentifier), &QueryGraphEdge)> =
            self.edges.iter().collect();
        edges.sort_by(|a, b| match (a.0).0.cmp(&(b.0).0) {
            Ordering::Equal => (a.0).1.cmp(&(b.0).1),
            x => x,
        });
        edges.hash(state);

        let mut group_by = self.group_by.iter().collect::<Vec<_>>();
        group_by.sort();
        group_by.hash(state);

        // columns and join_order are Vecs, so already ordered
        self.aggregates.hash(state);
        self.columns.hash(state);
        self.join_order.hash(state);
        self.global_predicates.hash(state);
        self.pagination.hash(state);
    }
}

/// Splits top level conjunctions into multiple predicates
fn split_conjunctions(ces: Vec<Expression>) -> Vec<Expression> {
    let mut new_ces = Vec::new();
    for ce in ces {
        match ce {
            Expression::BinaryOp {
                op: BinaryOperator::And,
                lhs,
                rhs,
            } => {
                new_ces.extend(split_conjunctions(vec![*lhs.clone()]));
                new_ces.extend(split_conjunctions(vec![*rhs.clone()]));
            }
            _ => {
                new_ces.push(ce.clone());
            }
        }
    }

    new_ces
}

// 1. Extract any predicates with placeholder parameters. We push these down to the edge
//    nodes, since we cannot instantiate the parameters inside the data flow graph (except for
//    non-materialized nodes).
// 2. Extract local predicates
// 3. Extract join predicates
// 4. Collect remaining predicates as global predicates
fn classify_conditionals(
    ce: &Expression,
    tables: &[Table],
    local: &mut HashMap<SqlIdentifier, Vec<Expression>>,
    join: &mut Vec<JoinPredicate>,
    global: &mut Vec<Expression>,
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
        Expression::BinaryOp { op, lhs, rhs } => {
            if let Ok(op) = LogicalOp::try_from(*op) {
                // first, we recurse on both sides, collected the result of nested predicate
                // analysis in separate collections. What do do with these depends
                // on whether we're an AND or an OR clause:
                //  1) AND can be split into separate local predicates one one or more tables
                //  2) OR predictes must be preserved in their entirety, and we only use the nested
                //     local predicates discovered to decide if the OR is over one table (so it can
                //     remain a local predicate) or over several (so it must be a global predicate)
                let mut new_params = Vec::new();
                let mut new_join = Vec::new();
                let mut new_local = HashMap::new();
                let mut new_global = Vec::new();

                classify_conditionals(
                    lhs.as_ref(),
                    tables,
                    &mut new_local,
                    &mut new_join,
                    &mut new_global,
                    &mut new_params,
                )?;
                classify_conditionals(
                    rhs.as_ref(),
                    tables,
                    &mut new_local,
                    &mut new_join,
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
                                "can only combine two or fewer ConditionExpression's"
                            );
                            if ces.len() == 2 {
                                let new_ce = Expression::BinaryOp {
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
                        if !new_join.is_empty() {
                            unsupported!("can't handle OR expressions between JOIN predicates")
                        }
                        if !new_params.is_empty() {
                            unsupported!(
                                "can't handle OR expressions between query parameter predicates"
                            );
                        }
                        if new_local.keys().len() == 1 && new_global.is_empty() {
                            // OR over a single table => local predicate
                            let (t, ces) = new_local.into_iter().next().unwrap();
                            if ces.len() != 2 {
                                unsupported!("should combine only 2 ConditionExpressions");
                            }
                            let new_ce = Expression::BinaryOp {
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

                join.extend(new_join);
                params.extend(new_params);
            } else if is_predicate(op) {
                // atomic selection predicate
                match **rhs {
                    // right-hand side is a column, so this could be a comma join
                    // or a security policy using UserContext
                    Expression::Column(ref rf) => {
                        match **lhs {
                            // column/column comparison
                            Expression::Column(ref lf)
                                if lf.table.is_some()
                                    && tables.contains(&Table::from(
                                        lf.table.as_ref().unwrap().as_str(),
                                    ))
                                    && rf.table.is_some()
                                    && tables.contains(&Table::from(
                                        rf.table.as_ref().unwrap().as_str(),
                                    ))
                                    && lf.table != rf.table =>
                            {
                                // both columns' tables appear in table list and the tables are
                                // different --> comma join
                                if *op == BinaryOperator::Equal {
                                    // equi-join between two tables
                                    let mut jp = JoinPredicate {
                                        left: (**lhs).clone(),
                                        right: (**rhs).clone(),
                                    };
                                    if let Ordering::Less =
                                        rf.table.as_ref().cmp(&lf.table.as_ref())
                                    {
                                        mem::swap(&mut jp.left, &mut jp.right);
                                    }
                                    join.push(jp);
                                } else {
                                    // non-equi-join?
                                    unsupported!("non-equi-join?");
                                }
                            }
                            _ => {
                                // not a comma join, just an ordinary comparison with a
                                // computed column. This must be a global predicate because it
                                // crosses "tables" (the computed column has no associated
                                // table)
                                global.push(ce.clone());
                            }
                        }
                    }
                    // right-hand side is a placeholder, so this must be a query parameter
                    // We carry placeholder numbers all the way to reader nodes so that they can be
                    // mapped to a reader key column
                    Expression::Literal(Literal::Placeholder(ref placeholder)) => {
                        if let Expression::Column(ref lf) = **lhs {
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
                    }
                    // right-hand side is a non-placeholder literal, so this is a predicate
                    Expression::Literal(_) => {
                        if let Expression::Column(ref lf) = **lhs {
                            // we assume that implied table names have previously been expanded
                            // and thus all non-computed columns carry table names
                            if lf.table.is_some() {
                                let e = local.entry(lf.table.clone().unwrap()).or_default();
                                e.push(ce.clone());
                            } else {
                                // comparisons between computed columns and literals are global
                                // predicates
                                global.push(ce.clone());
                            }
                        }
                    }
                    Expression::NestedSelect(_) => {
                        unsupported!("nested SELECTs are unsupported")
                    }
                    Expression::Call(_)
                    | Expression::BinaryOp { .. }
                    | Expression::UnaryOp { .. }
                    | Expression::CaseWhen { .. }
                    | Expression::Exists(_)
                    | Expression::Between { .. }
                    | Expression::Cast { .. }
                    | Expression::In { .. }
                    | Expression::Variable(_) => {
                        unsupported!(
                            "Unsupported right-hand side of condition expression: {}",
                            rhs
                        )
                    }
                }
            } else {
                unsupported!("Arithmetic not supported here")
            }
        }
        Expression::In {
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
                // TODO(grfn): This limitation probably isn't too hard to lift
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
        Expression::UnaryOp {
            op: UnaryOperator::Not,
            ..
        } => {
            internal!("negation should have been removed earlier");
        }
        Expression::Exists(_) => {
            // TODO(grfn): Look into the query for correlated references to see if it's actually a
            // local predicate in disguise
            global.push(ce.clone())
        }
        Expression::Between { .. } => {
            internal!("Between should have been removed earlier")
        }
        expr => {
            // don't expect to see a base here: we ought to exit when classifying its
            // parent selection predicate
            internal!(
                "encountered unexpected standalone base of condition expression {:?}",
                expr
            );
        }
    }
    Ok(())
}

/// Convert the given `Expression`, which should be a set of AND-ed together direct
/// comparison predicates, into a list of predicate expressions
fn collect_join_predicates(cond: Expression, out: &mut Vec<JoinPredicate>) -> ReadySetResult<()> {
    match cond {
        Expression::BinaryOp {
            op: BinaryOperator::Equal,
            lhs,
            rhs,
        } => {
            out.push(JoinPredicate {
                left: *lhs,
                right: *rhs,
            });
            Ok(())
        }
        Expression::BinaryOp {
            lhs,
            op: BinaryOperator::And,
            rhs,
        } => {
            collect_join_predicates(*lhs, out)?;
            collect_join_predicates(*rhs, out)?;
            Ok(())
        }
        _ => {
            unsupported!("Only direct comparisons combined with AND supported for join conditions")
        }
    }
}

/// Convert limit and offset fields to usize and Option<ViewPlaceholder>
pub(crate) fn extract_limit_offset(
    limit: &LimitClause,
) -> ReadySetResult<(usize, Option<ViewPlaceholder>)> {
    let offset = limit
        .offset
        .as_ref()
        .and_then(|offset| match offset {
            Expression::Literal(Literal::Placeholder(ItemPlaceholder::DollarNumber(idx))) => {
                Some(Ok(ViewPlaceholder::Offset(*idx as usize)))
            }
            // For now, remove offset if it is a literal 0
            Expression::Literal(Literal::Integer(0)) => None,
            _ => Some(Err(internal_err("Numeric OFFSETs must be parametrized"))),
        })
        .transpose()?;
    let limit = match limit.limit {
        Expression::Literal(Literal::Integer(val)) => {
            if val < 0 {
                unsupported!("LIMIT field cannot have a negative value")
            } else {
                val as usize
            }
        }
        Expression::Literal(Literal::Placeholder(_)) => {
            unsupported!("ReadySet does not support parametrized LIMIT fields")
        }
        _ => unsupported!("Invalid LIMIT statement"),
    };
    Ok((limit, offset))
}

#[allow(clippy::cognitive_complexity)]
pub fn to_query_graph(st: &SelectStatement) -> ReadySetResult<QueryGraph> {
    let mut qg = QueryGraph::new();

    if st.tables.is_empty() {
        unsupported!("SELECT statements with no tables are unsupported")
    }

    // a handy closure for making new relation nodes
    let new_node = |rel: SqlIdentifier,
                    preds: Vec<Expression>,
                    st: &SelectStatement|
     -> ReadySetResult<QueryGraphNode> {
        Ok(QueryGraphNode {
            rel_name: rel.clone(),
            predicates: preds,
            columns: st
                .fields
                .iter()
                .map(|field| {
                    Ok(match field {
                        // unreachable because SQL rewrite passes will have expanded these already
                        FieldDefinitionExpression::All => {
                            internal!("* should have been expanded already")
                        }
                        FieldDefinitionExpression::AllInTable(_) => {
                            internal!("<table>.* should have been expanded already")
                        }
                        FieldDefinitionExpression::Expression {
                            expr: Expression::Column(c),
                            ..
                        } => match c.table.as_ref() {
                            None => internal!("No table name set for column {} on {}", c.name, rel),
                            Some(t) => {
                                if *t == rel {
                                    Some(c.clone())
                                } else {
                                    None
                                }
                            }
                        },
                        FieldDefinitionExpression::Expression { .. } => {
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

    // 1. Add any relations mentioned in the query to the query graph.
    // This is needed so that we don't end up with an empty query graph when there are no
    // conditionals, but rather with a one-node query graph that has no predicates.
    for table in &st.tables {
        qg.relations.insert(
            table.name.clone(),
            new_node(table.name.clone(), Vec::new(), st)?,
        );
    }
    for jc in &st.join {
        match &jc.right {
            JoinRightSide::Table(table) => {
                if !qg.relations.contains_key(&table.name) {
                    qg.relations.insert(
                        table.name.clone(),
                        new_node(table.name.clone(), Vec::new(), st)?,
                    );
                }
            }
            JoinRightSide::NestedSelect(subquery, alias) => {
                if !qg.relations.contains_key(alias) {
                    let mut node = new_node(alias.clone(), vec![], st)?;
                    node.subgraph = Some((Box::new(to_query_graph(subquery)?), *subquery.clone()));
                    qg.relations.insert(alias.clone(), node);
                }
            }
            JoinRightSide::Tables(_) => unsupported!("JoinRightSide::Tables not yet implemented"),
        }
    }

    // 2. Add edges for each pair of joined relations. Note that we must keep track of the join
    //    predicates here already, but more may be added when processing the WHERE clause lateron.
    let mut join_predicates = Vec::new();
    let col_expr = |tbl: &str, col: &str| -> Expression {
        Expression::Column(Column {
            table: Some(tbl.into()),
            name: (col.into()),
        })
    };

    // 2a. Explicit joins
    // The table specified in the query is available for USING joins.
    let prev_table = Some(st.tables.last().as_ref().unwrap().name.clone());
    for jc in &st.join {
        let rhs_name = match &jc.right {
            JoinRightSide::Table(table) => &table.name,
            JoinRightSide::NestedSelect(_, alias) => alias,
            _ => internal!(),
        };
        // will be defined by join constraint
        let left_table;
        let right_table;

        let join_preds = match &jc.constraint {
            JoinConstraint::On(cond) => {
                use nom_sql::analysis::ReferredTables;

                // find all distinct tables mentioned in the condition
                // conditions for now.
                let mut tables_mentioned: Vec<SqlIdentifier> =
                    cond.referred_tables().into_iter().map(|t| t.name).collect();

                let mut join_preds = vec![];
                collect_join_predicates(cond.clone(), &mut join_preds)?;

                if tables_mentioned.len() == 2 {
                    // tables can appear in any order in the join predicate, but
                    // we cannot just rely on that order, since it may lead us to
                    // flip LEFT JOINs by accident (yes, this happened)
                    if tables_mentioned[1] != *rhs_name {
                        // tables are in the wrong order in join predicate, swap
                        tables_mentioned.swap(0, 1);
                        invariant_eq!(tables_mentioned[1], *rhs_name);
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
                    let l = match &pred.left {
                        Expression::Column(f) => f,
                        ref x => unsupported!("join condition not supported: {:?}", x),
                    };
                    let r = match &pred.right {
                        Expression::Column(f) => f,
                        ref x => unsupported!("join condition not supported: {:?}", x),
                    };
                    if *l.table.as_ref().unwrap() == right_table
                        && *r.table.as_ref().unwrap() == left_table
                    {
                        mem::swap(&mut pred.left, &mut pred.right);
                    }
                }

                join_preds
            }
            JoinConstraint::Using(cols) => {
                invariant_eq!(cols.len(), 1);
                let col = cols.iter().next().unwrap();

                left_table = prev_table.as_ref().unwrap().clone();
                right_table = rhs_name.clone();

                vec![JoinPredicate {
                    left: col_expr(&left_table, &col.name),
                    right: col_expr(&right_table, &col.name),
                }]
            }
            JoinConstraint::Empty => {
                left_table = prev_table.as_ref().unwrap().clone();
                right_table = rhs_name.clone();
                // An empty predicate indicates a cartesian product is expected
                vec![]
            }
        };

        // add edge for join
        // FIXME(eta): inefficient cloning!
        if let std::collections::hash_map::Entry::Vacant(e) =
            qg.edges.entry((left_table.clone(), right_table.clone()))
        {
            e.insert(match jc.operator {
                JoinOperator::LeftJoin | JoinOperator::LeftOuterJoin => {
                    QueryGraphEdge::LeftJoin { on: join_preds }
                }
                JoinOperator::Join | JoinOperator::InnerJoin => {
                    QueryGraphEdge::Join { on: join_preds }
                }
                _ => unsupported!("join operator not supported"),
            });
        }
    }

    if let Some(ref cond) = st.where_clause {
        let mut local_predicates = HashMap::new();
        let mut global_predicates = Vec::new();
        let mut query_parameters = Vec::new();
        // Let's classify the predicates we have in the query
        classify_conditionals(
            cond,
            &st.tables,
            &mut local_predicates,
            &mut join_predicates,
            &mut global_predicates,
            &mut query_parameters,
        )?;

        for (_, ces) in local_predicates.iter_mut() {
            *ces = split_conjunctions(ces.clone());
        }

        // 1. Add local predicates for each node that has them
        for (rel, preds) in local_predicates {
            if !qg.relations.contains_key(&rel) {
                // can't have predicates on tables that do not appear in the FROM part of the
                // statement
                internal!(
                    "predicate(s) {:?} on relation {} that is not in query graph",
                    preds,
                    rel
                );
            } else {
                qg.relations.get_mut(&rel).unwrap().predicates.extend(preds);
            }
        }

        // 2. Add predicates for implied (comma) joins
        for jp in join_predicates {
            if let Expression::Column(l) = &jp.left {
                if let Expression::Column(r) = &jp.right {
                    let nn = new_node(l.table.clone().unwrap(), Vec::new(), st)?;
                    // If tables aren't already in the relations, add them.
                    qg.relations
                        .entry(l.table.clone().unwrap())
                        .or_insert_with(|| nn.clone());

                    qg.relations
                        .entry(r.table.clone().unwrap())
                        .or_insert_with(|| nn.clone());

                    let e = qg
                        .edges
                        .entry((l.table.clone().unwrap(), r.table.clone().unwrap()))
                        .or_insert_with(|| QueryGraphEdge::Join { on: vec![] });
                    match *e {
                        QueryGraphEdge::Join { on: ref mut preds } => preds.push(jp.clone()),
                        _ => internal!("Expected join edge for join condition {:#?}", jp),
                    };
                }
            }
        }

        // 3. Add any columns that are query parameters, and which therefore must appear in the leaf
        //    node for this query. Such columns will be carried all the way through the operators
        //    implementing the query (unlike in a traditional query plan, where the predicates on
        //    parameters might be evaluated sooner).
        for param in query_parameters.into_iter() {
            match param.col.table {
                None => {
                    unsupported!("each parameter's column must have an associated table! (no such column \"{}\")", param.col);
                }
                Some(ref table) => {
                    let rel = qg.relations.get_mut(table).unwrap();
                    if !rel.columns.contains(&param.col) {
                        rel.columns.push(param.col.clone());
                    }
                    // the parameter column is included in the projected columns of the output, but
                    // we also separately register it as a parameter so that we can set keys
                    // correctly on the leaf view
                    rel.parameters.push(param.clone());
                }
            }
        }

        // 4. Add global predicates
        qg.global_predicates = global_predicates;
    }

    for field in st.fields.iter() {
        match field {
            FieldDefinitionExpression::All | FieldDefinitionExpression::AllInTable(_) => {
                internal!("Stars should have been expanded by now!")
            }
            FieldDefinitionExpression::Expression { expr, alias } => {
                let name: SqlIdentifier = alias.clone().unwrap_or_else(|| expr.to_string().into());
                match expr {
                    Expression::Literal(l) => {
                        qg.columns.push(OutputColumn::Literal(LiteralColumn {
                            name,
                            table: None,
                            value: l.clone(),
                        }))
                    }
                    Expression::Column(c) => {
                        qg.columns.push(OutputColumn::Data {
                            alias: alias.clone().unwrap_or_else(|| c.name.clone()),
                            column: c.clone(),
                        });
                    }
                    Expression::Call(function) if is_aggregate(function) => {
                        qg.aggregates.push((function.clone(), name.clone()));
                        qg.columns.push(OutputColumn::Data {
                            alias: name.clone(),
                            column: Column { name, table: None },
                        })
                    }
                    _ => {
                        let mut expr = expr.clone();
                        let aggs = map_aggregates(&mut expr);
                        qg.aggregates.extend(aggs);

                        qg.columns.push(OutputColumn::Expression(ExpressionColumn {
                            name,
                            table: None,
                            expression: expr,
                        }))
                    }
                };
            }
        }
    }

    if let Some(group_by_clause) = &st.group_by {
        qg.group_by.extend(group_by_clause.columns.clone());
    }

    if let Some(ref order) = st.order {
        // For each column in the `ORDER BY` clause, check if it needs to be projected
        order
            .order_by
            .iter()
            .for_each(|(ord_expr, _)| match ord_expr {
                Expression::Column(Column { table: None, .. }) => {
                    // This is a reference to a projected column, otherwise the table value
                    // would be assigned in the `rewrite_selection` pass
                }
                Expression::Column(col @ Column { table: Some(_), .. }) => {
                    // This is a reference to a column in a table, we need to project it if it is
                    // not yet projected in order to be able to execute `ORDER
                    // BY` post lookup.
                    if !qg
                        .columns
                        .iter()
                        .any(|e| matches!(e, OutputColumn::Data {  column, .. } if column == col))
                    {
                        // The projected column does not already contains that column, so add it
                        qg.columns.push(OutputColumn::Data {
                            alias: col.name.clone(),
                            column: col.clone(),
                        })
                    }
                }
                Expression::Call(func) if is_aggregate(func) => {
                    // This is an aggregate expression that we need to add to the list of projected
                    // columns and aggregates
                    qg.aggregates.push((func.clone(), func.to_string().into()));
                    qg.columns.push(OutputColumn::Expression(ExpressionColumn {
                        name: ord_expr.to_string().into(),
                        table: None,
                        expression: ord_expr.clone(),
                    }));
                }
                expr => {
                    // This is an expression that we need to add to the list of projected columns
                    qg.columns.push(OutputColumn::Expression(ExpressionColumn {
                        name: expr.to_string().into(),
                        table: None,
                        expression: expr.clone(),
                    }));
                }
            })
    }

    // Extract pagination parameters
    if let Some(ref limit) = st.limit {
        let (limit, offset) = extract_limit_offset(limit)?;

        qg.pagination = Some(Pagination {
            order: st.order.as_ref().map(|o| {
                o.order_by
                    .iter()
                    .cloned()
                    .map(|(expr, ot)| (expr, ot.unwrap_or(OrderType::OrderAscending)))
                    .collect()
            }),
            limit,
            offset,
        })
    }

    // create initial join order
    {
        let mut sorted_edges: Vec<(&(SqlIdentifier, SqlIdentifier), &QueryGraphEdge)> =
            qg.edges.iter().collect();
        // Sort the edges to ensure deterministic join order.
        sorted_edges.sort_by(|&(a, _), &(b, _)| b.0.cmp(&a.0).then_with(|| a.1.cmp(&b.1)));

        qg.join_order
            .extend(sorted_edges.iter().map(|((src, dst), _)| JoinRef {
                src: src.clone(),
                dst: dst.clone(),
            }));
    }

    Ok(qg)
}

#[cfg(test)]
mod tests {
    use nom_sql::{parse_query, Dialect, FunctionExpression, SqlQuery};

    use super::*;

    fn make_query_graph(sql: &str) -> QueryGraph {
        let query = match parse_query(Dialect::MySQL, sql).unwrap() {
            SqlQuery::Select(stmt) => stmt,
            q => panic!(
                "Unexpected query type; expected SelectStatement but got {:?}",
                q
            ),
        };

        to_query_graph(&query).unwrap()
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
            vec![(
                FunctionExpression::Max(Box::new(Expression::Column("t1.x".into()))),
                "max(`t1`.`x`)".into()
            )]
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
            vec![(
                FunctionExpression::Max(Box::new(Expression::Column("t1.x".into()))),
                "max_x".into()
            )]
        );
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

        let subquery_rel = qg.relations.get("sq").unwrap();
        assert!(subquery_rel.subgraph.is_some());
    }

    mod view_key {
        use super::*;

        #[test]
        fn bogokey_key() {
            let qg = make_query_graph("SELECT t.x FROM t");
            let key = qg.view_key(&Default::default()).unwrap();

            assert_eq!(key.index_type, IndexType::HashMap);
            assert_eq!(
                key.columns,
                vec![(
                    mir::Column::new(None, "bogokey"),
                    ViewPlaceholder::Generated
                )]
            );
        }

        #[test]
        fn one_to_one_equal_key() {
            let qg = make_query_graph("SELECT t.x FROM t WHERE t.x = $1");
            let key = qg.view_key(&Default::default()).unwrap();

            assert_eq!(key.index_type, IndexType::HashMap);
            assert_eq!(
                key.columns,
                vec![(
                    mir::Column::new(Some("t"), "x"),
                    ViewPlaceholder::OneToOne(1)
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
                        ViewPlaceholder::OneToOne(2)
                    ),
                    (
                        mir::Column::new(Some("t"), "x"),
                        ViewPlaceholder::OneToOne(1)
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
                        ViewPlaceholder::OneToOne(1)
                    ),
                    (
                        mir::Column::new(Some("t"), "x"),
                        ViewPlaceholder::OneToOne(2)
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
                        ViewPlaceholder::OneToOne(2)
                    ),
                    (
                        mir::Column::new(Some("Cats"), "name"),
                        ViewPlaceholder::OneToOne(1)
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
                    ViewPlaceholder::OneToOne(1)
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
                        ViewPlaceholder::OneToOne(2)
                    ),
                    (
                        mir::Column::new(Some("t"), "x"),
                        ViewPlaceholder::OneToOne(1)
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
                        ViewPlaceholder::OneToOne(3)
                    ),
                    (
                        mir::Column::new(Some("t"), "x"),
                        ViewPlaceholder::OneToOne(1)
                    ),
                    (
                        mir::Column::new(Some("t"), "y"),
                        ViewPlaceholder::OneToOne(2)
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
                        ViewPlaceholder::OneToOne(3)
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
                        ViewPlaceholder::OneToOne(4)
                    ),
                    (
                        mir::Column::new(Some("t"), "x"),
                        ViewPlaceholder::Between(1, 2)
                    ),
                    (
                        mir::Column::new(Some("t"), "y"),
                        ViewPlaceholder::OneToOne(3)
                    )
                ]
            );
        }
    }
}
