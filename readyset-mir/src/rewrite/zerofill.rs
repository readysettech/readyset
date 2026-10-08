//! Disallow a MySQL query in which a `ZEROFILL` column is used in an unsupported way.
//!
//! In text results from MySQL, a `ZEROFILL` value is padded with zeros to the display width.

use std::collections::HashSet;
use std::convert::Infallible;
use std::fmt;

use petgraph::visit::{Bfs, Reversed};
use petgraph::Direction;
use readyset_errors::{unsupported, ReadySetResult};
use readyset_sql::analysis::visit::{self, Visitor};
use readyset_sql::ast::{
    BinaryOperator, ColumnConstraint, Expr, FunctionExpr, InValue, Literal, SelectStatement,
};
use readyset_sql::{Dialect, DialectDisplay};

use crate::graph::MirGraph;
use crate::node::{MirNodeInner, ProjectExpr};
use crate::query::MirQuery;
use crate::{Column, NodeIndex};

/// A use of an input column in a node.
#[derive(Debug)]
enum Use<'a> {
    /// An operand of an expression, or `None` for a column that is the whole expression.
    Operand(Option<&'a Expr>),
    /// The input of an aggregate or window function.
    Aggregated,
    /// A column of the result set, at this position.
    Output(usize),
}

impl fmt::Display for Use<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Use::Operand(Some(expr)) => write!(f, "{}", expr.display(Dialect::MySQL)),
            Use::Operand(None) => f.write_str("an expression"),
            Use::Aggregated => f.write_str("an aggregate or window function"),
            Use::Output(_) => f.write_str("the result set"),
        }
    }
}

/// Whether only the number matters in this use of a column with a `ZEROFILL` source.
fn supported(column_use: &Use<'_>) -> bool {
    match column_use {
        Use::Operand(Some(Expr::BinaryOp { op, .. })) => {
            op.is_ordering_comparison()
                || matches!(
                    op,
                    BinaryOperator::Equal
                        | BinaryOperator::NotEqual
                        | BinaryOperator::Is
                        | BinaryOperator::IsNot
                )
        }
        Use::Operand(Some(Expr::In {
            rhs: InValue::List(_),
            ..
        })) => true,
        // The result of COALESCE with an integer fallback is an unpadded number, as in the
        // projection for COUNT(*)
        Use::Operand(Some(Expr::Call(FunctionExpr::Coalesce(arguments)))) => matches!(
            arguments.as_slice(),
            [
                Expr::Column(_),
                Expr::Literal(Literal::Integer(_) | Literal::UnsignedInteger(_))
            ]
        ),
        Use::Operand(_) | Use::Aggregated | Use::Output(_) => false,
    }
}

/// Each column of an expression, with the enclosing expression.
#[derive(Default)]
struct Operands<'a> {
    parents: Vec<&'a Expr>,
    uses: Vec<(Column, Use<'a>)>,
}

impl<'a> Visitor<'a> for Operands<'a> {
    type Error = Infallible;

    fn visit_expr(&mut self, expr: &'a Expr) -> Result<(), Self::Error> {
        if let Expr::Column(column) = expr {
            let parent = self.parents.last().copied();
            self.uses.push((column.into(), Use::Operand(parent)));
            return Ok(());
        }
        self.parents.push(expr);
        visit::walk_expr(self, expr)?;
        self.parents.pop();
        Ok(())
    }

    fn visit_select_statement(&mut self, _: &'a SelectStatement) -> Result<(), Self::Error> {
        Ok(())
    }
}

/// The uses of the input columns of a node.
///
/// Keys of joins, groupings, orderings, and lookups are excluded.
fn uses(inner: &MirNodeInner) -> Vec<(Column, Use<'_>)> {
    let mut operands = Operands::default();
    let mut uses = vec![];
    match inner {
        MirNodeInner::Filter { conditions } => {
            let Ok(()) = operands.visit_expr(conditions);
        }
        MirNodeInner::Project { emit } => {
            for expr in emit {
                if let ProjectExpr::Expr { expr, .. } = expr {
                    let Ok(()) = operands.visit_expr(expr);
                }
            }
        }
        MirNodeInner::LeftJoin {
            left_local_preds, ..
        }
        | MirNodeInner::DependentLeftJoin {
            left_local_preds, ..
        } => {
            for predicate in left_local_preds {
                let Ok(()) = operands.visit_expr(predicate);
            }
        }
        MirNodeInner::Aggregation { on, .. }
        | MirNodeInner::Extremum { on, .. }
        | MirNodeInner::Accumulator { on, .. } => uses.push((on.clone(), Use::Aggregated)),
        MirNodeInner::Window { args, .. } => {
            uses.extend(args.iter().map(|column| (column.clone(), Use::Aggregated)));
        }
        MirNodeInner::Leaf {
            returned_cols,
            aggregates,
            ..
        } => {
            if let Some(aggregates) = aggregates {
                uses.extend(
                    aggregates
                        .aggregates
                        .iter()
                        .map(|aggregate| (aggregate.column.clone(), Use::Aggregated)),
                );
            }
            uses.extend(
                returned_cols
                    .iter()
                    .flatten()
                    .enumerate()
                    .map(|(position, column)| (column.clone(), Use::Output(position))),
            );
        }
        MirNodeInner::Base { .. }
        | MirNodeInner::Join { .. }
        | MirNodeInner::DependentJoin { .. }
        | MirNodeInner::Distinct { .. }
        | MirNodeInner::JoinAggregates { .. }
        | MirNodeInner::TopK { .. }
        | MirNodeInner::Paginate { .. }
        | MirNodeInner::ViewKey { .. }
        | MirNodeInner::AliasTable { .. }
        | MirNodeInner::Identity
        | MirNodeInner::Union { .. }
        | MirNodeInner::Constant { .. } => {}
    }
    uses.append(&mut operands.uses);
    uses
}

/// The parent column that `column`, an input of `node`, refers to.
fn sources<'a>(
    graph: &'a MirGraph,
    node: NodeIndex,
    column: &'a Column,
) -> impl Iterator<Item = (NodeIndex, usize)> + 'a {
    // For projected columns and DISTINCT keys, the lookup is by alias first
    let by_alias = matches!(
        graph[node].inner,
        MirNodeInner::Project { .. } | MirNodeInner::Distinct { .. }
    );
    graph
        .neighbors_directed(node, Direction::Incoming)
        .filter_map(move |parent| {
            let position = if by_alias {
                graph.find_source_for_child_column(parent, column)
            } else {
                graph.column_id_for_column(parent, column).ok()
            };
            position.map(|position| (parent, position))
        })
}

/// The origin of the values of a column.
#[derive(Debug, PartialEq, Eq, Hash)]
enum Source {
    /// A `ZEROFILL` table column.
    ZeroFill,
    /// A table column without `ZEROFILL`, or a computed column.
    Plain,
}

/// The distinct sources of the columns at `pending`, each a node and a position in its columns.
fn source_types(graph: &MirGraph, mut pending: Vec<(NodeIndex, usize)>) -> HashSet<Source> {
    let mut found = HashSet::new();
    let mut visited = HashSet::new();
    while let Some((source, position)) = pending.pop() {
        if !visited.insert((source, position)) {
            continue;
        }
        let parents = || graph.neighbors_directed(source, Direction::Incoming);
        let read = match &graph[source].inner {
            MirNodeInner::Base { column_specs, .. } => {
                let source = match column_specs.get(position) {
                    Some(spec) if spec.constraints.contains(&ColumnConstraint::ZeroFill) => {
                        Source::ZeroFill
                    }
                    _ => Source::Plain,
                };
                found.insert(source);
                continue;
            }
            MirNodeInner::Project { emit } => match emit.get(position) {
                Some(ProjectExpr::Column(column)) => Some(column),
                _ => None,
            },
            MirNodeInner::Aggregation { group_by, .. }
            | MirNodeInner::Extremum { group_by, .. }
            | MirNodeInner::Accumulator { group_by, .. }
            | MirNodeInner::Distinct { group_by } => group_by.get(position),
            MirNodeInner::Join { project, .. }
            | MirNodeInner::LeftJoin { project, .. }
            | MirNodeInner::DependentJoin { project, .. }
            | MirNodeInner::DependentLeftJoin { project, .. } => project.get(position),
            MirNodeInner::JoinAggregates { .. } => {
                if let Some(column) = graph.columns(source).get(position) {
                    pending.extend(sources(graph, source, column));
                }
                continue;
            }
            MirNodeInner::Union { emit, .. } => {
                for (parent, emit) in graph.sorted_ancestors(source).zip(emit) {
                    if let Some(position) = emit
                        .get(position)
                        .and_then(|column| graph.column_id_for_column(parent, column).ok())
                    {
                        pending.push((parent, position));
                    }
                }
                continue;
            }
            // The columns of the parent, followed by one generated column
            MirNodeInner::Paginate { .. } | MirNodeInner::Window { .. }
                if position + 1 == graph.columns(source).len() =>
            {
                None
            }
            MirNodeInner::Paginate { .. }
            | MirNodeInner::Window { .. }
            | MirNodeInner::Filter { .. }
            | MirNodeInner::Identity
            | MirNodeInner::TopK { .. }
            | MirNodeInner::Leaf { .. }
            | MirNodeInner::ViewKey { .. }
            | MirNodeInner::AliasTable { .. } => {
                pending.extend(parents().map(|parent| (parent, position)));
                continue;
            }
            MirNodeInner::Constant { .. } => None,
        };
        match read {
            Some(column) => pending.extend(sources(graph, source, column)),
            None => {
                found.insert(Source::Plain);
            }
        }
    }
    found
}

/// Refuse a query in which a `ZEROFILL` column is used in a way not allowed by [`supported`].
pub(super) fn refuse_unsupported_zerofill_uses(query: &MirQuery<'_>) -> ReadySetResult<()> {
    let graph = &*query.graph;
    let mut nodes = vec![];
    let mut bfs = Bfs::new(Reversed(&**graph), query.leaf());
    while let Some(node) = bfs.next(Reversed(&**graph)) {
        nodes.push(node);
    }
    let has_zerofill = |node: &NodeIndex| match &graph[*node].inner {
        MirNodeInner::Base { column_specs, .. } => column_specs
            .iter()
            .any(|spec| spec.constraints.contains(&ColumnConstraint::ZeroFill)),
        _ => false,
    };
    if !nodes.iter().any(has_zerofill) {
        return Ok(());
    }
    for node in nodes {
        if let MirNodeInner::Leaf {
            returned_cols: None,
            ..
        } = graph[node].inner
        {
            unsupported!(
                "the result set cannot be traced to table columns, so the ZEROFILL padding is \
                 unknown"
            );
        }
        for (column, column_use) in uses(&graph[node].inner) {
            if supported(&column_use) {
                continue;
            }
            // The result columns are the first columns of the parent of the reader, in order
            let pending = match column_use {
                Use::Output(position) => graph
                    .neighbors_directed(node, Direction::Incoming)
                    .map(|parent| (parent, position))
                    .collect(),
                _ => sources(graph, node, &column).collect(),
            };
            let found = source_types(graph, pending);
            if found.is_empty() {
                unsupported!(
                    "column {column} in {column_use} cannot be traced to a table column, so the \
                     ZEROFILL padding is unknown"
                );
            }
            if found.iter().any(|source| *source == Source::ZeroFill) {
                unsupported!("ZEROFILL column {column} in {column_use}");
            }
        }
    }
    Ok(())
}
