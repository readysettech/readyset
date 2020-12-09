use super::keys::provenance_of;
use super::recipe::{Recipe, Schema};
use dataflow::ops;
use dataflow::prelude::*;
use nom_sql::{Column, ColumnSpecification, SqlType};

use slog;

type Path = std::vec::Vec<(
    petgraph::graph::NodeIndex,
    std::vec::Vec<std::option::Option<usize>>,
)>;

fn to_sql_type(d: &DataType) -> Option<SqlType> {
    match d {
        DataType::Int(_) => Some(SqlType::Int(32)),
        DataType::UnsignedInt(_) => Some(SqlType::UnsignedInt(32)),
        DataType::BigInt(_) => Some(SqlType::Bigint(64)),
        DataType::UnsignedBigInt(_) => Some(SqlType::UnsignedBigint(64)),
        DataType::Real(_, _) => Some(SqlType::Real),
        DataType::Text(_) => Some(SqlType::Text),
        DataType::TinyText(_) => Some(SqlType::Varchar(8)),
        // TODO(malte): There is no SqlType for `NULL` (as it's not a
        // type), so caller must handle appropriately.
        DataType::None => None,
        DataType::Timestamp(_) => Some(SqlType::Timestamp),
    }
}

fn type_for_internal_column(
    node: &dataflow::node::Node,
    column_index: usize,
    next_node_on_path: NodeIndex,
    recipe: &Recipe,
    graph: &Graph,
    log: &slog::Logger,
) -> Option<SqlType> {
    // column originates at internal view: literal, aggregation output
    // FIXME(malte): return correct type depending on what column does
    match *(*node) {
        ops::NodeOperator::Project(ref o) => {
            let emits = o.emits();
            assert!(column_index >= emits.0.len());
            if column_index < emits.0.len() + emits.2.len() {
                // computed expression
                // TODO(malte): trace the actual column types, since this could be a
                // real-valued arithmetic operation
                Some(SqlType::Bigint(64))
            } else {
                // literal
                let off = column_index - (emits.0.len() + emits.2.len());
                to_sql_type(&emits.1[off])
            }
        }
        ops::NodeOperator::Sum(_) | ops::NodeOperator::FilterSum(_) => {
            // computed column is always emitted last
            if column_index == node.fields().len() - 1 {
                // counts and sums always produce integral columns
                Some(SqlType::Bigint(64))
            } else {
                // no column that isn't the aggregation result column should ever trace
                // back to an aggregation.
                unreachable!();
            }
        }
        ops::NodeOperator::Extremum(ref o) => {
            let over_columns = o.over_columns();
            assert_eq!(over_columns.len(), 1);
            // use type of the "over" column
            column_schema(graph, next_node_on_path, recipe, over_columns[0], log)
                .map(|cs| cs.sql_type)
        }
        ops::NodeOperator::Concat(_) => {
            // group_concat always outputs a string as the last column
            if column_index == node.fields().len() - 1 {
                Some(SqlType::Text)
            } else {
                // no column that isn't the concat result column should ever trace
                // back to a group_concat.
                unreachable!();
            }
        }
        ops::NodeOperator::Join(_) => {
            // join doesn't "generate" columns, but they may come from one of the other
            // ancestors; so keep iterating to try the other paths
            None
        }
        // no other operators should every generate columns
        _ => unreachable!(),
    }
}

fn type_for_base_column(
    recipe: &Recipe,
    base: &str,
    column_index: usize,
    log: &slog::Logger,
) -> Option<SqlType> {
    if let Some(schema) = recipe.schema_for(base) {
        // projected base table column
        match schema {
            Schema::Table(ref s) => Some(s.fields[column_index].sql_type.clone()),
            _ => unreachable!(),
        }
    } else {
        error!(log, "no schema for base '{}'", base);
        None
    }
}

fn trace_column_type_on_path(
    path: Path,
    graph: &Graph,
    recipe: &Recipe,
    log: &slog::Logger,
) -> Option<SqlType> {
    // column originates at last element of the path whose second element is not None
    if let Some(pos) = path.iter().rposition(|e| e.1.iter().any(Option::is_some)) {
        let (ni, cols) = &path[pos];

        // We invoked provenance_of with a singleton slice, so must have got
        // results for a single column
        assert_eq!(cols.len(), 1);

        let source_node = &graph[*ni];
        let source_column_index = cols[0].unwrap();

        if source_node.is_base() {
            type_for_base_column(
                recipe,
                source_node.name(),
                cols.first().unwrap().unwrap(),
                log,
            )
        } else {
            let parent_node_index = path[pos + 1].0;

            type_for_internal_column(
                source_node,
                source_column_index,
                parent_node_index,
                recipe,
                graph,
                log,
            )
        }
    } else {
        None
    }
}

pub(super) fn column_schema(
    graph: &Graph,
    view: NodeIndex,
    recipe: &Recipe,
    column_index: usize,
    log: &slog::Logger,
) -> Option<ColumnSpecification> {
    trace!(
        log,
        "tracing provenance of {} on {} for schema",
        column_index,
        view.index()
    );
    let paths = provenance_of(graph, view, &[column_index], |_, _, _| None);
    let vn = &graph[view];

    let mut col_type = None;
    for p in paths {
        trace!(log, "considering path {:?}", p);
        if let t @ Some(_) = trace_column_type_on_path(p, graph, recipe, log) {
            col_type = t;
        }
    }

    // found something, so return a ColumnSpecification
    let cs = ColumnSpecification::new(
        Column {
            name: vn.fields()[column_index].to_owned(),
            table: Some(vn.name().to_owned()),
            alias: None,
            function: None,
        },
        // ? in case we found no schema for this column
        col_type?,
    );
    Some(cs)
}
