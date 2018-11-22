use crate::controller::keys::provenance_of;
use crate::controller::recipe::{Recipe, Schema};
use dataflow::ops;
use dataflow::prelude::*;
use nom_sql::{Column, ColumnSpecification, SqlType};

use slog;

fn to_sql_type(d: &DataType) -> Option<SqlType> {
    match d {
        DataType::Int(_) => Some(SqlType::Int(32)),
        DataType::BigInt(_) => Some(SqlType::Bigint(64)),
        DataType::Real(_, _) => Some(SqlType::Real),
        DataType::Text(_) => Some(SqlType::Text),
        DataType::TinyText(_) => Some(SqlType::Varchar(8)),
        // TODO(malte): There is no SqlType for `NULL` (as it's not a
        // type), so caller must handle appropriately.
        DataType::None => None,
        DataType::Timestamp(_) => Some(SqlType::Timestamp),
    }
}

pub fn column_schema(
    graph: &Graph,
    view: NodeIndex,
    recipe: &Recipe,
    column_index: usize,
    log: &slog::Logger,
) -> Option<ColumnSpecification> {
    let paths = provenance_of(graph, view, &[column_index], |_, _, _| None);
    let vn = &graph[view];
    debug!(
        log,
        "provenance of {} ({}) is: {:?}",
        view.index(),
        vn.name(),
        paths
    );

    let mut col_type = None;
    for p in paths {
        // column originates at last element of the path whose second element is not None
        if let Some((ni, cols)) = p.into_iter().rfind(|e| e.1.iter().any(|c| c.is_some())) {
            // We invoked provenance_of with a singleton slice, so must have got
            // results for a single column
            assert_eq!(cols.len(), 1);

            let source_node = &graph[ni];
            let source_column_index = cols[0].unwrap();

            if source_node.is_base() {
                if let Some(schema) = recipe.schema_for(source_node.name()) {
                    // projected base table column
                    col_type = match schema {
                        Schema::Table(ref s) => {
                            Some(s.fields[cols.first().unwrap().unwrap()].sql_type.clone())
                        }
                        _ => unreachable!(),
                    };
                } else {
                    warn!(log, "no schema for base '{}'", source_node.name());
                }
            } else {
                // column originates at internal view: literal, aggregation output
                // FIXME(malte): return correct type depending on what column does
                match *(*source_node) {
                    ops::NodeOperator::Project(ref o) => {
                        let emits = o.emits();
                        assert!(source_column_index >= emits.0.len());
                        if source_column_index < emits.0.len() + emits.2.len() {
                            // computed expression
                            // TODO(malte): trace the actual column types, since this could be a
                            // real-valued arithmetic operation
                            col_type = Some(SqlType::Bigint(64));
                        } else {
                            // literal
                            let off = source_column_index - (emits.0.len() + emits.2.len());
                            col_type = to_sql_type(&emits.1[off]);
                        }
                    }
                    ops::NodeOperator::Sum(_) => {
                        if source_column_index == source_node.fields().len() - 1 {
                            // counts and sums always produce integral columns
                            col_type = Some(SqlType::Bigint(64));
                        } else {
                            // no column that isn't the aggregation result column should ever trace
                            // back to an aggregation.
                            unreachable!();
                        }
                    }
                    ops::NodeOperator::Extremum(_) => {
                        // TODO(malte): use type of the "over" column
                        unimplemented!();
                    }
                    ops::NodeOperator::Concat(_) => {
                        // group_concat always outputs a string
                        if source_column_index == source_node.fields().len() - 1 {
                            col_type = Some(SqlType::Text);
                        } else {
                            // no column that isn't the concat result column should ever trace
                            // back to a group_concat.
                            unreachable!();
                        }
                    }
                    ops::NodeOperator::Join(_) => {
                        // join doesn't "generate" columns, but they may come from one of the other
                        // ancestors; so keep iterating to try the other paths
                        ()
                    }
                    // no other operators should every generate columns
                    _ => unreachable!(),
                };
            }
        }
    }

    // we found no schema for this column
    if col_type.is_none() {
        return None;
    }

    // found something, so return a ColumnSpecification
    let cs = ColumnSpecification::new(
        Column {
            name: vn.fields()[column_index].to_owned(),
            table: Some(vn.name().to_owned()),
            alias: None,
            function: None,
        },
        col_type.unwrap(),
    );
    debug!(log, "CS: {:?}", cs);
    Some(cs)
}
