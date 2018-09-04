use controller::keys::provenance_of;
use controller::recipe::{Recipe, Schema};
use dataflow::ops;
use dataflow::prelude::*;
use nom_sql::{Column, ColumnSpecification, SqlType};

use slog;

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
                    warn!(log, "no schema for view '{}'", source_node.name());
                }
            } else {
                // column originates at internal view: literal, aggregation output
                // FIXME(malte): return correct type depending on what column does
                col_type = match *(*source_node) {
                    ops::NodeOperator::Project(ref o) => {
                        let emits = o.emits();
                        assert!(column_index > emits.0.len());
                        if column_index <= emits.0.len() + emits.2.len() {
                            // computed expression
                            unimplemented!();
                        } else {
                            // literal
                            let off = column_index - (emits.0.len() + emits.2.len());
                            match emits.1[off] {
                                DataType::Int(_) => Some(SqlType::Int(32)),
                                _ => unimplemented!(),
                            }
                        }
                    }
                    _ => unimplemented!(),
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
