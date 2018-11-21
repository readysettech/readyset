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
                    warn!(log, "no schema for view '{}'", source_node.name());
                }
            } else {
                // column originates at internal view: literal, aggregation output
                // FIXME(malte): return correct type depending on what column does
                col_type = match *(*source_node) {
                    ops::NodeOperator::Project(ref o) => {
                        let emits = o.emits();
                        assert!(source_column_index >= emits.0.len());
                        if source_column_index < emits.0.len() + emits.2.len() {
                            // computed expression
                            unimplemented!();
                        } else {
                            // literal
                            let off = source_column_index - (emits.0.len() + emits.2.len());
                            to_sql_type(&emits.1[off])
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
