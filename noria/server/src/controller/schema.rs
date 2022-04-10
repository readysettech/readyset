use dataflow::prelude::*;
use nom_sql::{Column, ColumnSpecification, SqlType};
use noria::{ColumnBase, ColumnSchema};
use tracing::trace;

use super::keys::provenance_of;
use super::recipe::{Recipe, Schema};

type Path<'a> = &'a [(
    petgraph::graph::NodeIndex,
    std::vec::Vec<std::option::Option<usize>>,
)];

fn type_for_internal_column(
    node: &dataflow::node::Node,
    column_index: usize,
) -> ReadySetResult<Option<SqlType>> {
    // TODO: Replace Option<SqlType> with Type
    Ok(node
        .columns()
        .get(column_index)
        .ok_or_else(|| {
            internal_err(format!(
                "Invalid index into node's columns, node={:?}, index={}",
                node.global_addr(),
                column_index
            ))
        })?
        .ty()
        .clone()
        .into())
}

fn trace_column_type_on_path(path: Path, graph: &Graph) -> ReadySetResult<Option<SqlType>> {
    // column originates at last element of the path whose second element is not None
    if let Some(pos) = path.iter().rposition(|e| e.1.iter().any(Option::is_some)) {
        let (ni, cols) = &path[pos];

        // We invoked provenance_of with a singleton slice, so must have got
        // results for a single column
        invariant_eq!(cols.len(), 1);

        let source_node = &graph[*ni];
        let source_column_index = cols[0].unwrap();
        type_for_internal_column(source_node, source_column_index)
    } else {
        Ok(None)
    }
}

fn get_base_for_column(
    path: Path,
    graph: &Graph,
    recipe: &Recipe,
) -> ReadySetResult<Option<ColumnBase>> {
    // column originates at last element of the path whose second element is not None
    if let Some(pos) = path.iter().rposition(|e| e.1.iter().any(Option::is_some)) {
        let (ni, cols) = &path[pos];

        let source_node = &graph[*ni];
        if source_node.is_base() {
            if let Some(Schema::Table(ref schema)) = recipe.schema_for(source_node.name()) {
                let col_index = cols.first().unwrap().unwrap();
                return Ok(Some(ColumnBase {
                    column: schema.fields[col_index].column.name.clone(),
                    table: schema.fields[col_index].column.table.clone().unwrap(),
                }));
            }
        }
    }

    Ok(None)
}

pub(super) fn column_schema(
    graph: &Graph,
    view: NodeIndex,
    recipe: &Recipe,
    column_index: usize,
) -> ReadySetResult<Option<ColumnSchema>> {
    trace!(
        "tracing provenance of {} on {} for schema",
        column_index,
        view.index()
    );
    let paths = provenance_of(graph, view, &[column_index])?;
    let vn = &graph[view];

    let mut col_type = None;
    let mut col_base = None;
    for ref p in paths {
        trace!("considering path {:?}", p);
        if let t @ Some(_) = trace_column_type_on_path(p, graph)? {
            col_type = t;
            col_base = get_base_for_column(p, graph, recipe)?;
        }
    }

    if let Some(col_type) = col_type {
        // found something, so return a ColumnSpecification
        let cs = ColumnSpecification::new(
            Column {
                name: vn.columns()[column_index].name().into(),
                table: Some(vn.name().to_owned()),
            },
            // ? in case we found no schema for this column
            col_type,
        );

        Ok(Some(ColumnSchema {
            spec: cs,
            base: col_base,
        }))
    } else {
        Ok(None)
    }
}
