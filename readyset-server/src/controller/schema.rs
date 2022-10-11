use dataflow::prelude::*;
use nom_sql::Column;
use readyset::{ColumnBase, ColumnSchema};
use readyset_data::DfType;
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
) -> ReadySetResult<&DfType> {
    Ok(node
        .columns()
        .get(column_index)
        .ok_or_else(|| {
            internal_err!(
                "Invalid index into node's columns, node={:?}, index={}",
                node.global_addr(),
                column_index
            )
        })?
        .ty())
}

fn trace_column_type_on_path<'g>(path: Path, graph: &'g Graph) -> ReadySetResult<&'g DfType> {
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
        Ok(&DfType::Unknown)
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
                #[allow(clippy::unwrap_used)] // occurs after implied table rewrite
                return Ok(Some(ColumnBase {
                    column: schema.fields[col_index].column.name.clone(),
                    table: schema.fields[col_index]
                        .column
                        .table
                        .as_ref()
                        .unwrap()
                        .clone(),
                    constraints: schema.fields[col_index].constraints.clone(),
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

    let mut col_type = &DfType::Unknown;
    let mut col_base = None;
    for ref p in paths {
        trace!("considering path {:?}", p);

        let ty = trace_column_type_on_path(p, graph)?;
        if !ty.is_unknown() {
            col_type = ty;
            col_base = get_base_for_column(p, graph, recipe)?;
        }
    }

    Ok(Some(ColumnSchema {
        base: col_base,
        column: Column {
            name: vn.columns()[column_index].name().into(),
            table: Some(vn.name().clone()),
        },
        column_type: col_type.clone(),
    }))
}
