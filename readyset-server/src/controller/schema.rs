use dataflow::prelude::*;
use readyset_client::{ColumnBase, ColumnSchema};
use readyset_data::DfType;
use readyset_sql::ast::Column;
use tracing::trace;

use super::keys::provenance_of;
use super::sql::{BaseSchema, Recipe, Schema};

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
            if let Some(Schema::Table(BaseSchema { statement, pg_meta })) =
                recipe.schema_for(source_node.name())
            {
                let col_index = cols.first().unwrap().unwrap();
                let column_name = statement.fields[col_index].column.name.clone();
                #[allow(clippy::unwrap_used)] // occurs after implied table rewrite
                return Ok(Some(ColumnBase {
                    attnum: pg_meta
                        .as_ref()
                        .and_then(|m| m.column_oids.get(&column_name))
                        .copied(),
                    column: column_name,
                    table: statement.fields[col_index]
                        .column
                        .table
                        .as_ref()
                        .unwrap()
                        .clone(),
                    constraints: statement.fields[col_index].constraints.clone(),
                    table_oid: pg_meta.as_ref().map(|m| m.oid),
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
