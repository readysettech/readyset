use dataflow::prelude::*;
use readyset_client::schema::{ColumnBase, ColumnSchema};
use readyset_data::dialect::SqlEngine;
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

/// The table column at the end of `path`, and whether NULL can appear in the column of the result.
fn get_base_for_column(
    path: Path,
    graph: &Graph,
    recipe: &Recipe,
) -> ReadySetResult<(Option<ColumnBase>, bool)> {
    let left_join_right_column = path
        .iter()
        .any(|(ni, cols)| match graph[*ni].as_internal() {
            Some(NodeOperator::Join(join)) => cols
                .iter()
                .flatten()
                .any(|&col| join.is_left_join_right_column(col)),
            _ => false,
        });
    // column originates at last element of the path whose second element is not None
    if let Some(pos) = path.iter().rposition(|e| e.1.iter().any(Option::is_some)) {
        let (ni, cols) = &path[pos];

        let source_node = &graph[*ni];
        if matches!(source_node.as_internal(), Some(NodeOperator::TopK(_))) {
            // This TopK node happens to be the node farthest up this path from the reader node
            // where the column index is still known.  Continue searching above this TopK node
            // using the same column index.
            let cols = cols.iter().flatten().copied().collect::<Vec<_>>();
            let mut paths = vec![];
            for parent in graph.neighbors_directed(*ni, petgraph::EdgeDirection::Incoming) {
                paths.extend(provenance_of(graph, parent, &cols)?);
            }
            let (base, nullable) = get_base_for_paths(&paths, graph, recipe)?;
            return Ok((base, nullable || left_join_right_column));
        }
        if source_node.is_base() {
            if let Some(Schema::Table(BaseSchema { statement, pg_meta })) =
                recipe.schema_for(source_node.name())
            {
                let col_index = cols.first().unwrap().unwrap();
                let column_name = statement.fields[col_index].column.name.clone();
                let sql_type = statement.fields[col_index].sql_type.clone();
                #[allow(clippy::unwrap_used)] // occurs after implied table rewrite
                let base = ColumnBase {
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
                    sql_type,
                };
                let nullable = left_join_right_column || !base.is_not_null();
                return Ok((Some(base), nullable));
            }
        }
    }

    Ok((None, true))
}

/// The table column of a result column with these paths, and whether NULL can appear in the
/// column of the result on any path.
fn get_base_for_paths(
    paths: &[Vec<(NodeIndex, Vec<Option<usize>>)>],
    graph: &Graph,
    recipe: &Recipe,
) -> ReadySetResult<(Option<ColumnBase>, bool)> {
    let mut bases = vec![];
    let mut nullable = paths.is_empty();
    for path in paths {
        if trace_column_type_on_path(path, graph)?.is_unknown() {
            nullable = true;
        } else {
            let (base, path_nullable) = get_base_for_column(path, graph, recipe)?;
            bases.push(base);
            nullable |= path_nullable;
        }
    }
    let union_all = paths.iter().flatten().any(|(ni, _)| {
        matches!(graph[*ni].as_internal(), Some(NodeOperator::Union(union)) if union.is_union_all())
    });
    let base = if union_all {
        // In MySQL and PostgreSQL, a column of a UNION ALL result is not reported as a table
        // column, but in PostgreSQL the type modifier is kept when every branch has the same type
        let same_type = bases.windows(2).all(|pair| match pair {
            [Some(a), Some(b)] => a.sql_type == b.sql_type,
            _ => false,
        });
        match bases.into_iter().next().flatten() {
            Some(base) if same_type && recipe.dialect().engine() == SqlEngine::PostgreSQL => {
                Some(ColumnBase {
                    constraints: vec![],
                    table_oid: None,
                    attnum: None,
                    ..base
                })
            }
            _ => None,
        }
    } else {
        bases.pop().flatten()
    };
    Ok((base, nullable))
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
    for p in &paths {
        trace!("considering path {:?}", p);

        let ty = trace_column_type_on_path(p, graph)?;
        if !ty.is_unknown() {
            col_type = ty;
        }
    }

    let (base, nullable) = get_base_for_paths(&paths, graph, recipe)?;
    Ok(Some(ColumnSchema {
        base,
        nullable,
        column: Column {
            name: vn.columns()[column_index].name().into(),
            table: Some(vn.name().clone()),
        },
        column_type: col_type.clone(),
    }))
}
