use super::keys::provenance_of;
use super::recipe::{Recipe, Schema};
use dataflow::ops;
use dataflow::prelude::*;
use nom_sql::{Column, ColumnSpecification, SqlType};
use noria::{ColumnBase, ColumnSchema};
use ops::NodeOperator;
use tracing::{error, trace};

type Path<'a> = &'a [(
    petgraph::graph::NodeIndex,
    std::vec::Vec<std::option::Option<usize>>,
)];

fn type_for_internal_column(
    node: &dataflow::node::Node,
    column_index: usize,
    next_node_on_path: NodeIndex,
    recipe: &Recipe,
    graph: &Graph,
) -> ReadySetResult<Option<SqlType>> {
    // column originates at internal view: literal, aggregation output
    match *(node.as_internal().ok_or(ReadySetError::NonInternalNode)?) {
        NodeOperator::Project(ref o) => o.column_type(column_index, |parent_col| {
            Ok(column_schema(graph, next_node_on_path, recipe, parent_col)?
                .map(ColumnSchema::take_type))
        }),
        NodeOperator::Aggregation(ref grouped_op) => {
            // computed column is always emitted last
            if column_index == node.fields().len() - 1 {
                if let Some(res) = grouped_op.output_col_type() {
                    Ok(Some(res))
                } else {
                    // if none, output column type is same as over column type
                    // use type of the "over" column
                    Ok(
                        column_schema(graph, next_node_on_path, recipe, grouped_op.over_column())?
                            .map(ColumnSchema::take_type),
                    )
                }
            } else {
                Ok(
                    column_schema(graph, next_node_on_path, recipe, column_index)?
                        .map(ColumnSchema::take_type),
                )
            }
        }
        NodeOperator::Extremum(ref o) => {
            // use type of the "over" column
            if column_index == node.fields().len() - 1 {
                Ok(
                    column_schema(graph, next_node_on_path, recipe, o.over_column())?
                        .map(ColumnSchema::take_type),
                )
            } else {
                Ok(
                    column_schema(graph, next_node_on_path, recipe, column_index)?
                        .map(ColumnSchema::take_type),
                )
            }
        }
        NodeOperator::Concat(_) => {
            // group_concat always outputs a string as the last column
            if column_index == node.fields().len() - 1 {
                Ok(Some(SqlType::Text))
            } else {
                Ok(
                    column_schema(graph, next_node_on_path, recipe, column_index)?
                        .map(ColumnSchema::take_type),
                )
            }
        }
        NodeOperator::Join(_) => {
            // join doesn't "generate" columns, but they may come from one of the other
            // ancestors; so keep iterating to try the other paths
            Ok(None)
        }
        NodeOperator::ParamFilter(_) => unsupported!("ParamFilter isn't implemented"),
        NodeOperator::Latest(_)
        | NodeOperator::Union(_)
        | NodeOperator::Identity(_)
        | NodeOperator::Filter(_)
        | NodeOperator::TopK(_) => {
            Ok(
                column_schema(graph, next_node_on_path, recipe, column_index)?
                    .map(ColumnSchema::take_type),
            )
        }
    }
}

fn trace_column_type_on_path(
    path: Path,
    graph: &Graph,
    recipe: &Recipe,
) -> ReadySetResult<Option<SqlType>> {
    // column originates at last element of the path whose second element is not None
    if let Some(pos) = path.iter().rposition(|e| e.1.iter().any(Option::is_some)) {
        let (ni, cols) = &path[pos];

        // We invoked provenance_of with a singleton slice, so must have got
        // results for a single column
        invariant_eq!(cols.len(), 1);

        let source_node = &graph[*ni];
        let source_column_index = cols[0].unwrap();

        if source_node.is_base() {
            let base = source_node.name();
            if let Some(schema) = recipe.schema_for(base) {
                // projected base table column
                match schema {
                    Schema::Table(ref s) => {
                        Ok(Some(s.fields[source_column_index].sql_type.clone()))
                    }
                    _ => internal!("Base node {:?} has non-Table schema: {:?}", ni, schema),
                }
            } else {
                error!(table_name = %base, "no schema for base table");
                Ok(None)
            }
        } else {
            let parent_node_index = path[pos + 1].0;

            type_for_internal_column(
                source_node,
                source_column_index,
                parent_node_index,
                recipe,
                graph,
            )
        }
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
        if let t @ Some(_) = trace_column_type_on_path(p, graph, recipe)? {
            col_type = t;
            col_base = get_base_for_column(p, graph, recipe)?;
        }
    }

    if let Some(col_type) = col_type {
        // found something, so return a ColumnSpecification
        let cs = ColumnSpecification::new(
            Column {
                name: vn.fields()[column_index].to_owned(),
                table: Some(vn.name().to_owned()),
                function: None,
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
