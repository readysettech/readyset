use std::slice;

use dataflow::node::{Column as DfColumn, ColumnOrigin};
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

/// The table column `origin` holds, and whether NULL can appear in it.
fn get_base_for_column(
    origin: &ColumnOrigin,
    recipe: &Recipe,
) -> ReadySetResult<(Option<ColumnBase>, bool)> {
    if let ColumnOrigin::Table(table, col_index) = origin {
        if let Some(Schema::Table(BaseSchema { statement, pg_meta })) = recipe.schema_for(table) {
            let col_index = *col_index;
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
            let nullable = !base.is_not_null();
            return Ok((Some(base), nullable));
        }
    }

    Ok((None, true))
}

/// The table column of a result column, and whether NULL can appear in it.
fn get_base_for_origins(
    column: &DfColumn,
    recipe: &Recipe,
) -> ReadySetResult<(Option<ColumnBase>, bool)> {
    let origins = match column.origin() {
        ColumnOrigin::UnionAll(origins) => origins.as_slice(),
        origin => slice::from_ref(origin),
    };
    let mut bases = vec![];
    let mut nullable = column.outer_nullable();
    for origin in origins {
        let (base, origin_nullable) = get_base_for_column(origin, recipe)?;
        bases.push(base);
        nullable |= origin_nullable;
    }
    let union_all = matches!(column.origin(), ColumnOrigin::UnionAll(_));
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
    let vn = &graph[view];
    let column = &vn.columns()[column_index];
    // A UNION ALL takes the column of its first branch, whose type is unknown when that branch is
    // a NULL, so use the type of a branch that has one
    let paths = match column.origin() {
        ColumnOrigin::UnionAll(_) => provenance_of(graph, view, &[column_index])?,
        _ => vec![],
    };

    let mut col_type = column.ty();
    for p in &paths {
        trace!("considering path {:?}", p);

        let ty = trace_column_type_on_path(p, graph)?;
        if !ty.is_unknown() {
            col_type = ty;
        }
    }

    let (base, nullable) = get_base_for_origins(column, recipe)?;
    Ok(Some(ColumnSchema {
        base,
        nullable,
        zerofill: column.zerofill(),
        display_width: column.display_width(),
        column: Column {
            name: vn.columns()[column_index].name().into(),
            table: Some(vn.name().clone()),
        },
        column_type: col_type.clone(),
    }))
}
