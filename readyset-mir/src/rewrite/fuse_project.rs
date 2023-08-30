use nom_sql::analysis::visit_mut::{self, VisitorMut};
use nom_sql::Expr;
use readyset_errors::{internal, invariant_eq, ReadySetResult};
use tracing::trace;

use crate::node::{MirNodeInner, ProjectExpr};
use crate::query::MirQuery;
use crate::Column;

fn inline_parent_expr_references(expr: &mut Expr, parent_emit: &[ProjectExpr]) {
    struct InlineParentExprReferencesVisitor<'a>(&'a [ProjectExpr]);
    impl<'a, 'ast> VisitorMut<'ast> for InlineParentExprReferencesVisitor<'a> {
        type Error = !;

        fn visit_expr(&mut self, expr: &'ast mut Expr) -> Result<(), Self::Error> {
            if let Expr::Column(nom_sql::Column { name, table: None }) = expr {
                let name = name.clone();
                if let Some(parent_expr) = self.0.iter().find_map(|expr| match expr {
                    ProjectExpr::Expr { expr, alias } if *alias == name => Some(expr.clone()),
                    _ => None,
                }) {
                    *expr = parent_expr;
                }
                Ok(())
            } else {
                visit_mut::walk_expr(self, expr)
            }
        }
    }

    let Ok(()) = InlineParentExprReferencesVisitor(parent_emit).visit_expr(expr);
}

/// Rewrite the given query to fuse subsequent [`Project`] nodes into one node, inlining
/// expressions into column references
///
/// Given that we don't have any common subexpression analysis, this is essentially always an
/// optimization, as it allows us to avoid extra project nodes (and extra intermediary result sets!)
/// in the final query graph.
///
/// [`Project`]: MirNodeInner::Project
pub(crate) fn fuse_project_nodes(query: &mut MirQuery<'_>) -> ReadySetResult<()> {
    let mut projects_to_fuse = vec![];
    for child_ni in query.topo_nodes() {
        let node = query.get_node(child_ni).unwrap();
        if !matches!(node.inner, MirNodeInner::Project { .. }) {
            continue;
        }

        let ancestors = query.ancestors(child_ni)?;
        if ancestors.is_empty() {
            // This node's ancestor might not be in the same query! If so, there's no fusing we can
            // do (since we can't remove existing nodes)
            continue;
        }
        invariant_eq!(
            ancestors.len(),
            1,
            "Project nodes can only have one parent (node: {})",
            child_ni.index()
        );
        let parent_ni = ancestors[0];
        let parent = query.get_node(parent_ni).unwrap();
        // TODO(aspen): Fuse through AliasTable?
        if !matches!(parent.inner, MirNodeInner::Project { .. }) {
            continue;
        }

        projects_to_fuse.push((parent_ni, child_ni));
    }

    for (parent_ni, child_ni) in projects_to_fuse {
        trace!(
            parent_ni = %parent_ni.index(),
            child_ni = %child_ni.index(),
            "Fusing project nodes"
        );
        let parent_emit = match query.remove_node(parent_ni)?.unwrap().inner {
            MirNodeInner::Project { emit } => emit,
            _ => internal!(),
        };
        let Some(child_node) = query.get_node_mut(child_ni) else {
            continue
        };
        let emit = match &mut child_node.inner {
            MirNodeInner::Project { emit } => emit,
            _ => internal!(),
        };

        for project_expr in emit {
            let name = match project_expr {
                ProjectExpr::Column(Column {
                    name, table: None, ..
                }) => name.clone(),
                ProjectExpr::Expr { expr, .. } => {
                    inline_parent_expr_references(expr, &parent_emit);
                    continue;
                }
                ProjectExpr::Column(Column { table: Some(_), .. }) => continue,
            };
            trace!(
                parent_ni = %parent_ni.index(),
                child_ni = %child_ni.index(),
                %name,
                "Trying to find column in parent"
            );

            if let Some(parent_expr) = parent_emit.iter().find_map(|expr| match expr {
                ProjectExpr::Expr { expr, alias } if *alias == name => Some(expr.clone()),
                _ => None,
            }) {
                trace!(
                    %name,
                    parent_expr = %parent_expr.display(nom_sql::Dialect::MySQL),
                    "Found column in parent"
                );
                *project_expr = ProjectExpr::Expr {
                    expr: parent_expr,
                    alias: name,
                }
            }
        }
    }

    Ok(())
}
