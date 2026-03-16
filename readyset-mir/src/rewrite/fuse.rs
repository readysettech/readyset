use std::mem::replace;

use readyset_errors::{internal, invariant_eq, ReadySetResult};
use readyset_sql::analysis::visit_mut::{self, VisitorMut};
use readyset_sql::ast::{self, BinaryOperator, Expr, Literal};
use readyset_sql::DialectDisplay;
use tracing::trace;

use crate::node::{MirNode, MirNodeInner, ProjectExpr};
use crate::query::MirQuery;
use crate::{Column, NodeIndex};

/// Fuse consecutive pairs of candidate nodes in the query.
/// Returns `true` if at least one pair was fused.
fn fuse_nodes(
    query: &mut MirQuery<'_>,
    is_candidate: impl Fn(&MirNode) -> bool,
    fuse_into: impl Fn(&mut MirNode, MirNode) -> ReadySetResult<()>,
) -> ReadySetResult<bool> {
    let mut nodes_to_fuse: Vec<(NodeIndex, NodeIndex, Option<NodeIndex>)> = vec![];
    for child_ni in query.topo_nodes() {
        let node = query.get_node(child_ni).unwrap();
        if !is_candidate(node) {
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

        if is_candidate(parent) {
            nodes_to_fuse.push((parent_ni, child_ni, None));
        } else if matches!(parent.inner, MirNodeInner::AliasTable { .. }) {
            // Fuse through AliasTable: it produces no dataflow node, so two candidate
            // nodes separated by one become adjacent after lowering.
            let grandparents = query.ancestors(parent_ni)?;
            if grandparents.len() == 1 {
                if let Some(gp) = query.get_node(grandparents[0]) {
                    if is_candidate(gp) {
                        nodes_to_fuse.push((grandparents[0], child_ni, Some(parent_ni)));
                    }
                }
            }
        }
    }

    let mut fused = false;
    for (parent_ni, child_ni, intermediate) in nodes_to_fuse {
        trace!(
            parent_ni = %parent_ni.index(),
            child_ni = %child_ni.index(),
            "Fusing nodes"
        );

        let descendants = query.descendants(parent_ni)?;
        // Calling remove_node has an invariant that there be only one descendant, so filter out
        // any that don't satisfy this
        if descendants.len() > 1 {
            continue;
        }
        if let Some(mid) = intermediate {
            if query.descendants(mid)?.len() > 1 {
                continue;
            }
        }

        // Don't remove a node that is shared with other queries — removing it
        // would invalidate their MIR subgraphs and the `relations` map.
        if query.graph[parent_ni].owners().len() > 1 {
            continue;
        }

        // When fusing through AliasTable, don't fuse if a child's table-qualified
        // Column would match a parent Expr — inlining would lose the table qualifier,
        // breaking downstream column resolution.
        if intermediate.is_some() {
            if let (
                MirNodeInner::Project { emit: parent_emit },
                Some(MirNodeInner::Project { emit: child_emit }),
            ) = (
                &query.graph[parent_ni].inner,
                query.get_node(child_ni).map(|n| &n.inner),
            ) {
                let has_conflict = child_emit.iter().any(|ce| {
                    if let ProjectExpr::Column(Column {
                        table: Some(_),
                        name,
                        ..
                    }) = ce
                    {
                        parent_emit.iter().any(
                            |pe| matches!(pe, ProjectExpr::Expr { alias, .. } if alias == name),
                        )
                    } else {
                        false
                    }
                });
                if has_conflict {
                    continue;
                }
            }
        }

        // Remove only the target parent (grandparent when fusing through AliasTable).
        // The intermediate AliasTable, if any, stays.  It still renames columns for the
        // child and downstream nodes, and produces no dataflow node.
        let parent_node = query.remove_node(parent_ni)?.unwrap();
        let Some(child_node) = query.get_node_mut(child_ni) else {
            continue;
        };

        fuse_into(child_node, parent_node)?;
        fused = true;
    }

    Ok(fused)
}

/// Replace column references in `expr` with the corresponding expression from `parent_emit`.
///
/// For each column reference whose name matches a `ProjectExpr::Expr` alias in the parent,
/// substitutes the parent's expression in place. Plain column pass-throughs (no matching
/// `Expr` in the parent) are left unchanged.
fn inline_expr_references(expr: &mut Expr, parent_emit: &[ProjectExpr]) {
    struct InlineParentExprReferencesVisitor<'a>(&'a [ProjectExpr]);

    impl<'ast> VisitorMut<'ast> for InlineParentExprReferencesVisitor<'_> {
        type Error = std::convert::Infallible;

        fn visit_expr(&mut self, expr: &'ast mut Expr) -> Result<(), Self::Error> {
            if let Expr::Column(ast::Column { name, .. }) = expr {
                if let Some(parent_expr) = self.0.iter().find_map(|expr| match expr {
                    ProjectExpr::Expr { expr, alias } if alias == name => Some(expr.clone()),
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
/// Returns `true` if at least one pair of project nodes was fused.
pub(crate) fn fuse_project_nodes(query: &mut MirQuery<'_>) -> ReadySetResult<bool> {
    fuse_nodes(
        query,
        |node| matches!(node.inner, MirNodeInner::Project { .. }),
        |node, other_node| {
            let other_emit = match other_node.inner {
                MirNodeInner::Project { emit } => emit,
                _ => internal!(),
            };
            let emit = match &mut node.inner {
                MirNodeInner::Project { emit } => emit,
                _ => internal!(),
            };

            for project_expr in emit {
                let name = match project_expr {
                    ProjectExpr::Column(Column { name, .. }) => name,
                    ProjectExpr::Expr { expr, .. } => {
                        inline_expr_references(expr, &other_emit);
                        continue;
                    }
                };
                if let Some(parent_expr) = other_emit.iter().find_map(|expr| match expr {
                    ProjectExpr::Expr { expr, alias } if alias == name => Some(expr.clone()),
                    _ => None,
                }) {
                    trace!(
                        %name,
                        parent_expr = %parent_expr.display(readyset_sql::Dialect::MySQL),
                        "Found column in parent"
                    );
                    *project_expr = ProjectExpr::Expr {
                        expr: parent_expr,
                        alias: name.clone(),
                    }
                }
            }

            Ok(())
        },
    )
}

/// Rewrite the given query to fuse subsequent [`Filter`] nodes into one node, combining conditions
/// using an [`And`] binary op expr
///
/// Given that we don't have any common subexpression analysis, this is essentially always an
/// optimization, as it allows us to avoid extra filter nodes (and extra intermediary result sets!)
/// in the final query graph.
///
/// [`Project`]: MirNodeInner::Project
/// [`And`]: BinaryOperator::And
/// Returns `true` if at least one pair of filter nodes was fused.
pub(crate) fn fuse_filter_nodes(query: &mut MirQuery<'_>) -> ReadySetResult<bool> {
    fuse_nodes(
        query,
        |node| matches!(node.inner, MirNodeInner::Filter { .. }),
        |node, other_node| {
            let cond = match &mut node.inner {
                MirNodeInner::Filter { conditions } => conditions,
                _ => internal!(),
            };
            let other_cond = match other_node.inner {
                MirNodeInner::Filter { conditions } => conditions,
                _ => internal!(),
            };

            *cond = Expr::BinaryOp {
                lhs: Box::new(replace(cond, Expr::Literal(Literal::Null))),
                op: BinaryOperator::And,
                rhs: Box::new(other_cond),
            };

            Ok(())
        },
    )
}

#[cfg(test)]
mod tests {
    use common::IndexType;
    use readyset_client::ViewPlaceholder;
    use readyset_sql::ast::{self, ColumnSpecification, Relation, SqlType};

    use super::*;
    use crate::graph::MirGraph;
    use crate::node::MirNode;

    fn simple_cspec(name: &str) -> ColumnSpecification {
        ColumnSpecification::new(ast::Column::from(name), SqlType::Int(None))
    }

    fn base_node(g: &mut MirGraph, cols: &[&str]) -> NodeIndex {
        g.add_node(MirNode::new(
            "base".into(),
            MirNodeInner::Base {
                column_specs: cols.iter().map(|c| simple_cspec(c)).collect(),
                primary_key: None,
                unique_keys: Default::default(),
            },
        ))
    }

    fn leaf_node(g: &mut MirGraph, key: &str) -> NodeIndex {
        g.add_node(MirNode::new(
            "leaf".into(),
            MirNodeInner::leaf(
                vec![(
                    Column::named(key),
                    ViewPlaceholder::OneToOne(1, BinaryOperator::Equal),
                )],
                IndexType::HashMap,
            ),
        ))
    }

    fn project_node(g: &mut MirGraph, emit: Vec<ProjectExpr>) -> NodeIndex {
        g.add_node(MirNode::new("prj".into(), MirNodeInner::Project { emit }))
    }

    fn alias_node(g: &mut MirGraph, table: &str) -> NodeIndex {
        g.add_node(MirNode::new(
            "alias".into(),
            MirNodeInner::AliasTable {
                table: table.into(),
            },
        ))
    }

    /// Chain nodes with edges: n[0]→n[1]→...→n[last].
    fn chain(g: &mut MirGraph, nodes: &[NodeIndex]) {
        for w in nodes.windows(2) {
            g.add_edge(w[0], w[1], 0);
        }
    }

    fn fuse(g: &mut MirGraph, nodes: &[NodeIndex], leaf: NodeIndex) -> bool {
        let qn: Relation = "q".into();
        for &n in nodes {
            g[n].add_owner(qn.clone());
        }
        let mut query = MirQuery::new(qn, leaf, g);
        fuse_project_nodes(&mut query).expect("fuse failed")
    }

    #[test]
    fn fuse_adjacent_projects_with_table_qualifiers() {
        let mut g = MirGraph::new();
        let base = base_node(&mut g, &["a", "b", "c"]);
        let p1 = project_node(
            &mut g,
            vec![
                ProjectExpr::Column(Column::new(Some("t"), "c")),
                ProjectExpr::Column(Column::new(Some("t"), "a")),
                ProjectExpr::Column(Column::new(Some("t"), "b")),
            ],
        );
        let p2 = project_node(
            &mut g,
            vec![
                ProjectExpr::Column(Column::new(Some("t"), "c")),
                ProjectExpr::Column(Column::new(Some("t"), "a")),
            ],
        );
        let leaf = leaf_node(&mut g, "c");
        chain(&mut g, &[base, p1, p2, leaf]);

        assert!(fuse(&mut g, &[base, p1, p2, leaf], leaf));
        assert!(g.node_weight(p1).is_none());
        let MirNodeInner::Project { emit } = &g[p2].inner else {
            panic!("expected Project");
        };
        assert_eq!(emit.len(), 2);
    }

    #[test]
    fn fuse_through_alias_table() {
        // base → p1(reorder) → alias("t") → p2(subset) → leaf
        // After fuse: base → alias("t") → p2 → leaf, p1 removed.
        let mut g = MirGraph::new();
        let base = base_node(&mut g, &["a", "b", "c"]);
        let p1 = project_node(
            &mut g,
            vec![
                ProjectExpr::Column(Column::named("c")),
                ProjectExpr::Column(Column::named("a")),
                ProjectExpr::Column(Column::named("b")),
            ],
        );
        let alias = alias_node(&mut g, "t");
        let p2 = project_node(
            &mut g,
            vec![
                ProjectExpr::Column(Column::new(Some("t"), "c")),
                ProjectExpr::Column(Column::new(Some("t"), "a")),
            ],
        );
        let leaf = leaf_node(&mut g, "c");
        chain(&mut g, &[base, p1, alias, p2, leaf]);

        assert!(fuse(&mut g, &[base, p1, alias, p2, leaf], leaf));
        // p1 removed, alias stays
        assert!(g.node_weight(p1).is_none());
        assert!(g.node_weight(alias).is_some());
        let MirNodeInner::Project { emit } = &g[p2].inner else {
            panic!("expected Project");
        };
        assert_eq!(emit.len(), 2);
    }

    #[test]
    fn no_fuse_through_alias_when_grandparent_has_multiple_descendants() {
        // base → p1 → alias → p2 → leaf
        //           ↘ other
        let mut g = MirGraph::new();
        let base = base_node(&mut g, &["a", "b"]);
        let p1 = project_node(
            &mut g,
            vec![
                ProjectExpr::Column(Column::named("a")),
                ProjectExpr::Column(Column::named("b")),
            ],
        );
        let alias = alias_node(&mut g, "t");
        let p2 = project_node(
            &mut g,
            vec![ProjectExpr::Column(Column::new(Some("t"), "a"))],
        );
        let other = leaf_node(&mut g, "a");
        let leaf = leaf_node(&mut g, "a");
        chain(&mut g, &[base, p1, alias, p2, leaf]);
        g.add_edge(p1, other, 1);

        assert!(!fuse(&mut g, &[base, p1, alias, p2, other, leaf], leaf));
        assert!(g.node_weight(p1).is_some());
    }

    #[test]
    fn no_fuse_through_alias_when_expr_would_lose_table_qualifier() {
        // base → p1(Expr{Literal(1), "present_"}) → alias("NP_3VL")
        //      → p2(Column{table: Some("NP_3VL"), name: "present_"}) → leaf
        // Fusion must NOT happen: inlining would lose the table qualifier.
        let mut g = MirGraph::new();
        let base = base_node(&mut g, &["a"]);
        let p1 = project_node(
            &mut g,
            vec![ProjectExpr::Expr {
                expr: Expr::Literal(Literal::Integer(1)),
                alias: "present_".into(),
            }],
        );
        let alias = alias_node(&mut g, "NP_3VL");
        let p2 = project_node(
            &mut g,
            vec![ProjectExpr::Column(Column::new(Some("NP_3VL"), "present_"))],
        );
        let leaf = leaf_node(&mut g, "present_");
        chain(&mut g, &[base, p1, alias, p2, leaf]);

        assert!(!fuse(&mut g, &[base, p1, alias, p2, leaf], leaf));
        assert!(g.node_weight(p1).is_some(), "p1 should NOT be removed");
    }

    #[test]
    fn no_fuse_through_alias_when_alias_has_multiple_descendants() {
        // base → p1 → alias → p2 → leaf
        //                    ↘ other
        let mut g = MirGraph::new();
        let base = base_node(&mut g, &["a", "b"]);
        let p1 = project_node(
            &mut g,
            vec![
                ProjectExpr::Column(Column::named("a")),
                ProjectExpr::Column(Column::named("b")),
            ],
        );
        let alias = alias_node(&mut g, "t");
        let p2 = project_node(
            &mut g,
            vec![ProjectExpr::Column(Column::new(Some("t"), "a"))],
        );
        let other = leaf_node(&mut g, "a");
        let leaf = leaf_node(&mut g, "a");
        chain(&mut g, &[base, p1, alias, p2, leaf]);
        g.add_edge(alias, other, 1);

        assert!(!fuse(&mut g, &[base, p1, alias, p2, other, leaf], leaf));
        assert!(g.node_weight(p1).is_some());
    }
}
