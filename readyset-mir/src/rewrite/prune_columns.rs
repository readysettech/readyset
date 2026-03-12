//! Demand-driven column pruning for a single MirQuery.
//!
//! Walks from the leaf toward roots, removing columns from join `project` lists
//! and projection `emit` lists that no downstream node demands. Also eliminates
//! identity projections whose output matches the parent exactly.

use readyset_errors::ReadySetResult;
use readyset_sql::analysis::ReferredColumns;
use readyset_sql::ast::Expr;
use tracing::trace;

use crate::node::node_inner::{MirNodeInner, ProjectExpr};
use crate::query::MirQuery;
use crate::{Column, NodeIndex};

/// Union of `referenced_columns(child)` for every immediate downstream child.
///
/// AliasTable nodes rename column tables, which breaks Column equality against
/// the parent's namespace. Since AliasTable is a full pass-through, we demand
/// all of the parent's own output columns instead.
fn compute_demand(query: &MirQuery<'_>, ni: NodeIndex) -> ReadySetResult<Vec<Column>> {
    let mut demanded: Vec<Column> = vec![];
    for &child in &query.descendants(ni)? {
        for col in query.graph.referenced_columns(child) {
            if !demanded.contains(&col) {
                demanded.push(col);
            }
        }
    }
    Ok(demanded)
}

/// Columns a join must keep regardless of demand: on-keys + left_local_preds refs.
fn join_required_columns(on: &[(Column, Column)], preds: &[Expr]) -> Vec<Column> {
    let mut req: Vec<Column> = on
        .iter()
        .flat_map(|(l, r)| [l.clone(), r.clone()])
        .collect();
    for pred in preds {
        for c in pred.referred_columns() {
            let mc = Column::from(c.clone());
            if !req.contains(&mc) {
                req.push(mc);
            }
        }
    }
    req
}

/// Keep only demanded + required columns from `project`.
/// Returns `Some(pruned)` if anything was removed, `None` if unchanged.
fn pruned_project(
    project: &[Column],
    demanded: &[Column],
    required: &[Column],
) -> Option<Vec<Column>> {
    let new: Vec<Column> = project
        .iter()
        .filter(|c| demanded.contains(c) || required.contains(c))
        .cloned()
        .collect();
    (new.len() < project.len()).then_some(new)
}

/// True if the Project at `ni` emits the same columns in the same order as its
/// single parent (all Column variants, no expressions).
fn is_identity_project(query: &MirQuery<'_>, ni: NodeIndex) -> bool {
    let Some(node) = query.get_node(ni) else {
        return false;
    };
    let MirNodeInner::Project { emit } = &node.inner else {
        return false;
    };
    let Ok(ancestors) = query.ancestors(ni) else {
        return false;
    };
    if ancestors.len() != 1 {
        return false;
    }
    let parent_cols = query.graph.columns(ancestors[0]);
    emit.len() == parent_cols.len()
        && emit
            .iter()
            .zip(parent_cols.iter())
            // Compare by name only: Column::eq's alias matching can falsely
            // equate renamed join columns (e.g. votes.story == stories.id),
            // while name+table rejects view aliases (v1.a vs t1.a) that ARE
            // identity passes after AliasTable inlining.
            .all(|(e, pc)| matches!(e, ProjectExpr::Column(c) if c.name == pc.name))
}

/// Try to prune a join node's `project` list in place. Returns `true` if changed.
fn try_prune_join(inner: &mut MirNodeInner, demanded: &[Column]) -> bool {
    let (on, project, preds) = match inner {
        MirNodeInner::Join { on, project } | MirNodeInner::DependentJoin { on, project } => {
            (on as &_, project as &_, &[][..])
        }
        MirNodeInner::LeftJoin {
            on,
            project,
            left_local_preds,
        }
        | MirNodeInner::DependentLeftJoin {
            on,
            project,
            left_local_preds,
        } => (on as &_, project as &_, left_local_preds.as_slice()),
        _ => return false,
    };
    let required = join_required_columns(on, preds);
    let Some(new) = pruned_project(project, demanded, &required) else {
        return false;
    };
    match inner {
        MirNodeInner::Join { project, .. }
        | MirNodeInner::LeftJoin { project, .. }
        | MirNodeInner::DependentJoin { project, .. }
        | MirNodeInner::DependentLeftJoin { project, .. } => *project = new,
        _ => unreachable!(),
    }
    true
}

/// Try to prune a Project node's `emit` list in place. Returns `true` if changed.
fn try_prune_emit(inner: &mut MirNodeInner, demanded: &[Column]) -> bool {
    let MirNodeInner::Project { emit } = inner else {
        return false;
    };
    let before = emit.len();
    emit.retain(|e| {
        let col = match e {
            ProjectExpr::Column(c) => c.clone(),
            ProjectExpr::Expr { alias, .. } => Column::named(alias),
        };
        demanded.contains(&col)
    });
    emit.len() < before
}

/// Prune unused columns and eliminate identity projections.
/// Returns `true` if any change was made.
pub(crate) fn prune_unused_columns(query: &mut MirQuery<'_>) -> ReadySetResult<bool> {
    let mut changed = false;

    // Demand-driven pruning, leaf-to-root so children are pruned before parents.
    for &ni in query.topo_nodes().iter().rev() {
        if query.descendants(ni)?.is_empty() {
            continue;
        }
        let demanded = compute_demand(query, ni)?;

        // get_node returns None for nodes not owned by this query (e.g. shared
        // base tables) — that's expected, not an error.
        if query.get_node(ni).is_none() {
            continue;
        }

        let inner = &mut query.graph[ni].inner;
        changed |= try_prune_join(inner, &demanded) | try_prune_emit(inner, &demanded);
    }

    // Identity projection elimination.
    let identities: Vec<NodeIndex> = query
        .topo_nodes()
        .into_iter()
        // Skip the query's leaf node: view queries (e.g. `SELECT a, b FROM t1`)
        // use their final projection as the leaf, and removing it breaks
        // MIR-to-dataflow conversion ("Leaf must have a dataflow node assigned").
        .filter(|&ni| ni != query.leaf() && is_identity_project(query, ni))
        .collect();
    for ni in identities {
        if query.get_node(ni).is_none() {
            continue;
        }
        trace!(node = %ni.index(), "Removing identity projection");
        query.remove_node(ni)?;
        changed = true;
    }

    Ok(changed)
}

#[cfg(test)]
mod tests {
    use common::IndexType;
    use readyset_client::ViewPlaceholder;
    use readyset_sql::ast::{self, BinaryOperator, ColumnSpecification, Relation, SqlType};

    use super::*;
    use crate::graph::MirGraph;
    use crate::node::MirNode;

    fn cspec(table: &str, name: &str) -> ColumnSpecification {
        ColumnSpecification::new(
            ast::Column {
                table: Some(table.into()),
                name: name.into(),
            },
            SqlType::Int(None),
        )
    }

    fn simple_cspec(name: &str) -> ColumnSpecification {
        ColumnSpecification::new(ast::Column::from(name), SqlType::Int(None))
    }

    /// Add all `nodes` to query ownership, build MirQuery, run prune.
    fn run_prune(g: &mut MirGraph, nodes: &[NodeIndex], leaf: NodeIndex) -> bool {
        let qn: Relation = "q".into();
        for &n in nodes {
            g[n].add_owner(qn.clone());
        }
        let mut query = MirQuery::new(qn, leaf, g);
        prune_unused_columns(&mut query).expect("prune failed")
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

    fn base_node(g: &mut MirGraph, name: &str, specs: Vec<ColumnSpecification>) -> NodeIndex {
        g.add_node(MirNode::new(
            name.into(),
            MirNodeInner::Base {
                column_specs: specs,
                primary_key: None,
                unique_keys: Default::default(),
            },
        ))
    }

    /// base([a, b]) → project(emit) → leaf(key)
    /// Returns (base, prj, leaf).
    fn base_project_leaf(
        g: &mut MirGraph,
        emit: Vec<ProjectExpr>,
        key: &str,
    ) -> (NodeIndex, NodeIndex, NodeIndex) {
        let base = base_node(g, "base", vec![simple_cspec("a"), simple_cspec("b")]);
        let prj = g.add_node(MirNode::new("prj".into(), MirNodeInner::Project { emit }));
        let leaf = leaf_node(g, key);
        g.add_edge(base, prj, 0);
        g.add_edge(prj, leaf, 0);
        (base, prj, leaf)
    }

    #[test]
    fn eliminate_identity_projection() {
        let mut g = MirGraph::new();
        let (base, prj, leaf) = base_project_leaf(
            &mut g,
            vec![
                ProjectExpr::Column(Column::named("a")),
                ProjectExpr::Column(Column::named("b")),
            ],
            "a",
        );

        let changed = run_prune(&mut g, &[base, prj, leaf], leaf);
        assert!(changed);
        assert!(g.node_weight(prj).is_none());
    }

    #[test]
    fn keep_reordering_projection() {
        let mut g = MirGraph::new();
        let (base, prj, leaf) = base_project_leaf(
            &mut g,
            vec![
                ProjectExpr::Column(Column::named("b")),
                ProjectExpr::Column(Column::named("a")),
            ],
            "b",
        );

        let changed = run_prune(&mut g, &[base, prj, leaf], leaf);
        assert!(!changed);
        let MirNodeInner::Project { emit } = &g[prj].inner else {
            panic!("expected Project");
        };
        assert_eq!(emit.len(), 2);
        assert!(matches!(&emit[0], ProjectExpr::Column(c) if *c == Column::named("b")));
        assert!(matches!(&emit[1], ProjectExpr::Column(c) if *c == Column::named("a")));
    }

    #[test]
    fn prune_join_excess_columns() {
        let mut g = MirGraph::new();
        let left = base_node(
            &mut g,
            "left",
            vec![cspec("l", "id"), cspec("l", "a"), cspec("l", "b")],
        );
        let right = base_node(
            &mut g,
            "right",
            vec![cspec("r", "id"), cspec("r", "x"), cspec("r", "y")],
        );
        let join = g.add_node(MirNode::new(
            "join".into(),
            MirNodeInner::Join {
                on: vec![(Column::new(Some("l"), "id"), Column::new(Some("r"), "id"))],
                project: vec![
                    Column::new(Some("l"), "id"),
                    Column::new(Some("l"), "a"),
                    Column::new(Some("l"), "b"),
                    Column::new(Some("r"), "id"),
                    Column::new(Some("r"), "x"),
                    Column::new(Some("r"), "y"),
                ],
            },
        ));
        g.add_edge(left, join, 0);
        g.add_edge(right, join, 1);
        let prj = g.add_node(MirNode::new(
            "prj".into(),
            MirNodeInner::Project {
                emit: vec![
                    ProjectExpr::Column(Column::new(Some("l"), "a")),
                    ProjectExpr::Column(Column::new(Some("r"), "x")),
                ],
            },
        ));
        g.add_edge(join, prj, 0);
        let leaf = leaf_node(&mut g, "a");
        g.add_edge(prj, leaf, 0);

        let changed = run_prune(&mut g, &[left, right, join, prj, leaf], leaf);
        assert!(changed);

        let MirNodeInner::Join { project, .. } = &g[join].inner else {
            panic!("expected Join");
        };
        assert_eq!(
            *project,
            vec![
                Column::new(Some("l"), "id"),
                Column::new(Some("l"), "a"),
                Column::new(Some("r"), "id"),
                Column::new(Some("r"), "x"),
            ]
        );
    }
}
