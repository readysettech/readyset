//! Reject a join when a lookup into one input resolves where another input's delta also passes.
//!
//! A base applies a write to its own state before dispatching to any of its children, so when one
//! input of a join reads it directly, both inputs' deltas look up a base that already holds the
//! write and the pair they form is emitted twice (REA-6138, ENG-411).  Until the runtime can
//! compensate, the only safe answer is to refuse to cache these queries.
//!
//! A node that keeps state of its own takes the write only when the write is dispatched to it, so
//! a lookup stopping there resolves at a node the write has not reached yet.  That is the whole
//! rule: the join's key is never read.  A cross join and an inequality join are admitted because
//! `add_bogokey_if_necessary` gives a keyless join a literal-carrying projection above each
//! input, and a projection carrying any expression keeps state of its own.
//!
//! The graph has to be the finished one, after decorrelation and the other rewrites have built
//! the joins they build.

use std::collections::HashSet;

use itertools::Itertools;
use petgraph::visit::{EdgeRef, IntoNeighbors, Reversed};
use petgraph::Direction;
use readyset_errors::{internal, unsupported, ReadySetResult};
use tracing::trace;

use crate::node::node_inner::ProjectExpr;
use crate::node::MirNodeInner;
use crate::query::MirQuery;
use crate::NodeIndex;

/// Whether a lookup into this node is hoisted past it, or resolves at state it keeps itself.
///
/// This mirrors [`Ingredient::can_query_through`], which is what the materialization planner
/// consults when it decides how far up an index obligation travels.  Only a filter and a
/// projection of plain columns are hoisted through; every other operator is given state of its
/// own, so a lookup stops there rather than reading whatever lies below.
///
/// Listed exhaustively on purpose.  Answering `false` says the node keeps state, which stops the
/// walk and admits the join, so a variant added later must not reach a default: a new node that
/// lowers to nothing, or to an operator that is query-through, would silently admit a shared base
/// read through it.  Adding a variant should break this build.
///
/// [`Ingredient::can_query_through`]: dataflow::prelude::Ingredient::can_query_through
fn hoisted_past(inner: &MirNodeInner) -> bool {
    match inner {
        // Lowers to no dataflow node at all, so there is nothing for a lookup to stop at.
        MirNodeInner::AliasTable { .. } => true,
        MirNodeInner::Filter { .. } => true,
        // A leaf belongs to another query: `CREATE CACHE` lowers with `LeafBehavior::Leaf`, and a
        // query naming that cache joins to its leaf.  Stopping here would hide the base the two
        // caches share, so the walk steps through to what feeds it.
        MirNodeInner::Leaf { .. } => true,
        MirNodeInner::Project { emit } => emit.iter().all(|e| matches!(e, ProjectExpr::Column(_))),
        // Each of these lowers to an operator taking the `can_query_through` default of `false`.
        MirNodeInner::Accumulator { .. }
        | MirNodeInner::Aggregation { .. }
        | MirNodeInner::Base { .. }
        | MirNodeInner::Constant { .. }
        | MirNodeInner::DependentJoin { .. }
        | MirNodeInner::DependentLeftJoin { .. }
        | MirNodeInner::Distinct { .. }
        | MirNodeInner::Extremum { .. }
        | MirNodeInner::Identity
        | MirNodeInner::Join { .. }
        | MirNodeInner::JoinAggregates { .. }
        | MirNodeInner::LeftJoin { .. }
        | MirNodeInner::Paginate { .. }
        | MirNodeInner::TopK { .. }
        | MirNodeInner::Union { .. }
        | MirNodeInner::ViewKey { .. }
        | MirNodeInner::Window { .. } => false,
    }
}

/// What one input of a join reads, as the join reads it.
#[derive(Debug)]
struct Reads {
    /// The nodes a lookup into this input can resolve at: the first node on each path up that
    /// keeps state of its own.  A write is visible here only once that node has processed it.
    stops: HashSet<NodeIndex>,
    /// Every node above this input, whether or not a lookup reaches it.  A write anywhere in
    /// here arrives at the join as a delta on this input.
    reaches: HashSet<NodeIndex>,
}

/// Collect both sets for one input of a join.
///
/// The walk ignores query ownership: a query that reads from a view or another cached query owns
/// only that relation's leaf node, and what lies behind it still feeds the join.
fn reads_from(query: &MirQuery<'_>, node: NodeIndex) -> Reads {
    let graph = Reversed(&**query.graph);
    let mut stack = vec![(node, true)];
    let mut seen = HashSet::new();
    let mut reads = Reads {
        stops: HashSet::new(),
        reaches: HashSet::new(),
    };
    while let Some((n, hoisted)) = stack.pop() {
        if !seen.insert((n, hoisted)) {
            continue;
        }
        reads.reaches.insert(n);
        // A lookup travelling up stops at the first node holding state of its own; above that
        // the walk continues only to collect what a delta can still arrive from.
        let stops_here = hoisted && !hoisted_past(&query.graph[n].inner);
        if stops_here {
            reads.stops.insert(n);
        }
        stack.extend(graph.neighbors(n).map(|p| (p, hoisted && !stops_here)));
    }
    reads
}

pub(crate) fn reject_self_joins(query: &MirQuery<'_>) -> ReadySetResult<()> {
    for node in query.topo_nodes() {
        if !matches!(
            &query.graph[node].inner,
            MirNodeInner::Join { .. }
                | MirNodeInner::LeftJoin { .. }
                | MirNodeInner::JoinAggregates { .. }
                | MirNodeInner::DependentJoin { .. }
                | MirNodeInner::DependentLeftJoin { .. }
        ) {
            continue;
        }
        // Lowering accepts exactly two inputs for a join, and `make_join` wires exactly two for
        // every kind matched above, so anything else is a graph the compiler does not build.
        // The edges are read unfiltered: `reads_from` ignores ownership too, and an input
        // dropped for belonging to another query would leave the join half-examined.
        let parents: Vec<NodeIndex> = query
            .graph
            .edges_directed(node, Direction::Incoming)
            .sorted_by_key(|e| e.weight())
            .map(|e| e.source())
            .collect();
        let [left, right] = parents[..] else {
            internal!("join has {} inputs, expected 2", parents.len());
        };
        // A write reaches a node once, and that node materializes it before passing it on, so a
        // lookup resolving there sees the write in full.  Where one input's lookup resolves at a
        // node the other input's delta also passes through, both directions of the join can match
        // the write against itself and the pairs are counted twice.  Where each input's lookup
        // resolves somewhere the other never touches, every delta is matched against state the
        // write has not reached yet, which is what makes an ordinary incremental join correct.
        //
        // The question is asked both ways round: it is not symmetric, since it asks of one input
        // where its lookup lands and of the other only what it passes through.
        let (l, r) = (reads_from(query, left), reads_from(query, right));
        if let Some(&shared) = l
            .stops
            .intersection(&r.reaches)
            .chain(r.stops.intersection(&l.reaches))
            .next()
        {
            trace!(?node, ?l, ?r, "Refusing a join whose inputs share a lookup");
            // Named by the relation rather than the index, so the message is stable across runs
            // and across unrelated migrations that shift node numbering.
            let name = query.graph[shared].name().clone();
            if query.graph[shared].is_base() {
                unsupported!(
                    "Base table {} reaches both sides of a join",
                    name.display_unquoted()
                );
            }
            unsupported!(
                "Relation {} reaches both sides of a join",
                name.display_unquoted()
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::iter;

    use common::IndexType;
    use dataflow::ops::grouped::aggregate::Aggregation;
    use readyset_sql::ast::{self, ColumnSpecification, Relation, SqlType};

    use super::*;
    use crate::graph::MirGraph;
    use crate::node::node_inner::ProjectExpr;
    use crate::node::MirNode;
    use crate::Column;

    /// One join key, as `((left table, left column), (right table, right column))`.
    type QualifiedKey = ((&'static str, &'static str), (&'static str, &'static str));

    /// What a row builds.  One variant per node kind the tests need.
    #[derive(Clone, Copy)]
    enum Kind {
        Base,
        /// A base `q` does not own, as behind a view.
        ForeignBase,
        Alias,
        /// A projection of plain columns, which a lookup is hoisted past.
        Passthrough,
        /// `Passthrough` owned by a view as well as by `q`, as a view's leaf is.
        ForeignPassthrough,
        /// A leaf owned by another query as well as by `q`, as a cache's leaf is.
        ForeignLeaf,
        /// The projection `add_bogokey_if_necessary` puts above each parent of a keyless join:
        /// the parent's columns plus a literal.
        BogoProject,
        Agg,
        UngroupedAgg,
        Distinct,
        TopK,
        Join,
        /// A join keyed on the given column pairs, each `(left, right)` unqualified.
        JoinOn(&'static [(&'static str, &'static str)]),
        /// A join keyed on qualified columns.
        JoinOnQualified(&'static [QualifiedKey]),
        LeftJoin,
        JoinAggregates,
        DependentJoin,
    }

    use Kind::*;

    impl Kind {
        /// The node a row of this kind builds.  `parent_columns` is only read by `BogoProject`,
        /// which projects what its parent emits.
        fn inner(self, name: &str, parent_columns: Vec<Column>) -> MirNodeInner {
            match self {
                Base | ForeignBase => MirNodeInner::Base {
                    column_specs: vec![ColumnSpecification {
                        column: ast::Column::from(format!("{name}.x").as_str()),
                        sql_type: SqlType::Int(None),
                        generated: None,
                        constraints: vec![],
                        comment: None,
                        invisible: false,
                    }],
                    primary_key: Some([Column::new(Some(name), "x")].into()),
                    unique_keys: Default::default(),
                },
                Alias => MirNodeInner::AliasTable { table: name.into() },
                Passthrough | ForeignPassthrough => MirNodeInner::Project {
                    emit: vec![ProjectExpr::Column(Column::named("x"))],
                },
                ForeignLeaf => MirNodeInner::leaf(vec![], IndexType::HashMap),
                BogoProject => MirNodeInner::Project {
                    emit: parent_columns
                        .into_iter()
                        .map(ProjectExpr::Column)
                        .chain(iter::once(ProjectExpr::Expr {
                            expr: ast::Expr::Literal(0.into()),
                            alias: "bogokey".into(),
                        }))
                        .collect(),
                },
                Agg => MirNodeInner::Aggregation {
                    on: Column::named("x"),
                    group_by: vec![Column::named("g")],
                    output_column: Column::named(name),
                    kind: Aggregation::Sum,
                },
                UngroupedAgg => MirNodeInner::Aggregation {
                    on: Column::named("x"),
                    group_by: vec![],
                    output_column: Column::named(name),
                    kind: Aggregation::Sum,
                },
                Distinct => MirNodeInner::Distinct {
                    group_by: vec![Column::named("g")],
                },
                TopK => MirNodeInner::TopK {
                    order: vec![],
                    group_by: vec![Column::named("g")],
                    limit: 3,
                    topk_buffer_multiplier: None,
                    query_name: "q".into(),
                },
                Join => JoinOn(&[("x", "x")]).inner(name, vec![]),
                JoinOn(on) => MirNodeInner::Join {
                    on: on
                        .iter()
                        .map(|(l, r)| (Column::named(*l), Column::named(*r)))
                        .collect(),
                    project: vec![],
                },
                JoinOnQualified(on) => MirNodeInner::Join {
                    on: on
                        .iter()
                        .map(|((lt, lc), (rt, rc))| {
                            (Column::new(Some(*lt), *lc), Column::new(Some(*rt), *rc))
                        })
                        .collect(),
                    project: vec![],
                },
                LeftJoin => MirNodeInner::LeftJoin {
                    on: vec![(Column::named("x"), Column::named("x"))],
                    project: vec![],
                    left_local_preds: vec![],
                },
                JoinAggregates => MirNodeInner::JoinAggregates {
                    group_by: vec![Column::named("g")],
                },
                DependentJoin => MirNodeInner::DependentJoin {
                    // A key, so the join this becomes is an equi-join and still in scope.
                    on: vec![(Column::named("x"), Column::named("x"))],
                    project: vec![],
                },
            }
        }
    }

    /// Rows of `(name, kind, parents)`, each parent named by an earlier row.
    type Rows<'a> = &'a [(&'a str, Kind, &'a [&'a str])];

    /// Builds query `q` from `rows`, with the last row feeding its leaf.  Every node is owned by
    /// `q` except a `Foreign*` one, which belongs to a query named after the row and is owned by
    /// `q` as well only where a reader would own it.
    fn build(rows: Rows<'_>) -> (MirGraph, NodeIndex) {
        let mut graph = MirGraph::new();
        let q = Relation::from("q");
        let mut by_name: Vec<(&str, NodeIndex)> = Vec::new();
        let mut last = None;
        for (name, kind, parents) in rows {
            let parent_columns = match (kind, parents.first()) {
                (BogoProject, Some(first)) => {
                    let p = by_name
                        .iter()
                        .find(|(n, _)| n == first)
                        .unwrap_or_else(|| panic!("unknown parent {first}"))
                        .1;
                    graph.columns(p)
                }
                _ => vec![],
            };
            let n = graph.add_node(MirNode::new(
                (*name).into(),
                kind.inner(name, parent_columns),
            ));
            match kind {
                ForeignBase => {
                    graph[n].add_owner(Relation::from(*name));
                }
                ForeignPassthrough | ForeignLeaf => {
                    graph[n].add_owner(Relation::from(*name));
                    graph[n].add_owner(q.clone());
                }
                _ => {
                    graph[n].add_owner(q.clone());
                }
            }
            for (i, parent) in parents.iter().enumerate() {
                let p = by_name
                    .iter()
                    .find(|(n, _)| n == parent)
                    .unwrap_or_else(|| panic!("unknown parent {parent}"))
                    .1;
                graph.add_edge(p, n, i);
            }
            by_name.push((name, n));
            last = Some(n);
        }
        let top = last.expect("at least one row");
        let leaf = graph.add_node(MirNode::new(
            q.clone(),
            MirNodeInner::leaf(vec![], IndexType::HashMap),
        ));
        graph[leaf].add_owner(q);
        graph.add_edge(top, leaf, 0);
        (graph, leaf)
    }

    fn check(rows: Rows<'_>) -> ReadySetResult<()> {
        let (mut graph, leaf) = build(rows);
        reject_self_joins(&MirQuery::new("q".into(), leaf, &mut graph))
    }

    /// Asserts the graph is refused, and returns the message.
    #[track_caller]
    fn refused(rows: Rows<'_>) -> String {
        let err = check(rows).expect_err("expected a refusal");
        assert!(err.is_unsupported(), "got {err:?}");
        err.to_string()
    }

    #[track_caller]
    fn allowed(rows: Rows<'_>) {
        check(rows).expect("expected the join to be allowed");
    }

    #[test]
    fn rejects_basic_self_join() {
        refused(&[
            ("t", Base, &[]),
            ("l", Alias, &["t"]),
            ("r", Alias, &["t"]),
            ("j", Join, &["l", "r"]),
        ]);
    }

    #[test]
    fn rejects_left_join_shared_base() {
        refused(&[
            ("t", Base, &[]),
            ("l", Alias, &["t"]),
            ("r", Alias, &["t"]),
            ("j", LeftJoin, &["l", "r"]),
        ]);
    }

    #[test]
    fn rejects_self_join_through_project_chain() {
        refused(&[
            ("t", Base, &[]),
            ("lp", Passthrough, &["t"]),
            ("l", Alias, &["lp"]),
            ("rp", Passthrough, &["t"]),
            ("r", Alias, &["rp"]),
            ("j", Join, &["l", "r"]),
        ]);
    }

    /// What a cross join looks like by the time this pass runs: `add_bogokey_if_necessary` has
    /// given each input a literal-carrying projection, which keeps state of its own.
    #[test]
    fn allows_bogokey_join_over_a_shared_base() {
        allowed(&[
            ("t", Base, &[]),
            ("ta", Alias, &["t"]),
            ("tb", Alias, &["t"]),
            ("a", BogoProject, &["ta"]),
            ("b", BogoProject, &["tb"]),
            ("j", JoinOn(&[("bogokey", "bogokey")]), &["a", "b"]),
        ]);
    }

    /// The same admission reached from SQL rather than from `add_bogokey_if_necessary`: a
    /// derived table projecting a literal beside its columns stops a lookup just as well.
    #[test]
    fn allows_a_shared_base_behind_a_literal_carrying_projection() {
        allowed(&[
            ("t", Base, &[]),
            ("a", BogoProject, &["t"]),
            ("b", BogoProject, &["t"]),
            ("j", Join, &["a", "b"]),
        ]);
    }

    /// Where the shared node is not a base there is no table to name, so the message has to name
    /// the relation instead.
    #[test]
    fn names_a_shared_relation_when_the_shared_node_is_not_a_base() {
        let msg = refused(&[
            ("t", Base, &[]),
            ("counts", Agg, &["t"]),
            ("ca", Alias, &["counts"]),
            ("cb", Alias, &["counts"]),
            ("j", Join, &["ca", "cb"]),
        ]);
        assert!(
            msg.contains("Relation counts"),
            "a shared node that is not a base is named as a relation, got {msg}"
        );
    }

    /// The key is never read, so a user column that happens to be called `bogokey` buys nothing:
    /// what admits a cross join is the projection carrying the literal, not the name.
    #[test]
    fn rejects_join_on_a_user_column_named_bogokey() {
        refused(&[
            ("t", Base, &[]),
            ("ta", Alias, &["t"]),
            ("tb", Alias, &["t"]),
            ("j", JoinOn(&[("bogokey", "bogokey")]), &["ta", "tb"]),
        ]);
    }

    /// `CREATE CACHE` lowers with `LeafBehavior::Leaf`, so a query naming two caches joins their
    /// leaves and owns each of them.  A leaf keeps reader state, but a join does not look a
    /// reader up: it reads the materialization feeding it, which here is the one shared base.
    #[test]
    fn refuses_a_base_reached_through_another_query_s_leaf() {
        let msg = refused(&[
            ("t", Base, &[]),
            ("c1", ForeignLeaf, &["t"]),
            ("c2", ForeignLeaf, &["t"]),
            ("j", Join, &["c1", "c2"]),
        ]);
        assert!(
            msg.contains("reaches both sides of a join"),
            "expected the shared-base refusal, got {msg}"
        );
    }

    /// `j1` and `j2` each read both bases, and `j3` joins them.  A lookup stops at either, so a
    /// delta arriving at `j3` from one looks up the other's own state.
    #[test]
    fn allows_two_joins_that_share_their_bases() {
        allowed(&[
            ("a", Base, &[]),
            ("b", Base, &[]),
            ("a1", Alias, &["a"]),
            ("b1", Alias, &["b"]),
            ("j1", Join, &["a1", "b1"]),
            ("a2", Alias, &["a"]),
            ("b2", Alias, &["b"]),
            ("j2", Join, &["a2", "b2"]),
            ("j3", Join, &["j1", "j2"]),
        ]);
    }

    /// `add_bogokey_if_necessary` runs first and gives a keyless join a constant key, so a join
    /// still keyless here never had a bogo projection and reads the base directly.
    #[test]
    fn rejects_keyless_join_over_a_shared_base() {
        refused(&[
            ("t", Base, &[]),
            ("ta", Alias, &["t"]),
            ("tb", Alias, &["t"]),
            ("j", JoinOn(&[]), &["ta", "tb"]),
        ]);
    }

    /// Single-base in both inputs, but only because the left input is itself a join over the
    /// same base.  Covers `t JOIN t t1 JOIN t t2` after the inner join collapses.
    #[test]
    fn rejects_three_way_self_join() {
        refused(&[
            ("t", Base, &[]),
            ("ta", Alias, &["t"]),
            ("tb", Alias, &["t"]),
            ("tc", Alias, &["t"]),
            ("inner", Join, &["ta", "tb"]),
            ("outer", Join, &["inner", "tc"]),
        ]);
    }

    #[test]
    fn allows_distinct_tables_join() {
        allowed(&[("a", Base, &[]), ("b", Base, &[]), ("j", Join, &["a", "b"])]);
    }

    /// REA-6360: `a JOIN b ON a.x = b.x AND a.z = a.z`.  One key compares a column with itself,
    /// which a predicate reading the ON for a same-column comparison takes for a self-join.  The
    /// inputs reach different bases, so this pass must allow it.
    #[test]
    fn allows_mixed_table_join_with_same_table_predicate() {
        allowed(&[
            ("a", Base, &[]),
            ("b", Base, &[]),
            (
                "j",
                JoinOnQualified(&[(("a", "x"), ("b", "x")), (("a", "z"), ("a", "z"))]),
                &["a", "b"],
            ),
        ]);
    }

    #[test]
    fn allows_a_shared_base_behind_state_on_every_input() {
        allowed(&[
            ("t", Base, &[]),
            ("agg_l", Agg, &["t"]),
            ("agg_r", Agg, &["t"]),
            ("j", Join, &["agg_l", "agg_r"]),
        ]);
    }

    #[test]
    fn rejects_a_shared_base_read_directly_by_one_input() {
        refused(&[
            ("t", Base, &[]),
            ("direct", Alias, &["t"]),
            ("agg", Agg, &["t"]),
            ("j", Join, &["direct", "agg"]),
        ]);
    }

    /// An ungrouped aggregate is a scalar subquery's shape, and keeps state like any other.
    #[test]
    fn rejects_a_base_read_directly_beside_an_ungrouped_aggregate() {
        refused(&[
            ("t", Base, &[]),
            ("direct", Alias, &["t"]),
            ("agg", UngroupedAgg, &["t"]),
            ("j", Join, &["direct", "agg"]),
        ]);
    }

    #[test]
    fn allows_a_shared_base_behind_an_ungrouped_aggregate_on_every_input() {
        allowed(&[
            ("t", Base, &[]),
            ("agg_l", UngroupedAgg, &["t"]),
            ("agg_r", UngroupedAgg, &["t"]),
            ("j", Join, &["agg_l", "agg_r"]),
        ]);
    }

    #[test]
    fn allows_a_shared_base_behind_distinct_on_every_input() {
        allowed(&[
            ("t", Base, &[]),
            ("distinct_l", Distinct, &["t"]),
            ("distinct_r", Distinct, &["t"]),
            ("j", Join, &["distinct_l", "distinct_r"]),
        ]);
    }

    #[test]
    fn allows_a_shared_base_behind_topk_on_every_input() {
        allowed(&[
            ("t", Base, &[]),
            ("topk_l", TopK, &["t"]),
            ("topk_r", TopK, &["t"]),
            ("j", Join, &["topk_l", "topk_r"]),
        ]);
    }

    /// The condition is asked of both inputs, so it does not matter which one reads the base.
    #[test]
    fn rejects_a_shared_base_read_directly_by_the_second_input() {
        refused(&[
            ("t", Base, &[]),
            ("agg", Agg, &["t"]),
            ("direct", Alias, &["t"]),
            ("j", Join, &["agg", "direct"]),
        ]);
    }

    /// Two views over one base, joined.  `q` owns each view's leaf but nothing behind it, and
    /// the walk has to cross that boundary to find the base they share.
    #[test]
    fn rejects_self_join_through_unowned_view_nodes() {
        refused(&[
            ("t", ForeignBase, &[]),
            ("v1", ForeignPassthrough, &["t"]),
            ("v2", ForeignPassthrough, &["t"]),
            ("l", Alias, &["v1"]),
            ("r", Alias, &["v2"]),
            ("j", Join, &["l", "r"]),
        ]);
    }

    /// Two aggregates over one base table share that base on both inputs of the
    /// [`JoinAggregates`], which is the shape every multi-aggregate query builds.  The walk does
    /// run for it, and admits it: its parents are aggregates by construction, so each input's
    /// lookup resolves at its own.
    ///
    /// [`JoinAggregates`]: MirNodeInner::JoinAggregates
    #[test]
    fn allows_join_aggregates_over_one_base() {
        allowed(&[
            ("t", Base, &[]),
            ("sum_x", Agg, &["t"]),
            ("sum_y", Agg, &["t"]),
            ("ja", JoinAggregates, &["sum_x", "sum_y"]),
        ]);
    }

    /// `JoinAggregates` is inspected like any other join kind, and admitted here: every parent
    /// it can be given is an aggregate or a previous `JoinAggregates`, and a lookup stops at
    /// either, so each input's lookup resolves at its own parent.
    #[test]
    fn allows_a_shared_base_behind_join_aggregates_on_every_input() {
        allowed(&[
            ("t", Base, &[]),
            ("l_sum", Agg, &["t"]),
            ("l_cnt", Agg, &["t"]),
            ("l", JoinAggregates, &["l_sum", "l_cnt"]),
            ("r_sum", Agg, &["t"]),
            ("r_cnt", Agg, &["t"]),
            ("r", JoinAggregates, &["r_sum", "r_cnt"]),
            ("j", Join, &["l", "r"]),
        ]);
    }

    /// It keeps state of its own, so a join reading through one is safe, while a join reading
    /// the base beside it is not.
    #[test]
    fn rejects_a_base_read_directly_beside_join_aggregates() {
        refused(&[
            ("t", Base, &[]),
            ("ta", Alias, &["t"]),
            ("sum_x", Agg, &["t"]),
            ("cnt_x", Agg, &["t"]),
            ("aggregated", JoinAggregates, &["sum_x", "cnt_x"]),
            ("j", Join, &["ta", "aggregated"]),
        ]);
    }

    /// A dependent join is matched in its own right, so a shared base behind one is caught
    /// whether or not `eliminate_dependent_joins` has rewritten it into a plain [`Join`] yet.
    ///
    /// [`Join`]: MirNodeInner::Join
    #[test]
    fn rejects_shared_base_behind_a_dependent_join() {
        let (mut graph, leaf) = build(&[
            ("t", Base, &[]),
            ("l", Alias, &["t"]),
            ("r", Alias, &["t"]),
            ("dj", DependentJoin, &["l", "r"]),
        ]);
        let mut query = MirQuery::new("q".into(), leaf, &mut graph);

        crate::rewrite::decorrelate::eliminate_dependent_joins(&mut query)
            .expect("nothing here is actually correlated, so the join just loses its dependency");
        assert!(
            query
                .topo_nodes()
                .iter()
                .all(|n| !query.graph[*n].inner.is_dependent_join()),
            "eliminate_dependent_joins must leave no dependent join behind"
        );

        let err = reject_self_joins(&query).expect_err("expected self-join rejection");
        assert!(err.is_unsupported(), "got {err:?}");
    }

    /// `hoisted_past` answers, over MIR, the question `Ingredient::can_query_through` answers
    /// over dataflow.  The two cannot share an implementation: this pass runs before lowering,
    /// and the emit it inspects is `ProjectExpr`, not the dataflow `Expr`.  The dataflow side is
    /// pinned by `project::tests::literal_emit_is_not_query_through`, which names this pass; this
    /// end asserts the half that lives here.
    #[test]
    fn hoisted_past_matches_the_lowered_projection_rule() {
        assert!(
            hoisted_past(&MirNodeInner::Project {
                emit: vec![ProjectExpr::Column(Column::named("x"))],
            }),
            "a projection of plain columns is hoisted past"
        );
        assert!(
            !hoisted_past(&MirNodeInner::Project {
                emit: vec![ProjectExpr::Expr {
                    expr: ast::Expr::Literal(0.into()),
                    alias: "bogokey".into(),
                }],
            }),
            "a projection carrying an expression keeps state of its own"
        );
        assert!(
            !hoisted_past(&MirNodeInner::Join {
                on: vec![],
                project: vec![],
            }),
            "and a join keeps state of its own, so a lookup stops at it"
        );
    }
}
