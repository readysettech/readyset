use std::collections::HashMap;
use std::convert::TryFrom;

pub use common::IndexRef;
use dataflow::prelude::*;
use noria::ReadySetError;
use vec1::Vec1;

// TODO: rewrite as iterator
/// *DEPRECATED*: trace the provenance of the given set ofa columns from the given node up the
/// graph.
///
/// Returns segments in *trace order*, with the target node first and the source node last
pub fn provenance_of(
    graph: &Graph,
    node: NodeIndex,
    columns: &[usize],
) -> Result<Vec<Vec<(NodeIndex, Vec<Option<usize>>)>>, ReadySetError> {
    let path = vec![(node, columns.iter().map(|&v| Some(v)).collect())];
    trace(graph, path)
}

/// The return value of `deduce_column_source`.
struct DeducedColumnSource {
    /// Node ancestors to continue building replay paths through. Empty if the node is a base node.
    ancestors: Vec<IndexRef>,
    /// Whether or not the replay path should be broken at the node passed to
    /// `deduced_column_source`.
    ///
    /// When columns are generated, we terminate the current replay path at
    /// the node generating the columns, and start a new replay path from that
    /// node to the node(s) referenced by the node's `column_source` implementation.
    break_path: bool,
}

/// Find the ancestors of the node provided for the given column index (if present), and also
/// check whether the current replay path should be broken at this node.
///
/// (see the `DeducedColumnSource` documentation for more)
fn deduce_column_source(
    graph: &Graph,
    &IndexRef { node, ref index }: &IndexRef,
) -> ReadySetResult<DeducedColumnSource> {
    let parents: Vec<_> = graph
        .neighbors_directed(node, petgraph::EdgeDirection::Incoming)
        .collect();

    if parents.is_empty() {
        // stop here
        return Ok(DeducedColumnSource {
            ancestors: vec![],
            break_path: false,
        });
    }

    let n = &graph[node];

    if !n.is_internal() {
        // we know all non-internal nodes use an identity mapping
        let parent = parents[0];
        return Ok(DeducedColumnSource {
            ancestors: vec![IndexRef {
                node: parent,
                index: index.clone(),
            }],
            break_path: false,
        });
    }

    let colsrc = if let Some(ref index) = *index {
        n.column_source(&index.columns)
    } else {
        // if we don't have any columns to pass to `column_source`, we're going for
        // a full materialization.

        if let Some(mra) = n.must_replay_among() {
            // nodes can specify that they only want replays from some of their parents in
            // the full materialization case. a good example of this is the left join.
            ColumnSource::RequiresFullReplay(
                Vec1::try_from(mra.into_iter().collect::<Vec<_>>()).unwrap(),
            )
        } else {
            // otherwise, we just replay through ALL of the node's parents.

            // unwrap: always succeeds since we checked `parents.is_empty` earlier
            ColumnSource::RequiresFullReplay(Vec1::try_from(parents).unwrap())
        }
    };

    Ok(match colsrc {
        ColumnSource::ExactCopy(ColumnRef { node, columns }) => DeducedColumnSource {
            ancestors: vec![IndexRef::partial(
                node,
                #[allow(clippy::unwrap_used)] // we only hit ExactCopy if we have a partial index
                Index::new(index.as_ref().unwrap().index_type, columns.to_vec()),
            )],
            break_path: false,
        },
        ColumnSource::Union(refs) => DeducedColumnSource {
            ancestors: refs
                .into_iter()
                .map(|ColumnRef { node, columns }| {
                    IndexRef::partial(
                        node,
                        #[allow(clippy::unwrap_used)]
                        // we only hit Union if we have a partial index
                        Index::new(index.as_ref().unwrap().index_type, columns.to_vec()),
                    )
                })
                .collect(),
            break_path: false,
        },
        ColumnSource::GeneratedFromColumns(refs) => DeducedColumnSource {
            ancestors: refs
                .into_iter()
                .map(|ColumnRef { node, columns }| {
                    IndexRef::partial(
                        node,
                        #[allow(clippy::unwrap_used)]
                        // we only hit GeneratedFromColumns if we have a partial index
                        Index::new(index.as_ref().unwrap().index_type, columns.to_vec()),
                    )
                })
                .collect(),
            break_path: true,
        },
        ColumnSource::RequiresFullReplay(nodes) => DeducedColumnSource {
            ancestors: nodes.into_iter().map(|n| IndexRef::full(n)).collect(),
            break_path: false,
        },
    })
}

/// Recursively extend the `current` replay path.
///
/// In the simplest case (i.e. a straight line from base table to source node, with no branching or
/// nodes with multiple ancestors), this will just add the next ancestor to `current`, recursively
/// call itself, and then return `vec![current]` when it gets to the base table.
///
/// In cases where the current node has multiple ancestors, this recursively calls itself *for each
/// ancestor*, and then returns a vector of all of the replay paths generated by doing so.
///
/// Replay paths returned may not all start at the same node; if replay paths are broken midway due
/// to the use of generated columns, some will begin at the node where the generated columns were
/// used.
fn continue_replay_path<F>(
    graph: &Graph,
    mut current: Vec<IndexRef>,
    stop_at: &F,
) -> ReadySetResult<Vec<Vec<IndexRef>>>
where
    F: Fn(NodeIndex) -> bool,
{
    let current_colref = current.last().cloned().unwrap();

    if stop_at(current_colref.node) || graph[current_colref.node].is_base() {
        // do not continue resolving past the current node
        return Ok(vec![current]);
    }

    let dcs = deduce_column_source(graph, &current_colref)?;

    match dcs.ancestors.len() {
        // stop and return the current replay path in full
        0 => Ok(vec![current]),
        1 => {
            // only one ancestor, and its column reference is:
            let next_colref = dcs.ancestors.into_iter().next().unwrap();
            // don't break the path if we're only a 1-node replay path
            if dcs.break_path && current.len() > 1 {
                let mut ret = vec![current];
                let next = vec![current_colref, next_colref];
                ret.extend(continue_replay_path(graph, next, stop_at)?);
                Ok(ret)
            } else {
                current.push(next_colref);
                continue_replay_path(graph, current, stop_at)
            }
        }
        _ => {
            // multiple ancestors
            if dcs.break_path && current.len() > 1 {
                let mut ret = vec![current];
                for next_colref in dcs.ancestors {
                    let next = vec![current_colref.clone(), next_colref];
                    ret.extend(continue_replay_path(graph, next, stop_at)?);
                }
                Ok(ret)
            } else {
                let mut ret = vec![];
                for next_colref in dcs.ancestors {
                    let mut next = current.clone();
                    next.push(next_colref);
                    ret.extend(continue_replay_path(graph, next, stop_at)?);
                }
                Ok(ret)
            }
        }
    }
}

/// Given a `ColumnRef`, and an `IndexType`, generate all replay paths needed to generate a
/// **partial** materialization for those columns on that node, if one is possible.
///
/// If a partial materialization is not desired, use the `replay_paths_for_opt` API instead, which
/// will not try and handle generated columns.
///
/// If `stop_at` is provided, replay paths will not continue past (but will include) nodes for which
/// `stop_at(node_index)` returns `true`. (The intended usage here is for `stop_at` to return
/// `true` for nodes that already have a materialization.)
///
/// Note that this function returns replay paths in *replay order*, with the source node first and
/// the target node last
pub fn replay_paths_for<F>(
    graph: &Graph,
    ColumnRef { node, columns }: ColumnRef,
    index_type: IndexType,
    stop_at: F,
) -> ReadySetResult<Vec<Vec<IndexRef>>>
where
    F: Fn(NodeIndex) -> bool,
{
    replay_paths_for_opt(
        graph,
        IndexRef::partial(node, Index::new(index_type, columns.to_vec())),
        stop_at,
    )
}

/// Like `replay_paths_for`, but allows specifying an `IndexRef` instead of a `ColumnRef`.
///
/// This is useful for full materialization cases where specifying a `ColumnRef` will try and
/// generate split replay paths for generated columns, which is highly undesirable unless we're
/// doing partial.
///
/// Note that this function returns replay paths in *replay order*, with the source node first and
/// the target node last
pub fn replay_paths_for_opt<F>(
    graph: &Graph,
    colref: IndexRef,
    stop_at: F,
) -> ReadySetResult<Vec<Vec<IndexRef>>>
where
    F: Fn(NodeIndex) -> bool,
{
    let mut paths = continue_replay_path(graph, vec![colref], &stop_at)?;
    paths.iter_mut().for_each(|p| p.reverse());
    Ok(paths)
}

/// Like `replay_paths_for`, but `stop_at` always returns `false`.
///
/// Note that this function returns replay paths in *replay order*, with the source node first and
/// the target node last
pub fn replay_paths_for_nonstop(
    graph: &Graph,
    colref: ColumnRef,
    index_type: IndexType,
) -> ReadySetResult<Vec<Vec<IndexRef>>> {
    let never_stop = |_| false;
    replay_paths_for(graph, colref, index_type, never_stop)
}

fn trace(
    graph: &Graph,
    mut path: Vec<(NodeIndex, Vec<Option<usize>>)>,
) -> ReadySetResult<Vec<Vec<(NodeIndex, Vec<Option<usize>>)>>> {
    // figure out what node/column we're looking up
    let (node, columns) = path.last().cloned().unwrap();
    let cols = columns.len();

    let parents: Vec<_> = graph
        .neighbors_directed(node, petgraph::EdgeDirection::Incoming)
        .collect();

    if parents.is_empty() {
        // this path reached the source node.
        // but we should have stopped at base nodes above...
        internal!("considering a node with no parents");
    }

    let n = &graph[node];

    // have we reached a base node?
    if n.is_base() {
        return Ok(vec![path]);
    }

    // we know all non-internal nodes use an identity mapping
    if !n.is_internal() {
        let parent = parents[0];
        path.push((parent, columns));
        return trace(graph, path);
    }

    // if all our inputs are None, our job is trivial
    // we just go trace back to all ancestors
    if columns.iter().all(Option::is_none) {
        // except if must_replay_among is defined, in which case we replay through the set
        // provided in that API
        if let Some(mra) = n.must_replay_among() {
            let mut paths = Vec::with_capacity(mra.len());
            for p in mra {
                let mut path = path.clone();
                path.push((p, vec![None; cols]));
                paths.extend(trace(graph, path)?);
            }
            return Ok(paths);
        }

        let mut paths = Vec::with_capacity(parents.len());
        for p in parents {
            let mut path = path.clone();
            path.push((p, vec![None; cols]));
            paths.extend(trace(graph, path)?);
        }
        return Ok(paths);
    }

    // try to resolve the currently selected columns
    let mut resolved = columns
        .iter()
        .enumerate()
        .filter_map(|(i, &c)| c.map(|c| (i, c)))
        .map(|(i, c)| Ok((i, n.parent_columns(c))))
        .collect::<Result<Vec<(usize, Vec<(NodeIndex, Option<usize>)>)>, ReadySetError>>()?
        .iter()
        .flat_map(|(i, origins)| {
            assert!(!origins.is_empty());
            origins.iter().map(move |o| (i, o))
        })
        .fold(
            HashMap::new(),
            |mut by_ancestor, (coli, (ancestor, column))| {
                {
                    let resolved = by_ancestor
                        .entry(*ancestor)
                        .or_insert_with(|| vec![None; cols]);
                    resolved[*coli] = *column;
                }
                by_ancestor
            },
        );
    assert!(!resolved.is_empty(), "Some(col) resolved into no ancestors");

    // are any of the columns generated?
    if let Some(columns) = resolved.remove(&node) {
        // some are, so at this point we know we'll need to yield None for those columns all the
        // way back to the root of the graph.

        // resolving to Some on self makes no sense...
        assert!(columns.iter().all(Option::is_none));

        if parents.len() != 1 {
            // TODO: we have a join-like thing, so we'd need to call on_join
            // like in the case of all our inputs being None above.
            unimplemented!();
        }

        let mut paths = Vec::with_capacity(parents.len());
        for p in parents {
            let mut path = path.clone();
            path.push((p, resolved.remove(&p).unwrap_or_else(|| vec![None; cols])));
            paths.extend(trace(graph, path)?);
        }
        return Ok(paths);
    }

    // no, it resolves to at least one parent column
    // if there is only one parent, we can step right to that
    if resolved.len() == 1 {
        let (parent, resolved) = resolved.into_iter().next().unwrap();
        path.push((parent, resolved));
        return trace(graph, path);
    }

    // traverse up all the paths
    let mut paths = Vec::with_capacity(parents.len());
    for (parent, columns) in resolved {
        let mut path = path.clone();
        path.push((parent, columns));
        paths.extend(trace(graph, path)?);
    }
    Ok(paths)
}

#[cfg(test)]
mod tests {
    use dataflow::{node, ops};

    use super::*;

    fn bases() -> (Graph, NodeIndex, NodeIndex) {
        let mut g = petgraph::Graph::new();
        let src = g.add_node(node::Node::new(
            "source",
            &["because-type-inference"],
            node::special::Source,
        ));

        let a = g.add_node(node::Node::new(
            "a",
            &["a1", "a2"],
            node::special::Base::default(),
        ));
        g.add_edge(src, a, ());

        let b = g.add_node(node::Node::new(
            "b",
            &["b1", "b2"],
            node::special::Base::default(),
        ));
        g.add_edge(src, b, ());

        (g, a, b)
    }

    #[test]
    fn base_trace() {
        let (g, a, b) = bases();
        assert_eq!(
            replay_paths_for_nonstop(
                &g,
                ColumnRef {
                    node: a,
                    columns: vec1![0]
                },
                IndexType::HashMap
            )
            .unwrap(),
            vec![vec![IndexRef::partial(a, Index::hash_map(vec![0]))]]
        );
        assert_eq!(
            replay_paths_for_nonstop(
                &g,
                ColumnRef {
                    node: b,
                    columns: vec1![0]
                },
                IndexType::HashMap
            )
            .unwrap(),
            vec![vec![IndexRef::partial(b, Index::hash_map(vec![0]))]]
        );

        // multicol
        assert_eq!(
            replay_paths_for_nonstop(
                &g,
                ColumnRef {
                    node: a,
                    columns: vec1![0, 1]
                },
                IndexType::HashMap
            )
            .unwrap(),
            vec![vec![IndexRef::partial(a, Index::hash_map(vec![0, 1]))]]
        );

        assert_eq!(
            replay_paths_for_nonstop(
                &g,
                ColumnRef {
                    node: a,
                    columns: vec1![1, 0]
                },
                IndexType::HashMap
            )
            .unwrap(),
            vec![vec![IndexRef::partial(a, Index::hash_map(vec![1, 0]))]]
        );
    }

    #[test]
    fn internal_passthrough() {
        let (mut g, a, _) = bases();

        let x = g.add_node(node::Node::new("x", &["x1", "x2"], node::special::Ingress));
        g.add_edge(a, x, ());

        assert_eq!(
            replay_paths_for_nonstop(
                &g,
                ColumnRef {
                    node: x,
                    columns: vec1![0]
                },
                IndexType::HashMap
            )
            .unwrap(),
            vec![vec![
                IndexRef::partial(a, Index::hash_map(vec![0])),
                IndexRef::partial(x, Index::hash_map(vec![0])),
            ]]
        );

        assert_eq!(
            replay_paths_for_nonstop(
                &g,
                ColumnRef {
                    node: x,
                    columns: vec1![0, 1]
                },
                IndexType::HashMap
            )
            .unwrap(),
            vec![vec![
                IndexRef::partial(a, Index::hash_map(vec![0, 1])),
                IndexRef::partial(x, Index::hash_map(vec![0, 1])),
            ]]
        );
    }

    #[test]
    fn col_reorder() {
        let (mut g, a, _) = bases();

        let x = g.add_node(node::Node::new(
            "x",
            &["x2", "x1"],
            ops::NodeOperator::Project(ops::project::Project::new(a, &[1, 0], None, None)),
        ));
        g.add_edge(a, x, ());

        assert_eq!(
            replay_paths_for_nonstop(
                &g,
                ColumnRef {
                    node: x,
                    columns: vec1![0]
                },
                IndexType::HashMap
            )
            .unwrap(),
            vec![vec![
                IndexRef::partial(a, Index::hash_map(vec![1])),
                IndexRef::partial(x, Index::hash_map(vec![0])),
            ]]
        );
        assert_eq!(
            replay_paths_for_nonstop(
                &g,
                ColumnRef {
                    node: x,
                    columns: vec1![0, 1]
                },
                IndexType::HashMap
            )
            .unwrap(),
            vec![vec![
                IndexRef::partial(a, Index::hash_map(vec![1, 0])),
                IndexRef::partial(x, Index::hash_map(vec![0, 1])),
            ]]
        );
    }

    #[test]
    fn requires_full_replay() {
        use std::convert::TryFrom;

        let (mut g, a, _) = bases();

        let x = g.add_node(node::Node::new(
            "x",
            &["x1", "foo"],
            ops::NodeOperator::Project(ops::project::Project::new(
                a,
                &[0],
                Some(vec![DataType::try_from(42).unwrap()]),
                None,
            )),
        ));
        g.add_edge(a, x, ());

        assert_eq!(
            replay_paths_for_nonstop(
                &g,
                ColumnRef {
                    node: x,
                    columns: vec1![0]
                },
                IndexType::HashMap
            )
            .unwrap(),
            vec![vec![
                IndexRef::partial(a, Index::hash_map(vec![0])),
                IndexRef::partial(x, Index::hash_map(vec![0])),
            ]]
        );
        assert_eq!(
            replay_paths_for_nonstop(
                &g,
                ColumnRef {
                    node: x,
                    columns: vec1![1]
                },
                IndexType::HashMap
            )
            .unwrap(),
            vec![vec![
                IndexRef::full(a),
                IndexRef::partial(x, Index::hash_map(vec![1])),
            ]]
        );
        assert_eq!(
            replay_paths_for_nonstop(
                &g,
                ColumnRef {
                    node: x,
                    columns: vec1![0, 1]
                },
                IndexType::HashMap
            )
            .unwrap(),
            vec![vec![
                IndexRef::full(a),
                IndexRef::partial(x, Index::hash_map(vec![0, 1])),
            ]]
        );
    }

    #[test]
    fn union_straight() {
        let (mut g, a, b) = bases();

        let x = g.add_node(node::Node::new(
            "x",
            &["x1", "x2"],
            ops::NodeOperator::Union(
                ops::union::Union::new(
                    vec![(a, vec![0, 1]), (b, vec![0, 1])].into_iter().collect(),
                    ops::union::DuplicateMode::UnionAll,
                )
                .unwrap(),
            ),
        ));
        g.add_edge(a, x, ());
        g.add_edge(b, x, ());

        let mut paths = replay_paths_for_nonstop(
            &g,
            ColumnRef {
                node: x,
                columns: vec1![0],
            },
            IndexType::HashMap,
        )
        .unwrap();
        paths.sort_unstable();
        assert_eq!(
            paths,
            vec![
                vec![
                    IndexRef::partial(a, Index::hash_map(vec![0])),
                    IndexRef::partial(x, Index::hash_map(vec![0])),
                ],
                vec![
                    IndexRef::partial(b, Index::hash_map(vec![0])),
                    IndexRef::partial(x, Index::hash_map(vec![0])),
                ],
            ]
        );

        let mut paths = replay_paths_for_nonstop(
            &g,
            ColumnRef {
                node: x,
                columns: vec1![0, 1],
            },
            IndexType::HashMap,
        )
        .unwrap();
        paths.sort_unstable();
        assert_eq!(
            paths,
            vec![
                vec![
                    IndexRef::partial(a, Index::hash_map(vec![0, 1])),
                    IndexRef::partial(x, Index::hash_map(vec![0, 1])),
                ],
                vec![
                    IndexRef::partial(b, Index::hash_map(vec![0, 1])),
                    IndexRef::partial(x, Index::hash_map(vec![0, 1])),
                ],
            ]
        );
    }

    #[test]
    fn inner_join() {
        let (mut g, a, b) = bases();

        let x = g.add_node(node::Node::new(
            "x",
            &["a1", "a2b1", "b2"],
            ops::NodeOperator::Join(ops::join::Join::new(
                a,
                b,
                ops::join::JoinType::Inner,
                vec![
                    ops::join::JoinSource::L(0),
                    ops::join::JoinSource::B(1, 0),
                    ops::join::JoinSource::R(1),
                ],
            )),
        ));
        g.add_edge(a, x, ());
        g.add_edge(b, x, ());

        // left-only column should replay via the left only
        assert_eq!(
            replay_paths_for_nonstop(
                &g,
                ColumnRef {
                    node: x,
                    columns: vec1![0]
                },
                IndexType::HashMap
            )
            .unwrap(),
            vec![vec![
                IndexRef::partial(a, Index::hash_map(vec![0])),
                IndexRef::partial(x, Index::hash_map(vec![0])),
            ]]
        );

        // right-only column should replay via the right only
        assert_eq!(
            replay_paths_for_nonstop(
                &g,
                ColumnRef {
                    node: x,
                    columns: vec1![2]
                },
                IndexType::HashMap
            )
            .unwrap(),
            vec![vec![
                IndexRef::partial(b, Index::hash_map(vec![1])),
                IndexRef::partial(x, Index::hash_map(vec![2])),
            ]]
        );

        // middle column is in left node, so will replay from left
        let mut paths = replay_paths_for_nonstop(
            &g,
            ColumnRef {
                node: x,
                columns: vec1![1],
            },
            IndexType::HashMap,
        )
        .unwrap();
        paths.sort_unstable();
        assert_eq!(
            paths,
            vec![vec![
                IndexRef::partial(a, Index::hash_map(vec![1])),
                IndexRef::partial(x, Index::hash_map(vec![1])),
            ],]
        );

        // indexing (left, middle) should replay (left, middle) from left
        let mut paths = replay_paths_for_nonstop(
            &g,
            ColumnRef {
                node: x,
                columns: vec1![0, 1],
            },
            IndexType::HashMap,
        )
        .unwrap();
        paths.sort_unstable();
        assert_eq!(
            paths,
            vec![vec![
                IndexRef::partial(a, Index::hash_map(vec![0, 1])),
                IndexRef::partial(x, Index::hash_map(vec![0, 1])),
            ],]
        );

        // indexing (middle, right) should replay (middle, right) from right
        let mut paths = replay_paths_for_nonstop(
            &g,
            ColumnRef {
                node: x,
                columns: vec1![1, 2],
            },
            IndexType::HashMap,
        )
        .unwrap();
        paths.sort_unstable();
        assert_eq!(
            paths,
            vec![vec![
                IndexRef::partial(b, Index::hash_map(vec![0, 1])),
                IndexRef::partial(x, Index::hash_map(vec![1, 2])),
            ],]
        );

        // indexing (left, middle, right) should replay (left, middle) from left and (right) from
        // right
        let mut paths = replay_paths_for_nonstop(
            &g,
            ColumnRef {
                node: x,
                columns: vec1![0, 1, 2],
            },
            IndexType::HashMap,
        )
        .unwrap();
        paths.sort_unstable();
        assert_eq!(
            paths,
            vec![
                vec![
                    IndexRef::partial(a, Index::hash_map(vec![0, 1])),
                    IndexRef::partial(x, Index::hash_map(vec![0, 1, 2])),
                ],
                vec![
                    IndexRef::partial(b, Index::hash_map(vec![1])),
                    IndexRef::partial(x, Index::hash_map(vec![0, 1, 2])),
                ],
            ]
        );

        // indexing (left, right) should replay (left) from left and (right) from right
        let mut paths = replay_paths_for_nonstop(
            &g,
            ColumnRef {
                node: x,
                columns: vec1![0, 2],
            },
            IndexType::HashMap,
        )
        .unwrap();
        paths.sort_unstable();
        assert_eq!(
            paths,
            vec![
                vec![
                    IndexRef::partial(a, Index::hash_map(vec![0])),
                    IndexRef::partial(x, Index::hash_map(vec![0, 2])),
                ],
                vec![
                    IndexRef::partial(b, Index::hash_map(vec![1])),
                    IndexRef::partial(x, Index::hash_map(vec![0, 2])),
                ],
            ]
        );
    }
}
