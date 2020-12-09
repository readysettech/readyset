//! Functions for assigning new nodes to thread domains.

use dataflow::prelude::*;
use petgraph;
use slog::Logger;

pub fn assign(log: &Logger, graph: &mut Graph, topo_list: &[NodeIndex], ndomains: &mut usize) {
    // we need to walk the data flow graph and assign domains to all new nodes.
    // we generally want as few domains as possible, but in *some* cases we must make new ones.
    // specifically:
    //
    //  - the child of a Sharder is always in a different domain from the sharder
    //  - shard merge nodes are never in the same domain as their sharded ancestors

    let mut next_domain = || {
        *ndomains += 1;
        *ndomains - 1
    };

    for &node in topo_list {
        #[allow(clippy::cognitive_complexity)]
        let assignment = (|| {
            let graph = &*graph;
            let n = &graph[node];

            // TODO: the code below is probably _too_ good at keeping things in one domain.
            // having all bases in one domain (e.g., if sharding is disabled) isn't great because
            // write performance will suffer terribly.

            if n.is_shard_merger() {
                // shard mergers are always in their own domain.
                // we *could* use the same domain for multiple separate shard mergers
                // but it's unlikely that would do us any good.
                return next_domain();
            }

            if n.is_reader() {
                // readers always re-materialize, so sharing a domain doesn't help them much.
                // having them in their own domain also means that they get to aggregate reader
                // replay requests in their own little thread, and not interfere as much with other
                // internal traffic.
                return next_domain();
            }

            if n.is_base() {
                // bases are in a little bit of an awkward position becuase they can't just blindly
                // join in domains of other bases in the face of sharding. consider the case of two
                // bases, A and B, where A is sharded by A[0] and B by B[0]. Can they share a
                // domain? The way we deal with this is that we walk *down* from the base until we
                // hit any sharders or shard mergers, and then we walk *up* from each node visited
                // on the way down until we hit a base without traversing a sharding.
                // XXX: maybe also do this extended walk for non-bases?
                let mut children_same_shard = Vec::new();
                let mut frontier: Vec<_> = graph
                    .neighbors_directed(node, petgraph::EdgeDirection::Outgoing)
                    .collect();
                while !frontier.is_empty() {
                    for cni in frontier.split_off(0) {
                        let c = &graph[cni];
                        if c.is_sharder() || c.is_shard_merger() {
                        } else {
                            assert_eq!(n.sharded_by().is_none(), c.sharded_by().is_none());
                            children_same_shard.push(cni);
                            frontier.extend(
                                graph.neighbors_directed(cni, petgraph::EdgeDirection::Outgoing),
                            );
                        }
                    }
                }

                let mut friendly_base = None;
                frontier = children_same_shard;
                'search: while !frontier.is_empty() {
                    for pni in frontier.split_off(0) {
                        if pni == node {
                            continue;
                        }

                        let p = &graph[pni];
                        if p.is_source() || p.is_sharder() || p.is_shard_merger() {
                        } else if p.is_base() {
                            if p.has_domain() {
                                friendly_base = Some(p);
                                break 'search;
                            }
                        } else {
                            assert_eq!(n.sharded_by().is_none(), p.sharded_by().is_none());
                            frontier.extend(
                                graph.neighbors_directed(pni, petgraph::EdgeDirection::Incoming),
                            );
                        }
                    }
                }

                return if let Some(friendly_base) = friendly_base {
                    friendly_base.domain().index()
                } else {
                    // there are no bases like us, so we need a new domain :'(
                    next_domain()
                };
            }

            if graph[node].name().starts_with("BOUNDARY_") {
                return next_domain();
            }

            let any_parents = move |prime: &dyn Fn(&Node) -> bool,
                                    check: &dyn Fn(&Node) -> bool| {
                let mut stack: Vec<_> = graph
                    .neighbors_directed(node, petgraph::EdgeDirection::Incoming)
                    .filter(move |&p| prime(&graph[p]))
                    .collect();
                while let Some(p) = stack.pop() {
                    if graph[p].is_source() {
                        continue;
                    }
                    if check(&graph[p]) {
                        return true;
                    }
                    stack.extend(graph.neighbors_directed(p, petgraph::EdgeDirection::Incoming));
                }
                false
            };

            let parents: Vec<_> = graph
                .neighbors_directed(node, petgraph::EdgeDirection::Incoming)
                .map(|ni| (ni, &graph[ni]))
                .collect();

            let mut assignment = None;
            for &(_, ref p) in &parents {
                if p.is_sharder() {
                    // we're a child of a sharder (which currently has to be unsharded). we
                    // can't be in the same domain as the sharder (because we're starting a new
                    // sharding)
                    assert!(p.sharded_by().is_none());
                } else if p.is_source() {
                    // the source isn't a useful source of truth
                } else if assignment.is_none() {
                    // the key may move to a different column, so we can't actually check for
                    // ByColumn equality. this'll do for now.
                    assert_eq!(p.sharded_by().is_none(), n.sharded_by().is_none());
                    if p.has_domain() {
                        assignment = Some(p.domain().index())
                    }
                }

                if let Some(candidate) = assignment {
                    // let's make sure we don't construct a-b-a path
                    if any_parents(
                        &|p| p.has_domain() && p.domain().index() != candidate,
                        &|pp| pp.domain().index() == candidate,
                    ) {
                        assignment = None;
                        continue;
                    }
                    break;
                }
            }

            if assignment.is_none() {
                // check our siblings too
                // XXX: we could keep traversing here to find cousins and such
                for &(pni, _) in &parents {
                    let siblings = graph
                        .neighbors_directed(pni, petgraph::EdgeDirection::Outgoing)
                        .map(|ni| &graph[ni]);
                    for s in siblings {
                        if !s.has_domain() {
                            continue;
                        }
                        if s.sharded_by().is_none() != n.sharded_by().is_none() {
                            continue;
                        }
                        let candidate = s.domain().index();
                        if any_parents(
                            &|p| p.has_domain() && p.domain().index() != candidate,
                            &|pp| pp.domain().index() == candidate,
                        ) {
                            continue;
                        }
                        assignment = Some(candidate);
                        break;
                    }
                }
            }

            assignment.unwrap_or_else(|| {
                // no other options left -- we need a new domain
                next_domain()
            })
        })();

        debug!(log, "node added to domain";
           "node" => node.index(),
           "type" => ?graph[node],
           "domain" => ?assignment);
        graph[node].add_to(assignment.into());
    }
}
