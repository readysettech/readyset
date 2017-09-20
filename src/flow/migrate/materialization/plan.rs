use std::collections::{HashMap, HashSet};
use flow::payload::TriggerEndpoint;
use flow::prelude::*;
use flow::domain;
use flow::keys;
use petgraph;

const FILTER_SPECIFICITY: usize = 10;

pub(crate) struct Plan<'a> {
    m: &'a mut super::Materializations,
    graph: &'a Graph,
    node: NodeIndex,
    empty: &'a HashSet<NodeIndex>,
    domains: &'a mut HashMap<domain::Index, domain::DomainHandle>,
    partial: bool,

    tags: HashMap<Vec<usize>, Vec<(Tag, domain::Index)>>,
    paths: HashMap<Tag, Vec<NodeIndex>>,
    pending: Vec<PendingReplay>,
}

#[derive(Debug)]
pub(crate) struct PendingReplay {
    pub tag: Tag,
    pub source: LocalNodeIndex,
    pub source_domain: domain::Index,
    pub target_domain: domain::Index,
}

impl<'a> Plan<'a> {
    pub fn new(
        m: &'a mut super::Materializations,
        graph: &'a Graph,
        node: NodeIndex,
        empty: &'a HashSet<NodeIndex>,
        domains: &'a mut HashMap<domain::Index, domain::DomainHandle>,
    ) -> Plan<'a> {
        let partial = m.partial.contains(&node);
        Plan {
            m,
            graph,
            node,
            empty,
            domains,

            partial,

            pending: Vec::new(),
            tags: Default::default(),
            paths: Default::default(),
        }
    }

    fn paths(&mut self, columns: &[usize]) -> Vec<Vec<(NodeIndex, Option<usize>)>> {
        let graph = self.graph;
        let ni = self.node;
        let paths = if self.partial {
            // FIXME: compound key
            let mut on_join = Plan::partial_on_join(graph);
            keys::provenance_of(graph, ni, columns[0], &mut *on_join)
        } else {
            let mut on_join = self.full_on_join();
            keys::provenance_of(graph, ni, columns[0], &mut *on_join)
        };

        // cut paths so they only reach to the the closest materialized node
        let mut paths: Vec<_> = paths
            .into_iter()
            .map(|path| -> Vec<_> {
                let mut found = false;
                let mut path: Vec<_> = path.into_iter()
                    .enumerate()
                    .take_while(|&(i, (node, _))| {
                        // remember, the paths are "backwards", so the first node is target node
                        if i == 0 {
                            return true;
                        }

                        // keep taking until we get our first materialized node
                        // (`found` helps us emulate `take_while_inclusive`)
                        if found {
                            // we've already found a materialized node
                            return false;
                        }

                        if self.m.have.contains_key(&node) {
                            // we want to take this node, but not any later ones
                            found = true;
                        }
                        true
                    })
                    .map(|(_, segment)| segment)
                    .collect();
                path.reverse();
                path
            })
            .collect();

        // since we cut off part of each path, we *may* now have multiple paths that are the same
        // (i.e., if there was a union above the nearest materialization). this would be bad, as it
        // would cause a domain to request replays *twice* for a key from one view!
        paths.sort();
        paths.dedup();

        // all columns better resolve if we're doing partial
        assert!(!self.partial || paths.iter().all(|p| p[0].1.is_some()));

        paths
    }

    /// Finds the appropriate replay paths for the given index, and inform all domains on those
    /// paths about them. It also notes if any data backfills will need to be run, which is
    /// eventually reported back by `finalize`.
    pub fn add(&mut self, index_on: Vec<usize>) {
        if !self.partial && !self.paths.is_empty() {
            // non-partial views should not have one replay path per index. that would cause us to
            // replay several times, even though one full replay should always be sufficient.
            // we do need to keep track of the fact that there should be an index here though.
            self.tags.entry(index_on).or_insert_with(Vec::new);
            return;
        }

        // inform domains about replay paths
        let mut tags = Vec::new();
        for path in self.paths(&index_on[..]) {
            let tag = self.m.next_tag();
            self.paths
                .insert(tag, path.iter().map(|&(ni, _)| ni).collect());

            // what key are we using for partial materialization (if any)?
            let mut partial = None;
            if self.partial {
                if let Some(&(_, Some(ref key))) = path.first() {
                    partial = Some(key.clone());
                } else {
                    unreachable!();
                }
            }

            // first, find out which domains we are crossing
            let mut segments = Vec::new();
            let mut last_domain = None;
            for (node, key) in path {
                let domain = self.graph[node].domain();
                if last_domain.is_none() || domain != last_domain.unwrap() {
                    segments.push((domain, Vec::new()));
                    last_domain = Some(domain);

                    if self.partial && self.graph[node].is_transactional() {
                        self.m
                            .domains_on_path
                            .entry(tag.clone())
                            .or_insert_with(Vec::new)
                            .push(domain);
                    }
                }

                segments.last_mut().unwrap().1.push((node, key));
            }

            info!(self.m.log, "domain replay path is {:?}", segments; "tag" => tag.id());

            // tell all the domains about their segment of this replay path
            let mut pending = None;
            let mut seen = HashSet::new();
            for (i, &(domain, ref nodes)) in segments.iter().enumerate() {
                // TODO:
                //  a domain may appear multiple times in this list if a path crosses into the same
                //  domain more than once. currently, that will cause a deadlock.
                assert!(
                    !seen.contains(&domain),
                    "a-b-a domain replays are not yet supported"
                );
                seen.insert(domain);

                // we're not replaying through the starter node
                // *unless* it's a Base (because it might need to add defaults)
                let mut skip_first = 0;
                if i == 0 {
                    let n = &self.graph[segments[0].1[0].0];
                    if !n.is_internal() || n.get_base().is_none() {
                        skip_first = 1;
                    }
                }

                // use the local index for each node
                let locals: Vec<_> = nodes
                    .iter()
                    .skip(skip_first)
                    .map(|&(ni, key)| {
                        ReplayPathSegment {
                            node: *self.graph[ni].local_addr(),
                            partial_key: key,
                        }
                    })
                    .collect();

                // the first domain in the chain may *only* have the source node
                // in which case it doesn't need to know about the path
                if locals.is_empty() {
                    assert_eq!(i, 0);
                    continue;
                }

                // build the message we send to this domain to tell it about this replay path.
                let mut setup = box Packet::SetupReplayPath {
                    tag: tag,
                    source: None,
                    path: locals,
                    notify_done: false,
                    trigger: TriggerEndpoint::None,
                };

                // the first domain also gets to know source node
                if i == 0 {
                    if let box Packet::SetupReplayPath { ref mut source, .. } = setup {
                        *source = Some(*self.graph[nodes[0].0].local_addr());
                    }
                }


                if let Some(ref key) = partial {
                    // for partial materializations, nodes need to know how to trigger replays
                    if let box Packet::SetupReplayPath {
                        ref mut trigger, ..
                    } = setup
                    {
                        if segments.len() == 1 {
                            // replay is entirely contained within one domain
                            *trigger = TriggerEndpoint::Local(vec![*key]);
                        } else if i == 0 {
                            // first domain needs to be told about partial replay trigger
                            *trigger = TriggerEndpoint::Start(vec![*key]);
                        } else if i == segments.len() - 1 {
                            // otherwise, should know how to trigger partial replay
                            let shards = self.domains.get_mut(&segments[0].0).unwrap().shards();
                            *trigger = TriggerEndpoint::End(segments[0].0.clone(), shards);
                        }
                    } else {
                        unreachable!();
                    }
                } else {
                    // for full materializations, the last domain should report when it's done
                    if i == segments.len() - 1 {
                        if let box Packet::SetupReplayPath {
                            ref mut notify_done,
                            ..
                        } = setup
                        {
                            *notify_done = true;
                            assert!(pending.is_none());
                            pending = Some(PendingReplay {
                                tag: tag,
                                source: *self.graph[segments[0].1[0].0].local_addr(),
                                source_domain: segments[0].0,
                                target_domain: domain,
                            });
                        }
                    }
                }

                if i != segments.len() - 1 {
                    // since there is a later domain, the last node of any non-final domain must either
                    // be an egress or a Sharder. If it's an egress, we need to tell it about this
                    // replay path so that it knows what path to forward replay packets on.
                    let n = &self.graph[nodes.last().unwrap().0];
                    if n.is_egress() {
                        self.domains
                            .get_mut(&domain)
                            .unwrap()
                            .send(box Packet::UpdateEgress {
                                node: *n.local_addr(),
                                new_tx: None,
                                new_tag: Some((tag, segments[i + 1].1[0].0.into())),
                            })
                            .unwrap();
                    } else {
                        assert!(n.is_sharder());
                    }
                }

                trace!(self.m.log, "telling domain about replay path"; "domain" => domain.index());
                let ctx = self.domains.get_mut(&domain).unwrap();
                ctx.send(setup).unwrap();
                ctx.wait_for_ack().unwrap();
            }

            if !self.partial {
                // this path requires doing a replay and then waiting for the replay to finish
                self.pending
                    .push(pending.expect("no replay for full materialization?"));
            }
            tags.push((tag, last_domain.unwrap()));
        }

        self.tags
            .entry(index_on)
            .or_insert_with(Vec::new)
            .extend(tags);
    }

    /// Instructs the target node to set up appropriate state for any new indices that have been
    /// added. For new indices added to full materializations, this may take some time (a
    /// re-indexing has to happen), whereas for new indices to partial views it should be nearly
    /// instantaneous.
    ///
    /// Returns a list of backfill replays that need to happen before the migration is complete.
    pub fn finalize(mut self) -> Vec<PendingReplay> {
        use flow::payload::InitialState;

        // NOTE: we cannot use the impl of DerefMut here, since it (reasonably) disallows getting
        // mutable references to taken state.
        let s = self.graph[self.node]
            .with_reader(|r| {
                if self.partial {
                    // we only currently support replay to readers with a single path. supporting
                    // multiple paths (i.e., through unions) would require that the clients know to
                    // request replays for all paths. instead of thinking whether that's possible
                    // now, we'll just leave this restriction in place for the time being.
                    assert_eq!(self.tags.len(), 1);
                    assert!(r.is_materialized());

                    let first_index = self.tags.iter().next().unwrap().1;
                    assert_eq!(first_index.len(), 1);
                    let tag = &first_index[0];
                    let last_domain = tag.1;
                    let num_shards = self.domains[&last_domain].shards();

                    // since we're partially materializing a reader node,
                    // we need to give it a way to trigger replays.
                    InitialState::PartialGlobal {
                        gid: self.node,
                        cols: self.graph[self.node].fields().len(),
                        key: r.key().unwrap(),
                        tag: tag.0,
                        trigger_domain: (last_domain, num_shards),
                    }
                } else {
                    InitialState::Global {
                        cols: self.graph[self.node].fields().len(),
                        key: r.key().unwrap(),
                        gid: self.node,
                    }
                }
            })
            .unwrap_or_else(|| {
                // not a reader
                if self.partial {
                    let indices = self.tags
                        .drain()
                        .map(|(k, paths)| {
                            (k, paths.into_iter().map(|(tag, _)| tag).collect())
                        })
                        .collect();
                    InitialState::PartialLocal(indices)
                } else {
                    let indices = self.tags.drain().map(|(k, _)| k).collect();
                    InitialState::IndexedLocal(indices)
                }
            });

        self.domains
            .get_mut(&self.graph[self.node].domain())
            .unwrap()
            .send(box Packet::PrepareState {
                node: *self.graph[self.node].local_addr(),
                state: s,
            })
            .unwrap();

        if !self.partial {
            // we know that this must be a *new* fully materialized node:
            //
            //  - finalize() is only called by setup()
            //  - setup() is only called for existing nodes if they are partial
            //  - this branch has !self.partial
            //
            // if we're constructing a new view, there is no reason to replay any given path more
            // than once. we do need to be careful here though: the fact that the source and
            // destination of a path are the same does *not* mean that the path is the same (b/c of
            // unions), and we do not want to eliminate different paths!
            let mut distinct_paths = HashSet::new();
            let paths = &self.paths;
            self.pending.retain(|p| {
                // keep if this path is different
                distinct_paths.insert(&paths[&p.tag])
            });
            assert!(!self.pending.is_empty());
        } else {
            assert!(self.pending.is_empty());
        }
        self.pending
    }

    pub(crate) fn partial_on_join<'b>(
        graph: &'b Graph,
    ) -> Box<FnMut(NodeIndex, Option<usize>, &[NodeIndex]) -> Option<NodeIndex> + 'b> {
        Box::new(move |node, col, parents| {
            // we hit a join and need to choose a branch to follow.
            //
            // it turns out that this is slightlyc complicated:
            //  - if `col` is the join key, we must pick among must_replay_among
            //  - if it isn't, we must pick the table that sources those columns
            //  - XXX: if we have multiple columns that resolve to different sources, we
            //    can't do partial.
            //
            // NOTE: it is *not* okay for us to return None here
            if col.is_none() {
                // we've already failed to resolve, and so won't do partial anyway
                // can pick any ancestor
                return Some(parents[0]);
            }
            let col = col.unwrap();

            let r = graph[node].parent_columns(col);
            if r.len() == parents.len() {
                // we could be cleverer here when we have a choice
                match graph[node].must_replay_among() {
                    Some(anc) => {
                        // it is *extremely* important that this choice is deterministic.
                        // if it is not, the code that decides what indices to create could choose
                        // a *different* parent than the code that sets up the replay paths, which
                        // would be *bad*.
                        let mut parents = parents
                            .iter()
                            .filter(|p| anc.contains(p))
                            .map(|&p| p)
                            .collect::<Vec<_>>();
                        assert!(!parents.is_empty());
                        parents.sort_by_key(|p| p.index());
                        Some(parents[0])
                    }
                    None => Some(parents[0]),
                }
            } else {
                assert_eq!(r.len(), 1);
                Some(r[0].0)
            }
        })
    }

    fn full_on_join<'b>(
        &'b mut self,
    ) -> Box<FnMut(NodeIndex, Option<usize>, &[NodeIndex]) -> Option<NodeIndex> + 'b> {
        Box::new(move |node, col, parents| {
            // this function should only be called when there's a choice
            assert!(parents.len() > 1);

            // and only internal nodes have multiple parents
            let n = &self.graph[node];
            assert!(n.is_internal());

            // keep track of remaining parents
            let mut parents = Vec::from(parents);

            // the node dictates that we *must* replay the state of some ancestor(s)
            let options = n.must_replay_among()
                .expect("join did not have must replay preference");
            parents.retain(|&parent| options.contains(&parent));
            assert!(!parents.is_empty());

            // we want to prefer source paths where we can translate the key
            if let Some(c) = col {
                let srcs = n.parent_columns(c);
                let has = |p: &NodeIndex| {
                    for &(ref src, ref col) in &srcs {
                        if src == p && col.is_some() {
                            return true;
                        }
                    }
                    false
                };

                // we only want to prune non-resolving parents if there's at least one resolving.
                // otherwise, we might end up pruning all the parents!
                if parents.iter().any(&has) {
                    parents.retain(&has);
                }
            }

            // if there is only one left, we don't have a choice
            if parents.len() == 1 {
                // no need to pick
                return parents.pop();
            }

            // if *all* the options are empty, we can safely pick any of them
            if parents.iter().all(|p| self.empty.contains(p)) {
                return parents.pop();
            }

            // if any required parent is empty, and we know we're building a full materialization,
            // the join must be empty (since outer join targets aren't required), and therefore
            // we can just pick that parent and get a free full materialization.
            if let Some(&parent) = parents.iter().find(|&p| self.empty.contains(p)) {
                return Some(parent);
            }

            // we want to pick the ancestor that causes us to do the least amount of work.
            // this is really a balancing act between
            //
            //   a) how many records we are going to join; and
            //   b) how much state we need to replay
            //
            // consider the case where we are replaying a node downstream of a join, and the
            // join has two ancestors: a base node (A) and a filter (F) over a base node (B).
            // when should we choose to replay one or the other?
            //
            //  - the cost of replaying A is
            //        |A| joins
            //      + |A| lookups in B through F
            //  - the cost of replaying B through F is
            //        |B| filter operations
            //      + |F(B)| join operations
            //      + |F(B)| lookups in A
            //
            // which of these is more costly? even assuming we know |A| and |B|, it is not
            // clear, because we don't know F's specificity. let's assume some things:
            //
            //  - filters are cheaper than joins (~10x)
            //  - replaying is cheaper than filtering (~10x)
            //  - a filter emits one record for every FILTER_SPECIFICITY input records
            //
            // given those rough estimates, what's the best choice? well, we should pick a node
            // N with filters F1..Fn to replay which minimizes
            //
            //    1   * |N|                                 # replay cost
            let replay_cost = 1;
            //  + 10  * ( ∑i |N| / FILTER_SPECIFICITY ^ i ) # filter cost
            let filter_cost = 10;
            //  + 100 * |N| / FILTER_SPECIFICITY ^ n        # join cost
            let join_cost = 100;
            //
            // it is worth pointing out that this heuristic does *not* capture the fact that
            // replaying A above will encounter more expensive lookups on the join path (since
            // the lookups are in F(B), as opposed to directly in A).
            //
            // to compute this, we need to find |N| and n for each candidate node.
            // let's do that now
            parents
                .into_iter()
                .map(|p| {
                    let mut intermediates = vec![];
                    let mut stateful = p;

                    // find the nearest materialized ancestor, and keep track of filters we pass
                    while !self.m
                        .have
                        .get(&stateful)
                        .map(|m| !m.is_empty())
                        .unwrap_or(false)
                    {
                        let n = &self.graph[stateful];
                        // joins require their inputs to be materialized. therefore, we know that
                        // any non-materialized ancestors *must* be query_through.
                        assert!(n.is_internal());
                        assert!(n.can_query_through());
                        // if this node is selective (e.g., a filter), increase the filter factor.
                        // we need to keep track of non-selective project/permute nodes too though,
                        // as they increase the cost
                        intermediates.push(n.is_selective());
                        // now walk to the parent.
                        let mut ps = self.graph
                            .neighbors_directed(stateful, petgraph::EdgeDirection::Incoming);
                        // of which there must be at least one
                        stateful = ps.next().expect("recursed all the way to source");
                        // there shouldn't ever be multiple, because neither join nor union
                        // are query_through.
                        assert_eq!(ps.count(), 0);
                    }

                    // find the size of the state we would end up replaying
                    let stateful = &self.graph[stateful];
                    let domain = self.domains.get_mut(&stateful.domain()).unwrap();
                    domain
                        .send(box Packet::StateSizeProbe {
                            node: *stateful.local_addr(),
                        })
                        .unwrap();
                    let mut size = domain.wait_for_state_size().unwrap();

                    // compute the total cost
                    // replay cost
                    let mut cost = replay_cost * size;
                    // filter cost
                    for does_filter in intermediates {
                        cost += filter_cost * size;
                        if does_filter {
                            size /= FILTER_SPECIFICITY;
                        }
                    }
                    // join cost
                    cost += join_cost * size;

                    debug!(self.m.log, "cost of replaying from {:?}: {}", p, cost);
                    (p, cost)
                })
                .min_by_key(|&(_, cost)| cost)
                .map(|(node, cost)| {
                    debug!(self.m.log, "picked replay source {:?}", node; "cost" => cost);
                    node
                })
        })
    }
}
