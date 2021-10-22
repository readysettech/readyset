#![deny(
    clippy::dbg_macro,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::unimplemented,
    clippy::unreachable
)]

use crate::controller::keys::{self, IndexRef};
use crate::controller::migrate::DomainMigrationPlan;
use crate::controller::state::graphviz;
use dataflow::payload::{ReplayPathSegment, SourceSelection, TriggerEndpoint};
use dataflow::prelude::*;
use dataflow::DomainRequest;
use noria::ReadySetError;
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::convert::TryFrom;
use tracing::{debug, trace};
use vec1::Vec1;

pub(super) struct Plan<'a> {
    m: &'a mut super::Materializations,
    graph: &'a Graph,
    node: NodeIndex,
    dmp: &'a mut DomainMigrationPlan,
    partial: bool,

    tags: HashMap<Index, Vec<(Tag, DomainIndex)>>,
    /// New paths added in this run of the planner.
    paths: HashMap<Tag, Vec<NodeIndex>>,
    /// Paths that already exist for this node.
    old_paths: HashMap<Tag, Vec<NodeIndex>>,
    pending: Vec<PendingReplay>,
}

#[derive(Debug)]
pub(super) struct PendingReplay {
    pub(super) tag: Tag,
    pub(super) source: LocalNodeIndex,
    pub(super) source_domain: DomainIndex,
}

impl<'a> Plan<'a> {
    pub(super) fn new(
        m: &'a mut super::Materializations,
        graph: &'a Graph,
        node: NodeIndex,
        dmp: &'a mut DomainMigrationPlan,
    ) -> Plan<'a> {
        let partial = m.partial.contains(&node);
        let old_paths = m
            .paths
            .entry(node)
            .or_insert_with(|| HashMap::new())
            .clone();
        Plan {
            m,
            graph,
            node,
            dmp,
            paths: Default::default(),
            old_paths,

            partial,

            pending: Vec::new(),
            tags: Default::default(),
        }
    }

    fn paths(&mut self, index: &Index) -> Result<Vec<Vec<IndexRef>>, ReadySetError> {
        let graph = self.graph;
        let ni = self.node;
        let mut paths = keys::replay_paths_for_opt(
            graph,
            IndexRef {
                node: ni,
                index: if self.partial {
                    Some(index.clone())
                } else {
                    None
                },
            },
            |stop_ni| {
                stop_ni != ni
                    && self
                        .m
                        .have
                        .get(&stop_ni)
                        .map(|x| !x.is_empty())
                        .unwrap_or(false)
            },
        )?
        .into_iter()
        .map(|mut path| {
            path.reverse();
            path
        })
        .collect::<Vec<_>>();

        // it doesn't make sense for a replay path to have <=1 nodes in it; this can, however,
        // happen now that we run the materialization planner when adding indices to already
        // materialized nodes (presumably because they might originate the columns indices are
        // being added to?)
        //
        // *NOTE* changing this invariant (replay paths contain >=1 nodes) will hit an
        // `unreachable!` later on!
        //
        // also, don't include paths that don't end at this node.
        #[allow(clippy::unwrap_used)] // We check that len() > 1 before the unwrap
        paths.retain(|x| x.len() > 1 && x.last().unwrap().node == ni);

        // since we cut off part of each path, we *may* now have multiple paths that are the same
        // (i.e., if there was a union above the nearest materialization). this would be bad, as it
        // would cause a domain to request replays *twice* for a key from one view!
        //
        // As for this `sort_by`, story time!
        // Imagine you had two unions in a diamond shape:
        // a -> b -> u_1 -> d -> u_2 -> f
        //   -> c ->     -> e ->
        //
        // There are going to be four different paths:
        // a -> b -> u_1 -> d -> u_2 -> f
        // a -> b -> u_1 -> e -> u_2 -> f
        // a -> c -> u_1 -> d -> u_2 -> f
        // a -> c -> u_1 -> e -> u_2 -> f
        //
        // For the sake of making this easier to understand, let's represent those paths based only
        // on the node they choose at each bifurcation:
        // 1. (b, d)
        // 2. (b, e)
        // 3. (c, d)
        // 4. (c, e)
        //
        // Now, before this `sort_by` was introduced, the paths were sorted like that.
        // The problem is that unions, when receiving replay messages, wait until they receive all
        // of them to release the full replay.
        // That waiting (buffering) mechanism relies solely on how many messages they get.
        // Once they get as many messages as ancestors they have, they release the full replay.
        //
        // Now, following the example, this is what happens:
        // 1. u_1 receives message from b, and buffers it
        // 2. u_1 receives message from b (which will be the same as before), now it has two
        // messages, so it releases it. WRONG.
        //
        // So, the paths must be sorted based on the node indexes, but backwards
        // 1. (b, d)
        // 2. (c, d)
        // 3. (b, e)
        // 4. (c, e)
        //
        // That's what we are doing here.
        // TODO(fran): Unless there's a better fix that I can't picture right now, this is still
        //  less than ideal imho (very prone to errors).
        //  A better fix would require a redesign of how we handle replay paths in the presence of
        //  unions, since even with this fix, we still generate paths that will be traversed more
        //  than once (in the example, we go through b and c twice).
        paths.sort_by(|a, b| {
            if a.len() != b.len() {
                a.cmp(b)
            } else {
                a.iter()
                    .rev()
                    .zip(b.iter().rev())
                    .fold(Ordering::Equal, |acc, (item_a, item_b)| {
                        acc.then(item_a.node.cmp(&item_b.node))
                    })
            }
        });
        paths.dedup();

        // all columns better resolve if we're doing partial
        if self.partial && !paths.iter().all(|p| p.iter().all(|cr| cr.index.is_some())) {
            internal!("tried to be partial over replay paths that require full materialization: paths = {:?}", paths);
        }

        Ok(paths)
    }

    /// Finds the appropriate replay paths for the given index, and inform all domains on those
    /// paths about them. It also notes if any data backfills will need to be run, which is
    /// eventually reported back by `finalize`.
    #[allow(clippy::cognitive_complexity)]
    pub(super) fn add(&mut self, index_on: Index) -> Result<(), ReadySetError> {
        // if we're full and we already have some paths added... (either this run, or from previous
        // runs)
        if !self.partial && (!self.paths.is_empty() || !self.old_paths.is_empty()) {
            // ...don't add any more replay paths, because...
            // non-partial views should not have one replay path per index. that would cause us to
            // replay several times, even though one full replay should always be sufficient.
            // we do need to keep track of the fact that there should be an index here though.
            self.tags.entry(index_on).or_default();
            return Ok(());
        }

        let paths = self.paths(&index_on)?;

        // all right, story time!
        //
        // image you have this graph:
        //
        //     a     b
        //     +--+--+
        //        |
        //       u_1
        //        |
        //     +--+--+
        //     c     d
        //     +--+--+
        //        |
        //       u_2
        //        |
        //     +--+--+
        //     e     f
        //     +--+--+
        //        |
        //       u_3
        //        |
        //        v
        //
        // where c-f are all stateless. you will end up with 8 paths for replays to v.
        // a and b will both appear as the root of 4 paths, and will be upqueried that many times.
        // while inefficient (TODO), that is not in and of itself a problem. the issue arises at
        // the unions, which need to do union buffering (that is, they need to forward _one_
        // upquery response for each set of upquery responses they get). specifically, u_1 should
        // forward 4 responses, even though it receives 8. u_2 should forward 2 responses, even
        // though it gets 4, etc. we may later optimize that (in theory u_1 should be able to only
        // forward _one_ response to multiple children, and a and b should only be upqueried
        // _once_), but for now we need to deal with the correctness issue that arises if the
        // unions do not buffer correctly.
        //
        // the issue, ultimately, is what the unions "group" upquery responses by. they can't group
        // by tag (like shard mergers do), since there are 8 tags here, so there'd be 8 groups each
        // with one response. here are the replay paths for u_1:
        //
        //  1. a -> c -> e
        //  2. a -> c -> f
        //  3. a -> d -> e
        //  4. a -> d -> f
        //  5. b -> c -> e
        //  6. b -> c -> f
        //  7. b -> d -> e
        //  8. b -> d -> f
        //
        // we want to merge 1 with 5 since they're "going the same way". similarly, we want to
        // merge 2 and 6, 3 and 7, and 4 and 8. the "grouping" here then is really the suffix of
        // the replay's path beyond the union we're looking at. for u_2:
        //
        //  1/5. a/b -> c -> e
        //  2/6. a/b -> c -> f
        //  3/7. a/b -> d -> e
        //  4/8. a/b -> d -> f
        //
        // we want to merge 1/5 and 3/7, again since they are going the same way _from here_.
        // and similarly, we want to merge 2/6 and 4/8.
        //
        // so, how do we communicate this grouping to each of the unions?
        // well, most of the infrastructure is actually already there in the domains.
        // for each tag, each domain keeps some per-node state (`ReplayPathSegment`).
        // we can inject the information there!
        //
        // we're actually going to play an additional trick here, as it allows us to simplify the
        // implementation a fair amount. since we know that tags 1 and 5 are identical beyond u_1
        // (that's what we're grouping by after all!), why don't we just rewrite all 1 tags to 5s?
        // and all 2s to 6s, and so on. that way, at u_2, there will be no replays with tag 1 or 3,
        // only 5 and 7. then we can pull the same trick there -- rewrite all 5s to 7s, so that at
        // u_3 we only need to deal with 7s (and 8s). this simplifies the implementation since
        // unions can now _always_ just group by tags, and it'll just magically work.
        //
        // this approach also gives us the property that we have a deterministic subset of the tags
        // (and of strictly decreasing cardinality!) tags downstream of unions. this may (?)
        // improve cache locality, but could perhaps also allow further optimizations later (?).

        // find all paths through each union with the same suffix
        let assigned_tags: Vec<_> = paths.iter().map(|_| self.m.next_tag()).collect();
        let union_suffixes = paths
            .iter()
            .enumerate()
            .flat_map(|(pi, path)| {
                let graph = &self.graph;
                path.iter()
                    .enumerate()
                    .filter_map(move |(at, &IndexRef { node, .. })| {
                        #[allow(clippy::indexing_slicing)] // replay paths contain valid indices
                        let n = &graph[node];
                        if n.is_union() && !n.is_shard_merger() {
                            let suffix = match path.get((at + 1)..) {
                                Some(x) => x,
                                None => {
                                    // FIXME(eta): would like to return a proper internal!() here
                                    return None;
                                }
                            };
                            Some(((node, suffix), pi))
                        } else {
                            None
                        }
                    })
            })
            .fold(BTreeMap::new(), |mut map, (key, pi)| {
                map.entry(key).or_insert_with(Vec::new).push(pi);
                map
            });

        // map each suffix-sharing group of paths at each union to one tag at that union
        let path_grouping: HashMap<_, _> = union_suffixes
            .into_iter()
            .flat_map(|((union, _suffix), paths)| {
                // at this union, all the given paths share a suffix
                // make all of the paths use a single identifier from that point on

                // paths contains a set of `pi` from above, which are generated from `paths.iter().enumerate()`
                // we have one assigned_tag for each `pi` by construction.
                #[allow(clippy::indexing_slicing)]
                let tag_all_as = assigned_tags[paths[0]];
                paths.into_iter().map(move |pi| ((union, pi), tag_all_as))
            })
            .collect();

        // inform domains about replay paths
        let mut tags = Vec::new();
        for (pi, path) in paths.into_iter().enumerate() {
            // paths contains a set of `pi` from above, which are generated from `paths.iter().enumerate()`
            // we have one assigned_tag for each `pi` by construction.
            #[allow(clippy::indexing_slicing)]
            let tag = assigned_tags[pi];
            // TODO(eta): figure out a way to check partial replay path idempotency
            self.paths.insert(
                tag,
                path.iter().map(|&IndexRef { node, .. }| node).collect(),
            );

            // what index are we using for partial materialization (if any)?
            let mut partial: Option<Index> = None;
            if self.partial {
                #[allow(clippy::unwrap_used)]
                if let Some(IndexRef { index, .. }) = path.first() {
                    // unwrap: ok since `Plan::paths` validates paths if we're partial
                    partial = Some(index.clone().unwrap());
                } else {
                    internal!("Plan::paths should have deleted zero-length path");
                }
            }

            // if this is a partial replay path, and the target node is sharded, then we need to
            // make sure that the last sharder on the path knows to only send the replay response
            // to the requesting shard, as opposed to all shards. in order to do that, that sharder
            // needs to know who it is!
            let mut partial_unicast_sharder = None;
            #[allow(clippy::indexing_slicing)] // paths contain valid node indices
            #[allow(clippy::unwrap_used)] // paths aren't 0-length (internal!'d earlier)
            if partial.is_some() && !self.graph[path.last().unwrap().node].sharded_by().is_none() {
                partial_unicast_sharder = path
                    .iter()
                    .rev()
                    .map(|&IndexRef { node, .. }| node)
                    .find(|&ni| self.graph[ni].is_sharder());
            }

            // first, find out which domains we are crossing
            let mut segments = Vec::new();
            let mut last_domain = None;
            for IndexRef { node, index } in path.clone() {
                #[allow(clippy::indexing_slicing)] // paths contain valid node indices
                let domain = self.graph[node].domain();

                #[allow(clippy::unwrap_used)]
                if last_domain.is_none() || domain != last_domain.unwrap() {
                    segments.push((domain, Vec::new()));
                    last_domain = Some(domain);
                }

                invariant!(!segments.is_empty());

                #[allow(clippy::unwrap_used)] // checked by invariant!()
                segments.last_mut().unwrap().1.push((node, index));
            }

            // technically redundant because path.len() > 1, but still
            invariant!(!segments.is_empty());

            debug!(%tag, "domain replay path is {:?}", segments);

            // tell all the domains about their segment of this replay path
            let mut pending = None;
            let mut seen = HashSet::new();
            for (i, &(domain, ref nodes)) in segments.iter().enumerate() {
                invariant!(!nodes.is_empty());
                // TODO:
                //  a domain may appear multiple times in this list if a path crosses into the same
                //  domain more than once. currently, that will cause a deadlock.
                if seen.contains(&domain) {
                    debug!("{}", graphviz(self.graph, true, self.m));
                    internal!("detected A-B-A domain replay path");
                }
                seen.insert(domain);

                if i == 0 {
                    // check to see if the column index is generated; if so, inform the domain
                    #[allow(clippy::unwrap_used)] // checked by invariant!()
                    let first = nodes.first().unwrap();
                    if let Some(ref index) = first.1 {
                        let mut generated = false;
                        #[allow(clippy::indexing_slicing)] // replay paths contain valid nodes
                        if self.graph[first.0].is_internal() {
                            if let ColumnSource::GeneratedFromColumns(..) =
                                self.graph[first.0].column_source(&index.columns)
                            {
                                generated = true;
                            }
                        }
                        if generated {
                            debug!(
                                domain = %domain.index(),
                                "telling domain about generated columns {:?} on {}",
                                index.columns,
                                first.0.index()
                            );

                            #[allow(clippy::indexing_slicing)] // replay paths contain valid nodes
                            self.dmp.add_message(
                                domain,
                                DomainRequest::GeneratedColumns {
                                    node: self.graph[first.0].local_addr(),
                                    cols: index.columns.clone(),
                                },
                            )?;
                        }
                    }
                }

                // we're not replaying through the starter node
                let skip_first = if i == 0 { 1 } else { 0 };

                // use the local index for each node
                #[allow(clippy::indexing_slicing)] // replay paths contain valid nodes
                let locals: Vec<_> = nodes
                    .iter()
                    .skip(skip_first)
                    .map(|&(ni, ref key)| ReplayPathSegment {
                        node: self.graph[ni].local_addr(),
                        partial_index: key.clone(),
                        force_tag_to: path_grouping.get(&(ni, pi)).copied(),
                    })
                    .collect();

                // the first domain in the chain may *only* have the source node
                // in which case it doesn't need to know about the path
                let locals = if let Ok(locals) = Vec1::try_from(locals) {
                    locals
                } else {
                    invariant_eq!(i, 0);
                    continue;
                };

                // build the message we send to this domain to tell it about this replay path.
                let mut setup = DomainRequest::SetupReplayPath {
                    tag,
                    source: None,
                    path: locals,
                    notify_done: false,
                    partial_unicast_sharder,
                    trigger: TriggerEndpoint::None,
                    raw_path: path.clone(),
                };

                // the first domain also gets to know source node
                if i == 0 {
                    // replay paths contain valid nodes & nodes.len() > 0 checked by invariant!()
                    #[allow(clippy::indexing_slicing)]
                    if let DomainRequest::SetupReplayPath { ref mut source, .. } = setup {
                        *source = Some(self.graph[nodes[0].0].local_addr());
                    }
                }

                if let Some(ref key) = partial {
                    // for partial materializations, nodes need to know how to trigger replays
                    if let DomainRequest::SetupReplayPath {
                        ref mut trigger, ..
                    } = setup
                    {
                        if segments.len() == 1 {
                            // replay is entirely contained within one domain
                            *trigger = TriggerEndpoint::Local(key.clone());
                        } else if i == 0 {
                            // first domain needs to be told about partial replay trigger
                            *trigger = TriggerEndpoint::Start(key.clone());
                        } else if i == segments.len() - 1 {
                            // if the source is sharded, we need to know whether we should ask all
                            // the shards, or just one. if the replay key is the same as the
                            // sharding key, we just ask one, and all is good. if the replay key
                            // and the sharding key differs, we generally have to query *all* the
                            // shards.
                            //
                            // there is, however, an exception to this: if we have two domains that
                            // have the same sharding, but a replay path between them on some other
                            // key than the sharding key, the right thing to do is to *only* query
                            // the same shard as ourselves. this is because any answers from other
                            // shards would necessarily just be with records that do not match our
                            // sharding key anyway, and that we should thus never see.

                            // Note : segments[0] looks something like this :
                            // ( DomainIndex(0),
                            //     [
                            //          (NodeIndex(1), Some([1])),
                            //          (NodeIndex(3), Some([1])),
                            //          (NodeIndex(4), Some([0])),
                            //          (NodeIndex(11), Some([0]))
                            //     ]
                            // )
                            //
                            // NOTE(eta): I have mixed feelings about how the writer of the above
                            //            note thought to be helpful and put the structure thing
                            //            there, but did not think to be helpful enough to refactor
                            //            it to use a more sane data structure...
                            //
                            // NOTE(eta): this code is mild copypasta; another copy exists a few
                            //            lines down

                            #[allow(clippy::indexing_slicing)] // checked by above invariant!()
                            let first_domain_segments = &segments[0].1;
                            invariant!(!first_domain_segments.is_empty());

                            #[allow(clippy::indexing_slicing)] // checked by above invariant!()
                            let first_domain_node_key = &first_domain_segments[0];

                            #[allow(clippy::indexing_slicing)] // replay path node indices are valid
                            let src_sharding = self.graph[first_domain_node_key.0].sharded_by();
                            let shards = src_sharding.shards().unwrap_or(1);
                            let lookup_key_index_to_shard = match src_sharding {
                                Sharding::Random(..) => None,
                                Sharding::ByColumn(c, _) => {
                                    // we want to check the source of the key column. If the source node
                                    // is sharded by that column, we are able to ONLY look at a single
                                    // shard on the source. Otherwise, we need to check all of the shards.
                                    let key_column_source = &first_domain_node_key.1;
                                    match key_column_source {
                                        Some(source_index) => {
                                            if source_index.len() == 1 {
                                                #[allow(clippy::indexing_slicing)] // len == 1
                                                if source_index[0] == c {
                                                    // the source node is sharded by the key column.
                                                    Some(0)
                                                } else {
                                                    // the node is sharded by a different column than
                                                    // the key column, so we need to go ahead and query all
                                                    // shards. BUMMER.
                                                    None
                                                }
                                            } else {
                                                // we're using a compound key to look up into a node that's
                                                // sharded by a single column. if the sharding key is one
                                                // of the lookup keys, then we indeed only need to look at
                                                // one shard, otherwise we need to ask all
                                                source_index
                                                    .columns
                                                    .iter()
                                                    .position(|source_column| source_column == &c)
                                            }
                                        }
                                        // This means the key column has no provenance in
                                        // the source. This could be because it comes from multiple columns.
                                        // Or because the node is fully materialized so replays are not necessary.
                                        None => None,
                                    }
                                }
                                s if s.is_none() => None,
                                s => internal!("unhandled new sharding pattern {:?}", s),
                            };

                            let selection = if let Some(i) = lookup_key_index_to_shard {
                                // if we are not sharded, all is okay.
                                //
                                // if we are sharded:
                                //
                                //  - if there is a shuffle above us, a shard merger + sharder
                                //    above us will ensure that we hear the replay response.
                                //
                                //  - if there is not, we are sharded by the same column as the
                                //    source. this also means that the replay key in the
                                //    destination is the sharding key of the destination. to see
                                //    why, consider the case where the destination is sharded by x.
                                //    the source must also be sharded by x for us to be in this
                                //    case. we also know that the replay lookup key on the source
                                //    must be x since lookup_on_shard_key == true. since no shuffle
                                //    was introduced, src.x must resolve to dst.x assuming x is not
                                //    aliased in dst. because of this, it should be the case that
                                //    KeyShard == SameShard; if that were not the case, the value
                                //    in dst.x should never have reached dst in the first place.
                                SourceSelection::KeyShard {
                                    key_i_to_shard: i,
                                    nshards: shards,
                                }
                            } else {
                                // replay key != sharding key
                                // normally, we'd have to query all shards, but if we are sharded
                                // the same as the source (i.e., there are no shuffles between the
                                // source and us), then we should *only* query the same shard of
                                // the source (since it necessarily holds all records we could
                                // possibly see).
                                //
                                // note that the no-sharding case is the same as "ask all shards"
                                // except there is only one (shards == 1).
                                #[allow(clippy::indexing_slicing)] // replay path node indices valid
                                if src_sharding.is_none()
                                    || segments
                                        .iter()
                                        .flat_map(|s| s.1.iter())
                                        .any(|&(n, _)| self.graph[n].is_shard_merger())
                                {
                                    SourceSelection::AllShards(shards)
                                } else {
                                    SourceSelection::SameShard
                                }
                            };

                            debug!(policy = ?selection, %tag, "picked source selection policy");
                            #[allow(clippy::indexing_slicing)] // checked by invariant!() earlier
                            {
                                *trigger = TriggerEndpoint::End(selection, segments[0].0);
                            }
                        }
                    }
                } else {
                    // for full materializations, the last domain should report when it's done
                    if i == segments.len() - 1 {
                        if let DomainRequest::SetupReplayPath {
                            ref mut notify_done,
                            ..
                        } = setup
                        {
                            *notify_done = true;
                            invariant!(pending.is_none());

                            // NOTE(eta): this code is mild copypasta; another copy exists a few
                            //            lines above
                            #[allow(clippy::indexing_slicing)]
                            // checked by segments.len() > 0 invariant!()
                            let first_domain_segments_data = &segments[0];
                            invariant!(!first_domain_segments_data.1.is_empty());

                            #[allow(clippy::indexing_slicing)] // checked by above invariant!()
                            let first_domain_node_key = &first_domain_segments_data.1[0];

                            #[allow(clippy::indexing_slicing)] // replay path node indices valid
                            {
                                pending = Some(PendingReplay {
                                    tag,
                                    source: self.graph[first_domain_node_key.0].local_addr(),
                                    source_domain: first_domain_segments_data.0,
                                });
                            }
                        }
                    }
                }

                if i != segments.len() - 1 {
                    // since there is a later domain, the last node of any non-final domain
                    // must either be an egress or a Sharder. If it's an egress, we need
                    // to tell it about this replay path so that it knows
                    // what path to forward replay packets on.
                    #[allow(clippy::unwrap_used)] // nodes>0 checked by invariant
                    // node indices from replay paths valid
                    #[allow(clippy::indexing_slicing)]
                    let n = &self.graph[nodes.last().unwrap().0];
                    if n.is_egress() {
                        #[allow(clippy::indexing_slicing)] // checked by enclosing if blocks
                        let later_domain_segments = &segments[i + 1].1;
                        invariant!(!later_domain_segments.is_empty());
                        #[allow(clippy::indexing_slicing)] // checked by invariant
                        let later_domain_first_node_key = &later_domain_segments[0];
                        self.dmp.add_message(
                            domain,
                            DomainRequest::UpdateEgress {
                                node: n.local_addr(),
                                new_tx: None,
                                new_tag: Some((tag, later_domain_first_node_key.0)),
                            },
                        )?;
                    } else {
                        invariant!(n.is_sharder());
                    }
                }

                trace!(domain = %domain.index(), "telling domain about replay path");
                self.dmp.add_message(domain, setup)?;
            }

            if !self.partial {
                // this path requires doing a replay and then waiting for the replay to finish
                if let Some(pending) = pending {
                    self.pending.push(pending);
                } else {
                    internal!(
                        "no replay planned for non-partially materialized node {}!",
                        self.node.index()
                    );
                }
            }
            invariant!(last_domain.is_some());
            #[allow(clippy::unwrap_used)] // checked by invariant!()
            tags.push((tag, last_domain.unwrap()));
        }

        self.tags.entry(index_on).or_default().extend(tags);

        Ok(())
    }

    /// Instructs the target node to set up appropriate state for any new indices that have been
    /// added. For new indices added to full materializations, this may take some time (a
    /// re-indexing has to happen), whereas for new indices to partial views it should be nearly
    /// instantaneous.
    ///
    /// Returns a list of backfill replays that need to happen before the migration is complete,
    /// and a set of replay paths for this node indexed by tag.
    pub(super) fn finalize(
        mut self,
    ) -> ReadySetResult<(Vec<PendingReplay>, HashMap<Tag, Vec<NodeIndex>>)> {
        use dataflow::payload::InitialState;

        #[allow(clippy::indexing_slicing)] // the node index we were created with is in graph...
        let our_node = &self.graph[self.node];

        // NOTE: we cannot use the impl of DerefMut here, since it (reasonably) disallows getting
        // mutable references to taken state.
        let s = if let Some(r) = our_node.as_reader() {
            let key = Vec::from(r.key().ok_or_else(|| internal_err("Reader has no key"))?);

            if self.partial {
                invariant!(r.is_materialized());

                #[allow(clippy::indexing_slicing)]
                // the node index we were created with is in graph...
                let last_domain = self.graph[self.node].domain();
                let num_shards = self.dmp.num_shards(last_domain)?;

                // since we're partially materializing a reader node,
                // we need to give it a way to trigger replays.
                #[allow(clippy::indexing_slicing)]
                // the node index we were created with is in graph...
                InitialState::PartialGlobal {
                    gid: self.node,
                    cols: self.graph[self.node].fields().len(),
                    key,
                    trigger_domain: (last_domain, num_shards),
                }
            } else {
                #[allow(clippy::indexing_slicing)]
                // the node index we were created with is in graph...
                InitialState::Global {
                    cols: self.graph[self.node].fields().len(),
                    key,
                    gid: self.node,
                }
            }
        } else {
            // not a reader

            let weak = self.m.added_weak.remove(&self.node).unwrap_or_default();

            if self.partial {
                let strict = self
                    .tags
                    .drain()
                    .map(|(k, paths)| (k, paths.into_iter().map(|(tag, _)| tag).collect()))
                    .collect();
                InitialState::PartialLocal { strict, weak }
            } else {
                let strict = self.tags.drain().map(|(k, _)| k).collect();
                InitialState::IndexedLocal { strict, weak }
            }
        };

        self.dmp.add_message(
            our_node.domain(),
            DomainRequest::PrepareState {
                node: our_node.local_addr(),
                state: s,
            },
        )?;

        if self.m.config.packet_filters_enabled {
            self.setup_packet_filter()?;
        }

        if !self.partial {
            // if we're constructing a new view, there is no reason to replay any given path more
            // than once. we do need to be careful here though: the fact that the source and
            // destination of a path are the same does *not* mean that the path is the same (b/c of
            // unions), and we do not want to eliminate different paths!
            let mut distinct_paths = HashSet::new();
            let paths = &self.paths;
            self.pending.retain(|p| {
                // keep if this path is different
                if let Some(path) = paths.get(&p.tag) {
                    distinct_paths.insert(path)
                } else {
                    // FIXME(eta): proper error handling here! (would be nice to have try_retain)
                    false
                }
            });
        } else {
            invariant!(self.pending.is_empty());
        }
        Ok((self.pending, self.paths))
    }

    fn setup_packet_filter(&mut self) -> ReadySetResult<()> {
        // If the node is partial and also a reader, then traverse the
        // graph upwards to notify the egress node that it should filter
        // any packet that was not requested.
        #[allow(clippy::indexing_slicing)] // the node index we were created with is in graph...
        let our_node = &self.graph[self.node];
        return if self.partial && our_node.is_reader() {
            // Since reader nodes belong to their own domains, their
            // domains should consist only of them + an ingress node.
            // It's fair to assume that the reader node has an ingress node as an ancestor.
            #[allow(clippy::indexing_slicing)] // neighbors_directed returns valid indices
            let ingress_opt = self
                .graph
                .neighbors_directed(self.node, petgraph::Incoming)
                .find(|&n| self.graph[n].is_ingress());
            let ingress = match ingress_opt {
                None => internal!("The current node is a reader, it MUST belong in its own domain, and therefore must be an ingress node ancestor."),
                Some(i) => i
            };
            // Now we look for the egress node, which should be an ancestor of the ingress node.
            #[allow(clippy::indexing_slicing)] // neighbors_directed returns valid indices
            let egress_opt = self
                .graph
                .neighbors_directed(ingress, petgraph::Incoming)
                .find(|&n| self.graph[n].is_egress());
            let egress = match egress_opt {
                // If an ingress node does not have an incoming egress node, that means this reader domain
                // is behind a shard merger.
                // We skip the packet filter setup for now.
                // TODO(fran): Implement packet filtering for shard mergers (https://readysettech.atlassian.net/browse/ENG-183).
                None => return Ok(()),
                Some(e) => e,
            };
            // Get the egress node's local address within that domain.
            #[allow(clippy::indexing_slicing)] // neighbors_directed returns valid indices
            let egress_local_addr = self.graph[egress].local_addr();
            // Communicate the egress node's domain that any packet sent
            // to the reader's domain ingress node should filter any
            // packets not previously requested.
            #[allow(clippy::indexing_slicing)] // neighbors_directed returns valid indices
            self.dmp.add_message(
                self.graph[egress].domain(),
                DomainRequest::AddEgressFilter {
                    egress_node: egress_local_addr,
                    target_node: ingress,
                },
            )?;
            Ok(())
        } else {
            Ok(())
        };
    }
}
