use std::collections::{BTreeMap, HashMap, HashSet};

use prelude::*;

#[derive(Clone, Debug, Serialize, Deserialize)]
enum Emit {
    AllFrom(IndexPair, Sharding),
    Project {
        emit: HashMap<IndexPair, Vec<usize>>,

        // generated
        emit_l: BTreeMap<LocalNodeIndex, Vec<usize>>,
        cols: HashMap<IndexPair, usize>,
        cols_l: BTreeMap<LocalNodeIndex, usize>,
    },
}

#[derive(Debug, Serialize, Deserialize)]
enum FullWait {
    None,
    Ongoing {
        started: HashSet<LocalNodeIndex>,
        finished: usize,
        buffered: Records,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ReplayPieces {
    buffered: HashMap<LocalNodeIndex, Records>,
    evict: bool,
}

/// A union of a set of views.
#[derive(Debug, Serialize, Deserialize)]
pub struct Union {
    emit: Emit,

    // to sanity check that we're not asked to manage multiple replay paths with different keys
    replay_key_orig: Vec<usize>,

    replay_key: Option<HashMap<LocalNodeIndex, Vec<usize>>>,
    replay_pieces: HashMap<Vec<DataType>, ReplayPieces>,

    required: usize,

    full_wait_state: FullWait,
}

impl Clone for Union {
    fn clone(&self) -> Self {
        Union {
            emit: self.emit.clone(),
            required: self.required,
            replay_key_orig: Vec::new(),
            // nothing can have been received yet
            replay_key: None,
            replay_pieces: HashMap::new(),
            full_wait_state: FullWait::None,
        }
    }
}

impl Union {
    /// Construct a new union operator.
    ///
    /// When receiving an update from node `a`, a union will emit the columns selected in `emit[a]`.
    /// `emit` only supports omitting columns, not rearranging them.
    pub fn new(emit: HashMap<NodeIndex, Vec<usize>>) -> Union {
        assert!(!emit.is_empty());
        for emit in emit.values() {
            let mut last = &emit[0];
            for i in emit {
                if i < last {
                    unimplemented!(
                        "union doesn't support column reordering; got emit = {:?}",
                        emit
                    );
                }
                last = i;
            }
        }
        let emit: HashMap<_, _> = emit.into_iter().map(|(k, v)| (k.into(), v)).collect();
        let parents = emit.len();
        Union {
            emit: Emit::Project {
                emit,
                emit_l: BTreeMap::new(),
                cols: HashMap::new(),
                cols_l: BTreeMap::new(),
            },
            required: parents,
            replay_key_orig: Vec::new(),
            replay_key: None,
            replay_pieces: HashMap::new(),
            full_wait_state: FullWait::None,
        }
    }

    /// Construct a new union operator meant to de-shard a sharded data-flow subtree.
    pub fn new_deshard(parent: NodeIndex, sharding: Sharding) -> Union {
        let shards = sharding.shards().unwrap();
        Union {
            emit: Emit::AllFrom(parent.into(), sharding),
            required: shards,
            replay_key_orig: Vec::new(),
            replay_key: None,
            replay_pieces: HashMap::new(),
            full_wait_state: FullWait::None,
        }
    }

    pub fn is_shard_merger(&self) -> bool {
        if let Emit::AllFrom(..) = self.emit {
            true
        } else {
            false
        }
    }
}

impl Ingredient for Union {
    fn take(&mut self) -> NodeOperator {
        Clone::clone(self).into()
    }

    fn ancestors(&self) -> Vec<NodeIndex> {
        match self.emit {
            Emit::AllFrom(p, _) => vec![p.as_global()],
            Emit::Project { ref emit, .. } => emit.keys().map(|k| k.as_global()).collect(),
        }
    }

    fn on_connected(&mut self, g: &Graph) {
        if let Emit::Project {
            ref mut cols,
            ref emit,
            ..
        } = self.emit
        {
            cols.extend(emit.keys().map(|&n| (n, g[n.as_global()].fields().len())));
        }
    }

    fn on_commit(&mut self, _: NodeIndex, remap: &HashMap<NodeIndex, IndexPair>) {
        match self.emit {
            Emit::Project {
                ref mut emit,
                ref mut cols,
                ref mut emit_l,
                ref mut cols_l,
            } => {
                use std::mem;
                let mapped_emit = emit
                    .drain()
                    .map(|(mut k, v)| {
                        k.remap(remap);
                        emit_l.insert(*k, v.clone());
                        (k, v)
                    })
                    .collect();
                let mapped_cols = cols
                    .drain()
                    .map(|(mut k, v)| {
                        k.remap(remap);
                        cols_l.insert(*k, v);
                        (k, v)
                    })
                    .collect();
                mem::replace(emit, mapped_emit);
                mem::replace(cols, mapped_cols);
            }
            Emit::AllFrom(ref mut p, _) => {
                p.remap(remap);
            }
        }
    }

    fn on_input(
        &mut self,
        _: &mut Executor,
        from: LocalNodeIndex,
        rs: Records,
        _: &mut Tracer,
        _: Option<&[usize]>,
        _: &DomainNodes,
        _: &StateMap,
    ) -> ProcessingResult {
        match self.emit {
            Emit::AllFrom(..) => ProcessingResult {
                results: rs,
                ..Default::default()
            },
            Emit::Project { ref emit_l, .. } => {
                let rs = rs
                    .into_iter()
                    .map(move |rec| {
                        let (r, pos) = rec.extract();

                        // yield selected columns for this source
                        // TODO: if emitting all in same order then avoid clone
                        let res = emit_l[&from].iter().map(|&col| r[col].clone()).collect();

                        // return new row with appropriate sign
                        if pos {
                            Record::Positive(res)
                        } else {
                            Record::Negative(res)
                        }
                    })
                    .collect();
                ProcessingResult {
                    results: rs,
                    ..Default::default()
                }
            }
        }
    }

    fn on_input_raw(
        &mut self,
        ex: &mut Executor,
        from: LocalNodeIndex,
        rs: Records,
        tracer: &mut Tracer,
        replay: &ReplayContext,
        n: &DomainNodes,
        s: &StateMap,
    ) -> RawProcessingResult {
        use std::mem;

        // NOTE: in the special case of us being a shard merge node (i.e., when
        // self.emit.is_empty()), `from` will *actually* hold the shard index of
        // the sharded egress that sent us this record. this should make everything
        // below just work out.
        match *replay {
            ReplayContext::None => {
                // prepare for a little song-and-dance for the borrow-checker
                let mut absorb_for_full = false;
                if let FullWait::Ongoing { ref started, .. } = self.full_wait_state {
                    // ongoing full replay. is this a record we need to not disappear (i.e.,
                    // message 2 in the explanation)?
                    if started.len() != self.required && started.contains(&from) {
                        // yes! keep it.
                        // but we can't borrow self mutably here to call on_input, since we
                        // borrowed started immutably above...
                        absorb_for_full = true;
                    }
                }

                if absorb_for_full {
                    // we shouldn't be stepping on any partial materialization toes, but let's
                    // make sure. i'm not 100% sure at this time if it's true.
                    //
                    // hello future self. clearly, i was correct that i might be incorrect. let me
                    // help: the only reason this assert is here is because we consume rs so that
                    // we can process it. the replay code below seems to require rs to be
                    // *unprocessed* (not sure why), and so once we add it to our buffer, we can't
                    // also execute the code below. if you fix that, you should be all good!
                    assert!(self.replay_key.is_none() || self.replay_pieces.is_empty());

                    // process the results (self is okay to have mutably borrowed here)
                    let rs = self.on_input(ex, from, rs, tracer, None, n, s).results;

                    // *then* borrow self.full_wait_state again
                    if let FullWait::Ongoing {
                        ref mut buffered, ..
                    } = self.full_wait_state
                    {
                        // absorb into the buffer
                        buffered.extend(rs.iter().cloned());
                        // we clone above so that we can also return the processed results
                        return RawProcessingResult::Regular(ProcessingResult {
                            results: rs,
                            ..Default::default()
                        });
                    } else {
                        unreachable!();
                    }
                }

                if self.replay_key.is_none() || self.replay_pieces.is_empty() {
                    // no replay going on, so we're done.
                    return RawProcessingResult::Regular(
                        self.on_input(ex, from, rs, tracer, None, n, s),
                    );
                }

                // partial replays are flowing through us, and at least one piece is being waited
                // for. we need to keep track of any records that succeed a replay piece (and thus
                // aren't included in it) before the other pieces come in. note that it's perfectly
                // safe for us to also forward them, since they'll just be dropped when they miss
                // in the downstream node. in fact, we *must* forward them, becuase there may be
                // *other* nodes downstream that do *not* have holes for the key in question.
                for r in &rs {
                    let k = &self.replay_key.as_ref().unwrap()[&from];
                    // XXX: the clone + collect here is really sad
                    if let Some(ref mut pieces) = self
                        .replay_pieces
                        .get_mut(&k.iter().map(|&c| r[c].clone()).collect::<Vec<_>>())
                    {
                        if let Some(ref mut rs) = pieces.buffered.get_mut(&from) {
                            // we've received a replay piece from this ancestor already for this
                            // key, and are waiting for replay pieces from other ancestors. we need
                            // to incorporate this record into the replay piece so that it doesn't
                            // end up getting lost.
                            rs.push(r.clone());
                        } else {
                            // we haven't received a replay piece for this key from this ancestor
                            // yet, so we know that the eventual replay piece must include this
                            // record.
                        }
                    } else {
                        // we're not waiting on replay pieces for this key
                    }
                }

                RawProcessingResult::Regular(self.on_input(ex, from, rs, tracer, None, n, s))
            }
            ReplayContext::Full { last } => {
                // this part is actually surpringly straightforward, but the *reason* it is
                // straightforward is not. let's walk through what we know first:
                //
                //  - we know that there is only exactly one full replay going on
                //  - we know that the target domain buffers any messages not tagged as
                //    replays once it has seen the *first* replay
                //  - we know that the target domain will apply all bufferd messages after it sees
                //    last = true
                //
                // we therefore have two jobs to do:
                //
                //  1. ensure that we only send one message with last = true.
                //  2. ensure that all messages we forward after we allow the first replay message
                //     through logically follow the replay.
                //
                // step 1 is pretty easy -- we only set last = true when we've seen last = true
                // from all our ancestors. until that is the case, we just set last = false in all
                // our outgoing messages (even if they had last set).
                //
                // step 2 is trickier. consider the following in a union U
                // across two ancestors, L and R:
                //
                //  1. L sends first replay
                //  2. L sends a normal message
                //  3. R sends a normal message
                //  4. R sends first replay
                //  5. U receives L's replay
                //  6. U receives R's message
                //  7. U receives R's replay
                //
                // when should U emit the first replay? if it does it eagerly (i.e., at 1), then
                // R's normal message at 3 (which is also present in R's replay) will be buffered
                // and replayed at the target domain, since it comes after the first replay
                // message. instead, we must delay sending the first replay until we have seen the
                // first replay from *every* ancestor. in other words, 1 must be captured, and only
                // emitted at 5. unfortunately, 2 also wants to cause us pain. it must *not* be
                // sent until after 5 either, because otherwise it would be dropped by the target
                // domain, which is *not* okay since it is not included in L's replay.
                //
                // phew.
                //
                // first, how do we emit *two* replay messages at 5? it turns out that we're in
                // luck. because only one replay can be going on at a time, the target domain
                // doesn't actually care about which tag we use for the forward (well, as long as
                // it is *one* of the full replay tags). and since we're a union, we can simply
                // fold 1 and 4 into a single update, and then emit that!
                //
                // second, how do we ensure that 2 also gets sent *after* the replay has started.
                // again, we're in luck. we can simply absorb 2 into the replay when we detect that
                // there's a replay which hasn't started yet! we do that above (in the other match
                // arm). feel free to go check. interestingly enough, it's also fine for us to
                // still emit 2 (i.e., not capture it), since it'll just be dropped by the target
                // domain.
                let mut rs = self.on_input(ex, from, rs, tracer, None, n, s).results;
                if let FullWait::None = self.full_wait_state {
                    if self.required == 1 {
                        // no need to ever buffer
                        return RawProcessingResult::FullReplay(rs, last);
                    }

                    // we need to hold this back until we've received one from every ancestor
                    let mut s = HashSet::new();
                    s.insert(from);
                    self.full_wait_state = FullWait::Ongoing {
                        started: s,
                        finished: if last { 1 } else { 0 },
                        buffered: rs,
                    };
                    return RawProcessingResult::CapturedFull;
                }

                let exit;
                match self.full_wait_state {
                    FullWait::Ongoing {
                        ref mut started,
                        ref mut finished,
                        ref mut buffered,
                    } => {
                        if last {
                            *finished += 1;
                        }

                        if *finished == self.required {
                            // we can just send everything and we're done!
                            // make sure to include what's in *this* replay.
                            buffered.append(&mut *rs);
                            exit =
                                RawProcessingResult::FullReplay(buffered.split_off(0).into(), true);
                        // fall through to below match where we'll set FullWait::None
                        } else {
                            if started.len() != self.required {
                                if started.insert(from) && started.len() == self.required {
                                    // we can release all buffered replays!
                                    buffered.append(&mut *rs);
                                    return RawProcessingResult::FullReplay(
                                        buffered.split_off(0).into(),
                                        false,
                                    );
                                }
                            } else {
                                // common case: replay has started, and not yet finished
                                // no need to buffer, nothing to see here, move along
                                debug_assert_eq!(buffered.len(), 0);
                                return RawProcessingResult::FullReplay(rs, false);
                            }

                            // if we fell through here, it means we're still missing the first
                            // replay from at least one ancestor, so we need to buffer
                            buffered.append(&mut *rs);
                            return RawProcessingResult::CapturedFull;
                        }
                    }
                    _ => unreachable!(),
                }

                // we only fall through here if we're done!
                // and it's only because we can't change self.full_wait_state while matching on it
                self.full_wait_state = FullWait::None;
                exit
            }
            ReplayContext::Partial {
                ref key_cols,
                ref keys,
            } => {
                // FIXME: with multi-partial indices, we may now need to track *multiple* ongoing
                // replays!

                // TODO: which node is key_col an index of?
                if let Emit::AllFrom(_, Sharding::ByColumn(shard_col, _)) = self.emit {
                    assert_eq!(key_cols.len(), 1);
                    if shard_col == key_cols[0] {
                        // No need to buffer since request should only be for one shard
                        assert!(self.replay_pieces.is_empty());
                        return RawProcessingResult::ReplayPiece {
                            rows: rs,
                            keys: keys.iter().cloned().collect(),
                            captured: HashSet::new(),
                        };
                    }
                }

                if self.replay_key.is_none() {
                    // the replay key is for our *output* column
                    // which might translate to different columns in our inputs
                    match self.emit {
                        Emit::AllFrom(..) => {
                            self.replay_key =
                                Some(Some((from, key_cols.clone())).into_iter().collect());
                        }
                        Emit::Project { ref emit_l, .. } => {
                            self.replay_key = Some(
                                emit_l
                                    .iter()
                                    .map(|(src, emit)| {
                                        (*src, key_cols.iter().map(|&c| emit[c]).collect())
                                    })
                                    .collect(),
                            );
                        }
                    }
                    self.replay_key_orig = key_cols.clone();
                } else {
                    // make sure multiple different replay paths aren't getting mixed
                    if &self.replay_key_orig != key_cols {
                        unimplemented!();
                    }
                }

                let mut rs_by_key = rs
                    .into_iter()
                    .map(|r| {
                        (
                            key_cols.iter().map(|&c| r[c].clone()).collect::<Vec<_>>(),
                            r,
                        )
                    })
                    .fold(HashMap::new(), |mut hm, (key, r)| {
                        hm.entry(key).or_insert_with(Records::default).push(r);
                        hm
                    });

                // we're going to pull a little hack here for the sake of performance.
                // (heard that before...)
                // we can't borrow self in both closures below, even though `self.on_input` doesn't
                // access `self.replay_pieces`. if only the compiler was more clever. we get around
                // this by mem::swapping a temporary (empty) HashMap (which doesn't allocate).
                let mut replay_pieces_tmp = HashMap::with_capacity(0);
                mem::swap(&mut self.replay_pieces, &mut replay_pieces_tmp);

                let required = self.required; // can't borrow self in closures below
                let mut released = HashSet::new();
                let mut captured = HashSet::new();
                let rs = {
                    keys.iter()
                        .filter_map(|key| {
                            let rs = rs_by_key.remove(&key[..]).unwrap_or_else(Records::default);

                            // store this replay piece
                            use std::collections::hash_map::Entry;
                            match replay_pieces_tmp.entry(key.clone()) {
                                Entry::Occupied(e) => {
                                    if e.get().buffered.contains_key(&from) {
                                        // chained unions are not yet supported.
                                        // we'd need to keep a queue of replays from each side,
                                        // apply writes to all queued replays from that side, and
                                        // then emit all front-of-queue replays in lock-step.
                                        unimplemented!("detected chained union");
                                    }
                                    if e.get().buffered.len() == required - 1 {
                                        // release!
                                        let mut m = e.remove();
                                        m.buffered.insert(from, rs);
                                        Some((key, m))
                                    } else {
                                        e.into_mut().buffered.insert(from, rs);
                                        captured.insert(key.clone());
                                        None
                                    }
                                }
                                Entry::Vacant(h) => {
                                    let mut m = HashMap::new();
                                    m.insert(from, rs);
                                    if required == 1 {
                                        Some((
                                            key,
                                            ReplayPieces {
                                                buffered: m,
                                                evict: false,
                                            },
                                        ))
                                    } else {
                                        h.insert(ReplayPieces {
                                            buffered: m,
                                            evict: false,
                                        });
                                        captured.insert(key.clone());
                                        None
                                    }
                                }
                            }
                        })
                        .flat_map(|(key, pieces)| {
                            if pieces.evict {
                                // TODO XXX TODO XXX TODO XXX TODO
                                eprintln!("!!! need to issue an eviction after replaying key");
                            }
                            released.insert(key.clone());
                            pieces.buffered.into_iter()
                        })
                        .flat_map(|(from, rs)| {
                            self.on_input(ex, from, rs, tracer, Some(&key_cols[..]), n, s)
                                .results
                        })
                        .collect()
                };

                // and swap back replay pieces
                mem::swap(&mut self.replay_pieces, &mut replay_pieces_tmp);

                RawProcessingResult::ReplayPiece {
                    rows: rs,
                    keys: released,
                    captured,
                }
            }
        }
    }

    fn on_eviction(
        &mut self,
        from: LocalNodeIndex,
        key_columns: &[usize],
        keys: &mut Vec<Vec<DataType>>,
    ) {
        if self.replay_key_orig.is_empty() {
            return;
        }

        if &self.replay_key_orig[..] != key_columns {
            unimplemented!("multiple different replay paths flowing through union");
        }

        keys.retain(|key| {
            if let Some(e) = self.replay_pieces.get_mut(key) {
                if e.buffered.contains_key(&from) {
                    // we've already received something from left, but it has now been evicted.
                    // we can't remove the buffered replay, since we'll then get confused when the
                    // other parts of the replay arrive from the other sides. we also can't drop
                    // the eviction, because the eviction might be there to, say, ensure key
                    // monotonicity in the face of joins. instead, we have to *buffer* the eviction
                    // and emit it immediately after releasing the replay for this key. we do need
                    // to emit the replay, as downstream nodes are waiting for it.
                    //
                    // NOTE: e.evict may already be true here, as we have no guarantee that
                    // upstream nodes won't send multiple evictions in a row (e.g., joins evictions
                    // could cause this).
                    e.evict = true;
                    return false;
                } else if !e.buffered.is_empty() {
                    // we've received replay pieces for this key from other ancestors, but not from
                    // this one. this indicates that the eviction happened logically before the
                    // replay requset came in, so we do in fact want to do the replay first
                    // downstream.
                }
            }
            true
        });
    }

    fn suggest_indexes(&self, _: NodeIndex) -> HashMap<NodeIndex, Vec<usize>> {
        // index nothing (?)
        HashMap::new()
    }

    fn resolve(&self, col: usize) -> Option<Vec<(NodeIndex, usize)>> {
        match self.emit {
            Emit::AllFrom(p, _) => Some(vec![(p.as_global(), col)]),
            Emit::Project { ref emit, .. } => Some(
                emit.iter()
                    .map(|(src, emit)| (src.as_global(), emit[col]))
                    .collect(),
            ),
        }
    }

    fn description(&self, detailed: bool) -> String {
        // Ensure we get a consistent output by sorting.
        match self.emit {
            Emit::AllFrom(..) => "⊍".to_string(),
            Emit::Project { .. } if !detailed => String::from("⋃"),
            Emit::Project { ref emit, .. } => {
                let mut emit = emit.iter().collect::<Vec<_>>();
                emit.sort();
                emit.iter()
                    .map(|&(src, emit)| {
                        let cols = emit
                            .iter()
                            .map(|e| e.to_string())
                            .collect::<Vec<_>>()
                            .join(", ");
                        format!("{}:[{}]", src.as_global().index(), cols)
                    })
                    .collect::<Vec<_>>()
                    .join(" ⋃ ")
            }
        }
    }
    fn parent_columns(&self, col: usize) -> Vec<(NodeIndex, Option<usize>)> {
        match self.emit {
            Emit::AllFrom(p, _) => vec![(p.as_global(), Some(col))],
            Emit::Project { ref emit, .. } => emit
                .iter()
                .map(|(src, emit)| (src.as_global(), Some(emit[col])))
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use ops;

    fn setup() -> (ops::test::MockGraph, IndexPair, IndexPair) {
        let mut g = ops::test::MockGraph::new();
        let l = g.add_base("left", &["l0", "l1"]);
        let r = g.add_base("right", &["r0", "r1", "r2"]);

        let mut emits = HashMap::new();
        emits.insert(l.as_global(), vec![0, 1]);
        emits.insert(r.as_global(), vec![0, 2]);
        g.set_op("union", &["u0", "u1"], Union::new(emits), false);
        (g, l, r)
    }

    #[test]
    fn it_describes() {
        let (u, l, r) = setup();
        assert_eq!(
            u.node().description(true),
            format!("{}:[0, 1] ⋃ {}:[0, 2]", l, r)
        );
    }

    #[test]
    fn it_works() {
        let (mut u, l, r) = setup();

        // forward from left should emit original record
        let left = vec![1.into(), "a".into()];
        assert_eq!(u.one_row(l, left.clone(), false), vec![left].into());

        // forward from right should emit subset record
        let right = vec![1.into(), "skipped".into(), "x".into()];
        assert_eq!(
            u.one_row(r, right.clone(), false),
            vec![vec![1.into(), "x".into()]].into()
        );
    }

    #[test]
    fn it_suggests_indices() {
        use std::collections::HashMap;
        let (u, _, _) = setup();
        let me = 1.into();
        assert_eq!(u.node().suggest_indexes(me), HashMap::new());
    }

    #[test]
    fn it_resolves() {
        let (u, l, r) = setup();
        let r0 = u.node().resolve(0);
        assert!(r0
            .as_ref()
            .unwrap()
            .iter()
            .any(|&(n, c)| n == l.as_global() && c == 0));
        assert!(r0
            .as_ref()
            .unwrap()
            .iter()
            .any(|&(n, c)| n == r.as_global() && c == 0));
        let r1 = u.node().resolve(1);
        assert!(r1
            .as_ref()
            .unwrap()
            .iter()
            .any(|&(n, c)| n == l.as_global() && c == 1));
        assert!(r1
            .as_ref()
            .unwrap()
            .iter()
            .any(|&(n, c)| n == r.as_global() && c == 2));
    }
}
