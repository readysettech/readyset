use noria::{invariant, KeyComparison, ReadySetError};
use slog::Logger;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::convert::TryFrom;
use vec1::Vec1;

use crate::prelude::*;
use noria::errors::ReadySetResult;

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

    /// This is a map from (Tag, LocalNodeIndex) to ColumnList
    replay_key: HashMap<(Tag, usize), Vec<usize>>,

    /// Buffered upquery responses that are waiting for more replay pieces.
    ///
    /// Stored as a btreemap so that when we iterate, we first get all the replays of one tag, then
    /// all the records of another tag, etc. This lets us avoid looking up info related to the same
    /// tag more than once. By placing the upquery key in the btreemap key, we can also effectively
    /// check the replay pieces for all values of `requesting_shard` (the `usize`) if we do find a
    /// key match for an update.
    ///
    /// This map's key is really (Tag, Key, requesting_shard)
    replay_pieces: BTreeMap<(Tag, Vec1<DataType>, usize), ReplayPieces>,

    required: usize,

    full_wait_state: FullWait,

    me: Option<NodeIndex>,
}

impl Clone for Union {
    fn clone(&self) -> Self {
        Union {
            emit: self.emit.clone(),
            required: self.required,
            replay_key: Default::default(),
            replay_pieces: Default::default(),
            full_wait_state: FullWait::None,

            me: self.me,
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
            replay_key: Default::default(),
            replay_pieces: Default::default(),
            full_wait_state: FullWait::None,
            me: None,
        }
    }

    /// Construct a new union operator meant to de-shard a sharded data-flow subtree.
    pub fn new_deshard(parent: NodeIndex, sharding: Sharding) -> Union {
        let shards = sharding.shards().unwrap();
        Union {
            emit: Emit::AllFrom(parent.into(), sharding),
            required: shards,
            replay_key: Default::default(),
            replay_pieces: Default::default(),
            full_wait_state: FullWait::None,
            me: None,
        }
    }

    pub fn is_shard_merger(&self) -> bool {
        matches!(self.emit, Emit::AllFrom(..))
    }
}

impl Ingredient for Union {
    fn take(&mut self) -> NodeOperator {
        Clone::clone(self).into()
    }

    fn ancestors(&self) -> Vec<NodeIndex> {
        match self.emit {
            Emit::AllFrom(p, _) => vec![p.as_global()],
            Emit::Project { ref emit, .. } => emit.keys().map(IndexPair::as_global).collect(),
        }
    }

    fn probe(&self) -> HashMap<String, String> {
        let mut hm = HashMap::new();
        hm.insert("captured".into(), format!("{}", self.replay_pieces.len()));
        hm
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

    fn on_commit(&mut self, me: NodeIndex, remap: &HashMap<NodeIndex, IndexPair>) {
        self.me = Some(me);
        match self.emit {
            Emit::Project {
                ref mut emit,
                ref mut cols,
                ref mut emit_l,
                ref mut cols_l,
            } => {
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
                *emit = mapped_emit;
                *cols = mapped_cols;
            }
            Emit::AllFrom(ref mut p, _) => {
                p.remap(remap);
            }
        }
    }

    fn on_input(
        &mut self,
        _: &mut dyn Executor,
        from: LocalNodeIndex,
        rs: Records,
        _: Option<&[usize]>,
        _: &DomainNodes,
        _: &StateMap,
    ) -> ReadySetResult<ProcessingResult> {
        Ok(match self.emit {
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
        })
    }

    fn on_input_raw(
        &mut self,
        ex: &mut dyn Executor,
        from: LocalNodeIndex,
        rs: Records,
        replay: ReplayContext,
        n: &DomainNodes,
        s: &StateMap,
        log: &Logger,
    ) -> ReadySetResult<RawProcessingResult> {
        use std::mem;

        // NOTE: in the special case of us being a shard merge node (i.e., when
        // self.emit.is_empty()), `from` will *actually* hold the shard index of
        // the sharded egress that sent us this record. this should make everything
        // below just work out.
        match replay {
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
                    trace!(log, "union absorbing update for full replay");

                    // we shouldn't be stepping on any partial materialization toes, but let's
                    // make sure. i'm not 100% sure at this time if it's true.
                    //
                    // hello future self. clearly, i was correct that i might be incorrect. let me
                    // help: the only reason this assert is here is because we consume rs so that
                    // we can process it. the replay code below seems to require rs to be
                    // *unprocessed* (not sure why), and so once we add it to our buffer, we can't
                    // also execute the code below. if you fix that, you should be all good!
                    //
                    // TODO: why is this an || past self?
                    invariant!(self.replay_key.is_empty() || self.replay_pieces.is_empty());

                    // process the results (self is okay to have mutably borrowed here)
                    let rs = self.on_input(ex, from, rs, None, n, s)?.results;

                    // *then* borrow self.full_wait_state again
                    if let FullWait::Ongoing {
                        ref mut buffered, ..
                    } = self.full_wait_state
                    {
                        // absorb into the buffer
                        buffered.extend(rs.iter().cloned());
                        // we clone above so that we can also return the processed results
                        return Ok(RawProcessingResult::Regular(ProcessingResult {
                            results: rs,
                            ..Default::default()
                        }));
                    } else {
                        unreachable!();
                    }
                }

                if self.replay_pieces.is_empty() {
                    // no replay going on, so we're done.
                    return Ok(RawProcessingResult::Regular(
                        self.on_input(ex, from, rs, None, n, s)?,
                    ));
                }

                // partial replays are flowing through us, and at least one piece is being waited
                // for. we need to keep track of any records that succeed a replay piece (and thus
                // aren't included in it) before the other pieces come in. note that it's perfectly
                // safe for us to also forward them, since they'll just be dropped when they miss
                // in the downstream node. in fact, we *must* forward them, becuase there may be
                // *other* nodes downstream that do *not* have holes for the key in question.
                // TODO: is the *must* still true now that we take tags into account?
                //
                // unfortunately, finding out which things we need to merge is a bit of a pain,
                // since the bufferd upquery responses may be for different upquery paths with
                // different key columns. in other words, for each record, we conceptually need to
                // check each buffered replay.
                //
                // we have two options here. either, we iterate over the records in an outer loop
                // and the buffered upquery responses in the inner loop, or the other way around.
                // since iterating over the buffered upquery respones includes a btree loopup, we
                // want to do fewer of those, so we do those in the outer loop.
                let replays = self.replay_pieces.iter_mut();
                let mut replay_key = None;
                let mut last_tag = None;

                let rkey_from = if let Emit::AllFrom(..) = self.emit {
                    // from is the shard index
                    0
                } else {
                    from.id()
                };

                for (&(tag, ref replaying_key, _), ref mut pieces) in replays {
                    invariant!(
                        !pieces.buffered.is_empty(),
                        "empty pieces bucket left in replay pieces"
                    );

                    // first, let's see if _any_ of the records in this batch even affect this
                    // buffered upquery response.
                    let buffered = if let Some(rs) = pieces.buffered.get_mut(&from) {
                        rs
                    } else {
                        // we haven't received a replay piece for this key from this ancestor yet,
                        // so we know that the eventual replay piece must include any records in
                        // this batch.
                        continue;
                    };

                    // make sure we use the right key columns for this tag
                    if last_tag.map(|lt| lt != tag).unwrap_or(true) {
                        // starting a new tag
                        replay_key = Some(&self.replay_key[&(tag, rkey_from)]);
                    }
                    let k = replay_key.unwrap();
                    last_tag = Some(tag);

                    // and finally, check all the records
                    for r in &rs {
                        let hit = k
                            .iter()
                            .enumerate()
                            .all(|(ki, &c)| r[c] == replaying_key[ki]);
                        if !hit {
                            // this record is irrelevant as far as this buffered upquery response
                            // goes, since its key does not match the upquery's key.
                            continue;
                        }

                        // we've received a replay piece from this ancestor already for this
                        // key, and are waiting for replay pieces from other ancestors. we need
                        // to incorporate this record into the replay piece so that it doesn't
                        // end up getting lost.
                        buffered.push(r.clone());

                        // it'd be nice if we could avoid doing this exact same key check multiple
                        // times if the same key is being replayed by multiple `requesting_shard`s.
                        // in theory, the btreemap could let us do this by walking forward in the
                        // iterator until we hit the next key or tag, and the rewinding back to
                        // where we were before continuing to the same record. but that won't work
                        // because https://github.com/rust-lang/rfcs/pull/2896.
                        //
                        // we could emulate the same thing by changing `ReplayPieces` to
                        // `RefCell<ReplayPieces>`, using an ref-only iterator that is `Clone`, and
                        // then play some games from there, but it seems not worth it.
                    }
                }

                Ok(RawProcessingResult::Regular(
                    self.on_input(ex, from, rs, None, n, s)?,
                ))
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
                let mut rs = self.on_input(ex, from, rs, None, n, s)?.results;
                if let FullWait::None = self.full_wait_state {
                    if self.required == 1 {
                        // no need to ever buffer
                        return Ok(RawProcessingResult::FullReplay(rs, last));
                    }

                    debug!(
                        log,
                        "union captured start of full replay; has: {}, need: {}", 1, self.required
                    );

                    // we need to hold this back until we've received one from every ancestor
                    let mut s = HashSet::new();
                    s.insert(from);
                    self.full_wait_state = FullWait::Ongoing {
                        started: s,
                        finished: if last { 1 } else { 0 },
                        buffered: rs,
                    };
                    return Ok(RawProcessingResult::CapturedFull);
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
                            debug!(log, "union releasing end of full replay");
                            exit =
                                RawProcessingResult::FullReplay(buffered.split_off(0).into(), true);
                        // fall through to below match where we'll set FullWait::None
                        } else {
                            if started.len() != self.required {
                                if started.insert(from) && started.len() == self.required {
                                    // we can release all buffered replays!
                                    debug!(log, "union releasing full replay");
                                    buffered.append(&mut *rs);
                                    return Ok(RawProcessingResult::FullReplay(
                                        buffered.split_off(0).into(),
                                        false,
                                    ));
                                }
                            } else {
                                // common case: replay has started, and not yet finished
                                // no need to buffer, nothing to see here, move along
                                debug_assert_eq!(buffered.len(), 0);
                                return Ok(RawProcessingResult::FullReplay(rs, false));
                            }

                            debug!(
                                log,
                                "union captured start of full replay; has: {}, need: {}",
                                started.len(),
                                self.required
                            );

                            // if we fell through here, it means we're still missing the first
                            // replay from at least one ancestor, so we need to buffer
                            buffered.append(&mut *rs);
                            return Ok(RawProcessingResult::CapturedFull);
                        }
                    }
                    _ => unreachable!(),
                }

                // we only fall through here if we're done!
                // and it's only because we can't change self.full_wait_state while matching on it
                self.full_wait_state = FullWait::None;
                Ok(exit)
            }
            ReplayContext::Partial {
                key_cols,
                keys,
                requesting_shard,
                unishard,
                tag,
            } => {
                let mut is_shard_merger = false;
                if let Emit::AllFrom(_, _) = self.emit {
                    if unishard {
                        // No need to buffer since request should only be for one shard
                        invariant!(self.replay_pieces.is_empty());
                        return Ok(RawProcessingResult::ReplayPiece {
                            rows: rs,
                            keys: keys.iter().cloned().collect(),
                            captured: HashSet::new(),
                        });
                    }
                    is_shard_merger = true;
                }

                let rkey_from = if let Emit::AllFrom(..) = self.emit {
                    // from is the shard index
                    0
                } else {
                    from.id()
                };

                use std::collections::hash_map::Entry;
                if let Entry::Vacant(v) = self.replay_key.entry((tag, rkey_from)) {
                    // the replay key is for our *output* column
                    // which might translate to different columns in our inputs
                    match self.emit {
                        Emit::AllFrom(..) => {
                            v.insert(Vec::from(key_cols));
                        }
                        Emit::Project { ref emit_l, .. } => {
                            let emit = &emit_l[&from];
                            v.insert(key_cols.iter().map(|&c| emit[c]).collect());

                            // Also insert for all the other sources while we're at it
                            for (&src, emit) in emit_l {
                                if src != from {
                                    self.replay_key.insert(
                                        (tag, src.id()),
                                        key_cols.iter().map(|&c| emit[c]).collect(),
                                    );
                                }
                            }
                        }
                    }
                } else {
                    // we already know the meta info for this tag
                }

                trace!(
                    log,
                    "union got replay piece: {:?} with context {:?}",
                    rs,
                    replay
                );

                let mut rs_by_key = rs
                    .into_iter()
                    .map(|r| {
                        (
                            Vec1::try_from(
                                key_cols.iter().map(|&c| r[c].clone()).collect::<Vec<_>>(),
                            )
                            .unwrap(),
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
                let mut replay_pieces_tmp = mem::take(&mut self.replay_pieces);

                let me = self.me;
                let required = self.required; // can't borrow self in closures below
                let mut released = HashSet::new();
                let mut captured = HashSet::new();
                let rs = {
                    keys.iter()
                        .filter_map(|key| {
                            let key = key.equal().expect("Unions can't handle range queries yet");
                            let rs = rs_by_key.remove(key).unwrap_or_else(Records::default);

                            // store this replay piece
                            use std::collections::btree_map::Entry;
                            match replay_pieces_tmp.entry((tag, key.clone(), requesting_shard)) {
                                Entry::Occupied(e) => {
                                    if e.get().buffered.contains_key(&from) {
                                        // got two upquery responses for the same key for the same
                                        // downstream shard. waaaaaaat?
                                        unimplemented!(
                                            "downstream shard double-requested key (node: {}, src: {}, key cols: {:?})",
                                            me.unwrap().index(),
                                            if is_shard_merger {
                                                format!("shard {}", from.id())
                                            } else {
                                                format!(
                                                    "node {}",
                                                    n[from].borrow().global_addr().index()
                                                )
                                            },
                                            key_cols,
                                        );
                                    }
                                    if e.get().buffered.len() == required - 1 {
                                        // release!
                                        let mut m = e.remove();
                                        m.buffered.insert(from, rs);
                                        Some((key, m))
                                    } else {
                                        e.into_mut().buffered.insert(from, rs);
                                        captured.insert(KeyComparison::from(key.clone()));
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
                                        captured.insert(KeyComparison::from(key.clone()));
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
                            released.insert(KeyComparison::from(key.clone()));
                            pieces.buffered.into_iter()
                        })
                        .map(|(from, rs)| {
                            Ok(self.on_input(ex, from, rs, Some(&key_cols[..]), n, s)?
                                .results)
                        })
                        // FIXME(eta): this iterator / result stuff makes me sad, and is probably
                        // inefficient...
                        .collect::<ReadySetResult<Vec<_>>>()?
                        .into_iter()
                        .flatten()
                        .collect()
                };

                // and swap back replay pieces
                self.replay_pieces = replay_pieces_tmp;

                // here's another bit that's a little subtle:
                //
                // remember how, above, we stripped out the upquery identifier from the replay's
                // tag? consider what happens if we buffer a replay with, say, tag 7.2 (so, upquery
                // 7, path 2). later, when some other replay comes along with, say, tag 7.1, we
                // decide that we're done buffering. we then release the buffered records from the
                // first replay alongside the ones that were in the 7.1 replay. but, we just
                // effectively _changed_ the tag for the records in that first replay! is that ok?
                // it turns out it _is_, and here is the argument for why:
                //
                // first, for a given upquery, let's consider what paths flow through us when there
                // is only a single union on the upquery's path. we know there is then one path for
                // each parent we have. an upquery from below must query each of our parents once
                // to get the complete results. those queries will all have the same upquery id,
                // but different path ids. since there is no union above us or below us on the
                // path, there are exactly as many paths as we have ancestors, and those paths only
                // branch _above_ us. below us, those different paths are the _same_. this means
                // that no matter what path discriminator we forward something with, it will follow
                // the right path.
                //
                // now, what happens if there is a exactly one union on the upquery's path, at or
                // above one of our parents. well, _that_ parent will have as many distinct path
                // identifiers as that union has parents. we know that it will only produce _one_
                // upquery response through (since the union will buffer), and that the repsonse
                // will have (an arbitrary chosen) one of those path identifiers. we know that path
                // discriminators are distinct, so whichever identifier our union ancestor chooses,
                // it will be distinct from the paths that go through our _other_ parents.
                // furthermore, we know that all those path identifiers ultimately share the same
                // path below us. and we also know that the path identifiers of the paths through
                // our other parents share that same path. so choosing any of them is fine.
                //
                // if we have unions above multiple of our parents, the same argument holds.
                //
                // if those unions again have ancestors that are unions, the same argument holds.
                //
                // the missing piece then is how a union that has a union as a _descendant_ knows
                // that any choice it makes for path identifier is fine for that descendant. this
                // is trickier to argue, but the argument goes like this:
                //
                // imagine you have a union immediately followed by a union, followed by some node
                // that wishes to make an upquery. imagine that each union has two incoming edges:
                // the bottom union has two edges to the top union (a "diamond"), and the top union
                // has two incoming edges from disjoint parts of the graph. i don't know why you'd
                // have that, but let's imagine. for the one upquery id here, there are four path
                // identifiers: bottom-left:top-left, bottom-left:top-right, bottom-right:top-left,
                // and bottom-right:top-right. as the top union, we will therefore receive two
                // NOPE
                //
                // FIXME(eta): why does the above comment just suddenly end in NOPE?

                Ok(RawProcessingResult::ReplayPiece {
                    rows: rs,
                    keys: released,
                    captured,
                })
            }
        }
    }

    fn on_eviction(&mut self, from: LocalNodeIndex, tag: Tag, keys: &[KeyComparison]) {
        for key in keys {
            let key = key.equal().expect("Unions can't handle range queries yet");
            // TODO: the key.clone()s here are really sad
            for (_, e) in self
                .replay_pieces
                .range_mut((tag, key.clone(), 0)..=(tag, key.clone(), usize::max_value()))
            {
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
                } else if !e.buffered.is_empty() {
                    // we've received replay pieces for this key from other ancestors, but not from
                    // this one. this indicates that the eviction happened logically before the
                    // replay requset came in, so we do in fact want to do the replay first
                    // downstream.
                }
            }
        }
    }

    fn suggest_indexes(&self, _: NodeIndex) -> HashMap<NodeIndex, Index> {
        // index nothing (?)
        HashMap::new()
    }

    fn resolve(&self, col: usize) -> Result<Option<Vec<(NodeIndex, usize)>>, ReadySetError> {
        match self.emit {
            Emit::AllFrom(p, _) => Ok(Some(vec![(p.as_global(), col)])),
            Emit::Project { ref emit, .. } => Ok(Some(
                emit.iter()
                    .map(|(src, emit)| (src.as_global(), emit[col]))
                    .collect(),
            )),
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
                            .map(ToString::to_string)
                            .collect::<Vec<_>>()
                            .join(", ");
                        format!("{}:[{}]", src.as_global().index(), cols)
                    })
                    .collect::<Vec<_>>()
                    .join(" ⋃ ")
            }
        }
    }

    fn parent_columns(&self, col: usize) -> Result<Vec<(NodeIndex, Option<usize>)>, ReadySetError> {
        match self.emit {
            Emit::AllFrom(p, _) => Ok(vec![(p.as_global(), Some(col))]),
            Emit::Project { ref emit, .. } => Ok(emit
                .iter()
                .map(|(src, emit)| (src.as_global(), Some(emit[col])))
                .collect()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::ops;

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
            u.one_row(r, right, false),
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
        let r0 = u.node().resolve(0).unwrap();
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
        let r1 = u.node().resolve(1).unwrap();
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

    mod replay_capturing {
        use super::*;
        use maplit::hashset;
        use vec1::vec1;

        #[test]
        fn left_then_right() {
            let (mut g, left, right) = setup();
            let tag = Tag::new(0);
            let key: KeyComparison = vec1![1.into()].into();
            let keys = hashset![key.clone()];
            let replay_ctx = || ReplayContext::Partial {
                key_cols: &[0],
                keys: &keys,
                requesting_shard: 0,
                tag,
                unishard: true,
            };

            // point-key replay on the left
            let res = g.input_raw(
                left,
                vec![vec![DataType::from(1), DataType::from("a")]],
                replay_ctx(),
                false,
            );
            match res {
                RawProcessingResult::ReplayPiece { rows, captured, .. } => {
                    // We should capture the replay and emit no records
                    assert_eq!(rows, Records::default());
                    assert_eq!(captured, hashset![key.clone()]);
                }
                _ => unreachable!("expected replay piece, got: {:?}", res),
            }

            // then a point-key replay on the right
            let res = g.input_raw(
                right,
                vec![vec![
                    DataType::from(1),
                    DataType::from("skipped"),
                    DataType::from("b"),
                ]],
                replay_ctx(),
                false,
            );

            match res {
                RawProcessingResult::ReplayPiece {
                    rows,
                    captured,
                    keys,
                    ..
                } => {
                    // we should emit both the originally captured record from the left and the one
                    // from the right
                    assert!(rows.contains(&Record::Positive(vec![1.into(), "a".into()])));
                    assert!(rows.contains(&Record::Positive(vec![1.into(), "b".into()])));

                    assert!(captured.is_empty());
                    assert_eq!(keys, hashset![key]);
                }
                _ => unreachable!("Expected replay piece, got: {:?}", res),
            }
        }

        #[test]
        fn left_then_update_then_right() {
            let (mut g, left, right) = setup();
            let tag = Tag::new(0);
            let key: KeyComparison = vec1![1.into()].into();
            let keys = hashset![key.clone()];
            let replay_ctx = || ReplayContext::Partial {
                key_cols: &[0],
                keys: &keys,
                requesting_shard: 0,
                tag,
                unishard: true,
            };

            // replay on the left
            let res = g.input_raw(
                left,
                vec![
                    vec![DataType::from(1), DataType::from("a")],
                    vec![DataType::from(1), DataType::from("c")],
                ],
                replay_ctx(),
                false,
            );
            match res {
                RawProcessingResult::ReplayPiece { rows, captured, .. } => {
                    // We should capture the replay and emit no records
                    assert_eq!(rows, Records::default());
                    assert_eq!(captured, hashset![key.clone()]);
                }
                _ => unreachable!("expected replay piece, got: {:?}", res),
            }

            // then an update from the left
            let res = g.input_raw(
                left,
                vec![
                    (vec![DataType::from(1), DataType::from("d")], true),
                    (vec![DataType::from(1), DataType::from("c")], false),
                ],
                ReplayContext::None,
                false,
            );
            assert!(matches!(res, RawProcessingResult::Regular(_)));

            // then a replay on the right
            let res = g.input_raw(
                right,
                vec![vec![
                    DataType::from(1),
                    DataType::from("skipped"),
                    DataType::from("b"),
                ]],
                replay_ctx(),
                false,
            );

            match res {
                RawProcessingResult::ReplayPiece {
                    rows,
                    captured,
                    keys,
                    ..
                } => {
                    dbg!(&rows);
                    // we should emit both the captured record from the left with the updates
                    // applied, and the records from the right
                    assert!(rows.contains(&Record::Positive(vec![1.into(), "a".into()])));
                    assert!(rows.contains(&Record::Positive(vec![1.into(), "b".into()])));
                    // assert!(!rows.contains(&Record::Positive(vec![1.into(), "c".into()])));
                    assert!(rows.contains(&Record::Positive(vec![1.into(), "d".into()])));

                    assert!(captured.is_empty());
                    assert_eq!(keys, hashset![key]);
                }
                _ => unreachable!("Expected replay piece, got: {:?}", res),
            }
        }
    }
}
