use itertools::Itertools;
use launchpad::hash::hash;
use launchpad::intervals::{cmp_endbound, cmp_startbound};
use launchpad::Indices;
use noria::KeyComparison;
use noria_errors::invariant;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::{hash_map, BTreeMap, HashMap, HashSet};
use std::convert::{TryFrom, TryInto};
use std::ops::Bound;
use test_strategy::Arbitrary;
use tracing::{debug, error, trace};
use vec1::Vec1;

use crate::prelude::*;
use crate::processing::{ColumnRef, ColumnSource, SuggestedIndex};
use noria_errors::ReadySetResult;

use super::Side;

/// Specification for how the union operator should operate with respect to rows that exist in both
/// the left and right parents.
#[derive(Clone, Copy, Eq, PartialEq, Debug, Serialize, Deserialize)]
pub enum DuplicateMode {
    /// "Bag" (aka multi-set) union mode.
    ///
    /// For each distinct row in either parent this returns a number of duplicates of that row equal
    /// to the maximum number of duplicates of that row per parent.
    ///
    /// For example, a bag union of the bags `{1, 2, 3, 3, 3}` and `{3, 4, 5}` is equal to `{1, 2,
    /// 3, 3, 3, 4, 5}`
    ///
    /// Because it makes the implementation of the algorithm significantly simpler, BagUnion is
    /// currently only supported for Union nodes with exactly two parents
    BagUnion,

    /// "All" union mode - no duplicate removal is done. Each row in each parent is passed through
    /// unchanged
    UnionAll,
}

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

/// State for [`DuplicateMode::BagUnion`]
///
/// Internally, this is a map from the hash of unique rows (so we don't retain the whole row) to the
/// parent with the most duplicates of that row, and the difference between number of copies of that
/// row stored in that parent and the number of copies stored in the other parent
#[derive(Clone, Debug, Serialize, Deserialize, Default)]
struct BagUnionState(HashMap<u64, (Side, usize)>);

impl BagUnionState {
    /// Process a single record through the bag union state, and return whether that record should
    /// be emitted
    fn process(&mut self, from_side: Side, record: &Record) -> bool {
        let row_hash = hash(record.row());
        match self.0.entry(row_hash) {
            hash_map::Entry::Occupied(mut entry) => {
                // If we already have state for this row, just update the size accordingly
                let (row_side, size) = entry.get_mut();
                let res = if *row_side == from_side {
                    if record.is_positive() {
                        *size += 1;
                    } else {
                        *size = size.saturating_sub(1);
                    }
                    true
                } else {
                    // deltas on a *different* side than the current maximum should be *subtracted*
                    // from the count
                    if record.is_positive() {
                        *size = size.saturating_sub(1);
                    } else {
                        *size += 1;
                    }
                    false
                };

                // If we end up with a size of 0, that means the row has been deleted from both
                // sides or has reached the same number of duplicates on both sides - either way, we
                // can remove it from the map
                if *size == 0 {
                    entry.remove();
                }
                res
            }

            hash_map::Entry::Vacant(entry) => {
                // If we don't have any state for this row, either we've never seen it before or
                // we've seen exactly the same number of duplicates of this row on both sides
                if record.is_positive() {
                    entry.insert((from_side, 1));
                    true
                } else {
                    // If the record is negative, then as a result the *other* side has one more
                    // duplicate than us
                    entry.insert((from_side.other_side(), 1));
                    false
                }
            }
        }
    }
}

/// Key type for [`Union::replay_pieces`]. See the documentation on that field for more information
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Arbitrary)]
pub struct BufferedReplayKey {
    tag: Tag,
    key: KeyComparison,
    requesting_shard: usize,
}

impl Ord for BufferedReplayKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.tag
            .cmp(&other.tag)
            .then_with(|| match (&self.key, &other.key) {
                (KeyComparison::Equal(k1), KeyComparison::Equal(k2)) => k1.cmp(k2),
                (KeyComparison::Range((l1, u1)), KeyComparison::Range((l2, u2))) => {
                    cmp_startbound(l1.as_ref(), l2.as_ref())
                        .then_with(|| cmp_endbound(u1.as_ref(), u2.as_ref()))
                }
                (KeyComparison::Equal(k), KeyComparison::Range((l, u))) => {
                    cmp_startbound(Bound::Included(k), l.as_ref())
                        .then_with(|| cmp_endbound(Bound::Included(k), u.as_ref()))
                }
                (KeyComparison::Range((u, l)), KeyComparison::Equal(k)) => {
                    cmp_startbound(l.as_ref(), Bound::Included(k))
                        .then_with(|| cmp_endbound(u.as_ref(), Bound::Included(k)))
                }
            })
            .then_with(|| self.requesting_shard.cmp(&other.requesting_shard))
    }
}

impl PartialOrd for BufferedReplayKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// A union of a set of views.
#[derive(Debug, Serialize, Deserialize)]
pub struct Union {
    emit: Emit,

    /// State for implementing [`DuplicateMode::BagUnion`]. If this is None, then the duplicate mode
    /// is [`DuplicateMode::UnionAll`]
    bag_union_state: Option<BagUnionState>,

    /// This is a map from (Tag, LocalNodeIndex) to ColumnList
    replay_key: HashMap<(Tag, usize), Vec<usize>>,

    /// Buffered upquery responses that are waiting for more replay pieces.
    ///
    /// Stored as a btreemap so that when we iterate, we first get all the replays of one tag, then
    /// all the records of another tag, etc. This lets us avoid looking up info related to the same
    /// tag more than once. By placing the upquery key in the btreemap key, we can also effectively
    /// check the replay pieces for all values of [`BufferedReplayKey::requesting_shard`]  if we do
    /// find a key match for an update.
    replay_pieces: BTreeMap<BufferedReplayKey, ReplayPieces>,

    required: usize,

    full_wait_state: FullWait,

    me: Option<NodeIndex>,
}

impl Clone for Union {
    fn clone(&self) -> Self {
        Union {
            emit: self.emit.clone(),
            bag_union_state: self.bag_union_state.clone(),
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
    ///
    /// Invariants:
    ///
    /// * `emit` argument's values must be ordered lists already. Union does not support
    /// re-arranging columns, only omitting them.
    pub fn new(
        emit: HashMap<NodeIndex, Vec<usize>>,
        duplicate_mode: DuplicateMode,
    ) -> ReadySetResult<Union> {
        invariant!(!emit.is_empty());
        for emit in emit.values() {
            let mut last = &emit[0];
            for i in emit {
                invariant!(
                    i >= last,
                    "union doesn't support column reordering; got emit = {:?}",
                    emit
                );
                last = i;
            }
        }
        let emit: HashMap<_, _> = emit.into_iter().map(|(k, v)| (k.into(), v)).collect();
        let parents = emit.len();
        Ok(Union {
            emit: Emit::Project {
                emit,
                emit_l: BTreeMap::new(),
                cols: HashMap::new(),
                cols_l: BTreeMap::new(),
            },
            bag_union_state: match duplicate_mode {
                DuplicateMode::BagUnion => Some(BagUnionState::default()),
                DuplicateMode::UnionAll => None,
            },
            required: parents,
            replay_key: Default::default(),
            replay_pieces: Default::default(),
            full_wait_state: FullWait::None,
            me: None,
        })
    }

    /// Construct a new union operator meant to de-shard a sharded data-flow subtree.
    pub fn new_deshard(parent: NodeIndex, sharding: Sharding) -> Union {
        let shards = sharding.shards().unwrap();
        Union {
            emit: Emit::AllFrom(parent.into(), sharding),
            bag_union_state: None,
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
        _: &ReplayContext,
        _: &DomainNodes,
        _: &StateMap,
    ) -> ReadySetResult<ProcessingResult> {
        let mut results = match self.emit {
            Emit::AllFrom(..) => rs,
            Emit::Project { ref emit_l, .. } => {
                rs.into_iter()
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
                    .collect()
            }
        };

        if let Some(bus) = &mut self.bag_union_state {
            if let Some(parent) = match self.emit {
                Emit::Project { ref cols_l, .. } => {
                    let first_parent = cols_l.keys().next().ok_or_else(|| {
                        internal_err(
                            "Union node with DuplicateMode::BagUnion must have exactly 2 parents",
                        )
                    })?;
                    if from == *first_parent {
                        Some(Side::Left)
                    } else {
                        Some(Side::Right)
                    }
                }
                _ => None,
            } {
                results.retain(|rec| bus.process(parent, rec));
            }
        }

        Ok(ProcessingResult {
            results,
            ..Default::default()
        })
    }

    #[allow(clippy::unreachable, clippy::unimplemented)]
    fn on_input_raw(
        &mut self,
        ex: &mut dyn Executor,
        from: LocalNodeIndex,
        rs: Records,
        replay: ReplayContext,
        n: &DomainNodes,
        s: &StateMap,
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
                    trace!("union absorbing update for full replay");

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
                    let rs = self.on_input(ex, from, rs, &replay, n, s)?.results;

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
                        // absorb_for_full will only be true if we've matched on this exactly. This
                        // only appears to be here as a hack around the ownership system
                        // regarding rs, so rs doesn't need to be cloned.
                        unreachable!();
                    }
                }

                if self.replay_pieces.is_empty() {
                    // no replay going on, so we're done.
                    return Ok(RawProcessingResult::Regular(
                        self.on_input(ex, from, rs, &replay, n, s)?,
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

                for (
                    &BufferedReplayKey {
                        tag,
                        key: ref replaying_key,
                        ..
                    },
                    ref mut pieces,
                ) in replays
                {
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
                        if !replaying_key.contains(r.indices(k.iter().copied()).unwrap()) {
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
                    self.on_input(ex, from, rs, &replay, n, s)?,
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
                let mut rs = self.on_input(ex, from, rs, &replay, n, s)?.results;
                let exit;
                match self.full_wait_state {
                    FullWait::None => {
                        if self.required == 1 {
                            // no need to ever buffer
                            return Ok(RawProcessingResult::FullReplay(rs, last));
                        }

                        debug!(
                            "union captured start of full replay; has: {}, need: {}",
                            1, self.required
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
                            debug!("union releasing end of full replay");
                            exit =
                                RawProcessingResult::FullReplay(buffered.split_off(0).into(), true);
                        // fall through to below match where we'll set FullWait::None
                        } else {
                            if started.len() != self.required {
                                if started.insert(from) && started.len() == self.required {
                                    // we can release all buffered replays!
                                    debug!("union releasing full replay");
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

                trace!("union got replay piece: {:?} with context {:?}", rs, replay);

                let mut rs_by_key = rs
                    .into_iter()
                    .map(|r| {
                        (
                            Vec1::try_from(r.cloned_indices(key_cols.iter().copied()).unwrap())
                                .unwrap(),
                            r,
                        )
                    })
                    .fold(BTreeMap::new(), |mut m, (key, r)| {
                        m.entry(key).or_insert_with(Records::default).push(r);
                        m
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
                            let rs = rs_by_key
                                .range_mut::<Vec1<DataType>, _>(key)
                                .flat_map(|(_, rs)| mem::take(rs))
                                .collect();

                            // store this replay piece
                            use std::collections::btree_map::Entry;
                            match replay_pieces_tmp.entry(BufferedReplayKey {
                                tag,
                                key: key.clone(),
                                requesting_shard
                            }) {
                                Entry::Occupied(e) => {
                                    if e.get().buffered.contains_key(&from) {
                                        // This is a serious problem if we get two upquery
                                        // responses for the same key for the same downstream
                                        // shards. In this case we should panic with a stacktrace
                                        // to diagnose how this could have possibly happened.
                                        unimplemented!(
                                            // got two upquery responses for the same key for the same
                                            // downstream shard. waaaaaaat?
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
                                error!("!!! need to issue an eviction after replaying key");
                            }
                            released.insert(key.clone());
                            pieces.buffered.into_iter()
                        })
                        .map(|(from, rs)| {
                            Ok(self.on_input(ex, from, rs, &replay, n, s)?
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
            // TODO: the key.clone()s here are really sad
            for (_, e) in self.replay_pieces.range_mut(
                BufferedReplayKey {
                    tag,
                    key: key.clone(),
                    requesting_shard: 0,
                }..=BufferedReplayKey {
                    tag,
                    key: key.clone(),
                    requesting_shard: usize::max_value(),
                },
            ) {
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

    fn suggest_indexes(&self, _: NodeIndex) -> HashMap<NodeIndex, SuggestedIndex> {
        // index nothing (?)
        HashMap::new()
    }

    fn column_source(&self, cols: &[usize]) -> ColumnSource {
        match self.emit {
            Emit::AllFrom(p, _) => {
                ColumnSource::exact_copy(p.as_global(), cols.try_into().unwrap())
            }
            Emit::Project { ref emit, .. } => ColumnSource::Union(
                emit.iter()
                    .map(|(src, emit)| ColumnRef {
                        node: src.as_global(),
                        columns: cols
                            .iter()
                            .map(|&idx| emit[idx])
                            .collect::<Vec<_>>()
                            .try_into()
                            .unwrap(),
                    })
                    .collect::<Vec<_>>()
                    .try_into()
                    .unwrap(),
            ),
        }
    }

    fn description(&self, detailed: bool) -> String {
        // Ensure we get a consistent output by sorting.
        match self.emit {
            Emit::AllFrom(..) => "⊍".to_string(),
            Emit::Project { .. } if !detailed => String::from("⋃"),
            Emit::Project { ref emit, .. } => {
                let symbol = if self.bag_union_state.is_none() {
                    '⋃' // DuplicateMode::UnionAll
                } else {
                    '⊎' // DuplicateMode::BagUnion
                };
                if detailed {
                    emit.iter()
                        .sorted()
                        .map(|(src, emit)| {
                            let cols = emit
                                .iter()
                                .map(ToString::to_string)
                                .collect::<Vec<_>>()
                                .join(", ");
                            format!("{}:[{}]", src.as_global().index(), cols)
                        })
                        .join(&format!(" {} ", symbol))
                } else {
                    String::from(symbol)
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::ops;

    fn setup(duplicate_mode: DuplicateMode) -> (ops::test::MockGraph, IndexPair, IndexPair) {
        let mut g = ops::test::MockGraph::new();
        let l = g.add_base("left", &["l0", "l1"]);
        let r = g.add_base("right", &["r0", "r1", "r2"]);

        let mut emits = HashMap::new();
        emits.insert(l.as_global(), vec![0, 1]);
        emits.insert(r.as_global(), vec![0, 2]);
        g.set_op(
            "union",
            &["u0", "u1"],
            Union::new(emits, duplicate_mode).unwrap(),
            false,
        );
        (g, l, r)
    }

    #[test]
    fn it_describes() {
        let (u, l, r) = setup(DuplicateMode::UnionAll);
        assert_eq!(
            u.node().description(true),
            format!("{}:[0, 1] ⋃ {}:[0, 2]", l, r)
        );
    }

    #[test]
    fn it_works() {
        let (mut u, l, r) = setup(DuplicateMode::UnionAll);

        // forward from left should emit original record
        let left = vec![1.into(), "a".try_into().unwrap()];
        assert_eq!(u.one_row(l, left.clone(), false), vec![left].into());

        // forward from right should emit subset record
        let right = vec![
            1.into(),
            "skipped".try_into().unwrap(),
            "x".try_into().unwrap(),
        ];
        assert_eq!(
            u.one_row(r, right, false),
            vec![vec![1.into(), "x".try_into().unwrap()]].into()
        );
    }

    #[test]
    fn it_suggests_indices() {
        use std::collections::HashMap;
        let (u, _, _) = setup(DuplicateMode::UnionAll);
        let me = 1.into();
        assert_eq!(u.node().suggest_indexes(me), HashMap::new());
    }

    #[test]
    fn it_resolves() {
        let (u, l, r) = setup(DuplicateMode::UnionAll);
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

    mod buffered_replay_key {
        use super::*;

        use test_strategy::proptest;

        #[proptest]
        fn ord_dual(a: BufferedReplayKey, b: BufferedReplayKey) {
            if a < b {
                assert!(b > a);
            }
            if b < a {
                assert!(a > b);
            }
        }

        #[proptest]
        fn le_transitive(a: BufferedReplayKey, b: BufferedReplayKey, c: BufferedReplayKey) {
            if a < b && b < c {
                assert!(a < c);
            }
        }

        #[proptest]
        fn gt_transitive(a: BufferedReplayKey, b: BufferedReplayKey, c: BufferedReplayKey) {
            if a > b && b > c {
                assert!(a > c);
            }
        }

        #[proptest]
        fn ord_trichotomy(a: BufferedReplayKey, b: BufferedReplayKey) {
            let less = a < b;
            let greater = a > b;
            let eq = a == b;

            if less {
                assert!(!greater);
                assert!(!eq);
            }

            if greater {
                assert!(!less);
                assert!(!eq);
            }

            if eq {
                assert!(!less);
                assert!(!greater);
            }
        }
    }

    mod replay_capturing {
        use super::*;
        use maplit::hashset;
        use vec1::vec1;

        #[test]
        fn left_then_right() {
            let (mut g, left, right) = setup(DuplicateMode::UnionAll);
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
                vec![vec![DataType::from(1), DataType::try_from("a").unwrap()]],
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
                    DataType::try_from("skipped").unwrap(),
                    DataType::try_from("b").unwrap(),
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
                    assert!(
                        rows.contains(&Record::Positive(vec![1.into(), "a".try_into().unwrap()]))
                    );
                    assert!(
                        rows.contains(&Record::Positive(vec![1.into(), "b".try_into().unwrap()]))
                    );

                    assert!(captured.is_empty());
                    assert_eq!(keys, hashset![key]);
                }
                _ => unreachable!("Expected replay piece, got: {:?}", res),
            }
        }

        #[test]
        fn left_then_update_then_right() {
            let (mut g, left, right) = setup(DuplicateMode::UnionAll);
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
                    vec![DataType::from(1), DataType::try_from("a").unwrap()],
                    vec![DataType::from(1), DataType::try_from("c").unwrap()],
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
                    (
                        vec![DataType::from(1), DataType::try_from("d").unwrap()],
                        true,
                    ),
                    (
                        vec![DataType::from(1), DataType::try_from("c").unwrap()],
                        false,
                    ),
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
                    DataType::try_from("skipped").unwrap(),
                    DataType::try_from("b").unwrap(),
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
                    assert!(
                        rows.contains(&Record::Positive(vec![1.into(), "a".try_into().unwrap()]))
                    );
                    assert!(
                        rows.contains(&Record::Positive(vec![1.into(), "b".try_into().unwrap()]))
                    );
                    // assert!(!rows.contains(&Record::Positive(vec![1.into(), "c".try_into().unwrap()])));
                    assert!(
                        rows.contains(&Record::Positive(vec![1.into(), "d".try_into().unwrap()]))
                    );

                    assert!(captured.is_empty());
                    assert_eq!(keys, hashset![key]);
                }
                _ => unreachable!("Expected replay piece, got: {:?}", res),
            }
        }
    }

    /// An oracle implementation of the "bag union" algorithm that unions implement for
    /// [`DuplicateMode::BagUnion`], and a property test showing its theoretical correctness
    mod bag_union {
        use super::*;
        use std::cmp::max;
        use test_strategy::{proptest, Arbitrary};

        #[derive(Debug, Arbitrary, PartialEq, Eq, Clone, Copy)]
        enum Input {
            PosLeft,
            NegLeft,
            PosRight,
            NegRight,
        }

        #[proptest]
        fn bag_union_state_returns_correct_state_size(inputs: Vec<Input>) {
            let (total_lefts, total_rights) =
                inputs
                    .iter()
                    .fold((0_isize, 0_isize), |(l, r), input| match input {
                        Input::PosLeft => (l + 1, r),
                        Input::NegLeft => (l - 1, r),
                        Input::PosRight => (l, r + 1),
                        Input::NegRight => (l, r - 1),
                    });
            let expected = max(total_lefts, total_rights);
            let row = vec![DataType::from(1i32)];
            let mut bag_union_state = BagUnionState::default();

            let state_size = inputs
                .into_iter()
                .map(|input| match input {
                    Input::PosLeft => (Side::Left, Record::Positive(row.clone())),
                    Input::NegLeft => (Side::Left, Record::Negative(row.clone())),
                    Input::PosRight => (Side::Right, Record::Positive(row.clone())),
                    Input::NegRight => (Side::Right, Record::Negative(row.clone())),
                })
                .filter(|(side, rec)| bag_union_state.process(*side, rec))
                .map(|(_, rec)| rec)
                .fold(
                    0_isize,
                    |acc, rec| {
                        if rec.is_positive() {
                            acc + 1
                        } else {
                            acc - 1
                        }
                    },
                );
            assert_eq!(state_size, expected)
        }

        #[test]
        fn bag_union_ingredient_returns_correct_results() {
            let (mut u, l, r) = setup(DuplicateMode::BagUnion);

            let left_row = vec![1.into(), "a".try_into().unwrap()];
            let right_row = vec![
                1.into(),
                "skipped".try_into().unwrap(),
                "a".try_into().unwrap(),
            ];
            assert_eq!(
                u.one_row(l, left_row.clone(), false),
                vec![left_row.clone()].into()
            );
            assert_eq!(
                u.one_row(l, left_row.clone(), false),
                vec![left_row.clone()].into()
            );
            assert_eq!(u.one_row(r, right_row.clone(), false), Default::default());
            assert_eq!(u.one_row(r, right_row.clone(), false), Default::default());
            assert_eq!(u.one_row(r, right_row, false), vec![left_row].into());
        }
    }
}
