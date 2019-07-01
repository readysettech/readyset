use petgraph::graph::NodeIndex;
use std::borrow::Cow;
use std::cell;
use std::cmp;
use std::collections::{HashMap, HashSet, VecDeque};
use std::mem;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time;

use futures;
use group_commit::GroupCommitQueueSet;
use noria::channel::{self, TcpSender};
pub use noria::internal::DomainIndex as Index;
use payload::{ControlReplyPacket, ReplayPieceContext};
use prelude::*;
use slog::Logger;
use stream_cancel::Valve;

use timekeeper::{RealTime, SimpleTracker, ThreadTime, Timer, TimerSet};
use tokio::{self, prelude::*};
use Readers;

#[derive(Debug)]
pub enum PollEvent {
    ResumePolling,
    Process(Box<Packet>),
    Timeout,
}

#[derive(Debug)]
pub enum ProcessResult {
    KeepPolling(Option<time::Duration>),
    Processed,
    StopPolling,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Config {
    pub concurrent_replays: usize,
    pub replay_batch_timeout: time::Duration,
}

const BATCH_SIZE: usize = 256;

#[derive(Debug)]
enum DomainMode {
    Forwarding,
    Replaying {
        to: LocalNodeIndex,
        buffered: VecDeque<Box<Packet>>,
        passes: usize,
    },
}

impl PartialEq for DomainMode {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (&DomainMode::Forwarding, &DomainMode::Forwarding) => true,
            _ => false,
        }
    }
}

enum TriggerEndpoint {
    None,
    Start(Vec<usize>),
    End {
        ask_all: bool,
        options: Vec<Box<dyn channel::Sender<Item = Box<Packet>> + Send>>,
    },
    Local(Vec<usize>),
}

struct ReplayPath {
    source: Option<LocalNodeIndex>,
    path: Vec<ReplayPathSegment>,
    notify_done: bool,
    trigger: TriggerEndpoint,
}

type Hole = (Vec<usize>, Vec<DataType>);
type Redo = (Tag, Vec<DataType>);
/// When a replay misses while being processed, it triggers a replay to backfill the hole that it
/// missed in. We need to ensure that when this happens, we re-run the original replay to fill the
/// hole we *originally* were trying to fill.
///
/// This comes with some complexity:
///
///  - If two replays both hit the *same* hole, we should only request a backfill of it once, but
///    need to re-run *both* replays when the hole is filled.
///  - If one replay hits two *different* holes, we should backfill both holes, but we must ensure
///    that we only re-run the replay once when both holes have been filled.
///
/// To keep track of this, we use the `Waiting` structure below. One is created for every node with
/// at least one outstanding backfill, and contains the necessary bookkeeping to ensure the two
/// behaviors outlined above.
///
/// Note that in the type aliases above, we have chosen to use Vec<usize> instead of Tag to
/// identify a hole. This is because there may be more than one Tag used to fill a given hole, and
/// the set of columns uniquely identifies the set of tags.
#[derive(Debug, Default)]
struct Waiting {
    /// For each eventual redo, how many holes are we waiting for?
    holes: HashMap<Redo, usize>,
    /// For each hole, which redos do we expect we'll have to do?
    redos: HashMap<Hole, HashSet<Redo>>,
}

/// Struct sent to a worker to start a domain.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DomainBuilder {
    /// The domain's index.
    pub index: Index,
    /// The shard ID represented by this `DomainBuilder`.
    pub shard: Option<usize>,
    /// The number of shards in the domain.
    pub nshards: usize,
    /// The nodes in the domain.
    pub nodes: DomainNodes,
    /// The domain's persistence setting.
    pub persistence_parameters: PersistenceParameters,
    /// Configuration parameters for the domain.
    pub config: Config,
}

unsafe impl Send for DomainBuilder {}

impl DomainBuilder {
    /// Starts up the domain represented by this `DomainBuilder`.
    pub fn build(
        self,
        log: Logger,
        readers: Readers,
        channel_coordinator: Arc<ChannelCoordinator>,
        control_addr: SocketAddr,
        shutdown_valve: &Valve,
        state_size: Arc<AtomicUsize>,
    ) -> Domain {
        // initially, all nodes are not ready
        let not_ready = self
            .nodes
            .values()
            .map(|n| n.borrow().local_addr())
            .collect();

        let log = log.new(o!("domain" => self.index.index(), "shard" => self.shard.unwrap_or(0)));
        let control_reply_tx = TcpSender::connect(&control_addr).unwrap();
        let group_commit_queues = GroupCommitQueueSet::new(&self.persistence_parameters);

        Domain {
            index: self.index,
            shard: self.shard,
            _nshards: self.nshards,

            persistence_parameters: self.persistence_parameters,
            nodes: self.nodes,
            state: StateMap::default(),
            log,
            not_ready,
            mode: DomainMode::Forwarding,
            waiting: Default::default(),
            reader_triggered: Default::default(),
            replay_paths: Default::default(),
            replay_paths_by_dst: Default::default(),

            ingress_inject: Default::default(),

            shutdown_valve: shutdown_valve.clone(),
            readers,
            control_reply_tx,
            channel_coordinator,

            buffered_replay_requests: Default::default(),
            replay_batch_timeout: self.config.replay_batch_timeout,
            timed_purges: Default::default(),

            concurrent_replays: 0,
            max_concurrent_replays: self.config.concurrent_replays,
            replay_request_queue: Default::default(),
            delayed_for_self: Default::default(),

            group_commit_queues,

            state_size,
            total_time: Timer::new(),
            total_ptime: Timer::new(),
            wait_time: Timer::new(),
            process_times: TimerSet::new(),
            process_ptimes: TimerSet::new(),

            total_replay_time: Timer::new(),
            total_forward_time: Timer::new(),
        }
    }
}

#[derive(Clone, Debug)]
struct TimedPurge {
    time: time::Instant,
    view: LocalNodeIndex,
    tag: Tag,
    keys: HashSet<Vec<DataType>>,
}

pub struct Domain {
    index: Index,
    shard: Option<usize>,
    _nshards: usize,

    nodes: DomainNodes,
    state: StateMap,
    log: Logger,

    not_ready: HashSet<LocalNodeIndex>,

    ingress_inject: Map<(usize, Vec<DataType>)>,

    persistence_parameters: PersistenceParameters,

    mode: DomainMode,
    waiting: Map<Waiting>,
    replay_paths: HashMap<Tag, ReplayPath>,
    reader_triggered: Map<HashSet<Vec<DataType>>>,
    timed_purges: VecDeque<TimedPurge>,

    replay_paths_by_dst: Map<HashMap<Vec<usize>, Vec<Tag>>>,

    concurrent_replays: usize,
    max_concurrent_replays: usize,
    replay_request_queue: VecDeque<(Tag, Vec<DataType>)>,

    shutdown_valve: Valve,
    readers: Readers,
    control_reply_tx: TcpSender<ControlReplyPacket>,
    channel_coordinator: Arc<ChannelCoordinator>,

    buffered_replay_requests: HashMap<Tag, (time::Instant, HashSet<Vec<DataType>>)>,
    replay_batch_timeout: time::Duration,
    delayed_for_self: VecDeque<Box<Packet>>,

    group_commit_queues: GroupCommitQueueSet,

    state_size: Arc<AtomicUsize>,
    total_time: Timer<SimpleTracker, RealTime>,
    total_ptime: Timer<SimpleTracker, ThreadTime>,
    wait_time: Timer<SimpleTracker, RealTime>,
    process_times: TimerSet<LocalNodeIndex, SimpleTracker, RealTime>,
    process_ptimes: TimerSet<LocalNodeIndex, SimpleTracker, ThreadTime>,

    /// time spent processing replays
    total_replay_time: Timer<SimpleTracker, RealTime>,
    /// time spent processing ordinary, forward updates
    total_forward_time: Timer<SimpleTracker, RealTime>,
}

impl Domain {
    fn find_tags_and_replay(
        &mut self,
        miss_key: Vec<DataType>,
        miss_columns: &[usize],
        miss_in: LocalNodeIndex,
    ) {
        let mut tags = Vec::new();
        if let Some(ref candidates) = self.replay_paths_by_dst.get(miss_in) {
            if let Some(ts) = candidates.get(miss_columns) {
                // the clone is a bit sad; self.request_partial_replay doesn't use
                // self.replay_paths_by_dst.
                tags = ts.clone();
            }
        }

        for &tag in &tags {
            // send a message to the source domain(s) responsible
            // for the chosen tag so they'll start replay.
            let key = miss_key.clone(); // :(
            if let TriggerEndpoint::Local(..) = self.replay_paths[&tag].trigger {
                // *in theory* we could just call self.seed_replay, and everything would be good.
                // however, then we start recursing, which could get us into sad situations where
                // we break invariants where some piece of code is assuming that it is the only
                // thing processing at the time (think, e.g., borrow_mut()).
                //
                // for example, consider the case where two misses occurred on the same key.
                // normally, those requests would be deduplicated so that we don't get two replay
                // responses for the same key later. however, the way we do that is by tracking
                // keys we have requested in self.waiting.redos (see `redundant` in
                // `on_replay_miss`). in particular, on_replay_miss is called while looping over
                // all the misses that need replays, and while the first miss of a given key will
                // trigger a replay, the second will not. if we call `seed_replay` directly here,
                // that might immediately fill in this key and remove the entry. when the next miss
                // (for the same key) is then hit in the outer iteration, it will *also* request a
                // replay of that same key, which gets us into trouble with `State::mark_filled`.
                //
                // so instead, we simply keep track of the fact that we have a replay to handle,
                // and then get back to it after all processing has finished (at the bottom of
                // `Self::handle()`)
                self.delayed_for_self
                    .push_back(box Packet::RequestPartialReplay { tag, key });
                continue;
            }

            // NOTE: due to max_concurrent_replays, it may be that we only replay from *some* of
            // these ancestors now, and some later. this will cause more of the replay to be
            // buffered up at the union above us, but that's probably fine.
            self.request_partial_replay(tag, key);
        }

        if tags.is_empty() {
            unreachable!(format!(
                "no tag found to fill missing value {:?} in {}.{:?}",
                miss_key, miss_in, miss_columns
            ));
        }
    }

    fn on_replay_miss(
        &mut self,
        miss_in: LocalNodeIndex,
        miss_columns: &[usize],
        replay_key: Vec<DataType>,
        miss_key: Vec<DataType>,
        needed_for: Tag,
    ) {
        use std::collections::hash_map::Entry;
        use std::ops::AddAssign;

        // when the replay eventually succeeds, we want to re-do the replay.
        let mut w = self.waiting.remove(miss_in).unwrap_or_default();

        let mut redundant = false;
        let redo = (needed_for, replay_key.clone());
        match w.redos.entry((Vec::from(miss_columns), miss_key.clone())) {
            Entry::Occupied(e) => {
                // we have already requested backfill of this key
                // remember to notify this Redo when backfill completes
                if e.into_mut().insert(redo.clone()) {
                    // this Redo should wait for this backfill to complete before redoing
                    w.holes.entry(redo).or_default().add_assign(1);
                }
                redundant = true;
            }
            Entry::Vacant(e) => {
                // we haven't already requested backfill of this key
                let mut redos = HashSet::new();
                // remember to notify this Redo when backfill completes
                redos.insert(redo.clone());
                e.insert(redos);
                // this Redo should wait for this backfill to complete before redoing
                w.holes.entry(redo).or_default().add_assign(1);
            }
        }

        self.waiting.insert(miss_in, w);
        if redundant {
            return;
        }

        self.find_tags_and_replay(miss_key, miss_columns, miss_in);
    }

    fn send_partial_replay_request(&mut self, tag: Tag, key: Vec<DataType>) {
        debug_assert!(self.concurrent_replays < self.max_concurrent_replays);
        if let TriggerEndpoint::End {
            ask_all,
            ref mut options,
        } = self.replay_paths.get_mut(&tag).unwrap().trigger
        {
            if ask_all && options.len() != 1 {
                // source is sharded by a different key than we are doing lookups for,
                // so we need to trigger on all the shards.
                self.concurrent_replays += 1;
                trace!(self.log, "sending shuffled shard replay request";
                "tag" => ?tag,
                "key" => ?key,
                "buffered" => self.replay_request_queue.len(),
                "concurrent" => self.concurrent_replays,
                );

                for trigger in options {
                    if trigger
                        .send(box Packet::RequestPartialReplay {
                            tag,
                            key: key.clone(), // sad to clone here
                        })
                        .is_err()
                    {
                        // we're shutting down -- it's fine.
                    }
                }
                return;
            }

            let shard = if options.len() == 1 {
                0
            } else {
                assert_eq!(key.len(), 1);
                ::shard_by(&key[0], options.len())
            };
            self.concurrent_replays += 1;
            trace!(self.log, "sending replay request";
            "tag" => ?tag,
            "key" => ?key,
            "buffered" => self.replay_request_queue.len(),
            "concurrent" => self.concurrent_replays,
            );
            if options[shard]
                .send(box Packet::RequestPartialReplay { tag, key })
                .is_err()
            {
                // we're shutting down -- it's fine.
            }
        } else {
            unreachable!("asked to replay along non-existing path")
        }
    }

    fn request_partial_replay(&mut self, tag: Tag, key: Vec<DataType>) {
        if self.concurrent_replays < self.max_concurrent_replays {
            assert_eq!(self.replay_request_queue.len(), 0);
            self.send_partial_replay_request(tag, key);
        } else {
            trace!(self.log, "buffering replay request";
            "tag" => ?tag,
            "key" => ?key,
            "buffered" => self.replay_request_queue.len(),
            );
            self.replay_request_queue.push_back((tag, key));
        }
    }

    fn finished_partial_replay(&mut self, tag: Tag, num: usize) {
        match self.replay_paths[&tag].trigger {
            TriggerEndpoint::End { .. } => {
                // A backfill request we made to another domain was just satisfied!
                // We can now issue another request from the concurrent replay queue.
                // However, since unions require multiple backfill requests, but produce only one
                // backfill reply, we need to check how many requests we're now free to issue. If
                // we just naively release one slot here, a union with two parents would mean that
                // `self.concurrent_replays` constantly grows by +1 (+2 for the backfill requests,
                // -1 when satisfied), which would lead to a deadlock!
                let mut requests_satisfied = 0;
                let last = self.replay_paths[&tag].path.last().unwrap();
                if let Some(ref cs) = self.replay_paths_by_dst.get(last.node) {
                    if let Some(ref tags) = cs.get(last.partial_key.as_ref().unwrap()) {
                        requests_satisfied = tags
                            .iter()
                            .filter(|tag| {
                                if let TriggerEndpoint::End { .. } = self.replay_paths[tag].trigger
                                {
                                    true
                                } else {
                                    false
                                }
                            })
                            .count();
                    }
                }

                // we also sent that many requests *per key*.
                requests_satisfied *= num;

                // TODO: figure out why this can underflow
                self.concurrent_replays =
                    self.concurrent_replays.saturating_sub(requests_satisfied);
                trace!(self.log, "notified of finished replay";
                "#done" => requests_satisfied,
                "ongoing" => self.concurrent_replays,
                );
                debug_assert!(self.concurrent_replays < self.max_concurrent_replays);
                while self.concurrent_replays < self.max_concurrent_replays {
                    if let Some((tag, key)) = self.replay_request_queue.pop_front() {
                        trace!(self.log, "releasing replay request";
                        "tag" => ?tag,
                        "key" => ?key,
                        "left" => self.replay_request_queue.len(),
                        "ongoing" => self.concurrent_replays,
                        );
                        self.send_partial_replay_request(tag, key);
                    } else {
                        return;
                    }
                }
            }
            TriggerEndpoint::Local(..) => {
                // didn't count against our quote, so we're also not decementing
            }
            TriggerEndpoint::Start(..) | TriggerEndpoint::None => {
                unreachable!();
            }
        }
    }

    fn dispatch(&mut self, m: Box<Packet>, sends: &mut EnqueuedSends, executor: &mut dyn Executor) {
        let src = m.src();
        let me = m.dst();

        match self.mode {
            DomainMode::Forwarding => (),
            DomainMode::Replaying {
                ref to,
                ref mut buffered,
                ..
            } if to == &me => {
                buffered.push_back(m);
                return;
            }
            DomainMode::Replaying { .. } => (),
        }

        if !self.not_ready.is_empty() && self.not_ready.contains(&me) {
            return;
        }

        let (mut m, evictions) = {
            let mut n = self.nodes[me].borrow_mut();
            self.process_times.start(me);
            self.process_ptimes.start(me);
            let mut m = Some(m);
            let (misses, _, captured) = n.process(
                &mut m,
                None,
                &mut self.state,
                &self.nodes,
                self.shard,
                true,
                sends,
                executor,
            );
            assert_eq!(captured.len(), 0);
            self.process_ptimes.stop();
            self.process_times.stop();

            if m.is_none() {
                // no need to deal with our children if we're not sending them anything
                return;
            }

            // normally, we ignore misses during regular forwarding.
            // however, we have to be a little careful in the case of joins.
            let evictions = if n.is_internal() && n.is_join() && !misses.is_empty() {
                // there are two possible cases here:
                //
                //  - this is a write that will hit a hole in every downstream materialization.
                //    dropping it is totally safe!
                //  - this is a write that will update an entry in some downstream materialization.
                //    this is *not* allowed! we *must* ensure that downstream remains up to date.
                //    but how can we? we missed in the other side of the join, so we can't produce
                //    the necessary output record... what we *can* do though is evict from any
                //    downstream, and then we guarantee that we're in case 1!
                //
                // if you're curious about how we may have ended up in case 2 above, here are two
                // ways:
                //
                //  - some downstream view is partial over the join key. some time in the past, it
                //    requested a replay of key k. that replay produced *no* rows from the side
                //    that was replayed. this in turn means that no lookup was performed on the
                //    other side of the join, and so k wasn't replayed to that other side (which
                //    then still has a hole!). in that case, any subsequent write with k in the
                //    join column from the replay side will miss in the other side.
                //  - some downstream view is partial over a column that is *not* the join key. in
                //    the past, it replayed some key k, which means that we aren't allowed to drop
                //    any write with k in that column. now, a write comes along with k in that
                //    replay column, but with some hitherto unseen key z in the join column. if the
                //    replay of k never caused a lookup of z in the other side of the join, then
                //    the other side will have a hole. thus, we end up in the situation where we
                //    need to forward a write through the join, but we miss.
                //
                // unfortunately, we can't easily distinguish between the case where we have to
                // evict and the case where we don't (at least not currently), so we *always* need
                // to evict when this happens. this shouldn't normally be *too* bad, because writes
                // are likely to be dropped before they even reach the join in most benign cases
                // (e.g., in an ingress). this can be remedied somewhat in the future by ensuring
                // that the first of the two causes outlined above can't happen (by always doing a
                // lookup on the replay key, even if there are now rows). then we know that the
                // *only* case where we have to evict is when the replay key != the join key.
                //
                // but, for now, here we go:
                // first, what partial replay paths go through this node?
                let from = self.nodes[src].borrow().global_addr();
                // TODO: this is a linear walk of replay paths -- we should make that not linear
                let deps: Vec<_> = self
                    .replay_paths
                    .iter()
                    .filter_map(|(&tag, rp)| {
                        rp.path
                            .iter()
                            .find(|rps| rps.node == me)
                            .and_then(|rps| rps.partial_key.as_ref())
                            .and_then(|keys| {
                                // we need to find the *input* column that produces that output.
                                //
                                // if one of the columns for this replay path's keys does not
                                // resolve into the ancestor we got the update from, we don't need
                                // to issue an eviction for that path. this is because we *missed*
                                // on the join column in the other side, so we *know* it can't have
                                // forwarded anything related to the write we're now handling.
                                keys.iter()
                                    .map(|&k| {
                                        n.parent_columns(k)
                                            .into_iter()
                                            .find(|&(ni, _)| ni == from)
                                            .ok_or(())
                                            .map(|k| k.1.unwrap())
                                    })
                                    .collect::<Result<Vec<_>, _>>()
                                    .ok()
                            })
                            .map(move |k| (tag, k))
                    })
                    .collect();

                let mut evictions = HashMap::new();
                for miss in misses {
                    for &(tag, ref keys) in &deps {
                        evictions
                            .entry(tag)
                            .or_insert_with(HashSet::new)
                            .insert(keys.iter().map(|&key| miss.record[key].clone()).collect());
                    }
                }

                Some(evictions)
            } else {
                None
            };

            (m, evictions)
        };

        if let Some(evictions) = evictions {
            // now send evictions for all the (tag, [key]) things in evictions
            for (tag, keys) in evictions {
                self.handle_eviction(
                    Box::new(Packet::EvictKeys {
                        keys: keys.into_iter().collect(),
                        link: Link::new(src, me),
                        tag,
                    }),
                    sends,
                );
            }
        }

        match m.as_ref().unwrap() {
            m @ &box Packet::Message { .. } if m.is_empty() => {
                // no need to deal with our children if we're not sending them anything
                return;
            }
            &box Packet::Message { .. } => {}
            &box Packet::ReplayPiece { .. } => {
                unreachable!("replay should never go through dispatch");
            }
            ref m => unreachable!("dispatch process got {:?}", m),
        }

        // NOTE: we can't directly iterate over .children due to self.dispatch in the loop
        let nchildren = self.nodes[me].borrow().children().len();
        for i in 0..nchildren {
            // avoid cloning if we can
            let mut m = if i == nchildren - 1 {
                m.take().unwrap()
            } else {
                m.as_ref().map(|m| box m.clone_data()).unwrap()
            };

            let childi = self.nodes[me].borrow().children()[i];
            let child_is_merger = {
                // XXX: shouldn't NLL make this unnecessary?
                let c = self.nodes[childi].borrow();
                c.is_shard_merger()
            };

            if child_is_merger {
                // we need to preserve the egress src (which includes shard identifier)
            } else {
                m.link_mut().src = me;
            }
            m.link_mut().dst = childi;

            self.dispatch(m, sends, executor);
        }
    }

    #[allow(clippy::cognitive_complexity)]
    fn handle(
        &mut self,
        m: Box<Packet>,
        sends: &mut EnqueuedSends,
        executor: &mut dyn Executor,
        top: bool,
    ) {
        self.wait_time.stop();
        m.trace(PacketEvent::Handle);

        match *m {
            Packet::Message { .. } | Packet::Input { .. } => {
                // WO for https://github.com/rust-lang/rfcs/issues/1403
                self.total_forward_time.start();
                self.dispatch(m, sends, executor);
                self.total_forward_time.stop();
            }
            Packet::ReplayPiece { .. } => {
                self.total_replay_time.start();
                self.handle_replay(m, sends, executor);
                self.total_replay_time.stop();
            }
            Packet::Evict { .. } | Packet::EvictKeys { .. } => {
                self.handle_eviction(m, sends);
            }
            consumed => {
                match consumed {
                    // workaround #16223
                    Packet::AddNode { node, parents } => {
                        let addr = node.local_addr();
                        self.not_ready.insert(addr);

                        for p in parents {
                            self.nodes
                                .get_mut(p)
                                .unwrap()
                                .borrow_mut()
                                .add_child(node.local_addr());
                        }
                        self.nodes.insert(addr, cell::RefCell::new(node));
                        trace!(self.log, "new node incorporated"; "local" => addr.id());
                    }
                    Packet::RemoveNodes { nodes } => {
                        for &node in &nodes {
                            self.nodes[node].borrow_mut().remove();
                            self.state.remove(node);
                            trace!(self.log, "node removed"; "local" => node.id());
                        }

                        for node in nodes {
                            for cn in self.nodes.iter_mut() {
                                cn.1.borrow_mut().try_remove_child(node);
                                // NOTE: since nodes are always removed leaves-first, it's not
                                // important to update parent pointers here
                            }
                        }
                    }
                    Packet::AddBaseColumn {
                        node,
                        field,
                        default,
                    } => {
                        let mut n = self.nodes[node].borrow_mut();
                        n.add_column(&field);
                        if let Some(b) = n.get_base_mut() {
                            b.add_column(default);
                        } else if n.is_ingress() {
                            self.ingress_inject
                                .entry(node)
                                .or_insert_with(|| (n.fields().len(), Vec::new()))
                                .1
                                .push(default);
                        } else {
                            unreachable!("node unrelated to base got AddBaseColumn");
                        }
                        self.control_reply_tx
                            .send(ControlReplyPacket::ack())
                            .unwrap();
                    }
                    Packet::DropBaseColumn { node, column } => {
                        let mut n = self.nodes[node].borrow_mut();
                        n.get_base_mut()
                            .expect("told to drop base column from non-base node")
                            .drop_column(column);
                        self.control_reply_tx
                            .send(ControlReplyPacket::ack())
                            .unwrap();
                    }
                    Packet::UpdateEgress {
                        node,
                        new_tx,
                        new_tag,
                    } => {
                        let mut n = self.nodes[node].borrow_mut();
                        n.with_egress_mut(move |e| {
                            if let Some((node, local, addr)) = new_tx {
                                e.add_tx(node, local, addr);
                            }
                            if let Some(new_tag) = new_tag {
                                e.add_tag(new_tag.0, new_tag.1);
                            }
                        });
                    }
                    Packet::UpdateSharder { node, new_txs } => {
                        let mut n = self.nodes[node].borrow_mut();
                        n.with_sharder_mut(move |s| {
                            s.add_sharded_child(new_txs.0, new_txs.1);
                        });
                    }
                    Packet::AddStreamer { node, new_streamer } => {
                        let mut n = self.nodes[node].borrow_mut();
                        n.with_reader_mut(|r| r.add_streamer(new_streamer).unwrap())
                            .unwrap();
                    }
                    Packet::StateSizeProbe { node } => {
                        let row_count = self.state.get(node).map(|r| r.rows()).unwrap_or(0);
                        let mem_size = self.state.get(node).map(|s| s.deep_size_of()).unwrap_or(0);
                        self.control_reply_tx
                            .send(ControlReplyPacket::StateSize(row_count, mem_size))
                            .unwrap();
                    }
                    Packet::PrepareState { node, state } => {
                        use payload::InitialState;
                        match state {
                            InitialState::PartialLocal(index) => {
                                if !self.state.contains_key(node) {
                                    self.state.insert(node, box MemoryState::default());
                                }
                                let state = self.state.get_mut(node).unwrap();
                                for (key, tags) in index {
                                    info!(self.log, "told to prepare partial state";
                                           "key" => ?key,
                                           "tags" => ?tags);
                                    state.add_key(&key[..], Some(tags));
                                }
                            }
                            InitialState::IndexedLocal(index) => {
                                if !self.state.contains_key(node) {
                                    self.state.insert(node, box MemoryState::default());
                                }
                                let state = self.state.get_mut(node).unwrap();
                                for idx in index {
                                    info!(self.log, "told to prepare full state";
                                           "key" => ?idx);
                                    state.add_key(&idx[..], None);
                                }
                            }
                            InitialState::PartialGlobal {
                                gid,
                                cols,
                                key,
                                trigger_domain: (trigger_domain, shards),
                            } => {
                                use backlog;
                                let k = key.clone(); // ugh
                                let txs = (0..shards)
                                    .map(|shard| {
                                        let key = key.clone();
                                        let (tx, rx) = futures::sync::mpsc::unbounded();
                                        let sender = self
                                            .channel_coordinator
                                            .builder_for(&(trigger_domain, shard))
                                            .unwrap()
                                            .build_async()
                                            .unwrap();

                                        tokio::spawn(
                                            self.shutdown_valve
                                                .wrap(rx)
                                                .map(move |miss| box Packet::RequestReaderReplay {
                                                    key: miss,
                                                    cols: key.clone(),
                                                    node,
                                                })
                                                .fold(sender, move |sender, m| {
                                                    sender.send(m).map_err(|e| {
                                                        // domain went away?
                                                        eprintln!(
                                                            "replay source went away: {:?}",
                                                            e
                                                        );
                                                    })
                                                })
                                                .map(|_| ()),
                                        );
                                        tx
                                    })
                                    .collect::<Vec<_>>();
                                let (r_part, w_part) =
                                    backlog::new_partial(cols, &k[..], move |miss| {
                                        let n = txs.len();
                                        let tx = if n == 1 {
                                            &txs[0]
                                        } else {
                                            // TODO: compound reader
                                            assert_eq!(miss.len(), 1);
                                            &txs[::shard_by(&miss[0], n)]
                                        };
                                        tx.unbounded_send(Vec::from(miss)).is_ok()
                                    });

                                let mut n = self.nodes[node].borrow_mut();
                                n.with_reader_mut(|r| {
                                    assert!(self
                                        .readers
                                        .lock()
                                        .unwrap()
                                        .insert((gid, *self.shard.as_ref().unwrap_or(&0)), r_part)
                                        .is_none());

                                    // make sure Reader is actually prepared to receive state
                                    r.set_write_handle(w_part)
                                })
                                .unwrap();
                            }
                            InitialState::Global { gid, cols, key } => {
                                use backlog;
                                let (r_part, w_part) = backlog::new(cols, &key[..]);

                                let mut n = self.nodes[node].borrow_mut();
                                n.with_reader_mut(|r| {
                                    assert!(self
                                        .readers
                                        .lock()
                                        .unwrap()
                                        .insert((gid, *self.shard.as_ref().unwrap_or(&0)), r_part)
                                        .is_none());

                                    // make sure Reader is actually prepared to receive state
                                    r.set_write_handle(w_part)
                                })
                                .unwrap();
                            }
                        }
                    }
                    Packet::SetupReplayPath {
                        tag,
                        source,
                        path,
                        notify_done,
                        trigger,
                    } => {
                        // let coordinator know that we've registered the tagged path
                        self.control_reply_tx
                            .send(ControlReplyPacket::ack())
                            .unwrap();

                        if notify_done {
                            info!(self.log,
                                  "told about terminating replay path {:?}",
                                  path;
                                  "tag" => tag.id()
                            );
                        // NOTE: we set self.replaying_to when we first receive a replay with
                        // this tag
                        } else {
                            info!(self.log, "told about replay path {:?}", path; "tag" => tag.id());
                        }

                        use payload;
                        let trigger = match trigger {
                            payload::TriggerEndpoint::None => TriggerEndpoint::None,
                            payload::TriggerEndpoint::Start(v) => TriggerEndpoint::Start(v),
                            payload::TriggerEndpoint::Local(v) => TriggerEndpoint::Local(v),
                            payload::TriggerEndpoint::End(selection, domain) => {
                                let shard = |shardi| {
                                    // TODO: make async
                                    self.channel_coordinator
                                        .builder_for(&(domain, shardi))
                                        .unwrap()
                                        .build_sync()
                                        .unwrap()
                                };

                                let (ask_all, options) = match selection {
                                    payload::SourceSelection::AllShards(nshards) => {
                                        (true, (0..nshards).map(shard).collect())
                                    }
                                    payload::SourceSelection::SameShard => (
                                        true,
                                        vec![shard(self.shard.expect(
                                            "told to replay from same shard, but not sharded",
                                        ))],
                                    ),
                                    payload::SourceSelection::KeyShard(nshards) => {
                                        (false, (0..nshards).map(shard).collect())
                                    }
                                };

                                TriggerEndpoint::End { ask_all, options }
                            }
                        };

                        if let TriggerEndpoint::End { .. } | TriggerEndpoint::Local(..) = trigger {
                            let last = path.last().unwrap();
                            self.replay_paths_by_dst
                                .entry(last.node)
                                .or_insert_with(HashMap::new)
                                .entry(last.partial_key.clone().unwrap())
                                .or_insert_with(Vec::new)
                                .push(tag);
                        }

                        self.replay_paths.insert(
                            tag,
                            ReplayPath {
                                source,
                                path,
                                notify_done,
                                trigger,
                            },
                        );
                    }
                    Packet::RequestReaderReplay { key, cols, node } => {
                        self.total_replay_time.start();
                        // the reader could have raced with us filling in the key after some
                        // *other* reader requested it, so let's double check that it indeed still
                        // misses!
                        let still_miss = self.nodes[node]
                            .borrow_mut()
                            .with_reader_mut(|r| {
                                let w = r
                                    .writer_mut()
                                    .expect("reader replay requested for non-materialized reader");
                                // ensure that all writes have been applied
                                w.swap();
                                w.with_key(&key[..])
                                    .try_find_and(|_| ())
                                    .expect("reader replay requested for non-ready reader")
                                    .0
                                    .is_none()
                            })
                            .expect("reader replay requested for non-reader node");

                        // ensure that we haven't already requested a replay of this key
                        if still_miss
                            && self
                                .reader_triggered
                                .entry(node)
                                .or_default()
                                .insert(key.clone())
                        {
                            self.find_tags_and_replay(key, &cols[..], node);
                        }
                        self.total_replay_time.stop();
                    }
                    Packet::RequestPartialReplay { tag, key } => {
                        trace!(
                            self.log,
                           "got replay request";
                           "tag" => tag.id(),
                           "key" => format!("{:?}", key)
                        );
                        self.total_replay_time.start();
                        self.seed_replay(tag, &key[..], sends, executor);
                        self.total_replay_time.stop();
                    }
                    Packet::StartReplay { tag, from } => {
                        use std::thread;
                        assert_eq!(self.replay_paths[&tag].source, Some(from));

                        let start = time::Instant::now();
                        self.total_replay_time.start();
                        info!(self.log, "starting replay");

                        // we know that the node is materialized, as the migration coordinator
                        // picks path that originate with materialized nodes. if this weren't the
                        // case, we wouldn't be able to do the replay, and the entire migration
                        // would fail.
                        //
                        // we clone the entire state so that we can continue to occasionally
                        // process incoming updates to the domain without disturbing the state that
                        // is being replayed.
                        let state = self
                            .state
                            .get(from)
                            .expect("migration replay path started with non-materialized node")
                            .cloned_records();

                        debug!(self.log,
                               "current state cloned for replay";
                               "μs" => start.elapsed().as_micros()
                        );

                        let link = Link::new(from, self.replay_paths[&tag].path[0].node);

                        // we're been given an entire state snapshot, but we need to digest it
                        // piece by piece spawn off a thread to do that chunking. however, before
                        // we spin off that thread, we need to send a single Replay message to tell
                        // the target domain to start buffering everything that follows. we can't
                        // do that inside the thread, because by the time that thread is scheduled,
                        // we may already have processed some other messages that are not yet a
                        // part of state.
                        let p = box Packet::ReplayPiece {
                            tag,
                            link,
                            context: ReplayPieceContext::Regular {
                                last: state.is_empty(),
                            },
                            data: Vec::<Record>::new().into(),
                        };

                        if !state.is_empty() {
                            let log = self.log.new(o!());

                            let added_cols = self.ingress_inject.get(from).cloned();
                            let default = {
                                let n = self.nodes[from].borrow();
                                let mut default = None;
                                if let Some(b) = n.get_base() {
                                    let mut row = Vec::new();
                                    b.fix(&mut row);
                                    default = Some(row);
                                }
                                default
                            };
                            let fix = move |mut r: Vec<DataType>| -> Vec<DataType> {
                                if let Some((start, ref added)) = added_cols {
                                    let rlen = r.len();
                                    r.extend(added.iter().skip(rlen - start).cloned());
                                } else if let Some(ref defaults) = default {
                                    let rlen = r.len();
                                    r.extend(defaults.iter().skip(rlen).cloned());
                                }
                                r
                            };

                            let replay_tx_desc = self
                                .channel_coordinator
                                .builder_for(&(self.index, self.shard.unwrap_or(0)))
                                .unwrap();

                            thread::Builder::new()
                                .name(format!(
                                    "replay{}.{}",
                                    self.nodes
                                        .values()
                                        .next()
                                        .unwrap()
                                        .borrow()
                                        .domain()
                                        .index(),
                                    link.src
                                ))
                                .spawn(move || {
                                    use itertools::Itertools;

                                    // TODO: make async
                                    let mut chunked_replay_tx =
                                        replay_tx_desc.build_sync().unwrap();

                                    let start = time::Instant::now();
                                    debug!(log, "starting state chunker"; "node" => %link.dst);

                                    let iter = state.into_iter().chunks(BATCH_SIZE);
                                    let mut iter = iter.into_iter().enumerate().peekable();

                                    // process all records in state to completion within domain
                                    // and then forward on tx (if there is one)
                                    while let Some((i, chunk)) = iter.next() {
                                        use std::iter::FromIterator;
                                        let chunk = Records::from_iter(chunk.map(&fix));
                                        let len = chunk.len();
                                        let last = iter.peek().is_none();
                                        let p = box Packet::ReplayPiece {
                                            tag,
                                            link, // to is overwritten by receiver
                                            context: ReplayPieceContext::Regular { last },
                                            data: chunk,
                                        };

                                        trace!(log, "sending batch"; "#" => i, "[]" => len);
                                        if chunked_replay_tx.send(p).is_err() {
                                            warn!(log, "replayer noticed domain shutdown");
                                            break;
                                        }
                                    }

                                    debug!(log,
                                       "state chunker finished";
                                       "node" => %link.dst,
                                       "μs" => start.elapsed().as_micros()
                                    );
                                })
                                .unwrap();
                        }
                        self.handle_replay(p, sends, executor);

                        self.total_replay_time.stop();
                    }
                    Packet::Finish(tag, ni) => {
                        self.total_replay_time.start();
                        self.finish_replay(tag, ni, sends, executor);
                        self.total_replay_time.stop();
                    }
                    Packet::Ready { node, purge, index } => {
                        assert_eq!(self.mode, DomainMode::Forwarding);

                        self.nodes[node].borrow_mut().purge = purge;

                        if !index.is_empty() {
                            let mut s: Box<dyn State> = {
                                let n = self.nodes[node].borrow();
                                let params = &self.persistence_parameters;
                                match (n.get_base(), &params.mode) {
                                    (Some(base), &DurabilityMode::DeleteOnExit)
                                    | (Some(base), &DurabilityMode::Permanent) => {
                                        let base_name = format!(
                                            "{}-{}-{}",
                                            params.log_prefix,
                                            n.name(),
                                            self.shard.unwrap_or(0),
                                        );

                                        box PersistentState::new(base_name, base.key(), &params)
                                    }
                                    _ => box MemoryState::default(),
                                }
                            };
                            for idx in index {
                                s.add_key(&idx[..], None);
                            }
                            assert!(self.state.insert(node, s).is_none());
                        } else {
                            // NOTE: just because index_on is None does *not* mean we're not
                            // materialized
                        }

                        if self.not_ready.remove(&node) {
                            trace!(self.log, "readying empty node"; "local" => node.id());
                        }

                        // swap replayed reader nodes to expose new state
                        {
                            let mut n = self.nodes[node].borrow_mut();
                            if n.is_reader() {
                                n.with_reader_mut(|r| {
                                    if let Some(ref mut state) = r.writer_mut() {
                                        trace!(self.log, "swapping state"; "local" => node.id());
                                        state.swap();
                                        trace!(self.log, "state swapped"; "local" => node.id());
                                    }
                                })
                                .unwrap();
                            }
                        }

                        self.control_reply_tx
                            .send(ControlReplyPacket::ack())
                            .unwrap();
                    }
                    Packet::GetStatistics => {
                        let domain_stats = noria::debug::stats::DomainStats {
                            total_time: self.total_time.num_nanoseconds(),
                            total_ptime: self.total_ptime.num_nanoseconds(),
                            total_replay_time: self.total_replay_time.num_nanoseconds(),
                            total_forward_time: self.total_forward_time.num_nanoseconds(),
                            wait_time: self.wait_time.num_nanoseconds(),
                        };

                        let node_stats = self
                            .nodes
                            .values()
                            .filter_map(|nd| {
                                let n = &*nd.borrow();
                                let local_index = n.local_addr();
                                let node_index: NodeIndex = n.global_addr();

                                let time = self.process_times.num_nanoseconds(local_index);
                                let ptime = self.process_ptimes.num_nanoseconds(local_index);
                                let mem_size = if n.is_reader() {
                                    let mut size = 0;
                                    n.with_reader(|r| size = r.state_size().unwrap_or(0))
                                        .unwrap();
                                    size
                                } else {
                                    self.state
                                        .get(local_index)
                                        .map(|s| s.deep_size_of())
                                        .unwrap_or(0)
                                };

                                let mat_state = if !n.is_reader() {
                                    match self.state.get(local_index) {
                                        Some(ref s) => {
                                            if s.is_partial() {
                                                MaterializationStatus::Partial {
                                                    beyond_materialization_frontier: n.purge,
                                                }
                                            } else {
                                                MaterializationStatus::Full
                                            }
                                        }
                                        None => MaterializationStatus::Not,
                                    }
                                } else {
                                    n.with_reader(|r| {
                                        if r.is_partial() {
                                            MaterializationStatus::Partial {
                                                beyond_materialization_frontier: n.purge,
                                            }
                                        } else {
                                            MaterializationStatus::Full
                                        }
                                    })
                                    .unwrap()
                                };

                                if time.is_some() && ptime.is_some() {
                                    Some((
                                        node_index,
                                        noria::debug::stats::NodeStats {
                                            desc: format!("{:?}", n),
                                            process_time: time.unwrap(),
                                            process_ptime: ptime.unwrap(),
                                            mem_size,
                                            materialized: mat_state,
                                        },
                                    ))
                                } else {
                                    None
                                }
                            })
                            .collect();

                        self.control_reply_tx
                            .send(ControlReplyPacket::Statistics(domain_stats, node_stats))
                            .unwrap();
                    }
                    Packet::UpdateStateSize => {
                        self.update_state_sizes();
                    }
                    Packet::Quit => unreachable!("Quit messages are handled by event loop"),
                    Packet::Spin => {
                        // spinning as instructed
                    }
                    _ => unreachable!(),
                }
            }
        }

        self.total_replay_time.start();
        if !self.buffered_replay_requests.is_empty() {
            let now = time::Instant::now();
            let to = self.replay_batch_timeout;
            let elapsed_replays: Vec<_> = {
                self.buffered_replay_requests
                    .iter_mut()
                    .filter_map(|(&tag, &mut (first, ref mut keys))| {
                        if !keys.is_empty() && now.duration_since(first) > to {
                            // will be removed by retain below
                            Some((tag, mem::replace(keys, HashSet::new())))
                        } else {
                            None
                        }
                    })
                    .collect()
            };
            self.buffered_replay_requests
                .retain(|_, (_, ref keys)| !keys.is_empty());
            for (tag, keys) in elapsed_replays {
                self.seed_all(tag, keys, sends, executor);
            }
        }
        self.total_replay_time.stop();

        let mut swap = HashSet::new();
        while let Some(tp) = self.timed_purges.front() {
            let now = time::Instant::now();
            if tp.time <= now {
                let tp = self.timed_purges.pop_front().unwrap();
                let mut node = self.nodes[tp.view].borrow_mut();
                trace!(self.log, "eagerly purging state from reader"; "node" => node.global_addr().index());
                node.with_reader_mut(|r| {
                    if let Some(wh) = r.writer_mut() {
                        for key in tp.keys {
                            wh.mut_with_key(&key[..]).mark_hole();
                        }
                        swap.insert(tp.view);
                    }
                })
                .unwrap();
            } else {
                break;
            }
        }
        for n in swap {
            self.nodes[n]
                .borrow_mut()
                .with_reader_mut(|r| {
                    if let Some(wh) = r.writer_mut() {
                        wh.swap();
                    }
                })
                .unwrap();
        }

        if top {
            while let Some(m) = self.delayed_for_self.pop_front() {
                trace!(self.log, "handling local transmission");
                // we really want this to just use tail recursion.
                // but alas, the compiler doesn't seem to want to do that.
                // instead, we ensure that only the topmost call to handle() walks delayed_for_self

                // WO for https://github.com/rust-lang/rfcs/issues/1403
                self.handle(m, sends, executor, false);
            }
        }
        self.wait_time.start();
    }

    fn seed_row<'a>(&self, source: LocalNodeIndex, row: Cow<'a, [DataType]>) -> Record {
        if let Some(&(start, ref defaults)) = self.ingress_inject.get(source) {
            let mut v = Vec::with_capacity(start + defaults.len());
            v.extend(row.iter().cloned());
            v.extend(defaults.iter().cloned());
            return (v, true).into();
        }

        let n = self.nodes[source].borrow();
        if let Some(b) = n.get_base() {
            let mut row = row.into_owned();
            b.fix(&mut row);
            return Record::Positive(row);
        }

        row.into_owned().into()
    }

    fn seed_all(
        &mut self,
        tag: Tag,
        keys: HashSet<Vec<DataType>>,
        sends: &mut EnqueuedSends,
        ex: &mut dyn Executor,
    ) {
        let (m, source, is_miss) = match self.replay_paths[&tag] {
            ReplayPath {
                source: Some(source),
                trigger: TriggerEndpoint::Start(ref cols),
                ref path,
                ..
            }
            | ReplayPath {
                source: Some(source),
                trigger: TriggerEndpoint::Local(ref cols),
                ref path,
                ..
            } => {
                let state = self
                    .state
                    .get(source)
                    .expect("migration replay path started with non-materialized node");

                let mut rs = Vec::new();
                let (keys, misses): (HashSet<_>, _) = keys.into_iter().partition(|key| match state
                    .lookup(&cols[..], &KeyType::from(key))
                {
                    LookupResult::Some(res) => {
                        rs.extend(res.into_iter().map(|r| self.seed_row(source, r)));
                        true
                    }
                    LookupResult::Missing => false,
                });

                let m = if !keys.is_empty() {
                    Some(box Packet::ReplayPiece {
                        link: Link::new(source, path[0].node),
                        tag,
                        context: ReplayPieceContext::Partial {
                            for_keys: keys,
                            ignore: false,
                        },
                        data: rs.into(),
                    })
                } else {
                    None
                };

                let miss = if !misses.is_empty() {
                    Some((cols.clone(), misses))
                } else {
                    None
                };

                (m, source, miss)
            }
            _ => unreachable!(),
        };

        if let Some((cols, misses)) = is_miss {
            // we have missed in our lookup, so we have a partial replay through a partial replay
            // trigger a replay to source node, and enqueue this request.
            for key in misses {
                trace!(self.log,
                       "missed during replay request";
                       "tag" => tag.id(),
                       "key" => ?key);
                self.on_replay_miss(source, &cols[..], key.clone(), key, tag);
            }
        }

        if let Some(m) = m {
            if let box Packet::ReplayPiece {
                context: ReplayPieceContext::Partial { ref for_keys, .. },
                ..
            } = m
            {
                trace!(self.log,
                       "satisfied replay request";
                       "tag" => tag.id(),
                       //"data" => ?m.as_ref().unwrap().data(),
                       "keys" => ?for_keys,
                );
            } else {
                unreachable!();
            }

            self.handle_replay(m, sends, ex);
        }
    }

    fn seed_replay(
        &mut self,
        tag: Tag,
        key: &[DataType],
        sends: &mut EnqueuedSends,
        ex: &mut dyn Executor,
    ) {
        if let ReplayPath {
            trigger: TriggerEndpoint::Start(..),
            ..
        }
        | ReplayPath {
            trigger: TriggerEndpoint::Local(..),
            ..
        } = self.replay_paths[&tag]
        {
            // maybe delay this seed request so that we can batch respond later?
            // TODO
            use std::collections::hash_map::Entry;
            let key = Vec::from(key);
            match self.buffered_replay_requests.entry(tag) {
                Entry::Occupied(o) => {
                    assert!(!o.get().1.is_empty());
                    o.into_mut().1.insert(key);
                }
                Entry::Vacant(v) => {
                    let mut ks = HashSet::new();
                    ks.insert(key);
                    v.insert((time::Instant::now(), ks));
                }
            }

            // TODO: if timer has expired, call seed_all(tag, _, sends) immediately
            return;
        }

        let (m, source, is_miss) = match self.replay_paths[&tag] {
            ReplayPath {
                source: Some(source),
                trigger: TriggerEndpoint::Start(ref cols),
                ref path,
                ..
            }
            | ReplayPath {
                source: Some(source),
                trigger: TriggerEndpoint::Local(ref cols),
                ref path,
                ..
            } => {
                let rs = self
                    .state
                    .get(source)
                    .expect("migration replay path started with non-materialized node")
                    .lookup(&cols[..], &KeyType::from(&key[..]));

                let mut k = HashSet::new();
                k.insert(Vec::from(key));
                if let LookupResult::Some(rs) = rs {
                    use std::iter::FromIterator;
                    let data = Records::from_iter(rs.into_iter().map(|r| self.seed_row(source, r)));

                    let m = Some(box Packet::ReplayPiece {
                        link: Link::new(source, path[0].node),
                        tag,
                        context: ReplayPieceContext::Partial {
                            for_keys: k,
                            ignore: false,
                        },
                        data,
                    });
                    (m, source, None)
                } else {
                    (None, source, Some(cols.clone()))
                }
            }
            _ => unreachable!(),
        };

        if let Some(cols) = is_miss {
            // we have missed in our lookup, so we have a partial replay through a partial replay
            // trigger a replay to source node, and enqueue this request.
            self.on_replay_miss(source, &cols[..], Vec::from(key), Vec::from(key), tag);
            trace!(self.log,
                   "missed during replay request";
                   "tag" => tag.id(),
                   "key" => ?key);
        } else {
            trace!(self.log,
                   "satisfied replay request";
                   "tag" => tag.id(),
                   //"data" => ?m.as_ref().unwrap().data(),
                   "key" => ?key,
            );
        }

        if let Some(m) = m {
            self.handle_replay(m, sends, ex);
        }
    }

    #[allow(clippy::cognitive_complexity)]
    fn handle_replay(&mut self, m: Box<Packet>, sends: &mut EnqueuedSends, ex: &mut dyn Executor) {
        let tag = m.tag().unwrap();
        if self.nodes[self.replay_paths[&tag].path.last().unwrap().node]
            .borrow()
            .is_dropped()
        {
            return;
        }

        let mut finished = None;
        let mut need_replay = Vec::new();
        let mut finished_partial = 0;

        // this loop is just here so we have a way of giving up the borrow of self.replay_paths
        #[allow(clippy::never_loop)]
        'outer: loop {
            let ReplayPath {
                ref path,
                ref source,
                notify_done,
                ..
            } = self.replay_paths[&tag];

            match self.mode {
                DomainMode::Forwarding if notify_done => {
                    // this is the first message we receive for this tagged replay path. only at
                    // this point should we start buffering messages for the target node. since the
                    // node is not yet marked ready, all previous messages for this node will
                    // automatically be discarded by dispatch(). the reason we should ignore all
                    // messages preceeding the first replay message is that those have already been
                    // accounted for in the state we are being replayed. if we buffered them and
                    // applied them after all the state has been replayed, we would double-apply
                    // those changes, which is bad.
                    self.mode = DomainMode::Replaying {
                        to: path.last().unwrap().node,
                        buffered: VecDeque::new(),
                        passes: 0,
                    };
                }
                DomainMode::Forwarding => {
                    // we're replaying to forward to another domain
                }
                DomainMode::Replaying { .. } => {
                    // another packet the local state we are constructing
                }
            }

            // will look somewhat nicer with https://github.com/rust-lang/rust/issues/15287
            let m = *m; // workaround for #16223
            match m {
                Packet::ReplayPiece {
                    tag,
                    link,
                    mut data,
                    mut context,
                } => {
                    if let ReplayPieceContext::Partial { ref for_keys, .. } = context {
                        trace!(
                            self.log,
                            "replaying batch";
                            "#" => data.len(),
                            "tag" => tag.id(),
                            "keys" => ?for_keys,
                        );
                    } else {
                        debug!(self.log, "replaying batch"; "#" => data.len());
                    }

                    // let's collect some information about the destination of this replay
                    let dst = path.last().unwrap().node;
                    let dst_is_reader = self.nodes[dst]
                        .borrow()
                        .with_reader(|r| r.is_materialized())
                        .unwrap_or(false);
                    let dst_is_target = !self.nodes[dst].borrow().is_sender();

                    if dst_is_target {
                        // prune keys and data for keys we're not waiting for
                        if let ReplayPieceContext::Partial {
                            ref mut for_keys, ..
                        } = context
                        {
                            let had = for_keys.len();
                            let partial_keys = path.last().unwrap().partial_key.as_ref().unwrap();
                            if let Some(w) = self.waiting.get(dst) {
                                // discard all the keys that we aren't waiting for
                                for_keys.retain(|k| {
                                    w.redos.contains_key(&(partial_keys.clone(), k.clone()))
                                });
                            } else if let Some(ref prev) = self.reader_triggered.get(dst) {
                                // discard all the keys that we aren't waiting for
                                for_keys.retain(|k| prev.contains(k));
                            } else {
                                // this packet contained no keys that we're waiting for, so it's
                                // useless to us.
                                return;
                            }

                            if for_keys.is_empty() {
                                return;
                            } else if for_keys.len() != had {
                                // discard records in data associated with the keys we weren't
                                // waiting for
                                // note that we need to use the partial_keys column IDs from the
                                // *start* of the path here, as the records haven't been processed
                                // yet
                                let partial_keys =
                                    path.first().unwrap().partial_key.as_ref().unwrap();
                                data.retain(|r| {
                                    for_keys.iter().any(|k| {
                                        partial_keys.iter().enumerate().all(|(i, c)| r[*c] == k[i])
                                    })
                                });
                            }
                        }
                    }

                    // forward the current message through all local nodes.
                    let m = box Packet::ReplayPiece {
                        link,
                        tag,
                        data,
                        context: context.clone(),
                    };
                    let mut m = Some(m);

                    for (i, segment) in path.iter().enumerate() {
                        let mut n = self.nodes[segment.node].borrow_mut();
                        let is_reader = n.with_reader(|r| r.is_materialized()).unwrap_or(false);

                        // keep track of whether we're filling any partial holes
                        let partial_key_cols = segment.partial_key.as_ref();
                        let mut backfill_keys = if let ReplayPieceContext::Partial {
                            ref mut for_keys,
                            ..
                        } = context
                        {
                            debug_assert!(partial_key_cols.is_some());
                            Some(for_keys)
                        } else {
                            None
                        };

                        // figure out if we're the target of a partial replay.
                        // this is the case either if the current node is waiting for a replay,
                        // *or* if the target is a reader. the last case is special in that when a
                        // client requests a replay, the Reader isn't marked as "waiting".
                        let target = backfill_keys.is_some()
                            && i == path.len() - 1
                            && (is_reader || !n.is_sender());

                        // targets better be last
                        assert!(!target || i == path.len() - 1);

                        // are we about to fill a hole?
                        if target {
                            let backfill_keys = backfill_keys.as_ref().unwrap();
                            // mark the state for the key being replayed as *not* a hole otherwise
                            // we'll just end up with the same "need replay" response that
                            // triggered this replay initially.
                            if let Some(state) = self.state.get_mut(segment.node) {
                                for key in backfill_keys.iter() {
                                    state.mark_filled(key.clone(), tag);
                                }
                            } else {
                                n.with_reader_mut(|r| {
                                    // we must be filling a hole in a Reader. we need to ensure
                                    // that the hole for the key we're replaying ends up being
                                    // filled, even if that hole is empty!
                                    if let Some(wh) = r.writer_mut() {
                                        for key in backfill_keys.iter() {
                                            wh.mut_with_key(&key[..]).mark_filled();
                                        }
                                    }
                                })
                                .unwrap();
                            }
                        }

                        // process the current message in this node
                        let (mut misses, lookups, captured) = n.process(
                            &mut m,
                            segment.partial_key.as_ref(),
                            &mut self.state,
                            &self.nodes,
                            self.shard,
                            false,
                            sends,
                            ex,
                        );

                        // ignore duplicate misses
                        misses.sort_unstable_by(|a, b| {
                            a.on.cmp(&b.on)
                                .then_with(|| a.replay_cols.cmp(&b.replay_cols))
                                .then_with(|| a.lookup_idx.cmp(&b.lookup_idx))
                                .then_with(|| a.lookup_cols.cmp(&b.lookup_cols))
                                .then_with(|| a.lookup_key().cmp(b.lookup_key()))
                                .then_with(|| a.replay_key().unwrap().cmp(b.replay_key().unwrap()))
                        });
                        misses.dedup();

                        let missed_on = if backfill_keys.is_some() {
                            let mut missed_on = HashSet::with_capacity(misses.len());
                            for miss in &misses {
                                let k: Vec<_> = miss.replay_key_vec().unwrap();
                                missed_on.insert(k);
                            }
                            missed_on
                        } else {
                            HashSet::new()
                        };

                        if target {
                            if !misses.is_empty() {
                                // we missed while processing
                                // it's important that we clear out any partially-filled holes.
                                if let Some(state) = self.state.get_mut(segment.node) {
                                    for miss in &missed_on {
                                        state.mark_hole(&miss[..], tag);
                                    }
                                } else {
                                    n.with_reader_mut(|r| {
                                        if let Some(wh) = r.writer_mut() {
                                            for miss in &missed_on {
                                                wh.mut_with_key(&miss[..]).mark_hole();
                                            }
                                        }
                                    })
                                    .unwrap();
                                }
                            } else if is_reader {
                                // we filled a hole! swap the reader.
                                n.with_reader_mut(|r| {
                                    if let Some(wh) = r.writer_mut() {
                                        wh.swap();
                                    }
                                })
                                .unwrap();
                                // and also unmark the replay request
                                if let Some(ref mut prev) =
                                    self.reader_triggered.get_mut(segment.node)
                                {
                                    for key in backfill_keys.as_ref().unwrap().iter() {
                                        prev.remove(&key[..]);
                                    }
                                }
                            }
                        }

                        if target && !captured.is_empty() {
                            // materialized union ate some of our keys,
                            // so we didn't *actually* fill those keys after all!
                            if let Some(state) = self.state.get_mut(segment.node) {
                                for key in &captured {
                                    state.mark_hole(&key[..], tag);
                                }
                            } else {
                                n.with_reader_mut(|r| {
                                    if let Some(wh) = r.writer_mut() {
                                        for key in &captured {
                                            wh.mut_with_key(&key[..]).mark_hole();
                                        }
                                    }
                                })
                                .unwrap();
                            }
                        }

                        // we're done with the node
                        drop(n);

                        if m.is_none() {
                            // eaten full replay
                            assert_eq!(misses.len(), 0);

                            // it's been captured, so we need to *not* consider the replay finished
                            // (which the logic below matching on context would do)
                            break 'outer;
                        }

                        // we need to track how many replays we completed, and we need to do so
                        // *before* we prune keys that missed. these conditions are all important,
                        // so let's walk through them
                        //
                        //  1. this applies only to partial backfills
                        //  2. we should only set finished_partial if it hasn't already been set.
                        //     this is important, as misses will cause backfill_keys to be pruned
                        //     over time, which would cause finished_partial to hold the wrong
                        //     value!
                        //  3. if the last node on this path is a reader, or is a ::End (so we
                        //     triggered the replay) then we need to decrement the concurrent
                        //     replay count! note that it's *not* sufficient to check if the
                        //     *current* node is a target/reader, because we could miss during a
                        //     join along the path.
                        if backfill_keys.is_some()
                            && finished_partial == 0
                            && (dst_is_reader || dst_is_target)
                        {
                            finished_partial = backfill_keys.as_ref().unwrap().len();
                        }

                        // only continue with the keys that weren't captured
                        if let box Packet::ReplayPiece {
                            context:
                                ReplayPieceContext::Partial {
                                    ref mut for_keys, ..
                                },
                            ..
                        } = m.as_mut().unwrap()
                        {
                            backfill_keys
                                .as_mut()
                                .unwrap()
                                .retain(|k| for_keys.contains(&k[..]));
                        }

                        // if we missed during replay, we need to do another replay
                        if backfill_keys.is_some() && !misses.is_empty() {
                            for miss in misses {
                                need_replay.push((
                                    miss.on,
                                    miss.replay_key_vec().unwrap(),
                                    miss.lookup_key_vec(),
                                    miss.lookup_idx,
                                    tag,
                                ));
                            }

                            // we should only finish the replays for keys that *didn't* miss
                            backfill_keys
                                .as_mut()
                                .unwrap()
                                .retain(|k| !missed_on.contains(k));

                            // prune all replayed records for keys where any replayed record for
                            // that key missed.
                            let partial_col = partial_key_cols.as_ref().unwrap();
                            m.as_mut().unwrap().map_data(|rs| {
                                rs.retain(|r| {
                                    // XXX: don't we technically need to translate the columns a
                                    // bunch here? what if two key columns are reordered?
                                    // XXX: this clone and collect here is *really* sad
                                    let r = r.rec();
                                    !missed_on.contains(
                                        &partial_col
                                            .iter()
                                            .map(|&c| r[c].clone())
                                            .collect::<Vec<_>>(),
                                    )
                                })
                            });
                        }

                        // no more keys to replay, so we might as well terminate early
                        if backfill_keys
                            .as_ref()
                            .map(|b| b.is_empty())
                            .unwrap_or(false)
                        {
                            break 'outer;
                        }

                        // we successfully processed some upquery responses!
                        //
                        // at this point, we can discard the state that the replay used in n's
                        // ancestors if they are beyond the materialization frontier (and thus
                        // should not be allowed to amass significant state).
                        //
                        // we want to make sure we only remove state once it will no longer be
                        // looked up into though. consider this dataflow graph:
                        //
                        //  (a)     (b)
                        //   |       |
                        //   |       |
                        //  (q)      |
                        //   |       |
                        //   `--(j)--`
                        //       |
                        //
                        // where j is a join, a and b are materialized, q is query-through. if we
                        // removed state the moment a replay has passed through the next operator,
                        // then the following could happen: a replay then comes from a, passes
                        // through q, q then discards state from a and forwards to j. j misses in
                        // b. replay happens to b, and re-triggers replay from a. however, state in
                        // a is discarded, so replay to a needs to happen a second time. that's not
                        // _wrong_, and we will eventually make progress, but it is pretty
                        // inefficient.
                        //
                        // insted, we probably want the join to do the eviction. we achieve this by
                        // only evicting from a after the replay has passed the join (or, more
                        // generally, the operator that might perform lookups into a)
                        if backfill_keys.is_some() {
                            // first and foremost -- evict the source of the replay (if we own it).
                            // we only do this when the replay has reached its target, or if it's
                            // about to leave the domain, otherwise we might evict state that a
                            // later operator (like a join) will still do lookups into.
                            if i == path.len() - 1 {
                                // only evict if we own the state where the replay originated
                                if let Some(src) = source {
                                    let n = self.nodes[*src].borrow();
                                    if n.beyond_mat_frontier() {
                                        let state = self
                                            .state
                                            .get_mut(*src)
                                            .expect("replay sourced at non-materialized node");
                                        trace!(self.log, "clearing keys from purgeable replay source after replay"; "node" => n.global_addr().index(), "keys" => ?backfill_keys.as_ref().unwrap());
                                        for key in backfill_keys.as_ref().unwrap().iter() {
                                            state.mark_hole(&key[..], tag);
                                        }
                                    }
                                }
                            }

                            // next, evict any state that we had to look up to process this replay.
                            let mut evict_tag = None;
                            let mut pns_for = None;
                            let mut pns = Vec::new();
                            let mut tmp = Vec::new();
                            for lookup in lookups {
                                // don't evict from our own state
                                if lookup.on == segment.node {
                                    continue;
                                }

                                // resolve any lookups through query-through nodes
                                if pns_for != Some(lookup.on) {
                                    pns.clear();
                                    assert!(tmp.is_empty());
                                    tmp.push(lookup.on);

                                    while let Some(pn) = tmp.pop() {
                                        if self.state.contains_key(pn) {
                                            if self.nodes[pn].borrow().beyond_mat_frontier() {
                                                // we should evict from this!
                                                pns.push(pn);
                                            } else {
                                                // we should _not_ evict from this
                                            }
                                            continue;
                                        }

                                        // this parent needs to be resolved further
                                        let pn = self.nodes[pn].borrow();
                                        if !pn.can_query_through() {
                                            unreachable!("lookup into non-materialized, non-query-through node");
                                        }

                                        for &ppn in pn.parents() {
                                            tmp.push(ppn);
                                        }
                                    }
                                    pns_for = Some(lookup.on);
                                }

                                let tag_match = |rp: &ReplayPath, pn| {
                                    rp.path.last().unwrap().node == pn
                                        && rp.path.last().unwrap().partial_key.as_ref().unwrap()
                                            == &lookup.cols
                                };

                                for &pn in &pns {
                                    let state = self.state.get_mut(pn).unwrap();
                                    assert!(state.is_partial());

                                    // this is a node that we were doing lookups into as part of
                                    // the replay -- make sure we evict any state we may have added
                                    // there.
                                    if let Some(tag) = evict_tag {
                                        if !tag_match(&self.replay_paths[&tag], pn) {
                                            // we can't re-use this
                                            evict_tag = None;
                                        }
                                    }

                                    if evict_tag.is_none() {
                                        if let Some(ref cs) = self.replay_paths_by_dst.get(pn) {
                                            if let Some(ref tags) = cs.get(&lookup.cols) {
                                                // this is the tag we would have used to
                                                // fill a lookup hole in this ancestor, so
                                                // this is the tag we need to evict from.

                                                // TODO: could there have been multiple
                                                assert_eq!(tags.len(), 1);
                                                evict_tag = Some(tags[0]);
                                            }
                                        }
                                    }

                                    if let Some(tag) = evict_tag {
                                        // NOTE: this assumes that the key order is the same
                                        trace!(self.log, "clearing keys from purgeable materialization after replay"; "node" => self.nodes[pn].borrow().global_addr().index(), "key" => ?&lookup.key);
                                        state.mark_hole(&lookup.key[..], tag);
                                    } else {
                                        unreachable!(
                                            "no tag found for lookup target {:?}({:?}) (really {:?})",
                                            self.nodes[lookup.on].borrow().global_addr(),
                                            lookup.cols,
                                            self.nodes[pn].borrow().global_addr(),
                                        );
                                    }
                                }
                            }
                        }

                        // we're all good -- continue propagating
                        if m.as_ref().map(|m| m.is_empty()).unwrap_or(true) {
                            if let ReplayPieceContext::Regular { last: false } = context {
                                trace!(self.log, "dropping empty non-terminal full replay packet");
                                // don't continue processing empty updates, *except* if this is the
                                // last replay batch. in that case we need to send it so that the
                                // next domain knows that we're done
                                // TODO: we *could* skip ahead to path.last() here
                                break;
                            }
                        }

                        if i != path.len() - 1 {
                            // update link for next iteration
                            if self.nodes[path[i + 1].node].borrow().is_shard_merger() {
                                // we need to preserve the egress src for shard mergers
                                // (which includes shard identifier)
                            } else {
                                m.as_mut().unwrap().link_mut().src = segment.node;
                            }
                            m.as_mut().unwrap().link_mut().dst = path[i + 1].node;
                        }

                        // preserve whatever `last` flag that may have been set during processing
                        if let Some(box Packet::ReplayPiece {
                            context: ReplayPieceContext::Regular { last },
                            ..
                        }) = m
                        {
                            if let ReplayPieceContext::Regular {
                                last: ref mut old_last,
                            } = context
                            {
                                *old_last = last;
                            }
                        }

                        // feed forward any changes to the context (e.g., backfill_keys)
                        if let Some(box Packet::ReplayPiece {
                            context: ref mut mcontext,
                            ..
                        }) = m
                        {
                            *mcontext = context.clone();
                        }
                    }

                    match context {
                        ReplayPieceContext::Regular { last } if last => {
                            debug!(self.log,
                                   "last batch processed";
                                   "terminal" => notify_done
                            );
                            if notify_done {
                                debug!(self.log, "last batch received"; "local" => dst.id());
                                finished = Some((tag, dst, None));
                            }
                        }
                        ReplayPieceContext::Regular { .. } => {
                            debug!(self.log, "batch processed");
                        }
                        ReplayPieceContext::Partial { for_keys, ignore } => {
                            assert!(!ignore);
                            if dst_is_reader {
                                if self.nodes[dst].borrow().beyond_mat_frontier() {
                                    // make sure we eventually evict these from here
                                    self.timed_purges.push_back(TimedPurge {
                                        time: time::Instant::now()
                                            + time::Duration::from_millis(50),
                                        keys: for_keys,
                                        view: dst,
                                        tag,
                                    });
                                }
                                assert_ne!(finished_partial, 0);
                            } else if dst_is_target {
                                trace!(self.log, "partial replay completed"; "local" => dst.id());
                                if finished_partial == 0 {
                                    assert!(for_keys.is_empty());
                                }
                                finished = Some((tag, dst, Some(for_keys)));
                            } else {
                                // we're just on the replay path
                            }
                        }
                    }
                }
                _ => unreachable!(),
            }
            break;
        }

        if finished_partial != 0 {
            self.finished_partial_replay(tag, finished_partial);
        }

        for (node, while_replaying_key, miss_key, miss_cols, tag) in need_replay {
            trace!(self.log,
                   "missed during replay processing";
                   "tag" => tag.id(),
                   "during" => ?while_replaying_key,
                   "missed" => ?miss_key,
                   "on" => %node,
            );
            self.on_replay_miss(node, &miss_cols[..], while_replaying_key, miss_key, tag);
        }

        if let Some((tag, ni, for_keys)) = finished {
            trace!(self.log, "partial replay finished";
                   "node" => ?ni,
                   "keys" => ?for_keys);
            if let Some(mut waiting) = self.waiting.remove(ni) {
                trace!(
                    self.log,
                    "partial replay finished to node with waiting backfills";
                    "keys" => ?for_keys,
                    "waiting" => ?waiting,
                );

                let key_cols = self.replay_paths[&tag]
                    .path
                    .last()
                    .unwrap()
                    .partial_key
                    .clone()
                    .unwrap();

                // we got a partial replay result that we were waiting for. it's time we let any
                // downstream nodes that missed in us on that key know that they can (probably)
                // continue with their replays.
                for key in for_keys.unwrap() {
                    let hole = (key_cols.clone(), key);
                    let replay = waiting.redos.remove(&hole).unwrap_or_else(|| {
                        panic!(
                            "got backfill for unnecessary key {:?} via tag {:?}",
                            hole.1, tag
                        )
                    });

                    // we may need more holes to fill before some replays should be re-attempted
                    let replay: Vec<_> = replay
                        .into_iter()
                        .filter_map(|tagged_replay_key| {
                            let left = {
                                let left = waiting.holes.get_mut(&tagged_replay_key).unwrap();
                                *left -= 1;
                                *left
                            };

                            if left == 0 {
                                trace!(self.log, "filled last hole for key, triggering replay";
                                   "k" => ?tagged_replay_key);

                                // we've filled all holes that prevented the replay previously!
                                waiting.holes.remove(&tagged_replay_key);
                                Some(tagged_replay_key)
                            } else {
                                trace!(self.log, "filled hole for key, not triggering replay";
                                   "k" => ?tagged_replay_key,
                                   "left" => left);
                                None
                            }
                        })
                        .collect();

                    for (tag, replay_key) in replay {
                        self.delayed_for_self
                            .push_back(box Packet::RequestPartialReplay {
                                tag,
                                key: replay_key,
                            });
                    }
                }

                if !waiting.holes.is_empty() {
                    // there are still holes, so there must still be pending redos
                    assert!(!waiting.redos.is_empty());

                    // restore Waiting in case seeding triggers more replays
                    self.waiting.insert(ni, waiting);
                } else {
                    // there are no more holes that are filling, so there can't be more redos
                    assert!(waiting.redos.is_empty());
                }
                return;
            } else if for_keys.is_some() {
                unreachable!("got unexpected replay of {:?} for {:?}", for_keys, ni)
            } else {
                // must be a full replay
                // NOTE: node is now ready, in the sense that it shouldn't ignore all updates since
                // replaying_to is still set, "normal" dispatch calls will continue to be buffered, but
                // this allows finish_replay to dispatch into the node by overriding replaying_to.
                self.not_ready.remove(&ni);
                self.delayed_for_self.push_back(box Packet::Finish(tag, ni));
            }
        }
    }

    fn finish_replay(
        &mut self,
        tag: Tag,
        node: LocalNodeIndex,
        sends: &mut EnqueuedSends,
        ex: &mut dyn Executor,
    ) {
        let mut was = mem::replace(&mut self.mode, DomainMode::Forwarding);
        let finished = if let DomainMode::Replaying {
            ref to,
            ref mut buffered,
            ref mut passes,
        } = was
        {
            if *to != node {
                // we're told to continue replay for node a, but not b is being replayed
                unreachable!();
            }
            // log that we did another pass
            *passes += 1;

            let mut handle = buffered.len();
            if handle > 100 {
                handle /= 2;
            }

            let mut handled = 0;
            while let Some(m) = buffered.pop_front() {
                // some updates were propagated to this node during the migration. we need to
                // replay them before we take even newer updates. however, we don't want to
                // completely block the domain data channel, so we only process a few backlogged
                // updates before yielding to the main loop (which might buffer more things).

                if let m @ box Packet::Message { .. } = m {
                    // NOTE: we specifically need to override the buffering behavior that our
                    // self.replaying_to = Some above would initiate.
                    self.mode = DomainMode::Forwarding;
                    self.dispatch(m, sends, ex);
                } else {
                    unreachable!();
                }

                handled += 1;
                if handled == handle {
                    // we want to make sure we actually drain the backlog we've accumulated
                    // but at the same time we don't want to completely stall the system
                    // therefore we only handle half the backlog at a time
                    break;
                }
            }

            buffered.is_empty()
        } else {
            // we're told to continue replay, but nothing is being replayed
            unreachable!();
        };
        mem::replace(&mut self.mode, was);

        if finished {
            // node is now ready, and should start accepting "real" updates
            if let DomainMode::Replaying { passes, .. } =
                mem::replace(&mut self.mode, DomainMode::Forwarding)
            {
                debug!(self.log,
                       "node is fully up-to-date";
                       "local" => node.id(),
                       "passes" => passes
                );
            } else {
                unreachable!();
            }

            if self.replay_paths[&tag].notify_done {
                // NOTE: this will only be Some for non-partial replays
                info!(self.log, "acknowledging replay completed"; "node" => node.id());
                self.control_reply_tx
                    .send(ControlReplyPacket::ack())
                    .unwrap();
            } else {
                unreachable!()
            }
        } else {
            // we're not done -- inject a request to continue handling buffered things
            self.delayed_for_self
                .push_back(box Packet::Finish(tag, node));
        }
    }

    pub fn handle_eviction(&mut self, m: Box<Packet>, sends: &mut EnqueuedSends) {
        #[allow(clippy::too_many_arguments)]
        fn trigger_downstream_evictions(
            log: &Logger,
            key_columns: &[usize],
            keys: &[Vec<DataType>],
            node: LocalNodeIndex,
            sends: &mut EnqueuedSends,
            not_ready: &HashSet<LocalNodeIndex>,
            replay_paths: &HashMap<Tag, ReplayPath>,
            shard: Option<usize>,
            state: &mut StateMap,
            nodes: &mut DomainNodes,
        ) {
            // TODO: this is a linear walk of replay paths -- we should make that not linear
            for (tag, ref path) in replay_paths {
                if path.source == Some(node) {
                    // Check whether this replay path is for the same key.
                    match path.trigger {
                        TriggerEndpoint::Local(ref key) | TriggerEndpoint::Start(ref key) => {
                            // what if just key order changed?
                            if &key[..] != key_columns {
                                continue;
                            }
                        }
                        _ => unreachable!(),
                    };

                    let mut keys = Vec::from(keys);
                    walk_path(&path.path[..], &mut keys, *tag, shard, nodes, sends);

                    if let TriggerEndpoint::Local(_) = path.trigger {
                        let target = replay_paths[&tag].path.last().unwrap();
                        if nodes[target.node].borrow().is_reader() {
                            // already evicted from in walk_path
                            continue;
                        }
                        if !state.contains_key(target.node) {
                            // this is probably because
                            if !not_ready.contains(&target.node) {
                                debug!(log, "got eviction for ready but stateless node";
                                       "node" => target.node.id());
                            }
                            continue;
                        }

                        state[target.node].evict_keys(*tag, &keys[..]);
                        trigger_downstream_evictions(
                            log,
                            &target.partial_key.as_ref().unwrap()[..],
                            &keys[..],
                            target.node,
                            sends,
                            not_ready,
                            replay_paths,
                            shard,
                            state,
                            nodes,
                        );
                    }
                }
            }
        }

        fn walk_path(
            path: &[ReplayPathSegment],
            keys: &mut Vec<Vec<DataType>>,
            tag: Tag,
            shard: Option<usize>,
            nodes: &mut DomainNodes,
            sends: &mut EnqueuedSends,
        ) {
            let mut from = path[0].node;
            for segment in path {
                nodes[segment.node].borrow_mut().process_eviction(
                    from,
                    &segment.partial_key.as_ref().unwrap()[..],
                    keys,
                    tag,
                    shard,
                    sends,
                );
                from = segment.node;
            }
        }

        match (*m,) {
            (Packet::Evict { node, num_bytes },) => {
                let node = node.map(|n| (n, num_bytes)).or_else(|| {
                    self.nodes
                        .values()
                        .filter_map(|nd| {
                            let n = &*nd.borrow();
                            let local_index = n.local_addr();

                            if n.is_reader() {
                                let mut size = 0;
                                n.with_reader(|r| {
                                    if r.is_partial() {
                                        size = r.state_size().unwrap_or(0)
                                    }
                                })
                                .unwrap();
                                Some((local_index, size))
                            } else {
                                self.state
                                    .get(local_index)
                                    .filter(|state| state.is_partial())
                                    .map(|state| (local_index, state.deep_size_of()))
                            }
                        })
                        .filter(|&(_, s)| s > 0)
                        .max_by_key(|&(_, s)| s)
                        .map(|(n, s)| {
                            trace!(self.log, "chose to evict from node {:?} with size {}", n, s);
                            (n, cmp::min(num_bytes, s as usize))
                        })
                });

                if let Some((node, num_bytes)) = node {
                    let mut freed = 0u64;
                    while freed < num_bytes as u64 {
                        if self.nodes[node].borrow().is_dropped() {
                            break; // Node was dropped. Give up.
                        } else if self.nodes[node].borrow().is_reader() {
                            // we can only evict one key a time here because the freed memory
                            // calculation is based on the key that *will* be evicted. We may count
                            // the same individual key twice if we batch evictions here.
                            let freed_now = self.nodes[node]
                                .borrow_mut()
                                .with_reader_mut(|r| r.evict_random_key())
                                .unwrap();

                            freed += freed_now;
                            if freed_now == 0 {
                                break;
                            }
                        } else {
                            let (key_columns, keys, bytes) = {
                                let k = self.state[node].evict_random_keys(100);
                                (k.0.to_vec(), k.1, k.2)
                            };
                            freed += bytes;

                            trigger_downstream_evictions(
                                &self.log,
                                &key_columns[..],
                                &keys[..],
                                node,
                                sends,
                                &self.not_ready,
                                &self.replay_paths,
                                self.shard,
                                &mut self.state,
                                &mut self.nodes,
                            );

                            if self.state[node].deep_size_of() == 0 {
                                break;
                            }
                        }
                    }
                }
            }
            (Packet::EvictKeys {
                link: Link { dst, .. },
                mut keys,
                tag,
            },) => {
                let (trigger, path) = if let Some(rp) = self.replay_paths.get(&tag) {
                    (&rp.trigger, &rp.path)
                } else {
                    debug!(self.log, "got eviction for tag that has not yet been finalized";
                           "tag" => tag.id());
                    return;
                };

                let i = path
                    .iter()
                    .position(|ps| ps.node == dst)
                    .expect("got eviction for non-local node");
                walk_path(
                    &path[i..],
                    &mut keys,
                    tag,
                    self.shard,
                    &mut self.nodes,
                    sends,
                );

                match trigger {
                    TriggerEndpoint::End { .. } | TriggerEndpoint::Local(..) => {
                        // This path terminates inside the domain. Find the target node, evict
                        // from it, and then propagate the eviction further downstream.
                        let target = path.last().unwrap().node;
                        // We've already evicted from readers in walk_path
                        if self.nodes[target].borrow().is_reader() {
                            return;
                        }
                        // No need to continue if node was dropped.
                        if self.nodes[target].borrow().is_dropped() {
                            return;
                        }
                        if let Some(evicted) = self.state[target].evict_keys(tag, &keys) {
                            let key_columns = evicted.0.to_vec();
                            trigger_downstream_evictions(
                                &self.log,
                                &key_columns[..],
                                &keys[..],
                                target,
                                sends,
                                &self.not_ready,
                                &self.replay_paths,
                                self.shard,
                                &mut self.state,
                                &mut self.nodes,
                            );
                        }
                    }
                    TriggerEndpoint::None | TriggerEndpoint::Start(..) => {}
                }
            }
            _ => unreachable!(),
        };
    }

    pub fn id(&self) -> (Index, usize) {
        (self.index, self.shard.unwrap_or(0))
    }

    pub fn booted(&mut self, addr: SocketAddr) {
        info!(self.log, "booted domain"; "nodes" => self.nodes.len());
        self.control_reply_tx
            .send(ControlReplyPacket::Booted(self.shard.unwrap_or(0), addr))
            .unwrap();
        self.wait_time.start();
    }

    pub fn update_state_sizes(&mut self) {
        let total: u64 = self
            .nodes
            .values()
            .map(|nd| {
                let n = &*nd.borrow();
                let local_index = n.local_addr();

                if n.is_reader() {
                    // We are a reader, which has its own kind of state
                    let mut size = 0;
                    n.with_reader(|r| {
                        if r.is_partial() {
                            size = r.state_size().unwrap_or(0)
                        }
                    })
                    .unwrap();
                    size
                } else {
                    // Not a reader, state is with domain
                    self.state
                        .get(local_index)
                        .filter(|state| state.is_partial())
                        .map(|s| s.deep_size_of())
                        .unwrap_or(0)
                }
            })
            .sum();

        self.state_size.store(total as usize, Ordering::Relaxed);
        // no response sent, as worker will read the atomic
    }

    pub fn on_event(
        &mut self,
        executor: &mut dyn Executor,
        event: PollEvent,
        sends: &mut EnqueuedSends,
    ) -> ProcessResult {
        self.wait_time.stop();
        //self.total_time.start();
        //self.total_ptime.start();
        let res = match event {
            PollEvent::ResumePolling => {
                // when do we need to be woken up again?
                let now = time::Instant::now();
                let opt1 = self
                    .buffered_replay_requests
                    .iter()
                    .filter(|&(_, &(_, ref keys))| !keys.is_empty())
                    .map(|(_, &(first, _))| {
                        self.replay_batch_timeout
                            .checked_sub(now.duration_since(first))
                            .unwrap_or(time::Duration::from_millis(0))
                    })
                    .min();
                let opt2 = self.group_commit_queues.duration_until_flush();
                let opt3 = self.timed_purges.front().map(|tp| {
                    if tp.time > now {
                        tp.time - now
                    } else {
                        time::Duration::from_millis(0)
                    }
                });

                let mut timeout = opt1.or(opt2).or(opt3);
                if let Some(opt2) = opt2 {
                    timeout = Some(std::cmp::min(timeout.unwrap(), opt2));
                }
                if let Some(opt3) = opt3 {
                    timeout = Some(std::cmp::min(timeout.unwrap(), opt3));
                }
                ProcessResult::KeepPolling(timeout)
            }
            PollEvent::Process(packet) => {
                if let Packet::Quit = *packet {
                    return ProcessResult::StopPolling;
                }

                // TODO: Initialize tracer here, and when flushing group commit
                // queue.
                if self.group_commit_queues.should_append(&packet, &self.nodes) {
                    packet.trace(PacketEvent::ExitInputChannel);
                    if let Some(packet) = self.group_commit_queues.append(packet) {
                        self.handle(packet, sends, executor, true);
                    }
                } else {
                    self.handle(packet, sends, executor, true);
                }

                while let Some(m) = self.group_commit_queues.flush_if_necessary() {
                    self.handle(m, sends, executor, true);
                }

                ProcessResult::Processed
            }
            PollEvent::Timeout => {
                while let Some(m) = self.group_commit_queues.flush_if_necessary() {
                    self.handle(m, sends, executor, true);
                }

                if !self.buffered_replay_requests.is_empty() || !self.timed_purges.is_empty() {
                    self.handle(box Packet::Spin, sends, executor, true);
                }

                ProcessResult::Processed
            }
        };
        self.wait_time.start();
        res
    }
}
