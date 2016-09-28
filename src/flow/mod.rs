use petgraph;
use clocked_dispatch;

use std::fmt;
use std::fmt::Debug;
use std::sync::mpsc;
use std::sync;
use std::thread;
use std::cmp::Ordering;

use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::BinaryHeap;

pub use petgraph::graph::NodeIndex;

use ops;

pub trait View<Q: Clone + Send>: Debug + 'static + Send + Sync {
    type Update: Clone + Send;
    type Data: Clone + Send;

    /// Method to prime a view before forwarding commences.
    ///
    /// This method is run once after a view is created, but before it is started (and thus, before
    /// it receives any updates). The node should obtain handles to any parent nodes it needs from
    /// the provided graph, and return a list of the nodes it depends on. The nodes in the returned
    /// list are the ones this node will receive forwarded updates for. If the returned list is
    /// empty, the node is assumed to be a base node.
    ///
    /// Note that this method may be called before the entire graph has been assembled. Only the
    /// ancestors (all the way to the root) are guaranteed to be present.
    fn prime(&mut self, &petgraph::Graph<Option<sync::Arc<View<Q, Update=Self::Update, Data=Self::Data>>>, ()>) -> Vec<NodeIndex>;

    /// Execute a single query, producing all matching records.
    fn find<'a>(&'a self, Option<Q>, Option<i64>) -> Vec<(Self::Data, i64)>;

    /// Process a new update. This may optionally produce a new update to propagate to child nodes
    /// in the data flow graph.
    fn process(&self, Self::Update, NodeIndex, i64) -> Option<Self::Update>;

    /// Suggest fields of this view, or its ancestors, that would benefit from having an index.
    /// The passed node index is the index of the current node.
    fn suggest_indexes(&self, NodeIndex) -> HashMap<NodeIndex, Vec<usize>>;

    /// Resolve where the given field originates from. If this view is materialized, None should be
    /// returned.
    fn resolve(&self, usize) -> Option<Vec<(NodeIndex, usize)>>;

    /// Add an index on the given field.
    fn add_index(&self, usize);

    /// Called to indicate that the node will not receive any future queries with timestamps
    /// earlier than or equal to the given timestamp.
    fn safe(&self, i64);

    /// Called when a view is added after the system has already been operational (i.e., ts > 0).
    /// This is called before the view is given any operations to process, and should initialize
    /// any internal state (such as materialized data) such that the node is ready to process
    /// subsequent updates at timestamps after the given initialization timestamp.
    fn init_at(&self, i64);

    /// Returns the underlying operator for this view (if any).
    /// This is a bit of a hack, but the only way to introspect on views for the purpose of graph
    /// transformations.
    fn operator(&self) -> Option<&ops::NodeType>;

    /// Returns the name of this view.
    fn name(&self) -> &str;

    /// Returns the arguments to this view.
    fn args(&self) -> &[String];
}

/// Holds flow graph node state that may change as new nodes are added to the graph.
struct Context<T: Clone + Send> {
    txs: Vec<mpsc::SyncSender<(NodeIndex, Option<T>, i64)>>,

    // this deserves some attention.
    // this contains the set of minimum timestamp trackers it should check in order to see whether
    // it's safe to absorb up to a given timestamp. For example, say that node A has descendants B
    // and C. B is materialized, C is not. C has the descendant D, which is materialized. No
    // queries below B or D should ever reach A, and thus it is safe for A to absorb updates with
    // a timestamp lower than that of min(ts_B, ts_D). In this case, the map would contain an
    // entry {A => [ts_B, ts_D]}. The map is inside an arc-lock, such that it can be shared
    // between threads, but can still be updated if the data flow graph is extended (which might
    // add additional descendants).
    min_check: Vec<sync::Arc<sync::atomic::AtomicIsize>>,
}
type SharedContext<T: Clone + Send> = sync::Arc<sync::Mutex<Context<T>>>;

struct NodeState<Q: Clone + Send + Sync, U: Clone + Send, D: Clone + Send> {
    name: NodeIndex,
    inner: sync::Arc<View<Q, Update = U, Data = D>>,
    srcs: Vec<NodeIndex>,
    input: mpsc::Receiver<(NodeIndex, Option<U>, i64)>,
    context: SharedContext<U>,
    processed_ts: sync::Arc<sync::atomic::AtomicIsize>,
    init_ts: i64,
}

/// `Delayed` is used to keep track of messages that cannot yet be safely delivered because it
/// would violate the in-order guarantees.
///
/// `Delayed` structs are ordered by their timestamp such that the *lowest* is the "highest". This
/// is so that `Delayed` can easily be used in a `BinaryHeap`.
struct Delayed<T> {
    ts: i64,
    data: T,
}

impl<T> PartialEq for Delayed<T> {
    fn eq(&self, other: &Delayed<T>) -> bool {
        other.ts == self.ts
    }
}

impl<T> PartialOrd for Delayed<T> {
    fn partial_cmp(&self, other: &Delayed<T>) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> Eq for Delayed<T> {}

impl<T> Ord for Delayed<T> {
    fn cmp(&self, other: &Delayed<T>) -> Ordering {
        other.ts.cmp(&self.ts)
    }
}

/// `FlowGraph` is the main entry point for all of distributary. It provides methods to construct,
/// update, and execute a data flow graph.
///
/// The `FlowGraph` is heavily parameterized to simplify testing. Admittedly, the type signature
/// could probably be simplified while maintaining enough flexibility for testing, but oh well. The
/// type arguments are:
///
///  - `Q`: The type used for queries that are issued across the edges of the graph.
///  - `U`: The type used for updates that propagate through the graph.
///  - `D`: The type used for records (`D`ata), i.e., what is returned by view queries, and what is
///         inserted by users of `FlowGraph` using the channels returned by `run()`.
///
/// When a new `FlowGraph` has been constructed, it will first be used to construct a graph using
/// `incorporate()`. This method takes a `View`, and asks the operator for its set of ancestor
/// nodes to hook it into the graph. In general, any `NodeType` can be used to make a view by
/// passing it to `new` along with its field names and a boolean marking whether that node should
/// be materialized. To construct a simple two-node graph that logs votes and keeps vote counts up
/// to date:
///
/// ```
/// use distributary::*;
///
/// let mut g = FlowGraph::new();
///
/// // set up a base node and a view
/// let vote = g.incorporate(new("vote", &["user", "id"], true, Base {}));
/// let votecount = g.incorporate(
///     new("vc", &["id", "votes"], true, Aggregation::COUNT.new(vote, 0))
/// );
/// # drop(vote);
/// # drop(votecount);
/// ```
///
/// Once a graph has been constructed, `run()` should be called to start execution of the data flow
/// graph. `run()` returns objects that can be used to insert records and query the various views
/// in the graph:
///
/// ```
/// # #[macro_use] extern crate shortcut;
/// # #[macro_use] extern crate distributary;
/// # fn main() {
/// # use std::time::Duration;
/// # use std::thread::sleep;
/// # use distributary::*;
/// # let mut g = FlowGraph::new();
/// # let vote = g.incorporate(new("vote", &["user", "id"], true, Base {}));
/// # let votecount = g.incorporate(
/// #     new("vc", &["id", "votes"], true, Aggregation::COUNT.new(vote, 0))
/// # );
/// // start the data flow graph
/// let (put, get) = g.run(10);
///
/// // put can now be used to insert votes
/// put[&vote].send(vec![1.into(), 1.into()]);
/// put[&vote].send(vec![2.into(), 1.into()]);
/// put[&vote].send(vec![3.into(), 1.into()]);
/// put[&vote].send(vec![1.into(), 2.into()]);
/// put[&vote].send(vec![2.into(), 2.into()]);
///
/// // allow them to propagate before querying
/// sleep(Duration::from_millis(100));
///
/// // get can be used to query votecount
/// assert_eq!(
///     {
///         use shortcut::{Condition,Comparison,Value};
///         get[&votecount](Some(Query::new(
///             &[true, true], // select both fields
///             vec![Condition {
///                 column: 0, // filter on id
///                 cmp: Comparison::Equal(Value::Const(1.into())), // == 1
///             }]
///         )))
///     }[0], // get the first (and only) result row
///     vec![1.into(), 3.into()]
/// )
/// # }
/// ```
pub struct FlowGraph<Q, U, D> where
    Q: Clone + Send + Sync,
    U: Clone + Send,
    D: Clone + Send
{
    graph: petgraph::Graph<Option<sync::Arc<View<Q, Update=U, Data=D>>>,()>,
    source: petgraph::graph::NodeIndex,
    mins: HashMap<petgraph::graph::NodeIndex, sync::Arc<sync::atomic::AtomicIsize>>,
    wait: Vec<thread::JoinHandle<()>>,
    dispatch: clocked_dispatch::Dispatcher<D>,

    contexts: HashMap<petgraph::graph::NodeIndex, SharedContext<U>>,
}

impl<Q, U, D> FlowGraph<Q, U, D>
    where Q: 'static + Clone + Debug + Send + Sync,
          U: 'static + Clone + Send,
          D: 'static + Clone + Send + Into<U>
{
    /// Construct a new, empy data flow graph.
    pub fn new() -> FlowGraph<Q, U, D> {
        let mut graph = petgraph::Graph::new();
        let source = graph.add_node(None);
        FlowGraph {
            graph: graph,
            source: source,
            mins: HashMap::default(),
            wait: Vec::default(),
            dispatch: clocked_dispatch::new(20),
            contexts: HashMap::default(),
        }
    }

    /// Return a reference to the internal graph, as well as the identifier for the root node.
    pub fn graph(&self) -> (
        &petgraph::Graph<Option<sync::Arc<View<Q, Update=U, Data=D>>>, ()>,
        NodeIndex) {
        (&self.graph, self.source)
    }

    /// Query all nodes for what indices they believe should be maintained, and apply those to the
    /// graph.
    ///
    /// This function is somewhat complicated by the fact that we need to push indices through
    /// non-materialized views so that they end up on the columns of the views that will actually
    /// query into a table of some sort.
    fn add_indices(&mut self) {
        // figure out what indices we should add
        let mut indices = petgraph::BfsIter::new(&self.graph, self.source)
            .filter(|&node| node != self.source)
            .flat_map(|node| self.graph[node].as_ref().unwrap().suggest_indexes(node).into_iter())
            .fold(HashMap::new(), |mut hm, (v, idxs)| {
                assert!(v != self.source);
                hm.entry(v).or_insert_with(HashSet::new).extend(idxs.into_iter());
                hm
            });

        // only index on materialized views
        {
            let mut leftover_indices: HashMap<_, _> = indices.drain().collect();
            let mut tmp = HashMap::new();
            while !leftover_indices.is_empty() {
                for (v, cols) in leftover_indices.drain() {
                    assert!(v != self.source);

                    let node = self.graph[v].as_ref().unwrap();

                    for col in cols.into_iter() {
                        let really = node.resolve(col);
                        if let Some(really) = really {
                            // this view is not materialized. the index should instead be placed on
                            // the corresponding columns of this view's inputs
                            for (v, col) in really.into_iter() {
                                tmp.entry(v).or_insert_with(HashSet::new).insert(col);
                            }
                        } else {
                            // this view is materialized, so we should index this column
                            indices.entry(v).or_insert_with(HashSet::new).insert(col);
                        }
                    }
                }
                leftover_indices.extend(tmp.drain());
            }
        }

        // add the indices we found
        for (v, cols) in indices.into_iter() {
            let node = self.graph[v].as_ref().unwrap();
            for col in cols {
                println!("adding index on column {:?} of view {:?}", col, v);
                // TODO: don't re-add indices that already exist
                node.add_index(col);
            }
        }
    }

    /// Build and update the node state used by `FlowGraph::inner` to track information about its
    /// outgoing neighbors for the purposes of absorbption.
    fn build_contexts(&mut self,
                      start: &mut HashMap<NodeIndex, mpsc::Receiver<(NodeIndex, Option<U>, i64)>>,
                      new: &HashSet<NodeIndex>,
                      buf: usize)
                      -> HashMap<NodeIndex, i64> {
        use std::collections::hash_map::Entry;

        // allocate contexts for all new nodes
        let mut new_ins = HashMap::with_capacity(new.len());
        for node in new.iter() {
            if let Entry::Vacant(v) = self.contexts.entry(*node) {
                let (tx, rx) = mpsc::sync_channel(buf);
                v.insert(sync::Arc::new(sync::Mutex::new(Context {
                    txs: vec![],
                    min_check: Vec::new(),
                })));
                if !start.contains_key(node) {
                    new_ins.insert(*node, tx);
                    start.insert(*node, rx);
                }
            }
        }

        // there are two things we need to do in order to set up all the contexts correctly (this
        // includes setting up new ones as well as updating old ones). first, we need to ensure
        // that every node's min_check contains all of its immediate children. second, we need to
        // add every node to its parents' output buses. note that we update the nodes in
        // topological order. this is necessary because the locks need to be taken such that an
        // upstream node isn't holding the lock while trying to send to a downstream node that we
        // have already locked.
        let mut max_absorbed = HashMap::new();
        let mut topo = petgraph::visit::SubTopo::from_node(&self.graph, self.source);
        while let Some(node) = topo.next(&self.graph) {
            if node == self.source {
                continue;
            }

            let mut ctx = self.contexts[&node].lock().unwrap();

            // find all of this node's closest materialized descendants and leaves
            let mut descendants = Vec::new();
            let mut visit = self.graph.neighbors(node).collect::<Vec<_>>();
            while !visit.is_empty() {
                let mut tmp = Vec::new();
                for desc in visit.drain(..) {
                    let d = self.graph[desc].as_ref().unwrap();
                    if d.resolve(0).is_none() {
                        // materialized
                        descendants.push(desc);
                    } else {
                        // not materialized
                        // is it a leaf node?
                        let mut neighbors = self.graph.neighbors(desc).peekable();
                        if neighbors.peek().is_none() {
                            // yes -- we are bound by its ts since it might issue queries with
                            // its current timestamp if invoked by a client
                            descendants.push(desc);
                        } else {
                            // no -- look for materialized nodes/leaves in its children
                            //
                            // TODO
                            // what if an external query is issued to *this* node?
                            // does it break in that case?
                            tmp.extend(neighbors);
                        }
                    }
                }
                visit.extend(tmp.drain(..));
            }

            // keep track of how much each node *may* have absorbed, as this places a lower bound
            // on what timestamp its new children can initialize on. we know that this value can't
            // increase until all new children have been initialized, because those new children
            // will be the new min with their processed_ts = 0.
            let may_have_absorbed = descendants.iter()
                .filter(|&n| !new.contains(n))
                .map(|&n| &self.mins[&n])
                .map(|m| m.load(sync::atomic::Ordering::Relaxed) as i64)
                .min();

            // if the view used to have no children, it is allowed to absorb immediately. in that
            // case, we should consider it to be absorbed up to its processed_ts.
            let may_have_absorbed =
                may_have_absorbed.unwrap_or_else(|| {
                    self.mins[&node].load(sync::atomic::Ordering::Relaxed) as i64
                });

            max_absorbed.insert(node, may_have_absorbed);

            // find all their atomic min counters
            let mins = descendants.into_iter()
                .map(|desc| self.mins[&desc].clone())
                .collect::<Vec<_>>();

            // and update the node's entry in min_check so it will see any new nodes
            ctx.min_check.clear();
            ctx.min_check.extend(mins.into_iter());

            // if the node has any new children, add those as receivers to our output bus
            for new_child in self.graph
                .neighbors_directed(node, petgraph::EdgeDirection::Outgoing)
                .filter(|ni| new.contains(ni)) {
                ctx.txs.push(new_ins[&new_child].clone());
            }
        }

        let latest =
            self.mins.values().map(|m| m.load(sync::atomic::Ordering::Relaxed) as i64).max();

        // when a node initializes itself, it is going to query all its ancestors at some ts. that
        // ts needs to be >= the latest timestamp absorbed by those ancestors. at this point, the
        // ancestors all know about the new children (with ts = 0), so they won't absorb any more
        // until the children have initialized, so it's safe to initialize at whatever the max
        // absorbed ts was amongst all parents.
        //
        // we need to iterate through the nodes in topological order so that we can set
        // max_absorbed for the new nodes along the way. if we didn't, new nodes that only depend
        // on other new nodes would observe only max_absorbed[..] = 0 and hence would see their
        // init_ts at 0. this is not correct. and furthermore, even if those new nodes *tried* to
        // init at that time, they would fail, because that time would be in the absorbed state.
        let mut topo = petgraph::visit::SubTopo::from_node(&self.graph, self.source);
        while let Some(node) = topo.next(&self.graph) {
            if !new.contains(&node) {
                continue;
            }

            let may_find_at = self.graph
                .neighbors_directed(node, petgraph::EdgeDirection::Incoming)
                .filter(|&n| n != self.source)
                .map(|p| max_absorbed[&p])
                .max()
                .or(latest.clone()); // base nodes are fully up-to-date

            if let Some(ts) = may_find_at {
                max_absorbed.insert(node, ts);
            } else {
                // None would be if there were no previous nodes (and so latest wasn't set).
                // in that case, the correct value for max_absorbed is 0, which is what it'll
                // already be.
            }
        }

        new.iter().map(|&n| (n, max_absorbed.remove(&n).unwrap())).collect()
    }

    /// Execute any changes to the data flow graph since the last call to `run()`.
    ///
    /// `run()` sets up ancestor query functions, finds indices, and builds the state each new node
    /// needs to keep track of in order to function correctly. This includes telling any existing
    /// ancestors of new nodes about their new children.
    ///
    /// Once the necessary bookkeeping has been done, `run()` will spawn two different types of
    /// threads (ignoring `clocked_dispatch`):
    ///
    ///  - For every node, a thread is spawned that runs `FlowGraph::inner()` for that node.
    ///    `inner()` listens for incoming records from all of a node's ancestors, delays them if
    ///    necessary, and then calls the node's `process()` method to process the received updates.
    ///  - For every `Base` node, a thread is spawned that listens for incoming records from the
    ///    `clocked_dispatch` dispatcher, and wraps the record in a tuple with
    ///
    ///     - source node
    ///     - the record as a `U`
    ///     - time stamp
    ///
    ///    This is done so that the `Base` nodes can use the same `.inner` method as the other
    ///    nodes in the graph (i.e., so that they can process the same kind of incoming tuples)
    ///    without requiring the user to insert those tuples.
    ///
    /// It may be instructive to read through `FlowGraph::inner()`, as well as look at what state
    /// is passed to it in `NodeState` to better understand what each node is doing.
    /// `FlowGraph::run()` is likely to be less interesting.
    ///
    /// `run()` will return two maps, one mapping the `NodeIndex` of every `Base` node to a channel
    /// on which new records can be sent, and one mapping the `NodeIndex` of every node to a
    /// function that will query that node when called.
    pub fn run(&mut self,
               buf: usize)
               -> (HashMap<NodeIndex, clocked_dispatch::ClockedSender<D>>,
                   HashMap<NodeIndex, Box<Fn(Option<Q>) -> Vec<D> + 'static + Send + Sync>>) {

        // which nodes are new?
        let new = petgraph::BfsIter::new(&self.graph, self.source)
            .filter(|&n| n != self.source)
            .filter(|n| !self.contexts.contains_key(n))
            .collect::<HashSet<_>>();

        // TODO: transform the graph to aid in view re-use

        // create an entry in the min map for each new node to track how up-to-date it is
        for node in new.iter() {
            self.mins.insert(*node, sync::Arc::new(sync::atomic::AtomicIsize::new(0)));
        }

        // set up in-channels for each base record node
        let mut incoming = HashMap::new();
        let mut start = HashMap::new();
        for base in self.graph.neighbors(self.source) {
            if !new.contains(&base) {
                continue;
            }

            let (tx, rx) = self.dispatch.new(format!("{}-in", base.index()),
                                             format!("{}-out", base.index()));

            // automatically add source node and timestamp to all incoming facts so users don't
            // have to add this themselves.
            let (px_tx, px_rx) = mpsc::sync_channel(buf);
            let root = self.source;
            thread::spawn(move || {
                for (r, ts) in rx.into_iter() {
                    px_tx.send((root, r.map(|r| r.into()), ts as i64)).unwrap();
                }
            });
            incoming.insert(base, tx);
            start.insert(base, px_rx);
        }

        // initialize all new contexts (min_check + bus)
        // and hook them into existing contexts (i.e., connect buses and update min_checks)
        let init_at = self.build_contexts(&mut start, &new, buf);

        // expose queries in a friendly format to outsiders
        let mut qs = HashMap::with_capacity(init_at.len());
        for node in new.iter() {
            let n = self.graph[*node].as_ref().unwrap().clone();
            let func = Box::new(move |q: Option<Q>| -> Vec<D> {
                n.find(q, None).into_iter().map(|(r, _)| r).collect()
            }) as Box<Fn(Option<Q>) -> Vec<D> + 'static + Send + Sync>;

            qs.insert(*node, func);
        }

        // add any new indices
        self.add_indices();

        // spin up all the worker threads
        for (node, init_ts) in init_at.into_iter() {
            let srcs = self.graph
                .edges_directed(node, petgraph::EdgeDirection::Incoming)
                .map(|(ni, _)| ni)
                .collect::<Vec<_>>();

            let state = NodeState {
                name: node,
                inner: self.graph[node].as_ref().unwrap().clone(),
                srcs: srcs,
                input: start.remove(&node).unwrap(),
                context: self.contexts[&node].clone(),
                processed_ts: self.mins[&node].clone(),
                init_ts: init_ts,
            };

            // start a thread for managing this node.
            // basically just a rx->process->tx loop.
            self.wait.push(thread::spawn(move || Self::inner(state)));
        }

        (incoming, qs)
    }

    fn inner(state: NodeState<Q, U, D>) {
        use std::collections::VecDeque;

        let NodeState { name,
                        inner,
                        srcs,
                        input,
                        context,
                        processed_ts,
                        init_ts } = state;

        let mut delayed = BinaryHeap::new();
        let mut freshness: HashMap<_, _> = srcs.into_iter().map(|ni| (ni, 0i64)).collect();
        let mut min = 0i64;
        let mut desc_min: Option<(usize, i64)> = None;

        // if there are queued items that we missed during migration, we want to handle those
        // first. however, we also don't want to completely block the sender either. we'd instead
        // like to apply smooth back pressure to the node above us. we do this by processing two
        // updates from the queue for every one we read from the incoming channel. note that
        // reading from the channel just entails adding to the back of the queue (since we need to
        // process in order).
        let mut missed = VecDeque::new();
        let mut siphon = false;

        if init_ts != 0 {
            let mig_done = sync::Arc::new(sync::atomic::AtomicBool::new(false));

            // spin off the migration in a separate thread so we don't block our upstream
            let inner_cp = inner.clone();
            let mig_done_cp = mig_done.clone();
            let proc_ts = processed_ts.clone();

            let mig = thread::spawn(move || {
                inner_cp.init_at(init_ts);
                proc_ts.store(init_ts as isize, sync::atomic::Ordering::Release);
                mig_done_cp.store(true, sync::atomic::Ordering::SeqCst);
            });

            // we now want to wait for the migration to be done, but also receive anything on our
            // incoming channel. once the migration is done, we want to start processing normally.
            // there is, unfortunately, no neat way to achieve this currently, as Rust doesn't
            // currently have a good select!() mechanism, which would allow us to have the
            // migration thread send us a message on some one-off channel when it was done. and
            // even if it did, clocked-dispatch doesn't currently support it.
            //
            // so, we instead resort to good-ol' polling.
            while !mig_done.load(sync::atomic::Ordering::SeqCst) {
                use std::sync::mpsc;
                match input.try_recv() {
                    Ok(x) => missed.push_back(x),
                    Err(mpsc::TryRecvError::Disconnected) => {
                        // there are no more things
                        // we're just waiting for migration to finish
                        // then we need to process the backlog
                        // and then we can finish
                        // no need to keep trying to receive
                        break;
                    }
                    Err(mpsc::TryRecvError::Empty) => {
                        // wait and try again
                        thread::yield_now();
                    }
                }
            }

            mig.join().unwrap();
        }

        'rx: while let Some((src, u, ts)) = missed.pop_front().or_else(|| input.recv().ok()) {
            assert!(ts >= min);

            // for every two updates we process, we want to receive once from our input, to avoid
            // blocking our upstream completely. we do this by alternating a flag, only reading
            // from our input when the flag is true (and we still have a backlog).
            if !missed.is_empty() {
                if siphon {
                    // we didn't receive last time through, so we'll receive now. there might be
                    // nothing for us, in which case we might as well use the time to keep
                    // processing our backlog instead.
                    if let Ok(x) = input.try_recv() {
                        missed.push_back(x);
                    }
                    siphon = false;
                } else {
                    // we received last time, so to achieve the 2:1 ratio, we shouldn't receive
                    // this time. instead, just set the flag so we'll receive next time.
                    siphon = true;
                }
            }

            if ts == min {
                let u = u.and_then(|u| {
                    inner.process(u, src, ts)
                });
                processed_ts.store(ts as isize, sync::atomic::Ordering::Release);

                for tx in context.lock().unwrap().txs.iter_mut() {
                    tx.send((name, u.clone(), ts)).unwrap();
                }
                continue;
            }

            if let Some(u) = u {
                // this *may* be taken out again immediately if the min is raised to the
                // given ts, but meh, we accept that overhead for the simplicity of the
                // code.
                delayed.push(Delayed {
                    data: (src, u),
                    ts: ts,
                });
            }

            let old_ts = freshness[&src];
            *freshness.get_mut(&src).unwrap() = ts;

            if old_ts != min {
                // min can't have changed, so there's nothing to process yet
                continue;
            }

            let new_min = freshness.values()
                .min()
                .and_then(|m| Some(*m))
                .unwrap_or(i64::max_value() - 1);

            if new_min == min {
                // min didn't change, so no updates have been released
                continue;
            }

            // the min has changed!
            min = new_min;
            // process any delayed updates *in order*

            // keep track of the largest timestamp we've processed a message with.
            // this is so that, if there was no data for the current ts, we'll still
            // remember to forward a None for the latest time.
            let mut forwarded = 0;

            // keep looking for a candidate to send
            loop {
                // find the smallest in `delay`
                let next = delayed.peek().and_then(|d| Some(d.ts)).unwrap_or(min + 1);
                //  process it if it is early enough
                if next <= min {
                    let d = delayed.pop().unwrap();
                    let ts = d.ts;
                    let (src, u) = d.data;

                    let u = inner.process(u, src, ts);
                    processed_ts.store(ts as isize, sync::atomic::Ordering::Release);

                    if u.is_some() {
                        forwarded = ts;
                        for tx in context.lock().unwrap().txs.iter_mut() {
                            tx.send((name, u.clone(), ts)).unwrap();
                        }
                    }
                    continue;
                }

                // no delayed message has a timestamp <= min
                break;
            }

            // make sure all dependents know how up-to-date we are
            // even if we didn't send a delayed message for the min
            if forwarded < min && min != i64::max_value() - 1 {
                processed_ts.store(min as isize, sync::atomic::Ordering::Release);
                for tx in context.lock().unwrap().txs.iter_mut() {
                    tx.send((name, None, min)).unwrap();
                }
            }

            // check if descendant min has changed so we can absorb?
            let mut ctx = context.lock().unwrap();
            let mc = &mut ctx.min_check;
            let mut previous = 0;
            if let Some((n, min)) = desc_min {
                // the min certainly hasn't changed if the previous min is still there
                let new = mc[n].load(sync::atomic::Ordering::Relaxed) as i64;
                if new > min {
                    previous = min;
                    desc_min = None;
                }
            }

            if desc_min.is_none() {
                // we don't know if the current min has changed, so check all descendants
                desc_min = mc.iter()
                    .map(|m| m.load(sync::atomic::Ordering::Relaxed) as i64)
                    .enumerate()
                    .min_by_key(|&(_, m)| m);

                if let Some((_, m)) = desc_min {
                    if m > previous {
                        // min changed -- safe to absorb
                        inner.safe(m - 1);
                    }
                } else {
                    // there are no materialized descendants
                    // TODO: what is the right thing to do here?
                    // for now, we simply always absorb in this case
                    inner.safe(min - 1);
                }
            }
        }
    }

    /// Add the given `View` node into the graph as a child of the given ancestors.
    // TODO
    // we need a better way of ensuring that the Q for each node matches what the node *thinks*
    // that query should be. for example, an aggregation expects to be able to query over all the
    // fields except its input field (self.over), latest expects the query to be on all key fields
    // (and only those fields), in order, etc.
    pub fn incorporate<V: View<Q, Update = U, Data = D>>(&mut self, mut node: V)
        -> petgraph::graph::NodeIndex {

        let ancestors = node.prime(&self.graph);
        let idx = self.graph.add_node(Some(sync::Arc::new(node)));
        if ancestors.is_empty() {
            // base record node
            self.graph.add_edge(self.source, idx, ());
        } else {
            // derived node -- add edges from all ancestor nodes to node
            for ancestor in ancestors.into_iter() {
                self.graph.add_edge(ancestor, idx, ());
            }
        }
        idx
    }
}

impl<Q, U, D> Debug for FlowGraph<Q, U, D>
    where Q: Clone + Debug + Send + Sync,
          U: Clone + Send,
          D: Clone + Send
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let dotgraph = petgraph::dot::Dot::new(&self.graph);
        write!(f, "{:?}", dotgraph)
    }
}

impl<Q, U, D> Drop for FlowGraph<Q, U, D>
    where Q: Clone + Send + Sync,
          U: Clone + Send,
          D: Clone + Send
{
    fn drop(&mut self) {
        // need to clear all contexts so the every bus is closed
        self.contexts.clear();
        for w in self.wait.drain(..) {
            w.join().unwrap();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time;
    use std::sync;
    use std::thread;
    use std::collections::HashMap;

    use petgraph;
    use ops;

    type V = sync::Arc<View<(), Update = u32, Data = u32>>;

    #[derive(Debug)]
    struct Counter(bool, String, sync::Arc<sync::Mutex<u32>>, Vec<NodeIndex>, Vec<V>);

    impl Counter {
        pub fn new(name: &str, parents: Vec<NodeIndex>) -> Counter {
            Counter(parents.is_empty(), name.into(), Default::default(), parents, vec![])
        }
    }

    impl View<()> for Counter {
        type Update = u32;
        type Data = u32;

        fn prime(&mut self, g: &petgraph::Graph<Option<V>, ()>) -> Vec<NodeIndex> {
            self.4.extend(self.3.iter().map(|&i| g[i].as_ref().unwrap().clone()));
            self.3.clone()
        }

        fn find<'a>(&'a self, _: Option<()>, _: Option<i64>) -> Vec<(Self::Data, i64)> {
            if self.0 {
                vec![(*self.2.lock().unwrap(), 0)]
            } else {
                vec![(self.4.iter().map(|n| n.find(None, Some(0))[0].0).sum(), 0)]
            }
        }

        fn process(&self, u: Self::Update, _: NodeIndex, _: i64) -> Option<Self::Update> {
            use std::ops::AddAssign;
            let mut x = self.2.lock().unwrap();
            x.add_assign(u);
            Some(u)
        }

        fn suggest_indexes(&self, _: NodeIndex) -> HashMap<NodeIndex, Vec<usize>> {
            HashMap::new()
        }

        fn init_at(&self, _: i64) {
            if self.0 {
                // base table is already initialized
                return;
            }

            let mut x = self.2.lock().unwrap();
            *x = self.find(None, None)[0].0;
        }

        fn resolve(&self, _: usize) -> Option<Vec<(NodeIndex, usize)>> {
            None
        }

        fn add_index(&self, _: usize) {
            unreachable!();
        }

        fn safe(&self, _: i64) {}

        fn operator(&self) -> Option<&ops::NodeType> {
            None
        }

        fn name(&self) -> &str {
            &*self.1
        }

        fn args(&self) -> &[String] {
            &[]
        }
    }

    #[test]
    fn simple_graph() {
        // set up graph
        let mut g = FlowGraph::new();
        let a = g.incorporate(Counter::new("a", vec![]));
        let (put, get) = g.run(10);

        // send a value
        put[&a].send(1);

        // give it some time to propagate
        thread::sleep(time::Duration::new(0, 10_000_000));

        // send a query
        assert_eq!(get[&a](None), vec![1]);

        // update value again
        put[&a].send(1);

        // give it some time to propagate
        thread::sleep(time::Duration::new(0, 10_000_000));

        // check that value was updated again
        assert_eq!(get[&a](None), vec![2]);
    }

    #[test]
    fn topo_graph1() {
        // This test + topo_graph2 check that the system can correctly initialize graphs regardless
        // of the order the nodes appear in. Specifically, they set up the following two graphs:
        //
        //      A   B       A   B
        //      |   |       |   |
        //      C   |       |   C
        //       \ /         \ /
        //        D           D
        //
        //     graph1       graph2
        //
        // A plain BFS exploration would set up D before C in one of these cases, which would break
        // things since C had not yet been initialized. By building both, we ensure that the code
        // handles this case (probably by using a topological traversal).
        let mut g = FlowGraph::new();
        let a = g.incorporate(Counter::new("a", vec![]));
        let b = g.incorporate(Counter::new("b", vec![]));
        let c = g.incorporate(Counter::new("c", vec![a]));
        let d = g.incorporate(Counter::new("d", vec![b, c]));
        let (put, get) = g.run(10);
        assert!(get.contains_key(&a));
        assert!(get.contains_key(&b));
        assert!(get.contains_key(&c));
        assert!(get.contains_key(&d));
        assert!(put.contains_key(&a));
        assert!(put.contains_key(&b));
    }

    #[test]
    fn topo_graph2() {
        // See topo_graph1.
        let mut g = FlowGraph::new();
        let a = g.incorporate(Counter::new("a", vec![]));
        let b = g.incorporate(Counter::new("b", vec![]));
        let c = g.incorporate(Counter::new("c", vec![b]));
        let d = g.incorporate(Counter::new("d", vec![a, c]));
        let (put, get) = g.run(10);
        assert!(get.contains_key(&a));
        assert!(get.contains_key(&b));
        assert!(get.contains_key(&c));
        assert!(get.contains_key(&d));
        assert!(put.contains_key(&a));
        assert!(put.contains_key(&b));
    }

    #[test]
    fn join_graph() {
        // set up graph
        let mut g = FlowGraph::new();
        let a = g.incorporate(Counter::new("a", vec![]));
        let b = g.incorporate(Counter::new("b", vec![]));
        let c = g.incorporate(Counter::new("c", vec![a, b]));
        let (put, get) = g.run(10);

        // send a value on a
        put[&a].send(1);

        // give it some time to propagate
        thread::sleep(time::Duration::new(0, 10_000_000));

        // send a query to c
        assert_eq!(get[&c](None), vec![1]);

        // update value again
        put[&b].send(1);

        // give it some time to propagate
        thread::sleep(time::Duration::new(0, 10_000_000));

        // check that value was updated again
        assert_eq!(get[&c](None), vec![2]);
    }

    #[test]
    fn join_and_forward() {
        // set up graph
        let mut g = FlowGraph::new();
        let a = g.incorporate(Counter::new("a", vec![]));
        let b = g.incorporate(Counter::new("b", vec![]));
        let c = g.incorporate(Counter::new("c", vec![a, b]));
        let d = g.incorporate(Counter::new("d", vec![c]));
        let (put, get) = g.run(10);

        // send a value on a
        put[&a].send(1);

        // give it some time to propagate
        thread::sleep(time::Duration::new(0, 10_000_000));

        // send a query to d
        assert_eq!(get[&d](None), vec![1]);

        // update value again
        put[&b].send(1);

        // give it some time to propagate
        thread::sleep(time::Duration::new(0, 10_000_000));

        // check that value was updated again
        assert_eq!(get[&d](None), vec![2]);
    }

    #[test]
    fn disjoint_migration() {
        // set up graph
        let mut g = FlowGraph::new();
        let _ = g.incorporate(Counter::new("x", vec![]));
        let (_p, _g) = g.run(10);

        let a = g.incorporate(Counter::new("a", vec![]));
        let b = g.incorporate(Counter::new("b", vec![]));
        let c = g.incorporate(Counter::new("c", vec![a, b]));
        let d = g.incorporate(Counter::new("d", vec![c]));
        let (put, get) = g.run(10);

        // send a value on a
        put[&a].send(1);

        // give it some time to propagate
        thread::sleep(time::Duration::new(0, 10_000_000));

        // send a query to d
        assert_eq!(get[&d](None), vec![1]);

        // update value again
        put[&b].send(2);

        // give it some time to propagate
        thread::sleep(time::Duration::new(0, 10_000_000));

        // check that value was updated again
        assert_eq!(get[&d](None), vec![3]);
    }

    #[test]
    fn overlap_migration() {
        // set up graph
        let mut g = FlowGraph::new();
        let a = g.incorporate(Counter::new("a", vec![]));
        let x = g.incorporate(Counter::new("x", vec![a]));
        let (put_1, get_1) = g.run(10);

        let b = g.incorporate(Counter::new("b", vec![]));
        let c = g.incorporate(Counter::new("c", vec![a, b]));
        let d = g.incorporate(Counter::new("d", vec![c]));
        let (put, get) = g.run(10);

        // send a value on a
        put_1[&a].send(1);

        // give it some time to propagate
        thread::sleep(time::Duration::new(0, 10_000_000));

        // see that result appeared at d
        assert_eq!(get[&d](None), vec![1]);

        // and at x
        assert_eq!(get_1[&x](None), vec![1]);

        // update value again
        put[&b].send(1);

        // give it some time to propagate
        thread::sleep(time::Duration::new(0, 10_000_000));

        // check that value was updated again
        assert_eq!(get[&d](None), vec![2]);
    }

    #[test]
    fn migration_initialization() {
        // set up graph
        let mut g = FlowGraph::new();
        let a = g.incorporate(Counter::new("a", vec![]));
        let x = g.incorporate(Counter::new("x", vec![a]));
        let (put_1, get_1) = g.run(10);

        // send a value on a
        put_1[&a].send(1);

        // give it some time to propagate
        thread::sleep(time::Duration::new(0, 10_000_000));

        // see that result appeared at x
        assert_eq!(get_1[&x](None), vec![1]);

        // perform migration
        let b = g.incorporate(Counter::new("b", vec![]));
        let c = g.incorporate(Counter::new("c", vec![a, b]));
        let d = g.incorporate(Counter::new("d", vec![c]));
        let (put, get) = g.run(10);

        // give it some time to initialize
        thread::sleep(time::Duration::new(0, 10_000_000));

        // check that new views see old data
        assert_eq!(get[&d](None), vec![1]);

        // update value again
        put[&b].send(1);

        // give it some time to propagate
        thread::sleep(time::Duration::new(0, 10_000_000));

        // check that value was updated again
        assert_eq!(get[&d](None), vec![2]);
    }
}
