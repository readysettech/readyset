pub mod sql_to_flow;
mod sql;

use petgraph;
use clocked_dispatch;
use regex::Regex;

use std::fmt;
use std::fmt::{Debug, Display};
use std::sync::mpsc;
use std::sync;
use std::thread;
use std::cmp::Ordering;

use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::BinaryHeap;

pub use petgraph::graph::NodeIndex;

use ops;

type Graph<Q, U, D> = petgraph::Graph<Option<sync::Arc<View<Q, Update = U, Data = D>>>, ()>;

/// A `ProcessingResult` is used to indicate the result of processing a single update at a node.
pub enum ProcessingResult<U: Clone + Send> {
    /// The update was accepted, and the node is done with the current timestamp. No more updates
    /// should be sent to this node with the timestamp given, and the wrapped update should be
    /// emitted to all children.
    Done(U),

    /// The update was accepted, but the node needs more updates for the current timestamp before
    /// it is willing to produce an update.
    Accepted,

    /// The node does not wish to perform additional computation at this timestamp, and no update
    /// should be sent to the node's children. This should translate into sending a single None to
    /// every child to inform then that no update was emitted for this timestamp from this node.
    Skip,
}

impl<U: Clone + Send> ProcessingResult<U> {
    pub fn is_done(&self) -> bool {
        match *self {
            ProcessingResult::Done(_) |
            ProcessingResult::Skip => true,
            _ => false,
        }
    }

    #[cfg(test)]
    pub fn unwrap(self) -> U {
        match self {
            ProcessingResult::Done(u) => u,
            _ => unreachable!(),
        }
    }
}

impl<U: Clone + Send> From<Option<U>> for ProcessingResult<U> {
    fn from(o: Option<U>) -> Self {
        if let Some(u) = o {
            ProcessingResult::Done(u)
        } else {
            ProcessingResult::Skip
        }
    }
}

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
    fn prime(&mut self, &Graph<Q, Self::Update, Self::Data>) -> Vec<NodeIndex>;

    /// Execute a single query, producing all matching records.
    fn find(&self, Option<&Q>, Option<i64>) -> Vec<(Self::Data, i64)>;

    /// Process a single update from an ancestor node.
    ///
    /// The update may be None if the ancestor did not send any update for this timestamp. In the
    /// common case, `process` will not even be called in this instance. However, if `process`
    /// returns `ProcessingResult::Accepted` for all previous updates, and the last ancestor sends
    /// a `None`, `process` is called with a `None`.
    ///
    /// The node is passed the update, the update's source (i.e., which ancestor it arrived from),
    /// the current timestamp, and a boolean indicating whether this is the last update for the
    /// given timestamp.
    ///
    /// If a node does not return `ProcessingResult::Done` or `ProcessingResult::Skip` when the
    /// boolean argument is true (i.e., when there are no more updates for this timestamp), the
    /// node will panic, and terminate.
    fn process(&self,
               Option<Self::Update>,
               NodeIndex,
               i64,
               bool)
               -> ProcessingResult<Self::Update>;

    /// Suggest fields of this view, or its ancestors, that would benefit from having an index.
    /// The passed node index is the index of the current node.
    fn suggest_indexes(&self, NodeIndex) -> HashMap<NodeIndex, Vec<usize>>;

    /// Resolve where the given field originates from. If the view is materialized, or the value is
    /// otherwise created by this view, None should be returned.
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

    /// Returns the underlying node for this view (if any).
    /// This is a bit of a hack, but the only way to introspect on views for the purpose of graph
    /// transformations.
    fn node(&self) -> Option<&ops::Node>;

    /// Returns the name of this view.
    fn name(&self) -> &str;

    /// Returns the arguments to this view.
    fn args(&self) -> &[String];
}

/// A `FreshnessProbe` allows external observers to query the freshness of a particular graph node.
pub struct FreshnessProbe(sync::Arc<sync::atomic::AtomicIsize>);

impl FreshnessProbe {
    /// Gives a lower bound on the write timestamps processed by this node.
    ///
    /// Subsequent queries to this node are guaranteed to results that include *at least* any write
    /// with a timestamp lower or equal to the returned value.
    pub fn lower_bound(&self) -> i64 {
        self.0.load(sync::atomic::Ordering::Acquire) as i64
    }
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
type SharedContext<T> = sync::Arc<sync::Mutex<Context<T>>>;

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
///  - `Q`: The type used for queries that are issued across the edges of the graph in
///         `View::find()`.
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
///     new("vc", &["id", "votes"], true, Aggregation::COUNT.over(vote, 0, &[1]))
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
/// #     new("vc", &["id", "votes"], true, Aggregation::COUNT.over(vote, 0, &[1]))
/// # );
/// // start the data flow graph
/// let (put, get) = g.run(10);
///
/// // put can now be used to insert votes
/// put[&vote](vec![1.into(), 1.into()]);
/// put[&vote](vec![2.into(), 1.into()]);
/// put[&vote](vec![3.into(), 1.into()]);
/// put[&vote](vec![1.into(), 2.into()]);
/// put[&vote](vec![2.into(), 2.into()]);
///
/// // allow them to propagate before querying
/// sleep(Duration::from_millis(100));
///
/// // get can be used to query votecount
/// assert_eq!(
///     {
///         use shortcut::{Condition,Comparison,Value};
///         get[&votecount](Some(&Query::new(
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
pub struct FlowGraph<Q, U, D>
    where Q: Clone + Send + Sync,
          U: Clone + Send,
          D: Clone + Send
{
    graph: Graph<Q, U, D>,
    source: petgraph::graph::NodeIndex,
    mins: HashMap<petgraph::graph::NodeIndex, sync::Arc<sync::atomic::AtomicIsize>>,
    wait: Vec<thread::JoinHandle<()>>,
    dispatch: clocked_dispatch::Dispatcher<U>,

    contexts: HashMap<petgraph::graph::NodeIndex, SharedContext<U>>,
    ts_src: sync::Arc<sync::atomic::AtomicUsize>,
    named: HashMap<String, petgraph::graph::NodeIndex>,
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
            ts_src: sync::Arc::new(sync::atomic::AtomicUsize::new(1)),
            named: HashMap::default(),
        }
    }

    /// Return a reference to the internal graph, as well as the identifier for the root node.
    pub fn graph(&self) -> (&Graph<Q, U, D>, NodeIndex) {
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

                    for col in cols {
                        let really = node.resolve(col);
                        if let Some(really) = really {
                            // this view is not materialized. the index should instead be placed on
                            // the corresponding columns of this view's inputs
                            for (v, col) in really {
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
        for (v, cols) in indices {
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
                      start: &mut HashMap<NodeIndex,
                                          sync::mpsc::Receiver<(NodeIndex, Option<U>, i64)>>,
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
                .or(latest); // base nodes are fully up-to-date

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
    /// `run()` finds indices, and builds the state each new node needs to keep track of in order
    /// to function correctly. This includes telling any existing ancestors of new nodes about
    /// their new children.
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
               -> (HashMap<NodeIndex, Box<Fn(D) -> i64 + 'static + Send>>,
                   HashMap<NodeIndex, Box<Fn(Option<&Q>) -> Vec<D> + 'static + Send + Sync>>) {

        // which nodes are new?
        let new = petgraph::BfsIter::new(&self.graph, self.source)
            .filter(|&n| n != self.source)
            .filter(|n| !self.contexts.contains_key(n))
            .collect::<HashSet<_>>();

        // TODO: transform the graph to aid in view re-use

        // create an entry in the min map for each new node to track how up-to-date it is
        for node in &new {
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

            let root = self.source;
            let ts_src = self.ts_src.clone();
            incoming.insert(base,
                            Box::new(move |data: D| {
                // the user wants to do a put
                // first, let's pick a timestamp
                let ts = ts_src.fetch_add(1, sync::atomic::Ordering::AcqRel);
                // next, let's make their data an update
                let u: U = data.into();
                // and send it into the dispatcher
                tx.forward(Some(u), ts);
                ts as i64
            }) as Box<Fn(D) -> i64 + 'static + Send>);

            // automatically add source node to all incoming facts
            // TODO: get rid of this extra thread per base node
            let (px_tx, px_rx) = mpsc::sync_channel(buf);
            thread::spawn(move || {
                for (u, ts) in rx {
                    px_tx.send((root, u, ts as i64)).unwrap();
                }
            });
            start.insert(base, px_rx);
        }

        // initialize all new contexts (min_check + bus)
        // and hook them into existing contexts (i.e., connect buses and update min_checks)
        let init_at = self.build_contexts(&mut start, &new, buf);

        // expose queries in a friendly format to outsiders
        let mut qs = HashMap::with_capacity(init_at.len());
        for node in &new {
            let n = self.graph[*node].as_ref().unwrap().clone();
            let func = Box::new(move |q: Option<&Q>| -> Vec<D> {
                n.find(q, None).into_iter().map(|(r, _)| r).collect()
            }) as Box<Fn(Option<&Q>) -> Vec<D> + 'static + Send + Sync>;

            qs.insert(*node, func);
        }

        // add any new indices
        self.add_indices();

        // spin up all the worker threads
        for (node, init_ts) in init_at {
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

    /// Return a probe that can be used to determine a bound on the freshness of subsequent reads
    /// to the given node.
    pub fn freshness(&self, node: NodeIndex) -> Option<FreshnessProbe> {
        self.mins.get(&node).map(|m| FreshnessProbe(m.clone()))
    }

    fn inner(state: NodeState<Q, U, D>) {
        use std::collections::VecDeque;

        let NodeState { name, inner, srcs, input, context, processed_ts, init_ts } = state;

        let nsrcs = srcs.len();
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

        while let Some((src, u, ts)) = missed.pop_front().or_else(|| input.recv().ok()) {
            // we have already received an update from all our parents for min. this guarantees
            // that we should never see any ts with a *lower* timestamp. since no single node
            // should ever emit multiple updates with the same timestamp, we should also never see
            // any ts with a *duplicate* timestamp. Hence any update we receive must be *later*
            // than our min ts.
            assert!(ts > min);

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

            // this *may* be taken out again immediately if the min is raised to the
            // given ts, but meh, we accept that overhead for the simplicity of the
            // code (for now).
            if let Some(u) = u {
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

            // hold on to the context, as we're going to interact with our children for a while
            let mut ctx = context.lock().unwrap();

            {
                let mut term = |(src, u, uts), last| -> bool {
                    // do the processing dicated by our inner
                    let u = inner.process(u, src, uts, last);
                    if !u.is_done() {
                        if last {
                            unreachable!("last update for a given timestamp must end computation");
                        }
                        return false;
                    }

                    // we're done with this time -- let the world know!
                    processed_ts.store(uts as isize, sync::atomic::Ordering::Release);

                    // we only forward Some() things for efficiency. sending None's is unnecessary
                    // as long as an update will be sent later with a later timestamp. we use
                    // `forwarded` below to figure out whether we need to also send a final None.
                    if let ProcessingResult::Done(u) = u {
                        let mut u = Some(u);
                        forwarded = uts;

                        // avoid cloning on the last send. this will avoid clone altogether for
                        // nodes with only one child, which is a common case.
                        let mut txit = ctx.txs.iter_mut().peekable();
                        while let Some(tx) = txit.next() {
                            if txit.peek().is_some() {
                                tx.send((name, u.clone(), uts)).unwrap();
                            } else {
                                tx.send((name, u.take(), uts)).unwrap();
                            }
                        }
                    }

                    return true;
                };

                // keep looking for a candidate to send
                let mut prev_ts = -1;
                let mut nrcv = nsrcs;
                let mut done = true;
                let mut lastsrc = *freshness.keys().next().unwrap();
                loop {
                    // find the smallest in `delay`
                    let ts = delayed.peek().and_then(|d| Some(d.ts)).unwrap_or(min + 1);

                    // stop if no delayed message has a timestamp <= min
                    if ts > min {
                        break;
                    }

                    // otherwise, deal with the next delayed update
                    let d = delayed.pop().unwrap();

                    // keep track of how many parents we've received from
                    if ts == prev_ts {
                        nrcv += 1;
                    } else {
                        // we've moved to the next timestamp. if we didn't finish the previous
                        // timestamp, and didn't run it with last == true, then do so now.
                        if !done {
                            term((lastsrc, None, ts), true);
                        }
                        // start the next timestamp
                        done = false;
                        nrcv = 1;
                    }
                    prev_ts = ts;

                    // we're already done with this timestamp
                    if done {
                        continue;
                    }

                    // track the sender, in case we have to retroactively mark it as the last sender
                    lastsrc = d.data.0;

                    // process the update
                    done = term((d.data.0, Some(d.data.1), ts), nrcv == nsrcs);
                }

                // we could have received only updates from a few ancestors for the last timestamp,
                // and thus not have called term with last == true. if that were the case, call it
                // one last time now.
                if !done {
                    term((lastsrc, None, prev_ts), true);
                }
            }

            // make sure all dependents know how up-to-date we are
            // even if we didn't send a delayed message for the min
            if forwarded < min && min != i64::max_value() - 1 {
                processed_ts.store(min as isize, sync::atomic::Ordering::Release);
                for tx in &mut ctx.txs {
                    tx.send((name, None, min)).unwrap();
                }
            }

            // check if descendant min has changed so we can absorb?
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

                // TODO: lazy absorbption will speed up things quite significantly
                if let Some((_, m)) = desc_min {
                    if m > previous {
                        // min changed. since every node only ever processes updates that are
                        // *newer* than their stored min, it is safe for us to absorb up to our
                        // current min.
                        inner.safe(m);
                    }
                } else {
                    // there are no materialized descendants
                    // TODO: what is the right thing to do here?
                    // for now, we simply always absorb in this case
                    inner.safe(min);
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
    pub fn incorporate<V: View<Q, Update = U, Data = D>>(&mut self,
                                                         mut node: V)
                                                         -> petgraph::graph::NodeIndex {
        let ancestors = node.prime(&self.graph);
        let name = String::from(node.name());
        let idx = self.graph.add_node(Some(sync::Arc::new(node)));
        self.named.insert(String::from(name), idx);
        if ancestors.is_empty() {
            // base record node
            self.graph.add_edge(self.source, idx, ());
        } else {
            // derived node -- add edges from all ancestor nodes to node
            for ancestor in ancestors {
                self.graph.add_edge(ancestor, idx, ());
            }
        }
        idx
    }
}

impl<Q, U, D> Display for FlowGraph<Q, U, D>
    where Q: 'static + Clone + Send + Sync,
          U: 'static + Clone + Send,
          D: 'static + Clone + Send
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use ops::NodeOp;

        let indentln = |f: &mut fmt::Formatter| write!(f, "    ");
        let escape = |s: &str| Regex::new("([\"|{}])").unwrap().replace_all(s, "\\$1");

        // Output header.
        writeln!(f, "digraph {{")?;

        // Output global formatting.
        indentln(f)?;
        writeln!(f, "node [shape=record, fontsize=10]")?;

        // Output node descriptions.
        for index in self.graph.node_indices() {
            indentln(f)?;
            write!(f, "{}", index.index())?;
            write!(f, " [label=\"")?;
            match self.graph[index].as_ref() {
                None => write!(f, "(source)")?,
                Some(n) => {
                    write!(f, "{{")?;

                    // Output node name and description. First row.
                    write!(f,
                           "{{ {} / {} | {} }}",
                           index.index(),
                           escape(n.name()),
                           escape(&n.node().unwrap().operator().description()))?;

                    // Output node outputs. Second row.
                    write!(f, " | {}", n.args().join(",\\n "))?;

                    // Maybe output node's HAVING conditions. Optional third row.
                    if let Some(conds) = n.node().unwrap().having_conditions() {
                        let conds = conds.iter()
                            .map(|c| format!("{}", c))
                            .collect::<Vec<_>>()
                            .join(" ∧ ");
                        write!(f, " | σ({})", escape(&conds))?;
                    }

                    write!(f, " }}")?;
                }
            }
            writeln!(f, "\"]")?;
        }

        // Output edges.
        for (_, edge) in self.graph.raw_edges().iter().enumerate() {
            indentln(f)?;
            writeln!(f, "{} -> {}", edge.source().index(), edge.target().index())?;
        }

        // Output footer.
        write!(f, "}}")?;

        Ok(())
    }
}

impl<Q, U, D> Debug for FlowGraph<Q, U, D>
    where Q: Clone + Debug + Send + Sync,
          U: Clone + Send,
          D: Clone + Send
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use petgraph::dot::{Dot, Config};
        write!(f,
               "{:?}",
               Dot::with_config(&self.graph, &[Config::EdgeNoLabel]))
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
            Counter(parents.is_empty(),
                    name.into(),
                    Default::default(),
                    parents,
                    vec![])
        }
    }

    impl View<()> for Counter {
        type Update = u32;
        type Data = u32;

        fn prime(&mut self, g: &petgraph::Graph<Option<V>, ()>) -> Vec<NodeIndex> {
            self.4.extend(self.3.iter().map(|&i| g[i].as_ref().unwrap().clone()));
            self.3.clone()
        }

        fn find<'a>(&'a self, _: Option<&()>, _: Option<i64>) -> Vec<(Self::Data, i64)> {
            if self.0 {
                vec![(*self.2.lock().unwrap(), 0)]
            } else {
                vec![(self.4.iter().map(|n| n.find(None, Some(0))[0].0).sum(), 0)]
            }
        }

        fn process(&self,
                   u: Option<Self::Update>,
                   _: NodeIndex,
                   _: i64,
                   _: bool)
                   -> ProcessingResult<Self::Update> {
            use std::ops::AddAssign;
            if let Some(u) = u {
                let mut x = self.2.lock().unwrap();
                x.add_assign(u);
                ProcessingResult::Done(u)
            } else {
                ProcessingResult::Skip
            }
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

        fn node(&self) -> Option<&ops::Node> {
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
        put[&a](1);

        // give it some time to propagate
        thread::sleep(time::Duration::new(0, 10_000_000));

        // send a query
        assert_eq!(get[&a](None), vec![1]);

        // update value again
        put[&a](1);

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
        put[&a](1);

        // give it some time to propagate
        thread::sleep(time::Duration::new(0, 10_000_000));

        // send a query to c
        assert_eq!(get[&c](None), vec![1]);

        // update value again
        put[&b](1);

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
        put[&a](1);

        // give it some time to propagate
        thread::sleep(time::Duration::new(0, 10_000_000));

        // send a query to d
        assert_eq!(get[&d](None), vec![1]);

        // update value again
        put[&b](1);

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
        put[&a](1);

        // give it some time to propagate
        thread::sleep(time::Duration::new(0, 10_000_000));

        // send a query to d
        assert_eq!(get[&d](None), vec![1]);

        // update value again
        put[&b](2);

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
        put_1[&a](1);

        // give it some time to propagate
        thread::sleep(time::Duration::new(0, 10_000_000));

        // see that result appeared at d
        assert_eq!(get[&d](None), vec![1]);

        // and at x
        assert_eq!(get_1[&x](None), vec![1]);

        // update value again
        put[&b](1);

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
        put_1[&a](1);

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
        put[&b](1);

        // give it some time to propagate
        thread::sleep(time::Duration::new(0, 10_000_000));

        // check that value was updated again
        assert_eq!(get[&d](None), vec![2]);
    }

    #[test]
    fn freshness() {
        use ops::union::Union;
        use ops::grouped::aggregate::Aggregation;
        use ops::base::Base;
        use ops::gatedid::GatedIdentity;

        let mut g = FlowGraph::new();
        let cns = &["x", "y"];

        let a = g.incorporate(ops::new("a", cns, true, Base {}));
        let (ag, atx) = GatedIdentity::new(a);
        let ag = g.incorporate(ops::new("ag", cns, false, ag));

        let b = g.incorporate(ops::new("a", cns, true, Base {}));
        let (bg, btx) = GatedIdentity::new(b);
        let bg = g.incorporate(ops::new("bg", cns, false, bg));

        let c = g.incorporate(ops::new("c",
                                       &["y", "count"],
                                       true,
                                       Aggregation::COUNT.over(ag, 0, &[1])));
        let (cg, ctx) = GatedIdentity::new(c);
        let cg = g.incorporate(ops::new("cg", &["y", "count"], false, cg));

        let mut emits = HashMap::new();
        emits.insert(cg, vec![0, 1]);
        emits.insert(bg, vec![0, 1]);
        let u = Union::new(emits);
        let d = g.incorporate(ops::new("d", &["a", "b"], false, u));
        let (put, _) = g.run(10);

        // Wait until node ni reaches time stamp ts.
        let wait = |ref g: &FlowGraph<_, _, _>, ni: NodeIndex, ts: i64| {
            let mut f: i64 = 0;
            let nf = g.freshness(ni).unwrap();
            while f < ts {
                let nf = nf.lower_bound();
                assert!(nf <= ts);
                assert!(f <= nf);
                f = nf;
            }
        };

        // prepare freshness probes
        let af = g.freshness(a).unwrap();
        let bf = g.freshness(b).unwrap();
        let cf = g.freshness(c).unwrap();
        let df = g.freshness(d).unwrap();

        // check initial state
        assert_eq!(af.lower_bound(), 0);
        assert_eq!(bf.lower_bound(), 0);
        assert_eq!(cf.lower_bound(), 0);
        assert_eq!(df.lower_bound(), 0);

        // Send update to a.
        put[&a](vec![1.into(), 2.into()]);
        wait(&g, a, 1);
        wait(&g, b, 1);
        thread::sleep(time::Duration::from_millis(50));
        assert_eq!(cf.lower_bound(), 0);
        assert_eq!(df.lower_bound(), 0);

        atx.send(()).unwrap();
        wait(&g, c, 1);
        thread::sleep(time::Duration::from_millis(50));
        assert_eq!(df.lower_bound(), 0);

        ctx.send(()).unwrap();
        wait(&g, d, 1);

        // Send update to b.
        put[&b](vec![1.into(), 2.into()]);
        wait(&g, b, 2);
        wait(&g, c, 2);
        assert_eq!(af.lower_bound(), 2);
        thread::sleep(time::Duration::from_millis(50));
        assert_eq!(df.lower_bound(), 1);
        btx.send(()).unwrap();
        wait(&g, d, 2);
    }
}
