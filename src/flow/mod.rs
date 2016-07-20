use petgraph;
use bus::Bus;
use clocked_dispatch;
use parking_lot;

use std::sync::mpsc;
use std::sync;
use std::thread;
use std::cmp::Ordering;

use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::BinaryHeap;

pub use petgraph::graph::NodeIndex;

// TODO: add an "uninstantiated query" type

pub trait View<Q: Clone + Send> {
    type Update: Clone + Send;
    type Data: Clone + Send;
    type Params: Send;

    /// Execute a single concrete query, producing an iterator over all matching records.
    fn find<'a>(&'a self,
                &HashMap<NodeIndex, Box<Fn(Self::Params, i64) -> Vec<Self::Data> + Send + Sync>>,
                Option<Q>,
                Option<i64>)
                -> Vec<Self::Data>;

    /// Process a new update. This may optionally produce a new update to propagate to child nodes
    /// in the data flow graph.
    fn process(&self,
               Self::Update,
               NodeIndex,
               i64,
               &HashMap<NodeIndex, Box<Fn(Self::Params, i64) -> Vec<Self::Data> + Send + Sync>>)
               -> Option<Self::Update>;

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
}

pub trait FillableQuery {
    type Params;
    fn fill(&mut self, Self::Params);
}

/// Holds flow graph node state that may change as new nodes are added to the graph.
struct Context<T: Clone + Send> {
    bus: Bus<(Option<T>, i64)>,

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
type SharedContext<T: Clone + Send> = sync::Arc<parking_lot::Mutex<Context<T>>>;

struct NodeState<Q: Clone + Send + Sync, U: Clone + Send, D: Clone + Send, P: Send> {
    name: NodeIndex,
    inner: sync::Arc<View<Q, Update = U, Data = D, Params = P> + Send + Sync>,
    srcs: Vec<NodeIndex>,
    input: mpsc::Receiver<(NodeIndex, Option<U>, i64)>,
    aqfs: sync::Arc<HashMap<NodeIndex, Box<Fn(P, i64) -> Vec<D> + Send + Sync>>>,
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

pub struct FlowGraph<Q: Clone + Send + Sync, U: Clone + Send, D: Clone + Send, P: Send> {
    graph: petgraph::Graph<Option<sync::Arc<View<Q, Update=U, Data=D, Params=P> + 'static + Send + Sync>>,
                           Option<sync::Arc<Q>>>,
    source: petgraph::graph::NodeIndex,
    mins: HashMap<petgraph::graph::NodeIndex, sync::Arc<sync::atomic::AtomicIsize>>,
    wait: Vec<thread::JoinHandle<()>>,
    dispatch: clocked_dispatch::Dispatcher<U>,

    contexts: HashMap<petgraph::graph::NodeIndex, SharedContext<U>>,
}

impl<Q, U, D, P> FlowGraph<Q, U, D, P>
    where Q: 'static + FillableQuery<Params = P> + Clone + Send + Sync,
          U: 'static + Clone + Send,
          D: 'static + Clone + Send,
          P: 'static + Send
{
    pub fn new() -> FlowGraph<Q, U, D, P> {
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

    fn build_contexts(&mut self,
                      start: &mut HashMap<NodeIndex, mpsc::Receiver<(NodeIndex, Option<U>, i64)>>,
                      new: &HashSet<NodeIndex>,
                      buf: usize)
                      -> HashMap<NodeIndex, i64> {
        use std::collections::hash_map::Entry;

        // allocate contexts for all new nodes
        for node in new.iter() {
            if let Entry::Vacant(v) = self.contexts.entry(*node) {
                v.insert(sync::Arc::new(parking_lot::Mutex::new(Context {
                    // create a bus for the outgoing records from this node.
                    // size buf/2 since the sync_channels consuming from the bus are also buffered.
                    bus: Bus::new(buf / 2),
                    min_check: Vec::new(),
                })));
            }
        }

        // set up input multiplexer for each new node
        let mut new_ins = HashMap::with_capacity(new.len());
        for node in new.iter() {
            if !start.contains_key(&node) {
                // this node should receive updates from all its ancestors
                let (tx, rx) = mpsc::sync_channel(buf / 2);
                new_ins.insert(*node, tx);
                start.insert(*node, rx);
            }
        }

        // there are two things we need to do in order to set up all the contexts correctly (this
        // includes setting up new ones as well as updating old ones). first, we need to ensure
        // that every node's min_check contains all of its immediate children. second, we need to
        // add every node to its parents' output buses. note that we update the nodes in BFS order.
        // this is necessary because the locks need to be taken such that an upstream node isn't
        // holding the lock while trying to send to a downstream node that we have already locked.
        let mut max_absorbed = HashMap::new();
        for node in petgraph::BfsIter::new(&self.graph, self.source) {
            if node == self.source {
                continue;
            }

            let mut ctx = self.contexts[&node].lock();

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
                let tx = new_ins[&new_child].clone();
                let rx = ctx.bus.add_rx();

                // since Rust doesn't currently support select very well, we need one thread
                // per incoming edge, all sharing a single mpsc channel into the node in
                // question.
                thread::spawn(move || {
                    for (u, ts) in rx.into_iter() {
                        if let Err(..) = tx.send((node, u, ts)) {
                            break;
                        }
                    }
                });
            }
        }

        // when a node initializes itself, it is going to query all its ancestors at some ts. that
        // ts needs to be >= the latest timestamp absorbed by those ancestors. at this point, the
        // ancestors all know about the new children (with ts = 0), so they won't absorb any more
        // until the children have initialized, so it's safe to initialize at whatever the max
        // absorbed ts was amongst all parents.
        new.iter()
            .map(|&n| {
                self.graph
                    .neighbors_directed(n, petgraph::EdgeDirection::Incoming)
                    .filter(|&n| n != self.source)
                    .map(|p| max_absorbed[&p])
                    .max()
                    .map(move |init_ts| (n, init_ts))
                    .unwrap_or((n, 0)) // base nodes can init at any time
            })
            .collect()
    }

    /// Builds the ancestor query function for all nodes.
    ///
    /// In order to query a node, we need to know how to query all its ancestors. specifically, we
    /// need to combine the query along the edges to all ancestors witht he query function on those
    /// ancestors. for example, if we have a --[q1]--> b --[q2]--> c, and c wants to query from be,
    /// the arguments to b.query should be q2, along with a function that lets b query from a. that
    /// function should call a.query with q1, and a way for a to query its ancestors. this
    /// continues all the way back to the base nodes whose ancestor query function list is emtpy.
    fn make_aqfs(&self)
                 -> HashMap<NodeIndex,
                            sync::Arc<HashMap<NodeIndex,
                                              Box<Fn(P, i64) -> Vec<D> + 'static + Send + Sync>>>> {
        // TODO: technically we could re-use aqfs for "old" nodes
        let mut aqfs = HashMap::new();
        for node in petgraph::BfsIter::new(&self.graph, self.source) {
            if node == self.source {
                continue;
            }

            if self.graph.neighbors_directed(node, petgraph::EdgeDirection::Incoming).next() ==
               Some(self.source) {
                // we're a base node, so we can be queried without any ancestor query functions
                aqfs.insert(node, sync::Arc::new(HashMap::new()));
                continue;
            }

            // since we're doing a bfs, the ancestor queries for all our ancestors are already in
            // aqfs. the arguments we need are the queries stored in edges to us executed using the
            // .query for each corresponding ancestor.
            let aqf = self.graph
                .edges_directed(node, petgraph::EdgeDirection::Incoming)
                .map(|(ni, e)| {
                    // get the query for this ancestor
                    let q = e.as_ref().unwrap().clone();
                    // find the ancestor's node
                    let a = self.graph[ni].as_ref().unwrap().clone();
                    // and its min value
                    let m = self.mins[&ni].clone();
                    // find the ancestor query functions for the ancestor's .query
                    let aqf = aqfs[&ni].clone();
                    // execute the ancestor's .query using the query that connects it to us
                    let f = Box::new(move |p: P, ts: i64| -> Vec<D> {
                        let mut q_cur = (*q).clone();
                        q_cur.fill(p);
                        if ts != i64::max_value() {
                            while ts > m.load(sync::atomic::Ordering::Acquire) as i64 {
                                thread::yield_now();
                            }
                        }
                        a.find(&aqf, Some(q_cur), Some(ts))
                    }) as Box<Fn(P, i64) -> Vec<D> + 'static + Send + Sync>;
                    (ni, f)
                })
                .collect();

            aqfs.insert(node, sync::Arc::new(aqf));
        }
        aqfs
    }

    pub fn run(&mut self,
               buf: usize)
               -> (HashMap<NodeIndex, clocked_dispatch::ClockedSender<U>>,
                   HashMap<NodeIndex, Box<Fn(Option<Q>) -> Vec<D> + 'static + Send + Sync>>) {

        // which nodes are new?
        let new = petgraph::BfsIter::new(&self.graph, self.source)
            .filter(|&n| n != self.source)
            .filter(|n| !self.contexts.contains_key(n))
            .collect::<HashSet<_>>();

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
                for (u, ts) in rx.into_iter() {
                    px_tx.send((root, u, ts as i64)).unwrap();
                }
            });
            incoming.insert(base, tx);
            start.insert(base, px_rx);
        }

        // initialize all new contexts (min_check + bus)
        // and hook them into existing contexts (i.e., connect buses and update min_checks)
        let init_at = self.build_contexts(&mut start, &new, buf);

        // set up query functions
        let mut aqfs = self.make_aqfs();

        // expose queries in a friendly format to outsiders
        let mut qs = HashMap::with_capacity(aqfs.len());
        for node in new.iter() {
            let aqf = aqfs[node].clone();
            let aqf = aqf.clone();
            let n = self.graph[*node].as_ref().unwrap().clone();
            let func = Box::new(move |q: Option<Q>| -> Vec<D> {
                n.find(&aqf, q, None)
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
                aqfs: aqfs.remove(&node).unwrap(),
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


    fn inner(state: NodeState<Q, U, D, P>) {
        let NodeState { name, inner, srcs, input, aqfs, context, processed_ts, init_ts } = state;

        let mut delayed = BinaryHeap::new();
        let mut freshness: HashMap<_, _> = srcs.into_iter().map(|ni| (ni, 0i64)).collect();
        let mut min = 0i64;
        let mut desc_min: Option<(usize, i64)> = None;

        // TODO
        assert_eq!(init_ts, 0);

        for (src, u, ts) in input.into_iter() {
            assert!(ts >= min);

            if ts == min {
                let u = u.and_then(|u| inner.process(u, src, ts, &aqfs));
                processed_ts.store(ts as isize, sync::atomic::Ordering::Release);

                context.lock().bus.broadcast((u, ts));
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

                    let u = inner.process(u, src, ts, &aqfs);
                    processed_ts.store(ts as isize, sync::atomic::Ordering::Release);

                    if u.is_some() {
                        forwarded = ts;
                        context.lock().bus.broadcast((u, ts));
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
                context.lock().bus.broadcast((None, min));
            }

            // check if descendant min has changed so we can absorb?
            let mut ctx = context.lock();
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

    pub fn incorporate<V: 'static + Send + Sync + View<Q, Update = U, Data = D, Params = P>>
        (&mut self,
         node: V,
         ancestors: Vec<(Q, petgraph::graph::NodeIndex)>)
         -> petgraph::graph::NodeIndex {

        let idx = self.graph.add_node(Some(sync::Arc::new(node)));
        if ancestors.is_empty() {
            // base record node
            self.graph.add_edge(self.source, idx, None);
        } else {
            // derived node -- add edges from all ancestor nodes to node
            for (q, ancestor) in ancestors.into_iter() {
                self.graph.add_edge(ancestor, idx, Some(sync::Arc::new(q)));
            }
        }
        idx
    }
}

impl<Q, U, D, P> Drop for FlowGraph<Q, U, D, P>
    where Q: Clone + Send + Sync,
          U: Clone + Send,
          D: Clone + Send,
          P: Send
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
    struct Counter(String, sync::Arc<sync::Mutex<u32>>);

    impl View<()> for Counter {
        type Update = u32;
        type Data = u32;
        type Params = ();

        fn find(&self,
                _: &HashMap<NodeIndex,
                            Box<Fn(Self::Params, i64) -> Vec<Self::Data> + Send + Sync>>,
                _: Option<()>,
                _: Option<i64>)
                -> Vec<Self::Data> {
            vec![*self.1.lock().unwrap()]
        }

        fn process(&self,
                   u: Self::Update,
                   _: NodeIndex,
                   _: i64,
                   _: &HashMap<NodeIndex,
                               Box<Fn(Self::Params, i64) -> Vec<Self::Data> + Send + Sync>>)
                   -> Option<Self::Update> {
            use std::ops::AddAssign;
            let mut x = self.1.lock().unwrap();
            x.add_assign(u);
            Some(u)
        }

        fn suggest_indexes(&self, _: NodeIndex) -> HashMap<NodeIndex, Vec<usize>> {
            HashMap::new()
        }

        fn resolve(&self, _: usize) -> Option<Vec<(NodeIndex, usize)>> {
            None
        }

        fn add_index(&self, _: usize) {
            unreachable!();
        }

        fn safe(&self, _: i64) {}
    }

    impl FillableQuery for () {
        type Params = ();
        fn fill(&mut self, _: Self::Params) {}
    }

    #[test]
    fn simple_graph() {
        // set up graph
        let mut g = FlowGraph::new();
        let a = g.incorporate(Counter("a".into(), Default::default()), vec![]);
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
    fn join_graph() {
        // set up graph
        let mut g = FlowGraph::new();
        let a = g.incorporate(Counter("a".into(), Default::default()), vec![]);
        let b = g.incorporate(Counter("b".into(), Default::default()), vec![]);
        let c = g.incorporate(Counter("c".into(), Default::default()),
                              vec![((), a), ((), b)]);
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
        let a = g.incorporate(Counter("a".into(), Default::default()), vec![]);
        let b = g.incorporate(Counter("b".into(), Default::default()), vec![]);
        let c = g.incorporate(Counter("c".into(), Default::default()),
                              vec![((), a), ((), b)]);
        let d = g.incorporate(Counter("d".into(), Default::default()), vec![((), c)]);
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
        let _ = g.incorporate(Counter("x".into(), Default::default()), vec![]);
        let (_, _) = g.run(10);

        let a = g.incorporate(Counter("a".into(), Default::default()), vec![]);
        let b = g.incorporate(Counter("b".into(), Default::default()), vec![]);
        let c = g.incorporate(Counter("c".into(), Default::default()),
                              vec![((), a), ((), b)]);
        let d = g.incorporate(Counter("d".into(), Default::default()), vec![((), c)]);
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
    fn overlap_migration() {
        // set up graph
        let mut g = FlowGraph::new();
        let a = g.incorporate(Counter("a".into(), Default::default()), vec![]);
        let x = g.incorporate(Counter("x".into(), Default::default()), vec![((), a)]);
        let (put_1, get_1) = g.run(10);

        let b = g.incorporate(Counter("b".into(), Default::default()), vec![]);
        let c = g.incorporate(Counter("c".into(), Default::default()),
                              vec![((), a), ((), b)]);
        let d = g.incorporate(Counter("d".into(), Default::default()), vec![((), c)]);
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
    #[ignore]
    fn migration_initialization() {
        // set up graph
        let mut g = FlowGraph::new();
        let a = g.incorporate(Counter("a".into(), Default::default()), vec![]);
        let x = g.incorporate(Counter("x".into(), Default::default()), vec![((), a)]);
        let (put_1, get_1) = g.run(10);

        // send a value on a
        put_1[&a].send(1);

        // give it some time to propagate
        thread::sleep(time::Duration::new(0, 10_000_000));

        // see that result appeared at x
        assert_eq!(get_1[&x](None), vec![1]);

        // perform migration
        let b = g.incorporate(Counter("b".into(), Default::default()), vec![]);
        let c = g.incorporate(Counter("c".into(), Default::default()),
                              vec![((), a), ((), b)]);
        let d = g.incorporate(Counter("d".into(), Default::default()), vec![((), c)]);
        let (put, get) = g.run(10);

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
