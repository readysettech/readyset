use petgraph;
use bus::Bus;
use clocked_dispatch;

use std;
use std::collections::HashMap;
use std::sync::mpsc;
use std::sync;
use std::thread;

pub use petgraph::graph::NodeIndex;

// TODO: add an "uninstantiated query" type

pub trait View<Q: Clone + Send> {
    type Update: Clone + Send;
    type Data: Clone + Send;
    type Params: Send;

    /// Execute a single concrete query, producing an iterator over all matching records.
    fn find<'a>(&'a self,
             sync::Arc<HashMap<NodeIndex, Box<Fn(Self::Params, i64) -> Box<Iterator<Item = Self::Data>> + Send + Sync>>>,
             Option<Q>,
             i64) -> Box<Iterator<Item = Self::Data> + 'a>;

    /// Process a new update. This may optionally produce a new update to propagate to child nodes
    /// in the data flow graph.
    fn process(&self,
               Self::Update,
               NodeIndex,
               i64,
               sync::Arc<HashMap<NodeIndex, Box<Fn(Self::Params, i64) -> Box<Iterator<Item = Self::Data>> + Send + Sync>>>)
               -> Option<Self::Update>;
}

pub trait FillableQuery {
    type Params;
    fn fill(&mut self, Self::Params);
}

/// This is nasty little hack to allow View::find() to return an iterator bound to the lifetime of
/// &self. This makes it find more pleasant to implement, but forces us to do some additional work
/// on the caller side to ensure that the ref stays around for long enough. Basically, we make sure
/// to keep around a sync::Arc to the vertex in question, which allows us to transmute the iterator
/// to 'static (since we know the lifetime bound always holds).
struct NodeFind<D, P, Q, V: ?Sized>
    where D: Clone + Send,
          P: Send,
          Q: Clone + Send + Sync,
          V: Send + Sync + View<Q, Data = D, Params = P>
{
    _keep: sync::Arc<V>,
    _x: std::marker::PhantomData<P>,
    _y: std::marker::PhantomData<Q>,
    it: Box<Iterator<Item = D>>,
}

impl<D: Clone + Send, P: Send, Q: Clone + Send + Sync, V: ?Sized + Send + Sync + View<Q, Data = D, Params = P>> NodeFind<D, P, Q, V> {
    pub fn new(this: sync::Arc<V>,
               aqfs: sync::Arc<HashMap<NodeIndex,
                                       Box<Fn(P, i64) -> Box<Iterator<Item = D>> + Send + Sync>>>,
               q: Option<Q>,
               ts: i64)
               -> NodeFind<D, P, Q, V> {
        use std::mem;
// ok to make 'static as we hold on to the Arc for at least as long as the iterator lives
        let it = unsafe { mem::transmute(this.find(aqfs, q, ts)) };
        NodeFind {
            _keep: this,
            _x: std::marker::PhantomData,
            _y: std::marker::PhantomData,
            it: it,
        }
    }
}

impl<D: Clone + Send, P: Send, Q: Clone + Send + Sync, V: ?Sized + Send + Sync + View<Q, Data = D, Params = P>> Iterator for NodeFind<D, P, Q, V> {
    type Item = D;
    fn next(&mut self) -> Option<Self::Item> {
        self.it.next()
    }
}

pub struct FlowGraph<Q: Clone + Send + Sync, U: Clone + Send, D: Clone + Send, P: Send> {
    graph: petgraph::Graph<Option<sync::Arc<View<Q, Update=U, Data=D, Params=P> + 'static + Send + Sync>>,
                           Option<sync::Arc<Q>>>,
    source: petgraph::graph::NodeIndex,
    wait: Vec<thread::JoinHandle<()>>,
    dispatch: clocked_dispatch::Dispatcher<U>,
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
            wait: Vec::default(),
            dispatch: clocked_dispatch::new(20),
        }
    }

    pub fn run(&mut self,
               buf: usize)
               -> (HashMap<NodeIndex, clocked_dispatch::ClockedSender<U>>,
                   HashMap<NodeIndex,
                           Box<Fn(Option<Q>, i64) -> Box<Iterator<Item = D>> + Send + Sync>>) {
        // TODO: may be called again after more incorporates

        // set up in-channels for each base record node
        let mut incoming = HashMap::new();
        let mut start = HashMap::new();
        for base in self.graph.neighbors(self.source) {
            let (tx, rx) = self.dispatch.new(format!("{}-in", base.index()),
                                             format!("{}-out", base.index()));

            // automatically add source node and timestamp to all incoming facts so users don't
            // have to add this themselves.
            let (px_tx, px_rx) = mpsc::sync_channel(buf);
            let root = self.source;
            thread::spawn(move || {
                for (u, ts) in rx.into_iter() {
                    match u {
                        Some(u) => px_tx.send((root, u, ts as i64)).unwrap(),
                        None => (), // TODO
                    }
                }
            });
            incoming.insert(base, tx);
            start.insert(base, px_rx);
        }

        // set up the internal data-flow graph channels
        let mut busses = HashMap::new();
        for node in petgraph::BfsIter::new(&self.graph, self.source) {
            if node == self.source {
                continue;
            }

            // create a bus for the outgoing records from this node.
            // size buf/2 since the sync_channels consuming from the bus are also buffered.
            busses.insert(node, Bus::new(buf / 2));

            if !start.contains_key(&node) {
                // this node should receive updates from all its ancestors
                let ancestors = self.graph
                    .neighbors_directed(node, petgraph::EdgeDirection::Incoming);
                let (tx, rx) = mpsc::sync_channel(buf / 2);

                for ancestor in ancestors {
                    // since we're doing a BFS, we know that all of them already have a bus.
                    let rx = busses.get_mut(&ancestor).unwrap().add_rx();
                    let tx = tx.clone();

                    // since Rust doesn't currently support select very well, we need one thread
                    // per incoming edge, all sharing a single mpsc channel into the node in
                    // question.
                    thread::spawn(move || {
                        for (u, ts) in rx.into_iter() {
                            if let Err(..) = tx.send((ancestor, u, ts)) {
                                break;
                            }
                        }
                    });
                }

                // this is now what the node should receive on
                start.insert(node, rx);
            }
        }

        // in order to query a node, we need to know how to query all its ancestors. specifically,
        // we need to combine the query along the edges to all ancestors witht he query function on
        // those ancestors. for example, if we have a --[q1]--> b --[q2]--> c, and c wants to query
        // from be, the arguments to b.query should be q2, along with a function that lets b query
        // from a. that function should call a.query with q1, and a way for a to query its
        // ancestors. this continues all the way back to the base nodes whose ancestor query
        // function list is emtpy.
        //
        // we build the ancestor query functions inductively below.
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
                    // find the ancestor query functions for the ancestor's .query
                    let aqf = aqfs[&ni].clone();
                    // execute the ancestor's .query using the query that connects it to us
                    let f = Box::new(move |p: P, ts: i64| -> Box<Iterator<Item = D>> {
                        let mut q_cur = (*q).clone();
                        q_cur.fill(p);
                        Box::new(NodeFind::new(a.clone(), aqf.clone(), Some(q_cur), ts))
                    }) as Box<Fn(P, i64) -> Box<Iterator<Item = D>> + Send + Sync>;
                    (ni, f)
                })
                .collect();

            aqfs.insert(node, sync::Arc::new(aqf));
        }

        // expose .query in a friendly format to outsiders
        let mut qs = HashMap::with_capacity(aqfs.len());
        for (ni, aqf) in aqfs.iter() {
            let aqf = aqf.clone();
            let n = self.graph[*ni].as_ref().unwrap().clone();
            let func = Box::new(move |q: Option<Q>, ts: i64| -> Box<Iterator<Item = D>> {
                Box::new(NodeFind::new(n.clone(), aqf.clone(), q, ts))
            }) as Box<Fn(Option<Q>, i64) -> Box<Iterator<Item = D>> + Send + Sync>;

            qs.insert(*ni, func);
        }

        // spin up all the worker threads
        for node in petgraph::BfsIter::new(&self.graph, self.source) {
            if node == self.source {
                continue;
            }

            let aqf = aqfs.remove(&node).unwrap();
            let mut tx = busses.remove(&node).unwrap();
            let rx = start.remove(&node).unwrap();
            let node = self.graph[node].as_ref().unwrap().clone();

            // start a thread for managing this node.
            // basically just a rx->process->tx loop.
            self.wait.push(thread::spawn(move || {
                for (src, u, ts) in rx.into_iter() {
                    if let Some(u) = node.process(u, src, ts, aqf.clone()) {
                        tx.broadcast((u, ts));
                    }
                }
            }));

            // TODO: how do we get an &mut bus later for adding recipients?
        }

        (incoming, qs)
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
                 _: sync::Arc<HashMap<NodeIndex, Box<Fn(Self::Params, i64) -> Box<Iterator<Item = Self::Data>> + Send + Sync>>>,
                 _: Option<()>,
                 _: i64) -> Box<Iterator<Item = Self::Data>> {
            Box::new(Some(*self.1.lock().unwrap()).into_iter())
        }

        fn process(&self,
                   u: Self::Update,
                   _: NodeIndex,
                   _: i64,
                   _: sync::Arc<HashMap<NodeIndex, Box<Fn(Self::Params, i64) -> Box<Iterator<Item = Self::Data>> + Send + Sync>>>)
                   -> Option<Self::Update> {
            use std::ops::AddAssign;
            let mut x = self.1.lock().unwrap();
            x.add_assign(u);
            Some(u)
        }
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
        thread::sleep(time::Duration::new(0, 1_000_000));

        // send a query
        assert_eq!(get[&a](None, i64::max_value()).collect::<Vec<_>>(), vec![1]);

        // update value again
        put[&a].send(1);

        // give it some time to propagate
        thread::sleep(time::Duration::new(0, 1_000_000));

        // check that value was updated again
        assert_eq!(get[&a](None, i64::max_value()).collect::<Vec<_>>(), vec![2]);
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
        thread::sleep(time::Duration::new(0, 1_000_000));

        // send a query to c
        assert_eq!(get[&c](None, i64::max_value()).collect::<Vec<_>>(), vec![1]);

        // update value again
        put[&b].send(1);

        // give it some time to propagate
        thread::sleep(time::Duration::new(0, 1_000_000));

        // check that value was updated again
        assert_eq!(get[&c](None, i64::max_value()).collect::<Vec<_>>(), vec![2]);
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
        thread::sleep(time::Duration::new(0, 1_000_000));

        // send a query to d
        assert_eq!(get[&d](None, i64::max_value()).collect::<Vec<_>>(), vec![1]);

        // update value again
        put[&b].send(1);

        // give it some time to propagate
        thread::sleep(time::Duration::new(0, 1_000_000));

        // check that value was updated again
        assert_eq!(get[&d](None, i64::max_value()).collect::<Vec<_>>(), vec![2]);
    }
}
