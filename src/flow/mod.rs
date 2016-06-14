use petgraph;
use bus::Bus;

use std::collections::HashMap;
use std::sync::mpsc;
use std::sync;
use std::thread;

pub trait View<Q: Clone + Send> {
    type Update: Clone + Send;
    type Data: Clone + Send;
    type Params: Send;

    /// Process a stream of query parameters, producing all matching records. All matching records
    /// will be sent on the passed `Sender` channel. Parameters should be sent on the returned
    /// `Sender` channel.
    fn query(&self,
             sync::Arc<Vec<Box<Fn(mpsc::Sender<Self::Data>) -> mpsc::Sender<Self::Params> + Send + Sync>>>,
             Option<&Q>,
             mpsc::Sender<Self::Data>)
             -> mpsc::Sender<Self::Params>;

    /// Process a new update. This may optionally produce a new update to propagate to child nodes
    /// in the data flow graph.
    fn process(&self,
               Self::Update,
               sync::Arc<Vec<Box<Fn(mpsc::Sender<Self::Data>) -> mpsc::Sender<Self::Params> + Send + Sync>>>)
               -> Option<Self::Update>;
}

pub struct FlowGraph<Q: Clone + Send + Sync, V: Send + Sync + View<Q>> {
    graph: petgraph::Graph<Option<sync::Arc<V>>, Option<sync::Arc<Q>>>,
    source: petgraph::graph::NodeIndex,
    wait: Vec<thread::JoinHandle<()>>,
}

impl<Q: 'static + Clone + Send + Sync, V: 'static + Send + Sync + View<Q>> FlowGraph<Q, V> {
    pub fn new() -> FlowGraph<Q, V> {
        let mut graph = petgraph::Graph::new();
        let source = graph.add_node(None);
        FlowGraph {
            graph: graph,
            source: source,
            wait: Vec::default(),
        }
    }

    pub fn run(&mut self,
               buf: usize)
               -> (HashMap<petgraph::graph::NodeIndex, mpsc::SyncSender<V::Update>>,
                   HashMap<petgraph::graph::NodeIndex,
                           Box<Fn(mpsc::Sender<V::Data>) -> mpsc::Sender<V::Params> + Send + Sync>>) {
        // TODO: may be called again after more incorporates

        // set up in-channels for each base record node
        let mut incoming = HashMap::new();
        let mut start = HashMap::new();
        for base in self.graph.neighbors(self.source) {
            let (tx, rx) = mpsc::sync_channel(buf);
            incoming.insert(base, tx);
            start.insert(base, rx);
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
                        for u in rx.into_iter() {
                            if let Err(..) = tx.send(u) {
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
                aqfs.insert(node, sync::Arc::new(Vec::new()));
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
                    Box::new(move |tx: mpsc::Sender<V::Data>| -> mpsc::Sender<V::Params> {
                        a.query(aqf.clone(), Some(&q), tx)
                    }) as Box<Fn(mpsc::Sender<V::Data>) -> mpsc::Sender<V::Params> + Send + Sync>
                })
                .collect();

            aqfs.insert(node, sync::Arc::new(aqf));
        }

        // expose .query in a friendly format to outsiders
        let mut qs = HashMap::with_capacity(aqfs.len());
        for (ni, aqf) in aqfs.iter() {
            let aqf = aqf.clone();
            let n = self.graph[*ni].as_ref().unwrap().clone();
            let func = Box::new(move |tx: mpsc::Sender<V::Data>| -> mpsc::Sender<V::Params> {
                n.query(aqf.clone(), None, tx)
            }) as Box<Fn(mpsc::Sender<V::Data>) -> mpsc::Sender<V::Params> + Send + Sync>;

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
                for u in rx.into_iter() {
                    if let Some(u) = node.process(u, aqf.clone()) {
                        tx.broadcast(u);
                    }
                }
            }));

            // TODO: how do we get an &mut bus later for adding recipients?
        }

        (incoming, qs)
    }

    pub fn incorporate(&mut self,
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

impl<Q: Clone + Send + Sync, V: Send + Sync + View<Q>> Drop for FlowGraph<Q, V> {
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
    use std::sync::mpsc;
    struct Counter(String, sync::Arc<sync::Mutex<u32>>);

    impl View<()> for Counter {
        type Update = u32;
        type Data = u32;
        type Params = ();

        fn query(&self,
                 _: sync::Arc<Vec<Box<Fn(mpsc::Sender<Self::Data>) -> mpsc::Sender<Self::Params> + Send + Sync>>>,
                 _: Option<&()>,
                 tx: mpsc::Sender<Self::Data>)
                 -> mpsc::Sender<Self::Params> {
            let (ptx, prx) = mpsc::channel();
            let x = self.1.clone();
            thread::spawn(move || {
                for _ in prx.into_iter() {
                    tx.send(*x.lock().unwrap()).unwrap();
                }
            });
            ptx
        }

        fn process(&self,
                   u: Self::Update,
                   _: sync::Arc<Vec<Box<Fn(mpsc::Sender<Self::Data>) -> mpsc::Sender<Self::Params> + Send + Sync>>>)
                   -> Option<Self::Update> {
            use std::ops::AddAssign;
            let mut x = self.1.lock().unwrap();
            x.add_assign(u);
            Some(u)
        }
    }

    #[test]
    fn simple_graph() {
        // set up graph
        let mut g = FlowGraph::new();
        let a = g.incorporate(Counter("a".into(), Default::default()), vec![]);
        let (put, get) = g.run(10);

        // send a value
        put[&a].send(1).unwrap();

        // give it some time to propagate
        thread::sleep(time::Duration::new(0, 1_000_000));

        // prepare to query
        let (rtx, rrx) = mpsc::channel();
        let args = get[&a](rtx);

        // send a query
        args.send(()).unwrap();
        assert_eq!(rrx.recv(), Ok(1));

        // update value again
        put[&a].send(1).unwrap();

        // give it some time to propagate
        thread::sleep(time::Duration::new(0, 1_000_000));

        // check that value was updated again
        args.send(()).unwrap();
        assert_eq!(rrx.recv(), Ok(2));
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
        put[&a].send(1).unwrap();

        // give it some time to propagate
        thread::sleep(time::Duration::new(0, 1_000_000));

        // prepare to query
        let (rtx, rrx) = mpsc::channel();
        let args = get[&c](rtx);

        // send a query to c
        args.send(()).unwrap();
        assert_eq!(rrx.recv(), Ok(1));

        // update value again
        put[&b].send(1).unwrap();

        // give it some time to propagate
        thread::sleep(time::Duration::new(0, 1_000_000));

        // check that value was updated again
        args.send(()).unwrap();
        assert_eq!(rrx.recv(), Ok(2));
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
        put[&a].send(1).unwrap();

        // give it some time to propagate
        thread::sleep(time::Duration::new(0, 1_000_000));

        // prepare to query
        let (rtx, rrx) = mpsc::channel();
        let args = get[&d](rtx);

        // send a query to d
        args.send(()).unwrap();
        assert_eq!(rrx.recv(), Ok(1));

        // update value again
        put[&b].send(1).unwrap();

        // give it some time to propagate
        thread::sleep(time::Duration::new(0, 1_000_000));

        // check that value was updated again
        args.send(()).unwrap();
        assert_eq!(rrx.recv(), Ok(2));
    }
}
