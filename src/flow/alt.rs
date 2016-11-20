use petgraph;
use petgraph::graph::NodeIndex;
use ops;

use std::sync::mpsc;
use std::sync;

use std::collections::VecDeque;
use std::collections::HashMap;

use std::ops::Deref;

#[derive(Eq, PartialEq, Ord, PartialOrd, Hash, Clone, Copy)]
struct Domain(usize);

type U = ops::Update;

/// A Message exchanged over an edge in the graph.
#[derive(Clone)]
pub struct Message {
    pub from: NodeIndex,
    pub data: U,
}

macro_rules! broadcast {
    ($handoffs:ident, $m:expr, $children:ident) => {{
        let mut m = Some($m); // so we can .take() below
        for (i, to) in $children.iter().enumerate() {
            let u = if i == $children.len() - 1 {
                m.take()
            } else {
                m.clone()
            };

            $handoffs.get_mut(to).unwrap().push_back(u.unwrap());
        }
    }}
}


type Edge = ();

pub trait Ingredient
    where Self: Send
{
    fn ancestors(&self) -> Vec<NodeIndex>;
    fn process(&mut self, m: Message) -> Option<U>;
}

enum Node {
    Ingress(Domain, mpsc::Receiver<Message>),
    Internal(Domain, Box<Ingredient>),
    Egress(Domain, sync::Arc<sync::Mutex<Vec<mpsc::Sender<Message>>>>),
    Unassigned(Box<Ingredient>),
    Taken,
}

impl Node {
    fn domain(&self) -> Domain {
        match *self {
            Node::Ingress(d, _) |
            Node::Internal(d, _) |
            Node::Egress(d, _) => d,
            _ => unreachable!(),
        }
    }
}

impl Deref for Node {
    type Target = Ingredient;
    fn deref(&self) -> &Self::Target {
        match self {
            &Node::Internal(_, ref i) |
            &Node::Unassigned(ref i) => i.deref(),
            _ => unreachable!(),
        }
    }
}

pub struct Blender {
    ingredients: petgraph::Graph<Node, Edge>,
    source: NodeIndex,
    ndomains: usize,
}

pub struct Migration<'a> {
    mainline: &'a mut Blender,
    added: HashMap<NodeIndex, Option<Domain>>,
}

impl Blender {
    pub fn start_migration<'a>(&'a mut self) -> Migration<'a> {
        Migration {
            mainline: self,
            added: Default::default(),
        }
    }
}

impl<'a> Migration<'a> {
    pub fn add_domain(&mut self) -> Domain {
        self.mainline.ndomains += 1;
        Domain(self.mainline.ndomains - 1)
    }

    pub fn add_ingredient<I: Ingredient + 'static>(&mut self, i: I) -> NodeIndex {
        let parents = i.ancestors();
        // add to the graph
        let ni = self.mainline.ingredients.add_node(Node::Unassigned(Box::new(i)));
        // keep track of the fact that it's new
        self.added.insert(ni, None);
        // insert it into the graph
        if parents.is_empty() {
            self.mainline.ingredients.add_edge(self.mainline.source, ni, ());
        } else {
            for parent in parents {
                self.mainline.ingredients.add_edge(parent, ni, ());
            }
        }
        // and tell the caller its id
        ni
    }

    pub fn assign_domain(&mut self, n: NodeIndex, d: Domain) {
        assert!(self.added.insert(n, Some(d)).is_none());
    }

    fn boot_domain(&mut self, d: Domain, nodes: Vec<NodeIndex>) {
        use std::thread;

        let mut nodes: Vec<_> = nodes.into_iter()
            .map(|ni| {
                use std::mem;
                let n = match *self.mainline.ingredients.node_weight_mut(ni).unwrap() {
                    Node::Egress(d, ref txs) => {
                        // egress nodes can still be modified externally if subgraphs are added
                        // so we just make a new one with a clone of the Mutex-protected Vec
                        Node::Egress(d, txs.clone())
                    }
                    ref mut n @ Node::Ingress(..) => {
                        // no-one else will be using our ingress node, so we take it from the graph
                        mem::replace(n, Node::Taken)
                    }
                    ref mut n @ Node::Internal(..) => {
                        // same with internal nodes
                        mem::replace(n, Node::Taken)
                    }
                    _ => unreachable!(),
                };
                (ni, n)
            })
            .collect::<Vec<_>>() // because above closure mutably borrows self.mainline
            .into_iter()
            .map(|(ni, n)| {
                // also include all *internal* descendants
                let children: Vec<_> = self.mainline
                    .ingredients
                    .neighbors_directed(ni, petgraph::EdgeDirection::Outgoing)
                    .filter(|&c| self.mainline.ingredients[c].domain() == d)
                    .collect();
                (ni, n, children)
            })
            .collect();

        thread::spawn(move || {
            let mut handoffs: HashMap<NodeIndex, VecDeque<Message>> =
                nodes.iter().map(|&(ni, _, _)| (ni, VecDeque::new())).collect();

            loop {
                // `nodes` is already in topological order, so we just walk over them in order and
                // do the appropriate action for each one.
                for &mut (ni, ref mut n, ref children) in &mut nodes {
                    match n {
                        &mut Node::Ingress(_, ref mut rx) => {
                            // receive an update
                            debug_assert!(handoffs[&ni].is_empty());
                            broadcast!(handoffs, rx.recv().unwrap(), children);
                        }
                        &mut Node::Egress(_, ref txs) => {
                            // send any queued updates to all external children
                            let mut txs = txs.lock().unwrap();
                            let txn = txs.len() - 1;
                            while let Some(m) = handoffs.get_mut(&ni).unwrap().pop_front() {
                                let mut m = Some(m); // so we can use .take()
                                for (txi, tx) in txs.iter_mut().enumerate() {
                                    if txi == txn && children.is_empty() {
                                        tx.send(m.take().unwrap()).unwrap();
                                    } else {
                                        tx.send(m.clone().unwrap()).unwrap();
                                    }
                                }

                                if let Some(m) = m {
                                    broadcast!(handoffs, m, children);
                                } else {
                                    debug_assert!(children.is_empty());
                                }
                            }
                        }
                        &mut Node::Internal(_, ref mut i) => {
                            while let Some(m) = handoffs.get_mut(&ni).unwrap().pop_front() {
                                if let Some(u) = i.process(m) {
                                    broadcast!(handoffs,
                                               Message {
                                                   from: ni,
                                                   data: u,
                                               },
                                               children);
                                }
                            }
                        }
                        _ => unreachable!(),
                    }
                }
            }
        });
    }

    pub fn commit(mut self) {
        // the user wants us to commit to the changes contained within this Migration. this is
        // where all the magic happens.

        let mut targets = HashMap::new();
        let mut domain_nodes = HashMap::new();
        let mut topo = petgraph::visit::Topo::new(&self.mainline.ingredients);
        while let Some(node) = topo.next(&self.mainline.ingredients) {
            if node == self.mainline.source {
                continue;
            }
            if !self.added.contains_key(&node) {
                // not new
                continue;
            }

            let domain = self.added.get(&node).unwrap().unwrap_or_else(|| {
                // new node that doesn't belong to a domain
                // create a new domain just for that node
                self.add_domain()
            });

            let parents: Vec<_> = self.mainline
                .ingredients
                .neighbors_directed(node, petgraph::EdgeDirection::Incoming)
                .collect(); // collect so we can mutate mainline.ingredients

            let mut ingress = None;
            for parent in parents {
                if parent == self.mainline.source ||
                   domain != self.mainline.ingredients[parent].domain() {
                    // parent is in a different domain
                    // create an ingress node to handle that if we haven't already
                    if ingress.is_none() {
                        // it's going to need its own channel
                        let (tx, rx) = mpsc::channel();
                        // and it needs to be in the graph
                        ingress =
                            Some(self.mainline.ingredients.add_node(Node::Ingress(domain, rx)));

                        let ingress = ingress.unwrap();
                        // that ingress node also needs to run before us
                        domain_nodes.entry(domain).or_insert_with(Vec::new).push(ingress);
                        // we're also going to need to tell any egress node that will send to this
                        // node about the channel it should use. we'll do that later, so just keep
                        // track of this work for later for now.
                        targets.insert(ingress, tx);
                    }

                    let ingress = ingress.unwrap();
                    // we need to hook the ingress node in between us and this parent
                    let old = self.mainline.ingredients.find_edge(parent, node).unwrap();
                    self.mainline.ingredients.remove_edge(old);
                    self.mainline.ingredients.add_edge(parent, ingress, ());
                    self.mainline.ingredients.add_edge(ingress, node, ());
                }
            }

            // all user-supplied nodes are internal
            use std::mem;
            match mem::replace(&mut self.mainline.ingredients[node], Node::Taken) {
                Node::Unassigned(inner) => {
                    *self.mainline.ingredients.node_weight_mut(node).unwrap() =
                        Node::Internal(domain, inner);
                }
                _ => unreachable!(),
            }

            // in this domain, run this node after any parent ingress nodes we may have added
            domain_nodes.entry(domain).or_insert_with(Vec::new).push(node);

            let children: Vec<_> = self.mainline
                .ingredients
                .neighbors_directed(node, petgraph::EdgeDirection::Outgoing)
                .collect(); // collect so we can mutate mainline.ingredients

            for child in children {
                if domain != self.mainline.ingredients[child].domain() {
                    // child is in a different domain
                    // create an egress node to handle that
                    let egress = self.mainline
                        .ingredients
                        .add_node(Node::Egress(domain, Default::default()));
                    // we need to hook that node in between us and this child
                    let old = self.mainline.ingredients.find_edge(node, child).unwrap();
                    self.mainline.ingredients.remove_edge(old);
                    self.mainline.ingredients.add_edge(node, egress, ());
                    self.mainline.ingredients.add_edge(egress, child, ());
                    // that egress node also needs to run
                    domain_nodes.entry(domain).or_insert_with(Vec::new).push(egress);
                }
            }

            // TODO: what about leaf nodes?
        }

        // at this point, we've hooked up the graph such that, for any given domain, the graph
        // looks like this:
        //
        //      o (egress)
        //     +.\......................
        //     :  o (ingress)
        //     :  |
        //     :  o-------------+
        //     :  |             |
        //     :  o             o
        //     :  |             |
        //     :  o (egress)    o (egress)
        //     +..|...........+.|..........
        //     :  o (ingress) : o (ingress)
        //     :  |\          :  \
        //     :  | \         :   o
        //
        // etc.
        //
        // first, start up all the domains
        for (domain, nodes) in domain_nodes {
            self.boot_domain(domain, nodes);
        }
        // then, hook up the channels to new ingress nodes
        for (ingress, tx) in targets {
            if let Node::Ingress(..) = self.mainline.ingredients[ingress] {
                // any egress with an edge to this node needs to have a tx clone added to its tx
                // channel set.
                for egress in self.mainline
                    .ingredients
                    .neighbors_directed(ingress, petgraph::EdgeDirection::Incoming) {
                    if let Node::Egress(_, ref txs) = self.mainline.ingredients[egress] {
                        txs.lock().unwrap().push(tx.clone());
                    } else {
                        unreachable!("ingress parent is not egress");
                    }
                }
            } else {
                unreachable!("ingress node not of ingress type");
            }
        }
    }
}
