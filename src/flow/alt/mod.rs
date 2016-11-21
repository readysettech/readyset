use petgraph;
use petgraph::graph::NodeIndex;
use ops;

use std::sync::mpsc;
use std::sync;

use std::collections::HashMap;
use std::collections::HashSet;

use std::ops::Deref;

pub mod domain;

type U = ops::Update;

/// A Message exchanged over an edge in the graph.
#[derive(Clone)]
pub struct Message {
    pub from: NodeIndex,
    pub data: U,
}

pub type Edge = bool; // should the edge be materialized?

pub trait Ingredient
    where Self: Send
{
    fn ancestors(&self) -> Vec<NodeIndex>;
    fn process(&mut self, m: Message) -> Option<U>;
    fn should_materialize(&self) -> bool;
    fn fields(&self) -> &[&str];
}

pub enum Node {
    Ingress(domain::Index, mpsc::Receiver<Message>),
    Internal(domain::Index, Box<Ingredient>),
    Egress(domain::Index, sync::Arc<sync::Mutex<Vec<mpsc::Sender<Message>>>>),
    Unassigned(Box<Ingredient>),
    Taken,
}

impl Node {
    fn domain(&self) -> domain::Index {
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
    added: HashMap<NodeIndex, Option<domain::Index>>,
    materialize: HashSet<(NodeIndex, NodeIndex)>,
}

impl Blender {
    pub fn start_migration<'a>(&'a mut self) -> Migration<'a> {
        Migration {
            mainline: self,
            added: Default::default(),
            materialize: Default::default(),
        }
    }
}

impl<'a> Migration<'a> {
    pub fn add_domain(&mut self) -> domain::Index {
        self.mainline.ndomains += 1;
        (self.mainline.ndomains - 1).into()
    }

    pub fn add_ingredient<I: Ingredient + 'static>(&mut self, i: I) -> NodeIndex {
        let parents = i.ancestors();
        // add to the graph
        let ni = self.mainline.ingredients.add_node(Node::Unassigned(Box::new(i)));
        // keep track of the fact that it's new
        self.added.insert(ni, None);
        // insert it into the graph
        if parents.is_empty() {
            self.mainline.ingredients.add_edge(self.mainline.source, ni, false);
        } else {
            for parent in parents {
                self.mainline.ingredients.add_edge(parent, ni, false);
            }
        }
        // and tell the caller its id
        ni
    }

    pub fn materialize(&mut self, src: NodeIndex, dst: NodeIndex) {
        // TODO
        // what about if a user tries to materialize a cross-domain edge that has already been
        // converted to an egress/ingress pair?
        let e = self.mainline
            .ingredients
            .find_edge(src, dst)
            .expect("asked to materialize non-existing edge");

        let mut e = self.mainline.ingredients.edge_weight_mut(e).unwrap();
        if !*e {
            *e = true;
            // it'd be nice if we could just store the EdgeIndex here, but unfortunately that's not
            // guaranteed by petgraph to be stable in the presence of edge removals (which we do in
            // commit())
            self.materialize.insert((src, dst));
        }
    }

    pub fn assign_domain(&mut self, n: NodeIndex, d: domain::Index) {
        assert!(self.added.insert(n, Some(d)).is_none());
    }

    fn boot_domain(&mut self, d: domain::Index, nodes: Vec<NodeIndex>) {
        let d = domain::Domain::from_graph(d, nodes, &mut self.mainline.ingredients);
        d.boot();
    }

    pub fn commit(mut self) -> HashMap<NodeIndex, mpsc::Sender<U>> {
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
                    let was_materialized = self.mainline.ingredients.remove_edge(old).unwrap();
                    self.mainline.ingredients.add_edge(parent, ingress, false);
                    self.mainline.ingredients.add_edge(ingress, node, was_materialized);
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
                    // note that we *know* that this is an edge to an ingress node, as we prepended
                    // all nodes with a different-domain parent with an ingress node above.
                    if cfg!(debug_assertions) {
                        if let Node::Ingress(..) = self.mainline.ingredients[child] {
                        } else {
                            unreachable!("child of egress is not an ingress");
                        }
                    }

                    let old = self.mainline.ingredients.find_edge(node, child).unwrap();
                    self.mainline.ingredients.remove_edge(old);
                    // because all our outgoing edges are to ingress nodes,
                    // they should never be materialized
                    self.mainline.ingredients.add_edge(node, egress, false);
                    self.mainline.ingredients.add_edge(egress, child, false);
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
        let mut sources = HashMap::new();
        for (ingress, tx) in targets {
            if let Node::Ingress(..) = self.mainline.ingredients[ingress] {
                // any egress with an edge to this node needs to have a tx clone added to its tx
                // channel set.
                for egress in self.mainline
                    .ingredients
                    .neighbors_directed(ingress, petgraph::EdgeDirection::Incoming) {

                    if egress == self.mainline.source {
                        use std::thread;

                        // input node
                        debug_assert_eq!(self
                            .mainline
                            .ingredients
                            .neighbors_directed(ingress, petgraph::EdgeDirection::Incoming)
                            .count()
                            , 1);

                        // we want to avoid forcing the end-user to cook up Messages
                        // they should instead just make Us
                        // start a thread that does that conversion
                        let (tx2, rx2) = mpsc::channel();
                        let src = self.mainline.source;
                        thread::spawn(move || {
                            for u in rx2 {
                                tx.send(Message {
                                        from: src,
                                        data: u,
                                    })
                                    .unwrap();
                            }
                        });
                        sources.insert(ingress, tx2);
                        break;
                    }

                    if let Node::Egress(_, ref txs) = self.mainline.ingredients[egress] {
                        // connected to egress from other domain
                        txs.lock().unwrap().push(tx.clone());
                        continue;
                    }

                    unreachable!("ingress parent is not egress");
                }
            } else {
                unreachable!("ingress node not of ingress type");
            }
        }

        sources
    }
}
