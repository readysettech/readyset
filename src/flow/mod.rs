use shortcut;
use petgraph;
use petgraph::graph::NodeIndex;
use query;
use ops;

use std::sync::mpsc;

use std::collections::HashMap;
use std::collections::HashSet;

use std::fmt;

pub mod domain;
pub mod prelude;
pub mod node;

type U = ops::Update;
pub type Edge = bool; // should the edge be materialized?

/// A Message exchanged over an edge in the graph.
#[derive(Clone)]
pub struct Message {
    pub from: NodeIndex,
    pub to: NodeIndex,
    pub data: U,
}

pub trait Ingredient
    where Self: Send
{
    fn ancestors(&self) -> Vec<NodeIndex>;
    fn should_materialize(&self) -> bool;

    /// Should return true if this ingredient will ever query the state of an ancestor outside of a
    /// call to `Self::query()`.
    fn will_query(&self, materialized: bool) -> bool;

    /// Called whenever this node is being queried for records, and it is not materialized. The
    /// node should use the list of ancestor query functions to fetch relevant data from upstream,
    /// and emit resulting records as they come in. Note that there may be no query, in which case
    /// all records should be returned.
    fn query(&self,
             q: Option<&query::Query>,
             domain: &prelude::DomainNodes,
             states: &prelude::StateMap)
             -> ops::Datas;

    /// Suggest fields of this view, or its ancestors, that would benefit from having an index.
    fn suggest_indexes(&self, you: NodeIndex) -> HashMap<NodeIndex, Vec<usize>>;

    /// Resolve where the given field originates from. If the view is materialized, or the value is
    /// otherwise created by this view, None should be returned.
    fn resolve(&self, i: usize) -> Option<Vec<(NodeIndex, usize)>>;

    /// Returns true for base node types.
    fn is_base(&self) -> bool {
        false
    }

    /// Produce a compact, human-readable description of this node.
    ///
    ///  Symbol   Description
    /// --------|-------------
    ///    B    |  Base
    ///    ||   |  Concat
    ///    ⧖    |  Latest
    ///    γ    |  Group by
    ///   |*|   |  Count
    ///    𝛴    |  Sum
    ///    ⋈    |  Join
    ///    ⋉    |  Left join
    ///    ⋃    |  Union
    fn description(&self) -> String;

    fn on_connected(&mut self, graph: &petgraph::Graph<node::Node, Edge>);
    fn on_commit(&mut self, you: NodeIndex, remap: &HashMap<NodeIndex, NodeIndex>);
    fn on_input(&mut self,
                input: Message,
                domain: &prelude::DomainNodes,
                states: &HashMap<NodeIndex, shortcut::Store<query::DataType>>)
                -> Option<U>;
}

/// `Blender` is the core component of the alternate Soup implementation.
///
/// It keeps track of the structure of the underlying data flow graph and its domains. `Blender`
/// does not allow direct manipulation of the graph. Instead, changes must be instigated through a
/// `Migration`, which can be started using `Blender::start_migration`. Only one `Migration` can
/// occur at any given point in time.
pub struct Blender {
    ingredients: petgraph::Graph<node::Node, Edge>,
    source: NodeIndex,
    ndomains: usize,
}

impl Blender {
    /// Construct a new, empty `Blender`
    pub fn new() -> Self {
        let mut g = petgraph::Graph::new();
        let source =
            g.add_node(node::Node::new("source", &["because-type-inference"], node::Type::Source));
        Blender {
            ingredients: g,
            source: source,
            ndomains: 0,
        }
    }

    /// Start setting up a new `Migration`.
    pub fn start_migration<'a>(&'a mut self) -> Migration<'a> {
        Migration {
            mainline: self,
            added: Default::default(),
            materialize: Default::default(),
        }
    }
}

impl fmt::Display for Blender {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let indentln = |f: &mut fmt::Formatter| write!(f, "    ");

        // Output header.
        writeln!(f, "digraph {{")?;

        // Output global formatting.
        indentln(f)?;
        writeln!(f, "node [shape=record, fontsize=10]")?;

        // Output node descriptions.
        for index in self.ingredients.node_indices() {
            indentln(f)?;
            write!(f, "{}", index.index())?;
            self.ingredients[index].describe(f, index)?;
        }

        // Output edges.
        for (_, edge) in self.ingredients.raw_edges().iter().enumerate() {
            indentln(f)?;
            write!(f, "{} -> {}", edge.source().index(), edge.target().index())?;
            if !edge.weight {
                // not materialized
                writeln!(f, " [style=\"dashed\"]")?;
            } else {
                writeln!(f, "")?;
            }
        }

        // Output footer.
        write!(f, "}}")?;

        Ok(())
    }
}

/// A `Migration` encapsulates a number of changes to the Soup data flow graph.
///
/// Only one `Migration` can be in effect at any point in time. No changes are made to the running
/// graph until the `Migration` is committed (using `Migration::commit`).
pub struct Migration<'a> {
    mainline: &'a mut Blender,
    added: HashMap<NodeIndex, Option<domain::Index>>,
    materialize: HashSet<(NodeIndex, NodeIndex)>,
}

impl<'a> Migration<'a> {
    /// Add a new (empty) domain to the graph
    pub fn add_domain(&mut self) -> domain::Index {
        self.mainline.ndomains += 1;
        (self.mainline.ndomains - 1).into()
    }

    /// Add the given `Ingredient` to the Soup.
    ///
    /// The returned identifier can later be used to refer to the added ingredient.
    /// Edges in the data flow graph are automatically added based on the ingredient's reported
    /// `ancestors`.
    pub fn add_ingredient<S1, FS, S2, I>(&mut self, name: S1, fields: FS, i: I) -> NodeIndex
        where S1: ToString,
              S2: ToString,
              FS: IntoIterator<Item = S2>,
              I: Into<node::Type>
    {
        let mut i = i.into();
        i.on_connected(&self.mainline.ingredients);

        let parents = i.ancestors();

        // add to the graph
        let ni = self.mainline.ingredients.add_node(node::Node::new(name, fields, i));

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

    /// Mark the edge between `src` and `dst` in the graph as requiring materialization.
    ///
    /// The reason this is placed per edge rather than per node is that only some children of a
    /// node may require materialization of their inputs (i.e., only those that will query along
    /// this edge). Since we must materialize the output of a node in a foreign domain once for
    /// every receiving domain, this can save us some space if a child that doesn't require
    /// materialization is in its own domain. If multiple nodes in the same domain require
    /// materialization of the same parent, that materialized state will be shared.
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

    /// Assign the ingredient with identifier `n` to the thread domain `d`.
    ///
    /// `n` must be have been added in this migration.
    pub fn assign_domain(&mut self, n: NodeIndex, d: domain::Index) {
        // TODO: what if a node is added to an *existing* domain?
        assert_eq!(self.added.insert(n, Some(d)).unwrap(), None);
    }

    fn boot_domain(&mut self,
                   d: domain::Index,
                   nodes: Vec<NodeIndex>,
                   rx: mpsc::Receiver<Message>) {
        let d = domain::Domain::from_graph(d, nodes, &mut self.mainline.ingredients);
        d.boot(rx);
    }

    /// Commit the changes introduced by this `Migration` to the master `Soup`.
    ///
    /// This will spin up an execution thread for each new thread domain, and hook those new
    /// domains into the larger Soup graph. The returned map contains entry points through which
    /// new updates should be sent to introduce them into the Soup.
    pub fn commit(mut self)
                  -> (HashMap<NodeIndex, Box<Fn(Vec<query::DataType>) + Send + 'static>>,
                      HashMap<NodeIndex, Box<Fn(Option<&query::Query>) -> Vec<Vec<query::DataType>> + Send + Sync>>) {
        // the user wants us to commit to the changes contained within this Migration. this is
        // where all the magic happens.

        // some nodes that used to be the children of other nodes will now be children of ingress
        // nodes instead. keep track of this mapping for each domain.
        let mut domain_remap = HashMap::new();

        // for each domain, we need to keep track of its channel pair so any egress nodes in other
        // domain trying to send to it know where to send.
        let mut txs = HashMap::new();
        let mut rxs = HashMap::new();
        let mut targets = HashMap::new();

        // keep track of the nodes assigned to each domain.
        let mut domain_nodes = HashMap::new();

        // find all new nodes in topological order>
        // we collect first since we'll be mutating the graph below.
        // we need them to be in topological order here so that we know that parents have been
        // processed, and that children have not (which matters for ingress/egress setup).
        let mut topo_list = Vec::with_capacity(self.added.len());
        let mut topo = petgraph::visit::Topo::new(&self.mainline.ingredients);
        while let Some(node) = topo.next(&self.mainline.ingredients) {
            if node == self.mainline.source {
                continue;
            }
            if !self.added.contains_key(&node) {
                // not new
                continue;
            }
            topo_list.push(node);
        }

        for node in topo_list {
            let domain = self.added.get(&node).unwrap().unwrap_or_else(|| {
                // new node that doesn't belong to a domain
                // create a new domain just for that node
                self.add_domain()
            });

            // identity mapping
            domain_remap.entry(domain).or_insert_with(HashMap::new).insert(node, node);

            let parents: Vec<_> = self.mainline
                .ingredients
                .neighbors_directed(node, petgraph::EdgeDirection::Incoming)
                .collect(); // collect so we can mutate mainline.ingredients

            for parent in parents {
                if parent == self.mainline.source ||
                   domain != self.mainline.ingredients[parent].domain().unwrap() {
                    // parent is in a different domain
                    // create an ingress node to handle that
                    let proxy = self.mainline.ingredients[parent]
                        .mirror(node::Type::Ingress(domain));
                    let ingress = self.mainline.ingredients.add_node(proxy);

                    // that ingress node also needs to run before us
                    domain_nodes.entry(domain).or_insert_with(Vec::new).push(ingress);

                    // we also need to make sure there's a channel to reach the domain on
                    // we're also going to need to tell any egress node that will send to this
                    // node about the channel it should use. we'll do that later, so just keep
                    // track of this work for later for now.
                    if !rxs.contains_key(&domain) {
                        let (tx, rx) = mpsc::sync_channel(10);
                        rxs.insert(domain, rx);
                        txs.insert(domain, tx);
                    }
                    targets.insert(ingress, txs[&domain].clone());

                    // note that, since we are traversing in topological order, our parent in this
                    // case should either be the source node, or it should be an egress node!
                    if cfg!(debug_assertions) && parent != self.mainline.source {
                        if let node::Type::Egress(..) = *self.mainline.ingredients[parent] {
                        } else {
                            unreachable!("parent of ingress is not an egress");
                        }
                    }

                    // anything that used to depend on the *parent* of the egress node above us
                    // should now depend on this ingress node instead
                    if parent != self.mainline.source {
                        let original = self.mainline
                            .ingredients
                            .neighbors_directed(parent, petgraph::EdgeDirection::Incoming)
                            .next()
                            .unwrap();
                        domain_remap.entry(domain)
                            .or_insert_with(HashMap::new)
                            .insert(original, ingress);
                    }

                    // we need to hook the ingress node in between us and this parent
                    let old = self.mainline.ingredients.find_edge(parent, node).unwrap();
                    let was_materialized = self.mainline.ingredients.remove_edge(old).unwrap();
                    self.mainline.ingredients.add_edge(parent, ingress, false);
                    self.mainline.ingredients.add_edge(ingress, node, was_materialized);
                }
            }

            // all user-supplied nodes are internal
            self.mainline.ingredients[node].add_to(domain);

            // let node process the fact that its parents may have changed
            if let Some(remap) = domain_remap.get(&domain) {
                self.mainline
                    .ingredients
                    .node_weight_mut(node)
                    .unwrap()
                    .on_commit(node, remap);
            } else {
                self.mainline
                    .ingredients
                    .node_weight_mut(node)
                    .unwrap()
                    .on_commit(node, &HashMap::new());
            }

            // in this domain, run this node after any parent ingress nodes we may have added
            domain_nodes.entry(domain).or_insert_with(Vec::new).push(node);

            let children: Vec<_> = self.mainline
                .ingredients
                .neighbors_directed(node, petgraph::EdgeDirection::Outgoing)
                .collect(); // collect so we can mutate mainline.ingredients

            for child in children {
                let cdomain = self.mainline.ingredients[child]
                    .domain()
                    .or_else(|| self.added.get(&child).unwrap().clone());
                if cdomain.is_none() || domain != cdomain.unwrap() {
                    // child is in a different domain
                    // create an egress node to handle that
                    // NOTE: technically, this doesn't need to mirror its parent, but meh
                    let proxy = self.mainline.ingredients[node]
                        .mirror(node::Type::Egress(domain, Default::default()));
                    let egress = self.mainline.ingredients.add_node(proxy);

                    // we need to hook that node in between us and this child
                    let old = self.mainline.ingredients.find_edge(node, child).unwrap();
                    let was_materialized = self.mainline.ingredients.remove_edge(old).unwrap();
                    self.mainline.ingredients.add_edge(node, egress, false);
                    self.mainline.ingredients.add_edge(egress, child, was_materialized);
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
        // println!("{}", self.mainline);

        // first, start up all the domains
        for (domain, nodes) in domain_nodes {
            self.boot_domain(domain, nodes, rxs.remove(&domain).unwrap());
        }
        drop(rxs);
        drop(txs); // all necessary copies are in targets

        // then, hook up the channels to new ingress nodes
        let mut sources = HashMap::new();
        for (ingress, tx) in targets {
            // this node is of type ingress, but since the domain has since claimed ownership of
            // the node, it is now just a node::Type::Taken
            // any egress with an edge to this node needs to have a tx clone added to its tx
            // channel set.
            for egress in self.mainline
                .ingredients
                .neighbors_directed(ingress, petgraph::EdgeDirection::Incoming) {

                if egress == self.mainline.source {
                    // input node
                    debug_assert_eq!(self.mainline
                                         .ingredients
                                         .neighbors_directed(ingress,
                                                             petgraph::EdgeDirection::Incoming)
                                         .count(),
                                     1);

                    // the node index the user knows about is that of the original node
                    let idx = self.mainline
                        .ingredients
                        .neighbors_directed(ingress, petgraph::EdgeDirection::Outgoing)
                        .next()
                        .unwrap();

                    // we want to avoid forcing the end-user to cook up Messages
                    // they should instead just make Us
                    let src = self.mainline.source;
                    sources.insert(idx,
                                   Box::new(move |u: Vec<query::DataType>| {
                        tx.send(Message {
                                from: src,
                                to: ingress,
                                data: u.into(),
                            })
                            .unwrap()

                    }) as Box<Fn(_) + Send>);
                    break;
                }

                if let node::Type::Egress(_, ref txs) = *self.mainline.ingredients[egress] {
                    // connected to egress from other domain
                    txs.lock().unwrap().push((ingress, tx.clone()));
                    continue;
                }

                unreachable!("ingress parent is not egress");
            }
        }

        // TODO: getters
        (sources, HashMap::new())
    }
}
