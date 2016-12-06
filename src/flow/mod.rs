use petgraph;
use petgraph::graph::NodeIndex;
use query;
use ops;

use std::sync::mpsc;
use std::sync;

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
    pub from: NodeAddress,
    pub to: NodeAddress,
    pub data: U,
    pub ts: Option<(i64, NodeIndex)>,
}

/// A domain-local node identifier.
#[derive(Eq, PartialEq, Ord, PartialOrd, Hash, Clone, Copy, Debug)]
pub struct LocalNodeIndex {
    id: usize, // not a tuple struct so this field can be made private
}

impl LocalNodeIndex {
    pub fn id(&self) -> usize {
        self.id
    }
}

#[derive(Eq, PartialEq, Ord, PartialOrd, Hash, Clone, Copy, Debug)]
enum NodeAddress_ {
    Global(NodeIndex),
    Local(LocalNodeIndex), // XXX: maybe include domain here?
}

#[derive(Eq, PartialEq, Ord, PartialOrd, Hash, Clone, Copy, Debug)]
pub struct NodeAddress {
    addr: NodeAddress_, // wrap the enum so people can't create these accidentally
}

impl NodeAddress {
    fn make_local(id: usize) -> NodeAddress {
        NodeAddress { addr: NodeAddress_::Local(LocalNodeIndex { id: id }) }
    }

    fn make_global(id: NodeIndex) -> NodeAddress {
        NodeAddress { addr: NodeAddress_::Global(id) }
    }

    #[cfg(test)]
    pub fn mock_local(id: usize) -> NodeAddress {
        Self::make_local(id)
    }

    #[cfg(test)]
    pub fn mock_global(id: NodeIndex) -> NodeAddress {
        Self::make_global(id)
    }
}

impl fmt::Display for NodeAddress {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.addr {
            NodeAddress_::Global(ref ni) => write!(f, "g{}", ni.index()),
            NodeAddress_::Local(ref ni) => write!(f, "l{}", ni.id),
        }
    }
}

impl NodeAddress {
    pub fn as_global(&self) -> &NodeIndex {
        match self.addr {
            NodeAddress_::Global(ref ni) => ni,
            NodeAddress_::Local(_) => unreachable!("tried to use local address as global"),
        }
    }

    pub fn as_local(&self) -> &LocalNodeIndex {
        match self.addr {
            NodeAddress_::Local(ref i) => i,
            NodeAddress_::Global(_) => unreachable!("tried to use global address as local"),
        }
    }
}

pub trait Ingredient
    where Self: Send
{
    fn ancestors(&self) -> Vec<NodeAddress>;
    fn should_materialize(&self) -> bool;

    /// Should return true if this ingredient will ever query the state of an ancestor.
    fn will_query(&self, materialized: bool) -> bool;

    /// Suggest fields of this view, or its ancestors, that would benefit from having an index.
    fn suggest_indexes(&self, you: NodeAddress) -> HashMap<NodeAddress, usize>;

    /// Resolve where the given field originates from. If the view is materialized, or the value is
    /// otherwise created by this view, None should be returned.
    fn resolve(&self, i: usize) -> Option<Vec<(NodeAddress, usize)>>;

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

    /// Called when a node is first connected to the graph.
    ///
    /// All its ancestors are present, but this node and its children may not have been connected
    /// yet. Only addresses of the type `NodeAddress::Global` may be used.
    fn on_connected(&mut self, graph: &prelude::Graph);

    /// Called when a domain is finalized and is about to be booted.
    ///
    /// The provided arguments give mappings from global to local addresses. After this method has
    /// been invoked (and crucially, in `Ingredient::on_input`) only addresses of the type
    /// `NodeAddress::Local` may be used.
    fn on_commit(&mut self,
                 you: prelude::NodeAddress,
                 remap: &HashMap<prelude::NodeAddress, prelude::NodeAddress>);

    /// Process a single incoming message, optionally producing an update to be propagated to
    /// children.
    ///
    /// Only addresses of the type `NodeAddress::Local` may be used in this function.
    fn on_input(&mut self,
                input: Message,
                domain: &prelude::DomainNodes,
                states: &prelude::StateMap)
                -> Option<U>;

    fn can_query_through(&self) -> bool {
        false
    }

    fn query_through<'a>
        (&self,
         _column: usize,
         _value: &'a query::DataType,
         _states: &'a prelude::StateMap)
         -> Option<Box<Iterator<Item = &'a sync::Arc<Vec<query::DataType>>> + 'a>> {
        None
    }

    /// Process a single incoming message, optionally producing an update to be propagated to
    /// children.
    ///
    /// Only addresses of the type `NodeAddress::Local` may be used in this function.
    fn lookup<'a>(&self,
                  parent: prelude::NodeAddress,
                  column: usize,
                  value: &'a query::DataType,
                  domain: &prelude::DomainNodes,
                  states: &'a prelude::StateMap)
                  -> Option<Box<Iterator<Item = &'a sync::Arc<Vec<query::DataType>>> + 'a>> {
        states.get(parent.as_local())
            .map(move |state| Box::new(state.lookup(column, value).iter()) as Box<_>)
            .or_else(|| {
                // this is a long-shot.
                // if our ancestor can be queried *through*, then we just use that state instead
                domain.get(parent.as_local()).unwrap().borrow().query_through(column, value, states)
            })
    }
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

impl Default for Blender {
    fn default() -> Self {
        let mut g = petgraph::Graph::new();
        let source =
            g.add_node(node::Node::new("source", &["because-type-inference"], node::Type::Source));
        Blender {
            ingredients: g,
            source: source,
            ndomains: 0,
        }
    }
}

impl Blender {
    /// Construct a new, empty `Blender`
    pub fn new() -> Self {
        Blender::default()
    }

    /// Start setting up a new `Migration`.
    pub fn start_migration(&mut self) -> Migration {
        Migration {
            mainline: self,
            added: Default::default(),
            materialize: Default::default(),
            readers: Default::default(),
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
    readers: HashMap<NodeIndex, NodeIndex>,
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
    pub fn add_ingredient<S1, FS, S2, I>(&mut self, name: S1, fields: FS, i: I) -> NodeAddress
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
                self.mainline.ingredients.add_edge(*parent.as_global(), ni, false);
            }
        }
        // and tell the caller its id
        NodeAddress::make_global(ni)
    }

    /// Mark the edge between `src` and `dst` in the graph as requiring materialization.
    ///
    /// The reason this is placed per edge rather than per node is that only some children of a
    /// node may require materialization of their inputs (i.e., only those that will query along
    /// this edge). Since we must materialize the output of a node in a foreign domain once for
    /// every receiving domain, this can save us some space if a child that doesn't require
    /// materialization is in its own domain. If multiple nodes in the same domain require
    /// materialization of the same parent, that materialized state will be shared.
    pub fn materialize(&mut self, src: NodeAddress, dst: NodeAddress) {
        // TODO
        // what about if a user tries to materialize a cross-domain edge that has already been
        // converted to an egress/ingress pair?
        let e = self.mainline
            .ingredients
            .find_edge(*src.as_global(), *dst.as_global())
            .expect("asked to materialize non-existing edge");

        let mut e = self.mainline.ingredients.edge_weight_mut(e).unwrap();
        if !*e {
            *e = true;
            // it'd be nice if we could just store the EdgeIndex here, but unfortunately that's not
            // guaranteed by petgraph to be stable in the presence of edge removals (which we do in
            // commit())
            self.materialize.insert((*src.as_global(), *dst.as_global()));
        }
    }

    /// Assign the ingredient with identifier `n` to the thread domain `d`.
    ///
    /// `n` must be have been added in this migration.
    pub fn assign_domain(&mut self, n: NodeAddress, d: domain::Index) {
        // TODO: what if a node is added to an *existing* domain?
        assert_eq!(self.added.insert(*n.as_global(), Some(d)).unwrap(), None);
    }

    fn ensure_reader_for(&mut self, n: NodeAddress) {
        if !self.readers.contains_key(n.as_global()) {
            // make a reader
            let r = node::Reader::default();
            let r = node::Type::Reader(None, None, r);
            let r = self.mainline.ingredients[*n.as_global()].mirror(r);
            let r = self.mainline.ingredients.add_node(r);
            self.mainline.ingredients.add_edge(*n.as_global(), r, false);
            self.readers.insert(*n.as_global(), r);
        }
    }

    fn reader_for(&self, n: NodeAddress) -> &node::Reader {
        let ri = self.readers[n.as_global()];
        if let node::Type::Reader(_, _, ref inner) = *self.mainline.ingredients[ri] {
            &*inner
        } else {
            unreachable!("tried to use non-reader node as a reader")
        }
    }

    /// Set up the given node such that its output can be efficiently queried.
    ///
    /// The returned function can be called with a `Query`, and any matching results will be
    /// returned.
    pub fn maintain(&mut self,
                    n: NodeAddress,
                    key: usize)
                    -> Box<Fn(&query::DataType) -> ops::Datas + Send + Sync> {
        self.ensure_reader_for(n);

        let ri = self.readers[n.as_global()];

        // we need to do these here because we'll mutably borrow self.mainline in the if let
        let cols = self.mainline.ingredients[ri].fields().len();

        if let node::Type::Reader(_, ref mut wh, ref mut inner) = *self.mainline.ingredients[ri] {
            if inner.state.is_none() {
                use backlog;
                let (r, w) = backlog::new(cols, key).commit();
                inner.state = Some(r);
                *wh = Some(w);
            }

            // cook up a function to query this materialized state
            let arc = inner.state.as_ref().unwrap().clone();
            Box::new(move |q: &query::DataType| -> ops::Datas {
                arc.find_and(q, |rs| {
                    // without projection, we wouldn't need to clone here
                    // because we wouldn't need the "feed" below
                    rs.into_iter().map(|v| (&**v).clone()).collect::<Vec<_>>()
                })
            })
        } else {
            unreachable!("tried to use non-reader node as a reader")
        }
    }

    /// Obtain a channel that is fed by the output stream of the given node.
    ///
    /// As new updates are processed by the given node, its outputs will be streamed to the
    /// returned channel. Node that this channel is *not* bounded, and thus a receiver that is
    /// slower than the system as a hole will accumulate a large buffer over time.
    pub fn stream(&mut self, n: NodeAddress) -> mpsc::Receiver<prelude::Update> {
        self.ensure_reader_for(n);
        let (tx, rx) = mpsc::channel();
        self.reader_for(n).streamers.lock().unwrap().push(tx);
        rx
    }

    fn boot_domain(&mut self,
                   d: domain::Index,
                   nodes: Vec<(NodeIndex, NodeAddress)>,
                   base_nodes: &Vec<NodeIndex>,
                   rx: mpsc::Receiver<Message>) {
        let d = domain::Domain::from_graph(d, nodes, base_nodes, &mut self.mainline.ingredients);
        d.boot(rx);
    }

    /// Commit the changes introduced by this `Migration` to the master `Soup`.
    ///
    /// This will spin up an execution thread for each new thread domain, and hook those new
    /// domains into the larger Soup graph. The returned map contains entry points through which
    /// new updates should be sent to introduce them into the Soup.
    pub fn commit(mut self)
                  -> HashMap<NodeAddress, Box<Fn(Vec<query::DataType>) + Send + 'static>> {
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
                // not new, or a reader
                continue;
            }
            topo_list.push(node);
        }

        // Map from base node index to the index of the associated timestamp egress node.
        let mut time_egress_nodes: HashMap<NodeIndex, NodeIndex> = HashMap::new();
        let mut base_nodes: Vec<NodeIndex> = Vec::new();

        for node in topo_list {
            let domain = self.added[&node].unwrap_or_else(|| {
                // new node that doesn't belong to a domain
                // create a new domain just for that node
                self.add_domain()
            });

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
                    let no = NodeAddress::make_local(domain_nodes.entry(domain)
                        .or_insert_with(Vec::new)
                        .len());
                    domain_nodes.get_mut(&domain).unwrap().push((ingress, no));

                    // we also need to make sure there's a channel to reach the domain on
                    // we're also going to need to tell any egress node that will send to this
                    // node about the channel it should use. we'll do that later, so just keep
                    // track of this work for later for now.
                    if !rxs.contains_key(&domain) {
                        let (tx, rx) = mpsc::sync_channel(10);
                        rxs.insert(domain, rx);
                        txs.insert(domain, tx);
                    }
                    targets.insert(ingress, (no, txs[&domain].clone()));

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
                            .insert(NodeAddress::make_global(original), no);
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

            // assign the node a local identifier
            let no =
                NodeAddress::make_local(domain_nodes.entry(domain).or_insert_with(Vec::new).len());

            // record mapping to local id
            domain_remap.entry(domain)
                .or_insert_with(HashMap::new)
                .insert(NodeAddress::make_global(node), no);

            // in this domain, run this node after any parent ingress nodes we may have added
            domain_nodes.get_mut(&domain).unwrap().push((node, no));

            // let node process the fact that its parents may have changed
            if let Some(remap) = domain_remap.get(&domain) {
                self.mainline
                    .ingredients
                    .node_weight_mut(node)
                    .unwrap()
                    .on_commit(no, remap);
            } else {
                self.mainline
                    .ingredients
                    .node_weight_mut(node)
                    .unwrap()
                    .on_commit(no, &HashMap::new());
            }

            let children: Vec<_> = self.mainline
                .ingredients
                .neighbors_directed(node, petgraph::EdgeDirection::Outgoing)
                .collect(); // collect so we can mutate mainline.ingredients

            for child in children {
                if let node::Type::Reader(ref mut rd, _, _) = *self.mainline.ingredients[child] {
                    // readers are always in the same domain as their parent
                    // they also only ever have a single parent (i.e., the node they read)
                    // because of this, we know that we will only hit this if *exactly once*
                    // per reader. thus, this is a perfectly fine place to initialize the reader
                    // (which, in our case, just means adding it to the domain's list of nodes).
                    //
                    // XXX: this is not true if a reader was added to a node that existed before a
                    // migration. but lots of things are broken for migration. this seems like a
                    // minor case.
                    *rd = Some(domain);
                    let no = NodeAddress::make_local(domain_nodes[&domain].len());
                    domain_nodes.get_mut(&domain).unwrap().push((child, no));
                    continue;
                }

                let cdomain = self.mainline.ingredients[child]
                    .domain()
                    .or_else(|| self.added[&child]);
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
                    let no = NodeAddress::make_local(domain_nodes[&domain].len());
                    domain_nodes.get_mut(&domain).unwrap().push((egress, no));
                }
            }

            // Determine if this node is a base node, and if so add a TimestampEgress node below it.
            let is_base = if let node::Type::Internal(_, ref ingredient) = *self.mainline.ingredients[node] {
                ingredient.is_base()
            } else {
                false
            };

            if is_base {
                base_nodes.push(node);

                let proxy = self.mainline.ingredients[node]
                    .mirror(node::Type::TimestampEgress(domain));
                let time_egress = self.mainline.ingredients.add_node(proxy);

                // we need to hook that node
                self.mainline.ingredients.add_edge(node, time_egress, false);
                // that egress node also needs to run
                let no = NodeAddress::make_local(domain_nodes[&domain].len());
                domain_nodes.get_mut(&domain).unwrap().push((time_egress, no));

                time_egress_nodes.insert(node, time_egress);
            }
        }

        // Create a timestamp ingress node for each domain, and connect it to each of the timestamp
        // egress nodes associated with a base node that does not (perhaps transitively) send
        // updates to the domain.
        for (domain, mut nodes) in domain_nodes.iter_mut() {
            // TODO: set proper name for new node.
            let proxy = node::Node::new::<_,Vec<String>,_>("ts-ingress-node", vec![],
                                                           node::Type::TimestampIngress(*domain));
            let time_ingress = self.mainline.ingredients.add_node(proxy);

            // Place the new node into domain_nodes.
            let no = NodeAddress::make_local(nodes.len());
            nodes.push((time_ingress, no));

            for (base, time_egress) in time_egress_nodes.iter() {
                let path_exists = nodes.iter().any(|&(node, _)| {
                    petgraph::algo::has_path_connecting(&self.mainline.ingredients, *base, node, None)
                });

                if !path_exists {
                    self.mainline.ingredients.add_edge(*time_egress, time_ingress, false);
                }

            }
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
            self.boot_domain(domain, nodes, &base_nodes, rxs.remove(&domain).unwrap());
        }
        drop(rxs);
        drop(txs); // all necessary copies are in targets

        // then, hook up the channels to new ingress nodes
        let src = NodeAddress::make_global(self.mainline.source);
        let mut sources = HashMap::new();
        for (ingress, (no, tx)) in targets {
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
                    sources.insert(NodeAddress::make_global(idx),
                                   Box::new(move |u: Vec<query::DataType>| {
                        tx.send(Message {
                                from: src,
                                to: no,
                                data: u.into(),
                                ts: None,
                            })
                            .unwrap()

                    }) as Box<Fn(_) + Send>);
                    break;
                }

                if let node::Type::Egress(_, ref txs) = *self.mainline.ingredients[egress] {
                    // connected to egress from other domain
                    txs.lock().unwrap().push((no, tx.clone()));
                    continue;
                }

                unreachable!("ingress parent is not egress");
            }
        }

        sources
    }
}
