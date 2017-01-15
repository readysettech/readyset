use petgraph;
use petgraph::graph::NodeIndex;
use query;
use ops;
use checktable;

use std::sync::mpsc;
use std::sync::{Arc, Mutex};

use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt;

pub mod domain;
pub mod prelude;
pub mod node;
mod migrate;

pub type Edge = bool; // should the edge be materialized?

/// A Message exchanged over an edge in the graph.
#[derive(Clone)]
pub struct Message {
    pub from: NodeAddress,
    pub to: NodeAddress,
    pub data: prelude::Records,
    pub ts: Option<(i64, NodeIndex)>,
    pub token: Option<(checktable::Token, mpsc::Sender<checktable::TransactionResult>)>,
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
    /// Construct a new node from this node that will be given to the domain running this node.
    /// Whatever is left behind in self is what remains observable in the graph.
    fn take(&mut self) -> Box<Ingredient>;

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
                from: NodeAddress,
                data: ops::Records,
                domain: &prelude::DomainNodes,
                states: &prelude::StateMap)
                -> ops::Records;

    fn can_query_through(&self) -> bool {
        false
    }

    fn query_through<'a>(&self,
                         _column: usize,
                         _value: &'a query::DataType,
                         _states: &'a prelude::StateMap)
                         -> Option<Box<Iterator<Item = &'a Arc<Vec<query::DataType>>> + 'a>> {
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
                  -> Option<Box<Iterator<Item = &'a Arc<Vec<query::DataType>>> + 'a>> {
        states.get(parent.as_local())
            .map(move |state| Box::new(state.lookup(column, value).iter()) as Box<_>)
            .or_else(|| {
                // this is a long-shot.
                // if our ancestor can be queried *through*, then we just use that state instead
                let parent = domain.get(parent.as_local()).unwrap().borrow();
                if parent.is_internal() {
                    parent.query_through(column, value, states)
                } else {
                    None
                }
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
    checktable: Arc<Mutex<checktable::CheckTable>>,

    control_txs: HashMap<domain::Index, mpsc::SyncSender<domain::Control>>,
    data_txs: HashMap<domain::Index, mpsc::SyncSender<Message>>,
    time_txs: HashMap<domain::Index, mpsc::SyncSender<i64>>,
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
            checktable: Arc::new(Mutex::new(checktable::CheckTable::new())),

            control_txs: HashMap::default(),
            data_txs: HashMap::default(),
            time_txs: HashMap::default(),
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

    /// Get a boxed function which can be used to validate tokens.
    pub fn get_validator(&self) -> Box<Fn(&checktable::Token) -> bool> {
        let checktable = self.checktable.clone();
        return Box::new(move |ref t: &checktable::Token| {
            checktable.lock().unwrap().validate_token(&t)
        });
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


/// A FnTX is a function with which a transactional write can be submitted.
pub type FnTX = Box<Fn(Vec<query::DataType>, checktable::Token) ->
                    checktable::TransactionResult + Send + 'static>;

// A FnNTX is a function with which a non-transactional write can be submitted.
pub type FnNTX = Box<Fn(Vec<query::DataType>) + Send + 'static>;



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
            let base_parents: Vec<_> = self.mainline
                .ingredients
                .neighbors_directed(self.mainline.source, petgraph::EdgeDirection::Outgoing)
                .filter(|b| {
                    petgraph::algo::has_path_connecting(&self.mainline.ingredients,
                                                        *b,
                                                        *n.as_global(),
                                                        None)
                })
                .collect();

            let r = node::Reader::new(checktable::TokenGenerator::new(base_parents, vec![]));
            let r = node::Type::Reader(None, r);
            let r = self.mainline.ingredients[*n.as_global()].mirror(r);
            let r = self.mainline.ingredients.add_node(r);
            self.mainline.ingredients.add_edge(*n.as_global(), r, false);
            self.readers.insert(*n.as_global(), r);
        }
    }

    fn reader_for(&self, n: NodeAddress) -> &node::Reader {
        let ri = self.readers[n.as_global()];
        if let node::Type::Reader(_, ref inner) = *self.mainline.ingredients[ri] {
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

        if let node::Type::Reader(ref mut wh, ref mut inner) = *self.mainline.ingredients[ri] {
            if inner.state.is_none() {
                use backlog;
                let (r, w) = backlog::new(cols, key).commit();
                inner.state = Some(r);
                *wh = Some(w);
            }

            // cook up a function to query this materialized state
            let arc = inner.state.as_ref().unwrap().clone();
            Box::new(move |q: &query::DataType| -> ops::Datas {
                arc.find_and(q,
                              |rs| rs.into_iter().map(|v| (&**v).clone()).collect::<Vec<_>>())
                    .0
            })
        } else {
            unreachable!("tried to use non-reader node as a reader")
        }
    }

    /// Set up the given node such that its output can be efficiently queried, and the results can
    /// be used in transactions.
    ///
    /// The returned function can be called with a `Query`, and any matching results will be
    /// returned, along with a token.
    pub fn transactional_maintain
        (&mut self,
         n: NodeAddress,
         key: usize)
         -> Box<Fn(&query::DataType) -> (ops::Datas, checktable::Token) + Send + Sync> {
        self.ensure_reader_for(n);
        let ri = self.readers[n.as_global()];

        // we need to do these here because we'll mutably borrow self.mainline in the if let
        let cols = self.mainline.ingredients[ri].fields().len();

        if let node::Type::Reader(ref mut wh, ref mut inner) = *self.mainline.ingredients[ri] {
            if inner.state.is_none() {
                use backlog;
                let (r, w) = backlog::new(cols, key).commit();
                inner.state = Some(r);
                *wh = Some(w);
            }

            // cook up a function to query this materialized state
            let arc = inner.state.as_ref().unwrap().clone();
            let generator = inner.token_generator.clone();
            Box::new(move |q: &query::DataType| -> (ops::Datas, checktable::Token) {
                let (res, ts) = arc.find_and(q, |rs| {
                    rs.into_iter().map(|v| (&**v).clone()).collect::<Vec<_>>()
                });
                let token = generator.generate(ts, q.clone());
                (res, token)
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
    pub fn stream(&mut self, n: NodeAddress) -> mpsc::Receiver<prelude::Records> {
        self.ensure_reader_for(n);
        let (tx, rx) = mpsc::channel();
        self.reader_for(n).streamers.lock().unwrap().push(tx);
        rx
    }

    /// Commit the changes introduced by this `Migration` to the master `Soup`.
    ///
    /// This will spin up an execution thread for each new thread domain, and hook those new
    /// domains into the larger Soup graph. The returned map contains entry points through which
    /// new updates should be sent to introduce them into the Soup.
    pub fn commit(self) -> HashMap<NodeAddress, (FnNTX, FnTX)> {
        let mut new = HashSet::new();
        let mut changed_domains = HashSet::new();

        let mainline = self.mainline;

        // Make sure all new nodes are assigned to a domain
        for (node, domain) in self.added {
            let domain = domain.unwrap_or_else(|| {
                // new node that doesn't belong to a domain
                // create a new domain just for that node
                // NOTE: this is the same code as in add_domain(), but we can't use self here
                mainline.ndomains += 1;
                (mainline.ndomains - 1).into()

            });
            mainline.ingredients[node].add_to(domain);
            new.insert(node);
            changed_domains.insert(domain);
        }

        // Readers are nodes too.
        // And they should be assigned the same domain as their parents
        for (parent, reader) in self.readers {
            let domain = mainline.ingredients[parent].domain();
            mainline.ingredients[reader].add_to(domain);
            new.insert(reader);
        }

        // Set up ingress and egress nodes
        let mut swapped =
            migrate::routing::add(&mut mainline.ingredients, mainline.source, &mut new);

        // Find all domains that have changed
        let mut domain_nodes = mainline.ingredients
            .node_indices()
            .filter(|&ni| ni != mainline.source)
            .filter_map(|ni| {
                let domain = mainline.ingredients[ni].domain();
                if changed_domains.contains(&domain) {
                    Some((domain, ni, new.contains(&ni)))
                } else {
                    None
                }
            })
            .fold(HashMap::new(), |mut dns, (d, ni, new)| {
                dns.entry(d).or_insert_with(Vec::new).push((ni, new));
                dns
            });

        let mut rxs = HashMap::new();
        let mut time_rxs = HashMap::new();

        // Set up control and data channels for new domains
        for (domain, _) in &mut domain_nodes {
            if !mainline.data_txs.contains_key(domain) {
                let (tx, rx) = mpsc::sync_channel(10);
                rxs.insert(*domain, rx);
                mainline.data_txs.insert(*domain, tx);
            }

            if !mainline.time_txs.contains_key(domain) {
                let (tx, rx) = mpsc::sync_channel(10);
                time_rxs.insert(*domain, rx);
                mainline.time_txs.insert(*domain, tx);
            }
        }

        // Add transactional time nodes
        let new_time_egress = migrate::transactions::add_time_nodes(&mut domain_nodes,
                                                                    &mut mainline.ingredients,
                                                                    &mainline.time_txs);

        // Assign local addresses to all new nodes, and initialize them
        for (domain, nodes) in &mut domain_nodes {
            // Number of pre-existing nodes
            let mut nnodes = nodes.iter().filter(|&&(_, new)| !new).count();

            // Give local addresses to every (new) node
            for &(ni, new) in nodes.iter() {
                if new {
                    mainline.ingredients[ni].set_addr(NodeAddress::make_local(nnodes));
                    nnodes += 1;
                }
            }

            // Figure out all the remappings that have happened
            let mut remap = HashMap::new();
            // The global address of each node in this domain is now a local one
            for &(ni, _) in nodes.iter() {
                remap.insert(NodeAddress::make_global(ni),
                             mainline.ingredients[ni].addr());
            }
            // Parents in other domains have been swapped for ingress nodes.
            // Those ingress nodes' indices are now local.
            for (from, to) in swapped.remove(domain).unwrap_or_else(HashMap::new) {
                remap.insert(NodeAddress::make_global(from),
                             mainline.ingredients[to].addr());
            }

            // Initialize each new node
            for &(ni, new) in nodes.iter() {
                if new && mainline.ingredients[ni].is_internal() {
                    mainline.ingredients.node_weight_mut(ni).unwrap().on_commit(&remap);
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
        // println!("{}", mainline);

        // Determine what nodes to materialize
        let index = domain_nodes.iter()
            .map(|(domain, ref nodes)| {
                let mat = migrate::materialization::pick(&mainline.ingredients, &nodes[..]);
                let idx = migrate::materialization::index(&mainline.ingredients, &nodes[..], mat);
                (*domain, idx)
            })
            .collect();

        // Boot up new domains (they'll ignore all updates for now)
        for domain in changed_domains {
            if mainline.control_txs.contains_key(&domain) {
                // this is not a new domain
                continue;
            }

            // Start up new domain
            let ctx = migrate::booting::boot_new(&mut mainline.ingredients,
                                                 mainline.source,
                                                 domain_nodes.remove(&domain).unwrap(),
                                                 mainline.checktable.clone(),
                                                 rxs.remove(&domain).unwrap(),
                                                 time_rxs.remove(&domain).unwrap());
            mainline.control_txs.insert(domain, ctx);
        }
        drop(rxs);

        // Add any new nodes to existing domains (they'll also ignore all updates for now)
        migrate::augmentation::inform(&mut mainline.ingredients,
                                      &mut mainline.control_txs,
                                      domain_nodes);

        // Set up inter-domain connections
        // NOTE: once we do this, we are making existing domains block on new domains!
        migrate::routing::connect(&mut mainline.ingredients, &mainline.data_txs, &new);

        // And now, the last piece of the puzzle -- set up materializations
        migrate::materialization::initialize(&mainline.ingredients,
                                             mainline.source,
                                             &new,
                                             index,
                                             &mut mainline.control_txs);

        // Finally, set up input channels to any new base tables
        let src = NodeAddress::make_global(mainline.source);
        let mut sources = HashMap::new();
        for node in &new {
            let n = &mainline.ingredients[*node];
            if let node::Type::Ingress = **n {
                // check the egress connected to this ingress
            } else {
                continue;
            }

            for egress in mainline.ingredients
                .neighbors_directed(*node, petgraph::EdgeDirection::Incoming) {
                if egress != mainline.source {
                    continue;
                }

                // we want to avoid forcing the end-user to cook up Messages
                // they should instead just make data records
                let addr = n.addr();
                let tx = mainline.data_txs[&n.domain()].clone();
                let tx2 = tx.clone();
                let ftx: Box<Fn(_, _) -> checktable::TransactionResult + Send> =
                    Box::new(move |u: Vec<query::DataType>, t: checktable::Token| {
                        let (send, recv) = mpsc::channel();
                        tx.send(Message {
                                from: src,
                                to: addr,
                                data: vec![u].into(),
                                ts: None,
                                token: Some((t, send)),
                            })
                            .unwrap();
                        recv.recv().unwrap()
                    });

                let fntx: Box<Fn(_) + Send> = Box::new(move |u: Vec<query::DataType>| {
                    tx2.send(Message {
                            from: src,
                            to: addr,
                            data: vec![u].into(),
                            ts: None,
                            token: None,
                        })
                        .unwrap()
                });

                // the user is expecting to use the base node addresses to choose a particular
                // putter, not the ingress node addresses
                let base = mainline.ingredients
                    .neighbors_directed(*node, petgraph::EdgeDirection::Outgoing)
                    .next()
                    .unwrap();

                sources.insert(NodeAddress::make_global(base), (fntx, ftx));
                break;
            }
        }

        sources
    }
}
