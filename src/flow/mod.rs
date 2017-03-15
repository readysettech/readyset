pub mod sql_to_flow;
pub mod data;
mod sql;

use petgraph;
use petgraph::graph::NodeIndex;
use ops;
use checktable;

use std::sync::mpsc;
use std::sync::{Arc, Mutex};

use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt;
use std::time;

use slog;

pub mod domain;
pub mod prelude;
pub mod node;
pub mod payload;
pub mod statistics;
mod migrate;
mod transactions;

const NANOS_PER_SEC: u64 = 1_000_000_000;
macro_rules! dur_to_ns {
    ($d:expr) => {{
        let d = $d;
        d.as_secs() * NANOS_PER_SEC + d.subsec_nanos() as u64
    }}
}

pub type Edge = bool; // should the edge be materialized?

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

#[derive(Eq, PartialEq, Ord, PartialOrd, Hash, Clone, Copy)]
enum NodeAddress_ {
    Global(NodeIndex),
    Local(LocalNodeIndex), // XXX: maybe include domain here?
}

/// `NodeAddress` is a unique identifier that can be used to refer to nodes in the graph across
/// migrations.
#[derive(Eq, PartialEq, Ord, PartialOrd, Hash, Clone, Copy)]
pub struct NodeAddress {
    addr: NodeAddress_, // wrap the enum so people can't create these accidentally
}

impl fmt::Debug for NodeAddress {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.addr {
            NodeAddress_::Global(ref ni) => write!(f, "NodeAddress::Global({})", ni.index()),
            NodeAddress_::Local(ref li) => write!(f, "NodeAddress::Local({})", li.id()),
        }
    }
}

impl NodeAddress {
    fn make_local(id: usize) -> NodeAddress {
        NodeAddress { addr: NodeAddress_::Local(LocalNodeIndex { id: id }) }
    }

    fn make_global(id: NodeIndex) -> NodeAddress {
        NodeAddress { addr: NodeAddress_::Global(id) }
    }

    fn is_global(&self) -> bool {
        match self.addr {
            NodeAddress_::Global(_) => true,
            _ => false,
        }
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

impl Into<usize> for NodeAddress {
    fn into(self) -> usize {
        match self.addr {
            NodeAddress_::Global(ni) => ni.index(),
            _ => unreachable!(),
        }
    }
}

impl From<NodeIndex> for NodeAddress {
    fn from(o: NodeIndex) -> Self {
        NodeAddress { addr: NodeAddress_::Global(o) }
    }
}

impl From<usize> for NodeAddress {
    fn from(o: usize) -> Self {
        NodeAddress::from(NodeIndex::new(o))
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
    pub(crate) fn as_global(&self) -> &NodeIndex {
        match self.addr {
            NodeAddress_::Global(ref ni) => ni,
            NodeAddress_::Local(_) => unreachable!("tried to use local address as global"),
        }
    }

    pub(crate) fn as_local(&self) -> &LocalNodeIndex {
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

    /// May return a set of nodes such that *one* of the given ancestors *must* be the one to be
    /// replayed if this node's state is to be initialized.
    fn must_replay_among(&self, &HashSet<NodeAddress>) -> Option<HashSet<NodeAddress>> {
        None
    }

    /// Should return true if this ingredient will ever query the state of an ancestor.
    fn will_query(&self, materialized: bool) -> bool;

    /// Suggest fields of this view, or its ancestors, that would benefit from having an index.
    ///
    /// Note that a vector of length > 1 for any one node means that that node should be given a
    /// *compound* key, *not* that multiple columns should be independently indexed.
    fn suggest_indexes(&self, you: NodeAddress) -> HashMap<NodeAddress, Vec<usize>>;

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
                         _columns: &[usize],
                         _key: &prelude::KeyType<prelude::DataType>,
                         _states: &'a prelude::StateMap)
                         -> Option<Box<Iterator<Item = &'a Arc<Vec<prelude::DataType>>> + 'a>> {
        None
    }

    /// Process a single incoming message, optionally producing an update to be propagated to
    /// children.
    ///
    /// Only addresses of the type `NodeAddress::Local` may be used in this function.
    fn lookup<'a>(&self,
                  parent: prelude::NodeAddress,
                  columns: &[usize],
                  key: &prelude::KeyType<prelude::DataType>,
                  domain: &prelude::DomainNodes,
                  states: &'a prelude::StateMap)
                  -> Option<Box<Iterator<Item = &'a Arc<Vec<prelude::DataType>>> + 'a>> {
        states.get(parent.as_local())
            .map(move |state| Box::new(state.lookup(columns, key).iter()) as Box<_>)
            .or_else(|| {
                // this is a long-shot.
                // if our ancestor can be queried *through*, then we just use that state instead
                let parent = domain.get(parent.as_local()).unwrap().borrow();
                if parent.is_internal() {
                    parent.query_through(columns, key, states)
                } else {
                    None
                }
            })
    }

    // Translate a column in this ingredient into the corresponding column(s) in
    // parent ingredients. None for the column means that the parent doesn't
    // have an associated column. Similar to resolve, but does not depend on
    // materialization, and returns results even for computed columns.
    fn parent_columns(&self, column: usize) -> Vec<(NodeAddress, Option<usize>)>;

    /// Performance hint: should return true if this operator reduces the size of its input
    fn is_selective(&self) -> bool {
        false
    }
}

/// A `Mutator` is used to perform reads and writes to base nodes.
pub struct Mutator {
    src: NodeAddress,
    tx: mpsc::SyncSender<payload::Packet>,
    addr: NodeAddress,
    primary_key: Vec<usize>,
    tx_reply_channel: (mpsc::Sender<Result<i64, ()>>, mpsc::Receiver<Result<i64, ()>>),
}

impl Clone for Mutator {
    fn clone(&self) -> Self {
        Self {
            src: self.src.clone(),
            tx: self.tx.clone(),
            addr: self.addr.clone(),
            primary_key: self.primary_key.clone(),
            tx_reply_channel: mpsc::channel(),
        }
    }
}

impl Mutator {
    fn send(&self, r: prelude::Records) {
        let m = payload::Packet::Message {
            link: payload::Link::new(self.src, self.addr),
            data: r,
        };
        self.tx.clone().send(m).unwrap();
    }

    fn tx_send(&self, r: prelude::Records, t: checktable::Token) -> Result<i64, ()> {
        let send = self.tx_reply_channel.0.clone();
        let m = payload::Packet::Transaction {
            link: payload::Link::new(self.src, self.addr),
            data: r,
            state: payload::TransactionState::Pending(t, send),
        };
        self.tx.clone().send(m).unwrap();
        loop {
            match self.tx_reply_channel.1.try_recv() {
                Ok(r) => return r,
                Err(..) => {}
            }
        }
    }

    /// Perform a non-transactional write to the base node this Mutator was generated for.
    pub fn put<V>(&self, u: V)
        where V: Into<Vec<prelude::DataType>>
    {
        self.send(vec![u.into()].into())
    }

    /// Perform a transactional write to the base node this Mutator was generated for.
    pub fn transactional_put<V>(&self, u: V, t: checktable::Token) -> Result<i64, ()>
        where V: Into<Vec<prelude::DataType>>
    {
        self.tx_send(vec![u.into()].into(), t)
    }

    /// Perform a non-transactional delete frome the base node this Mutator was generated for.
    pub fn delete<I>(&self, key: I)
        where I: Into<Vec<prelude::DataType>>
    {
        self.send(vec![prelude::Record::DeleteRequest(key.into())].into())
    }

    /// Perform a transactional delete from the base node this Mutator was generated for.
    pub fn transactional_delete<I>(&self, key: I, t: checktable::Token) -> Result<i64, ()>
        where I: Into<Vec<prelude::DataType>>
    {
        self.tx_send(vec![prelude::Record::DeleteRequest(key.into())].into(), t)
    }

    /// Perform a non-transactional update (delete followed by put) to the base node this Mutator
    /// was generated for.
    pub fn update<V>(&self, u: V)
        where V: Into<Vec<prelude::DataType>>
    {
        assert!(!self.primary_key.is_empty(),
                "update operations can only be applied to base nodes with key columns");

        let u = u.into();
        self.send(vec![prelude::Record::DeleteRequest(self.primary_key
                           .iter()
                           .map(|&col| &u[col])
                           .cloned()
                           .collect()),
                       u.into()]
            .into())
    }

    /// Perform a transactional update (delete followed by put) to the base node this Mutator was
    /// generated for.
    pub fn transactional_update<V>(&self, u: V, t: checktable::Token) -> Result<i64, ()>
        where V: Into<Vec<prelude::DataType>>
    {
        assert!(!self.primary_key.is_empty(),
                "update operations can only be applied to base nodes with key columns");

        let u = u.into();
        let m = vec![prelude::Record::DeleteRequest(self.primary_key
                         .iter()
                         .map(|&col| &u[col])
                         .cloned()
                         .collect()),
                     u.into()]
            .into();
        self.tx_send(m, t)
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

    txs: HashMap<domain::Index, mpsc::SyncSender<payload::Packet>>,

    log: slog::Logger,
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

            txs: HashMap::default(),

            log: slog::Logger::root(slog::Discard, None),
        }
    }
}

impl Blender {
    /// Construct a new, empty `Blender`
    pub fn new() -> Self {
        Blender::default()
    }

    /// Set the `Logger` to use for internal log messages.
    ///
    /// By default, all log messages are discarded.
    pub fn log_with(&mut self, log: slog::Logger) {
        self.log = log;
    }

    /// Start setting up a new `Migration`.
    pub fn start_migration(&mut self) -> Migration {
        info!(self.log, "starting migration");
        let miglog = self.log.new(None);
        Migration {
            mainline: self,
            added: Default::default(),
            materialize: Default::default(),
            readers: Default::default(),

            start: time::Instant::now(),
            log: miglog,
        }
    }

    /// Get a boxed function which can be used to validate tokens.
    pub fn get_validator(&self) -> Box<Fn(&checktable::Token) -> bool> {
        let checktable = self.checktable.clone();
        Box::new(move |t: &checktable::Token| checktable.lock().unwrap().validate_token(t))
    }

    #[cfg(test)]
    pub fn graph(&self) -> &prelude::Graph {
        &self.ingredients
    }

    /// Get references to all known input nodes.
    ///
    /// Input nodes are here all nodes of type `Base`. The addresses returned by this function will
    /// all have been returned as a key in the map from `commit` at some point in the past.
    ///
    /// This function will only tell you which nodes are input nodes in the graph. To obtain a
    /// function for inserting writes, use `Blender::get_putter`.
    pub fn inputs(&self) -> Vec<(NodeAddress, &node::Node)> {
        self.ingredients
            .neighbors_directed(self.source, petgraph::EdgeDirection::Outgoing)
            .flat_map(|ingress| {
                self.ingredients.neighbors_directed(ingress, petgraph::EdgeDirection::Outgoing)
            })
            .map(|n| (n, &self.ingredients[n]))
            .filter(|&(_, base)| base.is_internal() && base.is_base())
            .map(|(n, base)| (NodeAddress::make_global(n), &*base))
            .collect()
    }

    /// Get a reference to all known output nodes.
    ///
    /// Output nodes here refers to nodes of type `Reader`, which is the nodes created in response
    /// to calling `.maintain` or `.stream` for a node during a migration.
    ///
    /// This function will only tell you which nodes are output nodes in the graph. To obtain a
    /// function for performing reads, call `.get_reader()` on the returned reader.
    pub fn outputs(&self) -> Vec<(NodeAddress, &node::Node, &node::Reader)> {
        self.ingredients
            .externals(petgraph::EdgeDirection::Outgoing)
            .filter_map(|n| {
                use flow::node;
                if let node::Type::Reader(_, ref inner) = *self.ingredients[n] {
                    // we want to give the the node that is being materialized
                    // not the reader node itself
                    let src = self.ingredients
                        .neighbors_directed(n, petgraph::EdgeDirection::Incoming)
                        .next()
                        .unwrap();
                    Some((NodeAddress::make_global(src), &self.ingredients[src], inner))
                } else {
                    None
                }
            })
            .collect()
    }

    /// Obtain a new function for querying a given (already maintained) reader node.
    pub fn get_getter
        (&self,
         node: NodeAddress)
         -> Option<Box<Fn(&prelude::DataType) -> Result<ops::Datas, ()> + Send + Sync>> {

        // reader should be a child of the given node
        trace!(self.log, "creating reader"; "for" => node.as_global().index());
        let reader = self.ingredients
            .neighbors_directed(*node.as_global(), petgraph::EdgeDirection::Outgoing)
            .filter_map(|ni| if let node::Type::Reader(_, ref inner) = *self.ingredients[ni] {
                Some(inner)
            } else {
                None
            })
            .next(); // there should be at most one

        reader.and_then(|r| r.get_reader())
    }

    /// Obtain a mutator that can be used to perform writes and deletes from the given base node.
    pub fn get_mutator(&self, base: NodeAddress) -> Mutator {
        let n = self.ingredients
            .neighbors_directed(*base.as_global(), petgraph::EdgeDirection::Incoming)
            .next()
            .unwrap();
        let node = &self.ingredients[n];
        let tx = self.txs[&node.domain()].clone();

        trace!(self.log, "creating mutator"; "for" => n.index());
        Mutator {
            src: NodeAddress::make_global(self.source),
            tx: tx,
            addr: node.addr(),
            primary_key: self.ingredients[*base.as_global()]
                .suggest_indexes(base)
                .remove(&base)
                .unwrap_or_else(Vec::new),
            tx_reply_channel: mpsc::channel(),
        }
    }

    /// Get statistics about the time spent processing different parts of the graph.
    pub fn get_statistics(&mut self) -> statistics::GraphStats {
        // TODO: request stats from domains in parallel.
        let domains = self.txs
            .iter()
            .map(|(di, s)| {
                let (tx, rx) = mpsc::sync_channel(1);
                s.send(payload::Packet::GetStatistics(tx)).unwrap();

                let (domain_stats, node_stats) = rx.recv().unwrap();
                let node_map = node_stats.into_iter()
                    .map(|(ni, ns)| (NodeAddress::make_global(ni), ns))
                    .collect();

                (*di, (domain_stats, node_map))
            })
            .collect();

        statistics::GraphStats { domains: domains }
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

    start: time::Instant,
    log: slog::Logger,
}

impl<'a> Migration<'a> {
    /// Add a new (empty) domain to the graph
    pub fn add_domain(&mut self) -> domain::Index {
        trace!(self.log, "creating new domain"; "domain" => self.mainline.ndomains);
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
        let ni = self.mainline.ingredients.add_node(node::Node::new(name.to_string(), fields, i));
        info!(self.log, "adding new node"; "node" => ni.index(), "type" => format!("{:?}", *self.mainline.ingredients[ni]));

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

    #[cfg(test)]
    pub fn graph(&self) -> &prelude::Graph {
        self.mainline.graph()
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

        debug!(self.log, "told to materialize"; "node" => src.as_global().index());

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
        debug!(self.log, "node manually assigned to domain"; "node" => n.as_global().index(), "domain" => d.index());
        assert_eq!(self.added.insert(*n.as_global(), Some(d)).unwrap(), None);
    }

    fn ensure_reader_for(&mut self, n: NodeAddress) {
        if !self.readers.contains_key(n.as_global()) {
            // make a reader
            let r = node::Type::Reader(None, Default::default());
            let r = self.mainline.ingredients[*n.as_global()].mirror(r);
            let r = self.mainline.ingredients.add_node(r);
            self.mainline.ingredients.add_edge(*n.as_global(), r, false);
            self.readers.insert(*n.as_global(), r);
        }
    }

    fn ensure_token_generator(&mut self, n: NodeAddress, key: usize) {
        let ri = self.readers[n.as_global()];
        if let node::Type::Reader(_, ref mut inner) = *self.mainline.ingredients[ri] {
            if inner.token_generator.is_some() {
                return;
            }
        } else {
            unreachable!("tried to add token generator to non-reader node");
        }

        let base_columns: Vec<(_, Option<_>)> = self.mainline.ingredients[*n.as_global()]
            .base_columns(key, &self.mainline.ingredients, *n.as_global());

        let coarse_parents = base_columns.iter()
            .filter_map(|&(ni, o)| if o.is_none() { Some(ni) } else { None })
            .collect();

        let granular_parents = base_columns.into_iter()
            .filter_map(|(ni, o)| if o.is_some() {
                Some((ni, o.unwrap()))
            } else {
                None
            })
            .collect();

        let token_generator = checktable::TokenGenerator::new(coarse_parents, granular_parents);
        self.mainline.checktable.lock().unwrap().track(&token_generator);

        if let node::Type::Reader(_, ref mut inner) = *self.mainline.ingredients[ri] {
            inner.token_generator = Some(token_generator);
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
                    -> Box<Fn(&prelude::DataType) -> Result<ops::Datas, ()> + Send + Sync> {
        self.ensure_reader_for(n);
        let ri = self.readers[n.as_global()];

        // we need to do these here because we'll mutably borrow self.mainline in the if let
        let cols = self.mainline.ingredients[ri].fields().len();

        if let node::Type::Reader(ref mut wh, ref mut inner) = *self.mainline.ingredients[ri] {
            if let Some(ref s) = inner.state {
                assert_eq!(s.key(), key);
            } else {
                use backlog;
                let (r, w) = backlog::new(cols, key);
                inner.state = Some(r);
                *wh = Some(w);
            }

            // cook up a function to query this materialized state
            inner.get_reader().unwrap()
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
         -> Box<Fn(&prelude::DataType) -> Result<(ops::Datas, checktable::Token), ()> + Send + Sync> {
        self.ensure_reader_for(n);
        self.ensure_token_generator(n, key);
        let ri = self.readers[n.as_global()];

        // we need to do these here because we'll mutably borrow self.mainline in the if let
        let cols = self.mainline.ingredients[ri].fields().len();

        if let node::Type::Reader(ref mut wh, ref mut inner) = *self.mainline.ingredients[ri] {
            if let Some(ref s) = inner.state {
                assert_eq!(s.key(), key);
            } else {
                use backlog;
                let (r, w) = backlog::new(cols, key);
                inner.state = Some(r);
                *wh = Some(w);
            }

            // cook up a function to query this materialized state
            let arc = inner.state.as_ref().unwrap().clone();
            let generator = inner.token_generator.clone().unwrap();
            Box::new(move |q: &prelude::DataType| -> Result<(ops::Datas, checktable::Token), ()> {
                arc.find_and(q,
                              |rs| rs.into_iter().map(|v| (&**v).clone()).collect::<Vec<_>>())
                    .map(|(res, ts)| {
                        let token = generator.generate(ts, q.clone());
                        (res, token)
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
    pub fn stream(&mut self, n: NodeAddress) -> mpsc::Receiver<Vec<node::StreamUpdate>> {
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
    pub fn commit(self) {
        info!(self.log, "finalizing migration"; "#nodes" => self.added.len());
        let mut new = HashSet::new();

        let log = self.log;
        let start = self.start;
        let mainline = self.mainline;

        // Make sure all new nodes are assigned to a domain
        for (node, domain) in self.added {
            let domain = domain.unwrap_or_else(|| {
                // new node that doesn't belong to a domain
                // create a new domain just for that node
                // NOTE: this is the same code as in add_domain(), but we can't use self here
                trace!(log, "node automatically added to domain"; "node" => node.index(), "domain" => mainline.ndomains);
                mainline.ndomains += 1;
                (mainline.ndomains - 1).into()

            });
            mainline.ingredients[node].add_to(domain);
            new.insert(node);
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
            migrate::routing::add(&log, &mut mainline.ingredients, mainline.source, &mut new);

        // Find all nodes for domains that have changed
        let changed_domains: HashSet<_> =
            new.iter().map(|&ni| mainline.ingredients[ni].domain()).collect();
        let mut domain_nodes = mainline.ingredients
            .node_indices()
            .filter(|&ni| ni != mainline.source)
            .map(|ni| {
                let domain = mainline.ingredients[ni].domain();
                (domain, ni, new.contains(&ni))
            })
            .fold(HashMap::new(), |mut dns, (d, ni, new)| {
                dns.entry(d).or_insert_with(Vec::new).push((ni, new));
                dns
            });

        let mut rxs = HashMap::new();

        // Set up input channels for new domains
        for domain in domain_nodes.keys() {
            if !mainline.txs.contains_key(domain) {
                let (tx, rx) = mpsc::sync_channel(10);
                rxs.insert(*domain, rx);
                mainline.txs.insert(*domain, tx);
            }
        }

        // Assign local addresses to all new nodes, and initialize them
        for (domain, nodes) in &mut domain_nodes {
            // Number of pre-existing nodes
            let mut nnodes = nodes.iter().filter(|&&(_, new)| !new).count();

            if nnodes == nodes.len() {
                // Nothing to do here
                continue;
            }

            let log = log.new(o!("domain" => domain.index()));

            // Give local addresses to every (new) node
            for &(ni, new) in nodes.iter() {
                if new {
                    debug!(log, "assigning local index"; "type" => format!("{:?}", *mainline.ingredients[ni]), "node" => ni.index(), "local" => nnodes);
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
                    trace!(log, "initializing new node"; "node" => ni.index());
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
        // NOTE: index will also contain the materialization information for *existing* domains
        debug!(log, "calculating materializations");
        let index = domain_nodes.iter()
            .map(|(domain, nodes)| {
                use self::migrate::materialization::{pick, index};
                debug!(log, "picking materializations"; "domain" => domain.index());
                let mat = pick(&log, &mainline.ingredients, &nodes[..]);
                debug!(log, "deriving indices"; "domain" => domain.index());
                let idx = index(&log, &mainline.ingredients, &nodes[..], mat);
                (*domain, idx)
            })
            .collect();

        let mut uninformed_domain_nodes = domain_nodes.clone();
        let ingresses_from_base = migrate::transactions::analyze_graph(&mainline.ingredients,
                                                                       mainline.source,
                                                                       domain_nodes);
        let (start_ts, end_ts, prevs) =
            mainline.checktable.lock().unwrap().perform_migration(&ingresses_from_base);

        info!(log, "migration claimed timestamp range"; "start" => start_ts, "end" => end_ts);

        // Boot up new domains (they'll ignore all updates for now)
        debug!(log, "booting new domains");
        for domain in changed_domains {
            if !rxs.contains_key(&domain) {
                // this is not a new domain
                continue;
            }

            // Start up new domain
            migrate::booting::boot_new(log.new(o!("domain" => domain.index())),
                                       domain.index().into(),
                                       &mut mainline.ingredients,
                                       uninformed_domain_nodes.remove(&domain).unwrap(),
                                       mainline.checktable.clone(),
                                       rxs.remove(&domain).unwrap(),
                                       start_ts);
        }
        drop(rxs);

        // Add any new nodes to existing domains (they'll also ignore all updates for now)
        debug!(log, "mutating existing domains");
        migrate::augmentation::inform(&log,
                                      &mut mainline.ingredients,
                                      mainline.source,
                                      &mut mainline.txs,
                                      uninformed_domain_nodes,
                                      start_ts,
                                      prevs.unwrap());

        // Set up inter-domain connections
        // NOTE: once we do this, we are making existing domains block on new domains!
        info!(log, "bringing up inter-domain connections");
        migrate::routing::connect(&log, &mut mainline.ingredients, &mainline.txs, &new);

        // And now, the last piece of the puzzle -- set up materializations
        info!(log, "initializing new materializations");
        migrate::materialization::initialize(&log,
                                             &mainline.ingredients,
                                             mainline.source,
                                             &new,
                                             index,
                                             &mut mainline.txs);

        info!(log, "finalizing migration");
        migrate::transactions::finalize(ingresses_from_base, &log, &mut mainline.txs, end_ts);

        warn!(log, "migration completed"; "ms" => dur_to_ns!(start.elapsed()) / 1_000_000);
    }
}

impl Drop for Blender {
    fn drop(&mut self) {
        for (_, tx) in &mut self.txs {
            // don't unwrap, because given domain may already have terminated
            drop(tx.send(payload::Packet::Quit));
        }
    }
}
