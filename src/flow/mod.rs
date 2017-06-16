use backlog;
use checktable;
use ops::base::Base;

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::sync::mpsc;
use std::time;
use std::fmt;
use std::io;

use slog;
use petgraph;
use petgraph::visit::Bfs;
use petgraph::graph::NodeIndex;

pub mod core;
pub mod domain;
pub mod hook;
pub mod keys;
pub mod migrate;
pub mod node;
pub mod payload;
pub mod persistence;
pub mod prelude;
pub mod statistics;

mod mutator;
mod transactions;

use self::prelude::Ingredient;

pub use self::mutator::{Mutator, MutatorError};

const NANOS_PER_SEC: u64 = 1_000_000_000;
macro_rules! dur_to_ns {
    ($d:expr) => {{
        let d = $d;
        d.as_secs() * NANOS_PER_SEC + d.subsec_nanos() as u64
    }}
}

lazy_static! {
    static ref VIEW_READERS: Mutex<HashMap<NodeIndex, backlog::ReadHandle>> = Mutex::default();
}

pub type Edge = bool; // should the edge be materialized?

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
    partial: HashSet<NodeIndex>,
    partial_enabled: bool,
    sharding_enabled: bool,

    /// Parameters for persistence code.
    persistence: persistence::Parameters,

    domains: HashMap<domain::Index, domain::DomainHandle>,

    log: slog::Logger,
}

impl Default for Blender {
    fn default() -> Self {
        let mut g = petgraph::Graph::new();
        let source = g.add_node(node::Node::new(
            "source",
            &["because-type-inference"],
            node::special::Source,
            true,
        ));
        Blender {
            ingredients: g,
            source: source,
            ndomains: 0,
            checktable: Arc::new(Mutex::new(checktable::CheckTable::new())),
            partial: Default::default(),
            partial_enabled: true,
            sharding_enabled: true,

            persistence: persistence::Parameters::default(),

            domains: Default::default(),

            log: slog::Logger::root(slog::Discard, o!()),
        }
    }
}

impl Blender {
    /// Construct a new, empty `Blender`
    pub fn new() -> Self {
        Blender::default()
    }

    /// Disable partial materialization for all subsequent migrations
    pub fn disable_partial(&mut self) {
        self.partial_enabled = false;
    }

    /// Disable sharding for all subsequent migrations
    pub fn disable_sharding(&mut self) {
        self.sharding_enabled = false;
    }

    /// Controls the persistence mode, and parameters related to persistence.
    ///
    /// Three modes are available:
    ///
    ///  1. `DurabilityMode::Permanent`: all writes to base nodes should be written to disk.
    ///  2. `DurabilityMode::DeleteOnExit`: all writes are written to disk, but the log is
    ///     deleted once the `Blender` is dropped. Useful for tests.
    ///  3. `DurabilityMode::MemoryOnly`: no writes to disk, store all writes in memory.
    ///     Useful for baseline numbers.
    ///
    /// `queue_capacity` indicates the number of packets that should be buffered until
    /// flushing, and `flush_timeout` indicates the length of time to wait before flushing
    /// anyway.
    ///
    /// Must be called before any domains have been created.
    pub fn with_persistence_options(&mut self, params: persistence::Parameters) {
        assert_eq!(self.ndomains, 0);
        self.persistence = params;
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
        let miglog = self.log.new(o!());
        Migration {
            mainline: self,
            added: Default::default(),
            columns: Default::default(),
            materialize: Default::default(),
            readers: Default::default(),

            start: time::Instant::now(),
            log: miglog,
        }
    }

    /// Get a boxed function which can be used to validate tokens.
    pub fn get_validator(&self) -> Box<Fn(&checktable::Token) -> bool> {
        let checktable = self.checktable.clone();
        Box::new(move |t: &checktable::Token| {
            checktable.lock().unwrap().validate_token(t)
        })
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
    pub fn inputs(&self) -> Vec<(prelude::NodeIndex, &node::Node)> {
        self.ingredients
            .neighbors_directed(self.source, petgraph::EdgeDirection::Outgoing)
            .map(|n| {
                let base = &self.ingredients[n];
                assert!(base.is_internal());
                assert!(base.get_base().is_some());
                (n.into(), base)
            })
            .collect()
    }

    /// Get a reference to all known output nodes.
    ///
    /// Output nodes here refers to nodes of type `Reader`, which is the nodes created in response
    /// to calling `.maintain` or `.stream` for a node during a migration.
    ///
    /// This function will only tell you which nodes are output nodes in the graph. To obtain a
    /// function for performing reads, call `.get_reader()` on the returned reader.
    pub fn outputs(&self) -> Vec<(prelude::NodeIndex, &node::Node)> {
        self.ingredients
            .externals(petgraph::EdgeDirection::Outgoing)
            .filter_map(|n| {
                self.ingredients[n].with_reader(|r| {
                    // we want to give the the node that is being materialized
                    // not the reader node itself
                    let src = r.is_for();
                    (src, &self.ingredients[src])
                })
            })
            .collect()
    }

    fn find_getter_for(&self, node: prelude::NodeIndex) -> Option<NodeIndex> {
        // reader should be a child of the given node. however, due to sharding, it may not be an
        // *immediate* child. furthermore, once we go beyond depth 1, we may accidentally hit an
        // *unrelated* reader node. to account for this, readers keep track of what node they are
        // "for", and we simply search for the appropriate reader by that metric. since we know
        // that the reader must be relatively close, a BFS search is the way to go.
        // presumably only
        let mut bfs = Bfs::new(&self.ingredients, node);
        let mut reader = None;
        while let Some(child) = bfs.next(&self.ingredients) {
            if self.ingredients[child]
                .with_reader(|r| r.is_for() == node)
                .unwrap_or(false)
            {
                reader = Some(child);
                break;
            }
        }

        reader
    }

    /// Obtain a new function for querying a given (already maintained) reader node.
    pub fn get_getter(
        &self,
        node: prelude::NodeIndex,
    ) -> Option<Box<Fn(&prelude::DataType, bool) -> Result<core::Datas, ()> + Send>> {

        // look up the read handle, clone it, and construct the read closure
        self.find_getter_for(node).and_then(|r| {
            let vr = VIEW_READERS.lock().unwrap();
            let rh: Option<backlog::ReadHandle> = vr.get(&r).cloned();
            rh.map(|rh| {
                Box::new(move |q: &prelude::DataType,
                      block: bool|
                      -> Result<prelude::Datas, ()> {
                    rh.find_and(
                        q,
                        |rs| {
                            rs.into_iter()
                                .map(|v| (&**v).into_iter().map(|v| v.external_clone()).collect())
                                .collect()
                        },
                        block,
                    ).map(|r| r.0.unwrap_or_else(Vec::new))
                }) as Box<_>
            })
        })
    }

    /// Obtain a new function for querying a given (already maintained) transactional reader node.
    pub fn get_transactional_getter(
        &self,
        node: prelude::NodeIndex,
    ) -> Result<
        Box<
            Fn(&prelude::DataType) -> Result<(core::Datas, checktable::Token), ()>
                + Send,
        >,
        (),
    > {

        if !self.ingredients[node].is_transactional() {
            return Err(());
        }

        // look up the read handle, clone it, and construct the read closure
        self.find_getter_for(node)
            .and_then(|r| {
                let inner = self.ingredients[r].with_reader(|r| r).unwrap();
                let vr = VIEW_READERS.lock().unwrap();
                let rh: Option<backlog::ReadHandle> = vr.get(&r).cloned();
                rh.map(|rh| {
                    let generator = inner.token_generator().cloned().unwrap();
                    Box::new(move |q: &prelude::DataType|
                                   -> Result<(core::Datas, checktable::Token), ()> {
                        rh.find_and(q,
                                      |rs| {
                                rs.into_iter()
                                    .map(|v| {
                                             (&**v)
                                                 .into_iter()
                                                 .map(|v| v.external_clone())
                                                 .collect()
                                         })
                                    .collect()
                            },
                                      true)
                            .map(|(res, ts)| {
                                     let token = generator.generate(ts, q.clone());
                                     (res.unwrap_or_else(Vec::new), token)
                                 })
                    }) as Box<_>
                })
            })
            .ok_or(())
    }

    /// Obtain a mutator that can be used to perform writes and deletes from the given base node.
    pub fn get_mutator(&self, base: prelude::NodeIndex) -> Mutator {
        let node = &self.ingredients[base];
        let tx = self.domains[&node.domain()].clone();

        trace!(self.log, "creating mutator"; "for" => base.index());

        let mut key = self.ingredients[base]
            .suggest_indexes(base)
            .remove(&base)
            .unwrap_or_else(Vec::new);
        let mut is_primary = false;
        if key.is_empty() {
            if let prelude::Sharding::ByColumn(col) = self.ingredients[base].sharded_by() {
                key = vec![col];
            }
        } else {
            is_primary = true;
        }

        let num_fields = node.fields().len();
        let base_operator = node.get_base()
            .expect("asked to get mutator for non-base node");
        Mutator {
            tx: tx,
            addr: (*node.local_addr()).into(),
            key: key,
            key_is_primary: is_primary,
            tx_reply_channel: mpsc::channel(),
            transactional: self.ingredients[base].is_transactional(),
            dropped: base_operator.get_dropped(),
            tracer: None,
            expected_columns: num_fields - base_operator.get_dropped().len(),
        }
    }

    /// Get statistics about the time spent processing different parts of the graph.
    pub fn get_statistics(&mut self) -> statistics::GraphStats {
        // TODO: request stats from domains in parallel.
        let domains = self.domains
            .iter_mut()
            .map(|(di, s)| {
                let (tx, rx) = mpsc::sync_channel(1);
                s.send(box payload::Packet::GetStatistics(tx)).unwrap();

                let (domain_stats, node_stats) = rx.recv().unwrap();
                // FIXME: can get multiple responses from sharded domains
                assert!(rx.recv().is_err());
                let node_map = node_stats
                    .into_iter()
                    .map(|(ni, ns)| (ni.into(), ns))
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

enum ColumnChange {
    Add(String, prelude::DataType),
    Drop(usize),
}

/// A `Migration` encapsulates a number of changes to the Soup data flow graph.
///
/// Only one `Migration` can be in effect at any point in time. No changes are made to the running
/// graph until the `Migration` is committed (using `Migration::commit`).
pub struct Migration<'a> {
    mainline: &'a mut Blender,
    added: Vec<NodeIndex>,
    columns: Vec<(NodeIndex, ColumnChange)>,
    readers: HashMap<NodeIndex, NodeIndex>,
    materialize: HashSet<(NodeIndex, NodeIndex)>,

    start: time::Instant,
    log: slog::Logger,
}

impl<'a> Migration<'a> {
    /// Add the given `Ingredient` to the Soup.
    ///
    /// The returned identifier can later be used to refer to the added ingredient.
    /// Edges in the data flow graph are automatically added based on the ingredient's reported
    /// `ancestors`.
    pub fn add_ingredient<S1, FS, S2, I>(
        &mut self,
        name: S1,
        fields: FS,
        mut i: I,
    ) -> prelude::NodeIndex
    where
        S1: ToString,
        S2: ToString,
        FS: IntoIterator<Item = S2>,
        I: prelude::Ingredient + Into<prelude::NodeOperator>,
    {
        i.on_connected(&self.mainline.ingredients);
        let parents = i.ancestors();

        let transactional = !parents.is_empty() &&
            parents
                .iter()
                .all(|&p| self.mainline.ingredients[p].is_transactional());

        // add to the graph
        let ni = self.mainline.ingredients.add_node(node::Node::new(
            name.to_string(),
            fields,
            i.into(),
            transactional,
        ));
        info!(self.log,
              "adding new node";
              "node" => ni.index(),
              "type" => format!("{:?}", self.mainline.ingredients[ni])
        );

        // keep track of the fact that it's new
        self.added.push(ni);
        // insert it into the graph
        if parents.is_empty() {
            self.mainline.ingredients.add_edge(
                self.mainline.source,
                ni,
                false,
            );
        } else {
            for parent in parents {
                self.mainline.ingredients.add_edge(parent, ni, false);
            }
        }
        // and tell the caller its id
        ni.into()
    }

    /// Add a transactional base node to the graph
    pub fn add_transactional_base<S1, FS, S2>(
        &mut self,
        name: S1,
        fields: FS,
        mut b: Base,
    ) -> prelude::NodeIndex
    where
        S1: ToString,
        S2: ToString,
        FS: IntoIterator<Item = S2>,
    {
        b.on_connected(&self.mainline.ingredients);
        let b: prelude::NodeOperator = b.into();

        // add to the graph
        let ni = self.mainline.ingredients.add_node(node::Node::new(
            name.to_string(),
            fields,
            b,
            true,
        ));
        info!(self.log,
              "adding new node";
              "node" => ni.index(),
              "type" => format!("{:?}", self.mainline.ingredients[ni])
        );

        // keep track of the fact that it's new
        self.added.push(ni);
        // insert it into the graph
        self.mainline.ingredients.add_edge(
            self.mainline.source,
            ni,
            false,
        );
        // and tell the caller its id
        ni.into()
    }

    /// Add a new column to a base node.
    ///
    /// Note that a default value must be provided such that old writes can be converted into this
    /// new type.
    pub fn add_column<S: ToString>(
        &mut self,
        node: prelude::NodeIndex,
        field: S,
        default: prelude::DataType,
    ) -> usize {
        // not allowed to add columns to new nodes
        assert!(!self.added.iter().any(|&ni| ni == node));

        let field = field.to_string();
        let base = &mut self.mainline.ingredients[node];
        assert!(base.is_internal() && base.get_base().is_some());

        // we need to tell the base about its new column and its default, so that old writes that
        // do not have it get the additional value added to them.
        let col_i1 = base.add_column(&field);
        // we can't rely on DerefMut, since it disallows mutating Taken nodes
        {
            let col_i2 = base.inner_mut()
                .get_base_mut()
                .unwrap()
                .add_column(default.clone());
            assert_eq!(col_i1, col_i2);
        }

        // also eventually propagate to domain clone
        self.columns.push((node, ColumnChange::Add(field, default)));

        col_i1
    }

    /// Drop a column from a base node.
    pub fn drop_column(&mut self, node: prelude::NodeIndex, column: usize) {
        // not allowed to drop columns from new nodes
        assert!(!self.added.iter().any(|&ni| ni == node));

        let base = &mut self.mainline.ingredients[node];
        assert!(base.is_internal() && base.get_base().is_some());

        // we need to tell the base about the dropped column, so that old writes that contain that
        // column will have it filled in with default values (this is done in Mutator).
        // we can't rely on DerefMut, since it disallows mutating Taken nodes
        base.inner_mut().get_base_mut().unwrap().drop_column(column);

        // also eventually propagate to domain clone
        self.columns.push((node, ColumnChange::Drop(column)));
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
    pub fn materialize(&mut self, src: prelude::NodeIndex, dst: prelude::NodeIndex) {
        // TODO
        // what about if a user tries to materialize a cross-domain edge that has already been
        // converted to an egress/ingress pair?
        let e = self.mainline
            .ingredients
            .find_edge(src, dst)
            .expect("asked to materialize non-existing edge");

        debug!(self.log, "told to materialize"; "node" => src.index());

        let mut e = self.mainline.ingredients.edge_weight_mut(e).unwrap();
        if !*e {
            *e = true;
            // it'd be nice if we could just store the EdgeIndex here, but unfortunately that's not
            // guaranteed by petgraph to be stable in the presence of edge removals (which we do in
            // commit())
            self.materialize.insert((src, dst));
        }
    }

    fn ensure_reader_for(&mut self, n: prelude::NodeIndex) {
        if !self.readers.contains_key(&n) {
            // make a reader
            let r = node::special::Reader::new(n);
            let r = self.mainline.ingredients[n].mirror(r);
            let r = self.mainline.ingredients.add_node(r);
            self.mainline.ingredients.add_edge(n, r, false);
            self.readers.insert(n, r);
        }
    }

    fn ensure_token_generator(&mut self, n: prelude::NodeIndex, key: usize) {
        let ri = self.readers[&n];
        if self.mainline.ingredients[ri]
            .with_reader(|r| r.token_generator().is_some())
            .expect("tried to add token generator to non-reader node")
        {
            return;
        }

        // A map from base node to the column in that base node whose value must match the value of
        // this node's column to cause a conflict. Is None for a given base node if any write to
        // that base node might cause a conflict.
        let base_columns: Vec<(_, Option<_>)> =
            keys::provenance_of(&self.mainline.ingredients, n, key, |_, _| None)
                .into_iter()
                .map(|path| {
                    // we want the base node corresponding to each path
                    path.into_iter().last().unwrap()
                })
                .collect();

        let coarse_parents = base_columns
            .iter()
            .filter_map(|&(ni, o)| if o.is_none() { Some(ni) } else { None })
            .collect();

        let granular_parents = base_columns
            .into_iter()
            .filter_map(|(ni, o)| if o.is_some() {
                Some((ni, o.unwrap()))
            } else {
                None
            })
            .collect();

        let token_generator = checktable::TokenGenerator::new(coarse_parents, granular_parents);
        self.mainline
            .checktable
            .lock()
            .unwrap()
            .track(&token_generator);

        self.mainline.ingredients[ri]
            .with_reader_mut(|r| { r.set_token_generator(token_generator); });
    }

    /// Set up the given node such that its output can be efficiently queried.
    ///
    /// To query into the maintained state, use `Blender::get_getter` or
    /// `Blender::get_transactional_getter`
    pub fn maintain(&mut self, n: prelude::NodeIndex, key: usize) {
        self.ensure_reader_for(n);
        if self.mainline.ingredients[n].is_transactional() {
            self.ensure_token_generator(n, key);
        }

        let ri = self.readers[&n];

        self.mainline.ingredients[ri].with_reader_mut(|r| r.set_key(key));
    }

    /// Obtain a channel that is fed by the output stream of the given node.
    ///
    /// As new updates are processed by the given node, its outputs will be streamed to the
    /// returned channel. Node that this channel is *not* bounded, and thus a receiver that is
    /// slower than the system as a hole will accumulate a large buffer over time.
    pub fn stream(&mut self, n: prelude::NodeIndex) -> mpsc::Receiver<Vec<node::StreamUpdate>> {
        self.ensure_reader_for(n);
        let (mut tx, rx) = mpsc::channel();

        // If the reader hasn't been incorporated into the graph yet, just add the streamer
        // directly.
        let ri = self.readers[&n];
        let mut res = None;
        self.mainline.ingredients[ri].with_reader_mut(|r| { res = Some(r.add_streamer(tx)); });
        tx = match res.unwrap() {
            Ok(_) => return rx,
            Err(tx) => tx,
        };


        // Otherwise, send a message to the reader's domain to have it add the streamer.
        let reader = &self.mainline.ingredients[self.readers[&n]];
        self.mainline
            .domains
            .get_mut(&reader.domain())
            .unwrap()
            .send(box payload::Packet::AddStreamer {
                node: *reader.local_addr(),
                new_streamer: tx,
            })
            .unwrap();

        rx
    }

    /// Set up the given node such that its output is stored in Memcached.
    pub fn memcached_hook(
        &mut self,
        n: prelude::NodeIndex,
        name: String,
        servers: &[(&str, usize)],
        key: usize,
    ) -> io::Result<prelude::NodeIndex> {
        let h = try!(hook::Hook::new(name, servers, vec![key]));
        let h = self.mainline.ingredients[n].mirror(h);
        let h = self.mainline.ingredients.add_node(h);
        self.mainline.ingredients.add_edge(n, h, false);
        Ok(h.into())
    }

    /// Commit the changes introduced by this `Migration` to the master `Soup`.
    ///
    /// This will spin up an execution thread for each new thread domain, and hook those new
    /// domains into the larger Soup graph. The returned map contains entry points through which
    /// new updates should be sent to introduce them into the Soup.
    pub fn commit(self) {
        info!(self.log, "finalizing migration"; "#nodes" => self.added.len());

        let log = self.log;
        let start = self.start;
        let mut mainline = self.mainline;
        let mut new: HashSet<_> = self.added.into_iter().collect();

        // Readers are nodes too.
        for (_parent, reader) in self.readers {
            new.insert(reader);
        }

        // Shard the graph as desired
        let mut swapped0 = if mainline.sharding_enabled {
            migrate::sharding::shard(&log, &mut mainline.ingredients, mainline.source, &mut new)
        } else {
            HashMap::default()
        };

        // Assign domains
        migrate::assignment::assign(
            &log,
            &mut mainline.ingredients,
            mainline.source,
            &new,
            &mut mainline.ndomains,
        );

        // Set up ingress and egress nodes
        let swapped1 =
            migrate::routing::add(&log, &mut mainline.ingredients, mainline.source, &mut new);

        // Merge the swap lists
        for ((dst, src), instead) in swapped1 {
            use std::collections::hash_map::Entry;
            match swapped0.entry((dst, src)) {
                Entry::Occupied(mut instead0) => {
                    if &instead != instead0.get() {
                        // This can happen if sharding decides to add a Sharder *under* a node,
                        // and routing decides to add an ingress/egress pair between that node
                        // and the Sharder. It's perfectly okay, but we should prefer the
                        // "bottommost" swap to take place (i.e., the node that is *now*
                        // closest to the dst node). This *should* be the sharding node, unless
                        // routing added an ingress *under* the Sharder. We resolve the
                        // collision by looking at which translation currently has an adge from
                        // `src`, and then picking the *other*, since that must then be node
                        // below.
                        if mainline.ingredients.find_edge(src, instead).is_some() {
                            // src -> instead -> instead0 -> [children]
                            // from [children]'s perspective, we should use instead0 for from, so
                            // we can just ignore the `instead` swap.
                        } else {
                            // src -> instead0 -> instead -> [children]
                            // from [children]'s perspective, we should use instead for src, so we
                            // need to prefer the `instead` swap.
                            *instead0.get_mut() = instead;
                        }
                    }
                }
                Entry::Vacant(hole) => {
                    hole.insert(instead);
                }
            }

            // we may also already have swapped the parents of some node *to* `src`. in
            // swapped0. we want to change that mapping as well, since lookups in swapped
            // aren't recursive.
            for (_, instead0) in swapped0.iter_mut() {
                if *instead0 == src {
                    *instead0 = instead;
                }
            }
        }
        let swapped = swapped0;

        // Find all nodes for domains that have changed
        let changed_domains: HashSet<domain::Index> = new.iter()
            .filter(|&&ni| !mainline.ingredients[ni].is_dropped())
            .map(|&ni| mainline.ingredients[ni].domain())
            .collect();
        let mut domain_nodes = mainline
            .ingredients
            .node_indices()
            .filter(|&ni| ni != mainline.source)
            .filter(|&ni| !mainline.ingredients[ni].is_dropped())
            .map(|ni| {
                (mainline.ingredients[ni].domain(), ni, new.contains(&ni))
            })
            .fold(HashMap::new(), |mut dns, (d, ni, new)| {
                dns.entry(d).or_insert_with(Vec::new).push((ni, new));
                dns
            });

        // Assign local addresses to all new nodes, and initialize them
        let mut local_remap = HashMap::new();
        let mut remap = HashMap::new();
        for (domain, nodes) in &mut domain_nodes {
            // Number of pre-existing nodes
            let mut nnodes = nodes.iter().filter(|&&(_, new)| !new).count();

            if nnodes == nodes.len() {
                // Nothing to do here
                continue;
            }

            let log = log.new(o!("domain" => domain.index()));

            // Give local addresses to every (new) node
            local_remap.clear();
            for &(ni, new) in nodes.iter() {
                if new {
                    debug!(log,
                           "assigning local index";
                           "type" => format!("{:?}", mainline.ingredients[ni]),
                           "node" => ni.index(),
                           "local" => nnodes
                    );

                    let mut ip: prelude::IndexPair = ni.into();
                    ip.set_local(unsafe { prelude::LocalNodeIndex::make(nnodes as u32) });
                    mainline.ingredients[ni].set_finalized_addr(ip);
                    local_remap.insert(ni, ip);
                    nnodes += 1;
                } else {
                    local_remap.insert(ni, *mainline.ingredients[ni].get_index());
                }
            }

            // Initialize each new node
            for &(ni, new) in nodes.iter() {
                if new && mainline.ingredients[ni].is_internal() {
                    // Figure out all the remappings that have happened
                    // NOTE: this has to be *per node*, since a shared parent may be remapped
                    // differently to different children (due to sharding for example). we just
                    // allocate it once though.
                    remap.clear();
                    remap.extend(local_remap.iter().map(|(&k, &v)| (k, v)));

                    // Parents in other domains have been swapped for ingress nodes.
                    // Those ingress nodes' indices are now local.
                    for (&(dst, src), &instead) in &swapped {
                        if dst != ni {
                            // ignore mappings for other nodes
                            continue;
                        }

                        let old = remap.insert(src, local_remap[&instead]);
                        assert_eq!(old, None);
                    }

                    trace!(log, "initializing new node"; "node" => ni.index());
                    mainline
                        .ingredients
                        .node_weight_mut(ni)
                        .unwrap()
                        .on_commit(&remap);
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
        // TODO: this should re-use materialization decisions across shard domains
        debug!(log, "calculating materializations");
        let index = domain_nodes
            .iter()
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
        let deps = migrate::transactions::analyze_graph(
            &mainline.ingredients,
            mainline.source,
            domain_nodes,
        );
        let (start_ts, end_ts, prevs) =
            mainline.checktable.lock().unwrap().perform_migration(&deps);

        info!(log, "migration claimed timestamp range"; "start" => start_ts, "end" => end_ts);

        // Boot up new domains (they'll ignore all updates for now)
        debug!(log, "booting new domains");
        for domain in changed_domains {
            if mainline.domains.contains_key(&domain) {
                // this is not a new domain
                continue;
            }

            let nodes = uninformed_domain_nodes.remove(&domain).unwrap();
            let mut d =
                domain::DomainHandle::new(domain, mainline.ingredients[nodes[0].0].sharded_by());
            d.boot(
                &log,
                &mut mainline.ingredients,
                nodes,
                mainline.persistence.clone(),
                mainline.checktable.clone(),
                start_ts,
            );
            mainline.domains.insert(domain, d);
        }

        // Add any new nodes to existing domains (they'll also ignore all updates for now)
        debug!(log, "mutating existing domains");
        migrate::augmentation::inform(
            &log,
            &mut mainline,
            uninformed_domain_nodes,
            start_ts,
            prevs.unwrap(),
        );

        // Tell all base nodes about newly added columns
        let acks: Vec<_> = self.columns
            .into_iter()
            .map(|(ni, change)| {
                let (tx, rx) = mpsc::sync_channel(1);
                let n = &mainline.ingredients[ni];
                let m = match change {
                    ColumnChange::Add(field, default) => {
                        box payload::Packet::AddBaseColumn {
                            node: *n.local_addr(),
                            field: field,
                            default: default,
                            ack: tx,
                        }
                    }
                    ColumnChange::Drop(column) => {
                        box payload::Packet::DropBaseColumn {
                            node: *n.local_addr(),
                            column: column,
                            ack: tx,
                        }
                    }
                };

                mainline
                    .domains
                    .get_mut(&n.domain())
                    .unwrap()
                    .send(m)
                    .unwrap();
                rx
            })
            .collect();
        // wait for all domains to ack. otherwise, we could have one domain request a replay from
        // another before the source domain has heard about a new default column it needed to add.
        for ack in acks {
            ack.recv().is_err();
        }

        // Set up inter-domain connections
        // NOTE: once we do this, we are making existing domains block on new domains!
        info!(log, "bringing up inter-domain connections");
        migrate::routing::connect(&log, &mut mainline.ingredients, &mut mainline.domains, &new);

        // And now, the last piece of the puzzle -- set up materializations
        info!(log, "initializing new materializations");
        let domains_on_path = migrate::materialization::initialize(&log, mainline, &new, index);

        info!(log, "finalizing migration");

        // Ideally this should happen as part of checktable::perform_migration(), but we don't know
        // the replay paths then. It is harmless to do now since we know the new replay paths won't
        // request timestamps until after the migration in finished.
        mainline
            .checktable
            .lock()
            .unwrap()
            .add_replay_paths(domains_on_path);

        migrate::transactions::finalize(deps, &log, &mut mainline.domains, end_ts);

        warn!(log, "migration completed"; "ms" => dur_to_ns!(start.elapsed()) / 1_000_000);
    }
}

impl Drop for Blender {
    fn drop(&mut self) {
        for (_, mut d) in &mut self.domains {
            // don't unwrap, because given domain may already have terminated
            drop(d.send(box payload::Packet::Quit));
        }
        for (_, mut d) in self.domains.drain() {
            d.wait();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Blender without any domains gets dropped once it leaves the scope.
    #[test]
    fn it_works_default() {
        // Blender gets dropped. It doesn't have Domains, so we don't see any dropped.
        let b = Blender::default();
        assert_eq!(b.ndomains, 0);
    }

    // Blender with a few domains drops them once it leaves the scope.
    #[test]
    fn it_works_blender_with_migration() {
        use Recipe;

        let r_txt = "CREATE TABLE a (x int, y int, z int);\n
                     CREATE TABLE b (r int, s int);\n";
        let mut r = Recipe::from_str(r_txt, None).unwrap();

        let mut b = Blender::new();
        {
            let mut mig = b.start_migration();
            assert!(r.activate(&mut mig, false).is_ok());
            mig.commit();
        }
    }
}
