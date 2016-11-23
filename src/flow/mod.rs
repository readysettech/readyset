use shortcut;
use petgraph;
use petgraph::graph::NodeIndex;
use query;
use ops;

use regex::Regex;

use std::sync::mpsc;
use std::sync;

use std::collections::HashMap;
use std::collections::HashSet;

use std::fmt;

use std::ops::{Deref, DerefMut};

pub mod domain;
pub mod prelude;

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
    fn should_materialize(&self) -> bool;

    /// Called whenever this node is being queried for records, and it is not materialized. The
    /// node should use the list of ancestor query functions to fetch relevant data from upstream,
    /// and emit resulting records as they come in. Note that there may be no query, in which case
    /// all records should be returned.
    fn query(&self,
             q: Option<&query::Query>,
             domain: &prelude::NodeList,
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

    fn on_connected(&mut self, graph: &petgraph::Graph<Node, Edge>);
    fn on_commit(&mut self, you: NodeIndex, remap: &HashMap<NodeIndex, NodeIndex>);
    fn on_input(&mut self,
                input: Message,
                domain: &domain::list::NodeList,
                states: &HashMap<NodeIndex, shortcut::Store<query::DataType>>)
                -> Option<U>;
}

pub enum NodeType {
    Ingress(domain::Index, mpsc::Receiver<Message>),
    Internal(domain::Index, Box<Ingredient>),
    Egress(domain::Index, sync::Arc<sync::Mutex<Vec<mpsc::Sender<Message>>>>),
    Unassigned(Box<Ingredient>),
    Taken,
    Source,
}

impl NodeType {
    fn domain(&self) -> domain::Index {
        match *self {
            NodeType::Ingress(d, _) |
            NodeType::Internal(d, _) |
            NodeType::Egress(d, _) => d,
            _ => unreachable!(),
        }
    }
}

impl Deref for NodeType {
    type Target = Ingredient;
    fn deref(&self) -> &Self::Target {
        match self {
            &NodeType::Internal(_, ref i) |
            &NodeType::Unassigned(ref i) => i.deref(),
            _ => unreachable!(),
        }
    }
}

impl DerefMut for NodeType {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            &mut NodeType::Internal(_, ref mut i) |
            &mut NodeType::Unassigned(ref mut i) => i.deref_mut(),
            _ => unreachable!(),
        }
    }
}

pub struct Node {
    inner: NodeType,
    name: String,
    fields: Vec<String>,
}

impl Node {
    pub fn mirror(&self, n: NodeType) -> Node {
        Node {
            inner: n,
            name: self.name.clone(),
            fields: self.fields.clone(),
        }
    }

    pub fn name(&self) -> &str {
        &*self.name
    }

    pub fn fields(&self) -> &[String] {
        &self.fields[..]
    }

    pub fn domain(&self) -> domain::Index {
        self.inner.domain()
    }

    pub fn take(&mut self) -> Node {
        use std::mem;
        let inner = match self.inner {
            NodeType::Egress(d, ref txs) => {
                // egress nodes can still be modified externally if subgraphs are added
                // so we just make a new one with a clone of the Mutex-protected Vec
                NodeType::Egress(d, txs.clone())
            }
            ref mut n @ NodeType::Ingress(..) => {
                // no-one else will be using our ingress node, so we take it from the graph
                mem::replace(n, NodeType::Taken)
            }
            ref mut n @ NodeType::Internal(..) => {
                // same with internal nodes
                mem::replace(n, NodeType::Taken)
            }
            _ => unreachable!(),
        };

        self.mirror(inner)
    }
}

impl Deref for Node {
    type Target = NodeType;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for Node {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

// TODO: what do we do about .having?
// impl Node {
//     /// Add an output filter to this node.
//     ///
//     /// Only records matching the given conditions will be output from this node. This filtering
//     /// applies both to feed-forward and to queries. Note that adding conditions in this way does
//     /// *not* modify a node's input, and so the node may end up performing computation whose result
//     /// will simply be discarded.
//     ///
//     /// Adding a HAVING condition will not reduce the size of the node's materialized state.
//     pub fn having(mut self, cond: Vec<shortcut::Condition<query::DataType>>) -> Self {
//         self.having = Some(query::Query::new(&[], cond));
//         self
//     }
//
//     /// Retrieve a list of this node's output filters.
//     pub fn having_conditions(&self) -> Option<&[shortcut::Condition<query::DataType>]> {
//         self.having.as_ref().map(|q| &q.having[..])
//     }
//
//     pub fn operator(&self) -> &NodeType {
//         &*self.inner
//     }
// }

/// `Blender` is the core component of the alternate Soup implementation.
///
/// It keeps track of the structure of the underlying data flow graph and its domains. `Blender`
/// does not allow direct manipulation of the graph. Instead, changes must be instigated through a
/// `Migration`, which can be started using `Blender::start_migration`. Only one `Migration` can
/// occur at any given point in time.
pub struct Blender {
    ingredients: petgraph::Graph<Node, Edge>,
    source: NodeIndex,
    ndomains: usize,
}

impl fmt::Display for Blender {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let indentln = |f: &mut fmt::Formatter| write!(f, "    ");
        let escape = |s: &str| Regex::new("([\"|{}])").unwrap().replace_all(s, "\\$1");

        // Output header.
        writeln!(f, "digraph {{")?;

        // Output global formatting.
        indentln(f)?;
        writeln!(f, "node [shape=record, fontsize=10]")?;

        // Output node descriptions.
        for index in self.ingredients.node_indices() {
            indentln(f)?;
            write!(f, "{}", index.index())?;
            write!(f, " [label=\"")?;

            let n = &self.ingredients[index];
            if let NodeType::Source = n.inner {
                write!(f, "(source)")?;
            } else {
                write!(f, "{{")?;

                // Output node name and description. First row.
                write!(f,
                       "{{ {} / {} | {} }}",
                       index.index(),
                       escape(n.name()),
                       escape(&n.description()))?;

                // Output node outputs. Second row.
                write!(f, " | {}", n.fields().join(", "))?;

                // Maybe output node's HAVING conditions. Optional third row.
                // TODO
                // if let Some(conds) = n.node().unwrap().having_conditions() {
                //     let conds = conds.iter()
                //         .map(|c| format!("{}", c))
                //         .collect::<Vec<_>>()
                //         .join(" ∧ ");
                //     write!(f, " | σ({})", escape(&conds))?;
                // }

                write!(f, " }}")?;
            }
            writeln!(f, "\"]")?;
        }

        // Output edges.
        for (_, edge) in self.ingredients.raw_edges().iter().enumerate() {
            indentln(f)?;
            writeln!(f, "{} -> {}", edge.source().index(), edge.target().index())?;
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

impl Blender {
    /// Start setting up a new `Migration`.
    pub fn start_migration<'a>(&'a mut self) -> Migration<'a> {
        Migration {
            mainline: self,
            added: Default::default(),
            materialize: Default::default(),
        }
    }
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
    pub fn add_ingredient<S1, FS, S2, I>(&mut self, name: S1, fields: FS, mut i: I) -> NodeIndex
        where S1: Into<String>,
              S2: Into<String>,
              FS: IntoIterator<Item = S2>,
              I: Ingredient + 'static
    {
        i.on_connected(&self.mainline.ingredients);

        let parents = i.ancestors();

        // add to the graph
        let ni = self.mainline.ingredients.add_node(Node {
            name: name.into(),
            fields: fields.into_iter().map(|s| s.into()).collect(),
            inner: NodeType::Unassigned(Box::new(i)),
        });

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
        assert!(self.added.insert(n, Some(d)).is_none());
    }

    fn boot_domain(&mut self, d: domain::Index, nodes: Vec<NodeIndex>) {
        let d = domain::Domain::from_graph(d, nodes, &mut self.mainline.ingredients);
        d.boot();
    }

    /// Commit the changes introduced by this `Migration` to the master `Soup`.
    ///
    /// This will spin up an execution thread for each new thread domain, and hook those new
    /// domains into the larger Soup graph. The returned map contains entry points through which
    /// new updates should be sent to introduce them into the Soup.
    pub fn commit(mut self) -> HashMap<NodeIndex, mpsc::Sender<U>> {
        // the user wants us to commit to the changes contained within this Migration. this is
        // where all the magic happens.

        let mut domain_remap = HashMap::new();
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
                        let proxy = self.mainline.ingredients[parent]
                            .mirror(NodeType::Ingress(domain, rx));
                        ingress = Some(self.mainline.ingredients.add_node(proxy));

                        let ingress = ingress.unwrap();
                        // that ingress node also needs to run before us
                        domain_nodes.entry(domain).or_insert_with(Vec::new).push(ingress);
                        // we're also going to need to tell any egress node that will send to this
                        // node about the channel it should use. we'll do that later, so just keep
                        // track of this work for later for now.
                        targets.insert(ingress, tx);
                    }

                    // keep track of the remapping
                    domain_remap.entry(domain).or_insert_with(HashMap::new).insert(parent, node);

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
            match mem::replace(&mut self.mainline.ingredients[node].inner, NodeType::Taken) {
                NodeType::Unassigned(inner) => {
                    self.mainline.ingredients.node_weight_mut(node).unwrap().inner =
                        NodeType::Internal(domain, inner);
                }
                _ => unreachable!(),
            }

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
                if domain != self.mainline.ingredients[child].domain() {
                    // child is in a different domain
                    // create an egress node to handle that
                    // NOTE: technically, this doesn't need to mirror its parent, but meh
                    let proxy = self.mainline.ingredients[node]
                        .mirror(NodeType::Egress(domain, Default::default()));
                    let egress = self.mainline.ingredients.add_node(proxy);

                    // we need to hook that node in between us and this child
                    // note that we *know* that this is an edge to an ingress node, as we prepended
                    // all nodes with a different-domain parent with an ingress node above.
                    if cfg!(debug_assertions) {
                        if let NodeType::Ingress(..) = self.mainline.ingredients[child].inner {
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
            if let NodeType::Ingress(..) = self.mainline.ingredients[ingress].inner {
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
                        thread::spawn(move || for u in rx2 {
                            tx.send(Message {
                                    from: src,
                                    data: u,
                                })
                                .unwrap();
                        });
                        sources.insert(ingress, tx2);
                        break;
                    }

                    if let NodeType::Egress(_, ref txs) = self.mainline.ingredients[egress].inner {
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
