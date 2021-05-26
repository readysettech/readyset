//! Logic for incorporating changes to a Soup graph into an already running graph.
//!
//! Performing a migration involves a number of steps:
//!
//!  - New nodes that are children of nodes in a different domain must be preceeded by an ingress
//!  - Egress nodes must be added to nodes that now have children in a different domain
//!  - Timestamp ingress nodes for existing domains must be connected to new base nodes
//!  - Timestamp ingress nodes must be added to all new domains
//!  - New nodes for existing domains must be sent to those domains
//!  - New domains must be booted up
//!  - Input channels must be set up for new base nodes
//!  - The graph must be analyzed for new materializations. These materializations must be
//!    *initialized* before data starts to flow to the new nodes. This may require two domains to
//!    communicate directly, and may delay migration completion.
//!  - Index requirements must be resolved, and checked for conflicts.
//!
//! Furthermore, these must be performed in the correct *order* so as to prevent dead- or
//! livelocks. This module defines methods for performing each step in relative isolation, as well
//! as a function for performing them in the right order.
//!
//! Beware, Here be dragons™

use dataflow::prelude::*;
use dataflow::{node, DomainRequest};
use metrics::counter;
use metrics::histogram;
use nom_sql::BinaryOperator;
use noria::metrics::recorded;
use noria::ReadySetError;
use std::collections::{HashMap, HashSet};
use std::time::Instant;

use crate::controller::{ControllerInner, WorkerIdentifier};
pub(crate) mod assignment;
mod augmentation;
pub(crate) mod materialization;
mod routing;
mod sharding;

#[derive(Clone)]
pub(super) enum ColumnChange {
    Add(String, DataType),
    Drop(usize),
}

/// A `Migration` encapsulates a number of changes to the Soup data flow graph.
///
/// Only one `Migration` can be in effect at any point in time. No changes are made to the running
/// graph until the `Migration` is committed (using `Migration::commit`).
// crate viz for tests
pub struct Migration<'a> {
    pub(super) mainline: &'a mut ControllerInner,
    pub(super) added: HashSet<NodeIndex>,
    pub(super) columns: Vec<(NodeIndex, ColumnChange)>,
    pub(super) readers: HashMap<NodeIndex, NodeIndex>,
    pub(super) worker: Option<WorkerIdentifier>,

    pub(super) start: Instant,
    pub(super) log: slog::Logger,

    /// Additional migration information provided by the client
    pub(super) context: HashMap<String, DataType>,
}

impl<'a> Migration<'a> {
    /// Add the given `Ingredient` to the Soup.
    ///
    /// The returned identifier can later be used to refer to the added ingredient.
    /// Edges in the data flow graph are automatically added based on the ingredient's reported
    /// `ancestors`.
    // crate viz for tests
    pub fn add_ingredient<S1, FS, S2, I>(&mut self, name: S1, fields: FS, i: I) -> NodeIndex
    where
        S1: ToString,
        S2: ToString,
        FS: IntoIterator<Item = S2>,
        I: Into<NodeOperator>,
    {
        let mut i = node::Node::new(name.to_string(), fields, i.into());
        i.on_connected(&self.mainline.ingredients);
        let parents = i.ancestors();
        assert!(!parents.is_empty());

        // add to the graph
        let ni = self.mainline.ingredients.add_node(i);
        info!(self.log,
              "adding new node";
              "node" => ni.index(),
              "type" => format!("{:?}", self.mainline.ingredients[ni])
        );

        // keep track of the fact that it's new
        self.added.insert(ni);
        // insert it into the graph
        for parent in parents {
            self.mainline.ingredients.add_edge(parent, ni, ());
        }
        // and tell the caller its id
        ni
    }

    /// Add the given `Base` to the Soup.
    ///
    /// The returned identifier can later be used to refer to the added ingredient.
    // crate viz for tests
    pub fn add_base<S1, FS, S2>(
        &mut self,
        name: S1,
        fields: FS,
        b: node::special::Base,
    ) -> NodeIndex
    where
        S1: ToString,
        S2: ToString,
        FS: IntoIterator<Item = S2>,
    {
        // add to the graph
        let ni = self
            .mainline
            .ingredients
            .add_node(node::Node::new(name.to_string(), fields, b));
        info!(self.log,
              "adding new base";
              "node" => ni.index(),
        );

        // keep track of the fact that it's new
        self.added.insert(ni);
        // insert it into the graph
        self.mainline
            .ingredients
            .add_edge(self.mainline.source, ni, ());
        // and tell the caller its id
        ni
    }

    /// Mark the given node as being beyond the materialization frontier.
    ///
    /// When a node is marked as such, it will quickly evict state after it is no longer
    /// immediately useful, making such nodes generally mostly empty. This reduces read
    /// performance (since most reads now need to do replays), but can significantly reduce memory
    /// overhead and improve write performance.
    ///
    /// Note that if a node is marked this way, all of its children transitively _also_ have to be
    /// marked.
    #[cfg(test)]
    pub(crate) fn mark_shallow(&mut self, ni: NodeIndex) {
        info!(self.log,
              "marking node as beyond materialization frontier";
              "node" => ni.index(),
        );
        self.mainline.ingredients.node_weight_mut(ni).unwrap().purge = true;

        if !self.added.contains(&ni) {
            unimplemented!("marking existing node as beyond materialization frontier");
        }
    }

    /// Returns the context of this migration
    pub(super) fn context(&self) -> &HashMap<String, DataType> {
        &self.context
    }

    /// Returns the universe in which this migration is operating in.
    /// If not specified, assumes `global` universe.
    pub(super) fn universe(&self) -> (DataType, Option<DataType>) {
        let id = match self.context.get("id") {
            Some(id) => id.clone(),
            None => "global".into(),
        };

        let group = match self.context.get("group") {
            Some(g) => Some(g.clone()),
            None => None,
        };

        (id, group)
    }

    /// Add a new column to a base node.
    ///
    /// Note that a default value must be provided such that old writes can be converted into this
    /// new type.
    // crate viz for tests
    pub fn add_column<S: ToString>(
        &mut self,
        node: NodeIndex,
        field: S,
        default: DataType,
    ) -> ReadySetResult<usize> {
        // not allowed to add columns to new nodes
        invariant!(!self.added.contains(&node));

        let field = field.to_string();
        let base = &mut self.mainline.ingredients[node];
        invariant!(base.is_base());

        // we need to tell the base about its new column and its default, so that old writes that
        // do not have it get the additional value added to them.
        let col_i1 = base.add_column(&field);
        // we can't rely on DerefMut, since it disallows mutating Taken nodes
        {
            let col_i2 = base.get_base_mut().unwrap().add_column(default.clone())?;
            invariant_eq!(col_i1, col_i2);
        }

        // also eventually propagate to domain clone
        self.columns.push((node, ColumnChange::Add(field, default)));

        Ok(col_i1)
    }

    /// Drop a column from a base node.
    // crate viz for tests
    pub fn drop_column(&mut self, node: NodeIndex, column: usize) -> ReadySetResult<()> {
        // not allowed to drop columns from new nodes
        invariant!(!self.added.contains(&node));

        let base = &mut self.mainline.ingredients[node];
        invariant!(base.is_base());

        // we need to tell the base about the dropped column, so that old writes that contain that
        // column will have it filled in with default values (this is done in Mutator).
        // we can't rely on DerefMut, since it disallows mutating Taken nodes
        base.get_base_mut().unwrap().drop_column(column)?;

        // also eventually propagate to domain clone
        self.columns.push((node, ColumnChange::Drop(column)));
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn graph(&self) -> &Graph {
        self.mainline.graph()
    }

    fn ensure_reader_for(
        &mut self,
        n: NodeIndex,
        name: Option<String>,
        operator: nom_sql::BinaryOperator,
    ) {
        use std::collections::hash_map::Entry;
        if let Entry::Vacant(e) = self.readers.entry(n) {
            // make a reader
            let r = node::special::Reader::new(n, operator);
            let mut r = if let Some(name) = name {
                self.mainline.ingredients[n].named_mirror(r, name)
            } else {
                self.mainline.ingredients[n].mirror(r)
            };
            if r.name().starts_with("SHALLOW_") {
                r.purge = true;
            }
            let r = self.mainline.ingredients.add_node(r);
            self.mainline.ingredients.add_edge(n, r, ());
            self.added.insert(r);
            e.insert(r);
        }
    }

    /// Set up the given node such that its output can be efficiently queried.
    ///
    /// To query into the maintained state, use `ControllerInner::get_getter`.
    pub fn maintain_anonymous(&mut self, n: NodeIndex, key: &[usize]) -> NodeIndex {
        self.ensure_reader_for(n, None, BinaryOperator::Equal);
        let ri = self.readers[&n];

        self.mainline.ingredients[ri]
            .with_reader_mut(|r| r.set_key(key))
            .unwrap();

        ri
    }

    /// Set up the given node such that its output can be efficiently queried.
    ///
    /// To query into the maintained state, use `ControllerInner::get_getter`.
    pub fn maintain(
        &mut self,
        name: String,
        n: NodeIndex,
        key: &[usize],
        operator: nom_sql::BinaryOperator,
    ) {
        self.ensure_reader_for(n, Some(name), operator);

        let ri = self.readers[&n];

        self.mainline.ingredients[ri]
            .with_reader_mut(|r| r.set_key(key))
            .unwrap();
    }

    /// Commit the changes introduced by this `Migration` to the master `Soup`.
    ///
    /// This will spin up an execution thread for each new thread domain, and hook those new
    /// domains into the larger Soup graph. The returned map contains entry points through which
    /// new updates should be sent to introduce them into the Soup.
    #[allow(clippy::cognitive_complexity)]
    pub(super) fn commit(self) -> Result<(), ReadySetError> {
        info!(self.log, "finalizing migration"; "#nodes" => self.added.len());

        let log = self.log;
        let start = self.start;
        let mut mainline = self.mainline;
        let mut new = self.added;
        let mut topo = mainline.topo_order(&new);

        // Shard the graph as desired
        let mut swapped0 = if let Some(shards) = mainline.sharding {
            let (t, swapped) =
                sharding::shard(&log, &mut mainline.ingredients, &mut new, &topo, shards)?;
            topo = t;

            swapped
        } else {
            HashMap::default()
        };

        // Assign domains
        assignment::assign(
            &log,
            &mut mainline.ingredients,
            &topo,
            &mut mainline.ndomains,
        )?;

        // Set up ingress and egress nodes
        let swapped1 = routing::add(
            &log,
            &mut mainline.ingredients,
            mainline.source,
            &mut new,
            &topo,
        )?;
        topo = mainline.topo_order(&new);

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
        let mut sorted_new = new.iter().collect::<Vec<_>>();
        sorted_new.sort();

        // Find all nodes for domains that have changed
        let changed_domains: HashSet<DomainIndex> = sorted_new
            .iter()
            .filter(|&&&ni| !mainline.ingredients[ni].is_dropped())
            .map(|&&ni| mainline.ingredients[ni].domain())
            .collect();

        let mut domain_new_nodes = sorted_new
            .iter()
            .filter(|&&&ni| ni != mainline.source)
            .filter(|&&&ni| !mainline.ingredients[ni].is_dropped())
            .map(|&&ni| (mainline.ingredients[ni].domain(), ni))
            .fold(HashMap::new(), |mut dns, (d, ni)| {
                dns.entry(d).or_insert_with(Vec::new).push(ni);
                dns
            });

        // Assign local addresses to all new nodes, and initialize them
        for (domain, nodes) in &mut domain_new_nodes {
            // Number of pre-existing nodes
            let mut nnodes = mainline.remap.get(domain).map(HashMap::len).unwrap_or(0);

            if nodes.is_empty() {
                // Nothing to do here
                continue;
            }

            let log = log.new(o!("domain" => domain.index()));

            // Give local addresses to every (new) node
            for &ni in nodes.iter() {
                debug!(log,
                       "assigning local index";
                       "type" => format!("{:?}", mainline.ingredients[ni]),
                       "node" => ni.index(),
                       "local" => nnodes
                );
                counter!(
                    recorded::DOMAIN_NODE_ADDED,
                    1,
                    "domain" => domain.index().to_string(),
                    "ntype" => (&mainline.ingredients[ni]).node_type_string()
                );

                let mut ip: IndexPair = ni.into();
                ip.set_local(unsafe { LocalNodeIndex::make(nnodes as u32) });
                mainline.ingredients[ni].set_finalized_addr(ip);
                mainline
                    .remap
                    .entry(*domain)
                    .or_insert_with(HashMap::new)
                    .insert(ni, ip);
                nnodes += 1;
            }

            // Initialize each new node
            for &ni in nodes.iter() {
                if mainline.ingredients[ni].is_internal() {
                    // Figure out all the remappings that have happened
                    // NOTE: this has to be *per node*, since a shared parent may be remapped
                    // differently to different children (due to sharding for example). we just
                    // allocate it once though.
                    let mut remap = mainline.remap[domain].clone();

                    // Parents in other domains have been swapped for ingress nodes.
                    // Those ingress nodes' indices are now local.
                    for (&(dst, src), &instead) in &swapped {
                        if dst != ni {
                            // ignore mappings for other nodes
                            continue;
                        }

                        let old = remap.insert(src, mainline.remap[domain][&instead]);
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

        if let Some(shards) = mainline.sharding {
            sharding::validate(&mainline.ingredients, &topo, shards)?
        };

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

        for &ni in &new {
            let n = &mainline.ingredients[ni];
            if ni != mainline.source && !n.is_dropped() {
                let di = n.domain();
                mainline
                    .domain_nodes
                    .entry(di)
                    .or_insert_with(Vec::new)
                    .push(ni);
            }
        }
        let mut uninformed_domain_nodes: HashMap<_, _> = changed_domains
            .iter()
            .map(|&di| {
                let mut m = mainline.domain_nodes[&di]
                    .iter()
                    .cloned()
                    .map(|ni| (ni, new.contains(&ni)))
                    .collect::<Vec<_>>();
                m.sort();
                (di, m)
            })
            .collect();

        // Boot up new domains (they'll ignore all updates for now)
        debug!(log, "booting new domains");
        for domain in changed_domains {
            if mainline.domains.contains_key(&domain) {
                // this is not a new domain
                continue;
            }

            let nodes = uninformed_domain_nodes.remove(&domain).unwrap();
            let d = mainline.place_domain(
                domain,
                mainline.ingredients[nodes[0].0].sharded_by().shards(),
                &log,
                nodes,
                self.worker.clone(),
            )?;
            mainline.domains.insert(domain, d);
        }

        // Add any new nodes to existing domains (they'll also ignore all updates for now)
        debug!(log, "mutating existing domains");
        augmentation::inform(&log, &mut mainline, uninformed_domain_nodes)?;

        // Tell all base nodes and base ingress children about newly added columns
        for (ni, change) in self.columns {
            let mut inform = if let ColumnChange::Add(..) = change {
                // we need to inform all of the base's children too,
                // so that they know to add columns to existing records when replaying
                mainline
                    .ingredients
                    .neighbors_directed(ni, petgraph::EdgeDirection::Outgoing)
                    .filter(|&eni| mainline.ingredients[eni].is_egress())
                    .flat_map(|eni| {
                        // find ingresses under this egress
                        mainline
                            .ingredients
                            .neighbors_directed(eni, petgraph::EdgeDirection::Outgoing)
                    })
                    .collect()
            } else {
                // ingress nodes don't need to know about deleted columns, because those are only
                // relevant when new writes enter the graph.
                Vec::new()
            };
            inform.push(ni);

            for ni in inform {
                let n = &mainline.ingredients[ni];
                let m = match change.clone() {
                    ColumnChange::Add(field, default) => DomainRequest::AddBaseColumn {
                        node: n.local_addr(),
                        field,
                        default,
                    },
                    ColumnChange::Drop(column) => DomainRequest::DropBaseColumn {
                        node: n.local_addr(),
                        column,
                    },
                };

                let domain = mainline.domains.get_mut(&n.domain()).unwrap();

                domain
                    .send_to_healthy_blocking::<()>(m, &mainline.workers)
                    .unwrap();
            }
        }

        // Set up inter-domain connections
        // NOTE: once we do this, we are making existing domains block on new domains!
        info!(log, "bringing up inter-domain connections");
        routing::connect(
            &log,
            &mut mainline.ingredients,
            &mut mainline.domains,
            &mainline.workers,
            &new,
        )?;

        // And now, the last piece of the puzzle -- set up materializations
        info!(log, "initializing new materializations");
        mainline.materializations.commit(
            &mut mainline.ingredients,
            &new,
            &mut mainline.domains,
            &mainline.workers,
        )?;

        histogram!(
            recorded::CONTROLLER_MIGRATION_TIME,
            start.elapsed().as_micros() as f64
        );
        warn!(log, "migration completed"; "ms" => start.elapsed().as_millis());

        Ok(())
    }
}
