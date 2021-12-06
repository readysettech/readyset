//! Logic for incorporating changes to a Noria graph into an already running graph.
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
//! This is split into two stages: the planning stage, where parts of the [`Leader`] are
//! cloned, and the list of new domains to spin up and messages to send to new and existing domains
//! is built (via the [`DomainMigrationPlan`]). If this completes without failing, a [`MigrationPlan`]
//! is created and then applied to the running [`Leader`].
//!
//! A failure during the planning stage is inconsequential, as no part of the running controller
//! is mutated. A failure during the apply stage currently might leave the cluster in an
//! inconsistent state. However, it is also likely that a failure in this stage is symptomatic
//! of a much larger problem (such as nodes being partitioned), seeing as the only things that
//! happen during application are domains being spun up and messages being sent.
//!
//! Beware, Here be slightly smaller dragons™

use dataflow::{node, DomainRequest};
use dataflow::{prelude::*, PostLookup};
use metrics::counter;
use metrics::histogram;
use noria::metrics::recorded;
use noria::ReadySetError;
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};
use tracing::{debug, debug_span, error, info, info_span, instrument, trace};

use crate::controller::migrate::materialization::{InvalidEdge, Materializations};
use crate::controller::migrate::scheduling::Scheduler;
use crate::controller::state::DataflowState;
use crate::controller::WorkerIdentifier;

pub(crate) mod assignment;
mod augmentation;
pub(crate) mod materialization;
mod routing;
mod scheduling;
mod sharding;

/// A [`DomainRequest`] with associated domain/shard information describing which domain it's for.
///
/// Used as part of [`DomainMigrationPlan`].
pub struct StoredDomainRequest {
    /// The index of the destination domain.
    pub domain: DomainIndex,
    /// A specific shard to send the request to. If `None`, sends to all shards.
    pub shard: Option<usize>,
    /// The request to send.
    pub req: DomainRequest,
}

impl StoredDomainRequest {
    pub async fn apply(self, mainline: &mut DataflowState) -> ReadySetResult<()> {
        let dom = mainline.domains.get_mut(&self.domain).ok_or_else(|| {
            ReadySetError::MigrationUnknownDomain {
                domain_index: self.domain.index(),
                shard: self.shard,
            }
        })?;
        if let DomainRequest::QueryReplayDone = self.req {
            debug!("waiting for a done message");

            invariant!(self.shard.is_none()); // QueryReplayDone isn't ever sent to just one shard

            let mut spins = 0;

            // FIXME(eta): this is a bit of a hack... (also, timeouts?)
            loop {
                if dom
                    .send_to_healthy::<bool>(DomainRequest::QueryReplayDone, &mainline.workers)
                    .await?
                    .into_iter()
                    .any(|x| x)
                {
                    break;
                }

                spins += 1;
                if spins == 10 {
                    info!("waiting for setup()-initiated replay to complete");
                    spins = 0;
                }
                std::thread::sleep(Duration::from_millis(200));
            }
        } else if let Some(shard) = self.shard {
            dom.send_to_healthy_shard::<()>(shard, self.req, &mainline.workers)
                .await?;
        } else {
            dom.send_to_healthy::<()>(self.req, &mainline.workers)
                .await?;
        }
        Ok(())
    }
}

/// A request to place a new domain, corresponding to the arguments passed to
/// [`Leader::place_domain`].
///
/// Used as part of [`DomainMigrationPlan`].
#[derive(Debug)]
pub struct PlaceRequest {
    /// The index the new domain will have.
    idx: DomainIndex,
    /// A map from domain shard to the worker to schedule the domain shard
    /// onto.
    shard_workers: Vec<WorkerIdentifier>,
    /// Indices of new nodes to add.
    ///
    /// FIXME: what the hell is the `bool` for? It seems entirely vestigial.
    nodes: Vec<(NodeIndex, bool)>,
}

/// A store for planned migration operations (spawning domains and sending messages).
///
/// This behaves a bit like a map of `DomainHandle`s.
pub struct DomainMigrationPlan {
    /// An (ordered!) list of domain requests to send on application.
    stored: Vec<StoredDomainRequest>,
    /// A list of domains to instantiate on application.
    place: Vec<PlaceRequest>,
    /// A map of valid domain indices to the number of shards in that domain.
    ///
    /// Used to validate sent messages during the planning stage.
    valid_domains: HashMap<DomainIndex, usize>,
}

/// A set of stored data sufficient to apply a migration.
pub struct MigrationPlan {
    // Basically all of these fields are just cloned from `Leader`.
    ingredients: Graph,
    domain_nodes: HashMap<DomainIndex, NodeMap<NodeIndex>>,
    ndomains: usize,
    remap: HashMap<DomainIndex, HashMap<NodeIndex, IndexPair>>,
    materializations: Materializations,
    // ...apart from this one
    dmp: DomainMigrationPlan,
}

impl MigrationPlan {
    /// Apply the migration plan to the provided `Leader`.
    ///
    /// If the plan fails, the `Leader`'s state is left unchanged; however, no attempt
    /// is made to roll back any destructive changes that may have occurred before the plan failed
    /// to apply.
    #[instrument(level = "info", name = "apply", skip(self, mainline))]
    pub async fn apply(self, mainline: &mut DataflowState) -> ReadySetResult<()> {
        let MigrationPlan {
            ingredients,
            domain_nodes,
            ndomains,
            remap,
            materializations,
            mut dmp,
        } = self;

        info!(
            new_domains = dmp.place.len(),
            messages = dmp.stored.len(),
            "applying migration plan",
        );

        let start = Instant::now();

        // We do *not* roll this back on failure, to try and reduce the risk of duplicating domain
        // indices.
        //
        // FIXME(eta): what about node indices?
        mainline.ndomains = ndomains;

        // Update the controller with the new state, but keep a copy of the old state for rollback.
        // If we don't do this, things like `place_domain` won't work.
        let ingredients = std::mem::replace(&mut mainline.ingredients, ingredients);
        let domain_nodes = std::mem::replace(&mut mainline.domain_nodes, domain_nodes);
        let remap = std::mem::replace(&mut mainline.remap, remap);
        let materializations = std::mem::replace(&mut mainline.materializations, materializations);

        let mut ret = Ok(());

        if let Err(e) = dmp.apply(mainline).await {
            error!(error = %e, "migration plan apply failed");
            mainline.ingredients = ingredients;
            mainline.domain_nodes = domain_nodes;
            mainline.remap = remap;
            mainline.materializations = materializations;
            ret = Err(ReadySetError::MigrationApplyFailed {
                source: Box::new(e),
            });
        } else {
            info!(ms = %start.elapsed().as_millis(), "migration plan applied");
        }

        ret
    }
}

impl DomainMigrationPlan {
    /// Make a new `DomainMigrationPlan`, noting which domains are valid based off the provided
    /// controller.
    pub fn new(mainline: &DataflowState) -> Self {
        Self {
            stored: vec![],
            place: vec![],
            valid_domains: mainline
                .domains
                .iter()
                .map(|(idx, hdl)| (*idx, hdl.shards()))
                .collect(),
        }
    }

    /// Enqueues a request to add a new domain `idx` with `nodes`, running on a worker per-shard
    /// given by `shard_workers`
    ///
    /// Arguments are passed to [`Leader::place_domain`] when the plan is applied.
    pub fn add_new_domain(
        &mut self,
        idx: DomainIndex,
        shard_workers: Vec<WorkerIdentifier>,
        nodes: Vec<(NodeIndex, bool)>,
    ) {
        self.place.push(PlaceRequest {
            idx,
            shard_workers,
            nodes,
        });
    }

    /// Return the number of shards a given domain has.
    pub fn num_shards(&self, domain: DomainIndex) -> ReadySetResult<usize> {
        self.valid_domains.get(&domain).copied().ok_or_else(|| {
            ReadySetError::MigrationUnknownDomain {
                domain_index: domain.index(),
                shard: None,
            }
        })
    }

    /// Apply all stored changes using the given controller object, placing new domains and sending
    /// messages added since the last time this method was called.
    pub async fn apply(&mut self, mainline: &mut DataflowState) -> ReadySetResult<()> {
        for place in self.place.drain(..) {
            let d = mainline
                .place_domain(place.idx, place.shard_workers, place.nodes)
                .await?;
            mainline.domains.insert(place.idx, d);
        }
        for req in std::mem::take(&mut self.stored) {
            req.apply(mainline).await?;
        }
        Ok(())
    }

    /// Enqueue a message to be sent to a specific shard of a domain on plan application.
    ///
    /// Like [`DomainHandle::send_to_healthy_shard_blocking`], but includes the `domain` to which
    /// the command should apply.
    pub fn add_message_for_shard(
        &mut self,
        domain: DomainIndex,
        shard: usize,
        req: DomainRequest,
    ) -> ReadySetResult<()> {
        if self
            .valid_domains
            .get(&domain)
            .map(|n_shards| *n_shards > shard)
            .unwrap_or(false)
        {
            self.stored.push(StoredDomainRequest {
                domain,
                shard: Some(shard),
                req,
            });
            Ok(())
        } else {
            Err(ReadySetError::MigrationUnknownDomain {
                domain_index: domain.index(),
                shard: Some(shard),
            })
        }
    }

    /// Enqueue a message to be sent to all shards of a domain on plan application.
    ///
    /// Like [`DomainHandle::send_to_healthy_blocking`], but includes the `domain` to which the
    /// command should apply.
    pub fn add_message(&mut self, domain: DomainIndex, req: DomainRequest) -> ReadySetResult<()> {
        if self.valid_domains.contains_key(&domain) {
            self.stored.push(StoredDomainRequest {
                domain,
                shard: None,
                req,
            });
            Ok(())
        } else {
            Err(ReadySetError::MigrationUnknownDomain {
                domain_index: domain.index(),
                shard: None,
            })
        }
    }
}

fn topo_order(ingredients: &Graph, source: NodeIndex, new: &HashSet<NodeIndex>) -> Vec<NodeIndex> {
    let mut topo_list = Vec::with_capacity(new.len());
    let mut topo = petgraph::visit::Topo::new(&ingredients);
    while let Some(node) = topo.next(&ingredients) {
        if node == source {
            continue;
        }
        if ingredients[node].is_dropped() {
            continue;
        }
        if !new.contains(&node) {
            continue;
        }
        topo_list.push(node);
    }
    topo_list
}

#[derive(Clone)]
pub(super) enum ColumnChange {
    Add(String, DataType),
    Drop(usize),
}

/// Add messages to the dmp to inform nodes that columns have been added or removed
fn inform_col_changes(
    dmp: &mut DomainMigrationPlan,
    columns: &[(NodeIndex, ColumnChange)],
    ingredients: &Graph,
) -> ReadySetResult<()> {
    // Tell all base nodes and base ingress children about newly added columns
    for (ni, change) in columns {
        let mut inform = if let ColumnChange::Add(..) = change {
            // we need to inform all of the base's children too,
            // so that they know to add columns to existing records when replaying
            ingredients
                .neighbors_directed(*ni, petgraph::EdgeDirection::Outgoing)
                .filter(|&eni| ingredients[eni].is_egress())
                .flat_map(|eni| {
                    // find ingresses under this egress
                    ingredients.neighbors_directed(eni, petgraph::EdgeDirection::Outgoing)
                })
                .collect()
        } else {
            // ingress nodes don't need to know about deleted columns, because those are only
            // relevant when new writes enter the graph.
            Vec::new()
        };
        inform.push(*ni);

        for ni in inform {
            let n = &ingredients[ni];
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

            dmp.add_message(n.domain(), m)?;
        }
    }
    Ok(())
}

/// A `Migration` encapsulates a number of changes to the Soup data flow graph.
///
/// Only one `Migration` can be in effect at any point in time. No changes are made to the running
/// graph until the `Migration` is committed (using `Migration::commit`).
pub struct Migration {
    pub(super) source: NodeIndex,
    pub(super) ingredients: Graph,
    pub(super) added: HashSet<NodeIndex>,
    pub(super) columns: Vec<(NodeIndex, ColumnChange)>,
    pub(super) readers: HashMap<NodeIndex, NodeIndex>,
    pub(super) worker: Option<WorkerIdentifier>,

    pub(super) start: Instant,

    /// Additional migration information provided by the client
    pub(super) context: HashMap<String, DataType>,
}

impl Migration {
    /// Add the given `Ingredient` to the dataflow graph.
    ///
    /// The returned identifier can later be used to refer to the added ingredient.
    /// Edges in the data flow graph are automatically added based on the ingredient's reported
    /// `ancestors`.
    pub fn add_ingredient<S1, FS, S2, I>(&mut self, name: S1, fields: FS, i: I) -> NodeIndex
    where
        S1: ToString,
        S2: ToString,
        FS: IntoIterator<Item = S2>,
        I: Into<NodeOperator>,
    {
        let mut i = node::Node::new(name.to_string(), fields, i.into());
        i.on_connected(&self.ingredients);
        // TODO(peter): Should we change this function signature to return a ReadySetResult?
        #[allow(clippy::unwrap_used)]
        let parents = i.ancestors().unwrap();
        assert!(!parents.is_empty());

        // add to the graph
        let ni = self.ingredients.add_node(i);
        debug!(
            node = ni.index(),
            node_type = ?self.ingredients[ni],
            "adding new node"
        );

        // keep track of the fact that it's new
        self.added.insert(ni);
        // insert it into the graph
        for parent in parents {
            self.ingredients.add_edge(parent, ni, ());
        }
        // and tell the caller its id
        ni
    }

    /// Add the given `Base` to the dataflow graph.
    ///
    /// The returned identifier can later be used to refer to the added ingredient.
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
            .ingredients
            .add_node(node::Node::new(name.to_string(), fields, b));
        debug!(node = ni.index(), "adding new base");

        // keep track of the fact that it's new
        self.added.insert(ni);
        // insert it into the graph
        self.ingredients.add_edge(self.source, ni, ());
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
        debug!(
            node = ni.index(),
            "marking node as beyond materialization frontier"
        );
        self.ingredients.node_weight_mut(ni).unwrap().purge = true;

        if !self.added.contains(&ni) {
            unimplemented!("marking existing node as beyond materialization frontier");
        }
    }

    /// Returns the context of this migration
    pub(super) fn context(&self) -> &HashMap<String, DataType> {
        &self.context
    }

    /// Add a new column to a base node.
    pub fn add_column<S: ToString>(
        &mut self,
        node: NodeIndex,
        field: S,
        default: DataType,
    ) -> ReadySetResult<usize> {
        // not allowed to add columns to new nodes
        invariant!(!self.added.contains(&node));

        let field = field.to_string();
        let base = &mut self.ingredients[node];
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
    pub fn drop_column(&mut self, node: NodeIndex, column: usize) -> ReadySetResult<()> {
        // not allowed to drop columns from new nodes
        invariant!(!self.added.contains(&node));

        let base = &mut self.ingredients[node];
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
        &self.ingredients
    }

    /// Ensure that a reader node exists as a child of `n`, optionally with the given name and set
    /// of post-lookup operations, returning the index of that reader.
    fn ensure_reader_for(
        &mut self,
        n: NodeIndex,
        name: Option<String>,
        post_lookup: PostLookup,
    ) -> NodeIndex {
        use std::collections::hash_map::Entry;
        match self.readers.entry(n) {
            Entry::Occupied(ni) => *ni.into_mut(),
            Entry::Vacant(e) => {
                // make a reader
                let r = node::special::Reader::new(n, post_lookup);
                let mut r = if let Some(name) = name {
                    self.ingredients[n].named_mirror(r, name)
                } else {
                    self.ingredients[n].mirror(r)
                };
                if r.name().starts_with("SHALLOW_") {
                    r.purge = true;
                }
                let r = self.ingredients.add_node(r);
                self.ingredients.add_edge(n, r, ());
                self.added.insert(r);
                *e.insert(r)
            }
        }
    }

    /// Set up the given node such that its output can be efficiently queried.
    ///
    /// To query into the maintained state, use `Leader::get_getter`.
    pub fn maintain_anonymous(&mut self, n: NodeIndex, index: &Index) -> NodeIndex {
        let ri = self.ensure_reader_for(n, None, Default::default());

        #[allow(clippy::unwrap_used)] // we know it's a reader - we just made it!
        self.ingredients[ri]
            .as_mut_reader()
            .unwrap()
            .set_index(index);

        ri
    }

    /// Set up the given node such that its output can be efficiently queried, with the given
    /// [`PostLookup`] operations to be performed on the results of all lookups
    ///
    /// To query into the maintained state, use `Leader::get_getter`.
    pub fn maintain_anonymous_with_post_lookup(
        &mut self,
        n: NodeIndex,
        index: &Index,
        post_lookup: PostLookup,
    ) -> NodeIndex {
        let ri = self.ensure_reader_for(n, None, post_lookup);

        #[allow(clippy::unwrap_used)] // we know it's a reader - we just made it!
        self.ingredients[ri]
            .as_mut_reader()
            .unwrap()
            .set_index(index);

        ri
    }

    /// Set up the given node such that its output can be efficiently queried.
    ///
    /// To query into the maintained state, use `Leader::get_getter`.
    pub fn maintain(&mut self, name: String, n: NodeIndex, index: &Index, post_lookup: PostLookup) {
        let ri = self.ensure_reader_for(n, Some(name), post_lookup);

        #[allow(clippy::unwrap_used)] // we know it's a reader - we just made it!
        self.ingredients[ri]
            .as_mut_reader()
            .unwrap()
            .set_index(index);
    }

    /// Build a `MigrationPlan` for this migration, and apply it if the planning stage succeeds.
    pub(super) async fn commit(self, mainline: &mut DataflowState) -> ReadySetResult<()> {
        let start = self.start;

        let plan = self
            .plan(mainline)
            .map_err(|e| ReadySetError::MigrationPlanFailed {
                source: Box::new(e),
            })?;
        plan.apply(mainline).await?;

        histogram!(
            recorded::CONTROLLER_MIGRATION_TIME,
            start.elapsed().as_micros() as f64
        );

        Ok(())
    }

    /// Build a `MigrationPlan` for this migration, computing all necessary changes to the
    /// controller to make it happen.
    ///
    /// See the module-level docs for more information on what a migration entails.
    #[allow(clippy::cognitive_complexity)]
    pub(super) fn plan(self, mainline: &mut DataflowState) -> ReadySetResult<MigrationPlan> {
        let span = info_span!("plan");
        let _g = span.enter();
        debug!(num_nodes = self.added.len(), "finalizing migration");

        let start = self.start;
        let mut ingredients = self.ingredients;
        let source = self.source;
        let mut new = self.added;
        let mut ndomains = mainline.ndomains;
        let mut remap = mainline.remap.clone();
        let mut topo = topo_order(&ingredients, source, &new);
        // Tracks partially materialized nodes that were duplicated as fully materialized in this
        // planning stage.
        let mut local_redundant_partial: HashMap<NodeIndex, NodeIndex> = Default::default();

        // Shard the graph as desired
        let mut swapped0 = if let Some(shards) = mainline.sharding {
            let (t, swapped) = sharding::shard(&mut ingredients, &mut new, &topo, shards)?;
            topo = t;

            swapped
        } else {
            HashMap::default()
        };

        // Assign domains
        assignment::assign(
            &mut ingredients,
            &topo,
            &mut ndomains,
            &mainline.node_restrictions,
        )?;

        // Set up ingress and egress nodes
        let swapped1 = routing::add(&mut ingredients, source, &mut new, &topo)?;

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
                        if ingredients.find_edge(src, instead).is_some() {
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
        let mut swapped = swapped0;
        loop {
            let mut sorted_new = new.iter().collect::<Vec<_>>();
            sorted_new.sort();

            // Find all nodes for domains that have changed
            let changed_domains: HashSet<DomainIndex> = sorted_new
                .iter()
                .filter(|&&&ni| !ingredients[ni].is_dropped())
                .map(|&&ni| ingredients[ni].domain())
                .collect();

            let mut domain_new_nodes = sorted_new
                .iter()
                .filter(|&&&ni| ni != source)
                .filter(|&&&ni| !ingredients[ni].is_dropped())
                .map(|&&ni| (ingredients[ni].domain(), ni))
                .fold(HashMap::new(), |mut dns, (d, ni)| {
                    dns.entry(d).or_insert_with(Vec::new).push(ni);
                    dns
                });

            // Assign local addresses to all new nodes, and initialize them
            for (domain, nodes) in &mut domain_new_nodes {
                // Number of pre-existing nodes
                let mut nnodes = remap.get(domain).map(HashMap::len).unwrap_or(0);

                if nodes.is_empty() {
                    // Nothing to do here
                    continue;
                }

                let span = debug_span!("domain", domain = domain.index());
                let _g = span.enter();

                // Give local addresses to every (new) node
                for &ni in nodes.iter() {
                    debug!(
                        node_type = ?ingredients[ni],
                        node = ni.index(),
                        local = nnodes,
                        "assigning local index"
                    );
                    counter!(
                        recorded::DOMAIN_NODE_ADDED,
                        1,
                        "domain" => domain.index().to_string(),
                        "ntype" => (&ingredients[ni]).node_type_string(),
                        "node" => nnodes.to_string()
                    );

                    let mut ip: IndexPair = ni.into();
                    ip.set_local(unsafe { LocalNodeIndex::make(nnodes as u32) });
                    ingredients[ni].set_finalized_addr(ip);
                    remap
                        .entry(*domain)
                        .or_insert_with(HashMap::new)
                        .insert(ni, ip);
                    nnodes += 1;
                }

                // Initialize each new node
                for &ni in nodes.iter() {
                    if ingredients[ni].is_internal() {
                        // Figure out all the remappings that have happened
                        // NOTE: this has to be *per node*, since a shared parent may be remapped
                        // differently to different children (due to sharding for example). we just
                        // allocate it once though.
                        let mut remap_ = remap[domain].clone();

                        // Parents in other domains have been swapped for ingress nodes.
                        // Those ingress nodes' indices are now local.
                        for (&(dst, src), &instead) in &swapped {
                            if dst != ni {
                                // ignore mappings for other nodes
                                continue;
                            }

                            remap_.insert(src, remap[domain][&instead]);
                        }

                        trace!(node = ni.index(), "initializing new node");
                        ingredients.node_weight_mut(ni).unwrap().on_commit(&remap_);
                    }
                }
            }

            topo = topo_order(&ingredients, mainline.source, &new);

            if let Some(shards) = mainline.sharding {
                sharding::validate(&ingredients, &topo, shards)?
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

            let mut domain_nodes = mainline.domain_nodes.clone();

            for &ni in &new {
                let n = &ingredients[ni];
                if ni != source && !n.is_dropped() {
                    let di = n.domain();
                    domain_nodes
                        .entry(di)
                        .or_default()
                        .insert(n.local_addr(), ni);
                }
            }
            let mut uninformed_domain_nodes: HashMap<_, _> = changed_domains
                .iter()
                .map(|&di| {
                    let mut m = domain_nodes[&di]
                        .values()
                        .cloned()
                        .map(|ni| (ni, new.contains(&ni)))
                        .collect::<Vec<_>>();
                    m.sort();
                    (di, m)
                })
                .collect();

            // Boot up new domains (they'll ignore all updates for now)
            debug!("booting new domains");
            let mut dmp = DomainMigrationPlan::new(mainline);
            let mut scheduler = Scheduler::new(mainline, &self.worker, &ingredients)?;

            for domain in changed_domains {
                if mainline.domains.contains_key(&domain) {
                    // this is not a new domain
                    continue;
                }

                let nodes = uninformed_domain_nodes.remove(&domain).unwrap();
                let worker_shards = scheduler.schedule_domain(domain, &nodes)?;

                let num_shards = worker_shards.len();
                dmp.add_new_domain(domain, worker_shards, nodes);
                dmp.valid_domains.insert(domain, num_shards);
            }

            // And now, the last piece of the puzzle -- set up materializations
            debug!("initializing new materializations");
            let mut materializations = mainline.materializations.clone();

            materializations.extend(&mut ingredients, &new)?;
            if let Some(InvalidEdge { parent, child }) =
                materializations.validate(&ingredients, &new)?
            {
                debug!(
                    ?child,
                    ?parent,
                    "rerouting full node found below partial node",
                );

                // Find fully materialized equivalent of parent or create one
                let (duplicate_index, is_new) =
                    if let Some(idx) = mainline.materializations.get_redundant(&parent) {
                        (*idx, false)
                    } else if let Some(idx) = local_redundant_partial.get(&parent) {
                        (*idx, false)
                    } else {
                        // create new node in the same domain as old
                        let duplicate_node = ingredients[parent].duplicate();
                        // add to graph
                        let idx = ingredients.add_node(duplicate_node);
                        local_redundant_partial.insert(parent, idx);
                        (idx, true)
                    };

                ingredients.add_edge(duplicate_index, child, ());
                if is_new {
                    // Recreate edges coming into parent on duplicate
                    let incoming: Vec<_> = ingredients
                        .neighbors_directed(parent, petgraph::EdgeDirection::Incoming)
                        .collect();
                    for ni in incoming {
                        ingredients.add_edge(ni, duplicate_index, ());
                    }
                    // Add to new nodes for processing in next loop iteration
                    new.insert(duplicate_index);
                }
                // Indicate that the incoming nodes have changed. This entry will be read during
                // the remapping stage in the next iteration of the loop
                swapped.insert((child, parent), duplicate_index);
                // remove old edge
                let old_edge = ingredients.find_edge(parent, child).unwrap();
                ingredients.remove_edge(old_edge);
            } else {
                // We have successfully made a valid graph! Now we can inform the dmp of all the
                // changes
                inform_col_changes(&mut dmp, &self.columns, &ingredients)?;
                // Add any new nodes to existing domains (they'll also ignore all updates for now)
                debug!("mutating existing domains");
                augmentation::inform(source, &mut ingredients, &mut dmp, uninformed_domain_nodes)?;

                // Set up inter-domain connections
                debug!("bringing up inter-domain connections");
                routing::connect(&ingredients, &mut dmp, &new)?;

                materializations.commit(&mut ingredients, &new, &mut dmp)?;

                info!(
                    ms = ?start.elapsed().as_millis(),
                    "migration planning completed"
                );

                materializations.extend_redundant_partial(local_redundant_partial);
                return Ok(MigrationPlan {
                    ingredients,
                    domain_nodes,
                    ndomains,
                    remap,
                    materializations,
                    dmp,
                });
            }
        }
    }
}
