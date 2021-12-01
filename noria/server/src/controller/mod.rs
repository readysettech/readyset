use crate::controller::inner::Leader;
use crate::controller::migrate::Migration;
use crate::controller::recipe::Recipe;
use crate::coordination::do_noria_rpc;
use crate::worker::{WorkerRequest, WorkerRequestKind};
use crate::{Config, ReadySetResult, VolumeId};
use anyhow::{format_err, Context};
use futures_util::StreamExt;
use hyper::http::{Method, StatusCode};
use itertools::Itertools;
use launchpad::select;
use metrics::{counter, histogram};
use nom_sql::SqlQuery;
use noria::consensus::{
    Authority, AuthorityControl, AuthorityWorkerHeartbeatResponse, GetLeaderResult,
    WorkerDescriptor, WorkerId,
};
use noria::metrics::recorded;
use noria::{ControllerDescriptor, ReplicationOffset};
use noria_errors::{internal, internal_err, ReadySetError};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use stream_cancel::Valve;
use tokio::sync::mpsc::{Receiver, Sender, UnboundedReceiver, UnboundedSender};
use tokio::sync::Notify;
use tracing::{error, info, warn};
use tracing_futures::Instrument;
use url::Url;

mod domain_handle;
mod inner;
mod keys;
pub(crate) mod migrate; // crate viz for tests
mod mir_to_flow;
pub(crate) mod recipe; // crate viz for tests
pub(crate) mod schema;
pub(crate) mod sql; // crate viz for tests

/// Time between leader state change checks without thread parking.
const LEADER_STATE_CHECK_INTERVAL: Duration = Duration::from_secs(1);
/// Amount of time to wait for watches on the authority.
const WATCH_DURATION: Duration = Duration::from_secs(5);

/// A set of placement restrictions applied to a domain
/// that a dataflow node is in. Each base table node can have
/// a set of DomainPlacementRestrictions. A domain's
/// DomainPlacementRestriction is the merged set of restrictions
/// of all contained dataflow nodes.
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct DomainPlacementRestriction {
    worker_volume: Option<VolumeId>,
}

/// The key for a DomainPlacemnetRestriction for a dataflow node.
/// Each dataflow node, shard pair may have a DomainPlacementRestriction.
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash, Debug)]
pub struct NodeRestrictionKey {
    node_name: String,
    shard: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct ControllerState {
    pub(crate) config: Config,

    recipe_version: usize,
    recipes: Vec<String>,
    replication_offset: Option<ReplicationOffset>,
    // Serde requires hash map's be keyed by a string, we workaround this by
    // serializing the hashmap as a tuple list.
    #[serde(with = "serde_with::rust::hashmap_as_tuple_list")]
    node_restrictions: HashMap<NodeRestrictionKey, DomainPlacementRestriction>,
}
impl ControllerState {
    /// This method interates over the recipes currently installed and only keeps
    /// the ones for CREATE/ALTER/DROP TABLE and CREATE VIEW.
    /// This option is pretty risky and should only be used if noria gets into an
    /// unmanagable state. In theory this will remove all of noria specific state
    /// preventing bad migrations, and only keep the DDL state, required to properly
    /// keep the base tables and the binlog replication functional.
    fn reset(&mut self) {
        let new_recipe = self
            .recipes
            .iter()
            .map(|q| Recipe::clean_queries(q))
            .flatten()
            .filter_map(|q| nom_sql::parse_query(nom_sql::Dialect::MySQL, &q).ok())
            .filter(|q| {
                matches!(
                    q,
                    SqlQuery::CreateTable(_)
                        | SqlQuery::DropTable(_)
                        | SqlQuery::AlterTable(_)
                        | SqlQuery::RenameTable(_)
                        | SqlQuery::CreateView(_),
                )
            })
            .join(";\n");

        self.recipes = vec![new_recipe];
        self.recipe_version = 1;
    }
}

pub struct Worker {
    healthy: bool,
    uri: Url,
    http: reqwest::Client,
    region: Option<String>,
    reader_only: bool,
    /// Volume associated with this worker's server.
    volume_id: Option<VolumeId>,
}

impl Worker {
    pub fn new(
        instance_uri: Url,
        region: Option<String>,
        reader_only: bool,
        volume_id: Option<VolumeId>,
    ) -> Self {
        Worker {
            healthy: true,
            uri: instance_uri,
            http: reqwest::Client::new(),
            region,
            reader_only,
            volume_id,
        }
    }
    pub async fn rpc<T: DeserializeOwned>(&self, req: WorkerRequestKind) -> ReadySetResult<T> {
        let body = hyper::Body::from(bincode::serialize(&req)?);
        Ok(do_noria_rpc(self.http.post(self.uri.join("worker_request")?).body(body)).await?)
    }
}

/// Type alias for "a worker's URI" (as reported in a `RegisterPayload`).
type WorkerIdentifier = Url;

/// Channel that can be used to pass errors from the leader back to the controller so that we can
/// gracefully kill the controller loop.
pub struct ReplicationErrorChannel {
    sender: UnboundedSender<ReadySetError>,
    receiver: UnboundedReceiver<ReadySetError>,
}

impl ReplicationErrorChannel {
    fn new() -> Self {
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        Self { sender, receiver }
    }

    fn sender(&self) -> UnboundedSender<ReadySetError> {
        self.sender.clone()
    }
}

/// An update on the leader election and failure detection.
#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub(crate) enum AuthorityUpdate {
    /// The current leader has changed.
    ///
    /// The King is dead; long live the King!
    LeaderChange(ControllerDescriptor),
    /// We are now the new leader.
    WonLeaderElection(ControllerState),
    /// New worker detected
    NewWorkers(Vec<WorkerDescriptor>),
    /// Worker failed.
    FailedWorkers(Vec<WorkerDescriptor>),
    /// An error occurred in the authority thread, which won't be restarted.
    AuthorityError(anyhow::Error),
}

/// An HTTP request made to a controller.
pub struct ControllerRequest {
    /// The HTTP method used.
    pub method: Method,
    /// The path of the request.
    pub path: String,
    /// The request's query string.
    pub query: Option<String>,
    /// The request body.
    pub body: hyper::body::Bytes,
    /// Sender to send the response down.
    pub reply_tx: tokio::sync::oneshot::Sender<Result<Result<Vec<u8>, Vec<u8>>, StatusCode>>,
}

/// A request made from a `Handle` to a controller.
pub enum HandleRequest {
    /// Inquires whether the controller is ready to accept requests (but see below for caveats).
    ///
    /// This is primarily used in tests; in this context, "ready" means "controller is the leader,
    /// with at least 1 worker attached". The result gets sent down the provided sender.
    #[allow(dead_code)]
    QueryReadiness(tokio::sync::oneshot::Sender<bool>),
    /// Performs a manual migration.
    PerformMigration {
        /// The migration function to perform.
        func: Box<
            dyn FnOnce(&mut crate::controller::migrate::Migration) -> ReadySetResult<()>
                + Send
                + 'static,
        >,
        /// The result of the migration gets sent down here.
        done_tx: tokio::sync::oneshot::Sender<ReadySetResult<()>>,
    },
}

/// A wrapper for the control plane of the server instance that handles: leader election,
/// control rpcs, and requests originated from this server's instance's `Handle`.
pub struct Controller {
    /// If we are the leader, the leader object to use for performing leader operations.
    pub(crate) inner: Option<Leader>,
    /// The `Authority` structure used for leadership elections & such state.
    authority: Arc<Authority>,
    /// Channel to the `Worker` running inside this server instance.
    ///
    /// This is used to convey changes in leadership state.
    worker_tx: Sender<WorkerRequest>,
    /// Receives external HTTP requests.
    http_rx: Receiver<ControllerRequest>,
    /// Receives requests from the controller's `Handle`.
    handle_rx: Receiver<HandleRequest>,
    /// A `ControllerDescriptor` that describes this server instance.
    our_descriptor: ControllerDescriptor,
    /// Valve for shutting down; triggered by the `Handle` when `Handle::shutdown()` is called.
    valve: Valve,
    /// The descriptor of the worker this controller's server is running.
    worker_descriptor: WorkerDescriptor,
    /// A handle to the authority task.
    authority_task: Option<tokio::task::JoinHandle<anyhow::Result<()>>>,
    /// The config associated with this controller's server.
    config: Config,
    /// Whether we are the leader and ready to handle requests.
    leader_ready: bool,
    /// A notify to be passed to leader's when created, used to notify the Controller that the
    /// leader is ready to handle requests.
    leader_ready_notification: Arc<Notify>,

    /// Channel that the replication task, if it exists, can use to propagate updates back to
    /// the parent controller.
    replication_error_channel: ReplicationErrorChannel,

    /// Indicates if state should be reset to only reflect DDL changes, and erase all Noria
    /// specific recipes. Also flattens all DDL changes into a single recipe.
    should_reset_state: bool,
}

impl Controller {
    pub(crate) fn new(
        authority: Arc<Authority>,
        worker_tx: Sender<WorkerRequest>,
        controller_rx: Receiver<ControllerRequest>,
        handle_rx: Receiver<HandleRequest>,
        our_descriptor: ControllerDescriptor,
        shutoff_valve: Valve,
        worker_descriptor: WorkerDescriptor,
        config: Config,
        should_reset_state: bool,
    ) -> Self {
        Self {
            inner: None,
            authority,
            worker_tx,
            http_rx: controller_rx,
            handle_rx,
            our_descriptor,
            valve: shutoff_valve,
            worker_descriptor,
            config,
            leader_ready: false,
            leader_ready_notification: Arc::new(Notify::new()),
            replication_error_channel: ReplicationErrorChannel::new(),
            authority_task: None,
            should_reset_state,
        }
    }

    /// Run the provided *blocking* closure with the `Leader` and the `Authority` if this
    /// server instance is currently the leader.
    ///
    /// If it isn't, returns `Err(ReadySetError::NotLeader)`, and doesn't run the closure.
    async fn with_controller_blocking<F, T>(&mut self, func: F) -> ReadySetResult<T>
    where
        F: FnOnce(&mut Leader, Arc<Authority>) -> ReadySetResult<T>,
    {
        // FIXME: this is potentially slow, and it's only like this because borrowck sucks
        let auth = self.authority.clone();
        if let Some(ref mut ci) = self.inner {
            Ok(tokio::task::block_in_place(|| func(ci, auth))?)
        } else {
            Err(ReadySetError::NotLeader)
        }
    }

    fn controller_inner(&mut self) -> ReadySetResult<&mut Leader> {
        if let Some(ref mut ci) = self.inner {
            Ok(ci)
        } else {
            Err(ReadySetError::NotLeader)
        }
    }

    /// Send the provided `WorkerRequestKind` to the worker running in the same server instance as
    /// this controller wrapper, but don't bother waiting for the response.
    ///
    /// Not waiting for the response avoids deadlocking when the controller and worker are in the
    /// same noria-server instance.
    async fn send_worker_request(&mut self, kind: WorkerRequestKind) -> ReadySetResult<()> {
        let (tx, _rx) = tokio::sync::oneshot::channel();
        self.worker_tx
            .send(WorkerRequest { kind, done_tx: tx })
            .await
            .map_err(|e| internal_err(format!("failed to send to instance worker: {}", e)))?;
        Ok(())
    }

    async fn handle_controller_request(&mut self, req: ControllerRequest) -> ReadySetResult<()> {
        let ControllerRequest {
            method,
            path,
            query,
            body,
            reply_tx,
        } = req;

        let request_start = Instant::now();
        let ret: Result<Result<Vec<u8>, Vec<u8>>, StatusCode> = {
            let resp = self
                .with_controller_blocking(|ctrl, auth| {
                    Ok(ctrl.external_request(method.clone(), path.clone(), query, body, &auth))
                })
                .await;
            match resp {
                // returned from `Leader::external_request`:
                Ok(Ok(r)) => Ok(Ok(r)),
                Ok(Err(ReadySetError::NoQuorum)) => Err(StatusCode::SERVICE_UNAVAILABLE),
                Ok(Err(ReadySetError::UnknownEndpoint)) => Err(StatusCode::NOT_FOUND),
                Ok(Err(e)) => Ok(Err(bincode::serialize(&e)?)),
                // errors returned by `with_controller_blocking`:
                Err(ReadySetError::NotLeader) => Err(StatusCode::SERVICE_UNAVAILABLE),
                Err(e) => Ok(Err(bincode::serialize(&e)?)),
            }
        };

        counter!(
            recorded::CONTROLLER_RPC_OVERALL_TIME,
            request_start.elapsed().as_micros() as u64,
            "path" => path.clone()
        );

        histogram!(
            recorded::CONTROLLER_RPC_REQUEST_TIME,
            request_start.elapsed().as_micros() as f64,
            "path" => path
        );

        if reply_tx.send(ret).is_err() {
            warn!("client hung up");
        }
        Ok(())
    }

    async fn handle_handle_request(&mut self, req: HandleRequest) -> ReadySetResult<()> {
        match req {
            HandleRequest::QueryReadiness(tx) => {
                let done = self.leader_ready
                    && self
                        .inner
                        .as_ref()
                        .map(|ctrl| !ctrl.workers.is_empty())
                        .unwrap_or(false);
                if tx.send(done).is_err() {
                    warn!("readiness query sender hung up!");
                }
            }
            HandleRequest::PerformMigration { func, done_tx } => {
                let inner = self.controller_inner()?;
                let ret = inner.migrate(move |m| func(m)).await?;
                if done_tx.send(ret).is_err() {
                    warn!("handle-based migration sender hung up!");
                }
            }
        }
        Ok(())
    }

    async fn handle_authority_update(&mut self, msg: AuthorityUpdate) -> ReadySetResult<()> {
        match msg {
            AuthorityUpdate::LeaderChange(descr) => {
                self.send_worker_request(WorkerRequestKind::NewController {
                    controller_uri: descr.controller_uri,
                })
                .await?;
            }
            AuthorityUpdate::WonLeaderElection(state) => {
                info!("won leader election, creating Leader");
                let mut leader = Leader::new(
                    state.clone(),
                    self.our_descriptor.controller_uri.clone(),
                    self.authority.clone(),
                    self.config.replication_url.clone(),
                    self.config.replication_server_id,
                );
                leader
                    .start(
                        self.leader_ready_notification.clone(),
                        self.replication_error_channel.sender(),
                    )
                    .await;
                self.leader_ready = false;

                self.inner = Some(leader);
                self.send_worker_request(WorkerRequestKind::NewController {
                    controller_uri: self.our_descriptor.controller_uri.clone(),
                })
                .await?;
            }
            AuthorityUpdate::NewWorkers(w) if self.inner.is_some() => {
                let inner = self.controller_inner()?;
                for worker in w {
                    inner.handle_register_from_authority(worker).await?;
                }
            }
            AuthorityUpdate::FailedWorkers(w) if self.inner.is_some() => {
                let inner = self.controller_inner()?;
                inner
                    .handle_failed_workers(w.into_iter().map(|desc| desc.worker_uri).collect())
                    .await?;
            }
            AuthorityUpdate::AuthorityError(e) => {
                // the authority won't be restarted, so the controller should hard-exit
                internal!("controller's authority thread failed: {}", e);
            }
            _ => {}
        }
        Ok(())
    }

    /// Run the controller wrapper continuously, processing leadership updates and external
    /// requests (if it gets elected).
    /// This function returns if the wrapper fails, or the controller request sender is dropped.
    pub async fn run(mut self) -> ReadySetResult<()> {
        // Start the authority thread responsible for leader election and liveness updates.
        let (authority_tx, mut authority_rx) = tokio::sync::mpsc::channel(16);
        self.authority_task = Some(tokio::spawn(
            crate::controller::authority_runner(
                authority_tx,
                self.authority.clone(),
                self.our_descriptor.clone(),
                self.worker_descriptor.clone(),
                self.config.clone(),
                self.should_reset_state,
            )
            .instrument(tracing::info_span!("authority")),
        ));

        loop {
            // produces a value when the `Valve` is closed
            let mut shutdown_stream = self.valve.wrap(futures_util::stream::pending::<()>());

            select! {
                req = self.handle_rx.recv() => {
                    if let Some(req) = req {
                        self.handle_handle_request(req).await?;
                    }
                    else {
                        info!("Controller shutting down after request handle dropped");
                        break;
                    }
                }
                req = self.http_rx.recv() => {
                    if let Some(req) = req {
                        self.handle_controller_request(req).await?;
                    }
                    else {
                        info!("Controller shutting down after HTTP handle dropped");
                        break;
                    }
                }
                req = authority_rx.recv() => {
                    match req {
                        Some(req) => self.handle_authority_update(req).await?,
                        None if self.inner.is_some() => info!("leadership campaign terminated normally"),
                        // this shouldn't ever happen: if the leadership campaign thread fails,
                        // it should send a `CampaignError` that we handle above first before
                        // ever hitting this bit
                        // still, good to be doubly sure
                        _ => internal!("leadership sender dropped without being elected"),
                    }
                }
                req = self.replication_error_channel.receiver.recv() => {
                    match req {
                        Some(e) => return Err(e),
                        _ => internal!("leader status invalid or channel dropped, leader failed")
                    }

                }
                _ = self.leader_ready_notification.notified() => {
                    self.leader_ready = true;
                }
                _ = shutdown_stream.next() => {
                    info!("Controller shutting down after valve shut");
                    break;
                }
            }
        }

        if let Some(inner) = &mut self.inner {
            inner.stop().await;

            if let Err(error) = self.authority.surrender_leadership().await {
                error!(%error, "failed to surrender leadership");
                internal!("failed to surrender leadership: {}", error)
            }
        }
        Ok(())
    }
}

/// Manages this authority's leader election state and sends update
/// along `event_tx` when the state changes.
struct AuthorityLeaderElectionState {
    event_tx: Sender<AuthorityUpdate>,
    authority: Arc<Authority>,
    descriptor: ControllerDescriptor,
    config: Config,
    /// True if we are eligible to become the leader.
    leader_eligible: bool,
    /// True if we are the current leader.
    is_leader: bool,
    should_reset_state: bool,
}

impl AuthorityLeaderElectionState {
    fn new(
        event_tx: Sender<AuthorityUpdate>,
        authority: Arc<Authority>,
        descriptor: ControllerDescriptor,
        config: Config,
        region: Option<String>,
        should_reset_state: bool,
    ) -> Self {
        // We are eligible to be a leader if we are in the primary region.
        let can_be_leader = if let Some(pr) = &config.primary_region {
            matches!(&region, Some(r) if pr == r)
        } else {
            true
        };

        Self {
            event_tx,
            authority,
            descriptor,
            config,
            leader_eligible: can_be_leader,
            is_leader: false,
            should_reset_state,
        }
    }

    fn is_leader(&self) -> bool {
        self.is_leader
    }

    async fn watch_leader(&self) -> anyhow::Result<()> {
        self.authority.watch_leader().await
    }

    async fn update_leader_state(&mut self) -> anyhow::Result<()> {
        let mut should_attempt_leader_election = false;
        match self.authority.try_get_leader().await? {
            // The leader has changed, inform the worker.
            GetLeaderResult::NewLeader(payload) => {
                self.is_leader = false;
                let authority_update = AuthorityUpdate::LeaderChange(payload);
                self.event_tx
                    .send(authority_update)
                    .await
                    .map_err(|_| format_err!("send failed"))?;
            }

            GetLeaderResult::NoLeader if self.leader_eligible => {
                should_attempt_leader_election = true;
            }
            _ => {}
        }

        if should_attempt_leader_election {
            // If we fail to become the leader restart, go back to checking for a new leader.
            if self
                .authority
                .become_leader(self.descriptor.clone())
                .await?
                .is_none()
            {
                return Ok(());
            }

            // We are the new leader, attempt to update the leader state with our state.
            let state = self
                .authority
                .update_controller_state(
                    |state: Option<ControllerState>| -> Result<ControllerState, ()> {
                        match state {
                            None => Ok(ControllerState {
                                config: self.config.clone(),
                                recipe_version: 0,
                                recipes: vec![],
                                replication_offset: None,
                                node_restrictions: HashMap::new(),
                            }),
                            Some(mut state) => {
                                if self.should_reset_state {
                                    state.reset();
                                }

                                // check that running config is compatible with the new
                                // configuration.
                                if state.config != self.config {
                                    warn!(
                                        authority_config = ?state.config,
                                        our_config = ?self.config,
                                        "Config in authority different than our config, changing to our config"
                                    );
                                }
                                state.config = self.config.clone();
                                Ok(state)
                            }
                        }
                    },
                )
                .await?;
            if state.is_err() {
                return Ok(());
            }

            // Notify our worker that we have won the leader election.
            self.event_tx
                .send(AuthorityUpdate::WonLeaderElection(state.unwrap()))
                .await
                .map_err(|_| format_err!("failed to announce who won leader election"))?;

            self.is_leader = true;
        }

        Ok(())
    }
}

/// Manages this authority's leader worker state and sends update
/// along `event_tx` when the state changes.
struct AuthorityWorkerState {
    event_tx: Sender<AuthorityUpdate>,
    authority: Arc<Authority>,
    descriptor: WorkerDescriptor,
    worker_id: Option<WorkerId>,
    active_workers: HashMap<WorkerId, WorkerDescriptor>,
}

impl AuthorityWorkerState {
    fn new(
        event_tx: Sender<AuthorityUpdate>,
        authority: Arc<Authority>,
        descriptor: WorkerDescriptor,
    ) -> Self {
        Self {
            event_tx,
            authority,
            descriptor,
            worker_id: None,
            active_workers: HashMap::new(),
        }
    }

    async fn register(&mut self) -> anyhow::Result<()> {
        self.worker_id = self
            .authority
            .register_worker(self.descriptor.clone())
            .await?;
        Ok(())
    }

    async fn heartbeat(&self) -> anyhow::Result<AuthorityWorkerHeartbeatResponse> {
        if let Some(id) = &self.worker_id {
            return self.authority.worker_heartbeat(id.clone()).await;
        }

        Ok(AuthorityWorkerHeartbeatResponse::Failed)
    }

    fn clear_active_workers(&mut self) {
        self.active_workers.clear();
    }

    async fn watch_workers(&self) -> anyhow::Result<()> {
        self.authority.watch_workers().await
    }

    async fn update_worker_state(&mut self) -> anyhow::Result<()> {
        // Retrieve the worker ids of current workers.
        let workers = self.authority.get_workers().await?;

        let failed_workers: Vec<_> = self
            .active_workers
            .iter()
            .filter_map(|(w, _)| {
                if !workers.contains(w) {
                    Some(w.clone())
                } else {
                    None
                }
            })
            .collect();

        // Get the descriptors of the failed workers, removing them
        // from the active worker set.
        let failed_descriptors: Vec<_> = failed_workers
            .iter()
            .map(|w| {
                // The key was just pulled from the map above.
                #[allow(clippy::unwrap_used)]
                self.active_workers.remove(w).unwrap()
            })
            .collect();

        if !failed_descriptors.is_empty() {
            self.event_tx
                .send(AuthorityUpdate::FailedWorkers(failed_descriptors))
                .await
                .map_err(|_| format_err!("failed to announce who won leader election"))?;
        }

        let new_workers = workers
            .into_iter()
            .filter(|w| !self.active_workers.contains_key(w))
            .collect();

        // Get the descriptors of the new workers, adding them to the
        // active workers set.
        let new_descriptor_map = self.authority.worker_data(new_workers).await?;
        let new_descriptors: Vec<WorkerDescriptor> = new_descriptor_map.values().cloned().collect();
        self.active_workers.extend(new_descriptor_map);

        if !new_descriptors.is_empty() {
            self.event_tx
                .send(AuthorityUpdate::NewWorkers(new_descriptors))
                .await
                .map_err(|_| format_err!("failed to announce who won leader election"))?;
        }

        Ok(())
    }
}

async fn authority_inner(
    event_tx: Sender<AuthorityUpdate>,
    authority: Arc<Authority>,
    descriptor: ControllerDescriptor,
    worker_descriptor: WorkerDescriptor,
    config: Config,
    should_reset_state: bool,
) -> anyhow::Result<()> {
    authority.init().await?;

    let mut leader_election_state = AuthorityLeaderElectionState::new(
        event_tx.clone(),
        authority.clone(),
        descriptor,
        config,
        worker_descriptor.region.clone(),
        should_reset_state,
    );

    let mut worker_state =
        AuthorityWorkerState::new(event_tx, authority.clone(), worker_descriptor);

    // Register the current server as a worker in the system.
    worker_state
        .register()
        .await
        .context("Registering worker")?;

    let mut last_leader_state = false;
    loop {
        leader_election_state
            .update_leader_state()
            .await
            .context("Updating leader state")?;
        // TODO(justin): Handle detected as failed.
        worker_state.heartbeat().await?;

        let is_leader = leader_election_state.is_leader();
        let became_leader = !last_leader_state && is_leader;
        last_leader_state = is_leader;

        // Reset the set of workers in the worker state, as we want to propagate
        // all the workers to a new leader. Technically, since we only update
        // worker state when we are the leader, the active workers list will
        // be empty. However, this guards against an insidious bug in a future
        // where a node can become the leader, lose leadership, and then regain
        // it.
        if became_leader {
            worker_state.clear_active_workers();
        }

        if is_leader {
            worker_state
                .update_worker_state()
                .await
                .context("Updating worker state")?;
        }

        if authority.can_watch() {
            select! {
                watch_result = leader_election_state.watch_leader() => {
                    if let Err(e) = watch_result {
                        tracing::warn!(error = %e, "failure creating worker watch");
                    }
                },
                watch_result = worker_state.watch_workers() => {
                    if let Err(e) = watch_result {
                        tracing::warn!(error = %e, "failure creating worker watch");
                    }
                },
                () = tokio::time::sleep(WATCH_DURATION) => {}
            };
        } else {
            tokio::time::sleep(LEADER_STATE_CHECK_INTERVAL).await;
        }
    }
}

pub(crate) async fn authority_runner(
    event_tx: Sender<AuthorityUpdate>,
    authority: Arc<Authority>,
    descriptor: ControllerDescriptor,
    worker_descriptor: WorkerDescriptor,
    config: Config,
    should_reset_state: bool,
) -> anyhow::Result<()> {
    if let Err(e) = authority_inner(
        event_tx.clone(),
        authority,
        descriptor,
        worker_descriptor,
        config,
        should_reset_state,
    )
    .await
    {
        let _ = event_tx.send(AuthorityUpdate::AuthorityError(e)).await;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::integration_utils::{sleep, start_simple};
    use std::error::Error;

    #[tokio::test(flavor = "multi_thread")]
    async fn extend_recipe_parse_failure() {
        let mut noria = start_simple("extend_recipe_parse_failure").await;
        let res = noria.extend_recipe("Invalid SQL").await;
        assert!(res.is_err());
        let err = res.err().unwrap();
        let source: &ReadySetError = err
            .source()
            .and_then(|e| e.downcast_ref::<Box<ReadySetError>>())
            .unwrap()
            .as_ref();
        assert!(
            matches!(*source, ReadySetError::UnparseableQuery { .. }),
            "source = {:?}",
            source
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn remove_query() {
        let mut noria = start_simple("remove_query").await;
        noria
            .extend_recipe(
                "CREATE TABLE users (id INT PRIMARY KEY, name TEXT);
                QUERY test_query: SELECT * FROM users;",
            )
            .await
            .unwrap();

        let queries = noria.outputs().await.unwrap();
        assert!(queries.contains_key("test_query"));

        noria.remove_query("test_query").await.unwrap();

        let queries = noria.outputs().await.unwrap();
        assert!(!queries.contains_key("test_query"));
    }

    mod replication_offsets {
        use std::convert::TryInto;

        use super::*;
        use noria::ReplicationOffset;
        use replicators::BinlogPosition;

        #[tokio::test(flavor = "multi_thread")]
        async fn same_log() {
            let mut noria = start_simple("replication_offsets").await;
            noria
                .extend_recipe(
                    "CREATE TABLE t1 (id int);
                     CREATE TABLE t2 (id int);",
                )
                .await
                .unwrap();
            let mut t1 = noria.table("t1").await.unwrap();
            let mut t2 = noria.table("t2").await.unwrap();

            let mut offset = ReplicationOffset {
                offset: 1,
                replication_log_name: "binlog".to_owned(),
            };

            noria
                .set_replication_offset(Some(offset.clone()))
                .await
                .unwrap();

            t1.set_replication_offset(offset.clone()).await.unwrap();
            offset.offset = 7;
            t2.set_replication_offset(offset.clone()).await.unwrap();

            sleep().await;

            assert_eq!(offset, noria.replication_offset().await.unwrap().unwrap());

            // Check that storing replication offset via the adapter directly works too
            offset.offset = 15;
            noria
                .set_replication_offset(Some(offset.clone()))
                .await
                .unwrap();

            sleep().await;
            assert_eq!(offset, noria.replication_offset().await.unwrap().unwrap());
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn same_log_with_pkey() {
            let mut noria = start_simple("replication_offsets").await;

            noria
                .set_replication_offset(Some(ReplicationOffset {
                    offset: 2,
                    replication_log_name: "binlog".to_owned(),
                }))
                .await
                .unwrap();

            noria
                .extend_recipe("CREATE TABLE t1 (id int primary key);")
                .await
                .unwrap();
            let mut t1 = noria.table("t1").await.unwrap();

            t1.perform_all(vec![
                noria::TableOperation::Insert(vec![noria::DataType::BigInt(10)]),
                noria::TableOperation::SetReplicationOffset(ReplicationOffset {
                    offset: 1,
                    replication_log_name: "binlog".to_owned(),
                }),
                noria::TableOperation::Insert(vec![noria::DataType::BigInt(20)]),
                noria::TableOperation::SetReplicationOffset(ReplicationOffset {
                    offset: 6,
                    replication_log_name: "binlog".to_owned(),
                }),
            ])
            .await
            .unwrap();

            sleep().await;

            let offset = noria.replication_offset().await.unwrap();
            assert_eq!(
                offset,
                Some(ReplicationOffset {
                    offset: 6,
                    replication_log_name: "binlog".to_owned(),
                })
            );
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn schema_offset() {
            let mut noria = start_simple("replication_offsets").await;
            let mut binlog = BinlogPosition {
                binlog_file: "mysql-binlog.000006".to_string(),
                position: 100,
            };

            noria
                .extend_recipe_with_offset(
                    "CREATE TABLE t1 (id int);
                     CREATE TABLE t2 (id int);",
                    Some((&binlog).try_into().unwrap()),
                )
                .await
                .unwrap();

            let mut t1 = noria.table("t1").await.unwrap();
            let mut t2 = noria.table("t2").await.unwrap();

            binlog.position = 200;

            t1.set_replication_offset((&binlog).try_into().unwrap())
                .await
                .unwrap();

            binlog.position = 300;

            t2.set_replication_offset((&binlog).try_into().unwrap())
                .await
                .unwrap();

            sleep().await;

            let offset: BinlogPosition = noria.replication_offset().await.unwrap().unwrap().into();
            assert_eq!(offset, binlog);

            binlog.position = 400;

            noria
                .extend_recipe_with_offset(
                    "CREATE VIEW v AS SELECT * FROM t1 WHERE id = 5;",
                    Some((&binlog).try_into().unwrap()),
                )
                .await
                .unwrap();

            let offset: BinlogPosition = noria.replication_offset().await.unwrap().unwrap().into();
            assert_eq!(offset, binlog);
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn different_log() {
            let mut noria = start_simple("replication_offsets").await;
            noria
                .set_replication_offset(Some(ReplicationOffset {
                    offset: 2,
                    replication_log_name: "binlog".to_owned(),
                }))
                .await
                .unwrap();

            noria
                .extend_recipe(
                    "CREATE TABLE t1 (id int);
                     CREATE TABLE t2 (id int);",
                )
                .await
                .unwrap();
            let mut t1 = noria.table("t1").await.unwrap();
            let mut t2 = noria.table("t2").await.unwrap();

            t1.set_replication_offset(ReplicationOffset {
                offset: 1,
                replication_log_name: "binlog".to_owned(),
            })
            .await
            .unwrap();
            t2.set_replication_offset(ReplicationOffset {
                offset: 7,
                replication_log_name: "linbog".to_owned(),
            })
            .await
            .unwrap();

            sleep().await;

            let offset = noria.replication_offset().await;
            assert!(offset.is_err());
            let err = offset.err().unwrap();
            assert!(
                matches!(
                    err,
                    ReadySetError::RpcFailed {
                        source: box ReadySetError::ReplicationOffsetLogDifferent(_, _),
                        ..
                    }
                ),
                "err = {:?}",
                err
            );
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn missing_in_one_table() {
            let mut noria = start_simple("missing_in_one_table").await;
            noria
                .set_replication_offset(Some(ReplicationOffset {
                    offset: 1,
                    replication_log_name: "binlog".to_owned(),
                }))
                .await
                .unwrap();
            noria
                .extend_recipe(
                    "CREATE TABLE t1 (id int);
                     CREATE TABLE t2 (id int);",
                )
                .await
                .unwrap();
            noria
                .table("t1")
                .await
                .unwrap()
                .set_replication_offset(ReplicationOffset {
                    offset: 1,
                    replication_log_name: "binlog".to_owned(),
                })
                .await
                .unwrap();

            let offset = noria.replication_offset().await.unwrap();
            assert!(offset.is_none());
        }
    }
}
