use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{format_err, Context};
use dataflow::node::{self, Column};
use dataflow::prelude::ChannelCoordinator;
use failpoint_macros::set_failpoint;
use futures_util::StreamExt;
use hyper::http::{Method, StatusCode};
use launchpad::select;
use metrics::{counter, gauge, histogram};
use nom_sql::SqlIdentifier;
use noria::consensus::{
    Authority, AuthorityControl, AuthorityWorkerHeartbeatResponse, GetLeaderResult,
    WorkerDescriptor, WorkerId, WorkerSchedulingConfig,
};
use noria::metrics::recorded;
use noria::ControllerDescriptor;
use noria_errors::{internal, internal_err, ReadySetError};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use stream_cancel::Valve;
use tokio::sync::mpsc::{Receiver, Sender, UnboundedReceiver, UnboundedSender};
use tokio::sync::{Notify, RwLock, RwLockReadGuard, RwLockWriteGuard};
use tracing::{error, info, warn};
use tracing_futures::Instrument;
use url::Url;

use crate::controller::inner::{ControllerRequestType, Leader};
use crate::controller::migrate::Migration;
use crate::controller::recipe::Recipe;
use crate::controller::state::DataflowState;
use crate::materialization::Materializations;
use crate::worker::{WorkerRequest, WorkerRequestKind};
use crate::{Config, ReadySetResult, VolumeId};

mod domain_handle;
mod inner;
mod keys;
pub(crate) mod migrate; // crate viz for tests
mod mir_to_flow;
pub(crate) mod recipe; // crate viz for tests
pub(crate) mod replication;
pub(crate) mod schema;
pub(crate) mod sql;
mod state;

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
    node_name: SqlIdentifier,
    shard: usize,
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct ControllerState {
    pub(crate) config: Config,
    pub(crate) dataflow_state: DataflowState,
}

// We implement [`Debug`] manually so that we can skip the [`DataflowState`] field.
// In the future, we might want to implement [`Debug`] for [`DataflowState`] as well and just derive
// it from [`Debug`] for [`ControllerState`].
impl Debug for ControllerState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ControllerState")
            .field("config", &self.config)
            .field(
                "schema_replication_offset",
                &self.dataflow_state.schema_replication_offset(),
            )
            .field("node_restrictions", &self.dataflow_state.node_restrictions)
            .finish()
    }
}

#[derive(Clone)]
pub struct Worker {
    healthy: bool,
    uri: Url,
    http: reqwest::Client,
    /// Configuration for how domains should be scheduled onto this worker
    domain_scheduling_config: WorkerSchedulingConfig,
    request_timeout: Duration,
}

impl Worker {
    pub fn new(
        instance_uri: Url,
        domain_scheduling_config: WorkerSchedulingConfig,
        request_timeout: Duration,
    ) -> Self {
        Worker {
            healthy: true,
            uri: instance_uri,
            http: reqwest::Client::new(),
            domain_scheduling_config,
            request_timeout,
        }
    }
    pub async fn rpc<T: DeserializeOwned>(&self, req: WorkerRequestKind) -> ReadySetResult<T> {
        let body = hyper::Body::from(bincode::serialize(&req)?);
        let req = self.http.post(self.uri.join("worker_request")?).body(body);
        let resp = req
            .timeout(self.request_timeout)
            .send()
            .await
            .map_err(|e| ReadySetError::HttpRequestFailed(e.to_string()))?;
        let status = resp.status();
        let body = resp
            .bytes()
            .await
            .map_err(|e| ReadySetError::HttpRequestFailed(e.to_string()))?;
        if !status.is_success() {
            if status == reqwest::StatusCode::SERVICE_UNAVAILABLE {
                return Err(ReadySetError::ServiceUnavailable);
            } else if status == reqwest::StatusCode::BAD_REQUEST {
                return Err(ReadySetError::SerializationFailed(
                    "remote server returned 400".into(),
                ));
            } else {
                let err: ReadySetError = bincode::deserialize(&body)?;
                return Err(err);
            }
        }
        Ok(bincode::deserialize::<T>(&body)?)
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
            dyn FnOnce(&mut crate::controller::migrate::Migration<'_>) -> ReadySetResult<()>
                + Send
                + 'static,
        >,
        /// The result of the migration gets sent down here.
        done_tx: tokio::sync::oneshot::Sender<ReadySetResult<()>>,
    },
}

/// A structure to hold and manage access to the [`Leader`].
/// The main purpose of this structure is to implement the interior mutability pattern,
/// allowing for multiple threads to have an [`Arc`] reference to it, and to read/write/replace
/// the inner [`Leader`] without having to worry about synchronization.
struct LeaderHandle {
    leader: RwLock<Option<Leader>>,
}

impl LeaderHandle {
    /// Creates a new, empty [`LeaderHandle`].
    fn new() -> Self {
        LeaderHandle {
            leader: RwLock::new(None),
        }
    }

    /// Replaces the current [`Leader`] with a new one.
    /// This method will block if there's a thread currently waiting to acquire or holding
    /// the [`Leader`] write lock.
    async fn replace(&self, leader: Leader) {
        let mut guard = self.leader.write().await;
        *guard = Some(leader);
    }

    /// Acquires a read lock on the [`Leader`].
    /// This method will block if there's a thread currently waiting to acquire or holding
    /// the [`Leader`] write lock.
    async fn read(&self) -> RwLockReadGuard<'_, Option<Leader>> {
        self.leader.read().await
    }

    /// Acquires a write lock on the [`Leader`].
    /// This method will block if there's a thread currently waiting to acquire or holding
    /// the [`Leader`] write lock.
    async fn write(&self) -> RwLockWriteGuard<'_, Option<Leader>> {
        self.leader.write().await
    }
}

/// A wrapper for the control plane of the server instance that handles: leader election,
/// control rpcs, and requests originated from this server's instance's `Handle`.
pub struct Controller {
    /// If we are the leader, the leader object to use for performing leader operations.
    inner: Arc<LeaderHandle>,
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
    /// A handle to the write processing task.
    write_processing_task: Option<tokio::task::JoinHandle<anyhow::Result<()>>>,
    /// A handle to the dry run processing task.
    dry_run_task: Option<tokio::task::JoinHandle<anyhow::Result<()>>>,
    /// The config associated with this controller's server.
    config: Config,
    /// Whether we are the leader and ready to handle requests.
    leader_ready: Arc<AtomicBool>,
    /// A notify to be passed to leader's when created, used to notify the Controller that the
    /// leader is ready to handle requests.
    leader_ready_notification: Arc<Notify>,

    /// Channel that the replication task, if it exists, can use to propagate updates back to
    /// the parent controller.
    replication_error_channel: ReplicationErrorChannel,
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
    ) -> Self {
        Self {
            inner: Arc::new(LeaderHandle::new()),
            authority,
            worker_tx,
            http_rx: controller_rx,
            handle_rx,
            our_descriptor,
            valve: shutoff_valve,
            worker_descriptor,
            config,
            leader_ready: Arc::new(AtomicBool::new(false)),
            leader_ready_notification: Arc::new(Notify::new()),
            replication_error_channel: ReplicationErrorChannel::new(),
            authority_task: None,
            write_processing_task: None,
            dry_run_task: None,
        }
    }

    /// Send the provided `WorkerRequestKind` to the worker running in the same server instance as
    /// this controller wrapper, but don't bother waiting for the response.
    ///
    /// Not waiting for the response avoids deadlocking when the controller and worker are in the
    /// same noria-server instance.
    async fn send_worker_request(&self, kind: WorkerRequestKind) -> ReadySetResult<()> {
        let (tx, _rx) = tokio::sync::oneshot::channel();
        self.worker_tx
            .send(WorkerRequest { kind, done_tx: tx })
            .await
            .map_err(|e| internal_err(format!("failed to send to instance worker: {}", e)))?;
        Ok(())
    }

    async fn handle_handle_request(&self, req: HandleRequest) -> ReadySetResult<()> {
        match req {
            HandleRequest::QueryReadiness(tx) => {
                let guard = self.inner.read().await;
                let leader_ready = self.leader_ready.load(Ordering::Acquire);
                let done = leader_ready
                    && match guard.as_ref() {
                        Some(leader) => {
                            let ds = leader.dataflow_state_handle.read().await;
                            !ds.workers.is_empty()
                        }
                        None => false,
                    };
                if tx.send(done).is_err() {
                    warn!("readiness query sender hung up!");
                }
            }
            HandleRequest::PerformMigration { func, done_tx } => {
                let mut guard = self.inner.write().await;
                if let Some(ref mut inner) = *guard {
                    let mut writer = inner.dataflow_state_handle.write().await;
                    let ds = writer.as_mut();
                    let ret = ds.migrate(false, move |m| func(m)).await?;
                    inner
                        .dataflow_state_handle
                        .commit(writer, &self.authority)
                        .await?;
                    if done_tx.send(ret).is_err() {
                        warn!("handle-based migration sender hung up!");
                    }
                } else {
                    return Err(ReadySetError::NotLeader);
                }
            }
        }
        Ok(())
    }

    async fn handle_authority_update(&self, msg: AuthorityUpdate) -> ReadySetResult<()> {
        match msg {
            AuthorityUpdate::LeaderChange(descr) => {
                gauge!(recorded::CONTROLLER_IS_LEADER, 0f64);
                self.send_worker_request(WorkerRequestKind::NewController {
                    controller_uri: descr.controller_uri,
                })
                .await?;
            }
            AuthorityUpdate::WonLeaderElection(state) => {
                info!("won leader election, creating Leader");
                gauge!(recorded::CONTROLLER_IS_LEADER, 1f64);
                let mut leader = Leader::new(
                    state,
                    self.our_descriptor.controller_uri.clone(),
                    self.authority.clone(),
                    self.config.replicator_config.clone(),
                    self.config.worker_request_timeout,
                );
                self.leader_ready.store(false, Ordering::Release);

                leader
                    .start(
                        self.leader_ready_notification.clone(),
                        self.replication_error_channel.sender(),
                    )
                    .await;

                self.inner.replace(leader).await;
                self.send_worker_request(WorkerRequestKind::NewController {
                    controller_uri: self.our_descriptor.controller_uri.clone(),
                })
                .await?;
            }
            AuthorityUpdate::NewWorkers(w) => {
                let mut guard = self.inner.write().await;
                if let Some(ref mut inner) = *guard {
                    inner.handle_register_from_authority(w).await?;
                } else {
                    return Err(ReadySetError::NotLeader);
                }
            }
            AuthorityUpdate::FailedWorkers(w) => {
                let mut guard = self.inner.write().await;
                if let Some(ref mut inner) = *guard {
                    inner
                        .handle_failed_workers(w.into_iter().map(|desc| desc.worker_uri).collect())
                        .await?;
                } else {
                    return Err(ReadySetError::NotLeader);
                }
            }
            AuthorityUpdate::AuthorityError(e) => {
                // the authority won't be restarted, so the controller should hard-exit
                internal!("controller's authority thread failed: {:#}", e);
            }
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
            )
            .instrument(tracing::info_span!("authority")),
        ));

        let (writer_tx, writer_rx) = tokio::sync::mpsc::channel(16);
        self.write_processing_task = Some(tokio::spawn(
            crate::controller::controller_req_processing_runner(
                writer_rx,
                self.authority.clone(),
                self.inner.clone(),
                self.valve.clone(),
                self.leader_ready.clone(),
            )
            .instrument(tracing::info_span!("write_processing")),
        ));
        let (dry_run_tx, dry_run_rx) = tokio::sync::mpsc::channel(16);
        self.dry_run_task = Some(tokio::spawn(
            crate::controller::controller_req_processing_runner(
                dry_run_rx,
                self.authority.clone(),
                self.inner.clone(),
                self.valve.clone(),
                self.leader_ready.clone(),
            )
            .instrument(tracing::info_span!("dry_run_processing")),
        ));

        let leader_ready = self.leader_ready.clone();
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
                        // Check if the request is a write request, dry run request, or read
                        // request.
                        // If it's a read request, then we can handle the request on this thread,
                        // since it will just read the current dataflow state.
                        // If it is a write request, we pass the request to the write processing
                        // task, which will also handle the request in the same way, but on a
                        // different thread. This is how we avoid blocking reads.
                        // Likewise if the request is a dry run request we handle the request on a
                        // dedicated dry run thread. This is to avoid blocking migrations
                        match crate::controller::inner::request_type(&req) {
                            ControllerRequestType::Read => {
                                let leader_ready = leader_ready.load(Ordering::Acquire);
                                crate::controller::handle_controller_request(
                                    req,
                                    self.authority.clone(),
                                    self.inner.clone(),
                                    leader_ready
                                ).await?;
                            }
                            ControllerRequestType::Write => {

                                if writer_tx.send(req).await.is_err() {
                                    internal!("write processing handle hung up!")
                                }
                            }
                            ControllerRequestType::DryRun => {

                                if dry_run_tx.send(req).await.is_err() {
                                    internal!("dry run processing handle hung up!")
                                }
                            }
                        }
                    }
                    else {
                        info!("Controller shutting down after HTTP handle dropped");
                        break;
                    }
                }
                req = authority_rx.recv() => {
                    match req {
                        Some(req) => self.handle_authority_update(req).await?,
                        None => {
                            // this shouldn't ever happen: if the leadership campaign thread fails,
                            // it should send a `CampaignError` in `handle_authority_update`.
                            internal!("leadership thread has unexpectedly failed.")
                        }
                    }
                }
                req = self.replication_error_channel.receiver.recv() => {
                    match req {
                        Some(e) => return Err(e),
                        _ => internal!("leader status invalid or channel dropped, leader failed")
                    }

                }
                _ = self.leader_ready_notification.notified() => {
                    self.leader_ready.store(true, Ordering::Release);
                }
                _ = shutdown_stream.next() => {
                    info!("Controller shutting down after valve shut");
                    break;
                }
            }
        }

        let mut guard = self.inner.write().await;
        if let Some(ref mut inner) = *guard {
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
}

impl AuthorityLeaderElectionState {
    fn new(
        event_tx: Sender<AuthorityUpdate>,
        authority: Arc<Authority>,
        descriptor: ControllerDescriptor,
        config: Config,
        leader_eligible: bool,
    ) -> Self {
        Self {
            event_tx,
            authority,
            descriptor,
            config,
            leader_eligible,
            is_leader: false,
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
                            None => {
                                let mut g = petgraph::Graph::new();
                                // Create the root node in the graph.
                                let source = g.add_node(node::Node::new::<_, _, Vec<Column>, _>(
                                    "source",
                                    Vec::new(),
                                    node::special::Source,
                                ));

                                let mut materializations = Materializations::new();
                                materializations.set_config(self.config.materialization_config.clone());

                                let cc = Arc::new(ChannelCoordinator::new());
                                assert_ne!(self.config.quorum, 0);

                                let recipe = Recipe::with_config(
                                    crate::sql::Config {
                                        reuse_type: self.config.reuse,
                                        ..Default::default()
                                    },
                                    self.config.mir_config.clone(),
                                );

                                let dataflow_state = DataflowState::new(
                                    g,
                                    source,
                                    0,
                                    self.config.sharding,
                                    self.config.domain_config.clone(),
                                    self.config.persistence.clone(),
                                    materializations,
                                    recipe,
                                    None,
                                    HashMap::new(),
                                    cc,
                                    self.config.keep_prior_recipes,
                                    self.config.replication_strategy,
                                );
                                Ok(ControllerState {
                                    config: self.config.clone(),
                                    dataflow_state,
                                })
                            },
                            Some(mut state) => {
                                // check that running config is compatible with the new
                                // configuration.
                                if state.config != self.config {
                                    warn!(
                                    authority_config = ?state.config,
                                    our_config = ?self.config,
                                    "Config in authority different than our config, changing to our config"
                                );
                                }
                                state.dataflow_state.domain_config = self.config.domain_config.clone();
                                state.dataflow_state.replication_strategy = self.config.replication_strategy;
                                state.config = self.config.clone();
                                Ok(state)
                            }
                        }
                    },
                    |state: &mut ControllerState| {
                        state.dataflow_state.touch_up();
                    }
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
) -> anyhow::Result<()> {
    authority.init().await?;

    let mut leader_election_state = AuthorityLeaderElectionState::new(
        event_tx.clone(),
        authority.clone(),
        descriptor,
        config,
        worker_descriptor.leader_eligible,
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
        set_failpoint!("authority");
        leader_election_state
            .update_leader_state()
            .await
            .context("Updating leader state")?;
        if let AuthorityWorkerHeartbeatResponse::Failed = worker_state.heartbeat().await? {
            anyhow::bail!("This node is considered failed by consul");
        }

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
) -> anyhow::Result<()> {
    if let Err(e) = authority_inner(
        event_tx.clone(),
        authority,
        descriptor,
        worker_descriptor,
        config,
    )
    .await
    {
        let _ = event_tx.send(AuthorityUpdate::AuthorityError(e)).await;
        anyhow::bail!("Authority runner failed");
    }
    Ok(())
}

/// Designed to be spun up in a task that handles [`ControllerRequest`]s of various types.
async fn controller_req_processing_runner(
    mut request_rx: Receiver<ControllerRequest>,
    authority: Arc<Authority>,
    leader_handle: Arc<LeaderHandle>,
    shutdown_stream: Valve,
    leader_ready: Arc<AtomicBool>,
) -> anyhow::Result<()> {
    loop {
        let mut shutdown_stream = shutdown_stream.wrap(futures_util::stream::pending::<()>());
        select! {
            request = request_rx.recv() => {
                if let Some(req) = request {
                    let leader_ready = leader_ready.load(Ordering::Acquire);
                    crate::controller::handle_controller_request(
                        req,
                        authority.clone(),
                        leader_handle.clone(),
                        leader_ready
                    ).await?;
                } else {
                    info!("Controller shutting down after write processing handle dropped");
                    break;
                }
            },
            _ = shutdown_stream.next() => {
                info!("Write processing task shutting down after valve shut");
                break;
            }
        }
    }
    Ok(())
}

async fn handle_controller_request(
    req: ControllerRequest,
    authority: Arc<Authority>,
    leader_handle: Arc<LeaderHandle>,
    leader_ready: bool,
) -> ReadySetResult<()> {
    let ControllerRequest {
        method,
        path,
        query,
        body,
        reply_tx,
    } = req;

    let request_start = Instant::now();
    let ret: Result<Result<Vec<u8>, Vec<u8>>, StatusCode> = {
        let guard = leader_handle.read().await;
        let resp = match (&method, path.as_str()) {
            // Requests that do not need to be handled by the leader.
            #[cfg(feature = "failure_injection")]
            (&Method::GET, "/failpoint") => {
                let (name, action): (String, String) = bincode::deserialize(&body)?;
                Ok(Ok(::bincode::serialize(&fail::cfg(name, &action))?))
            }
            _ => {
                if let Some(ref ci) = *guard {
                    Ok(tokio::task::block_in_place(|| {
                        ci.external_request(
                            method,
                            path.as_ref(),
                            query,
                            body,
                            &authority,
                            leader_ready,
                        )
                    }))
                } else {
                    Err(ReadySetError::NotLeader)
                }
            }
        };

        match resp {
            // returned from `Leader::external_request`:
            Ok(Ok(r)) => Ok(Ok(r)),
            Ok(Err(ReadySetError::NoQuorum)) => Err(StatusCode::SERVICE_UNAVAILABLE),
            Ok(Err(ReadySetError::UnknownEndpoint)) => Err(StatusCode::NOT_FOUND),
            Ok(Err(e)) => Ok(Err(bincode::serialize(&e)?)),
            // something else failed:
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

#[cfg(test)]
mod tests {
    use noria::replication::ReplicationOffset;
    use noria::KeyCount;

    use crate::integration_utils::start_simple;

    #[tokio::test(flavor = "multi_thread")]
    async fn remove_query() {
        let mut noria = start_simple("remove_query").await;
        noria
            .extend_recipe(
                "CREATE TABLE users (id INT PRIMARY KEY, name TEXT);
                 CREATE CACHE test_query FROM SELECT * FROM users;"
                    .parse()
                    .unwrap(),
            )
            .await
            .unwrap();

        let queries = noria.outputs().await.unwrap();
        assert!(queries.contains_key("test_query"));

        noria.remove_query("test_query").await.unwrap();

        let queries = noria.outputs().await.unwrap();
        assert!(!queries.contains_key("test_query"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn remove_all_queries() {
        let mut noria = start_simple("remove_all_queries").await;
        noria
            .extend_recipe(
                "CREATE TABLE users (id INT PRIMARY KEY, name TEXT);
                 CREATE CACHE q1 FROM SELECT id FROM users;
                 CREATE CACHE q2 FROM SELECT name FROM users where id = ?;"
                    .parse()
                    .unwrap(),
            )
            .await
            .unwrap();

        let queries = noria.outputs().await.unwrap();
        assert!(queries.contains_key("q1"));
        assert!(queries.contains_key("q2"));

        noria.remove_all_queries().await.unwrap();

        let queries = noria.outputs().await.unwrap();
        assert!(queries.is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn replication_offsets() {
        let mut noria = start_simple("all_tables").await;

        let offset = ReplicationOffset {
            offset: 1,
            replication_log_name: "binlog".to_owned(),
        };

        noria
            .set_schema_replication_offset(Some(&offset))
            .await
            .unwrap();
        noria
            .extend_recipe(
                "CREATE TABLE t1 (id int);
                     CREATE TABLE t2 (id int);
                     CREATE TABLE t3 (id int);"
                    .parse()
                    .unwrap(),
            )
            .await
            .unwrap();

        let mut t1 = noria.table("t1").await.unwrap();
        let mut t2 = noria.table("t2").await.unwrap();

        t1.set_replication_offset(ReplicationOffset {
            offset: 2,
            ..offset.clone()
        })
        .await
        .unwrap();

        t2.set_replication_offset(ReplicationOffset {
            offset: 3,
            ..offset.clone()
        })
        .await
        .unwrap();

        let offsets = noria.replication_offsets().await.unwrap();

        assert_eq!(offsets.schema.unwrap().offset, 1);
        assert_eq!(offsets.tables["t1"].as_ref().unwrap().offset, 2);
        assert_eq!(offsets.tables["t2"].as_ref().unwrap().offset, 3);
        assert_eq!(offsets.tables["t3"], None);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn key_count_rpc() {
        let mut noria = start_simple("all_tables").await;

        noria
            .extend_recipe(
                "CREATE TABLE key_count_test (id INT PRIMARY KEY, stuff TEXT);
                 CREATE CACHE q1 FROM SELECT * FROM key_count_test;"
                    .parse()
                    .unwrap(),
            )
            .await
            .unwrap();

        let key_counts = noria.node_key_counts().await.unwrap();

        let mut table = noria.table("key_count_test").await.unwrap();
        // The table only contains the local index, so we use `inputs()` to get the global index
        let table_idx = noria.inputs().await.unwrap()["key_count_test"];
        let view_idx = noria.view("q1").await.unwrap().node().clone();

        assert_eq!(key_counts[&table_idx], KeyCount::EstimatedRowCount(0));
        assert_eq!(key_counts[&view_idx], KeyCount::ExactKeyCount(0));

        table.insert(vec![1.into(), "abc".into()]).await.unwrap();

        let key_counts = noria.node_key_counts().await.unwrap();

        assert_eq!(key_counts[&table_idx], KeyCount::EstimatedRowCount(1));
        assert_eq!(key_counts[&view_idx], KeyCount::ExactKeyCount(1));
    }
}
