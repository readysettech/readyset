use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{format_err, Context};
use dataflow::node::{self, Column};
use dataflow::prelude::ChannelCoordinator;
use failpoint_macros::set_failpoint;
use futures::future::Either;
use hyper::http::{Method, StatusCode};
use metrics::{counter, gauge, histogram};
use nom_sql::Relation;
use readyset_client::consensus::{
    Authority, AuthorityControl, AuthorityWorkerHeartbeatResponse, GetLeaderResult,
    WorkerDescriptor, WorkerId, WorkerSchedulingConfig,
};
use readyset_client::debug::stats::PersistentStats;
#[cfg(feature = "failure_injection")]
use readyset_client::failpoints;
use readyset_client::metrics::recorded;
use readyset_client::ControllerDescriptor;
use readyset_data::Dialect;
use readyset_errors::{internal, internal_err, ReadySetError, ReadySetResult};
use readyset_telemetry_reporter::TelemetrySender;
use readyset_util::select;
use readyset_util::shutdown::ShutdownReceiver;
use replicators::ReplicatorMessage;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::{self, Receiver, Sender, UnboundedReceiver, UnboundedSender};
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use tracing::{error, info, info_span, warn};
use tracing_futures::Instrument;
use url::Url;

use crate::controller::inner::Leader;
use crate::controller::migrate::Migration;
use crate::controller::sql::Recipe;
use crate::controller::state::DfState;
use crate::materialization::Materializations;
use crate::worker::{WorkerRequest, WorkerRequestKind, WorkerRequestType};
use crate::{Config, VolumeId};

mod domain_handle;
mod inner;
mod keys;
pub(crate) mod migrate; // crate viz for tests
mod mir_to_flow;
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

/// The key for a DomainPlacementRestriction for a dataflow node.
/// Each dataflow node, shard pair may have a DomainPlacementRestriction.
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash, Debug)]
pub struct NodeRestrictionKey {
    node_name: Relation,
    shard: usize,
}

/// The full (metadata) state of a running ReadySet cluster.
///
/// This struct is the root data structure that is serialized atomically and written to the
/// [`Authority`] upon changes to the state of the cluster. It includes:
/// - All configuration for the cluster
/// - The full schema of the database (both `CREATE TABLE` statements taken from the upstream
///   database and ReadySet-specific configuration including `CREATE CACHE` statements)
/// - The full state of the graph, including both [`MIR`][] and the dataflow graph itself
///
/// [`MIR`]: readyset_mir
#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct ControllerState {
    /// The user-provided configuration for the cluster
    pub(crate) config: Config,
    /// The full state of the dataflow engine
    pub(crate) dataflow_state: DfState,
}

// We implement [`Debug`] manually so that we can skip the [`DfState`] field.
// In the future, we might want to implement [`Debug`] for [`DfState`] as well and just derive
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

impl ControllerState {
    /// Initialize a new, empty [`ControllerState`] with the given configuration, and with the given
    /// value for the `permissive_writes` setting
    fn new(config: Config, permissive_writes: bool) -> Self {
        let mut g = petgraph::Graph::new();
        // Create the root node in the graph.
        let source = g.add_node(node::Node::new::<_, _, Vec<Column>, _>(
            "source",
            Vec::new(),
            node::special::Source,
        ));

        let mut materializations = Materializations::new();
        materializations.set_config(config.materialization_config.clone());

        let cc = Arc::new(ChannelCoordinator::new());
        assert_ne!(config.min_workers, 0);

        let recipe = Recipe::with_config(
            crate::sql::Config {
                reuse_type: config.reuse,
                ..Default::default()
            },
            config.mir_config.clone(),
            permissive_writes,
        );

        let dataflow_state = DfState::new(
            g,
            source,
            0,
            config.sharding,
            config.domain_config.clone(),
            config.persistence.clone(),
            materializations,
            recipe,
            None,
            HashMap::new(),
            cc,
            config.replication_strategy,
        );

        Self {
            config,
            dataflow_state,
        }
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
        let http_req = self.http.post(self.uri.join("worker_request")?).body(body);
        let resp = http_req
            .timeout(self.request_timeout)
            .send()
            .await
            .map_err(|e| ReadySetError::HttpRequestFailed {
                request: format!("{:?}", WorkerRequestType::from(&req)),
                message: e.to_string(),
            })?;
        let status = resp.status();
        let body = resp
            .bytes()
            .await
            .map_err(|e| ReadySetError::HttpRequestFailed {
                request: format!("{:?}", WorkerRequestType::from(&req)),
                message: e.to_string(),
            })?;
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

/// Channel used to notify the controller of replicator events. This channel conveys information on
/// replicator status and allows us to gracefully kill the controller loop in the event of an
/// unrecoverable replicator error.
pub struct ReplicatorChannel {
    sender: UnboundedSender<ReplicatorMessage>,
    receiver: UnboundedReceiver<ReplicatorMessage>,
}

impl ReplicatorChannel {
    fn new() -> Self {
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        Self { sender, receiver }
    }

    fn sender(&self) -> UnboundedSender<ReplicatorMessage> {
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
        /// The SQL dialect to use for all migrated queries and types
        dialect: Dialect,
        /// The result of the migration gets sent down here.
        done_tx: tokio::sync::oneshot::Sender<ReadySetResult<()>>,
    },
    /// Set a failpoint
    #[cfg(feature = "failure_injection")]
    Failpoint {
        name: String,
        action: String,
        done_tx: tokio::sync::oneshot::Sender<()>,
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
    /// Receives notifications that background tasks have failed
    background_task_failed_rx: Receiver<ReadySetError>,
    /// Clone to send notifications that background tasks have failed
    background_task_failed_tx: Sender<ReadySetError>,
    /// A `ControllerDescriptor` that describes this server instance.
    our_descriptor: ControllerDescriptor,
    /// The descriptor of the worker this controller's server is running.
    worker_descriptor: WorkerDescriptor,
    /// The config associated with this controller's server.
    config: Config,
    /// Whether we are the leader and ready to handle requests.
    leader_ready: Arc<AtomicBool>,

    /// Channel used to notify the controller of replicator events.
    replicator_channel: ReplicatorChannel,

    /// Provides the ability to report metrics to Segment
    telemetry_sender: TelemetrySender,

    /// Whether or not to consider failed writes to base tables as no-ops
    permissive_writes: bool,

    /// Handle used to receive a shutdown signal
    shutdown_rx: ShutdownReceiver,
}

impl Controller {
    pub(crate) fn new(
        authority: Arc<Authority>,
        worker_tx: Sender<WorkerRequest>,
        controller_rx: Receiver<ControllerRequest>,
        handle_rx: Receiver<HandleRequest>,
        our_descriptor: ControllerDescriptor,
        worker_descriptor: WorkerDescriptor,
        telemetry_sender: TelemetrySender,
        config: Config,
        shutdown_rx: ShutdownReceiver,
    ) -> Self {
        // If we don't have an upstream, we allow permissive writes to base tables.
        let permissive_writes = config.replicator_config.upstream_db_url.is_none();
        let (background_task_failed_tx, background_task_failed_rx) = mpsc::channel(1);
        Self {
            inner: Arc::new(LeaderHandle::new()),
            authority,
            worker_tx,
            http_rx: controller_rx,
            handle_rx,
            background_task_failed_tx,
            background_task_failed_rx,
            our_descriptor,
            worker_descriptor,
            config,
            leader_ready: Arc::new(AtomicBool::new(false)),
            replicator_channel: ReplicatorChannel::new(),
            telemetry_sender,
            permissive_writes,
            shutdown_rx,
        }
    }

    /// Send the provided `WorkerRequestKind` to the worker running in the same server instance as
    /// this controller wrapper, but don't bother waiting for the response.
    ///
    /// Not waiting for the response avoids deadlocking when the controller and worker are in the
    /// same readyset-server instance.
    async fn send_worker_request(&self, kind: WorkerRequestKind) -> ReadySetResult<()> {
        let (tx, _rx) = tokio::sync::oneshot::channel();
        self.worker_tx
            .send(WorkerRequest { kind, done_tx: tx })
            .await
            .map_err(|e| internal_err!("failed to send to instance worker: {}", e))?;
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
                            leader.running_recovery.is_none() && {
                                let ds = leader.dataflow_state_handle.read().await;
                                !ds.workers.is_empty()
                            }
                        }
                        None => false,
                    };
                if tx.send(done).is_err() {
                    warn!("readiness query sender hung up!");
                }
            }
            HandleRequest::PerformMigration {
                func,
                dialect,
                done_tx,
            } => {
                let mut guard = self.inner.write().await;
                if let Some(ref mut inner) = *guard {
                    let mut writer = inner.dataflow_state_handle.write().await;
                    let ds = writer.as_mut();
                    let res = ds.migrate(false, dialect, move |m| func(m)).await;
                    if res.is_ok() {
                        inner
                            .dataflow_state_handle
                            .commit(writer, &self.authority)
                            .await?;
                    }

                    if done_tx.send(res).is_err() {
                        warn!("handle-based migration sender hung up!");
                    }
                } else {
                    return Err(ReadySetError::NotLeader);
                }
            }
            #[cfg(feature = "failure_injection")]
            HandleRequest::Failpoint {
                name,
                action,
                done_tx,
            } => {
                info!(%name, %action, "handling failpoint request");
                fail::cfg(name, action.as_str()).expect("failed to set failpoint");
                if done_tx.send(()).is_err() {
                    warn!("handle-based failpoint sender hung up!");
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
                let background_task_failed_tx = self.background_task_failed_tx.clone();
                let mut leader = Leader::new(
                    state,
                    self.our_descriptor.controller_uri.clone(),
                    self.authority.clone(),
                    background_task_failed_tx,
                    self.config.replicator_statement_logging,
                    self.config.replicator_config.clone(),
                    self.config.worker_request_timeout,
                );
                self.leader_ready.store(false, Ordering::Release);

                leader
                    .start(
                        self.replicator_channel.sender(),
                        self.telemetry_sender.clone(),
                        self.shutdown_rx.clone(),
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
        tokio::spawn(
            crate::controller::authority_runner(
                authority_tx,
                self.authority.clone(),
                self.our_descriptor.clone(),
                self.worker_descriptor.clone(),
                self.config.clone(),
                self.permissive_writes,
                self.shutdown_rx.clone(),
            )
            .instrument(info_span!("authority")),
        );

        let leader_ready = self.leader_ready.clone();
        loop {
            // There is either...
            let running_recovery = self
                .inner
                .read()
                .await
                .as_ref()
                .and_then(|leader| {
                    leader.running_recovery.clone().map(|mut rx| {
                        // a. A currently running recovery, which when it's done will place its
                        //    result in a `tokio::sync::watch`. ...
                        let inner = Arc::clone(&self.inner);
                        Either::Left(async move {
                            // ... We wait until that recovery is done...
                            let _ = rx.changed().await;
                            // ... then yield the result of the recovery.
                            inner
                                .read()
                                .await
                                .as_ref()
                                .and_then(|leader| leader.running_recovery.as_ref())
                                .map(|rx| (*rx.borrow()).clone())
                                .unwrap_or(Ok(()))
                        })
                    })
                })
                // b. No currently running recovery, in which case we don't need to wait for
                //    anything (but we make a `pending()` future so that we can have a single value
                //    to await on in the `select`)
                .unwrap_or_else(|| Either::Right(future::pending::<ReadySetResult<()>>()));

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
                        let leader_ready = leader_ready.load(Ordering::Acquire);
                        tokio::spawn(handle_controller_request(
                            req,
                            self.authority.clone(),
                            self.inner.clone(),
                            leader_ready
                        ));
                    }
                    else {
                        info!("Controller shutting down after HTTP handle dropped");
                        break;
                    }
                }
                req = authority_rx.recv() => {
                    set_failpoint!(failpoints::AUTHORITY);
                    match req {
                        Some(req) => match self.handle_authority_update(req).await {
                            Ok(()) => {},
                            Err(_) if self.shutdown_rx.signal_received() => {
                                // If we've encountered an error but the shutdown signal has been received, the
                                // error probably occurred because the server is shutting down
                                info!("Controller shutting down after shutdown signal received");
                                break;
                            }
                            Err(e) => return Err(e),
                        }
                        None => {
                            if self.shutdown_rx.signal_received() {
                                // If we've encountered an error but the shutdown signal has been received, the
                                // error probably occurred because the server is shutting down
                                info!("Controller shutting down after shutdown signal received");
                                break;
                            } else {
                                // this shouldn't ever happen: if the leadership campaign thread fails,
                                // it should send a `CampaignError` in `handle_authority_update`.
                                internal!("leadership thread has unexpectedly failed.")
                            }
                        }
                    }
                }
                req = self.replicator_channel.receiver.recv() => {
                    match req {
                        Some(msg) => match msg {
                            ReplicatorMessage::Error(e)=> return Err(e),
                            ReplicatorMessage::SnapshotDone => {
                                self.leader_ready.store(true, Ordering::Release);

                                let now = SystemTime::now()
                                        .duration_since(UNIX_EPOCH)
                                        .unwrap()
                                        .as_millis() as u64;

                                if let Err(error) = self.authority.update_persistent_stats(|stats: Option<PersistentStats>| {
                                    let mut stats = stats.unwrap_or_default();
                                    stats.last_completed_snapshot = Some(now);
                                    Ok(stats)
                                }).await {
                                    error!(%error, "Failed to persist status in the Authority");
                                }
                            },
                            ReplicatorMessage::ReplicationStarted => {
                                let now = SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap()
                                .as_millis() as u64;
                                if let Err(error) = self.authority.update_persistent_stats(|stats: Option<PersistentStats>| {
                                    let mut stats = stats.unwrap_or_default();
                                    stats.last_started_replication = Some(now);
                                    Ok(stats)
                                }).await {
                                    error!(%error, "Failed to persist status in the Authority");
                                }

                            },
                        },
                        _ => {
                            if self.shutdown_rx.signal_received() {
                                // If we've encountered an error but the shutdown signal has been received, the
                                // error probably occurred because the server is shutting down
                                info!("Controller shutting down after shutdown signal received");
                                break;
                            } else {
                                internal!("leader status invalid or channel dropped, leader failed")
                            }
                        }
                    }

                }
                res = running_recovery => {
                    res?; // If recovery fails, fail the whole controller (there's not much else we
                          // can do!)
                    if let Some(leader) = self.inner.write().await.as_mut() {
                        // Recovery is done, so we can write back None (and avoid the whole
                        // rigamarole with .await'ing it)
                        leader.running_recovery = None;
                    }
                }
                err = self.background_task_failed_rx.recv() => {
                    if let Some(err) = err {
                        return Err(err)
                    }
                }
                _ = self.shutdown_rx.recv() => {
                    info!("Controller shutting down after shutdown signal received");
                    break;
                }
            }
        }

        if let Err(error) = self.authority.surrender_leadership().await {
            error!(%error, "failed to surrender leadership");
            internal!("failed to surrender leadership: {}", error)
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
    /// Whether or not to treat failed writes to base nodes as no-ops
    permissive_writes: bool,
}

impl AuthorityLeaderElectionState {
    fn new(
        event_tx: Sender<AuthorityUpdate>,
        authority: Arc<Authority>,
        descriptor: ControllerDescriptor,
        permissive_writes: bool,
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
            permissive_writes,
        }
    }

    fn is_leader(&self) -> bool {
        self.is_leader
    }

    async fn watch_leader(&self) -> ReadySetResult<()> {
        self.authority.watch_leader().await
    }

    async fn update_leader_state(&mut self) -> ReadySetResult<()> {
        let mut should_attempt_leader_election = false;
        match self.authority.try_get_leader().await? {
            // The leader has changed, inform the worker.
            GetLeaderResult::NewLeader(payload) => {
                self.is_leader = false;
                let authority_update = AuthorityUpdate::LeaderChange(payload);
                self.event_tx
                    .send(authority_update)
                    .await
                    .map_err(|_| internal_err!("send failed"))?;
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
            let update_res = self
                .authority
                .update_controller_state(
                    |state: Option<ControllerState>| -> Result<ControllerState, ()> {
                        match state {
                            None => {
                                Ok(ControllerState::new(self.config.clone(), self.permissive_writes))
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
                .await;

            let state = match update_res {
                Ok(Ok(state)) => state,
                Ok(Err(_)) => return Ok(()),
                Err(error) if error.caused_by_serialization_failed() => {
                    warn!(
                        %error,
                        "Error deserializing controller state, wiping state and starting fresh \
                         (NOTE: this will drop all caches!)"
                    );
                    let state = ControllerState::new(self.config.clone(), self.permissive_writes);
                    let new_state = state.clone(); // needs to be in a `let` binding for Send reasons...
                    self.authority.overwrite_controller_state(new_state).await?;
                    state
                }
                Err(e) => return Err(e),
            };

            // Notify our worker that we have won the leader election.
            self.event_tx
                .send(AuthorityUpdate::WonLeaderElection(state))
                .await
                .map_err(|_| internal_err!("failed to announce who won leader election"))?;

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

    async fn register(&mut self) -> ReadySetResult<()> {
        self.worker_id = self
            .authority
            .register_worker(self.descriptor.clone())
            .await?;
        Ok(())
    }

    async fn heartbeat(&self) -> ReadySetResult<AuthorityWorkerHeartbeatResponse> {
        if let Some(id) = &self.worker_id {
            return self.authority.worker_heartbeat(id.clone()).await;
        }

        Ok(AuthorityWorkerHeartbeatResponse::Failed)
    }

    fn clear_active_workers(&mut self) {
        self.active_workers.clear();
    }

    async fn watch_workers(&self) -> ReadySetResult<()> {
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
                .map_err(|_| format_err!("failed to announce failed workers"))?;
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
                .map_err(|_| format_err!("failed to announce new workers"))?;
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
    permissive_writes: bool,
) -> anyhow::Result<()> {
    authority.init().await?;

    let mut leader_election_state = AuthorityLeaderElectionState::new(
        event_tx.clone(),
        authority.clone(),
        descriptor,
        permissive_writes,
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
        set_failpoint!(failpoints::AUTHORITY);
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
                        warn!(error = %e, "failure creating worker watch");
                    }
                },
                watch_result = worker_state.watch_workers() => {
                    if let Err(e) = watch_result {
                        warn!(error = %e, "failure creating worker watch");
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
    permissive_writes: bool,
    mut shutdown_rx: ShutdownReceiver,
) -> anyhow::Result<()> {
    tokio::select! {
        result = authority_inner(
            event_tx.clone(),
            authority,
            descriptor,
            worker_descriptor,
            config,
            permissive_writes,
        ) => if let Err(e) = result
        {
            if shutdown_rx.signal_received() {
                // If we've encountered an error but the shutdown signal has been received, the
                // error probably occurred because the server is shutting down
                info!("Authority runner shutting down after shutdown signal received");
            } else {
                let _ = event_tx.send(AuthorityUpdate::AuthorityError(e)).await;
                anyhow::bail!("Authority runner failed");
            }
        },
        _ = shutdown_rx.recv() => {
            info!("Authority runner shutting down after shutdown signal received");
        }
    }
    Ok(())
}

async fn handle_controller_request(
    req: ControllerRequest,
    authority: Arc<Authority>,
    leader_handle: Arc<LeaderHandle>,
    leader_ready: bool,
) {
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
        let resp = {
            if let Some(ref ci) = *guard {
                Ok(ci
                    .external_request(method, path.as_ref(), query, body, &authority, leader_ready)
                    .await)
            } else {
                Err(ReadySetError::NotLeader)
            }
        };

        match resp {
            // returned from `Leader::external_request`:
            Ok(Ok(r)) => Ok(Ok(r)),
            Ok(Err(ReadySetError::NoQuorum)) => Err(StatusCode::SERVICE_UNAVAILABLE),
            Ok(Err(ReadySetError::UnknownEndpoint)) => Err(StatusCode::NOT_FOUND),
            // something else failed:
            Err(ReadySetError::NotLeader) => Err(StatusCode::SERVICE_UNAVAILABLE),
            Ok(Err(e)) | Err(e) => Ok(Err(bincode::serialize(&e)
                .expect("Bincode serialization of ReadySetError should not fail"))),
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
}

#[cfg(test)]
mod tests {

    use std::collections::{BTreeMap, HashSet};

    use dataflow::DomainIndex;
    use nom_sql::{parse_create_table, parse_select_statement, Dialect, Relation};
    use readyset_client::recipe::changelist::{Change, ChangeList};
    use readyset_client::{KeyCount, TableReplicationStatus, TableStatus, ViewCreateRequest};
    use readyset_data::Dialect as DataDialect;
    use readyset_util::eventually;
    use replication_offset::ReplicationOffset;
    use replicators::MySqlPosition;

    use crate::integration_utils::start_simple;

    #[tokio::test(flavor = "multi_thread")]
    async fn remove_query() {
        let (mut noria, shutdown_tx) = start_simple("remove_query").await;
        noria
            .extend_recipe(
                ChangeList::from_str(
                    "CREATE TABLE users (id INT PRIMARY KEY, name TEXT);
                 CREATE CACHE test_query FROM SELECT * FROM users;",
                    DataDialect::DEFAULT_MYSQL,
                )
                .unwrap(),
            )
            .await
            .unwrap();

        let queries = noria.views().await.unwrap();
        assert!(queries.contains_key(&"test_query".into()));

        noria.remove_query(&"test_query".into()).await.unwrap();

        let queries = noria.views().await.unwrap();
        assert!(!queries.contains_key(&"test_query".into()));

        shutdown_tx.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn remove_all_queries() {
        let (mut noria, shutdown_tx) = start_simple("remove_all_queries").await;
        noria
            .extend_recipe(
                ChangeList::from_str(
                    "CREATE TABLE users (id INT PRIMARY KEY, name TEXT);
                 CREATE CACHE q1 FROM SELECT id FROM users;
                 CREATE CACHE q2 FROM SELECT name FROM users where id = ?;",
                    DataDialect::DEFAULT_MYSQL,
                )
                .unwrap(),
            )
            .await
            .unwrap();

        let queries = noria.views().await.unwrap();
        assert!(queries.contains_key(&"q1".into()));
        assert!(queries.contains_key(&"q2".into()));

        noria.remove_all_queries().await.unwrap();

        let queries = noria.views().await.unwrap();
        assert!(queries.is_empty());

        shutdown_tx.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn replication_offsets() {
        let (mut noria, shutdown_tx) = start_simple("all_tables").await;

        let offset = ReplicationOffset::MySql(
            MySqlPosition::from_file_name_and_position("binlog.00001".to_owned(), 1).unwrap(),
        );

        noria
            .set_schema_replication_offset(Some(&offset))
            .await
            .unwrap();
        noria
            .extend_recipe(
                ChangeList::from_str(
                    "CREATE TABLE t1 (id int);
                     CREATE TABLE t2 (id int);
                     CREATE TABLE t3 (id int);",
                    DataDialect::DEFAULT_MYSQL,
                )
                .unwrap(),
            )
            .await
            .unwrap();

        let mut t1 = noria.table("t1").await.unwrap();
        let mut t2 = noria.table("t2").await.unwrap();

        t1.set_replication_offset(ReplicationOffset::MySql(
            MySqlPosition::from_file_name_and_position("binlog.00001".to_owned(), 2).unwrap(),
        ))
        .await
        .unwrap();

        t2.set_replication_offset(ReplicationOffset::MySql(
            MySqlPosition::from_file_name_and_position("binlog.00001".to_owned(), 3).unwrap(),
        ))
        .await
        .unwrap();

        let offsets = noria.replication_offsets().await.unwrap();

        let mysql_pos: MySqlPosition = offsets.schema.unwrap().try_into().unwrap();
        assert_eq!(mysql_pos.position, 1);

        let mysql_pos: MySqlPosition = offsets.tables[&"t1".into()]
            .as_ref()
            .unwrap()
            .try_into()
            .unwrap();
        assert_eq!(mysql_pos.position, 2);

        let mysql_pos: MySqlPosition = offsets.tables[&"t2".into()]
            .as_ref()
            .unwrap()
            .try_into()
            .unwrap();
        assert_eq!(mysql_pos.position, 3);

        assert_eq!(offsets.tables[&"t3".into()], None);

        shutdown_tx.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn key_count_rpc() {
        let (mut noria, shutdown_tx) = start_simple("all_tables").await;

        noria
            .extend_recipe(
                ChangeList::from_str(
                    "CREATE TABLE key_count_test (id INT PRIMARY KEY, stuff TEXT);
                 CREATE CACHE q1 FROM SELECT * FROM key_count_test;",
                    DataDialect::DEFAULT_MYSQL,
                )
                .unwrap(),
            )
            .await
            .unwrap();

        let mut table = noria.table("key_count_test").await.unwrap();
        // The table only contains the local index, so we use `tables()` to get the global index
        let table_idx = noria.tables().await.unwrap()[&"key_count_test".into()];
        let view_idx = *noria
            .view("q1")
            .await
            .unwrap()
            .into_reader_handle()
            .unwrap()
            .node();

        let key_counts = noria.node_sizes().await.unwrap();

        assert_eq!(
            key_counts[&table_idx].key_count,
            KeyCount::EstimatedRowCount(0)
        );
        assert_eq!(key_counts[&view_idx].key_count, KeyCount::ExactKeyCount(0));

        table.insert(vec![1.into(), "abc".into()]).await.unwrap();

        eventually!(run_test: {
            noria.node_sizes().await
        },
        then_assert: |key_counts| {
            let key_counts = key_counts.unwrap();
            assert_eq!(key_counts[&table_idx].key_count, KeyCount::EstimatedRowCount(1));
            assert_eq!(key_counts[&view_idx].key_count, KeyCount::ExactKeyCount(1));
        });

        shutdown_tx.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn view_statuses() {
        let (mut noria, shutdown_tx) = start_simple("view_statuses").await;

        let query1 = parse_select_statement(Dialect::MySQL, "SELECT * FROM t1").unwrap();
        let query2 = parse_select_statement(Dialect::MySQL, "SELECT * FROM t2").unwrap();

        let schema_search_path = vec!["s1".into()];

        let res1 = noria
            .view_statuses(
                vec![
                    ViewCreateRequest::new(query1.clone(), schema_search_path.clone()),
                    ViewCreateRequest::new(query2.clone(), schema_search_path.clone()),
                ],
                DataDialect::DEFAULT_MYSQL,
            )
            .await
            .unwrap();
        assert_eq!(res1, vec![false, false]);

        noria
            .extend_recipe(
                ChangeList::from_str(
                    "CREATE TABLE t1 (x int); CREATE CACHE FROM SELECT * FROM t1;",
                    DataDialect::DEFAULT_MYSQL,
                )
                .unwrap()
                .with_schema_search_path(schema_search_path.clone()),
            )
            .await
            .unwrap();

        let res2 = noria
            .view_statuses(
                vec![
                    ViewCreateRequest::new(query1.clone(), schema_search_path.clone()),
                    ViewCreateRequest::new(query2.clone(), schema_search_path.clone()),
                ],
                DataDialect::DEFAULT_MYSQL,
            )
            .await
            .unwrap();
        assert_eq!(res2, vec![true, false]);

        // A syntactically distinct, but semantically equivalent query
        let query1_equivalent = parse_select_statement(Dialect::MySQL, "SELECT x FROM t1").unwrap();
        let res3 = noria
            .view_statuses(
                vec![ViewCreateRequest::new(
                    query1_equivalent,
                    schema_search_path.clone(),
                )],
                DataDialect::DEFAULT_MYSQL,
            )
            .await
            .unwrap();
        assert_eq!(res3, vec![true]);

        // A change in schema_search_path that doesn't change the semantics
        let query1_equivalent = parse_select_statement(Dialect::MySQL, "SELECT x FROM t1").unwrap();
        let res3 = noria
            .view_statuses(
                vec![ViewCreateRequest::new(
                    query1_equivalent,
                    vec!["s2".into(), "s1".into()],
                )],
                DataDialect::DEFAULT_MYSQL,
            )
            .await
            .unwrap();
        assert_eq!(res3, vec![true]);

        // A change in schema_search_path that *does* change the semantics
        noria
            .extend_recipe(
                ChangeList::from_str("CREATE TABLE s2.t1 (x int)", DataDialect::DEFAULT_MYSQL)
                    .unwrap(),
            )
            .await
            .unwrap();

        let query1_equivalent = parse_select_statement(Dialect::MySQL, "SELECT x FROM t1").unwrap();
        let res3 = noria
            .view_statuses(
                vec![ViewCreateRequest::new(
                    query1_equivalent,
                    vec!["s2".into(), "s1".into()],
                )],
                DataDialect::DEFAULT_MYSQL,
            )
            .await
            .unwrap();
        assert_eq!(res3, vec![false]);

        shutdown_tx.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn non_replicated_relations() {
        let (mut noria, shutdown_tx) = start_simple("non_replicated_tables").await;
        noria
            .extend_recipe(ChangeList::from_changes(
                vec![
                    Change::AddNonReplicatedRelation(Relation {
                        schema: Some("s1".into()),
                        name: "t".into(),
                    }),
                    Change::AddNonReplicatedRelation(Relation {
                        schema: Some("s2".into()),
                        name: "t".into(),
                    }),
                ],
                DataDialect::DEFAULT_MYSQL,
            ))
            .await
            .unwrap();

        let rels = noria.non_replicated_relations().await.unwrap();
        assert_eq!(
            rels,
            HashSet::from([
                Relation {
                    schema: Some("s1".into()),
                    name: "t".into(),
                },
                Relation {
                    schema: Some("s2".into()),
                    name: "t".into(),
                },
            ])
        );

        shutdown_tx.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn table_statuses() {
        let (mut noria, shutdown_tx) = start_simple("table_status").await;
        noria
            .extend_recipe(ChangeList::from_changes(
                vec![
                    Change::AddNonReplicatedRelation(Relation {
                        schema: Some("s1".into()),
                        name: "t".into(),
                    }),
                    Change::CreateTable(
                        parse_create_table(
                            Dialect::MySQL,
                            "CREATE TABLE s2.snapshotting_t (x int);",
                        )
                        .unwrap(),
                    ),
                    Change::CreateTable(
                        parse_create_table(
                            Dialect::MySQL,
                            "CREATE TABLE s2.snapshotted_t (x int);",
                        )
                        .unwrap(),
                    ),
                ],
                DataDialect::DEFAULT_MYSQL,
            ))
            .await
            .unwrap();

        noria
            .table(Relation {
                schema: Some("s2".into()),
                name: "snapshotting_t".into(),
            })
            .await
            .unwrap()
            .set_snapshot_mode(true)
            .await
            .unwrap();

        let mut snapshotted = noria
            .table(Relation {
                schema: Some("s2".into()),
                name: "snapshotted_t".into(),
            })
            .await
            .unwrap();
        snapshotted.set_snapshot_mode(false).await.unwrap();
        snapshotted
            .set_replication_offset(ReplicationOffset::MySql(
                MySqlPosition::from_file_name_and_position("log.00001".into(), 1).unwrap(),
            ))
            .await
            .unwrap();

        let res = noria.table_statuses().await.unwrap();
        assert_eq!(
            res,
            BTreeMap::from([
                (
                    Relation {
                        schema: Some("s1".into()),
                        name: "t".into(),
                    },
                    TableStatus {
                        replication_status: TableReplicationStatus::NotReplicated
                    }
                ),
                (
                    Relation {
                        schema: Some("s2".into()),
                        name: "snapshotting_t".into(),
                    },
                    TableStatus {
                        replication_status: TableReplicationStatus::Snapshotting
                    }
                ),
                (
                    Relation {
                        schema: Some("s2".into()),
                        name: "snapshotted_t".into(),
                    },
                    TableStatus {
                        replication_status: TableReplicationStatus::Snapshotted
                    }
                ),
            ])
        );

        shutdown_tx.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn domains() {
        let (mut noria, shutdown_tx) = start_simple("domains").await;
        noria
            .extend_recipe(ChangeList::from_change(
                Change::CreateTable(
                    parse_create_table(Dialect::MySQL, "CREATE TABLE t1 (x int);").unwrap(),
                ),
                DataDialect::DEFAULT_MYSQL,
            ))
            .await
            .unwrap();

        let res = noria.domains().await.unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(
            res.keys().copied().collect::<Vec<_>>(),
            vec![DomainIndex::from(0)]
        );

        shutdown_tx.shutdown().await;
    }
}
