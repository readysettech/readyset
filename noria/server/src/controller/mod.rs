use crate::controller::inner::ControllerInner;
use crate::controller::migrate::Migration;
use crate::controller::recipe::Recipe;
use crate::coordination::do_noria_rpc;
use crate::errors::internal_err;
use crate::worker::{WorkerRequest, WorkerRequestKind};
use crate::{Config, ReadySetResult, VolumeId};
use anyhow::{format_err, Context};
use futures_util::StreamExt;
use hyper::http::{Method, StatusCode};
use launchpad::select;
use noria::consensus::{AuthorityWorkerHeartbeatResponse, WorkerId};
use noria::ControllerDescriptor;
use noria::{
    consensus::{Authority, AuthorityControl, GetLeaderResult, WorkerDescriptor},
    ReplicationOffset,
};
use noria::{internal, ReadySetError};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use stream_cancel::Valve;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{error, info, warn};
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
        Ok(do_noria_rpc(
            self.http
                .post(self.uri.join("worker_request")?)
                .body(hyper::Body::from(bincode::serialize(&req)?)),
        )
        .await?)
    }
}

/// Type alias for "a worker's URI" (as reported in a `RegisterPayload`).
type WorkerIdentifier = Url;

/// An update on the leader election and failure detection.
#[derive(Debug)]
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

/// A wrapper for a potential controller, also handling leader elections, HTTP requests that aren't
/// destined for the worker, and requests originated from this server instance's `Handle`.
pub struct ControllerOuter {
    /// If we're the leader, the actual controller.
    pub(crate) inner: Option<ControllerInner>,
    /// The `Authority` structure used for leadership elections & such state.
    pub(crate) authority: Arc<Authority>,
    /// Channel to the `Worker` running inside this server instance.
    ///
    /// This is used to convey changes in leadership state.
    pub(crate) worker_tx: Sender<WorkerRequest>,
    /// Receives updates from the authority thread.
    pub(crate) authority_rx: Receiver<AuthorityUpdate>,
    /// Receives external HTTP requests.
    pub(crate) http_rx: Receiver<ControllerRequest>,
    /// Receives requests from the controller's `Handle`.
    pub(crate) handle_rx: Receiver<HandleRequest>,
    /// A `ControllerDescriptor` that describes this server instance.
    pub(crate) our_descriptor: ControllerDescriptor,
    /// Valve for shutting down; triggered by the `Handle` when `Handle::shutdown()` is called.
    pub(crate) valve: Valve,
    /// Primary MySQL/PostgresSQL server connection URL that Noria replicates from
    pub(crate) replicator_url: Option<String>,
    /// A handle to the replicator task
    pub(crate) replicator_task: Option<tokio::task::JoinHandle<()>>,
}
impl ControllerOuter {
    /// Run the provided *blocking* closure with the `ControllerInner` and the `Authority` if this
    /// server instance is currently the leader.
    ///
    /// If it isn't, returns `Err(ReadySetError::NotLeader)`, and doesn't run the closure.
    async fn with_controller_blocking<F, T>(&mut self, func: F) -> ReadySetResult<T>
    where
        F: FnOnce(&mut ControllerInner, Arc<Authority>) -> ReadySetResult<T>,
    {
        // FIXME: this is potentially slow, and it's only like this because borrowck sucks
        let auth = self.authority.clone();
        if let Some(ref mut ci) = self.inner {
            Ok(tokio::task::block_in_place(|| func(ci, auth))?)
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

        let ret: Result<Result<Vec<u8>, Vec<u8>>, StatusCode> = {
            let resp = self
                .with_controller_blocking(|ctrl, auth| {
                    Ok(ctrl.external_request(method.clone(), path.clone(), query, body, &auth))
                })
                .await;
            match resp {
                // returned from `ControllerInner::external_request`:
                Ok(Ok(r)) => Ok(Ok(r)),
                Ok(Err(ReadySetError::NoQuorum)) => Err(StatusCode::SERVICE_UNAVAILABLE),
                Ok(Err(ReadySetError::UnknownEndpoint)) => Err(StatusCode::NOT_FOUND),
                Ok(Err(e)) => Ok(Err(bincode::serialize(&e)?)),
                // errors returned by `with_controller_blocking`:
                Err(ReadySetError::NotLeader) => Err(StatusCode::SERVICE_UNAVAILABLE),
                Err(e) => Ok(Err(bincode::serialize(&e)?)),
            }
        };

        if reply_tx.send(ret).is_err() {
            warn!("client hung up");
        }
        Ok(())
    }

    async fn handle_handle_request(&mut self, req: HandleRequest) -> ReadySetResult<()> {
        match req {
            HandleRequest::QueryReadiness(tx) => {
                let done = self
                    .inner
                    .as_ref()
                    .map(|ctrl| !ctrl.workers.is_empty())
                    .unwrap_or(false);
                if tx.send(done).is_err() {
                    warn!("readiness query sender hung up!");
                }
            }
            HandleRequest::PerformMigration { func, done_tx } => {
                let ret = self
                    .with_controller_blocking(|ctrl, _| {
                        ctrl.migrate(move |m| func(m))??;
                        Ok(())
                    })
                    .await;
                if done_tx.send(ret).is_err() {
                    warn!("handle-based migration sender hung up!");
                }
            }
        }
        Ok(())
    }

    async fn stop_replication_task(&mut self) {
        if let Some(handle) = self.replicator_task.take() {
            handle.abort();
            let _ = handle.await;
        }
    }

    /// Start replication/binlog synchronization in an infinite loop
    /// on any error the task will retry again and again, because in case
    /// a connection to the primary was lost for any reason, all we want is to
    /// connect again, and catch up from the binlog
    ///
    /// TODO: how to handle the case where we need a full new replica
    fn start_replication_task(&mut self) {
        let url = match &self.replicator_url {
            Some(url) => url.to_string(),
            None => {
                info!("No primary instance specified");
                return;
            }
        };

        let authority = Arc::clone(&self.authority);
        self.replicator_task = Some(tokio::spawn(async move {
            loop {
                let noria: noria::ControllerHandle =
                    noria::ControllerHandle::new(Arc::clone(&authority)).await;

                if let Err(err) = replicators::NoriaAdapter::start_with_url(&url, noria, None).await
                {
                    // On each replication error we wait for 30 seconds and then try again
                    tracing::error!(error = %err, "replication error");
                    tokio::time::sleep(Duration::from_secs(30)).await;
                }
            }
        }));
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
                info!("won leader election, creating ControllerInner");
                self.inner = Some(ControllerInner::new(
                    state.clone(),
                    self.our_descriptor.controller_uri.clone(),
                ));
                self.send_worker_request(WorkerRequestKind::NewController {
                    controller_uri: self.our_descriptor.controller_uri.clone(),
                })
                .await?;

                // After the controller becomes the leader, we need it to start listening on
                // the binlog, it should stop doing it if it stops being a leader
                self.start_replication_task();
            }
            AuthorityUpdate::NewWorkers(w) if self.inner.is_some() => {
                for worker in w {
                    self.with_controller_blocking(|ctrl, _| {
                        ctrl.handle_register_from_authority(worker)
                    })
                    .await?;
                }
            }
            AuthorityUpdate::FailedWorkers(w) if self.inner.is_some() => {
                self.with_controller_blocking(|ctrl, _| {
                    ctrl.handle_failed_workers(w.into_iter().map(|desc| desc.worker_uri).collect())
                })
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
        loop {
            // produces a value when the `Valve` is closed
            let mut shutdown_stream = self.valve.wrap(futures_util::stream::pending::<()>());

            select! {
                req = self.handle_rx.recv() => {
                    if let Some(req) = req {
                        self.handle_handle_request(req).await?;
                    }
                    else {
                        info!("ControllerOuter shutting down after request handle dropped");
                        break;
                    }
                }
                req = self.http_rx.recv() => {
                    if let Some(req) = req {
                        self.handle_controller_request(req).await?;
                    }
                    else {
                        info!("ControllerOuter shutting down after HTTP handle dropped");
                        break;
                    }
                }
                req = self.authority_rx.recv() => {
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
                _ = shutdown_stream.next() => {
                    info!("ControllerOuter shutting down after valve shut");
                    break;
                }
            }
        }
        if self.inner.is_some() {
            self.stop_replication_task().await;

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
        region: Option<String>,
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
                                // check that running config is compatible with the new
                                // configuration.
                                assert_eq!(
                                    state.config, self.config,
                                    "Config in authority is not compatible with requested config!"
                                );
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
        }

        self.is_leader = true;

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
    region: Option<String>,
) -> anyhow::Result<()> {
    authority.init().await?;

    let mut leader_election_state = AuthorityLeaderElectionState::new(
        event_tx.clone(),
        authority.clone(),
        descriptor,
        config,
        region,
    );

    let mut worker_state =
        AuthorityWorkerState::new(event_tx, authority.clone(), worker_descriptor);

    // Register the current server as a worker in the system.
    worker_state
        .register()
        .await
        .context("Registering worker")?;
    loop {
        leader_election_state
            .update_leader_state()
            .await
            .context("Updating leader state")?;
        // TODO(justin): Handle detected as failed.
        worker_state.heartbeat().await?;

        if leader_election_state.is_leader() {
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
    region: Option<String>,
) -> anyhow::Result<()> {
    if let Err(e) = authority_inner(
        event_tx.clone(),
        authority,
        descriptor,
        worker_descriptor,
        config,
        region,
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

            let offset: BinlogPosition = noria.replication_offset().await.unwrap().unwrap().into();
            assert_eq!(offset, binlog);

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
    }
}
