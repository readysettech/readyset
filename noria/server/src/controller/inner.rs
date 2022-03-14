#![deny(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::unimplemented,
    clippy::unreachable
)]

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use failpoint_macros::failpoint;
use hyper::Method;
use noria::consensus::Authority;
use noria::replication::ReplicationOffset;
use noria::status::{ReadySetStatus, SnapshotStatus};
use noria::{RecipeSpec, WorkerDescriptor};
use noria_errors::{ReadySetError, ReadySetResult};
use reqwest::Url;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::Notify;
use tracing::{error, info, warn};

use crate::controller::state::{DataflowState, DataflowStateHandle};
use crate::controller::{ControllerRequest, ControllerState, Worker, WorkerIdentifier};
use crate::coordination::DomainDescriptor;
use crate::worker::WorkerRequestKind;

/// The Noria leader, responsible for making control-plane decisions for the whole of a Noria
/// cluster.
///
/// This runs inside a `Controller` when it is elected as leader.
///
/// It keeps track of the structure of the underlying data flow graph and its domains. `Controller`
/// does not allow direct manipulation of the graph. Instead, changes must be instigated through a
/// `Migration`, which can be performed using `Leader::migrate`. Only one `Migration` can
/// occur at any given point in time.
pub struct Leader {
    pub(super) dataflow_state_handle: DataflowStateHandle,

    pending_recovery: bool,

    quorum: usize,
    controller_uri: Url,

    /// The amount of time to wait for a worker request to complete.
    worker_request_timeout: Duration,
    /// The amount of time to wait before restarting the replicator on error.
    replicator_restart_timeout: Duration,
    /// Upstream database URL to issue replicator commands to.
    pub(super) replicator_url: Option<String>,
    /// A handle to the replicator task
    pub(super) replicator_task: Option<tokio::task::JoinHandle<()>>,
    /// A client to the current authority.
    pub(super) authority: Arc<Authority>,
    /// Optional server id to use when registering for a slot for binlog replication.
    pub(super) server_id: Option<u32>,
}

impl Leader {
    /// Run all tasks required to be the leader. This may spawn tasks that
    /// may become ready asyncronously. Use the notification to indicate
    /// to the Controller that the leader is ready to handle requests.
    pub(super) async fn start(
        &mut self,
        ready_notification: Arc<Notify>,
        replication_error: UnboundedSender<ReadySetError>,
    ) {
        // When the controller becomes the leader, we need to read updates
        // from the binlog.
        self.start_replication_task(ready_notification, replication_error)
            .await;
    }

    pub(super) async fn stop(&mut self) {
        self.stop_replication_task().await;
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
    async fn start_replication_task(
        &mut self,
        ready_notification: Arc<Notify>,
        replication_error: UnboundedSender<ReadySetError>,
    ) {
        let url = match &self.replicator_url {
            Some(url) => url.to_string(),
            None => {
                ready_notification.notify_one();
                info!("No primary instance specified");
                return;
            }
        };

        let server_id = self.server_id;
        let authority = Arc::clone(&self.authority);
        let replicator_restart_timeout = self.replicator_restart_timeout;
        self.replicator_task = Some(tokio::spawn(async move {
            loop {
                let noria: noria::ControllerHandle =
                    noria::ControllerHandle::new(Arc::clone(&authority)).await;

                match replicators::NoriaAdapter::start_with_url(
                    &url,
                    noria,
                    server_id,
                    Some(ready_notification.clone()),
                )
                .await
                {
                    Ok(_) => {}
                    // Unrecoverable errors, propagate the error the controller and kill the loop.
                    Err(err @ ReadySetError::RecipeInvariantViolated(_)) => {
                        if let Err(e) = replication_error.send(err) {
                            error!(error = %e, "Could not notify controller of critical error. The system may be in an invalid state");
                        }
                        break;
                    }
                    Err(err) => {
                        // On each replication error we wait for 30 seconds and then try again
                        tracing::error!(error = %err, "replication error");
                        tokio::time::sleep(replicator_restart_timeout).await;
                    }
                }
            }
        }));
    }

    #[allow(unused_variables)] // `query` is not used unless debug_assertions is enabled
    #[failpoint("controller-request")]
    pub(super) fn external_request(
        &self,
        method: hyper::Method,
        path: &str,
        query: Option<String>,
        body: hyper::body::Bytes,
        authority: &Arc<Authority>,
        leader_ready: bool,
    ) -> ReadySetResult<Vec<u8>> {
        macro_rules! return_serialized {
            ($expr:expr) => {{
                return Ok(::bincode::serialize(&$expr)?);
            }};
        }

        let require_leader_ready = || -> ReadySetResult<()> {
            if !leader_ready {
                Err(ReadySetError::LeaderNotReady)
            } else {
                Ok(())
            }
        };

        // *** Read methods that don't require a quorum***

        macro_rules! check_quorum {
            ($ds:expr) => {{
                if self.pending_recovery || $ds.workers.len() < self.quorum {
                    return Err(ReadySetError::NoQuorum);
                }
            }};
        }

        {
            match (&method, path) {
                (&Method::GET, "/simple_graph") => {
                    let ds = futures::executor::block_on(self.dataflow_state_handle.read());
                    return Ok(ds.graphviz(false).into_bytes());
                }
                (&Method::POST, "/simple_graphviz") => {
                    let ds = futures::executor::block_on(self.dataflow_state_handle.read());
                    return_serialized!(ds.graphviz(false));
                }
                (&Method::GET, "/graph") => {
                    let ds = futures::executor::block_on(self.dataflow_state_handle.read());
                    return Ok(ds.graphviz(true).into_bytes());
                }
                (&Method::POST, "/graphviz") => {
                    let ds = futures::executor::block_on(self.dataflow_state_handle.read());
                    return_serialized!(ds.graphviz(true));
                }
                (&Method::GET | &Method::POST, "/get_statistics") => {
                    let ret = futures::executor::block_on(async move {
                        let ds = self.dataflow_state_handle.read().await;
                        ds.get_statistics().await
                    });
                    return_serialized!(ret);
                }
                (&Method::GET | &Method::POST, "/instances") => {
                    let ds = futures::executor::block_on(self.dataflow_state_handle.read());
                    return_serialized!(ds.get_instances());
                }
                (&Method::GET | &Method::POST, "/controller_uri") => {
                    let ds = futures::executor::block_on(self.dataflow_state_handle.read());
                    return_serialized!(self.controller_uri);
                }
                (&Method::GET, "/workers") | (&Method::POST, "/workers") => {
                    let ds = futures::executor::block_on(self.dataflow_state_handle.read());
                    return_serialized!(&ds.workers.keys().collect::<Vec<_>>())
                }
                (&Method::GET, "/healthy_workers") | (&Method::POST, "/healthy_workers") => {
                    let ds = futures::executor::block_on(self.dataflow_state_handle.read());
                    return_serialized!(&ds
                        .workers
                        .iter()
                        .filter(|w| w.1.healthy)
                        .map(|w| w.0)
                        .collect::<Vec<_>>());
                }
                _ => {}
            }

            // *** Read methods that do require quorum ***

            match (&method, path) {
                (&Method::GET | &Method::POST, "/controller_uri") => {
                    return_serialized!(self.controller_uri);
                }
                (&Method::POST, "/inputs") => {
                    let ds = futures::executor::block_on(self.dataflow_state_handle.read());
                    check_quorum!(ds);
                    return_serialized!(ds.inputs())
                }
                (&Method::POST, "/outputs") => {
                    let ds = futures::executor::block_on(self.dataflow_state_handle.read());
                    check_quorum!(ds);
                    return_serialized!(ds.outputs())
                }
                (&Method::POST, "/verbose_outputs") => {
                    let ds = futures::executor::block_on(self.dataflow_state_handle.read());
                    check_quorum!(ds);
                    return_serialized!(ds.verbose_outputs())
                }
                (&Method::GET | &Method::POST, "/instances") => {
                    let ds = futures::executor::block_on(self.dataflow_state_handle.read());
                    check_quorum!(ds);
                    return_serialized!(ds.get_instances());
                }
                (&Method::GET | &Method::POST, "/workers") => {
                    let ds = futures::executor::block_on(self.dataflow_state_handle.read());
                    check_quorum!(ds);
                    return_serialized!(ds.workers.keys().collect::<Vec<_>>())
                }
                (&Method::GET | &Method::POST, "/healthy_workers") => {
                    let ds = futures::executor::block_on(self.dataflow_state_handle.read());
                    check_quorum!(ds);
                    return_serialized!(ds
                        .workers
                        .iter()
                        .filter(|w| w.1.healthy)
                        .map(|w| w.0)
                        .collect::<Vec<_>>());
                }
                (&Method::GET, "/nodes") => {
                    let ds = futures::executor::block_on(self.dataflow_state_handle.read());
                    check_quorum!(ds);
                    let nodes = if let Some(query) = &query {
                        let pairs = querystring::querify(query);
                        if let Some((_, worker)) = &pairs.into_iter().find(|(k, _)| *k == "w") {
                            ds.nodes_on_worker(Some(&worker.parse()?))
                                .into_iter()
                                .flat_map(|(_, ni)| ni)
                                .collect::<Vec<_>>()
                        } else {
                            ds.nodes_on_worker(None)
                                .into_iter()
                                .flat_map(|(_, ni)| ni)
                                .collect::<Vec<_>>()
                        }
                    } else {
                        // all data-flow nodes
                        ds.nodes_on_worker(None)
                            .into_iter()
                            .flat_map(|(_, ni)| ni)
                            .collect::<Vec<_>>()
                    };
                    return_serialized!(&nodes
                        .into_iter()
                        .filter_map(|ni| {
                            #[allow(clippy::indexing_slicing)]
                            let n = &ds.ingredients[ni];
                            if n.is_internal() {
                                Some((ni, n.name(), n.description(true)))
                            } else if n.is_base() {
                                Some((ni, n.name(), "Base table".to_owned()))
                            } else if n.is_reader() {
                                Some((ni, n.name(), "Leaf view".to_owned()))
                            } else {
                                None
                            }
                        })
                        .collect::<Vec<_>>())
                }
                (&Method::POST, "/table_builder") => {
                    // NOTE(eta): there is DELIBERATELY no `?` after the `table_builder` call,
                    // because            the receiving end expects a
                    // `ReadySetResult` to be serialized.
                    let body = bincode::deserialize(&body)?;
                    let ds = futures::executor::block_on(self.dataflow_state_handle.read());
                    check_quorum!(ds);
                    let ret = ds.table_builder(body);
                    return_serialized!(ret);
                }
                (&Method::POST, "/view_builder") => {
                    // NOTE(eta): same as above applies
                    require_leader_ready()?;
                    let body = bincode::deserialize(&body)?;
                    let ds = futures::executor::block_on(self.dataflow_state_handle.read());
                    check_quorum!(ds);
                    let ret = ds.view_builder(body);
                    return_serialized!(ret);
                }
                (&Method::POST, "/get_info") => {
                    let ds = futures::executor::block_on(self.dataflow_state_handle.read());
                    check_quorum!(ds);
                    return_serialized!(ds.get_info()?)
                }
                (&Method::POST, "/replication_offsets") => {
                    // this method can't be `async` since `Leader` isn't Send because `Graph`
                    // isn't Send :(
                    let res = futures::executor::block_on(async move {
                        let ds = self.dataflow_state_handle.read().await;
                        check_quorum!(ds);
                        ds.replication_offsets().await
                    })?;
                    return_serialized!(res);
                }
                (&Method::POST, "/snapshotting_tables") => {
                    // this method can't be `async` since `Leader` isn't Send because `Graph`
                    // isn't Send :(
                    let res = futures::executor::block_on(async move {
                        let ds = self.dataflow_state_handle.read().await;
                        check_quorum!(ds);
                        ds.snapshotting_tables().await
                    })?;
                    return_serialized!(res);
                }
                (&Method::POST, "/leader_ready") => {
                    return_serialized!(leader_ready);
                }
                (&Method::POST, "/status") => {
                    let status = ReadySetStatus {
                        // Use whether the leader is ready or not as a proxy for if we have
                        // completed snapshotting.
                        snapshot_status: if leader_ready {
                            SnapshotStatus::Completed
                        } else {
                            SnapshotStatus::InProgress
                        },
                    };
                    return_serialized!(status);
                }
                (&Method::POST, "/dry_run") => {
                    let body: RecipeSpec = bincode::deserialize(&body)?;
                    if body.require_leader_ready() {
                        require_leader_ready()?;
                    }
                    let ret = futures::executor::block_on(async move {
                        let mut state_copy: DataflowState = {
                            let reader = self.dataflow_state_handle.read().await;
                            check_quorum!(reader);
                            reader.clone()
                        };
                        state_copy.extend_recipe(body, true).await
                    })?;
                    return_serialized!(ret);
                }

                _ => {}
            }
        }

        // *** Write methods (all of them require quorum) ***

        match (method, path) {
            (Method::GET, "/flush_partial") => {
                let ret = futures::executor::block_on(async move {
                    let mut writer = self.dataflow_state_handle.write().await;
                    check_quorum!(writer.as_ref());
                    let r = writer.as_mut().flush_partial().await?;
                    self.dataflow_state_handle.commit(writer, authority).await?;
                    Ok(r)
                })?;
                return_serialized!(ret);
            }
            (Method::POST, "/extend_recipe") => {
                let body: RecipeSpec = bincode::deserialize(&body)?;
                if body.require_leader_ready() {
                    require_leader_ready()?;
                }
                let ret = futures::executor::block_on(async move {
                    let mut writer = self.dataflow_state_handle.write().await;
                    check_quorum!(writer.as_ref());
                    let r = writer.as_mut().extend_recipe(body, false).await?;
                    self.dataflow_state_handle.commit(writer, authority).await?;
                    Ok(r)
                })?;
                return_serialized!(ret);
            }
            (Method::POST, "/install_recipe") => {
                let body: RecipeSpec = bincode::deserialize(&body)?;
                if body.require_leader_ready() {
                    require_leader_ready()?;
                }
                let ret = futures::executor::block_on(async move {
                    let mut writer = self.dataflow_state_handle.write().await;
                    check_quorum!(writer.as_ref());
                    let r = writer.as_mut().install_recipe(body).await?;
                    self.dataflow_state_handle.commit(writer, authority).await?;
                    Ok(r)
                })?;
                return_serialized!(ret);
            }
            (Method::POST, "/remove_query") => {
                require_leader_ready()?;
                let query_name = bincode::deserialize(&body)?;
                let ret = futures::executor::block_on(async move {
                    let mut writer = self.dataflow_state_handle.write().await;
                    check_quorum!(writer.as_ref());
                    let r = writer.as_mut().remove_query(query_name).await?;
                    self.dataflow_state_handle.commit(writer, authority).await?;
                    Ok(r)
                })?;
                return_serialized!(ret);
            }
            (Method::POST, "/set_schema_replication_offset") => {
                let body: Option<ReplicationOffset> = bincode::deserialize(&body)?;
                let ret = futures::executor::block_on(async move {
                    let mut writer = self.dataflow_state_handle.write().await;
                    check_quorum!(writer.as_ref());
                    writer.as_mut().set_schema_replication_offset(body);
                    self.dataflow_state_handle.commit(writer, authority).await
                })?;
                return_serialized!(ret);
            }
            (Method::POST, "/replicate_readers") => {
                let body = bincode::deserialize(&body)?;
                let ret = futures::executor::block_on(async move {
                    let mut writer = self.dataflow_state_handle.write().await;
                    check_quorum!(writer.as_ref());
                    let r = writer.as_mut().replicate_readers(body).await?;
                    self.dataflow_state_handle.commit(writer, authority).await?;
                    Ok(r)
                })?;
                return_serialized!(ret);
            }
            (Method::POST, "/remove_node") => {
                require_leader_ready()?;
                let body = bincode::deserialize(&body)?;
                let ret = futures::executor::block_on(async move {
                    let mut writer = self.dataflow_state_handle.write().await;
                    check_quorum!(writer.as_ref());
                    let r = writer.as_mut().remove_nodes(vec![body].as_slice()).await?;
                    self.dataflow_state_handle.commit(writer, authority).await?;
                    Ok(r)
                })?;
                return_serialized!(ret);
            }
            _ => Err(ReadySetError::UnknownEndpoint),
        }
    }

    pub(super) async fn handle_register_from_authority(
        &mut self,
        workers: Vec<WorkerDescriptor>,
    ) -> ReadySetResult<()> {
        let mut writer = self.dataflow_state_handle.write().await;
        let ds = writer.as_mut();

        for desc in workers {
            let WorkerDescriptor {
                worker_uri,
                reader_addr,
                region,
                reader_only,
                volume_id,
            } = desc;

            info!(%worker_uri, %reader_addr, "received registration payload from worker");

            let ws = Worker::new(
                worker_uri.clone(),
                region,
                reader_only,
                volume_id,
                self.worker_request_timeout,
            );

            let mut domain_addresses = Vec::new();
            for (index, handle) in &ds.domains {
                for i in 0..handle.shards.len() {
                    let socket_addr =
                        ds.channel_coordinator
                            .get_addr(&(*index, i))
                            .ok_or_else(|| ReadySetError::NoSuchDomain {
                                domain_index: index.index(),
                                shard: i,
                            })?;

                    domain_addresses.push(DomainDescriptor::new(*index, i, socket_addr));
                }
            }

            // We currently require that any worker that enters the system has no domains.
            // If a worker has domains we are unaware of, we may try to perform a duplicate
            // operation during a migration.
            if let Err(e) = ws.rpc::<()>(WorkerRequestKind::ClearDomains).await {
                error!(
                    %worker_uri,
                    %e,
                    "Worker could not be reached to clear its domain.",
                );
            }

            if let Err(e) = ws
                .rpc::<()>(WorkerRequestKind::GossipDomainInformation(domain_addresses))
                .await
            {
                error!(
                    %worker_uri,
                    %e,
                    "Worker could not be reached and was not updated on domain information",
                );
            }

            ds.workers.insert(worker_uri.clone(), ws);
            ds.read_addrs.insert(worker_uri, reader_addr);

            info!(
                "now have {} of {} required workers",
                ds.workers.len(),
                self.quorum
            );
        }

        if ds.workers.len() >= self.quorum && self.pending_recovery {
            self.pending_recovery = false;
            let domain_nodes = ds
                .domain_nodes
                .iter()
                .map(|(k, v)| (*k, v.values().copied().collect::<HashSet<_>>()))
                .collect::<HashMap<_, _>>();
            ds.recover(&domain_nodes).await?;
            info!("Finished restoring graph configuration");
        }

        self.dataflow_state_handle
            .commit(writer, &self.authority)
            .await
    }

    pub(super) async fn handle_failed_workers(
        &mut self,
        failed: Vec<WorkerIdentifier>,
    ) -> ReadySetResult<()> {
        let mut writer = self.dataflow_state_handle.write().await;
        let ds = writer.as_mut();

        // first, translate from the affected workers to affected data-flow nodes
        let mut affected_nodes = HashMap::new();
        for wi in failed {
            warn!(worker = ?wi, "handling failure of worker");
            let mut domain_nodes_on_worker = ds.nodes_on_worker(Some(&wi));
            for (domain_index, node_indices) in domain_nodes_on_worker.drain() {
                ds.domains.remove(&domain_index);
                ds.materializations.remove_nodes(&node_indices);
                affected_nodes
                    .entry(domain_index)
                    .or_insert_with(|| HashSet::new())
                    .extend(node_indices);
            }
            ds.workers.remove(&wi);
        }

        ds.recover(&affected_nodes).await?;

        self.dataflow_state_handle
            .commit(writer, &self.authority)
            .await
    }

    /// Construct `Leader` with a specified listening interface
    pub(super) fn new(
        state: ControllerState,
        controller_uri: Url,
        authority: Arc<Authority>,
        replicator_url: Option<String>,
        server_id: Option<u32>,
        worker_request_timeout: Duration,
        replicator_restart_timeout: Duration,
    ) -> Self {
        assert_ne!(state.config.quorum, 0);

        // TODO(fran): I feel like this is a little bit hacky. It is true that
        //   if we have more than 1 node (we know there's always going to be _at least_ 1,
        //   namely the `self.source` node) then that means we are recovering; but maybe
        //   we can be more explicit and store a `recovery` flag in the persisted
        // [`ControllerState`]   itself.
        let pending_recovery = state.dataflow_state.ingredients.node_indices().count() > 1;

        let dataflow_state_handle = DataflowStateHandle::new(state.dataflow_state);

        Leader {
            dataflow_state_handle,
            pending_recovery,

            quorum: state.config.quorum,

            controller_uri,

            replicator_url,
            replicator_task: None,
            authority,
            server_id,
            worker_request_timeout,
            replicator_restart_timeout,
        }
    }
}

/// Helper method to distinguish if the given [`ControllerRequest`] actually
/// requires modifying the dataflow graph state.
pub(super) fn request_type(req: &ControllerRequest) -> ControllerRequestType {
    match (&req.method, req.path.as_ref()) {
        (&Method::GET, "/flush_partial")
        | (&Method::GET | &Method::POST, "/controller_uri")
        | (&Method::POST, "/extend_recipe")
        | (&Method::POST, "/install_recipe")
        | (&Method::POST, "/remove_query")
        | (&Method::POST, "/set_replication_offset")
        | (&Method::POST, "/replicate_readers")
        | (&Method::POST, "/remove_node") => ControllerRequestType::Write,
        (&Method::POST, "/dry_run") => ControllerRequestType::DryRun,
        _ => ControllerRequestType::Read,
    }
}

pub(super) enum ControllerRequestType {
    Write,
    Read,
    DryRun,
}
