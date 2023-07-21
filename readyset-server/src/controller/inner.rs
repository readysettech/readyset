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

use database_utils::UpstreamConfig;
use dataflow::DomainIndex;
use failpoint_macros::failpoint;
use futures::future::Fuse;
use futures::FutureExt;
use hyper::Method;
use readyset_client::consensus::Authority;
use readyset_client::recipe::{ExtendRecipeResult, ExtendRecipeSpec, MigrationStatus};
use readyset_client::status::{ReadySetStatus, SnapshotStatus};
use readyset_client::{SingleKeyEviction, WorkerDescriptor};
use readyset_errors::{internal_err, ReadySetError, ReadySetResult};
use readyset_telemetry_reporter::TelemetrySender;
use readyset_util::futures::abort_on_panic;
use readyset_util::shutdown::ShutdownReceiver;
use readyset_version::RELEASE_VERSION;
use replication_offset::ReplicationOffset;
use reqwest::Url;
use slotmap::{DefaultKey, Key, KeyData, SlotMap};
use tokio::select;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::{watch, Mutex, Notify};
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tracing::{debug, error, info, warn};

use crate::controller::state::{DfState, DfStateHandle};
use crate::controller::{ControllerState, Worker, WorkerIdentifier};
use crate::worker::WorkerRequestKind;

/// Maximum amount of time to wait for an `extend_recipe` request to run synchronously, before we
/// let it run in the background and return [`ExtendRecipeResult::Pending`].
const EXTEND_RECIPE_MAX_SYNC_TIME: Duration = Duration::from_secs(5);

/// A handle to a migration running in the background. Used as part of
/// [`Leader::running_migrations`].
type RunningMigration = Fuse<JoinHandle<ReadySetResult<()>>>;

/// The ReadySet leader, responsible for making control-plane decisions for the whole of a ReadySet
/// cluster.
///
/// This runs inside a `Controller` when it is elected as leader.
///
/// It keeps track of the structure of the underlying data flow graph and its domains. `Controller`
/// does not allow direct manipulation of the graph. Instead, changes must be instigated through a
/// `Migration`, which can be performed using `Leader::migrate`. Only one `Migration` can
/// occur at any given point in time.
pub struct Leader {
    pub(super) dataflow_state_handle: Arc<DfStateHandle>,

    /// Number of workers to wait for before we start trying to run any domains at all
    min_workers: usize,
    controller_uri: Url,

    /// The amount of time to wait for a worker request to complete.
    worker_request_timeout: Duration,
    /// Whether to log statements received by the replicators
    replicator_statement_logging: bool,
    /// Configuration for the replicator
    pub(super) replicator_config: UpstreamConfig,
    /// A client to the current authority.
    pub(super) authority: Arc<Authority>,

    /// A map of currently running migrations.
    ///
    /// Requests to `/extend_recipe` run for at least [`EXTEND_RECIPE_MAX_SYNC_TIME`], after which
    /// a handle to the running migration is placed here, where it can be queried via an rpc to
    /// `/migration_status`.
    running_migrations: Mutex<SlotMap<DefaultKey, RunningMigration>>,

    pub(super) running_recovery: Option<watch::Receiver<ReadySetResult<()>>>,
}

impl Leader {
    /// Run all tasks required to be the leader. This may spawn tasks that
    /// may become ready asynchronously. Use the notification to indicate
    /// to the Controller that the leader is ready to handle requests.
    pub(super) async fn start(
        &mut self,
        ready_notification: Arc<Notify>,
        replication_error: UnboundedSender<ReadySetError>,
        telemetry_sender: TelemetrySender,
        shutdown_rx: ShutdownReceiver,
    ) {
        // When the controller becomes the leader, we need to read updates
        // from the binlog.
        self.start_replication_task(
            ready_notification,
            replication_error,
            telemetry_sender,
            shutdown_rx,
        )
        .await;
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
        telemetry_sender: TelemetrySender,
        mut shutdown_rx: ShutdownReceiver,
    ) {
        if self.replicator_config.upstream_db_url.is_none() {
            ready_notification.notify_one();
            info!("No primary instance specified");
            return;
        }

        let authority = Arc::clone(&self.authority);
        let replicator_restart_timeout = self.replicator_config.replicator_restart_timeout;
        let config = self.replicator_config.clone();
        let replicator_statement_logging = self.replicator_statement_logging;

        // The replication task ideally won't panic, but if it does and we arent replicating, that
        // will mean the data we return, will be more and more stale, and the transaction logs on
        // the upstream will be filling up disk
        // So, we abort on any panic of the replicator task.
        tokio::spawn(abort_on_panic(async move {
            let replication_future = async move {
                // The replicator wants to know if we're restarting the server so that it can
                // resnapshot to capture changes made to replication-tables.
                let mut server_startup = true;
                loop {
                    let noria: readyset_client::ReadySetHandle =
                        readyset_client::ReadySetHandle::new(Arc::clone(&authority)).await;

                    match replicators::NoriaAdapter::start(
                        noria,
                        config.clone(),
                        Some(ready_notification.clone()),
                        telemetry_sender.clone(),
                        server_startup,
                        replicator_statement_logging,
                    )
                    .await
                    {
                        // Unrecoverable errors, propagate the error the controller and kill the
                        // loop.
                        Err(err @ ReadySetError::RecipeInvariantViolated(_)) => {
                            if let Err(e) = replication_error.send(err) {
                                error!(error = %e, "Could not notify controller of critical error. The system may be in an invalid state");
                            }
                            break;
                        }
                        Err(error) => {
                            // On each replication error we wait for `replicator_restart_timeout`
                            // then try again
                            error!(
                                target: "replicators",
                                %error,
                                timeout_sec=replicator_restart_timeout.as_secs(),
                                "Error in replication, will retry after timeout"
                            );
                            tokio::time::sleep(replicator_restart_timeout).await;
                        }
                    }
                    server_startup = false;
                }
            };

            tokio::select! {
                _ = replication_future => {},
                _ = shutdown_rx.recv() => {},
            }
        }));
    }

    #[failpoint("controller-request")]
    #[allow(clippy::let_unit_value)]
    pub(super) async fn external_request(
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
                debug!(%method, %path, "successfully handled HTTP request");
                return Ok(::bincode::serialize(&$expr)?);
            }};
        }

        debug!(%method, %path, "received external HTTP request");

        let require_leader_ready = move || -> ReadySetResult<()> {
            if !leader_ready {
                Err(ReadySetError::LeaderNotReady)
            } else {
                Ok(())
            }
        };

        match (&method, path) {
            (&Method::GET, "/simple_graph") => {
                let ds = self.dataflow_state_handle.read().await;
                Ok(ds.graphviz(false, None).into_bytes())
            }
            (&Method::POST, "/simple_graphviz") => {
                let ds = self.dataflow_state_handle.read().await;
                return_serialized!(ds.graphviz(false, None));
            }
            (&Method::GET, "/graph") => {
                let ds = self.dataflow_state_handle.read().await;
                let node_sizes = ds.node_sizes().await?;
                Ok(ds.graphviz(true, Some(node_sizes)).into_bytes())
            }
            (&Method::POST, "/graphviz") => {
                let ds = self.dataflow_state_handle.read().await;
                let node_sizes = ds.node_sizes().await?;
                return_serialized!(ds.graphviz(true, Some(node_sizes)));
            }
            (&Method::GET | &Method::POST, "/get_statistics") => {
                let ds = self.dataflow_state_handle.read().await;
                return_serialized!(ds.get_statistics().await);
            }
            (&Method::GET | &Method::POST, "/instances") => {
                let ds = self.dataflow_state_handle.read().await;
                return_serialized!(ds.get_instances());
            }
            (&Method::GET | &Method::POST, "/controller_uri") => {
                return_serialized!(self.controller_uri);
            }
            (&Method::GET, "/workers") | (&Method::POST, "/workers") => {
                let ds = self.dataflow_state_handle.read().await;
                return_serialized!(&ds.workers.keys().collect::<Vec<_>>())
            }
            (&Method::GET, "/healthy_workers") | (&Method::POST, "/healthy_workers") => {
                let ds = self.dataflow_state_handle.read().await;
                return_serialized!(&ds
                    .workers
                    .iter()
                    .filter(|w| w.1.healthy)
                    .map(|w| w.0)
                    .collect::<Vec<_>>());
            }
            (&Method::GET | &Method::POST, "/domains") => {
                let ds = self.dataflow_state_handle.read().await;
                let res: HashMap<DomainIndex, Vec<Vec<Option<WorkerIdentifier>>>> = ds
                    .domains
                    .iter()
                    .map(|(di, dh)| (*di, dh.shards().map(|wis| wis.to_vec()).collect::<Vec<_>>()))
                    .collect();
                return_serialized!(res)
            }
            (&Method::GET, "/allocated_bytes") => {
                let alloc_bytes = tikv_jemalloc_ctl::epoch::mib()
                    .and_then(|m| m.advance())
                    .and_then(|_| tikv_jemalloc_ctl::stats::allocated::mib())
                    .and_then(|m| m.read())
                    .ok();
                return_serialized!(alloc_bytes);
            }
            (&Method::POST, "/set_memory_limit") => {
                let (period, limit) = bincode::deserialize(&body)?;
                let res: Result<(), ReadySetError> = {
                    let ds = self.dataflow_state_handle.read().await;
                    for (_, worker) in ds.workers.iter() {
                        worker
                            .rpc::<()>(WorkerRequestKind::SetMemoryLimit { period, limit })
                            .await?;
                    }
                    Ok(())
                };
                return_serialized!(res);
            }
            (&Method::GET | &Method::POST, "/version") => {
                return_serialized!(RELEASE_VERSION);
            }
            (&Method::POST, "/tables") => {
                let ds = self.dataflow_state_handle.read().await;
                return_serialized!(ds.tables())
            }
            (&Method::POST, "/table_statuses") => {
                let res = {
                    let ds = self.dataflow_state_handle.read().await;
                    ds.table_statuses().await
                };
                return_serialized!(res)
            }
            (&Method::POST, "/non_replicated_relations") => {
                let ds = self.dataflow_state_handle.read().await;
                return_serialized!(ds.non_replicated_relations())
            }
            (&Method::POST, "/views") => {
                let ds = self.dataflow_state_handle.read().await;
                return_serialized!(ds.views())
            }
            (&Method::POST, "/verbose_views") => {
                let ds = self.dataflow_state_handle.read().await;
                return_serialized!(ds.verbose_views())
            }
            (&Method::POST, "/view_statuses") => {
                let (queries, dialect) = bincode::deserialize(&body)?;
                let ds = self.dataflow_state_handle.read().await;
                return_serialized!(ds.view_statuses(queries, dialect))
            }
            (&Method::GET, "/nodes") => {
                let ds = self.dataflow_state_handle.read().await;
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
                // because the receiving end expects a `ReadySetResult` to be serialized.
                let body = bincode::deserialize(&body)?;
                let ds = self.dataflow_state_handle.read().await;
                let ret = ds.table_builder(&body);
                return_serialized!(ret);
            }
            (&Method::POST, "/table_builder_by_index") => {
                // NOTE(eta): there is DELIBERATELY no `?` after the `table_builder` call,
                // because the receiving end expects a `ReadySetResult` to be serialized.
                let body = bincode::deserialize(&body)?;
                let ds = self.dataflow_state_handle.read().await;
                let ret = ds.table_builder_by_index(body);
                return_serialized!(ret);
            }
            (&Method::POST, "/view_builder") => {
                // NOTE(eta): same as above applies
                require_leader_ready()?;
                let body = bincode::deserialize(&body)?;
                let ds = self.dataflow_state_handle.read().await;
                let ret = ds.view_builder(body);
                return_serialized!(ret);
            }
            (&Method::POST, "/get_info") => {
                let ds = self.dataflow_state_handle.read().await;
                return_serialized!(ds.get_info()?)
            }
            (&Method::POST, "/replication_offsets") => {
                let res = {
                    let ds = self.dataflow_state_handle.read().await;
                    ds.replication_offsets().await
                }?;
                return_serialized!(res);
            }
            (&Method::POST, "/snapshotting_tables") => {
                let res = {
                    let ds = self.dataflow_state_handle.read().await;
                    ds.snapshotting_tables().await
                }?;
                return_serialized!(res);
            }
            (&Method::POST, "/all_tables_compacted") => {
                let res = {
                    let ds = self.dataflow_state_handle.read().await;
                    ds.all_tables_compacted().await
                }?;
                return_serialized!(res);
            }
            (&Method::POST, "/node_sizes") => {
                let res = {
                    let ds = self.dataflow_state_handle.read().await;
                    ds.node_sizes().await
                }?;
                return_serialized!(res);
            }
            (&Method::POST, "/leader_ready") => {
                return_serialized!(leader_ready);
            }
            (&Method::POST, "/status") => {
                let ds = self.dataflow_state_handle.read().await;
                let replication_offsets = if ds.all_replicas_placed() {
                    Some(ds.replication_offsets().await?)
                } else {
                    None
                };

                let status = ReadySetStatus {
                    // Use whether the leader is ready or not as a proxy for if we have
                    // completed snapshotting.
                    snapshot_status: if leader_ready {
                        SnapshotStatus::Completed
                    } else {
                        SnapshotStatus::InProgress
                    },

                    max_replication_offset: replication_offsets
                        .as_ref()
                        .and_then(|offs| offs.max_offset().ok().flatten().cloned()),
                    min_replication_offset: replication_offsets
                        .as_ref()
                        .and_then(|offs| offs.min_present_offset().ok().flatten().cloned()),
                };
                return_serialized!(status);
            }
            (&Method::POST, "/dry_run") => {
                let body: ExtendRecipeSpec = bincode::deserialize(&body)?;
                if body.require_leader_ready {
                    require_leader_ready()?;
                }
                let mut state_copy: DfState = {
                    let reader = self.dataflow_state_handle.read().await;
                    reader.clone()
                };
                state_copy.extend_recipe(body, true).await?;
                return_serialized!(ExtendRecipeResult::Done);
            }
            (&Method::GET | &Method::POST, "/supports_pagination") => {
                let ds = self.dataflow_state_handle.read().await;
                let supports =
                    ds.recipe.mir_config().allow_paginate && ds.recipe.mir_config().allow_topk;
                return_serialized!(supports)
            }
            (&Method::POST, "/evict_single") => {
                let body: Option<SingleKeyEviction> = bincode::deserialize(&body)?;
                let ds = self.dataflow_state_handle.read().await;
                let key = ds.evict_single(body).await?;
                return_serialized!(key);
            }

            (&Method::GET, "/flush_partial") => {
                let ret = {
                    let mut writer = self.dataflow_state_handle.write().await;
                    let r = writer.as_mut().flush_partial().await?;
                    self.dataflow_state_handle.commit(writer, authority).await?;
                    r
                };
                return_serialized!(ret);
            }
            (&Method::POST, "/extend_recipe") => {
                let body: ExtendRecipeSpec = bincode::deserialize(&body)?;
                if body.require_leader_ready {
                    require_leader_ready()?;
                }
                let ret = {
                    // Start the migration running in the background
                    let dataflow_state_handle = Arc::clone(&self.dataflow_state_handle);
                    let authority = Arc::clone(authority);
                    let mut migration = tokio::spawn(async move {
                        let mut writer = dataflow_state_handle.write().await;
                        writer.as_mut().extend_recipe(body, false).await?;
                        dataflow_state_handle.commit(writer, &authority).await?;
                        Ok(())
                    })
                    .fuse();

                    // Either the migration completes synchronously (under
                    // EXTEND_RECIPE_MAX_SYNC_TIME), or we place it in `self.running_migrations` and
                    // return a `Pending` result.
                    select! {
                        res = &mut migration => {
                            res.map_err(|e| internal_err!("{e}"))?.map(|_| ExtendRecipeResult::Done)
                        }
                        _ = sleep(EXTEND_RECIPE_MAX_SYNC_TIME) => {
                            let mut running_migrations = self.running_migrations.lock().await;
                            let migration_id = running_migrations.insert(migration);
                            Ok(ExtendRecipeResult::Pending(migration_id.data().as_ffi()))
                        }
                    }
                }?;
                return_serialized!(ret);
            }
            (&Method::POST, "/migration_status") => {
                let migration_id: u64 = bincode::deserialize(&body)?;
                let migration_key = DefaultKey::from(KeyData::from_ffi(migration_id));
                let ret = {
                    let mut running_migrations = self.running_migrations.lock().await;
                    let mut migration: &mut RunningMigration = running_migrations
                        .get_mut(migration_key)
                        .ok_or_else(|| ReadySetError::UnknownMigration(migration_id))?;

                    match (&mut migration).now_or_never() {
                        None => ReadySetResult::Ok(MigrationStatus::Pending),
                        Some(res) => {
                            // Migration is done, remove it from the map
                            // Note that this means that only one thread can poll on the status of a
                            // particular migration!
                            running_migrations.remove(migration_key);
                            res.map_err(|e| internal_err!("{e}"))??;
                            Ok(MigrationStatus::Done)
                        }
                    }
                }?;
                return_serialized!(ret)
            }
            (&Method::POST, "/remove_query") => {
                require_leader_ready()?;
                let query_name = bincode::deserialize(&body)?;
                let mut writer = self.dataflow_state_handle.write().await;
                writer.as_mut().remove_query(&query_name).await?;
                self.dataflow_state_handle.commit(writer, authority).await?;
                return_serialized!(());
            }
            (&Method::POST, "/remove_all_queries") => {
                require_leader_ready()?;
                let mut writer = self.dataflow_state_handle.write().await;
                writer.as_mut().remove_all_queries().await?;
                self.dataflow_state_handle.commit(writer, authority).await?;
                return_serialized!(ReadySetResult::Ok(()));
            }
            (&Method::POST, "/set_schema_replication_offset") => {
                let body: Option<ReplicationOffset> = bincode::deserialize(&body)?;
                let mut writer = self.dataflow_state_handle.write().await;
                writer.as_mut().set_schema_replication_offset(body);
                self.dataflow_state_handle.commit(writer, authority).await?;
                return_serialized!(ReadySetResult::Ok(()));
            }
            (&Method::POST, "/remove_node") => {
                require_leader_ready()?;
                let body = bincode::deserialize(&body)?;
                let mut writer = self.dataflow_state_handle.write().await;
                writer.as_mut().remove_nodes(vec![body].as_slice()).await?;
                self.dataflow_state_handle.commit(writer, authority).await?;
                return_serialized!(());
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
                domain_scheduling_config,
                ..
            } = desc;

            info!(%worker_uri, %reader_addr, "received registration payload from worker");

            let ws = Worker::new(
                worker_uri.clone(),
                domain_scheduling_config,
                self.worker_request_timeout,
            );
            let domain_addresses = ds.domain_addresses();

            // Clean up any potential stale domains that may have been running on that worker
            if let Err(e) = ws.rpc::<()>(WorkerRequestKind::ClearDomains).await {
                error!(
                    %worker_uri,
                    %e,
                    "Worker could not be reached to clear its domain.",
                );
            }

            // Then, tell the worker about the addresses of all the other domains within the cluster
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
                self.min_workers
            );
        }

        let dmp = if ds.workers.len() >= self.min_workers && !ds.all_replicas_placed() {
            let domain_nodes = ds.unplaced_domain_nodes();
            debug!(
                num_unplaced_domains = domain_nodes.len(),
                "Attempting to place unplaced domains"
            );
            let dmp = ds.plan_recovery(&domain_nodes).await?;

            if dmp.failed_placement().is_empty() {
                info!("Finished planning recovery with all domains placed");
            } else {
                info!(
                    num_unplaced_domains = dmp.failed_placement().len(),
                    "Finished planning recovery with some domains unplaced"
                );
            }
            Some(dmp)
        } else {
            None
        };

        self.dataflow_state_handle
            .commit(writer, &self.authority)
            .await?;

        if let Some(dmp) = dmp {
            let dataflow_state_handle = Arc::clone(&self.dataflow_state_handle);
            let authority = Arc::clone(&self.authority);
            let (tx, rx) = watch::channel(Ok(()));
            let apply_handle = tokio::spawn(async move {
                debug!("Waiting for dataflow state write handle to apply dmp");
                let mut writer = dataflow_state_handle.write().await;
                if let Err(error) = dmp.apply(writer.as_mut()).await {
                    error!(%error, "Error applying domain migration plan");
                    Err(error)
                } else {
                    dataflow_state_handle.commit(writer, &authority).await?;
                    Ok(())
                }
            });
            tokio::spawn(abort_on_panic(async move {
                let res = apply_handle
                    .await
                    .map_err(|e| internal_err!("Error during recovery: {e}"))
                    .flatten();
                debug!(?res, "Recovery finished");
                let _ = tx.send_replace(res);
            }));
            self.running_recovery = Some(rx);
        }

        Ok(())
    }

    pub(super) async fn handle_failed_workers(
        &mut self,
        failed: Vec<WorkerIdentifier>,
    ) -> ReadySetResult<()> {
        let mut writer = self.dataflow_state_handle.write().await;
        let ds = writer.as_mut();

        // Remove references to the worker from any internal state, collecting downstream domains to
        // kill
        let mut downstream_domains = HashSet::new();
        for wi in failed {
            warn!(worker = %wi, "handling failure of worker");
            for di in ds.remove_worker(&wi) {
                downstream_domains.extend(ds.downstream_domains(di.domain_index)?);
            }
        }

        if !downstream_domains.is_empty() {
            info!(
                num_downstream_domains = downstream_domains.len(),
                "Killing domains downstream of failed worker"
            );
            ds.kill_domains(downstream_domains).await?;
        }

        self.dataflow_state_handle
            .commit(writer, &self.authority)
            .await
    }

    /// Construct a new `Leader`
    pub(super) fn new(
        state: ControllerState,
        controller_uri: Url,
        authority: Arc<Authority>,
        replicator_statement_logging: bool,
        replicator_config: UpstreamConfig,
        worker_request_timeout: Duration,
    ) -> Self {
        assert_ne!(state.config.min_workers, 0);

        let dataflow_state_handle = Arc::new(DfStateHandle::new(state.dataflow_state));

        Leader {
            dataflow_state_handle,
            min_workers: state.config.min_workers,

            controller_uri,

            replicator_statement_logging,
            replicator_config,
            authority,
            worker_request_timeout,
            running_migrations: Default::default(),
            running_recovery: None,
        }
    }
}
