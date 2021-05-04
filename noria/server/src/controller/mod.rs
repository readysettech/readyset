use crate::controller::inner::ControllerInner;
use crate::controller::migrate::Migration;
use crate::controller::recipe::Recipe;
use crate::coordination::do_noria_rpc;
use crate::errors::internal_err;
use crate::worker::{WorkerRequest, WorkerRequestKind};
use crate::{Config, ReadySetResult};
use futures_util::StreamExt;
use hyper::{self, Method, StatusCode};
use noria::consensus::{Authority, Epoch, STATE_KEY};
use noria::ControllerDescriptor;
use noria::{internal, ReadySetError};
use serde::de::DeserializeOwned;
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time;
use stream_cancel::Valve;
use tokio::sync::mpsc::{Receiver, Sender};
use url::Url;

mod domain_handle;
mod inner;
mod keys;
pub(crate) mod migrate; // crate viz for tests
mod mir_to_flow;
pub(crate) mod recipe; // crate viz for tests
mod schema;
mod security;
pub(crate) mod sql; // crate viz for tests

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct ControllerState {
    pub(crate) config: Config,
    pub(crate) epoch: Epoch,

    recipe_version: usize,
    recipes: Vec<String>,
}

pub struct Worker {
    healthy: bool,
    last_heartbeat: time::Instant,
    uri: Url,
    http: reqwest::Client,
    region: Option<String>,
}

impl Worker {
    pub fn new(instance_uri: Url, region: Option<String>) -> Self {
        Worker {
            healthy: true,
            last_heartbeat: time::Instant::now(),
            uri: instance_uri,
            http: reqwest::Client::new(),
            region,
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

/// An update on the controller's ongoing election campaign.
pub(crate) enum CampaignUpdate {
    /// The current leader has changed.
    ///
    /// The King is dead; long live the King!
    LeaderChange(ControllerState, ControllerDescriptor),
    /// We are now the new leader.
    WonLeaderElection(ControllerState),
    /// An error occurred in the leadership election process, which won't be restarted.
    CampaignError(anyhow::Error),
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
pub struct ControllerOuter<A> {
    /// `slog` logging thingie.
    pub(crate) log: slog::Logger,
    /// If we're the leader, the actual controller.
    pub(crate) inner: Option<ControllerInner>,
    /// The `Authority` structure used for leadership elections & such state.
    pub(crate) authority: Arc<A>,
    /// Channel to the `Worker` running inside this server instance.
    ///
    /// This is used to convey changes in leadership state.
    pub(crate) worker_tx: Sender<WorkerRequest>,
    /// Receives updates from the election campaign thread.
    pub(crate) campaign_rx: Receiver<CampaignUpdate>,
    /// Receives external HTTP requests.
    pub(crate) http_rx: Receiver<ControllerRequest>,
    /// Receives requests from the controller's `Handle`.
    pub(crate) handle_rx: Receiver<HandleRequest>,
    /// A `ControllerDescriptor` that describes this server instance.
    pub(crate) our_descriptor: ControllerDescriptor,
    /// Valve for shutting down; triggered by the `Handle` when `Handle::shutdown()` is called.
    pub(crate) valve: Valve,
}
impl<A> ControllerOuter<A>
where
    A: Authority + 'static,
{
    /// Run the provided *blocking* closure with the `ControllerInner` and the `Authority` if this
    /// server instance is currently the leader.
    ///
    /// If it isn't, returns `Err(ReadySetError::NotLeader)`, and doesn't run the closure.
    async fn with_controller_blocking<F, T>(&mut self, func: F) -> ReadySetResult<T>
    where
        F: FnOnce(&mut ControllerInner, Arc<A>) -> ReadySetResult<T>,
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
                // ok ok ok
                Ok(Ok(Ok(r))) => Ok(Ok(r)),
                Ok(Ok(Err(e))) => Ok(Err(bincode::serialize(&e)?)),
                // HACK(eta): clients retry on 503, but not on `NotLeader`, so fudge the gap here.
                Err(ReadySetError::NotLeader) => Err(StatusCode::SERVICE_UNAVAILABLE),
                Err(e) => Ok(Err(bincode::serialize(&e)?)),
                Ok(Err(s)) => Err(s),
            }
        };

        if reply_tx.send(ret).is_err() {
            warn!(self.log, "client hung up");
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
                    warn!(self.log, "readiness query sender hung up!");
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
                    warn!(self.log, "handle-based migration sender hung up!");
                }
            }
        }
        Ok(())
    }

    async fn handle_campaign_update(&mut self, msg: CampaignUpdate) -> ReadySetResult<()> {
        match msg {
            CampaignUpdate::LeaderChange(state, descr) => {
                self.send_worker_request(WorkerRequestKind::NewController {
                    controller_uri: descr.controller_uri,
                    heartbeat_every: state.config.heartbeat_every,
                    epoch: state.epoch,
                })
                .await?;
            }
            CampaignUpdate::WonLeaderElection(state) => {
                info!(self.log, "won leader election, creating ControllerInner");

                self.inner = Some(ControllerInner::new(self.log.clone(), state.clone()));
                self.send_worker_request(WorkerRequestKind::NewController {
                    controller_uri: self.our_descriptor.controller_uri.clone(),
                    heartbeat_every: state.config.heartbeat_every,
                    epoch: state.epoch,
                })
                .await?;
            }
            CampaignUpdate::CampaignError(e) => {
                // the campaign can't be restarted, so the controller should hard-exit
                internal!("controller's leadership campaign failed: {}", e);
            }
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

            tokio::select! {
                req = self.handle_rx.recv() => {
                    if let Some(req) = req {
                        self.handle_handle_request(req).await?;
                    }
                    else {
                        info!(self.log, "ControllerOuter shutting down after request handle dropped");
                        break;
                    }
                }
                req = self.http_rx.recv() => {
                    if let Some(req) = req {
                        self.handle_controller_request(req).await?;
                    }
                    else {
                        info!(self.log, "ControllerOuter shutting down after HTTP handle dropped");
                        break;
                    }
                }
                // note: the campaign sender gets closed when we become the leader, so don't
                // try to receive from it any more
                req = self.campaign_rx.recv(), if self.inner.is_none() => {
                    if let Some(req) = req {
                        self.handle_campaign_update(req).await?;
                    }
                    else {
                        if self.inner.is_some() {
                            info!(self.log, "leadership campaign terminated normally");
                        }
                        else {
                            // this shouldn't ever happen: if the leadership campaign thread fails,
                            // it should send a `CampaignError` that we handle above first before
                            // ever hitting this bit
                            // still, good to be doubly sure
                            internal!("leadership sender dropped without being elected");
                        }
                    }
                }
                _ = shutdown_stream.next() => {
                    info!(self.log, "ControllerOuter shutting down after valve shut");
                    break;
                }
            }
        }
        if self.inner.is_some() {
            if let Err(e) = self.authority.surrender_leadership() {
                error!(self.log, "failed to surrender leadership");
                internal!("failed to surrender leadership: {}", e)
            }
        }
        Ok(())
    }
}

pub(crate) fn instance_campaign<A: Authority + 'static>(
    event_tx: Sender<CampaignUpdate>,
    authority: Arc<A>,
    descriptor: ControllerDescriptor,
    config: Config,
    rt_handle: tokio::runtime::Handle,
) -> JoinHandle<()> {
    let descriptor_bytes = serde_json::to_vec(&descriptor).unwrap();
    let campaign_inner = move |event_tx: Sender<CampaignUpdate>| -> Result<(), anyhow::Error> {
        let payload_to_event = |payload: Vec<u8>| -> Result<CampaignUpdate, anyhow::Error> {
            let descriptor: ControllerDescriptor = serde_json::from_slice(&payload[..])?;
            let state: ControllerState =
                serde_json::from_slice(&authority.try_read(STATE_KEY).unwrap().unwrap())?;
            Ok(CampaignUpdate::LeaderChange(state, descriptor))
        };

        loop {
            // WORKER STATE - watch for leadership changes
            //
            // If there is currently a leader, then loop until there is a period without a
            // leader, notifying the main thread every time a leader change occurs.
            let mut epoch;
            if let Some(leader) = authority.try_get_leader()? {
                epoch = leader.0;
                rt_handle.block_on(async {
                    event_tx
                        .send(payload_to_event(leader.1)?)
                        .await
                        .map_err(|_| format_err!("send failed"))?;
                    while let Some(leader) =
                        tokio::task::block_in_place(|| authority.await_new_epoch(epoch))?
                    {
                        epoch = leader.0;
                        event_tx
                            .send(payload_to_event(leader.1)?)
                            .await
                            .map_err(|_| format_err!("send failed"))?;
                    }
                    Ok::<(), anyhow::Error>(())
                })?;
            }

            // ELECTION STATE - attempt to become leader
            //
            // Becoming leader requires creating an ephemeral key and then doing an atomic
            // update to another.
            let epoch = match authority.become_leader(descriptor_bytes.clone())? {
                Some(epoch) => epoch,
                None => continue,
            };
            let state = authority.read_modify_write(
                STATE_KEY,
                |state: Option<ControllerState>| match state {
                    None => Ok(ControllerState {
                        config: config.clone(),
                        epoch,
                        recipe_version: 0,
                        recipes: vec![],
                    }),
                    Some(ref state) if state.epoch > epoch => Err(()),
                    Some(mut state) => {
                        state.epoch = epoch;
                        // check that running config is the same that builder requested
                        assert_eq!(
                            state.config, config,
                            "Config in Zk does not match requested config!"
                        );
                        Ok(state)
                    }
                },
            )?;
            if state.is_err() {
                continue;
            }

            // LEADER STATE - manage system
            //
            // It is not currently possible to safely handle involuntary loss of leadership status
            // (and there is nothing that can currently trigger it), so don't bother watching for
            // it.
            rt_handle.block_on(async move {
                event_tx
                    .send(CampaignUpdate::WonLeaderElection(state.clone().unwrap()))
                    .await
                    .map_err(|_| format_err!("failed to announce who won leader election"))?;
                event_tx
                    .send(CampaignUpdate::LeaderChange(
                        state.unwrap(),
                        descriptor.clone(),
                    ))
                    .await
                    .map_err(|_| format_err!("failed to announce leader change"))?;
                Ok::<(), anyhow::Error>(())
            })?;
            break Ok(());
        }
    };

    thread::Builder::new()
        .name("srv-zk".to_owned())
        .spawn(move || {
            if let Err(e) = campaign_inner(event_tx.clone()) {
                let _ = event_tx.send(CampaignUpdate::CampaignError(e));
            }
        })
        .unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::integration::start_simple;
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
}
