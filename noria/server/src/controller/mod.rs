use crate::controller::inner::ControllerInner;
use crate::controller::migrate::Migration;
use crate::controller::recipe::Recipe;
use crate::coordination::CoordinationMessage;
use crate::coordination::CoordinationPayload;
use crate::metrics::MetricsDump;
use crate::startup::Event;
use crate::NoriaMetricsRecorder;
use crate::{Config, ReadySetResult};
use async_bincode::AsyncBincodeReader;
use dataflow::payload::ControlReplyPacket;
use futures_util::{
    future::FutureExt,
    future::TryFutureExt,
    sink::SinkExt,
    stream::{StreamExt, TryStreamExt},
};
use hyper::{self, Method, StatusCode};
use noria::channel::TcpSender;
use noria::consensus::{Authority, Epoch, STATE_KEY};
use noria::metrics::recorded;
use noria::ControllerDescriptor;
use noria::{internal, ReadySetError};
use std::net::SocketAddr;
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time;
use stream_cancel::Valve;
use tokio::sync::mpsc::UnboundedSender;

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

struct Worker {
    healthy: bool,
    last_heartbeat: time::Instant,
    sender: TcpSender<CoordinationMessage>,
}

impl Worker {
    fn new(sender: TcpSender<CoordinationMessage>) -> Self {
        Worker {
            healthy: true,
            last_heartbeat: time::Instant::now(),
            sender,
        }
    }
}

type WorkerIdentifier = SocketAddr;

/// External requests that can be handled without a controller at this
/// noria-server.
fn external_request(
    method: hyper::Method,
    path: String,
) -> Result<Result<String, ReadySetError>, StatusCode> {
    use serde_json as json;

    match (&method, path.as_ref()) {
        (&Method::POST, "/metrics_dump") => {
            let recorder = NoriaMetricsRecorder::get();
            let (counters, gauges, histograms) =
                recorder.with_metrics(|c, g, h| (c.clone(), g.clone(), h.clone()));
            let md = MetricsDump::from_metrics(counters, gauges, histograms);
            Ok(Ok(json::to_string(&md).unwrap()))
        }
        (&Method::POST, "/reset_metrics") => {
            let recorder = NoriaMetricsRecorder::get();
            recorder.clear();
            // Callers convert this to a 200 response to the client.
            Ok(Ok("".to_string()))
        }
        _ => Err(StatusCode::NOT_FOUND),
    }
}

pub(super) async fn main<A: Authority + 'static>(
    alive: tokio::sync::mpsc::Sender<()>,
    valve: Valve,
    config: Config,
    descriptor: ControllerDescriptor,
    mut ctrl_rx: tokio::sync::mpsc::UnboundedReceiver<Event>,
    cport: tokio::net::TcpListener,
    log: slog::Logger,
    authority: Arc<A>,
    tx: tokio::sync::mpsc::UnboundedSender<Event>,
) -> Result<(), ReadySetError> {
    let (dtx, drx) = tokio::sync::mpsc::unbounded_channel();

    tokio::spawn(listen_domain_replies(
        alive.clone(),
        valve.clone(),
        log.clone(),
        dtx,
        cport,
    ));

    // note that we do not start up the data-flow until we find a controller!

    let campaign = instance_campaign(tx.clone(), authority.clone(), descriptor, config);

    // state that this instance will take if it becomes the controller
    let mut campaign = Some(campaign);
    let mut drx = Some(drx);

    let mut controller: Option<ControllerInner> = None;
    while let Some(e) = ctrl_rx.next().await {
        match e {
            Event::InternalMessage(msg) => match msg.payload {
                CoordinationPayload::Deregister => {
                    internal!();
                }
                CoordinationPayload::CreateUniverse(universe) => {
                    if let Some(ref mut ctrl) = controller {
                        tokio::task::block_in_place(|| ctrl.create_universe(universe).unwrap());
                    }
                }
                CoordinationPayload::Register { .. } => {
                    if let Some(ref mut ctrl) = controller {
                        tokio::task::block_in_place(|| {
                            if let Err(e) = ctrl.handle_register(msg) {
                                warn!(log, "worker registered and then immediately left: {:?}", e);
                            }
                        });
                    }
                }
                CoordinationPayload::Heartbeat => {
                    if let Some(ref mut ctrl) = controller {
                        tokio::task::block_in_place(|| ctrl.handle_heartbeat(msg).unwrap());
                    }
                }
                _ => internal!(),
            },
            Event::ExternalRequest(method, path, query, body, reply_tx) => {
                if let Some(ref mut ctrl) = controller {
                    let authority = &authority;
                    let reply = tokio::task::block_in_place(|| {
                        let resp = ctrl.external_request(
                            method.clone(),
                            path.clone(),
                            query,
                            body,
                            &authority,
                        );
                        match resp {
                            Ok(r) => Ok(r.map_err(|e| serde_json::to_string(&e).unwrap())),
                            Err(s) => {
                                // If the request is not supported with a controller, check if we
                                // can use the external request path that does not use a
                                // controller.
                                if let StatusCode::NOT_FOUND = s {
                                    external_request(method, path)
                                        .map(|r| r.map_err(|e| serde_json::to_string(&e).unwrap()))
                                } else {
                                    Err(s)
                                }
                            }
                        }
                    });

                    if reply_tx.send(reply).is_err() {
                        warn!(log, "client hung up");
                    }
                } else {
                    // There is no controller, however, we may still support an
                    // external request to this noria-server. If the external
                    // request is not supported, NOT_FOUND will be returned.
                    metrics::increment_counter!(recorded::SERVER_EXTERNAL_REQUESTS);
                    let reply = tokio::task::block_in_place(|| {
                        external_request(method, path)
                            .map(|r| r.map_err(|e| serde_json::to_string(&e).unwrap()))
                    });
                    if reply_tx.send(reply).is_err() {
                        warn!(log, "client hung up");
                    }
                }
            }
            Event::ManualMigration { f, done } => {
                if let Some(ref mut ctrl) = controller {
                    if !ctrl.workers.is_empty() {
                        tokio::task::block_in_place(|| -> ReadySetResult<()> {
                            ctrl.migrate(move |m| f(m))??;
                            done.send(()).unwrap();
                            Ok(())
                        })?
                    }
                } else {
                    internal!("got migration closure before becoming leader");
                }
            }
            #[cfg(test)]
            Event::IsReady(reply) => {
                reply
                    .send(
                        controller
                            .as_ref()
                            .map(|ctrl| !ctrl.workers.is_empty())
                            .unwrap_or(false),
                    )
                    .unwrap();
            }
            Event::WonLeaderElection(state) => {
                let c = campaign.take().unwrap();
                tokio::task::block_in_place(move || c.join().unwrap());
                let drx = drx.take().unwrap();
                controller = Some(ControllerInner::new(log.clone(), state, drx));
            }
            Event::CampaignError(e) => {
                internal!("{:?}", e);
            }
            e => internal!("{:?} is not a controller event", e),
        }
    }

    // shutting down
    if controller.is_some() {
        if let Err(e) = authority.surrender_leadership() {
            error!(log, "failed to surrender leadership");
            internal!("failed to surrender leadership: {}", e)
        }
    }

    Ok(())
}

async fn listen_domain_replies(
    alive: tokio::sync::mpsc::Sender<()>,
    valve: Valve,
    log: slog::Logger,
    reply_tx: UnboundedSender<ControlReplyPacket>,
    mut on: tokio::net::TcpListener,
) {
    let mut incoming = valve.wrap(on.incoming());
    while let Some(sock) = incoming.next().await {
        match sock {
            Err(e) => {
                warn!(log, "domain reply connection failed: {:?}", e);
                break;
            }
            Ok(sock) => {
                let alive = alive.clone();
                tokio::spawn(
                    valve
                        .wrap(AsyncBincodeReader::from(sock))
                        .map_err(anyhow::Error::from)
                        .forward(
                            crate::ImplSinkForSender(reply_tx.clone())
                                .sink_map_err(|_| format_err!("main event loop went away")),
                        )
                        .map_err(|e| panic!("{:?}", e))
                        .map(move |_| {
                            let _ = alive;
                            ()
                        }),
                );
            }
        }
    }
}

fn instance_campaign<A: Authority + 'static>(
    event_tx: UnboundedSender<Event>,
    authority: Arc<A>,
    descriptor: ControllerDescriptor,
    config: Config,
) -> JoinHandle<()> {
    let descriptor_bytes = serde_json::to_vec(&descriptor).unwrap();
    let campaign_inner = move |event_tx: UnboundedSender<Event>| -> Result<(), anyhow::Error> {
        let payload_to_event = |payload: Vec<u8>| -> Result<Event, anyhow::Error> {
            let descriptor: ControllerDescriptor = serde_json::from_slice(&payload[..])?;
            let state: ControllerState =
                serde_json::from_slice(&authority.try_read(STATE_KEY).unwrap().unwrap())?;
            Ok(Event::LeaderChange(state, descriptor))
        };

        loop {
            // WORKER STATE - watch for leadership changes
            //
            // If there is currently a leader, then loop until there is a period without a
            // leader, notifying the main thread every time a leader change occurs.
            let mut epoch;
            if let Some(leader) = authority.try_get_leader()? {
                epoch = leader.0;
                event_tx
                    .send(payload_to_event(leader.1)?)
                    .map_err(|_| format_err!("send failed"))?;
                while let Some(leader) = authority.await_new_epoch(epoch)? {
                    epoch = leader.0;
                    event_tx
                        .send(payload_to_event(leader.1)?)
                        .map_err(|_| format_err!("send failed"))?;
                }
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
            event_tx
                .send(Event::WonLeaderElection(state.clone().unwrap()))
                .map_err(|_| format_err!("failed to announce who won leader election"))?;
            event_tx
                .send(Event::LeaderChange(state.unwrap(), descriptor.clone()))
                .map_err(|_| format_err!("failed to announce leader change"))?;
            break Ok(());
        }
    };

    thread::Builder::new()
        .name("srv-zk".to_owned())
        .spawn(move || {
            if let Err(e) = campaign_inner(event_tx.clone()) {
                let _ = event_tx.send(Event::CampaignError(e));
            }
        })
        .unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::integration::start_simple;
    use std::error::Error;

    #[tokio::test(threaded_scheduler)]
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
