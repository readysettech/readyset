use crate::controller::inner::ControllerInner;
use crate::controller::migrate::Migration;
use crate::controller::recipe::Recipe;
use crate::coordination::CoordinationMessage;
use crate::coordination::CoordinationPayload;
use crate::startup::Event;
use crate::Config;
use async_bincode::AsyncBincodeReader;
use dataflow::payload::ControlReplyPacket;
use futures::sync::mpsc::UnboundedSender;
use futures::{self, Future, Sink, Stream};
use hyper::{self, StatusCode};
use noria::channel::TcpSender;
use noria::consensus::{Authority, Epoch, STATE_KEY};
use noria::ControllerDescriptor;
use serde_json;
use slog;
use std::net::SocketAddr;
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time;
use stream_cancel::Valve;
use tokio;

mod domain_handle;
mod inner;
mod keys;
crate mod migrate; // crate viz for tests
mod mir_to_flow;
crate mod recipe; // crate viz for tests
mod schema;
mod security;
crate mod sql; // crate viz for tests

#[derive(Clone, Serialize, Deserialize)]
crate struct ControllerState {
    crate config: Config,
    crate epoch: Epoch,

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

pub(super) fn main<A: Authority + 'static>(
    valve: &Valve,
    config: Config,
    descriptor: ControllerDescriptor,
    ctrl_rx: futures::sync::mpsc::UnboundedReceiver<Event>,
    cport: tokio::net::tcp::TcpListener,
    log: slog::Logger,
    authority: Arc<A>,
    tx: futures::sync::mpsc::UnboundedSender<Event>,
) -> impl Future<Item = (), Error = ()> {
    let (dtx, drx) = futures::sync::mpsc::unbounded();

    tokio::spawn(listen_domain_replies(valve, log.clone(), dtx, cport));

    // note that we do not start up the data-flow until we find a controller!

    let campaign = instance_campaign(tx.clone(), authority.clone(), descriptor, config);

    let log = log;
    let authority = authority.clone();

    let log2 = log.clone();
    let authority2 = authority.clone();

    // state that this instance will take if it becomes the controller
    let mut campaign = Some(campaign);
    let mut drx = Some(drx);
    ctrl_rx
        .map_err(|_| unreachable!())
        .fold(None, move |mut controller: Option<ControllerInner>, e| {
            match e {
                Event::InternalMessage(msg) => match msg.payload {
                    CoordinationPayload::Deregister => {
                        unimplemented!();
                    }
                    CoordinationPayload::CreateUniverse(universe) => {
                        if let Some(ref mut ctrl) = controller {
                            crate::block_on(|| ctrl.create_universe(universe).unwrap());
                        }
                    }
                    CoordinationPayload::Register {
                        ref addr,
                        ref read_listen_addr,
                        ..
                    } => {
                        if let Some(ref mut ctrl) = controller {
                            crate::block_on(|| {
                                ctrl.handle_register(&msg, addr, read_listen_addr.clone())
                                    .unwrap()
                            });
                        }
                    }
                    CoordinationPayload::Heartbeat => {
                        if let Some(ref mut ctrl) = controller {
                            crate::block_on(|| ctrl.handle_heartbeat(&msg).unwrap());
                        }
                    }
                    _ => unreachable!(),
                },
                Event::ExternalRequest(method, path, query, body, reply_tx) => {
                    if let Some(ref mut ctrl) = controller {
                        let authority = &authority;
                        let reply = crate::block_on(|| {
                            ctrl.external_request(method, path, query, body, &authority)
                        });

                        if reply_tx.send(reply).is_err() {
                            warn!(log, "client hung up");
                        }
                    } else if reply_tx.send(Err(StatusCode::NOT_FOUND)).is_err() {
                        warn!(log, "client hung up for 404");
                    }
                }
                #[cfg(test)]
                Event::ManualMigration { f, done } => {
                    if let Some(ref mut ctrl) = controller {
                        if !ctrl.workers.is_empty() {
                            crate::block_on(|| {
                                ctrl.migrate(move |m| f.call_box((m,)));
                                done.send(()).unwrap();
                            });
                        }
                    } else {
                        unreachable!("got migration closure before becoming leader");
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
                    crate::block_on(move || c.join().unwrap());
                    let drx = drx.take().unwrap();
                    controller = Some(ControllerInner::new(log.clone(), state.clone(), drx));
                }
                Event::CampaignError(e) => {
                    panic!("{:?}", e);
                }
                e => unreachable!("{:?} is not a controller event", e),
            }
            Ok(controller)
        })
        .and_then(move |controller| {
            // shutting down
            if controller.is_some() {
                if let Err(e) = authority2.surrender_leadership() {
                    error!(log2, "failed to surrender leadership");
                    eprintln!("{:?}", e);
                }
            }
            Ok(())
        })
        .map_err(|e| panic!("{:?}", e))
}

fn listen_domain_replies(
    valve: &Valve,
    log: slog::Logger,
    reply_tx: UnboundedSender<ControlReplyPacket>,
    on: tokio::net::TcpListener,
) -> impl Future<Item = (), Error = ()> {
    let valve = valve.clone();
    valve
        .wrap(on.incoming())
        .map_err(failure::Error::from)
        .for_each(move |sock| {
            tokio::spawn(
                valve
                    .wrap(AsyncBincodeReader::from(sock))
                    .map_err(failure::Error::from)
                    .forward(
                        reply_tx
                            .clone()
                            .sink_map_err(|_| format_err!("main event loop went away")),
                    )
                    .map(|_| ())
                    .map_err(|e| panic!("{:?}", e)),
            );
            Ok(())
        })
        .map_err(move |e| {
            warn!(log, "domain reply connection failed: {:?}", e);
        })
}

fn instance_campaign<A: Authority + 'static>(
    event_tx: UnboundedSender<Event>,
    authority: Arc<A>,
    descriptor: ControllerDescriptor,
    config: Config,
) -> JoinHandle<()> {
    let descriptor_bytes = serde_json::to_vec(&descriptor).unwrap();
    let campaign_inner = move |mut event_tx: UnboundedSender<Event>| -> Result<(), failure::Error> {
        let payload_to_event = |payload: Vec<u8>| -> Result<Event, failure::Error> {
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
                event_tx = event_tx
                    .send(payload_to_event(leader.1)?)
                    .wait()
                    .map_err(|_| format_err!("send failed"))?;
                while let Some(leader) = authority.await_new_epoch(epoch)? {
                    epoch = leader.0;
                    event_tx = event_tx
                        .send(payload_to_event(leader.1)?)
                        .wait()
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
                        if state.config != config {
                            panic!("Config in Zk does not match requested config!")
                        }
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
            break event_tx
                .send(Event::WonLeaderElection(state.clone().unwrap()))
                .and_then(|event_tx| {
                    event_tx.send(Event::LeaderChange(state.unwrap(), descriptor.clone()))
                })
                .wait()
                .map(|_| ())
                .map_err(|_| format_err!("send failed"));
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
