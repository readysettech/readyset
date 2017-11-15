use std;
use std::io;
use std::net::{self, SocketAddr};
use std::time::Duration;
use std::collections::HashMap;
use std::sync::{mpsc, Arc, Mutex, TryLockError};

use slab::Slab;
use vec_map::VecMap;
use mio::{self, Events, Poll, PollOpt, Ready, Token};
use mio::net::{TcpListener, TcpStream};

use channel::{TcpReceiver, TcpSender};
use dataflow::Packet;

pub struct NewReplica {
    inner: Replica,
    inputs: Vec<TcpStream>,
    outputs: Vec<(ReplicaIndex, net::TcpStream)>,
}

pub struct WorkerDelta {
    replicas: Vec<NewReplica>,
    inputs: Vec<(ReplicaIndex, TcpStream)>,
}

type Replica = ();
type ReplicaIndex = ();
type EnqueuedSend = (ReplicaIndex, Box<Packet>);

struct ReplicaContext {
    /// A map from socket token to `TcpReceiver`.
    receivers: VecMap<TcpReceiver<Box<Packet>>>,
    replica: Replica,
    outputs: HashMap<ReplicaIndex, TcpSender<Packet>>,
}

#[derive(Clone)]
struct WorkerDeltaInner {
    /// New replicas that should be added to the replica Slab.
    ///
    /// Note that these *must* be added in order so that the replica tokens are correctly mapped.
    new_replicas: Vec<(ReplicaIndex, Arc<Mutex<ReplicaContext>>)>,

    /// New socket tokens, and the replica token they map to.
    new_socket_tokens: Vec<usize>,

    ack: mpsc::SyncSender<()>,
}

struct SharedWorkerState {
    /// A mapping from socket token to replica token.
    sockets: Slab<usize>,
    /// A mapping from replica index token to the context associated with that replica.
    replicas: Slab<Arc<Mutex<ReplicaContext>>>,
    /// A mapping from replica index to replica token.
    revmap: HashMap<ReplicaIndex, usize>,
}

// can be replaced with derive(Default) with next release of Slab
impl Default for SharedWorkerState {
    fn default() -> Self {
        SharedWorkerState {
            sockets: Slab::new(),
            replicas: Slab::new(),
            revmap: Default::default(),
        }
    }
}

pub struct WorkerPool {
    workers: Vec<Worker>,
    poll: Arc<Poll>,
    notify: Vec<mpsc::SyncSender<WorkerDeltaInner>>,
    wstate: SharedWorkerState,
}

impl WorkerPool {
    pub fn new(n: usize) -> io::Result<Self> {
        let poll = Arc::new(Poll::new()?);
        let (notify_tx, notify_rx): (_, Vec<_>) = (0..n).map(|_| mpsc::sync_channel(1)).unzip();
        let workers = notify_rx
            .into_iter()
            .map(|notify| {
                Worker {
                    shared: Default::default(),
                    all: poll.clone(),
                    notify,
                }
            })
            .collect();

        Ok(WorkerPool {
            workers,
            poll,
            notify: notify_tx,
            wstate: Default::default(),
        })
    }

    pub fn run(mut self, more: mpsc::Receiver<WorkerDelta>) {
        use std::thread;
        let jhs: Vec<_> = self.workers
            .into_iter()
            .map(|worker| thread::spawn(move || worker.run()))
            .collect();

        for delta in more {
            let (ack_tx, ack_rx) = mpsc::sync_channel(self.notify.len());
            let mut notify = WorkerDeltaInner {
                new_replicas: Default::default(),
                new_socket_tokens: Default::default(),
                ack: ack_tx,
            };

            let mut poll_more = VecMap::new();
            for replica in delta.replicas {
                let ri = replica.inner.id();

                let mut rc = ReplicaContext {
                    receivers: VecMap::new(),
                    replica: replica.inner,
                    outputs: HashMap::new(),
                };

                // Hook up outputs
                for (target, tx) in replica.outputs {
                    rc.outputs.insert(target, TcpSender::new(tx, None).unwrap());
                }

                // Prepare for workers
                let r = Arc::new(Mutex::new(rc));
                let rit = self.wstate.replicas.insert(r.clone());
                self.wstate.revmap.insert(ri, rit);

                // Keep track of new sockets to listen on (but don't listen yet!)
                {
                    let mut rxs = Vec::with_capacity(replica.inputs.len());
                    let mut r = r.lock().unwrap();
                    for rx in replica.inputs {
                        let token = self.wstate.sockets.insert(rit);
                        notify.new_socket_tokens.push(rit);
                        rxs.push((token, rx));
                    }
                    poll_more.insert(rit, rxs);
                }

                notify.new_replicas.push((ri, r));
            }

            for (ri, rx) in delta.inputs {
                let rit = self.wstate.revmap[&ri];
                let token = self.wstate.sockets.insert(rit);
                notify.new_socket_tokens.push(rit);
                poll_more
                    .entry(rit)
                    .or_insert_with(Vec::default)
                    .push((token, rx))
            }

            // Notify all workers about new replicas and sockets
            for tx in &mut self.notify {
                tx.send(notify.clone());
            }
            // Wait for all senders to ack (drop) WorkerDeltaInner
            ack_rx.recv().is_err();

            // Update Poll so workers start getting events for new connections
            for (rit, rxs) in poll_more {
                let r = self.wstate.replicas.get_mut(rit).unwrap();
                let mut r = r.lock().unwrap();
                for (token, rx) in rxs {
                    self.poll
                        .register(
                            &rx,
                            Token(token),
                            Ready::readable(),
                            PollOpt::edge() | PollOpt::oneshot(),
                        )
                        .unwrap();
                    let tcp = TcpReceiver::new(rx);
                    r.receivers.insert(token, tcp);
                }
            }
        }

        for jh in jhs {
            jh.join().unwrap();
        }
    }
}

pub struct Worker {
    shared: SharedWorkerState,
    all: Arc<Poll>,
    notify: mpsc::Receiver<WorkerDeltaInner>,
}

impl Worker {
    pub fn run(mut self) -> ! {
        // we want to only process a single domain, otherwise we might hold on to work that another
        // worker could be doing. and that'd be silly.
        let mut events = Events::with_capacity(1);

        loop {
            if let Ok(delta) = self.notify.try_recv() {
                for (ri, r) in delta.new_replicas {
                    let rit = self.shared.replicas.insert(r);
                    self.shared.revmap.insert(ri, rit);
                }
                for rit in delta.new_socket_tokens {
                    // NOTE: main thread is responsible for registering token with Poll
                    self.shared.sockets.insert(rit);
                }
                // ack to main thread
                drop(delta.ack);
            }

            self.all.poll(&mut events, None).expect("OS poll failed");
            let context = events
                .get(0)
                .map(|e| e.token())
                .map(|Token(token)| {
                    (
                        token,
                        *self.shared
                            .sockets
                            .get(token)
                            .expect("got event for unknown token"),
                    )
                })
                .map(|(token, rit)| {
                    (
                        token,
                        self.shared
                            .replicas
                            .get(rit)
                            .expect("token resolves to unknown replica"),
                    )
                })
                .and_then(|(token, domain)| match domain.try_lock() {
                    Ok(d) => Some((token, d)),
                    Err(TryLockError::WouldBlock) => None,
                    Err(TryLockError::Poisoned(e)) => panic!("found poisoned lock: {}", e),
                });

            let mut next = None;
            if let Some((token, mut context)) = context {
                let ReplicaContext {
                    ref mut outputs,
                    ref mut replica,
                    ref mut receivers,
                    ..
                } = *context;

                // we're responsible for running the given domain, and we have its lock
                let channel = receivers
                    .get_mut(token)
                    .expect("target replica does not have receiver for registered token");

                let mut packet = channel.try_recv().ok();
                while let Some(m) = packet.take() {
                    let output = self.process(replica, m);

                    // if there is at least one more *complete* Packet in `channel` then we should
                    // send everything in `output` and process the next Packet. however, if there
                    // are no complete Packets in `channel`, we should instead look for a local
                    // destination in `output`, send the others, and then process that packet
                    // immediately to save some syscalls and a trip through the network stack..
                    for (ri, m) in output {
                        if channel.is_empty() && next.is_none() && self.is_local(&ri) {
                            next = Some((ri, m));
                            continue;
                        }

                        // deliver this packet to its destination TCP queue
                        // XXX: unwrap
                        outputs.get_mut(&ri).unwrap().send_ref(&m).unwrap();
                    }

                    if !channel.is_empty() {
                        assert!(next.is_none());
                        packet = channel.try_recv().ok();
                    }
                }

                // we have a packet we can handle directly!
                // NOTE: this deadlocks *hard* if you have A-B-A
                while let Some((ri, m)) = next.take() {
                    let rit = self.shared.revmap[&ri];
                    let rc = self.shared
                        .replicas
                        .get(rit)
                        .expect("packet is for unknown replica");
                    match rc.try_lock() {
                        Ok(mut context) => {
                            // no other thread is operating on the target domain, so we can do it
                            let output = self.process(replica, m);
                            for (ri, m) in output {
                                if next.is_none() && self.is_local(&ri) {
                                    next = Some((ri, m));
                                    continue;
                                }

                                // deliver this packet to its destination TCP queue
                                // XXX: unwrap
                                // NOTE: uses shadowed ri
                                outputs.get_mut(&ri).unwrap().send_ref(&m).unwrap();
                            }
                        }
                        Err(e) => {
                            // we couldn't get the lock, so we push it on the TCP queue for later
                            // XXX: unwrap
                            outputs.get_mut(&ri).unwrap().send_ref(&m).unwrap();
                        }
                    }
                }
            }
        }
    }

    fn is_local(&self, ri: &ReplicaIndex) -> bool {
        // TODO
        // false will never give *incorrect* behavior, just lower performance, so no rush
        false
    }

    fn process(&self, domain: &mut Replica, packet: Box<Packet>) -> Vec<EnqueuedSend> {
        unimplemented!()
    }
}
