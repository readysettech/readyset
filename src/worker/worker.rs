use std;
use std::io;
use std::thread;
use std::net::{self, SocketAddr};
use std::time::Duration;
use std::collections::HashMap;
use std::sync::{mpsc, Arc, Mutex, TryLockError};

use ferris::CopyWheel as TimerWheel;
use ferris::{Resolution, Wheel};
use slab::Slab;
use vec_map::VecMap;
use mio::{self, Events, Poll, PollOpt, Ready, Token};
use mio::unix::EventedFd;
use mio::net::{TcpListener, TcpStream};

use channel::{TcpReceiver, TcpSender};
use channel::poll::{PollEvent, ProcessResult};
use dataflow::{self, Domain, Packet};

pub struct NewReplica {
    pub inner: Replica,
    pub listener: TcpListener,
    pub outputs: Vec<(ReplicaIndex, net::TcpStream)>,
}

type Replica = Domain;
type ReplicaIndex = (dataflow::Index, usize);
type EnqueuedSend = (ReplicaIndex, Box<Packet>);

struct ReplicaContext {
    /// A map from socket token to `TcpReceiver`.
    receivers: VecMap<TcpReceiver<Box<Packet>>>,
    listener: Option<TcpListener>,
    replica: Replica,
    outputs: HashMap<ReplicaIndex, TcpSender<Packet>>,
}

#[derive(Clone)]
struct NewReplicaInternal {
    ri: ReplicaIndex,
    rc: Arc<Mutex<ReplicaContext>>,
    rit: usize,
}

use std::os::unix::io::RawFd;
#[derive(Clone)]
struct SocketContext {
    rit: usize,
    listening: bool,
    fd: RawFd,
}

struct SharedWorkerState {
    /// A mapping from socket token to replica token.
    sockets: Slab<SocketContext>,
    /// A mapping from replica index token to the context associated with that replica.
    replicas: Slab<Arc<Mutex<ReplicaContext>>>,
    /// A mapping from replica index to replica token.
    revmap: HashMap<ReplicaIndex, usize>,
    /// The "true" mapping from socket token to replica token.
    ///
    /// This is lazily pulled in by workers when they miss in `sockets`.
    truth: Arc<Mutex<Slab<SocketContext>>>,
}

impl SharedWorkerState {
    fn new(truth: Arc<Mutex<Slab<SocketContext>>>) -> Self {
        SharedWorkerState {
            sockets: Slab::new(),
            replicas: Slab::new(),
            revmap: Default::default(),
            truth,
        }
    }
}

pub struct WorkerPool {
    workers: Vec<thread::JoinHandle<()>>,
    poll: Arc<Poll>,
    notify: Vec<mpsc::Sender<NewReplicaInternal>>,
    wstate: SharedWorkerState,
    checktable_addr: SocketAddr,
}

impl WorkerPool {
    pub fn new(n: usize, controller_addr: &str) -> io::Result<Self> {
        use std::net::ToSocketAddrs;
        let mut checktable_addr = controller_addr.to_socket_addrs().unwrap().next().unwrap();
        checktable_addr.set_port(8500);

        let poll = Arc::new(Poll::new()?);
        let (notify_tx, notify_rx): (_, Vec<_>) = (0..n).map(|_| mpsc::channel()).unzip();
        let truth = Arc::new(Mutex::new(Slab::new()));
        let workers = notify_rx
            .into_iter()
            .map(|notify| {
                let w = Worker {
                    shared: SharedWorkerState::new(truth.clone()),
                    all: poll.clone(),
                    notify,
                };
                let checktable_addr = checktable_addr.clone();
                thread::spawn(move || w.run(checktable_addr))
            })
            .collect();

        let mut pool = WorkerPool {
            workers,
            poll,
            notify: notify_tx,
            wstate: SharedWorkerState::new(truth),
            checktable_addr,
        };
        Ok(pool)
    }

    pub fn add_replica(&mut self, replica: NewReplica) {
        let NewReplica {
            inner,
            listener,
            outputs,
        } = replica;
        let ri = inner.id();
        let addr = listener.local_addr().unwrap();

        let mut rc = ReplicaContext {
            receivers: VecMap::new(),
            replica: inner,
            outputs: HashMap::new(),
            listener: None,
        };

        // Hook up outputs
        for (target, tx) in outputs {
            rc.outputs.insert(target, TcpSender::new(tx, None).unwrap());
        }

        // Prepare for workers
        let rc = Arc::new(Mutex::new(rc));
        let rit = self.wstate.replicas.insert(rc.clone());
        self.wstate.revmap.insert(ri, rit);

        // Keep track of new socket to listen on
        use std::os::unix::io::AsRawFd;
        let token = self.wstate.truth.lock().unwrap().insert(SocketContext {
            rit,
            fd: listener.as_raw_fd(),
            listening: true,
        });

        // Notify all workers about new replicas and sockets
        let notify = NewReplicaInternal { ri, rc, rit };
        for tx in &mut self.notify {
            tx.send(notify.clone());
        }

        // Update Poll so workers start notifications about connections to the new replica
        let r = self.wstate.replicas.get_mut(rit).unwrap();
        let mut r = r.lock().unwrap();
        self.poll
            .register(
                &listener,
                Token(token),
                Ready::readable(),
                PollOpt::level() | PollOpt::oneshot(),
            )
            .unwrap();
        r.listener = Some(listener);
        r.replica.booted(addr);
    }

    pub fn wait(&mut self) {
        self.notify.clear();
        for jh in self.workers.drain(..) {
            jh.join().unwrap();
        }
    }
}

pub struct Worker {
    shared: SharedWorkerState,
    all: Arc<Poll>,
    notify: mpsc::Receiver<NewReplicaInternal>,
}

impl Worker {
    pub fn run(mut self, checktable_addr: SocketAddr) {
        // we want to only process a single domain, otherwise we might hold on to work that another
        // worker could be doing. and that'd be silly.
        let mut events = Events::with_capacity(1);
        let mut timers = TimerWheel::new(vec![Resolution::Ms, Resolution::TenMs]);
        let mut durtmp = None;

        dataflow::connect_thread_checktable(checktable_addr);

        loop {
            for rit in timers.expire() {
                // NOTE: try_lock is okay, because if another worker is handling it, the timeout is
                // also being dealt with.
                if let Ok(mut context) = self.shared.replicas[rit].try_lock() {
                    match context.replica.on_event(PollEvent::Timeout) {
                        ProcessResult::KeepPolling => {
                            // FIXME: this could return a bunch of things to be sent
                        }
                        ProcessResult::StopPolling => unreachable!(),
                    }
                }
            }

            self.all.poll(&mut events, None).expect("OS poll failed");
            let token = events
                .get(0)
                .map(|e| e.token())
                .map(|Token(token)| token)
                .unwrap();

            if !self.shared.sockets.contains(token) {
                // unknown socket -- we need to update our cached state
                {
                    let truth = self.shared.truth.lock().unwrap();
                    self.shared.sockets = (*truth).clone();
                }

                // also learn about new replicas while we're at it
                while let Ok(added) = self.notify.try_recv() {
                    // register replica
                    let rit = self.shared.replicas.insert(added.rc);

                    // register
                    assert_eq!(rit, added.rit);
                    self.shared.revmap.insert(added.ri, rit);
                }

                // if this is a listening socket, we know the main thread doesn't register it with
                // the Poll until it holds the lock, so for us to receive this event, and then get
                // the lock, the main thread *must* have added it.
                //
                // if this is a read socket,
                assert!(self.shared.sockets.contains(token));
            }

            let sc = self.shared
                .sockets
                .get(token)
                .expect("got event for unknown token");
            let replica = self.shared
                .replicas
                .get(sc.rit)
                .expect("token resolves to unknown replica");

            if sc.listening {
                // listening socket -- we need to accept a new connection
                let mut replica = replica.lock().unwrap();
                if let Ok((stream, _src)) = replica.listener.as_mut().unwrap().accept() {
                    // NOTE: we're taking two locks at the same time here! we need to be sure that
                    // anything that holds the `truth` lock will eventually relinquish it.
                    //
                    //   a) add_replica immediately relinquishes
                    //   b) cloning truth (above) immediately relinquishes
                    //   c) we immediately relinquish
                    //
                    // so we should be safe from deadlocks
                    use std::os::unix::io::AsRawFd;
                    let token = self.shared.truth.lock().unwrap().insert(SocketContext {
                        rit: sc.rit,
                        fd: stream.as_raw_fd(),
                        listening: false,
                    });

                    self.all
                        .register(
                            &stream,
                            Token(token),
                            Ready::readable(),
                            PollOpt::level() | PollOpt::oneshot(),
                        )
                        .unwrap();
                    let tcp = TcpReceiver::new(stream);
                    replica.receivers.insert(token, tcp);
                }

                continue;
            }

            let context = match replica.try_lock() {
                Ok(r) => Some(r),
                Err(TryLockError::WouldBlock) => {
                    // this can happen with oneshot if there is more than one input to a
                    // replica, and both receive data. not entirely clear what the best thing
                    // to do here is. we can either block on the lock, or release the socket
                    // again (which will cause another epoll to be woken up since we're
                    // level-triggered).
                    self.all.reregister(
                        &EventedFd(&sc.fd),
                        Token(token),
                        Ready::readable(),
                        PollOpt::level() | PollOpt::oneshot(),
                    );
                    None
                }
                Err(TryLockError::Poisoned(e)) => panic!("found poisoned lock: {}", e),
            };

            let mut next = None;
            if let Some(mut context) = context {
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
                    let output = match self.process(replica, m) {
                        Some(o) => o,
                        None => {
                            // told to exit?
                            return;
                        }
                    };

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

                // Register timeout for replica
                replica.on_event(PollEvent::ResumePolling(&mut durtmp));
                if let Some(timeout) = durtmp.take() {
                    use time;
                    // https://github.com/andrewjstone/ferris/issues/2
                    timers.start(sc.rit, time::Duration::from_std(timeout).unwrap());
                } else {
                    timers.stop(sc.rit);
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
                            let output = match self.process(replica, m) {
                                Some(o) => o,
                                None => {
                                    // told to exit?
                                    return;
                                }
                            };

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

                            // Register timeout for replica
                            context
                                .replica
                                .on_event(PollEvent::ResumePolling(&mut durtmp));
                            if let Some(timeout) = durtmp.take() {
                                use time;
                                // https://github.com/andrewjstone/ferris/issues/2
                                timers.start(rit, time::Duration::from_std(timeout).unwrap());
                            } else {
                                timers.stop(rit);
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

    fn process(&self, domain: &mut Replica, packet: Box<Packet>) -> Option<Vec<EnqueuedSend>> {
        match domain.on_event(PollEvent::Process(packet)) {
            ProcessResult::KeepPolling => {
                // FIXME: in theory this should return a bunch of things to be sent
                None
            }
            ProcessResult::StopPolling => None,
        }
    }
}
