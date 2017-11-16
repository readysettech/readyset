use std;
use std::io;
use std::thread;
use std::net::{self, SocketAddr, ToSocketAddrs};
use std::time::{Duration, Instant};
use std::collections::HashMap;
use std::sync::{mpsc, Arc, Mutex, TryLockError};

use slab::Slab;
use vec_map::VecMap;
use mio::{self, Events, Poll, PollOpt, Ready, Token};
use mio::unix::EventedFd;
use mio::net::{TcpListener, TcpStream};
use slog::Logger;

use channel::{TcpReceiver, TcpSender};
use channel::poll::{PollEvent, ProcessResult};
use channel::tcp::TryRecvError;
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
    log: Logger,
    checktable_addr: SocketAddr,
}

impl WorkerPool {
    pub fn new<A: ToSocketAddrs>(n: usize, log: &Logger, checktable_addr: A) -> io::Result<Self> {
        let checktable_addr = checktable_addr.to_socket_addrs().unwrap().next().unwrap();

        let poll = Arc::new(Poll::new()?);
        let (notify_tx, notify_rx): (_, Vec<_>) = (0..n).map(|_| mpsc::channel()).unzip();
        let truth = Arc::new(Mutex::new(Slab::new()));
        let workers = notify_rx
            .into_iter()
            .enumerate()
            .map(|(i, notify)| {
                let w = Worker {
                    shared: SharedWorkerState::new(truth.clone()),
                    log: log.new(o!("worker" => i)),
                    all: poll.clone(),
                    notify,
                };
                let checktable_addr = checktable_addr.clone();
                thread::Builder::new()
                    .name(format!("worker{}", i + 1))
                    .spawn(move || w.run(checktable_addr))
                    .unwrap()
            })
            .collect();

        let mut pool = WorkerPool {
            workers,
            poll,
            notify: notify_tx,
            wstate: SharedWorkerState::new(truth),
            checktable_addr,
            log: log.new(o!()),
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
        debug!(self.log, "new replica added"; "rit" => rit);
        self.wstate.revmap.insert(ri, rit);

        // Keep track of new socket to listen on
        use std::os::unix::io::AsRawFd;
        let token = self.wstate.truth.lock().unwrap().insert(SocketContext {
            rit,
            fd: listener.as_raw_fd(),
            listening: true,
        });
        debug!(self.log, "new replica listener added"; "token" => token);

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
    log: Logger,
    notify: mpsc::Receiver<NewReplicaInternal>,
}

impl Worker {
    pub fn run(mut self, checktable_addr: SocketAddr) {
        // we want to only process a single domain, otherwise we might hold on to work that another
        // worker could be doing. and that'd be silly.
        let mut events = Events::with_capacity(1);
        let mut timers = VecMap::<Instant>::new();
        let mut durtmp = None;
        let mut force_new_truth = false;

        dataflow::connect_thread_checktable(checktable_addr);

        loop {
            // have any timers expired?
            let now = Instant::now();
            let mut next = None;
            for (rit, rc) in &self.shared.replicas {
                use vec_map::Entry;
                if let Entry::Occupied(mut timeout) = timers.entry(rit) {
                    if timeout.get() <= &now {
                        timeout.remove();

                        // NOTE: try_lock is okay, because if another worker is handling it, the
                        // timeout is also being dealt with.
                        if let Ok(mut context) = rc.try_lock() {
                            match context.replica.on_event(PollEvent::Timeout) {
                                ProcessResult::KeepPolling => {
                                    // FIXME: this could return a bunch of things to be sent
                                }
                                ProcessResult::StopPolling => unreachable!(),
                            }
                        }
                    } else {
                        let dur = timeout.get().duration_since(now);
                        if dur.as_secs() != 0 && next.is_none() {
                            // ensure we wake up in the next second
                            next = Some(Duration::new(1, 0));
                            continue;
                        }

                        if let Some(ref mut next) = next {
                            use std::cmp::min;
                            *next = min(*next, dur);
                        } else {
                            next = Some(dur);
                        }
                    }
                }
            }

            if let Err(e) = self.all.poll(&mut events, next) {
                if e.kind() == io::ErrorKind::Interrupted {
                    // spurious wakeup
                    continue;
                } else if e.kind() == io::ErrorKind::TimedOut {
                    // need to re-check timers
                    // *should* be handled by mio and return Ok() with no events
                    continue;
                } else {
                    panic!("{}", e);
                }
            }

            if events.is_empty() {
                // we must have timed out -- check timers
                continue;
            }

            let token = events
                .get(0)
                .map(|e| {
                    assert!(e.readiness().is_readable());
                    e.token()
                })
                .map(|Token(token)| token)
                .unwrap();
            trace!(self.log, "worker polled"; "token" => token);

            if !self.shared.sockets.contains(token) || force_new_truth {
                // unknown socket -- we need to update our cached state
                {
                    let truth = self.shared.truth.lock().unwrap();
                    self.shared.sockets = (*truth).clone();
                }
                debug!(self.log, "worker updated their notion of truth");

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
                assert!(self.shared.sockets.contains(token));
                force_new_truth = false;
            }

            let sc = self.shared
                .sockets
                .get(token)
                .expect("got event for unknown token");
            let replica = self.shared
                .replicas
                .get(sc.rit)
                .expect("token resolves to unknown replica");
            trace!(self.log, "worker handling replica event"; "replica" => sc.rit);

            let all = &self.all;
            let ready = || {
                all.reregister(
                    &EventedFd(&sc.fd),
                    Token(token),
                    Ready::readable(),
                    PollOpt::level() | PollOpt::oneshot(),
                )
            };

            if sc.listening {
                // listening socket -- we need to accept a new connection
                let mut replica = replica.lock().unwrap();
                if let Ok((stream, _src)) = replica.listener.as_mut().unwrap().accept() {
                    // NOTE: we're taking two locks at the same time here! we need to be sure that
                    // anything that holds the `truth` lock will eventually relinquish it.
                    //
                    //   a) add_replica immediately relinquishes
                    //   b) cloning truth (above) immediately relinquishes
                    //   c) on disconnect, we take locks in same order, and immediately release
                    //   d) we immediately relinquish
                    //
                    // so we should be safe from deadlocks
                    use std::os::unix::io::AsRawFd;
                    let token = self.shared.truth.lock().unwrap().insert(SocketContext {
                        rit: sc.rit,
                        fd: stream.as_raw_fd(),
                        listening: false,
                    });
                    debug!(self.log, "worker accepted new connection"; "token" => token);

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

                ready();
                continue;
            }

            let mut context = match replica.try_lock() {
                Ok(r) => r,
                Err(TryLockError::WouldBlock) => {
                    // this can happen with oneshot if there is more than one input to a
                    // replica, and both receive data. not entirely clear what the best thing
                    // to do here is. we can either block on the lock, or release the socket
                    // again (which will cause another epoll to be woken up since we're
                    // level-triggered).
                    ready();
                    continue;
                }
                Err(TryLockError::Poisoned(e)) => panic!("found poisoned lock: {}", e),
            };

            // track if we can handle a local output directly
            let mut next = None;

            // unless the connection is dropped, we want to rearm the socket
            let mut rearm = true;

            {
                // deref guard explicitly so that we can field borrows are tracked separately. if we
                // didn't do this, the first thing that borrows from context would borrow *all* of
                // context.
                let context = &mut *context;

                // we're responsible for running the given domain, and we have its lock
                use vec_map::Entry;
                let mut channel = match context.receivers.entry(token) {
                    Entry::Vacant(_) => {
                        // this means our `truth` is out of date and a token has been reused. let's
                        // update our truth, and try again. we could add lots of complicated log to
                        // do that right here, but let's just retry instead:
                        force_new_truth = true;
                        ready();
                        continue;
                    }
                    Entry::Occupied(mut e) => e,
                };

                loop {
                    let mut m = match channel.get_mut().try_recv() {
                        Ok(p) => p,
                        Err(TryRecvError::Empty) => break,
                        Err(TryRecvError::Disconnected) => {
                            // how can this even happen?
                            // mutator that goes away maybe?
                            // in any case, we can unregister the socket.
                            debug!(self.log, "worker dropped lost connection"; "token" => token);
                            let rx = channel.remove();

                            // no deadlock for same reason as adding a socket to poll
                            self.shared.truth.lock().unwrap().remove(token);

                            // dropping rx will automatically deregister the socket (in theory)
                            //
                            // see
                            // https://github.com/carllerche/mio/issues/351#issuecomment-183746183
                            // and
                            // https://docs.rs/mio/0.6.11/mio/struct.Poll.html#method.deregister
                            //
                            // however, there is no harm in explicitly deregistering it as well
                            self.all.deregister(rx.get_ref()).is_err();
                            rearm = false;
                            drop(rx);

                            // we might have processed some packets before we got Disconnected, and
                            // maybe even a `next`, so we can't just continue the outer polling
                            // loop here unfortunately.
                            break;
                        }
                        Err(TryRecvError::DeserializationError(e)) => panic!("{}", e),
                    };

                    let output = match self.process(&mut context.replica, m) {
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
                        if channel.get().is_empty() && next.is_none() && self.is_local(&ri) {
                            next = Some((ri, m));
                            continue;
                        }

                        // deliver this packet to its destination TCP queue
                        // XXX: unwrap
                        context.outputs.get_mut(&ri).unwrap().send_ref(&m).unwrap();
                    }

                    if channel.get().is_empty() {
                        break;
                    }
                }

                // Register timeout for replica
                context
                    .replica
                    .on_event(PollEvent::ResumePolling(&mut durtmp));
                if let Some(timeout) = durtmp.take() {
                    timers.insert(sc.rit, Instant::now() + timeout);
                } else {
                    timers.remove(sc.rit);
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
                            let output = match self.process(&mut context.replica, m) {
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
                                context.outputs.get_mut(&ri).unwrap().send_ref(&m).unwrap();
                            }

                            // Register timeout for replica
                            context
                                .replica
                                .on_event(PollEvent::ResumePolling(&mut durtmp));
                            if let Some(timeout) = durtmp.take() {
                                timers.insert(sc.rit, Instant::now() + timeout);
                            } else {
                                timers.remove(sc.rit);
                            }
                        }
                        Err(e) => {
                            // we couldn't get the lock, so we push it on the TCP queue for later
                            // XXX: unwrap
                            context.outputs.get_mut(&ri).unwrap().send_ref(&m).unwrap();
                        }
                    }
                }
            }

            drop(context);

            if rearm {
                // we've released the replica, so it's safe to let other workers handle events
                ready();
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
                Some(vec![])
            }
            ProcessResult::StopPolling => None,
        }
    }
}
