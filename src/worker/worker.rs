use std;
use std::io;
use std::thread;
use std::net::{self, SocketAddr, ToSocketAddrs};
use std::time::{Duration, Instant};
use std::collections::HashMap;
use std::sync::{mpsc, Arc, Mutex, TryLockError};
use std::os::unix::io::AsRawFd;

use fnv::FnvHashMap;
use slab::Slab;
use vec_map::VecMap;
use mio::{self, Evented, Events, Poll, PollOpt, Ready, Token};
use mio::unix::EventedFd;
use mio::net::{TcpListener, TcpStream};
use slog::Logger;

use channel::{self, TcpReceiver, TcpSender};
use channel::poll::{PollEvent, ProcessResult};
use channel::tcp::{SendError, TryRecvError};
use dataflow::{self, Domain, Packet};

pub struct NewReplica {
    pub inner: Replica,
    pub listener: TcpListener,
}

type Replica = Domain;
type ReplicaIndex = (dataflow::Index, usize);
type EnqueuedSend = (ReplicaIndex, Box<Packet>);
type ChannelCoordinator = channel::ChannelCoordinator<ReplicaIndex>;

struct ReplicaContext {
    /// A map from socket token to `TcpReceiver`.
    receivers: VecMap<TcpReceiver<Box<Packet>>>,
    listener: Option<TcpListener>,
    replica: Replica,
    outputs: FnvHashMap<ReplicaIndex, (TcpSender<Packet>, bool)>,
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
    revmap: FnvHashMap<ReplicaIndex, usize>,
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
    pub fn new<A: ToSocketAddrs>(
        n: usize,
        log: &Logger,
        checktable_addr: A,
        channel_coordinator: Arc<ChannelCoordinator>,
    ) -> io::Result<Self> {
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
                    channel_coordinator: channel_coordinator.clone(),
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
        let NewReplica { inner, listener } = replica;
        let ri = inner.id();
        let addr = listener.local_addr().unwrap();

        let mut rc = ReplicaContext {
            receivers: VecMap::new(),
            replica: inner,
            outputs: Default::default(),
            listener: None,
        };

        // Prepare for workers
        let rc = Arc::new(Mutex::new(rc));
        let rit = self.wstate.replicas.insert(rc.clone());
        debug!(self.log, "new replica added"; "rit" => rit);
        self.wstate.revmap.insert(ri, rit);

        // Keep track of new socket to listen on
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
    channel_coordinator: Arc<ChannelCoordinator>,
}

impl Worker {
    pub fn run(mut self, checktable_addr: SocketAddr) {
        // we want to only process a single domain, otherwise we might hold on to work that another
        // worker could be doing. and that'd be silly.
        let mut events = Events::with_capacity(1);
        let mut timers = VecMap::<Instant>::new();
        let mut durtmp = None;
        let mut force_refresh_truth = false;
        let mut sends = Vec::new();

        dataflow::connect_thread_checktable(checktable_addr);

        let cc = &self.channel_coordinator;
        let send = |outputs: &mut FnvHashMap<ReplicaIndex, (TcpSender<Packet>, bool)>,
                    ri: ReplicaIndex,
                    mut m: Box<Packet>|
         -> Result<(), SendError> {
            let &mut (ref mut tx, is_local) = outputs.entry(ri).or_insert_with(|| {
                let mut tx = None;
                while tx.is_none() {
                    tx = cc.get_tx(&ri);
                }
                tx.unwrap()
            });
            if is_local {
                m = m.make_local();
            }
            tx.send_ref(&m)
        };

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
                            match context.replica.on_event(PollEvent::Timeout, &mut sends) {
                                ProcessResult::KeepPolling => {
                                    // FIXME: this could return a bunch of things to be sent
                                    for (ri, m) in sends.drain(..) {
                                        // TODO: handle one local packet without sending
                                        // deliver this packet to its destination TCP queue
                                        // XXX: unwrap
                                        send(&mut context.outputs, ri, m).unwrap();
                                    }
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

            let refresh_truth = |shared: &mut SharedWorkerState| {
                let truth = shared.truth.lock().unwrap();
                shared.sockets = (*truth).clone();
            };

            if force_refresh_truth
                || self.shared
                    .sockets
                    .get(token)
                    .and_then(|sc| self.shared.replicas.get(sc.rit))
                    .is_none()
            {
                // unknown socket -- we need to update our cached state
                refresh_truth(&mut self.shared);
                debug!(self.log, "worker updated their notion of truth");

                // also learn about new replicas while we're at it
                while let Ok(added) = self.notify.try_recv() {
                    // register replica
                    let rit = self.shared.replicas.insert(added.rc);

                    // register
                    assert_eq!(rit, added.rit);
                    self.shared.revmap.insert(added.ri, rit);
                }
                force_refresh_truth = false;
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
            let ready = |e: &Evented| {
                all.reregister(
                    e,
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

                ready(replica.listener.as_ref().unwrap());
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
                    //
                    // in either case, we need to figure out what stream to re-register. we can't
                    // trust the cached sc.fd, since the token *might* have been remapped (same as
                    // the Entry::Vacant case below), so we must get the fd right from the truth.
                    ready(&EventedFd(&self.shared.truth.lock().unwrap()[token].fd));

                    // XXX: NLL would let us directly refresh truth here, which would save us one
                    // lock acquisition, but we don't have NLL yet :(
                    force_refresh_truth = true;
                    continue;
                }
                Err(TryLockError::Poisoned(e)) => panic!("found poisoned lock: {}", e),
            };

            // track if we can handle a local output directly
            // XXX: MAJOR PROBLUM:
            //
            //  what if we decided *not* to use `next` to a given replica in the previous iteration
            //  over this replica, and then in this replica, we *do* decide to use `next`? Now the
            //  message from the previous iteration is (potentially) sitting in the input queue for
            //  the target replica, and yet we're directly executing this (following) packet. Not
            //  good. Not good at all.
            let mut next = None;

            // unless the connection is dropped, we want to rearm the socket
            let mut rearm = true;

            let mut resume_polling = |rit: usize, replica: &mut Replica| {
                let mut sends = Vec::new();
                replica.on_event(PollEvent::ResumePolling(&mut durtmp), &mut sends);
                if let Some(timeout) = durtmp.take() {
                    timers.insert(rit, Instant::now() + timeout);
                } else {
                    timers.remove(rit);
                }
                if !sends.is_empty() {
                    // ResumePolling is not allowed to send packets
                    unimplemented!();
                }
            };

            let fd = {
                // deref guard explicitly so that we can field borrows are tracked separately.
                // if we didn't do this, the first thing that borrows from context would borrow
                // *all* of context.
                let context = &mut *context;

                // we're responsible for running the given domain, and we have its lock
                use vec_map::Entry;
                let mut channel = match context.receivers.entry(token) {
                    Entry::Vacant(_) => {
                        // this means our `truth` is out of date and a token has been reused.
                        // we need to update our truth, and try again. but how do we
                        // re-register this stream so that it'll get returned to a thread later
                        // with an updated mapping? we can't safely use sc.fd, because clearly
                        // that mapping is for an old assignment for `token` (it pointed us to
                        // the wrong domain!). since we know we are the only thread currently
                        // working with this `token` (EPOLL_ONESHOT guarantees that), we know
                        // that it can't *currently* be remapped by another thread, so if we
                        // read from the truth, we know we'll get the right value.
                        ready(&EventedFd(&self.shared.truth.lock().unwrap()[token].fd));

                        // XXX: NLL would let us directly refresh truth here, which would save
                        // us one lock acquisition, but we don't have NLL yet :(
                        force_refresh_truth = true;
                        continue;
                    }
                    Entry::Occupied(mut e) => e,
                };

                // we *still* can't trust sc.fd.
                let fd = channel.get().get_ref().as_raw_fd();

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

                            // we might have processed some packets before we got Disconnected,
                            // and maybe even a `next`, so we can't just continue the outer
                            // polling loop here unfortunately.
                            break;
                        }
                        Err(TryRecvError::DeserializationError(e)) => panic!("{}", e),
                    };

                    if !self.process(&mut context.replica, m, &mut sends) {
                        // told to exit?
                        ready(&EventedFd(&fd));
                        warn!(self.log, "worker told to exit");
                        return;
                    }

                    // XXX: MAJOR PROBLUM
                    // what if another worker picks up target domain lock, and processes the
                    // message we sent that comes *after* next? out-of-order...

                    // if there is at least one more *complete* Packet in `channel` then we
                    // should send everything in `output` and process the next Packet. however,
                    // if there are no complete Packets in `channel`, we should instead look
                    // for a local destination in `output`, send the others, and then process
                    // that packet immediately to save some syscalls and a trip through the
                    // network stack..
                    let last = channel.get().is_empty();
                    let mut give_up_next = false;
                    for (ri, m) in sends.drain(..) {
                        if last && next.is_none() && !give_up_next && self.is_local(&ri) {
                            // we have to take the *first* packet, not the last, otherwise we
                            // might process out-of-order.
                            next = Some((ri, m));
                            continue;
                        }

                        if next.as_ref()
                            .map(|&(ref nri, _)| nri == &ri)
                            .unwrap_or(false)
                        {
                            // second packet for that replica.
                            //
                            // it is *not* safe for us to handle `next` without a TCP send,
                            // because some other thread could come along and take the lock for
                            // the target replica the *moment* we send `m`, and process it.
                            // Since `m` comes *after* `next`, that would result in
                            // out-of-order delivery, which is not okay. So we must give up on
                            // next here.
                            let (ri, m) = next.take().unwrap();
                            send(&mut context.outputs, ri, m).unwrap();

                            // we must *also* ensure that we don't then pick up a `next` again.
                            // in theory this could be okay if the next `next`'s ri is not this
                            // ri, but checking that is annoying, so TODO.
                            give_up_next = true;
                        }

                        // deliver this packet to its destination TCP queue
                        // XXX: unwrap
                        send(&mut context.outputs, ri, m).unwrap();
                    }

                    if last {
                        break;
                    }
                }

                fd
            };

            // Register timeout for replica
            resume_polling(sc.rit, &mut context.replica);

            // we have a packet we can handle directly!
            let mut give_up_next = false;
            let mut context_of_next_origin = context;
            let mut carry = 0;
            while let Some((ri, m)) = next.take() {
                carry += 1;
                assert!(sends.is_empty());
                let rit = self.shared.revmap[&ri];
                let rc = self.shared
                    .replicas
                    .get(rit)
                    .expect("packet is for unknown replica");
                match rc.try_lock() {
                    Ok(mut context) => {
                        // no other thread is operating on the target domain, so we can do it
                        if !self.process(&mut context.replica, m, &mut sends) {
                            // told to exit?
                            ready(&EventedFd(&fd));
                            warn!(self.log, "worker told to exit");
                            return;
                        }

                        for (ri, m) in sends.drain(..) {
                            if next.is_none() && !give_up_next && self.is_local(&ri) {
                                next = Some((ri, m));
                                continue;
                            }

                            if next.as_ref()
                                .map(|&(ref nri, _)| nri == &ri)
                                .unwrap_or(false)
                            {
                                // second packet for that replica.
                                // we have the same race as in the primary delivery loop above
                                let (ri, m) = next.take().unwrap();
                                send(&mut context.outputs, ri, m).unwrap();
                                give_up_next = true;
                            }


                            // deliver this packet to its destination TCP queue
                            // XXX: unwrap
                            send(&mut context.outputs, ri, m).unwrap();
                        }

                        // Register timeout for replica
                        resume_polling(rit, &mut context.replica);

                        // We can't give up the lock on `context` yet, because then another
                        // thread could swoop in, take the lock on this replica, process a
                        // packet, and send some packet (that should come after `next`) to the
                        // same replica as `next` is going to. if a worker then takes the lock
                        // for the target of `next`, it will see the queued packet, and process
                        // it, before we process `next`, which is OOO!
                        //
                        // it *is* safe at this point to release the lock for the *previous*
                        // context, because we have now processed all of its outputs.
                        context_of_next_origin = context;
                    }
                    Err(e) => {
                        // we couldn't get the lock, so we push it on the TCP queue for later
                        // NOTE: this is only okay because we know that only a single send to
                        // ri happened, so we won't cause an out-of-order delivery.
                        // XXX: unwrap
                        send(&mut context_of_next_origin.outputs, ri, m).unwrap();
                    }
                }

                if carry == 1 {
                    // we're no longer holding the lock for the replica we originally polled from,
                    // so we can let other workers handle it.
                    ready(&EventedFd(&fd));
                }
            }

            drop(context_of_next_origin);
            if carry == 0 {
                // there were no next()'s to ready the original socket
                ready(&EventedFd(&fd));
            }
        }
    }

    fn is_local(&self, ri: &ReplicaIndex) -> bool {
        // self.shared.revmap.contains_key(ri)
        false
    }

    // returns true if processing should continue
    fn process(
        &self,
        domain: &mut Replica,
        packet: Box<Packet>,
        sends: &mut Vec<EnqueuedSend>,
    ) -> bool {
        match domain.on_event(PollEvent::Process(packet), sends) {
            ProcessResult::KeepPolling => true,
            ProcessResult::StopPolling => false,
        }
    }
}
