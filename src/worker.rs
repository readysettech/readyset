use std::cell::RefCell;
use std::io;
use std::ops::AddAssign;
use std::os::unix::io::{AsRawFd, RawFd};
use std::sync::{mpsc, Arc, Mutex, TryLockError};
use std::thread;

use fnv::FnvHashMap;
use mio::net::TcpListener;
use mio_pool::poll::{Events, Poll, Token};
use slab::Slab;
use slog::Logger;
use timer_heap::{TimerHeap, TimerType};
use vec_map::VecMap;

use api;
use channel::poll::{PollEvent, ProcessResult};
use channel::tcp::{SendError, TryRecvError};
use channel::{
    self, DomainConnectionBuilder, DualTcpReceiver, TcpReceiver, TcpSender, CONNECTION_FROM_BASE,
};
use dataflow::payload::SourceChannelIdentifier;
use dataflow::{self, Domain, Packet};

struct CachedRawFd(RawFd);

impl AsRawFd for CachedRawFd {
    fn as_raw_fd(&self) -> RawFd {
        self.0
    }
}

pub struct NewReplica {
    pub inner: Replica,
    pub listener: TcpListener,
}

type Replica = Domain;
type ReplicaIndex = (dataflow::Index, usize);
type EnqueuedSend = (ReplicaIndex, Box<Packet>);
type ChannelCoordinator = channel::ChannelCoordinator<ReplicaIndex>;
type ReplicaToken = usize;

type ReplicaReceiversInner = VecMap<
    RefCell<(
        DualTcpReceiver<Box<Packet>, api::Input>,
        Option<ReplicaIndex>,
    )>,
>;

#[derive(Default)]
struct ReplicaReceivers(ReplicaReceiversInner);

use std::ops::{Deref, DerefMut};
impl Deref for ReplicaReceivers {
    type Target = ReplicaReceiversInner;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl DerefMut for ReplicaReceivers {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

struct ReplicaContext {
    /// A map from socket token to `TcpReceiver`.
    receivers: ReplicaReceivers,
    listener: Option<TcpListener>,
    replica: Replica,
    outputs: FnvHashMap<ReplicaIndex, (TcpSender<Packet>, bool)>,

    /// Number of packets received from each *local* replica
    #[cfg(feature = "carry_local")]
    recvd_from_local: FnvHashMap<ReplicaIndex, usize>,
    /// Number of packets sent to each *local* replica
    #[cfg(feature = "carry_local")]
    sent_to_local: FnvHashMap<ReplicaIndex, usize>,
}

#[derive(Clone)]
struct NewReplicaInternal {
    ri: ReplicaIndex,
    rc: Arc<Mutex<ReplicaContext>>,
    rit: ReplicaToken,
}

#[derive(Clone)]
struct SocketContext {
    rit: ReplicaToken,
    listening: bool,
    fd: RawFd,
}

struct SharedWorkerState {
    /// A mapping from socket token to replica token.
    sockets: Slab<SocketContext>,
    /// A mapping from replica index token to the context associated with that replica.
    replicas: Slab<Arc<Mutex<ReplicaContext>>>,
    /// A mapping from replica index to replica token.
    revmap: FnvHashMap<ReplicaIndex, ReplicaToken>,
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

// NOTE: mio::net::TcpStream doesn't expose underlying stream :(
fn set_nonblocking(s: &::mio::net::TcpStream, on: bool) {
    use std::net::TcpStream;
    use std::os::unix::io::{AsRawFd, FromRawFd, IntoRawFd};
    let t = unsafe { TcpStream::from_raw_fd(s.as_raw_fd()) };
    t.set_nonblocking(on).unwrap();
    // avoid closing on Drop
    t.into_raw_fd();
}

pub struct WorkerPool {
    workers: Vec<thread::JoinHandle<()>>,
    poll: Arc<Poll>,
    notify: Vec<mpsc::Sender<NewReplicaInternal>>,
    wstate: SharedWorkerState,
    log: Logger,
}

impl WorkerPool {
    pub fn new(
        n: usize,
        log: &Logger,
        channel_coordinator: Arc<ChannelCoordinator>,
    ) -> io::Result<Self> {
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
                thread::Builder::new()
                    .name(format!("worker{}", i + 1))
                    .spawn(move || w.run())
                    .unwrap()
            })
            .collect();

        Ok(WorkerPool {
            workers,
            poll,
            notify: notify_tx,
            wstate: SharedWorkerState::new(truth),
            log: log.new(o!()),
        })
    }

    pub fn add_replica(&mut self, replica: NewReplica) {
        let NewReplica { inner, listener } = replica;
        let ri = inner.id();
        let addr = listener.local_addr().unwrap();

        let rc = ReplicaContext {
            receivers: Default::default(),
            replica: inner,
            outputs: Default::default(),
            listener: None,

            #[cfg(feature = "carry_local")]
            recvd_from_local: Default::default(),
            #[cfg(feature = "carry_local")]
            sent_to_local: Default::default(),
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
            tx.send(notify.clone()).unwrap();
        }

        // Update Poll so workers start notifications about connections to the new replica
        let r = self.wstate.replicas.get_mut(rit).unwrap();
        let mut r = r.lock().unwrap();
        self.poll.register(&listener, Token(token)).unwrap();
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
    pub fn run(mut self) {
        // we want to only process a single domain, otherwise we might hold on to work that another
        // worker could be doing. and that'd be silly.
        let mut events = Events::with_capacity(1);
        let mut timers = TimerHeap::new();
        let mut expired = Vec::new();
        let mut durtmp = None;
        let mut force_refresh_truth = false;
        let mut sends = Vec::new();

        let cc = &self.channel_coordinator;
        let send = |me: &ReplicaIndex,
                    outputs: &mut FnvHashMap<ReplicaIndex, (TcpSender<Packet>, bool)>,
                    ri: ReplicaIndex,
                    mut m: Box<Packet>|
         -> Result<(), SendError> {
            let &mut (ref mut tx, is_local) = outputs.entry(ri).or_insert_with(|| {
                let mut dest = None;
                while dest.is_none() {
                    dest = cc.get_dest(&ri);
                }
                let (addr, is_local) = dest.unwrap();
                let mut tx = DomainConnectionBuilder::for_domain(addr).build().unwrap();
                if is_local {
                    tx.send_ref(&Packet::Hey(me.0, me.1)).unwrap();
                }
                (tx, is_local)
            });
            if is_local {
                m = m.make_local();
            }
            tx.send_ref(&m)
        };

        loop {
            // have any timers expired?
            expired.extend(timers.expired());
            for rit in expired.drain(..) {
                // NOTE: try_lock is okay, because if another worker is handling it, the
                // timeout is also being dealt with.
                let rc = if let Some(rc) = self.shared.replicas.get(rit) {
                    rc
                } else {
                    continue;
                };

                if let Ok(mut context) = rc.try_lock() {
                    let context = &mut *context;
                    match context.replica.on_event(
                        &mut context.receivers,
                        PollEvent::Timeout,
                        &mut sends,
                    ) {
                        ProcessResult::KeepPolling => {
                            let from_ri = context.replica.id();
                            for (ri, m) in sends.drain(..) {
                                // TODO: handle one local packet without sending here too?
                                #[cfg(feature = "carry_local")]
                                context.sent_to_local.entry(ri).or_insert(0).add_assign(1);
                                // deliver this packet to its destination TCP queue
                                // XXX: unwrap
                                send(&from_ri, &mut context.outputs, ri, m).unwrap();
                            }

                            // what's now the next timeout?
                            context.replica.on_event(
                                &mut context.receivers,
                                PollEvent::ResumePolling(&mut durtmp),
                                &mut sends,
                            );
                            if let Some(timeout) = durtmp.take() {
                                timers.upsert(rit, timeout, TimerType::Oneshot);
                            } else {
                                timers.remove(rit);
                            }
                            if !sends.is_empty() {
                                // ResumePolling is not allowed to send packets
                                unimplemented!();
                            }
                        }
                        ProcessResult::StopPolling => unreachable!(),
                    }
                }
            }

            let next = timers.time_remaining();
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

            let token = match events.into_iter().next() {
                Some(Token(t)) => t,
                None => {
                    // we must have timed out -- check timers
                    continue;
                }
            };
            //trace!(self.log, "worker polled"; "token" => token);

            let refresh_truth = |shared: &mut SharedWorkerState| {
                let truth = shared.truth.lock().unwrap();
                shared.sockets = (*truth).clone();
            };

            if force_refresh_truth
                || self
                    .shared
                    .sockets
                    .get(token)
                    .and_then(|sc| self.shared.replicas.get(sc.rit))
                    .is_none()
            {
                // unknown socket -- we need to update our cached state
                refresh_truth(&mut self.shared);
                trace!(self.log,
                       "worker updated their notion of truth";
                       "forced" => force_refresh_truth);

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

            let sc = match self.shared.sockets.get(token) {
                Some(sc) => sc,
                None => {
                    // got event for unknown token
                    // normally, this should be an error, but mio seems to sometimes give us
                    // spurious wakeups for old tokens. so, we just ignore the wakeup instead.
                    debug!(self.log, "spurious wakeup for unknown token"; "token" => token);
                    continue;
                }
            };
            let replica = self
                .shared
                .replicas
                .get(sc.rit)
                .expect("token resolves to unknown replica");

            //trace!(self.log, "worker handling replica event"; "replica" => sc.rit);

            let all = &self.all;
            let ready = |fd: RawFd| all.reregister(&CachedRawFd(fd), Token(token));

            if sc.listening {
                // listening socket -- we need to accept a new connection
                let mut replica = replica.lock().unwrap();

                // NOTE: only really a while loop to let us break early
                while let Ok((mut stream, _src)) = replica.listener.as_mut().unwrap().accept() {
                    // we know that any new connection to a domain will first send a one-byte
                    // token to indicate whether the connection is from a base or not.
                    set_nonblocking(&stream, false);
                    let mut tag = [0];
                    use std::io::Read;
                    if let Err(e) = stream.read_exact(&mut tag[..]) {
                        // well.. that failed quickly..
                        info!(self.log, "worker discarded new connection: {:?}", e);
                        break;
                    }
                    let is_base = tag[0] == CONNECTION_FROM_BASE;
                    set_nonblocking(&stream, true);

                    // NOTE: we're taking two locks at the same time here! we need to be sure that
                    // anything that holds the `truth` lock will eventually relinquish it.
                    //
                    //   a) add_replica immediately relinquishes
                    //   b) cloning truth (above) immediately relinquishes
                    //   c) on disconnect, we take locks in same order, and immediately release
                    //   d) we immediately relinquish
                    //
                    // so we should be safe from deadlocks
                    let token = self.shared.truth.lock().unwrap().insert(SocketContext {
                        rit: sc.rit,
                        fd: stream.as_raw_fd(),
                        listening: false,
                    });
                    debug!(self.log, "worker accepted new connection"; "token" => token);

                    self.all.register(&stream, Token(token)).unwrap();

                    let tcp = if is_base {
                        DualTcpReceiver::upgrade(stream, move |input| {
                            Box::new(Packet::Input {
                                inner: input,
                                src: Some(SourceChannelIdentifier { token: token }),
                                senders: Vec::new(),
                            })
                        })
                    } else {
                        TcpReceiver::with_capacity(2 * 1024 * 1024, stream).into()
                    };
                    replica.receivers.insert(token, RefCell::new((tcp, None)));
                    break;
                }

                ready(replica.listener.as_ref().unwrap().as_raw_fd()).unwrap();
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
                    // XXX
                    if let Some(fd) = self.shared.truth.lock().unwrap().get(token).map(|sc| sc.fd) {
                        ready(fd).unwrap();
                    } else {
                        // spurious wakeup from mio for old (deregistered) token
                        debug!(self.log,
                               "failed spurious wakeup for unknown token";
                               "token" => token);
                    }
                    continue;
                }
                Err(TryLockError::Poisoned(e)) => panic!("found poisoned lock: {}", e),
            };

            // track if we can handle a local output directly
            #[cfg(feature = "carry_local")]
            let mut next = None;

            // unless the connection is dropped, we want to rearm the socket
            let mut rearm = true;

            let mut from_ri = context.replica.id();
            trace!(self.log, "worker handling replica event"; "replica" => ?from_ri);

            let mut resume_polling = |rit: usize, context: &mut ReplicaContext| {
                let mut sends = Vec::new();
                context.replica.on_event(
                    &mut context.receivers,
                    PollEvent::ResumePolling(&mut durtmp),
                    &mut sends,
                );
                if let Some(timeout) = durtmp.take() {
                    timers.upsert(rit, timeout, TimerType::Oneshot);
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
                let channel = match context.receivers.get(token) {
                    Some(ch) => ch,
                    None => {
                        // this means our `truth` is out of date and a token has been reused.
                        // we need to update our truth, and try again. but how do we
                        // re-register this stream so that it'll get returned to a thread later
                        // with an updated mapping? we can't safely use sc.fd, because clearly
                        // that mapping is for an old assignment for `token` (it pointed us to
                        // the wrong domain!). since we know we are the only thread currently
                        // working with this `token` (EPOLL_ONESHOT guarantees that), we know
                        // that it can't *currently* be remapped by another thread, so if we
                        // read from the truth, we know we'll get the right value.
                        if let Some(fd) =
                            self.shared.truth.lock().unwrap().get(token).map(|sc| sc.fd)
                        {
                            ready(fd).unwrap();
                            // XXX: NLL would let us directly refresh truth here, which would save
                            // us one lock acquisition, but we don't have NLL yet :(
                            force_refresh_truth = true;
                        } else {
                            // NOTE: this *shouldn't* be possible. it means we're notified about a
                            // token that does not exist. however, in certain cases, it seems like
                            // mio nonetheless wakes us up right after we deregister a token, so we
                            // need to not fail. we should *not* re-arm any fd though.
                            // unreachable!();
                            debug!(self.log,
                                   "spurious wakeup for unknown channel token";
                                   "token" => token);
                        }
                        continue;
                    }
                };

                // we *still* can't trust sc.fd.
                let fd = channel.borrow().0.get_ref().as_raw_fd();

                channel.borrow_mut().0.syscall_limit(Some(1));
                loop {
                    let m = channel.borrow_mut().0.try_recv();
                    // even with nll we need to *first* try_recv, and *then* match. otherwise the
                    // borrow_mut() handle will be kept until the end of the match, which would
                    // prevent us from doing context.receivers.remove().
                    let m = match m {
                        Ok(p) => p,
                        Err(TryRecvError::Empty) => break,
                        Err(TryRecvError::Disconnected) => {
                            // how can this even happen?
                            // base that goes away maybe?
                            // in any case, we can unregister the socket.
                            debug!(self.log, "worker dropped lost connection"; "token" => token);
                            let rx = context.receivers.remove(token).unwrap();

                            // dropping rx will automatically deregister the socket (in theory)
                            //
                            // see
                            // https://github.com/carllerche/mio/issues/351#issuecomment-183746183
                            // and
                            // https://docs.rs/mio/0.6.11/mio/struct.Poll.html#method.deregister
                            //
                            // however, there is no harm in explicitly deregistering it as well
                            // NOTE: we *must* deregister before freeing `token`, because otherwise
                            // another connection can take (and register) `token`. This isn't a
                            // problem as far as file descriptors go, but mio may get confused.
                            self.all.deregister(rx.borrow().0.get_ref()).is_err();

                            // no deadlock for same reason as adding a socket to poll
                            self.shared.truth.lock().unwrap().remove(token);

                            // we might have processed some packets before we got Disconnected,
                            // and maybe even a `next`, so we can't just continue the outer
                            // polling loop here unfortunately.
                            rearm = false;
                            break;
                        }
                        Err(TryRecvError::DeserializationError(e)) => {
                            error!(self.log, "connection deserialization error: {:?}", e);
                            // just drop it.
                            let rx = context.receivers.remove(token).unwrap();
                            self.all.deregister(rx.borrow().0.get_ref()).is_err();
                            self.shared.truth.lock().unwrap().remove(token);
                            rearm = false;
                            break;
                        }
                    };

                    if let Packet::Hey(di, shard) = *m {
                        let ri = (di, shard);
                        channel.borrow_mut().1 = Some(ri);
                        #[cfg(feature = "carry_local")]
                        context.recvd_from_local.insert(ri, 0);
                        continue;
                    }

                    if let Some(ri) = channel.borrow().1.as_ref() {
                        #[cfg(feature = "carry_local")]
                        context
                            .recvd_from_local
                            .get_mut(ri)
                            .as_mut()
                            .unwrap()
                            .add_assign(1);
                    }

                    // we're in a bit of a tough spot here from a borrowck perspective, becuase the
                    // replica may want to send to the channel we are currently holding a handle
                    // to. the compiler is completely right about saying that borrowing
                    // context.receivers here is *not* okay. However, *we* know that reading
                    // context.receivers here is fine, since we hold the &mut context.receivers,
                    // and we are not concurrently modifying it.
                    if !self.process(&mut context.replica, &context.receivers, m, &mut sends) {
                        // told to exit?
                        if rearm {
                            ready(fd).unwrap();
                        }
                        warn!(self.log, "worker told to exit");
                        return;
                    }

                    // if there is at least one more *complete* Packet in `channel` then we
                    // should send everything in `output` and process the next Packet. however,
                    // if there are no complete Packets in `channel`, we should instead look
                    // for a local destination in `output`, send the others, and then process
                    // that packet immediately to save some syscalls and a trip through the
                    // network stack..
                    let last = channel.borrow().0.is_empty();
                    let mut give_up_next = false;
                    for (ri, m) in sends.drain(..) {
                        #[cfg(feature = "carry_local")]
                        {
                            if last && next.is_none() && !give_up_next && self.is_local(&ri) {
                                // we have to take the *first* packet, not the last, otherwise we
                                // might process out-of-order.
                                next = Some((ri, m));
                                continue;
                            }

                            if next
                                .as_ref()
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
                                context.sent_to_local.entry(ri).or_insert(0).add_assign(1);
                                send(&from_ri, &mut context.outputs, ri, m).unwrap();

                                // we must *also* ensure that we don't then pick up a `next` again.
                                // in theory this could be okay if the next `next`'s ri is not this
                                // ri, but checking that is annoying, so TODO.
                                give_up_next = true;
                            }

                            // deliver this packet to its destination TCP queue
                            context.sent_to_local.entry(ri).or_insert(0).add_assign(1);
                        }
                        // XXX: unwrap
                        send(&from_ri, &mut context.outputs, ri, m).unwrap();
                    }

                    if last {
                        break;
                    }
                }

                fd
            };

            // Register timeout for replica
            resume_polling(sc.rit, &mut *context);

            // we have a packet we can handle directly!
            let mut give_up_next = false;
            let mut context_of_next_origin = context;
            let mut carry = 0;
            #[cfg(feature = "carry_local")]
            while let Some((ri, m)) = next.take() {
                assert!(sends.is_empty());
                let rit = self.shared.revmap[&ri];
                let rc = self
                    .shared
                    .replicas
                    .get(rit)
                    .expect("packet is for unknown replica");
                match rc.try_lock() {
                    Ok(mut context) => {
                        carry += 1;

                        // ensure we haven't sent any packets in past iterations to this replica
                        // that have not yet been processed (and that logically preceede `next`).
                        if context_of_next_origin.sent_to_local.get(&ri)
                            != context.recvd_from_local.get(&from_ri)
                        {
                            // there are, so we can't handle `next` directly after all
                            context_of_next_origin
                                .sent_to_local
                                .entry(ri)
                                .or_insert(0)
                                .add_assign(1);
                            send(&from_ri, &mut context_of_next_origin.outputs, ri, m).unwrap();
                            break;
                        }

                        // record the fact that we "sent" and "received" this packet.
                        context_of_next_origin
                            .sent_to_local
                            .entry(ri)
                            .or_insert(0)
                            .add_assign(1);
                        context
                            .recvd_from_local
                            .entry(from_ri)
                            .or_insert(0)
                            .add_assign(1);

                        // We can't give up the lock on `context` for a while, because then another
                        // thread could swoop in, take the lock on this replica, process a
                        // packet, and send some packet (that should come after `next`) to the
                        // same replica as `next` is going to. if a worker then takes the lock
                        // for the target of `next`, it will see the queued packet, and process
                        // it, before we process `next`, which is OOO!
                        //
                        // it *is* safe at this point to release the lock for the *previous*
                        // context, because we now guarantee that we'll handle `next`, so if
                        // another thread takes that context and produces some packets, they'll be
                        // processed strictly later.
                        context_of_next_origin = context;

                        // just a little aliasing trick to avoid using the long name below
                        let mut context = context_of_next_origin;

                        // we need to ready the fd for the original replica so that another thread
                        // will be notified if there's more work for it.
                        if carry == 1 && rearm {
                            ready(fd).unwrap();
                            rearm = false;
                        }

                        // no other thread is operating on the target domain, so we can do it
                        {
                            // borrowck gets confused through DerefMut
                            let context = &mut *context;
                            if !self.process(
                                &mut context.replica,
                                &context.receivers,
                                m,
                                &mut sends,
                            ) {
                                // told to exit?
                                if rearm {
                                    ready(fd).unwrap();
                                }
                                warn!(self.log, "worker told to exit");
                                return;
                            }
                        }

                        from_ri = ri;
                        for (ri, m) in sends.drain(..) {
                            if next.is_none() && !give_up_next && self.is_local(&ri) {
                                next = Some((ri, m));
                                continue;
                            }

                            if next
                                .as_ref()
                                .map(|&(ref nri, _)| nri == &ri)
                                .unwrap_or(false)
                            {
                                // second packet for that replica.
                                // we have the same race as in the primary delivery loop above
                                let (ri, m) = next.take().unwrap();
                                context.sent_to_local.entry(ri).or_insert(0).add_assign(1);
                                send(&from_ri, &mut context.outputs, ri, m).unwrap();
                                give_up_next = true;
                            }

                            // deliver this packet to its destination TCP queue
                            context.sent_to_local.entry(ri).or_insert(0).add_assign(1);
                            // XXX: unwrap
                            send(&from_ri, &mut context.outputs, ri, m).unwrap();
                        }

                        // Register timeout for replica
                        resume_polling(rit, &mut context);
                        context_of_next_origin = context;
                    }
                    Err(_) => {
                        // we couldn't get the lock, so we push it on the TCP queue for later
                        // NOTE: this is only okay because we know that only a single send to
                        // ri happened, so we won't cause an out-of-order delivery.
                        context_of_next_origin
                            .sent_to_local
                            .entry(ri)
                            .or_insert(0)
                            .add_assign(1);
                        // XXX: unwrap
                        send(&from_ri, &mut context_of_next_origin.outputs, ri, m).unwrap();
                    }
                }
            }

            drop(context_of_next_origin);
            if rearm {
                // there were no next()'s to ready the original socket, or we failed to handle it
                // make sure to mark it as ready for other workers.
                ready(fd).unwrap();
            }
        }
    }

    fn is_local(&self, ri: &ReplicaIndex) -> bool {
        self.shared.revmap.contains_key(ri)
    }

    // returns true if processing should continue
    fn process(
        &self,
        replica: &mut Replica,
        receivers: &ReplicaReceivers,
        mut packet: Box<Packet>,
        sends: &mut Vec<EnqueuedSend>,
    ) -> bool {
        if let Some(local) = packet.extract_local() {
            packet = local;
        }

        match replica.on_event(receivers, PollEvent::Process(packet), sends) {
            ProcessResult::KeepPolling => true,
            ProcessResult::StopPolling => false,
        }
    }
}

impl dataflow::prelude::Executor for ReplicaReceivers {
    fn send_back(&self, channel: SourceChannelIdentifier, reply: ()) {
        if let Some(ch) = self.get(channel.token) {
            use bincode;
            use std::io::{self, Write};

            let bytes = bincode::serialize(&reply).unwrap();
            let mut bytes = &bytes[..];

            // XXX: what if the token has been reused?
            let stream = ch.borrow();
            let mut stream = stream.0.get_ref();

            let mut undo_blocking = false;
            while !bytes.is_empty() {
                // NOTE: we unfortunately can't just serialize_into the stream
                // https://github.com/TyOverby/bincode/issues/229
                match stream.write(bytes) {
                    Ok(n) => {
                        bytes = &bytes[n..];
                    }
                    Err(e) => match e.kind() {
                        io::ErrorKind::WouldBlock => {
                            // transient -- retry (and start blocking)
                            set_nonblocking(stream, false);
                            undo_blocking = true;
                        }
                        io::ErrorKind::Interrupted | io::ErrorKind::TimedOut => {
                            // transient -- retry
                        }
                        _ => {
                            // permanent, client disconnected, ignore and swallow
                            return;
                        }
                    },
                }
            }

            while let Err(e) = stream.flush() {
                match e.kind() {
                    io::ErrorKind::WouldBlock => {
                        // transient -- retry (and start blocking)
                        set_nonblocking(stream, false);
                        undo_blocking = true;
                    }
                    io::ErrorKind::Interrupted | io::ErrorKind::TimedOut => {
                        // transient -- retry
                    }
                    _ => {
                        // client connection lost, ignore and swallow
                        return;
                    }
                }
            }

            if undo_blocking {
                set_nonblocking(stream, true);
            }
        }
    }
}
