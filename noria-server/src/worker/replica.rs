/// Only allow processing this many inputs in a domain before we handle timer events, acks, etc.
const FORCE_INPUT_YIELD_EVERY: usize = 64;

use super::ChannelCoordinator;
use crate::coordination::CoordinationPayload;
use async_bincode::AsyncDestination;
use async_timer::Oneshot;
use bincode;
use dataflow::{
    payload::SourceChannelIdentifier,
    prelude::{DataType, Executor},
    Domain, Packet, PollEvent, ProcessResult,
};
use failure::{self, ResultExt};
use fnv::{FnvHashMap, FnvHashSet};
use futures_util::ready;
use futures_util::stream::futures_unordered::FuturesUnordered;
use noria::channel::{DualTcpStream, CONNECTION_FROM_BASE};
use noria::internal::DomainIndex;
use noria::internal::LocalOrNot;
use noria::{Input, Tagged};
use pin_project::pin_project;
use slog;
use std::collections::{HashMap, VecDeque};
use std::io;
use std::sync::Arc;
use std::time;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use stream_cancel::{Valve, Valved};
use streamunordered::{StreamUnordered, StreamYield};
use tokio::prelude::*;
use tokio_io::{BufReader, BufStream, BufWriter};

pub(super) type ReplicaAddr = (DomainIndex, usize);

// https://github.com/rust-lang/rust/issues/64445
type Incoming =
    Box<dyn Stream<Item = Result<tokio::net::tcp::TcpStream, tokio::io::Error>> + Unpin + Send>;
type FirstByte = Pin<
    Box<dyn Future<Output = Result<(tokio::net::tcp::TcpStream, u8), tokio::io::Error>> + Send>,
>;

#[pin_project]
pub(super) struct Replica {
    domain: Domain,
    log: slog::Logger,

    coord: Arc<ChannelCoordinator>,

    retry: Option<Box<Packet>>,

    #[pin]
    incoming: Valved<Incoming>,

    #[pin]
    first_byte: FuturesUnordered<FirstByte>,

    locals: tokio_sync::mpsc::UnboundedReceiver<Box<Packet>>,

    #[pin]
    inputs: StreamUnordered<
        DualTcpStream<
            BufStream<tokio::net::TcpStream>,
            Box<Packet>,
            Tagged<LocalOrNot<Input>>,
            AsyncDestination,
        >,
    >,

    outputs: FnvHashMap<
        ReplicaAddr,
        (
            Box<dyn Sink<Box<Packet>, Error = bincode::Error> + Send + Unpin>,
            bool,
        ),
    >,

    #[pin]
    timeout: Option<async_timer::oneshot::Timer>,
    timed_out: bool,

    out: Outboxes,
}

impl Replica {
    pub(super) fn new(
        valve: &Valve,
        mut domain: Domain,
        on: tokio::net::TcpListener,
        locals: tokio::sync::mpsc::UnboundedReceiver<Box<Packet>>,
        ctrl_tx: tokio::sync::mpsc::UnboundedSender<CoordinationPayload>,
        log: slog::Logger,
        cc: Arc<ChannelCoordinator>,
    ) -> Self {
        let id = domain.id();
        let id = format!("{}.{}", id.0.index(), id.1);
        domain.booted(on.local_addr().unwrap());
        Replica {
            coord: cc,
            domain,
            retry: None,
            incoming: valve.wrap(Box::new(on.incoming())),
            first_byte: FuturesUnordered::new(),
            locals,
            log: log.new(o! {"id" => id}),
            inputs: Default::default(),
            outputs: Default::default(),
            out: Outboxes::new(ctrl_tx),
            timeout: None,
            timed_out: false,
        }
    }

    fn try_acks(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Result<(), failure::Error> {
        let this = self.project();

        let mut inputs = this.inputs;
        let pending = &mut this.out.pending;

        // first, queue up any additional writes we have to do
        let mut err = Vec::new();
        this.out.back.retain(|&streami, tags| {
            let mut stream = Pin::new(&mut inputs[streami]);
            let mut no_more = false;

            let had = tags.len();
            tags.retain(|&tag| {
                if no_more {
                    return true;
                }

                match stream.as_mut().poll_ready(cx) {
                    Poll::Ready(Ok(())) => {}
                    Poll::Pending => {
                        no_more = true;
                        return true;
                    }
                    Poll::Ready(Err(e)) => {
                        err.push(e.into());
                        no_more = true;
                        return true;
                    }
                }

                match stream.as_mut().start_send(Tagged { tag, v: () }) {
                    Ok(()) => false,
                    Err(e) => {
                        // start_send shouldn't generally error
                        err.push(e.into());
                        no_more = true;
                        true
                    }
                }
            });

            if had != tags.len() {
                pending.insert(streami);
            }

            !tags.is_empty()
        });

        if !err.is_empty() {
            return Err(err.swap_remove(0));
        }

        // then, try to send on any streams we may be able to
        pending.retain(|&streami| {
            let stream = &mut inputs[streami];
            match Pin::new(stream).poll_flush(cx) {
                Poll::Pending => true,
                Poll::Ready(Ok(())) => false,
                Poll::Ready(Err(box bincode::ErrorKind::Io(e))) => {
                    match e.kind() {
                        io::ErrorKind::BrokenPipe
                        | io::ErrorKind::NotConnected
                        | io::ErrorKind::UnexpectedEof
                        | io::ErrorKind::ConnectionAborted
                        | io::ErrorKind::ConnectionReset => {
                            // connection went away, no need to try more
                            false
                        }
                        _ => {
                            err.push(e.into());
                            true
                        }
                    }
                }
                Poll::Ready(Err(e)) => {
                    err.push(e.into());
                    true
                }
            }
        });

        if !err.is_empty() {
            return Err(err.swap_remove(0));
        }

        Ok(())
    }

    fn try_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Result<(), failure::Error> {
        let this = self.project();

        let cc = this.coord;
        let outputs = this.outputs;

        // just like in try_acks:
        // first, queue up any additional writes we have to do
        let mut err = Vec::new();
        for (&ri, ms) in &mut this.out.domains {
            if ms.is_empty() {
                continue;
            }

            let &mut (ref mut tx, ref mut pending) = outputs.entry(ri).or_insert_with(|| {
                while !cc.has(&ri) {}
                let tx = cc.builder_for(&ri).unwrap().build_async().unwrap();
                (tx, true)
            });

            let mut tx = Pin::new(tx);

            while !ms.is_empty() {
                match tx.as_mut().poll_ready(cx) {
                    Poll::Ready(Ok(())) => {}
                    Poll::Pending => break,
                    Poll::Ready(Err(e)) => {
                        err.push(e);
                        break;
                    }
                }

                let m = ms.pop_front().expect("!is_empty");
                match tx.as_mut().start_send(m) {
                    Ok(()) => {
                        // we queued something, so we'll need to send!
                        *pending = true;
                    }
                    Err(e) => {
                        err.push(e);
                        break;
                    }
                }
            }
        }

        if !err.is_empty() {
            return Err(err.swap_remove(0).into());
        }

        // then, try to do any sends that are still pending
        for &mut (ref mut tx, ref mut pending) in outputs.values_mut() {
            if !*pending {
                continue;
            }

            match Pin::new(tx).poll_flush(cx) {
                Poll::Ready(Ok(())) => {
                    *pending = false;
                }
                Poll::Pending => {}
                Poll::Ready(Err(e)) => err.push(e),
            }
        }

        if !err.is_empty() {
            return Err(err.swap_remove(0).into());
        }

        Ok(())
    }

    fn try_new(self: Pin<&mut Self>, cx: &mut Context<'_>) -> io::Result<bool> {
        let mut this = self.project();

        while let Poll::Ready(stream) = this.incoming.as_mut().poll_next(cx)? {
            match stream {
                Some(mut stream) => {
                    // we know that any new connection to a domain will first send a one-byte
                    // token to indicate whether the connection is from a base or not.
                    debug!(this.log, "accepted new connection"; "from" => ?stream.peer_addr().unwrap());
                    this.first_byte.push(Box::pin(async move {
                        let mut byte = [0; 1];
                        let n = stream.read_exact(&mut byte[..]).await?;
                        assert_eq!(n, 1);
                        Ok((stream, byte[0]))
                    }));
                }
                None => {
                    return Ok(false);
                }
            }
        }

        while let Poll::Ready(Some((stream, tag))) = this.first_byte.as_mut().poll_next(cx)? {
            let is_base = tag == CONNECTION_FROM_BASE;

            debug!(this.log, "established new connection"; "base" => ?is_base);
            let slot = this.inputs.stream_entry();
            let token = slot.token();
            if let Err(e) = stream.set_nodelay(true) {
                warn!(this.log,
                      "failed to set TCP_NODELAY for new connection: {:?}", e;
                      "from" => ?stream.peer_addr().unwrap());
            }
            let tcp = if is_base {
                DualTcpStream::upgrade(
                    tokio_io::BufStream::new(stream),
                    move |Tagged { v: input, tag }| {
                        Box::new(Packet::Input {
                            inner: input,
                            src: Some(SourceChannelIdentifier { token, tag }),
                            senders: Vec::new(),
                        })
                    },
                )
            } else {
                tokio_io::BufStream::from(BufReader::with_capacity(
                    2 * 1024 * 1024,
                    BufWriter::with_capacity(4 * 1024, stream),
                ))
                .into()
            };
            slot.insert(tcp);
        }
        Ok(true)
    }

    fn try_timeout(self: Pin<&mut Self>, cx: &mut Context<'_>) {
        let mut this = self.project();

        if let Some(to) = this.timeout.as_mut().as_pin_mut() {
            if let Poll::Ready(()) = to.poll(cx) {
                this.timeout.set(None);
                *this.timed_out = true;
            }
        }

        if *this.timed_out {
            *this.timed_out = false;
            let try_block = tokio_executor::threadpool::blocking(|| {
                this.domain.on_event(this.out, PollEvent::Timeout);
            });
            if let Poll::Pending = try_block {
                // this is a little awkward. we failed to block, so we failed to notify about
                // the timeout expiry. we'll need to make sure we get back to the code above to
                // try again. note that we are already scheduled for a wake-up since blocking
                // returned Pending.
                *this.timed_out = true;
            }
        }
    }
}

struct Outboxes {
    // anything new to send?
    dirty: bool,

    // messages for other domains
    domains: FnvHashMap<ReplicaAddr, VecDeque<Box<Packet>>>,

    // map from inputi to number of (empty) ACKs
    back: FnvHashMap<usize, Vec<u32>>,
    pending: FnvHashSet<usize>,

    // for sending messages to the controller
    ctrl_tx: tokio::sync::mpsc::UnboundedSender<CoordinationPayload>,
}

impl Outboxes {
    fn new(ctrl_tx: tokio::sync::mpsc::UnboundedSender<CoordinationPayload>) -> Self {
        Outboxes {
            domains: Default::default(),
            back: Default::default(),
            pending: Default::default(),
            ctrl_tx,
            dirty: false,
        }
    }
}

impl Executor for Outboxes {
    fn ack(&mut self, id: SourceChannelIdentifier) {
        self.dirty = true;
        self.back.entry(id.token).or_default().push(id.tag);
    }

    fn create_universe(&mut self, universe: HashMap<String, DataType>) {
        self.ctrl_tx
            .try_send(CoordinationPayload::CreateUniverse(universe))
            .expect("asked to send to controller, but controller has gone away");
    }
    fn send(&mut self, dest: ReplicaAddr, m: Box<Packet>) {
        self.dirty = true;
        self.domains.entry(dest).or_default().push_back(m);
    }
}

impl Future for Replica {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let r: Poll<Result<(), failure::Error>> = try {
            loop {
                // FIXME: check if we should call update_state_sizes (every evict_every)

                // are there are any new connections?
                if !self
                    .as_mut()
                    .try_new(cx)
                    .context("check for new connections")?
                {
                    // incoming socket closed -- no more clients will arrive
                    return Poll::Ready(());
                }

                // have any of our timers expired?
                self.as_mut().try_timeout(cx);

                // we have three logical input sources: receives from local domains, receives from
                // remote domains, and remote mutators. we want to achieve some kind of fairness among
                // these, but bias the data-flow towards finishing work it has accepted (i.e., domain
                // operations) to accepting new work. however, this is complicated by two facts:
                //
                //  - we cannot (currently) differentiate between receives from remote domains and
                //    receives from mutators. they are all remote tcp channels. we *could*
                //    differentiate them using `is_base` in `try_new` to store them separately, but
                //    it's also unclear how that would change the receive heuristic.
                //  - domain operations are not all "completing starting work". in many cases, traffic
                //    from domains will be replay-related, in which case favoring domains would favor
                //    writes over reads. while we do in general want reads to be fast, we don't want
                //    them to fully starve writes.
                //
                // the current stategy is therefore that we alternate reading once from the local
                // channel and once from the set of remote channels. this biases slightly in favor of
                // local sends, without starving either. we also stop alternating once either source is
                // depleted.
                let mut local_done = false;
                let mut remote_done = false;
                let mut check_local = true;
                let readiness = 'ready: loop {
                    let mut this = self.as_mut().project();
                    let d = this.domain;
                    let out = this.out;

                    macro_rules! process {
                        ($retry:expr, $p:expr, $pp:expr) => {{
                            $retry = Some($p);
                            let retry = &mut $retry;
                            match tokio_executor::threadpool::blocking(|| {
                                $pp(retry.take().unwrap())
                            }) {
                                Poll::Ready(Ok(ProcessResult::StopPolling)) => {
                                    // domain got a message to quit
                                    // TODO: should we finish up remaining work?
                                    return Poll::Ready(());
                                }
                                Poll::Ready(Ok(_)) => {}
                                Poll::Pending => {
                                    // NOTE: the packet is still left in $retry, so we'll try again
                                    break 'ready Poll::Pending;
                                }
                                Poll::Ready(Err(e)) => {
                                    unreachable!("trying to block without tokio runtime: {:?}", e)
                                }
                            }
                        }};
                    }

                    let mut interrupted = false;
                    if let Some(p) = this.retry.take() {
                        // first try the thing we failed to process last time again
                        process!(*this.retry, p, |p| d.on_event(out, PollEvent::Process(p),));
                    }

                    for i in 0..FORCE_INPUT_YIELD_EVERY {
                        if !local_done && (check_local || remote_done) {
                            match this.locals.poll_recv(cx) {
                                Poll::Ready(Some(packet)) => {
                                    process!(*this.retry, packet, |p| d
                                        .on_event(out, PollEvent::Process(p),));
                                }
                                Poll::Ready(None) => {
                                    // local input stream finished
                                    // TODO: should we finish up remaining work?
                                    return Poll::Ready(());
                                }
                                Poll::Pending => {
                                    local_done = true;
                                }
                            }
                        }

                        if !remote_done && (!check_local || local_done) {
                            match this.inputs.as_mut().poll_next(cx) {
                                Poll::Ready(Some((StreamYield::Item(Ok(packet)), _))) => {
                                    process!(*this.retry, packet, |p| d
                                        .on_event(out, PollEvent::Process(p),));
                                }
                                Poll::Ready(Some((StreamYield::Finished, streami))) => {
                                    out.back.remove(&streami);
                                    out.pending.remove(&streami);
                                    // FIXME: what about if a later flush flushes to this stream?
                                }
                                Poll::Ready(None) => {
                                    // we probably haven't booted yet
                                    remote_done = true;
                                }
                                Poll::Pending => {
                                    remote_done = true;
                                }
                                Poll::Ready(Some((StreamYield::Item(Err(e)), _))) => {
                                    error!(this.log, "input stream failed: {:?}", e);
                                    remote_done = true;
                                    break;
                                }
                            }
                        }

                        // alternate between input sources
                        check_local = !check_local;

                        // nothing more to do -- wait to be polled again
                        if local_done && remote_done {
                            break;
                        }

                        if i == FORCE_INPUT_YIELD_EVERY - 1 {
                            // we could keep processing inputs, but make sure we send some ACKs too!
                            interrupted = true;
                        }
                    }

                    // send to downstream
                    // TODO: send fail == exiting?
                    self.as_mut()
                        .try_flush(cx)
                        .context("downstream flush (after)")?;

                    // send acks
                    self.as_mut().try_acks(cx)?;

                    if interrupted {
                        // resume reading from our non-depleted inputs
                        continue;
                    }
                    break Poll::Pending;
                };

                // check if we now need to set a timeout
                let mut this = self.as_mut().project();
                this.out.dirty = false;
                match this.domain.on_event(this.out, PollEvent::ResumePolling) {
                    ProcessResult::KeepPolling(timeout) => {
                        if let Some(timeout) = timeout {
                            if timeout == time::Duration::new(0, 0) {
                                this.timeout.set(None);
                                *this.timed_out = true;
                            } else {
                                // TODO: how about we don't create a new timer each time?
                                this.timeout
                                    .set(Some(async_timer::oneshot::Timer::new(timeout)));
                            }

                            // we need to poll the timer to ensure we'll get woken up
                            self.as_mut().try_timeout(cx);
                        }

                        if self.out.dirty {
                            // more stuff appeared in our outboxes
                            // can't yield yet -- we need to try to send it
                            continue;
                        }
                    }
                    pr => {
                        // TODO: just have resume_polling be a method...
                        unreachable!("unexpected ResumePolling result {:?}", pr)
                    }
                }

                break readiness;
            }
        };

        if let Err(e) = ready!(r) {
            let this = self.project();
            crit!(this.log, "replica failure: {:?}", e);
        }
        Poll::Ready(())
    }
}
