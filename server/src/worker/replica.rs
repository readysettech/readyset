/// Only allow processing this many inputs in a domain before we handle timer events, acks, etc.
const FORCE_INPUT_YIELD_EVERY: usize = 32;

use super::ChannelCoordinator;
use crate::coordination::CoordinationPayload;
use ahash::{AHashMap, AHashSet};
use async_bincode::AsyncDestination;
use async_timer::Oneshot;
use bincode;
use dataflow::{
    payload::SourceChannelIdentifier,
    prelude::{DataType, Executor},
    Domain, Packet, PollEvent, ProcessResult,
};
use failure::{self, Fail, ResultExt};
use futures_util::{
    sink::Sink,
    stream::{futures_unordered::FuturesUnordered, Stream},
};
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
use strawpoll::Strawpoll;
use stream_cancel::Valve;
use streamunordered::{StreamUnordered, StreamYield};
use tokio::io::{AsyncReadExt, BufReader, BufStream, BufWriter};

pub(super) type ReplicaAddr = (DomainIndex, usize);

// https://github.com/rust-lang/rust/issues/64445
type FirstByte = impl Future<Output = Result<(tokio::net::TcpStream, u8), tokio::io::Error>> + Send;

/// Read the first byte of a stream.
fn read_first_byte(mut stream: tokio::net::TcpStream) -> FirstByte {
    async move {
        let mut byte = [0; 1];
        let n = stream.read_exact(&mut byte[..]).await?;
        assert_eq!(n, 1);
        Ok((stream, byte[0]))
    }
}

#[pin_project]
pub(super) struct Replica {
    domain: Domain,
    pub(super) log: slog::Logger,

    coord: Arc<ChannelCoordinator>,

    retry: Option<Box<Packet>>,

    #[pin]
    valve: Valve,

    #[pin]
    incoming: Strawpoll<tokio::net::TcpListener>,

    #[pin]
    first_byte: FuturesUnordered<FirstByte>,

    locals: tokio::sync::mpsc::UnboundedReceiver<Box<Packet>>,

    #[pin]
    inputs: StreamUnordered<
        DualTcpStream<
            BufStream<tokio::net::TcpStream>,
            Box<Packet>,
            Tagged<LocalOrNot<Input>>,
            AsyncDestination,
        >,
    >,

    outputs: AHashMap<
        ReplicaAddr,
        (
            Box<dyn Sink<Box<Packet>, Error = bincode::Error> + Send + Unpin>,
            bool,
        ),
    >,

    #[pin]
    timeout: Strawpoll<async_timer::oneshot::Timer>,
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
            valve: valve.clone(),
            incoming: Strawpoll::from(on),
            first_byte: FuturesUnordered::new(),
            locals,
            log: log.new(o! {"id" => id}),
            inputs: Default::default(),
            outputs: Default::default(),
            out: Outboxes::new(ctrl_tx),
            timeout: Strawpoll::from(async_timer::oneshot::Timer::new(time::Duration::from_secs(
                3600,
            ))),
            timed_out: false,
        }
    }

    fn try_acks(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Result<(), failure::Error> {
        let this = self.project();

        let mut inputs = this.inputs;
        let conns = &mut this.out.connections;
        let pending = &mut this.out.pending;

        // first, queue up any additional writes we have to do
        let mut err = Vec::new();
        for &streami in &*pending {
            let conn = &mut conns[streami];
            let mut stream = Pin::new(&mut inputs[streami]);
            let mut sent = 0;

            for &tag in &conn.tag_acks {
                match stream.as_mut().poll_ready(cx) {
                    Poll::Ready(Ok(())) => {}
                    Poll::Pending => break,
                    Poll::Ready(Err(e)) => {
                        err.push(e.into());
                        break;
                    }
                }

                if let Err(e) = stream.as_mut().start_send(Tagged { tag, v: () }) {
                    // start_send shouldn't generally error
                    err.push(e.into());
                    break;
                }

                sent += 1;
            }

            let _ = conn.tag_acks.drain(0..sent).count();

            if sent > 0 {
                conn.pending_flush = true;
            }
        }

        if !err.is_empty() {
            return Err(err.swap_remove(0));
        }

        // then, try to send on any streams we may be able to
        let mut close = Vec::new();
        pending.retain(|&streami| {
            let conn = &mut conns[streami];
            if !conn.pending_flush {
                // there better be unwritten tags, otherwise why is the stream in pending?
                assert!(!conn.tag_acks.is_empty());
                return true;
            }

            let stream = &mut inputs[streami];
            match Pin::new(stream).poll_flush(cx) {
                Poll::Pending => true,
                Poll::Ready(Ok(())) => {
                    conn.pending_flush = false;
                    if inputs.is_finished(streami).unwrap()
                        && conn.unacked == 0
                        && conn.tag_acks.is_empty()
                    {
                        // no more inputs on this stream, and nothing more to flush!
                        close.push(streami);
                    }

                    // there may stil be more tags to write out
                    !conn.tag_acks.is_empty()
                }
                Poll::Ready(Err(e)) => {
                    let mut e = Some(e);
                    if let bincode::ErrorKind::Io(ref ioe) = **e.as_ref().unwrap() {
                        match ioe.kind() {
                            io::ErrorKind::BrokenPipe
                            | io::ErrorKind::NotConnected
                            | io::ErrorKind::UnexpectedEof
                            | io::ErrorKind::ConnectionAborted
                            | io::ErrorKind::ConnectionReset => {
                                // connection went away, let's not bother the user with it
                                let _ = e.take();
                            }
                            _ => {}
                        }
                    }

                    // there's no point in trying to write more things, so:
                    conn.pending_flush = false;
                    conn.unacked = 0;
                    conn.tag_acks.clear();
                    if inputs.is_finished(streami).unwrap() {
                        close.push(streami);
                    } else {
                        // the read side of this stream will probably error soon too. when it does,
                        // try_retire will likely succeed, and it will remove the connection. if an
                        // ack comes in after that, we'll just hit if again, though then take the
                        // branch above.
                    }

                    if let Some(e) = e {
                        err.push(e.into());
                    }

                    false
                }
            }
        });

        if !err.is_empty() {
            return Err(err.swap_remove(0));
        }

        for streami in close {
            if this.out.try_retire(streami) {
                // this stream has no more inputs and no more outputs
                assert!(inputs.as_mut().is_finished(streami).unwrap());
                inputs.as_mut().remove(streami);
            }
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

    fn try_new(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Result<bool, failure::Error> {
        let mut this = self.project();

        if let Poll::Ready(true) = this.valve.poll_closed(cx) {
            return Ok(false);
        } else {
            while let Poll::Ready((stream, _)) = this
                .incoming
                .as_mut()
                .poll_fn(cx, |mut i, cx| i.poll_accept(cx))
                .map_err(|e| e.context("poll_accept"))?
            {
                // we know that any new connection to a domain will first send a one-byte
                // token to indicate whether the connection is from a base or not.
                debug!(this.log, "accepted new connection"; "from" => ?stream.peer_addr().unwrap());
                this.first_byte.push(read_first_byte(stream));
            }
        }

        while let Poll::Ready(Some(r)) = this.first_byte.as_mut().poll_next(cx) {
            let (stream, tag) = match r {
                Ok((s, t)) => (s, t),
                Err(e) => {
                    if let io::ErrorKind::BrokenPipe
                    | io::ErrorKind::NotConnected
                    | io::ErrorKind::UnexpectedEof
                    | io::ErrorKind::ConnectionAborted
                    | io::ErrorKind::ConnectionReset = e.kind()
                    {
                        // connection went away right after connecting,
                        // let's not bother the user with it
                        continue;
                    }
                    Err(e).context("poll_next")?;
                    unreachable!();
                }
            };
            let is_base = tag == CONNECTION_FROM_BASE;

            debug!(this.log, "established new connection"; "base" => ?is_base);
            let slot = this.inputs.stream_entry();
            let token = slot.token();
            let epoch = if let Some(e) = this.out.connections.get_mut(token) {
                e.epoch
            } else {
                let epoch = 1;
                let t = this.out.connections.insert(ConnState {
                    unacked: 0,
                    tag_acks: Vec::new(),
                    epoch,
                    pending_flush: false,
                });
                assert_eq!(t, token);
                epoch
            };
            if let Err(e) = stream.set_nodelay(true) {
                warn!(this.log,
                      "failed to set TCP_NODELAY for new connection: {:?}", e;
                      "from" => ?stream.peer_addr().unwrap());
            }
            let tcp = if is_base {
                DualTcpStream::upgrade(
                    tokio::io::BufStream::new(stream),
                    move |Tagged { v: input, tag }| {
                        Box::new(Packet::Input {
                            inner: input,
                            src: Some(SourceChannelIdentifier { token, tag, epoch }),
                            senders: Vec::new(),
                        })
                    },
                )
            } else {
                tokio::io::BufStream::from(BufReader::with_capacity(
                    2 * 1024 * 1024,
                    BufWriter::with_capacity(4 * 1024, stream),
                ))
                .into()
            };
            slot.insert(tcp);
        }
        Ok(true)
    }

    // returns true if on_event(Timeout) was called
    fn try_timeout(self: Pin<&mut Self>, cx: &mut Context<'_>) -> bool {
        let mut processed = false;
        let mut this = self.project();

        if !this.timeout.is_expired() {
            if let Poll::Ready(()) = this.timeout.as_mut().poll(cx) {
                *this.timed_out = true;
            }
        }

        if *this.timed_out {
            *this.timed_out = false;
            this.domain.on_event(this.out, PollEvent::Timeout);
            processed = true;
        }

        processed
    }
}

struct ConnState {
    // number of unacked inputs
    unacked: usize,

    // unsent acks (value is the tag)
    tag_acks: Vec<u32>,

    // epoch counter for each stream index (since they're re-used)
    epoch: usize,

    // do we have stuff to flush
    pending_flush: bool,
}

struct Outboxes {
    // anything new to send?
    dirty: bool,

    // messages for other domains
    domains: AHashMap<ReplicaAddr, VecDeque<Box<Packet>>>,

    // connection state for each stream
    connections: slab::Slab<ConnState>,

    // which connections have pending writes
    pending: AHashSet<usize>,

    // for sending messages to the controller
    ctrl_tx: tokio::sync::mpsc::UnboundedSender<CoordinationPayload>,
}

impl Outboxes {
    fn new(ctrl_tx: tokio::sync::mpsc::UnboundedSender<CoordinationPayload>) -> Self {
        let mut connections = slab::Slab::new();

        // index 0 is reserved
        connections.insert(ConnState {
            unacked: 0,
            tag_acks: Vec::new(),
            epoch: 0,
            pending_flush: false,
        });

        Outboxes {
            domains: Default::default(),
            connections,
            pending: Default::default(),
            ctrl_tx,
            dirty: false,
        }
    }

    fn saw_input(&mut self, token: usize, epoch: usize) {
        let mut c = &mut self.connections[token];
        if c.epoch == epoch {
            c.unacked += 1;
        }
    }

    fn try_retire(&mut self, streami: usize) -> bool {
        let mut c = &mut self.connections[streami];
        if c.unacked == 0 && c.tag_acks.is_empty() && !c.pending_flush {
            // nothing more to send back on this connection -- fine to retire!
            // increment the epoch to detect stale acks
            c.epoch += 1;

            // no unwritten tags and not pending flush, so we shouldn't be in pending
            assert!(!self.pending.contains(&streami));

            // NOTE: the ConnState will be re-used for another connection
            true
        } else {
            false
        }
    }
}

impl Executor for Outboxes {
    fn ack(&mut self, id: SourceChannelIdentifier) {
        self.dirty = true;
        let mut c = &mut self.connections[id.token];
        if id.epoch == c.epoch {
            // if the epoch doesn't match, the stream was closed and a new one has been established
            // note that this only matters for connections that do not wait for all acks!
            c.tag_acks.push(id.tag);

            // NOTE: it's a little sad we can't crash on underflow here.
            // it is because if a send fails, we set c.unacked = 0, and should the domain _then_
            // produce an ack, a checked underflow would fail.
            c.unacked = c.unacked.saturating_sub(1);

            // we now have stuff to send for this connection
            self.pending.insert(id.token);
        }
    }

    fn create_universe(&mut self, universe: HashMap<String, DataType>) {
        self.ctrl_tx
            .send(CoordinationPayload::CreateUniverse(universe))
            .expect("asked to send to controller, but controller has gone away");
    }

    fn send(&mut self, dest: ReplicaAddr, m: Box<Packet>) {
        self.dirty = true;
        self.domains.entry(dest).or_default().push_back(m);
    }
}

impl Future for Replica {
    type Output = Result<(), failure::Error>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        'process: loop {
            // FIXME: check if we should call update_state_sizes (every evict_every)

            // are there any new connections?
            if !self
                .as_mut()
                .try_new(cx)
                .context("check for new connections")?
            {
                // incoming socket closed -- no more clients will arrive
                return Poll::Ready(Ok(()));
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
            let mut this = self.as_mut().project();
            let d = this.domain;
            let out = this.out;

            macro_rules! process {
                ($retry:expr, $outbox:expr, $p:expr, $pp:expr) => {{
                    $retry = Some($p);
                    let retry = &mut $retry;
                    if let ProcessResult::StopPolling = {
                        let packet = retry.take().unwrap();
                        if let Packet::Input {
                            src: Some(SourceChannelIdentifier { token, epoch, .. }),
                            ..
                        } = *packet
                        {
                            $outbox.saw_input(token, epoch);
                        }
                        $pp(packet)
                    } {
                        // domain got a message to quit
                        // TODO: should we finish up remaining work?
                        return Poll::Ready(Ok(()));
                    }
                }};
            }

            if let Some(p) = this.retry.take() {
                // first try the thing we failed to process last time again
                process!(*this.retry, out, p, |p| d
                    .on_event(out, PollEvent::Process(p),));
            }

            for _ in 0..FORCE_INPUT_YIELD_EVERY {
                if !local_done && (check_local || remote_done) {
                    match this.locals.poll_recv(cx) {
                        Poll::Ready(Some(packet)) => {
                            process!(*this.retry, out, packet, |p| d
                                .on_event(out, PollEvent::Process(p),));
                        }
                        Poll::Ready(None) => {
                            // local input stream finished
                            // TODO: should we finish up remaining work?
                            warn!(this.log, "local input stream ended");
                            return Poll::Ready(Ok(()));
                        }
                        Poll::Pending => {
                            local_done = true;
                        }
                    }
                }

                if !remote_done && (!check_local || local_done) {
                    match this.inputs.as_mut().poll_next(cx) {
                        Poll::Ready(Some((StreamYield::Item(Ok(packet)), _))) => {
                            process!(*this.retry, out, packet, |p| d
                                .on_event(out, PollEvent::Process(p),));
                        }
                        Poll::Ready(Some((StreamYield::Finished(f), streami))) => {
                            if out.try_retire(streami) {
                                f.remove(this.inputs.as_mut());
                            } else {
                                // We still have responses to send, even though there are no
                                // more requests. Keep the stream around for now. We'll clean
                                // it up when the sends have finished.
                                f.keep();
                            }
                        }
                        Poll::Ready(None) => {
                            // we probably haven't booted yet
                            remote_done = true;
                        }
                        Poll::Pending => {
                            remote_done = true;
                        }
                        Poll::Ready(Some((StreamYield::Item(Err(e)), streami))) => {
                            error!(this.log, "input stream failed: {:?}", e);
                            // we want to _forcibly_ retire streami
                            this.inputs.as_mut().remove(streami);
                            let c = &mut out.connections[streami];
                            c.epoch += 1;
                            c.unacked = 0;
                            c.tag_acks.clear();
                            c.pending_flush = false;
                            out.pending.remove(&streami);
                        }
                    }
                }

                if local_done && remote_done {
                    break;
                }

                // alternate between input sources
                check_local = !check_local;
            }

            // send to downstream
            // TODO: send fail == exiting?
            self.as_mut()
                .try_flush(cx)
                .context("downstream flush (after)")?;

            // send acks
            self.as_mut().try_acks(cx)?;

            if !local_done || !remote_done {
                // we're yielding voluntarily to not block the executor and must ensure we wake
                // up again
                cx.waker().wake_by_ref();
            }

            // check if we now need to set a timeout
            self.out.dirty = false;
            loop {
                let mut this = self.as_mut().project();
                match this.domain.on_event(this.out, PollEvent::ResumePolling) {
                    ProcessResult::KeepPolling(timeout) => {
                        if let Some(timeout) = timeout {
                            if timeout == time::Duration::new(0, 0) {
                                *this.timed_out = true;
                            } else {
                                this.timeout.restart(timeout, cx.waker());
                            }

                            // we need to poll the timer to ensure we'll get woken up
                            if self.as_mut().try_timeout(cx) {
                                // a timeout occurred, so we may have to set a new timer
                                if self.out.dirty {
                                    // if we're already dirty, we'll re-do processing anyway
                                } else {
                                    // try to resume polling again
                                    continue;
                                }
                            }
                        }

                        if self.out.dirty {
                            // more stuff appeared in our outboxes
                            // can't yield yet -- we need to try to send it
                            continue 'process;
                        }
                    }
                    pr => {
                        // TODO: just have resume_polling be a method...
                        unreachable!("unexpected ResumePolling result {:?}", pr)
                    }
                }
                break;
            }

            break Poll::Pending;
        }
    }
}
