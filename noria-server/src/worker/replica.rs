/// Only allow processing this many inputs in a domain before we handle timer events, acks, etc.
const FORCE_INPUT_YIELD_EVERY: usize = 64;

use super::ChannelCoordinator;
use crate::coordination::CoordinationPayload;
use async_bincode::AsyncDestination;
use bincode;
use bufstream::BufStream;
use dataflow::{
    payload::SourceChannelIdentifier,
    prelude::{DataType, Executor},
    Domain, Packet, PollEvent, ProcessResult,
};
use failure::{self, ResultExt};
use fnv::{FnvHashMap, FnvHashSet};
use futures::stream::futures_unordered::FuturesUnordered;
use futures::{self, Future, Sink, Stream};
use noria::channel::{DualTcpStream, CONNECTION_FROM_BASE};
use noria::internal::DomainIndex;
use noria::internal::LocalOrNot;
use noria::{Input, Tagged};
use slog;
use std::collections::{HashMap, VecDeque};
use std::io;
use std::sync::Arc;
use std::time;
use stream_cancel::{Valve, Valved};
use streamunordered::{StreamUnordered, StreamYield};
use tokio;
use tokio::prelude::*;

pub(super) type ReplicaIndex = (DomainIndex, usize);

pub(super) struct Replica {
    domain: Domain,
    log: slog::Logger,

    coord: Arc<ChannelCoordinator>,

    incoming: Valved<tokio::net::tcp::Incoming>,
    first_byte: FuturesUnordered<tokio::io::ReadExact<tokio::net::tcp::TcpStream, Vec<u8>>>,
    locals: futures::sync::mpsc::UnboundedReceiver<Box<Packet>>,
    inputs: StreamUnordered<
        DualTcpStream<
            BufStream<tokio::net::TcpStream>,
            Box<Packet>,
            Tagged<LocalOrNot<Input>>,
            AsyncDestination,
        >,
    >,
    outputs: FnvHashMap<
        ReplicaIndex,
        (
            Box<dyn Sink<SinkItem = Box<Packet>, SinkError = bincode::Error> + Send>,
            bool,
        ),
    >,

    outbox: FnvHashMap<ReplicaIndex, VecDeque<Box<Packet>>>,
    timeout: Option<tokio::timer::Delay>,
    oob: OutOfBand,
}

impl Replica {
    pub(super) fn new(
        valve: &Valve,
        mut domain: Domain,
        on: tokio::net::TcpListener,
        locals: futures::sync::mpsc::UnboundedReceiver<Box<Packet>>,
        ctrl_tx: futures::sync::mpsc::UnboundedSender<CoordinationPayload>,
        log: slog::Logger,
        cc: Arc<ChannelCoordinator>,
    ) -> Self {
        let id = domain.id();
        let id = format!("{}.{}", id.0.index(), id.1);
        domain.booted(on.local_addr().unwrap());
        Replica {
            coord: cc,
            domain,
            incoming: valve.wrap(on.incoming()),
            first_byte: FuturesUnordered::new(),
            locals,
            log: log.new(o! {"id" => id}),
            inputs: Default::default(),
            outputs: Default::default(),
            outbox: Default::default(),
            oob: OutOfBand::new(ctrl_tx),
            timeout: None,
        }
    }

    fn try_oob(&mut self) -> Result<(), failure::Error> {
        let inputs = &mut self.inputs;
        let pending = &mut self.oob.pending;

        // first, queue up any additional writes we have to do
        let mut err = Vec::new();
        self.oob.back.retain(|&streami, tags| {
            let stream = &mut inputs[streami];

            let had = tags.len();
            tags.retain(|&tag| {
                match stream.start_send(Tagged { tag, v: () }) {
                    Ok(AsyncSink::Ready) => false,
                    Ok(AsyncSink::NotReady(_)) => {
                        // TODO: also break?
                        true
                    }
                    Err(e) => {
                        // start_send shouldn't generally error
                        err.push(e.into());
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
            match stream.poll_complete() {
                Ok(Async::Ready(())) => false,
                Ok(Async::NotReady) => true,
                Err(box bincode::ErrorKind::Io(e)) => {
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
                Err(e) => {
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

    fn try_flush(&mut self) -> Result<(), failure::Error> {
        let cc = &self.coord;
        let outputs = &mut self.outputs;

        // just like in try_oob:
        // first, queue up any additional writes we have to do
        let mut err = Vec::new();
        for (&ri, ms) in &mut self.outbox {
            if ms.is_empty() {
                continue;
            }

            let &mut (ref mut tx, ref mut pending) = outputs.entry(ri).or_insert_with(|| {
                while !cc.has(&ri) {}
                let tx = cc.builder_for(&ri).unwrap().build_async().unwrap();
                (tx, true)
            });

            while let Some(m) = ms.pop_front() {
                match tx.start_send(m) {
                    Ok(AsyncSink::Ready) => {
                        // we queued something, so we'll need to send!
                        *pending = true;
                    }
                    Ok(AsyncSink::NotReady(m)) => {
                        // put back the m we tried to send
                        ms.push_front(m);
                        // there's also no use in trying to enqueue more packets
                        break;
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

            match tx.poll_complete() {
                Ok(Async::Ready(())) => {
                    *pending = false;
                }
                Ok(Async::NotReady) => {}
                Err(e) => err.push(e),
            }
        }

        if !err.is_empty() {
            return Err(err.swap_remove(0).into());
        }

        Ok(())
    }

    fn try_new(&mut self) -> io::Result<bool> {
        while let Async::Ready(stream) = self.incoming.poll()? {
            match stream {
                Some(stream) => {
                    // we know that any new connection to a domain will first send a one-byte
                    // token to indicate whether the connection is from a base or not.
                    debug!(self.log, "accepted new connection"; "from" => ?stream.peer_addr().unwrap());
                    self.first_byte
                        .push(tokio::io::read_exact(stream, vec![0; 1]));
                }
                None => {
                    return Ok(false);
                }
            }
        }

        while let Async::Ready(Some((stream, tag))) = self.first_byte.poll()? {
            let is_base = tag[0] == CONNECTION_FROM_BASE;

            debug!(self.log, "established new connection"; "base" => ?is_base);
            let slot = self.inputs.stream_slot();
            let token = slot.token();
            if let Err(e) = stream.set_nodelay(true) {
                warn!(self.log,
                      "failed to set TCP_NODELAY for new connection: {:?}", e;
                      "from" => ?stream.peer_addr().unwrap());
            }
            let tcp = if is_base {
                DualTcpStream::upgrade(BufStream::new(stream), move |Tagged { v: input, tag }| {
                    Box::new(Packet::Input {
                        inner: input,
                        src: Some(SourceChannelIdentifier { token, tag }),
                        senders: Vec::new(),
                    })
                })
            } else {
                BufStream::with_capacities(2 * 1024 * 1024, 4 * 1024, stream).into()
            };
            slot.insert(tcp);
        }
        Ok(true)
    }

    fn try_timeout(&mut self) -> Poll<(), tokio::timer::Error> {
        if let Some(mut to) = self.timeout.take() {
            if let Async::Ready(()) = to.poll()? {
                crate::block_on(|| {
                    self.domain
                        .on_event(&mut self.oob, PollEvent::Timeout, &mut self.outbox)
                });
                return Ok(Async::Ready(()));
            } else {
                self.timeout = Some(to);
            }
        }
        Ok(Async::NotReady)
    }
}

struct OutOfBand {
    // map from inputi to number of (empty) ACKs
    back: FnvHashMap<usize, Vec<u32>>,
    pending: FnvHashSet<usize>,

    // for sending messages to the controller
    ctrl_tx: futures::sync::mpsc::UnboundedSender<CoordinationPayload>,
}

impl OutOfBand {
    fn new(ctrl_tx: futures::sync::mpsc::UnboundedSender<CoordinationPayload>) -> Self {
        OutOfBand {
            back: Default::default(),
            pending: Default::default(),
            ctrl_tx,
        }
    }
}

impl Executor for OutOfBand {
    fn ack(&mut self, id: SourceChannelIdentifier) {
        self.back.entry(id.token).or_default().push(id.tag);
    }

    fn create_universe(&mut self, universe: HashMap<String, DataType>) {
        self.ctrl_tx
            .unbounded_send(CoordinationPayload::CreateUniverse(universe))
            .expect("asked to send to controller, but controller has gone away");
    }
}

impl Future for Replica {
    type Item = ();
    type Error = ();
    fn poll(&mut self) -> Result<Async<Self::Item>, Self::Error> {
        let r: Result<Async<Self::Item>, failure::Error> = try {
            loop {
                // FIXME: check if we should call update_state_sizes (every evict_every)

                // are there are any new connections?
                if !self.try_new().context("check for new connections")? {
                    // incoming socket closed -- no more clients will arrive
                    return Ok(Async::Ready(()));
                }

                // have any of our timers expired?
                self.try_timeout().context("check timeout")?;

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
                let readiness = loop {
                    let mut interrupted = false;
                    for i in 0..FORCE_INPUT_YIELD_EVERY {
                        if !local_done && (check_local || remote_done) {
                            match self.locals.poll() {
                                Ok(Async::Ready(Some(packet))) => {
                                    let d = &mut self.domain;
                                    let oob = &mut self.oob;
                                    let ob = &mut self.outbox;

                                    if let ProcessResult::StopPolling = crate::block_on(|| {
                                        d.on_event(oob, PollEvent::Process(packet), ob)
                                    }) {
                                        // domain got a message to quit
                                        // TODO: should we finish up remaining work?
                                        return Ok(Async::Ready(()));
                                    }
                                }
                                Ok(Async::Ready(None)) => {
                                    // local input stream finished?
                                    // TODO: should we finish up remaining work?
                                    return Ok(Async::Ready(()));
                                }
                                Ok(Async::NotReady) => {
                                    local_done = true;
                                }
                                Err(e) => {
                                    error!(self.log, "local input stream failed: {:?}", e);
                                    local_done = true;
                                    break;
                                }
                            }
                        }

                        if !remote_done && (!check_local || local_done) {
                            match self.inputs.poll() {
                                Ok(Async::Ready(Some((StreamYield::Item(packet), _)))) => {
                                    let d = &mut self.domain;
                                    let oob = &mut self.oob;
                                    let ob = &mut self.outbox;

                                    if let ProcessResult::StopPolling = crate::block_on(|| {
                                        d.on_event(oob, PollEvent::Process(packet), ob)
                                    }) {
                                        // domain got a message to quit
                                        // TODO: should we finish up remaining work?
                                        return Ok(Async::Ready(()));
                                    }
                                }
                                Ok(Async::Ready(Some((
                                    StreamYield::Finished(_stream),
                                    streami,
                                )))) => {
                                    self.oob.back.remove(&streami);
                                    self.oob.pending.remove(&streami);
                                    // FIXME: what about if a later flush flushes to this stream?
                                }
                                Ok(Async::Ready(None)) => {
                                    // we probably haven't booted yet
                                    remote_done = true;
                                }
                                Ok(Async::NotReady) => {
                                    remote_done = true;
                                }
                                Err(e) => {
                                    error!(self.log, "input stream failed: {:?}", e);
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
                    self.try_flush().context("downstream flush (after)")?;

                    // send acks
                    self.try_oob()?;

                    if interrupted {
                        // resume reading from our non-depleted inputs
                        continue;
                    }
                    break Async::NotReady;
                };

                // check if we now need to set a timeout
                match self.domain.on_event(
                    &mut self.oob,
                    PollEvent::ResumePolling,
                    &mut self.outbox,
                ) {
                    ProcessResult::KeepPolling(timeout) => {
                        if let Some(timeout) = timeout {
                            self.timeout =
                                Some(tokio::timer::Delay::new(time::Instant::now() + timeout));

                            // we need to poll the delay to ensure we'll get woken up
                            if let Async::Ready(()) =
                                self.try_timeout().context("check timeout after setting")?
                            {
                                // the timer expired and we did some stuff
                                // make sure we don't return while there's more work to do
                                continue;
                            }
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

        match r {
            Ok(k) => Ok(k),
            Err(e) => {
                crit!(self.log, "replica failure: {:?}", e);
                Err(())
            }
        }
    }
}
