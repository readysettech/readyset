use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::collections::{HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::sync::{atomic, Arc};
use std::time::Duration;

use dataflow::payload::packets::*;
use dataflow::payload::{MaterializedState, SourceChannelIdentifier};
use dataflow::{
    BaseWriteStream, Domain, DomainReceiver, DomainRequest, Outboxes, Packet, ReplayReceiver,
};
use futures_util::sink::{Sink, SinkExt};
use futures_util::stream::StreamExt;
use futures_util::FutureExt;
use readyset_client::internal::DomainIndex;
use readyset_client::{
    KeyComparison, PacketData, PacketPayload, Tagged, CONNECTION_FROM_BASE, CONNECTION_MAGIC_NUMBER,
};
use readyset_errors::{ReadySetError, ReadySetResult};
use readyset_util::logging::{rate_limit, TCP_CONNECTION_LOG_RECEIVED_FROM_UNKNOWN_SOURCE};
use readyset_util::time_scope;
use strawpoll::Strawpoll;
use tokio::io::{AsyncReadExt, BufStream};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc, oneshot, Mutex};
use tokio_stream::wrappers::IntervalStream;
use tracing::{debug, error, info, info_span, trace, warn, Instrument, Span};

use super::{BarrierManager, ChannelCoordinator};

type Outputs = HashMap<DomainIndex, Box<dyn Sink<Packet, Error = bincode::Error> + Send + Unpin>>;

const SLOW_LOOP_THRESHOLD: Duration = Duration::from_secs(1);

// For use in the select loop.  If None, exit, otherwise check for error and continue.
macro_rules! call {
    ($call:expr) => {{
        if let Some(res) = $call {
            res?;
        } else {
            return Ok(());
        }
    }};
    ($name:literal, $op:expr, $call:expr) => {{
        let span = info_span!(target: "readyset_server::worker::replica", $name, op = ?$op);
        let _time = time_scope(span, SLOW_LOOP_THRESHOLD);
        call!($call)
    }};
}

/// Generates a monotonically incrementing u64 value to be used as a token for our connections
fn next_token() -> u64 {
    static NEXT_TOKEN: atomic::AtomicU64 = atomic::AtomicU64::new(0);
    NEXT_TOKEN.fetch_add(1, atomic::Ordering::Relaxed)
}

/// Whether a *failed* domain request should bring the whole domain down -- propagated as an error
/// out of the replica loop, which exits the domain so the controller rebuilds it -- rather than
/// only being reported back to the controller. `StartReplay` qualifies: a replay that fails to
/// start can leave a detached chunker thread we can't cancel (REA-6688), and only tearing the
/// domain down also tears that down, preventing a half-finished replay from completing behind the
/// controller's back. Every other request reports its error and the domain keeps running.
fn domain_request_is_fatal_on_error(req: &DomainRequest) -> bool {
    matches!(req, DomainRequest::StartReplay { .. })
}

/// A domain request wrapped together with the sender to which to send the processing result
pub struct WrappedDomainRequest {
    pub req: DomainRequest,
    pub done_tx: oneshot::Sender<ReadySetResult<Option<Vec<u8>>>>,
}

/// [`Replica`] is a wrapper for a [`Domain`], handling intra Domain communication and coordination
pub struct Replica {
    /// Wrapped domain
    domain: Domain,

    coord: Arc<ChannelCoordinator>,

    /// How often to update state sizes (hardcoded to 500 ms ATM, should probably change)
    /// NOTE: if `aggressively_update_state_sizes` updates will happen every packet
    refresh_sizes: IntervalStream,

    /// Incoming TCP connections from base-table writers (the replicator and any
    /// external base writers). Inter-domain traffic stays in-process via
    /// `ChannelCoordinator`.
    incoming: Strawpoll<TcpListener>,

    /// A receiver for locally sent messages
    locals: DomainReceiver,

    /// A receiver for domain messages
    requests: mpsc::Receiver<WrappedDomainRequest>,

    init_state_reqs: mpsc::Receiver<MaterializedState>,

    /// Bounded channel for receiving chunked replay packets from the replay thread.
    /// Provides backpressure so the replay thread doesn't outrun the domain.
    replay_rx: ReplayReceiver,

    /// Stores pending outgoing messages
    out: Outboxes,

    /// Worker's barrier accounting. Barrier credits the domain accumulates in `out` are
    /// drained into this directly each select-loop iteration.
    barriers: Arc<BarrierManager>,
}

impl Replica {
    pub(super) fn new(
        domain: Domain,
        on: TcpListener,
        locals: DomainReceiver,
        requests: mpsc::Receiver<WrappedDomainRequest>,
        init_state_reqs: mpsc::Receiver<MaterializedState>,
        replay_rx: ReplayReceiver,
        cc: Arc<ChannelCoordinator>,
        barriers: Arc<BarrierManager>,
    ) -> Self {
        Replica {
            coord: cc,
            domain,
            incoming: Strawpoll::from(on),
            locals,
            out: Outboxes::new(),
            refresh_sizes: IntervalStream::new(tokio::time::interval(Duration::from_millis(500))),
            requests,
            init_state_reqs,
            replay_rx,
            barriers,
        }
    }
}

/// Merge multiple [`RequestReaderReplay`] packets into a single packet
fn flatten_request_reader_replay(
    n: readyset_client::internal::LocalNodeIndex,
    c: &[usize],
    unique_keys: &mut HashSet<KeyComparison>,
    packets: &mut VecDeque<Packet>,
) {
    let mut i = 0;
    while i < packets.len() {
        match packets.get_mut(i) {
            Some(Packet::RequestReaderReplay(RequestReaderReplay {
                node, cols, keys, ..
            })) if *node == n && *cols == c => {
                unique_keys.extend(keys.drain(..));
                packets.remove(i);
            }
            _ => i += 1,
        }
    }
}

impl Replica {
    fn span(&self) -> Span {
        info_span!(
            // Target readyset_dataflow::domain rather than readyset_server::worker::replica so
            // that a log level of `readyset_dataflow=trace` still logs the domain address
            target: "readyset_dataflow::domain",
            "domain",
            address = %self.domain.address(),
        )
    }

    /// Read the first 4 bytes of a connection to determine if it has the correct magic
    /// number, and then read the next byte to confirm it is a base-table connection
    /// (the only kind that should arrive at this listener now that inter-domain TCP is
    /// gone). Upgrade to a `BaseWriteStream` and return a unique token plus the upgraded
    /// connection.
    async fn handle_new_connection(
        mut stream: TcpStream,
    ) -> ReadySetResult<(u64, BaseWriteStream)> {
        let mut magic: [u8; 4] = [0; 4];
        stream.read_exact(&mut magic).await?;
        if magic != CONNECTION_MAGIC_NUMBER {
            rate_limit(
                true,
                TCP_CONNECTION_LOG_RECEIVED_FROM_UNKNOWN_SOURCE,
                || {
                    warn!(
                "Replica received connection from unknown source: Magic: {:?} Address: {:?}",
                magic,
                    stream.peer_addr().unwrap()
                );
                },
            );

            return Err(ReadySetError::TcpSendError(
                "Replica received connection from unknown source".to_string(),
            ));
        }

        let mut tag: u8 = 0;
        stream.read_exact(std::slice::from_mut(&mut tag)).await?;
        if tag != CONNECTION_FROM_BASE {
            return Err(ReadySetError::TcpSendError(format!(
                "Replica received non-base connection (tag={tag}); only base-table \
                 writers should connect here in single-worker mode"
            )));
        }

        debug!("established new base-table connection");

        let token = next_token();
        let _ = stream.set_nodelay(true);

        let tcp = BaseWriteStream::upgrade(BufStream::new(stream), move |Tagged { v, tag }| {
            let input: PacketData = v;
            // Peek at its type.
            match input.data {
                PacketPayload::Input(_) => Packet::Input(Input {
                    inner: input,
                    src: SourceChannelIdentifier { token, tag },
                }),
            }
        });

        Ok((token, tcp))
    }

    /// Receive packets from local and remote connections
    async fn receive_packets(
        locals: &mut DomainReceiver,
        connections: &mut tokio_stream::StreamMap<u64, BaseWriteStream>,
    ) -> ReadySetResult<Option<VecDeque<Packet>>> {
        const MAX_PACKETS_PER_CALL: usize = 64;

        let mut packets = VecDeque::with_capacity(MAX_PACKETS_PER_CALL);

        // Read packets from either the local connection or the remote connections, tokio
        // select order is random, so if both are ready some degree of forward progress will
        // happen for both
        tokio::select! {
            local_req = locals.recv() => match local_req {
                None => return Ok(None),
                Some(packet) => {
                    packets.push_back(packet);
                    // Try to read more packets without waiting
                    while let Some(packet) = locals.recv().now_or_never() {
                        match packet {
                            None => {
                                return Ok(None)
                            }
                            Some(packet) => {
                                packets.push_back(packet);
                                if packets.len() >= MAX_PACKETS_PER_CALL {
                                    break;
                                }
                            }
                        }
                    }
                }
            },

            Some((_, packet)) = connections.next() => {
                let packet = packet?;
                packets.push_back(packet);
                // Try to read more packets without waiting
                while let Some(Some((_, packet))) = connections.next().now_or_never() {
                    packets.push_back(packet?);
                    if packets.len() > MAX_PACKETS_PER_CALL {
                        break;
                    }
                }
            }
        }

        Ok(Some(packets))
    }

    /// Sends response packets asynchronously, the future takes ownership of a set of packets and
    /// their destination, therefore it can't be dropped before completion without risking some
    /// packets being lost
    async fn send_packets(
        to_send: Vec<(DomainIndex, VecDeque<Packet>)>,
        connections: &tokio::sync::Mutex<Outputs>,
        coord: &ChannelCoordinator,
        failed: &Mutex<HashSet<SocketAddr>>,
    ) -> ReadySetResult<()> {
        let mut lock = connections.lock().await;

        let connections = &mut *lock;
        for (domain, mut messages) in to_send {
            if messages.is_empty() {
                continue;
            }

            let tx = match connections.entry(domain) {
                Occupied(entry) => {
                    trace!(%domain, "Reusing existing domain connection");
                    entry.into_mut()
                }
                Vacant(entry) => {
                    let Some(addr) = coord.get_addr(&domain) else {
                        trace!(
                            target = %domain,
                            num_messages = messages.len(),
                            "Missing channel for domain, dropping messages"
                        );
                        continue;
                    };

                    if failed.lock().await.contains(&addr) {
                        warn!(
                            target = %domain,
                            num_messages = messages.len(),
                            "Skipping packets to domain as it may have failed"
                        );
                        continue;
                    }

                    debug!(%domain, %addr, "Establishing connection to domain");
                    entry.insert(coord.connect_to(&domain)?)
                }
            };

            // Send the messages to the sink associated with each target domain.
            // If an error occurs when performing `feed` or `flush`, add the address
            // for the domain to the failed set, remove the channel from the local
            // channel coordinator, and skip the remaining packets.
            //
            // The next time a batch of packets is sent to a domain, a new connection
            // will be established if the domain has been repaired.
            let mut send_err = false;
            while let Some(m) = messages.pop_front() {
                if let Err(e) = tx.feed(m).await {
                    error!(err = ?e, "Error on feed.");
                    send_err = true;
                    break;
                }
            }

            if !send_err {
                if let Err(e) = tx.flush().await {
                    error!(err = ?e, "Error on flush.");
                    send_err = true;
                }
            }

            if send_err {
                connections.remove(&domain);
                if let Some(addr) = coord.get_addr(&domain) {
                    failed.lock().await.insert(addr);
                } else {
                    warn!(target = ?domain, "Failure on sending packet to domain")
                }
            }
        }
        Ok(())
    }

    async fn handle_address_change(
        outputs: &Mutex<Outputs>,
        recv: Result<DomainIndex, broadcast::error::RecvError>,
    ) {
        match recv {
            Ok(domain) => {
                // We've received a notification that the socket address for a domain
                // has changed - remove its cached connection from `outputs` so that
                // when we try to send to it later we re-lookup the addr and reconnect
                if outputs.lock().await.remove(&domain).is_some() {
                    info!(%domain, "Removed connection for domain");
                }
            }
            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                // If we've lagged behind, that means we've missed some changes to
                // domain addresses, so we need to consider all connections invalid
                warn!(
                    %skipped,
                    "Coordinator change broadcast receiver lagged behind; reconnecting \
                    to all domains"
                );
                outputs.lock().await.clear();
            }
            Err(broadcast::error::RecvError::Closed) => {
                panic!("ChannelCoordinator dropped!");
            }
        }
    }

    fn handle_domain_request(
        domain: &mut Domain,
        req: Option<WrappedDomainRequest>,
        out: &mut Outboxes,
    ) -> Option<ReadySetResult<()>> {
        match req {
            Some(req) => {
                let fatal_on_error = domain_request_is_fatal_on_error(&req.req);
                let res = domain.domain_request(req.req, out);
                // Report the error to the controller as usual, then, for a fatal request, propagate
                // it to end the replica loop and bring the domain down.
                let fatal = res.as_ref().err().filter(|_| fatal_on_error).cloned();
                if req.done_tx.send(res).is_err() {
                    warn!("domain request sender hung up");
                }
                match fatal {
                    Some(error) => Some(Err(error)),
                    None => Some(Ok(())),
                }
            }
            None => {
                warn!("domain request stream ended");
                None
            }
        }
    }

    fn handle_state_request(
        domain: &mut Domain,
        state: Option<MaterializedState>,
    ) -> Option<ReadySetResult<()>> {
        match state {
            Some(MaterializedState { node, state }) => {
                Some(domain.process_state_for_node(node, *state))
            }
            None => {
                warn!("domain state initialization stream ended");
                None
            }
        }
    }

    async fn handle_one_packet(
        packets: &mut VecDeque<Packet>,
        established: &mut tokio_stream::StreamMap<u64, BaseWriteStream>,
        domain: &mut Domain,
        out: &mut Outboxes,
        mut packet: Packet,
    ) -> Option<ReadySetResult<()>> {
        let span = info_span!(
            target: "readyset_server::worker::replica",
            "handle_one_packet",
            op = ?packet,
        );
        let _time = time_scope(span, SLOW_LOOP_THRESHOLD);

        let ack = match &mut packet {
            Packet::Input(Input {
                src: SourceChannelIdentifier { token, tag },
                ..
            }) => {
                // After processing we need to ack timestamp and input messages from
                // base
                established
                    .iter_mut()
                    .find(|(t, _)| *t == *token)
                    .map(|(_, conn)| (*tag, conn))
            }
            Packet::RequestReaderReplay(RequestReaderReplay {
                node, cols, keys, ..
            }) => {
                // We want to batch multiple reader replay requests into a single call
                // while deduplicating non unique keys
                let mut unique_keys = keys.drain(..).collect();
                flatten_request_reader_replay(*node, cols, &mut unique_keys, packets);
                keys.extend(unique_keys.drain());
                None
            }
            _ => None,
        };

        if let Err(e) = domain.handle_packet(packet, out) {
            return Some(Err(e));
        }

        if let Some((tag, conn)) = ack {
            if let Err(e) = conn.send(Tagged { tag, v: () }).await {
                return Some(Err(e.into()));
            }
        }

        Some(Ok(()))
    }

    async fn handle_packets(
        packets: Option<VecDeque<Packet>>,
        established: &mut tokio_stream::StreamMap<u64, BaseWriteStream>,
        domain: &mut Domain,
        out: &mut Outboxes,
    ) -> Option<ReadySetResult<()>> {
        match packets {
            None => {
                warn!("local input stream ended");
                None
            }
            Some(mut packets) => {
                while let Some(pkt) = packets.pop_front() {
                    let res =
                        Self::handle_one_packet(&mut packets, established, domain, out, pkt).await;
                    if res != Some(Ok(())) {
                        return res;
                    }
                }
                Some(Ok(()))
            }
        }
    }

    fn sleep(domain: &Domain) -> tokio::time::Sleep {
        tokio::time::sleep(
            domain
                .next_poll_duration()
                .unwrap_or_else(|| Duration::from_secs(3600)),
        )
    }

    /// Start the event loop for a Replica
    pub async fn run_inner(mut self) -> ReadySetResult<()> {
        let mut accepted = futures::stream::FuturesUnordered::new(); // conns being upgraded
        let mut established = Default::default();
        let mut channel_changes = self.coord.subscribe();

        // A cache of established connections to other Replicas we may send messages to. Sadly
        // have to use Mutex here to make it possible to pass a mutable reference to outputs
        // to an async function.
        let outputs = Mutex::new(Outputs::default());
        let failed = Default::default();

        // Will only ever hold a single future that handles sending packets, wrap it for convenience
        // into FuturesUnordered in general we don't want to drop the future for
        // send_packets until it finished, otherwise some packets might get lost. So the
        // solution for that is to do a `pin_mut` on a `Fuse` future (which requires first issuing a
        // fake call) and then keeping it updated once `Fuse` reports `terminated`, or
        // instead simply using `FuturesUnordered` with one entry, and adding the next
        // future when it is empty.
        let mut send_packets = futures::stream::FuturesUnordered::new();

        let Replica {
            domain,
            coord,
            refresh_sizes,
            incoming,
            locals,
            requests,
            out,
            init_state_reqs,
            replay_rx,
            barriers,
        } = &mut self;

        loop {
            // We have three logical input sources: receives from local domains, receives from
            // remote domains, and remote mutators. If adding new statements, make sure they
            // are cancellation safe.  https://docs.rs/tokio/latest/tokio/macro.select.html
            tokio::select! {
                // Accept incoming connections
                res = incoming.accept() => {
                    let (conn, addr) = res?;
                    debug!(from = ?addr, "accepted new connection");
                    accepted.push(Self::handle_new_connection(conn));
                },

                // Accepted but still converting to BaseWriteStream
                Some(established_conn) = accepted.next() => {
                    let (token, tcp) = match established_conn {
                        Err(_) => continue, // errors on unestablished connections don't matter
                        Ok(tcp) => tcp,
                    };
                    established.insert(token, tcp);
                },

                // Changes to the addresses of individual domains
                recv = channel_changes.recv() => {
                    Self::handle_address_change(&outputs, recv).await;
                }

                // Domain requests
                req = requests.recv() => call!(
                    "handle_domain_request",
                    req.as_ref().map(|x| x.req.clone()),
                    Self::handle_domain_request(domain, req, out)
                ),

                state = init_state_reqs.recv() => call!(
                    "handle_state_request",
                    state.as_ref().map(|x| x.node),
                    Self::handle_state_request(domain, state)
                ),

                // Handle incoming messages
                packets = Self::receive_packets(locals, &mut established) => call!(
                    "handle_packets",
                    packets.as_ref().map_or(None, |x| x.as_ref().map(|x| x.len())),
                    Self::handle_packets(packets?, &mut established, domain, out).await
                ),

                // Receive chunked replay packets from the bounded replay channel
                Some(packet) = replay_rx.recv() => {
                    let mut replay_packets = VecDeque::with_capacity(1);
                    replay_packets.push_back(packet);
                    call!(
                        "handle_packets",
                        Some(replay_packets.len()),
                        Self::handle_packets(
                            Some(replay_packets),
                            &mut established,
                            domain,
                            out,
                        ).await
                    )
                },

                // Poll the send packets future and reissue if outstanding packets are present
                Some(res) = send_packets.next() => res?,

                // Update domain sizes when `refresh_sizes` expires
                Some(_) = refresh_sizes.next() => domain.update_state_sizes(),

                // Wait for a possible sleep
                _ = Self::sleep(domain) => domain.handle_timeout()?,
            }

            // If the previous batch of send packets is done, issue a new batch if needed
            if send_packets.is_empty() && out.have_messages() {
                let to_send = out.take_messages();
                send_packets.push(Self::send_packets(to_send, &outputs, coord, &failed));
            }

            // Return any accumulated barrier credits to the worker's BarrierManager in-process.
            if out.have_barrier_credits() {
                for credit in out.take_barrier_credits() {
                    barriers.add(credit).await;
                }
            }
        }
    }

    pub async fn run(self) -> ReadySetResult<()> {
        let span = self.span();
        self.run_inner().instrument(span).await
    }
}

#[cfg(test)]
mod tests {
    use common::Tag;
    use readyset_client::internal::LocalNodeIndex;

    use super::*;

    #[test]
    fn only_start_replay_failure_brings_down_the_domain() {
        let start_replay = DomainRequest::StartReplay {
            tag: Tag::new(0),
            from: LocalNodeIndex::make(0),
            targeting_domain: DomainIndex::new(0),
        };
        assert!(domain_request_is_fatal_on_error(&start_replay));

        // A failed QueryReplayDone is reported to the controller; the domain keeps running.
        let other = DomainRequest::QueryReplayDone {
            node: LocalNodeIndex::make(0),
        };
        assert!(!domain_request_is_fatal_on_error(&other));
    }
}
