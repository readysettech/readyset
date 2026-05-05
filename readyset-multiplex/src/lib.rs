//! Minimal multiplexed request/response transport for tower services.
//!
//! This crate replaces the parts of `tokio-tower::multiplex` that we use:
//! - [`Client`] — a `tower_service::Service<Req>` that ships requests to a remote peer over a
//!   `Sink + Stream` transport, attaches a tag to each request, and routes the matching tagged
//!   response back to the caller.
//! - [`server::Server`] — a future that accepts requests off a transport, dispatches them to an
//!   inner `Service` concurrently, and writes the responses back as they complete.
//! - [`TagStore`] — the trait the caller implements to assign/recover the tag carried in each
//!   request and response. The wire format and tag policy are entirely the caller's choice; this
//!   crate just orchestrates dispatch.
//!
//! Errors are box-erased ([`BoxError`]) so the public API has no transport generics. The downside
//! is callers can't pattern-match on the inner cause, but our consumers only use the `Display`
//! representation, so this trade is fine.

use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};

use futures::{Sink, SinkExt, TryStream, TryStreamExt};
use tokio::sync::{mpsc, oneshot};
use tower::load::Load;
use tower_service::Service;
use tracing::trace;

pub mod server;

/// Boxed `std::error::Error + Send + Sync` for transport failures whose specific type the caller
/// doesn't need to inspect.
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Strategy for assigning request tags and recovering them from responses.
///
/// Implementors typically maintain an internal allocator (e.g. `slab::Slab`) keyed by the
/// in-flight tag. The crate calls [`assign_tag`](Self::assign_tag) once per outbound request and
/// [`finish_tag`](Self::finish_tag) once per inbound response.
pub trait TagStore<Req, Rsp> {
    /// The tag type carried in both directions on the wire.
    type Tag: Copy + Eq + std::hash::Hash;

    /// Allocate a tag for `req`, mutating it as needed (typically writing the tag into a header
    /// field on `req` itself), and return the tag for bookkeeping.
    fn assign_tag(&mut self, req: &mut Req) -> Self::Tag;

    /// Inspect `rsp`, recover the tag the peer echoed back, free any allocator bookkeeping for
    /// it, and return the tag.
    fn finish_tag(&mut self, rsp: &Rsp) -> Self::Tag;
}

/// Errors observed by a [`Client`] talking to a remote peer.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The transport refused to accept a new request because of backpressure or an internal
    /// limit.
    #[error("transport at capacity")]
    TransportFull,
    /// The client's background dispatcher exited before the request completed.
    #[error("client dispatcher dropped")]
    ClientDropped,
    /// The peer sent a response whose tag did not match any in-flight request — protocol
    /// violation.
    #[error("response tag did not match any in-flight request")]
    Desynchronized,
    /// The transport's `Sink` half failed.
    #[error("transport send failed: {0}")]
    BrokenTransportSend(BoxError),
    /// The transport's `Stream` half failed (or yielded `None` while requests were in flight).
    #[error("transport recv failed{}", match .0 { Some(e) => format!(": {e}"), None => String::new() })]
    BrokenTransportRecv(Option<BoxError>),
}

/// A `tower_service::Service<Req>` over a multiplexed transport.
///
/// Construct with [`Client::with_error_handler`]. Each `call(req)` queues the request to a
/// background dispatcher task that owns the transport; the returned future resolves with the
/// matching response (or [`Error::ClientDropped`] if the dispatcher exited).
pub struct Client<Req, Rsp> {
    tx: mpsc::UnboundedSender<DispatchItem<Req, Rsp>>,
    /// Number of requests handed to the dispatcher that have not yet produced a response. Used
    /// for `tower::load::Load` so callers can put the client behind a `Balance`.
    in_flight: Arc<AtomicUsize>,
}

impl<Req, Rsp> fmt::Debug for Client<Req, Rsp> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Client").finish_non_exhaustive()
    }
}

impl<Req, Rsp> Clone for Client<Req, Rsp> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            in_flight: Arc::clone(&self.in_flight),
        }
    }
}

struct DispatchItem<Req, Rsp> {
    req: Req,
    reply: oneshot::Sender<Result<Rsp, Error>>,
}

impl<Req, Rsp> Client<Req, Rsp>
where
    Req: Send + 'static,
    Rsp: Send + 'static,
{
    /// Build a `Client` over `transport`, using `store` to mint and recover tags. `on_error` is
    /// invoked once if the dispatcher exits because of a transport failure or protocol violation;
    /// it is not called on a clean shutdown (transport closed with no in-flight requests).
    pub fn with_error_handler<T, S, F>(transport: T, store: S, on_error: F) -> Self
    where
        T: Sink<Req> + TryStream<Ok = Rsp> + Send + Unpin + 'static,
        <T as Sink<Req>>::Error: std::error::Error + Send + Sync + 'static,
        <T as TryStream>::Error: std::error::Error + Send + Sync + 'static,
        S: TagStore<Req, Rsp> + Send + 'static,
        S::Tag: Send,
        F: FnOnce(Error) + Send + 'static,
    {
        let (tx, rx) = mpsc::unbounded_channel();
        let in_flight = Arc::new(AtomicUsize::new(0));
        let in_flight_dispatcher = Arc::clone(&in_flight);
        tokio::spawn(async move {
            if let Err(e) =
                run_dispatcher::<T, S, Req, Rsp>(transport, store, rx, in_flight_dispatcher).await
            {
                trace!(error = %e, "multiplex dispatcher exiting with error");
                on_error(e);
            }
        });
        Self { tx, in_flight }
    }
}

impl<Req, Rsp> Load for Client<Req, Rsp> {
    type Metric = usize;

    fn load(&self) -> Self::Metric {
        self.in_flight.load(Ordering::Relaxed)
    }
}

impl<Req, Rsp> Service<Req> for Client<Req, Rsp>
where
    Req: Send + 'static,
    Rsp: Send + 'static,
{
    type Response = Rsp;
    type Error = Error;
    type Future = ResponseFuture<Rsp>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // Unbounded mpsc never blocks; the dispatcher's death surfaces as `ClientDropped` from
        // the response future.
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Req) -> Self::Future {
        let (reply, rx) = oneshot::channel();
        // Send failure means the dispatcher already exited — mirror that as `ClientDropped` on
        // the receive side, where `call`'s future will observe it.
        let _ = self.tx.send(DispatchItem { req, reply });
        ResponseFuture { rx }
    }
}

/// Future returned from [`Client::call`].
pub struct ResponseFuture<Rsp> {
    rx: oneshot::Receiver<Result<Rsp, Error>>,
}

impl<Rsp> Future for ResponseFuture<Rsp> {
    type Output = Result<Rsp, Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match Pin::new(&mut this.rx).poll(cx) {
            Poll::Ready(Ok(Ok(rsp))) => Poll::Ready(Ok(rsp)),
            Poll::Ready(Ok(Err(e))) => Poll::Ready(Err(e)),
            Poll::Ready(Err(_)) => Poll::Ready(Err(Error::ClientDropped)),
            Poll::Pending => Poll::Pending,
        }
    }
}

async fn run_dispatcher<T, S, Req, Rsp>(
    mut transport: T,
    mut store: S,
    mut incoming: mpsc::UnboundedReceiver<DispatchItem<Req, Rsp>>,
    in_flight: Arc<AtomicUsize>,
) -> Result<(), Error>
where
    T: Sink<Req> + TryStream<Ok = Rsp> + Unpin,
    <T as Sink<Req>>::Error: std::error::Error + Send + Sync + 'static,
    <T as TryStream>::Error: std::error::Error + Send + Sync + 'static,
    S: TagStore<Req, Rsp>,
{
    let mut pending: HashMap<S::Tag, oneshot::Sender<Result<Rsp, Error>>> = HashMap::new();
    let mut closed = false;

    loop {
        // `biased` so we always drain a ready response before pulling a new request. That keeps
        // the in-flight `pending` map from growing unbounded if the peer is faster than us at
        // producing responses, and gives the response future for an already-completed request a
        // chance to fire before we issue another one.
        tokio::select! {
            biased;

            // Inbound: a response (or stream end) arrived.
            recv = transport.try_next() => {
                match recv {
                    Ok(Some(rsp)) => {
                        let tag = store.finish_tag(&rsp);
                        match pending.remove(&tag) {
                            Some(reply) => {
                                in_flight.fetch_sub(1, Ordering::Relaxed);
                                let _ = reply.send(Ok(rsp));
                            }
                            None => return Err(Error::Desynchronized),
                        }
                    }
                    Ok(None) => {
                        // Peer closed its half. If we still have outstanding requests they'll
                        // see `ClientDropped` when we drop the dispatcher.
                        return Ok(());
                    }
                    Err(e) => return Err(Error::BrokenTransportRecv(Some(Box::new(e)))),
                }
            }

            // Outbound: a new request arrived from `Client::call`.
            new = incoming.recv(), if !closed => {
                match new {
                    Some(DispatchItem { mut req, reply }) => {
                        let tag = store.assign_tag(&mut req);
                        pending.insert(tag, reply);
                        in_flight.fetch_add(1, Ordering::Relaxed);
                        // `send` flushes; if the sink is at capacity this awaits, but we still
                        // resume reading responses on the next select! iteration once it
                        // unblocks. This matches tokio-tower's behaviour on a back-pressured
                        // transport.
                        if let Err(e) = transport.send(req).await {
                            return Err(Error::BrokenTransportSend(Box::new(e)));
                        }
                    }
                    None => {
                        // All `Client` handles dropped. Don't poll `incoming` again, but keep
                        // draining `transport` until in-flight responses complete.
                        closed = true;
                        if pending.is_empty() {
                            return Ok(());
                        }
                    }
                }
            }
        }
    }
}
