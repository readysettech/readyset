//! Multiplexed server side of the transport.
//!
//! [`Server::new`] adapts a `Sink + Stream` transport plus a `tower_service::Service` into a
//! future that pulls requests off the transport, dispatches them concurrently to the inner
//! service, and ships responses back as they complete.
//!
//! Tag round-tripping is the inner service's responsibility — this module is request-/response-
//! type agnostic. In our usage the inner service operates on `Tagged<Req>`/`Tagged<Rsp>` and
//! preserves the tag itself, so the server just needs to round-trip whole values.

use std::collections::VecDeque;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::stream::FuturesUnordered;
use futures::{Sink, StreamExt, TryStream};
use tower_service::Service;

use crate::BoxError;

/// Errors observed by a [`Server`].
#[derive(Debug, thiserror::Error)]
pub enum Error<SvcErr> {
    /// The inner service returned an error.
    #[error("service error: {0}")]
    Service(SvcErr),
    /// The transport's `Sink` half failed while shipping a response.
    #[error("transport send failed: {0}")]
    BrokenTransportSend(BoxError),
    /// The transport's `Stream` half failed.
    #[error("transport recv failed: {0}")]
    BrokenTransportRecv(BoxError),
}

/// A future that drives a multiplexed `Service` over a `Sink + Stream` transport.
///
/// Yields `Ok(())` when the transport's stream half has ended cleanly and every spawned service
/// call has finished. Yields an [`Error`] on the first transport or service failure.
pub struct Server<T, Svc>
where
    Svc: Service<<T as TryStream>::Ok>,
    T: TryStream,
{
    transport: T,
    service: Svc,
    in_flight: FuturesUnordered<Svc::Future>,
    send_queue: VecDeque<Svc::Response>,
    /// Whether `transport` has produced `None`. We still drain `in_flight` after this so any
    /// already-accepted requests get a response.
    stream_eof: bool,
}

impl<T, Svc> fmt::Debug for Server<T, Svc>
where
    Svc: Service<<T as TryStream>::Ok>,
    T: TryStream,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Server")
            .field("in_flight", &self.in_flight.len())
            .field("send_queue", &self.send_queue.len())
            .field("stream_eof", &self.stream_eof)
            .finish_non_exhaustive()
    }
}

impl<T, Svc, Req, Rsp> Server<T, Svc>
where
    T: Sink<Rsp> + TryStream<Ok = Req> + Unpin,
    Svc: Service<Req, Response = Rsp>,
{
    /// Build a `Server` over `transport` that dispatches each incoming request to `service`.
    pub fn new(transport: T, service: Svc) -> Self {
        Self {
            transport,
            service,
            in_flight: FuturesUnordered::new(),
            send_queue: VecDeque::new(),
            stream_eof: false,
        }
    }
}

impl<T, Svc, Req, Rsp> Future for Server<T, Svc>
where
    T: Sink<Rsp> + TryStream<Ok = Req> + Unpin,
    <T as Sink<Rsp>>::Error: std::error::Error + Send + Sync + 'static,
    <T as TryStream>::Error: std::error::Error + Send + Sync + 'static,
    Svc: Service<Req, Response = Rsp> + Unpin,
    Svc::Future: Unpin,
    Rsp: Unpin,
{
    type Output = Result<(), Error<Svc::Error>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        loop {
            let mut progress = false;

            // 1. Drain finished service futures into `send_queue`.
            while let Poll::Ready(Some(result)) = this.in_flight.poll_next_unpin(cx) {
                progress = true;
                match result {
                    Ok(rsp) => this.send_queue.push_back(rsp),
                    Err(e) => return Poll::Ready(Err(Error::Service(e))),
                }
            }

            // 2. Flush `send_queue` into the transport.
            while !this.send_queue.is_empty() {
                match Pin::new(&mut this.transport).poll_ready(cx) {
                    Poll::Ready(Ok(())) => {
                        let rsp = this
                            .send_queue
                            .pop_front()
                            .expect("queue non-empty in branch");
                        if let Err(e) = Pin::new(&mut this.transport).start_send(rsp) {
                            return Poll::Ready(Err(Error::BrokenTransportSend(Box::new(e))));
                        }
                        progress = true;
                    }
                    Poll::Ready(Err(e)) => {
                        return Poll::Ready(Err(Error::BrokenTransportSend(Box::new(e))));
                    }
                    Poll::Pending => break,
                }
            }
            // Best-effort flush: don't return Pending on flush alone (the next iteration will
            // pick it up if there's still something in the buffer), but propagate hard errors.
            match Pin::new(&mut this.transport).poll_flush(cx) {
                Poll::Ready(Err(e)) => {
                    return Poll::Ready(Err(Error::BrokenTransportSend(Box::new(e))));
                }
                Poll::Ready(Ok(())) | Poll::Pending => {}
            }

            // 3. Accept new requests, but only if the inner service is `poll_ready`. Holding
            //    requests back on `Pending` here is intentional — it propagates the service's
            //    backpressure all the way through to the transport buffer / the peer's TCP
            //    write window.
            if !this.stream_eof {
                match this.service.poll_ready(cx) {
                    Poll::Ready(Ok(())) => match Pin::new(&mut this.transport).try_poll_next(cx) {
                        Poll::Ready(Some(Ok(req))) => {
                            this.in_flight.push(this.service.call(req));
                            progress = true;
                        }
                        Poll::Ready(Some(Err(e))) => {
                            return Poll::Ready(Err(Error::BrokenTransportRecv(Box::new(e))));
                        }
                        Poll::Ready(None) => {
                            this.stream_eof = true;
                        }
                        Poll::Pending => {}
                    },
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(Error::Service(e))),
                    Poll::Pending => {}
                }
            }

            // 4. Termination: stream is at EOF AND no in-flight service futures AND nothing
            //    queued to send. Drive `poll_close` to `Ready` before returning so any bytes
            //    still buffered in the sink actually reach the wire — important when the peer
            //    half-closed and is still listening for late responses. A close-time error is
            //    still a clean exit (the peer hung up first); we just don't lose data on a
            //    healthy half-close.
            if this.stream_eof && this.in_flight.is_empty() && this.send_queue.is_empty() {
                return match Pin::new(&mut this.transport).poll_close(cx) {
                    Poll::Ready(_) => Poll::Ready(Ok(())),
                    Poll::Pending => Poll::Pending,
                };
            }

            if !progress {
                return Poll::Pending;
            }
        }
    }
}
