//! Integration tests for `readyset-multiplex`.
//!
//! Each test pairs a `Client` and a `server::Server` over an in-memory `tokio::io::duplex` pipe
//! framed by a hand-written length-delimited codec, exercising the dispatcher logic without
//! pulling in a serialization crate.

use std::collections::VecDeque;
use std::convert::Infallible;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use futures::{Sink, Stream, StreamExt};
use readyset_multiplex::{Client, Error, TagStore, server};
use tokio::sync::{Mutex, mpsc};
use tower::ServiceExt;
use tower_service::Service;

/// A minimal tagged message: `tag` is the multiplex correlation id, `payload` is the actual data.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Msg<T> {
    tag: u32,
    payload: T,
}

/// `TagStore` impl that allocates monotonically increasing tags and tracks live ones.
#[derive(Default)]
struct Counter {
    next: u32,
    live: std::collections::HashSet<u32>,
}

impl<T> TagStore<Msg<T>, Msg<T>> for Counter {
    type Tag = u32;

    fn assign_tag(&mut self, req: &mut Msg<T>) -> u32 {
        let tag = self.next;
        self.next = self.next.wrapping_add(1);
        self.live.insert(tag);
        req.tag = tag;
        tag
    }

    fn finish_tag(&mut self, rsp: &Msg<T>) -> u32 {
        self.live.remove(&rsp.tag);
        rsp.tag
    }
}

/// Bidirectional in-memory transport: a pair of `mpsc` channels carrying `Msg<T>` values.
struct DuplexEnd<T> {
    tx: mpsc::UnboundedSender<Msg<T>>,
    rx: mpsc::UnboundedReceiver<Msg<T>>,
    /// If set, every `start_send` returns this error instead of forwarding to `tx`.
    fail_send: Arc<AtomicBool>,
}

impl<T> Sink<Msg<T>> for DuplexEnd<T> {
    type Error = TransportError;

    fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, item: Msg<T>) -> Result<(), Self::Error> {
        if self.fail_send.load(Ordering::SeqCst) {
            return Err(TransportError::Send);
        }
        self.tx.send(item).map_err(|_| TransportError::PeerClosed)
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}

impl<T> Stream for DuplexEnd<T> {
    type Item = Result<Msg<T>, TransportError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match this.rx.poll_recv(cx) {
            Poll::Ready(Some(m)) => Poll::Ready(Some(Ok(m))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[derive(Debug, thiserror::Error)]
enum TransportError {
    #[error("send disabled (test fault injection)")]
    Send,
    #[error("peer closed")]
    PeerClosed,
}

fn duplex<T>() -> (DuplexEnd<T>, DuplexEnd<T>) {
    let (a_to_b, a_to_b_rx) = mpsc::unbounded_channel();
    let (b_to_a, b_to_a_rx) = mpsc::unbounded_channel();
    let a = DuplexEnd {
        tx: a_to_b,
        rx: b_to_a_rx,
        fail_send: Arc::new(AtomicBool::new(false)),
    };
    let b = DuplexEnd {
        tx: b_to_a,
        rx: a_to_b_rx,
        fail_send: Arc::new(AtomicBool::new(false)),
    };
    (a, b)
}

/// Echo service: returns the request unchanged, preserving its tag.
struct Echo;

impl Service<Msg<u32>> for Echo {
    type Response = Msg<u32>;
    type Error = Infallible;
    type Future =
        std::pin::Pin<Box<dyn std::future::Future<Output = Result<Msg<u32>, Infallible>> + Send>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Msg<u32>) -> Self::Future {
        Box::pin(async move { Ok(req) })
    }
}

#[tokio::test]
async fn round_trip_single_request() {
    let (client_end, server_end) = duplex::<u32>();
    let mut client =
        Client::<Msg<u32>, Msg<u32>>::with_error_handler(client_end, Counter::default(), |e| {
            panic!("dispatcher exited: {e}")
        });
    tokio::spawn(server::Server::new(server_end, Echo));

    let rsp = client
        .ready()
        .await
        .unwrap()
        .call(Msg {
            tag: 0,
            payload: 42,
        })
        .await
        .unwrap();
    assert_eq!(rsp.payload, 42);
}

#[tokio::test]
async fn round_trip_many_concurrent_requests() {
    let (client_end, server_end) = duplex::<u32>();
    let client =
        Client::<Msg<u32>, Msg<u32>>::with_error_handler(client_end, Counter::default(), |e| {
            panic!("dispatcher exited: {e}")
        });
    tokio::spawn(server::Server::new(server_end, Echo));

    let mut handles = Vec::new();
    for i in 0..256u32 {
        let mut c = client.clone();
        handles.push(tokio::spawn(async move {
            let rsp = c
                .ready()
                .await
                .unwrap()
                .call(Msg { tag: 0, payload: i })
                .await
                .unwrap();
            assert_eq!(rsp.payload, i);
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
}

#[tokio::test]
async fn unknown_response_tag_is_desync() {
    // A client whose dispatcher exits with `Desynchronized` when the (manually-driven) "server"
    // sends back a tag for which there is no in-flight request.
    let (client_end, server_end) = duplex::<u32>();
    let saw_desync = Arc::new(AtomicBool::new(false));
    let saw_desync_clone = Arc::clone(&saw_desync);

    let mut client = Client::<Msg<u32>, Msg<u32>>::with_error_handler(
        client_end,
        Counter::default(),
        move |e| {
            if matches!(e, Error::Desynchronized) {
                saw_desync_clone.store(true, Ordering::SeqCst);
            }
        },
    );

    let mut server_end = server_end;
    // Issue one real request so the wire is open.
    let call_fut = tokio::spawn({
        let mut c = client.clone();
        async move {
            c.ready()
                .await
                .unwrap()
                .call(Msg { tag: 0, payload: 1 })
                .await
        }
    });

    // Receive the request and respond with a *bogus* tag.
    let _ = server_end.next().await.unwrap().unwrap();
    Pin::new(&mut server_end)
        .start_send(Msg {
            tag: 9999,
            payload: 0,
        })
        .unwrap();

    // The dispatcher exits on Desynchronized → the in-flight call sees ClientDropped.
    let result = call_fut.await.unwrap();
    assert!(matches!(result, Err(Error::ClientDropped)));

    // Give the on_error closure a moment to run.
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(saw_desync.load(Ordering::SeqCst));

    // Subsequent calls also fail fast.
    let after = client
        .ready()
        .await
        .unwrap()
        .call(Msg { tag: 0, payload: 2 })
        .await;
    assert!(matches!(after, Err(Error::ClientDropped)));
}

#[tokio::test]
async fn transport_send_failure_propagates() {
    let (client_end, _server_end) = duplex::<u32>();
    let fail = Arc::clone(&client_end.fail_send);
    let saw = Arc::new(Mutex::new(None::<Error>));
    let saw_clone = Arc::clone(&saw);

    let mut client = Client::<Msg<u32>, Msg<u32>>::with_error_handler(
        client_end,
        Counter::default(),
        move |e| {
            // Synchronous mutex acquired then released — fine inside a tokio runtime since the
            // closure runs on the dispatcher's exit path with no other contenders.
            *saw_clone.try_lock().expect("uncontended") = Some(e);
        },
    );

    fail.store(true, Ordering::SeqCst);
    let r = client
        .ready()
        .await
        .unwrap()
        .call(Msg { tag: 0, payload: 1 })
        .await;
    assert!(matches!(r, Err(Error::ClientDropped)));

    tokio::time::sleep(Duration::from_millis(20)).await;
    let captured = saw.lock().await.take().expect("error captured");
    assert!(matches!(captured, Error::BrokenTransportSend(_)));
}

#[tokio::test]
async fn dropping_last_client_lets_dispatcher_finish_cleanly() {
    let (client_end, server_end) = duplex::<u32>();
    let exited_with_error = Arc::new(AtomicBool::new(false));
    let exited_clone = Arc::clone(&exited_with_error);

    let client = Client::<Msg<u32>, Msg<u32>>::with_error_handler(
        client_end,
        Counter::default(),
        move |_| exited_clone.store(true, Ordering::SeqCst),
    );
    tokio::spawn(server::Server::new(server_end, Echo));

    drop(client);
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !exited_with_error.load(Ordering::SeqCst),
        "on_error should not fire on clean shutdown"
    );
}

/// Service that only completes after a `tx.send()` from the test, exercising the FuturesUnordered
/// path on the server side (multiple in-flight calls completing out of order).
struct Manual {
    queue: Arc<Mutex<VecDeque<tokio::sync::oneshot::Sender<Msg<u32>>>>>,
}

impl Service<Msg<u32>> for Manual {
    type Response = Msg<u32>;
    type Error = Infallible;
    type Future =
        std::pin::Pin<Box<dyn std::future::Future<Output = Result<Msg<u32>, Infallible>> + Send>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Msg<u32>) -> Self::Future {
        let queue = Arc::clone(&self.queue);
        Box::pin(async move {
            let (tx, rx) = tokio::sync::oneshot::channel();
            queue.lock().await.push_back(tx);
            let _ = rx.await;
            Ok(req)
        })
    }
}

/// Sink wrapper whose `poll_close` returns `Pending` until `release` is dropped. Used to verify
/// that the server doesn't return `Ready` until the sink has actually closed — which on a real
/// transport is what guarantees buffered bytes reach the wire before the task drops the
/// transport.
struct GatedCloseEnd<T> {
    inner: DuplexEnd<T>,
    release: Arc<AtomicBool>,
    waker: Arc<Mutex<Option<std::task::Waker>>>,
}

impl<T> Sink<Msg<T>> for GatedCloseEnd<T> {
    type Error = TransportError;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.inner).poll_ready(cx)
    }

    fn start_send(mut self: Pin<&mut Self>, item: Msg<T>) -> Result<(), Self::Error> {
        Pin::new(&mut self.inner).start_send(item)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if self.release.load(Ordering::SeqCst) {
            Poll::Ready(Ok(()))
        } else {
            // Park; the test will wake us once it sets `release`.
            *self.waker.try_lock().expect("uncontended") = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}

impl<T> Stream for GatedCloseEnd<T> {
    type Item = Result<Msg<T>, TransportError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner).poll_next(cx)
    }
}

#[tokio::test]
async fn server_awaits_sink_close_before_returning() {
    let (client_end, server_end) = duplex::<u32>();
    let release = Arc::new(AtomicBool::new(false));
    let waker = Arc::new(Mutex::new(None));
    let gated = GatedCloseEnd {
        inner: server_end,
        release: Arc::clone(&release),
        waker: Arc::clone(&waker),
    };

    // Run the server on the gated transport; spawn so we can observe completion.
    let server_handle = tokio::spawn(server::Server::new(gated, Echo));

    // Issue and complete one round-trip so the service has done work before the peer hangs up.
    let mut client =
        Client::<Msg<u32>, Msg<u32>>::with_error_handler(client_end, Counter::default(), |_| {});
    let _ = client
        .ready()
        .await
        .unwrap()
        .call(Msg { tag: 0, payload: 7 })
        .await
        .unwrap();

    // Drop the client → its dispatcher exits → the server's transport sees stream EOF.
    drop(client);

    // The server should now be waiting on `poll_close`; it must not have returned yet.
    tokio::time::sleep(Duration::from_millis(40)).await;
    assert!(
        !server_handle.is_finished(),
        "server returned before sink close completed"
    );

    // Release the gate; the server should observe `poll_close` ready and complete.
    release.store(true, Ordering::SeqCst);
    if let Some(w) = waker.lock().await.take() {
        w.wake();
    }
    let result = tokio::time::timeout(Duration::from_millis(500), server_handle)
        .await
        .expect("server did not complete after release")
        .expect("server task panicked");
    assert!(result.is_ok());
}

#[tokio::test]
async fn server_dispatches_responses_out_of_order() {
    let (client_end, server_end) = duplex::<u32>();
    let queue = Arc::new(Mutex::new(VecDeque::new()));
    let svc = Manual {
        queue: Arc::clone(&queue),
    };
    tokio::spawn(server::Server::new(server_end, svc));

    let client =
        Client::<Msg<u32>, Msg<u32>>::with_error_handler(client_end, Counter::default(), |e| {
            panic!("dispatcher exited: {e}")
        });

    let mut c1 = client.clone();
    let mut c2 = client.clone();
    let f1 = tokio::spawn(async move {
        c1.ready()
            .await
            .unwrap()
            .call(Msg { tag: 0, payload: 1 })
            .await
            .unwrap()
    });
    let f2 = tokio::spawn(async move {
        c2.ready()
            .await
            .unwrap()
            .call(Msg { tag: 0, payload: 2 })
            .await
            .unwrap()
    });

    // Wait until the server has both requests queued.
    while queue.lock().await.len() < 2 {
        tokio::task::yield_now().await;
    }

    // Complete request 2 first, then request 1 → the client must still match each response to
    // the right caller via the tag.
    let mut q = queue.lock().await;
    let tx_first = q.pop_front().unwrap(); // request 1
    let tx_second = q.pop_front().unwrap(); // request 2
    drop(q);
    let _ = tx_second.send(Msg {
        tag: 0, /* server preserves the tag the multiplexer sent */
        payload: 2,
    });
    let _ = tx_first.send(Msg { tag: 0, payload: 1 });
    // Note: we set tag=0 above, but the server-side service should have received messages with
    // the actual tags the client assigned, and the responses must echo those tags back. Our
    // Manual service preserves whatever tag came in on the request, so that property holds.

    assert_eq!(f1.await.unwrap().payload, 1);
    assert_eq!(f2.await.unwrap().payload, 2);
}
