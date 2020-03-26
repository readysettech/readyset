use async_bincode::AsyncBincodeStream;
use dataflow::prelude::DataType;
use dataflow::prelude::*;
use dataflow::Readers;
use dataflow::SingleReadHandle;
use futures_util::{
    future,
    future::Either,
    future::{FutureExt, TryFutureExt},
    ready,
    stream::{Stream, StreamExt, TryStreamExt},
};
use noria::{ReadQuery, ReadReply, Tagged};
use pin_project::pin_project;
use std::cell::RefCell;
use std::collections::HashMap;
use std::mem;
use std::time;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use stream_cancel::Valve;
use tokio_tower::multiplex::server;
use tower::service_fn;

/// Retry reads every this often.
const RETRY_TIMEOUT_MS: u64 = 1;

/// If a blocking reader finds itself waiting this long for a backfill to complete, it will
/// re-issue the replay request. To avoid the system falling over if replays are slow for a little
/// while, waiting readers will use exponential backoff on this delay if they continue to miss.
const TRIGGER_TIMEOUT_MS: u64 = 10;

thread_local! {
    static READERS: RefCell<HashMap<
        (NodeIndex, usize),
        SingleReadHandle,
    >> = Default::default();
}

pub(super) async fn listen(
    alive: tokio::sync::mpsc::Sender<()>,
    valve: Valve,
    mut on: tokio::net::TcpListener,
    readers: Readers,
) {
    // future that ensures all blocking reads are handled in FIFO order
    // and avoid hogging the executors with read retries
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<(
        BlockingRead,
        tokio::sync::oneshot::Sender<Result<Tagged<ReadReply>, ()>>,
    )>();
    tokio::spawn(async move {
        while let Some((blocking, ack)) = rx.next().await {
            // if this errors, the client just went away
            let _ = ack.send(blocking.await);
        }
    });

    let mut stream = valve.wrap(on.incoming()).into_stream();
    while let Some(stream) = stream.next().await {
        if let Err(_) = stream {
            // io error from client: just ignore it
            continue;
        }

        let stream = stream.unwrap();
        let readers = readers.clone();
        stream.set_nodelay(true).expect("could not set TCP_NODELAY");
        let alive = alive.clone();
        let mut tx = tx.clone();
        tokio::spawn(
            server::Server::new(
                AsyncBincodeStream::from(stream).for_async(),
                service_fn(move |req| handle_message(req, &readers, &mut tx)),
            )
            .map_err(|e| {
                match e {
                    server::Error::Service(()) => {
                        // server is shutting down -- no need to report this error
                        return;
                    }
                    server::Error::BrokenTransportRecv(ref e)
                    | server::Error::BrokenTransportSend(ref e) => {
                        if let bincode::ErrorKind::Io(ref e) = **e {
                            if e.kind() == std::io::ErrorKind::BrokenPipe
                                || e.kind() == std::io::ErrorKind::ConnectionReset
                            {
                                // client went away
                                return;
                            }
                        }
                    }
                }
                eprintln!("!!! reader client protocol error: {:?}", e);
            })
            .map(move |r| {
                let _ = alive;
                r
            }),
        );
    }
}

fn dup<'a>(rs: impl IntoIterator<Item = &'a Vec<DataType>>) -> Vec<Vec<DataType>> {
    let rs = rs.into_iter();
    let mut outer = Vec::with_capacity(rs.size_hint().0);
    for r in rs {
        let mut inner = Vec::with_capacity(r.len());
        for v in r {
            inner.push(v.deep_clone())
        }
        outer.push(inner);
    }
    outer
}

fn handle_message(
    m: Tagged<ReadQuery>,
    s: &Readers,
    wait: &mut tokio::sync::mpsc::UnboundedSender<(
        BlockingRead,
        tokio::sync::oneshot::Sender<Result<Tagged<ReadReply>, ()>>,
    )>,
) -> impl Future<Output = Result<Tagged<ReadReply>, ()>> + Send {
    let tag = m.tag;
    match m.v {
        ReadQuery::Normal {
            target,
            mut keys,
            block,
        } => {
            let immediate = READERS.with(|readers_cache| {
                let mut readers_cache = readers_cache.borrow_mut();
                let reader = readers_cache.entry(target).or_insert_with(|| {
                    let readers = s.lock().unwrap();
                    readers.get(&target).unwrap().clone()
                });

                let mut ret = Vec::with_capacity(keys.len());

                // first do non-blocking reads for all keys to see if we can return immediately
                let mut i = -1;
                let mut ready = true;
                let mut pending = Vec::new();
                keys.retain(|key| {
                    i += 1;
                    if !ready {
                        ret.push(Vec::new());
                        return false;
                    }
                    let rs = reader.try_find_and(key, |rs| dup(rs)).map(|r| r.0);
                    match rs {
                        Ok(Some(rs)) => {
                            // immediate hit!
                            ret.push(rs);
                            false
                        }
                        Err(()) => {
                            // map not yet ready
                            ready = false;
                            ret.push(Vec::new());
                            false
                        }
                        Ok(None) => {
                            // need to trigger partial replay for this key
                            pending.push(i as usize);
                            ret.push(Vec::new());
                            true
                        }
                    }
                });

                if !ready {
                    return Ok(Tagged {
                        tag,
                        v: ReadReply::Normal(Err(())),
                    });
                }

                if keys.is_empty() {
                    // we hit on all the keys!
                    assert!(pending.is_empty());
                    return Ok(Tagged {
                        tag,
                        v: ReadReply::Normal(Ok(ret)),
                    });
                }

                // trigger backfills for all the keys we missed on
                reader.trigger(keys.iter().map(Vec::as_slice));

                Err((keys, ret, pending))
            });

            match immediate {
                Ok(reply) => Either::Left(Either::Left(future::ready(Ok(reply)))),
                Err((keys, ret, pending)) => {
                    if !block {
                        Either::Left(Either::Left(future::ready(Ok(Tagged {
                            tag,
                            v: ReadReply::Normal(Ok(ret)),
                        }))))
                    } else {
                        let (tx, rx) = tokio::sync::oneshot::channel();
                        let trigger = time::Duration::from_millis(TRIGGER_TIMEOUT_MS);
                        let retry = time::Duration::from_millis(RETRY_TIMEOUT_MS);
                        let now = time::Instant::now();
                        let r = wait.send((
                            BlockingRead {
                                tag,
                                target,
                                keys,
                                pending,
                                read: ret,
                                truth: s.clone(),
                                retry: tokio::time::interval_at(
                                    tokio::time::Instant::from_std(now + retry),
                                    retry,
                                ),
                                trigger_timeout: trigger,
                                next_trigger: now,
                                first: now,
                            },
                            tx,
                        ));
                        if r.is_err() {
                            // we're shutting down
                            return Either::Left(Either::Left(future::ready(Err(()))));
                        }
                        Either::Left(Either::Right(rx.map(|r| match r {
                            Err(_) => Err(()),
                            Ok(r) => r,
                        })))
                    }
                }
            }
        }
        ReadQuery::Size { target } => {
            let size = READERS.with(|readers_cache| {
                let mut readers_cache = readers_cache.borrow_mut();
                let reader = readers_cache.entry(target).or_insert_with(|| {
                    let readers = s.lock().unwrap();
                    readers.get(&target).unwrap().clone()
                });

                reader.len()
            });

            Either::Right(future::ready(Ok(Tagged {
                tag,
                v: ReadReply::Size(size),
            })))
        }
    }
}

#[pin_project]
struct BlockingRead {
    tag: u32,
    target: (NodeIndex, usize),
    // records for keys we have already read
    read: Vec<Vec<Vec<DataType>>>,
    // keys we have yet to read
    keys: Vec<Vec<DataType>>,
    // index in self.read that each entyr in keys corresponds to
    pending: Vec<usize>,
    truth: Readers,

    #[pin]
    retry: tokio::time::Interval,

    trigger_timeout: time::Duration,
    next_trigger: time::Instant,
    first: time::Instant,
}

impl Future for BlockingRead {
    type Output = Result<Tagged<ReadReply>, ()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        loop {
            ready!(this.retry.as_mut().poll_next(cx));

            READERS.with(|readers_cache| {
                let mut readers_cache = readers_cache.borrow_mut();
                let s = &this.truth;
                let target = &this.target;
                let reader = readers_cache.entry(*this.target).or_insert_with(|| {
                    let readers = s.lock().unwrap();
                    readers.get(target).unwrap().clone()
                });

                let now = time::Instant::now();
                let read = &mut this.read;
                let next_trigger = *this.next_trigger;

                // here's the trick we're going to play:
                // we're going to re-try the lookups starting with the _last_ key.
                // if it hits, we move on to the second-to-last, and so on.
                // the moment we miss again, we yield immediately, rather than continue.
                // this avoids shuffling around self.pending and self.keys, and probably doesn't
                // really cost us anything -- we couldn't return ready anyway!

                while let Some(read_i) = this.pending.pop() {
                    let key = this.keys.pop().expect("pending.len() == keys.len()");
                    match reader.try_find_and(&key, |rs| dup(rs)).map(|r| r.0) {
                        Ok(Some(rs)) => {
                            read[read_i] = rs;
                        }
                        Err(()) => {
                            // map has been deleted, so server is shutting down
                            this.pending.clear();
                            this.keys.clear();
                            return Err(());
                        }
                        Ok(None) => {
                            // we still missed! restore key + pending
                            this.pending.push(read_i);
                            this.keys.push(key);
                            break;
                        }
                    }
                }
                debug_assert_eq!(this.pending.len(), this.keys.len());

                if !this.keys.is_empty() && now > next_trigger {
                    // maybe the key got filled, then evicted, and we missed it?
                    if !reader.trigger(this.keys.iter().map(Vec::as_slice)) {
                        // server is shutting down and won't do the backfill
                        return Err(());
                    }

                    *this.trigger_timeout *= 2;
                    *this.next_trigger = now + *this.trigger_timeout;
                }

                if !this.keys.is_empty() {
                    let waited = now - *this.first;
                    *this.first = now;
                    if waited > time::Duration::from_secs(7) {
                        eprintln!(
                            "warning: read has been stuck waiting on {:?} for {:?}",
                            this.keys, waited
                        );
                    }
                }

                Ok(())
            })?;

            if this.keys.is_empty() {
                return Poll::Ready(Ok(Tagged {
                    tag: *this.tag,
                    v: ReadReply::Normal(Ok(mem::replace(&mut this.read, Vec::new()))),
                }));
            }
        }
    }
}
