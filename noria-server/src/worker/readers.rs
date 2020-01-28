use async_bincode::AsyncBincodeStream;
use dataflow::prelude::DataType;
use dataflow::prelude::*;
use dataflow::Readers;
use dataflow::SingleReadHandle;
use futures_util::{
    future, future::Either, future::FutureExt, ready, try_future::TryFutureExt,
    try_stream::TryStreamExt,
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
use tokio::prelude::*;
use tokio_tower::multiplex::server;
use tower::service_fn;

/// Retry reads every this often.
const RETRY_TIMEOUT_US: u64 = 1000;

/// If a blocking reader finds itself waiting this long for a backfill to complete, it will
/// re-issue the replay request. To avoid the system falling over if replays are slow for a little
/// while, waiting readers will use exponential backoff on this delay if they continue to miss.
const TRIGGER_TIMEOUT_US: u64 = 10_000;

thread_local! {
    static READERS: RefCell<HashMap<
        (NodeIndex, usize),
        SingleReadHandle,
    >> = Default::default();
}

pub(super) fn listen(
    valve: &Valve,
    ioh: &tokio_io_pool::Handle,
    on: tokio::net::TcpListener,
    readers: Readers,
) -> impl Future<Output = ()> {
    ioh.spawn_all(
        valve
            .wrap(on.incoming())
            .into_stream()
            .filter_map(|c| {
                // io error from client: just ignore it
                async move { c.ok() }
            })
            .map(Ok)
            .map_ok(move |stream| {
                let readers = readers.clone();
                stream.set_nodelay(true).expect("could not set TCP_NODELAY");
                server::Server::new(
                    AsyncBincodeStream::from(stream).for_async(),
                    service_fn(move |req| handle_message(req, &readers)),
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
                .map(|_| ())
            }),
    )
    .map_err(|e: tokio_io_pool::StreamSpawnError<()>| {
        eprintln!(
            "io pool is shutting down, so can't handle more reads: {:?}",
            e
        );
    })
    .map(|_| ())
}

fn dup(rs: &[Vec<DataType>]) -> Vec<Vec<DataType>> {
    let mut outer = Vec::with_capacity(rs.len());
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
                ret.resize(keys.len(), Vec::new());

                // first do non-blocking reads for all keys to see if we can return immediately
                let found = keys
                    .iter_mut()
                    .map(|key| {
                        let rs = reader.try_find_and(key, dup).map(|r| r.0);
                        (key, rs)
                    })
                    .enumerate();

                let mut ready = true;
                let mut replaying = false;
                for (i, (key, v)) in found {
                    match v {
                        Ok(Some(rs)) => {
                            // immediate hit!
                            ret[i] = rs;
                            *key = vec![];
                        }
                        Err(()) => {
                            // map not yet ready
                            ready = false;
                            *key = vec![];
                            break;
                        }
                        Ok(None) => {
                            // triggered partial replay
                            replaying = true;
                        }
                    }
                }

                if !ready {
                    return Ok(Tagged {
                        tag,
                        v: ReadReply::Normal(Err(())),
                    });
                }

                if !replaying {
                    // we hit on all the keys!
                    return Ok(Tagged {
                        tag,
                        v: ReadReply::Normal(Ok(ret)),
                    });
                }

                // trigger backfills for all the keys we missed on for later
                reader.trigger(keys.iter().filter(|key| !key.is_empty()).map(Vec::as_slice));

                Err((keys, ret))
            });

            match immediate {
                Ok(reply) => Either::Left(Either::Left(future::ready(Ok(reply)))),
                Err((keys, ret)) => {
                    if !block {
                        Either::Left(Either::Left(future::ready(Ok(Tagged {
                            tag,
                            v: ReadReply::Normal(Ok(ret)),
                        }))))
                    } else {
                        let trigger = time::Duration::from_micros(TRIGGER_TIMEOUT_US);
                        let retry = time::Duration::from_micros(RETRY_TIMEOUT_US);
                        let now = time::Instant::now();
                        Either::Left(Either::Right(BlockingRead {
                            tag,
                            target,
                            keys,
                            read: ret,
                            truth: s.clone(),
                            retry: async_timer::interval(retry),
                            trigger_timeout: trigger,
                            next_trigger: now,
                            first: now,
                        }))
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
    read: Vec<Vec<Vec<DataType>>>,
    target: (NodeIndex, usize),
    keys: Vec<Vec<DataType>>,
    truth: Readers,

    #[pin]
    retry: async_timer::Interval<async_timer::oneshot::Timer>,

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

            let missing = READERS.with(|readers_cache| {
                let mut readers_cache = readers_cache.borrow_mut();
                let s = &this.truth;
                let target = &this.target;
                let reader = readers_cache.entry(*this.target).or_insert_with(|| {
                    let readers = s.lock().unwrap();
                    readers.get(target).unwrap().clone()
                });

                let mut triggered = false;
                let mut missing = false;
                let mut ended = false;
                let now = time::Instant::now();
                let read = &mut this.read;
                let next_trigger = *this.next_trigger;
                let still_missing = this.keys.iter_mut().enumerate().filter_map(|(i, key)| {
                    if key.is_empty() {
                        // already have this value
                        None
                    } else {
                        // note that this *does* mean we'll trigger replay multiple times for things
                        // that miss and aren't replayed in time, which is a little sad. but at the
                        // same time, that replay trigger will just be ignored by the target domain.
                        match reader.try_find_and(key, dup).map(|r| r.0) {
                            Ok(Some(rs)) => {
                                read[i] = rs;
                                key.clear();
                                None
                            }
                            Err(()) => {
                                // map has been deleted, so server is shutting down
                                ended = true;
                                None
                            }
                            Ok(None) => {
                                missing = true;
                                if now > next_trigger {
                                    // maybe the key was filled but then evicted, and we missed it?
                                    triggered = true;
                                    Some(&key[..])
                                } else {
                                    None
                                }
                            }
                        }
                    }
                });

                if !reader.trigger(still_missing) {
                    // server is shutting down and won't do the backfill
                    return Err(());
                }

                if ended {
                    return Err(());
                }

                if missing {
                    let now = time::Instant::now();
                    let waited = now - *this.first;
                    *this.first = now;
                    if waited > time::Duration::from_secs(7) {
                        eprintln!(
                            "warning: read has been stuck waiting on {:?} for {:?}",
                            this.keys, waited
                        );
                    }
                }

                if triggered {
                    *this.trigger_timeout *= 2;
                    *this.next_trigger = now + *this.trigger_timeout;
                }

                Ok(missing)
            })?;

            if !missing {
                return Poll::Ready(Ok(Tagged {
                    tag: *this.tag,
                    v: ReadReply::Normal(Ok(mem::replace(&mut this.read, Vec::new()))),
                }));
            }
        }
    }
}
