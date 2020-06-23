use async_bincode::AsyncBincodeStream;
use dataflow::prelude::DataType;
use dataflow::prelude::*;
use dataflow::Readers;
use dataflow::SingleReadHandle;
use futures_util::{
    future,
    future::Either,
    future::{FutureExt, TryFutureExt},
    stream::{StreamExt, TryStreamExt},
};
use noria::{ReadQuery, ReadReply, Tagged};
use pin_project::pin_project;
use std::cell::RefCell;
use std::collections::HashMap;
use std::mem;
use std::time;
use std::{future::Future, task::Poll};
use stream_cancel::Valve;
use tokio::task_local;
use tokio_tower::multiplex::server;
use tower::service_fn;

/// Retry reads every this often.
const RETRY_TIMEOUT: time::Duration = time::Duration::from_micros(100);

/// If a blocking reader finds itself waiting this long for a backfill to complete, it will
/// re-issue the replay request. To avoid the system falling over if replays are slow for a little
/// while, waiting readers will use exponential backoff on this delay if they continue to miss.
const TRIGGER_TIMEOUT_MS: u64 = 20;

task_local! {
    static READERS: RefCell<HashMap<
        (NodeIndex, usize),
        SingleReadHandle,
    >>;
}

type SerializedReadReplyBatch = Vec<u8>;
type Ack = tokio::sync::oneshot::Sender<Result<Tagged<ReadReply<SerializedReadReplyBatch>>, ()>>;

pub(super) async fn listen(
    alive: tokio::sync::mpsc::Sender<()>,
    valve: Valve,
    mut on: tokio::net::TcpListener,
    readers: Readers,
) {
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

        // future that ensures all blocking reads are handled in FIFO order
        // and avoid hogging the executors with read retries
        let (mut tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<(BlockingRead, Ack)>();

        let retries = READERS.scope(Default::default(), async move {
            use async_timer::Oneshot;
            let mut retry = async_timer::oneshot::Timer::new(RETRY_TIMEOUT);
            let mut pending = None::<(BlockingRead, Ack)>;
            loop {
                if let Some((ref mut blocking, _)) = pending {
                    // we have a pending read — see if it can complete
                    if let Poll::Ready(res) = blocking.check() {
                        // it did! let's tell the caller.
                        let (_, ack) = pending.take().expect("we matched on Some above");
                        // if this errors, the client just went away
                        let _ = ack.send(res);
                    // the loop will take care of looking for the next request
                    } else {
                        // we have a pending request, but it is still blocked
                        // time for us to wait...
                        futures_util::future::poll_fn(|cx| {
                            // we need the poll_fn so we can get the waker
                            retry.restart(RETRY_TIMEOUT, cx.waker());
                            Poll::Ready(())
                        })
                        .await;
                        // we need `(&mut )` here so that we can re-use it
                        (&mut retry).await;
                    }
                } else {
                    // no point in waiting for a timer if we've got nothing to wait for
                    // so let's get another request
                    if let Some(read) = rx.next().await {
                        pending = Some(read);
                    } else {
                        break;
                    }
                }
            }
        });
        tokio::spawn(retries);

        let server = READERS.scope(
            Default::default(),
            server::Server::new(
                AsyncBincodeStream::from(stream).for_async(),
                service_fn(move |req| handle_message(req, &readers, &mut tx)),
            ),
        );
        tokio::spawn(
            server
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

fn serialize<'a, I>(rs: I) -> Vec<u8>
where
    I: IntoIterator<Item = &'a Vec<DataType>>,
    I::IntoIter: ExactSizeIterator,
{
    let mut it = rs.into_iter().peekable();
    let ln = it.len();
    let fst = it.peek();
    let mut v = Vec::with_capacity(
        fst.as_ref()
            .map(|fst| {
                // assume all rows are the same length
                ln * fst.len() * std::mem::size_of::<DataType>()
            })
            .unwrap_or(0)
            + std::mem::size_of::<u64>(/* seq.len */),
    );

    use serde::ser::{SerializeSeq, Serializer};
    let mut ser = bincode::Serializer::new(&mut v, bincode::DefaultOptions::default());
    let mut seq = ser.serialize_seq(Some(it.len())).unwrap();
    for r in it {
        seq.serialize_element(r).unwrap();
    }
    seq.end().unwrap();
    v
}

fn handle_message(
    m: Tagged<ReadQuery>,
    s: &Readers,
    wait: &mut tokio::sync::mpsc::UnboundedSender<(BlockingRead, Ack)>,
) -> impl Future<Output = Result<Tagged<ReadReply<SerializedReadReplyBatch>>, ()>> + Send {
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
                    let rs = reader.try_find_and(key, |rs| serialize(rs)).map(|r| r.0);
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
                        let now = time::Instant::now();
                        let r = wait.send((
                            BlockingRead {
                                tag,
                                target,
                                keys,
                                pending,
                                read: ret,
                                truth: s.clone(),
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
    // serialized records for keys we have already read
    read: Vec<Vec<u8>>,
    // keys we have yet to read
    keys: Vec<Vec<DataType>>,
    // index in self.read that each entyr in keys corresponds to
    pending: Vec<usize>,
    truth: Readers,

    trigger_timeout: time::Duration,
    next_trigger: time::Instant,
    first: time::Instant,
}

impl std::fmt::Debug for BlockingRead {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlockingRead")
            .field("tag", &self.tag)
            .field("target", &self.target)
            .field("read", &self.read)
            .field("keys", &self.keys)
            .field("pending", &self.pending)
            .field("trigger_timeout", &self.trigger_timeout)
            .field("next_trigger", &self.next_trigger)
            .field("first", &self.first)
            .finish()
    }
}

impl BlockingRead {
    fn check(&mut self) -> Poll<Result<Tagged<ReadReply<SerializedReadReplyBatch>>, ()>> {
        READERS.with(|readers_cache| {
            let mut readers_cache = readers_cache.borrow_mut();
            let s = &self.truth;
            let target = &self.target;
            let reader = readers_cache.entry(self.target).or_insert_with(|| {
                let readers = s.lock().unwrap();
                readers.get(target).unwrap().clone()
            });

            let now = time::Instant::now();
            let read = &mut self.read;
            let next_trigger = self.next_trigger;

            // here's the trick we're going to play:
            // we're going to re-try the lookups starting with the _last_ key.
            // if it hits, we move on to the second-to-last, and so on.
            // the moment we miss again, we yield immediately, rather than continue.
            // this avoids shuffling around self.pending and self.keys, and probably doesn't
            // really cost us anything -- we couldn't return ready anyway!

            while let Some(read_i) = self.pending.pop() {
                let key = self.keys.pop().expect("pending.len() == keys.len()");
                match reader.try_find_and(&key, |rs| serialize(rs)).map(|r| r.0) {
                    Ok(Some(rs)) => {
                        read[read_i] = rs;
                    }
                    Err(()) => {
                        // map has been deleted, so server is shutting down
                        self.pending.clear();
                        self.keys.clear();
                        return Err(());
                    }
                    Ok(None) => {
                        // we still missed! restore key + pending
                        self.pending.push(read_i);
                        self.keys.push(key);
                        break;
                    }
                }
            }
            debug_assert_eq!(self.pending.len(), self.keys.len());

            if !self.keys.is_empty() && now > next_trigger {
                // maybe the key got filled, then evicted, and we missed it?
                if !reader.trigger(self.keys.iter().map(Vec::as_slice)) {
                    // server is shutting down and won't do the backfill
                    return Err(());
                }

                self.trigger_timeout *= 2;
                self.next_trigger = now + self.trigger_timeout;
            }

            if !self.keys.is_empty() {
                let waited = now - self.first;
                self.first = now;
                if waited > time::Duration::from_secs(7) {
                    eprintln!(
                        "warning: read has been stuck waiting on {:?} for {:?}",
                        self.keys, waited
                    );
                }
            }

            Ok(())
        })?;

        if self.keys.is_empty() {
            Poll::Ready(Ok(Tagged {
                tag: self.tag,
                v: ReadReply::Normal(Ok(mem::take(&mut self.read))),
            }))
        } else {
            Poll::Pending
        }
    }
}

#[cfg(test)]
mod readreply {
    use super::SerializedReadReplyBatch;
    use noria::{DataType, ReadReply, Tagged};

    fn rtt_ok(data: Vec<Vec<Vec<DataType>>>) {
        let got: Tagged<ReadReply> = bincode::deserialize(
            &bincode::serialize(&Tagged {
                tag: 32,
                v: ReadReply::Normal::<SerializedReadReplyBatch>(Ok(data
                    .iter()
                    .map(|d| bincode::serialize(d).unwrap())
                    .collect())),
            })
            .unwrap(),
        )
        .unwrap();

        match got {
            Tagged {
                v: ReadReply::Normal(Ok(got)),
                tag: 32,
            } => {
                assert_eq!(got.len(), data.len());
                for (got, expected) in got.into_iter().zip(data.into_iter()) {
                    assert_eq!(&*got, &expected);
                }
            }
            r => panic!("{:?}", r),
        }
    }

    #[test]
    fn rtt_normal_empty() {
        let got: Tagged<ReadReply> = bincode::deserialize(
            &bincode::serialize(&Tagged {
                tag: 32,
                v: ReadReply::Normal::<SerializedReadReplyBatch>(Ok(Vec::new())),
            })
            .unwrap(),
        )
        .unwrap();

        match got {
            Tagged {
                v: ReadReply::Normal(Ok(data)),
                tag: 32,
            } => {
                assert!(data.is_empty());
            }
            r => panic!("{:?}", r),
        }
    }

    #[test]
    fn rtt_normal_one() {
        rtt_ok(vec![vec![vec![DataType::from(1)]]]);
    }

    #[test]
    fn rtt_normal_multifield() {
        rtt_ok(vec![vec![vec![DataType::from(1), DataType::from(42)]]]);
    }

    #[test]
    fn rtt_normal_multirow() {
        rtt_ok(vec![vec![
            vec![DataType::from(1)],
            vec![DataType::from(42)],
        ]]);
    }

    #[test]
    fn rtt_normal_multibatch() {
        rtt_ok(vec![
            vec![vec![DataType::from(1)]],
            vec![vec![DataType::from(42)]],
        ]);
    }

    #[test]
    fn rtt_normal_multi() {
        rtt_ok(vec![
            vec![
                vec![DataType::from(1), DataType::from(42)],
                vec![DataType::from(43), DataType::from(2)],
            ],
            vec![
                vec![DataType::from(2), DataType::from(43)],
                vec![DataType::from(44), DataType::from(3)],
            ],
        ]);
    }

    #[test]
    fn rtt_normal_err() {
        let got: Tagged<ReadReply> = bincode::deserialize(
            &bincode::serialize(&Tagged {
                tag: 32,
                v: ReadReply::Normal::<SerializedReadReplyBatch>(Err(())),
            })
            .unwrap(),
        )
        .unwrap();

        assert!(matches!(
            got,
            Tagged {
                tag: 32,
                v: ReadReply::Normal(Err(()))
            }
        ));
    }

    #[test]
    fn rtt_size() {
        let got: Tagged<ReadReply> = bincode::deserialize(
            &bincode::serialize(&Tagged {
                tag: 32,
                v: ReadReply::Size::<SerializedReadReplyBatch>(42),
            })
            .unwrap(),
        )
        .unwrap();
        assert!(matches!(
            got,
            Tagged {
                tag: 32,
                v: ReadReply::Size(42)
            }
        ));
    }
}
