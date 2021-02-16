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
use noria::consistency::Timestamp;
use noria::util::like::LikePattern;
use noria::{KeyComparison, ReadQuery, ReadReply, Tagged, ViewQuery, ViewQueryFilter};
use pin_project::pin_project;
use serde::ser::Serializer;
use std::cell::RefCell;
use std::collections::HashMap;
use std::convert::TryInto;
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

#[derive(Serialize, Debug)]
#[repr(transparent)]
#[serde(transparent)]
struct SerializedReadReplyBatch(Vec<u8>);

impl SerializedReadReplyBatch {
    fn empty() -> Self {
        serialize(&[])
    }
}

impl From<Vec<u8>> for SerializedReadReplyBatch {
    fn from(v: Vec<u8>) -> Self {
        Self(v)
    }
}

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

fn serialize<'a, I>(rs: I) -> SerializedReadReplyBatch
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

    let mut ser = bincode::Serializer::new(&mut v, bincode::DefaultOptions::default());
    ser.collect_seq(it).unwrap();
    SerializedReadReplyBatch(v)
}

fn handle_message(
    m: Tagged<ReadQuery>,
    s: &Readers,
    wait: &mut tokio::sync::mpsc::UnboundedSender<(BlockingRead, Ack)>,
) -> impl Future<Output = Result<Tagged<ReadReply<SerializedReadReplyBatch>>, ()>> + Send {
    let tag = m.tag;
    match m.v {
        ReadQuery::Normal { target, query } => {
            Either::Left(handle_normal_read_query(tag, s, wait, target, query))
        }
        ReadQuery::Size { target } => {
            Either::Right(Either::Left(handle_size_query(tag, s, target)))
        }
        ReadQuery::Keys { target } => {
            Either::Right(Either::Right(handle_keys_query(tag, s, target)))
        }
    }
}

fn handle_normal_read_query(
    tag: u32,
    s: &Readers,
    wait: &mut tokio::sync::mpsc::UnboundedSender<(BlockingRead, Ack)>,
    target: (NodeIndex, usize),
    query: ViewQuery,
) -> impl Future<Output = Result<Tagged<ReadReply<SerializedReadReplyBatch>>, ()>> + Send {
    let ViewQuery {
        mut key_comparisons,
        block,
        order_by,
        limit,
        filter,
        timestamp,
    } = query;
    let immediate = READERS.with(|readers_cache| {
        let mut readers_cache = readers_cache.borrow_mut();
        let reader = readers_cache.entry(target).or_insert_with(|| {
            let readers = s.lock().unwrap();
            readers.get(&target).unwrap().clone()
        });

        let mut ret = Vec::with_capacity(key_comparisons.len());
        let consistency_miss = !has_sufficient_timestamp(reader, &timestamp);

        // first do non-blocking reads for all keys to see if we can return immediately
        let mut ready = true;
        let mut pending = Vec::new();
        let mut misses = Vec::new();
        for (i, key_comparison) in key_comparisons.drain(..).enumerate() {
            if !ready {
                ret.push(SerializedReadReplyBatch::empty());
                continue;
            }

            // If a lookup misses on consistency miss, we still want to trigger a partial
            // replay. Otherwise, if the key exists in the map, we need to wait for the map
            // to be sufficiently up to date before returning the key. We still do the lookup
            // on consistency miss to know if we must trigger a partial replay.
            use dataflow::LookupError::*;
            match do_lookup(reader, &key_comparison, order_by, limit, &filter) {
                Ok(rs) => {
                    if consistency_miss {
                        // The key is pending on an upstream update before it
                        // can be returned.
                        pending.push(i as usize);
                    } else {
                        // immediate hit!
                        ret.push(rs);
                    }
                }
                Err(NotReady) => {
                    // map not yet ready
                    ready = false;
                    ret.push(SerializedReadReplyBatch::empty());
                }
                Err(miss) => {
                    // need to trigger partial replay for this key
                    pending.push(i as usize);
                    ret.push(SerializedReadReplyBatch::empty());
                    misses.extend(miss.into_misses())
                }
            }
        }

        if !ready {
            return Ok(Tagged {
                tag,
                v: ReadReply::Normal(Err(())),
            });
        }

        if misses.is_empty() {
            // we hit on all the keys!
            assert!(pending.is_empty());
            return Ok(Tagged {
                tag,
                v: ReadReply::Normal(Ok(ret)),
            });
        }

        // trigger backfills for all the keys we missed on
        reader.trigger(misses.iter());

        Err((misses, ret, pending))
    });

    match immediate {
        Ok(reply) => Either::Left(future::ready(Ok(reply))),
        Err((keys, ret, pending)) => {
            if !block {
                Either::Left(future::ready(Ok(Tagged {
                    tag,
                    v: ReadReply::Normal(Ok(ret)),
                })))
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
                        warned: false,
                        order_by,
                        limit,
                        filter,
                        timestamp,
                    },
                    tx,
                ));
                if r.is_err() {
                    // we're shutting down
                    return Either::Left(future::ready(Err(())));
                }
                Either::Right(rx.map(|r| match r {
                    Err(_) => Err(()),
                    Ok(r) => r,
                }))
            }
        }
    }
}

fn handle_size_query(
    tag: u32,
    s: &Readers,
    target: (NodeIndex, usize),
) -> impl Future<Output = Result<Tagged<ReadReply<SerializedReadReplyBatch>>, ()>> + Send {
    let size = READERS.with(|readers_cache| {
        let mut readers_cache = readers_cache.borrow_mut();
        let reader = readers_cache.entry(target).or_insert_with(|| {
            let readers = s.lock().unwrap();
            readers.get(&target).unwrap().clone()
        });

        reader.len()
    });

    future::ready(Ok(Tagged {
        tag,
        v: ReadReply::Size(size),
    }))
}

fn handle_keys_query(
    tag: u32,
    s: &Readers,
    target: (NodeIndex, usize),
) -> impl Future<Output = Result<Tagged<ReadReply<SerializedReadReplyBatch>>, ()>> + Send {
    let keys = READERS.with(|readers_cache| {
        let mut readers_cache = readers_cache.borrow_mut();
        let reader = readers_cache.entry(target).or_insert_with(|| {
            let readers = s.lock().unwrap();
            readers.get(&target).unwrap().clone()
        });

        reader.keys()
    });
    future::ready(Ok(Tagged {
        tag,
        v: ReadReply::Keys(keys),
    }))
}

/// A container for four different exact-size iterators.
///
/// This type exists to avoid having to return a `dyn Iterator` when applying an ORDER BY / LIMIT
/// to the results of a query. It implements `Iterator` and `ExactSizeIterator` iff all of its
/// type parameters implement `Iterator<Item = &Vec<DataType>>`.
enum OrderedLimitedIter<I, J, K, L> {
    Original(I),
    Ordered(J),
    Limited(K),
    OrderedLimited(L),
}

/// WARNING: This impl does NOT delegate calls to `len()` to the underlying iterators.
impl<'a, I, J, K, L> ExactSizeIterator for OrderedLimitedIter<I, J, K, L>
where
    I: Iterator<Item = &'a Vec<DataType>>,
    J: Iterator<Item = &'a Vec<DataType>>,
    K: Iterator<Item = &'a Vec<DataType>>,
    L: Iterator<Item = &'a Vec<DataType>>,
{
}

impl<'a, I, J, K, L> Iterator for OrderedLimitedIter<I, J, K, L>
where
    I: Iterator<Item = &'a Vec<DataType>>,
    J: Iterator<Item = &'a Vec<DataType>>,
    K: Iterator<Item = &'a Vec<DataType>>,
    L: Iterator<Item = &'a Vec<DataType>>,
{
    type Item = &'a Vec<DataType>;
    fn next(&mut self) -> Option<Self::Item> {
        use self::OrderedLimitedIter::*;
        match self {
            Original(i) => i.next(),
            Ordered(i) => i.next(),
            Limited(i) => i.next(),
            OrderedLimited(i) => i.next(),
        }
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        use self::OrderedLimitedIter::*;
        match self {
            Original(i) => i.size_hint(),
            Ordered(i) => i.size_hint(),
            Limited(i) => i.size_hint(),
            OrderedLimited(i) => i.size_hint(),
        }
    }
}

fn do_order<'a, I>(iter: I, idx: usize, reverse: bool) -> impl Iterator<Item = &'a Vec<DataType>>
where
    I: Iterator<Item = &'a Vec<DataType>>,
{
    // TODO(eta): is there a way to avoid buffering all the results?
    let mut results = iter.collect::<Vec<_>>();
    results.sort_by(|a, b| {
        let ret = a[idx].cmp(&b[idx]);
        if reverse {
            ret.reverse()
        } else {
            ret
        }
    });
    results.into_iter()
}

fn do_order_limit<'a, I>(
    iter: I,
    order_by: Option<(usize, bool)>,
    limit: Option<usize>,
) -> impl Iterator<Item = &'a Vec<DataType>> + ExactSizeIterator
where
    I: Iterator<Item = &'a Vec<DataType>> + ExactSizeIterator,
{
    match (order_by, limit) {
        (None, None) => OrderedLimitedIter::Original(iter),
        (Some((idx, reverse)), None) => OrderedLimitedIter::Ordered(do_order(iter, idx, reverse)),
        (None, Some(lim)) => OrderedLimitedIter::Limited(iter.take(lim)),
        (Some((idx, reverse)), Some(lim)) => {
            OrderedLimitedIter::OrderedLimited(do_order(iter, idx, reverse).take(lim))
        }
    }
}

fn post_lookup<'a, I>(
    iter: I,
    order_by: Option<(usize, bool)>,
    limit: Option<usize>,
    filter: &Option<ViewQueryFilter>,
) -> impl Iterator<Item = &'a Vec<DataType>>
where
    I: Iterator<Item = &'a Vec<DataType>> + ExactSizeIterator,
{
    let ordered_limited = do_order_limit(iter, order_by, limit);
    let like_pattern = filter.as_ref().map(
        |ViewQueryFilter {
             value,
             operator,
             column,
         }| { (LikePattern::new(value, (*operator).into()), *column) },
    );
    ordered_limited.filter(move |rec| {
        like_pattern
            .as_ref()
            .map(|(pat, col)| {
                pat.matches(
                    (&rec[*col])
                        .try_into()
                        .expect("Type mismatch: LIKE and ILIKE can only be applied to strings"),
                )
            })
            .unwrap_or(true)
    })
}

fn do_lookup(
    reader: &SingleReadHandle,
    key: &KeyComparison,
    order_by: Option<(usize, bool)>,
    limit: Option<usize>,
    filter: &Option<ViewQueryFilter>,
) -> Result<SerializedReadReplyBatch, dataflow::LookupError> {
    if let Some(equal) = &key.equal() {
        reader
            .try_find_and(&*equal, |rs| {
                serialize(post_lookup(rs.into_iter(), order_by, limit, filter).collect::<Vec<_>>())
            })
            .map(|r| r.0)
    } else {
        reader
            .try_find_range_and(&key, |r| r.into_iter().cloned().collect::<Vec<_>>())
            .map(|(rs, _)| {
                serialize(
                    post_lookup(
                        rs.into_iter().flatten().collect::<Vec<_>>().iter(),
                        order_by,
                        limit,
                        filter,
                    )
                    .collect::<Vec<_>>(),
                )
            })
    }
}

/// Verifies that the timestamp in the reader node associated with the read handle, `reader`,
/// has a greater timestamp than `timestamp`. A greater reader timestamp indicates the writes
/// in the node include all of the writes associated with `timestamp`.
fn has_sufficient_timestamp(reader: &SingleReadHandle, timestamp: &Option<Timestamp>) -> bool {
    if timestamp.is_none() {
        return true;
    }

    let dataflow_timestamp = reader.timestamp();
    if dataflow_timestamp.is_none() {
        return false;
    }

    // The data in the map is newer than the requested timestamp.
    dataflow_timestamp >= *timestamp
}

#[pin_project]
struct BlockingRead {
    tag: u32,
    target: (NodeIndex, usize),
    order_by: Option<(usize, bool)>,
    limit: Option<usize>,
    // serialized records for keys we have already read
    read: Vec<SerializedReadReplyBatch>,
    // keys we have yet to read
    keys: Vec<KeyComparison>,
    // index in self.read that each entyr in keys corresponds to
    pending: Vec<usize>,
    truth: Readers,
    filter: Option<ViewQueryFilter>,

    trigger_timeout: time::Duration,
    next_trigger: time::Instant,
    first: time::Instant,
    warned: bool,
    timestamp: Option<Timestamp>,
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
            .field("timestamp", &self.timestamp)
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

            // If the map is sufficiently up to date to serve the query, we start looking up
            // keys, otherwise we continue to wait.
            if has_sufficient_timestamp(reader, &self.timestamp) {
                // here's the trick we're going to play:
                // we're going to re-try the lookups starting with the _last_ key.
                // if it hits, we move on to the second-to-last, and so on.
                // the moment we miss again, we yield immediately, rather than continue.
                // this avoids shuffling around self.pending and self.keys, and probably doesn't
                // really cost us anything -- we couldn't return ready anyway!
                while let Some(read_i) = self.pending.pop() {
                    let key = self.keys.pop().expect("pending.len() == keys.len()");

                    match do_lookup(reader, &key, self.order_by, self.limit, &self.filter) {
                        Ok(rs) => {
                            read[read_i] = rs;
                        }
                        Err(e) => {
                            if e.is_miss() {
                                // we still missed! restore key + pending
                                self.pending.push(read_i);
                                self.keys.push(key);
                                break;
                            } else {
                                // map has been deleted, so server is shutting down
                                self.pending.clear();
                                self.keys.clear();
                                return Err(());
                            }
                        }
                    }
                }
            }
            debug_assert_eq!(self.pending.len(), self.keys.len());

            if !self.keys.is_empty() && now > next_trigger {
                // maybe the key got filled, then evicted, and we missed it?
                if !reader.trigger(self.keys.iter()) {
                    // server is shutting down and won't do the backfill
                    return Err(());
                }

                self.trigger_timeout *= 2;
                self.next_trigger = now + self.trigger_timeout;
            }

            if !self.keys.is_empty() {
                let waited = now - self.first;
                if waited > time::Duration::from_secs(7) && !self.warned {
                    eprintln!(
                        "warning: read has been stuck waiting on {} for {:?}",
                        if self.keys.len() < 8 {
                            format!("{:?}", self.keys)
                        } else {
                            format!(
                                "{:?} (and {} more keys)",
                                &self.keys[..8],
                                self.keys.len() - 8
                            )
                        },
                        waited
                    );
                    self.warned = true;
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
                    .map(|d| super::serialize(d))
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

    async fn async_bincode_rtt_ok(data: Vec<Vec<Vec<DataType>>>) {
        use futures_util::{SinkExt, StreamExt};

        let mut z = Vec::new();
        let mut w = async_bincode::AsyncBincodeWriter::from(&mut z).for_async();

        for tag in 0..10 {
            w.send(Tagged {
                tag,
                v: ReadReply::Normal::<SerializedReadReplyBatch>(Ok(data
                    .iter()
                    .map(|d| super::serialize(d))
                    .collect())),
            })
            .await
            .unwrap();
        }

        let mut r = async_bincode::AsyncBincodeReader::<_, Tagged<ReadReply>>::from(
            std::io::Cursor::new(&z[..]),
        );

        for tag in 0..10 {
            let got: Tagged<ReadReply> = r.next().await.unwrap().unwrap();

            match got {
                Tagged {
                    v: ReadReply::Normal(Ok(got)),
                    tag: t,
                } => {
                    assert_eq!(tag, t);
                    assert_eq!(got.len(), data.len());
                    for (got, expected) in got.into_iter().zip(data.iter()) {
                        assert_eq!(&*got, expected);
                    }
                }
                r => panic!("{:?}", r),
            }
        }
    }

    #[tokio::test]
    async fn async_bincode_rtt_empty() {
        async_bincode_rtt_ok(vec![]).await;
    }

    #[tokio::test]
    async fn async_bincode_rtt_one_empty() {
        async_bincode_rtt_ok(vec![vec![]]).await;
    }

    #[tokio::test]
    async fn async_bincode_rtt_one_one_empty() {
        async_bincode_rtt_ok(vec![vec![vec![]]]).await;
    }

    #[tokio::test]
    async fn async_bincode_rtt_multi() {
        async_bincode_rtt_ok(vec![
            vec![
                vec![DataType::from(1), DataType::from(42)],
                vec![DataType::from(43), DataType::from(2)],
            ],
            vec![
                vec![DataType::from(2), DataType::from(43)],
                vec![DataType::from(44), DataType::from(3)],
            ],
            vec![vec![]],
            vec![],
        ])
        .await;
    }
}
