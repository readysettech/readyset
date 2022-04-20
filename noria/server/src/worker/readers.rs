#![allow(missing_docs)]

use core::task::Context;
use std::borrow::Cow;
use std::cell::RefCell;
use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::task::Poll;
use std::time::Duration;
use std::{iter, mem, time};

use async_bincode::AsyncBincodeStream;
use dataflow::prelude::{DataType, *};
use dataflow::{Expression as DataflowExpression, LookupError, Readers, SingleReadHandle};
use derive_more::From;
use failpoint_macros::set_failpoint;
use futures_util::future;
use futures_util::future::{Either, FutureExt, TryFutureExt};
use futures_util::stream::{StreamExt, TryStreamExt};
use launchpad::intervals;
use nom_sql::SqlIdentifier;
use noria::consistency::Timestamp;
use noria::metrics::recorded;
use noria::{KeyComparison, LookupResult, ReadQuery, ReadReply, ReadReplyStats, Tagged, ViewQuery};
use noria_errors::internal_err;
use pin_project::pin_project;
use readyset_tracing::presampled::instrument_if_enabled;
use serde::ser::Serializer;
use serde::Serialize;
use stream_cancel::Valve;
use tokio::task_local;
use tokio_stream::wrappers::TcpListenerStream;
use tokio_tower::multiplex::server;
use tower::Service;
use tracing::{error, instrument, warn};

/// Retry reads every this often.
const RETRY_TIMEOUT: Duration = Duration::from_micros(100);

/// If a blocking reader finds itself waiting this long for a backfill to complete, it will
/// re-issue the replay request. To avoid the system falling over if replays are slow for a little
/// while, waiting readers will use exponential backoff on this delay if they continue to miss.
const TRIGGER_TIMEOUT_MS: u64 = 20;

task_local! {
    pub static READERS: RefCell<HashMap<
        (NodeIndex, SqlIdentifier, usize),
        SingleReadHandle,
    >>;
}

/// An in-progress batch of records that are the result of the lookup of a single key within a read
///
/// This type exists to optimally support three cases in the read path, corresponding to the three
/// enum variants:
///
/// * We missed on the lookup for a key initially and initiated an upquery for it, in which case we
///   store [`Empty`] in the batch of responses to a read, then overwrite it with the actual results
///   once the replay completes
/// * We found no holes in the lookup for a key initially - this means we want to serialize
///   immediately (within the scope of data owned by a [`reader_map`] handle) to avoid cloning - we
///   store these serialized records in [`Serialized`].
/// * Our lookup key was a range, a subset of which missed while another subset didn't miss - we
///   can't serialize the results for the subset of the range that *didn't* miss, since that data is
///   owned by the `reader_map` handle which we need to release so the replay can process - instead,
///   we clone those results and store them in [`Unserialized`]. Once the replays for the subranges
///   that missed complete, they can be added to the [`Unserialized`] batch with [`try_extend`].
///
/// [`Empty`]: ServerReadReplyBatch::Empty
/// [`Serialized`]: ServerReadReplyBatch::Serialized
/// [`Unserialized`]: ServerReadReplyBatch::Unserialized
/// [`try_extend`]: ServerReadReplyBatch::try_extend
#[derive(Debug, From)]
pub enum ServerReadReplyBatch {
    Empty,
    Serialized(Box<[u8]>),
    Unserialized(Vec<Vec<DataType>>),
}

impl ServerReadReplyBatch {
    /// Construct a [`ServerReadReplyBatch`] by serializing a result set, and storing the serialized
    /// bytes.
    fn serialize<I, R, DT>(rs: I) -> Self
    where
        I: IntoIterator<Item = R>,
        I::IntoIter: ExactSizeIterator,
        R: Serialize + AsRef<Vec<DT>>,
    {
        let rs = rs.into_iter();
        if rs.len() == 0 {
            return Self::Empty;
        }
        let mut it = rs.peekable();
        let ln = it.len();
        let fst = it.peek();
        let mut v = Vec::with_capacity(
            fst.as_ref()
                .map(|fst| {
                    // assume all rows are the same length
                    ln * fst.as_ref().len() * std::mem::size_of::<DataType>()
                })
                .unwrap_or(0)
                + std::mem::size_of::<u64>(/* seq.len */),
        );

        let mut ser = bincode::Serializer::new(&mut v, bincode::DefaultOptions::default());
        ser.collect_seq(it).unwrap();
        Self::Serialized(v.into())
    }

    /// If `self` and `rs` are variants of [`ServerReadReplyBatch`] which can be appended (either
    /// [`Empty`] or [`Unserialized`]), mutate `self` in-place by extending it with the records in
    /// `rs`. Otherwise, returns an error
    ///
    /// [`Empty`]: ServerReadReplyBatch::Empty
    /// [`Unserialized`]: ServerReadReplyBatch::Unserialized
    fn try_extend(&mut self, rs: ServerReadReplyBatch) -> ReadySetResult<()> {
        if self.is_empty() {
            *self = rs;
            return Ok(());
        }

        match (self, rs) {
            (ServerReadReplyBatch::Empty, _) => unreachable!("Matched on is_empty abov"),
            (_, ServerReadReplyBatch::Empty) => {}
            (ServerReadReplyBatch::Serialized(_), _) => {
                internal!("Tried to overwite existing serialized read batch")
            }
            (_, ServerReadReplyBatch::Serialized(_)) => {
                internal!("Can't extend existing read batch with serialized batch",)
            }
            (ServerReadReplyBatch::Unserialized(rows), ServerReadReplyBatch::Unserialized(rs)) => {
                rows.extend(rs)
            }
        }

        Ok(())
    }

    /// Is this [`ServerReadReplyBatch`] *verifiably* empty?
    ///
    /// This is true if `self` is [`Empty`], or [`Unserialized`] with no rows, but *not* true if
    /// `self` is [`Serialized`] with no rows (currently serialized result sets are opaque).
    ///
    /// [`Empty`]: ServerReadReplyBatch::Empty
    /// [`Unserialized`]: ServerReadReplyBatch::Unserialized
    /// [`Serialized`]: ServerReadReplyBatch::Serialized
    fn is_empty(&self) -> bool {
        match self {
            ServerReadReplyBatch::Empty => true,
            ServerReadReplyBatch::Unserialized(rs) if rs.is_empty() => true,
            _ => false,
        }
    }

    /// Return this [`ServerReadReplyBatch`] as its unserialized Vec<Vec<DataType>> if it is
    /// [`Unserialized`], otherwise, consume the object and return None.
    ///
    /// This should only be used in cases when we expect the result to be [`Unserialized`].
    pub fn into_unserialized(self) -> Option<Vec<Vec<DataType>>> {
        if let ServerReadReplyBatch::Unserialized(d) = self {
            Some(d)
        } else {
            None
        }
    }
}

/// Ensures all variants are fungible - a [`ServerReadReplyBatch::Unserialized`] serializes
/// identically to a [`ServerReadReplyBatch::Serialized`] constructed with the same rows, and a
/// [`ServerReadReplyBatch::Empty`] serializes identically to both `Unserialized` and `Serialized`
/// constructed with an empty result set.
impl serde::Serialize for ServerReadReplyBatch {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            ServerReadReplyBatch::Empty => {
                let mut bytes = Vec::with_capacity(std::mem::size_of::<u64>());
                bincode::Serializer::new(&mut bytes, bincode::DefaultOptions::default())
                    .collect_seq(iter::empty::<Vec<DataType>>())
                    .unwrap();
                ServerReadReplyBatch::Serialized(bytes.into()).serialize(serializer)
            }
            ServerReadReplyBatch::Serialized(bytes) => serializer.serialize_bytes(bytes),
            ServerReadReplyBatch::Unserialized(rows) => Self::serialize(rows).serialize(serializer),
        }
    }
}

pub type Ack =
    tokio::sync::oneshot::Sender<Result<Tagged<ReadReply<ServerReadReplyBatch>>, ReadySetError>>;

/// Creates a handler that can be used to perform read queries against a set of
/// Readers.
#[derive(Clone)]
pub struct ReadRequestHandler {
    readers: Readers,
    wait: tokio::sync::mpsc::UnboundedSender<(BlockingRead, Ack)>,
    miss_ctr: metrics::Counter,
    hit_ctr: metrics::Counter,
    upquery_timeout: Duration,
}

impl ReadRequestHandler {
    /// Creates a new request handler that can be used to query Readers.
    pub fn new(
        readers: Readers,
        wait: tokio::sync::mpsc::UnboundedSender<(BlockingRead, Ack)>,
        upquery_timeout: Duration,
    ) -> Self {
        Self {
            readers,
            wait,
            miss_ctr: metrics::register_counter!(recorded::SERVER_VIEW_QUERY_MISS),
            hit_ctr: metrics::register_counter!(recorded::SERVER_VIEW_QUERY_HIT),
            upquery_timeout,
        }
    }

    /// Always returns `ServerReadReplyBatch::Unserialized` if `raw_result` is passed.
    pub fn handle_normal_read_query(
        &mut self,
        tag: u32,
        target: (NodeIndex, SqlIdentifier, usize),
        query: ViewQuery,
        raw_result: bool,
    ) -> impl Future<Output = Result<Tagged<ReadReply<ServerReadReplyBatch>>, ReadySetError>> + Send
    {
        let ViewQuery {
            mut key_comparisons,
            block,
            timestamp,
            filter,
        } = query;
        let immediate = READERS.with(|readers_cache| {
            macro_rules! reply_with_error {
                ($e: expr) => {
                    return Ok(Ok(Tagged {
                        tag,
                        v: ReadReply::Normal(Err($e)),
                    }))
                };
            }

            let mut readers_cache = readers_cache.borrow_mut();
            let reader = match readers_cache.entry(target.clone()) {
                Occupied(v) => v.into_mut(),
                Vacant(v) => {
                    let readers = self.readers.lock().unwrap();

                    v.insert(
                        readers
                            .get(&target)
                            .ok_or(ReadySetError::ReaderNotFound)?
                            .clone(),
                    )
                }
            };

            let mut ret = Vec::with_capacity(key_comparisons.len());
            let consistency_miss = !has_sufficient_timestamp(reader, &timestamp);

            // A miss is either a RYW consistency miss or a key that is not materialized.
            // In the case of a RYW consistency miss, all keys will be added to `missed_keys`
            let mut miss_keys = Vec::new();

            // To keep track of the index within the output vector that each entry in `missed_keys`
            // corresponds to. This is needed when only a subset of the keys are missing due to not
            // being materialized.
            let mut miss_indices = Vec::new();

            // A key needs to be replayed only in the case of a materialization miss.
            // `keys_to_replay` is separate from `miss_keys` because `miss_keys` also
            // includes consistency misses.
            let mut keys_to_replay = Vec::new();

            let mut ready = true;

            // First do non-blocking reads for all keys to see if we can return immediately.
            // We execute this loop even if there is RYW miss, since we still want to trigger a
            // partial replay if the key has been evicted. However, if there is RYW
            // miss, a successful lookup is actually still a miss since the row is not
            // sufficiently up to date.
            let num_keys = key_comparisons.len();
            for (i, key) in key_comparisons.drain(..).enumerate() {
                if !ready {
                    ret.push(ServerReadReplyBatch::Empty);
                    continue;
                }

                use dataflow::LookupError::*;
                match do_lookup(reader, &key, &filter, !raw_result) {
                    Ok(rs) => {
                        if consistency_miss {
                            ret.push(ServerReadReplyBatch::Empty);
                        } else {
                            // immediate hit!
                            ret.push(rs);
                        }
                    }
                    Err(NotReady) => {
                        // map not yet ready
                        ready = false;
                        ret.push(ServerReadReplyBatch::Empty);
                    }
                    Err(Error(e)) => reply_with_error!(e),
                    Err(miss) => {
                        let misses = miss.into_misses();

                        // if we did a lookup of a range, it might be the case that part of the
                        // range *didn't* miss - we still want the results
                        // for the bits that didn't miss, so subtract the
                        // misses from our original keys and do the lookups now
                        // NOTE(grfn): The asymptotics here are bad, but we only ever do range
                        // queries with one key at a time so it doesn't
                        // matter - if that changes, we may want to
                        // improve this somewhat, but for now it's actually *faster* to do this
                        // linearly rather than trying to make a data
                        // structure out of it
                        if key.is_range() {
                            let non_miss_ranges =
                                misses.iter().fold(vec![key.clone()], |acc, miss| {
                                    acc.into_iter()
                                        .flat_map(|key| {
                                            intervals::difference(&key, miss)
                                                .map(|(l, u)| {
                                                    KeyComparison::Range((l.cloned(), u.cloned()))
                                                })
                                                .collect::<Vec<_>>()
                                        })
                                        .collect()
                                });

                            let mut records = vec![];
                            for key in non_miss_ranges {
                                match reader.try_find_range_and(&key, |r| {
                                    Ok(reader
                                        .post_lookup
                                        .process(r, &filter)?
                                        .into_iter()
                                        .map(|r| {
                                            r.into_iter().map(Cow::into_owned).collect::<Vec<_>>()
                                        })
                                        .collect::<Vec<_>>())
                                }) {
                                    Ok((rows, _)) => records.extend(rows.into_iter().flatten()),
                                    Err(LookupError::Error(e)) => reply_with_error!(e),
                                    Err(_) => {
                                        reply_with_error!(internal_err("Reader didn't miss before"))
                                    }
                                }
                            }

                            // Store the records *unserialized* in the result, so we can add the
                            // results for the subranges that missed once their replays complete.
                            ret.push(ServerReadReplyBatch::Unserialized(records))
                        } else {
                            ret.push(ServerReadReplyBatch::Empty);
                        }

                        for key in misses {
                            // We do not push a lookup miss to `missing_keys` and `missing_indices`
                            // here If there is a `consistency_miss` the
                            // key will be pushed at the end of the loop
                            // no matter if it is materialized or not.
                            if !consistency_miss {
                                miss_keys.push(key.clone());
                                miss_indices.push(i as usize);
                            }
                            keys_to_replay.push(key);
                        }
                    }
                };

                // Every key is a missed key if there is a `consistency_miss` since the reader is
                // not sufficiently up to date.
                if consistency_miss {
                    miss_keys.push(key);
                    miss_indices.push(i as usize);
                }
            }
            debug_assert_eq!(ret.len(), num_keys);

            if !ready {
                return Ok(Ok(Tagged {
                    tag,
                    v: ReadReply::Normal(Err(ReadySetError::ViewNotYetAvailable)),
                }));
            }

            // Hit on all the keys and were RYW consistent
            if !consistency_miss && miss_keys.is_empty() {
                self.hit_ctr.increment(1);
                return Ok(Ok(Tagged {
                    tag,
                    v: ReadReply::Normal(Ok(LookupResult::Results(ret, ReadReplyStats::default()))),
                }));
            }
            self.miss_ctr.increment(1);

            // Trigger backfills for all the keys we missed on, regardless of a consistency hit/miss
            if !keys_to_replay.is_empty() {
                reader.trigger(keys_to_replay.iter());
            }

            Ok(Err((miss_keys, ret, miss_indices)))
        });

        let immediate = match immediate {
            Ok(v) => v,
            Err(e) => return Either::Right(Either::Left(async move { Err(e) })),
        };

        match immediate {
            Ok(reply) => Either::Left(future::ready(Ok(reply))),
            Err((pending_keys, ret, pending_indices)) => {
                if !block {
                    Either::Left(future::ready(Ok(Tagged {
                        tag,
                        v: ReadReply::Normal(Ok(LookupResult::NonBlockingMiss)),
                    })))
                } else {
                    let (tx, rx) = tokio::sync::oneshot::channel();
                    let trigger = Duration::from_millis(TRIGGER_TIMEOUT_MS);
                    let now = time::Instant::now();

                    let r = self.wait.send((
                        BlockingRead {
                            tag,
                            target,
                            pending_keys,
                            pending_indices,
                            read: ret,
                            truth: self.readers.clone(),
                            trigger_timeout: trigger,
                            next_trigger: now,
                            first: now,
                            warned: false,
                            filter,
                            timestamp,
                            upquery_timeout: self.upquery_timeout,
                            raw_result,
                        },
                        tx,
                    ));
                    if r.is_err() {
                        // we're shutting down
                        return Either::Left(future::ready(Err(ReadySetError::ServerShuttingDown)));
                    }
                    Either::Right(Either::Right(rx.map(|r| match r {
                        Err(e) => Err(internal_err(e.to_string())),
                        Ok(r) => r,
                    })))
                }
            }
        }
    }

    fn handle_size_query(
        &mut self,
        tag: u32,
        target: (NodeIndex, SqlIdentifier, usize),
    ) -> impl Future<Output = Result<Tagged<ReadReply<ServerReadReplyBatch>>, ReadySetError>> + Send
    {
        let size = READERS.with(|readers_cache| {
            let mut readers_cache = readers_cache.borrow_mut();
            let reader = readers_cache.entry(target.clone()).or_insert_with(|| {
                let readers = self.readers.lock().unwrap();
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
        &mut self,
        tag: u32,
        target: (NodeIndex, SqlIdentifier, usize),
    ) -> impl Future<Output = Result<Tagged<ReadReply<ServerReadReplyBatch>>, ReadySetError>> + Send
    {
        let keys = READERS.with(|readers_cache| {
            let mut readers_cache = readers_cache.borrow_mut();
            let read_handle = self.readers.lock().unwrap().get(&target).unwrap().clone();
            readers_cache.entry(target).or_insert(read_handle).keys()
        });
        future::ready(Ok(Tagged {
            tag,
            v: ReadReply::Keys(keys),
        }))
    }
}

#[pin_project(project = ReadResponseFutureProj)]
enum ReadResponseFuture<N, S, K>
where
    N: Future<Output = Result<Tagged<ReadReply<ServerReadReplyBatch>>, ReadySetError>> + Send,
    S: Future<Output = Result<Tagged<ReadReply<ServerReadReplyBatch>>, ReadySetError>> + Send,
    K: Future<Output = Result<Tagged<ReadReply<ServerReadReplyBatch>>, ReadySetError>> + Send,
{
    Normal(#[pin] N),
    Size(#[pin] S),
    Keys(#[pin] K),
}

impl<N, S, K> Future for ReadResponseFuture<N, S, K>
where
    N: Future<Output = Result<Tagged<ReadReply<ServerReadReplyBatch>>, ReadySetError>> + Send,
    S: Future<Output = Result<Tagged<ReadReply<ServerReadReplyBatch>>, ReadySetError>> + Send,
    K: Future<Output = Result<Tagged<ReadReply<ServerReadReplyBatch>>, ReadySetError>> + Send,
{
    type Output = Result<Tagged<ReadReply<ServerReadReplyBatch>>, ReadySetError>;
    #[inline]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.project() {
            ReadResponseFutureProj::Normal(f) => f.poll(cx),
            ReadResponseFutureProj::Size(f) => f.poll(cx),
            ReadResponseFutureProj::Keys(f) => f.poll(cx),
        }
    }
}

impl Service<Tagged<ReadQuery>> for ReadRequestHandler {
    type Response = Tagged<ReadReply<ServerReadReplyBatch>>;
    type Error = ReadySetError;
    type Future = impl Future<Output = Result<Self::Response, Self::Error>> + Send;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    #[instrument(level = "info", skip_all)]
    #[inline]
    fn call(&mut self, m: Tagged<ReadQuery>) -> Self::Future {
        let tag = m.tag;
        match m.v {
            ReadQuery::Normal { target, query } => {
                let future = self.handle_normal_read_query(tag, target, query, false);
                let span = readyset_tracing::child_span!(INFO, "normal_read_query");
                ReadResponseFuture::Normal(instrument_if_enabled(future, span))
            }
            ReadQuery::Size { target } => {
                let future = self.handle_size_query(tag, target);
                let span = readyset_tracing::child_span!(INFO, "size_query");
                ReadResponseFuture::Size(instrument_if_enabled(future, span))
            }
            ReadQuery::Keys { target } => {
                let future = self.handle_keys_query(tag, target);
                let span = readyset_tracing::child_span!(INFO, "keys_query");
                ReadResponseFuture::Keys(instrument_if_enabled(future, span))
            }
        }
    }
}

pub(crate) async fn listen(
    valve: Valve,
    on: tokio::net::TcpListener,
    readers: Readers,
    upquery_timeout: Duration,
) {
    let mut stream = valve.wrap(TcpListenerStream::new(on)).into_stream();
    while let Some(stream) = stream.next().await {
        set_failpoint!("read-query");
        if stream.is_err() {
            // io error from client: just ignore it
            continue;
        }

        let stream = stream.unwrap();
        let readers = readers.clone();
        stream.set_nodelay(true).expect("could not set TCP_NODELAY");

        // future that ensures all blocking reads are handled in FIFO order
        // and avoid hogging the executors with read retries
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<(BlockingRead, Ack)>();

        let retries = READERS.scope(Default::default(), async move {
            let upquery_hist = metrics::register_histogram!(recorded::SERVER_VIEW_UPQUERY_DURATION);
            while let Some((mut pending, ack)) = rx.recv().await {
                // A blocking read always comes immediately after a miss, so no reason to retry it
                // right away better to wait a bit
                tokio::time::sleep(RETRY_TIMEOUT / 4).await;
                loop {
                    if let Poll::Ready(res) = pending.check() {
                        upquery_hist.record(pending.first.elapsed().as_micros() as f64);
                        let _ = ack.send(res);
                        break;
                    }
                    tokio::time::sleep(RETRY_TIMEOUT).await;
                }
            }
        });
        tokio::spawn(retries);

        let r = ReadRequestHandler::new(readers, tx, upquery_timeout);

        let server = READERS.scope(
            Default::default(),
            server::Server::new(AsyncBincodeStream::from(stream).for_async(), r),
        );
        tokio::spawn(server.map_err(|e| {
            match e {
                // server is shutting down -- no need to report this error
                server::Error::Service(ReadySetError::ServerShuttingDown) => {}
                server::Error::BrokenTransportRecv(ref e)
                | server::Error::BrokenTransportSend(ref e) => {
                    if let bincode::ErrorKind::Io(ref e) = **e {
                        if e.kind() == std::io::ErrorKind::BrokenPipe
                            || e.kind() == std::io::ErrorKind::ConnectionReset
                        {
                            // client went away
                        }
                    } else {
                        error!(error = %e, "client transport error");
                    }
                }
                e => error!(error = %e, "reader service error"),
            }
        }));
    }
}

fn do_lookup<'a>(
    reader: &'a SingleReadHandle,
    key: &KeyComparison,
    filter: &Option<DataflowExpression>,
    serialize_results: bool,
) -> Result<ServerReadReplyBatch, dataflow::LookupError> {
    fn maybe_serialize<'a, I>(rs: I, serialize_results: bool) -> ServerReadReplyBatch
    where
        I: IntoIterator<Item = Vec<Cow<'a, DataType>>>,
        I::IntoIter: ExactSizeIterator,
    {
        if serialize_results {
            ServerReadReplyBatch::serialize(rs)
        } else {
            ServerReadReplyBatch::Unserialized(
                rs.into_iter()
                    .map(|row| row.into_iter().map(Cow::into_owned).collect())
                    .collect(),
            )
        }
    }

    if let Some(equal) = &key.equal() {
        reader
            .try_find_and(*equal, |rs| {
                Ok(maybe_serialize(
                    reader.post_lookup.process(rs, filter)?,
                    serialize_results,
                ))
            })
            .map(|r| r.0)
    } else {
        if key.is_reversed_range() {
            warn!("Reader received lookup for range with start bound above end bound; returning empty result set");
            return Ok(ServerReadReplyBatch::Empty);
        }

        reader
            .try_find_range_and(&key, |r| Ok(r.into_iter().cloned().collect::<Vec<_>>()))
            .and_then(|(rs, _)| {
                Ok(maybe_serialize(
                    reader
                        .post_lookup
                        .process(rs.into_iter().flatten().collect::<Vec<_>>().iter(), filter)?,
                    serialize_results,
                ))
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

    dataflow_timestamp
        .unwrap()
        .satisfies(timestamp.as_ref().unwrap())
}

/// Issues a blocking read against a reader. This can be repeatedly polled via `check` for
/// completion.
#[pin_project]
pub struct BlockingRead {
    tag: u32,
    target: (NodeIndex, SqlIdentifier, usize),
    // results for keys we have already read
    read: Vec<ServerReadReplyBatch>,
    // keys we have yet to read and their corresponding indices in the reader
    pending_keys: Vec<KeyComparison>,
    pending_indices: Vec<usize>,
    truth: Readers,
    filter: Option<DataflowExpression>,
    trigger_timeout: Duration,
    next_trigger: time::Instant,
    first: time::Instant,
    warned: bool,
    timestamp: Option<Timestamp>,
    upquery_timeout: Duration,
    raw_result: bool,
}

impl std::fmt::Debug for BlockingRead {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlockingRead")
            .field("tag", &self.tag)
            .field("target", &self.target)
            .field("read", &self.read)
            .field("pending_keys", &self.pending_keys)
            .field("pending_indices", &self.pending_indices)
            .field("trigger_timeout", &self.trigger_timeout)
            .field("next_trigger", &self.next_trigger)
            .field("first", &self.first)
            .field("timestamp", &self.timestamp)
            .finish()
    }
}

impl BlockingRead {
    /// Check if we have the results for this blocking read.
    pub fn check(
        &mut self,
    ) -> Poll<Result<Tagged<ReadReply<ServerReadReplyBatch>>, ReadySetError>> {
        READERS.with(|readers_cache| {
            let mut readers_cache = readers_cache.borrow_mut();
            let s = &self.truth;
            let target = &self.target;
            let reader = readers_cache.entry(self.target.clone()).or_insert_with(|| {
                let readers = s.lock().unwrap();
                readers.get(target).unwrap().clone()
            });

            let now = time::Instant::now();
            let read = &mut self.read;
            let next_trigger = self.next_trigger;

            let consistency_miss = !has_sufficient_timestamp(reader, &self.timestamp);

            // If the map is sufficiently up to date to serve the query, we start looking up
            // keys.
            if !consistency_miss {
                // here's the trick we're going to play:
                // we're going to re-try the lookups starting with the _last_ key.
                // if it hits, we move on to the second-to-last, and so on.
                // the moment we miss again, we yield immediately, rather than continue.
                // this avoids shuffling around self.pending_indices and self.pending_keys, and
                // probably doesn't really cost us anything -- we couldn't return ready anyway!
                while let Some(read_i) = self.pending_indices.pop() {
                    let key = self
                        .pending_keys
                        .pop()
                        .expect("pending.len() == keys.len()");

                    match do_lookup(
                        reader,
                        &key,
                        &self.filter,
                        // Serialize the results, avoiding a clone, unless we already have read
                        // some records for this batch
                        read[read_i].is_empty() && !self.raw_result,
                    ) {
                        Ok(rs) => {
                            read[read_i].try_extend(rs)?;
                        }
                        Err(e) => {
                            if e.is_miss() {
                                // we still missed! restore key + pending
                                self.pending_indices.push(read_i);
                                self.pending_keys.push(key);
                                break;
                            } else {
                                // map has been deleted, so server is shutting down
                                self.pending_indices.clear();
                                self.pending_keys.clear();
                                return Err(ReadySetError::ServerShuttingDown);
                            }
                        }
                    }
                }

                if now > next_trigger && !self.pending_keys.is_empty() {
                    // Retrigger all un-read keys. Its possible they could have been filled and then
                    // evicted again without us reading it.
                    if !reader.trigger(self.pending_keys.iter()) {
                        // server is shutting down and won't do the backfill
                        return Err(ReadySetError::ServerShuttingDown);
                    }

                    self.trigger_timeout =
                        Duration::min(self.trigger_timeout * 2, self.upquery_timeout / 20);
                    self.next_trigger = now + self.trigger_timeout;
                }
            }

            // log that we are still waiting
            if !self.pending_keys.is_empty() {
                let waited = now - self.first;
                if waited > Duration::from_secs(7) && !self.warned {
                    warn!(
                        "read has been stuck waiting on {} for {:?}",
                        if self.pending_keys.len() < 8 {
                            format!("{:?}", self.pending_keys)
                        } else {
                            format!(
                                "{:?} (and {} more keys)",
                                &self.pending_keys[..8],
                                self.pending_keys.len() - 8
                            )
                        },
                        waited
                    );
                    self.warned = true;
                }

                if waited > self.upquery_timeout {
                    return Err(ReadySetError::UpqueryTimeout);
                }
            }

            Ok(())
        })?;

        if self.pending_keys.is_empty() {
            Poll::Ready(Ok(Tagged {
                tag: self.tag,
                v: ReadReply::Normal(Ok(LookupResult::Results(
                    mem::take(&mut self.read),
                    ReadReplyStats { cache_misses: 1 },
                ))),
            }))
        } else {
            Poll::Pending
        }
    }
}

#[cfg(test)]
mod readreply {
    use std::borrow::Cow;

    use noria::{LookupResult, ReadReply, ReadReplyStats, ReadySetError, Tagged};
    use noria_data::DataType;
    use test_strategy::proptest;

    use super::*;

    #[proptest(cases = 5)]
    fn server_read_reply_batch_fungibility(rows: Vec<Vec<DataType>>) {
        assert_eq!(
            bincode::serialize(&ServerReadReplyBatch::Unserialized(rows.clone())).unwrap(),
            bincode::serialize(&ServerReadReplyBatch::serialize(rows)).unwrap()
        );
    }

    fn rtt_ok(data: Vec<Vec<Vec<DataType>>>) {
        let got: Tagged<ReadReply> = bincode::deserialize(
            &bincode::serialize(&Tagged {
                tag: 32,
                v: ReadReply::Normal::<ServerReadReplyBatch>(Ok(LookupResult::Results(
                    data.iter()
                        .map(|d| {
                            ServerReadReplyBatch::serialize(
                                d.iter()
                                    .map(|v| v.iter().map(Cow::Borrowed).collect::<Vec<_>>()),
                            )
                        })
                        .collect(),
                    ReadReplyStats::default(),
                ))),
            })
            .unwrap(),
        )
        .unwrap();

        match got {
            Tagged {
                v: ReadReply::Normal(Ok(LookupResult::Results(got, _))),
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
                v: ReadReply::Normal::<ServerReadReplyBatch>(Ok(LookupResult::Results(
                    Vec::new(),
                    ReadReplyStats::default(),
                ))),
            })
            .unwrap(),
        )
        .unwrap();

        match got {
            Tagged {
                v: ReadReply::Normal(Ok(LookupResult::Results(data, _))),
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
                v: ReadReply::Normal::<ServerReadReplyBatch>(Err(
                    ReadySetError::ViewNotYetAvailable,
                )),
            })
            .unwrap(),
        )
        .unwrap();

        assert!(matches!(
            got,
            Tagged {
                tag: 32,
                v: ReadReply::Normal(Err(ReadySetError::ViewNotYetAvailable))
            }
        ));
    }

    #[test]
    fn rtt_size() {
        let got: Tagged<ReadReply> = bincode::deserialize(
            &bincode::serialize(&Tagged {
                tag: 32,
                v: ReadReply::Size::<ServerReadReplyBatch>(42),
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
        use futures_util::SinkExt;

        let mut z = Vec::new();
        let mut w = async_bincode::AsyncBincodeWriter::from(&mut z).for_async();

        for tag in 0..10 {
            w.send(Tagged {
                tag,
                v: ReadReply::Normal::<ServerReadReplyBatch>(Ok(LookupResult::Results(
                    data.iter()
                        .map(|d| ServerReadReplyBatch::serialize(d))
                        .collect(),
                    ReadReplyStats::default(),
                ))),
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
                    v: ReadReply::Normal(Ok(LookupResult::Results(got, _))),
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
