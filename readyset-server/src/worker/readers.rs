#![allow(missing_docs)]

use core::task::Context;
use std::collections::hash_map::Entry::Occupied;
use std::future::Future;
use std::task::Poll;
use std::time;
use std::time::Duration;

use async_bincode::AsyncBincodeStream;
use bincode::Options;
use dataflow::prelude::*;
use dataflow::{
    Expr as DfExpr, LookupError, ReaderMap, ReaderUpdatedNotifier, Readers, SingleReadHandle,
};
use failpoint_macros::set_failpoint;
use futures::pin_mut;
use futures_util::future::TryFutureExt;
use pin_project::pin_project;
use readyset_client::consistency::Timestamp;
#[cfg(feature = "failure_injection")]
use readyset_client::failpoints;
use readyset_client::metrics::recorded;
use readyset_client::results::ResultIterator;
use readyset_client::{
    KeyComparison, LookupResult, ReadQuery, ReadReply, ReadReplyStats, ReaderAddress, Tagged,
    ViewQuery,
};
use readyset_errors::internal_err;
use readyset_util::shutdown::ShutdownReceiver;
use serde::ser::Serializer;
use serde::Serialize;
use streaming_iterator::StreamingIterator;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::oneshot;
use tokio_stream::wrappers::TcpListenerStream;
use tokio_stream::StreamExt;
use tokio_tower::multiplex::server;
use tower::Service;
use tracing::{error, instrument, warn};

/// Retry consistency missed reads every this often.
const RETRY_TIMEOUT: Duration = Duration::from_micros(100);

const WAIT_BEFORE_WARNING: Duration = Duration::from_secs(7);

/// A batch of records either intended for local consumption only via the
/// [`ServerReadReplyBatch::Unserialized`] variant, that avoids cloning entirely or for remote
/// serialization using the [`ServerReadReplyBatch: :Serialized`] variant.
#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum ServerReadReplyBatch {
    Serialized {
        /// The serialized data as a bytes slice
        serialized_data: Box<[u8]>,
        /// The number of bytes to skip in [`serialized_data`] before the actual data begins
        skip_bytes: usize,
    },
    Unserialized(ResultIterator),
}

impl ServerReadReplyBatch {
    /// Construct a [`ServerReadReplyBatch`] by serializing a result set, and storing the serialized
    /// bytes.
    fn serialize(mut rs: ResultIterator) -> Self {
        let mut v = Vec::with_capacity(16 * 1024);

        let options = bincode::DefaultOptions::default();

        let mut ser = bincode::Serializer::new(&mut v, options);

        usize::MAX.serialize(&mut ser).unwrap(); // Prepend the maximum possible room for length encoding

        let mut n = 0usize;
        while let Some(row) = rs.next() {
            row.serialize(&mut ser).unwrap();
            n += 1;
        }

        let max_len_enc = options.serialized_size(&usize::MAX).unwrap();
        let len_enc = options.serialized_size(&n).unwrap();
        let skip_bytes = (max_len_enc - len_enc) as usize;

        let mut ser = bincode::Serializer::new(&mut v[skip_bytes..], options);
        // Now encode the proper length
        n.serialize(&mut ser).unwrap();

        Self::Serialized {
            serialized_data: v.into(),
            skip_bytes,
        }
    }

    /// Return this [`ServerReadReplyBatch`] as its unserialized [`ResultIterator`] if it is
    /// [`Unserialized`], otherwise, consume the object and return None.
    ///
    /// This should only be used in cases when we expect the result to be [`Unserialized`].
    pub fn into_unserialized(self) -> Option<ResultIterator> {
        if let ServerReadReplyBatch::Unserialized(d) = self {
            Some(d)
        } else {
            None
        }
    }
}

impl serde::Serialize for ServerReadReplyBatch {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            ServerReadReplyBatch::Serialized {
                serialized_data,
                skip_bytes,
            } => serializer.serialize_bytes(&serialized_data[*skip_bytes..]),
            ServerReadReplyBatch::Unserialized(_) => unreachable!(
                "Unserialized should not be constructed where serialization is expected"
            ),
        }
    }
}

type Reply = ReadySetResult<Tagged<ReadReply<ServerReadReplyBatch>>>;

/// An Ack to resolve a blocking read.
pub type Ack = Option<oneshot::Sender<Reply>>;

/// Creates a handler that can be used to perform read queries against a set of
/// Readers.
#[derive(Clone)]
pub struct ReadRequestHandler {
    global_readers: Readers,
    readers_cache: ReaderMap,
    wait: tokio::sync::mpsc::UnboundedSender<(BlockingRead, Ack)>,
    miss_ctr: metrics::Counter,
    hit_ctr: metrics::Counter,
    upquery_timeout: Duration,
}

/// Represents either a result that was resolved synchronously or one that has to await on a channel
pub enum CallResult<F: Future<Output = Reply>> {
    /// The call was resolved immediately
    Immediate(Reply),
    /// The call will be resolved by polling the provided future
    Async(F),
}

impl ReadRequestHandler {
    /// Creates a new request handler that can be used to query Readers.
    pub fn new(
        readers: Readers,
        wait: tokio::sync::mpsc::UnboundedSender<(BlockingRead, Ack)>,
        upquery_timeout: Duration,
    ) -> Self {
        Self {
            global_readers: readers,
            readers_cache: Default::default(),
            wait,
            miss_ctr: metrics::register_counter!(recorded::SERVER_VIEW_QUERY_MISS),
            hit_ctr: metrics::register_counter!(recorded::SERVER_VIEW_QUERY_HIT),
            upquery_timeout,
        }
    }

    /// Always returns `ServerReadReplyBatch::Unserialized` if `raw_result` is passed. The response
    /// is either an immediate response or a deffered response via a channel
    pub fn handle_normal_read_query(
        &mut self,
        tag: u32,
        target: ReaderAddress,
        query: ViewQuery,
        raw_result: bool,
    ) -> CallResult<impl Future<Output = Reply>> {
        tracing::info!(?query, %raw_result, "handle_normal_read_query");
        let ViewQuery {
            key_comparisons,
            block,
            timestamp,
            filter,
            limit,
            offset,
        } = query;

        macro_rules! reply_with_ok {
            ($e: expr) => {
                return CallResult::Immediate(Ok(Tagged {
                    tag,
                    v: ReadReply::Normal(Ok($e)),
                }))
            };
        }

        macro_rules! reply_with_error {
            ($e: expr) => {
                return CallResult::Immediate(Ok(Tagged {
                    tag,
                    v: ReadReply::Normal(Err($e)),
                }))
            };
        }

        let reader = get_reader_from_cache(&target, &mut self.readers_cache, &self.global_readers);
        let reader = match reader {
            Ok(r) => r,
            Err(e) => reply_with_error!(e),
        };

        let consistency_miss = !has_sufficient_timestamp(reader, &timestamp);

        let (keys_to_replay, receiver) = match reader.get_multi_with_notifier(&key_comparisons) {
            Err(LookupError::NotReady) => reply_with_error!(ReadySetError::ViewNotYetAvailable),
            Err(LookupError::Destroyed) => reply_with_error!(ReadySetError::ViewDestroyed),
            Err(LookupError::Error(e)) => reply_with_error!(e),
            // We missed some keys
            Err(LookupError::Miss((misses, _))) if consistency_miss => (misses, None),
            Err(LookupError::Miss((misses, notifier))) => (misses, Some(notifier)),
            // We hit on all keys, but there is a consistency miss. This just counts as a miss,
            // but no keys needs triggering.
            Ok(_) if consistency_miss => (vec![], None),
            Ok(hit) => {
                tracing::info!(?hit, "handle_normal_read_query hit");
                // We hit on all keys, and there is no consistency miss, can return results
                // immediately
                self.hit_ctr.increment(1);

                let results = ResultIterator::new(hit, &reader.post_lookup, limit, offset, filter);

                let results = if raw_result {
                    ServerReadReplyBatch::Unserialized(results)
                } else {
                    ServerReadReplyBatch::serialize(results)
                };

                reply_with_ok!(LookupResult::Results(
                    vec![results],
                    ReadReplyStats::default()
                ));
            }
        };
        tracing::info!(?receiver, "handle_normal_read_query miss");

        self.miss_ctr.increment(1);

        // Trigger backfills for all the keys we missed on, regardless of a consistency hit/miss
        if !keys_to_replay.is_empty() {
            reader.trigger(keys_to_replay.into_iter().map(|k| k.into_owned()));
        }

        let read = BlockingRead {
            tag,
            target,
            key_comparisons,
            truth: self.global_readers.clone(),
            first: time::Instant::now(),
            warned: false,
            limit,
            offset,
            filter,
            timestamp,
            upquery_timeout: self.upquery_timeout,
            raw_result,
            receiver,
            eviction_epoch: reader.eviction_epoch(),
        };

        if !block {
            let _ = self.wait.send((read, None));
            reply_with_ok!(LookupResult::NonBlockingMiss);
        } else {
            let (tx, rx) = oneshot::channel();

            let r = self.wait.send((read, Some(tx)));

            if r.is_err() {
                // we're shutting down
                return CallResult::Immediate(Err(ReadySetError::ServerShuttingDown));
            }

            CallResult::Async(rx.map_ok_or_else(|e| Err(internal_err!("{e}")), |o| o))
        }
    }

    fn handle_size_query(&mut self, tag: u32, target: &ReaderAddress) -> Reply {
        let reader = get_reader_from_cache(target, &mut self.readers_cache, &self.global_readers)?;

        Ok(Tagged {
            tag,
            v: ReadReply::Size(reader.len()),
        })
    }

    fn handle_keys_query(&mut self, tag: u32, target: &ReaderAddress) -> Reply {
        let reader = get_reader_from_cache(target, &mut self.readers_cache, &self.global_readers)?;

        Ok(Tagged {
            tag,
            v: ReadReply::Keys(reader.keys()),
        })
    }
}

impl Service<Tagged<ReadQuery>> for ReadRequestHandler {
    type Response = Tagged<ReadReply<ServerReadReplyBatch>>;
    type Error = ReadySetError;
    type Future = impl Future<Output = Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    #[instrument(level = "info", skip_all)]
    #[inline]
    fn call(&mut self, m: Tagged<ReadQuery>) -> Self::Future {
        let tag = m.tag;
        let res = match m.v {
            ReadQuery::Normal { target, query } => {
                let span = readyset_tracing::child_span!(INFO, "normal_read_query");
                let _g = span.enter();
                self.handle_normal_read_query(tag, target, query, false)
            }
            ReadQuery::Size { ref target } => {
                let span = readyset_tracing::child_span!(INFO, "size_query");
                let _g = span.enter();
                CallResult::Immediate(self.handle_size_query(tag, target))
            }
            ReadQuery::Keys { ref target } => {
                let span = readyset_tracing::child_span!(INFO, "keys_query");
                let _g = span.enter();
                CallResult::Immediate(self.handle_keys_query(tag, target))
            }
        };

        async {
            match res {
                CallResult::Immediate(immediate_response) => immediate_response,
                CallResult::Async(async_response) => async_response.await,
            }
        }
    }
}

/// A spawned task responsible for repeating reads that could not be immediately served from cache,
/// until they succeed.
pub async fn retry_misses(mut rx: UnboundedReceiver<(BlockingRead, Ack)>) {
    let upquery_hist = metrics::register_histogram!(recorded::SERVER_VIEW_UPQUERY_DURATION);
    let mut reader_cache: ReaderMap = Default::default();

    while let Some((mut pending, ack)) = rx.recv().await {
        loop {
            if let Some(recv) = &mut pending.receiver {
                // If a receiever is available (on miss) then we simply wait for a notification that
                // a hole has been filled, then recheck
                let _ = recv.recv().await;
                while !recv.is_empty() {
                    // This drains all the messages from the notifier so we don't get woken right up
                    // again
                    let _ = recv.try_recv();
                }
            } else {
                // For consistency misses we don't get notifications, so check periodically
                tokio::time::sleep(RETRY_TIMEOUT).await;
            }

            if let Poll::Ready(res) = pending.check(&mut reader_cache) {
                upquery_hist.record(pending.first.elapsed().as_micros() as f64);
                if let Some(a) = ack {
                    let _ = a.send(res);
                };
                break;
            }
        }
    }
}

pub(crate) async fn listen(
    on: tokio::net::TcpListener,
    readers: Readers,
    upquery_timeout: Duration,
    shutdown_rx: ShutdownReceiver,
) {
    let stream = shutdown_rx.clone().wrap_stream(TcpListenerStream::new(on));
    pin_mut!(stream);
    while let Some(stream) = stream.next().await {
        let mut shutdown_rx = shutdown_rx.clone();
        set_failpoint!(failpoints::READ_QUERY);
        if stream.is_err() {
            // io error from client: just ignore it
            continue;
        }

        let stream = stream.unwrap();
        let readers = readers.clone();
        stream.set_nodelay(true).expect("could not set TCP_NODELAY");

        // future that ensures all blocking reads are handled in FIFO order
        // and avoid hogging the executors with read retries
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<(BlockingRead, Ack)>();
        let mut retry_misses_shutdown_rx = shutdown_rx.clone();
        tokio::spawn(async move {
            tokio::select! {
                _ = retry_misses(rx) => {},
                _ = retry_misses_shutdown_rx.recv() => {},
            }
        });

        let r = ReadRequestHandler::new(readers, tx, upquery_timeout);

        let server =
            server::Server::new(AsyncBincodeStream::from(stream).for_async(), r).map_err(|e| {
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
            });

        tokio::spawn(async move {
            tokio::select! {
                _ = server => {},
                _ = shutdown_rx.recv() => {},
            }
        });
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
    target: ReaderAddress,
    key_comparisons: Vec<KeyComparison>,
    truth: Readers,
    limit: Option<usize>,
    offset: Option<usize>,
    filter: Option<DfExpr>,
    first: time::Instant,
    warned: bool,
    timestamp: Option<Timestamp>,
    upquery_timeout: Duration,
    raw_result: bool,
    receiver: Option<ReaderUpdatedNotifier>,
    eviction_epoch: usize,
}

impl std::fmt::Debug for BlockingRead {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlockingRead")
            .field("tag", &self.tag)
            .field("target", &self.target)
            .field("key_comparisons", &self.key_comparisons)
            .field("first", &self.first)
            .field("timestamp", &self.timestamp)
            .field("eviction_epoch", &self.eviction_epoch)
            .finish()
    }
}

impl BlockingRead {
    /// Check if we have the results for this blocking read.
    pub fn check(&mut self, reader_cache: &mut ReaderMap) -> Poll<Reply> {
        let s = &self.truth;
        let target = &self.target;
        let reader = reader_cache.entry(self.target.clone()).or_insert_with(|| {
            let readers = s.lock().unwrap();
            readers.get(target).unwrap().clone()
        });

        let consistency_miss = !has_sufficient_timestamp(reader, &self.timestamp);

        let still_waiting = match reader.get_multi(&self.key_comparisons) {
            // We hit on all keys, but there is a consistency miss. This just counts as a miss,
            // but no keys needs triggering.
            Ok(_) if consistency_miss => vec![],
            Err(LookupError::Miss((misses, _))) => misses,
            Err(_) => return Poll::Ready(Err(ReadySetError::ServerShuttingDown)),
            Ok(hit) => {
                // We hit on all keys, and there is no consistency miss, can return results
                let results = ResultIterator::new(
                    hit,
                    &reader.post_lookup,
                    self.limit,
                    self.offset,
                    self.filter.take(),
                );

                let results = if self.raw_result {
                    ServerReadReplyBatch::Unserialized(results)
                } else {
                    ServerReadReplyBatch::serialize(results)
                };

                return Poll::Ready(Ok(Tagged {
                    tag: self.tag,
                    v: ReadReply::Normal(Ok(LookupResult::Results(
                        vec![results],
                        ReadReplyStats::default(),
                    ))),
                }));
            }
        };

        // Check if we reached a warning timeout
        if self.first.elapsed() > WAIT_BEFORE_WARNING && !self.warned {
            warn!(reader=%target.name.display_unquoted(), keys = ?still_waiting, "Read stuck waiting for keys");
            self.warned = true;
        }

        let cur_eviction_epoch = reader.eviction_epoch();
        // Only retrigger if there was an eviction since we last checked
        if cur_eviction_epoch > self.eviction_epoch {
            self.eviction_epoch = cur_eviction_epoch;
            // Retrigger all un-read keys. Its possible they could have been filled and then
            // evicted again without us reading it.
            if !reader.trigger(still_waiting.into_iter().map(|v| v.into_owned())) {
                // server is shutting down and won't do the backfill
                return Poll::Ready(Err(ReadySetError::ServerShuttingDown));
            }
        }

        if self.first.elapsed() > self.upquery_timeout {
            Poll::Ready(Err(ReadySetError::UpqueryTimeout))
        } else {
            Poll::Pending
        }
    }
}

fn get_reader_from_cache<'a>(
    target: &ReaderAddress,
    readers_cache: &'a mut ReaderMap,
    global_readers: &Readers,
) -> ReadySetResult<&'a mut SingleReadHandle> {
    Ok(match readers_cache.entry(target.clone()) {
        Occupied(v) if !v.get().was_dropped() => v.into_mut(),
        // If the entry is Vacant _or_ the WriteHandle was dropped out from under us, we need to
        // refer to the global_readers.
        entry => {
            let readers = global_readers.lock().unwrap();
            #[allow(clippy::significant_drop_in_scrutinee)]
            match readers.get(target) {
                None => {
                    if let Occupied(v) = entry {
                        // This reader's write handle was dropped, so it will not be usable
                        // anymore.
                        v.remove();
                    }
                    return Err(ReadySetError::ReaderNotFound);
                }
                Some(reader) => entry.insert_entry(reader.clone()).into_mut(),
            }
        }
    })
}

#[cfg(test)]
mod readreply {
    use readyset_client::results::SharedResults;
    use readyset_client::{LookupResult, ReadReply, ReadReplyStats, Tagged};
    use readyset_data::DfValue;
    use readyset_errors::ReadySetError;

    use super::*;

    fn rtt_ok(data: SharedResults) {
        let got: Tagged<ReadReply> = bincode::deserialize(
            &bincode::serialize(&Tagged {
                tag: 32,
                v: ReadReply::Normal::<ServerReadReplyBatch>(Ok(LookupResult::Results(
                    data.iter()
                        .cloned()
                        .map(|d| {
                            ServerReadReplyBatch::serialize(ResultIterator::new(
                                [d].into(),
                                &Default::default(),
                                None,
                                None,
                                None,
                            ))
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
                for (got, expected) in got
                    .into_iter()
                    .flatten()
                    .zip(data.iter().flat_map(|r| r.iter()))
                {
                    assert_eq!(got.as_slice(), &expected[..]);
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

    fn rows_vec<III, II, I>(data: III) -> SharedResults
    where
        III: IntoIterator<Item = II>,
        II: IntoIterator<Item = I>,
        I: IntoIterator<Item = DfValue>,
    {
        data.into_iter()
            .map(|rows| {
                triomphe::Arc::new(
                    rows.into_iter()
                        .map(|row| row.into_iter().collect())
                        .collect(),
                )
            })
            .collect()
    }

    #[test]
    fn rtt_normal_one() {
        rtt_ok(rows_vec([[[DfValue::from(1)]]]));
    }

    #[test]
    fn rtt_normal_multifield() {
        rtt_ok(rows_vec([[[DfValue::from(1), DfValue::from(42)]]]));
    }

    #[test]
    fn rtt_normal_multirow() {
        rtt_ok(rows_vec([[[DfValue::from(1)], [DfValue::from(42)]]]));
    }

    #[test]
    fn rtt_normal_multibatch() {
        rtt_ok(rows_vec([[[DfValue::from(1)]], [[DfValue::from(42)]]]));
    }

    #[test]
    fn rtt_normal_multi() {
        rtt_ok(rows_vec([
            [
                [DfValue::from(1), DfValue::from(42)],
                [DfValue::from(43), DfValue::from(2)],
            ],
            [
                [DfValue::from(2), DfValue::from(43)],
                [DfValue::from(44), DfValue::from(3)],
            ],
        ]));
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

    async fn async_bincode_rtt_ok(data: SharedResults) {
        use futures_util::SinkExt;

        let mut z = Vec::new();
        let mut w = async_bincode::AsyncBincodeWriter::from(&mut z).for_async();

        for tag in 0..10 {
            w.send(Tagged {
                tag,
                v: ReadReply::Normal::<ServerReadReplyBatch>(Ok(LookupResult::Results(
                    data.iter()
                        .cloned()
                        .map(|d| {
                            ServerReadReplyBatch::serialize(ResultIterator::new(
                                [d].into(),
                                &Default::default(),
                                None,
                                None,
                                None,
                            ))
                        })
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
                    for (got, expected) in got
                        .into_iter()
                        .flatten()
                        .zip(data.iter().flat_map(|r| r.iter()))
                    {
                        assert_eq!(got.as_slice(), &expected[..]);
                    }
                }
                r => panic!("{:?}", r),
            }
        }
    }

    #[tokio::test]
    async fn async_bincode_rtt_empty() {
        async_bincode_rtt_ok(rows_vec::<[[[DfValue; 0]; 0]; 0], _, _>([])).await;
    }

    #[tokio::test]
    async fn async_bincode_rtt_one_empty() {
        async_bincode_rtt_ok(rows_vec::<[[[DfValue; 0]; 0]; 1], _, _>([[]])).await;
    }

    #[tokio::test]
    async fn async_bincode_rtt_one_one_empty() {
        async_bincode_rtt_ok(rows_vec([[[]]])).await;
    }

    #[tokio::test]
    async fn async_bincode_rtt_multi() {
        async_bincode_rtt_ok(rows_vec([
            vec![
                vec![DfValue::from(1), DfValue::from(42)],
                vec![DfValue::from(43), DfValue::from(2)],
            ],
            vec![
                vec![DfValue::from(2), DfValue::from(43)],
                vec![DfValue::from(44), DfValue::from(3)],
            ],
            vec![vec![]],
            vec![],
        ]))
        .await;
    }
}
