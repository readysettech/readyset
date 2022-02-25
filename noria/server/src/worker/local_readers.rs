//! A duplicate of the `readers` file that instead returns a ReadReplyBatch rather than a
//! SerializedReadReplyBatch.

#![allow(missing_docs)]
use std::borrow::Cow;
use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::future::Future;
use std::task::Poll;
use std::time::Duration;
use std::{mem, time};

use dataflow::prelude::*;
use dataflow::{Expression as DataflowExpression, Readers, SingleReadHandle};
use futures_util::future;
use futures_util::future::{Either, FutureExt};
use launchpad::intervals;
use nom_sql::SqlIdentifier;
use noria::consistency::Timestamp;
use noria::metrics::recorded;
use noria::{
    KeyComparison, LookupResult, ReadReply, ReadReplyBatch, ReadReplyStats, Tagged, ViewQuery,
};
use noria_errors::internal_err;
use pin_project::pin_project;
use tracing::warn;

use crate::worker::readers::READERS;

/// If a blocking reader finds itself waiting this long for a backfill to complete, it will
/// re-issue the replay request. To avoid the system falling over if replays are slow for a little
/// while, waiting readers will use exponential backoff on this delay if they continue to miss.
const TRIGGER_TIMEOUT_MS: u64 = 20;

/// An Ack to resolve a blocking read.
pub type Ack =
    tokio::sync::oneshot::Sender<Result<Tagged<ReadReply<ReadReplyBatch>>, ReadySetError>>;

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

    pub fn handle_normal_read_query(
        &mut self,
        tag: u32,
        target: (NodeIndex, SqlIdentifier, usize),
        query: ViewQuery,
    ) -> impl Future<Output = Result<Tagged<ReadReply<ReadReplyBatch>>, ReadySetError>> + Send {
        let ViewQuery {
            mut key_comparisons,
            block,
            timestamp,
            filter,
        } = query;
        let immediate = READERS.with(|readers_cache| {
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
            for (i, key) in key_comparisons.drain(..).enumerate() {
                if !ready {
                    ret.push(ReadReplyBatch::empty());
                    continue;
                }

                use dataflow::LookupError::*;
                match do_lookup(reader, &key, &filter) {
                    Ok(rs) => {
                        if consistency_miss {
                            ret.push(ReadReplyBatch::empty());
                        } else {
                            // immediate hit!
                            ret.push(rs);
                        }
                    }
                    Err(NotReady) => {
                        // map not yet ready
                        ready = false;
                        ret.push(ReadReplyBatch::empty());
                    }
                    Err(Error(e)) => {
                        return Ok(Ok(Tagged {
                            tag,
                            v: ReadReply::Normal(Err(e)),
                        }))
                    }
                    Err(miss) => {
                        // need to trigger partial replay for this key
                        ret.push(ReadReplyBatch::empty());

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
                            ret.extend(
                                non_miss_ranges
                                    .into_iter()
                                    .flat_map(|key| do_lookup(reader, &key, &filter)),
                            )
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
}

fn sorry_cows(cows: Vec<Vec<Cow<'_, DataType>>>) -> Vec<Vec<DataType>> {
    cows.into_iter()
        .map(|v| v.into_iter().map(|cow| (*cow).clone()).collect::<Vec<_>>())
        .collect()
}

fn do_lookup(
    reader: &SingleReadHandle,
    key: &KeyComparison,
    filter: &Option<DataflowExpression>,
) -> Result<ReadReplyBatch, dataflow::LookupError> {
    if let Some(equal) = &key.equal() {
        reader
            .try_find_and(*equal, |rs| {
                let filtered = reader.post_lookup.process(rs, filter)?;
                Ok(ReadReplyBatch(sorry_cows(filtered)))
            })
            .map(|r| r.0)
    } else {
        if key.is_reversed_range() {
            warn!("Reader received lookup for range with start bound above end bound; returning empty result set");
            return Ok(ReadReplyBatch::empty());
        }

        reader
            .try_find_range_and(&key, |r| Ok(r.into_iter().cloned().collect::<Vec<_>>()))
            .and_then(|(rs, _)| {
                Ok(ReadReplyBatch(sorry_cows(reader.post_lookup.process(
                    rs.into_iter().flatten().collect::<Vec<_>>().iter(),
                    filter,
                )?)))
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
    // serialized records for keys we have already read
    read: Vec<ReadReplyBatch>,
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
    pub fn check(&mut self) -> Poll<Result<Tagged<ReadReply<ReadReplyBatch>>, ReadySetError>> {
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

                    match do_lookup(reader, &key, &self.filter) {
                        Ok(rs) => {
                            read[read_i] = rs;
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
                    ReadReplyStats::default(),
                ))),
            }))
        } else {
            Poll::Pending
        }
    }
}
