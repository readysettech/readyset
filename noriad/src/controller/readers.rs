use bincode;
use dataflow::backlog::SingleReadHandle;
use dataflow::prelude::*;
use dataflow::Readers;
use futures::future::{self, Either};
use std::cell::RefCell;
use std::collections::HashMap;
use std::{mem, time};
use tokio;
use tokio::prelude::*;

use api::{ReadQuery, ReadReply};

/// If a blocking reader finds itself waiting this long for a backfill to complete, it will
/// re-issue the replay request. To avoid the system falling over if replays are slow for a little
/// while, waiting readers will use exponential backoff on this delay if they continue to miss.
const RETRY_TIMEOUT_US: u64 = 1_000;

thread_local! {
    static READERS: RefCell<HashMap<
        (NodeIndex, usize),
        SingleReadHandle,
    >> = Default::default();
}

fn dup(rs: &[Vec<DataType>]) -> Vec<Vec<DataType>> {
    rs.into_iter()
        .map(|r| r.iter().map(|v| v.deep_clone()).collect())
        .collect()
}

pub(crate) fn handle_message(
    m: ReadQuery,
    s: &mut Readers,
) -> impl Future<Item = ReadReply, Error = bincode::Error> + Send {
    match m {
        ReadQuery::Normal {
            target,
            mut keys,
            block,
        } => {
            let immediate = READERS.with(|readers_cache| {
                let mut readers_cache = readers_cache.borrow_mut();
                let reader = readers_cache.entry(target.clone()).or_insert_with(|| {
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
                        }
                    }
                }

                if !ready {
                    return Ok(ReadReply::Normal(Err(())));
                }

                if !block {
                    // trigger backfills for all the keys we missed on for later
                    for key in &keys {
                        if !key.is_empty() {
                            reader.trigger(key);
                        }
                    }
                }

                Err((keys, ret))
            });

            match immediate {
                Ok(reply) => Either::A(Either::A(future::ok(reply))),
                Err((keys, ret)) => {
                    if !block {
                        Either::A(Either::A(future::ok(ReadReply::Normal(Ok(ret)))))
                    } else {
                        let trigger = time::Duration::from_micros(RETRY_TIMEOUT_US);
                        let retry = time::Duration::from_micros(10);
                        let now = time::Instant::now();
                        Either::A(Either::B(BlockingRead {
                            target,
                            keys,
                            read: ret,
                            truth: s.clone(),
                            retry: tokio::timer::Interval::new(now + retry, retry),
                            trigger_timeout: trigger,
                            next_trigger: now,
                        }))
                    }
                }
            }
        }
        ReadQuery::Size { target } => {
            let size = READERS.with(|readers_cache| {
                let mut readers_cache = readers_cache.borrow_mut();
                let reader = readers_cache.entry(target.clone()).or_insert_with(|| {
                    let readers = s.lock().unwrap();
                    readers.get(&target).unwrap().clone()
                });

                reader.len()
            });

            Either::B(future::ok(ReadReply::Size(size)))
        }
    }
}

struct BlockingRead {
    read: Vec<Vec<Vec<DataType>>>,
    target: (NodeIndex, usize),
    keys: Vec<Vec<DataType>>,
    truth: Readers,
    retry: tokio::timer::Interval,
    trigger_timeout: time::Duration,
    next_trigger: time::Instant,
}

impl Future for BlockingRead {
    type Item = ReadReply;
    type Error = bincode::Error;
    fn poll(&mut self) -> Result<Async<Self::Item>, Self::Error> {
        READERS.with(move |readers_cache| {
            let mut readers_cache = readers_cache.borrow_mut();
            let s = &self.truth;
            let target = &self.target;
            let reader = readers_cache.entry(self.target.clone()).or_insert_with(|| {
                let readers = s.lock().unwrap();
                readers.get(target).unwrap().clone()
            });

            let mut triggered = false;
            let mut missing = false;
            let now = time::Instant::now();
            for (i, key) in self.keys.iter_mut().enumerate() {
                if key.is_empty() {
                    // already have this value
                } else {
                    // note that this *does* mean we'll trigger replay multiple times for things
                    // that miss and aren't replayed in time, which is a little sad. but at the
                    // same time, that replay trigger will just be ignored by the target domain.
                    match reader.try_find_and(key, dup).map(|r| r.0) {
                        Ok(Some(rs)) => {
                            self.read[i] = rs;
                            key.clear();
                        }
                        Err(()) => {
                            unreachable!("map became not ready?");
                        }
                        Ok(None) => {
                            if now > self.next_trigger {
                                // maybe the key was filled but then evicted, and we missed it?
                                reader.trigger(key);
                                triggered = true;
                            }
                            missing = true;
                        }
                    }
                }
            }

            if triggered {
                self.trigger_timeout *= 2;
                self.next_trigger = now + self.trigger_timeout;
            }

            if missing {
                loop {
                    match self.retry.poll() {
                        Ok(Async::Ready(Some(_))) => {}
                        Ok(Async::Ready(None)) => unreachable!("interval stopped yielding"),
                        Ok(Async::NotReady) => return Ok(Async::NotReady),
                        Err(e) => unreachable!("{:?}", e),
                    }
                }
            } else {
                Ok(Async::Ready(ReadReply::Normal(Ok(mem::replace(
                    &mut self.read,
                    Vec::new(),
                )))))
            }
        })
    }
}
