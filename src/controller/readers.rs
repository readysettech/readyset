use bincode;
use dataflow::backlog::SingleReadHandle;
use dataflow::prelude::*;
use dataflow::Readers;
use futures::future::{self, poll_fn, Either};
use std::cell::RefCell;
use std::collections::HashMap;
use tokio::prelude::*;
use tokio_threadpool::blocking;

use api::{ReadQuery, ReadReply};

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
            let immediate = READERS.with(move |readers_cache| {
                let mut readers_cache = readers_cache.borrow_mut();
                let reader = readers_cache.entry(target.clone()).or_insert_with(|| {
                    let readers = s.lock().unwrap();
                    readers.get(&target).unwrap().clone()
                });

                let mut ret = Vec::with_capacity(keys.len());
                ret.resize(keys.len(), Vec::new());

                // first do non-blocking reads for all keys to trigger any replays
                let found = keys
                    .iter_mut()
                    .map(|key| {
                        let rs = reader.find_and(key, dup, false).map(|r| r.0);
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

                Err((keys, ret))
            });

            match immediate {
                Ok(reply) => Either::A(Either::A(future::ok(reply))),
                Err((keys, ret)) => {
                    if !block {
                        Either::A(Either::A(future::ok(ReadReply::Normal(Ok(ret)))))
                    } else {
                        // TODO: don't actually block here. instead, be a future.
                        Either::A(Either::B(poll_fn(move || {
                            blocking(move || {
                                READERS.with(move |readers_cache| {
                                    // block on all remaining keys
                                    let readers_cache = readers_cache.borrow();
                                    let reader = readers_cache.get(&target).unwrap();
                                    for (i, key) in keys.iter().enumerate() {
                                        if key.is_empty() {
                                            // already have this value
                                        } else {
                                            // note that this *does* mean we'll trigger replay twice for things that
                                            // miss and aren't replayed in time, which is a little sad. but at the same
                                            // time, that replay trigger will just be ignored by the target domain.
                                            ret[i] = reader
                                                .find_and(key, dup, true)
                                                .map(|r| r.0.unwrap_or_default())
                                                .expect(
                                                    "evmap *was* ready, then *stopped* being ready",
                                                )
                                        }
                                    }

                                    ReadReply::Normal(Ok(ret))
                                })
                            }).map_err(|e| panic!(e))
                        })))
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
