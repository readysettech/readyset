use bincode;
use dataflow::backlog::SingleReadHandle;
use dataflow::prelude::*;
use dataflow::Readers;
use futures::{
    self, future::{self, Either},
};
use std::cell::RefCell;
use std::collections::HashMap;
use std::mem;
use tokio::prelude::*;

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
            let immediate = READERS.with(|readers_cache| {
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
                        Either::A(Either::B(BlockingRead {
                            target,
                            keys,
                            read: ret,
                            truth: s.clone(),
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

            let mut missing = false;
            for (i, key) in self.keys.iter_mut().enumerate() {
                if key.is_empty() {
                    // already have this value
                } else {
                    // note that this *does* mean we'll trigger replay multiple times for things
                    // that miss and aren't replayed in time, which is a little sad. but at the
                    // same time, that replay trigger will just be ignored by the target domain.
                    // TODO: maybe introduce a tokio::timer::Delay here?
                    match reader.find_and(key, dup, false).map(|r| r.0) {
                        Ok(Some(rs)) => {
                            self.read[i] = rs;
                            key.clear();
                        }
                        Err(()) => {
                            unreachable!("map became not ready?");
                        }
                        Ok(None) => {
                            // triggered partial replay
                            missing = true;
                        }
                    }
                }
            }

            if missing {
                futures::task::current().notify();
                Ok(Async::NotReady)
            } else {
                Ok(Async::Ready(ReadReply::Normal(Ok(mem::replace(
                    &mut self.read,
                    Vec::new(),
                )))))
            }
        })
    }
}
