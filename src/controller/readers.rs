use std::collections::HashMap;
use channel::rpc::RpcServiceEndpoint;
use dataflow::Readers;
use dataflow::checktable::TokenGenerator;
use dataflow::backlog::SingleReadHandle;
use dataflow::prelude::*;
use std::cell::RefCell;

use controller::{LocalOrNot, ReadQuery, ReadReply};

pub(crate) type Rpc = RpcServiceEndpoint<LocalOrNot<ReadQuery>, LocalOrNot<ReadReply>>;

thread_local! {
    static READERS: RefCell<HashMap<
        (NodeIndex, usize),
        (SingleReadHandle, Option<TokenGenerator>),
    >> = Default::default();
}

pub(crate) fn handle_message(m: LocalOrNot<ReadQuery>, conn: &mut Rpc, s: &mut Readers) {
    let is_local = m.is_local();
    conn.send(&LocalOrNot::make(
        match unsafe { m.take() } {
            ReadQuery::Normal {
                target,
                mut keys,
                block,
            } => ReadReply::Normal(READERS.with(move |readers_cache| {
                let mut readers_cache = readers_cache.borrow_mut();
                let &mut (ref mut reader, _) =
                    readers_cache.entry(target.clone()).or_insert_with(|| {
                        let readers = s.lock().unwrap();
                        readers.get(&target).unwrap().clone()
                    });

                let mut ret = Vec::with_capacity(keys.len());
                ret.resize(keys.len(), Ok(Vec::new()));

                let dup = |rs: &[Vec<DataType>]| {
                    rs.into_iter()
                        .map(|r| r.iter().map(|v| v.deep_clone()).collect())
                        .collect()
                };
                let dup = &dup;

                // first do non-blocking reads for all keys to trigger any replays
                let found = keys.iter_mut()
                    .map(|key| {
                        let rs = reader.find_and(key, dup, false).map(|r| r.0);
                        (key, rs)
                    })
                    .enumerate();

                for (i, (key, v)) in found {
                    match v {
                        Ok(Some(rs)) => {
                            // immediate hit!
                            ret[i] = Ok(rs);
                            *key = DataType::None;
                        }
                        Err(()) => {
                            // map not yet ready
                            ret[i] = Err(());
                            *key = DataType::None;
                        }
                        Ok(None) => {
                            // triggered partial replay
                        }
                    }
                }

                if !block {
                    return ret;
                }

                // block on all remaining keys
                for (i, key) in keys.iter().enumerate() {
                    if let DataType::None = *key {
                        // already have this value
                    } else {
                        // note that this *does* mean we'll trigger replay twice for things that
                        // miss and aren't replayed in time, which is a little sad. but at the same
                        // time, that replay trigger will just be ignored by the target domain.
                        ret[i] = reader
                            .find_and(key, dup, true)
                            .map(|r| r.0.unwrap_or_default());
                    }
                }

                ret
            })),
            ReadQuery::WithToken { target, keys } => ReadReply::WithToken(
                keys.into_iter()
                    .map(|key| {
                        READERS.with(|readers_cache| {
                            let mut readers_cache = readers_cache.borrow_mut();
                            let &mut (ref mut reader, ref mut generator) =
                                readers_cache.entry(target.clone()).or_insert_with(|| {
                                    let readers = s.lock().unwrap();
                                    readers.get(&target).unwrap().clone()
                                });

                            reader
                                .find_and(
                                    &key,
                                    |rs| {
                                        rs.into_iter()
                                            .map(|r| r.iter().map(|v| v.deep_clone()).collect())
                                            .collect()
                                    },
                                    true,
                                )
                                .map(|r| (r.0.unwrap_or_else(Vec::new), r.1))
                                .map(|r| (r.0, generator.as_ref().unwrap().generate(r.1, key)))
                        })
                    })
                    .collect(),
            ),
            ReadQuery::Size { target } => {
                let size = READERS.with(|readers_cache| {
                    let mut readers_cache = readers_cache.borrow_mut();
                    let &mut (ref mut reader, _) =
                        readers_cache.entry(target.clone()).or_insert_with(|| {
                            let readers = s.lock().unwrap();
                            readers.get(&target).unwrap().clone()
                        });

                    reader.len()
                });

                ReadReply::Size(size)
            }
        },
        is_local,
    )).unwrap();
}
