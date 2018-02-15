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
                keys,
                block,
            } => ReadReply::Normal(
                keys.iter()
                    .map(|key| {
                        READERS.with(|readers_cache| {
                            let mut readers_cache = readers_cache.borrow_mut();
                            let &mut (ref mut reader, _) =
                                readers_cache.entry(target.clone()).or_insert_with(|| {
                                    let readers = s.lock().unwrap();
                                    readers.get(&target).unwrap().clone()
                                });

                            reader
                                .find_and(
                                    key,
                                    |rs| {
                                        rs.into_iter()
                                            .map(|r| r.iter().map(|v| v.deep_clone()).collect())
                                            .collect()
                                    },
                                    block,
                                )
                                .map(|r| r.0)
                                .map(|r| r.unwrap_or_else(Vec::new))
                        })
                    })
                    .collect(),
            ),
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
        },
        is_local,
    )).unwrap();
}
