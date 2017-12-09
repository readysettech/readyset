use std::collections::HashMap;
use channel::rpc::RpcServiceEndpoint;
use channel::tcp::TryRecvError;
use dataflow::Readers;
use dataflow::checktable::TokenGenerator;
use dataflow::backlog::SingleReadHandle;
use dataflow::prelude::*;
use std::io;
use std::sync::{Arc, Mutex};
use std::cell::RefCell;
use std::time::Duration;
use rayon;
use mio::{Events, Poll, PollOpt, Ready, Token};
use slab::Slab;
use vec_map::VecMap;
use std::sync::{atomic, mpsc};

use controller::{LocalOrNot, ReadQuery, ReadReply};

type Rpc = RpcServiceEndpoint<LocalOrNot<ReadQuery>, LocalOrNot<ReadReply>>;

thread_local! {
    static POLL: RefCell<Option<Arc<Poll>>> = Default::default();
    static TX: RefCell<Option<mpsc::Sender<
        (
            usize,
            Option<Rpc>,
        ),
    >>> = Default::default();
    static ALL_READERS: RefCell<Option<Readers>> = Default::default();
    static READERS: RefCell<HashMap<
        (NodeIndex, usize),
        (SingleReadHandle, Option<TokenGenerator>),
    >> = Default::default();
}

/// Use the given polling loop and readers object to serve reads.
pub(crate) fn serve(
    listener: ::mio::net::TcpListener,
    readers: Readers,
    pool_size: usize,
    exit: Arc<atomic::AtomicBool>,
) {
    let mut conns_tokens: Slab<()> = Slab::new();
    let mut conns: VecMap<Rpc> = Default::default();

    let poll = Arc::new(Poll::new().unwrap());
    let listen_token = conns_tokens.insert(());
    poll.register(
        &listener,
        Token(listen_token),
        Ready::readable(),
        PollOpt::level(),
    ).unwrap();

    let (tx, rx) = mpsc::channel();
    let pool_poll = poll.clone();
    let tx = Arc::new(Mutex::new(tx));
    let pool = rayon::Configuration::new()
        .num_threads(pool_size)
        .thread_name(|i| format!("reader{}", i))
        .start_handler(move |_| {
            let tx = tx.lock().unwrap().clone();
            ALL_READERS.with(|s| *s.borrow_mut() = Some(readers.clone()));
            POLL.with(|s| *s.borrow_mut() = Some(pool_poll.clone()));
            TX.with(|s| *s.borrow_mut() = Some(tx));
        })
        .build()
        .unwrap();

    let mut events = Events::with_capacity(10);
    while !exit.load(atomic::Ordering::SeqCst) {
        if let Err(e) = poll.poll(&mut events, Some(Duration::from_secs(1))) {
            if e.kind() == io::ErrorKind::Interrupted {
                // spurious wakeup
                continue;
            } else if e.kind() == io::ErrorKind::TimedOut {
                // need to re-check timers
                // *should* be handled by mio and return Ok() with no events
                continue;
            } else {
                panic!("{}", e);
            }
        }

        if events.is_empty() {
            // we must have timed out -- check timers
            continue;
        }

        for e in &events {
            assert!(e.readiness().is_readable());
            let Token(t) = e.token();

            if t == listen_token {
                while let Ok((stream, _src)) = listener.accept() {
                    let token = conns_tokens.insert(());
                    poll.register(
                        &stream,
                        Token(token),
                        Ready::readable(),
                        PollOpt::level() | PollOpt::oneshot(),
                    ).unwrap();

                    let rpc = RpcServiceEndpoint::new(stream);
                    conns.insert(token, rpc);
                }
            } else {
                while !conns.contains_key(t) {
                    // worker must have re-registered this connection
                    // which means it must also have returned it on rx
                    let (tok, rse) = rx.recv().unwrap();

                    if let Some(rse) = rse {
                        conns.insert(tok, rse);
                    } else {
                        // connection was dropped
                        conns_tokens.remove(tok);
                        if tok == t {
                            unimplemented!();
                        }
                    }
                }

                let conn = conns.remove(t).unwrap();
                pool.spawn(move || handle_conn(t, conn));
            }
        }
    }
}

fn handle_conn(token: usize, mut conn: Rpc) {
    TX.with(|tx| {
        let mut tx = tx.borrow_mut();
        let tx = tx.as_mut().unwrap();

        loop {
            match conn.try_recv() {
                Ok(m) => {
                    handle_message(m, &mut conn);
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::DeserializationError(..)) => {
                    // XXX
                    tx.send((token, None)).is_ok();
                    return;
                }
                Err(TryRecvError::Disconnected) => {
                    tx.send((token, None)).is_ok();
                    return;
                }
            }
        }

        // re-register the socket so we get events for it again. it'd be better if we send before
        // we re-register, but we can't do that since we give up the stream (and thus can't produce
        // an Evented to give to reregister)
        POLL.with(|poll| {
            poll.borrow()
                .as_ref()
                .unwrap()
                .reregister(
                    conn.get_ref(),
                    Token(token),
                    Ready::readable(),
                    PollOpt::level() | PollOpt::oneshot(),
                )
                .unwrap()
        });

        tx.send((token, Some(conn))).is_ok();
    });
}

fn handle_message(m: LocalOrNot<ReadQuery>, conn: &mut Rpc) {
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
                                    ALL_READERS.with(|readers| {
                                        let readers = readers.borrow();
                                        let readers = readers.as_ref().unwrap().lock().unwrap();
                                        readers.get(&target).unwrap().clone()
                                    })
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
                                    ALL_READERS.with(|readers| {
                                        let readers = readers.borrow();
                                        let readers = readers.as_ref().unwrap().lock().unwrap();
                                        readers.get(&target).unwrap().clone()
                                    })
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
