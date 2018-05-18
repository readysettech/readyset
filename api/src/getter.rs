use channel::rpc::RpcClient;
use {ExclusiveConnection, LocalBypass, LocalOrNot, SharedConnection};

use basics::*;

use std::cell::RefCell;
use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::rc::Rc;

pub(crate) type GetterRpc = Rc<RefCell<RpcClient<LocalOrNot<ReadQuery>, LocalOrNot<ReadReply>>>>;

/// A request to read a specific key.
#[derive(Serialize, Deserialize, Debug)]
pub enum ReadQuery {
    /// Read normally
    Normal {
        /// Where to read from
        target: (NodeIndex, usize),
        /// Keys to read with
        keys: Vec<Vec<DataType>>,
        /// Whether to block if a partial replay is triggered
        block: bool,
    },
    /*
    /// Read and also get a checktable token
    WithToken {
        /// Where to read from
        target: (NodeIndex, usize),
        /// Keys to read with
        keys: Vec<Vec<DataType>>,
    },
    */
    /// Read the size of a leaf view
    Size {
        /// Where to read from
        target: (NodeIndex, usize),
    },
}

/// The contents of a specific key
#[derive(Serialize, Deserialize, Debug)]
pub enum ReadReply {
    /// Read normally.
    /// Errors if view isn't ready yet.
    Normal(Result<Vec<Datas>, ()>),
    /*
    /// Read and got checktable tokens.
    /// Errors if view isn't ready yet.
    WithToken(Result<Vec<(Datas, checktable::Token)>, ()>),
    */
    /// Read size of view
    Size(usize),
}

/// Serializeable version of a `RemoteGetter`.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[doc(hidden)]
pub struct RemoteGetterBuilder {
    pub node: NodeIndex,
    pub columns: Vec<String>,
    pub shards: Vec<(SocketAddr, bool)>,
    // one per shard
    pub local_ports: Vec<u16>,
}

impl RemoteGetterBuilder {
    /// Build a `RemoteGetter` out of a `RemoteGetterBuilder`
    pub fn build_exclusive(self) -> RemoteGetter<ExclusiveConnection> {
        let conns = self
            .shards
            .iter()
            .map(move |&(ref addr, is_local)| {
                Rc::new(RefCell::new(RpcClient::connect(addr, is_local).unwrap()))
            })
            .collect();

        RemoteGetter {
            node: self.node,
            columns: self.columns,
            shard_addrs: self.shards,
            shards: conns,
            exclusivity: ExclusiveConnection,
        }
    }

    /// Set the local port to bind to when making the shared connection.
    pub(crate) fn with_local_port(mut self, port: u16) -> RemoteGetterBuilder {
        assert!(self.local_ports.is_empty());
        self.local_ports = vec![port];
        self
    }

    /// Build a `RemoteGetter` out of a `RemoteGetterBuilder`
    pub(crate) fn build(
        mut self,
        rpcs: &mut HashMap<(SocketAddr, usize), GetterRpc>,
    ) -> RemoteGetter<SharedConnection> {
        let sports = &mut self.local_ports;
        let conns = self
            .shards
            .iter()
            .enumerate()
            .map(move |(shardi, &(ref addr, is_local))| {
                use std::collections::hash_map::Entry;

                // one entry per shard so that we can send sharded requests in parallel even if
                // they happen to be targeting the same machine.
                match rpcs.entry((*addr, shardi)) {
                    Entry::Occupied(e) => Rc::clone(e.get()),
                    Entry::Vacant(h) => {
                        let c =
                            RpcClient::connect_from(sports.get(shardi).map(|&p| p), addr, is_local)
                                .unwrap();
                        if shardi >= sports.len() {
                            assert!(shardi == sports.len());
                            sports.push(c.local_addr().unwrap().port());
                        }

                        let c = Rc::new(RefCell::new(c));
                        h.insert(Rc::clone(&c));
                        c
                    }
                }
            })
            .collect();

        RemoteGetter {
            node: self.node,
            columns: self.columns,
            shard_addrs: self.shards,
            shards: conns,
            exclusivity: SharedConnection,
        }
    }
}

/// Struct to query the contents of a materialized view.
pub struct RemoteGetter<E = SharedConnection> {
    node: NodeIndex,
    columns: Vec<String>,
    shards: Vec<GetterRpc>,
    shard_addrs: Vec<(SocketAddr, bool)>,

    #[allow(dead_code)]
    exclusivity: E,
}

impl Clone for RemoteGetter<SharedConnection> {
    fn clone(&self) -> Self {
        RemoteGetter {
            node: self.node,
            columns: self.columns.clone(),
            shards: self.shards.clone(),
            shard_addrs: self.shard_addrs.clone(),
            exclusivity: SharedConnection,
        }
    }
}

unsafe impl Send for RemoteGetter<ExclusiveConnection> {}

impl RemoteGetter<SharedConnection> {
    /// Change this getter into one with a dedicated connection the server so it can be sent across
    /// threads.
    pub fn into_exclusive(self) -> RemoteGetter<ExclusiveConnection> {
        RemoteGetterBuilder {
            node: self.node,
            local_ports: vec![],
            columns: self.columns,
            shards: self.shard_addrs,
        }.build_exclusive()
    }
}

impl<E> RemoteGetter<E> {
    /// Get the local address this getter is bound to.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.shards[0].borrow().local_addr()
    }

    /// Return the column schema of the view this getter is associated with.
    pub fn columns(&self) -> &[String] {
        self.columns.as_slice()
    }

    /// Query for the size of a specific view.
    pub fn len(&mut self) -> Result<usize, ()> {
        if self.shards.len() == 1 {
            let mut shard = self.shards[0].borrow_mut();
            let is_local = shard.is_local();
            let reply = shard
                .send(&LocalOrNot::make(
                    ReadQuery::Size {
                        target: (self.node, 0),
                    },
                    is_local,
                ))
                .unwrap();
            match unsafe { reply.take() } {
                ReadReply::Size(rows) => Ok(rows),
                _ => unreachable!(),
            }
        } else {
            let shard_queries = 0..self.shards.len();

            let len = shard_queries.into_iter().fold(0, |acc, shardi| {
                let mut shard = self.shards[shardi].borrow_mut();
                let is_local = shard.is_local();
                let reply = shard
                    .send(&LocalOrNot::make(
                        ReadQuery::Size {
                            target: (self.node, shardi),
                        },
                        is_local,
                    ))
                    .unwrap();

                match unsafe { reply.take() } {
                    ReadReply::Size(rows) => acc + rows,
                    _ => unreachable!(),
                }
            });
            Ok(len)
        }
    }

    /// Query for the results for the given keys, optionally blocking if it is not yet available.
    pub fn multi_lookup(
        &mut self,
        keys: Vec<Vec<DataType>>,
        block: bool,
    ) -> Result<Vec<Datas>, ()> {
        if self.shards.len() == 1 {
            let mut shard = self.shards[0].borrow_mut();
            let is_local = shard.is_local();
            let reply = shard
                .send(&LocalOrNot::make(
                    ReadQuery::Normal {
                        target: (self.node, 0),
                        keys,
                        block,
                    },
                    is_local,
                ))
                .unwrap();
            match unsafe { reply.take() } {
                ReadReply::Normal(rows) => rows,
                _ => unreachable!(),
            }
        } else {
            assert!(keys.iter().all(|k| k.len() == 1));
            let mut shard_queries = vec![Vec::new(); self.shards.len()];
            for key in keys {
                let shard = shard_by(&key[0], self.shards.len());
                shard_queries[shard].push(key);
            }

            let mut borrow_all: Vec<_> = self.shards.iter().map(|s| s.borrow_mut()).collect();

            let qs: Vec<_> = borrow_all
                .iter_mut()
                .enumerate()
                .filter_map(|(shardi, shard)| {
                    use std::mem;

                    if shard_queries[shardi].is_empty() {
                        return None;
                    }

                    let is_local = shard.is_local();
                    Some(
                        shard
                            .send_async(&LocalOrNot::make(
                                ReadQuery::Normal {
                                    target: (self.node, shardi),
                                    keys: mem::replace(&mut shard_queries[shardi], Vec::new()),
                                    block,
                                },
                                is_local,
                            ))
                            .unwrap(),
                    )
                })
                .collect();

            let mut err = false;
            let rows = qs
                .into_iter()
                .flat_map(|res| {
                    let reply = res.wait().unwrap();
                    match unsafe { reply.take() } {
                        ReadReply::Normal(Ok(rows)) => rows,
                        ReadReply::Normal(Err(())) => {
                            err = true;
                            Vec::new()
                        }
                        _ => unreachable!(),
                    }
                })
                .collect();

            if err {
                Err(())
            } else {
                Ok(rows)
            }
        }
    }

    /*
    /// Query for the results for the given keys, optionally blocking if it is not yet available.
    pub fn transactional_multi_lookup(
        &mut self,
        keys: Vec<Vec<DataType>>,
    ) -> Result<Vec<(Datas, checktable::Token)>, ()> {
        if self.shards.len() == 1 {
            let mut shard = self.shards[0].borrow_mut();
            let is_local = shard.is_local();
            let reply = shard
                .send(&LocalOrNot::make(
                    ReadQuery::WithToken {
                        target: (self.node, 0),
                        keys,
                    },
                    is_local,
                ))
                .unwrap();
            match unsafe { reply.take() } {
                ReadReply::WithToken(rows) => rows,
                _ => unreachable!(),
            }
        } else {
            assert!(keys.iter().all(|k| k.len() == 1));
            let mut shard_queries = vec![Vec::new(); self.shards.len()];
            for key in keys {
                let shard = shard_by(&key[0], self.shards.len());
                shard_queries[shard].push(key);
            }

            let mut err = false;
            let rows = shard_queries
                .into_iter()
                .enumerate()
                .flat_map(|(shardi, keys)| {
                    let mut shard = self.shards[shardi].borrow_mut();
                    let is_local = shard.is_local();
                    let reply = shard
                        .send(&LocalOrNot::make(
                            ReadQuery::WithToken {
                                target: (self.node, shardi),
                                keys,
                            },
                            is_local,
                        ))
                        .unwrap();

                    match unsafe { reply.take() } {
                        ReadReply::WithToken(Ok(rows)) => rows,
                        ReadReply::WithToken(Err(())) => {
                            err = true;
                            Vec::new()
                        }
                        _ => unreachable!(),
                    }
                })
                .collect();
            if err {
                Err(())
            } else {
                Ok(rows)
            }
        }
    }
    */

    /// Lookup a single key.
    pub fn lookup(&mut self, key: &[DataType], block: bool) -> Result<Datas, ()> {
        // TODO: Optimized version of this function?
        self.multi_lookup(vec![Vec::from(key)], block)
            .map(|rs| rs.into_iter().next().unwrap())
    }

    /*
    /// Do a transactional lookup for a single key.
    pub fn transactional_lookup(
        &mut self,
        key: &[DataType],
    ) -> Result<(Datas, checktable::Token), ()> {
        // TODO: Optimized version of this function?
        self.transactional_multi_lookup(vec![Vec::from(key)])
            .map(|rs| rs.into_iter().next().unwrap())
    }
    */
}
