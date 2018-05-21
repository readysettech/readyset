use basics::*;
use channel::rpc::RpcClient;
use std::cell::RefCell;
use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::rc::Rc;
use {ExclusiveConnection, SharedConnection};

pub(crate) type ViewRpc = Rc<RefCell<RpcClient<ReadQuery, ReadReply>>>;

#[doc(hidden)]
#[derive(Serialize, Deserialize, Debug)]
pub enum ReadQuery {
    /// Read from a leaf view
    Normal {
        /// Where to read from
        target: (NodeIndex, usize),
        /// Keys to read with
        keys: Vec<Vec<DataType>>,
        /// Whether to block if a partial replay is triggered
        block: bool,
    },
    /// Read the size of a leaf view
    Size {
        /// Where to read from
        target: (NodeIndex, usize),
    },
}

#[doc(hidden)]
#[derive(Serialize, Deserialize, Debug)]
pub enum ReadReply {
    /// Errors if view isn't ready yet.
    Normal(Result<Vec<Datas>, ()>),
    /// Read size of view
    Size(usize),
}

#[doc(hidden)]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ViewBuilder {
    pub node: NodeIndex,
    pub columns: Vec<String>,
    pub shards: Vec<SocketAddr>,
    // one per shard
    pub local_ports: Vec<u16>,
}

impl ViewBuilder {
    #[doc(hidden)]
    pub fn build_exclusive(self) -> View<ExclusiveConnection> {
        let conns = self
            .shards
            .iter()
            .map(move |addr| Rc::new(RefCell::new(RpcClient::connect(addr, false).unwrap())))
            .collect();

        View {
            node: self.node,
            columns: self.columns,
            shard_addrs: self.shards,
            shards: conns,
            exclusivity: ExclusiveConnection,
        }
    }

    /// Set the local port to bind to when making the shared connection.
    pub(crate) fn with_local_port(mut self, port: u16) -> ViewBuilder {
        assert!(self.local_ports.is_empty());
        self.local_ports = vec![port];
        self
    }

    /// Build a `View` out of a `ViewBuilder`
    pub(crate) fn build(
        mut self,
        rpcs: &mut HashMap<(SocketAddr, usize), ViewRpc>,
    ) -> View<SharedConnection> {
        let sports = &mut self.local_ports;
        let conns = self
            .shards
            .iter()
            .enumerate()
            .map(move |(shardi, addr)| {
                use std::collections::hash_map::Entry;

                // one entry per shard so that we can send sharded requests in parallel even if
                // they happen to be targeting the same machine.
                match rpcs.entry((*addr, shardi)) {
                    Entry::Occupied(e) => Rc::clone(e.get()),
                    Entry::Vacant(h) => {
                        let c =
                            RpcClient::connect_from(sports.get(shardi).map(|&p| p), addr, false)
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

        View {
            node: self.node,
            columns: self.columns,
            shard_addrs: self.shards,
            shards: conns,
            exclusivity: SharedConnection,
        }
    }
}

/// A `View` is used to query previously defined external views.
///
/// If you create multiple `View` handles from a single `ControllerHandle`, they may share
/// connections to the Soup workers. For this reason, `View` is *not* `Send` or `Sync`. To
/// get a handle that can be sent to a different thread (i.e., one with its own dedicated
/// connections), call `View::into_exclusive`.
pub struct View<E = SharedConnection> {
    node: NodeIndex,
    columns: Vec<String>,
    shards: Vec<ViewRpc>,
    shard_addrs: Vec<SocketAddr>,

    #[allow(dead_code)]
    exclusivity: E,
}

impl Clone for View<SharedConnection> {
    fn clone(&self) -> Self {
        View {
            node: self.node,
            columns: self.columns.clone(),
            shards: self.shards.clone(),
            shard_addrs: self.shard_addrs.clone(),
            exclusivity: SharedConnection,
        }
    }
}

unsafe impl Send for View<ExclusiveConnection> {}

impl View<SharedConnection> {
    /// Produce a `View` with dedicated Soup connections so it can be safely sent across
    /// threads.
    pub fn into_exclusive(self) -> View<ExclusiveConnection> {
        ViewBuilder {
            node: self.node,
            local_ports: vec![],
            columns: self.columns,
            shards: self.shard_addrs,
        }.build_exclusive()
    }
}

impl<E> View<E> {
    /// Get the list of columns in this view.
    pub fn columns(&self) -> &[String] {
        self.columns.as_slice()
    }

    /// Get the local address this `View` is bound to.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.shards[0].borrow().local_addr()
    }

    /// Get the current size of this view.
    pub fn len(&mut self) -> Result<usize, ()> {
        if self.shards.len() == 1 {
            let mut shard = self.shards[0].borrow_mut();
            let reply = shard
                .send(&ReadQuery::Size {
                    target: (self.node, 0),
                })
                .unwrap();
            match reply {
                ReadReply::Size(rows) => Ok(rows),
                _ => unreachable!(),
            }
        } else {
            let shard_queries = 0..self.shards.len();

            let len = shard_queries.into_iter().fold(0, |acc, shardi| {
                let mut shard = self.shards[shardi].borrow_mut();
                let reply = shard
                    .send(&ReadQuery::Size {
                        target: (self.node, shardi),
                    })
                    .unwrap();

                match reply {
                    ReadReply::Size(rows) => acc + rows,
                    _ => unreachable!(),
                }
            });
            Ok(len)
        }
    }

    /// Retrieve the query results for the given parameter values.
    ///
    /// The method will block if the results are not yet available only when `block` is `true`.
    pub fn multi_lookup(
        &mut self,
        keys: Vec<Vec<DataType>>,
        block: bool,
    ) -> Result<Vec<Datas>, ()> {
        if self.shards.len() == 1 {
            let mut shard = self.shards[0].borrow_mut();
            let reply = shard
                .send(&ReadQuery::Normal {
                    target: (self.node, 0),
                    keys,
                    block,
                })
                .unwrap();
            match reply {
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

                    Some(
                        shard
                            .send_async(&ReadQuery::Normal {
                                target: (self.node, shardi),
                                keys: mem::replace(&mut shard_queries[shardi], Vec::new()),
                                block,
                            })
                            .unwrap(),
                    )
                })
                .collect();

            let mut err = false;
            let rows = qs
                .into_iter()
                .flat_map(|res| {
                    let reply = res.wait().unwrap();
                    match reply {
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

    /// Retrieve the query results for the given parameter value.
    ///
    /// The method will block if the results are not yet available only when `block` is `true`.
    pub fn lookup(&mut self, key: &[DataType], block: bool) -> Result<Datas, ()> {
        // TODO: Optimized version of this function?
        self.multi_lookup(vec![Vec::from(key)], block)
            .map(|rs| rs.into_iter().next().unwrap())
    }
}
