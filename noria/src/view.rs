use crate::channel::rpc::RpcClient;
use crate::data::*;
use crate::error::TransportError;
use crate::{ExclusiveConnection, SharedConnection};
use petgraph::graph::NodeIndex;
use nom_sql::CreateViewStatement;
use std::cell::RefCell;
use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::rc::Rc;

pub(crate) type ViewRpc = Rc<RefCell<RpcClient<ReadQuery, ReadReply>>>;

/// A failed View operation.
#[derive(Debug, Fail)]
pub enum ViewError {
    /// The given view is not yet available.
    #[fail(display = "the view is not yet available")]
    NotYetAvailable,
    /// A lower-level error occurred while communicating with Soup.
    #[fail(display = "{}", _0)]
    TransportError(#[cause] TransportError),
}

impl From<TransportError> for ViewError {
    fn from(e: TransportError) -> Self {
        ViewError::TransportError(e)
    }
}

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
    pub schema: Option<CreateViewStatement>,
    pub shards: Vec<SocketAddr>,
    // one per shard
    pub local_ports: Vec<u16>,
}

impl ViewBuilder {
    #[doc(hidden)]
    pub fn build_exclusive(self) -> io::Result<View<ExclusiveConnection>> {
        let conns = self
            .shards
            .iter()
            .map(move |addr| RpcClient::connect(addr, false).map(|rpc| Rc::new(RefCell::new(rpc))))
            .collect::<io::Result<Vec<_>>>()?;

        Ok(View {
            node: self.node,
            columns: self.columns,
            schema: self.schema,
            shard_addrs: self.shards,
            shards: conns,
            exclusivity: ExclusiveConnection,
        })
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
    ) -> io::Result<View<SharedConnection>> {
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
                    Entry::Occupied(e) => Ok(Rc::clone(e.get())),
                    Entry::Vacant(h) => {
                        let c = RpcClient::connect_from(sports.get(shardi).cloned(), addr, false)?;
                        if shardi >= sports.len() {
                            assert!(shardi == sports.len());
                            sports.push(c.local_addr()?.port());
                        }

                        let c = Rc::new(RefCell::new(c));
                        h.insert(Rc::clone(&c));
                        Ok(c)
                    }
                }
            })
            .collect::<io::Result<Vec<_>>>()?;

        Ok(View {
            node: self.node,
            columns: self.columns,
            schema: self.schema,
            shard_addrs: self.shards,
            shards: conns,
            exclusivity: SharedConnection,
        })
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
    schema: Option<CreateViewStatement>,

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
            schema: self.schema.clone(),
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
    pub fn into_exclusive(self) -> io::Result<View<ExclusiveConnection>> {
        ViewBuilder {
            node: self.node,
            local_ports: vec![],
            columns: self.columns,
            schema: self.schema,
            shards: self.shard_addrs,
        }
        .build_exclusive()
    }
}

#[cfg_attr(
    feature = "cargo-clippy",
    allow(clippy::len_without_is_empty)
)]
impl<E> View<E> {
    /// Get the list of columns in this view.
    pub fn columns(&self) -> &[String] {
        self.columns.as_slice()
    }

    /// Get the schema definition of this view.
    pub fn schema(&self) -> Option<&CreateViewStatement> {
        self.schema.as_ref()
    }

    /// Get the local address this `View` is bound to.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.shards[0].borrow().local_addr()
    }

    /// Get the current size of this view.
    pub fn len(&mut self) -> Result<usize, ViewError> {
        if self.shards.len() == 1 {
            let mut shard = self.shards[0].borrow_mut();
            let reply = shard
                .send(&ReadQuery::Size {
                    target: (self.node, 0),
                })
                .map_err(TransportError::from)?;
            match reply {
                ReadReply::Size(rows) => Ok(rows),
                _ => unreachable!(),
            }
        } else {
            let shard_queries = 0..self.shards.len();
            shard_queries.fold(Ok(0), |acc, shardi| {
                acc.and_then(|acc| {
                    let mut shard = self.shards[shardi].borrow_mut();
                    let reply = shard
                        .send(&ReadQuery::Size {
                            target: (self.node, shardi),
                        })
                        .map_err(TransportError::from)?;

                    match reply {
                        ReadReply::Size(rows) => Ok(acc + rows),
                        _ => unreachable!(),
                    }
                })
            })
        }
    }

    /// Retrieve the query results for the given parameter values.
    ///
    /// The method will block if the results are not yet available only when `block` is `true`.
    /// If `block` is false, misses will be returned as empty results. Any requested keys that have
    /// missing state will be backfilled (asynchronously if `block` is `false`).
    pub fn multi_lookup(
        &mut self,
        keys: Vec<Vec<DataType>>,
        block: bool,
    ) -> Result<Vec<Datas>, ViewError> {
        if self.shards.len() == 1 {
            let mut shard = self.shards[0].borrow_mut();
            let reply = shard
                .send(&ReadQuery::Normal {
                    target: (self.node, 0),
                    keys,
                    block,
                })
                .map_err(TransportError::from)?;
            match reply {
                ReadReply::Normal(Ok(rows)) => Ok(rows),
                ReadReply::Normal(Err(())) => Err(ViewError::NotYetAvailable),
                _ => unreachable!(),
            }
        } else {
            assert!(keys.iter().all(|k| k.len() == 1));
            let mut shard_queries = vec![Vec::new(); self.shards.len()];
            for key in keys {
                let shard = crate::shard_by(&key[0], self.shards.len());
                shard_queries[shard].push(key);
            }

            let mut borrow_all: Vec<_> = self.shards.iter().map(|s| s.borrow_mut()).collect();

            let qs = borrow_all
                .iter_mut()
                .enumerate()
                .zip(shard_queries.iter_mut())
                .filter(|&(_, ref sq)| !sq.is_empty())
                .map(|((shardi, shard), shard_queries)| {
                    use std::mem;
                    Ok(shard
                        .send_async(&ReadQuery::Normal {
                            target: (self.node, shardi),
                            keys: mem::replace(shard_queries, Vec::new()),
                            block,
                        })
                        .map_err(TransportError::from)?)
                })
                .collect::<Result<Vec<_>, ViewError>>()?;

            let mut results = Vec::new();
            for res in qs {
                let reply = res.wait().map_err(TransportError::from)?;
                match reply {
                    ReadReply::Normal(Ok(rows)) => {
                        results.extend(rows);
                    }
                    ReadReply::Normal(Err(())) => return Err(ViewError::NotYetAvailable),
                    _ => unreachable!(),
                }
            }
            Ok(results)
        }
    }

    /// Retrieve the query results for the given parameter value.
    ///
    /// The method will block if the results are not yet available only when `block` is `true`.
    pub fn lookup(&mut self, key: &[DataType], block: bool) -> Result<Datas, ViewError> {
        // TODO: Optimized version of this function?
        self.multi_lookup(vec![Vec::from(key)], block)
            .map(|rs| rs.into_iter().next().unwrap())
    }
}
