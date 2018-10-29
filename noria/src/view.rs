use async_bincode::{AsyncBincodeStream, AsyncDestination};
use crate::data::*;
use net2;
use petgraph::graph::NodeIndex;
use std::collections::HashMap;
use std::io;
use std::net::Ipv4Addr;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tokio::prelude::*;
use tokio_tower::buffer::Buffer;
use tokio_tower::multiplex::Client;
use tower_balance::Balance;
//use tower_buffer::Buffer;
use tower_service::Service;

type Transport =
    AsyncBincodeStream<tokio::net::tcp::TcpStream, ReadReply, ReadQuery, AsyncDestination>;

// TODO: Need to update Balance to handle DirectService
// TODO: How do we add another connection to Balance dynamically?
pub(crate) type ViewRpc = Buffer<Balance<Client<Transport, ViewError>, ReadQuery>, ReadQuery>;

/// A failed View operation.
#[derive(Debug, Fail)]
pub enum ViewError {
    /// The given view is not yet available.
    #[fail(display = "the view is not yet available")]
    NotYetAvailable,
    /// A lower-level error occurred while communicating with Soup.
    #[fail(display = "{}", _0)]
    TransportError(#[cause] tokio_tower::pipeline::ClientError<Transport>),
}

impl From<tokio_tower::pipeline::ClientError<Transport>> for ViewError {
    fn from(e: tokio_tower::pipeline::ClientError<Transport>) -> Self {
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
    pub shards: Vec<SocketAddr>,
    // one per shard
    pub local_ports: Vec<u16>,
}

impl ViewBuilder {
    /// Set the local port to bind to when making the shared connection.
    pub(crate) fn with_local_port(mut self, port: u16) -> ViewBuilder {
        assert!(self.local_ports.is_empty());
        self.local_ports = vec![port];
        self
    }

    /// Build a `View` out of a `ViewBuilder`
    pub(crate) fn build(
        mut self,
        rpcs: Arc<Mutex<HashMap<(SocketAddr, usize), ViewRpc>>>,
    ) -> impl Future<Item = View, Error = io::Error> {
        let node = self.node;
        let columns = self.columns;
        let shards = self.shards;
        let sports = self.local_ports;
        future::join_all(shards.into_iter().enumerate().map(move |(shardi, addr)| {
            use std::collections::hash_map::Entry;

            // one entry per shard so that we can send sharded requests in parallel even if
            // they happen to be targeting the same machine.
            let mut rpcs = rpcs.lock().unwrap();
            match rpcs.entry((addr, shardi)) {
                Entry::Occupied(e) => Ok((
                    addr,
                    e.get_mut().shared_clone().expect("no running executor?"),
                )),
                Entry::Vacant(h) => {
                    let s = net2::TcpBuilder::new_v4()?
                        .reuse_address(true)?
                        .bind((
                            Ipv4Addr::UNSPECIFIED,
                            sports.get(shardi).cloned().unwrap_or(0),
                        ))?
                        .connect(addr)?;

                    let s = tokio::net::tcp::TcpStream::from_std(
                        s,
                        &tokio::reactor::Handle::default(),
                    )?;
                    let laddr = s.local_addr()?;

                    let c = AsyncBincodeStream::from(s).for_async();
                    let c = Client::new(c);
                    let c = Balance::new(c);
                    let c = Buffer::new(c & tokio::executor::DefaultExecutor::current())
                        .unwrap_or_else(|_| unreachable!("no running executor?"));
                    h.insert(c.clone());
                    Ok((addr, c))
                }
            }
        }))
        .map(move |shards| {
            let (addrs, conns) = shards.into_iter().unzip();
            View {
                node,
                columns,
                shard_addrs: addrs,
                shards: conns,
            }
        })
    }
}

/// A `View` is used to query previously defined external views.
///
/// If you create multiple `View` handles from a single `ControllerHandle`, they may share
/// connections to the Soup workers. For this reason, `View` is *not* `Send` or `Sync`. To
/// get a handle that can be sent to a different thread (i.e., one with its own dedicated
/// connections), call `View::into_exclusive`.
pub struct View {
    node: NodeIndex,
    columns: Vec<String>,
    shards: Vec<ViewRpc>,
    shard_addrs: Vec<SocketAddr>,
}

#[cfg_attr(
    feature = "cargo-clippy",
    allow(clippy::len_without_is_empty)
)]
impl View {
    /// Get the list of columns in this view.
    pub fn columns(&self) -> &[String] {
        self.columns.as_slice()
    }

    /// Get the local addresses this `View` is bound to.
    pub fn local_addr(&self) -> impl Iterator<Item = &SocketAddr> {
        self.shards.iter().map(|v| &v.local_addr)
    }

    /// Get the current size of this view.
    ///
    /// Note that you must also continue to poll this `View` for the returned future to resolve.
    pub fn len(&mut self) -> impl Future<Item = usize, Error = ViewError> {
        let mut futures = futures::stream::futures_unordered::FuturesUnordered::new();
        let node = self.node;
        for (shardi, shard) in self.shards.iter_mut().enumerate() {
            futures.push(
                shard
                    .call(ReadQuery::Size {
                        target: (node, shardi),
                    })
                    .map(move |reply| match reply {
                        ReadReply::Size(rows) => rows,
                        _ => unreachable!(),
                    }),
            );
        }
        futures.fold(0, |acc, rs| future::ok::<_, ViewError>(acc + rs))
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
    ) -> impl Future<Item = Vec<Datas>, Error = ViewError> {
        if self.shards.len() == 1 {
            future::Either::A(
                self.shards
                    .get_mut(0)
                    .unwrap()
                    .call(ReadQuery::Normal {
                        target: (self.node, 0),
                        keys,
                        block,
                    })
                    .and_then(|reply| match reply {
                        ReadReply::Normal(Ok(rows)) => Ok(rows),
                        ReadReply::Normal(Err(())) => Err(ViewError::NotYetAvailable),
                        _ => unreachable!(),
                    }),
            )
        } else {
            assert!(keys.iter().all(|k| k.len() == 1));
            let mut shard_queries = vec![Vec::new(); self.shards.len()];
            for key in keys {
                let shard = crate::shard_by(&key[0], self.shards.len());
                shard_queries[shard].push(key);
            }

            future::Either::B(
                futures::stream::futures_ordered(
                    self.shards
                        .iter_mut()
                        .enumerate()
                        .zip(shard_queries.into_iter())
                        .filter(|&(_, ref sq)| !sq.is_empty())
                        .map(|((shardi, shard), shard_queries)| {
                            shard.call(ReadQuery::Normal {
                                target: (self.node, shardi),
                                keys: shard_queries,
                                block,
                            })
                        }),
                )
                .fold(Vec::new(), |mut results, reply| match reply {
                    ReadReply::Normal(Ok(rows)) => {
                        results.extend(rows);
                        Ok(results)
                    }
                    ReadReply::Normal(Err(())) => Err(ViewError::NotYetAvailable),
                    _ => unreachable!(),
                }),
            )
        }
    }

    /// Retrieve the query results for the given parameter value.
    ///
    /// The method will block if the results are not yet available only when `block` is `true`.
    pub fn lookup(
        &mut self,
        key: &[DataType],
        block: bool,
    ) -> impl Future<Item = Datas, Error = ViewError> {
        // TODO: Optimized version of this function?
        self.multi_lookup(vec![Vec::from(key)], block)
            .map(|rs| rs.into_iter().next().unwrap())
    }
}
