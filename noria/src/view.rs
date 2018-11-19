use async_bincode::{AsyncBincodeStream, AsyncDestination};
use crate::data::*;
use crate::{Tagged, Tagger};
use petgraph::graph::NodeIndex;
use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tokio::prelude::*;
use tokio_tower::multiplex;
use tokio_tower::NewTransport;
use tower_balance::{choose, Pool};
use tower_buffer::Buffer;
use tower_service::Service;

type Transport = AsyncBincodeStream<
    tokio::net::tcp::TcpStream,
    Tagged<ReadReply>,
    Tagged<ReadQuery>,
    AsyncDestination,
>;

#[derive(Debug)]
#[doc(hidden)]
// only pub because we use it to figure out the error type for ViewError
pub struct ViewEndpoint(SocketAddr);

impl NewTransport<Tagged<ReadQuery>> for ViewEndpoint {
    type InitError = tokio::io::Error;
    type Transport = multiplex::MultiplexTransport<Transport, Tagger>;
    type TransportFut = Box<Future<Item = Self::Transport, Error = Self::InitError> + Send>;

    fn new_transport(&self) -> Self::TransportFut {
        Box::new(
            tokio::net::TcpStream::connect(&self.0)
                .map(AsyncBincodeStream::from)
                .map(AsyncBincodeStream::for_async)
                .map(|t| multiplex::MultiplexTransport::new(t, Tagger::default())),
        )
    }
}

pub(crate) type ViewRpc = Buffer<
    Pool<
        choose::RoundRobin,
        multiplex::client::Maker<ViewEndpoint, Tagged<ReadQuery>>,
        (),
        Tagged<ReadQuery>,
    >,
    Tagged<ReadQuery>,
>;

/// A failed View operation.
#[derive(Debug, Fail)]
pub enum ViewError {
    /// The given view is not yet available.
    #[fail(display = "the view is not yet available")]
    NotYetAvailable,
    /// A lower-level error occurred while communicating with Soup.
    #[fail(display = "{}", _0)]
    TransportError(#[cause] <ViewRpc as Service<Tagged<ReadQuery>>>::Error),
}

impl From<<ViewRpc as Service<Tagged<ReadQuery>>>::Error> for ViewError {
    fn from(e: <ViewRpc as Service<Tagged<ReadQuery>>>::Error) -> Self {
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
}

impl ViewBuilder {
    /// Build a `View` out of a `ViewBuilder`
    pub(crate) fn build(
        &self,
        rpcs: Arc<Mutex<HashMap<(SocketAddr, usize), ViewRpc>>>,
    ) -> impl Future<Item = View, Error = io::Error> {
        let node = self.node.clone();
        let columns = self.columns.clone();
        let shards = self.shards.clone();
        future::join_all(shards.into_iter().enumerate().map(move |(shardi, addr)| {
            use std::collections::hash_map::Entry;

            // one entry per shard so that we can send sharded requests in parallel even if
            // they happen to be targeting the same machine.
            let mut rpcs = rpcs.lock().unwrap();
            match rpcs.entry((addr, shardi)) {
                Entry::Occupied(e) => Ok((addr, e.get().clone())),
                Entry::Vacant(h) => {
                    // TODO: maybe always use the same local port?
                    let c = Buffer::new(
                        Pool::new(
                            multiplex::client::Maker::new(ViewEndpoint(addr)),
                            (),
                            choose::RoundRobin::default(),
                        ),
                        0,
                        &tokio::executor::DefaultExecutor::current(),
                    )
                    .unwrap_or_else(|_| panic!("no active tokio runtime"));
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
/// Note that if you create multiple `View` handles from a single `ControllerHandle`, they may
/// share connections to the Soup workers.
///
/// TODO: load scaling
#[derive(Clone)]
pub struct View {
    node: NodeIndex,
    columns: Vec<String>,
    shards: Vec<ViewRpc>,
    shard_addrs: Vec<SocketAddr>,
}

#[cfg_attr(feature = "cargo-clippy", allow(clippy::len_without_is_empty))]
impl View {
    /// Get the list of columns in this view.
    pub fn columns(&self) -> &[String] {
        self.columns.as_slice()
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
                    .call(
                        ReadQuery::Size {
                            target: (node, shardi),
                        }
                        .into(),
                    )
                    .map(move |reply| match reply.v {
                        ReadReply::Size(rows) => rows,
                        _ => unreachable!(),
                    })
                    .map_err(ViewError::from),
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
                    .call(
                        ReadQuery::Normal {
                            target: (self.node, 0),
                            keys,
                            block,
                        }
                        .into(),
                    )
                    .map_err(ViewError::from)
                    .and_then(|reply| match reply.v {
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

            let node = self.node;
            future::Either::B(
                futures::stream::futures_ordered(
                    self.shards
                        .iter_mut()
                        .enumerate()
                        .zip(shard_queries.into_iter())
                        .filter(|&(_, ref sq)| !sq.is_empty())
                        .map(|((shardi, shard), shard_queries)| {
                            shard
                                .call(
                                    ReadQuery::Normal {
                                        target: (node, shardi),
                                        keys: shard_queries,
                                        block,
                                    }
                                    .into(),
                                )
                                .map_err(ViewError::from)
                        }),
                )
                .fold(Vec::new(), |mut results, reply| match reply.v {
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
