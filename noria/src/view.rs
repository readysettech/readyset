use crate::data::*;
use crate::BoxDynError;
use crate::{Tagged, Tagger};
use async_bincode::{AsyncBincodeStream, AsyncDestination};
use nom_sql::ColumnSpecification;
use petgraph::graph::NodeIndex;
use std::collections::HashMap;
use std::fmt;
use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tokio::prelude::*;
use tokio_tower::multiplex;
use tower_balance::{choose, pool, Pool};
use tower_buffer::Buffer;
use tower_service::Service;
use tower_util::ServiceExt;

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

impl Service<()> for ViewEndpoint {
    type Response = multiplex::MultiplexTransport<Transport, Tagger>;
    type Error = tokio::io::Error;
    // have to repeat types because https://github.com/rust-lang/rust/issues/57807
    existential type Future: Future<
        Item = multiplex::MultiplexTransport<Transport, Tagger>,
        Error = tokio::io::Error,
    >;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        Ok(Async::Ready(()))
    }

    fn call(&mut self, _: ()) -> Self::Future {
        tokio::net::TcpStream::connect(&self.0)
            .and_then(|s| {
                s.set_nodelay(true)?;
                Ok(s)
            })
            .map(AsyncBincodeStream::from)
            .map(AsyncBincodeStream::for_async)
            .map(|t| multiplex::MultiplexTransport::new(t, Tagger::default()))
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

type E = <ViewRpc as Service<Tagged<ReadQuery>>>::Error;

/// A failed [`View`] operation.
#[derive(Debug)]
pub struct AsyncViewError {
    /// The `View` whose operation failed.
    ///
    /// Not available if the underlying transport failed.
    pub view: Option<View>,

    /// The error that caused the operation to fail.
    pub error: ViewError,
}

impl From<E> for AsyncViewError {
    fn from(e: E) -> Self {
        AsyncViewError {
            view: None,
            error: ViewError::from(e),
        }
    }
}

impl From<BoxDynError<E>> for AsyncViewError {
    fn from(e: BoxDynError<E>) -> Self {
        From::from(e.into_inner())
    }
}

/// A failed [`SyncView`] operation.
#[derive(Debug, Fail)]
pub enum ViewError {
    /// The given view is not yet available.
    #[fail(display = "the view is not yet available")]
    NotYetAvailable,
    /// A lower-level error occurred while communicating with Soup.
    #[fail(display = "{}", _0)]
    TransportError(#[cause] BoxDynError<E>),
}

impl From<E> for ViewError {
    fn from(e: E) -> Self {
        ViewError::TransportError(BoxDynError::from(e))
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
    pub schema: Option<Vec<ColumnSpecification>>,
    pub shards: Vec<SocketAddr>,
}

impl ViewBuilder {
    /// Build a `View` out of a `ViewBuilder`
    #[doc(hidden)]
    pub fn build(
        &self,
        rpcs: Arc<Mutex<HashMap<(SocketAddr, usize), ViewRpc>>>,
    ) -> impl Future<Item = View, Error = io::Error> + Send {
        let node = self.node;
        let columns = self.columns.clone();
        let shards = self.shards.clone();
        let schema = self.schema.clone();
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
                        pool::Builder::new()
                            .urgency(0.03)
                            .loaded_above(0.2)
                            .underutilized_below(0.00001)
                            .build(
                                multiplex::client::Maker::new(ViewEndpoint(addr)),
                                (),
                                choose::RoundRobin::default(),
                            ),
                        1,
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
                schema,
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
#[derive(Clone)]
pub struct View {
    node: NodeIndex,
    columns: Vec<String>,
    schema: Option<Vec<ColumnSpecification>>,

    shards: Vec<ViewRpc>,
    shard_addrs: Vec<SocketAddr>,
}

impl fmt::Debug for View {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("View")
            .field("node", &self.node)
            .field("columns", &self.columns)
            .field("shard_addrs", &self.shard_addrs)
            .finish()
    }
}

impl Service<(Vec<Vec<DataType>>, bool)> for View {
    type Response = Vec<Datas>;
    type Error = ViewError;
    // have to repeat types because https://github.com/rust-lang/rust/issues/57807
    existential type Future: Future<Item = Vec<Datas>, Error = ViewError>;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        for s in &mut self.shards {
            try_ready!(s.poll_ready().map_err(ViewError::from));
        }
        Ok(Async::Ready(()))
    }

    fn call(&mut self, (keys, block): (Vec<Vec<DataType>>, bool)) -> Self::Future {
        // TODO: optimize for when there's only one shard
        assert!(keys.iter().all(|k| k.len() == 1));
        let mut shard_queries = vec![Vec::new(); self.shards.len()];
        for key in keys {
            let shard = crate::shard_by(&key[0], self.shards.len());
            shard_queries[shard].push(key);
        }

        let node = self.node;
        futures::stream::futures_ordered(
            self.shards
                .iter_mut()
                .enumerate()
                .zip(shard_queries.into_iter())
                .filter_map(|((shardi, shard), shard_queries)| {
                    if shard_queries.is_empty() {
                        // poll_ready reserves a sender slot which we have to release
                        // we do that by dropping the old handle and replacing it with a clone
                        // https://github.com/tokio-rs/tokio/issues/898
                        *shard = shard.clone();
                        None
                    } else {
                        Some(((shardi, shard), shard_queries))
                    }
                })
                .map(move |((shardi, shard), shard_queries)| {
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
                        .and_then(|reply| match reply.v {
                            ReadReply::Normal(Ok(rows)) => Ok(rows),
                            ReadReply::Normal(Err(())) => Err(ViewError::NotYetAvailable),
                            _ => unreachable!(),
                        })
                }),
        )
        .concat2()
    }
}

#[allow(clippy::len_without_is_empty)]
impl View {
    /// Get the list of columns in this view.
    pub fn columns(&self) -> &[String] {
        self.columns.as_slice()
    }

    /// Get the schema definition of this view.
    pub fn schema(&self) -> Option<&[ColumnSpecification]> {
        self.schema.as_ref().map(|s| s.as_slice())
    }

    /// Get the current size of this view.
    ///
    /// Note that you must also continue to poll this `View` for the returned future to resolve.
    pub fn len(mut self) -> impl Future<Item = (Self, usize), Error = AsyncViewError> + Send {
        let node = self.node;
        futures::stream::futures_ordered(self.shards.drain(..).enumerate().map(
            |(shardi, shard)| {
                shard
                    .ready()
                    .map_err(AsyncViewError::from)
                    .and_then(move |mut svc| {
                        svc.call(
                            ReadQuery::Size {
                                target: (node, shardi),
                            }
                            .into(),
                        )
                        .map_err(AsyncViewError::from)
                        .map(move |reply| match reply.v {
                            ReadReply::Size(rows) => (svc, rows),
                            _ => unreachable!(),
                        })
                    })
            },
        ))
        .fold((self, 0), |(mut this, acc), (svc, rows)| {
            this.shards.push(svc);
            future::ok::<_, AsyncViewError>((this, acc + rows))
        })
    }

    /// Retrieve the query results for the given parameter values.
    ///
    /// The method will block if the results are not yet available only when `block` is `true`.
    /// If `block` is false, misses will be returned as empty results. Any requested keys that have
    /// missing state will be backfilled (asynchronously if `block` is `false`).
    pub fn multi_lookup(
        self,
        keys: Vec<Vec<DataType>>,
        block: bool,
    ) -> impl Future<Item = (Self, Vec<Datas>), Error = AsyncViewError> + Send {
        self.ready()
            .map_err(|e| match e {
                ViewError::NotYetAvailable => unreachable!("can't occur in poll_ready"),
                ViewError::TransportError(e) => AsyncViewError::from(e),
            })
            .and_then(move |mut svc| {
                svc.call((keys, block)).then(move |res| match res {
                    Ok(res) => Ok((svc, res)),
                    Err(e) => Err(AsyncViewError {
                        view: Some(svc),
                        error: e,
                    }),
                })
            })
    }

    /// Retrieve the query results for the given parameter value.
    ///
    /// The method will block if the results are not yet available only when `block` is `true`.
    pub fn lookup(
        self,
        key: &[DataType],
        block: bool,
    ) -> impl Future<Item = (Self, Datas), Error = AsyncViewError> + Send {
        // TODO: Optimized version of this function?
        self.multi_lookup(vec![Vec::from(key)], block)
            .map(|(this, rs)| (this, rs.into_iter().next().unwrap()))
    }

    /// Switch to a synchronous interface for this view.
    pub fn into_sync(self) -> SyncView {
        SyncView(Some(self))
    }
}

/// A synchronous wrapper around [`View`] where all methods block (using `wait`) for the operation
/// to complete before returning.
#[derive(Clone, Debug)]
pub struct SyncView(Option<View>);

macro_rules! sync {
    ($self:ident.$method:ident($($args:expr),*)) => {
        match $self
            .0
            .take()
            .expect("tried to use View after its transport has failed")
            .$method($($args),*)
            .wait()
        {
            Ok((this, res)) => {
                $self.0 = Some(this);
                Ok(res)
            }
            Err(e) => {
                $self.0 = e.view;
                Err(e.error)
            },
        }
    };
}

#[allow(clippy::len_without_is_empty)]
impl SyncView {
    /// Get the list of columns in this view.
    pub fn columns(&self) -> &[String] {
        self.0
            .as_ref()
            .expect("tried to use View after its transport has failed")
            .columns()
    }

    /// Get the schema definition of this view.
    pub fn schema(&self) -> Option<&[ColumnSpecification]> {
        self.0
            .as_ref()
            .expect("tried to use View after its transport has failed")
            .schema()
    }

    /// See [`View::len`].
    pub fn len(&mut self) -> Result<usize, ViewError> {
        sync!(self.len())
    }

    /// See [`View::multi_lookup`].
    pub fn multi_lookup(
        &mut self,
        keys: Vec<Vec<DataType>>,
        block: bool,
    ) -> Result<Vec<Datas>, ViewError> {
        sync!(self.multi_lookup(keys, block))
    }

    /// See [`View::lookup`].
    pub fn lookup(&mut self, key: &[DataType], block: bool) -> Result<Datas, ViewError> {
        sync!(self.lookup(key, block))
    }

    /// Switch back to an asynchronous interface for this view.
    pub fn into_async(mut self) -> View {
        self.0
            .take()
            .expect("tried to use View after its transport has failed")
    }
}
