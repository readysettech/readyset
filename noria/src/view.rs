use crate::data::*;
use crate::{Tagged, Tagger};
use async_bincode::{AsyncBincodeStream, AsyncDestination};
use futures_util::{
    future, future::TryFutureExt, ready, stream::futures_unordered::FuturesUnordered,
    stream::StreamExt, stream::TryStreamExt,
};
use nom_sql::ColumnSpecification;
use petgraph::graph::NodeIndex;
use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use tokio_tower::multiplex;
use tower_balance::pool::{self, Pool};
use tower_buffer::Buffer;
use tower_service::Service;

type Transport = AsyncBincodeStream<
    tokio::net::TcpStream,
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
    type Future = impl Future<
        Output = Result<multiplex::MultiplexTransport<Transport, Tagger>, tokio::io::Error>,
    >;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _: ()) -> Self::Future {
        let f = tokio::net::TcpStream::connect(self.0);
        async move {
            let s = f.await?;
            s.set_nodelay(true)?;
            let s = AsyncBincodeStream::from(s).for_async();
            Ok(multiplex::MultiplexTransport::new(s, Tagger::default()))
        }
    }
}

pub(crate) type ViewRpc = Buffer<
    Pool<multiplex::client::Maker<ViewEndpoint, Tagged<ReadQuery>>, (), Tagged<ReadQuery>>,
    Tagged<ReadQuery>,
>;

/// A failed [`SyncView`] operation.
#[derive(Debug, Fail)]
pub enum ViewError {
    /// The given view is not yet available.
    #[fail(display = "the view is not yet available")]
    NotYetAvailable,
    /// A lower-level error occurred while communicating with Soup.
    #[fail(display = "{}", _0)]
    TransportError(#[cause] failure::Error),
}

impl From<Box<dyn std::error::Error + Send + Sync>> for ViewError {
    fn from(e: Box<dyn std::error::Error + Send + Sync>) -> Self {
        ViewError::TransportError(failure::Error::from_boxed_compat(e))
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
    ) -> Result<View, io::Error> {
        let node = self.node;
        let columns = self.columns.clone();
        let shards = self.shards.clone();
        let schema = self.schema.clone();

        let mut addrs = Vec::with_capacity(shards.len());
        let mut conns = Vec::with_capacity(shards.len());

        for (shardi, &addr) in shards.iter().enumerate() {
            use std::collections::hash_map::Entry;

            addrs.push(addr);

            // one entry per shard so that we can send sharded requests in parallel even if
            // they happen to be targeting the same machine.
            let mut rpcs = rpcs.lock().unwrap();
            let s = match rpcs.entry((addr, shardi)) {
                Entry::Occupied(e) => e.get().clone(),
                Entry::Vacant(h) => {
                    // TODO: maybe always use the same local port?
                    let c = Buffer::new(
                        pool::Builder::new()
                            .urgency(0.03)
                            .loaded_above(0.2)
                            .underutilized_below(0.000_000_001)
                            .max_services(Some(32))
                            .build(multiplex::client::Maker::new(ViewEndpoint(addr)), ()),
                        50,
                    );
                    h.insert(c.clone());
                    c
                }
            };
            conns.push(s);
        }

        let tracer = tracing::dispatcher::get_default(|d| d.clone());
        Ok(View {
            node,
            schema,
            columns,
            shard_addrs: addrs,
            shards: conns,
            tracer,
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

    tracer: tracing::Dispatch,
}

impl fmt::Debug for View {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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
    type Future = impl Future<Output = Result<Vec<Datas>, ViewError>> + Send;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        for s in &mut self.shards {
            ready!(s.poll_ready(cx)).map_err(ViewError::from)?;
        }
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, (keys, block): (Vec<Vec<DataType>>, bool)) -> Self::Future {
        let span = if crate::trace_next_op() {
            Some(tracing::trace_span!(
                "view-request",
                ?keys,
                node = self.node.index()
            ))
        } else {
            None
        };

        if self.shards.len() == 1 {
            let request = Tagged::from(ReadQuery::Normal {
                target: (self.node, 0),
                keys,
                block,
            });

            let _guard = span.as_ref().map(tracing::Span::enter);
            tracing::trace!("submit request");

            return future::Either::Left(
                self.shards[0]
                    .call(request)
                    .map_err(ViewError::from)
                    .and_then(|reply| async move {
                        match reply.v {
                            ReadReply::Normal(Ok(rows)) => Ok(rows),
                            ReadReply::Normal(Err(())) => Err(ViewError::NotYetAvailable),
                            _ => unreachable!(),
                        }
                    }),
            );
        }

        if let Some(ref span) = span {
            span.in_scope(|| tracing::trace!("shard request"));
        }
        assert!(keys.iter().all(|k| k.len() == 1));
        let mut shard_queries = vec![Vec::new(); self.shards.len()];
        for key in keys {
            let shard = crate::shard_by(&key[0], self.shards.len());
            shard_queries[shard].push(key);
        }

        let node = self.node;
        future::Either::Right(
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
                    let request = Tagged::from(ReadQuery::Normal {
                        target: (node, shardi),
                        keys: shard_queries,
                        block,
                    });

                    let _guard = span.as_ref().map(tracing::Span::enter);
                    // make a span per shard
                    let span = if span.is_some() {
                        Some(tracing::trace_span!("view-shard", shardi))
                    } else {
                        None
                    };
                    let _guard = span.as_ref().map(tracing::Span::enter);
                    tracing::trace!("submit request shard");

                    shard
                        .call(request)
                        .map_err(ViewError::from)
                        .and_then(|reply| async move {
                            match reply.v {
                                ReadReply::Normal(Ok(rows)) => Ok(rows),
                                ReadReply::Normal(Err(())) => Err(ViewError::NotYetAvailable),
                                _ => unreachable!(),
                            }
                        })
                })
                .collect::<FuturesUnordered<_>>()
                .try_concat(),
        )
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
        self.schema.as_deref()
    }

    /// Get the current size of this view.
    ///
    /// Note that you must also continue to poll this `View` for the returned future to resolve.
    pub async fn len(&mut self) -> Result<usize, ViewError> {
        future::poll_fn(|cx| self.poll_ready(cx)).await?;

        let node = self.node;
        let mut rsps = self
            .shards
            .iter_mut()
            .enumerate()
            .map(|(shardi, shard)| {
                shard.call(Tagged::from(ReadQuery::Size {
                    target: (node, shardi),
                }))
            })
            .collect::<FuturesUnordered<_>>();

        let mut nrows = 0;
        while let Some(reply) = rsps.next().await.transpose()? {
            if let ReadReply::Size(rows) = reply.v {
                nrows += rows;
            } else {
                unreachable!();
            }
        }

        Ok(nrows)
    }

    /// Retrieve the query results for the given parameter values.
    ///
    /// The method will block if the results are not yet available only when `block` is `true`.
    /// If `block` is false, misses will be returned as empty results. Any requested keys that have
    /// missing state will be backfilled (asynchronously if `block` is `false`).
    pub async fn multi_lookup(
        &mut self,
        keys: Vec<Vec<DataType>>,
        block: bool,
    ) -> Result<Vec<Datas>, ViewError> {
        future::poll_fn(|cx| self.poll_ready(cx)).await?;
        self.call((keys, block)).await
    }

    /// Retrieve the query results for the given parameter value.
    ///
    /// The method will block if the results are not yet available only when `block` is `true`.
    pub async fn lookup(&mut self, key: &[DataType], block: bool) -> Result<Datas, ViewError> {
        // TODO: Optimized version of this function?
        let rs = self.multi_lookup(vec![Vec::from(key)], block).await?;
        Ok(rs.into_iter().next().unwrap())
    }
}
