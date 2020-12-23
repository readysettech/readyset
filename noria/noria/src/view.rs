use crate::data::*;
use crate::util::BoundFunctor;
use crate::{Tagged, Tagger};
use async_bincode::{AsyncBincodeStream, AsyncDestination};
use futures_util::{
    future, future::TryFutureExt, ready, stream::futures_unordered::FuturesUnordered,
    stream::StreamExt, stream::TryStreamExt,
};
use nom_sql::ColumnSpecification;
use petgraph::graph::NodeIndex;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::fmt;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::ops::{Bound, RangeBounds};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use tokio_tower::multiplex;
use tower_balance::p2c::Balance;
use tower_buffer::Buffer;
use tower_discover::ServiceStream;
use tower_limit::concurrency::ConcurrencyLimit;
use tower_service::Service;
use vec1::Vec1;

type Transport = AsyncBincodeStream<
    tokio::net::TcpStream,
    Tagged<ReadReply>,
    Tagged<ReadQuery>,
    AsyncDestination,
>;

#[derive(Debug)]
struct Endpoint(SocketAddr);

type InnerService = multiplex::Client<
    multiplex::MultiplexTransport<Transport, Tagger>,
    tokio_tower::Error<multiplex::MultiplexTransport<Transport, Tagger>, Tagged<ReadQuery>>,
    Tagged<ReadQuery>,
>;

impl Service<()> for Endpoint {
    type Response = InnerService;
    type Error = tokio::io::Error;

    #[cfg(not(doc))]
    type Future = impl Future<Output = Result<Self::Response, Self::Error>>;
    #[cfg(doc)]
    type Future = crate::doc_mock::Future<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _: ()) -> Self::Future {
        let f = tokio::net::TcpStream::connect(self.0);
        async move {
            let s = f.await?;
            s.set_nodelay(true)?;
            let s = AsyncBincodeStream::from(s).for_async();
            let t = multiplex::MultiplexTransport::new(s, Tagger::default());
            Ok(multiplex::Client::with_error_handler(t, |e| {
                eprintln!("view server went away: {}", e)
            }))
        }
    }
}

fn make_views_stream(
    addr: SocketAddr,
) -> impl futures_util::stream::TryStream<
    Ok = tower_discover::Change<usize, InnerService>,
    Error = tokio::io::Error,
> {
    // TODO: use whatever comes out of https://github.com/tower-rs/tower/issues/456 instead of
    // creating _all_ the connections every time.
    (0..crate::VIEW_POOL_SIZE)
        .map(|i| async move {
            let svc = Endpoint(addr).call(()).await?;
            Ok(tower_discover::Change::Insert(i, svc))
        })
        .collect::<futures_util::stream::FuturesUnordered<_>>()
}

fn make_views_discover(addr: SocketAddr) -> Discover {
    ServiceStream::new(make_views_stream(addr))
}

// Unpin + Send bounds are needed due to https://github.com/rust-lang/rust/issues/55997
#[cfg(not(doc))]
type Discover = impl tower_discover::Discover<Key = usize, Service = InnerService, Error = tokio::io::Error>
    + Unpin
    + Send;
#[cfg(doc)]
type Discover = crate::doc_mock::Discover<InnerService>;

pub(crate) type ViewRpc =
    Buffer<ConcurrencyLimit<Balance<Discover, Tagged<ReadQuery>>>, Tagged<ReadQuery>>;

/// A failed [`View`] operation.
#[derive(Debug, Fail)]
pub enum ViewError {
    /// The given view is not yet available.
    #[fail(display = "the view is not yet available")]
    NotYetAvailable,
    /// A lower-level error occurred while communicating with Soup.
    #[fail(display = "{}", _0)]
    TransportError(#[cause] failure::Error),
    /// The query specified an empty lookup key
    #[fail(display = "the query specified an empty lookup key")]
    EmptyKey,
}

impl From<Box<dyn std::error::Error + Send + Sync>> for ViewError {
    fn from(e: Box<dyn std::error::Error + Send + Sync>) -> Self {
        ViewError::TransportError(failure::Error::from_boxed_compat(e))
    }
}

/// Representation for a comparison predicate against a set of keys
#[derive(Serialize, Deserialize, Debug, Eq, PartialEq, Clone)]
pub enum KeyComparison {
    /// Look up exactly one key
    Equal(Vec1<DataType>),

    /// Look up all keys within a range
    Range((Bound<Vec1<DataType>>, Bound<Vec1<DataType>>)),
}

impl KeyComparison {
    /// Project a KeyComparison into an optional equality predicate, or return None if it's a range
    /// predicate
    pub fn equal(&self) -> Option<&Vec1<DataType>> {
        match self {
            KeyComparison::Equal(ref key) => Some(key),
            _ => None,
        }
    }

    /// Project a KeyComparison into an optional range predicate, or return None if it's a range
    /// predicate
    pub fn range(&self) -> Option<&(Bound<Vec1<DataType>>, Bound<Vec1<DataType>>)> {
        match self {
            KeyComparison::Range(ref range) => Some(range),
            _ => None,
        }
    }

    /// Returns the shard key(s) that this [`KeyComparison`] must target, given the total number of
    /// shards
    pub fn shard_keys(&self, num_shards: usize) -> Vec<usize> {
        match self {
            KeyComparison::Equal(key) => vec![crate::shard_by(&key[0], num_shards)],
            // Since we currently implement hash-based sharding, any non-point query must target all
            // shards. This restriction could be lifted in the future by implementing (perhaps
            // optional) range-based sharding, likely with rebalancing. See Guillote-Blouin, J.
            // (2020) Implementing Range Queries and Write Policies in a Partially-Materialized
            // Data-Flow [Unpublished Master's thesis]. Harvard University S 2.4
            _ => (0..num_shards).collect(),
        }
    }
}

impl TryFrom<Vec<DataType>> for KeyComparison {
    type Error = vec1::Size0Error;

    /// Converts to a [`KeyComparison::Equal`]. Returns an error if the input vector is empty
    fn try_from(value: Vec<DataType>) -> Result<Self, Self::Error> {
        Ok(Vec1::try_from(value)?.into())
    }
}

impl From<Vec1<DataType>> for KeyComparison {
    /// Converts to a [`KeyComparison::Equal`]
    fn from(key: Vec1<DataType>) -> Self {
        KeyComparison::Equal(key)
    }
}

impl RangeBounds<Vec1<DataType>> for KeyComparison {
    fn start_bound(&self) -> Bound<&Vec1<DataType>> {
        use Bound::*;
        use KeyComparison::*;
        match self {
            Equal(ref key) => Included(key),
            Range((Unbounded, _)) => Unbounded,
            Range((Included(ref k), _)) => Included(k),
            Range((Excluded(ref k), _)) => Excluded(k),
        }
    }

    fn end_bound(&self) -> Bound<&Vec1<DataType>> {
        use Bound::*;
        use KeyComparison::*;
        match self {
            Equal(ref key) => Included(key),
            Range((_, Unbounded)) => Unbounded,
            Range((_, Included(ref k))) => Included(k),
            Range((_, Excluded(ref k))) => Excluded(k),
        }
    }
}

impl RangeBounds<Vec<DataType>> for KeyComparison {
    fn start_bound(&self) -> Bound<&Vec<DataType>> {
        self.start_bound().map(Vec1::as_vec)
    }

    fn end_bound(&self) -> Bound<&Vec<DataType>> {
        self.end_bound().map(Vec1::as_vec)
    }
}

impl RangeBounds<Vec<DataType>> for &KeyComparison {
    fn start_bound(&self) -> Bound<&Vec<DataType>> {
        (**self).start_bound()
    }

    fn end_bound(&self) -> Bound<&Vec<DataType>> {
        (**self).end_bound()
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
        key_comparisons: Vec<KeyComparison>,
        /// Whether to block if a partial replay is triggered
        block: bool,
    },
    /// Read the size of a leaf view
    Size {
        /// Where to read from
        target: (NodeIndex, usize),
    },
    /// Read all keys from a leaf view (for debugging)
    /// TODO(alex): queries with this value are not totally implemented, and might not actually work
    Keys {
        /// Where to read from
        target: (NodeIndex, usize),
    },
}

#[doc(hidden)]
#[derive(Serialize, Deserialize, Debug)]
pub enum ReadReply<D = ReadReplyBatch> {
    /// Errors if view isn't ready yet.
    Normal(Result<Vec<D>, ()>),
    /// Read size of view
    Size(usize),
    // Read keys of view
    Keys(Vec<Vec<DataType>>),
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
                    let (c, w) = Buffer::pair(
                        ConcurrencyLimit::new(
                            Balance::from_entropy(make_views_discover(addr)),
                            crate::PENDING_LIMIT,
                        ),
                        crate::BUFFER_TO_POOL,
                    );
                    use tracing_futures::Instrument;
                    tokio::spawn(w.instrument(tracing::debug_span!(
                        "view_worker",
                        addr = %addr,
                        shard = shardi
                    )));
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

pub(crate) mod results;
use self::results::{Results, Row};

impl Service<(Vec<KeyComparison>, bool)> for View {
    type Response = Vec<Results>;
    type Error = ViewError;

    #[cfg(not(doc))]
    type Future = impl Future<Output = Result<Self::Response, Self::Error>> + Send;
    #[cfg(doc)]
    type Future = crate::doc_mock::Future<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        for s in &mut self.shards {
            ready!(s.poll_ready(cx)).map_err(ViewError::from)?;
        }
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, (key_comparisons, block): (Vec<KeyComparison>, bool)) -> Self::Future {
        let span = if crate::trace_next_op() {
            Some(tracing::trace_span!(
                "view-request",
                ?key_comparisons,
                node = self.node.index()
            ))
        } else {
            None
        };

        let columns = Arc::from(&self.columns[..]);
        if self.shards.len() == 1 {
            let request = Tagged::from(ReadQuery::Normal {
                target: (self.node, 0),
                key_comparisons,
                block,
            });

            let _guard = span.as_ref().map(tracing::Span::enter);
            tracing::trace!("submit request");

            return future::Either::Left(
                self.shards[0]
                    .call(request)
                    .map_err(ViewError::from)
                    .and_then(move |reply| async move {
                        match reply.v {
                            ReadReply::Normal(Ok(rows)) => Ok(rows
                                .into_iter()
                                .map(|rows| Results::new(rows.into(), Arc::clone(&columns)))
                                .collect()),
                            ReadReply::Normal(Err(())) => Err(ViewError::NotYetAvailable),
                            _ => unreachable!(),
                        }
                    }),
            );
        }

        if let Some(ref span) = span {
            span.in_scope(|| tracing::trace!("shard request"));
        }
        let mut shard_queries = vec![Vec::new(); self.shards.len()];
        for comparison in key_comparisons {
            for shard in comparison.shard_keys(self.shards.len()) {
                shard_queries[shard].push(comparison.clone());
            }
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
                        key_comparisons: shard_queries,
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
                .try_concat()
                .map_ok(move |rows| {
                    rows.into_iter()
                        .map(|rows| Results::new(rows.into(), Arc::clone(&columns)))
                        .collect()
                }),
        )
    }
}

#[allow(clippy::len_without_is_empty)]
impl View {
    /// Get the list of columns in this view.
    pub fn columns(&self) -> &[String] {
        &*self.columns
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

    /// Get the current keys of this view. For debugging only.
    pub async fn keys(&mut self) -> Result<Vec<Vec<DataType>>, ViewError> {
        future::poll_fn(|cx| self.poll_ready(cx)).await?;

        let node = self.node;
        let mut rsps = self
            .shards
            .iter_mut()
            .enumerate()
            .map(|(shardi, shard)| {
                shard.call(Tagged::from(ReadQuery::Keys {
                    target: (node, shardi),
                }))
            })
            .collect::<FuturesUnordered<_>>();

        let mut vec = vec![];
        while let Some(reply) = rsps.next().await.transpose()? {
            if let ReadReply::Keys(mut keys) = reply.v {
                vec.append(&mut keys);
            } else {
                unreachable!();
            }
        }

        Ok(vec)
    }

    /// Retrieve the query results for the given parameter values.
    ///
    /// The method will block if the results are not yet available only when `block` is `true`.
    /// If `block` is false, misses will be returned as empty results. Any requested keys that have
    /// missing state will be backfilled (asynchronously if `block` is `false`).
    pub async fn multi_lookup(
        &mut self,
        key_comparisons: Vec<KeyComparison>,
        block: bool,
    ) -> Result<Vec<Results>, ViewError> {
        future::poll_fn(|cx| self.poll_ready(cx)).await?;
        self.call((key_comparisons, block)).await
    }

    /// Retrieve the query results for the given parameter value.
    ///
    /// The method will block if the results are not yet available only when `block` is `true`.
    pub async fn lookup(&mut self, key: &[DataType], block: bool) -> Result<Results, ViewError> {
        // TODO: Optimized version of this function?
        let key = Vec1::try_from_vec(key.into()).map_err(|_| ViewError::EmptyKey)?;
        let rs = self
            .multi_lookup(vec![KeyComparison::Equal(key)], block)
            .await?;
        Ok(rs.into_iter().next().unwrap())
    }

    /// Retrieve the query results for the given range of parameter values
    ///
    /// The method will block if the results are not yet available only when `block` is `true`.
    // pub async fn range(&mut self, key: &[DataType], block: bool) -> Result<Results, ViewError> {
    //     // TODO: Optimized version of this function?
    //     let rs = self
    //         .multi_lookup(vec![KeyComparison::Equal(Vec::from(key))], block)
    //         .await?;
    //     Ok(rs.into_iter().next().unwrap())
    // }

    /// Retrieve the first query result for the given parameter value.
    ///
    /// The method will block if the results are not yet available only when `block` is `true`.
    pub async fn lookup_first(
        &mut self,
        key: &[DataType],
        block: bool,
    ) -> Result<Option<Row>, ViewError> {
        // TODO: Optimized version of this function?
        let key = Vec1::try_from_vec(key.into()).map_err(|_| ViewError::EmptyKey)?;
        let rs = self
            .multi_lookup(vec![KeyComparison::Equal(key)], block)
            .await?;
        Ok(rs.into_iter().next().unwrap().into_iter().next())
    }
}

#[derive(Debug, Default)]
#[doc(hidden)]
#[repr(transparent)]
pub struct ReadReplyBatch(Vec<Vec<DataType>>);

use serde::de::{self, Deserialize, DeserializeSeed, Deserializer, Visitor};
impl<'de> Deserialize<'de> for ReadReplyBatch {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct Elem;

        impl<'de> Visitor<'de> for Elem {
            type Value = Vec<Vec<DataType>>;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("Vec<Vec<DataType>>")
            }

            fn visit_bytes<E>(self, bytes: &[u8]) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                use bincode::Options;
                bincode::options()
                    .deserialize(bytes)
                    .map_err(de::Error::custom)
            }
        }

        impl<'de> DeserializeSeed<'de> for Elem {
            type Value = Vec<Vec<DataType>>;

            fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
            where
                D: Deserializer<'de>,
            {
                deserializer.deserialize_bytes(self)
            }
        }

        deserializer.deserialize_bytes(Elem).map(ReadReplyBatch)
    }
}

impl Into<Vec<Vec<DataType>>> for ReadReplyBatch {
    fn into(self) -> Vec<Vec<DataType>> {
        self.0
    }
}

impl From<Vec<Vec<DataType>>> for ReadReplyBatch {
    fn from(v: Vec<Vec<DataType>>) -> Self {
        Self(v)
    }
}

impl IntoIterator for ReadReplyBatch {
    type Item = Vec<DataType>;
    type IntoIter = std::vec::IntoIter<Self::Item>;
    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl Extend<Vec<DataType>> for ReadReplyBatch {
    fn extend<T>(&mut self, iter: T)
    where
        T: IntoIterator<Item = Vec<DataType>>,
    {
        self.0.extend(iter)
    }
}

impl AsRef<[Vec<DataType>]> for ReadReplyBatch {
    fn as_ref(&self) -> &[Vec<DataType>] {
        &self.0[..]
    }
}

impl std::ops::Deref for ReadReplyBatch {
    type Target = Vec<Vec<DataType>>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for ReadReplyBatch {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
