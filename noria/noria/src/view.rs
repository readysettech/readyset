use crate::consistency::Timestamp;
use crate::data::*;
use crate::errors::{view_err, ReadySetError, ReadySetResult};
use crate::util::like::CaseSensitivityMode;
use crate::{rpc_err, Tagged, Tagger};
use async_bincode::{AsyncBincodeStream, AsyncDestination};
use futures_util::{
    future, future::TryFutureExt, ready, stream::futures_unordered::FuturesUnordered,
    stream::StreamExt, stream::TryStreamExt,
};
use launchpad::intervals::{BoundAsRef, BoundFunctor, BoundPair};
use nom_sql::{BinaryOperator, ColumnSpecification};
use petgraph::graph::NodeIndex;
use proptest::arbitrary::Arbitrary;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::fmt;
use std::future::Future;
use std::iter::FromIterator;
use std::net::SocketAddr;
use std::ops::{Bound, Range, RangeBounds};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use tokio_tower::multiplex;
use tower::balance::p2c::Balance;
use tower::buffer::Buffer;
use tower::limit::concurrency::ConcurrencyLimit;
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

    type Future = impl Future<Output = Result<Self::Response, Self::Error>>;

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
    Ok = tower::discover::Change<usize, InnerService>,
    Error = tokio::io::Error,
> {
    // TODO: use whatever comes out of https://github.com/tower-rs/tower/issues/456 instead of
    // creating _all_ the connections every time.
    (0..crate::VIEW_POOL_SIZE)
        .map(|i| async move {
            let svc = Endpoint(addr).call(()).await?;
            Ok(tower::discover::Change::Insert(i, svc))
        })
        .collect::<futures_util::stream::FuturesUnordered<_>>()
}

fn make_views_discover(addr: SocketAddr) -> Discover {
    make_views_stream(addr)
}

// Unpin + Send bounds are needed due to https://github.com/rust-lang/rust/issues/55997
type Discover =
    impl tower::discover::Discover<Key = usize, Service = InnerService, Error = tokio::io::Error>
        + Unpin
        + Send;

pub(crate) type ViewRpc =
    Buffer<ConcurrencyLimit<Balance<Discover, Tagged<ReadQuery>>>, Tagged<ReadQuery>>;

/// Representation for a comparison predicate against a set of keys
#[derive(Serialize, Deserialize, Debug, Eq, PartialEq, Clone, Hash)]
pub enum KeyComparison {
    /// Look up exactly one key
    /// TODO(eta): this comment is crap
    Equal(Vec1<DataType>),

    /// Look up all keys within a range
    Range(BoundPair<Vec1<DataType>>),
}

#[allow(clippy::len_without_is_empty)] // can never be empty
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
    pub fn range(&self) -> Option<&BoundPair<Vec1<DataType>>> {
        match self {
            KeyComparison::Range(ref range) => Some(range),
            _ => None,
        }
    }

    /// Build a [`KeyComparison`] from a range of keys
    pub fn from_range<R>(range: &R) -> Self
    where
        R: RangeBounds<Vec1<DataType>>,
    {
        KeyComparison::Range((range.start_bound().cloned(), range.end_bound().cloned()))
    }

    /// Returns the shard key(s) that the given cell in this [`KeyComparison`] must target, given
    /// the total number of shards
    pub fn shard_keys_at(&self, key_idx: usize, num_shards: usize) -> Vec<usize> {
        match self {
            KeyComparison::Equal(key) => vec![crate::shard_by(&key[key_idx], num_shards)],
            // Since we currently implement hash-based sharding, any non-point query must target all
            // shards. This restriction could be lifted in the future by implementing (perhaps
            // optional) range-based sharding, likely with rebalancing. See Guillote-Blouin, J.
            // (2020) Implementing Range Queries and Write Policies in a Partially-Materialized
            // Data-Flow [Unpublished Master's thesis]. Harvard University S 2.4
            _ => (0..num_shards).collect(),
        }
    }

    /// Returns the shard key(s) that the first column in this [`KeyComparison`] must target, given
    /// the total number of shards
    pub fn shard_keys(&self, num_shards: usize) -> Vec<usize> {
        self.shard_keys_at(0, num_shards)
    }

    /// Returns the length of the key this [`KeyComparison`] is comparing against, or None if this
    /// is an unbounded lookup
    ///
    /// Since all KeyComparisons wrap a [`Vec1`], this function will never return `Some(0)`
    pub fn len(&self) -> Option<usize> {
        match self {
            Self::Equal(key) => Some(key.len()),
            Self::Range((Bound::Unbounded, Bound::Unbounded)) => None,
            Self::Range(
                (Bound::Included(ref key) | Bound::Excluded(ref key), Bound::Unbounded)
                | (Bound::Unbounded, Bound::Included(ref key) | Bound::Excluded(ref key)),
            ) => Some(key.len()),
            Self::Range((
                Bound::Included(ref start) | Bound::Excluded(ref start),
                Bound::Included(ref end) | Bound::Excluded(ref end),
            )) => {
                debug_assert_eq!(start.len(), end.len());
                Some(start.len())
            }
        }
    }

    /// Returns true if the given `key` is covered by this [`KeyComparsion`].
    ///
    /// Concretely, this is the case if the [`KeyComparsion`] is either an
    /// [equality](KeyComparison::equal) match on `key`, or a [range](KeyComparison::range) match
    /// that covers `key`.
    pub fn contains(&self, key: &[DataType]) -> bool {
        match self {
            Self::Equal(equal) => key == equal.as_slice(),
            Self::Range((lower, upper)) => (
                lower.as_ref().map(Vec1::as_slice),
                upper.as_ref().map(Vec1::as_slice),
            )
                .contains(key),
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

impl TryFrom<(Vec<DataType>, BinaryOperator)> for KeyComparison {
    // FIXME(eta): proper error handling here
    type Error = String;

    fn try_from((value, binop): (Vec<DataType>, BinaryOperator)) -> Result<Self, Self::Error> {
        use self::BinaryOperator::*;

        let value = Vec1::try_from(value).map_err(|e| e.to_string())?;
        let inner = match binop {
            Greater => (Bound::Excluded(value), Bound::Unbounded),
            GreaterOrEqual => (Bound::Included(value), Bound::Unbounded),
            Less => (Bound::Unbounded, Bound::Excluded(value)),
            LessOrEqual => (Bound::Unbounded, Bound::Included(value)),
            Equal => return Ok(value.into()),
            _ => return Err("bad binop!".to_string()),
        };
        Ok(KeyComparison::Range(inner))
    }
}

impl From<Vec1<DataType>> for KeyComparison {
    /// Converts to a [`KeyComparison::Equal`]
    fn from(key: Vec1<DataType>) -> Self {
        KeyComparison::Equal(key)
    }
}

impl FromIterator<DataType> for KeyComparison {
    /// Collects into a [`KeyComparison::Equal`], panicking if the iterator yields no values
    fn from_iter<T: IntoIterator<Item = DataType>>(iter: T) -> Self {
        Self::try_from(iter.into_iter().collect::<Vec<_>>())
            .expect("Tried to build a KeyComparison from an iterator that yielded no values")
    }
}

impl From<Range<Vec1<DataType>>> for KeyComparison {
    fn from(range: Range<Vec1<DataType>>) -> Self {
        KeyComparison::Range((Bound::Included(range.start), Bound::Excluded(range.end)))
    }
}

impl TryFrom<BoundPair<Vec<DataType>>> for KeyComparison {
    type Error = vec1::Size0Error;

    /// Converts to a [`KeyComparison::Range`]
    fn try_from((lower, upper): BoundPair<Vec<DataType>>) -> Result<Self, Self::Error> {
        let convert_bound = |bound| match bound {
            Bound::Unbounded => Ok(Bound::Unbounded),
            Bound::Included(x) => Ok(Bound::Included(Vec1::try_from(x)?)),
            Bound::Excluded(x) => Ok(Bound::Excluded(Vec1::try_from(x)?)),
        };
        Ok(Self::Range((convert_bound(lower)?, convert_bound(upper)?)))
    }
}

impl From<BoundPair<Vec1<DataType>>> for KeyComparison {
    /// Converts to a [`KeyComparison::Range`]
    fn from(range: BoundPair<Vec1<DataType>>) -> Self {
        KeyComparison::Range(range)
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

impl Arbitrary for KeyComparison {
    type Parameters = ();
    type Strategy = proptest::strategy::BoxedStrategy<KeyComparison>;

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        use proptest::arbitrary::any_with;
        use proptest::prop_oneof;
        use proptest::strategy::Strategy;

        let bound = || {
            any_with::<Bound<Vec<DataType>>>(((1..100).into(), ()))
                .prop_map(|bound| bound.map(|k| Vec1::try_from_vec(k).unwrap()))
        };

        prop_oneof![
            any_with::<Vec<DataType>>(((1..100).into(), ()))
                .prop_map(|k| KeyComparison::try_from(k).unwrap()),
            (bound(), bound()).prop_map(KeyComparison::Range)
        ]
        .boxed()
    }
}

#[doc(hidden)]
#[derive(Serialize, Deserialize, Debug)]
pub enum ReadQuery {
    /// Read from a leaf view
    Normal {
        /// Where to read from
        target: (NodeIndex, usize),
        /// View query to run
        query: ViewQuery,
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
    /// Set of replicas for a view, this will only include one element
    /// if there is no reader replication.
    pub replicas: Vec<ViewReplica>,
}

/// A reader replica for a view.
#[doc(hidden)]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ViewReplica {
    pub node: NodeIndex,
    pub columns: Vec<String>,
    pub schema: Option<Vec<ColumnSpecification>>,
    pub shards: Vec<ReplicaShard>,
}

/// A shard of a reader replica.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReplicaShard {
    /// The address of the worker where the shard lives in.
    pub addr: SocketAddr,
    /// The region of a shard, as specified by the argument `region`
    /// by the noria-server, where the shard lives in.
    pub region: Option<String>,
}

impl ViewBuilder {
    /// Build a `View` out of a `ViewBuilder`
    #[doc(hidden)]
    pub fn build(&self, rpcs: Arc<Mutex<HashMap<(SocketAddr, usize), ViewRpc>>>) -> View {
        // TODO(justin): Replace this with replica selection based on location.
        let replica = &self.replicas[0];

        let node = replica.node;
        let columns = replica.columns.clone();
        let shards = replica.shards.clone();
        let schema = replica.schema.clone();

        let mut addrs = Vec::with_capacity(shards.len());
        let mut conns = Vec::with_capacity(shards.len());

        for (shardi, shard) in shards.iter().enumerate() {
            use std::collections::hash_map::Entry;

            addrs.push(shard.addr);

            // one entry per shard so that we can send sharded requests in parallel even if
            // they happen to be targeting the same machine.
            let mut rpcs = rpcs.lock().unwrap();
            let s = match rpcs.entry((shard.addr, shardi)) {
                Entry::Occupied(e) => e.get().clone(),
                Entry::Vacant(h) => {
                    // TODO: maybe always use the same local port?
                    let (c, w) = Buffer::pair(
                        ConcurrencyLimit::new(
                            Balance::new(make_views_discover(shard.addr)),
                            crate::PENDING_LIMIT,
                        ),
                        crate::BUFFER_TO_POOL,
                    );
                    use tracing_futures::Instrument;
                    tokio::spawn(w.instrument(tracing::debug_span!(
                        "view_worker",
                        addr = %shard.addr,
                        shard = shardi
                    )));
                    h.insert(c.clone());
                    c
                }
            };
            conns.push(s);
        }

        let tracer = tracing::dispatcher::get_default(|d| d.clone());
        View {
            node,
            schema,
            columns,
            shard_addrs: addrs,
            shards: conns,
            tracer,
        }
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

/// Binary predicate operator for a [`ViewQueryFilter`]
#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub enum ViewQueryOperator {
    /// String matching with LIKE
    Like,
    /// String matching with case-insensitive LIKE
    ILike,
}

impl From<ViewQueryOperator> for CaseSensitivityMode {
    fn from(op: ViewQueryOperator) -> Self {
        match op {
            ViewQueryOperator::Like => Self::CaseSensitive,
            ViewQueryOperator::ILike => Self::CaseInsensitive,
        }
    }
}

impl TryFrom<nom_sql::BinaryOperator> for ViewQueryOperator {
    type Error = nom_sql::BinaryOperator;

    fn try_from(op: nom_sql::BinaryOperator) -> Result<Self, Self::Error> {
        match op {
            nom_sql::BinaryOperator::Like => Ok(Self::Like),
            nom_sql::BinaryOperator::ILike => Ok(Self::ILike),
            op => Err(op),
        }
    }
}

/// Filter the results of a view query after they're returned from the underlying reader
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ViewQueryFilter {
    /// Column in the record to filter against
    pub column: usize,
    /// Operator to use when filtering
    pub operator: ViewQueryOperator,
    /// Value to match against. This is a String right now because the only supported operations are
    /// LIKE and ILIKE, which are string-only
    pub value: String,
}

/// A read query to be run against a view.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ViewQuery {
    /// Key comparisons to read with.
    pub key_comparisons: Vec<KeyComparison>,
    /// Whether the query should block.
    pub block: bool,
    /// Column index to order by, and whether or not to reverse
    pub order_by: Option<(usize, bool)>,
    /// Maximum number of records to return
    pub limit: Option<usize>,
    /// Filter to apply to values after they're returned from the underlying reader
    pub filter: Option<ViewQueryFilter>,
    /// Timestamp to compare against for reads, if a timestamp is passed into the
    /// view query, a read will only return once the timestamp is less than
    /// the timestamp associated with the data.
    // TODO(justin): Verify reads block on timestamps once timestamps have a definition
    // with Ord.
    pub timestamp: Option<Timestamp>,
}

// TODO(andrew): consolidate From impls once RYW fully adopted
impl From<(Vec<KeyComparison>, bool, Option<Timestamp>)> for ViewQuery {
    fn from(
        (key_comparisons, block, ticket): (Vec<KeyComparison>, bool, Option<Timestamp>),
    ) -> Self {
        Self {
            key_comparisons,
            block,
            order_by: None,
            limit: None,
            filter: None,
            timestamp: ticket,
        }
    }
}

impl From<(Vec<KeyComparison>, bool)> for ViewQuery {
    fn from((key_comparisons, block): (Vec<KeyComparison>, bool)) -> Self {
        Self {
            key_comparisons,
            block,
            order_by: None,
            limit: None,
            filter: None,
            timestamp: None,
        }
    }
}

impl Service<ViewQuery> for View {
    type Response = Vec<Results>;
    type Error = ReadySetError;

    type Future = impl Future<Output = Result<Self::Response, Self::Error>> + Send;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        for s in &mut self.shards {
            let ni = self.node;
            ready!(s.poll_ready(cx))
                .map_err(rpc_err!("<View as Service<ViewQuery>>::poll_ready"))
                .map_err(|e| view_err(ni, e))?
        }
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, mut query: ViewQuery) -> Self::Future {
        let ni = self.node;
        let span = if crate::trace_next_op() {
            Some(tracing::trace_span!(
                "view-request",
                ?query.key_comparisons,
                node = self.node.index()
            ))
        } else {
            None
        };

        let columns = Arc::from(&self.columns[..]);
        if self.shards.len() == 1 {
            let request = Tagged::from(ReadQuery::Normal {
                target: (self.node, 0),
                query,
            });

            let _guard = span.as_ref().map(tracing::Span::enter);
            tracing::trace!("submit request");

            return future::Either::Left(
                self.shards[0]
                    .call(request)
                    .map_err(rpc_err!("<View as Service<ViewQuery>>::call"))
                    .and_then(move |reply| async move {
                        match reply.v {
                            ReadReply::Normal(Ok(rows)) => Ok(rows
                                .into_iter()
                                .map(|rows| Results::new(rows.into(), Arc::clone(&columns)))
                                .collect()),
                            ReadReply::Normal(Err(())) => Err(ReadySetError::ViewNotYetAvailable),
                            _ => unreachable!(),
                        }
                    })
                    .map_err(move |e| view_err(ni, e)),
            );
        }

        if let Some(ref span) = span {
            span.in_scope(|| tracing::trace!("shard request"));
        }
        let mut shard_queries = vec![Vec::new(); self.shards.len()];
        for comparison in query.key_comparisons.drain(..) {
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
                        query: ViewQuery {
                            key_comparisons: shard_queries,
                            block: query.block,
                            // TODO(eta): is it valid to copy across the order_by like this?
                            order_by: query.order_by,
                            limit: query.limit,
                            filter: query.filter.clone(),
                            timestamp: query.timestamp.clone(),
                        },
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
                        .map_err(rpc_err!("<View as Service<ViewQuery>>::call"))
                        .and_then(|reply| async move {
                            match reply.v {
                                ReadReply::Normal(Ok(rows)) => Ok(rows),
                                ReadReply::Normal(Err(())) => {
                                    Err(ReadySetError::ViewNotYetAvailable)
                                }
                                _ => unreachable!(),
                            }
                        })
                        .map_err(move |e| view_err(ni, e))
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
    pub async fn len(&mut self) -> ReadySetResult<usize> {
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
        while let Some(reply) = rsps
            .next()
            .await
            .transpose()
            .map_err(rpc_err!("View::len"))?
        {
            if let ReadReply::Size(rows) = reply.v {
                nrows += rows;
            } else {
                unreachable!();
            }
        }

        Ok(nrows)
    }

    /// Get the current keys of this view. For debugging only.
    pub async fn keys(&mut self) -> ReadySetResult<Vec<Vec<DataType>>> {
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
        while let Some(reply) = rsps
            .next()
            .await
            .transpose()
            .map_err(rpc_err!("View::keys"))?
        {
            if let ReadReply::Keys(mut keys) = reply.v {
                vec.append(&mut keys);
            } else {
                unreachable!();
            }
        }

        Ok(vec)
    }

    // TODO(andrew): consolidate RYW and normal reads into cohesive API once API design is settled.
    // RYW functionality currently added as duplicate methods so as not to disrupt current
    // reader usage until RYW is fully adopted

    /// Issue a raw `ViewQuery` against this view, and return the results.
    ///
    /// The method will block if the results are not yet available only when `block` is `true`.
    /// If `block` is false, misses will be returned as empty results. Any requested keys that have
    /// missing state will be backfilled (asynchronously if `block` is `false`).
    pub async fn raw_lookup(&mut self, query: ViewQuery) -> ReadySetResult<Vec<Results>> {
        future::poll_fn(|cx| self.poll_ready(cx)).await?;
        self.call(query).await
    }

    /// Retrieve the query results for the given parameter value.
    ///
    /// The method will block if the results are not yet available only when `block` is `true`.
    pub async fn lookup(&mut self, key: &[DataType], block: bool) -> ReadySetResult<Results> {
        self.lookup_ryw(key, block, None).await
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
    ) -> ReadySetResult<Vec<Results>> {
        self.raw_lookup((key_comparisons, block, None).into()).await
    }

    /// Retrieve the first query result for the given parameter value.
    ///
    /// The method will block if the results are not yet available only when `block` is `true`.
    pub async fn lookup_first(
        &mut self,
        key: &[DataType],
        block: bool,
    ) -> ReadySetResult<Option<Row>> {
        // TODO: Optimized version of this function?
        self.lookup_first_ryw(key, block, None).await
    }

    /// Retrieve the query results for the given parameter value.
    ///
    /// The method will block if the results are not yet available or do not have a timestamp
    /// satisfying the `ticket ` requirement only when `block` is `true`.
    /// If `block` is false, misses will be returned as empty results. Any requested keys that have
    /// missing state will be backfilled (asynchronously if `block` is `false`).
    pub async fn lookup_ryw(
        &mut self,
        key: &[DataType],
        block: bool,
        ticket: Option<Timestamp>,
    ) -> ReadySetResult<Results> {
        // TODO: Optimized version of this function?
        let key = Vec1::try_from_vec(key.into())
            .map_err(|_| view_err(self.node, ReadySetError::EmptyKey))?;
        let rs = self
            .multi_lookup_ryw(vec![KeyComparison::Equal(key)], block, ticket)
            .await?;
        Ok(rs.into_iter().next().unwrap())
    }

    /// Retrieve the query results for the given parameter values
    ///
    /// The method will block if the results are not yet available or do not have a timestamp
    /// satisfying the `ticket ` requirement only when `block` is `true`.
    /// If `block` is false, misses will be returned as empty results. Any requested keys that have
    /// missing state will be backfilled (asynchronously if `block` is `false`).
    pub async fn multi_lookup_ryw(
        &mut self,
        key_comparisons: Vec<KeyComparison>,
        block: bool,
        ticket: Option<Timestamp>,
    ) -> ReadySetResult<Vec<Results>> {
        self.raw_lookup((key_comparisons, block, ticket).into())
            .await
    }

    /// Retrieve the first query result for the given parameter value.
    ///
    /// The method will block if the results are not yet available or do not have a timestamp
    /// satisfying the `ticket ` requirement only when `block` is `true`.
    /// If `block` is false, misses will be returned as empty results. Any requested keys that have
    /// missing state will be backfilled (asynchronously if `block` is `false`).
    pub async fn lookup_first_ryw(
        &mut self,
        key: &[DataType],
        block: bool,
        ticket: Option<Timestamp>,
    ) -> ReadySetResult<Option<Row>> {
        // TODO: Optimized version of this function?
        let key = Vec1::try_from_vec(key.into())
            .map_err(|_| view_err(self.node, ReadySetError::EmptyKey))?;
        let rs = self
            .multi_lookup_ryw(vec![KeyComparison::Equal(key)], block, ticket)
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
