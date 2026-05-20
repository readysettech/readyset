use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::convert::TryFrom;
use std::fmt;
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::net::SocketAddr;
use std::ops::{Range, RangeInclusive};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use async_bincode::tokio::AsyncBincodeStream;
use dataflow_expression::{BinaryOperator as DfBinaryOperator, Dialect, Expr as DfExpr};
use futures_util::future::BoxFuture;
use futures_util::future::TryFutureExt;
use futures_util::{future, ready, Stream};
use petgraph::graph::NodeIndex;
use proptest::arbitrary::Arbitrary;
use readyset_data::{
    Bound, BoundedRange, Collation, DfType, DfValue, IntoBoundedRange, RangeBounds,
};
use readyset_errors::{
    internal, internal_err, rpc_err, unsupported, view_err, ReadySetError, ReadySetResult,
};
use readyset_multiplex as multiplex;
use readyset_sql::ast::{
    BinaryOperator, Column, ColumnConstraint, ColumnSpecification, ItemPlaceholder, Literal,
    Relation, SelectStatement, ShallowCacheQuery, SqlIdentifier, SqlType,
};
use readyset_sql::TryFromDialect as _;
use readyset_sql_passes::anonymize::{Anonymize, Anonymizer};
use readyset_tracing::child_span;
use readyset_tracing::presampled::instrument_if_enabled;
use readyset_tracing::propagation::Instrumented;
use readyset_util::intervals::cmp_start_end;
use readyset_util::redacted::Sensitive;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tower::balance::p2c::Balance;
use tower::buffer::Buffer;
use tower::limit::concurrency::ConcurrencyLimit;
use tower::timeout::Timeout;
use tower::util::BoxService;
use tower::BoxError;
use tower_service::Service;
use tracing::{debug_span, error, trace};
use tracing_futures::Instrument;
use vec1::{vec1, Vec1};

pub(crate) mod results;

use self::results::{ResultIterator, Results};
use crate::{ReaderAddress, Tagged, Tagger};

/// Index of a key column as it exists in the underlying state. During a migration this will be
/// used throughout MIR. In steady state this will refer to the reader key columns.
pub type KeyColumnIdx = usize;

/// Index of a placeholder variable as it appears in the SQL query
pub type PlaceholderIdx = usize;

/// All the information necessary to create an (anonymous) view in the graph.
///
/// This structure is not used *directly* by any of the methods that actually create views in the
/// graph, but is used throughout the codebase as a uniquely identifiable stand-in for a
/// yet-to-be-created view
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ViewCreateRequest {
    /// The query itself, as provided by the user
    pub statement: SelectStatement,

    /// The schema search path to use to resolve table references within the changelist
    ///
    /// This is actually passed as [`recipe::changelist::ChangeList::schema_search_path`] when
    /// views are created.
    pub schema_search_path: Vec<SqlIdentifier>,
}

impl ViewCreateRequest {
    /// Initialize a new [`ViewCreateRequest`] with the given query and schema search path
    pub fn new(statement: SelectStatement, schema_search_path: Vec<SqlIdentifier>) -> Self {
        Self {
            statement,
            schema_search_path,
        }
    }

    /// Anonymize the statement and schema_search_path of the ViewCreateRequest in place
    pub fn to_anonymized_string(&self) -> String {
        let mut anon = self.clone();
        let mut anonymizer = Anonymizer::new();
        anon.statement.anonymize(&mut anonymizer);
        for id in anon.schema_search_path.iter_mut() {
            id.anonymize(&mut anonymizer);
        }

        format!("{anon:?}")
    }
}

/// All the information necessary to create a shallow cache view.
///
/// This structure is similar to [`ViewCreateRequest`] but stores the query as a `ShallowViewRequest`
/// instead of a `SelectStatement`, allowing it to handle queries with syntax unsupported by deep caching.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ShallowViewRequest {
    /// The query itself, stored as sqlparser AST to support unsupported syntax
    pub query: ShallowCacheQuery,

    /// The schema search path to use to resolve table references within the changelist
    ///
    /// This is actually passed as [`recipe::changelist::ChangeList::schema_search_path`] when
    /// views are created.
    pub schema_search_path: Vec<SqlIdentifier>,
}

impl ShallowViewRequest {
    /// Initialize a new [`ShallowViewRequest`] with the given query and schema search path
    pub fn new(query: ShallowCacheQuery, schema_search_path: Vec<SqlIdentifier>) -> Self {
        Self {
            query,
            schema_search_path,
        }
    }
}

/// Representation of how a key column in a [`View`] maps back to a placeholder in the original
/// query
#[derive(Hash, Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ViewPlaceholder {
    /// This key column was generated by ReadySet, and has no mapping to the original query. This
    /// is the case, for example, for a "bogokey" column generated for unparameterized queries.
    Generated,

    /// This key column corresponds one-to-one to a placeholder in the original query. Includes the
    /// BinaryOperator used in the placeholder comparison.
    OneToOne(PlaceholderIdx, BinaryOperator),

    /// This key column corresponds to a double-ended "BETWEEN"-style range lookup in the original
    /// query, with the given placeholder indexes for the lower and upper bounds of the range
    /// respectively
    Between(PlaceholderIdx, PlaceholderIdx),

    /// This key column is the page number of a paginated query, which must be calculated by
    /// dividing the value for the `OFFSET` clause by the value for the `LIMIT` in the query
    PageNumber {
        /// The index of the placeholder for the `OFFSET` clause of the original query
        offset_placeholder: PlaceholderIdx,
        /// The `LIMIT` (page size) in the query
        limit: u64,
    },
}

#[derive(Debug)]
struct Endpoint {
    addr: SocketAddr,
    timeout: Duration,
}

/// Identifies the source base table column for a projected column
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnBase {
    /// The name of the column in the base table
    pub column: SqlIdentifier,
    /// The name of the base table for this column
    pub table: Relation,
    /// A list of constraints on the column
    pub constraints: Vec<ColumnConstraint>,
    /// If known, the PostgreSQL OID for the column's base table
    pub table_oid: Option<u32>,
    /// If known, the PostgreSQL `attnum` for the column
    pub attnum: Option<i16>,
    /// Original SQL type
    pub sql_type: SqlType,
}

impl ColumnBase {
    pub fn has_default(&self) -> bool {
        self.constraints
            .iter()
            .any(|c| matches!(c, ColumnConstraint::DefaultValue(_)))
    }

    pub fn is_not_null(&self) -> bool {
        self.constraints
            .iter()
            .any(|c| matches!(c, ColumnConstraint::NotNull))
    }
}

/// Combines the specification for a columns with its base name
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnSchema {
    /// The name of the column
    pub column: Column,
    /// The column's type
    pub column_type: DfType,
    /// If the column is an alias, this field represents its base column
    pub base: Option<ColumnBase>,
}

impl ColumnSchema {
    /// Create a new ColumnSchema from a ColumnSpecification representing a column directly in a
    /// base table with the given name.
    pub fn from_base(
        spec: ColumnSpecification,
        table: Relation,
        dialect: Dialect,
    ) -> ReadySetResult<Self> {
        let collation = spec
            .get_collation()
            .map(|name| Collation::get_or_default(dialect, name));
        Ok(Self {
            base: Some(ColumnBase {
                column: spec.column.name.clone(),
                table,
                constraints: spec.constraints,
                table_oid: None,
                attnum: None,
                sql_type: spec.sql_type.clone(),
            }),
            column: spec.column,
            column_type: DfType::from_sql_type(
                &spec.sql_type,
                dialect,
                |_| None, /* Custom types not allowed for inserts via the adapter */
                collation,
            )?,
        })
    }
}

/// A `ViewSchema` is used to describe the columns of a stored ReadySet
/// view as a vector of columns. The ViewSchema contains a vector with all
/// projected columns and a vector with columns returned to the client.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ViewSchema {
    /// The set of columns returned to the client when executing this query.
    returned_cols: Vec<ColumnSchema>,
    /// The set of columns projected at the noria flowgraph reader node.
    projected_cols: Vec<ColumnSchema>,
}

/// SchemaType is passed to most ViewSchema functions to select between the two
/// schemas contained in the ViewSchema struct.
pub enum SchemaType {
    /// Used to select the schema returned to the client when executing this
    /// query.
    ReturnedSchema,
    /// Used to select the schema projected at the noria flowgraph reader node.
    ProjectedSchema,
}

type InnerService = multiplex::Client<Instrumented<Tagged<ReadQuery>>, Tagged<ReadReply>>;

impl Service<()> for Endpoint {
    type Response = InnerService;
    type Error = tokio::io::Error;

    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _: ()) -> Self::Future {
        let f = tokio::net::TcpStream::connect(self.addr);
        let timeout = self.timeout;
        Box::pin(async move {
            let s = tokio::time::timeout(timeout, f).await??;
            s.set_nodelay(true)?;
            let s = AsyncBincodeStream::from(s).for_async();
            Ok(multiplex::Client::with_error_handler(
                s,
                Tagger::default(),
                |e| error!(error = %e, "View server went away"),
            ))
        })
    }
}

impl ViewSchema {
    /// Create a ViewSchema from returned and projected column schema vectors
    pub fn new(returned_cols: Vec<ColumnSchema>, projected_cols: Vec<ColumnSchema>) -> ViewSchema {
        ViewSchema {
            returned_cols,
            projected_cols,
        }
    }

    /// Get the schema specified by the schema type
    pub fn schema(&self, schema_type: SchemaType) -> &[ColumnSchema] {
        match schema_type {
            SchemaType::ReturnedSchema => &self.returned_cols,
            SchemaType::ProjectedSchema => &self.projected_cols,
        }
    }

    /// Return a vector specifying the types of the columns for the requested indices
    pub fn col_types<I>(&self, indices: I, schema_type: SchemaType) -> ReadySetResult<Vec<&DfType>>
    where
        I: IntoIterator<Item = usize>,
    {
        let schema = self.schema(schema_type);
        indices
            .into_iter()
            .map(|i| schema.get(i).map(|c| &c.column_type))
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| internal_err!("Schema expects valid column indices"))
    }

    /// Convert the given iterator [`Columns`] to a `Vec` of [`ColumnSchema`]. The columns match if
    /// either the column name matches (the alias) or the real base name
    pub fn to_cols<'a, 'b, T>(
        &'a self,
        cols: T,
        schema_type: SchemaType,
    ) -> ReadySetResult<Vec<&'a ColumnSchema>>
    where
        T: IntoIterator<Item = &'b Column>,
    {
        let mut by_name = HashMap::new();
        let mut by_base_name = HashMap::new();
        for cs in self.schema(schema_type) {
            by_name.insert(&cs.column.name, cs);
            if let Some(base) = &cs.base {
                by_base_name.insert(&base.column, cs);
            }
        }

        cols.into_iter()
            .map(move |c| {
                by_name
                    .get(&c.name)
                    .or_else(|| by_base_name.get(&c.name))
                    .copied()
                    .ok_or_else(|| internal_err!("Column {} not found", c.display_unquoted()))
            })
            .collect()
    }

    /// Get the indices of the columns in the schema that correspond to the list of provided
    /// [`readyset_sql::ast::Column`]. The columns match if either the column name matches (the
    /// alias) or the real base name
    pub fn indices_for_cols<'a, T>(
        &self,
        cols: T,
        schema_type: SchemaType,
    ) -> ReadySetResult<Vec<usize>>
    where
        T: Iterator<Item = &'a Column>,
    {
        let schema = self.schema(schema_type);

        cols.map(|c| {
            schema.iter().position(|e| {
                e.column.name == c.name
                    || e.base.as_ref().map(|b| b.column == c.name).unwrap_or(false)
            })
        })
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| internal_err!("Schema expects all columns to be present"))
    }
}

impl ColumnSchema {
    /// Consume the schema, returning the type for the column
    pub fn into_type(self) -> DfType {
        self.column_type
    }
}

fn make_views_stream(addr: SocketAddr, timeout: Duration) -> Discover {
    // TODO: use whatever comes out of https://github.com/tower-rs/tower/issues/456 instead of
    // creating _all_ the connections every time.
    Box::pin(
        (0..crate::VIEW_POOL_SIZE)
            .map(|i| async move {
                let svc = Endpoint { addr, timeout }.call(()).await?;
                Ok(tower::discover::Change::Insert(i, svc))
            })
            .collect::<futures_util::stream::FuturesUnordered<_>>(),
    ) as Pin<Box<_>>
}

fn make_views_discover(addr: SocketAddr, timeout: Duration) -> Discover {
    make_views_stream(addr, timeout)
}

// Send bounds are needed due to https://github.com/rust-lang/rust/issues/55997
pub(crate) type Discover = Pin<
    Box<
        dyn Stream<Item = Result<tower::discover::Change<usize, InnerService>, tokio::io::Error>>
            + Send,
    >,
>;

pub(crate) type ViewRpc = Buffer<
    Instrumented<Tagged<ReadQuery>>,
    BoxFuture<'static, Result<Tagged<ReadReply>, BoxError>>,
>;

/// Representation for a comparison predicate against a set of keys
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum KeyComparison {
    /// Look up exactly one key
    Equal(Vec1<DfValue>),

    /// Look up all keys within a range
    Range(BoundedRange<Vec1<DfValue>>),
}

#[allow(clippy::len_without_is_empty)] // can never be empty
impl KeyComparison {
    /// Attempt to construct a key comparison comparing the given key using the given binary
    /// operator
    pub fn from_key_and_operator(
        key: Vec<DfValue>,
        operator: BinaryOperator,
    ) -> ReadySetResult<Self> {
        use BinaryOperator::*;

        let key = Vec1::try_from(key).map_err(|_| ReadySetError::EmptyKey)?;
        let inner = match operator {
            Greater => key.range_from(),
            GreaterOrEqual => key.range_from_inclusive(),
            Less => key.range_to(),
            LessOrEqual => key.range_to_inclusive(),
            Equal => return Ok(key.into()),
            _ => unsupported!("Unsupported operator `{operator}` in key"),
        };
        Ok(Self::Range(inner))
    }

    /// Project a KeyComparison into an optional equality predicate, or return None if it's a range
    /// predicate. Handles both [`Equal`] and single-length [`Range`]s
    pub fn equal(&self) -> Option<&Vec1<DfValue>> {
        match self {
            KeyComparison::Equal(ref key) => Some(key),
            KeyComparison::Range((Bound::Included(ref key), Bound::Included(ref key2)))
                if key == key2 =>
            {
                Some(key)
            }
            _ => None,
        }
    }

    /// Convert a KeyComparison into an optional equality predicate, consuming the key comparison,
    /// or return None if it's a range predicate. Handles both [`Equal`] and single-length
    /// [`Range`]s
    pub fn into_equal(self) -> Option<Vec1<DfValue>> {
        match self {
            KeyComparison::Equal(key) => Some(key),
            KeyComparison::Range((Bound::Included(key), Bound::Included(ref key2)))
                if key == *key2 =>
            {
                Some(key)
            }
            _ => None,
        }
    }

    /// Project a KeyComparison into an optional range predicate, or return None if it's a range
    /// predicate
    pub fn range(&self) -> Option<&BoundedRange<Vec1<DfValue>>> {
        match self {
            KeyComparison::Range(ref range) => Some(range),
            _ => None,
        }
    }

    /// Build a [`KeyComparison`] from a range of keys.
    ///
    /// If the range has length 1 (both the ends are inclusive bounds on the same value) will return
    /// [`KeyComparison::Equal`].
    pub fn from_range<R>(range: &R) -> Self
    where
        R: RangeBounds<Vec1<DfValue>>,
    {
        match (range.start_bound(), range.end_bound()) {
            (Bound::Included(key1), Bound::Included(key2)) if key1 == key2 => {
                KeyComparison::Equal(key1.clone())
            }
            (start, end) => KeyComparison::Range((start.cloned(), end.cloned())),
        }
    }

    /// Convert the [`KeyComparison`] into a [`KeyComparison::Range`].
    ///
    /// If self was an Equal comparison, return a Range comparison with inclusive upper and lower
    /// bounds equal to the Equal key.
    pub fn into_range(self) -> KeyComparison {
        match self {
            KeyComparison::Range(_) => self,
            KeyComparison::Equal(key) => {
                KeyComparison::Range((Bound::Included(key.clone()), Bound::Included(key)))
            }
        }
    }

    /// Returns true if this KeyComparison represents a range where the upper bound is less than the
    /// lower bound
    ///
    /// # Examples
    ///
    /// ```
    /// use readyset_client::KeyComparison;
    /// use readyset_data::DfValue;
    /// use vec1::vec1;
    ///
    /// let not_reversed =
    ///     KeyComparison::from_range(&(vec1![DfValue::from(0)]..=vec1![DfValue::from(1)]));
    /// assert!(!not_reversed.is_reversed_range());
    ///
    /// let reversed = KeyComparison::from_range(&(vec1![DfValue::from(1)]..=vec1![DfValue::from(0)]));
    /// assert!(reversed.is_reversed_range());
    /// ```
    pub fn is_reversed_range(&self) -> bool {
        cmp_start_end(
            <Self as RangeBounds<Vec1<DfValue>>>::start_bound(self).into(),
            <Self as RangeBounds<Vec1<DfValue>>>::end_bound(self).into(),
        ) == Ordering::Greater
    }

    /// Returns the length of the key this [`KeyComparison`] is comparing against.
    ///
    /// Since all KeyComparisons wrap a [`Vec1`], this function will never return 0
    pub fn len(&self) -> usize {
        match self {
            Self::Equal(key) => key.len(),
            Self::Range((
                Bound::Included(ref start) | Bound::Excluded(ref start),
                Bound::Included(ref end) | Bound::Excluded(ref end),
            )) => {
                debug_assert_eq!(start.len(), end.len());
                start.len()
            }
        }
    }

    /// Returns true if the given `key` is covered by this [`KeyComparison`].
    ///
    /// Concretely, this is the case if the [`KeyComparison`] is either an [equality][] match on
    /// `key`, or a [range][] match that covers `key`.
    ///
    /// [equality]: KeyComparison::equal
    /// [range]: KeyComparison::range
    ///
    /// # Examples
    ///
    /// Equal keys contain themselves and only themselves:
    ///
    /// ```rust
    /// use readyset_client::KeyComparison;
    /// use readyset_data::DfValue;
    /// use vec1::vec1;
    ///
    /// let key = KeyComparison::Equal(vec1![1.into(), 2.into()]);
    /// assert!(key.contains(&[1.into(), 2.into()]));
    /// assert!(!key.contains(&[1.into(), 3.into()]));
    /// ```
    ///
    /// Range keys contain anything in the range, comparing lexicographically
    ///
    /// ```rust
    /// use readyset_client::KeyComparison;
    /// use readyset_data::Bound::*;
    /// use readyset_data::DfValue;
    /// use vec1::vec1;
    ///
    /// let key = KeyComparison::Range((
    ///     Included(vec1![1.into(), 2.into()]),
    ///     Excluded(vec1![1.into(), 5.into()]),
    /// ));
    ///
    /// assert!(key.contains(&[1.into(), 3.into()]));
    /// assert!(!key.contains(&[2.into(), 2.into()]));
    /// ```
    pub fn contains<'a, I>(&'a self, key: I) -> bool
    where
        I: IntoIterator<Item = &'a DfValue> + Clone,
    {
        self.as_ref().contains(key)
    }

    /// Borrow as a [`KeyComparisonRef`], sharing the inner `Vec1<DfValue>` rather than
    /// cloning. The returned view borrows from `self` for `'_`.
    pub fn as_ref(&self) -> KeyComparisonRef<'_> {
        match self {
            KeyComparison::Equal(v) => KeyComparisonRef::Equal(v),
            KeyComparison::Range(r) => KeyComparisonRef::Range(r),
        }
    }

    /// Returns true if this [`KeyComparison`] is an equality comparison predicate.
    pub fn is_equal(&self) -> bool {
        matches!(self, KeyComparison::Equal(_))
    }

    /// Returns `true` if this [`KeyComparison`] is a [`Range`] key.
    ///
    /// [`Range`]: KeyComparison::Range
    pub fn is_range(&self) -> bool {
        matches!(self, KeyComparison::Range(..))
    }

    /// Construct a new [`KeyComparison`] of the same kind as `self` by mapping the given function
    /// over either the equal key or one or both endpoints of the range key
    #[must_use]
    pub fn map_endpoints<F>(self, mut f: F) -> Self
    where
        F: FnMut(Vec1<DfValue>) -> Vec1<DfValue>,
    {
        match self {
            KeyComparison::Equal(k) => KeyComparison::Equal(f(k)),
            KeyComparison::Range((lower, upper)) => {
                KeyComparison::Range((lower.map(&mut f), upper.map(&mut f)))
            }
        }
    }
}

impl PartialEq for KeyComparison {
    fn eq(&self, other: &Self) -> bool {
        use KeyComparison::*;
        match (self, other) {
            (Equal(k1), Equal(k2)) => k1 == k2,
            (Range(r1), Range(r2)) => r1 == r2,
            (Equal(eq), Range((Bound::Included(k1), Bound::Included(k2)))) => eq == k1 && k1 == k2,
            (Equal(_), Range(_)) => false,
            (Range(_), Equal(_)) => other == self,
        }
    }
}

impl Eq for KeyComparison {}

impl Hash for KeyComparison {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Self::Equal(k) => k.hash(state),
            Self::Range((Bound::Included(k1), Bound::Included(k2))) if k1 == k2 => k1.hash(state),
            Self::Range(r) => r.hash(state),
        }
    }
}

/// A borrowed view of a [`KeyComparison`] yielded by [`ReplayKeys`] iteration.
///
/// Holds the same `Equal` / `Range` variants as [`KeyComparison`] but borrows the inner
/// `Vec1<DfValue>` so hot-loop consumers can match on the variant without cloning. Obtain
/// one from a borrowed [`KeyComparison`] via [`KeyComparison::as_ref`].
///
/// The Ref's API splits in two:
/// - [`contains`](Self::contains) and [`to_owned`](Self::to_owned) mirror the corresponding
///   `KeyComparison` semantics exactly.
/// - [`equal`](Self::equal) is a Ref-only accessor with no `KeyComparison` analogue. It
///   keys off the variant tag alone, so a degenerate `Range((Included(k), Included(k)))`
///   yields `None`, even though [`KeyComparison`]'s `PartialEq`/`Hash` treat that range
///   and `Equal(k)` as the same key. That divergence is deliberate: lookup-path selection
///   (point vs. range) depends on the variant tag, not on set-theoretic equivalence.
#[derive(Copy, Clone, Debug)]
pub enum KeyComparisonRef<'a> {
    Equal(&'a Vec1<DfValue>),
    Range(&'a BoundedRange<Vec1<DfValue>>),
}

impl<'a> KeyComparisonRef<'a> {
    /// Materialize an owned [`KeyComparison`] from a borrowed view.
    pub fn to_owned(&self) -> KeyComparison {
        match self {
            KeyComparisonRef::Equal(v) => KeyComparison::Equal((*v).clone()),
            KeyComparisonRef::Range(r) => KeyComparison::Range((*r).clone()),
        }
    }

    /// Borrow the inner `Vec1<DfValue>` only when the variant tag is `Equal`. Returns
    /// `None` on `Range`, including the degenerate `Range((Included(k), Included(k)))`
    /// that [`KeyComparison`]'s `PartialEq` would treat as equal to `Equal(k)`.
    pub fn equal(&self) -> Option<&'a Vec1<DfValue>> {
        match self {
            KeyComparisonRef::Equal(v) => Some(v),
            KeyComparisonRef::Range(_) => None,
        }
    }

    /// True if the given multi-column key falls inside this comparison. Mirrors
    /// [`KeyComparison::contains`].
    pub fn contains<I>(self, key: I) -> bool
    where
        I: IntoIterator<Item = &'a DfValue> + Clone,
    {
        match self {
            KeyComparisonRef::Equal(equal) => key.into_iter().cmp(equal.iter()) == Ordering::Equal,
            KeyComparisonRef::Range((lower, upper)) => {
                (match lower {
                    Bound::Included(start) => key.clone().into_iter().cmp(start.iter()).is_ge(),
                    Bound::Excluded(start) => key.clone().into_iter().cmp(start.iter()).is_gt(),
                }) && (match upper {
                    Bound::Included(end) => key.into_iter().cmp(end.iter()).is_le(),
                    Bound::Excluded(end) => key.into_iter().cmp(end.iter()).is_lt(),
                })
            }
        }
    }
}

/// A collection of [`KeyComparison`]s for a partial replay, physically partitioned into
/// [`KeyComparison::Equal`] (held in one [`HashSet`]) and [`KeyComparison::Range`] (held in
/// another).
///
/// The partition keeps each key's original variant intact: an `Equal(k)` is stored as Equal,
/// a `Range((Included(k), Included(k)))` (a degenerate range) is stored as Range. Downstream
/// consumers select the lookup path (point vs. range) based on the variant, so collapsing a
/// degenerate range to Equal would redirect it onto the HashMap index when the source node
/// may only have a BTree index. The two variants are *semantically* equivalent under
/// [`KeyComparison`]'s `PartialEq`/`Hash`, but functionally distinct at the lookup layer.
///
/// # Why the partition exists
///
/// Consumers like `MissBuilder::build` need to ask "does any range here cover this record's
/// key?" With a flat `HashSet<KeyComparison>`, that question requires scanning the entire set
/// to find the Range entries -- catastrophic when replays carry millions of Equal keys with
/// no ranges. Splitting at construction time turns that question into a scan over only the
/// actual range keys (typically zero).
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplayKeys {
    equals: HashSet<Vec1<DfValue>>,
    ranges: HashSet<BoundedRange<Vec1<DfValue>>>,
}

impl ReplayKeys {
    /// Construct an empty [`ReplayKeys`]. Equivalent to [`Default::default`].
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert a [`KeyComparison`]. Returns `true` if the partition for the key's variant did
    /// not previously contain it. Variants are preserved as-is: a degenerate range stays in
    /// the range partition, never silently routed to the Equal partition.
    pub fn insert(&mut self, k: KeyComparison) -> bool {
        match k {
            KeyComparison::Equal(v) => self.equals.insert(v),
            KeyComparison::Range(r) => self.ranges.insert(r),
        }
    }

    /// Total number of keys (Equal + Range).
    pub fn len(&self) -> usize {
        self.equals.len() + self.ranges.len()
    }

    /// True if there are no keys at all.
    pub fn is_empty(&self) -> bool {
        self.equals.is_empty() && self.ranges.is_empty()
    }

    /// Iterate the [`KeyComparison::Range`] portion of the collection.
    ///
    /// Fast-path accessor used by `MissBuilder::build` to find a range covering a record's
    /// key without touching [`KeyComparison::Equal`] entries.
    pub fn ranges(&self) -> impl ExactSizeIterator<Item = &BoundedRange<Vec1<DfValue>>> + '_ {
        self.ranges.iter()
    }

    /// True if the range partition is non-empty. O(1), so hot-path consumers can
    /// short-circuit per-record range scans on the common case where a replay carries
    /// only point keys.
    pub fn has_any_range(&self) -> bool {
        !self.ranges.is_empty()
    }

    /// Iterate the [`KeyComparison::Equal`] portion of the collection.
    pub fn equals(&self) -> impl ExactSizeIterator<Item = &Vec1<DfValue>> + '_ {
        self.equals.iter()
    }

    /// True if the partition for the key's variant contains it. Each variant is looked up in
    /// its own partition; a degenerate range is *not* matched against an `Equal` of the same
    /// inner value.
    pub fn contains(&self, k: &KeyComparison) -> bool {
        self.contains_ref(k.as_ref())
    }

    /// Borrowed-key counterpart to [`contains`](Self::contains). Lets retain-style loops on
    /// borrowed [`KeyComparisonRef`] views probe membership without first materializing an
    /// owned [`KeyComparison`].
    pub fn contains_ref(&self, k: KeyComparisonRef<'_>) -> bool {
        match k {
            KeyComparisonRef::Equal(v) => self.equals.contains(v),
            KeyComparisonRef::Range(r) => self.ranges.contains(r),
        }
    }

    /// Iterate the keys as borrowed [`KeyComparisonRef`] views. No allocation per yield.
    ///
    /// This is the right iterator for hot-loop consumers that just need to match on the
    /// variant. If you need owned [`KeyComparison`] values (e.g., for serialization or for
    /// passing to a by-value API), use [`iter_owned`](Self::iter_owned).
    pub fn iter(&self) -> ReplayKeysIter<'_> {
        ReplayKeysIter {
            equals: self.equals.iter(),
            ranges: self.ranges.iter(),
        }
    }

    /// Iterate the keys as owned [`KeyComparison`] values. Clones the underlying
    /// `Vec1<DfValue>` per yield -- prefer [`iter`](Self::iter) when only matching on the
    /// variant.
    pub fn iter_owned(&self) -> impl Iterator<Item = KeyComparison> + '_ {
        self.iter().map(|k| k.to_owned())
    }

    /// Retain only the keys for which the predicate returns true. The predicate receives a
    /// borrowed [`KeyComparisonRef`], so no key allocation happens per element.
    pub fn retain<F>(&mut self, mut f: F)
    where
        F: FnMut(KeyComparisonRef<'_>) -> bool,
    {
        self.equals.retain(|v| f(KeyComparisonRef::Equal(v)));
        self.ranges.retain(|r| f(KeyComparisonRef::Range(r)));
    }

    /// Remove all keys.
    pub fn clear(&mut self) {
        self.equals.clear();
        self.ranges.clear();
    }

    /// Remove a single key. Returns true if the collection contained the key in the
    /// partition for its variant. A degenerate range is *not* matched against the Equal
    /// partition.
    pub fn remove(&mut self, k: &KeyComparison) -> bool {
        match k {
            KeyComparison::Equal(v) => self.equals.remove(v),
            KeyComparison::Range(r) => self.ranges.remove(r),
        }
    }
}

/// Borrowed iterator over a [`ReplayKeys`], yielding [`KeyComparisonRef`] views. Concrete
/// (not boxed) so the optimizer can inline `.next()` at every call site.
pub struct ReplayKeysIter<'a> {
    equals: std::collections::hash_set::Iter<'a, Vec1<DfValue>>,
    ranges: std::collections::hash_set::Iter<'a, BoundedRange<Vec1<DfValue>>>,
}

impl<'a> Iterator for ReplayKeysIter<'a> {
    type Item = KeyComparisonRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(r) = self.ranges.next() {
            return Some(KeyComparisonRef::Range(r));
        }
        self.equals.next().map(KeyComparisonRef::Equal)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let (lo_e, hi_e) = self.equals.size_hint();
        let (lo_r, hi_r) = self.ranges.size_hint();
        (lo_e + lo_r, hi_e.and_then(|e| hi_r.map(|r| e + r)))
    }
}

/// Owned iterator over a [`ReplayKeys`], yielding [`KeyComparison`] values. Concrete to keep
/// dispatch monomorphic at consumers.
pub struct ReplayKeysIntoIter {
    equals: std::collections::hash_set::IntoIter<Vec1<DfValue>>,
    ranges: std::collections::hash_set::IntoIter<BoundedRange<Vec1<DfValue>>>,
}

impl Iterator for ReplayKeysIntoIter {
    type Item = KeyComparison;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(r) = self.ranges.next() {
            return Some(KeyComparison::Range(r));
        }
        self.equals.next().map(KeyComparison::Equal)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let (lo_e, hi_e) = self.equals.size_hint();
        let (lo_r, hi_r) = self.ranges.size_hint();
        (lo_e + lo_r, hi_e.and_then(|e| hi_r.map(|r| e + r)))
    }
}

impl FromIterator<KeyComparison> for ReplayKeys {
    fn from_iter<I: IntoIterator<Item = KeyComparison>>(iter: I) -> Self {
        let mut rk = ReplayKeys::default();
        rk.extend(iter);
        rk
    }
}

impl Extend<KeyComparison> for ReplayKeys {
    fn extend<I: IntoIterator<Item = KeyComparison>>(&mut self, iter: I) {
        for k in iter {
            self.insert(k);
        }
    }
}

impl IntoIterator for ReplayKeys {
    type Item = KeyComparison;
    type IntoIter = ReplayKeysIntoIter;

    fn into_iter(self) -> Self::IntoIter {
        ReplayKeysIntoIter {
            equals: self.equals.into_iter(),
            ranges: self.ranges.into_iter(),
        }
    }
}

impl<'a> IntoIterator for &'a ReplayKeys {
    type Item = KeyComparisonRef<'a>;
    type IntoIter = ReplayKeysIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl Arbitrary for ReplayKeys {
    type Parameters = ();
    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        use proptest::arbitrary::any_with;
        use proptest::strategy::Strategy;

        any_with::<Vec<KeyComparison>>(((0..16).into(), ()))
            .prop_map(|ks| ks.into_iter().collect())
            .boxed()
    }

    type Strategy = proptest::strategy::BoxedStrategy<ReplayKeys>;
}

impl TryFrom<Vec<DfValue>> for KeyComparison {
    type Error = vec1::Size0Error;

    /// Converts to a [`KeyComparison::Equal`]. Returns an error if the input vector is empty
    fn try_from(value: Vec<DfValue>) -> Result<Self, Self::Error> {
        Ok(Vec1::try_from(value)?.into())
    }
}

impl From<Vec1<DfValue>> for KeyComparison {
    /// Converts to a [`KeyComparison::Equal`]
    fn from(key: Vec1<DfValue>) -> Self {
        KeyComparison::Equal(key)
    }
}

impl TryFrom<RangeInclusive<Vec<DfValue>>> for KeyComparison {
    type Error = vec1::Size0Error;

    fn try_from(range: RangeInclusive<Vec<DfValue>>) -> Result<Self, Self::Error> {
        let (start, end) = range.into_inner();
        Ok(KeyComparison::Range((
            Bound::Included(Vec1::try_from(start)?),
            Bound::Included(Vec1::try_from(end)?),
        )))
    }
}

impl From<Range<Vec1<DfValue>>> for KeyComparison {
    fn from(range: Range<Vec1<DfValue>>) -> Self {
        KeyComparison::Range((Bound::Included(range.start), Bound::Excluded(range.end)))
    }
}

impl TryFrom<BoundedRange<Vec<DfValue>>> for KeyComparison {
    type Error = vec1::Size0Error;

    /// Converts to a [`KeyComparison::Range`]
    fn try_from((lower, upper): BoundedRange<Vec<DfValue>>) -> Result<Self, Self::Error> {
        let convert_bound = |bound| match bound {
            Bound::Included(x) => Ok(Bound::Included(Vec1::try_from(x)?)),
            Bound::Excluded(x) => Ok(Bound::Excluded(Vec1::try_from(x)?)),
        };
        Ok(Self::Range((convert_bound(lower)?, convert_bound(upper)?)))
    }
}

impl RangeBounds<Vec1<DfValue>> for KeyComparison {
    fn start_bound(&self) -> Bound<&Vec1<DfValue>> {
        use Bound::*;
        use KeyComparison::*;
        match self {
            Equal(ref key) => Included(key),
            Range(bp) => bp.start_bound(),
        }
    }

    fn end_bound(&self) -> Bound<&Vec1<DfValue>> {
        use Bound::*;
        use KeyComparison::*;
        match self {
            Equal(ref key) => Included(key),
            Range(bp) => bp.end_bound(),
        }
    }
}

impl RangeBounds<Vec<DfValue>> for KeyComparison {
    fn start_bound(&self) -> Bound<&Vec<DfValue>> {
        self.start_bound().map(Vec1::as_vec)
    }

    fn end_bound(&self) -> Bound<&Vec<DfValue>> {
        self.end_bound().map(Vec1::as_vec)
    }
}

impl RangeBounds<Vec1<DfValue>> for &KeyComparison {
    fn start_bound(&self) -> Bound<&Vec1<DfValue>> {
        use KeyComparison::*;
        match self {
            Equal(ref key) => Bound::Included(key),
            Range(bp) => bp.start_bound(),
        }
    }

    fn end_bound(&self) -> Bound<&Vec1<DfValue>> {
        use KeyComparison::*;
        match self {
            Equal(ref key) => Bound::Included(key),
            Range(bp) => bp.end_bound(),
        }
    }
}

impl RangeBounds<Vec<DfValue>> for &KeyComparison {
    fn start_bound(&self) -> Bound<&Vec<DfValue>> {
        (**self).start_bound()
    }

    fn end_bound(&self) -> Bound<&Vec<DfValue>> {
        (**self).end_bound()
    }
}

impl std::ops::RangeBounds<Vec<DfValue>> for KeyComparison {
    fn start_bound(&self) -> std::ops::Bound<&Vec<DfValue>> {
        RangeBounds::start_bound(self).into()
    }

    fn end_bound(&self) -> std::ops::Bound<&Vec<DfValue>> {
        RangeBounds::end_bound(self).into()
    }
}

impl Arbitrary for KeyComparison {
    type Parameters = ();
    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        use proptest::arbitrary::any_with;
        use proptest::prop_oneof;
        use proptest::strategy::Strategy;

        let bound = || {
            any_with::<Bound<Vec<DfValue>>>(((1..100).into(), None)).prop_map(|bound| {
                #[allow(clippy::unwrap_used)]
                // This is only used for testing, so we allow calling `unwrap()`, and because we
                // know we are generating vectors of length 1 and beyond.
                bound.map(|k| Vec1::try_from_vec(k).unwrap())
            })
        };

        prop_oneof![
            any_with::<Vec<DfValue>>(((1..100).into(), None)).prop_map(|k| {
                #[allow(clippy::unwrap_used)]
                // This is only used for testing, so we allow calling `unwrap()`, and because we
                // know we are generating vectors of length 1 and beyond.
                KeyComparison::try_from(k).unwrap()
            }),
            (bound(), bound()).prop_map(KeyComparison::Range)
        ]
        .boxed()
    }

    type Strategy = proptest::strategy::BoxedStrategy<KeyComparison>;
}

#[derive(Serialize, Deserialize, Debug)]
#[allow(clippy::large_enum_variant)] // TODO: benchmark cost/benefit of boxing ViewQuery
pub enum ReadQuery {
    /// Read from a leaf view
    Normal {
        /// Where to read from
        target: ReaderAddress,
        /// View query to run
        query: ViewQuery,
    },
    /// Read the size of a leaf view
    Size {
        /// Where to read from
        target: ReaderAddress,
    },
    /// Read all keys from a leaf view (for debugging)
    /// TODO(alex): queries with this value are not totally implemented, and might not actually
    /// work
    Keys {
        /// Where to read from
        target: ReaderAddress,
    },
}

/// The result of a lookup to a view.
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct LookupResult<D> {
    pub results: Vec<D>,
    pub stats: ReadReplyStats,
}

impl<D> LookupResult<D> {
    /// Maps a set of lookup results from Vec<D> to Vec<U>.
    pub fn map_results<U, F>(self, mut f: F) -> LookupResult<U>
    where
        F: FnMut(D, &ReadReplyStats) -> U,
    {
        let Self { results, stats } = self;
        LookupResult {
            results: results.into_iter().map(|d| f(d, &stats)).collect(),
            stats,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Default, PartialEq, Eq, Clone)]
pub struct ReadReplyStats {
    /// The count of cache misses which have occurred
    pub cache_misses: u64,
}

impl ReadReplyStats {
    /// Creates a new [`ReadReplyStats`]
    #[must_use]
    pub fn merge(&self, other: &Self) -> Self {
        Self {
            cache_misses: self.cache_misses + other.cache_misses,
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub enum ReadReply<D = ReadReplyBatch> {
    /// A reply to a normal lookup request
    Normal(ReadySetResult<LookupResult<D>>),
    /// Read size of view
    Size(usize),
    // Read keys of view
    Keys(Vec<Vec<DfValue>>),
}

impl<D> ReadReply<D> {
    /// Convert this [`ReadReply`] into a [`ReadReply::Normal`], consuming self
    pub fn into_normal(self) -> Option<ReadySetResult<LookupResult<D>>> {
        if let Self::Normal(v) = self {
            Some(v)
        } else {
            None
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReaderHandleBuilder {
    pub name: Relation,

    pub node: NodeIndex,
    pub columns: Arc<[SqlIdentifier]>,
    pub schema: Option<ViewSchema>,

    /// Address of the worker hosting the reader, if running.
    pub read_addr: Option<SocketAddr>,

    /// (view_placeholder, key_column_index) pairs according to their mapping. Contains exactly one
    /// entry for each key column at the reader.
    pub key_mapping: Vec<(ViewPlaceholder, KeyColumnIdx)>,

    /// The amount of time before a view request RPC is terminated.
    pub view_request_timeout: Duration,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
/// Builds a [`ReusedReaderHandle`]
pub struct ReusedReaderHandleBuilder {
    /// The builder for the [`ReaderHandle`] that is being reused
    pub builder: ReaderHandleBuilder,
    /// Remapping from [`PlaceholderIdx`]s in the executed query to inlined [`Literal`]s in the
    /// cached query.
    ///
    /// This view can only be used if the values in this map match the values passed on execution
    /// for the given placeholders.
    pub required_values: HashMap<PlaceholderIdx, Literal>,
    /// Remapping from [`PlaceholderIdx`]s in the migrated query to [`Literal`]s in the executed
    /// query.
    ///
    /// This remapping is applied to [`ViewBuilder::key_mapping`] when building keys.
    pub key_remapping: HashMap<PlaceholderIdx, Literal>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
/// A single [`ReaderHandleBuilder`] or a collection of [`ReusedReaderHandleBuilder`]s
pub enum ViewBuilder {
    /// A [`ViewBuilder`]
    Single(ReaderHandleBuilder),
    /// A collection of [`ReusedReaderHandleBuilder`]s
    MultipleReused(Vec1<ReusedReaderHandleBuilder>),
}

impl ReaderHandleBuilder {
    /// Build a [`ReaderHandle`] out of a [`ReaderHandleBuilder`].
    pub async fn build(
        &self,
        rpcs: Arc<Mutex<HashMap<(SocketAddr, usize), ViewRpc>>>,
    ) -> ReadySetResult<ReaderHandle> {
        let node = self.node;
        let shard_addr = self
            .read_addr
            .ok_or(ReadySetError::ReaderNotRunning { node })?;

        let columns = self.columns.clone();
        let schema = self.schema.clone();
        let key_mapping = self.key_mapping.clone();

        let shard = {
            use std::collections::hash_map::Entry;

            let mut rpcs = rpcs.lock().await;
            #[allow(clippy::significant_drop_in_scrutinee)]
            match rpcs.entry((shard_addr, 0)) {
                Entry::Occupied(e) => e.get().clone(),
                Entry::Vacant(h) => {
                    // TODO: maybe always use the same local port?
                    let (c, w) = Buffer::pair(
                        BoxService::new(Timeout::new(
                            ConcurrencyLimit::new(
                                Balance::new(make_views_discover(
                                    shard_addr,
                                    self.view_request_timeout,
                                )),
                                crate::PENDING_LIMIT,
                            ),
                            self.view_request_timeout,
                        )),
                        crate::BUFFER_TO_POOL,
                    );
                    tokio::spawn(w.instrument(debug_span!(
                        "view_worker",
                        addr = %shard_addr,
                        shard = 0
                    )));
                    h.insert(c.clone());
                    c
                }
            }
        };

        Ok(ReaderHandle {
            name: self.name.clone(),
            node,
            schema,
            columns,
            key_mapping,
            shard,
        })
    }
}

impl ViewBuilder {
    /// Build a `View` from `ViewBuilder`. Wraps ReaderHandleBuilder::build().
    pub async fn build(
        &self,
        rpcs: Arc<Mutex<HashMap<(SocketAddr, usize), ViewRpc>>>,
    ) -> ReadySetResult<View> {
        match self {
            ViewBuilder::Single(builder) => Ok(View::Single(builder.build(rpcs).await?)),
            ViewBuilder::MultipleReused(builders) => {
                let mut handles = Vec::with_capacity(builders.len());
                for ReusedReaderHandleBuilder {
                    builder,
                    key_remapping,
                    required_values,
                } in builders
                {
                    let reader_handle = builder.build(rpcs.clone()).await?;
                    handles.push(ReusedReaderHandle {
                        reader_handle,
                        key_remapping: key_remapping.clone(),
                        required_values: required_values.clone(),
                    })
                }
                Ok(View::MultipleReused(handles.try_into().unwrap()))
            }
        }
    }
}

/// A `ReaderHandle` is used to query previously defined external reader nodes.
///
/// Note that if you create multiple `ReaderHandle`s from a single `ReadySetHandle`, they may
/// share connections to the Readyset workers.
#[derive(Clone)]
pub struct ReaderHandle {
    name: Relation,
    node: NodeIndex,
    columns: Arc<[SqlIdentifier]>,
    schema: Option<ViewSchema>,
    /// (view_placeholder, key_column_index) pairs according to their mapping. Contains exactly
    /// one entry for each key column at the reader.
    key_mapping: Vec<(ViewPlaceholder, KeyColumnIdx)>,
    shard: ViewRpc,
}

impl fmt::Debug for ReaderHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReaderHandle")
            .field("node", &self.node)
            .field("columns", &self.columns)
            .finish_non_exhaustive()
    }
}

/// A [`ReaderHandle`] to a cached query that is different than the query being executed.
///
/// A query can reuse a [`ReaderHandle`] for a cached query if the cached query is equivalent for
/// some set of parameter values passed at execution.
#[derive(Debug, Clone)]
pub struct ReusedReaderHandle {
    /// The [`ReaderHandle`] that reuses the reader of another query for this query.
    reader_handle: ReaderHandle,
    /// Remapping from [`PlaceholderIdx`]s in the executed query to inlined [`Literal`]s in the
    /// cached query.
    ///
    /// This view can only be used if the values in this map match the values passed on execution
    /// for the given placeholders.
    required_values: HashMap<PlaceholderIdx, Literal>,
    // Remapping from [`PlaceholderIdx`]s in the cached query to [`Literal`]s in the executed
    // query. This remapping is applied to [`key_mapping`] when building keys.
    key_remapping: HashMap<PlaceholderIdx, Literal>,
}

/// A wrapper to hold either a [`ReaderHandle`] or collection of [`ReusedReaderHandle`]s.
#[derive(Debug, Clone)]
pub enum View {
    /// A [`ReaderHandle`] for a cached query.
    Single(ReaderHandle),
    /// A collection of [`ReusedReaderHandle`]s that may satisfy a query for a given set of
    /// placeholders passed at execution.
    ///
    /// There may be multiple handles because a single query may map to multiple caches for
    /// different inlined values (e.g., `x = ?` maps to `x = 1` and `x = 2`)
    MultipleReused(Vec1<ReusedReaderHandle>),
}

/// A read query to be run against a [`View`].
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ViewQuery {
    /// Key comparisons to read with.
    pub key_comparisons: Vec<KeyComparison>,
    /// Expression to use to filter values after they're returned from the underlying reader.
    ///
    /// This expression will be evaluated on each of the rows returned from the reader, and any
    /// rows for which it evaluates to a non-[truthy][] value will be omitted from the result set.
    ///
    /// [truthy]: DfValue::is_truthy
    pub filter: Option<DfExpr>,
    /// An optional limit to the number of values to return
    pub limit: Option<usize>,
    /// An optional offset to skip the given number of rows from the beginning of the result set
    pub offset: Option<usize>,
}

impl From<Vec<KeyComparison>> for ViewQuery {
    fn from(key_comparisons: Vec<KeyComparison>) -> Self {
        Self {
            key_comparisons,
            filter: None,
            limit: None,
            offset: None,
        }
    }
}

impl Service<ViewQuery> for ReaderHandle {
    type Response = LookupResult<Results>;
    type Error = ReadySetError;

    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let ni = self.node;
        ready!(self.shard.poll_ready(cx))
            .map_err(rpc_err!("<View as Service<ViewQuery>>::poll_ready"))
            .map_err(|e| view_err(ni, e))?;
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, query: ViewQuery) -> Self::Future {
        let ni = self.node;
        let span = child_span!(
            INFO,
            "view-request",
            key_comparisons = ?Sensitive(&query.key_comparisons),
            node = self.node.index()
        );

        let request = span.in_scope(|| {
            Instrumented::from(Tagged::from(ReadQuery::Normal {
                target: ReaderAddress {
                    node: self.node,
                    name: self.name.clone(),
                },
                query,
            }))
        });

        trace!("submit request");

        Box::pin(
            self.shard
                .call(request)
                .map_err(rpc_err!("<View as Service<ViewQuery>>::call"))
                .and_then(move |reply| {
                    let future = async move {
                        reply
                            .v
                            .into_normal()
                            .ok_or_else(|| {
                                internal_err!("Unexpected response type from reader service")
                            })?
                            .map(|l| {
                                l.map_results(|rows, stats| {
                                    Results::with_stats(rows.into(), stats.clone())
                                })
                            })
                    };
                    instrument_if_enabled(future, span)
                })
                .map_err(move |e| view_err(ni, e)),
        )
    }
}

#[allow(clippy::len_without_is_empty)]
impl ReaderHandle {
    /// Get the list of columns in this view.
    pub fn columns(&self) -> &[SqlIdentifier] {
        &self.columns
    }

    /// Get a slice of the columns.
    pub fn column_slice(&self) -> Arc<[SqlIdentifier]> {
        Arc::clone(&self.columns)
    }

    /// Get the schema definition of this view.
    pub fn schema(&self) -> Option<&ViewSchema> {
        self.schema.as_ref()
    }

    /// Get the NodeIndex of the dataflow node that this
    /// view refers to.
    pub fn node(&self) -> &NodeIndex {
        &self.node
    }

    /// Name associated with the reader associated with the view.
    pub fn name(&self) -> &Relation {
        &self.name
    }

    /// Get the current size of this view.
    ///
    /// Note that you must also continue to poll this `View` for the returned future to resolve.
    pub async fn len(&mut self) -> ReadySetResult<usize> {
        future::poll_fn(|cx| self.poll_ready(cx)).await?;

        let reply = self
            .shard
            .call(Instrumented::from(Tagged::from(ReadQuery::Size {
                target: ReaderAddress {
                    node: self.node,
                    name: self.name.clone(),
                },
            })))
            .await
            .map_err(rpc_err!("View::len"))?;

        if let ReadReply::Size(rows) = reply.v {
            Ok(rows)
        } else {
            unreachable!()
        }
    }

    /// Get the placeholder to key column index mapping for the reader node
    /// Each pair represents a mapping from placeholder index to reader key column index
    pub fn key_map(&self) -> &[(ViewPlaceholder, KeyColumnIdx)] {
        &self.key_mapping
    }

    /// Get the current keys of this view. For debugging only.
    pub async fn keys(&mut self) -> ReadySetResult<Vec<Vec<DfValue>>> {
        future::poll_fn(|cx| self.poll_ready(cx)).await?;

        let reply = self
            .shard
            .call(Instrumented::from(Tagged::from(ReadQuery::Keys {
                target: ReaderAddress {
                    node: self.node,
                    name: self.name.clone(),
                },
            })))
            .await
            .map_err(rpc_err!("View::keys"))?;

        if let ReadReply::Keys(keys) = reply.v {
            Ok(keys)
        } else {
            unreachable!()
        }
    }

    /// Issue a raw `ViewQuery` against this view, and return the results.
    ///
    /// Blocks until the reader can satisfy all requested keys or the server-side upquery timeout
    /// fires (in which case the call returns [`ReadySetError::UpqueryTimeout`]).
    pub async fn raw_lookup(&mut self, query: ViewQuery) -> ReadySetResult<ResultIterator> {
        future::poll_fn(|cx| self.poll_ready(cx)).await?;
        Ok(ResultIterator::owned(self.call(query).await?.results))
    }

    /// Retrieve the query results for the given parameter value.
    pub async fn lookup(&mut self, key: &[DfValue]) -> ReadySetResult<ResultIterator> {
        let key = Vec1::try_from_vec(key.into())
            .map_err(|_| view_err(self.node, ReadySetError::EmptyKey))?;
        self.multi_lookup(vec![KeyComparison::Equal(key)]).await
    }

    /// Retrieve the query results for the given parameter values.
    pub async fn multi_lookup(
        &mut self,
        key_comparisons: Vec<KeyComparison>,
    ) -> ReadySetResult<ResultIterator> {
        self.raw_lookup(key_comparisons.into()).await
    }

    /// Build a [`ViewQuery`] for performing a lookup against this [`ReaderHandle`]
    fn build_view_query(
        &self,
        key_remap: Option<&HashMap<PlaceholderIdx, Literal>>,
        raw_keys: Vec<Cow<'_, [DfValue]>>,
        limit: Option<usize>,
        offset: Option<usize>,
        dialect: Dialect,
    ) -> ReadySetResult<ViewQuery> {
        trace!("select::lookup");

        let (keys, filters) = if raw_keys.is_empty() {
            let bogo = vec![vec1![DfValue::from(0i32)].into()];
            (bogo, Vec::new())
        } else {
            let mut key_comparison_builder = KeyComparisonBuilder::new(self, key_remap, dialect)?;

            let keys = raw_keys
                .into_iter()
                .filter_map(|key| key_comparison_builder.build_key(key).transpose())
                .collect::<ReadySetResult<Vec<_>>>()?;

            (keys, key_comparison_builder.filters)
        };

        trace!(?keys, ?filters, "Built view query");

        Ok(ViewQuery {
            key_comparisons: keys,
            filter: filters.into_iter().reduce(|expr1, expr2| DfExpr::Op {
                left: Box::new(expr1),
                op: DfBinaryOperator::And,
                right: Box::new(expr2),
                ty: DfType::Bool, // AND is a boolean operator
            }),
            limit,
            offset,
        })
    }
}

struct KeyComparisonBuilder<'a> {
    mixed_binops: bool,
    filters: Vec<DfExpr>,
    key_map: &'a [(ViewPlaceholder, KeyColumnIdx)],
    key_types: HashMap<usize, &'a DfType>,
    key_remap: Option<&'a HashMap<PlaceholderIdx, Literal>>,
    binop_to_use: BinaryOperator,
    dialect: Dialect,
}

impl<'a> KeyComparisonBuilder<'a> {
    fn new(
        reader_handle: &'a ReaderHandle,
        key_remap: Option<&'a HashMap<PlaceholderIdx, Literal>>,
        dialect: Dialect,
    ) -> ReadySetResult<KeyComparisonBuilder<'a>> {
        let schema = reader_handle
            .schema()
            .ok_or_else(|| internal_err!("No schema for view"))?;

        let key_types = schema.col_types(
            reader_handle
                .key_map()
                .iter()
                .map(|(_, key_column_idx)| *key_column_idx),
            SchemaType::ProjectedSchema,
        )?;

        let mut current_binop = None;
        // Whether we have multiple different binary operators in our key comparisons
        let mixed_binops =
            !reader_handle
                .key_map()
                .iter()
                .all(|(placeholder, _)| match placeholder {
                    // Mixed binops if we see any two different binops in OneToOne placeholders
                    ViewPlaceholder::OneToOne(_, binop) => {
                        current_binop.get_or_insert(*binop) == binop
                    }
                    // Between uses mixed binops
                    ViewPlaceholder::Between(_, _) => false,
                    // Generated and PageNumber placeholders can be used
                    ViewPlaceholder::Generated | ViewPlaceholder::PageNumber { .. } => true,
                });
        // The binary operator we will use to build our key if we do not have a mixed comparison
        let binop_to_use = current_binop.unwrap_or(BinaryOperator::Equal);

        let key_types: HashMap<usize, &DfType> = reader_handle
            .key_map()
            .iter()
            .zip(key_types)
            .map(|((_, key_column_idx), key_type)| (*key_column_idx, key_type))
            .collect();

        Ok(Self {
            key_remap,
            dialect,
            mixed_binops,
            binop_to_use,
            key_types,
            key_map: reader_handle.key_map(),
            filters: Vec::new(),
        })
    }

    // parameter numbering is 1-based, but vecs are 0-based, so subtract 1
    // also, no from_ty since the key value is a literal
    fn remap_key(
        &self,
        key: &[DfValue],
        idx: &PlaceholderIdx,
        key_type: &DfType,
    ) -> ReadySetResult<DfValue> {
        let key = match self.key_remap {
            Some(remap) => {
                match remap.get(idx).ok_or_else(|| {
                    internal_err!("Key remapping for ReusedReaderHandle is missing indices")
                })? {
                    Literal::Placeholder(ItemPlaceholder::DollarNumber(idx)) => {
                        key.get(*idx as usize - 1).ok_or_else(|| {
                            internal_err!(
                                "Key remapping for ReusedReaderHandle contains erroneous index"
                            )
                        })?
                    }
                    Literal::Placeholder(_) => {
                        internal!(
                            "Key remapping for ReusedReaderHandle contains non-numbered placeholder"
                        )
                    }
                    literal => &DfValue::try_from_dialect(literal, self.dialect.into())?,
                }
            }
            None => key.get(*idx - 1).ok_or_else(|| {
                internal_err!(
                    "Key remapping for ReusedReaderHandle tries to access non-existent index"
                )
            })?,
        };
        key.coerce_for_comparison(key_type)
    }

    fn build_key(&mut self, raw_key: Cow<'_, [DfValue]>) -> ReadySetResult<Option<KeyComparison>> {
        let mut k = vec![];
        let mut bounds: Option<(Vec<DfValue>, Vec<DfValue>)> = if self.mixed_binops {
            Some((vec![], vec![]))
        } else {
            None
        };

        // All ViewPlaceholder indices must be remapped using key_remap
        for (view_placeholder, key_column_idx) in self.key_map {
            match view_placeholder {
                ViewPlaceholder::Generated => continue,
                ViewPlaceholder::OneToOne(idx, binop) => {
                    let key_type = *self
                        .key_types
                        .get(key_column_idx)
                        .ok_or_else(|| internal_err!("No key_type for key"))?;

                    let value = self.remap_key(raw_key.as_ref(), idx, key_type)?;

                    // Skip the key entirely if the value in the key is NULL, since we don't want
                    // to return any results for comparisons against NULL values
                    if value.is_none() {
                        return Ok(None);
                    }

                    let make_op = |(op, negated): (DfBinaryOperator, bool)| {
                        let op = DfExpr::Op {
                            left: Box::new(DfExpr::Column {
                                index: *key_column_idx,
                                ty: key_type.clone(),
                            }),
                            op,
                            right: Box::new(DfExpr::Literal {
                                val: value.clone(),
                                ty: key_type.clone(),
                            }),
                            ty: DfType::Bool, // TODO: infer type
                        };
                        if negated {
                            DfExpr::Not {
                                expr: Box::new(op),
                                ty: DfType::Bool,
                            }
                        } else {
                            op
                        }
                    };

                    if let Some((lower_bound, upper_bound)) = &mut bounds {
                        match binop {
                            BinaryOperator::Equal => {
                                lower_bound.push(value.clone());
                                upper_bound.push(value);
                            }
                            BinaryOperator::GreaterOrEqual => {
                                self.filters
                                    .push(make_op((DfBinaryOperator::GreaterOrEqual, false)));
                                lower_bound.push(value);
                                upper_bound.push(DfValue::Max);
                            }
                            BinaryOperator::LessOrEqual => {
                                self.filters
                                    .push(make_op((DfBinaryOperator::LessOrEqual, false)));
                                lower_bound.push(DfValue::None); // NULL is the minimum DfValue
                                upper_bound.push(value);
                            }
                            BinaryOperator::Greater => {
                                self.filters
                                    .push(make_op((DfBinaryOperator::Greater, false)));
                                lower_bound.push(value);
                                upper_bound.push(DfValue::Max);
                            }
                            BinaryOperator::Less => {
                                self.filters.push(make_op((DfBinaryOperator::Less, false)));
                                lower_bound.push(DfValue::None); // NULL is the minimum DfValue
                                upper_bound.push(value);
                            }
                            op => unsupported!("Unsupported binary operator in query: `{}`", op),
                        }
                    } else {
                        // We need to additionally filter post-lookup for certain
                        // compound ranges, since we
                        // always sort keys lexicographically within the
                        // reader map. This is the case for...
                        if (
                            // All keys within open (exclusive) ranges (consider eg:
                            //     (1, 2) > (1, 1)
                            //     even though
                            //     NOT (1 > 1 && 2 > 1)
                            // )
                            matches!(
                                        self.binop_to_use,
                                        BinaryOperator::Less | BinaryOperator::Greater
                                    )
                                        // As long as the range is actually compound
                                        && self.key_map.len() > 1
                        ) || (
                            // Or all other range keys beyond the *first* key within a
                            // compound range
                            self.binop_to_use != BinaryOperator::Equal && !k.is_empty()
                        ) {
                            self.filters.push(make_op(DfBinaryOperator::from_sql_op(
                                self.binop_to_use,
                                self.dialect,
                                key_type,
                                key_type,
                            )?));
                        }
                        k.push(value);
                    }
                }
                ViewPlaceholder::Between(lower_idx, upper_idx) => {
                    let key_type = self.key_types[key_column_idx];

                    let lower_value = self.remap_key(raw_key.as_ref(), lower_idx, key_type)?;
                    let upper_value = self.remap_key(raw_key.as_ref(), upper_idx, key_type)?;

                    // Skip the key entirely if the value in the key is NULL, since we don't want
                    // to return any results for comparisons against NULL values
                    if lower_value.is_none() || upper_value.is_none() {
                        return Ok(None);
                    }

                    let (lower_key, upper_key) = bounds.get_or_insert_with(Default::default);
                    lower_key.push(lower_value);
                    upper_key.push(upper_value);
                }
                ViewPlaceholder::PageNumber {
                    offset_placeholder,
                    limit,
                } => {
                    // offset parameters should always be a BigInt
                    let offset: u64 = self
                        .remap_key(raw_key.as_ref(), offset_placeholder, &DfType::BigInt)?
                        .try_into()?;
                    if !offset.is_multiple_of(*limit) {
                        unsupported!("OFFSET must currently be an integer multiple of LIMIT");
                    }
                    let page_number = offset / *limit;
                    k.push(page_number.into());
                }
            };
        }

        if let Some((lower, upper)) = bounds {
            debug_assert!(k.is_empty());
            Ok(Some(KeyComparison::Range((
                Bound::Included(lower.try_into()?),
                Bound::Included(upper.try_into()?),
            ))))
        } else {
            KeyComparison::from_key_and_operator(k, self.binop_to_use).map(Some)
        }
    }
}

impl ReusedReaderHandle {
    /// Get a reference to the reused [`ReaderHandle`].
    pub fn inner(&self) -> &ReaderHandle {
        &self.reader_handle
    }

    /// Get a mut reference to the reused [`ReaderHandle`].
    pub fn inner_mut(&mut self) -> &mut ReaderHandle {
        &mut self.reader_handle
    }

    /// Get the remapping of [`PlaceholderIdx`] in the cached query to [`Literal`]s in a query that
    /// is reusing this ReaderHandle.
    pub fn key_remapping(&self) -> &HashMap<PlaceholderIdx, Literal> {
        &self.key_remapping
    }

    /// Get the [`PlaceholderIdx`]s that correspond to inlined values in the cache. The values
    /// passed to these placeholders on execution must match the [`Literal`] values in this map.
    pub fn required_values(&self) -> &HashMap<PlaceholderIdx, Literal> {
        &self.required_values
    }

    /// Build a view query
    fn build_view_query(
        &self,
        raw_keys: Vec<Cow<'_, [DfValue]>>,
        limit: Option<usize>,
        offset: Option<usize>,
        dialect: Dialect,
    ) -> ReadySetResult<Option<ViewQuery>> {
        // If any placeholders in our query correspond to inlined values in the migrated query,
        // verify that we are executing our query with these values.
        for params in raw_keys.iter() {
            for (idx, val) in &self.required_values {
                // Placeholders are 1-indexed, but params are 0-indexed
                let client_val = params.get(idx - 1).ok_or_else(|| {
                    internal_err!(
                        "Received fewer parameters than expected. Error indexing at position {}",
                        idx - 1
                    )
                })?;
                // Return None if keys do not satisfy the required values
                if DfValue::try_from_dialect(val, dialect.into())? != *client_val {
                    return Ok(None);
                }
            }
        }

        let raw_keys = if !raw_keys.is_empty() && self.required_values.len() == raw_keys[0].len() {
            vec![]
        } else {
            raw_keys
        };

        self.reader_handle
            .build_view_query(Some(&self.key_remapping), raw_keys, limit, offset, dialect)
            .map(Some)
    }
}

impl View {
    /// Builds a [`ViewQuery`] that can be run against a [`ReaderHandle`]. If self is
    /// [`View::MultipleReused`], then we will find a [`ReaderHandle`] for the first Reader that can
    /// satisfy the given keys or return None. Also returns a reference to the [`ReaderHandle`] to
    /// indicate which handle the [`ViewQuery`] corresponds to.
    pub fn build_view_query(
        &mut self,
        raw_keys: Vec<Cow<'_, [DfValue]>>,
        limit: Option<usize>,
        offset: Option<usize>,
        dialect: Dialect,
    ) -> ReadySetResult<Option<(&mut ReaderHandle, ViewQuery)>> {
        // If any placeholders in our query correspond to inlined values in the migrated query,
        // verify that we are executing our query with these values.
        match self {
            View::Single(handle) => handle
                .build_view_query(None, raw_keys, limit, offset, dialect)
                .map(|vq| Some((handle, vq))),
            View::MultipleReused(handles) => {
                let mut last_error = None;
                for reused_handle in handles {
                    match reused_handle.build_view_query(raw_keys.clone(), limit, offset, dialect) {
                        Ok(Some(vq)) => {
                            return Ok(Some((reused_handle.inner_mut(), vq)));
                        }
                        Err(err) => {
                            last_error = Some(err);
                        }
                        Ok(None) => {
                            /* Parameters did not satisfy inlined view so try another. */
                        }
                    }
                }

                if let Some(err) = last_error {
                    Err(err)
                } else {
                    Ok(None) // Parameters did not satisfy any inlined view
                }
            }
        }
    }

    /// Returns a single ReaderHandle if Self is [`View::Single`]
    pub fn into_reader_handle(self) -> Option<ReaderHandle> {
        match self {
            View::Single(rh) => Some(rh),
            View::MultipleReused(_) => None,
        }
    }

    /// Returns a mut reference to a single ReaderHandle if Self is [`View::Single`]
    pub fn as_mut_reader_handle(&mut self) -> Option<&mut ReaderHandle> {
        match self {
            View::Single(rh) => Some(rh),
            View::MultipleReused(_) => None,
        }
    }
}

#[derive(Debug, Default)]
#[repr(transparent)]
pub struct ReadReplyBatch(pub Vec<Vec<DfValue>>);

use serde::de::{self, DeserializeSeed, Deserializer, Visitor};
impl<'de> Deserialize<'de> for ReadReplyBatch {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct Elem;

        impl Visitor<'_> for Elem {
            type Value = Vec<Vec<DfValue>>;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("Vec<Vec<DfValue>>")
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
            type Value = Vec<Vec<DfValue>>;

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

impl From<ReadReplyBatch> for Vec<Vec<DfValue>> {
    fn from(val: ReadReplyBatch) -> Vec<Vec<DfValue>> {
        val.0
    }
}

impl From<Vec<Vec<DfValue>>> for ReadReplyBatch {
    fn from(v: Vec<Vec<DfValue>>) -> Self {
        Self(v)
    }
}

impl IntoIterator for ReadReplyBatch {
    type Item = Vec<DfValue>;
    type IntoIter = std::vec::IntoIter<Self::Item>;
    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl Extend<Vec<DfValue>> for ReadReplyBatch {
    fn extend<T>(&mut self, iter: T)
    where
        T: IntoIterator<Item = Vec<DfValue>>,
    {
        self.0.extend(iter)
    }
}

impl AsRef<[Vec<DfValue>]> for ReadReplyBatch {
    fn as_ref(&self) -> &[Vec<DfValue>] {
        &self.0[..]
    }
}

impl std::ops::Deref for ReadReplyBatch {
    type Target = Vec<Vec<DfValue>>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for ReadReplyBatch {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[allow(clippy::eq_op)]
    mod key_comparison {
        use readyset_util::eq_laws;

        use super::*;

        eq_laws!(KeyComparison);
    }

    mod replay_keys {
        use readyset_util::eq_laws;
        use test_strategy::proptest;
        use vec1::vec1;

        use super::*;

        eq_laws!(ReplayKeys);

        fn equal(k: i32) -> KeyComparison {
            KeyComparison::Equal(vec1![k.into()])
        }

        fn degenerate_range(k: i32) -> KeyComparison {
            KeyComparison::Range((
                Bound::Included(vec1![k.into()]),
                Bound::Included(vec1![k.into()]),
            ))
        }

        fn open_range(lo: i32, hi: i32) -> KeyComparison {
            KeyComparison::Range((
                Bound::Included(vec1![lo.into()]),
                Bound::Excluded(vec1![hi.into()]),
            ))
        }

        fn empty_excluded(k: i32) -> KeyComparison {
            KeyComparison::Range((
                Bound::Excluded(vec1![k.into()]),
                Bound::Excluded(vec1![k.into()]),
            ))
        }

        /// Variants are preserved: a degenerate range stays in the range partition rather
        /// than collapsing into Equal. Downstream consumers select the lookup path (point
        /// vs. range) based on the variant, and silently re-routing a range key onto the
        /// point-lookup path can panic when the source node only has a BTree index.
        #[test]
        fn degenerate_range_stays_in_ranges() {
            let mut rk = ReplayKeys::new();
            assert!(rk.insert(degenerate_range(7)));
            assert_eq!(rk.equals().count(), 0);
            assert_eq!(rk.ranges().count(), 1);
            assert!(!rk.contains(&equal(7)));
            assert!(rk.contains(&degenerate_range(7)));
        }

        #[test]
        fn empty_excluded_range_stays_in_ranges() {
            let mut rk = ReplayKeys::new();
            assert!(rk.insert(empty_excluded(5)));
            assert_eq!(rk.equals().count(), 0);
            assert_eq!(rk.ranges().count(), 1);
        }

        #[test]
        fn from_iter_separates_equal_and_degenerate_range() {
            let rk: ReplayKeys = [equal(3), degenerate_range(3), equal(3)]
                .into_iter()
                .collect();
            assert_eq!(rk.equals().count(), 1);
            assert_eq!(rk.ranges().count(), 1);
            assert_eq!(rk.len(), 2);
        }

        #[test]
        fn from_iter_dedupes_repeated_ranges() {
            let rk: ReplayKeys = [open_range(1, 5), open_range(1, 5)].into_iter().collect();
            assert_eq!(rk.len(), 1);
            assert_eq!(rk.ranges().count(), 1);
        }

        #[test]
        fn remove_is_variant_specific() {
            let mut rk = ReplayKeys::new();
            rk.insert(equal(9));
            // Removing the matching Range variant must NOT touch the Equal partition.
            assert!(!rk.remove(&degenerate_range(9)));
            assert_eq!(rk.equals().count(), 1);
            assert!(rk.remove(&equal(9)));
            assert!(rk.is_empty());

            rk.insert(degenerate_range(9));
            assert!(!rk.remove(&equal(9)));
            assert_eq!(rk.ranges().count(), 1);
            assert!(rk.remove(&degenerate_range(9)));
            assert!(rk.is_empty());
        }

        /// Headline property of this commit: every Equal key flows to the equals partition;
        /// `ranges()` is empty so `MissBuilder::build` does no per-key work.
        #[test]
        fn equals_only_input_yields_empty_ranges() {
            let rk: ReplayKeys = (0..1000).map(equal).collect();
            assert_eq!(rk.len(), 1000);
            assert_eq!(rk.ranges().count(), 0);
        }

        #[test]
        fn retain_sees_equal_and_range_variants() {
            let mut rk = ReplayKeys::new();
            rk.insert(equal(1));
            rk.insert(open_range(10, 20));
            let mut saw_equal = false;
            let mut saw_range = false;
            rk.retain(|k| {
                match k {
                    KeyComparisonRef::Equal(_) => saw_equal = true,
                    KeyComparisonRef::Range(_) => saw_range = true,
                }
                true
            });
            assert!(saw_equal && saw_range);
            assert_eq!(rk.len(), 2);
        }

        /// Order-independent equality: two `ReplayKeys` built from the same set of inputs in
        /// different orders must compare equal. Guards against a future refactor that puts
        /// ranges back in a `Vec`.
        #[test]
        fn equality_is_order_independent_on_ranges() {
            let a: ReplayKeys = [open_range(1, 2), open_range(3, 4)].into_iter().collect();
            let b: ReplayKeys = [open_range(3, 4), open_range(1, 2)].into_iter().collect();
            assert_eq!(a, b);
        }

        /// Structural partition: every input `KeyComparison::Equal(k)` ends up in the Equal
        /// partition; every input `KeyComparison::Range(_)` ends up in the Range partition,
        /// degenerate or not. Catches a hypothetical bug that would mis-route variants.
        #[proptest]
        fn partition_preserves_variant(input: Vec<KeyComparison>) {
            let expected_equals: HashSet<Vec1<DfValue>> = input
                .iter()
                .filter_map(|k| match k {
                    KeyComparison::Equal(v) => Some(v.clone()),
                    KeyComparison::Range(_) => None,
                })
                .collect();
            let expected_ranges: HashSet<BoundedRange<Vec1<DfValue>>> = input
                .iter()
                .filter_map(|k| match k {
                    KeyComparison::Range(r) => Some(r.clone()),
                    KeyComparison::Equal(_) => None,
                })
                .collect();

            let rk: ReplayKeys = input.into_iter().collect();
            let actual_equals: HashSet<Vec1<DfValue>> = rk.equals().cloned().collect();
            let actual_ranges: HashSet<BoundedRange<Vec1<DfValue>>> =
                rk.ranges().cloned().collect();
            assert_eq!(expected_equals, actual_equals);
            assert_eq!(expected_ranges, actual_ranges);
        }

        #[proptest]
        fn iter_round_trip(rk: ReplayKeys) {
            let collected: ReplayKeys = rk.iter_owned().collect();
            assert_eq!(rk, collected);
        }

        #[proptest]
        fn into_iter_round_trip(rk: ReplayKeys) {
            let collected: ReplayKeys = rk.clone().into_iter().collect();
            assert_eq!(rk, collected);
        }

        #[proptest]
        fn bincode_round_trip(rk: ReplayKeys) {
            let bytes = bincode::serialize(&rk).unwrap();
            let decoded: ReplayKeys = bincode::deserialize(&bytes).unwrap();
            assert_eq!(rk, decoded);
        }

        /// `KeyComparison::contains` now delegates through `as_ref()`, but keep this in
        /// place: it locks the owned and borrowed paths together so a future refactor
        /// can't silently let them desync.
        #[proptest]
        fn contains_matches_owned(k: KeyComparison, probe: Vec<DfValue>) {
            assert_eq!(k.contains(&probe), k.as_ref().contains(&probe));
        }
    }

    mod build_view_query {
        use std::net::{IpAddr, Ipv4Addr};

        use dataflow_expression::Dialect as DfDialect;
        use readyset_sql::{ast::Column, Dialect};
        use vec1::vec1;

        use super::*;

        fn make_build_query(
            raw_keys: Vec<Cow<'_, [DfValue]>>,
            limit: Option<usize>,
            offset: Option<usize>,
            key_map: &[(ViewPlaceholder, KeyColumnIdx)],
            dialect: Dialect,
        ) -> ViewQuery {
            let schema = ViewSchema::new(
                vec![
                    ColumnSchema {
                        column: Column {
                            name: "x".into(),
                            table: Some("t".into()),
                        },
                        column_type: DfType::Int,
                        base: Some(ColumnBase {
                            table: "t".into(),
                            column: "x".into(),
                            constraints: vec![],
                            table_oid: None,
                            attnum: None,
                            sql_type: SqlType::Int(None),
                        }),
                    },
                    ColumnSchema {
                        column: Column {
                            name: "y".into(),
                            table: Some("t".into()),
                        },
                        column_type: DfType::DEFAULT_TEXT,
                        base: Some(ColumnBase {
                            table: "t".into(),
                            column: "y".into(),
                            constraints: vec![],
                            table_oid: None,
                            attnum: None,
                            sql_type: SqlType::Text,
                        }),
                    },
                ],
                vec![
                    ColumnSchema {
                        column: Column {
                            name: "x".into(),
                            table: Some("t".into()),
                        },
                        column_type: DfType::Int,
                        base: Some(ColumnBase {
                            table: "t".into(),
                            column: "x".into(),
                            constraints: vec![],
                            table_oid: None,
                            attnum: None,
                            sql_type: SqlType::Int(None),
                        }),
                    },
                    ColumnSchema {
                        column: Column {
                            name: "y".into(),
                            table: Some("t".into()),
                        },
                        column_type: DfType::DEFAULT_TEXT,
                        base: Some(ColumnBase {
                            table: "t".into(),
                            column: "y".into(),
                            constraints: vec![],
                            table_oid: None,
                            attnum: None,
                            sql_type: SqlType::Text,
                        }),
                    },
                ],
            );
            // Create a fake shard - will not be used for test. Worker is not spawned
            let (c, _) = Buffer::pair(
                BoxService::new(Timeout::new(
                    ConcurrencyLimit::new(
                        Balance::new(make_views_discover(
                            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 0),
                            Duration::new(0, 10),
                        )),
                        crate::PENDING_LIMIT,
                    ),
                    Duration::new(1, 0),
                )),
                crate::BUFFER_TO_POOL,
            );
            // Only the schema and key_mapping are used to build a ViewQuery
            let reader_handle = ReaderHandle {
                name: Relation::from("test"), // Not used for test
                node: NodeIndex::new(0),      // Not used for test
                columns: Arc::new([]),        // Not used for test
                schema: Some(schema),
                key_mapping: key_map.to_vec(),
                shard: c, // Not used for test
            };
            let dataflow_dialect = match dialect {
                Dialect::MySQL => DfDialect::DEFAULT_MYSQL,
                Dialect::PostgreSQL => DfDialect::DEFAULT_POSTGRESQL,
            };
            let mut view = View::Single(reader_handle);
            view.build_view_query(raw_keys, limit, offset, dataflow_dialect)
                .unwrap()
                .unwrap()
                .1
        }

        #[test]
        fn simple_point_lookup() {
            // "SELECT t.x FROM t WHERE t.x = $1"
            let query = make_build_query(
                vec![Cow::Owned(vec![DfValue::from(1)])],
                None,
                None,
                &[(ViewPlaceholder::OneToOne(1, BinaryOperator::Equal), 0)],
                Dialect::MySQL,
            );

            assert!(query.filter.is_none());
            assert_eq!(
                query.key_comparisons,
                vec![KeyComparison::from(vec1![DfValue::from(1)])]
            );
        }

        #[test]
        fn single_between() {
            // "SELECT t.x FROM t WHERE t.x BETWEEN $1 AND $2"
            let query = make_build_query(
                vec![Cow::Owned(vec![DfValue::from(1), DfValue::from(2)])],
                None,
                None,
                &[(ViewPlaceholder::Between(1, 2), 0)],
                Dialect::MySQL,
            );

            assert!(query.filter.is_none());
            assert_eq!(
                query.key_comparisons,
                vec![KeyComparison::from_range(
                    &(vec1![DfValue::from(1)]..=vec1![DfValue::from(2)])
                )]
            );
        }

        #[test]
        fn mixed_equal_and_inclusive() {
            // "SELECT t.x FROM t WHERE t.x >= $1 AND t.y = $2"
            let query = make_build_query(
                vec![Cow::Owned(vec![DfValue::from(1), DfValue::from("a")])],
                None,
                None,
                &[
                    (ViewPlaceholder::OneToOne(2, BinaryOperator::Equal), 1),
                    (
                        ViewPlaceholder::OneToOne(1, BinaryOperator::GreaterOrEqual),
                        0,
                    ),
                ],
                Dialect::MySQL,
            );

            assert_eq!(
                query.key_comparisons,
                vec![KeyComparison::from_range(
                    &(vec1![DfValue::from("a"), DfValue::from(1)]
                        ..=vec1![DfValue::from("a"), DfValue::Max])
                )]
            );
        }

        #[test]
        fn mixed_equal_and_between() {
            // "SELECT t.x FROM t WHERE t.x BETWEEN $1 AND $2 AND t.y = $3"
            let query = make_build_query(
                vec![Cow::Owned(vec![
                    DfValue::from(1),
                    DfValue::from(2),
                    DfValue::from("a"),
                ])],
                None,
                None,
                &[
                    (ViewPlaceholder::OneToOne(3, BinaryOperator::Equal), 1),
                    (ViewPlaceholder::Between(1, 2), 0),
                ],
                Dialect::MySQL,
            );

            assert!(query.filter.is_none());
            assert_eq!(
                query.key_comparisons,
                vec![KeyComparison::from_range(
                    &(vec1![DfValue::from("a"), DfValue::from(1)]
                        ..=vec1![DfValue::from("a"), DfValue::from(2)])
                )]
            );
        }

        #[test]
        fn mixed_equal_and_exclusive() {
            // "SELECT t.x FROM t WHERE t.x > $1 AND t.y = $2"
            let query = make_build_query(
                vec![Cow::Owned(vec![DfValue::from(1), DfValue::from("a")])],
                None,
                None,
                &[
                    (ViewPlaceholder::OneToOne(2, BinaryOperator::Equal), 1),
                    (ViewPlaceholder::OneToOne(1, BinaryOperator::Greater), 0),
                ],
                Dialect::MySQL,
            );

            assert_eq!(
                query.filter,
                Some(DfExpr::Op {
                    left: Box::new(DfExpr::Column {
                        index: 0,
                        ty: DfType::Int
                    }),
                    op: DfBinaryOperator::Greater,
                    right: Box::new(DfExpr::Literal {
                        val: 1.into(),
                        ty: DfType::Int
                    }),
                    ty: DfType::Bool,
                })
            );
            assert_eq!(
                query.key_comparisons,
                vec![KeyComparison::from_range(
                    &(vec1![DfValue::from("a"), DfValue::from(1)]
                        ..=vec1![DfValue::from("a"), DfValue::Max])
                )]
            );
        }

        #[test]
        fn compound_range_open() {
            // "SELECT t.x FROM t WHERE t.x > $1 AND t.y > $2"
            let query = make_build_query(
                vec![Cow::Owned(vec![DfValue::from(1), DfValue::from("a")])],
                None,
                None,
                &[
                    (ViewPlaceholder::OneToOne(1, BinaryOperator::Greater), 0),
                    (ViewPlaceholder::OneToOne(2, BinaryOperator::Greater), 1),
                ],
                Dialect::MySQL,
            );

            assert_eq!(
                query.filter,
                Some(DfExpr::Op {
                    left: Box::new(DfExpr::Op {
                        left: Box::new(DfExpr::Column {
                            index: 0,
                            ty: DfType::Int
                        }),
                        op: DfBinaryOperator::Greater,
                        right: Box::new(DfExpr::Literal {
                            val: 1.into(),
                            ty: DfType::Int
                        }),
                        ty: DfType::Bool
                    }),
                    op: DfBinaryOperator::And,
                    right: Box::new(DfExpr::Op {
                        left: Box::new(DfExpr::Column {
                            index: 1,
                            ty: DfType::DEFAULT_TEXT
                        }),
                        op: DfBinaryOperator::Greater,
                        right: Box::new(DfExpr::Literal {
                            val: "a".into(),
                            ty: DfType::DEFAULT_TEXT
                        }),
                        ty: DfType::Bool
                    }),
                    ty: DfType::Bool
                })
            );

            assert_eq!(
                query.key_comparisons,
                vec![KeyComparison::Range(
                    vec1![DfValue::from(1), DfValue::from("a")].range_from()
                )]
            );
        }

        #[test]
        fn compound_range_closed() {
            // "SELECT t.x FROM t WHERE t.x >= $1 AND t.y >= $2"
            let query = make_build_query(
                vec![Cow::Owned(vec![DfValue::from(1), DfValue::from("a")])],
                None,
                None,
                &[
                    (
                        ViewPlaceholder::OneToOne(1, BinaryOperator::GreaterOrEqual),
                        0,
                    ),
                    (
                        ViewPlaceholder::OneToOne(2, BinaryOperator::GreaterOrEqual),
                        1,
                    ),
                ],
                Dialect::MySQL,
            );

            assert_eq!(
                query.filter,
                Some(DfExpr::Op {
                    left: Box::new(DfExpr::Column {
                        index: 1,
                        ty: DfType::DEFAULT_TEXT
                    }),
                    op: DfBinaryOperator::GreaterOrEqual,
                    right: Box::new(DfExpr::Literal {
                        val: "a".into(),
                        ty: DfType::DEFAULT_TEXT
                    }),
                    ty: DfType::Bool
                })
            );

            assert_eq!(
                query.key_comparisons,
                vec![KeyComparison::Range(
                    vec1![DfValue::from(1), DfValue::from("a")].range_from_inclusive(),
                )]
            );
        }

        #[test]
        fn paginated_with_key() {
            // "SELECT t.x FROM t WHERE t.x = $1 ORDER BY t.y ASC LIMIT 3 OFFSET $2"
            let query = make_build_query(
                vec![Cow::Owned(vec![DfValue::from(1), DfValue::from(3)])],
                Some(3),
                Some(3),
                &[
                    (ViewPlaceholder::OneToOne(1, BinaryOperator::Equal), 0),
                    (
                        ViewPlaceholder::PageNumber {
                            offset_placeholder: 2,
                            limit: 3,
                        },
                        1,
                    ),
                ],
                Dialect::MySQL,
            );

            assert_eq!(query.filter, None);

            assert_eq!(
                query.key_comparisons,
                vec![vec1![
                    DfValue::from(1),
                    DfValue::from(1) // page 2
                ]
                .into()]
            );
        }
    }
}
