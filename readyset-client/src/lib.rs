//! This create contains client bindings for [ReadySet](https://github.com/readysettech/readyset).
//!
//! # What is ReadySet?
//!
//! ReadySet is a new streaming data-flow system designed to act as a fast storage backend for
//! read-heavy web applications based on [this paper](https://jon.tsp.io/papers/osdi18-noria.pdf)
//! from [OSDI'18](https://www.usenix.org/conference/osdi18/presentation/gjengset). It acts like a
//! databases, but pre-computes and caches relational query results so that reads are blazingly
//! fast. ReadySet automatically keeps cached results up-to-date as the underlying data, stored in
//! persistent _base tables_ change. ReadySet uses partially-stateful data-flow to reduce memory
//! overhead, and supports dynamic, runtime data-flow and query change.
//!
//! # Infrastructure
//!
//! Like most databases, ReadySet follows a server-client model where many clients connect to a
//! (potentially distributed) server. The server in this case is the `readyset-server`
//! binary, and must be started before clients can connect. See `readyset-server --help` for details
//! and the [ReadySet repository README](https://github.com/readysettech/readyset/) for details.
//! ReadySet uses [HasiCorp Consul](https://www.consul.io/) to announce the location of its
//! servers, so Consul must also be running.
//!
//! # Quickstart example
//!
//! If you just want to get up and running quickly, here's some code to dig into. Note that this
//! requires a nightly release of Rust to run for the time being.
//!
//! ```no_run
//! # use readyset_client::*;
//! # use readyset_client::consensus::{Authority, ConsulAuthority};
//! # use readyset_data::{DfValue, Dialect};
//! # use readyset_client::recipe::ChangeList;
//!
//! #[tokio::main]
//! async fn main() {
//!     let consul_auth =
//!         Authority::from(ConsulAuthority::new("127.0.0.1:8500/quickstart").unwrap());
//!     let mut db = ReadySetHandle::new(consul_auth).await;
//!
//!     // if this is the first time we interact with ReadySet, we must give it the schema
//!     db.extend_recipe(
//!         ChangeList::from_str(
//!             "CREATE TABLE Article (aid int, title varchar(255), url text, PRIMARY KEY(aid));
//!              CREATE TABLE Vote (aid int, uid int);",
//!             Dialect::DEFAULT_MYSQL,
//!         )
//!         .unwrap(),
//!     )
//!     .await
//!     .unwrap();
//!
//!     // we can then get handles that let us insert into the new tables
//!     let mut article = db.table("Article").await.unwrap();
//!     let mut vote = db.table("Vote").await.unwrap();
//!
//!     // let's make a new article
//!     let aid = 42;
//!     let title = "I love Soup";
//!     let url = "https://pdos.csail.mit.edu";
//!     article
//!         .insert(vec![
//!             aid.into(),
//!             title.try_into().unwrap(),
//!             url.try_into().unwrap(),
//!         ])
//!         .await
//!         .unwrap();
//!
//!     // and then vote for it
//!     vote.insert(vec![aid.into(), 1.into()]).await.unwrap();
//!
//!     // we can also declare views that we want want to query
//!     db.extend_recipe(
//!         ChangeList::from_str(
//!             "
//!         VoteCount: \
//!           SELECT Vote.aid, COUNT(uid) AS votes \
//!           FROM Vote GROUP BY Vote.aid;
//!         CREATE CACHE ArticleWithVoteCount FROM \
//!           SELECT Article.aid, title, url, VoteCount.votes AS votes \
//!           FROM Article LEFT JOIN VoteCount ON (Article.aid = VoteCount.aid) \
//!           WHERE Article.aid = ?;",
//!             Dialect::DEFAULT_MYSQL,
//!         )
//!         .unwrap(),
//!     )
//!     .await
//!     .unwrap();
//!
//!     // and then get handles that let us execute those queries to fetch their results
//!     let mut awvc = db
//!         .view("ArticleWithVoteCount")
//!         .await
//!         .unwrap()
//!         .into_reader_handle()
//!         .unwrap();
//!     // looking up article 42 should yield the article we inserted with a vote count of 1
//!     assert_eq!(
//!         awvc.lookup(&[aid.into()], true).await.unwrap().into_vec(),
//!         vec![vec![
//!             DfValue::from(aid),
//!             title.try_into().unwrap(),
//!             url.try_into().unwrap(),
//!             1.into()
//!         ]]
//!     );
//! }
//! ```
//!
//! # Client model
//!
//! ReadySet accepts a set of parameterized SQL queries (think [prepared
//! statements](https://en.wikipedia.org/wiki/Prepared_statement)), and produces a [data-flow
//! program](https://en.wikipedia.org/wiki/Stream_processing) that maintains [materialized
//! views](https://en.wikipedia.org/wiki/Materialized_view) for the output of those queries. Reads
//! now become fast lookups directly into these materialized views, as if the value had been
//! directly read from a cache (like memcached). The views are automatically kept up-to-date by
//! ReadySet through the data-flow.
//!
//! Reads work quite differently in ReadySet compared to traditional relational databases. In
//! particular, a query, or _view_, must be _registered_ before it can be executed, much like SQL
//! prepared statements. Use [`ReadySetHandle::extend_recipe`] to register new base tables and
//! views. Once a view has been registered, you can get a handle that lets you execute the
//! corresponding query by passing the view's name to [`ReadySetHandle::view`]. The returned
//! [`View`] can be used to query the view with different values for its declared parameters
//! (values in place of `?` in the query) through [`View::lookup`] and [`View::multi_lookup`].
//!
//! Writes are fairly similar to those in relational databases. To add a new table, you extend the
//! recipe (using [`ReadySetHandle::extend_recipe`]) with a `CREATE TABLE` statement, and then
//! use [`ReadySetHandle::table`] to get a handle to the new base table. Base tables support
//! similar operations as SQL tables, such as [`Table::insert`], [`Table::update`],
//! [`Table::delete`], and also more esoteric operations like [`Table::insert_or_update`].
//!
//! # Alternatives
//!
//! ReadySet provides an
//! [adapter](https://github.com/readysettech/readyset/tree/main/readyset) that implements the
//! binary MySQL and PostgreSQL protocols, which provides a compatibility layer for applications
//! that wish to continue to issue ad-hoc MySQL or PostgreSQL queries through existing SQL client
//! libraries.
#![feature(
    result_flattening,
    type_alias_impl_trait,
    stmt_expr_attributes,
    bound_map,
    bound_as_ref,
    box_into_inner,
    is_sorted,
    once_cell
)]
#![deny(macro_use_extern_crate)]
#![deny(unused_extern_crates)]
#![deny(unreachable_pub)]
#![warn(rust_2018_idioms)]

/// Maximum number of requests that may be in-flight _to_ the connection pool at a time.
///
/// We want this to be > 1 so that multiple threads can enqueue requests at the same time without
/// immediately blocking one another. The exact value is somewhat arbitrary.
///
/// The value isn't higher, because it would unnecessarily consume memory.
///
/// The value isn't lower, because it would mean fewer concurrent enqueues.
///
/// NOTE: This value also places a soft-ish limit on the number of instances you can have of
/// `View`s or `Table`s that include a particular endpoint address when sharding is enabled. The
/// reason for this is kind of subtle: when you `poll_ready` a `View` or `Table`, we internally
/// `poll_ready` all the shards of that `View` or `Table`, each of which is a `tower-buffer`. When
/// you `poll_ready` a `tower-buffer`, it reserves a "slot" in its buffer for the coming request,
/// effectively reducing the capacity of the channel by 1 until the send happens. But, with
/// sharding, it may be that no request then goes to a particular shard. So, you may end up with
/// *all* the slots to a given resource taken up by `poll_ready`s that haven't been used yet. A
/// similar issue arises if you ever do:
///
/// ```ignore
/// view.ready().await;
/// let req = reqs.next().await;
/// view.call(req).await;
/// ```
///
/// When `ready` resolves, it will be holding up a slot in the buffer to `view`. It will continue
/// to hold that up all the way until the next request arrives from `reqs`, which may be a very
/// long time!
///
/// This problem is also described inhttps://github.com/tower-rs/tower/pull/425 and
/// https://github.com/tower-rs/tower/issues/408#issuecomment-593678194. Ultimately, we need
/// something like https://github.com/tower-rs/tower/issues/408, but for the time being, just make
/// sure this value is high enough.
pub(crate) const BUFFER_TO_POOL: usize = 1024;

/// The number of concurrent connections to a given backend table.
///
/// Since ReadySet connections are multiplexing, having this value > 1 _only_ allows us to do
/// serialization/deserialization in parallel on multiple threads. Nothing else really.
///
/// The value isn't higher for a couple of reasons:
///
///  - It is per table, which means it is per shard of a domain. Unless _all_ of your requests go to
///    a single shard of one table, you should be fine.
///  - Table operations are generally not bottlenecked on serialization, but on committing.
///
/// The value isn't lower, because we want _some_ concurrency in serialization.
pub(crate) const TABLE_POOL_SIZE: usize = 2;

/// The number of concurrent connections to a given backend view.
///
/// Since ReadySet connections are multiplexing, having this value > 1 _only_ allows us to do
/// serialization/deserialization in parallel on multiple threads. Nothing else really.
///
/// This value is set higher than the max pool size for tables for a couple of reasons:
///
///  - View connections are made per _host_. So, if you query multiple views that happen to be
///    hosted by a single machine (such as if there is only one ReadySet worker), this is the total
///    amount of serialization concurrency you will get.
///  - Reads are generally bottlenecked on serialization, so devoting more resources to it seems
///    reasonable.
///
/// The value isn't higher because we, _and the server_ only have so many cores.
pub(crate) const VIEW_POOL_SIZE: usize = 16;

/// Number of requests that can be pending to any particular target.
///
/// Keep in mind that this is really the number of requests that can be pending to any given shard
/// of a domain (for tables) or to any given ReadySet worker (for views). The value should arguably
/// be higher for views than for tables, since views are more likely to share a connection than
/// tables, but since this is really just a measure for "are we falling over", it can sort of be
/// arbitrarily high. If the system isn't keeping up, then it will fill up regardless, it'll just
/// take longer.
///
/// We need to limit this since `AsyncBincode` has unlimited buffering, and so will never apply
/// back-pressure otherwise. The backpressure is necessary so that the pool will eventually know
/// that another connection should be established to help with serialization/deserialization.
///
/// The value isn't higher, because it would mean we just allow more data to be buffered internally
/// in the system before we exhert backpressure.
///
/// The value isn't lower, because that give the server less work at a time, which means it can
/// batch less work, which means lower overall efficiency.
pub(crate) const PENDING_LIMIT: usize = 8192;

use std::fmt::{self, Display};
use std::ops::AddAssign;

use nom_sql::Relation;
use readyset_tracing::propagation::Instrumented;
use replication_offset::ReplicationOffset;
use tokio_tower::multiplex;

#[cfg(feature = "failure_injection")]
pub mod failpoints;

pub mod consistency;
mod controller;
pub mod metrics;
pub mod query;
pub mod status;
mod table;
mod view;
use std::convert::TryFrom;
use std::default::Default;
pub mod recipe;
pub mod replication;

pub mod channel;
#[allow(unreachable_pub)] // https://github.com/rust-lang/rust/issues/57411
pub mod consensus;
#[allow(unreachable_pub)] // https://github.com/rust-lang/rust/issues/57411
pub mod internal;

// for the row! macro
use std::future::Future;
use std::pin::Pin;

pub use nom_sql::{ColumnConstraint, SqlIdentifier};
use readyset_data::{DfType, DfValue};
use readyset_errors::{ReadySetError, ReadySetResult};
use serde::{Deserialize, Serialize};
use tokio::task_local;
pub use view::{
    ColumnBase, ColumnSchema, KeyColumnIdx, PlaceholderIdx, ReaderHandle, ViewPlaceholder,
    ViewSchema,
};

// FIXME(eta): get rid of these
use crate::internal::*;

/// The prelude contains most of the types needed in everyday operation.
pub mod prelude {
    pub use super::{ReadySetHandle, Table, View};
}

/// Wrapper types for ReadySet query results.
pub mod results {
    pub use super::view::results::{Key, ResultIterator, Results, Row, SharedResults, SharedRows};
}

task_local! {
    static TRACE_NEXT: ();
}

fn trace_next_op() -> bool {
    TRACE_NEXT.try_with(|_| true).unwrap_or(false)
}

/// The next ReadySet read or write issued from the current thread will be traced using tokio-trace.
///
/// The trace output is visible by setting the environment variable `LOG_LEVEL=trace`.
pub async fn trace_ops_in<T>(f: impl Future<Output = T>) -> T {
    TRACE_NEXT.scope((), f).await
}

#[derive(Debug, Default)]
// only pub because we use it to figure out the error type for ViewError
pub struct Tagger(slab::Slab<()>);

impl<Request, Response> multiplex::TagStore<Tagged<Request>, Tagged<Response>> for Tagger {
    type Tag = u32;

    fn assign_tag(mut self: Pin<&mut Self>, r: &mut Tagged<Request>) -> Self::Tag {
        r.tag = self.0.insert(()) as u32;
        r.tag
    }
    fn finish_tag(mut self: Pin<&mut Self>, r: &Tagged<Response>) -> Self::Tag {
        self.0.remove(r.tag as usize);
        r.tag
    }
}

impl<Request, Response> multiplex::TagStore<Instrumented<Tagged<Request>>, Tagged<Response>>
    for Tagger
{
    type Tag = u32;

    fn assign_tag(mut self: Pin<&mut Self>, r: &mut Instrumented<Tagged<Request>>) -> Self::Tag {
        r.inner_mut().tag = self.0.insert(()) as u32;
        r.inner_mut().tag
    }
    fn finish_tag(mut self: Pin<&mut Self>, r: &Tagged<Response>) -> Self::Tag {
        self.0.remove(r.tag as usize);
        r.tag
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Tagged<T> {
    pub v: T,
    pub tag: u32,
}

impl<T> From<T> for Tagged<T> {
    fn from(t: T) -> Self {
        Tagged { tag: 0, v: t }
    }
}

use url::Url;

pub use crate::consensus::WorkerDescriptor;
pub use crate::controller::{ControllerDescriptor, ReadySetHandle};
pub use crate::table::{
    Modification, Operation, PacketData, PacketPayload, PacketTrace, Table, TableOperation,
    TableReplicationStatus, TableRequest, TableStatus,
};
pub use crate::view::{
    KeyComparison, LookupResult, ReadQuery, ReadReply, ReadReplyBatch, ReadReplyStats, SchemaType,
    View, ViewCreateRequest, ViewQuery,
};

pub mod builders {
    pub use super::table::TableBuilder;
    pub use super::view::{ReaderHandleBuilder, ReusedReaderHandleBuilder, ViewBuilder};
}

/// Types used when debugging ReadySet.
pub mod debug;

/// Filters that can be used to filter the type of
/// view returned.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum ViewFilter {
    /// Pool of worker addresses. If the pool is not empty, this will
    /// look for a view reader in the pool.
    Workers(Vec<Url>),
    /// Request a specific replica of this view, returning an error if the given replica does not
    /// exist
    Replica(usize),
}

/// Represents a request for a view.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ViewRequest {
    /// Name of the view being requested.
    pub name: Relation,
    /// Filter to be applied when searching for a view.
    pub filter: Option<ViewFilter>,
}

/// A [`ReaderAddress`] is a unique identifier of a reader, it consists of the reader node in the
/// dataflow graph, the name of the reader and the shard index.
#[derive(Debug, Clone, Hash, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReaderAddress {
    /// The index of the reader node in the dataflow graph
    pub node: petgraph::graph::NodeIndex,
    /// The name of the reader
    pub name: Relation,
    /// The shard index
    pub shard: usize,
}

/// Represents an eviction of a single key from some partially materialized index identified by the
/// `tag`. `domain_idx` is included for convenience, because the controller does not store mappings
/// from Tag to `DomainIndex`.
///
/// This struct may represent the result of an eviction or may be used to trigger an eviction.
///
/// Note: `tag` is included as a u32 because it is defined in readyset-common, which imports this
/// crate.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SingleKeyEviction {
    /// The `DomainIndex` of the `Domain` that owns the partially materialized index.
    pub domain_idx: DomainIndex,
    /// The Tag identifying the partially materialized index.
    pub tag: u32,
    /// The key that was evicted from the partially materialized index.
    pub key: Vec<DfValue>,
}

/// Use to aggregate various node stats that describe its size
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeSize {
    /// The number of keys materialized for a node
    pub key_count: KeyCount,
    /// The approximate size of the materialized state in bytes
    pub bytes: NodeMaterializedSize,
}

/// Used to wrap key counts since we use row count estimates as a rough correlate of the key count
/// in the case of RocksDB nodes, and we want to keep track of when we do that so as to avoid any
/// confusion in other parts of the code.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum KeyCount {
    /// An exact key count, pulled from a memory node
    ExactKeyCount(usize),
    /// An estimate of the row count, pulled from a persistent storage node
    EstimatedRowCount(usize),
    /// This node does not keep rows, it uses materialization from another node
    ExternalMaterialization,
}

/// Used to wrap the materialized size of a node's state
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeMaterializedSize(usize);

impl Display for KeyCount {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            KeyCount::ExactKeyCount(count) => write!(f, "{}", count),
            KeyCount::EstimatedRowCount(count) => write!(f, "~{}", count),
            KeyCount::ExternalMaterialization => write!(f, ""),
        }
    }
}

impl Display for NodeMaterializedSize {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let size_b = self.0 as f64;
        let size_kb = size_b / 1024.;
        let size_mb = size_kb / 1024.;
        if size_kb < 1. {
            write!(f, "{size_b:.0} B")
        } else if size_mb < 1. {
            write!(f, "{size_kb:.2} KiB")
        } else {
            write!(f, "{size_mb:.2} MiB")
        }
    }
}

impl AddAssign for NodeSize {
    /// Adds the node size for the rhs node size to ourselves.
    fn add_assign(&mut self, rhs: Self) {
        self.key_count += rhs.key_count;
        self.bytes += rhs.bytes;
    }
}

impl AddAssign for KeyCount {
    /// Adds the key count for the rhs KeyCount to ourselves.
    ///
    /// # Panics
    ///
    /// Panics if the caller attempts to add an `ExactKeyCount` to an `EstimatedRowCount` or vice
    /// versa.
    #[track_caller]
    fn add_assign(&mut self, rhs: Self) {
        match (&self, rhs) {
            (KeyCount::ExactKeyCount(self_count), KeyCount::ExactKeyCount(rhs_count)) => {
                *self = KeyCount::ExactKeyCount(*self_count + rhs_count)
            }
            (KeyCount::EstimatedRowCount(self_count), KeyCount::EstimatedRowCount(rhs_count)) => {
                *self = KeyCount::EstimatedRowCount(*self_count + rhs_count)
            }
            _ => panic!(
                "Cannot add mismatched KeyCount types for values {}/{}",
                self, rhs
            ),
        };
    }
}

impl AddAssign for NodeMaterializedSize {
    /// Adds the node size for the rhs node size to ourselves.
    fn add_assign(&mut self, rhs: Self) {
        self.0 += rhs.0;
    }
}

#[inline]
pub fn shard_by(dt: &DfValue, shards: usize) -> usize {
    match *dt {
        DfValue::Int(n) => n as usize % shards,
        DfValue::UnsignedInt(n) => n as usize % shards,
        DfValue::Text(..) | DfValue::TinyText(..) | DfValue::TimestampTz(_) => {
            use std::hash::Hasher;
            let mut hasher = ahash::AHasher::new_with_keys(0x3306, 0x6033);
            // this unwrap should be safe because there are no error paths with a Text, TinyText,
            // nor Timestamp converting to Text
            #[allow(clippy::unwrap_used)]
            let str_dt = dt
                .coerce_to(&DfType::DEFAULT_TEXT, &DfType::Unknown)
                .unwrap();
            // this unwrap should be safe because we just coerced dt to a text
            #[allow(clippy::unwrap_used)]
            let s: &str = <&str>::try_from(&str_dt).unwrap();
            hasher.write(s.as_bytes());
            hasher.finish() as usize % shards
        }
        // a bit hacky: send all NULL values to the first shard
        DfValue::None | DfValue::Max => 0,
        DfValue::Float(_)
        | DfValue::Double(_)
        | DfValue::Time(_)
        | DfValue::ByteArray(_)
        | DfValue::Numeric(_)
        | DfValue::BitVector(_)
        | DfValue::Array(_)
        | DfValue::PassThrough(_) => {
            use std::hash::{Hash, Hasher};
            let mut hasher = ahash::AHasher::new_with_keys(0x3306, 0x6033);
            dt.hash(&mut hasher);
            hasher.finish() as usize % shards
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_count_formatting() {
        assert_eq!("42", KeyCount::ExactKeyCount(42).to_string());
        assert_eq!("~42", KeyCount::EstimatedRowCount(42).to_string());
    }

    #[test]
    fn key_count_add_assign() {
        let mut kc = KeyCount::ExactKeyCount(1);
        kc += KeyCount::ExactKeyCount(99);
        assert_eq!(KeyCount::ExactKeyCount(100), kc);
    }

    #[test]
    #[should_panic]
    fn key_count_add_assign_panic() {
        let mut kc = KeyCount::ExactKeyCount(1);
        kc += KeyCount::EstimatedRowCount(1);
    }
}
