//! This create contains client bindings for [Noria](https://github.com/mit-pdos/noria).
//!
//! # What is Noria?
//!
//! Noria is a new streaming data-flow system designed to act as a fast storage backend for
//! read-heavy web applications based on [this paper](https://jon.tsp.io/papers/osdi18-noria.pdf)
//! from [OSDI'18](https://www.usenix.org/conference/osdi18/presentation/gjengset). It acts like a
//! databases, but pre-computes and caches relational query results so that reads are blazingly
//! fast. Noria automatically keeps cached results up-to-date as the underlying data, stored in
//! persistent _base tables_ change. Noria uses partially-stateful data-flow to reduce memory
//! overhead, and supports dynamic, runtime data-flow and query change.
//!
//! # Infrastructure
//!
//! Like most databases, Noria follows a server-client model where many clients connect to a
//! (potentially distributed) server. The server in this case is the `noria-server`
//! binary, and must be started before clients can connect. See `noria-server --help` for details
//! and the [Noria repository README](https://github.com/mit-pdos/noria) for details. Noria uses
//! [Apache ZooKeeper](https://zookeeper.apache.org/) to announce the location of its servers, so
//! ZooKeeper must also be running.
//!
//! # Quickstart example
//!
//! If you just want to get up and running quickly, here's some code to dig into. Note that this
//! requires a nightly release of Rust to run for the time being.
//!
//! ```no_run
//! # use noria::*;
//! #[tokio::main]
//! async fn main() {
//!     let zookeeper_addr = "127.0.0.1:2181";
//!     let mut db = ControllerHandle::from_zk(zookeeper_addr).await.unwrap();
//!
//!     // if this is the first time we interact with Noria, we must give it the schema
//!     db.install_recipe("
//!         CREATE TABLE Article (aid int, title varchar(255), url text, PRIMARY KEY(aid));
//!         CREATE TABLE Vote (aid int, uid int);
//!     ").await.unwrap();
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
//!         .insert(vec![aid.into(), title.into(), url.into()])
//!         .await
//!         .unwrap();
//!
//!     // and then vote for it
//!     vote.insert(vec![aid.into(), 1.into()]).await.unwrap();
//!
//!     // we can also declare views that we want want to query
//!     db.extend_recipe("
//!         VoteCount: \
//!           SELECT Vote.aid, COUNT(uid) AS votes \
//!           FROM Vote GROUP BY Vote.aid;
//!         QUERY ArticleWithVoteCount: \
//!           SELECT Article.aid, title, url, VoteCount.votes AS votes \
//!           FROM Article LEFT JOIN VoteCount ON (Article.aid = VoteCount.aid) \
//!           WHERE Article.aid = ?;").await.unwrap();
//!
//!     // and then get handles that let us execute those queries to fetch their results
//!     let mut awvc = db.view("ArticleWithVoteCount").await.unwrap();
//!     // looking up article 42 should yield the article we inserted with a vote count of 1
//!     assert_eq!(
//!         awvc.lookup(&[aid.into()], true).await.unwrap(),
//!         vec![vec![DataType::from(aid), title.into(), url.into(), 1.into()]]
//!     );
//! }
//! ```
//!
//! # Client model
//!
//! Noria accepts a set of parameterized SQL queries (think [prepared
//! statements](https://en.wikipedia.org/wiki/Prepared_statement)), and produces a [data-flow
//! program](https://en.wikipedia.org/wiki/Stream_processing) that maintains [materialized
//! views](https://en.wikipedia.org/wiki/Materialized_view) for the output of those queries. Reads
//! now become fast lookups directly into these materialized views, as if the value had been
//! directly read from a cache (like memcached). The views are automatically kept up-to-date by
//! Noria through the data-flow.
//!
//! Reads work quite differently in Noria compared to traditional relational databases. In
//! particular, a query, or _view_, must be _registered_ before it can be executed, much like SQL
//! prepared statements. Use [`ControllerHandle::extend_recipe`] to register new base tables and
//! views. Once a view has been registered, you can get a handle that lets you execute the
//! corresponding query by passing the view's name to [`ControllerHandle::view`]. The returned
//! [`View`] can be used to query the view with different values for its declared parameters
//! (values in place of `?` in the query) through [`View::lookup`] and [`View::multi_lookup`].
//!
//! Writes are fairly similar to those in relational databases. To add a new table, you extend the
//! recipe (using [`ControllerHandle::extend_recipe`]) with a `CREATE TABLE` statement, and then
//! use [`ControllerHandle::table`] to get a handle to the new base table. Base tables support
//! similar operations as SQL tables, such as [`Table::insert`], [`Table::update`],
//! [`Table::delete`], and also more esoteric operations like [`Table::insert_or_update`].
//!
//! # Alternatives
//!
//! Noria provides a [MySQL adapter](https://github.com/mit-pdos/noria-mysql) that implements the
//! binary MySQL protocol, which provides a compatibility layer for applications that wish to
//! continue to issue ad-hoc MySQL queries through existing MySQL client libraries.
#![feature(type_alias_impl_trait)]
#![deny(missing_docs)]
#![deny(unused_extern_crates)]
#![deny(unreachable_pub)]
#![warn(rust_2018_idioms)]

#[macro_use]
extern crate failure;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate slog;

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
/// Since Noria connections are multiplexing, having this value > 1 _only_ allows us to do
/// serialization/deserialization in parallel on multiple threads. Nothing else really.
///
/// The value isn't higher for a couple of reasons:
///
///  - It is per table, which means it is per shard of a domain. Unless _all_ of your requests go
///    to a single shard of one table, you should be fine.
///  - Table operations are generally not bottlenecked on serialization, but on committing.
///
/// The value isn't lower, because we want _some_ concurrency in serialization.
pub(crate) const TABLE_POOL_SIZE: usize = 2;

/// The number of concurrent connections to a given backend view.
///
/// Since Noria connections are multiplexing, having this value > 1 _only_ allows us to do
/// serialization/deserialization in parallel on multiple threads. Nothing else really.
///
/// This value is set higher than the max pool size for tables for a couple of reasons:
///
///  - View connections are made per _host_. So, if you query multiple views that happen to be
///    hosted by a single machine (such as if there is only one Noria worker), this is the total
///    amount of serialization concurrency you will get.
///  - Reads are generally bottlenecked on serialization, so devoting more resources to it seems
///    reasonable.
///
/// The value isn't higher because we, _and the server_ only have so many cores.
pub(crate) const VIEW_POOL_SIZE: usize = 16;

/// Number of requests that can be pending to any particular target.
///
/// Keep in mind that this is really the number of requests that can be pending to any given shard
/// of a domain (for tables) or to any given Noria worker (for views). The value should arguably be
/// higher for views than for tables, since views are more likely to share a connection than
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

use petgraph::graph::NodeIndex;
use std::collections::HashMap;
use tokio_tower::multiplex;

mod controller;
mod data;
mod table;
mod view;

#[doc(hidden)]
pub mod channel;
#[doc(hidden)]
#[allow(unreachable_pub)] // https://github.com/rust-lang/rust/issues/57411
pub mod consensus;
#[doc(hidden)]
pub mod doc_mock;
#[doc(hidden)]
#[allow(unreachable_pub)] // https://github.com/rust-lang/rust/issues/57411
pub mod internal;

// for the row! macro
#[doc(hidden)]
pub use nom_sql::ColumnConstraint;

pub use crate::consensus::ZookeeperAuthority;
use crate::internal::*;
use std::future::Future;
use std::pin::Pin;
use tokio::task_local;

/// The prelude contains most of the types needed in everyday operation.
pub mod prelude {
    pub use super::ActivationResult;
    pub use super::ControllerHandle;
    pub use super::Table;
    pub use super::View;
}

/// Wrapper types for Noria query results.
pub mod results {
    pub use super::view::results::{ResultRow, Results, Row};
}

/// Noria errors.
pub mod error {
    pub use crate::table::TableError;
    pub use crate::view::ViewError;
}

task_local! {
    static TRACE_NEXT: ();
}

fn trace_next_op() -> bool {
    TRACE_NEXT.try_with(|_| true).unwrap_or(false)
}

/// The next Noria read or write issued from the current thread will be traced using tokio-trace.
///
/// The trace output is visible by setting the environment variable `RUST_LOG=trace`.
pub async fn trace_ops_in<T>(f: impl Future<Output = T>) -> T {
    TRACE_NEXT.scope((), f).await
}

#[derive(Debug, Default)]
#[doc(hidden)]
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

#[doc(hidden)]
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

pub use crate::controller::{ControllerDescriptor, ControllerHandle};
pub use crate::data::{DataType, Modification, Operation, TableOperation};
pub use crate::table::Table;
pub use crate::view::View;

#[doc(hidden)]
pub use crate::table::Input;

#[doc(hidden)]
pub use crate::view::{ReadQuery, ReadReply, ReadReplyBatch};

#[doc(hidden)]
pub mod builders {
    pub use super::table::TableBuilder;
    pub use super::view::ViewBuilder;
}

/// Types used when debugging Noria.
pub mod debug;

/// Represents the result of a recipe activation.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ActivationResult {
    /// Map of query names to `NodeIndex` handles for reads/writes.
    pub new_nodes: HashMap<String, NodeIndex>,
    /// List of leaf nodes that were removed.
    pub removed_leaves: Vec<NodeIndex>,
    /// Number of expressions the recipe added compared to the prior recipe.
    pub expressions_added: usize,
    /// Number of expressions the recipe removed compared to the prior recipe.
    pub expressions_removed: usize,
}

#[doc(hidden)]
#[inline]
pub fn shard_by(dt: &DataType, shards: usize) -> usize {
    match *dt {
        DataType::Int(n) => n as usize % shards,
        DataType::UnsignedInt(n) => n as usize % shards,
        DataType::BigInt(n) => n as usize % shards,
        DataType::UnsignedBigInt(n) => n as usize % shards,
        DataType::Text(..) | DataType::TinyText(..) => {
            use std::hash::Hasher;
            let mut hasher = ahash::AHasher::new_with_keys(0x3306, 0x6033);
            let s: &str = dt.into();
            hasher.write(s.as_bytes());
            hasher.finish() as usize % shards
        }
        // a bit hacky: send all NULL values to the first shard
        DataType::None => 0,
        ref x => {
            unimplemented!("asked to shard on value {:?}", x);
        }
    }
}
