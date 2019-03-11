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
//! use tokio::prelude::*;
//! # use noria::*;
//! # let zookeeper_addr = "127.0.0.1:2181";
//! let mut rt = tokio::runtime::Runtime::new().unwrap();
//! let mut db = SyncControllerHandle::from_zk(zookeeper_addr, rt.executor()).unwrap();
//!
//! // if this is the first time we interact with Noria, we must give it the schema
//! db.install_recipe("
//!     CREATE TABLE Article (aid int, title varchar(255), url text, PRIMARY KEY(aid));
//!     CREATE TABLE Vote (aid int, uid int);
//! ");
//!
//! // we can then get handles that let us insert into the new tables
//! let mut article = db.table("Article").unwrap().into_sync();
//! let mut vote = db.table("Vote").unwrap().into_sync();
//!
//! // let's make a new article
//! let aid = 42;
//! let title = "I love Soup";
//! let url = "https://pdos.csail.mit.edu";
//! article
//!     .insert(vec![aid.into(), title.into(), url.into()])
//!     .unwrap();
//!
//! // and then vote for it
//! vote.insert(vec![aid.into(), 1.into()]).unwrap();
//!
//! // we can also declare views that we want want to query
//! db.extend_recipe("
//!     VoteCount: \
//!       SELECT Vote.aid, COUNT(uid) AS votes \
//!       FROM Vote GROUP BY Vote.aid;
//!     QUERY ArticleWithVoteCount: \
//!       SELECT Article.aid, title, url, VoteCount.votes AS votes \
//!       FROM Article LEFT JOIN VoteCount ON (Article.aid = VoteCount.aid) \
//!       WHERE Article.aid = ?;");
//!
//! // and then get handles that let us execute those queries to fetch their results
//! let mut awvc = db.view("ArticleWithVoteCount").unwrap().into_sync();
//! // looking up article 42 should yield the article we inserted with a vote count of 1
//! assert_eq!(
//!     awvc.lookup(&[aid.into()], true).unwrap(),
//!     vec![vec![DataType::from(aid), title.into(), url.into(), 1.into()]]
//! );
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
//! # Synchronous and asynchronous operation
//!
//! By default, the Noria client operates with an asynchronous API based on futures. While this
//! allows great flexibility for clients as to how they schedule concurrent requests, it can be
//! awkward to work with when writing less performance-sensitive code. Most of the Noria API types
//! also have a synchronous version with a `Sync` prefix in the name that you can get at by calling
//! `into_sync` on the asynchronous handle.
//!
//! # Alternatives
//!
//! Noria provides a [MySQL adapter](https://github.com/mit-pdos/noria-mysql) that implements the
//! binary MySQL protocol, which provides a compatibility layer for applications that wish to
//! continue to issue ad-hoc MySQL queries through existing MySQL client libraries.
#![feature(allow_fail)]
#![feature(bufreader_buffer)]
#![feature(try_blocks)]
#![feature(existential_type)]
#![deny(missing_docs)]
#![deny(unused_extern_crates)]

#[macro_use]
extern crate failure;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate slog;
#[macro_use]
extern crate futures;

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
pub mod consensus;
#[doc(hidden)]
pub mod internal;

pub use crate::consensus::ZookeeperAuthority;
use crate::internal::*;

/// The prelude contains most of the types needed in everyday operation.
pub mod prelude {
    pub use super::ActivationResult;
    pub use super::ControllerHandle;
    pub use super::{SyncTable, Table};
    pub use super::{SyncView, View};
}

/// Noria errors.
pub mod error {
    pub use crate::table::TableError;
    pub use crate::view::ViewError;
}

#[derive(Debug, Default)]
#[doc(hidden)]
// only pub because we use it to figure out the error type for ViewError
pub struct Tagger(slab::Slab<()>);

impl<Request, Response> multiplex::TagStore<Tagged<Request>, Tagged<Response>> for Tagger {
    type Tag = u32;

    fn assign_tag(&mut self, r: &mut Tagged<Request>) -> Self::Tag {
        r.tag = self.0.insert(()) as u32;
        r.tag
    }
    fn finish_tag(&mut self, r: &Tagged<Response>) -> Self::Tag {
        self.0.remove(r.tag as usize);
        r.tag
    }
}

#[doc(hidden)]
#[derive(Serialize, Deserialize, Debug)]
pub struct Tagged<T> {
    pub tag: u32,
    pub v: T,
}

impl<T> From<T> for Tagged<T> {
    fn from(t: T) -> Self {
        Tagged { tag: 0, v: t }
    }
}

pub use crate::controller::{ControllerDescriptor, ControllerHandle, SyncControllerHandle};
pub use crate::data::{DataType, Modification, Operation, TableOperation};
pub use crate::table::{SyncTable, Table};
pub use crate::view::{SyncView, View};

#[doc(hidden)]
pub use crate::table::Input;

#[doc(hidden)]
pub use crate::view::{ReadQuery, ReadReply};

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
        DataType::BigInt(n) => n as usize % shards,
        DataType::Text(..) | DataType::TinyText(..) => {
            use std::borrow::Cow;
            use std::hash::Hasher;
            let mut hasher = fnv::FnvHasher::default();
            let s: Cow<str> = dt.into();
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

/// A `Box<dyn ::std::error::Error>` while we're waiting on rust-lang/rust#58974.
pub struct BoxDynError<E>(E);
use std::fmt;
impl<E: fmt::Display + fmt::Debug> ::std::error::Error for BoxDynError<E> {}
impl<E: fmt::Display + fmt::Debug> fmt::Debug for BoxDynError<E> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Debug::fmt(&self.0, f)
    }
}
impl<E: fmt::Display + fmt::Debug> fmt::Display for BoxDynError<E> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}
impl<E> From<E> for BoxDynError<E> {
    fn from(e: E) -> Self {
        BoxDynError(e)
    }
}
impl<E> BoxDynError<E> {
    fn into_inner(self) -> E {
        self.0
    }
}
