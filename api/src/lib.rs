//! This create contains client bindings for the [Soup](https://github.com/mit-pdos/distributary)
//! database.
//!
//! # What is Soup?
//!
//! Soup is a work-in-progress implementation of a new streaming data-flow system designed to
//! provide a high-performance storage backend for read-heavy applications. At a high level, it
//! takes a set of parameterized SQL queries similar to prepared SQL statements, and produces a
//! [data-flow program](https://en.wikipedia.org/wiki/Stream_processing) that maintains
//! [materialized views](https://en.wikipedia.org/wiki/Materialized_view) for the output of those
//! queries. This yields high read throughput, as queries can usually be satisfied with a single
//! key-value lookup, much like when using a cache. The data-flow provides _incremental view
//! maintenance_, and efficiently keeps the cached query results up-to-date as new writes arrive.
//! For further details, see https://github.com/mit-pdos/distributary.
//!
//! # Interacting with Soup
//!
//! Like most databases, Soup follows a server-client model where many clients connect to a
//! (potentially distributed) server. The server in this case is the binary `souplet` in the
//! [`distributary` crate](https://github.com/mit-pdos/distributary). The server must be started
//! before clients can connect.
//!
//! Soup uses [Apache ZooKeeper](https://zookeeper.apache.org/) to announce the location of its
//! servers. To connect to a Soup, pass the ZooKeeper address to [`ControllerHandle::from_zk`].
//! This will give yield a [`ControllerHandle`] which is then used to further issue commands to
//! Soup.
//!
//! Reads work quite differently in Soup compared to traditional relational databases. In
//! particular, a query, or _view_, must be _registered_ before it can be executed, much like SQL
//! prepared statements. To register new views, use [`ControllerHandle::extend_recipe`]. Once a
//! view has been registered, a handle for executing it is made by passing the view's name to
//! [`ControllerHandle::view`]. The returned [`View`] can be used to query the view with
//! different values for it's declared parameters (i.e., values in place of `?` in the query)
//! through [`View::lookup`] and [`View::multi_lookup`].
//!
//! Writes, in contrast, are quite similar to those in relational databases. To add a new table,
//! you extend the recipe (using [`ControllerHandle::extend_recipe`]) with a `CREATE TABLE`
//! statement, and then use [`ControllerHandle::table`] to get a handle to the new base table.
//! Table tables support similar operations and SQL tables, such as [`Table::insert`],
//! [`Table::update`], [`Table::delete`], and also more esoteric operations like
//! [`Table::insert_or_update`].
//!
//! More concretely, a first interaction with Soup might look like this:
//!
//! ```ignore
//! # use api::*;
//! let soup = ControllerHandle::from_zk(zookeeper_addr);
//!
//! // if this is the first time we interact with Soup, we must give it the schema
//! soup.install_recipe("
//!     CREATE TABLE Article (aid int, title varchar(255), \
//!                           url text, PRIMARY KEY(aid));
//!     CREATE TABLE Vote (aid int, uid int);");
//!
//! // we can then get handles that let us insert into the new tables
//! let mut article = soup.table("Article").unwrap();
//! let mut vote = soup.table("Vote").unwrap();
//!
//! // so let us make a new article
//! let aid = 1;
//! let title = "test title";
//! let url = "https://pdos.csail.mit.edu";
//! article
//!     .insert(vec![aid.into(), title.into(), url.into()])
//!     .unwrap();
//!
//! // and also a vote
//! vote.insert(vec![aid.into(), 42.into()]).unwrap();
//!
//! // we can also declare views that the application will want to query
//! soup.extend_recipe("
//!     VoteCount: \
//!       SELECT Vote.aid, COUNT(uid) AS votes \
//!       FROM Vote GROUP BY Vote.aid;
//!     QUERY ArticleWithVoteCount: \
//!       SELECT Article.aid, title, url, VoteCount.votes AS votes \
//!       FROM Article \
//!       LEFT JOIN VoteCount ON (Article.aid = VoteCount.aid) \
//!       WHERE Article.aid = ?;");
//!
//! // and then get handles that let us execute those queries
//! let mut awvc = soup.view("ArticleWithVoteCount").unwrap();
//! assert_eq!(awvc.lookup(&[1.into()], vec![aid.into(), title.into(), url.into(), 1.into()]));
//! ```
//!
//! # Is there an easier way? What about other languages?
//!
//! There is a [MySQL shim](https://github.com/mit-pdos/distributary-mysql) that can translate the
//! MySQL binary protocol directly to Soup operations. While it works decently well, and can be
//! useful to provide backwards-compatibility, we recommend using the client bindings directly
//! where possible.
#![feature(transpose_result)]
#![deny(missing_docs)]
#![deny(unused_extern_crates)]

#[cfg(debug_assertions)]
extern crate assert_infrequent;
extern crate basics;
extern crate bincode;
extern crate channel;
extern crate consensus;
#[macro_use]
extern crate failure;
extern crate futures;
extern crate hyper;
extern crate nom_sql;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
extern crate tokio;
extern crate vec_map;

use basics::*;
use std::collections::HashMap;

mod controller;
mod table;
mod view;

pub use basics::{DataType, Modification, Operation};

pub use consensus::{LocalAuthority, ZookeeperAuthority};

/// The prelude contains most of the types needed in everyday operation.
pub mod prelude {
    pub use super::ActivationResult;
    pub use super::ControllerHandle;
    pub use super::Table;
    pub use super::View;
}

pub use controller::{ControllerDescriptor, ControllerHandle, ControllerPointer};
pub use table::{Input, Table, TableError};
pub use view::{ReadQuery, ReadReply, View, ViewError};

#[doc(hidden)]
pub mod builders {
    pub use super::table::TableBuilder;
    pub use super::view::ViewBuilder;
}

/// Types used when debugging Soup.
pub mod debug;

/// Marker for a handle that has its own connection to Soup.
///
/// Such a handle can freely be sent between threads.
pub struct ExclusiveConnection;

/// Marker for a handle that shares its underlying connection with other handles owned by the same
/// thread.
///
/// This kind of handle can only be used on a single thread.
pub struct SharedConnection;

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

/// An error occured during transport (i.e., while sending or receiving).
#[derive(Debug, Fail)]
pub enum TransportError {
    /// A network-level error occurred.
    #[fail(display = "{}", _0)]
    Channel(#[cause] channel::tcp::SendError),
    /// A protocol-level error occurred.
    #[fail(display = "{}", _0)]
    Serialization(#[cause] bincode::Error),
}

impl From<channel::tcp::SendError> for TransportError {
    fn from(e: channel::tcp::SendError) -> Self {
        TransportError::Channel(e)
    }
}

impl From<bincode::Error> for TransportError {
    fn from(e: bincode::Error) -> Self {
        TransportError::Serialization(e)
    }
}
