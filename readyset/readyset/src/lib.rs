//! This crate contains client bindings for readyset

#![feature(
    type_alias_impl_trait,
    stmt_expr_attributes,
    bound_map,
    bound_as_ref,
    box_into_inner
)]
#![deny(missing_docs, macro_use_extern_crate)]
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
/// of a domain (for tables) or to any given ReadySet worker (for views). The value should arguably be
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

use std::collections::{HashMap, HashSet};

use nom_sql::SqlType;
use petgraph::graph::NodeIndex;
use readyset_tracing::propagation::Instrumented;
use replication::ReplicationOffset;
use tokio_tower::multiplex;

pub mod consistency;
mod controller;
pub mod metrics;
pub mod status;
mod table;
mod view;
use std::convert::TryFrom;
use std::default::Default;
pub mod recipe;
pub mod replication;

#[doc(hidden)]
pub mod channel;
#[doc(hidden)]
#[allow(unreachable_pub)] // https://github.com/rust-lang/rust/issues/57411
pub mod consensus;
#[doc(hidden)]
#[allow(unreachable_pub)] // https://github.com/rust-lang/rust/issues/57411
pub mod internal;

// for the row! macro
use std::future::Future;
use std::pin::Pin;

#[doc(hidden)]
pub use nom_sql::{ColumnConstraint, SqlIdentifier};
use readyset_data::DataType;
pub use readyset_errors::{ReadySetError, ReadySetResult};
use serde::{Deserialize, Serialize};
use tokio::task_local;
pub use view::{
    ColumnBase, ColumnSchema, KeyColumnIdx, PlaceholderIdx, ViewPlaceholder, ViewSchema,
};

pub use crate::consensus::ZookeeperAuthority;
// FIXME(eta): get rid of these
use crate::internal::*;

/// The prelude contains most of the types needed in everyday operation.
pub mod prelude {
    pub use super::{ActivationResult, ControllerHandle, Table, View};
}

/// Wrapper types for ReadySet query results.
pub mod results {
    pub use super::view::results::{ResultRow, Results, Row};
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

use url::Url;

pub use crate::consensus::WorkerDescriptor;
pub use crate::controller::{ControllerDescriptor, ControllerHandle};
pub use crate::table::{Modification, Operation, Table, TableOperation, TableRequest};
#[doc(hidden)]
pub use crate::table::{PacketData, PacketPayload, PacketTrace};
pub use crate::view::View;
#[doc(hidden)]
pub use crate::view::{
    KeyComparison, LookupResult, ReadQuery, ReadReply, ReadReplyBatch, ReadReplyStats, SchemaType,
    ViewQuery,
};

#[doc(hidden)]
pub mod builders {
    pub use super::table::TableBuilder;
    pub use super::view::{ReplicaShard, ViewBuilder, ViewReplica};
}

/// Types used when debugging ReadySet.
pub mod debug;

/// Represents the result of a recipe activation.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct ActivationResult {
    /// Map of query names to `NodeIndex` handles for reads/writes.
    pub new_nodes: HashMap<SqlIdentifier, NodeIndex>,
    /// List of leaf nodes that were removed.
    pub removed_leaves: HashSet<NodeIndex>,
    /// Number of expressions the recipe added compared to the prior recipe.
    pub expressions_added: usize,
    /// Number of expressions the recipe removed compared to the prior recipe.
    pub expressions_removed: usize,
}

/// Represents a request to replicate readers for the given queries into the given worker.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ReaderReplicationSpec {
    /// Name of the queries that will have their reader nodes replicated.
    pub queries: Vec<SqlIdentifier>,
    /// Worker URI.
    pub worker_uri: Option<Url>,
}

/// Represents the result of a reader replication.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ReaderReplicationResult {
    /// Map of query names to the new `DomainIndex`es and their `NodeIndex`es.
    pub new_readers: HashMap<SqlIdentifier, HashMap<DomainIndex, Vec<NodeIndex>>>,
}

/// Filters that can be used to filter the type of
/// view returned.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum ViewFilter {
    /// Pool of worker addresses. If the pool is not empty, this will
    /// look for a view reader in the pool.
    Workers(Vec<Url>),
    /// Region to request the view from.
    // TODO(justin): This parameter is currently not supported and
    // is a no-op.
    Region(String),
}

/// Represents a request for a view.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ViewRequest {
    /// Name of the view being requested.
    pub name: SqlIdentifier,
    /// Filter to be applied when searching for a view.
    pub filter: Option<ViewFilter>,
}

#[doc(hidden)]
#[inline]
pub fn shard_by(dt: &DataType, shards: usize) -> usize {
    match *dt {
        DataType::Int(n) => n as usize % shards,
        DataType::UnsignedInt(n) => n as usize % shards,
        DataType::Text(..) | DataType::TinyText(..) | DataType::TimestampTz(_) => {
            use std::hash::Hasher;
            let mut hasher = ahash::AHasher::new_with_keys(0x3306, 0x6033);
            // this unwrap should be safe because there are no error paths with a Text, TinyText,
            // nor Timestamp converting to Text
            #[allow(clippy::unwrap_used)]
            let str_dt = dt.coerce_to(&SqlType::Text).unwrap();
            // this unwrap should be safe because we just coerced dt to a text
            #[allow(clippy::unwrap_used)]
            let s: &str = <&str>::try_from(&str_dt).unwrap();
            hasher.write(s.as_bytes());
            hasher.finish() as usize % shards
        }
        // a bit hacky: send all NULL values to the first shard
        DataType::None | DataType::Max => 0,
        DataType::Float(_)
        | DataType::Double(_)
        | DataType::Time(_)
        | DataType::ByteArray(_)
        | DataType::Numeric(_)
        | DataType::BitVector(_) => {
            use std::hash::{Hash, Hasher};
            let mut hasher = ahash::AHasher::new_with_keys(0x3306, 0x6033);
            dt.hash(&mut hasher);
            hasher.finish() as usize % shards
        }
    }
}
