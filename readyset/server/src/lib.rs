//! Utilities to create all server components.

#![feature(
    type_alias_impl_trait,
    box_patterns,
    try_find,
    stmt_expr_attributes,
    result_flattening,
    drain_filter,
    hash_raw_entry
)]
#![deny(missing_docs)]
#![deny(unused_extern_crates)]
#![deny(macro_use_extern_crate)]
//#![deny(unreachable_pub)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::type_complexity)]
#![allow(clippy::redundant_closure)]

mod builder;
mod controller;
mod coordination;
mod handle;
mod http_router;

/// Utilities to create all server components.
pub mod startup;
/// The worker logic handling reads from the dataflow graph.
pub mod worker;

#[cfg(test)]
mod integration;
#[cfg(test)]
mod integration_serial;
#[cfg(test)]
mod integration_utils;
pub mod metrics;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[allow(missing_docs)]
pub enum ReuseConfigType {
    Finkelstein,
    Relaxed,
    Full,
}

use controller::migrate::materialization;
pub use controller::migrate::materialization::FrontierStrategy;
use controller::sql;
pub use dataflow::{DurabilityMode, PersistenceParameters};
pub use petgraph::graph::NodeIndex;
pub use readyset::consensus::{Authority, LocalAuthority};
pub use readyset::*;

pub use crate::builder::Builder;
pub use crate::handle::Handle;
pub use crate::metrics::NoriaMetricsRecorder;

#[doc(hidden)]
pub mod manual {
    pub use dataflow::node::special::Base;
    pub use dataflow::ops;

    pub use crate::controller::migrate::Migration;
}

use std::time::Duration;

use dataflow::DomainConfig;
use serde::{Deserialize, Serialize};

/// Configuration for an running ReadySet cluster
// WARNING: if you change this structure or any of the structures used in its fields, make sure to
// write a serialized instance of the previous version to tests/config_versions by running the
// following command *before* your change:
//
// ```
// cargo run --bin make_config_json
// ```
#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
pub struct Config {
    pub(crate) sharding: Option<usize>,
    #[serde(default)]
    pub(crate) materialization_config: materialization::Config,
    pub(crate) domain_config: DomainConfig,
    pub(crate) persistence: PersistenceParameters,
    pub(crate) quorum: usize,
    pub(crate) reuse: Option<ReuseConfigType>,
    pub(crate) primary_region: Option<String>,
    /// If set to true (the default), failing tokio tasks will cause a full-process abort.
    pub(crate) abort_on_task_failure: bool,
    /// Configuration for converting SQL to MIR
    pub(crate) mir_config: sql::mir::Config,
    pub(crate) replication_url: Option<String>,
    pub(crate) replication_server_id: Option<u32>,
    pub(crate) keep_prior_recipes: bool,
    /// The duration to wait before canceling the task waiting on an upquery.
    pub(crate) upquery_timeout: Duration,
    /// The duration to wait before canceling a task waiting on a worker request. Worker requests
    /// are typically issued as part of migrations.
    pub(crate) worker_request_timeout: Duration,
    // The duration to wait after a failure of the replication task before restarting it.
    pub(crate) replicator_restart_timeout: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            #[cfg(test)]
            sharding: Some(2),
            #[cfg(not(test))]
            sharding: None,
            materialization_config: Default::default(),
            domain_config: DomainConfig {
                concurrent_replays: 512,
                aggressively_update_state_sizes: false,
                view_request_timeout: Duration::from_millis(5000),
                // This RPC timeout must be long enough to handle compaction RPCs and extremely
                // high concurrency during snapshotting. We set this to the migration timeout for
                // now.
                table_request_timeout: Duration::from_millis(1800000),
                eviction_kind: dataflow::EvictionKind::Random,
            },
            persistence: Default::default(),
            quorum: 1,
            reuse: None,
            primary_region: None,
            abort_on_task_failure: true,
            mir_config: Default::default(),
            replication_url: None,
            replication_server_id: None,
            keep_prior_recipes: true,
            upquery_timeout: Duration::from_millis(5000),
            worker_request_timeout: Duration::from_millis(1800000),
            replicator_restart_timeout: Duration::from_secs(30),
        }
    }
}

use std::pin::Pin;
use std::task::{Context, Poll};

use futures_util::sink::Sink;
struct ImplSinkForSender<T>(tokio::sync::mpsc::UnboundedSender<T>);

impl<T> Sink<T> for ImplSinkForSender<T> {
    type Error = tokio::sync::mpsc::error::SendError<T>;

    fn poll_ready(self: Pin<&mut Self>, _: &mut Context) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, item: T) -> Result<(), Self::Error> {
        self.0.send(item)
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _: &mut Context) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}

// TODO: Change VolumeId type when we know this fixed size.
/// Id associated with the worker server's volume.
pub type VolumeId = String;
