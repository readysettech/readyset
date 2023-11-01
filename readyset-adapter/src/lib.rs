#![feature(box_syntax, box_patterns)]
#![feature(drain_filter)]
#![feature(async_closure)]
#![feature(never_type)]
#![feature(exhaustive_patterns)]
#![feature(is_sorted)]
#![feature(if_let_guard)]
#![feature(arc_unwrap_or_clone)]
#![deny(unreachable_pub)]

pub mod backend;
pub mod http_router;
pub mod metrics_handle;
pub mod migration_handler;
pub mod proxied_queries_reporter;
mod query_handler;
pub mod query_status_cache;
pub mod rewrite;
pub mod upstream_database;
mod utils;
pub mod views_synchronizer;

use std::fmt::{Display, Formatter, Result};
use std::str::FromStr;

use anyhow::anyhow;
use clap::ValueEnum;

pub use crate::backend::{Backend, BackendBuilder};
pub use crate::query_handler::{QueryHandler, SetBehavior};
pub use crate::upstream_database::{
    UpstreamConfig, UpstreamDatabase, UpstreamDestination, UpstreamPrepare,
};
pub use crate::views_synchronizer::ViewsSynchronizer;

/// If ReadySet should run in standalone mode with just the adapter.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, ValueEnum)]
pub enum DeploymentMode {
    /// The entire ReadySet stack should be run in a single process (adapter and server).
    #[value(name = "standalone")]
    #[default]
    Standalone,

    /// This process should execute as an adapter with embedded readers.
    #[value(name = "embedded-readers")]
    EmbeddedReaders,

    /// This process should execute as an adapter with no locally cached data.
    #[value(name = "adapter")]
    Adapter,
}

impl DeploymentMode {
    pub fn is_standalone(&self) -> bool {
        matches!(self, DeploymentMode::Standalone)
    }

    pub fn is_embedded_readers(&self) -> bool {
        matches!(self, DeploymentMode::EmbeddedReaders)
    }

    /// Will this process be running reader nodes?
    pub fn has_reader_nodes(&self) -> bool {
        matches!(
            self,
            DeploymentMode::Standalone | DeploymentMode::EmbeddedReaders
        )
    }
}

impl FromStr for DeploymentMode {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "standalone" => Ok(DeploymentMode::Standalone),
            "embedded-readers" => Ok(DeploymentMode::EmbeddedReaders),
            "adapter" => Ok(DeploymentMode::Adapter),
            other => Err(anyhow!("Invalid deployment-mode type: {}", other)),
        }
    }
}

impl Display for DeploymentMode {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        match self {
            DeploymentMode::Standalone => write!(f, "standalone"),
            DeploymentMode::EmbeddedReaders => write!(f, "embedded-readers"),
            DeploymentMode::Adapter => write!(f, "adapter"),
        }
    }
}
