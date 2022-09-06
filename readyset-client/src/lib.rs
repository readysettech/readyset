#![feature(box_syntax, box_patterns)]
#![feature(drain_filter)]
#![feature(async_closure)]
#![feature(never_type)]
#![feature(is_sorted)]
#![feature(if_let_guard)]
#![deny(unreachable_pub)]

pub mod backend;
pub mod http_router;
pub mod migration_handler;
pub mod outputs_synchronizer;
mod query_handler;
pub mod query_status_cache;
pub mod rewrite;
pub mod upstream_database;
mod utils;

pub use crate::backend::{Backend, BackendBuilder};
pub use crate::outputs_synchronizer::OutputsSynchronizer;
pub use crate::query_handler::QueryHandler;
pub use crate::upstream_database::{UpstreamDatabase, UpstreamPrepare};
