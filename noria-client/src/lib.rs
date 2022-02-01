#![feature(box_syntax, box_patterns)]
#![feature(nll)]
#![feature(allow_fail)]
#![feature(drain_filter)]
#![feature(async_closure)]
#![feature(never_type)]
#![feature(is_sorted)]
#![feature(if_let_guard)]
#![feature(bool_to_option)]

pub mod backend;
pub mod http_router;
#[allow(dead_code)]
pub mod migration_handler;
pub mod outputs_synchronizer;
mod query_handler;
#[allow(dead_code)] // TODO(ENG-685): Remove when utilized in main.
pub mod query_status_cache;
pub mod rewrite;
pub mod upstream_database;
mod utils;

pub use crate::backend::{Backend, BackendBuilder};
pub use crate::outputs_synchronizer::OutputsSynchronizer;
pub use crate::query_handler::QueryHandler;
pub use crate::upstream_database::{UpstreamDatabase, UpstreamPrepare};
