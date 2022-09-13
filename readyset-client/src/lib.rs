#![feature(box_syntax, box_patterns)]
#![feature(drain_filter)]
#![feature(async_closure)]
#![feature(never_type)]
#![feature(is_sorted)]
#![feature(if_let_guard)]
#![feature(arc_unwrap_or_clone)]
#![feature(generic_associated_types)]
#![deny(unreachable_pub)]

pub mod backend;
pub mod health_reporter;
pub mod http_router;
pub mod migration_handler;
mod query_handler;
pub mod query_status_cache;
pub mod rewrite;
pub mod upstream_database;
mod utils;
pub mod views_synchronizer;

pub use crate::backend::{Backend, BackendBuilder};
pub use crate::query_handler::{QueryHandler, SetBehavior};
pub use crate::upstream_database::{UpstreamDatabase, UpstreamPrepare};
pub use crate::views_synchronizer::ViewsSynchronizer;
