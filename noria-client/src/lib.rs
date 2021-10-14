#![warn(clippy::dbg_macro)]
#![feature(box_syntax, box_patterns)]
#![feature(nll)]
#![feature(allow_fail)]
#![feature(drain_filter)]
#![feature(async_closure)]
#![feature(never_type)]
#![feature(is_sorted)]

pub mod backend;
mod convert;
pub mod coverage;
mod query_handler;
#[allow(dead_code)]
pub mod query_reconciler;
#[allow(dead_code)] // TODO(ENG-685): Remove when utilized in main.
pub mod query_status_cache;
pub mod rewrite;
pub mod test_helpers;
mod upstream_database;
mod utils;

pub use crate::backend::{Backend, BackendBuilder};
pub use crate::query_handler::QueryHandler;
pub use crate::upstream_database::{UpstreamDatabase, UpstreamPrepare};
