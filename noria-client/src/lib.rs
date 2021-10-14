#![warn(clippy::dbg_macro)]
#![feature(box_syntax, box_patterns)]
#![feature(nll)]
#![feature(allow_fail)]
#![feature(drain_filter)]
#![feature(async_closure)]
#![feature(never_type)]
#![cfg_attr(debug_assertions, feature(is_sorted))]

pub mod backend;
mod convert;
pub mod coverage;
mod query_handler;
mod rewrite;
pub mod test_helpers;
mod upstream_database;
mod utils;

pub use crate::backend::{Backend, BackendBuilder};
pub use crate::query_handler::QueryHandler;
pub use crate::upstream_database::{UpstreamDatabase, UpstreamPrepare};
