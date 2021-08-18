#![warn(clippy::dbg_macro)]
#![feature(box_syntax, box_patterns)]
#![feature(nll)]
#![feature(allow_fail)]
#![feature(drain_filter)]
#![feature(async_closure)]

#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate tracing;

pub mod backend;
mod convert;
mod rewrite;
mod schema;
mod upstream_database;
mod utils;

pub use crate::backend::error::Error;
pub use crate::backend::{Backend, BackendBuilder};
pub use crate::schema::Schema;
pub use crate::upstream_database::UpstreamDatabase;
