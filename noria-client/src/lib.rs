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
mod utils;

pub use crate::schema::Schema;
