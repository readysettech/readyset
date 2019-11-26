#![feature(box_syntax, box_patterns)]
#![feature(nll)]
#![feature(allow_fail)]

#[macro_use]
extern crate failure;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate tracing;

mod backend;
mod convert;
mod referred_tables;
mod rewrite;
mod schema;
mod utils;

pub use crate::backend::NoriaBackend;
pub use crate::schema::Schema;
