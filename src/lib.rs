#![feature(box_syntax, box_patterns)]
#![feature(nll)]
#![feature(try_from)]
#![feature(allow_fail)]

extern crate noria;
extern crate arccstr;
extern crate chrono;
#[macro_use]
extern crate failure;
extern crate msql_srv;
extern crate nom_sql;
extern crate regex;

#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate slog;

mod convert;
mod referred_tables;
mod rewrite;
mod schema;
mod backend;
mod utils;

pub use schema::Schema;
pub use backend::NoriaBackend;
