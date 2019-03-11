#![feature(box_syntax, box_patterns)]
#![feature(nll)]
#![feature(allow_fail)]

extern crate arccstr;
extern crate chrono;
extern crate noria;
#[macro_use]
extern crate failure;
extern crate msql_srv;
extern crate nom_sql;
extern crate regex;

#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate slog;

mod backend;
mod convert;
mod referred_tables;
mod rewrite;
mod schema;
mod utils;

pub use backend::NoriaBackend;
pub use schema::Schema;
