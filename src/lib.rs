#![feature(box_syntax, box_patterns)]

extern crate distributary;
extern crate msql_srv;
extern crate nom_sql;
extern crate regex;

#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate slog;

mod soup_backend;
mod utils;

pub use soup_backend::SoupBackend;
