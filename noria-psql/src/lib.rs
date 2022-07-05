#![feature(box_patterns)]

mod backend;
mod error;
mod query_handler;
mod response;
mod resultset;
mod row;
mod schema;
mod upstream;
mod value;

pub use crate::backend::Backend;
pub use crate::error::Error;
pub use crate::query_handler::PostgreSqlQueryHandler;
pub use crate::upstream::PostgreSqlUpstream;
pub use crate::value::Value;

/// PostgreSQL-specific runtime adapter configuration
#[derive(Debug, Clone, Copy, Eq, PartialEq, Default)]
pub struct Config {
    pub disable_upstream_ssl_verification: bool,
}
