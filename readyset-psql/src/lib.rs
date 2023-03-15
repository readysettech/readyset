#![feature(box_patterns, type_alias_impl_trait, generic_associated_types)]
mod backend;
mod error;
mod query_handler;
mod response;
mod resultset;
mod row;
mod schema;
mod upstream;
mod value;

pub use crate::backend::{AuthenticationMethod, Backend, ParamRef};
pub use crate::error::Error;
pub use crate::query_handler::PostgreSqlQueryHandler;
pub use crate::upstream::PostgreSqlUpstream;
pub use crate::value::Value;
