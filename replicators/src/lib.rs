#![feature(never_type, hash_raw_entry)]

pub(crate) mod mysql_connector;
pub(crate) mod readyset_adapter;
pub(crate) mod postgres_connector;

pub use mysql_connector::BinlogPosition;
pub use readyset_adapter::{AdapterOpts, NoriaAdapter};
pub use postgres_connector::PostgresPosition;
