#![feature(never_type, hash_raw_entry)]

pub(crate) mod mysql_connector;
pub(crate) mod noria_adapter;
pub(crate) mod postgres_connector;

pub use mysql_connector::BinlogPosition;
pub use noria_adapter::{Config, NoriaAdapter};
pub use postgres_connector::PostgresPosition;
