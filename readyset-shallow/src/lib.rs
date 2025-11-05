use std::sync::Arc;
use std::time::Duration;

mod cache;
mod manager;

pub use cache::CacheInfo;
pub use manager::{CacheInsertGuard, CacheManager, CacheResult};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MySqlMetadata {
    pub columns: Arc<[mysql_async::Column]>,
}

#[derive(Default, Debug, Clone, PartialEq, Eq)]
pub struct PostgreSqlMetadata {
    pub schema: Vec<psql_srv::Column>,
    pub types: Vec<tokio_postgres::types::Type>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryMetadata {
    MySql(MySqlMetadata),
    PostgreSql(PostgreSqlMetadata),
}

impl QueryMetadata {
    #[cfg(test)]
    pub fn empty() -> Self {
        QueryMetadata::MySql(MySqlMetadata {
            columns: Arc::new([]),
        })
    }
}

#[derive(Debug, Clone)]
pub struct QueryResult<V> {
    pub values: Arc<Vec<V>>,
    pub metadata: Arc<QueryMetadata>,
}

#[derive(Debug, Clone, Copy)]
pub enum EvictionPolicy {
    Ttl(Duration),
}
