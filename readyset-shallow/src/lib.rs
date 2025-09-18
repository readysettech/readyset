use std::sync::Arc;
use std::time::Duration;

mod cache;
mod manager;

pub use manager::{CacheInsertGuard, CacheManager, CacheResult, Values};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MySqlMetadata {
    pub columns: Arc<[mysql_async::Column]>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryMetadata {
    MySql(MySqlMetadata),
    PostgreSql(()),
}

#[derive(Debug, Clone)]
pub struct QueryResult {
    pub values: Arc<Values>,
    pub metadata: Arc<QueryMetadata>,
}

pub enum EvictionPolicy {
    Ttl(Duration),
}
