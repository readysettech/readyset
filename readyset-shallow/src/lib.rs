use std::mem::size_of;
use std::sync::Arc;
use std::time::Duration;

mod cache;
mod manager;

use readyset_util::SizeOf;

pub use cache::CacheInfo;
pub use manager::{CacheInsertGuard, CacheManager, CacheResult};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MySqlMetadata {
    pub columns: Arc<[mysql_async::Column]>,
}

impl SizeOf for MySqlMetadata {
    fn deep_size_of(&self) -> usize {
        let mut sz = size_of::<Self>();
        for c in self.columns.iter() {
            sz += size_of::<mysql_async::Column>();
            sz += c.schema_ref().len()
                + c.table_ref().len()
                + c.org_table_ref().len()
                + c.name_ref().len()
                + c.org_name_ref().len();
        }
        sz
    }

    fn is_empty(&self) -> bool {
        false
    }
}

#[derive(Default, Debug, Clone, PartialEq, Eq)]
pub struct PostgreSqlMetadata {
    pub schema: Vec<psql_srv::Column>,
    pub types: Vec<tokio_postgres::types::Type>,
}

impl SizeOf for PostgreSqlMetadata {
    fn deep_size_of(&self) -> usize {
        self.schema.deep_size_of() + self.types.len() * size_of::<tokio_postgres::types::Type>()
    }

    fn is_empty(&self) -> bool {
        false
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryMetadata {
    MySql(MySqlMetadata),
    PostgreSql(PostgreSqlMetadata),
    #[cfg(test)]
    Test,
}

impl SizeOf for QueryMetadata {
    fn deep_size_of(&self) -> usize {
        size_of::<Self>()
            + match self {
                QueryMetadata::MySql(meta) => meta.deep_size_of(),
                QueryMetadata::PostgreSql(meta) => meta.deep_size_of(),
                #[cfg(test)]
                QueryMetadata::Test => 0,
            }
    }

    fn is_empty(&self) -> bool {
        false
    }
}

#[derive(Debug, Clone)]
pub struct QueryResult<V> {
    pub values: Arc<Vec<V>>,
    pub metadata: Arc<QueryMetadata>,
}

#[derive(Debug, Clone, Copy)]
pub enum EvictionPolicy {
    Ttl { ttl: Duration },
    TtlAndPeriod { ttl: Duration, refresh: Duration },
}

impl EvictionPolicy {
    pub fn ttl_ms(&self) -> u64 {
        match self {
            Self::Ttl { ttl } | Self::TtlAndPeriod { ttl, .. } => {
                ttl.as_millis().try_into().unwrap_or(u64::MAX)
            }
        }
    }

    pub fn refresh_ms(&self) -> u64 {
        match self {
            Self::Ttl { ttl } => ttl.as_millis().try_into().unwrap_or(u64::MAX) / 2,
            Self::TtlAndPeriod { refresh, .. } => {
                refresh.as_millis().try_into().unwrap_or(u64::MAX)
            }
        }
    }
}

#[cfg(test)]
mod test;
