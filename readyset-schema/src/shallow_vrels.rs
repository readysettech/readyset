use std::hash::Hash;

use readyset_client::query::QueryId;
use readyset_data::DfType;
use readyset_shallow::{CacheInfo, CacheManager};
use readyset_sql::{DialectDisplay, ast::Relation};
use readyset_util::SizeOf;

use crate::bind_vrel;
use crate::virtual_relation::{VrelContext, VrelRead, VrelRows};

/// Trait for accessing shallow cache state in virtual relations.
pub trait ShallowInfo: Send + Sync {
    fn list_caches(&self, query_id: Option<QueryId>, name: Option<&Relation>) -> Vec<CacheInfo>;
}

impl<K, V> ShallowInfo for CacheManager<K, V>
where
    K: Clone + Hash + Eq + Send + Sync + SizeOf + 'static,
    V: Send + Sync + SizeOf + 'static,
{
    fn list_caches(&self, query_id: Option<QueryId>, name: Option<&Relation>) -> Vec<CacheInfo> {
        self.list_caches(query_id, name)
    }
}

const SHALLOW_CACHES_SCHEMA: &[(&str, DfType)] = &[
    ("query_id", DfType::DEFAULT_TEXT),
    ("name", DfType::DEFAULT_TEXT),
    ("query", DfType::DEFAULT_TEXT),
    ("ttl_ms", DfType::UnsignedBigInt),
    ("refresh_ms", DfType::UnsignedBigInt),
    ("coalesce_ms", DfType::UnsignedBigInt),
    ("always", DfType::Bool),
    ("schedule", DfType::Bool),
];

fn shallow_caches_read(ctx: &VrelContext) -> VrelRead {
    let dialect = ctx.dialect;
    let caches = ctx.shallow.list_caches(None, None);
    Box::pin(async move {
        let rows: VrelRows = Box::new(caches.into_iter().map(move |cache| {
            vec![
                cache.query_id.map(|id| id.to_string()).into(),
                cache.name.map(|n| n.display_unquoted().to_string()).into(),
                cache.query.display(dialect).to_string().into(),
                cache.ttl_ms.into(),
                cache.refresh_ms.into(),
                cache.coalesce_ms.into(),
                cache.always.into(),
                cache.schedule.into(),
            ]
        }));
        Ok(rows)
    })
}
bind_vrel!(shallow_caches, SHALLOW_CACHES_SCHEMA, shallow_caches_read);
