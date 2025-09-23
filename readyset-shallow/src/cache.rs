use std::fmt::{Debug, Formatter};
use std::hash::Hash;
use std::sync::{Arc, OnceLock};

use moka::sync::Cache as MokaCache;

use readyset_client::query::QueryId;
use readyset_sql::ast::Relation;

use crate::manager::*;
use crate::{EvictionPolicy, QueryMetadata, QueryResult};

#[derive(Debug, Clone)]
pub struct CacheEntry {
    pub values: Arc<Values>,
    pub metadata: Option<Arc<QueryMetadata>>,
}

pub struct Cache<K> {
    _policy: EvictionPolicy,
    results: MokaCache<K, Arc<CacheEntry>>,
    cache_metadata: OnceLock<Arc<QueryMetadata>>,
    relation: Option<Relation>,
    query_id: Option<QueryId>,
}

impl<K> Debug for Cache<K>
where
    K: Eq + Hash + Send + Sync + 'static,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Cache")
            .field("relation", &self.relation)
            .field("query_id", &self.query_id)
            .finish_non_exhaustive()
    }
}

impl<K> Cache<K>
where
    K: Eq + Hash + Send + Sync + 'static,
{
    pub(crate) fn new(
        policy: EvictionPolicy,
        relation: Option<Relation>,
        query_id: Option<QueryId>,
    ) -> Self {
        Self {
            _policy: policy,
            results: MokaCache::new(10_000),
            cache_metadata: Default::default(),
            relation,
            query_id,
        }
    }

    pub fn insert(&self, k: K, v: Values, metadata: QueryMetadata) {
        let metadata = if let Some(existing) = self.cache_metadata.get() {
            if existing.as_ref() == &metadata {
                None
            } else {
                Some(Arc::new(metadata))
            }
        } else {
            let metadata = Arc::new(metadata);
            if let Err(existing) = self.cache_metadata.set(Arc::clone(&metadata)) {
                if existing.as_ref() == metadata.as_ref() {
                    None
                } else {
                    Some(metadata)
                }
            } else {
                None
            }
        };

        let entry = Arc::new(CacheEntry {
            values: Arc::new(v),
            metadata,
        });
        self.results.insert(k, entry);
    }

    pub fn get(&self, k: &K) -> Option<QueryResult> {
        self.results.get(k).map(|entry| {
            let metadata = entry
                .metadata
                .as_ref()
                .or_else(|| self.cache_metadata.get())
                .expect("No metadata available for cached result");

            QueryResult {
                values: Arc::clone(&entry.values),
                metadata: Arc::clone(metadata),
            }
        })
    }

    pub fn relation(&self) -> &Option<Relation> {
        &self.relation
    }

    pub fn query_id(&self) -> &Option<QueryId> {
        &self.query_id
    }
}
