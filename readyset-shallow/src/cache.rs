use std::fmt::{Debug, Formatter};
use std::hash::Hash;
use std::sync::{Arc, OnceLock};

use dashmap::DashMap;

use readyset_client::query::QueryId;
use readyset_sql::ast::Relation;

use crate::manager::*;
use crate::{EvictionPolicy, QueryMetadata, QueryResult};

pub struct Cache<K> {
    _policy: EvictionPolicy,
    results: DashMap<K, (Arc<Values>, Option<Arc<QueryMetadata>>)>,
    cache_metadata: OnceLock<Arc<QueryMetadata>>,
    relation: Option<Relation>,
    query_id: Option<QueryId>,
}

impl<K> Debug for Cache<K>
where
    K: Eq + Hash,
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
    K: Eq + Hash,
{
    pub(crate) fn new(
        policy: EvictionPolicy,
        relation: Option<Relation>,
        query_id: Option<QueryId>,
    ) -> Self {
        Self {
            _policy: policy,
            results: Default::default(),
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

        self.results.insert(k, (Arc::new(v), metadata));
    }

    pub fn get(&self, k: &K) -> Option<QueryResult> {
        self.results.get(k).map(|entry| {
            let (values, metadata) = entry.value();
            let metadata = metadata
                .as_ref()
                .or_else(|| self.cache_metadata.get())
                .expect("No metadata available for cached result");

            QueryResult {
                values: Arc::clone(values),
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
