use std::fmt::{Debug, Formatter};
use std::hash::Hash;
use std::sync::Arc;
use std::sync::Mutex;

use papaya::HashMap;
use tracing::info;

use readyset_client::query::QueryId;
use readyset_data::DfValue;
use readyset_errors::{ReadySetError, ReadySetResult, internal};
use readyset_sql::ast::Relation;

use crate::cache::Cache;
use crate::{EvictionPolicy, QueryMetadata};

pub type Row = Vec<DfValue>;
pub type Values = Vec<Vec<DfValue>>;

pub struct CacheManager<K> {
    caches: HashMap<u64, Arc<Cache<K>>>,
    names: HashMap<Relation, u64>,
    query_ids: HashMap<QueryId, u64>,
    // This lock also synchronizes inserts into the three HashMaps.
    next_id: Mutex<u64>,
}

// #[derive(Default)] adds a K: Default bound, which we don't want.
impl<K> Default for CacheManager<K>
where
    K: Hash + Eq + Send + Sync + 'static,
{
    fn default() -> Self {
        Self {
            caches: Default::default(),
            names: Default::default(),
            query_ids: Default::default(),
            next_id: Default::default(),
        }
    }
}

impl<K> CacheManager<K>
where
    K: Hash + Eq + Send + Sync + 'static,
{
    pub fn new() -> Self {
        Self::default()
    }

    fn check_identifiers(
        relation: Option<&Relation>,
        query_id: Option<&QueryId>,
    ) -> ReadySetResult<()> {
        if relation.is_some() || query_id.is_some() {
            Ok(())
        } else {
            internal!("no query id or name for cache");
        }
    }

    fn format_name(relation: Option<&Relation>, query_id: Option<&QueryId>) -> String {
        relation
            .map(|r| r.name.to_string())
            .or_else(|| query_id.map(|q| q.to_string()))
            .unwrap()
    }

    fn get_cache_id(&self, relation: Option<&Relation>, query_id: Option<&QueryId>) -> Option<u64> {
        if let Some(relation) = relation {
            let guard = self.names.pin();
            if let Some(id) = guard.get(relation) {
                return Some(*id);
            }
        }

        if let Some(query_id) = query_id {
            let guard = self.query_ids.pin();
            if let Some(id) = guard.get(query_id) {
                return Some(*id);
            }
        }

        None
    }

    pub fn create(
        &self,
        relation: Option<Relation>,
        query_id: Option<QueryId>,
        policy: EvictionPolicy,
    ) -> ReadySetResult<()> {
        Self::check_identifiers(relation.as_ref(), query_id.as_ref())?;
        let name = Self::format_name(relation.as_ref(), query_id.as_ref());

        let mut next_id = self
            .next_id
            .lock()
            .expect("couldn't lock next_id to create");
        let id = *next_id;
        *next_id += 1;

        if self
            .get_cache_id(relation.as_ref(), query_id.as_ref())
            .is_some()
        {
            return Err(ReadySetError::ViewAlreadyExists(name));
        }

        let cache = Arc::new(Cache::new(policy, relation.clone(), query_id));

        if let Some(relation) = relation {
            let guard = self.names.pin();
            guard.insert(relation, id);
        }
        if let Some(query_id) = query_id {
            let guard = self.query_ids.pin();
            guard.insert(query_id, id);
        }

        self.caches.pin().insert(id, cache);

        info!("created shallow cache {name}");
        Ok(())
    }

    pub fn drop(
        &self,
        relation: Option<&Relation>,
        query_id: Option<&QueryId>,
    ) -> ReadySetResult<()> {
        Self::check_identifiers(relation, query_id)?;
        let name = Self::format_name(relation, query_id);

        let _lock = self.next_id.lock().expect("couldn't lock next_id to drop");

        let Some(id) = self.get_cache_id(relation, query_id) else {
            return Err(ReadySetError::ViewNotFound(name));
        };

        let guard = self.caches.pin();
        let cache = guard.get(&id).ok_or_else(|| {
            let name = Self::format_name(relation, query_id);
            ReadySetError::ViewNotFound(name)
        })?;

        if let Some(relation) = cache.relation() {
            let relations_guard = self.names.pin();
            relations_guard.remove(relation);
        }
        if let Some(query_id) = cache.query_id() {
            let queries_guard = self.query_ids.pin();
            queries_guard.remove(query_id);
        }

        guard.remove(&id);

        info!("dropped shallow cache {name}");
        Ok(())
    }

    fn get(
        &self,
        relation: Option<&Relation>,
        query_id: Option<&QueryId>,
    ) -> Option<Arc<Cache<K>>> {
        let Ok(()) = Self::check_identifiers(relation, query_id) else {
            return None;
        };
        let id = self.get_cache_id(relation, query_id)?;
        self.caches.pin().get(&id).cloned()
    }

    pub fn get_or_start_insert(&self, query_id: &QueryId, key: K) -> CacheResult<K> {
        let Some(cache) = self.get(None, Some(query_id)) else {
            return CacheResult::NotCached;
        };
        if let Some(result) = cache.get(&key) {
            CacheResult::Hit(result)
        } else {
            CacheResult::Miss(CacheInsertGuard {
                cache,
                key: Some(key),
                results: Some(Vec::new()),
                metadata: None,
                filled: false,
            })
        }
    }
}

pub enum CacheResult<K>
where
    K: Hash + Eq + Send + Sync + 'static,
{
    NotCached,
    Miss(CacheInsertGuard<K>),
    Hit(crate::QueryResult),
}

impl<K> Debug for CacheResult<K>
where
    K: Hash + Eq + Send + Sync + 'static,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotCached => f.debug_struct("NotCached").finish_non_exhaustive(),
            Self::Miss(..) => f.debug_struct("Miss").finish_non_exhaustive(),
            Self::Hit(..) => f.debug_struct("Hit").finish_non_exhaustive(),
        }
    }
}

pub struct CacheInsertGuard<K>
where
    K: Hash + Eq + Send + Sync + 'static,
{
    cache: Arc<Cache<K>>,
    key: Option<K>,
    results: Option<Values>,
    metadata: Option<QueryMetadata>,
    filled: bool,
}

impl<K> Debug for CacheInsertGuard<K>
where
    K: Hash + Eq + Send + Sync + 'static,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CacheInsertGuard")
            .field("results", &self.results.as_ref().unwrap().len())
            .field("filled", &self.filled)
            .finish_non_exhaustive()
    }
}

impl<K> CacheInsertGuard<K>
where
    K: Hash + Eq + Send + Sync + 'static,
{
    pub fn filled(&mut self) {
        self.filled = true;
    }

    pub fn push(&mut self, row: Row) {
        self.results.as_mut().unwrap().push(row);
    }

    pub fn set_metadata(&mut self, metadata: QueryMetadata) {
        self.metadata = Some(metadata);
    }
}

impl<K> Drop for CacheInsertGuard<K>
where
    K: Hash + Eq + Send + Sync + 'static,
{
    fn drop(&mut self) {
        if self.filled {
            let Some(metadata) = self.metadata.take() else {
                panic!("no metadata for result set")
            };
            self.cache.insert(
                self.key.take().unwrap(),
                self.results.take().unwrap(),
                metadata,
            );
        }
    }
}
