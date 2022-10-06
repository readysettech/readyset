//! The fallback cache provides a thread-safe backup cache for queries that we can't parse, or
//! otherwise support in readyset-server.
//!
//! For now this is just a POC, and isn't intended for use by customers.
use dashmap::DashMap;

/// A cache of all queries that we can't currently parse.
#[derive(Debug, Clone)]
pub struct FallbackCache<R: Clone> {
    /// A thread-safe hash map that holds a cache of unparsed and unsupported queries to their
    /// repsective QueryResult.
    queries: DashMap<String, R>,
}

impl<R: Clone> Default for FallbackCache<R> {
    fn default() -> Self {
        Self::new()
    }
}

impl<R> FallbackCache<R>
where
    R: Clone,
{
    /// Constructs a new FallbackCache.
    pub fn new() -> FallbackCache<R> {
        FallbackCache {
            queries: DashMap::new(),
        }
    }

    /// Inserts a query along with it's upstream query result into the cache.
    pub fn insert(&self, q: String, result: R) {
        self.queries.insert(q, result);
    }

    /// Clear all cached queries.
    pub fn clear(&self) {
        self.queries.clear()
    }

    /// Retrieves the results for a query based on a given query string.
    pub fn get(&self, query: &str) -> Option<R> {
        self.queries.get(query).map(|r| (*r).clone())
    }
}
