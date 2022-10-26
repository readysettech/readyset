//! The fallback cache provides a thread-safe backup cache for queries that we can't parse, or
//! otherwise support in readyset-server.
//!
//! For now this is just a POC, and isn't intended for use by customers.
use std::time::{Duration, Instant};

use dashmap::DashMap;

/// A cache of all queries that we can't currently parse.
#[derive(Debug, Clone)]
pub struct FallbackCache<R: Clone> {
    /// A thread-safe hash map that holds a cache of unparsed and unsupported queries to their
    /// repsective QueryResult.
    queries: DashMap<String, QueryResult<R>>,
    /// The configured ttl for all queries cached in the FallbackCache.
    ttl: Duration,
}

#[derive(Debug, Clone)]
pub struct QueryResult<R: Clone> {
    /// The query results that were last cached from the upstream database.
    result: R,
    /// The time this query was last cached in the FallbackCache. Used in tandem with the ttl to
    /// determine when to refresh the queries result set.
    last_cached: Instant,
}

impl<R> FallbackCache<R>
where
    R: Clone,
{
    /// Constructs a new FallbackCache.
    pub fn new(ttl: Duration) -> FallbackCache<R> {
        FallbackCache {
            queries: DashMap::new(),
            ttl,
        }
    }

    /// Inserts a query along with it's upstream query result into the cache.
    pub fn insert(&self, q: String, result: R) {
        self.queries.insert(
            q,
            QueryResult {
                result,
                last_cached: Instant::now(),
            },
        );
    }

    /// Clear all cached queries.
    pub fn clear(&self) {
        self.queries.clear()
    }

    /// Retrieves the results for a query based on a given query string.
    pub fn get(&self, query: &str) -> Option<R> {
        self.queries
            .get(query)
            .filter(|r| r.last_cached.elapsed() < self.ttl)
            .map(|r| r.result.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_returns_none_past_ttl() {
        // Set a TTL of 0 seconds so we pass it immediately.
        let fallback_cache = FallbackCache::new(Duration::new(0, 0));
        let query = "SELECT * FROM t1".to_string();
        fallback_cache.insert(query.clone(), (0, 1));
        assert!(fallback_cache.get(&query).is_none())
    }

    #[test]
    fn get_returns_some_before_ttl() {
        let fallback_cache = FallbackCache::new(Duration::new(10_000, 0));
        let query = "SELECT * FROM t1".to_string();
        fallback_cache.insert(query.clone(), (0, 1));
        assert!(fallback_cache.get(&query).is_some())
    }

    #[test]
    fn multiple_insert_updates_results() {
        let fallback_cache = FallbackCache::new(Duration::new(10_000, 0));
        let query = "SELECT * FROM t1".to_string();
        fallback_cache.insert(query.clone(), (0, 1));
        fallback_cache.insert(query.clone(), (1, 2));
        assert_eq!(fallback_cache.get(&query), Some((1, 2)))
    }
}
