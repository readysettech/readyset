//! The fallback cache provides a thread-safe backup cache for queries that we can't parse, or
//! otherwise support in readyset-server.
//!
//! For now this is just a POC, and isn't intended for use by customers.
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use dashmap::DashMap;

// TODO: Also model SSD speeds as that may be more likely used.
/// This is naively based on averages for spinning disk found on Google. Generally standard HDD
/// read/write rates seem to be listed as 80-160 MB/s, so this is the median of that range (120
/// MB/s). Since this is for rough approximation benchmarks, this is probably fine.
const HDD_BYTES_PER_SEC: f64 = 125_829_120.0;

/// A cache of all queries that we can't currently parse.
#[derive(Debug, Clone)]
pub struct SimpleFallbackCache<R: Clone + Sized + Send + Sync> {
    /// A thread-safe hash map that holds a cache of unparsed and unsupported queries to their
    /// repsective QueryResult.
    queries: DashMap<String, QueryResult<R>>,
    /// The configured ttl for all queries cached in the FallbackCache.
    ttl: Duration,
}

#[derive(Debug, Clone)]
pub struct QueryResult<R: Clone + Sized + Send + Sync> {
    /// The query results that were last cached from the upstream database.
    result: R,
    /// The time this query was last cached in the FallbackCache. Used in tandem with the ttl to
    /// determine when to refresh the queries result set.
    last_cached: Instant,
}

#[async_trait]
pub trait FallbackCacheApi<R: Clone + Sized + Send + Sync> {
    /// Inserts a query along with it's upstream query result into the cache.
    async fn insert(&mut self, q: String, result: R);

    /// Clear all cached queries.
    async fn clear(&self);

    /// Retrieves the results for a query based on a given query string.
    async fn get(&self, query: &str) -> Option<R>;

    /// Revokes a query from the cache.
    async fn revoke(&self, query: &str);
}

#[derive(Debug, Clone)]
pub enum FallbackCache<R: Clone + Sized + Send + Sync> {
    Simple(SimpleFallbackCache<R>),
    Disk(DiskModeledCache<R>),
    Eviction(EvictionModeledCache<R>),
}

#[async_trait]
impl<R> FallbackCacheApi<R> for FallbackCache<R>
where
    R: Clone + Sized + Send + Sync,
{
    async fn insert(&mut self, q: String, result: R) {
        match self {
            FallbackCache::Simple(s) => s.insert(q, result).await,
            FallbackCache::Disk(d) => d.insert(q, result).await,
            FallbackCache::Eviction(e) => e.insert(q, result).await,
        }
    }

    async fn clear(&self) {
        match self {
            FallbackCache::Simple(s) => s.clear().await,
            FallbackCache::Disk(d) => d.clear().await,
            FallbackCache::Eviction(e) => e.clear().await,
        }
    }

    async fn get(&self, query: &str) -> Option<R> {
        match self {
            FallbackCache::Simple(s) => s.get(query).await,
            FallbackCache::Disk(d) => d.get(query).await,
            FallbackCache::Eviction(e) => e.get(query).await,
        }
    }

    async fn revoke(&self, query: &str) {
        match self {
            FallbackCache::Simple(s) => s.revoke(query).await,
            FallbackCache::Disk(d) => d.revoke(query).await,
            FallbackCache::Eviction(e) => e.revoke(query).await,
        }
    }
}

impl<R: Clone + Sized + Send + Sync> From<SimpleFallbackCache<R>> for FallbackCache<R> {
    fn from(simple: SimpleFallbackCache<R>) -> Self {
        FallbackCache::Simple(simple)
    }
}

impl<R: Clone + Sized + Send + Sync> From<DiskModeledCache<R>> for FallbackCache<R> {
    fn from(disk: DiskModeledCache<R>) -> Self {
        FallbackCache::Disk(disk)
    }
}

impl<R: Clone + Sized + Send + Sync> From<EvictionModeledCache<R>> for FallbackCache<R> {
    fn from(eviction: EvictionModeledCache<R>) -> Self {
        FallbackCache::Eviction(eviction)
    }
}

impl<R> SimpleFallbackCache<R>
where
    R: Clone + Sized + Send + Sync,
{
    /// Constructs a new FallbackCache.
    pub fn new(ttl: Duration) -> SimpleFallbackCache<R> {
        SimpleFallbackCache {
            queries: DashMap::new(),
            ttl,
        }
    }

    /// Converts the inner dashmap into an std::collections::HashMap.
    fn current_size(&self) -> usize {
        self.queries
            .iter()
            .map(|r| {
                std::mem::size_of_val::<[u8]>(r.key().clone().as_bytes())
                    + std::mem::size_of_val::<QueryResult<R>>(r.value())
            })
            .sum::<usize>()
    }
}

#[async_trait]
impl<R> FallbackCacheApi<R> for SimpleFallbackCache<R>
where
    R: Clone + Sized + Send + Sync,
{
    /// Inserts a query along with it's upstream query result into the cache.
    async fn insert(&mut self, q: String, result: R) {
        self.queries.insert(
            q,
            QueryResult {
                result,
                last_cached: Instant::now(),
            },
        );
    }

    /// Clear all cached queries.
    async fn clear(&self) {
        self.queries.clear()
    }

    /// Retrieves the results for a query based on a given query string.
    async fn get(&self, query: &str) -> Option<R> {
        self.queries
            .get(query)
            .filter(|r| r.last_cached.elapsed() < self.ttl)
            .map(|r| r.result.clone())
    }

    async fn revoke(&self, query: &str) {
        self.queries.remove(query);
    }
}

#[derive(Clone, Debug)]
pub struct DiskModeledCache<R: Clone + Sized + Send + Sync> {
    cache: SimpleFallbackCache<R>,
    /// Current size of the fallback cache in bytes as of the last write to it.
    current_size: usize,
}

impl<R> DiskModeledCache<R>
where
    R: Clone + Sized + Send + Sync,
{
    /// Constructs a new DiskModeledCacheWrapper.
    pub fn new(ttl: Duration) -> DiskModeledCache<R> {
        DiskModeledCache {
            cache: SimpleFallbackCache::new(ttl),
            current_size: 0,
        }
    }

    /// Simulates an hdd by adding an async delay calculated based on the curent size of the in
    /// memory cache, and average read/write rates for spinning disk.
    ///
    /// Takes in the current already elapsed time, which is used in combination with spinning disk
    /// rates to achieve the correct offset delay.
    async fn simulate_hdd_delay(&self, elapsed: Duration) {
        let delay_time = Duration::from_secs_f64(self.current_size as f64 / HDD_BYTES_PER_SEC);
        if elapsed >= delay_time {
            return;
        }
        tokio::time::sleep(delay_time - elapsed).await;
    }
}

#[async_trait]
impl<R> FallbackCacheApi<R> for DiskModeledCache<R>
where
    R: Clone + Sized + Send + Sync,
{
    /// Inserts a query along with it's upstream query result into the cache.
    /// On each write, we update the cache current size to a field on the DiskModeledCacheWrapper
    /// struct.
    ///
    /// Simulates writing to disk each time.
    async fn insert(&mut self, q: String, result: R) {
        let start = Instant::now();
        self.cache.insert(q, result).await;
        self.current_size = self.cache.current_size();
        self.simulate_hdd_delay(start.elapsed()).await;
    }

    /// Clear all cached queries.
    async fn clear(&self) {
        let start = Instant::now();
        self.cache.clear().await;
        self.simulate_hdd_delay(start.elapsed()).await;
    }

    /// Retrieves the results for a query based on a given query string.
    ///
    /// Simulates reading off disk each time.
    async fn get(&self, query: &str) -> Option<R> {
        let start = Instant::now();
        let res = self.cache.get(query).await;
        self.simulate_hdd_delay(start.elapsed()).await;
        res
    }

    async fn revoke(&self, query: &str) {
        let start = Instant::now();
        self.cache.revoke(query).await;
        self.simulate_hdd_delay(start.elapsed()).await;
    }
}

#[derive(Clone, Debug)]
pub struct EvictionModeledCache<R: Clone + Sized + Send + Sync> {
    cache: SimpleFallbackCache<R>,
    /// The rate that we randomly evict cached queries.
    eviction_rate: f64,
    /// A counter for the number of times we've looked up a query. Used in conjunction with the
    /// eviction rate.
    lookup_counter: Arc<AtomicU64>,
}

impl<R> EvictionModeledCache<R>
where
    R: Clone + Sized + Send + Sync,
{
    /// Constructs a new EvictionModeledCache.
    pub fn new(ttl: Duration, eviction_rate: f64) -> EvictionModeledCache<R> {
        EvictionModeledCache {
            cache: SimpleFallbackCache::new(ttl),
            eviction_rate,
            lookup_counter: Arc::new(AtomicU64::new(0)),
        }
    }

    async fn maybe_evict(&self, query: &str) {
        let rate_size = (1.0 / self.eviction_rate).round() as u64;
        if self.lookup_counter.load(Relaxed) % rate_size == 0 {
            self.revoke(query).await;
        }
    }
}

#[async_trait]
impl<R> FallbackCacheApi<R> for EvictionModeledCache<R>
where
    R: Clone + Sized + Send + Sync,
{
    /// Inserts a query along with it's upstream query result into the cache.
    /// On each write, we update the cache current size to a field on the DiskModeledCacheWrapper
    /// struct.
    async fn insert(&mut self, q: String, result: R) {
        self.cache.insert(q, result).await;
    }

    /// Clear all cached queries.
    async fn clear(&self) {
        self.cache.clear().await;
    }

    /// Retrieves the results for a query based on a given query string.
    async fn get(&self, query: &str) -> Option<R> {
        self.maybe_evict(query).await;
        let res = self.cache.get(query).await;
        self.lookup_counter.fetch_add(1, Relaxed);
        res
    }

    /// Revokes a query from the underlying cache.
    async fn revoke(&self, query: &str) {
        self.cache.revoke(query).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn get_returns_none_past_ttl() {
        // Set a TTL of 0 seconds so we pass it immediately.
        let mut fallback_cache = SimpleFallbackCache::new(Duration::new(0, 0));
        let query = "SELECT * FROM t1".to_string();
        fallback_cache.insert(query.clone(), (0, 1)).await;
        assert!(fallback_cache.get(&query).await.is_none())
    }

    #[tokio::test]
    async fn get_returns_some_before_ttl() {
        let mut fallback_cache = SimpleFallbackCache::new(Duration::new(10_000, 0));
        let query = "SELECT * FROM t1".to_string();
        fallback_cache.insert(query.clone(), (0, 1)).await;
        assert!(fallback_cache.get(&query).await.is_some())
    }

    #[tokio::test]
    async fn multiple_insert_updates_results() {
        let mut fallback_cache = SimpleFallbackCache::new(Duration::new(10_000, 0));
        let query = "SELECT * FROM t1".to_string();
        fallback_cache.insert(query.clone(), (0, 1)).await;
        fallback_cache.insert(query.clone(), (1, 2)).await;
        assert_eq!(fallback_cache.get(&query).await, Some((1, 2)))
    }
}
