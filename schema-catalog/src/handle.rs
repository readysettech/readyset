//! This private module is basically just here to cordon off updates to the schema catalog, making
//! it only possible for the synchronizer to update it.

use std::error::Error;
use std::sync::Arc;

use async_trait::async_trait;
use readyset_errors::{ReadySetResult, internal_err};
use readyset_util::{retry_with_exponential_backoff, shutdown::ShutdownReceiver};
use tokio::select;
use tokio::sync::RwLock;
use tracing::{debug, info, trace, warn};

use crate::SchemaCatalog;

/// A schema catalog provider that can be used to fetch the latest version of the schema catalog.
#[async_trait]
pub trait SchemaCatalogProvider: Send + Sync {
    async fn fetch_schema_catalog(&mut self)
    -> Result<SchemaCatalog, Box<dyn Error + Send + Sync>>;
}

#[derive(Debug)]
pub struct SchemaCatalogSynchronizer<P: SchemaCatalogProvider> {
    /// The Readyset connector used to query the schema catalog
    controller: P,
    /// The interval between subsequent pollings of the server for schema catalog updates
    poll_interval: std::time::Duration,
    /// The cached schema catalog, protected by RwLock for concurrent access; only updated by the
    /// synchronizer, while read access is provided by the [`SchemaCatalogHandle`].
    handle: SchemaCatalogHandle,
}

impl<P: SchemaCatalogProvider + Send + 'static> SchemaCatalogSynchronizer<P> {
    pub fn new(controller: P, poll_interval: std::time::Duration) -> (Self, SchemaCatalogHandle) {
        let handle = SchemaCatalogHandle::new();
        let synchronizer = SchemaCatalogSynchronizer {
            controller,
            poll_interval,
            handle: handle.clone(),
        };
        (synchronizer, handle)
    }

    pub async fn run(mut self, mut shutdown_recv: ShutdownReceiver) {
        let mut interval = tokio::time::interval(self.poll_interval);

        // Perform an initial poll to populate the cache immediately
        self.poll().await;

        let fut = async {
            loop {
                interval.tick().await;
                self.poll().await;
            }
        };

        select! {
            biased;
            _ = shutdown_recv.recv() => {
                info!("Schema Catalog Synchronizer shutting down after shutdown signal received");
            }
            _ = fut => unreachable!(),
        }
    }

    async fn poll(&mut self) {
        debug!("Schema catalog synchronizer polling");

        match self.controller.fetch_schema_catalog().await {
            Ok(catalog) => {
                trace!(
                    base_tables = ?catalog.base_schemas.keys().collect::<Vec<_>>(),
                    uncompiled_views = ?catalog.uncompiled_views,
                    custom_types = ?catalog.custom_types.keys().collect::<Vec<_>>(),
                    view_schemas = ?catalog.view_schemas.iter().collect::<Vec<_>>(),
                    non_replicated_relations = ?catalog.non_replicated_relations,
                    "Successfully retrieved schema catalog from server"
                );
                debug!(
                    base_tables = catalog.base_schemas.len(),
                    uncompiled_views = catalog.uncompiled_views.len(),
                    custom_types = catalog.custom_types.len(),
                    view_schemas = catalog.view_schemas.len(),
                    non_replicated_relations = catalog.non_replicated_relations.len(),
                    "Successfully retrieved schema catalog from server"
                );

                // Update the cached catalog. Check with read lock first to avoid blocking readers
                // during the comparison, then allocate outside the write lock.
                let needs_update = {
                    let cache = self.handle.inner.read().await;
                    cache.as_deref() != Some(&catalog)
                };

                if needs_update {
                    let new_arc = Arc::new(catalog);
                    let mut cache = self.handle.inner.write().await;
                    *cache = Some(new_arc);
                }
            }
            Err(error) => {
                warn!(%error, "Could not get schema catalog from server");
            }
        }
    }
}

/// A handle for read-only access the cached schema catalog. Only [`SchemaCatalogSynchronizer`] can
/// update it.
#[derive(Debug)]
pub struct SchemaCatalogHandle {
    /// The outer [`Arc`] is so the handle can be shared and readers can always get the latest
    /// version; the inner [`Arc`] is so that readers can hold on to a reference to that version
    /// until they are done with it, and the synchronizer can update it here without waiting for
    /// them to relinquish their copy.
    ///
    /// Public only in this module so the [`SchemaCatalogSynchronizer`] can update it.
    pub(in crate::handle) inner: Arc<RwLock<Option<Arc<SchemaCatalog>>>>,
}

impl SchemaCatalogHandle {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(None)),
        }
    }

    /// Get a copy of the current cached schema catalog, if available
    pub async fn get_catalog(&self) -> ReadySetResult<Arc<SchemaCatalog>> {
        self.inner
            .read()
            .await
            .clone()
            .ok_or_else(|| internal_err!("SchemaCatalog not initialized"))
    }

    /// A retrying wrapper around [`get_catalog`] for convenience, based on the default 100 ms
    /// polling interval.
    ///
    /// Likely can be removed after REA-6107 is implemented.
    ///
    /// We should only retry here if the the polling task makes its initial attempt before the
    /// server has finished starting and is able to respond.
    pub async fn get_catalog_retrying(&self) -> ReadySetResult<Arc<SchemaCatalog>> {
        retry_with_exponential_backoff!(
            { self.get_catalog().await },
            retries: 10,
            delay: 100,
            backoff: 1.2,
        )
    }

    /// Check if a schema catalog is currently cached
    pub async fn has_catalog(&self) -> bool {
        let cache = self.inner.read().await;
        cache.is_some()
    }
}

impl Default for SchemaCatalogHandle {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for SchemaCatalogHandle {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use readyset_util::shutdown;
    use std::collections::{HashMap, HashSet};
    use std::error::Error;
    use std::time::Duration;

    // Mock ReadySetHandle for testing
    struct MockReadySetHandle {
        should_fail: bool,
        catalog: SchemaCatalog,
    }

    impl MockReadySetHandle {
        fn new(should_fail: bool) -> Self {
            Self {
                should_fail,
                catalog: SchemaCatalog {
                    base_schemas: HashMap::new(),
                    uncompiled_views: Vec::new(),
                    custom_types: HashMap::new(),
                    view_schemas: HashMap::new(),
                    non_replicated_relations: HashSet::new(),
                },
            }
        }
    }

    #[async_trait]
    impl SchemaCatalogProvider for MockReadySetHandle {
        async fn fetch_schema_catalog(
            &mut self,
        ) -> Result<SchemaCatalog, Box<dyn Error + Send + Sync>> {
            if self.should_fail {
                Err("Mock error".into())
            } else {
                Ok(self.catalog.clone())
            }
        }
    }

    #[tokio::test]
    async fn test_schema_catalog_handle() {
        let handle = SchemaCatalogHandle::new();

        // Initially no catalog should be cached
        assert!(!handle.has_catalog().await);
        assert!(handle.get_catalog().await.is_err());

        // Set a catalog in the cache
        {
            let mut cache = handle.inner.write().await;
            *cache = Some(Arc::new(SchemaCatalog::new()));
        }

        // Now catalog should be available
        assert!(handle.has_catalog().await);
        handle.get_catalog().await.unwrap();
    }

    #[tokio::test]
    async fn test_synchronizer_creation() {
        let mock_handle = MockReadySetHandle::new(false);
        let poll_interval = Duration::from_millis(100);

        // Create synchronizer - this should work without panicking
        let (_synchronizer, handle) = SchemaCatalogSynchronizer::new(mock_handle, poll_interval);
        assert!(!handle.has_catalog().await);
    }

    #[tokio::test]
    async fn test_synchronizer_poll_success() {
        let mock_handle = MockReadySetHandle::new(false);
        let poll_interval = Duration::from_millis(10);

        let (mut synchronizer, handle) = SchemaCatalogSynchronizer::new(mock_handle, poll_interval);

        assert!(!handle.has_catalog().await);

        // Initial poll
        synchronizer.poll().await;

        assert!(handle.has_catalog().await);
        handle.get_catalog().await.unwrap();
    }

    #[tokio::test]
    async fn test_synchronizer_poll_failure() {
        let mock_handle = MockReadySetHandle::new(true);
        let poll_interval = Duration::from_millis(10);

        let (mut synchronizer, handle) = SchemaCatalogSynchronizer::new(mock_handle, poll_interval);

        assert!(!handle.has_catalog().await);

        synchronizer.poll().await;

        assert!(!handle.has_catalog().await);
    }

    #[tokio::test]
    async fn test_synchronizer_run_and_shutdown() {
        let mock_handle = MockReadySetHandle::new(false);
        let poll_interval = Duration::from_millis(10);
        let (synchronizer, handle) = SchemaCatalogSynchronizer::new(mock_handle, poll_interval);

        assert!(!handle.has_catalog().await);

        let (shutdown_tx, shutdown_rx) = shutdown::channel();
        tokio::spawn(synchronizer.run(shutdown_rx));

        // The initial poll happens before the loop.
        // We need to wait for the task to have run that part.
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(handle.has_catalog().await);

        shutdown_tx.shutdown().await;
    }
}
