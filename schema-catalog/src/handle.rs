//! This private module is basically just here to cordon off updates to the schema catalog, making
//! it only possible for the synchronizer to update it.

use std::error::Error;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
#[cfg(feature = "failure_injection")]
use failpoint_macros::set_failpoint;
use futures_util::StreamExt;
use readyset_errors::{ReadySetResult, internal_err};
use readyset_util::{retry_with_exponential_backoff, shutdown::ShutdownReceiver};
use tokio::select;
use tokio::sync::RwLock;
use tracing::{debug, info, trace, warn};

use crate::{SchemaCatalog, SchemaCatalogUpdate};

/// A schema catalog provider that can be used to fetch the latest version of the schema catalog.
#[async_trait]
pub trait SchemaCatalogProvider: Send + Sync {
    /// Ad-hoc fetch of the current schema catalog, used for initial poll.
    async fn fetch_schema_catalog(&mut self)
    -> Result<SchemaCatalog, Box<dyn Error + Send + Sync>>;

    /// Streaming source for schema catalog updates (e.g., via SSE). Returns a stream that yields
    /// [`SchemaCatalogUpdate`] messages.
    fn schema_catalog_update_stream(
        &mut self,
    ) -> Pin<Box<dyn futures_util::Stream<Item = SchemaCatalogUpdate> + Send>>;
}

#[derive(Debug)]
pub struct SchemaCatalogSynchronizer<P: SchemaCatalogProvider> {
    /// The Readyset connector used to query the schema catalog
    controller: P,
    /// The cached schema catalog, protected by RwLock for concurrent access; only updated by the
    /// synchronizer, while read access is provided by the [`SchemaCatalogHandle`].
    handle: SchemaCatalogHandle,
}

impl<P: SchemaCatalogProvider + Send + 'static> SchemaCatalogSynchronizer<P> {
    pub fn new(controller: P, _poll_interval: std::time::Duration) -> (Self, SchemaCatalogHandle) {
        let handle = SchemaCatalogHandle::new();
        let synchronizer = SchemaCatalogSynchronizer {
            controller,
            handle: handle.clone(),
        };
        (synchronizer, handle)
    }

    pub async fn run(mut self, mut shutdown_recv: ShutdownReceiver) {
        let mut updates_stream = self.controller.schema_catalog_update_stream();
        let mut initial_poll_pending = true;

        // Initial sync: accept either the first successful fetch or any stream update.
        while !self.handle.has_catalog().await {
            select! {
                biased;
                _ = shutdown_recv.recv() => {
                    info!("Schema Catalog Synchronizer shutting down before initial sync");
                    return;
                }
                update = updates_stream.next() => {
                    match update {
                        Some(update) => match SchemaCatalog::try_from(update) {
                            Ok(catalog) => {
                                self.apply_update(catalog).await;
                                initial_poll_pending = false;
                            }
                            Err(error) => {
                                warn!(%error, "Failed to decode schema catalog update during initial sync");
                            }
                        },
                        None => {
                            warn!("Schema catalog update stream ended during initial sync; re-subscribing");
                            if initial_poll_pending {
                                if let Err(error) = self.poll().await {
                                    warn!(%error, "Schema catalog fetch after stream end failed during initial sync");
                                } else {
                                    initial_poll_pending = false;
                                }
                            }
                            updates_stream = self.controller.schema_catalog_update_stream();
                        }
                    }
                }
                res = async { self.poll().await }, if initial_poll_pending => {
                    if let Err(error) = res {
                        warn!(%error, "Schema Catalog Synchronizer initial sync fetch failed");
                    }
                    initial_poll_pending = false;
                }
            }
        }

        loop {
            select! {
                biased;
                _ = shutdown_recv.recv() => {
                    info!("Schema Catalog Synchronizer shutting down after shutdown signal received");
                    return;
                }
                update = updates_stream.next() => {
                    match update {
                        Some(update) => match SchemaCatalog::try_from(update) {
                            Ok(catalog) => {
                                self.apply_update(catalog).await;
                            }
                            Err(error) => {
                                warn!(%error, "Failed to decode schema catalog update");
                            }
                        },
                        None => {
                            warn!("Schema catalog update stream ended; re-subscribing");
                            if let Err(error) = self.poll().await {
                                warn!(%error, "Schema catalog fetch after stream end failed");
                            }
                            updates_stream = self.controller.schema_catalog_update_stream();
                        }
                    }
                }
            }
        }
    }

    async fn poll(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let catalog = self.controller.fetch_schema_catalog().await?;
        self.apply_update(catalog).await;
        Ok(())
    }

    async fn apply_update(&self, catalog: SchemaCatalog) {
        trace!(
            base_tables = ?catalog.base_schemas.keys().collect::<Vec<_>>(),
            uncompiled_views = ?catalog.uncompiled_views,
            custom_types = ?catalog.custom_types.keys().collect::<Vec<_>>(),
            view_schemas = ?catalog.view_schemas.iter().collect::<Vec<_>>(),
            non_replicated_relations = ?catalog.non_replicated_relations,
            "Received schema catalog from server"
        );

        debug!(
            base_tables = catalog.base_schemas.len(),
            uncompiled_views = catalog.uncompiled_views.len(),
            custom_types = catalog.custom_types.len(),
            view_schemas = catalog.view_schemas.len(),
            non_replicated_relations = catalog.non_replicated_relations.len(),
            "Applying schema catalog update"
        );

        #[cfg(feature = "failure_injection")]
        {
            trace!("Failpoint: checking SCHEMA_CATALOG_SYNCHRONIZER_DELAY");
            set_failpoint!(readyset_util::failpoints::SCHEMA_CATALOG_SYNCHRONIZER_DELAY);
            trace!("Failpoint: passed SCHEMA_CATALOG_SYNCHRONIZER_DELAY");
        }

        let mut cache = self.handle.inner.write().await;
        if cache.as_deref() != Some(&catalog) {
            *cache = Some(Arc::new(catalog));
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
    use futures_util::stream;
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

        fn schema_catalog_update_stream(
            &mut self,
        ) -> Pin<Box<dyn futures_util::Stream<Item = SchemaCatalogUpdate> + Send>> {
            Box::pin(stream::empty())
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
        let _ = synchronizer.poll().await;

        assert!(handle.has_catalog().await);
        handle.get_catalog().await.unwrap();
    }

    #[tokio::test]
    async fn test_synchronizer_poll_failure() {
        let mock_handle = MockReadySetHandle::new(true);
        let poll_interval = Duration::from_millis(10);

        let (mut synchronizer, handle) = SchemaCatalogSynchronizer::new(mock_handle, poll_interval);

        assert!(!handle.has_catalog().await);

        let _ = synchronizer.poll().await;

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
