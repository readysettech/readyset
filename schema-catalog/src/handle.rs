//! This private module is basically just here to cordon off updates to the schema catalog, making
//! it only possible for the synchronizer to update it.

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

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
    /// Streaming source for schema catalog updates (e.g., via SSE). Returns a stream that yields
    /// [`SchemaCatalogUpdate`] messages. Implementors must ensure the first event on a new stream
    /// is a complete snapshot of the current catalog, so no separate fetch/poll is needed.
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
    pub fn new(controller: P) -> (Self, SchemaCatalogHandle) {
        let handle = SchemaCatalogHandle::new();
        let synchronizer = SchemaCatalogSynchronizer {
            controller,
            handle: handle.clone(),
        };
        (synchronizer, handle)
    }

    pub async fn run(mut self, mut shutdown_recv: ShutdownReceiver) {
        const BASE_RECONNECT_DELAY: Duration = Duration::from_millis(500);
        const MAX_RECONNECT_DELAY: Duration = Duration::from_secs(30);

        let mut updates_stream = self.controller.schema_catalog_update_stream();
        let mut reconnect_delay = BASE_RECONNECT_DELAY;

        loop {
            select! {
                biased;
                _ = shutdown_recv.recv() => {
                    info!("Schema Catalog Synchronizer shutting down");
                    return;
                }
                update = updates_stream.next() => {
                    match update {
                        Some(update) => match SchemaCatalog::try_from(update) {
                            Ok(catalog) => {
                                reconnect_delay = BASE_RECONNECT_DELAY;
                                self.apply_update(catalog).await;
                            }
                            Err(error) => {
                                metrics::counter!(crate::metrics::SCHEMA_CATALOG_DECODE_FAILED).increment(1);
                                warn!(%error, "Failed to decode schema catalog update");
                            }
                        },
                        None => {
                            metrics::counter!(crate::metrics::SCHEMA_CATALOG_STREAM_RECONNECTED).increment(1);
                            warn!(
                                delay_ms = reconnect_delay.as_millis() as u64,
                                "Schema catalog update stream ended; re-subscribing after backoff"
                            );
                            tokio::time::sleep(reconnect_delay).await;
                            reconnect_delay =
                                (reconnect_delay * 2).min(MAX_RECONNECT_DELAY);
                            updates_stream = self.controller.schema_catalog_update_stream();
                        }
                    }
                }
            }
        }
    }

    async fn apply_update(&self, catalog: SchemaCatalog) {
        trace!(
            generation = %catalog.generation,
            base_tables = ?catalog.base_schemas.keys().collect::<Vec<_>>(),
            uncompiled_views = ?catalog.uncompiled_views,
            custom_types = ?catalog.custom_types.keys().collect::<Vec<_>>(),
            view_schemas = ?catalog.view_schemas.iter().collect::<Vec<_>>(),
            non_replicated_relations = ?catalog.non_replicated_relations,
            "Received schema catalog from server"
        );

        let current_generation = {
            let cache = self.handle.inner.read().await;
            cache.as_ref().map(|c| c.generation)
        };

        if let Some(current) = current_generation
            && current != catalog.generation
            && !current.precedes(catalog.generation)
        {
            metrics::counter!(crate::metrics::SCHEMA_CATALOG_UNEXPECTED_GENERATION).increment(1);
            warn!(
                new_generation = %catalog.generation,
                current_generation = %current,
                "Schema update had unexpected generation"
            );
        }

        debug!(
            generation = %catalog.generation,
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
            if let Some(ref current) = *cache
                && current.generation == catalog.generation
            {
                warn!(
                    generation = %catalog.generation,
                    "Schema catalog content changed without generation advancing"
                );
            }
            metrics::counter!(crate::metrics::SCHEMA_CATALOG_UPDATE_APPLIED).increment(1);
            metrics::gauge!(crate::metrics::SCHEMA_CATALOG_CURRENT_GENERATION)
                .set(catalog.generation.get() as f64);
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

    /// A retrying wrapper around [`get_catalog`] for convenience. The catalog is populated by the
    /// SSE stream from the server; this retries in case the first stream event hasn't arrived yet
    /// (e.g., the server is still starting).
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
    use crate::SchemaGeneration;
    use async_trait::async_trait;
    use futures_util::stream;
    use readyset_util::shutdown;
    use std::collections::{HashMap, HashSet};
    use std::time::Duration;

    struct MockProvider {
        catalog: SchemaCatalog,
    }

    impl MockProvider {
        fn new() -> Self {
            Self {
                catalog: SchemaCatalog {
                    generation: SchemaGeneration::new(42).unwrap(),
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
    impl SchemaCatalogProvider for MockProvider {
        fn schema_catalog_update_stream(
            &mut self,
        ) -> Pin<Box<dyn futures_util::Stream<Item = SchemaCatalogUpdate> + Send>> {
            let update =
                SchemaCatalogUpdate::try_from(&self.catalog).expect("serialization failed");
            Box::pin(stream::once(async move { update }))
        }
    }

    struct EmptyStreamProvider;

    #[async_trait]
    impl SchemaCatalogProvider for EmptyStreamProvider {
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
        let mock = MockProvider::new();
        let (_synchronizer, handle) = SchemaCatalogSynchronizer::new(mock);
        assert!(!handle.has_catalog().await);
    }

    #[tokio::test]
    async fn test_synchronizer_run_receives_stream_update() {
        let mock = MockProvider::new();
        let (synchronizer, handle) = SchemaCatalogSynchronizer::new(mock);

        assert!(!handle.has_catalog().await);

        let (shutdown_tx, shutdown_rx) = shutdown::channel();
        tokio::spawn(synchronizer.run(shutdown_rx));

        // The stream delivers one update; wait for the synchronizer to apply it.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(handle.has_catalog().await);
        let catalog = handle.get_catalog().await.unwrap();
        assert_eq!(catalog.generation, SchemaGeneration::new(42).unwrap());

        shutdown_tx.shutdown().await;
    }

    #[tokio::test]
    async fn test_synchronizer_shutdown_before_update() {
        let mock = EmptyStreamProvider;
        let (synchronizer, handle) = SchemaCatalogSynchronizer::new(mock);

        let (shutdown_tx, shutdown_rx) = shutdown::channel();
        let join = tokio::spawn(synchronizer.run(shutdown_rx));

        // Shut down before any update arrives
        shutdown_tx.shutdown().await;
        join.await.unwrap();

        assert!(!handle.has_catalog().await);
    }
}
