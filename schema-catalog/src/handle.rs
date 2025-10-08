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
use tracing::{Instrument, debug, error, info, info_span, trace, warn};

use crate::{SchemaCatalog, SchemaCatalogUpdate, SchemaChangeHandler, SchemaChanges};

/// Payload carried by the watch channel between the synchronizer and the invalidation sidecar:
/// the latest [`SchemaCatalog`] together with the wall-clock time it was applied.
type CatalogSnapshot = (Arc<SchemaCatalog>, std::time::Instant);

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

pub struct SchemaCatalogSynchronizer<P: SchemaCatalogProvider> {
    /// The Readyset connector used to query the schema catalog
    controller: P,
    /// The cached schema catalog, protected by RwLock for concurrent access; only updated by the
    /// synchronizer, while read access is provided by the [`SchemaCatalogHandle`].
    handle: SchemaCatalogHandle,
    /// Optional handler that is notified of schema changes so it can invalidate cached query state.
    change_handler: Option<Arc<dyn SchemaChangeHandler>>,
}

impl<P: SchemaCatalogProvider + std::fmt::Debug> std::fmt::Debug for SchemaCatalogSynchronizer<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SchemaCatalogSynchronizer")
            .field("controller", &self.controller)
            .field("handle", &self.handle)
            .field("has_change_handler", &self.change_handler.is_some())
            .finish()
    }
}

impl<P: SchemaCatalogProvider + Send + 'static> SchemaCatalogSynchronizer<P> {
    pub fn new(controller: P) -> (Self, SchemaCatalogHandle) {
        let handle = SchemaCatalogHandle::new();
        let synchronizer = SchemaCatalogSynchronizer {
            controller,
            handle: handle.clone(),
            change_handler: None,
        };
        (synchronizer, handle)
    }

    /// Set a handler that will be notified when schema changes are detected, so it can invalidate
    /// cached query state for affected tables.
    pub fn with_change_handler(mut self, handler: Arc<dyn SchemaChangeHandler>) -> Self {
        self.change_handler = Some(handler);
        self
    }

    pub async fn run(mut self, mut shutdown_recv: ShutdownReceiver) {
        const BASE_RECONNECT_DELAY: Duration = Duration::from_millis(500);
        const MAX_RECONNECT_DELAY: Duration = Duration::from_secs(30);

        // If a change handler is configured, spawn a sidecar task for async
        // diffing/invalidation. The watch channel holds the latest catalog; intermediate
        // versions are automatically discarded if the sidecar falls behind.
        let (mut catalog_tx, mut sidecar_handle) = if let Some(ref handler) = self.change_handler {
            let (tx, rx) = tokio::sync::watch::channel::<Option<CatalogSnapshot>>(None);
            let handle = tokio::spawn(
                run_invalidation_sidecar(rx, Arc::clone(handler))
                    .instrument(info_span!("invalidation_sidecar")),
            );
            (Some(tx), Some(handle))
        } else {
            (None, None)
        };

        let mut updates_stream = self.controller.schema_catalog_update_stream();
        let mut reconnect_delay = BASE_RECONNECT_DELAY;

        loop {
            select! {
                biased;
                _ = shutdown_recv.recv() => {
                    info!("Schema Catalog Synchronizer shutting down");
                    // Returning drops `catalog_tx`, causing the sidecar's `changed()` to
                    // return Err and exit cleanly.
                    return;
                }
                result = async { sidecar_handle.as_mut().expect("guarded by if").await }, if sidecar_handle.is_some() => {
                    match result {
                        Ok(()) => error!("Invalidation sidecar exited unexpectedly; restarting"),
                        Err(e) => error!(%e, "Invalidation sidecar panicked; restarting"),
                    }
                    let handler = self.change_handler.as_ref()
                        .expect("change_handler present when sidecar was spawned");
                    let (tx, rx) = tokio::sync::watch::channel::<Option<CatalogSnapshot>>(None);
                    sidecar_handle = Some(tokio::spawn(
                        run_invalidation_sidecar(rx, Arc::clone(handler))
                            .instrument(info_span!("invalidation_sidecar")),
                    ));
                    catalog_tx = Some(tx);
                }
                update = updates_stream.next() => {
                    match update {
                        Some(update) => match SchemaCatalog::try_from(update) {
                            Ok(catalog) => {
                                reconnect_delay = BASE_RECONNECT_DELAY;
                                self.apply_update(catalog, catalog_tx.as_ref()).await;
                            }
                            Err(error) => {
                                metrics::counter!(crate::metrics::SCHEMA_CATALOG_DECODE_FAILED).increment(1);
                                warn!(%error, "Failed to decode schema catalog update");
                            }
                        },
                        None => {
                            // The update stream terminated. Possible causes:
                            //   1. SSE connection closed (leader loss, network error)
                            //   2. Broadcast receiver lagged (consumer too slow); the
                            //      stream implementation terminates on lag so the
                            //      synchronizer can reconnect with a fresh snapshot
                            //
                            // We apply exponential backoff in both cases. For lag specifically
                            // this is deliberate: lag means the consumer is slower than the
                            // producer, so immediate reconnection risks a lag-again loop.
                            // Each reconnection fetches a fresh snapshot, so no schema
                            // updates are permanently lost.
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

    async fn apply_update(
        &self,
        catalog: SchemaCatalog,
        catalog_tx: Option<&tokio::sync::watch::Sender<Option<CatalogSnapshot>>>,
    ) {
        trace!(
            generation = %catalog.generation,
            base_tables = ?catalog.base_schemas.keys(),
            uncompiled_views = ?catalog.uncompiled_views,
            custom_types = ?catalog.custom_types.keys(),
            view_schemas = ?catalog.view_schemas.keys(),
            non_replicated_relations = ?catalog.non_replicated_relations,
            "Received schema catalog from server"
        );

        // Generation validation is purely diagnostic (warn + antithesis assertion). It uses a
        // separate read lock so it doesn't hold the write lock longer than necessary. If this
        // ever needs to take action based on the generation (e.g. reject out-of-order updates),
        // it should be moved inside the write lock below to avoid a TOCTOU race.
        //
        // Validate generation ordering against the current cached catalog.
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
            antithesis_sdk::assert_unreachable!(
                "Schema catalog received unexpected generation",
                &serde_json::json!({
                    "new_generation": catalog.generation.get(),
                    "current_generation": current.get(),
                })
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
            let new_catalog = Arc::new(catalog);
            *cache = Some(Arc::clone(&new_catalog));

            // Send the new catalog to the sidecar for async diffing/invalidation.
            // This is non-blocking; if the sidecar is behind, intermediate values are
            // automatically coalesced by the watch channel.
            //
            // NOTE: The cache is updated above before the sidecar processes the
            // invalidation, so there is a brief window where the QSC may contain
            // stale entries that reference the old schema. This is acceptable because
            // the sidecar will catch up and invalidate promptly, and the alternative
            // (blocking on invalidation) would add latency to the synchronizer's hot
            // path. The SCHEMA_CATALOG_INVALIDATION_STALENESS metric tracks this gap.
            if let Some(tx) = catalog_tx
                && tx
                    .send(Some((new_catalog, std::time::Instant::now())))
                    .is_err()
            {
                warn!("Invalidation sidecar is gone; schema change invalidation disabled");
            }
        }
    }
}

/// Sidecar task that performs schema diffing and invalidation asynchronously, decoupled from the
/// synchronizer's hot path. Receives the latest catalog via a `watch` channel and diffs it against
/// the previously processed version.
///
/// The sidecar exits when the watch sender is dropped (i.e., when the synchronizer shuts down).
async fn run_invalidation_sidecar(
    mut catalog_rx: tokio::sync::watch::Receiver<Option<CatalogSnapshot>>,
    handler: Arc<dyn SchemaChangeHandler>,
) {
    let mut last_processed: Option<Arc<SchemaCatalog>> = None;

    loop {
        // Wait for a new catalog to arrive; returns Err when the sender is dropped.
        if catalog_rx.changed().await.is_err() {
            debug!("Invalidation sidecar shutting down (sender dropped)");
            return;
        }

        let (catalog, send_time) = {
            let borrowed = catalog_rx.borrow_and_update();
            match borrowed.as_ref() {
                Some((c, t)) => (Arc::clone(c), *t),
                None => continue, // Initial None value, skip
            }
        };

        let changes = {
            let _span = info_span!("schema_catalog_diff").entered();
            let start = std::time::Instant::now();
            let changes = match last_processed {
                Some(ref old) => old.diff(&catalog),
                None => SchemaChanges::All, // First update
            };
            let elapsed = start.elapsed();
            metrics::histogram!(crate::metrics::SCHEMA_CATALOG_DIFF_DURATION)
                .record(elapsed.as_secs_f64());
            debug!(
                elapsed_us = elapsed.as_micros() as u64,
                ?changes,
                "Schema catalog diff completed"
            );
            changes
        };

        if !matches!(&changes, SchemaChanges::None) {
            metrics::histogram!(crate::metrics::SCHEMA_CATALOG_INVALIDATION_STALENESS)
                .record(send_time.elapsed().as_secs_f64());
        }

        {
            let _span = info_span!("schema_catalog_invalidate").entered();
            let invalidation_start = std::time::Instant::now();
            match &changes {
                SchemaChanges::Relations(tables) if !tables.is_empty() => {
                    metrics::counter!(crate::metrics::SCHEMA_CATALOG_INVALIDATION_TARGETED)
                        .increment(1);
                    info!(
                        count = tables.len(),
                        ?tables,
                        "Invalidating queries for changed tables"
                    );
                    // NOTE: targeted invalidation is best-effort; schema-qualification
                    // mismatches may cause some stale entries to remain. See TODO REA-5970.
                    handler.invalidate_for_tables(tables);
                    antithesis_sdk::assert_reachable!(
                        "Targeted QSC invalidation for changed tables",
                        &serde_json::json!({"table_count": tables.len()})
                    );
                }
                SchemaChanges::All => {
                    metrics::counter!(crate::metrics::SCHEMA_CATALOG_INVALIDATION_FULL)
                        .increment(1);
                    info!("Invalidating all cached query state due to schema change");
                    handler.invalidate_all();
                    antithesis_sdk::assert_reachable!(
                        "Full QSC invalidation due to schema change",
                        &serde_json::json!({"generation": catalog.generation.get()})
                    );
                }
                SchemaChanges::None | SchemaChanges::Relations(_) => {}
            }
            let invalidation_elapsed = invalidation_start.elapsed();
            metrics::histogram!(crate::metrics::SCHEMA_CATALOG_INVALIDATION_DURATION)
                .record(invalidation_elapsed.as_secs_f64());
            debug!(
                elapsed_us = invalidation_elapsed.as_micros() as u64,
                "Schema catalog invalidation completed"
            );
        }

        last_processed = Some(catalog);
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

    /// Get a copy of the current cached schema catalog, if available.
    ///
    /// Returns an error if the catalog has not been populated yet (e.g., the SSE stream from the
    /// server has not delivered the initial catalog update).
    pub async fn get_catalog(&self) -> ReadySetResult<Arc<SchemaCatalog>> {
        self.inner.read().await.clone().ok_or_else(|| {
            trace!("SchemaCatalog requested but not yet initialized; SSE stream may not have delivered the initial update");
            internal_err!("SchemaCatalog not initialized")
        })
    }

    /// A retrying wrapper around [`get_catalog`] for convenience. The catalog is populated by the
    /// SSE stream from the server; this retries in case the first stream event hasn't arrived yet
    /// (e.g., the server is still starting).
    pub async fn get_catalog_retrying(&self) -> ReadySetResult<Arc<SchemaCatalog>> {
        let result = retry_with_exponential_backoff!(
            { self.get_catalog().await },
            retries: 10,
            delay: 100,
            backoff: 1.2,
        );
        if let Err(ref e) = result {
            warn!(
                error = %e,
                "SchemaCatalog not available after all retries; SSE stream may not have delivered the initial update"
            );
        }
        result
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
                    uncompiled_views: HashMap::new(),
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
        tokio::time::pause();
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

    /// A mock [`SchemaChangeHandler`] that records which invalidation methods were called.
    struct MockChangeHandler {
        invalidated_tables: std::sync::Mutex<Vec<Vec<readyset_sql::ast::Relation>>>,
        invalidated_all_count: std::sync::atomic::AtomicU32,
    }

    impl MockChangeHandler {
        fn new() -> Self {
            Self {
                invalidated_tables: std::sync::Mutex::new(Vec::new()),
                invalidated_all_count: std::sync::atomic::AtomicU32::new(0),
            }
        }
    }

    impl crate::SchemaChangeHandler for MockChangeHandler {
        fn invalidate_for_tables(&self, tables: &[readyset_sql::ast::Relation]) {
            self.invalidated_tables
                .lock()
                .expect("lock poisoned")
                .push(tables.to_vec());
        }

        fn invalidate_all(&self) {
            self.invalidated_all_count
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }

    /// Provider that delivers a sequence of catalogs, one per stream subscription.
    struct SequentialProvider {
        catalogs: std::sync::Mutex<Vec<SchemaCatalog>>,
    }

    #[async_trait]
    impl SchemaCatalogProvider for SequentialProvider {
        fn schema_catalog_update_stream(
            &mut self,
        ) -> Pin<Box<dyn futures_util::Stream<Item = SchemaCatalogUpdate> + Send>> {
            let catalog = self.catalogs.lock().expect("lock poisoned").pop();
            match catalog {
                Some(c) => {
                    let update = SchemaCatalogUpdate::try_from(&c).expect("serialization failed");
                    Box::pin(stream::once(async move { update }))
                }
                None => Box::pin(stream::empty()),
            }
        }
    }

    #[tokio::test]
    async fn test_first_catalog_triggers_invalidate_all() {
        tokio::time::pause();
        let handler = Arc::new(MockChangeHandler::new());

        let mock = MockProvider::new();
        let (synchronizer, handle) = SchemaCatalogSynchronizer::new(mock);
        let synchronizer = synchronizer.with_change_handler(handler.clone());

        let (shutdown_tx, shutdown_rx) = shutdown::channel();
        tokio::spawn(synchronizer.run(shutdown_rx));

        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(handle.has_catalog().await);
        assert_eq!(
            handler
                .invalidated_all_count
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );
        assert!(
            handler
                .invalidated_tables
                .lock()
                .expect("lock poisoned")
                .is_empty()
        );

        shutdown_tx.shutdown().await;
    }

    #[tokio::test]
    async fn test_changed_base_schema_triggers_targeted_invalidation() {
        tokio::time::pause();
        let handler = Arc::new(MockChangeHandler::new());

        use readyset_sql::ast::*;

        let body = CreateTableBody {
            fields: vec![ColumnSpecification {
                column: Column {
                    name: "x".into(),
                    table: None,
                },
                sql_type: SqlType::Int(None),
                generated: None,
                constraints: vec![],
                comment: None,
            }],
            keys: None,
        };
        let body2 = CreateTableBody {
            fields: vec![ColumnSpecification {
                column: Column {
                    name: "y".into(),
                    table: None,
                },
                sql_type: SqlType::Int(None),
                generated: None,
                constraints: vec![],
                comment: None,
            }],
            keys: None,
        };

        let mut catalog1 = SchemaCatalog::new();
        catalog1.generation = SchemaGeneration::new(1).expect("valid generation");
        catalog1
            .base_schemas
            .insert(Relation::from("foo"), body.clone());

        let mut catalog2 = SchemaCatalog::new();
        catalog2.generation = SchemaGeneration::new(2).expect("valid generation");
        catalog2.base_schemas.insert(Relation::from("foo"), body2);

        // Catalogs delivered in reverse order (pop from end)
        let provider = SequentialProvider {
            catalogs: std::sync::Mutex::new(vec![catalog2, catalog1]),
        };
        let (synchronizer, _handle) = SchemaCatalogSynchronizer::new(provider);
        let synchronizer = synchronizer.with_change_handler(handler.clone());

        let (shutdown_tx, shutdown_rx) = shutdown::channel();
        tokio::spawn(synchronizer.run(shutdown_rx));

        // Wait for both updates to be applied. The reconnect backoff starts at 500ms,
        // so we need to wait well beyond that for the second catalog to arrive and be
        // processed by the sidecar.
        tokio::time::sleep(Duration::from_millis(1500)).await;

        // First update triggers invalidate_all (no previous catalog)
        assert!(
            handler
                .invalidated_all_count
                .load(std::sync::atomic::Ordering::Relaxed)
                >= 1
        );

        // Second update should trigger targeted invalidation for "foo"
        {
            let tables = handler.invalidated_tables.lock().expect("lock poisoned");
            assert!(
                tables.iter().any(|t| t.contains(&Relation::from("foo"))),
                "Expected targeted invalidation for 'foo' but got: {tables:?}"
            );
        }

        shutdown_tx.shutdown().await;
    }

    #[tokio::test]
    async fn test_same_catalog_does_not_trigger_handler() {
        tokio::time::pause();
        let handler = Arc::new(MockChangeHandler::new());

        let catalog = SchemaCatalog {
            generation: SchemaGeneration::new(42).expect("valid generation"),
            base_schemas: HashMap::new(),
            uncompiled_views: HashMap::new(),
            custom_types: HashMap::new(),
            view_schemas: HashMap::new(),
            non_replicated_relations: HashSet::new(),
        };

        // Two identical catalogs
        let provider = SequentialProvider {
            catalogs: std::sync::Mutex::new(vec![catalog.clone(), catalog]),
        };
        let (synchronizer, _handle) = SchemaCatalogSynchronizer::new(provider);
        let synchronizer = synchronizer.with_change_handler(handler.clone());

        let (shutdown_tx, shutdown_rx) = shutdown::channel();
        tokio::spawn(synchronizer.run(shutdown_rx));

        tokio::time::sleep(Duration::from_millis(200)).await;

        // First delivery triggers invalidate_all, second is identical so no call
        assert_eq!(
            handler
                .invalidated_all_count
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );
        assert!(
            handler
                .invalidated_tables
                .lock()
                .expect("lock poisoned")
                .is_empty()
        );

        shutdown_tx.shutdown().await;
    }

    #[tokio::test]
    async fn test_sidecar_shuts_down_cleanly() {
        tokio::time::pause();
        let handler = Arc::new(MockChangeHandler::new());

        let mock = MockProvider::new();
        let (synchronizer, _handle) = SchemaCatalogSynchronizer::new(mock);
        let synchronizer = synchronizer.with_change_handler(handler);

        let (shutdown_tx, shutdown_rx) = shutdown::channel();
        let join = tokio::spawn(synchronizer.run(shutdown_rx));

        // Let the synchronizer process the first update
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Shut down — this drops the watch sender, which should cause the sidecar to exit
        shutdown_tx.shutdown().await;
        // The synchronizer task itself should complete without hanging
        join.await.expect("synchronizer task should not panic");
    }
}
