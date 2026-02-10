use std::sync::{Arc, RwLock};

use readyset_client::events::ControllerEvent;
use readyset_errors::ReadySetResult;
use schema_catalog::{SchemaCatalog, SchemaCatalogUpdate};
use tokio::{
    select,
    sync::broadcast::{self, error::RecvError},
};
use tracing::warn;

/// Default interval in seconds between heartbeat events sent to keep SSE connections alive.
const DEFAULT_HEARTBEAT_INTERVAL_SECS: &str = "5";

/// Handle for broadcasting controller events and providing schema catalog snapshots to new
/// SSE subscribers.
///
/// ## Lock ordering
///
/// `events_tx` and `latest_catalog` are independent [`std::sync::RwLock`]s. To prevent deadlocks:
///
/// - Never hold write locks on both simultaneously.
/// - [`Self::subscribe_with_snapshot`] acquires `events_tx` read, then `latest_catalog` read
///   (both are read locks, so no deadlock risk with each other or with writers on either lock
///   individually).
/// - [`Self::send_schema_catalog_update`] acquires `events_tx` read, then `latest_catalog` write
///   in a scoped block, then sends on the channel (all while holding the `events_tx` read guard,
///   so [`Self::stop`] cannot interleave).
/// - [`Self::stop`] acquires `events_tx` write, drops it, then acquires `latest_catalog` write.
#[derive(Debug, Clone)]
pub(crate) struct EventsHandle {
    /// A broadcast channel for sending events to all connected clients.
    ///
    /// [`None`] if this node is not the leader, or there is no leader.
    events_tx: Arc<RwLock<Option<broadcast::Sender<ControllerEvent>>>>,
    /// The most recent complete [`SchemaCatalog`], or [`None`] if this node is not the leader.
    /// Stored as the full catalog (not as an update event) so the type system guarantees we never
    /// accidentally store a delta. Provided as a snapshot to new SSE subscribers so they don't
    /// miss events that were broadcast before they connected.
    ///
    /// The inner [`Arc`] allows [`Self::subscribe_with_snapshot`] to hand out a reference without
    /// deep-cloning the catalog under the lock.
    ///
    /// Set to [`Some`] by [`Self::start`] with the catalog from the current dataflow state, kept
    /// up to date by [`Self::send_schema_catalog_update`], and cleared by [`Self::stop`].
    latest_catalog: Arc<RwLock<Option<Arc<SchemaCatalog>>>>,
}

impl EventsHandle {
    pub fn new() -> Self {
        Self {
            events_tx: Arc::new(RwLock::new(None)),
            latest_catalog: Arc::new(RwLock::new(None)),
        }
    }

    /// Initialize the events handle with a broadcast channel and the current schema catalog.
    /// Should only be called by the leader, and only once per election.
    pub(super) fn start(&self, initial_catalog: SchemaCatalog) {
        debug_assert!(self
            .events_tx
            .read()
            .expect("events_tx lock poisoned")
            .is_none());
        let initial_catalog = Arc::new(initial_catalog);
        {
            let mut guard = self
                .latest_catalog
                .write()
                .expect("latest_catalog lock poisoned");
            *guard = Some(initial_catalog);
        }
        let (events_tx, mut events_rx) = broadcast::channel(256);
        *self.events_tx.write().expect("events_tx lock poisoned") = Some(events_tx);
        // Spawn a heartbeat task to keep HTTP connections alive (arguably should live in the HTTP
        // server, not here, but I like that code not knowing anything about particular events)
        let heartbeat_interval = std::time::Duration::from_secs(
            std::env::var("CONTROLLER_EVENTS_HEARTBEAT_INTERVAL")
                .as_deref()
                .unwrap_or(DEFAULT_HEARTBEAT_INTERVAL_SECS)
                .parse()
                .unwrap(),
        );
        let heartbeat_handle = self.clone();
        tokio::spawn(async move {
            loop {
                select! {
                    event = events_rx.recv() => {
                        if matches!(event, Err(RecvError::Closed) | Ok(ControllerEvent::LeaderLost)) {
                            break
                        }
                    }
                    _ = tokio::time::sleep(heartbeat_interval) => {
                        heartbeat_handle.send(ControllerEvent::Heartbeat);
                    }
                }
            }
        });
    }

    /// Stop the heartbeat task by sending a [`ControllerEvent::LeaderLost`] event to all
    /// subscribers and removing the inner [`broadcast::Sender`]. Clears the catalog snapshot to
    /// free memory; it will be re-initialized on the next [`Self::start`].
    pub(super) fn stop(&self) {
        let mut events_tx = self.events_tx.write().expect("events_tx lock poisoned");
        if let Some(tx) = events_tx.take() {
            // This can only be an error if there are no active receivers; we don't care about that,
            // so ignore it.
            let _ = tx.send(ControllerEvent::LeaderLost);
        }
        drop(events_tx);
        *self
            .latest_catalog
            .write()
            .expect("latest_catalog lock poisoned") = None;
    }

    /// Subscribe to controller events, if we are the leader. Otherwise, return `None`.
    ///
    /// Returns a `(receiver, catalog)` pair. The catalog is the most recent complete
    /// [`SchemaCatalog`]. The receiver is subscribed **before** reading the catalog, so for any
    /// schema catalog update E:
    ///
    /// - If E was sent before subscribe: E (or a later value) is in the catalog.
    /// - If E was sent after subscribe: the receiver captures it.
    /// - Overlap (E in both): deduplicated by the adapter's `apply_update()`.
    ///
    /// Holding the `events_tx` read guard across both the subscribe and the catalog read
    /// prevents [`Self::stop`] from clearing the catalog between the two operations, because
    /// `stop` needs a write lock on `events_tx` which is blocked by the active read guard.
    pub fn subscribe_with_snapshot(
        &self,
    ) -> Option<(broadcast::Receiver<ControllerEvent>, Arc<SchemaCatalog>)> {
        let events_tx = self.events_tx.read().expect("events_tx lock poisoned");
        let receiver = events_tx.as_ref().map(|tx| tx.subscribe())?;
        let catalog = match self
            .latest_catalog
            .read()
            .expect("latest_catalog lock poisoned")
            .clone()
        {
            Some(c) => c,
            None => {
                warn!(
                    "events_tx is Some but latest_catalog is None; \
                     bug in start/stop lifecycle"
                );
                return None;
            }
        };
        Some((receiver, catalog))
    }

    /// Send a non-catalog event to all active subscribers. Does nothing if we are not the leader.
    ///
    /// For [`ControllerEvent::SchemaCatalogUpdate`] events, use
    /// [`Self::send_schema_catalog_update`] which also updates the snapshot for new subscribers.
    pub(super) fn send(&self, event: ControllerEvent) {
        debug_assert!(
            !matches!(event, ControllerEvent::SchemaCatalogUpdate(_)),
            "use send_schema_catalog_update() for SchemaCatalogUpdate events"
        );
        self.broadcast(event);
    }

    /// Serialize `catalog` into a [`SchemaCatalogUpdate`], store the full catalog as the snapshot
    /// for new subscribers, and broadcast the update event. Does nothing if we are not the leader.
    pub(super) fn send_schema_catalog_update(&self, catalog: SchemaCatalog) -> ReadySetResult<()> {
        // Serialize before acquiring the lock — serialization is pure and doesn't need events_tx,
        // so keeping it outside the critical section avoids blocking stop() during bincode+base64.
        let update = SchemaCatalogUpdate::try_from(&catalog)?;
        let events_tx = self.events_tx.read().expect("events_tx lock poisoned");
        let Some(tx) = events_tx.as_ref() else {
            return Ok(());
        };
        // Store the snapshot *before* broadcasting so that any subscriber created after the
        // broadcast but before the next update still sees this catalog via
        // `subscribe_with_snapshot()`.
        let catalog = Arc::new(catalog);
        {
            let mut guard = self
                .latest_catalog
                .write()
                .expect("latest_catalog lock poisoned");
            *guard = Some(catalog);
        }
        // This can only be an error if there are no active receivers; we don't care about that,
        // so ignore it.
        let _ = tx.send(ControllerEvent::SchemaCatalogUpdate(update));
        Ok(())
    }

    /// Broadcast an event to all active subscribers. Does nothing if we are not the leader.
    fn broadcast(&self, event: ControllerEvent) {
        let events_tx = self.events_tx.read().expect("events_tx lock poisoned");
        if let Some(tx) = events_tx.as_ref() {
            // This can only be an error if there are no active receivers; we don't care about
            // that, so ignore it.
            let _ = tx.send(event);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use readyset_client::events::ControllerEvent;
    use schema_catalog::SchemaGeneration;
    use std::time::Duration;

    fn test_catalog(generation: u64) -> SchemaCatalog {
        SchemaCatalog {
            generation: SchemaGeneration::new(generation).expect("generation must be non-zero"),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_events_handle_new() {
        let events_handle = EventsHandle::new();

        // There should be no events transmitter before start
        assert!(events_handle.events_tx.read().unwrap().is_none());

        // Subscribe should return None when not started
        assert!(events_handle.subscribe_with_snapshot().is_none());

        // Sending should not panic when not started
        events_handle.send(ControllerEvent::Heartbeat);
    }

    #[tokio::test]
    async fn test_events_handle_start() {
        let events_handle = EventsHandle::new();

        events_handle.start(test_catalog(1));

        assert!(events_handle.events_tx.read().unwrap().is_some());

        let (receiver, _) = events_handle
            .subscribe_with_snapshot()
            .expect("Should have a receiver after start");
        drop(receiver);
    }

    #[tokio::test]
    async fn test_events_handle_send_and_receive() {
        let events_handle = EventsHandle::new();
        events_handle.start(test_catalog(1));

        let (mut receiver, _) = events_handle
            .subscribe_with_snapshot()
            .expect("Should have a receiver after start");

        events_handle.send(ControllerEvent::Heartbeat);

        let received_event = receiver.recv().await.expect("Should receive event");
        assert!(matches!(received_event, ControllerEvent::Heartbeat));

        events_handle.send(ControllerEvent::LeaderLost);

        let received_event = receiver.recv().await.expect("Should receive event");
        assert!(matches!(received_event, ControllerEvent::LeaderLost));
    }

    #[tokio::test]
    async fn test_events_handle_multiple_subscribers() {
        let events_handle = EventsHandle::new();
        events_handle.start(test_catalog(1));

        let (mut receiver1, _) = events_handle
            .subscribe_with_snapshot()
            .expect("Should have a receiver after start");
        let (mut receiver2, _) = events_handle
            .subscribe_with_snapshot()
            .expect("Should have a receiver after start");

        events_handle.send(ControllerEvent::Heartbeat);

        let event1 = receiver1.recv().await.expect("Should receive event");
        let event2 = receiver2.recv().await.expect("Should receive event");

        assert!(matches!(event1, ControllerEvent::Heartbeat));
        assert!(matches!(event2, ControllerEvent::Heartbeat));
    }

    #[tokio::test]
    async fn test_heartbeat_task() {
        // Set a short heartbeat interval for testing
        std::env::set_var("CONTROLLER_EVENTS_HEARTBEAT_INTERVAL", "1");

        let events_handle = EventsHandle::new();
        events_handle.start(test_catalog(1));

        let (mut receiver, _) = events_handle
            .subscribe_with_snapshot()
            .expect("Should have a receiver after start");

        let event = tokio::time::timeout(Duration::from_secs(2), receiver.recv())
            .await
            .expect("Should receive heartbeat within timeout")
            .expect("Channel should not be closed");

        assert!(matches!(event, ControllerEvent::Heartbeat));

        std::env::remove_var("CONTROLLER_EVENTS_HEARTBEAT_INTERVAL");
    }

    #[tokio::test]
    async fn test_heartbeat_task_stops_on_leader_lost() {
        // Set a short heartbeat interval for testing
        std::env::set_var("CONTROLLER_EVENTS_HEARTBEAT_INTERVAL", "1");

        let events_handle = EventsHandle::new();
        events_handle.start(test_catalog(1));

        let (mut receiver, _) = events_handle
            .subscribe_with_snapshot()
            .expect("Should have a receiver after start");

        events_handle.send(ControllerEvent::LeaderLost);

        let event = receiver.recv().await.expect("Should receive event");
        assert!(matches!(event, ControllerEvent::LeaderLost));

        // Wait a bit to ensure no more heartbeats are sent
        let result = tokio::time::timeout(Duration::from_secs(2), receiver.recv()).await;

        // We expect a timeout because the heartbeat task should have stopped
        assert!(result.is_err());

        std::env::remove_var("CONTROLLER_EVENTS_HEARTBEAT_INTERVAL");
    }

    #[tokio::test]
    async fn test_events_handle_clone() {
        let events_handle = EventsHandle::new();
        let cloned_handle = events_handle.clone();

        // Both should refer to the same underlying channel
        assert!(events_handle.events_tx.read().unwrap().is_none());
        assert!(cloned_handle.events_tx.read().unwrap().is_none());

        events_handle.start(test_catalog(1));

        // Both should have a channel after starting on one handle
        assert!(events_handle.events_tx.read().unwrap().is_some());
        assert!(cloned_handle.events_tx.read().unwrap().is_some());
    }

    #[tokio::test]
    async fn test_subscribe_with_snapshot_returns_initial_catalog() {
        let events_handle = EventsHandle::new();
        events_handle.start(test_catalog(1));

        let (_, catalog) = events_handle
            .subscribe_with_snapshot()
            .expect("Should subscribe after start");

        assert_eq!(
            catalog.generation,
            SchemaGeneration::new(1).unwrap(),
            "snapshot should contain the initial catalog"
        );
    }

    #[tokio::test]
    async fn test_subscribe_with_snapshot_returns_latest_catalog_update() {
        let events_handle = EventsHandle::new();
        events_handle.start(test_catalog(1));

        // Send a schema catalog update before subscribing
        events_handle
            .send_schema_catalog_update(test_catalog(2))
            .expect("serialization should succeed");

        let (_, catalog) = events_handle
            .subscribe_with_snapshot()
            .expect("Should subscribe after start");

        assert_eq!(
            catalog.generation,
            SchemaGeneration::new(2).unwrap(),
            "snapshot should reflect the latest catalog update"
        );
    }

    #[tokio::test]
    async fn test_subscribe_with_snapshot_non_schema_events_dont_change_snapshot() {
        let events_handle = EventsHandle::new();
        events_handle.start(test_catalog(1));

        // Non-schema events should not affect the catalog snapshot
        events_handle.send(ControllerEvent::Heartbeat);

        let (_, catalog) = events_handle
            .subscribe_with_snapshot()
            .expect("Should subscribe after start");

        assert_eq!(
            catalog.generation,
            SchemaGeneration::new(1).unwrap(),
            "snapshot should still be the initial catalog"
        );
    }

    #[tokio::test]
    async fn test_subscribe_with_snapshot_receiver_captures_events_after_subscribe() {
        let events_handle = EventsHandle::new();
        events_handle.start(test_catalog(1));

        let (mut receiver, catalog) = events_handle
            .subscribe_with_snapshot()
            .expect("Should subscribe after start");

        assert_eq!(catalog.generation, SchemaGeneration::new(1).unwrap());

        // Send a catalog update after subscribing
        events_handle
            .send_schema_catalog_update(test_catalog(2))
            .expect("serialization should succeed");

        let event = receiver.recv().await.expect("Should receive event");
        assert!(
            matches!(&event, ControllerEvent::SchemaCatalogUpdate(_)),
            "receiver should capture events sent after subscription, got: {event:?}"
        );
    }

    #[tokio::test]
    async fn test_snapshot_covers_missed_broadcast() {
        let events_handle = EventsHandle::new();
        events_handle.start(test_catalog(1));

        // Broadcast with NO subscribers — event is lost from the channel
        events_handle
            .send_schema_catalog_update(test_catalog(2))
            .expect("serialization should succeed");

        // Subscribe after broadcast; snapshot should have the update
        let (mut receiver, catalog) = events_handle
            .subscribe_with_snapshot()
            .expect("Should subscribe after start");

        assert_eq!(catalog.generation, SchemaGeneration::new(2).unwrap());

        // The broadcast was missed by the receiver (no events in channel)
        assert!(matches!(
            receiver.try_recv(),
            Err(broadcast::error::TryRecvError::Empty)
        ));
    }

    #[tokio::test]
    async fn test_stop_clears_catalog_and_prevents_subscribe() {
        let events_handle = EventsHandle::new();
        events_handle.start(test_catalog(5));

        // Catalog should be present before stop
        assert!(events_handle.subscribe_with_snapshot().is_some());

        events_handle.stop();

        // After stop, subscribe returns None (no leader)
        assert!(
            events_handle.subscribe_with_snapshot().is_none(),
            "subscribe_with_snapshot() should return None after stop()"
        );

        // Catalog should be cleared
        assert!(
            events_handle.latest_catalog.read().unwrap().is_none(),
            "stop() should clear latest_catalog"
        );
    }
}
