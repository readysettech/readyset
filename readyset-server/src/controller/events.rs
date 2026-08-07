use std::sync::{Arc, RwLock};

use failpoint_macros::set_failpoint;
use readyset_client::events::ControllerEvent;
use readyset_errors::ReadySetResult;
use schema_catalog::{SchemaCatalog, SchemaCatalogUpdate};
use tokio::{
    select,
    sync::broadcast::{self, error::RecvError},
};

/// Default interval in seconds between heartbeat events sent to keep SSE connections alive.
const DEFAULT_HEARTBEAT_INTERVAL_SECS: &str = "5";

/// Handle for broadcasting controller events and providing schema catalog snapshots to new
/// SSE subscribers.
#[derive(Debug, Clone)]
pub(crate) struct EventsHandle {
    /// The broadcast channel for sending events to all connected clients, paired with the most
    /// recent complete [`SchemaCatalog`]. One lock covers both, so a subscriber's snapshot is
    /// never newer than the updates queued behind it.
    ///
    /// [`None`] if this node is not the leader, or there is no leader. Set by [`Self::start`],
    /// kept up to date by [`Self::send_schema_catalog_update`], and cleared by [`Self::stop`].
    inner: Arc<RwLock<Option<(broadcast::Sender<ControllerEvent>, Arc<SchemaCatalog>)>>>,
}

impl EventsHandle {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(None)),
        }
    }

    /// Initialize the events handle with a broadcast channel and the current schema catalog.
    /// Should only be called by the leader, and only once per election.
    pub(super) fn start(&self, initial_catalog: SchemaCatalog) {
        let (events_tx, mut events_rx) =
            broadcast::channel(readyset_client::events::BROADCAST_CHANNEL_CAPACITY);
        {
            let mut inner = self.inner.write().expect("events lock poisoned");
            debug_assert!(inner.is_none());
            *inner = Some((events_tx, Arc::new(initial_catalog)));
        }
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
        if let Some((events_tx, _)) = self.inner.write().expect("events lock poisoned").take() {
            // This can only be an error if there are no active receivers; we don't care about that,
            // so ignore it.
            let _ = events_tx.send(ControllerEvent::LeaderLost);
        }
    }

    /// Subscribe to controller events, if we are the leader. Otherwise, return `None`.
    ///
    /// Returns a receiver and latest [`SchemaCatalog`]; each catalog update reaches exactly one.
    pub fn subscribe_with_snapshot(
        &self,
    ) -> Option<(broadcast::Receiver<ControllerEvent>, Arc<SchemaCatalog>)> {
        let inner = self.inner.read().expect("events lock poisoned");
        let (events_tx, catalog) = inner.as_ref()?;
        set_failpoint!(readyset_util::failpoints::CONTROLLER_EVENTS_SUBSCRIBE_WINDOW);
        Some((events_tx.subscribe(), Arc::clone(catalog)))
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
        // Serialize before taking the lock: it's the slow part and touches no shared state.
        let update = SchemaCatalogUpdate::try_from(&catalog)?;
        let mut inner = self.inner.write().expect("events lock poisoned");
        let Some((events_tx, latest_catalog)) = inner.as_mut() else {
            return Ok(());
        };
        *latest_catalog = Arc::new(catalog);
        // This can only be an error if there are no active receivers; we don't care about that,
        // so ignore it.
        let _ = events_tx.send(ControllerEvent::SchemaCatalogUpdate(update));
        Ok(())
    }

    /// Broadcast an event to all active subscribers. Does nothing if we are not the leader.
    fn broadcast(&self, event: ControllerEvent) {
        // Use fail::eval instead of set_failpoint! so we can sleep *outside* the lock scope.
        // The fail crate's built-in sleep action uses std::thread::sleep, which would block the
        // tokio worker thread while holding the events read lock.
        // Configure with "return(delay_ms)" to trigger, e.g. "1*return(3000)".
        #[cfg(feature = "failure_injection")]
        if let Some(delay_ms) = fail::eval(
            readyset_util::failpoints::CONTROLLER_EVENTS_SSE_SEND_DELAY,
            |v| v.and_then(|s| s.parse::<u64>().ok()),
        )
        .flatten()
        {
            tracing::info!(delay_ms, "SSE send failpoint: delaying broadcast");
            std::thread::sleep(std::time::Duration::from_millis(delay_ms));
        }

        let inner = self.inner.read().expect("events lock poisoned");
        if let Some((events_tx, _)) = inner.as_ref() {
            // This can only be an error if there are no active receivers; we don't care about
            // that, so ignore it.
            let _ = events_tx.send(event);
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
        assert!(events_handle.inner.read().unwrap().is_none());

        // Subscribe should return None when not started
        assert!(events_handle.subscribe_with_snapshot().is_none());

        // Sending should not panic when not started
        events_handle.send(ControllerEvent::Heartbeat);
    }

    #[tokio::test]
    async fn test_events_handle_start() {
        let events_handle = EventsHandle::new();

        events_handle.start(test_catalog(1));

        assert!(events_handle.inner.read().unwrap().is_some());

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
        assert!(events_handle.inner.read().unwrap().is_none());
        assert!(cloned_handle.inner.read().unwrap().is_none());

        events_handle.start(test_catalog(1));

        // Both should have a channel after starting on one handle
        assert!(events_handle.inner.read().unwrap().is_some());
        assert!(cloned_handle.inner.read().unwrap().is_some());
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

    /// Sends during a subscribe reach the receiver, not the snapshot.
    #[cfg(feature = "failure_injection")]
    #[test]
    fn catalog_send_excluded_from_subscribe_window() {
        use std::sync::Barrier;

        let events_handle = EventsHandle::new();
        // Not start(): its heartbeat task needs a runtime and adds events.
        let (tx, _) = broadcast::channel(readyset_client::events::BROADCAST_CHANNEL_CAPACITY);
        *events_handle.inner.write().unwrap() = Some((tx, Arc::new(test_catalog(1))));

        // Park the subscriber mid-subscribe long enough for racing sends to land.
        let entered = Arc::new(Barrier::new(2));
        fail::cfg_callback(
            readyset_util::failpoints::CONTROLLER_EVENTS_SUBSCRIBE_WINDOW,
            {
                let entered = Arc::clone(&entered);
                move || {
                    entered.wait();
                    std::thread::sleep(Duration::from_millis(100));
                }
            },
        )
        .unwrap();
        let subscriber = std::thread::spawn({
            let handle = events_handle.clone();
            move || handle.subscribe_with_snapshot().unwrap()
        });
        entered.wait();
        for generation in [2, 3] {
            events_handle
                .send_schema_catalog_update(test_catalog(generation))
                .unwrap();
        }

        let (mut receiver, snapshot) = subscriber.join().unwrap();
        fail::remove(readyset_util::failpoints::CONTROLLER_EVENTS_SUBSCRIBE_WINDOW);
        assert_eq!(snapshot.generation.get(), 1);
        let streamed: Vec<_> = std::iter::from_fn(|| receiver.try_recv().ok())
            .map(|event| match event {
                ControllerEvent::SchemaCatalogUpdate(update) => {
                    SchemaCatalog::try_from(update).unwrap().generation.get()
                }
                other => panic!("unexpected event: {other:?}"),
            })
            .collect();
        assert_eq!(streamed, [2, 3]);
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
            events_handle.inner.read().unwrap().is_none(),
            "stop() should clear the catalog snapshot"
        );
    }
}
