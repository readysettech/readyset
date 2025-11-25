use std::sync::{Arc, RwLock};

use readyset_client::events::ControllerEvent;
use tokio::{
    select,
    sync::broadcast::{self, error::RecvError},
};

/// Default interval in seconds between heartbeat events sent to keep SSE connections alive.
const DEFAULT_HEARTBEAT_INTERVAL_SECS: &str = "5";

#[derive(Debug, Clone)]
pub(crate) struct EventsHandle {
    /// A broadcast channel for sending events to all connected clients.
    ///
    /// Should be [`None`] if we are not the leader, or there is no leader.
    events_tx: Arc<RwLock<Option<broadcast::Sender<ControllerEvent>>>>,
}

impl EventsHandle {
    pub fn new() -> Self {
        Self {
            events_tx: Arc::new(RwLock::new(None)),
        }
    }

    /// Initialize the events handle with a channel so we can send events. Should only be called by
    /// the leader, and only once per election.
    pub(super) fn start(&self) {
        debug_assert!(self.events_tx.read().unwrap().is_none());
        let (events_tx, mut events_rx) = broadcast::channel(256);
        *self.events_tx.write().unwrap() = Some(events_tx);
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
    /// subscribers and removing the inner [`broadcast::Sender`].
    pub(super) fn stop(&self) {
        if let Some(events_tx) = self.events_tx.write().unwrap().take() {
            // This can only be an error if there are no active receivers; we don't care about that,
            // so ignore it.
            let _ = events_tx.send(ControllerEvent::LeaderLost);
        }
    }

    /// Subscribe to controller events, if we are the leader. Otherwise, return `None`.
    pub fn subscribe(&self) -> Option<broadcast::Receiver<ControllerEvent>> {
        self.events_tx
            .read()
            .unwrap()
            .as_ref()
            .map(|events_tx| events_tx.subscribe())
    }

    /// If we are the leader (i.e. we've called [`EventsHandle::start`], so there is actually a
    /// broadcast channel), send a message. Otherwise, do nothing.
    pub(super) fn send(&self, event: ControllerEvent) {
        if let Some(events_tx) = self.events_tx.read().unwrap().as_ref() {
            // This can only be an error if there are no active receivers; we don't care about that,
            // so ignore it.
            let _ = events_tx.send(event);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use readyset_client::events::ControllerEvent;
    use std::time::Duration;

    #[tokio::test]
    async fn test_events_handle_new() {
        let events_handle = EventsHandle::new();

        // Initially, there should be no events transmitter
        assert!(events_handle.events_tx.read().unwrap().is_none());

        // Subscribe should return None when not started
        assert!(events_handle.subscribe().is_none());

        // Sending should not panic when not started
        events_handle.send(ControllerEvent::Heartbeat);
    }

    #[tokio::test]
    async fn test_events_handle_start() {
        let events_handle = EventsHandle::new();

        // Start the events handle
        events_handle.start();

        // Now there should be an events transmitter
        assert!(events_handle.events_tx.read().unwrap().is_some());

        // Subscribe should now return a receiver
        let receiver = events_handle
            .subscribe()
            .expect("Should have a receiver after start");
        drop(receiver); // Drop the receiver to avoid test warnings
    }

    #[tokio::test]
    async fn test_events_handle_send_and_receive() {
        let events_handle = EventsHandle::new();
        events_handle.start();

        // Subscribe to events
        let mut receiver = events_handle
            .subscribe()
            .expect("Should have a receiver after start");

        // Send an event
        events_handle.send(ControllerEvent::Heartbeat);

        // Receive the event
        let received_event = receiver.recv().await.expect("Should receive event");
        assert!(matches!(received_event, ControllerEvent::Heartbeat));

        // Send another event
        events_handle.send(ControllerEvent::LeaderLost);

        // Receive the second event
        let received_event = receiver.recv().await.expect("Should receive event");
        assert!(matches!(received_event, ControllerEvent::LeaderLost));
    }

    #[tokio::test]
    async fn test_events_handle_multiple_subscribers() {
        let events_handle = EventsHandle::new();
        events_handle.start();

        // Create multiple subscribers
        let mut receiver1 = events_handle
            .subscribe()
            .expect("Should have a receiver after start");
        let mut receiver2 = events_handle
            .subscribe()
            .expect("Should have a receiver after start");

        // Send an event
        events_handle.send(ControllerEvent::Heartbeat);

        // Both subscribers should receive the event
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
        events_handle.start();

        // Subscribe to events
        let mut receiver = events_handle
            .subscribe()
            .expect("Should have a receiver after start");

        // Wait for heartbeat
        let event = tokio::time::timeout(Duration::from_secs(2), receiver.recv())
            .await
            .expect("Should receive heartbeat within timeout")
            .expect("Channel should not be closed");

        assert!(matches!(event, ControllerEvent::Heartbeat));

        // Reset environment variable
        std::env::remove_var("CONTROLLER_EVENTS_HEARTBEAT_INTERVAL");
    }

    #[tokio::test]
    async fn test_heartbeat_task_stops_on_leader_lost() {
        // Set a short heartbeat interval for testing
        std::env::set_var("CONTROLLER_EVENTS_HEARTBEAT_INTERVAL", "1");

        let events_handle = EventsHandle::new();
        events_handle.start();

        // Subscribe to events
        let mut receiver = events_handle
            .subscribe()
            .expect("Should have a receiver after start");

        // Send a LeaderLost event
        events_handle.send(ControllerEvent::LeaderLost);

        // We should receive the LeaderLost event
        let event = receiver.recv().await.expect("Should receive event");
        assert!(matches!(event, ControllerEvent::LeaderLost));

        // Wait a bit to ensure no more heartbeats are sent
        let result = tokio::time::timeout(Duration::from_secs(2), receiver.recv()).await;

        // We expect a timeout because the heartbeat task should have stopped
        assert!(result.is_err());

        // Reset environment variable
        std::env::remove_var("CONTROLLER_EVENTS_HEARTBEAT_INTERVAL");
    }

    #[tokio::test]
    async fn test_events_handle_clone() {
        let events_handle = EventsHandle::new();

        // Clone should work
        let cloned_handle = events_handle.clone();

        // Both should refer to the same underlying channel
        assert!(events_handle.events_tx.read().unwrap().is_none());
        assert!(cloned_handle.events_tx.read().unwrap().is_none());

        // Start on one handle
        events_handle.start();

        // Both should now have a channel
        assert!(events_handle.events_tx.read().unwrap().is_some());
        assert!(cloned_handle.events_tx.read().unwrap().is_some());
    }
}
