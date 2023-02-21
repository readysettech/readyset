//! Defines types that allow you to gracefully shut down background tasks.
//!
//! # Examples
//!
//! ## Basic Usage
//! ```
//! # use readyset_util::shutdown;
//! # let mut rt = tokio::runtime::Runtime::new().unwrap();
//! # rt.block_on(async move {
//! # async fn some_async_task() {}
//! let (shutdown_tx, mut shutdown_rx) = shutdown::channel();
//!
//! tokio::spawn(async move {
//!     tokio::select! {
//!         _ = shutdown_rx.recv() => {
//!             println!("shutdown signal received!");
//!
//!             // Perform any cleanup here that needs to occur before the task is closed
//!         },
//!         _ = some_async_task() => {
//!             println!("task completed!");
//!         }
//!     }
//! });
//!
//! shutdown_tx.shutdown().await;
//! # });
//! ```
//!
//! ## Spawning multiple tasks
//! ```
//! # use readyset_util::shutdown;
//! # let mut rt = tokio::runtime::Runtime::new().unwrap();
//! # rt.block_on(async move {
//! # async fn some_async_task() {}
//! # async fn some_async_task_2() {}
//! let (shutdown_tx, mut shutdown_rx) = shutdown::channel();
//!
//! tokio::spawn(async move {
//!     loop {
//!         tokio::select! {
//!             _ = shutdown_rx.recv() => {
//!                 println!("shutdown signal received!");
//!
//!                 break;
//!             }
//!             _ = some_async_task() => {
//!                 let mut shutdown_rx = shutdown_rx.clone();
//!
//!                 tokio::spawn(async move {
//!                     tokio::select! {
//!                         _ = shutdown_rx.recv() => {},
//!                         _ = some_async_task_2() => {},
//!                     }
//!                 });
//!             }
//!         }
//!     }
//! });
//!
//! // Calling `ShutdownSender::shutdown` will send a shutdown signal to the original receiver as
//! // well as any receiver cloned from the original receiver
//! shutdown_tx.shutdown().await;
//! # });
//! ```

use std::time::Duration;

use futures::{Stream, StreamExt};
use tokio::sync::watch;

/// Creates a new shutdown channel, returning a [`ShutdownSender`] and a [`ShutdownReceiver`].
pub fn channel() -> (ShutdownSender, ShutdownReceiver) {
    let (shutdown_tx, shutdown_rx) = watch::channel(());

    (ShutdownSender(shutdown_tx), ShutdownReceiver(shutdown_rx))
}

/// A struct that can be used to broadcast a shutdown signal to all of the associated
/// [`ShutdownReceiver`]s.
#[derive(Debug)]
pub struct ShutdownSender(watch::Sender<()>);

impl ShutdownSender {
    /// Broadcast a shutdown signal to all of the [`ShutdownReceiver`]s associated with this sender.
    /// The future returned by this method will never resolve if there are existing
    /// [`ShutdownReceiver`]s that are not listening for a shutdown signal. If you want to send a
    /// shutdown signal and time out after a given duration, use
    /// [`ShutdownSender::shutdown_timeout`].
    pub async fn shutdown(self) {
        // The only situation in which this send will fail is if every receiver has been closed,
        // which is exactly what we want
        let _ = self.0.send(());
        self.0.closed().await;
    }

    /// Broadcast a shutdown signal to all of the [`ShutdownReceiver`]s associated with this sender
    /// and panic if it takes longer than the duration given by `timeout` for every associated
    /// [`ShutdownReceiver`] to drop.
    ///
    /// # Panics
    ///
    /// This method panics if the shutdown process takes longer than the duration given by
    /// `timeout` to complete.
    pub async fn shutdown_timeout(self, timeout: Duration) {
        if tokio::time::timeout(timeout, self.shutdown())
            .await
            .is_err()
        {
            panic!("shutdown process timed out: is every `ShutdownReceiver` listening for a shutdown signal?");
        }
    }

    /// Creates a new `ShutdownReceiver` registered with the given `ShutdownSender`.
    pub fn subscribe(&self) -> ShutdownReceiver {
        ShutdownReceiver(self.0.subscribe())
    }
}

/// A struct that can be used to wait for a shutdown signal from a [`ShutdownSender`]. A
/// [`ShutdownReceiver`] can be cloned and passed to another subtask -- the clone will be
/// associated with the same [`ShutdownSender`] as the original.
///
/// NOTE: Every [`ShutdownReceiver`] associated with a given [`ShutdownSender`] MUST be listening
/// for a shutdown signal. If there are any existing [`ShutdownReceiver`]s that are not listening
/// for a shutdown signal, [`ShutdownSender::shutdown`] will hang forever, since it specifically
/// waits for confirmation that every [`ShutdownReceiver`] has been dropped.
#[derive(Clone, Debug)]
pub struct ShutdownReceiver(watch::Receiver<()>);

impl ShutdownReceiver {
    /// Asynchronously wait for a shutdown signal from the associated [`ShutdownSender`]. It
    /// generally makes sense to use this method in a [`tokio::select!`] call, where another future
    /// can run simultaneously.
    pub async fn recv(&mut self) {
        let _ = self.0.changed().await;
    }

    /// A convenience method that allows you to generate a new stream that will short-circuit
    /// if the shutdown signal has been received.
    pub fn wrap_stream<S, I>(mut self, mut input: S) -> impl Stream<Item = I>
    where
        S: Stream<Item = I> + Unpin + 'static,
    {
        async_stream::stream! {
            loop {
                tokio::select! {
                    // We use `biased` here to ensure that our shutdown signal will be received and
                    // acted upon even if the stream has many messages where very little time passes
                    // between receipt of these messages. More information about this situation can
                    // be found in the docs for `tokio::select`.
                    biased;
                    _ = self.recv() => break,
                    maybe_item = input.next() => match maybe_item {
                        Some(item) => yield item,
                        None => break,
                    },
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::eventually;

    #[tokio::test]
    async fn test_shutdown() {
        let (shutdown_tx, mut shutdown_rx) = channel();

        // Spawn a background task that is waiting for a shutdown signal
        let background_task = tokio::spawn(async move {
            shutdown_rx.recv().await;
        });

        // Assert the task has not yet finished
        assert!(!background_task.is_finished());

        // Send the shutdown signal
        shutdown_tx.shutdown().await;

        // Assert the task eventually finishes
        eventually!(background_task.is_finished());
    }

    #[tokio::test]
    #[should_panic]
    async fn test_shutdown_timeout() {
        let (shutdown_tx, mut shutdown_rx) = channel();

        let timeout_ms = 10;

        // Spawn a background task that is waiting for a shutdown signal
        tokio::spawn(async move {
            shutdown_rx.recv().await;

            // Hang for just a tad longer than the timeout passed to
            // [`ShutdownSender::shutdown_timeout`] so we trigger a panic
            tokio::time::sleep(Duration::from_millis(timeout_ms * 2)).await;
        });

        // Send the shutdown signal. This should panic because the task doesn't finish within the
        // specified timeout period
        shutdown_tx
            .shutdown_timeout(Duration::from_millis(timeout_ms))
            .await;
    }
}
