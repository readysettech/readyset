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
    pub async fn shutdown(self) {
        // The only situation in which this send will fail is if every receiver has been closed,
        // which is exactly what we want
        let _ = self.0.send(());
        self.0.closed().await;
    }

    /// Waits until all of the receivers associated with this sender have closed. This method is
    /// only useful if you need to wait until all of the assocaited [`ShutdownReceiver`]s have been
    /// closed for reasons other than receiving a message from [`ShutdownSender::shutdown`].
    pub async fn wait_done(&self) {
        self.0.closed().await;
    }
}

/// A struct that can be used to wait for a shutdown signal from a [`ShutdownSender`]. A
/// [`ShutdownReceiver`] can be cloned and passed to another subtask -- the clone will be
/// associated with the same [`ShutdownSender`] as the original.
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
    use std::time::Duration;

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

        // Assert the task has finished
        eventually!(background_task.is_finished());
    }

    #[tokio::test]
    async fn test_wait_done() {
        let (shutdown_tx, mut shutdown_rx) = channel();

        // Spawn a background task that is waiting for a shutdown signal
        let background_task = tokio::spawn(async move {
            shutdown_rx.recv().await;

            // When the shutdown_rx handle is dropped, the shutdown_tx will be notified that the
            // last remaining [`ShutdownReceiver`] was dropped. For this test, we want to prevent
            // that from happening right away, so we sleep for a very long time. We abort this task
            // on the next line, so this test is not actually slow.
            tokio::time::sleep(Duration::from_secs(1000)).await;
        });

        // Assert the task has not yet finished
        assert!(!background_task.is_finished());

        tokio::select! {
            _ = async move {
                background_task.abort();
                let _ = background_task.await;
                tokio::time::sleep(Duration::from_secs(5)).await;
            } => panic!(),
            _ = shutdown_tx.wait_done() => {}
        }
    }

    /// Test that, when a [`ShutdownReceiver`] is dropped for a reason other than the invocation of
    /// [`ShutdownSender::shutdown`], the [`ShutdownSender`] still takes this to mean that the
    /// receiver has been closed.
    #[tokio::test]
    async fn test_drop_counts_as_shutdown() {
        let (shutdown_tx, shutdown_rx) = channel();

        tokio::select! {
            _ = shutdown_tx.wait_done() => {},
            _ = async {
                std::mem::drop(shutdown_rx);
                tokio::time::sleep(Duration::from_secs(5)).await;
            } => panic!(),
        }
    }
}
