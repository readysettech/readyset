use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use dataflow::prelude::*;
use readyset::consensus::Authority;
use readyset::prelude::*;
use reqwest::Url;
use stream_cancel::Trigger;
use tokio::sync::mpsc::Sender;

use crate::controller::migrate::Migration;
use crate::controller::HandleRequest;
use crate::ControllerDescriptor;

/// A handle to a controller that is running in the same process as this one.
pub struct Handle {
    /// Has a valid controller handle on `new` and is set to None if the
    /// controller has been shutdown.
    pub c: Option<ReadySetHandle>,
    #[allow(dead_code)]
    event_tx: Option<Sender<HandleRequest>>,
    kill: Option<Trigger>,
    descriptor: ControllerDescriptor,
}

impl Deref for Handle {
    type Target = ReadySetHandle;
    fn deref(&self) -> &Self::Target {
        self.c.as_ref().unwrap()
    }
}

impl DerefMut for Handle {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.c.as_mut().unwrap()
    }
}

impl Handle {
    pub(super) fn new(
        authority: Arc<Authority>,
        event_tx: Sender<HandleRequest>,
        kill: Trigger,
        descriptor: ControllerDescriptor,
    ) -> Self {
        let c = ReadySetHandle::make(authority, None, None);
        Handle {
            c: Some(c),
            event_tx: Some(event_tx),
            kill: Some(kill),
            descriptor,
        }
    }

    /// Returns the address of this ReadySet server.
    pub fn get_address(&self) -> &Url {
        &self.descriptor.controller_uri
    }

    /// Waits for the back-end to return that it is ready to process queries.
    /// Should not be used in production.
    pub async fn backend_ready(&mut self) {
        use std::time;

        loop {
            let (tx, rx) = tokio::sync::oneshot::channel();
            self.event_tx
                .as_mut()
                .unwrap()
                .send(HandleRequest::QueryReadiness(tx))
                .await
                .ok()
                .expect("Controller dropped, failed, or panicked");

            if rx.await.unwrap() {
                break;
            }

            tokio::time::sleep(time::Duration::from_millis(50)).await;
        }
    }

    #[doc(hidden)]
    pub async fn migrate<F, T>(&mut self, f: F) -> T
    where
        F: FnOnce(&mut Migration<'_>) -> T + Send + 'static,
        T: Send + 'static,
    {
        let (ret_tx, ret_rx) = tokio::sync::oneshot::channel();
        let (fin_tx, fin_rx) = tokio::sync::oneshot::channel();
        let b = Box::new(move |m: &mut Migration<'_>| {
            if ret_tx.send(f(m)).is_err() {
                internal!("could not return migration result")
            }
            Ok(())
        });

        self.event_tx
            .as_mut()
            .unwrap()
            .send(HandleRequest::PerformMigration {
                func: b,
                done_tx: fin_tx,
            })
            .await
            .ok()
            .expect("Controller dropped, failed, or panicked");

        fin_rx.await.unwrap().unwrap();
        ret_rx.await.unwrap()
    }

    /// Inform the local instance that it should exit.
    pub fn shutdown(&mut self) {
        if let Some(kill) = self.kill.take() {
            drop(self.c.take());
            drop(kill);
        }
    }

    /// Wait until the instance has exited.
    pub async fn wait_done(&mut self) {
        if let Some(etx) = self.event_tx.take() {
            etx.closed().await;
        }
    }
}

impl Drop for Handle {
    fn drop(&mut self) {
        self.shutdown();
    }
}
