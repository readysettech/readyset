use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use dataflow::prelude::*;
use readyset_client::consensus::Authority;
use readyset_client::prelude::*;
use readyset_data::Dialect;
#[cfg(feature = "failure_injection")]
use readyset_tracing::info;
use reqwest::Url;
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
        descriptor: ControllerDescriptor,
    ) -> Self {
        let c = ReadySetHandle::make(authority, None, None);
        Handle {
            c: Some(c),
            event_tx: Some(event_tx),
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

    /// Perform a migration to alter the state of the graph in the controller, using the MySQL
    /// dialect to normalize all queries
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
                // This function is only used in tests at the moment, so we just hardcode the MySQL
                // dialect
                dialect: Dialect::DEFAULT_MYSQL,
                done_tx: fin_tx,
            })
            .await
            .ok()
            .expect("Controller dropped, failed, or panicked");

        fin_rx.await.unwrap().unwrap();
        ret_rx.await.unwrap()
    }

    #[cfg(feature = "failure_injection")]
    /// Injects a failpoint with the provided name/action
    pub async fn set_failpoint<S: std::fmt::Display>(&mut self, name: S, action: S) {
        info!(%name, %action, "Handle::set_failpoint");
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.event_tx
            .as_mut()
            .unwrap()
            .send(HandleRequest::Failpoint {
                name: name.to_string(),
                action: action.to_string(),
                done_tx: tx,
            })
            .await
            .ok()
            .expect("Controller dropped, failed, or panicked");
        rx.await.expect("failed to get failpoint ack");
    }
}
