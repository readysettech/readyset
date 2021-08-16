use crate::controller::migrate::Migration;
use crate::controller::HandleRequest;
use crate::errors::bad_request_err;
use crate::ControllerDescriptor;
use dataflow::prelude::*;
use noria::consensus::Authority;
use noria::internal;
use noria::prelude::*;
use reqwest::Url;
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use stream_cancel::Trigger;
use tokio::sync::mpsc::Sender;

/// A handle to a controller that is running in the same process as this one.
pub struct Handle<A: Authority + 'static> {
    /// Has a valid controller handle on `new` and is set to None if the
    /// controller has been shutdown.
    pub c: Option<ControllerHandle<A>>,
    #[allow(dead_code)]
    event_tx: Option<Sender<HandleRequest>>,
    kill: Option<Trigger>,
    descriptor: ControllerDescriptor,
}

impl<A: Authority> Deref for Handle<A> {
    type Target = ControllerHandle<A>;
    fn deref(&self) -> &Self::Target {
        self.c.as_ref().unwrap()
    }
}

impl<A: Authority> DerefMut for Handle<A> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.c.as_mut().unwrap()
    }
}

impl<A: Authority + 'static> Handle<A> {
    pub(super) fn new(
        authority: Arc<A>,
        event_tx: Sender<HandleRequest>,
        kill: Trigger,
        descriptor: ControllerDescriptor,
    ) -> Self {
        let c = ControllerHandle::make(authority);
        Handle {
            c: Some(c),
            event_tx: Some(event_tx),
            kill: Some(kill),
            descriptor,
        }
    }

    /// Returns the address of this Noria server.
    pub fn get_address(&self) -> &Url {
        &self.descriptor.controller_uri
    }

    #[cfg(test)]
    pub(super) async fn backend_ready(&mut self) {
        use std::time;

        loop {
            let (tx, rx) = tokio::sync::oneshot::channel();
            self.event_tx
                .as_mut()
                .unwrap()
                .send(HandleRequest::QueryReadiness(tx))
                .await
                .ok()
                .expect("ControllerOuter dropped, failed, or panicked");

            if rx.await.unwrap() {
                break;
            }

            tokio::time::sleep(time::Duration::from_millis(50)).await;
        }
    }

    #[doc(hidden)]
    pub async fn migrate<F, T>(&mut self, f: F) -> T
    where
        F: FnOnce(&mut Migration) -> T + Send + 'static,
        T: Send + 'static,
    {
        let (ret_tx, ret_rx) = tokio::sync::oneshot::channel();
        let (fin_tx, fin_rx) = tokio::sync::oneshot::channel();
        let b = Box::new(move |m: &mut Migration| {
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
            .expect("ControllerOuter dropped, failed, or panicked");

        fin_rx.await.unwrap().unwrap();
        ret_rx.await.unwrap()
    }

    /// Install a new set of policies on the controller.
    pub async fn set_security_config(&mut self, p: String) -> Result<(), anyhow::Error> {
        self.rpc("set_security_config", p).await?;
        Ok(())
    }

    /// Install a new set of policies on the controller.
    pub async fn create_universe(
        &mut self,
        context: HashMap<String, DataType>,
    ) -> Result<(), anyhow::Error> {
        let mut c = self.c.clone().unwrap();

        let uid = context
            .get("id")
            .ok_or_else(|| bad_request_err("Universe context must have id"))?
            .clone();
        let _ = self.rpc::<_, ()>("create_universe", &context).await?;

        // Write to Context table
        let bname = match context.get("group") {
            None => format!("UserContext_{}", uid.to_string()),
            Some(g) => format!("GroupContext_{}_{}", g.to_string(), uid.to_string()),
        };

        let mut fields: Vec<_> = context.keys().collect();
        fields.sort();
        let record: Vec<DataType> = fields.iter().map(|&f| context[f].clone()).collect();

        let mut table = c.table(&bname).await?;
        let fut = table.insert(record);
        // can't await immediately because of
        // https://gist.github.com/nikomatsakis/fee0e47e14c09c4202316d8ea51e50a0
        fut.await
            .map_err(|e| format_err!("failed to make table: {:?}", e))
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

impl<A: Authority> Drop for Handle<A> {
    fn drop(&mut self) {
        self.shutdown();
    }
}
