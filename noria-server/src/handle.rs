use crate::controller::migrate::Migration;
use crate::startup::Event;
use dataflow::prelude::*;
use futures_util::sink::SinkExt;
use noria::consensus::Authority;
use noria::prelude::*;
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use stream_cancel::Trigger;
use tokio_io_pool;

/// A handle to a controller that is running in the same process as this one.
pub struct Handle<A: Authority + 'static> {
    c: Option<ControllerHandle<A>>,
    #[allow(dead_code)]
    event_tx: Option<tokio::sync::mpsc::UnboundedSender<Event>>,
    kill: Option<Trigger>,
    iopool: Option<tokio_io_pool::Runtime>,
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
    pub(super) async fn new(
        authority: Arc<A>,
        event_tx: tokio::sync::mpsc::UnboundedSender<Event>,
        kill: Trigger,
        io: tokio_io_pool::Runtime,
    ) -> Result<Self, failure::Error> {
        let c = ControllerHandle::make(authority).await?;
        Ok(Handle {
            c: Some(c),
            event_tx: Some(event_tx),
            kill: Some(kill),
            iopool: Some(io),
        })
    }

    #[cfg(test)]
    pub(super) async fn backend_ready(&mut self) {
        use std::time;

        loop {
            let (tx, rx) = tokio::sync::oneshot::channel();
            self.event_tx
                .as_mut()
                .unwrap()
                .send(Event::IsReady(tx))
                .await
                .unwrap();
            if rx.await.unwrap() {
                break;
            }

            tokio::timer::delay(time::Instant::now() + time::Duration::from_millis(50)).await;
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
                unreachable!("could not return migration result");
            }
        });

        self.event_tx
            .as_mut()
            .unwrap()
            .send(Event::ManualMigration { f: b, done: fin_tx })
            .await
            .unwrap();

        fin_rx.await.unwrap();
        ret_rx.await.unwrap()
    }

    /// Install a new set of policies on the controller.
    #[must_use]
    pub async fn set_security_config(&mut self, p: String) -> Result<(), failure::Error> {
        self.rpc("set_security_config", p, "failed to set security config")
            .await
    }

    /// Install a new set of policies on the controller.
    #[must_use]
    pub async fn create_universe(
        &mut self,
        context: HashMap<String, DataType>,
    ) -> Result<(), failure::Error> {
        let mut c = self.c.clone().unwrap();

        let uid = context
            .get("id")
            .expect("Universe context must have id")
            .clone();
        let _ = self
            .rpc::<_, ()>(
                "create_universe",
                &context,
                "failed to create security universe",
            )
            .await?;

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
        if let Some(io) = self.iopool.take() {
            drop(self.c.take());
            drop(self.event_tx.take());
            drop(self.kill.take());
            io.shutdown_on_idle();
        }
    }
}

impl<A: Authority> Drop for Handle<A> {
    fn drop(&mut self) {
        self.shutdown();
    }
}
