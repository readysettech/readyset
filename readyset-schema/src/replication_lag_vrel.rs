use std::future::Future;
use std::pin::Pin;

use readyset_client::ReadySetHandle;
use readyset_client::status::ReplicationLagStatus;
use readyset_data::DfType;

use crate::bind_vrel;
use crate::virtual_relation::{VrelContext, VrelRead, VrelRows};

/// Trait for accessing replication lag status in virtual relations.
pub trait ReplicationLagInfo: Send + Sync {
    fn lag_status(&self)
    -> Pin<Box<dyn Future<Output = Option<ReplicationLagStatus>> + Send + '_>>;
}

/// Provides replication lag status via controller RPC.
pub struct ControllerReplicationLag {
    handle: ReadySetHandle,
}

impl ControllerReplicationLag {
    pub fn new(handle: ReadySetHandle) -> Self {
        Self { handle }
    }
}

impl ReplicationLagInfo for ControllerReplicationLag {
    fn lag_status(
        &self,
    ) -> Pin<Box<dyn Future<Output = Option<ReplicationLagStatus>> + Send + '_>> {
        Box::pin(async {
            let mut handle = self.handle.clone();
            handle.replication_lag_status().await.ok().flatten()
        })
    }
}

/// A no-op implementation for when replication lag reporting is not available.
pub struct NoReplicationLag;

impl ReplicationLagInfo for NoReplicationLag {
    fn lag_status(
        &self,
    ) -> Pin<Box<dyn Future<Output = Option<ReplicationLagStatus>> + Send + '_>> {
        Box::pin(async { None })
    }
}

const REPLICATION_STATUS_SCHEMA: &[(&str, DfType)] = &[
    ("replication_mode", DfType::DEFAULT_TEXT),
    ("upstream_offset", DfType::DEFAULT_TEXT),
    ("replicator_offset", DfType::DEFAULT_TEXT),
    ("persisted_offset", DfType::DEFAULT_TEXT),
    ("consume_lag", DfType::UnsignedBigInt),
    ("persist_lag", DfType::UnsignedBigInt),
];

fn replication_status_read(ctx: &VrelContext) -> VrelRead {
    let repl_lag = ctx.repl_lag.clone();
    Box::pin(async move {
        let status = repl_lag.lag_status().await;
        let rows: VrelRows = match status {
            Some(s) => Box::new(std::iter::once(vec![
                s.mode.to_string().into(),
                s.upstream_offset.into(),
                s.replicator_offset.into(),
                s.persisted_offset.into(),
                (s.consume_lag).into(),
                (s.persist_lag).into(),
            ])),
            None => Box::new(std::iter::empty()),
        };
        Ok(rows)
    })
}
bind_vrel!(
    replication_status,
    REPLICATION_STATUS_SCHEMA,
    replication_status_read
);
