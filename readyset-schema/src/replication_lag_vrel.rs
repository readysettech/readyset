use readyset_data::{DfType, DfValue};

use crate::bind_vrel;
use crate::virtual_relation::{VrelContext, VrelRead, VrelRows};

const REPLICATION_STATUS_SCHEMA: &[(&str, DfType)] = &[
    ("replication_mode", DfType::DEFAULT_TEXT),
    ("upstream_offset", DfType::DEFAULT_TEXT),
    ("replicator_offset", DfType::DEFAULT_TEXT),
    ("persisted_offset", DfType::DEFAULT_TEXT),
    ("consume_lag", DfType::UnsignedBigInt),
    ("persist_lag", DfType::UnsignedBigInt),
    ("heartbeat_staleness_seconds", DfType::Double),
];

fn replication_status_read(ctx: &VrelContext) -> VrelRead {
    let mut controller = ctx.controller.clone();
    Box::pin(async move {
        let status = controller.replication_lag_status().await.ok().flatten();
        let rows: VrelRows = match status {
            Some(s) => Box::new(std::iter::once(vec![
                s.mode.to_string().into(),
                s.upstream_offset.into(),
                s.replicator_offset.into(),
                s.persisted_offset.into(),
                (s.consume_lag).into(),
                (s.persist_lag).into(),
                s.staleness_seconds
                    .map(DfValue::Double)
                    .unwrap_or(DfValue::None),
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
