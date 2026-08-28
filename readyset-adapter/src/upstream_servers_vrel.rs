use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use database_utils::{SharedUpstreamRegistry, UpstreamConfig, UpstreamServers};
use readyset_data::{DfType, DfValue};
use readyset_schema::bind_vrel;
use readyset_schema::virtual_relation::{
    UpstreamServerRow, UpstreamServersInfo, VrelContext, VrelRead, VrelRows,
};
use tokio::sync::RwLock;

const UPSTREAM_SERVERS_SCHEMA: &[(&str, DfType)] = &[
    ("host", DfType::DEFAULT_TEXT),
    ("port", DfType::UnsignedInt),
    ("database", DfType::DEFAULT_TEXT),
    ("user", DfType::DEFAULT_TEXT),
    ("weight", DfType::UnsignedInt),
    ("purpose", DfType::DEFAULT_TEXT),
    ("active_cdc", DfType::Bool),
    ("health", DfType::DEFAULT_TEXT),
    ("down_since", DfType::DEFAULT_TEXT),
    ("last_ok", DfType::DEFAULT_TEXT),
    ("connections", DfType::UnsignedBigInt),
];

/// The adapter's [`UpstreamServersInfo`]: the proxy and CDC lists with their health.
pub struct AdapterUpstreamServers {
    pub config: Arc<RwLock<UpstreamConfig>>,
    pub registry: Arc<SharedUpstreamRegistry>,
}

#[async_trait]
impl UpstreamServersInfo for AdapterUpstreamServers {
    async fn servers(&self) -> Vec<UpstreamServerRow> {
        let cdc: Option<UpstreamServers> = self.config.read().await.cdc_servers().ok();
        let proxy = self.registry.current();

        let mut rows: Vec<UpstreamServerRow> = Vec::new();
        if let Some(proxy) = &proxy {
            for (idx, server) in proxy.servers().iter().enumerate() {
                let state = proxy.state(idx);
                rows.push(UpstreamServerRow {
                    server: server.display().to_string(),
                    host: server.url().host().unwrap_or_default().to_string(),
                    port: server.url().port(),
                    database: server.url().db_name().map(str::to_owned),
                    user: server.url().user().map(str::to_owned),
                    weight: server.weight(),
                    proxy: true,
                    cdc: cdc
                        .as_ref()
                        .is_some_and(|c| c.position(server.display()).is_some()),
                    up: Some(state.is_up()),
                    down_since: state.down_since(),
                    last_ok: state.last_ok(),
                    connections: Some(state.connections()),
                });
            }
        }
        // CDC-only servers carry no adapter-side health or connections.
        if let Some(cdc) = &cdc {
            for server in cdc.iter() {
                if rows.iter().any(|r| r.server == server.display()) {
                    continue;
                }
                rows.push(UpstreamServerRow {
                    server: server.display().to_string(),
                    host: server.url().host().unwrap_or_default().to_string(),
                    port: server.url().port(),
                    database: server.url().db_name().map(str::to_owned),
                    user: server.url().user().map(str::to_owned),
                    weight: server.weight(),
                    proxy: false,
                    cdc: true,
                    up: None,
                    down_since: None,
                    last_ok: None,
                    connections: None,
                });
            }
        }
        rows
    }
}

fn time_value(at: Option<std::time::SystemTime>) -> DfValue {
    match at {
        Some(at) => DateTime::<Utc>::from(at).to_rfc3339().into(),
        None => DfValue::None,
    }
}

/// Backs `SELECT * FROM readyset.upstream_servers`. `active_cdc` is null when the controller is
/// unreachable.
fn upstream_servers_read(ctx: &VrelContext) -> VrelRead {
    let info = Arc::clone(&ctx.upstream_servers);
    let mut controller = ctx.controller.clone();
    Box::pin(async move {
        let active = controller.replication_server().await.ok();
        let rows = info.servers().await;
        let rows: VrelRows = Box::new(rows.into_iter().map(move |row| {
            let purpose = match (row.cdc, row.proxy) {
                (true, true) => "cdc, proxy",
                (true, false) => "cdc",
                (false, true) => "proxy",
                (false, false) => "",
            };
            let active_cdc = match &active {
                Some(active) => (active.as_deref() == Some(row.server.as_str())).into(),
                None => DfValue::None,
            };
            vec![
                row.host.into(),
                DfValue::UnsignedInt(row.port.into()),
                row.database.map(DfValue::from).unwrap_or(DfValue::None),
                row.user.map(DfValue::from).unwrap_or(DfValue::None),
                DfValue::UnsignedInt(row.weight.into()),
                purpose.into(),
                active_cdc,
                match row.up {
                    Some(true) => "up".into(),
                    Some(false) => "down".into(),
                    None => DfValue::None,
                },
                time_value(row.down_since),
                time_value(row.last_ok),
                row.connections
                    .map(DfValue::UnsignedInt)
                    .unwrap_or(DfValue::None),
            ]
        }));
        Ok(rows)
    })
}
bind_vrel!(
    upstream_servers,
    UPSTREAM_SERVERS_SCHEMA,
    upstream_servers_read
);

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn rows_join_the_proxy_and_cdc_lists() {
        let config = UpstreamConfig {
            upstream_db_url: "mysql://u:p@a:3306/db|60,mysql://u:p@b:3306/db|50"
                .parse::<UpstreamServers>()
                .unwrap()
                .into_vec(),
            cdc_db_url: "mysql://r:p@b:3306/db,mysql://r:p@c:3306/db"
                .parse::<UpstreamServers>()
                .unwrap()
                .into_vec(),
            ..Default::default()
        };
        let registry = Arc::new(SharedUpstreamRegistry::new(
            config.upstream_servers().unwrap(),
        ));
        registry.current().unwrap().mark_down(1, &"refused");
        let info = AdapterUpstreamServers {
            config: Arc::new(RwLock::new(config)),
            registry,
        };
        let rows = info.servers().await;
        let by_server = |s: &str| rows.iter().find(|r| r.server == s).expect("row exists");

        let a = by_server("a:3306");
        assert!(a.proxy && !a.cdc);
        assert_eq!(a.up, Some(true));
        assert_eq!(a.connections, Some(0));
        assert_eq!(a.user.as_deref(), Some("u"));

        let b = by_server("b:3306");
        assert!(b.proxy && b.cdc);
        assert_eq!(b.up, Some(false));
        assert!(b.down_since.is_some());

        let c = by_server("c:3306");
        assert!(!c.proxy && c.cdc);
        assert_eq!(c.up, None);
        assert_eq!(c.connections, None);
        assert_eq!(rows.len(), 3);
    }
}
