use std::num::ParseIntError;
use std::str::FromStr;
use std::time::Duration;

use database_utils::{DatabaseURL, QueryableConnection};
use readyset_client::status::CurrentStatus;
use readyset_data::DfValue;
use readyset_sql::{ast::ShowStatement, DialectDisplay};
use tracing::info;

pub mod generate;
pub mod multi_thread;
pub mod path;
pub mod query;
pub mod spec;

pub fn us_to_ms(us: u64) -> f64 {
    us as f64 / 1000.
}

pub fn seconds_as_str_to_duration(input: &str) -> std::result::Result<Duration, ParseIntError> {
    Ok(Duration::from_secs(u64::from_str(input)?))
}

/// Wait for replication to finish by lazy looping over "SHOW READYSET STATUS"
pub async fn readyset_ready(target: &str) -> anyhow::Result<()> {
    info!("Waiting for the target database to be ready...");
    // First attempt to connect to the readyset adapter at all
    let mut conn = loop {
        match DatabaseURL::from_str(target)?.connect(None).await {
            Ok(conn) => break conn,
            _ => tokio::time::sleep(Duration::from_secs(1)).await,
        }
    };

    // Then query status until snapshot is completed
    let q = ShowStatement::ReadySetStatus;
    loop {
        // We have to use simple query here because ReadySet does not support preparing `SHOW`
        // queries
        let res = conn
            .simple_query(q.display(readyset_sql::Dialect::MySQL).to_string())
            .await;

        if let Ok(data) = res {
            let snapshot_status = Vec::<Vec<DfValue>>::try_from(data)
                .unwrap()
                .into_iter()
                .find(|r| r[0] == "Status".into());
            let snapshot_status: String = if let Some(s) = snapshot_status {
                s[1].clone().try_into().unwrap()
            } else {
                continue;
            };

            if snapshot_status == CurrentStatus::Online.to_string() {
                info!("Database ready!");
                return Ok(());
            }
        }

        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}
