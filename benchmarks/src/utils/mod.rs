use std::num::ParseIntError;
use std::str::FromStr;
use std::time::Duration;

use tracing::info;

use database_utils::{tls::ServerCertVerification, DatabaseURL, QueryableConnection};
use readyset_client::status::CurrentStatus;
use readyset_data::{Collation, DfValue, Dialect};
use readyset_sql::{ast::ShowStatement, DialectDisplay};

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
        match DatabaseURL::from_str(target)?
            .connect(&ServerCertVerification::Default)
            .await
        {
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
            let rows = Vec::<Vec<DfValue>>::try_from(data).unwrap();
            let snapshot_status = rows.into_iter().find(|r| match &r[0] {
                DfValue::ByteArray(bytes) => std::str::from_utf8(bytes)
                    .map(|s| s == "Status")
                    .unwrap_or(false),
                DfValue::Text(s) => s.as_str() == "Status",
                DfValue::TinyText(s) => s.as_str() == "Status",
                _ => false,
            });

            if let Some(s) = snapshot_status {
                let status: String = s[1]
                    .coerce_to(
                        &readyset_data::DfType::Text(Collation::default_for(
                            Dialect::DEFAULT_MYSQL,
                        )),
                        &readyset_data::DfType::Unknown,
                    )
                    .unwrap()
                    .try_into()
                    .unwrap();

                if status == CurrentStatus::Online.to_string() {
                    info!("Database ready!");
                    return Ok(());
                }
            }
        }

        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}
