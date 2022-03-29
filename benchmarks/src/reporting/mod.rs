use std::time::SystemTime;

use anyhow::Context;
use hdrhistogram::Histogram;
use tokio_postgres::{Client, NoTls, Transaction};
use tracing::{info, warn};

use self::analysis::Stats;
use crate::benchmark::BenchmarkResults;

mod analysis;

pub use analysis::{Analysis, AnalysisInfo};

#[derive(clap::ArgEnum, Clone, Copy)]
pub enum ReportMode {
    StoreAndValidate,
    ValidateOnly,
}

impl ReportMode {
    fn should_store(&self) -> bool {
        match self {
            ReportMode::StoreAndValidate => true,
            ReportMode::ValidateOnly => false,
        }
    }

    fn should_validate(&self) -> bool {
        match self {
            ReportMode::StoreAndValidate => true,
            ReportMode::ValidateOnly => true,
        }
    }
}

type BenchSessionId = i32;
pub struct BenchSession {
    pub start_time: SystemTime,
    pub commit_id: String,
    pub template: String,
    pub profile_name: String,
}

pub async fn report(
    target: &str,
    session: &BenchSession,
    data: &BenchmarkResults,
    report_mode: ReportMode,
) -> anyhow::Result<Vec<AnalysisInfo>> {
    // Create client, open transaction
    let (mut client, connection) = tokio_postgres::connect(target, NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    let aggregated_data: Vec<_> = data
        .results
        .iter()
        .map(|(k, v)| {
            (
                k,
                &v.unit,
                v.desired_action,
                v.to_histogram(0.1, 0.9), /* Take the middle 80%
                                           * analysis::make_histogram(&v.values), */
            )
        })
        .collect();

    if report_mode.should_store() {
        let transaction = client.transaction().await?;
        let session_id = write_session(&transaction, session).await?;
        for (metric, unit, _, histogram) in &aggregated_data {
            write_aggregate(&transaction, session_id, metric, unit, histogram).await?;
        }

        transaction.commit().await?;
    }

    let mut out = vec![];
    if report_mode.should_validate() {
        for (metric, _, desired_action, ref histogram) in aggregated_data {
            let new_stats = Stats::with(histogram);
            let ordering =
                get_comparison_data(&client, &session.profile_name, metric, session.start_time)
                    .await?
                    .map(|old_stats| analysis::stdev_compare(&new_stats, &old_stats, 1.0));
            let metric = metric.to_owned();
            out.push(AnalysisInfo::new(metric, desired_action, ordering));
        }
    }

    Ok(out)
}

async fn get_comparison_data<'a>(
    client: &Client,
    profile: &str,
    metric: &str,
    start_time: SystemTime,
) -> anyhow::Result<Option<Stats>> {
    let statement = r#"
select
    ba.samples, ba.mean, ba.stdev
from
    bench_aggregate as ba, bench_session as bs
WHERE
    ba.session_id = bs.id AND start_time < $1 AND ba.metric = $2 AND bs.profile = $3 ORDER BY bs.start_time DESC LIMIT 1"#;
    let rows = client
        .query(statement, &[&start_time, &metric, &profile])
        .await
        .context("Failed fetching historic data")?;
    Ok(match &rows[..] {
        [] => {
            warn!(metric=%metric, "Historic comparison data not found.");
            None
        }
        [row, ..] => Some(Stats {
            samples: row.get(0),
            mean: row.get(1),
            stdev: row.get(2),
        }),
    })
}

async fn write_session<'a>(
    client: &Transaction<'a>,
    session: &BenchSession,
) -> anyhow::Result<BenchSessionId> {
    let query = r#"
INSERT INTO bench_session ("template", "profile", "start_time", "commit_id", "submission_ip")
VALUES ($1, $2, $3, $4, inet_client_addr())
RETURNING id"#;
    let row = client
        .query_one(
            query,
            &[
                &session.template,
                &session.profile_name,
                &session.start_time,
                &session.commit_id,
            ],
        )
        .await
        .context("Failed to write session data")?;
    let session_id = row.try_get(0)?;
    info!(session=%session_id, "Saving data to session");
    Ok(session_id)
}

async fn write_aggregate<'a>(
    client: &Transaction<'a>,
    session_id: BenchSessionId,
    metric: &str,
    unit: &str,
    hist: &Histogram<u64>,
) -> anyhow::Result<()> {
    let statement = r#"
INSERT INTO bench_aggregate (
    "session_id", "metric", "unit", "samples", "min", "max", "mean", "stdev", "p10", "p25",
    "p50", "p75", "p90", "p95", "p99", "p999", "p9999"
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)"#;
    let _ = client
        .execute(
            statement,
            &[
                &session_id,
                &metric,
                &unit,
                &(hist.len() as i64),
                &(hist.min() as i64),
                &(hist.max() as i64),
                &(hist.mean()),
                &(hist.stdev()),
                &(hist.value_at_quantile(0.10) as i64),
                &(hist.value_at_quantile(0.25) as i64),
                &(hist.value_at_quantile(0.5) as i64),
                &(hist.value_at_quantile(0.75) as i64),
                &(hist.value_at_quantile(0.90) as i64),
                &(hist.value_at_quantile(0.95) as i64),
                &(hist.value_at_quantile(0.99) as i64),
                &(hist.value_at_quantile(0.999) as i64),
                &(hist.value_at_quantile(0.9999) as i64),
            ],
        )
        .await
        .context("Failed to write aggregate data")?;

    Ok(())
}
