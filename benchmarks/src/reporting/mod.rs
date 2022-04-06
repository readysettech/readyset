use std::time::SystemTime;

pub use analysis::{Analysis, AnalysisInfo};
use hdrhistogram::Histogram;
use storage::*;

use self::analysis::Stats;
use crate::benchmark::{BenchmarkResults, MetricGoal};

mod analysis;
mod storage;

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

pub struct ProcessedData {
    metric: String,
    unit: String,
    goal: MetricGoal,
    histogram: Histogram<u64>,
}

impl ProcessedData {
    #[must_use]
    fn new(metric: String, unit: String, goal: MetricGoal, histogram: Histogram<u64>) -> Self {
        Self {
            metric,
            unit,
            goal,
            histogram,
        }
    }
}

pub async fn report(
    target: &str,
    session: &BenchSession,
    data: &BenchmarkResults,
    report_mode: ReportMode,
) -> anyhow::Result<Vec<AnalysisInfo>> {
    let mut storage = <dyn Storage>::new(target).await?;

    let aggregated_data = data
        .results
        .iter()
        .map(|(k, v)| {
            // Take the middle 80%
            let histogram = v.to_histogram(0.1, 0.9);
            ProcessedData::new(
                k.to_string(),
                v.unit.to_string(),
                v.desired_action,
                histogram,
            )
        })
        .collect::<Vec<_>>();

    if report_mode.should_store() {
        storage.write(session, &aggregated_data).await?;
    }

    let mut out = vec![];
    if report_mode.should_validate() {
        for item in aggregated_data {
            let new_stats = Stats::with(&item.histogram);
            let ordering = storage
                .get_comparison_data(
                    &session.template,
                    &session.profile_name,
                    &item.metric,
                    session.start_time,
                )
                .await?
                .map(|old_stats| analysis::stdev_compare(&new_stats, &old_stats, 1.0));
            out.push(AnalysisInfo::new(
                item.metric.to_owned(),
                item.goal,
                ordering,
            ));
        }
    }

    Ok(out)
}
