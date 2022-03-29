use std::cmp::Ordering;

use crate::benchmark::MetricGoal;

/// The conclusion from analyzing current vs historic benchmark data
#[derive(Debug)]
pub enum Analysis {
    Unanalyzed,
    Equivalent,
    Improved,
    Regressed,
}

#[derive(Debug)]
pub struct AnalysisInfo {
    pub metric: String,
    pub analysis: Analysis,
}

impl AnalysisInfo {
    #[must_use]
    pub fn new(
        metric: String,
        desired_action: MetricGoal,
        maybe_ordering: Option<Ordering>,
    ) -> AnalysisInfo {
        let analysis = match (maybe_ordering, desired_action) {
            (None, _) => Analysis::Unanalyzed,
            (Some(Ordering::Equal), _) => Analysis::Equivalent,
            (Some(Ordering::Greater), MetricGoal::Increasing) => Analysis::Improved,
            (Some(Ordering::Greater), MetricGoal::Decreasing) => Analysis::Regressed,
            (Some(Ordering::Less), MetricGoal::Increasing) => Analysis::Regressed,
            (Some(Ordering::Less), MetricGoal::Decreasing) => Analysis::Improved,
        };
        AnalysisInfo { metric, analysis }
    }
}

#[derive(Debug)]
pub struct Stats {
    pub samples: i64,
    pub mean: f64,
    pub stdev: f64,
}

impl Stats {
    pub fn with(hist: &hdrhistogram::Histogram<u64>) -> Stats {
        Stats {
            samples: hist.len() as i64,
            mean: hist.mean(),
            stdev: hist.stdev(),
        }
    }
}

pub(super) fn stdev_compare(stats1: &Stats, stats2: &Stats, deviations: f64) -> Ordering {
    let mean_delta = (stats1.mean - stats2.mean).abs();
    if mean_delta < (stats1.stdev * deviations) || mean_delta < (stats2.stdev * deviations) {
        Ordering::Equal
    } else if stats1.mean > stats2.mean {
        Ordering::Greater
    } else {
        Ordering::Less
    }
}
