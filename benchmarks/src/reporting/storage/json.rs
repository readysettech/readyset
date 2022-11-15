use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, BufWriter, ErrorKind};
use std::time::SystemTime;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use super::Storage;
use crate::benchmark::MetricGoal;
use crate::reporting::analysis::Stats;
use crate::reporting::{BenchSession, ProcessedData};

type Template = String;
type Profile = String;
type Metric = String;

#[derive(Serialize, Deserialize, Default)]
struct DiskFormat {
    templates: HashMap<Template, HashMap<Profile, HashMap<Metric, Vec<Data>>>>,
}

#[derive(Serialize, Deserialize)]
struct Data {
    timestamp: u128,
    unit: String,
    goal: MetricGoal,
    samples: u64,
    min: u64,
    max: u64,
    mean: f64,
    stdev: f64,
    p10: u64,
    p25: u64,
    p50: u64,
    p75: u64,
    p90: u64,
    p95: u64,
    p99: u64,
    p99_9: u64,
    p99_99: u64,
}

impl Data {
    fn to_stats(&self) -> Stats {
        Stats {
            samples: self.samples as i64,
            mean: self.mean,
            stdev: self.stdev,
        }
    }

    fn with(timestamp: u128, item: &ProcessedData) -> Data {
        Data {
            timestamp,
            unit: item.unit.to_owned(),
            goal: item.goal,
            samples: item.histogram.len(),
            min: item.histogram.min(),
            max: item.histogram.max(),
            mean: item.histogram.mean(),
            stdev: item.histogram.stdev(),
            p10: item.histogram.value_at_quantile(0.10),
            p25: item.histogram.value_at_quantile(0.25),
            p50: item.histogram.value_at_quantile(0.5),
            p75: item.histogram.value_at_quantile(0.75),
            p90: item.histogram.value_at_quantile(0.90),
            p95: item.histogram.value_at_quantile(0.95),
            p99: item.histogram.value_at_quantile(0.99),
            p99_9: item.histogram.value_at_quantile(0.999),
            p99_99: item.histogram.value_at_quantile(0.9999),
        }
    }
}

pub const PREFIX: &str = "file:";

pub struct JsonStorage {
    target: String,
    cache: DiskFormat,
}

impl JsonStorage {
    pub fn new(target: &str) -> anyhow::Result<JsonStorage> {
        Ok(JsonStorage {
            target: target.to_string(),
            cache: Self::prep_cache(target)?,
        })
    }

    fn prep_cache(target: &str) -> anyhow::Result<DiskFormat> {
        let file = match File::open(target) {
            Ok(file) => file,
            Err(err) if err.kind() == ErrorKind::NotFound => {
                return Ok(DiskFormat::default());
            }
            Err(err) => return Err(err.into()),
        };
        let reader = BufReader::new(file);
        Ok(serde_json::from_reader(reader)?)
    }
}

#[async_trait]
impl Storage for JsonStorage {
    async fn write(
        &mut self,
        session: &BenchSession,
        data: &[ProcessedData],
    ) -> anyhow::Result<()> {
        let historic = &mut self.cache;
        let metrics = historic
            .templates
            .entry(session.template.to_string())
            .or_default()
            .entry(session.profile_name.to_string())
            .or_default();
        for item in data.iter() {
            let data = Data::with(
                session
                    .start_time
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_millis(),
                item,
            );
            metrics
                .entry(item.metric.to_string())
                .or_default()
                .push(data);
        }

        let file = File::create(&self.target)?;
        serde_json::to_writer(BufWriter::new(file), &historic)?;

        Ok(())
    }

    async fn get_comparison_data(
        &self,
        template: &str,
        profile: &str,
        metric: &str,
        start_time: SystemTime,
    ) -> anyhow::Result<Option<Stats>> {
        let start_time = start_time
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis();

        let comparison_data = self
            .cache
            .templates
            .get(template)
            .and_then(|t| t.get(profile))
            .and_then(|p| p.get(metric))
            .and_then(|m| m.iter().rev().find(|item| item.timestamp != start_time))
            .map(Data::to_stats);

        Ok(comparison_data)
    }
}
