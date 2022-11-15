use std::time::SystemTime;

use async_trait::async_trait;

use super::analysis::Stats;
use super::{BenchSession, ProcessedData};

pub mod json;
pub mod postgres;

#[async_trait]
pub trait Storage {
    async fn write(&mut self, session: &BenchSession, data: &[ProcessedData])
        -> anyhow::Result<()>;

    async fn get_comparison_data(
        &self,
        template: &str,
        profile: &str,
        metric: &str,
        start_time: SystemTime,
    ) -> anyhow::Result<Option<Stats>>;
}

impl dyn Storage {
    pub async fn new(target: &str) -> anyhow::Result<Box<dyn Storage>> {
        if target.starts_with(postgres::PREFIX) {
            Ok(Box::new(postgres::PostgresStorage::new(target).await?))
        } else if target.starts_with(json::PREFIX) {
            Ok(Box::new(json::JsonStorage::new(
                target.split_at(json::PREFIX.len()).1,
            )?))
        } else {
            anyhow::bail!(
                "Unable to determine which engine to use for target: {}",
                target
            );
        }
    }
}
