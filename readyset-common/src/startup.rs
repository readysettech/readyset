//! Common startup logic that runs once per process.

use anyhow::Result;

use database_utils::UpstreamConfig;

use crate::ulimit::maybe_increase_nofile_limit;

pub fn init_early_common(config: &UpstreamConfig) -> Result<()> {
    maybe_increase_nofile_limit(config.ignore_ulimit_check)?;
    Ok(())
}
