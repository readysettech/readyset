use anyhow::{bail, Context};
use rlimit::increase_nofile_limit;
use tracing::info;

/// Check that file descriptor limit is large enough to support compaction of database
/// sizes up to approximately 2TB
pub fn maybe_increase_nofile_limit(ignore_ulimit_check: bool) -> anyhow::Result<()> {
    #[cfg(not(target_os = "macos"))]
    let desired = 1048576;
    #[cfg(target_os = "macos")]
    let desired = 32768;
    let actual =
        increase_nofile_limit(desired).context("Failed to increase soft file descriptor limit")?;
    if actual < desired {
        if ignore_ulimit_check {
            info!(
                "Attempted to set process file descriptor limit to {desired}, but capped out at \
                 {actual} instead."
            );
        } else {
            bail!(
                "Attempted to set process file descriptor limit to {desired}, but cannot increase \
                 beyond {actual}. Please increase this limit or use --ignore-ulimit-check \
                 (environment variable IGNORE_ULIMIT_CHECK=true) to continue anyway."
            );
        }
    }
    Ok(())
}
