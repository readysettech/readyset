use anyhow::{bail, Context};
use rlimit::increase_nofile_limit;
use tracing::warn;

const SOFT_NOFILE_LIMIT: u64 = 32768;

/// Check that file descriptor limit is large enough to support compaction of database
/// sizes up to approximately 2TB
pub fn maybe_increase_nofile_limit(ignore_ulimit_check: bool) -> anyhow::Result<()> {
    let desired = SOFT_NOFILE_LIMIT;
    let actual =
        increase_nofile_limit(desired).context("Failed to increase soft file descriptor limit")?;
    if actual < desired {
        if ignore_ulimit_check {
            warn!(
                "File descriptor limit (RLIMIT_NOFILE) is set too low at {actual}. ReadySet \
                 may need up to {desired}."
            );
        } else {
            bail!(
                "File descriptor limit (RLIMIT_NOFILE) is set too low at {actual}. ReadySet \
                 may need up to {desired}. Please increase this limit, or use \
                 --ignore-ulimit-check to continue anyway."
            );
        }
    }
    Ok(())
}
