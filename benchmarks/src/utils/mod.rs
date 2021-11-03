use anyhow::Result;
use std::future::Future;
use std::num::ParseIntError;
use std::str::FromStr;
use std::time::Duration;

pub mod generate;
pub mod multi_thread;
pub mod random;
pub mod spec;

pub fn seconds_as_str_to_duration(input: &str) -> std::result::Result<Duration, ParseIntError> {
    Ok(Duration::from_secs(u64::from_str(input)?))
}

pub async fn run_for(
    func: impl Future<Output = Result<()>>,
    duration: Option<Duration>,
) -> Result<()> {
    if let Some(duration) = duration {
        match tokio::time::timeout(duration, func).await {
            Ok(r) => r,
            Err(_) => Ok(()), //Timeout was hit without failing prior.
        }
    } else {
        func.await
    }
}
