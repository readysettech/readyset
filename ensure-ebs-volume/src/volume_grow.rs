use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{anyhow, Result};
use aws_sdk_ec2 as ec2;
use nix::sys::statvfs::statvfs;
use tokio::process::Command;
use tokio::time::sleep;
use tracing::{error, info, instrument, warn};

use crate::util::run_command;

const POLL_INTERVAL: Duration = Duration::from_secs(15);

/// Usage ratio threshold at which to grow volumes
const RESIZE_USAGE_THRESHOLD: f64 = 0.8;

async fn current_volume_size(ec2: &ec2::Client, ebs_volume_id: &str) -> Result<i32> {
    ec2.describe_volumes()
        .volume_ids(ebs_volume_id)
        .send()
        .await?
        .volumes
        .and_then(|vols| vols.first().cloned())
        .ok_or_else(|| anyhow!("Volume {} not found", &ebs_volume_id))?
        .size
        .ok_or_else(|| anyhow!("Volume {} missing size", &ebs_volume_id))
}

async fn grow_volume(
    ec2: &ec2::Client,
    ebs_volume_id: &str,
    new_size: i32,
    block_device_path: &Path,
) -> Result<()> {
    ec2.modify_volume()
        .volume_id(ebs_volume_id)
        .size(new_size)
        .send()
        .await?;

    info!("Waiting for volume to reach target size...");
    while current_volume_size(ec2, ebs_volume_id).await? < new_size {
        sleep(POLL_INTERVAL).await;
    }

    run_command(Command::new("resize2fs").arg(block_device_path)).await?;

    Ok(())
}

/// Run a loop that polls the block device at `block_device_path` mounted at `mount_path` (which
/// should have `ebs_volume_id` mounted to it) every [`POLL_INTERVAL`] and, if it's over
/// [`RESIZE_USAGE_THRESHOLD`] usage, doubles the size of the volume up to a max of
/// `max_volume_size`.
#[instrument(name = "volume_grow", skip(ec2))]
pub(crate) async fn run(
    ec2: ec2::Client,
    ebs_volume_id: String,
    block_device_path: PathBuf,
    mount_path: PathBuf,
    max_volume_size: Option<i32>,
) -> Result<()> {
    let mut volume_size = current_volume_size(&ec2, &ebs_volume_id).await?;
    info!(initial_volume_size = %volume_size);

    loop {
        sleep(POLL_INTERVAL).await;
        let stat = match statvfs(&mount_path) {
            Ok(res) => res,
            Err(error) => {
                error!(%error, "Error calling statvfs");
                continue;
            }
        };

        let usage_percent = 1.0 - (stat.blocks_available() as f64 / stat.blocks() as f64);
        if usage_percent >= RESIZE_USAGE_THRESHOLD {
            let new_size = volume_size * 2;

            if max_volume_size.iter().any(|max| new_size >= *max) {
                warn!(
                    %usage_percent,
                    ?max_volume_size,
                    %new_size,
                    "Above threshold, but not resizing volume as it would put us above max_volume_size"
                );
                // No point in continuing to check since we've hit the max size
                return Ok(());
            }

            info!(%usage_percent, %new_size, "Above threshold, resizing volume");
            if let Err(error) =
                grow_volume(&ec2, &ebs_volume_id, volume_size, &block_device_path).await
            {
                error!(%error, "Error growing volume");
                continue;
            }
            volume_size = new_size;
        }
    }
}
