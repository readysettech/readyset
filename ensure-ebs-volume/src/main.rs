#![feature(iter_intersperse)]

use std::env;
use std::iter;
use std::path::Path;
use std::path::PathBuf;
use std::str;
use std::time::Duration;

use anyhow::{anyhow, bail, Result};
use aws_config::provider_config::ProviderConfig;
use aws_sdk_ec2::model::{Filter, ResourceType, Tag, TagSpecification, VolumeState};
use aws_sdk_ec2::Client;
use aws_types::region::Region;
use clap::Clap;
use lazy_static::lazy_static;
use regex::Regex;
use tokio::fs::create_dir_all;
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::process::Command;
use tokio::task;
use tokio::time;
use tracing::debug;
use tracing::{info, instrument};
use tracing_subscriber::EnvFilter;

const WAIT_TIMER: Duration = Duration::from_secs(1);

#[derive(Clap, Debug)]
/// This utility will ensure that a persistent EBS volume is attached to the instance on which it
/// is running.  This includes creating a volume if one does not exist, attaching it to the
/// instance, formatting it, and mounting it.
struct Opts {
    /// Filesystem path at which to mount the device
    #[clap(default_value = "/data")]
    mountpoint: PathBuf,

    /// Block device name of the device to mount
    #[clap(default_value = "sdb")]
    device: PathBuf,

    /// Size of the volume to create in gigabytes
    #[clap(long, default_value = "32", env = "VOLUME_SIZE_GB")]
    volume_size_gb: i32,

    /// Tag key to use for volumes
    #[clap(long, default_value = "ReadySet:ServerVolume", env = "VOLUME_TAG_KEY")]
    volume_tag_key: String,

    /// Tag value to use for volumes
    #[clap(long, default_value = "true", env = "VOLUME_TAG_VALUE")]
    volume_tag_value: String,
}

fn filter<K, V>(key: K, value: V) -> Filter
where
    K: Into<String>,
    V: Into<String>,
{
    Filter::builder().name(key).values(value).build()
}

fn tag<K, V>(key: K, value: V) -> Tag
where
    K: Into<String>,
    V: Into<String>,
{
    Tag::builder().key(key).value(value).build()
}

async fn exists(path: &Path) -> Result<bool> {
    let path = path.to_path_buf();
    Ok(task::spawn_blocking(move || path.exists()).await?)
}

async fn run(command: &mut Command) -> Result<()> {
    let status = command.status().await?;
    if !status.success() {
        bail!("Command exited with {}", status);
    }
    Ok(())
}

#[derive(Debug)]
struct AttachedVolume {
    ebs_volume_id: String,
    block_device_path: PathBuf,
}

#[instrument(skip(ec2))]
async fn wait_for_volume_state(
    ec2: &Client,
    volume_id: &str,
    desired_state: VolumeState,
) -> Result<()> {
    loop {
        info!("Waiting for volume to become {}...", desired_state.as_str());
        let state = ec2
            .describe_volumes()
            .volume_ids(volume_id.to_string())
            .send()
            .await?
            .volumes
            .into_iter()
            .flat_map(|volumes| volumes.into_iter().next())
            .flat_map(|volume| volume.state)
            .next();
        if let Some(state) = state {
            if state == desired_state {
                break;
            }
        } else {
            bail!("Volume state not found");
        }
        time::sleep(WAIT_TIMER).await;
    }
    Ok(())
}

#[instrument]
async fn find_nvme_device(device_name: &Path) -> Result<Option<PathBuf>> {
    info!("Trying to find nvme device");
    let nvme_list_output = Command::new("nvme")
        .args(&["list", "-o", "json"])
        .output()
        .await?;
    if !nvme_list_output.status.success() {
        bail!("`nvme list` failed with {}", nvme_list_output.status)
    }

    let json = serde_json::from_slice::<serde_json::Value>(&nvme_list_output.stdout)?;
    let devices = json
        .get("Devices")
        .ok_or_else(|| anyhow!("Invalid JSON output from nvme list; expected Devices key"))?
        .as_array()
        .ok_or_else(|| {
            anyhow!("Invalid JSON output from nvme list; expected array at $.Devices")
        })?;

    for device in devices {
        let nvme_device_path = device
            .get("DevicePath")
            .ok_or_else(|| anyhow!("Invalid JSON output from nvme list; missing DevicePath"))?
            .as_str()
            .ok_or_else(|| {
                anyhow!("Invalid JSON output from nvme list; DevicePath must be a string")
            })?;

        // https://github.com/transferwise/ansible-ebs-automatic-nvme-mapping
        let id_ctrl_output = Command::new("nvme")
            .arg("id-ctrl")
            .arg("-vb")
            .arg(nvme_device_path)
            .output()
            .await?;
        if !id_ctrl_output.status.success() {
            bail!("`nvme id-ctrl` failed with {}", id_ctrl_output.status)
        }
        debug!(id_ctrl_output = %String::from_utf8_lossy(&id_ctrl_output.stdout));

        // Amazon stores the block device name associated with nvme devices in the "vendor data"
        // field of the id-ctrl struct, which if you do the math is bytes 3072-3104 of the
        // nvme_id_ctrl struct in linux/nvme.h.
        let ebs_block_dev = PathBuf::from(
            str::from_utf8(&id_ctrl_output.stdout[3072..=3104])?
                .trim_matches(|c| [' ', '\0'].contains(&c)),
        );
        debug!(ebs_block_dev = %ebs_block_dev.display());
        if ebs_block_dev == device_name {
            info!("Found!");
            return Ok(Some(nvme_device_path.into()));
        }
    }

    Ok(None)
}

/// Annoyingly, AWS is extremely inconsistent about how block devices actually get mounted to the
/// system. If you have an EBS device with a mount point like `sdb`, its *actual* block device path
/// could either be:
///
/// * `/dev/sdb`
/// * `/dev/xvdb`
/// * some random nvme device, where the only way to figure out the EBS block device mapping is to
///   parse the binary output of `nvme-cli`
///
/// And the only way to figure out which has happened is to try each in order until we find a block
/// device that actually exists on the filesystem! This function implements that logic.
#[instrument]
async fn find_block_device(device_name: &Path) -> Result<Option<PathBuf>> {
    let with_sd = PathBuf::from("/dev").join(device_name);

    if exists(&with_sd).await? {
        info!("Found with sd prefix");
        return Ok(Some(with_sd));
    }
    info!("Not found with sd prefix");

    lazy_static! {
        static ref SD_RE: Regex = Regex::new("^sd").unwrap();
    }
    let name_with_xv = SD_RE.replace(device_name.as_os_str().to_str().unwrap(), "xvd");

    let path_with_xv = PathBuf::from("/dev").join(name_with_xv.as_ref());
    if exists(&path_with_xv).await? {
        info!("Found with xvd prefix");
        return Ok(Some(path_with_xv));
    }
    info!("Not found with xvd prefix");

    // can't use Option::or_else bc async (give me effect composition!!!)
    match find_nvme_device(device_name).await? {
        Some(res) => Ok(Some(res)),
        None => find_nvme_device(Path::new(name_with_xv.as_ref())).await,
    }
}

#[instrument]
async fn configure_volume_id(environment_file: &Path, volume_id: &str) -> Result<()> {
    info!("writing VOLUME_ID for readyset-server");
    let mut env_file = File::open(environment_file).await?;
    let mut env = Vec::new();
    env_file.read_to_end(&mut env).await?;

    let new_env = env
        .split(|p| *p == b'\n')
        .filter(|line| !line.starts_with(b"VOLUME_ID="))
        .chain(iter::once(format!("VOLUME_ID={}", volume_id).as_bytes()))
        .intersperse(b"\n")
        .flatten()
        .copied()
        .collect::<Vec<_>>();

    File::create(environment_file)
        .await?
        .write_all(&new_env)
        .await?;
    Ok(())
}

#[instrument]
async fn start_readyset_server() -> Result<()> {
    run(Command::new("systemctl").arg("reset-failed")).await?;
    run(Command::new("systemctl").args(["enable", "readyset-server"])).await?;
    run(Command::new("systemctl").args(["restart", "readyset-server"])).await?;
    Ok(())
}

impl Opts {
    fn volume_tag_filter(&self) -> Filter {
        filter(
            format!("tag:{}", self.volume_tag_key),
            &self.volume_tag_value,
        )
    }

    fn volume_tag(&self) -> Tag {
        tag(&self.volume_tag_key, &self.volume_tag_value)
    }

    #[instrument(skip(self, ec2))]
    async fn find_attached_volume_id(
        &self,
        ec2: &Client,
        device: &Path,
        instance_id: &str,
    ) -> Result<String> {
        info!("Finding volume ID of attached volume");
        let description = ec2
            .describe_volumes()
            .filters(self.volume_tag_filter())
            .filters(filter("attachment.instance-id", instance_id))
            .filters(filter("status", "in-use"))
            .send()
            .await?;
        description
            .volumes
            .into_iter()
            .flat_map(|volumes| volumes.into_iter())
            .flat_map(|volumes| volumes.volume_id)
            .next()
            .ok_or_else(|| {
                anyhow!("Volume mounted, but could not find attached EBS volume ID in API response")
            })
    }

    #[instrument(skip(self, ec2))]
    async fn find_existing_volume_id(&self, ec2: &Client, az: &str) -> Result<Option<String>> {
        let description = ec2
            .describe_volumes()
            .filters(self.volume_tag_filter())
            .filters(filter("availability-zone", az))
            .filters(filter("status", "available"))
            .send()
            .await?;
        Ok(description
            .volumes
            .into_iter()
            .flat_map(|volumes| volumes.into_iter().next())
            .flat_map(|volume| volume.volume_id)
            .next())
    }

    #[instrument(skip(ec2))]
    async fn create_volume_and_return_id(&self, ec2: &Client, az: &str) -> Result<String> {
        let tag_specification = TagSpecification::builder()
            .resource_type(ResourceType::Volume)
            .tags(self.volume_tag())
            .build();
        let result = ec2
            .create_volume()
            .availability_zone(az)
            .size(self.volume_size_gb)
            .tag_specifications(tag_specification)
            .send()
            .await?;
        result
            .volume_id
            .ok_or_else(|| anyhow!("No volume ID was returned by EC2"))
    }

    #[instrument(skip(self, ec2))]
    async fn ensure_volume_exists(
        &self,
        ec2: &Client,
        instance_id: &str,
        region: &str,
        az: &str,
    ) -> Result<String> {
        info!("Searching for volume...");
        let volume_id = match self.find_existing_volume_id(ec2, az).await? {
            Some(vid) => {
                info!(volume_id = vid.as_str(), "...found");
                vid
            }
            None => {
                info!("...not found.  Creating...");
                let vid = self.create_volume_and_return_id(ec2, az).await?;
                info!(volume_id = vid.as_str(), "...created");
                vid
            }
        };
        wait_for_volume_state(ec2, &volume_id, VolumeState::Available).await?;
        Ok(volume_id)
    }

    #[instrument(skip(self, ec2))]
    async fn attach_volume(
        &self,
        ec2: &Client,
        volume_id: &str,
        instance_id: &str,
    ) -> Result<AttachedVolume> {
        info!("Attaching volume...");
        ec2.attach_volume()
            .device(self.device.to_string_lossy())
            .instance_id(instance_id)
            .volume_id(volume_id)
            .send()
            .await?;

        wait_for_volume_state(ec2, volume_id, VolumeState::InUse).await?;

        loop {
            info!("Waiting for volume to attach successfully...");
            match find_block_device(&self.device).await? {
                Some(block_device_path) => {
                    return Ok(AttachedVolume {
                        ebs_volume_id: volume_id.to_owned(),
                        block_device_path,
                    })
                }
                None => {
                    time::sleep(WAIT_TIMER).await;
                }
            }
        }
    }

    #[instrument(skip(self))]
    async fn ensure_volume_attached(&self) -> Result<AttachedVolume> {
        let imds = aws_config::imds::client::Client::builder()
            .configure(&ProviderConfig::with_default_region().await)
            .build()
            .await?;

        let instance_id = imds.get("/latest/meta-data/instance-id").await?;
        let region = imds.get("/latest/meta-data/placement/region").await?;
        let az = imds
            .get("/latest/meta-data/placement/availability-zone")
            .await?;

        let ec2 = {
            let shared_config = aws_config::from_env()
                .region(Region::new(region.clone()))
                .load()
                .await;
            Client::new(&shared_config)
        };

        info!("Checking to see whether device exists...");
        if let Some(block_device_path) = find_block_device(&self.device).await? {
            info!(path = %block_device_path.display(), "It does");
            let ebs_volume_id = self
                .find_attached_volume_id(&ec2, &self.device, &instance_id)
                .await?;
            return Ok(AttachedVolume {
                ebs_volume_id,
                block_device_path,
            });
        } else {
            info!("It does not");
        }

        let volume_id = self
            .ensure_volume_exists(&ec2, &instance_id, &region, &az)
            .await?;
        self.attach_volume(&ec2, &volume_id, &instance_id).await
    }

    #[instrument(skip(self))]
    async fn ensure_disk_formatted(&self, block_device_path: &Path) -> Result<()> {
        info!("Checking to see whether disk already contains an ext4 filesystem...");
        let magic = Command::new("file")
            .arg("-s")
            .arg(block_device_path)
            .output()
            .await?;
        let magic = String::from_utf8(magic.stdout)?;
        if magic.contains("ext4 filesystem data") {
            info!("It does");
            return Ok(());
        }
        info!("It does not; formatting disk...");
        Command::new("mkfs.ext4")
            .arg("-F")
            .arg(block_device_path)
            .spawn()?
            .wait()
            .await?;
        info!("Done");
        Ok(())
    }

    #[instrument(skip(self))]
    async fn ensure_filesystem_mounted(&self, block_device_path: &Path) -> Result<()> {
        info!("Ensuring mountpoint exists...");
        create_dir_all(&self.mountpoint).await?;

        info!("Checking to see whether mountpoint is mounted...");
        let result = Command::new("mountpoint")
            .arg("-q")
            .arg(&self.mountpoint)
            .output()
            .await?;
        if result.status.code() == Some(0) {
            info!("It is");
            return Ok(());
        }
        info!("It is not; mounting...");
        let result = Command::new("mount")
            .arg(block_device_path)
            .arg(&self.mountpoint)
            .spawn()?
            .wait()
            .await?;
        if !result.success() {
            bail!("mount {} {} failed")
        }

        info!("Done");
        Ok(())
    }

    async fn run(&self) -> Result<()> {
        let attached_volume = self.ensure_volume_attached().await?;
        self.ensure_disk_formatted(&attached_volume.block_device_path)
            .await?;
        self.ensure_filesystem_mounted(&attached_volume.block_device_path)
            .await?;
        configure_volume_id(
            Path::new("/etc/default/readyset-server"),
            &attached_volume.ebs_volume_id,
        )
        .await?;
        info!("Starting readyset-server");
        start_readyset_server().await?;

        Ok(())
    }
}

#[tokio::main]
pub async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .compact()
        .with_env_filter(EnvFilter::new(
            env::var("LOG_LEVEL").unwrap_or_else(|_| "info".to_owned()),
        ))
        .init();

    let opts = Opts::parse();
    opts.run().await?;
    info!("Done");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;
    use tokio::fs::{File, OpenOptions};

    #[tokio::test]
    async fn configure_volume_id_existing_volume_id() {
        let file = NamedTempFile::new().unwrap();
        OpenOptions::new()
            .write(true)
            .open(file.path())
            .await
            .unwrap()
            .write_all(b"X=y\nVOLUME_ID=asdf")
            .await
            .unwrap();

        configure_volume_id(file.path(), "new-volume-id")
            .await
            .unwrap();

        let mut new_content = Vec::new();
        File::open(file.path())
            .await
            .unwrap()
            .read_to_end(&mut new_content)
            .await
            .unwrap();
        let new_content = String::from_utf8(new_content).unwrap();
        assert_eq!(new_content, "X=y\nVOLUME_ID=new-volume-id");
    }
}
