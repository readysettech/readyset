use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{anyhow, bail, Result};
use aws_config::provider_config::ProviderConfig;
use aws_sdk_ec2::model::{Filter, ResourceType, Tag, TagSpecification, VolumeState};
use aws_sdk_ec2::Client;
use aws_types::region::Region;
use clap::Clap;
use tokio::fs::create_dir_all;
use tokio::process::Command;
use tokio::task;
use tokio::time;
use tracing::{info, instrument};
use tracing_subscriber::EnvFilter;

const WAIT_TIMER: Duration = Duration::from_secs(1);

#[derive(Clap, Debug)]
/// This utility will ensure that a persistent EBS volume is attached to the instance on which it
/// is running.  This includes creating a volume if one does not exist, attaching it to the
/// instance, formatting it, and mounting it.
struct Opts {
    #[clap(default_value = "/dev/xvdb")]
    device: PathBuf,

    #[clap(default_value = "/data")]
    mountpoint: PathBuf,

    /// Size of the volume to create in gigabytes
    #[clap(long, default_value = "32")]
    volume_size_gb: i32,

    /// Tag key to use for volumes
    #[clap(long, default_value = "ReadySet:ServerVolume")]
    volume_tag_key: String,

    /// Tag value to use for volumes
    #[clap(long, default_value = "true")]
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

#[instrument(skip(ec2, device))]
async fn attach_volume(
    ec2: &Client,
    volume_id: &str,
    instance_id: &str,
    device: &Path,
) -> Result<()> {
    info!("Attaching volume...");
    ec2.attach_volume()
        .device(device.to_string_lossy())
        .instance_id(instance_id)
        .volume_id(volume_id)
        .send()
        .await?;

    wait_for_volume_state(ec2, volume_id, VolumeState::InUse).await?;

    loop {
        info!("Waiting for volume to attach successfully...");
        let result = Command::new("sgdisk")
            .arg("-p")
            .arg(device)
            .output()
            .await?;
        if result.status.code() == Some(0) {
            break;
        }
        time::sleep(WAIT_TIMER).await;
    }
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

    #[instrument(skip(self))]
    async fn ensure_volume_attached(&self) -> Result<()> {
        info!("Checking to see whether device exists...");
        if exists(&self.device).await? {
            info!("It does");
            return Ok(());
        } else {
            info!("It does not");
        }

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

        let volume_id = self
            .ensure_volume_exists(&ec2, &instance_id, &region, &az)
            .await?;
        attach_volume(&ec2, &volume_id, &instance_id, &self.device).await?;

        Ok(())
    }

    #[instrument(skip(self))]
    async fn ensure_disk_formatted(&self) -> Result<()> {
        info!("Checking to see whether disk already contains an ext4 filesystem...");
        let magic = Command::new("file")
            .arg("-s")
            .arg(&self.device)
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
            .arg(&self.device)
            .spawn()?
            .wait()
            .await?;
        info!("Done");
        Ok(())
    }

    #[instrument(skip(self))]
    async fn ensure_filesystem_mounted(&self) -> Result<()> {
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
        Command::new("mount")
            .arg(&self.device)
            .arg(&self.mountpoint)
            .spawn()?
            .wait()
            .await?;
        info!("Done");
        Ok(())
    }

    async fn run(&self) -> Result<()> {
        self.ensure_volume_attached().await?;
        self.ensure_disk_formatted().await?;
        self.ensure_filesystem_mounted().await?;
        Ok(())
    }
}

#[tokio::main]
pub async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .compact()
        .with_env_filter(
            EnvFilter::from_default_env().add_directive("ensure-ebs-volume=info".parse()?),
        )
        .init();

    let opts = Opts::parse();
    opts.run().await?;
    info!("Done");

    Ok(())
}
