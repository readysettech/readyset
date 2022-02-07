use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, HashSet};

use anyhow::{anyhow, Result};
use aws_sdk_s3::{Client, Region};
use clap::Parser;
use tokio::process::Command;

#[derive(Parser, Debug)]
pub(crate) struct Opts {
    release_name: String,
}

const CUSTOMER_ARTIFACT_BUCKET_REGION: &str = "us-east-2";
const CUSTOMER_ARTIFACT_BUCKET: &str = "readysettech-customer-artifacts-us-east-2";
const DOCKER_RELEASE_ARTIFACTS: &[&str] = &[
    "readyset-server.tar.gz",
    "readyset-mysql.tar.gz",
    "readyset-psql.tar.gz",
];

pub(crate) fn run(opts: Opts) -> Result<()> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(run_inner(opts))
}

async fn run_inner(opts: Opts) -> Result<()> {
    let shared_config = aws_config::from_env()
        .region(Region::new(CUSTOMER_ARTIFACT_BUCKET_REGION))
        .load()
        .await;
    let client = Client::new(&shared_config);

    // Grab all objects associated with this release name
    let req = client
        .list_objects_v2()
        .bucket(CUSTOMER_ARTIFACT_BUCKET)
        .prefix(format!("docker-release-{}", opts.release_name));
    let resp = req.send().await?;
    let objects = resp
        .contents
        .ok_or_else(|| anyhow!("Could not find release matching name {}", opts.release_name))?;

    // Find latest release date by looking at all the object keys and verifying that we have a
    // complete collection.
    let mut date_to_images: BTreeMap<String, HashSet<String>> = BTreeMap::new();
    for object in objects {
        let key = object.key.as_deref().unwrap_or_default();
        let split: Vec<&str> = key.split('/').collect();
        assert!(split[0] == format!("docker-release-{}", opts.release_name));
        let date = split[1];
        let image = split[2];
        let entry = date_to_images.entry(date.to_owned());
        match entry {
            Entry::Occupied(mut occupied) => {
                occupied.get_mut().insert(image.to_owned());
            }
            Entry::Vacant(vacant) => {
                let image_set = vacant.insert(HashSet::new());
                image_set.insert(image.to_owned());
            }
        }
    }
    let complete_image_set = DOCKER_RELEASE_ARTIFACTS
        .iter()
        .map(|x| x.to_owned().to_owned())
        .collect();
    let mut complete_release_date = None;
    while let Some((release_date, images)) = date_to_images.iter().next_back() {
        if images.symmetric_difference(&complete_image_set).next() == None {
            complete_release_date = Some(release_date);
            break;
        }
    }
    let release_date = complete_release_date.ok_or_else(|| {
        anyhow!(
            "Could not find a complete release for {}",
            opts.release_name
        )
    })?;
    // TODO: Use STS to grab temporary credentials for this purpose. As of now, our AWS Access Key
    // IDs will be encoded into the shared URL.
    // TODO: Using the AWS command line for now because presigning is not implemented yet in the
    // AWS Rust SDK https://github.com/awslabs/aws-sdk-rust/issues/139
    for docker_release_artifact in DOCKER_RELEASE_ARTIFACTS {
        let s3_url = format!(
            "s3://{}/docker-release-{}/{}/{}",
            CUSTOMER_ARTIFACT_BUCKET, opts.release_name, release_date, docker_release_artifact
        );
        let output = Command::new("aws")
            .arg("--region")
            .arg(CUSTOMER_ARTIFACT_BUCKET_REGION)
            .arg("s3")
            .arg("presign")
            .arg(s3_url)
            .output()
            .await?;
        println!("{}", std::str::from_utf8(&output.stdout)?);
    }
    Ok(())
}
