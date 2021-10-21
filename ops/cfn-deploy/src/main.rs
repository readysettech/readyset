#![warn(clippy::dbg_macro)]
use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;

use anyhow::{bail, Result};
use clap::{AppSettings, Parser};
use serde::Deserialize;
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
enum Subcommand {
    BuildCfnTemplate,
}

#[derive(Parser, Debug)]
#[clap(setting=AppSettings::SubcommandRequired)]
struct Opts {
    #[clap(subcommand)]
    subcommand: Subcommand,
}

#[derive(Deserialize, Debug)]
#[allow(dead_code)] // TODO: remove when all fields are used
struct PackerBuild {
    name: String,
    builder_type: String,
    build_time: u64,
    files: Option<String>,
    artifact_id: String,
    packer_run_uuid: String,
    custom_data: Option<String>,
}

#[derive(Deserialize, Debug)]
struct PackerManifest {
    builds: Vec<PackerBuild>,
    last_run_uuid: String,
}

fn build_cfn_template() -> Result<()> {
    let packer_manifest_path = Path::new("ops/image-deploy/packer-manifest.json");
    let packer_manifest: PackerManifest =
        serde_json::from_reader(BufReader::new(File::open(&packer_manifest_path)?))?;
    let last_run_builds: Vec<&PackerBuild> = packer_manifest
        .builds
        .iter()
        .filter(|build| build.packer_run_uuid == packer_manifest.last_run_uuid)
        .collect();
    let mut aws_ami_region_maps_for_template: HashMap<&str, HashMap<&str, HashMap<&str, &str>>> =
        HashMap::new();
    for build in last_run_builds {
        let (mapping_name, templates) = match build.name.as_str() {
            "readyset-mysql-adapter" => {
                ("READYSETMYSQLADAPTER", vec!["readyset-mysql-template.yaml"])
            }
            "readyset-server" => (
                "READYSETSERVER",
                vec![
                    "readyset-mysql-template.yaml",
                    "readyset-postgresql-template.yaml",
                ],
            ),
            "readyset-monitor" => (
                "READYSETMONITOR",
                vec![
                    "readyset-mysql-template.yaml",
                    "readyset-postgresql-template.yaml",
                ],
            ),
            "readyset-authority-consul" => (
                "READYSETAUTHORITYCONSUL",
                vec!["readyset-authority-consul-template.yaml"],
            ),
            unknown_image_name => bail!(
                "Cannot map image named {} for CloudFormation",
                unknown_image_name
            ),
        };

        for artifact in build.artifact_id.split(',') {
            if let Some((region, ami_id)) = artifact.split_once(':') {
                for template in &templates {
                    let template_map = aws_ami_region_maps_for_template
                        .entry(template)
                        .or_default();
                    let region_map = template_map.entry(region).or_default();
                    region_map.insert(mapping_name, ami_id);
                }
            } else {
                bail!("Invalid packer manifest, artifact id not correct format")
            }
        }
    }

    for (template_name, template_ami_region_map) in aws_ami_region_maps_for_template.iter() {
        println!(
            "For {}:\n {}",
            template_name,
            serde_yaml::to_string(&template_ami_region_map)?
        );
    }

    Ok(())
}
fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .pretty()
        .without_time()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let opts = Opts::parse();

    match opts.subcommand {
        Subcommand::BuildCfnTemplate => build_cfn_template(),
    }
}
