use std::{
    io::{ErrorKind, Write},
    process::{Command, Stdio},
};

use anyhow::{anyhow, bail, Result};
use clap::{AppSettings, Clap};
use handlebars::Handlebars;
use serde::Serialize;

// Some draft commands have been added to this struct to give some guidance on what needs to be implemented.
// We should have commands that run outside Buildkite just in case Buildkite falls over and we need to run
// without Buildkite in case of emergency.
#[derive(Clap, Debug)]
enum Subcommand {
    // TerraformValidate
    // TerraformPlan
    // TerraformApply
    BuildkiteUploadPipeline,
    // BuildkiteTerraformValidate,
    // BuildkiteTerraformPlan,
    // BuildkiteTerraformApply,
    Example,
}

#[derive(Clap, Debug)]
#[clap(setting=AppSettings::SubcommandRequired)]
struct Opts {
    #[clap(subcommand)]
    subcommand: Subcommand,
}

#[derive(Serialize)]
struct Pipeline {}

static PIPELINE_TEMPLATE: &str = include_str!("templates/terraform_pipeline.yml.hbs");

fn buildkite_upload_pipeline(_opts: Opts) -> Result<()> {
    let mut handlebars = Handlebars::new();
    handlebars.register_escape_fn(handlebars::no_escape);
    let pipeline_definition = handlebars.render_template(PIPELINE_TEMPLATE, &Pipeline {})?;
    let mut upload_command_child = match Command::new("buildkite-agent")
        .arg("pipeline")
        .arg("upload")
        .stdin(Stdio::piped())
        .spawn()
    {
        Ok(child) => child,
        Err(err) => {
            if err.kind() == ErrorKind::NotFound {
                println!("{}", pipeline_definition);
                bail!("Could not find buildkite-agent so output pipeline above")
            } else {
                bail!("Could not spawn child: {}", err)
            }
        }
    };

    let mut stdin = upload_command_child
        .stdin
        .take()
        .ok_or_else(|| anyhow!("Could not get child stdin handle"))?;

    std::thread::spawn(move || stdin.write_all(pipeline_definition.as_bytes()));

    let exit_status = upload_command_child.wait()?;
    if exit_status.success() {
        Ok(())
    } else {
        bail!(
            "buildkite-agent pipeline upload failed with exit status {:?}",
            exit_status
        )
    }
}

fn example(_opts: Opts) -> Result<()> {
    println!("Example ran!");
    Ok(())
}

fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let opts = Opts::parse();
    match opts.subcommand {
        Subcommand::BuildkiteUploadPipeline => buildkite_upload_pipeline(opts),
        Subcommand::Example => example(opts),
    }
}
