use std::borrow::Cow;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::env::set_current_dir;
use std::fmt::Display;
use std::io::Write;
use std::path::PathBuf;
use std::process::Command;
use std::str::FromStr;

use anyhow::{anyhow, bail, Result};
use buildkite_pipeline::{BuildAttributes, CommandStep, Pipeline, Plugin, Step, TriggerStep};
use clap::Parser;
use maplit::hashmap;
use reqwest::blocking as http;
use reqwest::header::{HeaderMap, HeaderValue};
use serde_json::{json, Value};
use tempfile::NamedTempFile;
use tracing::{debug, info};

const BUILDKITE_API_URL: &str = "https://api.buildkite.com/v2";

const DOCKER_PLUGIN_VERSION: &str = "3.8.0";
const ECR_PLUGIN_VERSION: &str = "2.5.0";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BuildkiteJobState {
    Passed,
    Failed,
    Blocked,
    Canceled,
}

impl FromStr for BuildkiteJobState {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "passed" => Ok(Self::Passed),
            "failed" => Ok(Self::Failed),
            "blocked" => Ok(Self::Blocked),
            "canceled" => Ok(Self::Canceled),
            _ => bail!("Unknown job state {}", s),
        }
    }
}

type Commit = String;

#[derive(Parser, Debug)]
pub(crate) struct BisectCommand {
    #[clap(subcommand)]
    subcommand: Subcommand,
}

#[derive(Parser, Debug)]
enum Subcommand {
    Start(Opts),
    Step(Opts),
}

#[derive(Parser, Debug)]
pub(crate) struct Opts {
    #[clap(long, env = "BUILDKITE_ORGANIZATION_SLUG")]
    organization_slug: String,

    #[clap(long, env = "BUILDKITE_BUILD_NUMBER")]
    build_number: u64,

    #[clap(long, env = "BUILDKITE_API_TOKEN")]
    buildkite_api_token: String,

    #[clap(long, env = "BISECT_PIPELINE")]
    pipeline: String,

    #[clap(long, env = "FAILING_COMMIT")]
    failing_commit: Commit,

    #[clap(long, env = "GOOD_COMMIT")]
    good_commit: Option<Commit>,

    #[clap(long, env = "BISECT_LOG")]
    bisect_log: Option<String>,

    #[clap(long, env = "LAST_JOB_KEY")]
    last_job_key: Option<String>,

    #[clap(long, env = "LAST_JOB_COMMIT")]
    last_job_commit: Option<Commit>,

    /// Optionally change to this directory before running any git commands. Useful for local
    /// testing in combination with `git worktree`
    #[clap(short = 'C')]
    chdir: Option<PathBuf>,

    /// Skip running `buildkite-agent pipeline upload`, instead outputting the pipelines to upload
    /// to stdout. Useful for local testing.
    #[clap(long, short = 'n')]
    dry_run: bool,
}

fn api_url<P>(path: P) -> String
where
    P: Display,
{
    format!("{}/{}", BUILDKITE_API_URL, path)
}

macro_rules! api_url {
    ($($format_args: tt)*) => {
        api_url(format!($($format_args)*))
    }
}

macro_rules! command {
    ($command: expr $(,$arg:expr)* $(,)?) => {{
        debug!(command=%$command, args=?[$(AsRef::<::std::ffi::OsStr>::as_ref($arg),)*], "running command");
        Command::new($command)$(.arg($arg))*
    }}
}

macro_rules! exec_output {
    ($command: expr $(,$arg:expr)* $(,)?) => {{
        let output = command!($command $(, $arg)*).output()?;
        if !output.status.success() {
            bail!("`{}` command failed with {}", $command, output.status);
        }
        String::from_utf8(output.stdout)?
    }}
}

macro_rules! exec {
    ($command: expr $(, $arg:expr)* $(,)?) => {
        let status = command!($command $(, $arg)*).status()?;
        if !status.success() {
            bail!("`{}` command failed with {}", $command, status);
        }
    }
}

fn bisect_head() -> Result<Commit> {
    Ok(exec_output!("git", "rev-parse", "BISECT_HEAD")
        .trim()
        .to_owned())
}

fn bisect_log() -> Result<String> {
    Ok(exec_output!("git", "bisect", "log"))
}

fn first_bad_commit() -> Result<Commit> {
    Ok(exec_output!("git", "rev-parse", "refs/bisect/bad")
        .trim()
        .to_owned())
}

fn git_show(commit: &str) -> Result<String> {
    Ok(exec_output!("git", "show", "-s", commit))
}

pub(crate) struct Bisect {
    opts: Opts,
    buildkite: http::Client,
    good_commit: Option<String>,
}

impl Bisect {
    pub(crate) fn new(opts: Opts) -> Result<Self> {
        let mut headers = HeaderMap::new();
        let mut token = HeaderValue::try_from(format!("Bearer {}", opts.buildkite_api_token))?;
        token.set_sensitive(true);
        headers.insert("Authorization", token);

        let buildkite = http::Client::builder()
            .https_only(true)
            .default_headers(headers)
            .build()?;

        Ok(Self {
            good_commit: opts.good_commit.clone(),
            opts,
            buildkite,
        })
    }

    pub(crate) fn start(mut self) -> Result<()> {
        self.init()?;

        exec!(
            "git",
            "bisect",
            "start",
            "--no-checkout",
            &self.opts.failing_commit,
            self.good_commit()
        );

        let next_commit = bisect_head()?;
        info!(%next_commit);

        self.upload_pipeline(&self.step_pipeline(next_commit)?)?;

        Ok(())
    }

    pub(crate) fn step(mut self) -> Result<()> {
        self.init()?;

        let job_state = self.job_state(
            self.opts
                .last_job_key
                .as_ref()
                .ok_or_else(|| anyhow!("Missing LAST_JOB_KEY environment variable"))?,
        )?;
        info!(?job_state);

        let bisect_log = self
            .opts
            .bisect_log
            .as_ref()
            .ok_or_else(|| anyhow!("Missing BISECT_LOG environment variable"))?;

        let mut bisect_log_file = NamedTempFile::new()?;
        write!(bisect_log_file, "{}", bisect_log)?;

        exec!("git", "bisect", "replay", bisect_log_file.path());

        let bisect_subcommand = match job_state {
            BuildkiteJobState::Passed => "good",
            BuildkiteJobState::Failed => "bad",
            BuildkiteJobState::Canceled => "skip",
            BuildkiteJobState::Blocked => bail!("Don't know how to handle blocked jobs"),
        };

        let commit = self
            .opts
            .last_job_commit
            .as_ref()
            .ok_or_else(|| anyhow!("Missing LAST_JOB_COMMIT environment variable"))?;

        let output = exec_output!("git", "bisect", bisect_subcommand, commit);

        if output.contains("is the first bad commit") {
            // Done!
            let bad_commit = first_bad_commit()?;
            self.upload_annotation(&bad_commit)?;
        } else {
            let next_commit = bisect_head()?;
            info!(%next_commit);
            self.upload_pipeline(&self.step_pipeline(next_commit)?)?;
        }

        Ok(())
    }

    fn init(&mut self) -> Result<()> {
        if let Some(chdir) = &self.opts.chdir {
            debug!(chdir = %chdir.display(), "setting current directory");
            set_current_dir(chdir)?;
        }

        if self.good_commit.is_none() {
            let last_passing_commit = self.last_passing_commit()?.ok_or_else(|| {
                anyhow!(
                    "Could not find a passing commit for pipeline {}",
                    self.opts.pipeline
                )
            })?;
            info!(%last_passing_commit);
            self.good_commit = Some(last_passing_commit);
        }

        Ok(())
    }

    fn good_commit(&self) -> &str {
        self.good_commit.as_ref().expect("good_commit not set")
    }

    /// Iterate through the pages of builds for our pipeline to find the commit for the last passing
    /// build *before* our failing commit.
    fn last_passing_commit(&self) -> Result<Option<Commit>> {
        let mut page = 1;
        let mut found_failing_commit = false;
        loop {
            debug!(%page, "loading commits");
            let resp = self
                .buildkite
                .get(api_url!(
                    "organizations/{}/pipelines/{}/builds",
                    self.opts.organization_slug,
                    self.opts.pipeline
                ))
                .query(&[("page", page.to_string())])
                .send()?;
            if !resp.status().is_success() {
                bail!("Request failed with status {}", resp.status());
            }

            let mut data = resp.json::<Vec<Value>>()?;
            if data.is_empty() {
                break Ok(None);
            }

            if !found_failing_commit {
                data = data
                    .into_iter()
                    .take_while(|build| {
                        let is_failing_commit = build["commit"] != self.opts.failing_commit;
                        if is_failing_commit {
                            found_failing_commit = true;
                        }
                        is_failing_commit
                    })
                    .collect()
            }

            if let Some(build) = data
                .iter()
                .find(|build| build.get("state") == Some(&Cow::Borrowed("passed").into()))
            {
                break Ok(Some(build["commit"].as_str().unwrap().to_owned()));
            }
            page += 1;
        }
    }

    fn job_state(&self, step_key: &str) -> Result<BuildkiteJobState> {
        let resp = self
            .buildkite
            .get(api_url!(
                "organizations/{}/pipelines/{}/builds/{}",
                self.opts.organization_slug,
                self.opts.pipeline,
                self.opts.build_number,
            ))
            .send()?;

        if !resp.status().is_success() {
            bail!("Request failed with status {}", resp.status());
        }

        let data = resp.json::<HashMap<String, Value>>()?;
        let jobs_v = data
            .get("jobs")
            .ok_or_else(|| anyhow!("Invalid JSON response; missing \"jobs\" key"))?;
        let jobs = jobs_v
            .as_array()
            .ok_or_else(|| anyhow!("Invalid JSON response; \"jobs\" key must be an array"))?;
        let job = jobs
            .iter()
            .find_map(|job| {
                job.as_object().filter(|job| {
                    job.get("step_key").map_or(false, |step_key| {
                        step_key.as_str().map_or(false, |sk| sk == step_key)
                    })
                })
            })
            .ok_or_else(|| anyhow!("Could not find job with key \"{}\"", step_key))?;
        let state_v = job.get("state").ok_or_else(|| {
            anyhow!("Invalid JSON response; all objects in \"jobs\" must have a \"state\" key")
        })?;
        state_v
            .as_str()
            .ok_or_else(|| anyhow!("Invalid JSON response; job state must be a string"))?
            .parse()
    }

    fn trigger_branch(&self) -> String {
        format!(
            "bisect-{}-{}",
            &self.good_commit()[..=8],
            &self.opts.failing_commit[..=8],
        )
    }

    fn step_pipeline(&self, commit: Commit) -> Result<Pipeline> {
        let trigger_step_key = format!("trigger-{}-{}", self.opts.pipeline, commit);
        let trigger_step = Step::Trigger(TriggerStep {
            pipeline: self.opts.pipeline.clone(),
            label: Some(format!(
                ":git: :rocket: Run {} for {}",
                self.opts.pipeline,
                &commit[..=8]
            )),
            key: Some(trigger_step_key.clone()),
            build: Some(BuildAttributes {
                branch: Some(self.trigger_branch()),
                commit: Some(commit.clone()),
                env: Some(hashmap! {
                    "BISECT_GOOD_COMMIT".to_owned() => self.good_commit().to_owned(),
                    "BISECT_BAD_COMMIT".to_owned() => self.opts.failing_commit.clone(),
                }),
                ..Default::default()
            }),
            async_build: false,
            branches: None,
            condition: None,
            depends_on: None,
            allow_dependency_failure: false,
            skip: Default::default(),
        });

        let bisect_step_step = Step::Command(CommandStep {
            label: Some(":git:".to_owned()),
            command: None,
            depends_on: Some(vec![trigger_step_key.clone()]),
            allow_dependency_failure: true,
            env: hashmap! {
                "BISECT_PIPELINE".to_owned() => self.opts.pipeline.clone().into(),
                "FAILING_COMMIT".to_owned() => self.opts.failing_commit.clone().into(),
                "GOOD_COMMIT".to_owned() => self.good_commit().to_owned().into(),
                "BISECT_LOG".to_owned() => bisect_log()?.into(),
                "LAST_JOB_KEY".to_owned() => trigger_step_key.into(),
                "LAST_JOB_COMMIT".to_owned() => commit.into()
            },
            plugins: vec![
                Plugin {
                    name: format!("docker#v{}", DOCKER_PLUGIN_VERSION),
                    params: serde_yaml::to_value(json!({
                        "command": ["bisect", "step"],
                        "image": "305232526136.dkr.ecr.us-east-2.amazonaws.com/xtask:${BUILDKITE_COMMIT}",
                        "shell": false,
                        "propagate_environment": true,
                        "environment": ["BUILDKITE_API_TOKEN"],
                    }))?,
                },
                Plugin {
                    name: format!("ecr#v{}", ECR_PLUGIN_VERSION),
                    params: serde_yaml::to_value(json!({
                        "login": true,
                        "retries": 3,
                    }))?,
                },
            ]
            .into(),
            key: None,
            agents: None,
            artifact_paths: None,
            branches: None,
            timeout_in_minutes: None,
        });

        Ok(vec![trigger_step, bisect_step_step].into())
    }

    fn upload_pipeline(&self, pipeline: &Pipeline) -> Result<()> {
        let yaml = serde_yaml::to_string(pipeline)?;
        if self.opts.dry_run {
            println!("{}", yaml);
        } else {
            let mut pipeline_file = NamedTempFile::new()?;
            write!(pipeline_file, "{}", yaml)?;
            exec!(
                "buildkite-agent",
                "pipeline",
                "upload",
                pipeline_file.path()
            );
        }

        Ok(())
    }

    fn upload_annotation(&self, first_bad_commit: &str) -> Result<()> {
        let body = format!(
            "**[{sha}][commit] is the first bad commit**\n\
             \n\
             ```\n\
             {message}\n\
             ```\n\
             [commit]: https://gerrit.readyset.name/q/commit:{sha}",
            sha = first_bad_commit,
            message = git_show(first_bad_commit)?,
        );
        exec!("buildkite-agent", "annotate", &body, "--style", "success");
        Ok(())
    }
}

pub(crate) fn run(opts: BisectCommand) -> Result<()> {
    match opts.subcommand {
        Subcommand::Start(opts) => Bisect::new(opts)?.start(),
        Subcommand::Step(opts) => Bisect::new(opts)?.step(),
    }
}
