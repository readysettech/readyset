#![warn(clippy::dbg_macro)]
use std::convert::TryFrom;
use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context};
use clap::Clap;

pub mod ast;
pub mod generate;
pub mod parser;
pub mod runner;

use crate::generate::Generate;
use crate::runner::{RunOptions, TestScript};

#[derive(Clap)]
struct Opts {
    #[clap(subcommand)]
    subcommand: Command,
}

#[derive(Clap)]
enum Command {
    Parse(Parse),
    Verify(Verify),
    Generate(Generate),
}

impl Command {
    async fn run(self) -> anyhow::Result<()> {
        match self {
            Self::Parse(parse) => parse.run(),
            Self::Verify(verify) => verify.run().await,
            Self::Generate(generate) => generate.run().await,
        }
    }
}

/// The set of input files we are going to run over
#[derive(Default)]
struct InputFiles {
    /// The files we expect to pass
    expected_passes: Vec<(PathBuf, Box<dyn io::Read>)>,

    /// The files we expect to fail
    expected_failures: Vec<(PathBuf, Box<dyn io::Read>)>,
}

impl TryFrom<&[PathBuf]> for InputFiles {
    type Error = anyhow::Error;

    fn try_from(paths: &[PathBuf]) -> Result<Self, Self::Error> {
        if paths == vec![Path::new("-")] {
            Ok(InputFiles {
                expected_passes: vec![("stdin".to_string().into(), Box::new(io::stdin()))],
                ..Default::default()
            })
        } else {
            let (expected_failures, expected_passes) = paths
                .iter()
                .map(
                    |path| -> anyhow::Result<Vec<(PathBuf, Box<dyn io::Read>)>> {
                        if path.is_file() {
                            Ok(vec![(path.to_path_buf(), Box::new(File::open(path)?))])
                        } else if path.is_dir() {
                            Ok(path
                                .read_dir()?
                                .filter_map(|entry| -> Option<(PathBuf, Box<dyn io::Read>)> {
                                    let path = entry.ok()?.path();
                                    if path.is_file() {
                                        Some((path.clone(), Box::new(File::open(&path).unwrap())))
                                    } else {
                                        None
                                    }
                                })
                                .collect())
                        } else {
                            Err(anyhow!(
                                "Invalid path {}, must be a filename, directory, or `-`",
                                path.to_str().unwrap()
                            ))
                        }
                    },
                )
                .collect::<anyhow::Result<Vec<_>>>()?
                .into_iter()
                .flatten()
                .partition(|(name, _)| name.to_string_lossy().as_ref().ends_with(".fail.test"));

            Ok(InputFiles {
                expected_passes,
                expected_failures,
            })
        }
    }
}

#[derive(PartialEq, Eq, Clone, Copy)]
enum ExpectedResult {
    Pass,
    Fail,
}

struct InputFile {
    name: PathBuf,
    data: Box<dyn io::Read>,
    expected_result: ExpectedResult,
}

impl IntoIterator for InputFiles {
    type Item = InputFile;

    type IntoIter = Box<dyn Iterator<Item = InputFile>>;

    fn into_iter(self) -> Self::IntoIter {
        Box::new(
            self.expected_passes
                .into_iter()
                .map(|(name, data)| InputFile {
                    name,
                    data,
                    expected_result: ExpectedResult::Pass,
                })
                .chain(
                    self.expected_failures
                        .into_iter()
                        .map(|(name, data)| InputFile {
                            name,
                            data,
                            expected_result: ExpectedResult::Fail,
                        }),
                ),
        )
    }
}

/// Test the parser on one or more sqllogictest files
#[derive(Clap)]
struct Parse {
    /// Files or directories to parse. If `-`, will read from standard input
    #[clap(parse(from_str))]
    paths: Vec<PathBuf>,

    /// Output the resulting parsed records after parsing
    #[clap(short, long)]
    output: bool,
}

impl Parse {
    pub fn run(&self) -> anyhow::Result<()> {
        for InputFile { name, data, .. } in InputFiles::try_from(self.paths.as_slice())? {
            let filename = name.canonicalize()?;
            println!("Parsing records from {}", filename.to_string_lossy());
            match parser::read_records(data) {
                Ok(records) => {
                    println!(
                        "Successfully parsed {} record{}",
                        records.len(),
                        if records.len() == 1 { "" } else { "s" }
                    );
                    if self.output {
                        println!("{:#?}", records);
                    }
                }
                Err(e) => eprintln!("Error parsing {}: {}", filename.to_string_lossy(), e),
            };
        }
        Ok(())
    }
}

/// Run a test script, or all test scripts in a directory, against either Noria or a reference MySQL
/// database
#[derive(Clap)]
struct Verify {
    /// Files or directories containing test scripts to run. If `-`, will read from standard input
    ///
    /// Any files whose name ends in `.fail.test` will be run, but will be expected to *fail* for
    /// some reason - if any of them pass, the overall run will fail (and noria-logictest will exit
    /// with status code 0)
    #[clap(parse(from_str))]
    paths: Vec<PathBuf>,

    /// Zookeeper host to connect to
    #[clap(long, default_value = "127.0.0.1")]
    zookeeper_host: String,

    /// Zookeeper port to connect to
    #[clap(long, default_value = "2181")]
    zookeeper_port: u16,

    /// Connect to and run verification against a MySQL server rather than using noria
    #[clap(long)]
    mysql: bool,

    /// MySQL host to connect to. Ignored if `mysql` is not set
    #[clap(long, default_value = "localhost")]
    mysql_host: String,

    /// MySQL port to connect to. Ignored if `mysql` is not set
    #[clap(long, default_value = "3306")]
    mysql_port: u16,

    /// MySQL database to connect to. Ignored if `mysql` is not set
    #[clap(long, default_value = "sqllogictest")]
    mysql_db: String,

    /// Disable query graph reuse
    #[clap(long)]
    no_reuse: bool,

    /// Enable logging in both noria and noria-mysql
    #[clap(long, short)]
    verbose: bool,

    /// Enable a MySQL backend for the client, with binlog replication to Noria.
    /// All writes will pass through to MySQL and be replicated to Noria using binlog.
    /// The parameter to this argument is a MySQL URL with no database specified.
    #[clap(long)]
    binlog_mysql: Option<String>,
}

impl Verify {
    async fn run(&self) -> anyhow::Result<()> {
        let mut failed = false;

        for InputFile {
            name,
            data,
            expected_result,
        } in InputFiles::try_from(self.paths.as_slice())?
        {
            let script = TestScript::read(name, data)?;
            let run_opts: RunOptions = self.into();

            let result = script
                .run(run_opts)
                .await
                .with_context(|| format!("Running test script {}", script.name()));

            match result {
                Ok(_) if expected_result == ExpectedResult::Fail => {
                    failed = true;
                    eprintln!(
                        "Script {} didn't fail, but was expected to (maybe rename it to {}?)",
                        script.name(),
                        script.name().replace(".fail.test", ".test")
                    )
                }
                Err(e) if expected_result == ExpectedResult::Pass => {
                    failed = true;
                    eprintln!("{:#}", e);
                }
                _ => {}
            }
        }

        if failed {
            Err(anyhow!("One or more test scripts failed"))
        } else {
            Ok(())
        }
    }
}

impl Into<RunOptions> for &Verify {
    #[allow(clippy::field_reassign_with_default)]
    fn into(self) -> RunOptions {
        let mut opts = RunOptions::default();
        opts.zookeeper_host = self.zookeeper_host.clone();
        opts.zookeeper_port = self.zookeeper_port;
        opts.use_mysql = self.mysql;
        opts.mysql_host = self.mysql_host.clone();
        opts.mysql_port = self.mysql_port;
        opts.mysql_db = self.mysql_db.clone();
        opts.verbose = self.verbose;
        opts.disable_reuse = self.no_reuse;
        opts.binlog_url = self.binlog_mysql.clone();
        opts
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opts = Opts::parse();
    opts.subcommand.run().await
}
