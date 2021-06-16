#![warn(clippy::dbg_macro)]
use std::convert::TryFrom;
use std::fmt::{self, Display};
use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context};
use clap::Clap;
use colored::Colorize;
use walkdir::WalkDir;

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

#[derive(Clap)]
struct InputFileOptions {
    /// Files or directories containing test scripts to run. If `-`, will read from standard input
    ///
    /// Any files whose name ends in `.fail.test` will be run, but will be expected to *fail* for
    /// some reason - if any of them pass, the overall run will fail (and noria-logictest will exit
    /// with a non-zero status code)
    #[clap(parse(from_str))]
    paths: Vec<PathBuf>,

    /// Load input files from subdirectories of the given paths recursively
    #[clap(long, short = 'r')]
    recursive: bool,
}

/// The set of input files we are going to run over
#[derive(Default)]
struct InputFiles {
    /// The files we expect to pass
    expected_passes: Vec<(PathBuf, Box<dyn io::Read>)>,

    /// The files we expect to fail
    expected_failures: Vec<(PathBuf, Box<dyn io::Read>)>,
}

impl<'a> TryFrom<&'a InputFileOptions> for InputFiles {
    type Error = anyhow::Error;

    fn try_from(opts: &InputFileOptions) -> Result<Self, Self::Error> {
        if opts.paths == vec![Path::new("-")] {
            Ok(InputFiles {
                expected_passes: vec![("stdin".to_string().into(), Box::new(io::stdin()))],
                ..Default::default()
            })
        } else {
            let (expected_failures, expected_passes) = opts
                .paths
                .iter()
                .map(
                    |path| -> anyhow::Result<Vec<(PathBuf, Box<dyn io::Read>)>> {
                        if path.is_file() {
                            Ok(vec![(path.to_path_buf(), Box::new(File::open(path)?))])
                        } else if path.is_dir() {
                            let mut walker = WalkDir::new(path);
                            if !opts.recursive {
                                walker = walker.max_depth(1);
                            }

                            walker
                                .into_iter()
                                .filter(|e| e.as_ref().map_or(true, |e| e.file_type().is_file()))
                                .map(|entry| -> anyhow::Result<(PathBuf, Box<dyn io::Read>)> {
                                    let entry = entry?;
                                    let path = entry.path();
                                    Ok((path.to_owned(), Box::new(File::open(path)?)))
                                })
                                .collect()
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
    #[clap(flatten)]
    input_opts: InputFileOptions,

    /// Output the resulting parsed records after parsing
    #[clap(short, long)]
    output: bool,
}

impl Parse {
    pub fn run(&self) -> anyhow::Result<()> {
        for InputFile { name, data, .. } in InputFiles::try_from(&self.input_opts)? {
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
    #[clap(flatten)]
    input_opts: InputFileOptions,

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

#[derive(Default)]
struct VerifyResult {
    pub failures: Vec<String>,
    pub unexpected_passes: Vec<String>,
    pub passes: usize,
}

impl VerifyResult {
    pub fn is_success(&self) -> bool {
        self.failures.is_empty() && self.unexpected_passes.is_empty()
    }
}

impl Display for VerifyResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let n_scripts = |n| format!("{} test script{}", n, if n == 1 { "" } else { "s" });
        if self.passes > 0 {
            writeln!(
                f,
                "{}",
                format!("Successfully ran {}\n", n_scripts(self.passes)).green()
            )?;
        }

        if !self.failures.is_empty() {
            writeln!(f, "{} failed:\n", n_scripts(self.failures.len()))?;
            for script in &self.failures {
                writeln!(f, "    {}", script)?;
            }
        }

        if !self.unexpected_passes.is_empty() {
            writeln!(
                f,
                "{} {} expected to fail, but did not:\n",
                n_scripts(self.unexpected_passes.len()),
                if self.unexpected_passes.len() == 1 {
                    "was"
                } else {
                    "were"
                }
            )?;
            for script in &self.unexpected_passes {
                writeln!(f, "    {}", script)?;
            }
        }

        Ok(())
    }
}

impl Verify {
    async fn run(&self) -> anyhow::Result<()> {
        let mut result = VerifyResult::default();
        for InputFile {
            name,
            data,
            expected_result,
        } in InputFiles::try_from(&self.input_opts)?
        {
            let script = TestScript::read(name.clone(), data)
                .with_context(|| format!("Reading {}", name.to_string_lossy()))?;
            let run_opts: RunOptions = self.into();

            let script_result = script
                .run(run_opts)
                .await
                .with_context(|| format!("Running test script {}", script.name()));

            match script_result {
                Ok(_) if expected_result == ExpectedResult::Fail => {
                    result.unexpected_passes.push(script.name().into_owned());
                    eprintln!(
                        "Script {} didn't fail, but was expected to (maybe rename it to {}?)",
                        script.name(),
                        script.name().replace(".fail.test", ".test")
                    )
                }
                Err(e) if expected_result == ExpectedResult::Pass => {
                    result.failures.push(script.name().into_owned());
                    eprintln!("{:#}", e);
                }
                _ => {
                    result.passes += 1;
                }
            }
        }

        println!("{}", result);

        if result.is_success() {
            Ok(())
        } else {
            Err(anyhow!("Test run failed"))
        }
    }
}

impl From<&Verify> for RunOptions {
    fn from(verify: &Verify) -> Self {
        Self {
            use_mysql: verify.mysql,
            mysql_host: verify.mysql_host.clone(),
            mysql_port: verify.mysql_port,
            mysql_db: verify.mysql_db.clone(),
            verbose: verify.verbose,
            disable_reuse: verify.no_reuse,
            binlog_url: verify.binlog_mysql.clone(),
            ..Self::default()
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opts = Opts::parse();
    opts.subcommand.run().await
}
