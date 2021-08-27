#![warn(clippy::dbg_macro)]

use std::convert::{TryFrom, TryInto};
use std::fmt::{self, Display};
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::{env, io, process};

use anyhow::{anyhow, bail, Context};
use clap::Clap;
use colored::Colorize;
use futures::stream::futures_unordered::FuturesUnordered;
use futures::StreamExt;
use lazy_static::lazy_static;
use proptest::arbitrary::any;
use proptest::strategy::Strategy;
use proptest::test_runner::{self, TestCaseError, TestError, TestRng, TestRunner};
use query_generator::QuerySeed;
use tempfile::NamedTempFile;
use tokio::sync::Mutex;
use walkdir::WalkDir;

pub mod ast;
pub mod from_query_log;
pub mod generate;
pub mod parser;
pub mod runner;
pub mod upstream;

use crate::from_query_log::FromQueryLog;
use crate::generate::Generate;
use crate::runner::{RunOptions, TestScript};
use crate::upstream::{DatabaseType, DatabaseURL};

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
    FromQueryLog(FromQueryLog),
    Fuzz(Fuzz),
}

impl Command {
    fn run(self) -> anyhow::Result<()> {
        match self {
            Self::Parse(parse) => parse.run(),
            Self::Verify(verify) => verify.run(),
            Self::Generate(generate) => generate.run(),
            Self::FromQueryLog(convert) => convert.run(),
            Self::Fuzz(fuzz) => {
                // This will live as long as the program anyway, and we need to be able to reference
                // it from multiple different async tasks, so we can just leak a reference, which is
                // cheaper than putting it in an Arc or something
                let fuzz: &'static mut _ = Box::leak(Box::new(fuzz));
                fuzz.run()
            }
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

    /// If passed, connect to and run verification against the database with the given URL, which
    /// should start with either postgresql:// or mysql://, rather than using noria.
    #[clap(long)]
    database_url: Option<DatabaseURL>,

    /// Shorthand for `--database-url mysql://root:noria@localhost:3306/sqllogictest`
    #[clap(long, conflicts_with = "database-url")]
    mysql: bool,

    /// Shorthand for `--database-url postgresql://postgres:noria@localhost:5432/sqllogictest`
    #[clap(long, conflicts_with = "database-url")]
    postgresql: bool,

    /// Enable an upstream database backend for the client, with replication to Noria.  All writes
    /// will pass through to the given database and be replicated to Noria.
    ///
    /// The value should be a database URL starting with either postgresql:// or mysql://
    #[clap(long)]
    replication_url: Option<String>,

    /// Type of database to use for the adapter.
    ///
    /// Ignored if --database-url is passed, must match the database type of --replication-url if
    /// both are passed
    #[clap(long, default_value="mysql", possible_values=&["mysql", "postgresql"])]
    database_type: DatabaseType,

    /// Enable query graph reuse
    #[clap(long)]
    enable_reuse: bool,

    /// Enable logging in both noria and noria-mysql
    #[clap(long, short)]
    verbose: bool,

    /// Number of parallel tasks to use to run tests. Ignored if --binlog-mysql is passed
    #[clap(long, short = 't', default_value = "32", env = "NORIA_LOGICTEST_TASKS")]
    tasks: usize,

    /// When tests are encountered that are expected to fail but do not, rename the test file from
    /// .fail.test to .test
    #[clap(long)]
    rename_passing: bool,
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
            writeln!(
                f,
                "TIP: To rectify this, copy and paste the following commands in the relevant directory:"
            )?;
            for script in &self.unexpected_passes {
                writeln!(
                    f,
                    "    mv {} {}",
                    script,
                    script.replace(".fail.test", ".test")
                )?;
            }
        }

        Ok(())
    }
}

lazy_static! {
    static ref DEFAULT_MYSQL_URL: DatabaseURL = "mysql://root:noria@localhost:3306/sqllogictest"
        .parse()
        .unwrap();
    static ref DEFAULT_POSTGRESQL_URL: DatabaseURL =
        "postgresql://postgres:noria@localhost:5432/sqllogictest"
            .parse()
            .unwrap();
}

impl Verify {
    fn database_url(&self) -> Option<&DatabaseURL> {
        if self.mysql {
            Some(&*DEFAULT_MYSQL_URL)
        } else if self.postgresql {
            Some(&*DEFAULT_POSTGRESQL_URL)
        } else {
            self.database_url.as_ref()
        }
    }

    #[tokio::main]
    async fn run(&self) -> anyhow::Result<()> {
        let result = Arc::new(Mutex::new(VerifyResult::default()));
        let mut tasks = FuturesUnordered::new();

        let max_tasks = if self.replication_url.is_some() {
            // Can not parallelize tests when binlog is enabled, because each test reuses the same db
            1
        } else {
            self.tasks
        };

        for InputFile {
            name,
            data,
            expected_result,
        } in InputFiles::try_from(&self.input_opts)?
        {
            let script = TestScript::read(name.clone(), data)
                .with_context(|| format!("Reading {}", name.to_string_lossy()))?;
            let run_opts: RunOptions = self.into();
            let result = Arc::clone(&result);
            let rename_passing = self.rename_passing;

            tasks.push(tokio::spawn(async move {
                let script_result = script
                    .run(run_opts)
                    .await
                    .with_context(|| format!("Running test script {}", script.name()));

                match script_result {
                    Ok(_) if expected_result == ExpectedResult::Fail => {
                        result
                            .lock()
                            .await
                            .unexpected_passes
                            .push(script.name().into_owned());

                        let failing_fname = script.path().to_str().unwrap();
                        let passing_fname = failing_fname.replace(".fail.test", ".test");
                        eprintln!(
                            "Script {} didn't fail, but was expected to (maybe rename it to {}?)",
                            failing_fname, passing_fname,
                        );
                        if rename_passing {
                            eprintln!("Renaming {} to {}", failing_fname, passing_fname);
                            fs::rename(Path::new(failing_fname), Path::new(&passing_fname))
                                .unwrap();
                        }
                    }
                    Err(e) if expected_result == ExpectedResult::Pass => {
                        result
                            .lock()
                            .await
                            .failures
                            .push(script.name().into_owned());
                        eprintln!("{:#}", e);
                    }
                    Err(e) => {
                        eprintln!(
                            "Test script {} failed as expected:\n\n{:#}",
                            script.name(),
                            e
                        );
                        result.lock().await.passes += 1;
                    }
                    _ => {
                        result.lock().await.passes += 1;
                    }
                }
            }));

            if tasks.len() >= max_tasks {
                // We want to limit the number of concurrent tests, so we wait for one of the current tasks to finish first
                tasks.select_next_some().await.unwrap();
            }
        }

        while !tasks.is_empty() {
            tasks.select_next_some().await.unwrap();
        }

        println!("{}", result.lock().await);

        if result.lock().await.is_success() {
            Ok(())
        } else {
            Err(anyhow!("Test run failed"))
        }
    }
}

impl From<&Verify> for RunOptions {
    fn from(verify: &Verify) -> Self {
        Self {
            database_type: verify.database_type,
            verbose: verify.verbose,
            enable_reuse: verify.enable_reuse,
            upstream_database_url: verify.database_url().cloned(),
            replication_url: verify.replication_url.clone(),
            ..Default::default()
        }
    }
}

/// Representation for a test seed to be passed to proptest
#[derive(Debug, Clone, Copy)]
struct Seed([u8; 32]);

impl Display for Seed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", hex::encode(self.0))
    }
}

impl FromStr for Seed {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(hex::decode(s)?.try_into().map_err(|_| {
            anyhow!("Wrong number of bytes for seed, expected 32")
        })?))
    }
}

/// Fuzz-test noria by randomly generating queries and seed data, and ensuring that both Noria and a
/// reference database return the same results
#[derive(Clap, Debug, Clone)]
pub struct Fuzz {
    /// Number of test cases to generate
    ///
    /// Each test case consists of a list of queries that will be run against both Noria and the
    /// reference database
    #[clap(long, short = 'n', default_value = "100")]
    num_tests: u32,

    /// Hex-encoded seed for the random generator to use when generating test cases. Defaults to a
    /// randomly generated seed.
    #[clap(long)]
    seed: Option<Seed>,

    /// URL of a reference database to compare to. Currently supports `mysql://` URLs, but may be
    /// expanded in the future
    #[clap(long, parse(try_from_str))]
    compare_to: DatabaseURL,

    /// Enable verbose log output
    #[clap(long, short = 'v')]
    verbose: bool,
}

impl Fuzz {
    fn run(&'static self) -> anyhow::Result<()> {
        let mut runner = if let Some(Seed(seed)) = self.seed {
            TestRunner::new_with_rng(self.into(), TestRng::from_seed(Default::default(), &seed))
        } else {
            TestRunner::new(self.into())
        };

        let result = runner.run(&self.test_script_strategy(), move |test_script| {
            let rt = tokio::runtime::Runtime::new().unwrap();
            let _guard = rt.enter();
            rt.block_on(test_script.run(RunOptions {
                verbose: self.verbose,
                ..Default::default()
            }))
            .map_err(|err| TestCaseError::fail(format!("{:#}", err)))
        });

        if let Err(TestError::Fail(reason, script)) = result {
            let mut file = NamedTempFile::new()?;
            eprintln!(
                "Writing failing test script to {}",
                file.path().to_string_lossy()
            );
            script.write_to(&mut file)?;
            if env::var("BUILDKITE").is_ok() {
                process::Command::new("buildkite-agent")
                    .args(&["artifact", "upload", file.path().to_str().unwrap()])
                    .spawn()
                    .context("Uploading test script to Buildkite")?;
            }

            bail!("Found failing set of queries: {}", reason);
        }

        println!("No bugs found!");

        Ok(())
    }

    fn test_script_strategy(&self) -> impl Strategy<Value = TestScript> + 'static {
        (any::<Vec<QuerySeed>>(), self.generate_opts()).prop_map(|(query_seeds, generate_opts)| {
            let rt = tokio::runtime::Runtime::new().unwrap();
            let _guard = rt.enter();
            let mut seed = generate::Seed::try_from(query_seeds).unwrap();
            let script = rt.block_on(seed.run(generate_opts)).unwrap();
            script.clone()
        })
    }

    fn generate_opts(&self) -> impl Strategy<Value = generate::GenerateOpts> + 'static {
        let compare_to = self.compare_to.clone();
        let verbose = self.verbose;
        (0..100usize).prop_flat_map(move |rows_per_table| {
            let compare_to = compare_to.clone();
            (0..=rows_per_table).prop_map(move |rows_to_delete| generate::GenerateOpts {
                compare_to: compare_to.clone(),
                rows_per_table,
                verbose,
                random: true,
                include_deletes: true,
                rows_to_delete: Some(rows_to_delete),
            })
        })
    }
}

impl<'a> From<&'a Fuzz> for test_runner::Config {
    fn from(fuzz: &'a Fuzz) -> Self {
        Self {
            cases: fuzz.num_tests,
            verbose: if fuzz.verbose { 1 } else { 0 },
            ..Default::default()
        }
    }
}

fn main() -> anyhow::Result<()> {
    let opts = Opts::parse();
    opts.subcommand.run()
}
