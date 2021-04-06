#![warn(clippy::dbg_macro)]
use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context};
use clap::Clap;

pub mod ast;
pub mod parser;
pub mod runner;

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
}

impl Command {
    fn run(&self) -> anyhow::Result<()> {
        match self {
            Self::Parse(parse) => parse.run(),
            Self::Verify(verify) => verify.run(),
        }
    }
}

fn input_files(paths: &[PathBuf]) -> anyhow::Result<Vec<(PathBuf, Box<dyn io::Read>)>> {
    if paths == vec![Path::new("-")] {
        Ok(vec![("stdin".to_string().into(), Box::new(io::stdin()))])
    } else {
        Ok(paths
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
            .collect())
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
        for (filename, file) in input_files(&self.paths)? {
            let filename = filename.canonicalize()?;
            println!("Parsing records from {}", filename.to_string_lossy());
            match parser::read_records(file) {
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

    /// Enable logging in both noria and noria-mysql
    #[clap(long, short)]
    verbose: bool,
}

impl Verify {
    fn run(&self) -> anyhow::Result<()> {
        let mut failed = false;
        for (filename, file) in input_files(&self.paths)? {
            let script = TestScript::read(filename, file)?;
            let run_opts: RunOptions = self.into();
            if let Err(e) = script
                .run(run_opts)
                .with_context(|| format!("Running test script {}", script.name()))
            {
                failed = true;
                eprintln!("{:#}", e);
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
        opts
    }
}

fn main() -> anyhow::Result<()> {
    let opts = Opts::parse();
    opts.subcommand.run()
}
