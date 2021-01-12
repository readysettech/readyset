use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};

use anyhow::anyhow;
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

fn input_files(path: &Path) -> anyhow::Result<Vec<(PathBuf, Box<dyn io::Read>)>> {
    if path == Path::new("-") {
        Ok(vec![("stdin".to_string().into(), Box::new(io::stdin()))])
    } else if path.is_file() {
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
}

/// Test the parser on one or more sqllogictest files
#[derive(Clap)]
struct Parse {
    /// File or directory to parse. If `-`, will read from standard input
    #[clap(parse(from_str))]
    path: PathBuf,

    /// Output the resulting parsed records after parsing
    #[clap(short, long)]
    output: bool,
}

impl Parse {
    pub fn run(&self) -> anyhow::Result<()> {
        for (filename, file) in input_files(&self.path)? {
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

/// Run a test script, or all test scripts in a directory, against Noria
#[derive(Clap)]
struct Verify {
    /// File or directory containing test scripts to run. If `-`, will read from standard input
    #[clap(parse(from_str))]
    path: PathBuf,

    /// Zookeeper host to connect to
    #[clap(long, default_value = "127.0.0.1")]
    zookeeper_host: String,

    /// Zookeeper port to connect to
    #[clap(long, default_value = "2181")]
    zookeeper_port: u16,
}

impl Verify {
    fn run(&self) -> anyhow::Result<()> {
        for (filename, file) in input_files(&self.path)? {
            let script = TestScript::read(filename, file)?;
            script.run(self.run_options())?;
        }
        Ok(())
    }

    fn run_options(&self) -> RunOptions {
        let mut opts = RunOptions::default();
        opts.zookeeper_host = self.zookeeper_host.clone();
        opts.zookeeper_port = self.zookeeper_port;
        opts
    }
}

fn main() -> anyhow::Result<()> {
    let opts = Opts::parse();
    opts.subcommand.run()
}
