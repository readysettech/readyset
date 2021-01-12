use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};

use anyhow::anyhow;
use clap::Clap;

pub mod ast;
pub mod parser;

#[derive(Clap)]
struct Opts {
    #[clap(subcommand)]
    subcommand: Command,
}

#[derive(Clap)]
enum Command {
    #[clap(version = "0.1")]
    Parse(Parse),
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

fn main() -> anyhow::Result<()> {
    let opts = Opts::parse();
    match opts.subcommand {
        Command::Parse(parse) => {
            parse.run()?;
        }
    }
    Ok(())
}
