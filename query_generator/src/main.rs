//! A command-line interface to query_generator

#![feature(duration_zero)]

use anyhow::bail;
use clap::Clap;
use itertools::Either;
use query_generator::Operations;

mod benchmark;

#[derive(Clap)]
struct Opts {
    #[clap(subcommand)]
    subcommand: Command,
}

#[derive(Clap)]
enum Command {
    Generate(Generate),
    Benchmark(benchmark::Benchmark),
}

impl Command {
    pub fn run(self) -> anyhow::Result<()> {
        match self {
            Command::Generate(generate) => generate.run(),
            Command::Benchmark(benchmark) => benchmark.run(),
        }
    }
}

#[derive(Clap)]
struct Generate {
    /// Comma-separated list of query operations to generate queries with
    operations: Option<Operations>,

    #[clap(long, default_value = "3")]
    max_depth: usize,

    #[clap(long)]
    ddl_only: bool,

    #[clap(long)]
    queries_only: bool,
}

impl Generate {
    pub fn run(mut self) -> anyhow::Result<()> {
        if self.ddl_only && self.queries_only {
            bail!("Cannot specify both --ddl-only and --queries-only")
        }

        let mut gen = query_generator::GeneratorState::default();
        let queries = if let Some(Operations(operations)) = self.operations.take() {
            Either::Left(
                operations
                    .into_iter()
                    .map(|ops| gen.generate_query(&ops).statement),
            )
        } else {
            Either::Right(gen.generate_queries(self.max_depth))
        };

        if self.queries_only {
            for query in queries {
                println!("{}", query);
            }
        } else {
            let queries = queries.collect::<Vec<_>>();
            for create_table_statement in gen.ddl() {
                println!("{}", create_table_statement)
            }
            if !self.ddl_only {
                for query in queries {
                    println!("{}", query);
                }
            }
        }

        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    let opts = Opts::parse();
    opts.subcommand.run()
}
