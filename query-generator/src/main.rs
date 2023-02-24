//! A command-line interface to query_generator

use anyhow::bail;
use clap::Parser;
use query_generator::GenerateOpts;

#[derive(Parser)]
struct Opts {
    #[clap(flatten)]
    options: GenerateOpts,

    #[clap(long)]
    ddl_only: bool,

    #[clap(long)]
    queries_only: bool,
}

impl Opts {
    pub fn run(self) -> anyhow::Result<()> {
        if self.ddl_only && self.queries_only {
            bail!("Cannot specify both --ddl-only and --queries-only")
        }
        let mut gen = query_generator::GeneratorState::default();
        let queries = self
            .options
            .into_query_seeds()
            .map(|seed| gen.generate_query(seed).statement);

        if self.queries_only {
            for query in queries {
                // FIXME(ENG-2498): Use correct dialect.
                println!("{}", query.display(nom_sql::Dialect::MySQL));
            }
        } else {
            let queries = queries.collect::<Vec<_>>();
            for create_table_statement in gen.ddl() {
                // FIXME(ENG-2498): Use correct dialect.
                println!(
                    "{}",
                    create_table_statement.display(nom_sql::Dialect::MySQL)
                )
            }
            if !self.ddl_only {
                for query in queries {
                    // FIXME(ENG-2498): Use correct dialect.
                    println!("{}", query.display(nom_sql::Dialect::MySQL));
                }
            }
        }

        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    let opts = Opts::parse();
    opts.run()
}
