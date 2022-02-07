use clap::Parser;

use metrics_aggregator::{run, Options};

fn main() -> anyhow::Result<()> {
    let options = Options::parse();

    run(options)
}
