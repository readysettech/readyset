use clap::Parser;

use metrics_aggregator::run;
use metrics_aggregator::Options;

fn main() -> anyhow::Result<()> {
    let options = Options::parse();

    run(options)
}
