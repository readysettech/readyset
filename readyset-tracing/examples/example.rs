use clap::Parser;
use readyset_tracing::Options;
use tracing::{debug, error, info, span, trace, warn, Level};

fn main() -> anyhow::Result<()> {
    let opts = Options::parse();
    opts.init("example")?;
    trace!("A trace level event");
    debug!("A debug level event");
    info!("An info level event");
    warn!("A warn level event");
    error!("An error level event");

    let span = span!(Level::INFO, "An info level span", key = "value");
    let _guard = span.enter();
    warn!("A warn level event");

    Ok(())
}
