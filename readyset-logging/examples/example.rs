use clap::Clap;
use readyset_logging::Options;
use tracing::{debug, error, info, span, trace, warn, Level};

fn main() -> anyhow::Result<()> {
    let opts = Options::parse();
    opts.init()?;
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
