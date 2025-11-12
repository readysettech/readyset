use clap::{Parser, Subcommand};

mod events;
mod rocksdb;

#[derive(Parser)]
#[command(name = "readyset-support", about = "Readyset support tools")]
struct Options {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// RocksDB inspection commands
    Rocksdb(rocksdb::Options),
    /// Watch SSE events from Readyset server
    WatchEvents(events::Options),
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let options = Options::parse();

    match options.command {
        Commands::Rocksdb(options) => options.run(),
        Commands::WatchEvents(options) => options.run().await,
    }
}
