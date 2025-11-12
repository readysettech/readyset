use clap::{Parser, Subcommand, command};

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
}

fn main() -> anyhow::Result<()> {
    let options = Options::parse();

    match options.command {
        Commands::Rocksdb(options) => options.run(),
    }
}
