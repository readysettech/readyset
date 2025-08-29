use bincode::Options as BincodeOptions;
use clap::{Parser, ValueEnum, command};
use readyset_data::DfValue;
use serde_json::Value;
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "readyset-support", about = "Readyset support tools")]
struct Options {
    /// Path to RocksDB data directory
    #[arg(long, env = "READYSET_ROCKSDB_DATADIR")]
    rocksdb_datadir: PathBuf,

    /// Operation to perform
    #[arg(long)]
    operation: Operation,

    /// Name of the column family to dump
    #[arg(long)]
    column_family: Option<String>,
}

#[derive(Parser, Clone, ValueEnum)]
enum Operation {
    ListColumnFamilies,
    DumpData,
}

// todo: make persistent_State public

fn deserialize_row<T: AsRef<[u8]>>(bytes: T) -> Vec<DfValue> {
    bincode::options()
        .deserialize(bytes.as_ref())
        .expect("Deserializing from rocksdb")
}

fn main() -> anyhow::Result<()> {
    let options = Options::parse();
    // Now you can use options.myrocks_datadir
    // ...
    let column_families =
        rocksdb::DB::list_cf(&rocksdb::Options::default(), &options.rocksdb_datadir)?;

    match options.operation {
        Operation::ListColumnFamilies => {
            println!("Available column families:");
            for cf in &column_families {
                println!("  - {cf}");
            }
        }
        Operation::DumpData => {
            if let Some(cf) = options.column_family {
                println!("Dumping data from column family: {cf}");
                let db = rocksdb::DB::open_cf(
                    &rocksdb::Options::default(),
                    options.rocksdb_datadir,
                    column_families,
                )
                .unwrap();
                //dump all data from the supplied column family.
                if let Some(column_family) = db.cf_handle(&cf) {
                    let iter = db.iterator_cf(column_family, rocksdb::IteratorMode::Start);
                    let mut rows = 0;
                    for item in iter {
                        rows += 1;
                        match item {
                            Ok((key, value)) => {
                                if cf == "default" {
                                    println!(
                                        "Key: {:?}, Value: {}",
                                        (key),
                                        serde_json::from_slice::<Value>(&value)
                                            .map(|v| serde_json::to_string_pretty(&v).unwrap())
                                            .unwrap_or_else(
                                                |_| String::from_utf8_lossy(&value).to_string()
                                            )
                                    );
                                } else {
                                    println!(
                                        "Key: {:?}, Value: {:?}",
                                        (key),
                                        deserialize_row(value)
                                    );
                                }
                            }
                            Err(e) => println!("Error reading key-value pair: {e}"),
                        }
                    }
                    println!("Dumped {rows} rows");
                } else {
                    return Err(anyhow::anyhow!("Column family '{}' not found", cf));
                }
            } else {
                return Err(anyhow::anyhow!("Please specify a column family to dump"));
            }
        }
    }

    Ok(())
}
