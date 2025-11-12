use std::path::PathBuf;

use bincode::Options as _;
use clap::{Parser, Subcommand};
use readyset_data::DfValue;
use serde_json::Value;

#[derive(Subcommand)]
enum Operation {
    /// List all column families in the RocksDB
    #[command(visible_aliases(["l", "ls", "list"]))]
    ListColumnFamilies,
    /// Dump all rows from a specific column family
    #[command(visible_aliases(["d", "dump"]))]
    DumpData {
        /// The name of the column family to dump data from ("default" is metadata)
        #[arg(default_value = "default")]
        column_family: String,
    },
}

// todo: make persistent_State public
fn deserialize_row<T: AsRef<[u8]>>(bytes: T) -> Vec<DfValue> {
    bincode::options()
        .deserialize(bytes.as_ref())
        .expect("Deserializing from rocksdb")
}

#[derive(Parser)]
pub(crate) struct Options {
    /// Path to RocksDB data directory
    #[arg(short = 'd', env = "READYSET_ROCKSDB_DATADIR")]
    datadir: PathBuf,

    #[command(subcommand)]
    operation: Operation,
}

impl Options {
    pub(crate) fn run(self) -> anyhow::Result<()> {
        let column_families = rocksdb::DB::list_cf(&rocksdb::Options::default(), &self.datadir)?;

        match self.operation {
            Operation::ListColumnFamilies => {
                println!("Available column families:");
                for cf in &column_families {
                    println!("  - {cf}");
                }
            }
            Operation::DumpData { column_family } => {
                println!("Dumping data from column family: {column_family}");
                let db = rocksdb::DB::open_cf(
                    &rocksdb::Options::default(),
                    self.datadir,
                    column_families,
                )
                .unwrap();

                if let Some(cf_handle) = db.cf_handle(&column_family) {
                    let iter = db.iterator_cf(cf_handle, rocksdb::IteratorMode::Start);
                    let mut rows = 0;
                    for item in iter {
                        rows += 1;
                        match item {
                            Ok((key, value)) => {
                                if column_family == "default" {
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
                    return Err(anyhow::anyhow!(
                        "Column family '{}' not found",
                        column_family
                    ));
                }
            }
        }

        Ok(())
    }
}
