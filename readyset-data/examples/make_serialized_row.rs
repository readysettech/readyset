//! This binary generates a "reference" row of `DfValue`s serialized using bincode to a file, for
//! use in testing backwards compatibility of `DfValue`'s `serde::Serialize` impl.
//!
//! See `tests::serialize_backwards_compatibility` in `src/serde.rs` for more information.
//!
//! This is not really an "example", but the examples dir is the place where binaries that depend on
//! dev-dependencies go, so...

use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;

use readyset_data::DfValue;

fn main() -> anyhow::Result<()> {
    let out_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("serialized-row.bincode");

    let serialized = bincode::serialize(&DfValue::example_row())?;

    eprintln!("Writing serialized row to {}", out_path.display());
    OpenOptions::new()
        .truncate(true)
        .create(true)
        .write(true)
        .open(out_path)?
        .write_all(&serialized)?;

    Ok(())
}
