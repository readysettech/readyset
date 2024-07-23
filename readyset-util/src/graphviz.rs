//! This module provides functionality to create graphviz files on disk.
//! Requires the `dot` binary to be installed on the local machine.

use std::io::{Result, Write};
use std::path::Path;
use std::process::Command;

use tempfile::NamedTempFile;

/// Graphic file format in which to write out the graphviz file.
#[derive(Clone, Copy, Debug)]
pub enum FileFormat {
    /// Produce a PNG
    Png,
    /// Produce a SVG
    Svg,
}

/// Write a graphviz to disk, using the `dot` application.
pub fn write_graphviz(data: String, out_file: &Path, format: FileFormat) -> Result<()> {
    // write out as a temp file, get name, and pass as args to `dot`
    let mut temp_file = NamedTempFile::new()?;
    temp_file.write_all(data.as_bytes())?;

    let mut cmd = Command::new("dot");
    cmd.arg(format!("-o{}", out_file.to_str().unwrap()));
    match format {
        FileFormat::Png => cmd.arg("-Tpng"),
        FileFormat::Svg => cmd.arg("-Tsvg"),
    };

    cmd.arg(temp_file.path());

    // execute `dot`
    match cmd.output() {
        Ok(_) => Ok(()),
        Err(e) => Err(e),
    }
}
