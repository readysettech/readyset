use std::fs::File;
use std::path::Path;
use std::process::Command;
use std::str;

use noria_server::Config;

fn main() {
    let out_directory = Path::new(file!())
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("server/tests/config_versions");
    let commit = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .unwrap()
        .stdout;
    let commit = str::from_utf8(&commit).unwrap().trim();
    let out_path = out_directory.join(format!("{}.json", commit));
    let out_file = File::create(out_path.clone()).unwrap();
    serde_json::to_writer(out_file, &Config::default()).unwrap();
    println!("wrote {}", out_path.display());
}
