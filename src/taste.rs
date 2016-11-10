use repo::Workspace;

use std::str;
use std::process::{Command, ExitStatus};
use std::sync::Mutex;

fn benchmark(workdir: &str) {}

fn build(workdir: &str) -> ExitStatus {
    Command::new("cargo")
        .current_dir(workdir)
        .arg("build")
        .arg("--release")
        .status()
        .expect("Failed to execute 'cargo build'!")
}

pub fn taste_commit(wsl: &Mutex<Workspace>, id: &String) {
    println!("Tasting commit {}", id);
    let ws = wsl.lock().unwrap();
    ws.checkout_commit(id);

    // Try to build the commit
    if !build(&ws.path).success() {
        println!("Failed to build {}", id);
        return;
    }

    // Run benchmarks and collect results
    benchmark(&ws.path);
}
