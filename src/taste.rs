use repo::Workspace;

use std::str;
use std::process::{Command, ExitStatus};
use std::sync::Mutex;

fn benchmark(workdir: &str) {
    Command::new("cargo")
        .current_dir(workdir)
        .arg("bench")
        .arg("--release")
        .status()
        .expect("Failed to execute 'cargo bench'!")
}

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

    // Update dependencies
    if !update(&ws.path).success() {
        println!("Failed to update crates for {}", id);
        return;
    }

    // Try to build the commit
    if !build(&ws.path).success() {
        println!("Failed to build {}", id);
        return;
    }

    // Run benchmarks and collect results
    benchmark(&ws.path);
}

fn update(workdir: &str) -> ExitStatus {
    Command::new("cargo")
        .current_dir(workdir)
        .arg("update")
        .status()
        .expect("Failed to execute 'cargo update'!")
}
