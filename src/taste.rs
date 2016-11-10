use repo::Workspace;

use std::str;
use std::process::{Command, ExitStatus};
use std::sync::Mutex;

pub struct TastingResult {
    pub commit_id: String,
    pub build: bool,
    pub bench: bool,
}

fn benchmark(workdir: &str) -> ExitStatus {
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

pub fn taste_commit(wsl: &Mutex<Workspace>, id: &String) -> TastingResult {
    println!("Tasting commit {}", id);
    let ws = wsl.lock().unwrap();
    ws.checkout_commit(id);

    TastingResult {
        commit_id: id.clone(),
        build: update(&ws.path).success() && build(&ws.path).success(),
        bench: benchmark(&ws.path).success(),
    }
}

fn update(workdir: &str) -> ExitStatus {
    Command::new("cargo")
        .current_dir(workdir)
        .arg("update")
        .status()
        .expect("Failed to execute 'cargo update'!")
}
