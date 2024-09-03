use std::env;
use std::path::Path;

fn main() {
    env::set_current_dir(Path::new("..")).expect("failed to change path");
    let status = std::process::Command::new("cargo")
        .args([
            "--locked",
            "build",
            "--target-dir",
            "target/clustertest",
            "--release",
            "--bin",
            "readyset",
            "--bin",
            "readyset-server",
            "--features",
            "failure_injection",
        ])
        .status()
        .expect("failed to execute cargo");
    assert!(
        status.success(),
        "cargo exited with nonzero status: {status}"
    );
}
