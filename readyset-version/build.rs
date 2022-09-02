use std::process::Command;

fn main() {
    // This is best effort. A failure here shouldn't cause us to fail building entirely.

    // Declare re-run rules
    println!("cargo:rerun-if-changed=../../.git/HEAD");
    println!("cargo:rerun-if-env-changed=BUILDKITE_COMMIT");

    // Set COMMIT_ID to one of the following, in order:
    // - $BUILDKITE_COMMIT
    // - $(git rev-parse HEAD)
    // - "unknown-commit-id"
    let maybe_buildkite_commit = std::env::var("BUILDKITE_COMMIT");
    let maybe_git_hash = Command::new("git")
        .args(&["rev-parse", "HEAD"])
        .output()
        .ok()
        .and_then(|output| String::from_utf8(output.stdout).ok());
    match (maybe_buildkite_commit, maybe_git_hash) {
        // If both are present and they aren't the same, warn, but still take BUILDKITE_COMMIT
        (Ok(buildkite_commit), Some(git_hash)) if buildkite_commit != git_hash => {
            // These warnings were getting clipped, so now they're in separate statements
            println!(
                "cargo:warning=Found BUILDKITE_COMMIT={buildkite_commit}, but HEAD={git_hash}"
            );
            println!("cargo:warning=Using BUILDKITE_COMMIT as the commit ID for run-time version information");
            println!("cargo:rustc-env=COMMIT_ID={buildkite_commit}");
        }
        // If BUILDKITE_COMMIT is present, take it
        (Ok(buildkite_commit), _) => {
            println!("cargo:rustc-env=COMMIT_ID={buildkite_commit}");
        }
        // If only the git repo is present, take $(git rev-parse HEAD)
        (Err(_), Some(git_hash)) => {
            println!("cargo:rustc-env=COMMIT_ID={git_hash}");
        }
        // If neither is present, warn and take "unknown-commit-id"
        (Err(_), None) => {
            println!("cargo:warning=Failed to get git commit ID from either CI environment or local repository. It will be absent from run-time version information.");
            println!("cargo:rustc-env=COMMIT_ID=unknown-commit-id");
        }
    }
}
