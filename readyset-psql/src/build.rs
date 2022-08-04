use std::process::Command;
fn main() {
    // This is best effort. A failure here shouldn't cause us to fail building entirely.
    if let Ok(output) = Command::new("git").args(&["rev-parse", "HEAD"]).output() {
        if let Ok(git_hash) = String::from_utf8(output.stdout) {
            // Taken from https://stackoverflow.com/questions/43753491/include-git-commit-hash-as-string-into-rust-program.
            // By setting CARGO_PKG_VERSION we automatically get --version from clap to spit out
            // the git hash.
            println!("cargo:rustc-env=CARGO_PKG_VERSION={}", git_hash);
        }
    }
}
