use std::process::Command;

fn main() {
    println!("cargo:rerun-if-changed=../../.git/HEAD");

    set_version_info();
}

/// Sets the fields to populate ['VERSION']
/// This is best effort. A failure here shouldn't cause us to fail building entirely.
fn set_version_info() {
    set_release_version();
    set_commit_id();
    set_platform();
    set_rustc_version();
    set_profile();
    set_opt_level();
}

/// Set RELEASE_VERSION to one of the following, in order:
/// - $RELEASE_VERSION
/// - "unknown-release-version"
fn set_release_version() {
    env_or_unknown("RELEASE_VERSION", "release-version");
}

/// Set COMMIT_ID to one of the following, in order:
/// - $BUILDKITE_COMMIT
/// - $(git rev-parse HEAD)
/// - "unknown-commit-id"
fn set_commit_id() {
    let get_commit_id_from_git = || {
        Command::new("git")
            .args(&["rev-parse", "HEAD"])
            .output()
            .ok()
            .and_then(|output| {
                String::from_utf8(output.stdout)
                    .ok()
                    .map(|s| s.trim().to_owned())
            })
    };

    env_or_unknown_with_fallback("BUILDKITE_COMMIT", "commit-id", get_commit_id_from_git);
}

/// Set PLATFORM to one of the following, in order:
/// - $PLATFORM
/// - $TARGET (set by cargo)
/// - "unknown-platform"
fn set_platform() {
    let maybe_target = || std::env::var("TARGET").map(|s| s.trim().to_owned()).ok();
    env_or_unknown_with_fallback("PLATFORM", "platform", maybe_target);
}

/// Set RUSTC_VERSION to one of the following, in order:
/// - $RUSTC_VERSION
/// - $(rustc --version)
/// - "unknown-rustc-version"
fn set_rustc_version() {
    let maybe_rustc_version = || {
        Command::new("rustc")
            .args(&["--version"])
            .output()
            .ok()
            .and_then(|output| {
                String::from_utf8(output.stdout)
                    .ok()
                    .map(|s| s.trim().to_owned())
            })
    };
    env_or_unknown_with_fallback("RUSTC_VERSION", "rustc-version", maybe_rustc_version);
}

/// Set PROFILE to one of the following, in order:
/// - $PROFILE (set by cargo)
/// - "unknown-profile"
fn set_profile() {
    env_or_unknown("PROFILE", "profile");
}

/// Set OPT_LEVEL to one of the following, in order:
/// - $OPT_LEVEL (set by cargo)
/// - "unknown-profile"
fn set_opt_level() {
    env_or_unknown("OPT_LEVEL", "profile");
}

/// Sets cargo::rustc-env=$env if it is set,
/// Otherwise sets it to unknown-$unknown
fn env_or_unknown(env: &str, unknown: &str) {
    env_or_unknown_with_fallback(env, unknown, || None)
}

/// Sets cargo::rustc-env=$env if it is set and not empty,
/// otherwise runs fallback and sets it to that if it returns Some,
/// Otherwise sets it to unknown-$unknown
fn env_or_unknown_with_fallback<F>(env: &str, unknown: &str, fallback: F)
where
    F: Fn() -> Option<String>,
{
    println!("cargo:rerun-if-env-changed={env}");
    let rustc_env = std::env::var(env)
        .ok()
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(||
            fallback().unwrap_or_else(|| {
            println!("cargo:warning=Failed to get {unknown} from either CI env or local repository. It will be absent from run-time version information.");
            format!("unknown-{unknown}")
        }));
    println!("cargo:rustc-env={env}={rustc_env}");
}
