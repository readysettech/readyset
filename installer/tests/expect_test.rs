use std::iter;
use std::path::Path;
use std::process::Command;

use itertools::Itertools;
use rexpect::errors::Error;
use scopeguard::defer;

const INSTALLER_BIN_NAME: &str = env!("CARGO_PKG_NAME");
const DOWN_ARROW: &str = "\u{1B}[Bm"; // ANSI escape code for the down arrow key

const TEST_DEPLOYMENT_NAME: &str = "nick_test_deployment";
const TEST_DEPLOYMENT_PASSWORD: &str = "nick_test_pass";
const TEST_DEPLOYMENT_PORT: u16 = 34567;
// Runtime is usually about 80 seconds so 3 min should be plenty:
const TEST_TIMEOUT_MILLISECONDS: u64 = 1000 * 60 * 3;

// We use a shared volume for buildkite runs so that if a test run is interrupted, the orchestrator
// state files are still available in the next run for later test runs to run clean up:
const TEST_CI_STATE_DIR: &str = "/tmp/orchestrator-state";

/// Return the path to the installer binary
fn path_to_installer() -> String {
    let mut path = std::env::current_exe().unwrap();

    path.pop(); // Remove test executable name
    path.pop(); // Remove "deps" dir
    path.push(INSTALLER_BIN_NAME);

    path.to_str().unwrap().to_string()
}

/// Return an argument list to be used for invocations of the installer, which will either be an
/// empty list for local runs (i.e. if we want to use the default state dir location) or a list
/// with a string containing the custom state dir location (which is used for buildkite builds).
fn state_dir_args() -> Vec<&'static str> {
    // If the /orchestrator-state dir exists, then assume we are running under buildkite
    if Path::new(TEST_CI_STATE_DIR).is_dir() {
        vec![TEST_CI_STATE_DIR]
    } else {
        vec![]
    }
}

/// Attempt to clean up any lingering deployment from this test. If assert_success is true, assert
/// that the cleanup succeeded (we expect cleanup to usually fail when we run it at the start of
/// the test, since we only call this function in case a previous test failed to clean up somehow,
/// but cleanup should always work at the end of the test).
fn clean_up(installer_bin: &str, state_dir_args: &Vec<&str>, assert_success: bool) -> () {
    println!("about to run installer tear-down command for cleanup");
    let output = Command::new(installer_bin)
        .args(
            state_dir_args
                .iter()
                .chain([&"tear-down", &TEST_DEPLOYMENT_NAME]),
        )
        .output()
        .expect("failed to run installer tear-down command for cleanup");
    println!("output of cleanup was {:?}", output);
    if assert_success {
        assert!(output.status.success())
    }
}

/// Execute the installer binary, using rexpect to feed input and verify results.
#[test]
fn installer_smoke_test() -> Result<(), Error> {
    let installer_bin = path_to_installer();
    let state_dir_args = state_dir_args();

    // always try to clean up at the start of the test, in case a previous test run was interrupted
    clean_up(&installer_bin, &state_dir_args, false);
    defer! { // also always attempt to clean up at the end of the test, regardless of pass/fail
        clean_up(&installer_bin, &state_dir_args, true)
    }

    let installer_cmd = iter::once(&installer_bin.as_str())
        .chain(&state_dir_args)
        .join(" ");
    let mut p = rexpect::spawn(&installer_cmd, Some(TEST_TIMEOUT_MILLISECONDS))?;
    p.exp_string("Welcome to the ReadySet orchestrator.")?;
    // We mostly use exp_regex instead of exp_string since it's otherwise very
    // ugly to have to include all of the control characters and such inline.
    p.exp_regex(concat!(
        "ReadySet supports both the MySQL or Postgres wire protocols, ",
        "select which one to use."
    ))?;
    p.exp_regex("Choose backing database:")?;
    p.send_line(DOWN_ARROW)?; // select "MySQL" by emulating down arrow followed by enter

    p.exp_regex("ReadySet deployment name")?;
    p.send_line(TEST_DEPLOYMENT_NAME)?;

    // Before giving the actual password we'll use, test with deliberately mismatching passwords:
    p.exp_regex("Deployment password")?;
    p.send_line("fakepass1")?;
    p.exp_regex("Confirm password")?;
    p.send_line("mismatching_fakepass")?;
    p.exp_regex("Passwords mismatching")?;

    p.exp_regex("Deployment password")?;
    p.send_line(TEST_DEPLOYMENT_PASSWORD)?;
    p.exp_regex("Confirm password")?;
    p.send_line(TEST_DEPLOYMENT_PASSWORD)?;

    p.exp_regex("Which port should ReadySet listen on\\?")?;
    p.send_line(&TEST_DEPLOYMENT_PORT.to_string())?;

    p.exp_regex(concat!(
        "Create a new backing MySQL database now\\? ",
        "Selecting 'no' will provide an opportunity to supply an existing ",
        "backing database instead"
    ))?;
    p.send_line("")?;

    p.exp_regex("Downloading Docker images")?;
    p.exp_regex("Docker Compose file was saved to")?;
    p.exp_regex("Deploying with Docker Compose now")?;
    p.exp_regex("ReadySet should be available in a few seconds.")?;
    p.exp_regex("Run the following command to connect to ReadySet via the MySQL client:")?;
    p.exp_regex("To connect to ReadySet using an application, use the following ReadySet")?;
    p.exp_regex("connection string:")?;
    p.exp_regex("Access the ReadySet dashboard at ")?;

    Ok(())
}
