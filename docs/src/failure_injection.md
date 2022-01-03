# Failure Injection

Deterministically being able to trigger failures allows us to gain confidence
that we handle specific failures correctly. For example:
  * If we trigger a worker failure when handling a base table update, the
    update should be visible when we recover from the failure.
  * If we trigger a leader failure during migration application, the prior
    recipe’s views should still be queryable after we recover from the failure.

We can accomplish this through the use of **failpoints**, user annotated code
points where a failure may be dynamically injected.

Rust has an existing failpoint library,
[**fail-rs**](https://crates.io/crates/fail), that allows (1) Dynamically
enabling and disabling failpoints, (2) Configuring failpoint behavior.

## Annotating Failpoints
Failpoints introduce test code to into our production code. To prevent
failpoints from being compiled into the production binary, the feature flag
`failure_injection` should be used. We provide two convinience macros
for annotating failpoints that are dependent on the `failure_injection`
feature.

  * The `#[failpoint("failpoint-name")]` attribute macro, which creates a
    failpoint at the start of the annotated function.
  * The `set_failpoint!("failpoint-name")` macro. This is a wrapper
    around [`set_failpoint`] that includes checking for the
    `failure_injection` feature.

```rust
use failpoint_macros::{failpoint, set_failpoint}

// Creates a failpoint at the start of the function.
#[failpoint("critical-function")]
fn critical_function() -> Result<()> {

  // Introduces a failpoint within functions.
  set_failpoint!("critical-function-2");
}

```

If the feature flag does not exist for the crate it should be added and
should enable fail-rs's failpoints feature.

```toml
[features]
failure_injection = ["fail/failpoints"]

[dependencies]
fail = "0.5.0"
```

## Using Failpoints in unit and integration tests.
Failpoints are global across a process: failpoints enabled in one test
impact all other tests that are running. To prevent failpoints from impacting
behavior in other tests, tests involving failpoints should be scoped to their
own file and run using `serial_test::serial`. When used in clustertests, the
`#[clustertest]` attribute macro handles includes `#[serial]`.

Enabled failpoints should also be disabled following a test run. The fail-rs
library provides [`FailScenario`](https://docs.rs/fail/0.5.0/fail/struct.FailScenario.html) to wrap enabling and disabling failpoints for a specific test.

```rust
use fail::FailScenario;
use failpoint_macros::failpoint;
use serial_test::serial

#[failpoint("read-dir")]
fn do_fallible_work() {
    let _dir: Vec<_> = std::fs::read_dir(".").unwrap().collect();
    // ... do some work on the directory ...
}

#[test]
#[serial]
fn example_test() {
    let scenario = FailScenario::setup();
    do_fallible_work();
    scenario.teardown();
}
```

## Configuring failpoints in noria-server at runtime.
Noria-server, when built with the `failure_injection` feature supports
dynamically configuring and toggling failpoints at runtime with the
`failpoint` binary in `noria/tools`.

```rust
use failpoint_macros::set_failpoint;

fn do_fallible_work() {
    let _dir: Vec<_> = std::fs::read_dir(".").unwrap().collect();
    set_failpoint!("critical-code-failure");
    // ... do some work on the directory ...
}
```

**Enabling a failpoint.**

Running this command would trigger a panic 20% of the time the
`critical-code-failure` failpoint is reached.
```bash
cargo run --bin failpoint -- --controller_address <addr> critical-code-failure 20%panic
```

**Disabling a failpoint.**
```bash
cargo run --bin failpoint -- --controller_address <addr> critical-code-failure off
```

**Failpoint behavior**
In the above example `20%panic` indicates, we should trigger the failpoint
20% of the time and when we trigger it, panic.

The format for specifying the failpoint behavior is: `[p%][cnt*]task`.

p% is the expected probability that the action is triggered, and cnt\* is the
max times the action can be triggered. A useful subset of the supported tasks:
sleep(ms), panic, print(msg), delay(ms), return(str).

See [`fail:cfg!`](https://docs.rs/fail/0.5.0/fail/fn.cfg.html) for more details
on how to specify failpoint behavior.

For more information `cargo run --bin failpoint -- --help`

## Failpoints in clustertests.

Clustertests can be used to trigger failpoints that crash the process and
verify the correct recovery behavior occured.

```rust
impl ServerHandle {
  async fn set_failpoint(&self, name: &str, action: &str);
}
```

Example clustertest crashing the leader by setting a failpoint on controller
RPC to panic. When the second `healthy_workers` call is made, the server
running the controller panics.
```rust
/// Test that setting a failpoint triggers a panic on RPC.
#[clustertest]
async fn leader_failure_with_failpoints() {
    ...
    assert_eq!(deployment.handle.healthy_workers().await.unwrap().len(), 1);

    let controller_uri = deployment.handle.controller_uri().await.unwrap();
    let server_handle = deployment.server_handle(&controller_uri).unwrap();
    server_handle
       .set_failpoint("controller-request", "panic")
       .await;

    // Request times out because the server panics.
    assert!(
        tokio::time::timeout(Duration::from_secs(1), deployment.handle.healthy_workers())
            .await
            .is_err()
    );
    deployment.teardown().await.unwrap();
}
```

Note: the noria-server binary built for clustertests must be built with
 --features failure_injection.

