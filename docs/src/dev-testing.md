# Testing

## Unit testing

Unit tests typically live at the bottom of the file with the code you are testing.
See [Test Organization](https://doc.rust-lang.org/book/ch11-03-test-organization.html)
for more information on organizing your tests.

These tests live at the bottom of files and typically take the following form:

```rust
pub fn add_two(a: i32) -> i32 {
    a + 2
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_two_very_well() {
        assert_eq!(6, add_two(4));
    }
}
```

See [Unit testing](https://doc.rust-lang.org/rust-by-example/testing/unit_testing.html)
for information on common macros that can be used in unit tests such as
`assert_eq`.

> [`cargo test`](https://doc.rust-lang.org/cargo/commands/cargo-test.html) is used to run tests.
> By default tests it runs all the tests within the current package or workspace in parallel.
>
> ```bash
> # Run all tests in the noria-server package with fancy_test in their name.
> cargo test -p noria-server fancy_test
> ```
> ❗Some tests cannot be run in parallel with others, these tests are
> typically in files postfixed with `_serial`. Running the entire set
> of tests for a package, i.e. `cargo test -p noria-server` may fail
> if serial tests are included.
>
> ❗Running tests may require increasing file descriptor limits via running `ulimit -Sn 65535`.

## Integration tests
ReadySet integration tests create a multi-threaded noria deployment that can be used to test
queries against. These integration tests use a `LocalAuthority`, an in-process in-memory
authority.

### Noria Standalone

These integration tests verify noria-server behavior end-to-end with noria client queries.

 * `//noria/server/src/integration.rs`: Integration tests for noria standalone.
 * `//noria/server/src/integration_serial.rs`: For tests that include metrics that cannot be run in parallel.
 * `//noria/server/src/integration_utils.rs`: Common testing utilities for integration tests.

### Adapter + Server

These integration tests verify adapter + noria-server behavior end-to-end with SQL queries.

 * `//noria-mysql/integration.rs`: Integration tests including MySQL.
 * `//noria-mysql/fallback.rs`: Integration tests that exercise that ReadySet proxies queries to a customer database correctly.
 * `//noria-pysql/integration.rs`: Integration tests including
   PostgreSQL.

**Example integration test:**
```rust
#[test]
fn prepare_execute_query() {
  let mut conn = mysql::Conn::new(setup(true)).unwrap();
  conn.query_drop("CREATE TABLE posts (id int, title TEXT)")
    .unwrap();
  conn.query_drop("INSERT INTO posts (id, title) VALUES (1, 'hi')")
    .unwrap();

  let result: (u32, String) = conn
    .exec_first("SELECT id, title FROM posts WHERE id = ?", (1,))
    .unwrap()
    .unwrap();
  assert_eq!(result, (1, "hi".to_owned()));

}
```


## Logictests
Logictests verify that ReadySet computes results that match a SQL database (MySQL or PostgreSQL). We have implemented
our own logictests execution framework based on [Sqllogictest](https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki).
Logictests run an in-process version of the adapter and server, similar to integration tests.

See `//noria/noria-logictest/` for the implementation of the framework or to make changes.

### Running logictests
The verify subcommand is used to validate logictests run against Noria.
```bash
# Running the basic set of logictests.
cargo run --bin noria-logictest -- verify logictests

# Running logictests against a reference MySQL server, to validate test correctness
docker-compose up -d mysql
cargo run --bin noria-logictest -- verify logictests/simple-params.test --mysql

# Running logictests with MySQL replication.
cargo run --bin noria-logictest -- verify --replication-url mysql://root:noria@mysql/sqllogictest logictests

# Running logictests that should pass with fallback. These queries are likely unsupported in Noria.
cargo run --bin noria-logictest -- verify --replication-url mysql://root:noria@mysql/sqllogictest logictests/requires-fallback

# See additional options
cargo run --bin noria-logictest -- verify --help
```

> <b>Generated logictests</b>
>
> In our nightly CI pipeline nightly we run thousands of logictests generated using the `query_generator`.
> The query generator is a deterministic, exhaustive, parametric generator for SQL queries, and associated DDL.
> See `//query_generator` for more details.

## Vertical testing
Vertical testing is a framework that tests **sequence of operations** rather than testing **different queries**.

It performs operations like:
  * Perform a query for a key that doesn't exist.
  * Perform a query for a key that *does* exist.
  * Perform a delete for a key.
  * Evict from a node in the graph.

Vertical tests live in `//noria-mysql/tests/vertical.rs`. Adding a new vertical test can be done by
adding a new test case in the vertical_tests! macro. Example for a simple point lookup:

```rust
vertical_tests! {
  simple_point_lookup(
    "SELECT id, name FROM users WHERE id = ?";
    "users" => (
      "CREATE TABLE users (id INT, name TEXT, PRIMARY KEY (id))",
        schema: [id: i32, name: String],
        primary_key: 0,
        key_columns: [0],
      )
  );
}
```

## Clustertests
Clustertests are a multi-process deployment test framework. It enables local
blackbox testing on multi-server deployments with support for
programatically modifying the deployment, i.e. introducing faults,
adding new servers, replicating readers. Clustertests run with a MySQL database
and a consul authority run through docker containers.

This makes this framework well suited for:
  * Testing fault tolerance and failure recovery behavior. Clustertests can
    easily replicate failure recovery behavior (or lack thereof) via the `kill_server`
    and `start_server` API.
  * Testing behavior that is localized to specific workers. Each worker in
    a deployment can be queried for metrics via the deployment's `MetricsClient`.

Clustertests live in:
 * `//noria/clustertest/src/readyset.rs`: Noria standalone
 * `//noria/clustertest/src/readyset_mysql.rs`: Noria + MySQL adapter

See `//noria/clustertest/src/lib.rs` for more complete documentation on how to
write and run clusteretsts.

### Running clustertests
Run the complete set of clustertests using the default parameters with the following command.
```
cargo test -p clustertest
```

> This requires the ReadySet docker stack to be created with external resources. See
> [Getting Started](./getting_started.md) for more details on how
> to setup the ReadySet docker stacks.

### Failure Injection
ReadySet supports failure injection through dynamically trigger failures at code annotations.
When used with clustertest, this makes evaluating specific failures much simpler. The framework
allows generating extremely specific failures, such as:

*"Trigger a failure during a migration at a worker when it receives the second request from the leader"*

**Annotating code is also very simple, example:**
```
#[failpoint("critical-function")]
fn critical_function() {}
```

See **[Failure Injection](./failure_injection.md)** for much more details.
