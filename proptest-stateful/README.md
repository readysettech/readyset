# proptest-stateful

`proptest-stateful` is a Rust library created by
[ReadySet](https://readyset.io/) for writing stateful property-based tests
using the
[proptest](https://altsysrq.github.io/proptest-book/proptest/index.html) crate.

[API Reference
Documentation](https://docs.rs/proptest-stateful/latest/proptest_stateful/).

## Introduction

Property-based tests are often stateless: they generate an input, call a
function, and check some property on the output. They do this without
considering any underlying state of the system being tested.

`proptest-stateful` extends this testing paradigm to stateful systems: stateful
property tests generate a sequence of operations to run, executes them one at a
time, and checks postconditions for each operation along the way. If a test
fails, `proptest-stateful` will attempt to remove individual steps in the test
sequence to find a minimal failing case.

`proptest-stateful` provides a trait for defining stateful property tests, with
callbacks for specifying things like generation strategies, model states,
preconditions, and postconditions. Once these are defined, the
`proptest-stateful` code can do the heavy lifting of generating valid test
cases, running tests, and shrinking test failures.

## Quickstart

Suppose you want to test a simple `Counter` struct you've created:
```rust
struct Counter {
    count: usize,
}

impl Counter {
    fn new(count: usize) -> Self {
        Counter { count }
    }

    fn inc(&mut self) {
        self.count += 1;
    }

    fn dec(&mut self) {
        self.count -= 1;
    }
}
```
At the start of a test case, we'll create a new `Counter`, and generate a
sequence of `inc` and `dec` operations, so we first need to define an
`Operation` type to represent these operations:
```rust
#[derive(Clone, Debug)]
enum CounterOp {
    Inc,
    Dec,
}
```
The API requires us to define a state type, though for now it'll just be empty:
```rust
#[derive(Clone, Debug, Default)]
struct TestState {}
```
And we need a context type to hold runtime state when we're executing a test
case:
```rust
struct TestContext {
    counter: Counter,
}
```
Now we just need to define callbacks to tell the framework how to generate and
run test cases. (We currently only support async test cases, so this is a
little more complex than it needs to be since there's not really any need for
async in this test, but it still works fine.)
```rust
#[async_trait(?Send)]
impl ModelState for TestState {
    type Operation = CounterOp;
    type RunContext = TestContext;
    type OperationStrategy = BoxedStrategy<Self::Operation>;

    fn op_generators(&self) -> Vec<Self::OperationStrategy> {
        // For each step test, arbitrarily pick Inc or Dec, regardless of the test state:
        vec![Just(CounterOp::Inc).boxed(), Just(CounterOp::Dec).boxed()]
    }

    // No preconditions to worry about or test state to maintain yet
    fn preconditions_met(&self, _op: &Self::Operation) -> bool {
        true
    }
    fn next_state(&mut self, _op: &Self::Operation) {}

    async fn init_test_run(&self) -> Self::RunContext {
        let counter = Counter::new(3); // Start with 3 to make the failing cases more interesting
        TestContext { counter }
    }

    async fn run_op(&self, op: &Self::Operation, ctxt: &mut Self::RunContext) {
        match op {
            CounterOp::Inc => ctxt.counter.inc(),
            CounterOp::Dec => ctxt.counter.dec(),
        }
    }

    async fn check_postconditions(&self, _ctxt: &mut Self::RunContext) {}
    async fn clean_up_test_run(&self, _ctxt: &mut self::runcontext) {}
}
```
Finally, you can run a test like so:
```rust
#[test]
fn run_cases() {
    let config = ProptestStatefulConfig {
        min_ops: 10,
        max_ops: 20,
        test_case_timeout: Duration::from_secs(60),
        proptest_config: ProptestConfig::default(),
    };

    proptest_stateful::test::<TestState>(config);
}
```
If you run this, you should quickly see a failure, because we didn't account
for underflow! If a test case causes the counter value to drop below 0, the
test will fail.  The test will then proceed to shrink the failing case, which
will bring you from a random-looking string of increment/decrement operations
to this:
```
minimal failing input: [
    Dec,
    Dec,
    Dec,
    Dec,
]
```
(Since we start with the counter at 3, we need to decrement 4 times to trigger
an underflow.)

### Fixing This Example

Let's assume now that underflow is a known limitation, so we don't want to test
cases that will trigger underflow panics. To do this, we need to maintain an
actual model state of what we expect the current state of the counter to look
like:
```rust
struct TestState {
    model_count: usize,
}

impl Default for TestState {
    fn default() -> Self {
        TestState { model_count: 3 } // Set to match initial test value
    }
}
```
To keep it up to date as we generate test steps, we implement `next_state`:
```rust
    fn next_state(&mut self, op: &Self::Operation) {
        match op {
            CounterOp::Inc => {
                self.model_count += 1;
            }
            CounterOp::Dec => {
                self.model_count -= 1;
            }
        }
    }
```
And now we can use it for generators and preconditions:
```rust
    fn op_generators(&self) -> Vec<Self::OperationStrategy> {
        let mut ops = vec![Just(CounterOp::Inc).boxed()];
        if self.model_count > 0 {
            ops.push(Just(CounterOp::Dec).boxed());
        }
        ops
    }

    fn preconditions_met(&self, op: &Self::Operation) -> bool {
        match op {
            CounterOp::Inc => true,
            CounterOp::Dec => self.model_count > 0,
        }
    }
```
Running this test should now pass, because it will never generate a test case
that drops the counter value below 0.

You can see the completed code and run this yourself via the `tests/counter.rs`
file.

For more complex real-world examples, check out these test suites we've written
for ReadySet:
 * [ddl_vertical.rs](https://github.com/readysettech/readyset/blob/main/replicators/tests/ddl_vertical.rs)
 * [vertical.rs](https://github.com/readysettech/readyset/blob/main/readyset-mysql/tests/vertical.rs)

## Contributions

We welcome contributions! Check out our roadmap here. TODO link.

## Future Work

Note that this library is still relatively new, so we are releasing it as
pre-1.0 and expect there may regularly be breaking changes between versions. We
plan to release a stable version once we've used it enough to feel confident in
having implemented a solid API that holds up across a good variety of use
cases.

The current version of `proptest-stateful` only supports writing async tests
using [tokio](https://tokio.rs/), because that is the only type of test we've
needed to write thus far here at [ReadySet](https://readyset.io/). It almost
certainly would make sense to have a non-async version of the framework; we may
implement this ourselves at some point, but if this is something you would find
useful and you'd like to implement it directly, we are also always happy to
take outside contributions. Once a non-async version exists, we'll also likely
want to add a crate feature to allow us to disable the async functionality for
projects that don't need it.

Currently, we only shrink by removing entire operations, not by shrinking the
operations themselves. Shrinking an individual operation can be quite tricky,
however, because oftentimes later operations were generated based off the
specific operations that came before them. For some tests this may not be an
issue, but I have yet to personally encounter one. It may be possible to add
some kind of optional "fix operation" callback that can attempt to modify later
operations to make preconditions pass again after an earlier operation is
shrunk, but this seems like a tricky proposition, and in practice we've found
that the current shrinking method is sufficient; getting to a minimal set of
steps to produce a failure typically makes it easy to understand failing cases
even if the steps themselves are more complex than they need to be.

Finally, the shrinking algorithm we've implemented has worked well for the
tests we've written so far, but it is worth noting that it is limited to
removing a single test operation at a time during shrinking. (There is one
exception to this, which is that we automatically and immediately shrink out
all operations that occur after the step that triggered the initial test
failure as an optimization, but after that we only attempt to remove one step
at a time.) This results in a shrinking algorithm that was relatively simple to
implement and understand, but it brings two significant downsides:

 1. For tests with many operations, shrinking may be slow, especially if
    re-running a test case is itself a slow process.
 2. Depending on the preconditions specified, we may fail to shrink all the way
    to a minimal set of operations.

The latter issue is due to test cases that start with sequences like:

 1. `CREATE TABLE readyset`
 2. `DROP TABLE readyset`
 3. `CREATE TABLE readyset`

If step 3 is legitimately part of a minimal failing case, then we can't shrink
it out...but we also can't shrink out step 2 because then the precondition for
step 3 will fail (a table can't already exist before we create it), and we
can't shrink out step 1 because the precondition for step 2 will fail (a table
must exist for us to be able to drop it). So without the ability to try
shrinking out multiple steps at once, we're unable to shrink out the first two
steps.

All that said, it's not clear what the best alternative shrinking algorithm
might look like. More research is needed here, but if you have ideas or
suggestions in this area we'd love to hear from you 🙂
