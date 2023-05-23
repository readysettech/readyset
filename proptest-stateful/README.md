# proptest-stateful
## Introduction

`proptest-stateful` is a Rust library created by [ReadySet](https://readyset.io/)
for building stateful property-based tests using the
[proptest](https://altsysrq.github.io/proptest-book/proptest/index.html) crate.

Typical property-based tests are often stateless. That is, they do something
like generate an input, call a function, and check some property on the output,
all without depending on any particular underlying state in the system under
test. This is perfectly fine if you're testing a stateless piece of code where
the same input always generates the same output. But if you want to test
something stateful &mdash; like, say, a key/value store &mdash; then testing a
single function call at a time might not be sufficient to cover the full range
of behavior you expect the system to exhibit.

Stateful property tests generate a sequence of steps to run, and then execute
them one at a time, checking postconditions for each operation along the way.
If a test fails, the test framework will attempt to remove individual steps in
the test sequence to find a minimal failing case.

There are a few reasons that doing this is tricky enough to warrant a
standalone library:

 1. The expected result of an operation may depend on the underlying state of
    the system, so in order to check the result of each step, you may need to
    maintain a model of the expected state of the system under test.
 2. A given operation may not be expected to work in all possible states, so
    you may need to use your model of the system state to dictate which kinds
    of test steps can be generated at any given point in the sequence.
 3. When shrinking a failing case, the attempts to shrink must *still continue*
    to adhere to the same rules used during test case generation, or else a
    shrunken input may fail due to an operation being executed in an invalid
    state.

`proptest-stateful` provides a trait for defining stateful property tests, with
callbacks for specifying things like generation strategies, model states,
preconditions, and postconditions. Once these are defined, the
`proptest-stateful` code can do the heavy lifting of generating valid test
cases, running tests, and shrinking test failures.

## Example

To illustrate this in more concrete terms, suppose we're writing a stateful
test for a key/value store, and suppose reading from a non-existent key is not
supported and crashes the system, so we deliberately do not want to test this.
Possible operations might include writing a key/value pair, and reading a value
back from a key. If we start with an empty database, then the first operation
must *always* be a write, since there are no keys to read back yet.
Additionally, any read must be generated using a valid key from a prior write
operation.

If we just generate random operations with random inputs, we would frequently
try to read non-existent keys and incorrectly fail the test. Additionally,
without keeping track of which values we previously wrote, it would not be
possible to assert that a read operation yields the correct result. Finally, if
the test fails, we must be sure not to shrink out a write for a key that is
later read back again, or else failing cases may shrink out the wrong
operations.

TODO: actually code up an example test like this, and link to it here.

## Additional Resources

The API reference documentation is available
[here](https://docs.rs/proptest-stateful/latest/proptest_stateful/).

For more complex real-world examples, check out these test suites we've
written:
 * [ddl_vertical.rs](https://github.com/readysettech/readyset/blob/main/replicators/tests/ddl_vertical.rs)
 * [vertical.rs](https://github.com/readysettech/readyset/blob/main/readyset-mysql/tests/vertical.rs)

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

Currently, we only shrink by removing entire operations, not be shrinking the
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
