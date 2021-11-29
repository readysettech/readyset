# Working with Proptest in CI

We use the [proptest library][proptest] in a in a number of places in the
Readyset codebase to get easy randomized property tests, both as part of normal
rust tests and as part of the fuzzing support in the
[noria-logictest][noria-logictest] tool. This is great, because it's a really
easy way to find novel bugs that a human never could, but requires a little bit
of extra diligence when running in CI. Specifically, especially for tests with a
larger, more complex state space and more possible edge conditions, it's
possible a running proptest will find a novel failure case in a completely
unrelated build! In those cases, we need to make sure to remember about the new
failure case, and fix it at a later date.

## Failures in unit tests

If you see a new proptest failure locally, proptest has saved the random seed it
used to generate that failure case to a file named after the module the test is
in. For example, if you find a new failure in a proptest in
`launchpad/src/intervals.rs`, the seed will be located in
`launchpad/proptest-regressions/intervals.txt`, and will look something like:

```
cc 76517c7ebee7b98b39a24269458932fdaa7fcd0b9671e68a3a91f0473ebd9760 # shrinks to r = (Included(43), Excluded(43))
```

You can then put the seed in the description of the JIRA ticket you create to
report the new bug, and the developer that fixes the bug can put the seed in
that file to reproduce the issue. In the meantime, if the test fails rarely
enough you can just re-run it, or if you're blocked by the failure and it's
definitely unrelated to your work you can `#[ignore]` the test.

### In CI

If you see a new proptest failure in *CI*, the process is slightly more
involved - since proptest only saves the regressions it finds to a *file*, we
need a way to get access to a file. We achieve this by uploading the
`proptest-regressions` files for all tests to a buildkite [artifact][], which
can be downloaded via the `Artifacts` tab next to the build log in the build
step that failed. Once you download this file, follow the same steps as above

[proptest]: https://docs.rs/proptest/1.0.0/proptest/
[noria-logictest]: https://docs.readyset.io/noria_logictest/index.html
[artifact]: https://buildkite.com/docs/pipelines/artifacts
