# Buildkite

We currently use buildkite for our CICD pipeline. There is a minimal main
pipeline that will run clippy, tests, and build images. If you break this
pipeline with any PR, the PR will be blocked from merging. Separately we have
long running testing going on in a nightly pipeline. This pipeline does not
block merging. It's therefore crucial that any breakages that occur in the
nightly pipeline are actively fixed.

It's advised that you run the generated logictests locally if you are
fixing a bug. In the case of a bug fix, it may cause logic tests that were
expected to fail to now pass, causing failure in the pipeline. These tests need
to be flipped, by changing the suffix on the file from `.fail.test` to `.test`.

## Running logic tests locally

You can use the following command from the root directory of the monorepo to run the same tests locally as are run in
the nightly pipeline:

```
$ make nightly-tests
```

If you have issues with your local environment, you can try running the tests
using docker compose:

```
$ make docker-nightly-tests

```

## Running main tests locally

It's not possible to run the entirety of the main test suite locally because it
depends on a deployed MySQL instance for binlog testing. To test this locally,
you can run it using docker compose with the following command from the root
directory of the monorepo:

```
$ make docker-tests
```
