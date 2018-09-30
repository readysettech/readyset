# noria: a data-flow system for web applications

[![Build Status](https://travis-ci.org/mit-pdos/distributary.svg?branch=master)](https://travis-ci.org/mit-pdos/distributary)

This repository contains the work-in-process implementation of a new
streaming data-flow system designed to pre-compute relational query
results for efficient reads, with reasonable space overhead through
partially-materialized state and dynamic, runtime installation of new
queries.

At a high level, it takes a set of parameterized queries, and produces a
data-flow graph that maintains materialized views for the output of
those queries. This yields very high read throughput. Incremental
maintenance of the views through the data-flow graph also yields high
write throughput.

There is also a [binary MySQL protocol](https://github.com/mit-pdos/distributary-mysql)
adapter for Noria.

## Code

The code is written in [Rust](https://www.rust-lang.org/en-US/), and its
main entrypoint can be found in [`src/lib.rs`](src/lib.rs). That file
also contains a more extensive description of the internal system
architecture.

## Building

You need nightly Rust to run this code. [`rustup.rs`](https://rustup.rs/)
is the recommended tool for maintaining multiple versions of Rust (or
even for keeping a single nightly install up to date).

Once you have nightly rust, you can build the library:
```console
$ cargo build
```

Run the test suite:
```console
$ cargo test
```

Build the documentation (in `target/doc/noria/`):
```console
$ cargo doc
```

And run the example:
```console
$ cargo run --example basic-recipe
```

## Running a standalone instance

The `souplet` daemon provides a stand-alone server instance that can host
materialized views and which processes shards of the data-flow graph. The
`souplet` instances in the same _deployment_ automatically elect a leader
and discovery each other via [ZooKeeper](http://zookeeper.apache.org/). To
run `souplet`, you therefore need to have access to a ZooKeeper cluster.

Run the `souplet` as follows:
```console
$ cargo run --release --bin souplet -- --zookeeper 127.0.0.1:2181 --deployment my_deployment
```
The elected leader `souplet` runs a REST API bound to a random port. You
can read the port from Zookeeper via this command:
```console
$ echo "CONTROLLER API ON: $(cargo run --manifest-path=consensus/Cargo.toml \
    --bin zk-util -- --show --deployment testing | grep external | \
    cut -d' ' -f 4)"
```
A basic graphical UI runs at `http://IP:PORT/graph.html`. The UI show
the running data-flow graph and updates as it changes.

## Benchmarks

Soup comes with a relatively simple benchmarking tool that executes
reads and writes in a schema with articles and votes. Each write is a
vote for a single article (all articles are pre-populated), and the
reads are for the title and vote count for random articles.

To build the benchmarking tool, run:
```console
$ cargo build --manifest-path=benchmarks/Cargo.toml --bin vote --release
```

The `--release` enables compilation optimizations, and is necessary to
get sensible numbers. Note that compiling with optimizations will also
take longer than a regular debug build.

Once compiled, the benchmarker can be run with:
```console
$ target/release/vote --help
```
The help output explains how to use the benchmarker, and provides brief
descriptions of other targets it can run against for comparison.
