# distributary: a data-flow based database with automatic materialization

[![Build Status](https://travis-ci.com/mit-pdos/distributary.svg?token=BSd4zXamztCMoDZRewoH&branch=master)](https://travis-ci.com/mit-pdos/distributary)

This repository provides an implementation of the data storage system
model proposed in [Soup](https://github.com/mit-pdos/soup-paper).

At a high level, it takes a set of parameterized queries, and produces a
data-flow graph that maintains materialized views for the output of
those queries. This yields very high read throughput. Incremental
maintenance of the views through the data-flow graph also yields high
write throughput. See the papers in the repository above for further
information.

## Code

The code is written in [Rust](https://www.rust-lang.org/en-US/), and its
main entrypoint can be found in [`src/lib.rs`](src/lib.rs). That file
also contains a more extensive description of the internal system
architecture.

## Building

You need nightly Rust to run this code. [`rustup.rs`](https://rustup.rs/)
is the recommended tool for maintaining multiple versions of Rust (or
even for keeping a single nightly install up to date).

Once you have nightly rust, you can build the library
```console
$ cargo build
```

Run the test suite
```console
$ cargo test
```

And build the documentation (in `target/doc/distributary/`)
```console
$ cargo doc
```

## Benchmarks

Soup comes with a relatively simple benchmarking tool that executes
reads and writes in a schema with articles and votes. Each write is a
vote for a single article (all articles are pre-populated), and the
reads are for the title and vote count for random articles.

To build the benchmarking tool, run
```console
$ cargo build --bin vote --release
```

The `--release` enables compilation optimizations, and is necessary to
get sensible numbers. Note that compiling with optimizations will also
take longer than a regular debug build.

Once compiled, the benchmarker can be run with
```console
$ target/release/vote --runtime 30 --prepopulate-articles 10000 soup://
```

### Benchmarks against other targets

To compare distributary's performance against other backends, the `vote`
binary has to be compiled with the appropriate feature enabled. You can
enable multiple features by compiling with `--features "b_x b_y"`. The
additional features pull in more dependencies, so compilation will be
slower.

| target | `--features` | invocation argument | notes
|--------|--------------|------------|------
| Embedded soup | *none* | `soup://` |
| Soup over loopback | `b_netsoup` | `netsoup://127.0.0.1:7777` |
| PostgreSQL | `b_postgresql` | `postgresql://soup@127.0.0.1/bench_soup` | `soup` is the user, `bench_soup` the database -- note that the user must be allowed to drop and re-create the database.
| memcached | `b_memcached` | `memcached://127.0.0.1:11211` | the benchmarker does not purge the cache before starting, so you may want to restart memcached before running.
