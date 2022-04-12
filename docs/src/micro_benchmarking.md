# Micro Benchmarking

Micro Benchmarks can be viewed as the unit tests of the benchmarking
world. They're typically highly targeted, directly calling interenal
components of ReadySet.

## Criterion Benchmarks

[Criterion](https://github.com/bheisler/criterion.rs) is a Rust framework
for powerful microbenchmarking, providing tons of advanced features. Most
of our microbenchmarks are written using it.

## Basic Usage

Benchmarks are executed via the `cargo bench` subcommand, with the `bench` cargo feature enabled.

The simplest way to run micro benchmarks:
```
cargo bench --feature bench
```
This will run all the normal* micro benchmarks.

\* Inside `clustertest` are some special benchmarks which require
additionally passing the feature `slow_bench`. These tests are extremely
slow compared to the others, taking minutes instead of sub-seconds to
complete, so are disabled by default. These aren't really "micro"
benchmarks, but leverage the same framework as the rest.

### Run Specific Benchmarks

There's two ways to filter benchmarks:
* Specify a project with the `-p` flag. This can additionally reduce
  compile times, by being able to skip compiling benches outside the
  project.
* Specify a bench name filter. The only impacts execution, not
  compilation.

Project filter example:
```
cargo bench -p noria-server --features bench
```
This will run all the benchmarks inside the `noria-server` project.


Bench Name filter example:
```
cargo bench --features bench -- unique_misses
```
This will run the benchmarks that have `unique_misses` as a substring of
their name.