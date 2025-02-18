# Benchmarks

This directory contains all code that is used to benchmark performance on
ReadySet. Readyset deployment's used for benchmarking will build all targets
from this directory.

The big potential footgun in the current implementation is around lock contention.  The various
metric macros exported by this crate trade performance for convenience; they always make
registration calls, which acquire a write lock on metric descriptions.  We chose that to reduce the
number of calls that need to be written, under the assumption that these will be _written_
infrequently even if they're _called_ frequently (e.g. in a loop).  Current benchmarks aggregate
metrics in a single greenthread and call the macros from there.  If this becomes a significant
burden on benchmark implementations, we are open to changing it.

## Example

A concrete (strawman) example can be found in the top comment block in `src/lib.rs`.

## Testing

When you run the `benchmark` binary, you can pass `--prometheus http://localhost:9091`, e.g.
`cargo run --bin benchmark -- --job-name manual --prometheus
http://localhost:9091 my-benchmark`.
