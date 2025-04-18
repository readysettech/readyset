# Benchmarks

This directory contains all code that is used to benchmark performance on
ReadySet. Readyset deployment's used for benchmarking will build all targets
from this directory.

## Example

A concrete (strawman) example can be found in the top comment block in `src/lib.rs`.

## Testing

When you run the `benchmark` binary, you can pass `--prometheus http://localhost:9091`, e.g.
`cargo run --bin benchmark -- --job-name manual --prometheus
http://localhost:9091 my-benchmark`.
