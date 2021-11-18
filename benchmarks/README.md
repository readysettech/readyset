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

The less-big potential footgun in the current implementation is histograms.  In the current
implementation of `metrics-exporter-prometheus`, _every single data point_ is stored for the
lifetime of the recorder.  This presents itself as what appears to be a memory leak.  Because these
benchmarks are short-lived, this is _usually_ an acceptable limitation; "fixing" it in our own code
would require a custom recorder implementation that wraps the Prometheus recorder, while working
around it is, from a high level, a matter of using `hdrhistogram` to record a histogram and then
entering each percentile as a gauge.  The `metrics-rs` project has an open issue to this
internally:  https://github.com/metrics-rs/metrics/issues/245

TODO: Document writing a benchmark within the abstraction.

## Example

A concrete (strawman) example can be found in the top comment block in `src/lib.rs`.

## Testing

For local testing `xtask mock-prometheus-push-gateway` is provided, e.g.
`cargo run --bin xtask -- mock-prometheus-push-gateway`.  When you run the
`benchmark` binary, you can pass `--prometheus http://localhost:9091`, e.g.
`cargo run --bin benchmark -- --job-name manual --prometheus
http://localhost:9091 my-benchmark`.  This will print any metrics that are sent
to it to stdout for easy manual verification.

