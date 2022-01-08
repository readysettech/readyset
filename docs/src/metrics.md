# Metrics

Throughout the noria-server and noria adapter code we collect runtime metris. Some
examples include:
  * The amount of time performing an eviction at a dataflow node.
  * The number of times a query went to the upstream database vs. Noria.
  * The duration spent snapshotting the customer's database.

## Metrics Definition
The definition for the set of metrics recorded in our system are stored in `recorded`
modules. See: 
 * `//noria/noria/src/metrics/mod.rs`: The set of metrics recorded within noria-server.
 * `//noria-client/metrics/src/recorded.rs`: The set of metrics recorded within the adapter.

Metrics definitions are defined by the metric type: `Histogram`, `Gauge`, or `Counter`,
followed by a summary of the metric and a tag table. Tags can be used to differentiate
characteristics of the thing that is being measured.
```
/// <Metric Type>: <Description of the metric, when / how is it collected>
///
/// | Tag | Description |
/// | --- | ----------- |
/// | <tag> | <tag description> |
```

> **NOTE:** Do not use tags to store dimensions with high cardinality. Metrics are
> aggregated over every unique combination of tags - enough high cardinality will OOM
> the process. See [Prometheus Labels](https://prometheus.io/docs/practices/naming/)
> for more information.

## Recording a metric

We use the [`metrics-rs`](https://docs.rs/metrics/latest/metrics/) crate to collect
metrics. These metrics are typically exported to a prometheus exporter which is
scraped in our deployments. 

Collecting metrics is done through several macros:
```rust
use metrics::{histogram, counter};

histogram!(recorded::QUERY_LOG_EXECUTION_TIME, delta);

gauge!(recorded::ADAPTER_EXTERNAL_REQUESTS", 4);

counter!(
  recorded::MIGRATION_HANDLER_PROCESSED, 
  1, 
  query => "SELECT * FROM t".to_string()
);
```

> **NOTE:** Collecting metrics is not free, be especially careful when collecting metrics
> per dataflow packet.

## Testing metrics

See [Running ReadySet](./running-readyset.md) for more information on how to monitor
prometheus metrics locally. 

## Metrics Recorder
For the implementation of the wrapper around the metrics recorder in noria-server
see `//noria/server/src/metrics/mod.rs`.
