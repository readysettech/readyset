# Readyset Grafana Dashboards

Grafana dashboards for monitoring [Readyset](https://github.com/readysettech/readyset)
using the Prometheus metrics it exports.

| Dashboard                      | Description                                        |
|--------------------------------|----------------------------------------------------|
| `adapter_metrics.json`         | Adapter metrics                                    |
| `benchmarking.json`            | Graphs of metrics from Readyset's benchmark crate  |
| `evictions.json`               | Cache evictions                                    |
| `memory.json`                  | Memory usage                                       |
| `ops.json`                     | Operational overview                               |
| `perf_debugging.json`          | Performance debugging                              |
| `proxysql.json`                | ProxySQL integration                               |
| `query_overview.json`          | Metrics across all cached queries                  |
| `query_specific.json`          | Stats to examine a specific query                  |
| `replication_performance.json` | Replication pipeline performance                   |
| `shallow_caching.json`         | Shallow caching                                    |

## Usage

Import a dashboard through the Grafana UI (Dashboards > New > Import) and select your
Prometheus data source when prompted. You can also load them with Grafana's
file-based dashboard provisioning.

## Contributing

This repository is mirrored automatically from the Readyset source tree. Please
propose changes in
[readysettech/readyset](https://github.com/readysettech/readyset/tree/main/build/docker/grafana/dashboards).

## License

Apache License 2.0. See [LICENSE](LICENSE).
