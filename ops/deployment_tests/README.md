# Deployment tests.

This crate adds binaries that verify basic deployment behaviors.

## Verify Healthy Workers
This issues a controller RPC to validate that the number of workers in ReadySet
matches what we expect in our deployment.

```
verify_healthy_workers [OPTIONS] --deployment <DEPLOYMENT> --num-workers <NUM_WORKERS>
```

## Verify Prometheus Metrics
This issues PromQL queries against the prometheus server, specified by
`--prometheus-address <PROMETHEUS_ADDRESS`. In our cloudformation deployment this
is the monitor instance on port 9091. This asserts various properties on queries
executed against Noria and MySQL. As a result, it should only be run after
queries have been executed against the system.

```
verify_prometheus_metrics [OPTIONS] --prometheus-address <PROMETHEUS_ADDRESS> <SUBCOMMAND>
SUBCOMMANDS:
    end-to-end-latency: Noria queries all have end-to-end latencies < 20ms.
    mysql-queried: MySQL was queried atleast once through the adapter.
    noria-queried: MySQL was queried atleast once through the adapter.
```
