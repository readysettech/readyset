# Benchmarking your code

`//benchmarks` let you run macro-benchmarks for ReadySet performance. These include:
  * Executing any arbitrary query at a specific queries-per-second with a specific access pattern.
  * Comparing ReadySet cache hit and cache miss performance for a query.
  * Performing many migrations against the leader.
  * Churning connections.

Benchmarks are all executed as subcommands on the `benchmarks` binary.
The complete set of benchmarks can be seen with `cargo run --bin benchmarks -- --help`.

## Running benchmarks from specs
```
# From the root of the ReadySet repo.
cargo run --bin benchmarks -- --iterations 10 --benchmark benchmarks/src/yaml/benchmarks/read_benchmark_irl_small.yaml \
    --deployment benchmarks/src/yaml/deployments/local.yaml
```

The `--deployment` and `--benchmark` arguments can be used to pass in YAML that completely
define the deployment we are executing the benchmarks against, and the benchmark we are
running.

See `//benchmarks/src/yaml` for existing YAML specifications.

> <b>Useful Arguments</b>
>  
> * `--skip-setup`: Run a benhmark without performing setup. Setup will fail if the MySQL database
>                   already includes any of the tables.
> * `--iterations <N>`: Run a benchmark N times and calculate aggregates over the benchmark results.
>                       Not supported by all benchmarks.

## Specifying your own benchmark parameters.

```
cargo run --bin benchmarks -- <benchmark> <benchmark params>
```

**Deployment Parameters**

Benchmarks require specifying the parameters of the deployment we are testing, such as (1) the readyset-adapter
connection string, (2) the mysql database connection string, (3) optional prometheus parameters.

**Benchmark Parameters**

Each benchmark may specify a unique set of parameters. Run `cargo run --bin benchmarks -- <benchmark> --help`
for the set of benchmark parameters.

> <b>Annotations</b>
> 
> Annotations may be written for database schemas and queries to configure how to generate data,
> and how to generate queries in the benchmark, respectively. See: 
>  * `DistributionAnnotation` docs for the complete annotation spec format.
>  * `//benchmarks/src/data/irl` for examples on how to annotate schemas and query specs.

## Creating a new benchmark specification.

Executing any benchmark outputs a benchmark specification at the start of the run that can be copied to
a file. The argument `--only-to-spec <file>` may be used to write the benchmark spec to a file instead
of executing it.

## Data Generation

To perform data generation separate from a benchmark use the `data_generator` binary.

Sample usage:
```
cargo run --bin data_generator -- --schema benchmarks/src/data/irl/irl_db_small.sql
```

# TODO: Microbenchmark information.
