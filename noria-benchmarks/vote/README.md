# Noria micro-benchmark: vote

This benchmark measure the performance of a number of database-like
backends on the following SQL query in various forms:

```sql
SELECT a.id, a.title, vc.votes
FROM a JOIN (
 SELECT id, COUNT(user) AS votes FROM Vote GROUP BY id
) AS vc ON (a.id = vc.id)
```

It is the same benchmark that was used in §8.2 in the [OSDI'18 Noria
paper](https://jon.tsp.io/papers/osdi18-noria.pdf).

The benchmark implements the above query in MySQL with pre-computed vote
counts ([`vote mysql`](clients/mysql.rs)), the same with a memcached
cache ([`vote hybrid`](clients/hybrid.rs)), Microsoft SQL Server with
materialized views ([`vote mssql`](clients/mssql.rs)), a memcached-only
deployment ([`vote memcached`](clients/memcached.rs)), Noria with an
embedded server ([`vote localsoup`](clients/localsoup/)), and Noria with
a remote server ([`vote netsoup`]). If you are just benchmarking Noria,
you will likely want the `localsoup` backend.

The `vote` binary takes two sets of arguments, one to configure the
workload and one to configure the chosen backend. For example, to run
`localsoup`:

```console
$ cargo b --release --bin vote
$ target/release/vote \
   --target 100000 --write-every 20 --threads 16 \
   localsoup \
   --shards 2
```

Here, we're running `vote` with a target load of 100k requests per
second, where one in every 20 requests is a write (so a 95/5 read mix).
We run 16 load generation threads, which should be plenty (NOTE: this
argument will go away soon—it's annoying). We choose the `localsoup`
backend, and configure it to run with 2 shards (to increase write
throughput). See `vote --help` and `vote <backend> --help` for what
other arguments and backends are available.

When `vote` finishes, it prints the latencies it observed during
execution:

```text
# generated ops/s: 16673.66
# actual ops/s: 16683.94
# op   pct  sojourn  remote
write  50   1903     1271    (all µs)
read   50   847      311     (all µs)
write  95   4623     4039    (all µs)
read   95   1303     383     (all µs)
write  99   6535     5967    (all µs)
read   99   1879     1391    (all µs)
write  100  25343    24327   (all µs)
read   100  13567    12551   (all µs)
```

The "generated ops/s" value indicates how many operations were enqueued
for processing per second on average throughout the experiment. This
number should be close to the given `--target` value. The "actual ops/s"
is how many requests were retired/processed by the client threads per
second (again, on average). Each row is a particular point in the
latency histogram for reads or writes; by default the 50th (median),
95th, 99th, and 100th (max) percentiles are shown. You can get the full
histogram with `--histogram`. The time shown in the `remote` column is
the time from when a request was issued by a client until the backend
response came back. The time in the `sojourn` column is the time from
when a request was enqueued by the load generator until the request was
satisfied. If the backend is not keeping up, `sojourn` time will
increase as the queue grows. See [_Open Versus Closed: A Cautionary
Tale_](https://www.usenix.org/legacy/event/nsdi06/tech/full_papers/schroeder/schroeder.pdf)
for further details about this approach.

In general, to benchmark any backend, start with a low target
throughput, and increase it until the sojourn time skyrockets. Just
below that is the maximal supported throughput on your machine. You can
find the throughput and latency we observed on an Amazon EC2 VM
(`c5.4xlarge` in particular) in §8.2 of the [OSDI'18
paper](https://jon.tsp.io/papers/osdi18-noria.pdf).
