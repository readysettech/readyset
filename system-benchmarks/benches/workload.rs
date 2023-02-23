use std::fmt::Display;
use std::fs::read_dir;
use std::io::{Read, Write};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use benchmarks::spec::WorkloadSpec;
use benchmarks::utils::generate::DataGenerator;
use benchmarks::utils::path::benchmark_path;
use benchmarks::QuerySet;
use clap::Parser;
use criterion::{BatchSize, BenchmarkId, Criterion, Throughput};
use database_utils::{DatabaseType, DatabaseURL};
use fork::{fork, Fork};
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use mysql_async::prelude::Queryable;
use mysql_async::Conn;
use nperf_core::args::{FlamegraphArgs, RecordArgs};
use readyset::mysql::MySqlHandler;
use readyset::{NoriaAdapter, Options};
use readyset_client::get_metric;
use readyset_client::metrics::{recorded, MetricsDump};
use regex::Regex;
use structopt::StructOpt;
use tokio::sync::Mutex;
use tokio::time::sleep;

/// Subdirectory where the benchmarks are kept
const BENCHMARK_DATA_PATH: &str = "./bench_data";
/// The ReadySet MySQL adapter listen port
const BENCHMARK_PORT: u16 = 50000;
/// The MySQL database name where benchmark schemas are installed
const DB_NAME: &str = "rs_bench";
/// The batch size (number of queries) ] per benchmark iteration
const BENCH_BATCH_SIZE: u64 = 8192;
/// The duration of the data collection step of criterion
const WORKLOAD_DURATION: Duration = Duration::from_secs(30);
/// The controller address to make RPC calls to
const LEADER_URI: &str = "http://127.0.0.1:6033";

/// The MySQL instance backing the databases used in those benchmarks
fn mysql_url(db: impl Display) -> String {
    format!(
        "mysql://root:noria@{}:{}/{}",
        std::env::var("MYSQL_HOST").unwrap_or_else(|_| "127.0.0.1".into()),
        std::env::var("MYSQL_TCP_PORT").unwrap_or_else(|_| "3306".into()),
        db
    )
}

/// The ReadySet MySQL adapter url
fn readyset_url() -> String {
    format!("mysql://127.0.0.1:{BENCHMARK_PORT}")
}

fn rpc_url(rpc: &str) -> String {
    format!("{LEADER_URI}/{rpc}")
}

/// An established connection to MySQL that also holds a list of prepared statements, prepared on
/// this specific connection
struct PreparedConn {
    conn: mysql_async::Conn,
    statements: Vec<mysql_async::Statement>,
}

/// A pool of multiple [`PreparedConn`]
struct PreparedPool {
    conns: Vec<PreparedConn>,
}

impl PreparedPool {
    /// Try to create a new pool with `num` connections to the given URL
    async fn try_new(num: usize, url: &str) -> anyhow::Result<Self> {
        let mut conns = Vec::with_capacity(num);
        for _ in 0..num {
            let conn = Conn::from_url(&url).await?;
            let statements = Vec::new();
            conns.push(PreparedConn { conn, statements })
        }
        Ok(PreparedPool { conns })
    }

    /// Prepare the given set of queries on every connection in the pool
    async fn prepare_pool(&mut self, query_set: &QuerySet) -> anyhow::Result<()> {
        for conn in self.conns.iter_mut() {
            conn.statements = query_set.prepare_all(&mut conn.conn).await?;
        }
        Ok(())
    }

    /// Prepare the given set of queries on every connection in the pool, then iterate over all
    /// possible workload queries and execute them one by one to get them into ReadySet readers.
    async fn prepare_pool_and_warm_cache(
        &mut self,
        workload: &WorkloadSpec,
    ) -> anyhow::Result<QuerySet> {
        let distributions = workload
            .load_distributions(&mut Conn::from_url(mysql_url(DB_NAME)).await?)
            .await?;

        let query_set = workload
            .load_queries(&distributions, &mut Conn::from_url(readyset_url()).await?)
            .await?;

        self.prepare_pool(&query_set).await?;

        // Make sure *everything* is in cache
        for query in query_set.queries() {
            if !query.migrate {
                continue;
            }
            let mut commands = Vec::new();
            let mut idx = 0;
            while let Some(params) = query.get_params_index(idx) {
                commands.push((query.idx, params));
                idx += 1;
            }
            self.run_all(Arc::new(commands)).await?;
        }

        Ok(query_set)
    }

    /// Run the list of provided commands on this pool in parallel, a command is a tuple of query
    /// index, and parameters for that query
    async fn run_all(
        &mut self,
        commands: Arc<Vec<(usize, Vec<mysql_async::Value>)>>,
    ) -> anyhow::Result<()> {
        let cur = Arc::new(AtomicUsize::new(0));

        let mut running_statements = FuturesUnordered::new();

        while let Some(mut conn) = self.conns.pop() {
            let cur = cur.clone();
            let commands = commands.clone();
            running_statements.push(tokio::spawn(async move {
                while let Some(command) = {
                    let next_command = cur.fetch_add(1, Ordering::Relaxed);
                    commands.get(next_command)
                } {
                    while conn
                        .conn
                        .exec_drop(&conn.statements[command.0], &command.1)
                        .await
                        .is_err()
                    {
                        // This happens when there is a transaction deadlock
                    }
                }

                conn
            }));
        }

        while !running_statements.is_empty() {
            self.conns.push(running_statements.next().await.unwrap()?);
        }

        Ok(())
    }
}

/// Describes a benchmark directory with a schema file and all workloads for that schema
#[derive(Debug)]
struct Benchmark {
    name: String,
    schema: PathBuf,
    workloads: Vec<PathBuf>,
}

impl Benchmark {
    /// Enumerate all benchmarks in the default benchmarks directory
    fn find_benchmarks(filter: regex::Regex) -> anyhow::Result<Vec<Self>> {
        let bench_dir = Path::new(BENCHMARK_DATA_PATH);
        let subdirs = read_dir(benchmark_path(bench_dir)?)?;

        // Find all subdirectories in the benchmarks directory
        let subdirs = subdirs
            .filter(|d| d.as_ref().unwrap().metadata().unwrap().is_dir())
            .map(|d| std::fs::canonicalize(d.unwrap().path()).unwrap());

        // Find the schema file (.sql) and all workload files (.yaml) for each benchmark subdir
        Ok(subdirs
            .map(|path| {
                let mut schema = None;
                let mut workloads = Vec::new();

                let benchmark_dir = read_dir(&path).unwrap();
                let name = path.file_name().unwrap().to_string_lossy().to_string();
                for file in benchmark_dir
                    .filter(|d| d.as_ref().unwrap().metadata().unwrap().is_file())
                    .map(|d| std::fs::canonicalize(d.unwrap().path()).unwrap())
                {
                    if let Some(ext) = file.extension() {
                        if ext == "sql" && schema.replace(file.clone()).is_some() {
                            panic!("More than one schema for benchmark {name}");
                        } else if ext == "yaml"
                            && filter.is_match(&format!(
                                "{}/{}",
                                name,
                                file.file_stem().unwrap().to_string_lossy()
                            ))
                        {
                            workloads.push(file);
                        }
                    }
                }

                Benchmark {
                    name,
                    schema: schema.expect("No schema for benchmark {name}"),
                    workloads,
                }
            })
            .collect())
    }

    /// Run all workloads in this benchmark
    fn run_benchmark(&self, c: &mut Criterion, args: &SystemBenchArgs) -> anyhow::Result<()> {
        if self.workloads.is_empty() {
            return Ok(());
        }

        println!("Preparing benchmark {}", self.name);

        let hdl = AdapterHandle::generate_data_and_start_adapter(&self.schema)?;
        let rt = tokio::runtime::Runtime::new()?;
        let pool_size = num_cpus::get_physical() * 4;
        let mut readyset_pool = rt.block_on(PreparedPool::try_new(pool_size, &readyset_url()))?;
        let mut upstream_pool = args
            .compare_upstream
            .then(|| rt.block_on(PreparedPool::try_new(pool_size, &mysql_url(DB_NAME))))
            .transpose()?;

        let mut group = c.benchmark_group(&self.name);
        group.confidence_level(0.995);
        group.measurement_time(WORKLOAD_DURATION);
        group.throughput(Throughput::Elements(BENCH_BATCH_SIZE));

        for workload in self.workloads.iter() {
            let workload_name = workload.file_stem().unwrap().to_string_lossy();

            println!("Preparing workload {}", workload_name);

            let bytes_before_workload = get_allocated_bytes()?;

            set_memory_limit_bytes(None)?;

            let workload = WorkloadSpec::from_yaml(&std::fs::read_to_string(workload)?)?;
            let query_set = rt.block_on(readyset_pool.prepare_pool_and_warm_cache(&workload))?;
            if let Some(upstream_pool) = &mut upstream_pool {
                rt.block_on(upstream_pool.prepare_pool_and_warm_cache(&workload))?;
            }

            let mut do_bench = |param: &str, pool: &mut PreparedPool| -> anyhow::Result<()> {
                // Will collect data until dropped, then report flamegraph for the last
                // `WORKLOAD_DURATION`
                let _perf = if args.flamegraph {
                    Some(hdl.get_flamegraph(
                        &format!("{workload_name}_{param}"),
                        WORKLOAD_DURATION,
                        args.merge_threads,
                        args.line_granularity,
                    ))
                } else {
                    None
                };

                reset_metrics()?;

                group.bench_with_input(
                    BenchmarkId::new(workload_name.clone(), param),
                    &(&Mutex::new(pool), &query_set),
                    |b, (pool, query_set)| {
                        b.to_async(&rt).iter_batched(
                            || {
                                let mut commands = Vec::new();
                                for _ in 0..BENCH_BATCH_SIZE {
                                    let query = query_set.get_query();
                                    let params = query.get_params();
                                    commands.push((query.idx, params));
                                }
                                commands
                            },
                            |commands| async move {
                                let mut pool = pool.lock().await;
                                pool.run_all(Arc::new(commands)).await.unwrap();
                            },
                            BatchSize::SmallInput,
                        )
                    },
                );

                println!("Memory usage: {:.2} MiB", get_allocated_mib()?);
                println!("Cache hit rate {:.2}%", get_cache_hit_ratio()? * 100.);

                Ok(())
            };

            if let Some(upstream_pool) = &mut upstream_pool {
                do_bench("upstream", upstream_pool)?;
            }
            do_bench("no_memory_limit", &mut readyset_pool)?;

            let bytes_after_workload = get_allocated_bytes()?;
            assert!(bytes_after_workload > bytes_before_workload);
            let bytes_used = bytes_after_workload - bytes_before_workload;

            for memory_limit in args.memory_limit.iter() {
                let param = match memory_limit {
                    MemoryLimit::Relative { percent } => {
                        // The way percentage memory limit works is by getting the memory usage
                        // before the benchmark begins, this gives us a minimal baseline. Then we
                        // will compute how much *additional* memory the benchmark requires to be
                        // fully cached, and add the percantage of *that* to the baseline.
                        set_memory_limit_bytes(Some(
                            bytes_before_workload + bytes_used * percent / 100,
                        ))?;
                        format!("memory_limit_{percent}%")
                    }
                    MemoryLimit::Absolute { mib } => {
                        set_memory_limit_bytes(Some(mib * 1024 * 1024))?;
                        format!("memory_limit_{mib}MiB")
                    }
                };

                do_bench(&param, &mut readyset_pool)?;
            }

            rt.block_on(drop_cached_queries())?;
        }

        group.finish();

        // Required for subsequent benchmarks to properly get all cores and not incrimentally fewer
        unset_affinity();

        Ok(())
    }
}

fn get_allocated_bytes() -> anyhow::Result<usize> {
    Ok(bincode::deserialize::<Option<usize>>(
        &reqwest::blocking::get(rpc_url("allocated_bytes"))?.bytes()?[..],
    )?
    .unwrap_or(0))
}

fn get_allocated_mib() -> anyhow::Result<f64> {
    Ok(get_allocated_bytes()? as f64 / 1024. / 1024.)
}

fn get_metrics() -> anyhow::Result<MetricsDump> {
    let client = reqwest::blocking::Client::new();
    let body = client.post(rpc_url("metrics_dump")).send()?.bytes()?;
    Ok(serde_json::from_slice(&body[..])?)
}

fn get_cache_hit_ratio() -> anyhow::Result<f64> {
    let metrics = get_metrics()?;

    let hit = match get_metric!(metrics, recorded::SERVER_VIEW_QUERY_HIT).unwrap() {
        readyset_client::metrics::DumpedMetricValue::Counter(hit) => hit,
        _ => unreachable!(),
    };

    let miss = match get_metric!(metrics, recorded::SERVER_VIEW_QUERY_MISS).unwrap() {
        readyset_client::metrics::DumpedMetricValue::Counter(miss) => miss,
        _ => unreachable!(),
    };

    Ok(hit / (hit + miss))
}

fn reset_metrics() -> anyhow::Result<()> {
    let client = reqwest::blocking::Client::new();
    client.post(rpc_url("reset_metrics")).send()?;
    Ok(())
}

fn set_memory_limit_bytes(limit: Option<usize>) -> anyhow::Result<()> {
    let client = reqwest::blocking::Client::new();
    client
        .post(rpc_url("set_memory_limit"))
        .body(bincode::serialize(&(Some(Duration::from_secs(1)), limit))?)
        .send()?;
    Ok(())
}

struct AdapterHandle {
    pid: i32,
    write_hdl: UnixStream,
}

impl Drop for AdapterHandle {
    fn drop(&mut self) {
        drop(self.write_hdl.write(&[1]));
    }
}

struct PerfHandle {
    join_handle: Option<std::thread::JoinHandle<()>>,
}

impl AdapterHandle {
    /// Begin not-perf profiling of the ReadySet adapter, generating a flamegraph of the
    /// `last_seconds` seconds before profiling is stopped by dropping the handle, thus only
    /// reporting the benchmarked period.
    fn get_flamegraph(
        &self,
        name: &str,
        last_seconds: Duration,
        merge_threads: bool,
        line_granularity: bool,
    ) -> PerfHandle {
        let pid = self.pid;
        let name = name.to_string();
        let handle = std::thread::spawn(move || {
            let nperf_args = RecordArgs::from_iter([
                "nperf",
                "-p",
                &format!("{pid}"),
                "-o",
                &format!("{name}.perf"),
            ]);

            let start_time = Instant::now();
            if let Ok(()) = nperf_core::cmd_record::main(nperf_args) {
                let from = start_time
                    .elapsed()
                    .checked_sub(last_seconds)
                    .unwrap_or_default()
                    .as_secs();

                let mut args = vec![
                    "flame".to_string(),
                    format!("{name}.perf"),
                    "--from".to_string(),
                    format!("{from}"),
                    "-o".to_string(),
                    format!("{name}.svg"),
                ];

                if merge_threads {
                    args.push("--merge-threads".to_string());
                }

                if line_granularity {
                    args.push("--granularity".to_string());
                    args.push("line".to_string());
                }

                let flamegraph_args = FlamegraphArgs::from_iter(args);

                if nperf_core::cmd_flamegraph::main(flamegraph_args).is_err() {
                    println!("flamegraph failed");
                }

                drop(std::fs::remove_file(format!("{name}.perf")));
            } else {
                println!("nperf failed");
            }
        });

        PerfHandle {
            join_handle: Some(handle),
        }
    }

    /// Returns a write handle that we can write anything to to indicate benchmarks is done
    fn generate_data_and_start_adapter<P: Into<PathBuf>>(schema: P) -> anyhow::Result<Self> {
        let (mut sock1, mut sock2) = UnixStream::pair()?;
        // A word of warning: DO NOT CREATE A RUNTIME BEFORE FORKING, IT *WILL* MESS WITH TOKIO
        match fork().unwrap() {
            Fork::Child => {
                // We don't want the benchmarking process and the server to share CPU cores, to
                // reduce noise, therefore we schedule the processes to different
                // CPU cores alltogether
                set_cpu_affinity(true);
                drop(sock2);
                sock1.read_exact(&mut [0u8])?;
                std::thread::spawn(start_adapter);
                sock1.read_exact(&mut [0u8])?;
                std::process::exit(0);
            }
            Fork::Parent(child_pid) => {
                set_cpu_affinity(false);
                drop(sock1);

                let rt = tokio::runtime::Runtime::new()?;
                rt.block_on(prepare_db(schema))?;

                // Write a byte to indicate database is ready and fork can initiate replication
                sock2.write_all(&[1u8]).unwrap();

                rt.block_on(wait_adapter_ready());
                Ok(AdapterHandle {
                    pid: child_pid,
                    write_hdl: sock2,
                })
            }
        }
    }
}

impl Drop for PerfHandle {
    fn drop(&mut self) {
        // Send SIGINT to self in order to stop the collection
        unsafe { libc::kill(std::process::id() as _, libc::SIGINT) };
        self.join_handle.take().unwrap().join().unwrap();
    }
}

/// Creates a new database for benchmarking, installs the given schema and generates data for it
async fn prepare_db<P: Into<PathBuf>>(path: P) -> anyhow::Result<()> {
    let generator = DataGenerator::new(path);
    let mut conn = DatabaseURL::from_str(&mysql_url(""))?.connect().await?;
    conn.query_drop(format!("DROP DATABASE IF EXISTS {DB_NAME}"))
        .await?;
    conn.query_drop(format!("CREATE DATABASE {DB_NAME}"))
        .await?;
    drop(conn);
    let conn_str = &mysql_url(DB_NAME);
    generator.install(conn_str).await?;
    generator.generate(conn_str).await?;
    Ok(())
}

/// Start the ReadySet MySQL adapter in standalone mode without fallback cache enabled.
#[cfg(not(feature = "fallback_cache"))]
fn start_adapter() {
    start_adapter_with_options(FallbackCacheOptions {
        enable: false,
        disk_modeled: false,
    })
}

/// Start the ReadySet MySQL adapter in standalone mode with fallback cache enabled and disk
/// modeling disabled.
#[cfg(all(feature = "fallback_cache", not(feature = "disk_modeled")))]
fn start_adapter() {
    start_adapter_with_options(FallbackCacheOptions {
        enable: true,
        disk_modeled: false,
    })
}

/// Start the ReadySet MySQL adapter in standalone mode with fallback cache enabled and disk
/// modeling enabled.
#[cfg(all(feature = "fallback_cache", feature = "disk_modeled"))]
fn start_adapter() {
    start_adapter_with_options(FallbackCacheOptions {
        enable: true,
        disk_modeled: true,
    })
}

/// Various options for enabling and configuring the fallback cache.
struct FallbackCacheOptions {
    /// Whether the fallback cache is enabled or not.
    enable: bool,
    /// Whether the fallback cache should model running off spinning disk.
    disk_modeled: bool,
}

/// Start the ReadySet MySQL adapter in standalone mode with options.
fn start_adapter_with_options(fallback_cache_options: FallbackCacheOptions) {
    let temp_dir = temp_dir::TempDir::new().unwrap();
    let mysql_url = mysql_url(DB_NAME);
    let mut options = vec![
        "bench", // This is equivalent to the program name in argv, ignored
        "--deployment",
        DB_NAME,
        "--standalone",
        "--allow-unauthenticated-connections",
        "--upstream-db-url",
        mysql_url.as_str(),
        "--durability",
        "ephemeral",
        "--authority",
        "standalone",
        "--authority-address",
        temp_dir.path().to_str().unwrap(),
        "--log-level",
        "error",
        "--eviction-policy",
        "lru",
        "--noria-metrics",
        "--database-type=mysql",
    ];

    if fallback_cache_options.enable {
        options.push("--enable-fallback-cache");
    }

    if fallback_cache_options.disk_modeled {
        options.push("--model-disk");
    }

    let adapter_options = Options::parse_from(options);

    let mut adapter = NoriaAdapter {
        description: "ReadySet benchmark adapter",
        default_address: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), BENCHMARK_PORT),
        connection_handler: MySqlHandler {
            enable_statement_logging: false,
        },
        database_type: DatabaseType::MySQL,
        parse_dialect: nom_sql::Dialect::MySQL,
        expr_dialect: readyset_data::Dialect::DEFAULT_MYSQL,
    };

    adapter.run(adapter_options).unwrap();
}

/// Wait for replication to finish by lazy looping over "SHOW READYSET STATUS"
async fn wait_adapter_ready() {
    // First attempt to connect to the readyset adapter at all
    let mut conn = loop {
        match Conn::from_url(readyset_url()).await {
            Ok(conn) => break conn,
            _ => sleep(Duration::from_secs(1)).await,
        }
    };

    // Then query status until snaphot is completed
    let q = nom_sql::ShowStatement::ReadySetStatus;
    loop {
        let status: Result<Vec<(String, String)>, _> = conn.query(q.to_string()).await;
        match status {
            Ok(data)
                if data
                    .iter()
                    .any(|(s, v)| s == "Snapshot Status" && v == "Completed") =>
            {
                return
            }
            _ => sleep(Duration::from_secs(1)).await,
        }
    }
}

/// Drop all currently cached queries
async fn drop_cached_queries() -> anyhow::Result<()> {
    let mut conn = Conn::from_url(readyset_url()).await?;
    conn.query_drop(nom_sql::DropAllCachesStatement {}.to_string())
        .await?;
    Ok(())
}

// This will properly work only if there is no hyperthreading, or with 2way-SMT
fn set_cpu_affinity(for_adapter: bool) {
    let physical_cpus = num_cpus::get_physical();
    let logical_cpus = num_cpus::get();
    let pid = std::process::id();

    // We use ~2/3 of the cores for the adapter and the remaining for the benchmark process
    let adapter_cores = physical_cpus * 2 / 3;

    let cpu_list = if logical_cpus == physical_cpus {
        if for_adapter {
            // When SMT is not present the list is just all the cores from 0 and up for the adapter
            format!("0-{}", adapter_cores - 1)
        } else {
            // And the remaining ones for the benchmark
            format!("{}-{}", adapter_cores, physical_cpus - 1)
        }
    } else if for_adapter {
        // With SMT the logical cores follow the physical cores, so the first N cores are
        // physical and the remaining N cores are logical
        format!(
            "0-{},{}-{}",
            adapter_cores - 1,
            physical_cpus,
            physical_cpus + adapter_cores - 1
        )
    } else {
        format!(
            "{}-{},{}-{}",
            adapter_cores,
            physical_cpus - 1,
            physical_cpus + adapter_cores,
            logical_cpus - 1
        )
    };

    std::process::Command::new("taskset")
        .arg("-p")
        .arg("--cpu-list")
        .arg(cpu_list)
        .arg(pid.to_string())
        .output()
        .expect("failed to execute process");
}

fn unset_affinity() {
    let pid = std::process::id();
    std::process::Command::new("taskset")
        .arg("-p")
        .arg("--cpu-list")
        .arg("0-255")
        .arg(pid.to_string())
        .output()
        .expect("failed to execute process");
}

#[derive(Debug)]
enum MemoryLimit {
    Relative { percent: usize },
    Absolute { mib: usize },
}

impl FromStr for MemoryLimit {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some(percent) = s.strip_suffix('%') {
            let percent: usize = percent.parse()?;
            if percent == 0 || percent >= 100 {
                Err(anyhow::Error::msg("Expected 0 < percentage < 100"))?;
            }
            Ok(MemoryLimit::Relative { percent })
        } else {
            Ok(MemoryLimit::Absolute { mib: s.parse()? })
        }
    }
}

#[derive(Parser, Debug)]
struct SystemBenchArgs {
    /// If specified, only run benches containing this string in their names
    // This argument is the first argument passed by `cargo bench`
    #[clap(index(1))]
    benchname: Option<String>,
    /// If specified collect a flamegraph for each workload
    #[clap(long)]
    flamegraph: bool,
    /// If specified will merge flamegraph callstacks from all threads
    #[clap(long, requires("flamegraph"))]
    merge_threads: bool,
    /// If specified will collect the flamegraph with a line granlarity
    #[clap(long, requires("flamegraph"))]
    line_granularity: bool,
    /// Repeat each workload again but with the memory limit enabled, multiple memory limits can be
    /// provided, a memory limit is either an absolute value in MiB, or a relative percentage
    /// value, such as 90%, where the benchmark will compute the memory limit based on peak memory
    /// usage.
    #[clap(long, short)]
    memory_limit: Vec<MemoryLimit>,
    /// Names an explicit baseline and enables overwriting the previous results.
    #[clap(long)]
    save_baseline: Option<String>,
    /// Compare all benchmark results against the upstream database as well
    #[clap(long)]
    compare_upstream: bool,

    #[clap(long, hide(true))]
    /// Is present when executed with `cargo bench`
    bench: bool,
    #[clap(long, hide(true))]
    /// Is present when executed with `cargo test`
    test: bool,
}

fn main() -> anyhow::Result<()> {
    let mut args = SystemBenchArgs::parse();

    if args.test {
        // Move along citizen, no tests here
        return Ok(());
    }

    let filter = Regex::new(args.benchname.as_deref().unwrap_or(".*"))?;

    let mut criterion = Criterion::default();
    if let Some(baseline) = args.save_baseline.take() {
        criterion = criterion.save_baseline(baseline);
    }

    let benchmarks = Benchmark::find_benchmarks(filter)?;

    for benchmark in benchmarks {
        benchmark.run_benchmark(&mut criterion, &args)?;
    }

    criterion.final_summary();

    Ok(())
}
