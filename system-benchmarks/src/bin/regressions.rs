//! This executable scrapes the nightly builds for systems-benchmark artifacts and plots them on a
//! nice chart so that big performance changes are visible.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::str::FromStr;

use clap::Parser;
use database_utils::{DatabaseConnection, DatabaseType, DatabaseURL, QueryableConnection};
use readyset_data::DfValue;
use serde::{Deserialize, Serialize};

// How many build at most to check
const MAX_BUILDS_TO_CHECK: usize = 150;
// How many build to actually plot
const MAX_BUILDS_TO_PLOT: usize = 50;

const TEMPLATE: &str = include_str!("regressions.html.template");

const HTML_COLORS: &[&str] = &[
    "Blue", "Red", "Green", "Black", "Magenta", "Cyan", "Gray", "Maroon", "Khaki", "Orange",
    "Violet", "DarkGrey", "Lime", "Navy",
];

const HTML_DASH_STYLES: &[&str] = &["[1, 0]", "[1, 2]", "[5, 5]", "[8, 3]"];

/// An artifact description, as described in:
/// https://buildkite.com/docs/apis/rest-api/artifacts#list-artifacts-for-a-job
#[allow(dead_code)]
#[derive(Deserialize, Debug)]
struct JobArtifactDescriptor {
    id: String,
    job_id: String,
    url: String,
    download_url: String,
    state: String,
    path: String,
    dirname: String,
    filename: String,
    mime_type: String,
    file_size: usize,
    glob_path: Option<String>,
    original_path: Option<String>,
    sha1sum: String,
}

#[derive(Deserialize, Debug)]
struct BuildDescriptor {
    branch: String,
    commit: String,
}

/// Those are copied from https://github.com/BurntSushi/critcmp/blob/master/src/data.rs
/// and are required in order to properly deserialize a json encoded criterion benchmark
/// result.
#[derive(Deserialize, Debug)]
pub struct BaseBenchmarks {
    pub name: String,
    pub benchmarks: HashMap<String, Benchmark>,
}

#[derive(Deserialize, Debug)]
pub struct Benchmark {
    pub baseline: String,
    pub fullname: String,
    #[serde(rename = "criterion_benchmark_v1")]
    pub info: CBenchmark,
    #[serde(rename = "criterion_estimates_v1")]
    pub estimates: CEstimates,
}

#[derive(Deserialize, Debug)]
pub struct CBenchmark {
    pub group_id: String,
    pub function_id: Option<String>,
    pub value_str: Option<String>,
    pub throughput: Option<CThroughput>,
    pub full_id: String,
    pub directory_name: String,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "PascalCase")]
pub struct CThroughput {
    pub bytes: Option<u64>,
    pub elements: Option<u64>,
}

#[derive(Deserialize, Debug)]
pub struct CEstimates {
    pub mean: CStats,
    pub median: CStats,
    pub median_abs_dev: CStats,
    pub slope: Option<CStats>,
    pub std_dev: CStats,
}

#[derive(Deserialize, Debug)]
pub struct CStats {
    pub confidence_interval: CConfidenceInterval,
    pub point_estimate: f64,
    pub standard_error: f64,
}

#[derive(Deserialize, Debug)]
pub struct CConfidenceInterval {
    pub confidence_level: f64,
    pub lower_bound: f64,
    pub upper_bound: f64,
}

/// Find the artifact descriptor for a bench, if present for that build number.
async fn get_bench_descriptor(
    client: &reqwest::Client,
    token: &str,
    build: usize,
    database_type: DatabaseType,
) -> anyhow::Result<Option<JobArtifactDescriptor>> {
    // For each job, request the list of artifacts for that job:
    // https://buildkite.com/docs/apis/rest-api/artifacts#list-artifacts-for-a-job
    let url = format!("https://api.buildkite.com/v2/organizations/readyset/pipelines/readyset-nightly/builds/{build}/artifacts");
    let response = client.get(url).bearer_auth(token).send().await?;
    let artifact_list = response.text().await?;
    let artifact_list = serde_json::from_str::<Vec<JobArtifactDescriptor>>(&artifact_list)?;
    Ok(artifact_list.into_iter().find(|a| {
        // For backwards compatibility, MySQL files do not have a "-mysql" suffix
        match database_type {
            DatabaseType::MySQL => a.filename == format!("{build}-bench.json"),
            DatabaseType::PostgreSQL => a.filename == format!("{build}-bench-postgresql.json"),
            DatabaseType::Vitess => a.filename == format!("{build}-bench-vitess.json"),
        }
    }))
}

async fn get_build_descriptor(
    client: &reqwest::Client,
    token: &str,
    build: usize,
) -> anyhow::Result<BuildDescriptor> {
    // For each build check if it is the main branch
    // https://buildkite.com/docs/apis/rest-api/builds#get-a-build
    let url = format!("https://api.buildkite.com/v2/organizations/readyset/pipelines/readyset-nightly/builds/{build}");
    let response = client.get(url).bearer_auth(token).send().await?;
    let build_desc = response.text().await?;
    let build_desc = serde_json::from_str::<BuildDescriptor>(&build_desc)?;
    Ok(build_desc)
}

/// Download the json file for the benchmark and parse it
async fn get_bench(
    client: &reqwest::Client,
    token: &str,
    desc: &JobArtifactDescriptor,
) -> anyhow::Result<HashMap<String, Benchmark>> {
    let url = desc.download_url.as_str();
    let response = client.get(url).bearer_auth(token).send().await?;
    let benchmark_json = response.text().await?;
    Ok(serde_json::from_str::<BaseBenchmarks>(&benchmark_json)?.benchmarks)
}

#[derive(Serialize, Deserialize)]
pub struct BenchmarkResults {
    mean_tps: f64,
}

/// Upload results to either a mysql or postgres database. Results are a map of the build number to
/// the mean throughput.
async fn upload_results_to_database(
    client: &mut DatabaseConnection,
    commit: &str,
    results: &BTreeMap<String, DataSet>,
) -> anyhow::Result<()> {
    // Create the table if it doesn't already exist.
    let ddl = std::fs::read_to_string("regressions.sql")?;
    client.query_drop(&ddl).await?;

    let query = r"
    INSERT INTO benchmark_data (buildkite_commit, name, build, execution_end_time, data) VALUES
    ($1, $2, $3, NOW(), $4)"
        .to_string();
    for (
        name,
        DataSet {
            label: _,
            data,
            border_color: _,
            border_dash: _,
        },
    ) in results
    {
        for (build, mean_tps) in data {
            let data = BenchmarkResults {
                mean_tps: *mean_tps,
            };
            let params: Vec<DfValue> = vec![
                commit.into(),
                name.clone().into(),
                build.clone().into(),
                serde_json::to_value(&data)?.into(),
            ];
            client.execute(&query, params).await?;
        }
    }

    Ok(())
}

fn color_and_dash_for_idx(idx: usize) -> (&'static str, &'static str) {
    (
        HTML_COLORS[idx % HTML_COLORS.len()],
        HTML_DASH_STYLES[idx / HTML_COLORS.len()],
    )
}

#[derive(Debug, Clone)]
struct DataSet {
    label: String,
    data: BTreeMap<String, f64>,
    border_color: &'static str,
    border_dash: &'static str,
}

#[derive(Parser, Debug)]
struct RegressionsArgs {
    /// The type of the database for which we'd like to plot regressions
    #[clap(long)]
    database_type: DatabaseType,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = RegressionsArgs::parse();
    let this_build: usize = std::env::var("BUILD_NUMBER").expect("Need build").parse()?;
    let access_token = std::env::var("ACCESS_TOKEN").expect("Need token");

    // We wait to return an error in the case of an invalid database url, because there is still
    // value in running the benchmarks and writing out results to an html file. After writing the
    // html file out, we return the error.
    let db_url = std::env::var("DATABASE_URL")
        .ok()
        .map(|url| DatabaseURL::from_str(&url));

    let client = reqwest::Client::new();

    let mut labels = VecDeque::new();
    let mut datasets: BTreeMap<String, DataSet> = BTreeMap::new();
    let mut memory_limits: HashSet<String> = HashSet::new();

    let mut commit = "".to_string();
    for build in (this_build - MAX_BUILDS_TO_CHECK..=this_build).rev() {
        if labels.len() >= MAX_BUILDS_TO_PLOT {
            break;
        }

        let checked_build = get_build_descriptor(&client, &access_token, build).await?;

        if build != this_build && checked_build.branch != "refs/heads/main" {
            // Skip comparison with non-main branches
            continue;
        }

        if let Some(bench_artifact) =
            get_bench_descriptor(&client, &access_token, build, args.database_type).await?
        {
            let benchmarks_for_build = get_bench(&client, &access_token, &bench_artifact).await?;
            if checked_build.commit == commit {
                // Skip build with the same commit we already checked.
                continue;
            }
            commit = checked_build.commit;

            labels.push_front(build.to_string());

            for (name, results) in benchmarks_for_build {
                memory_limits.insert(results.info.value_str.unwrap_or_default());

                // Multiply the mean estimates by the benchmark batch size to arrive at a mean
                // throughput.
                let mean_tpt = 8192. * 1_000_000_000.0 / results.estimates.mean.point_estimate;

                let next_ds = datasets.len();

                datasets
                    .entry(name.clone())
                    .or_insert_with(|| {
                        let (border_color, border_dash) = color_and_dash_for_idx(next_ds);
                        DataSet {
                            label: name,
                            data: BTreeMap::new(),
                            border_color,
                            border_dash,
                        }
                    })
                    .data
                    .insert(build.to_string(), mean_tpt);
            }
        }
    }

    let label_string = itertools::join(labels.iter().map(|l| format!("'{l}'")), ",");
    let dataset_string = itertools::join(
        datasets.clone().into_values().map(
            |DataSet {
                 label,
                 border_color,
                 border_dash,
                 data,
             }| {
                let data_string = itertools::join(
                    labels.iter().map(|l| match data.get(l) {
                        Some(data) => format!("{data:.2}"),
                        None => "null".to_string(),
                    }),
                    ",",
                );

                format!(
                    "{{
                         label: '{label}',
                         borderColor: '{border_color}',
                         borderDash: {border_dash},
                         data: [{data_string}],
                       }}"
                )
            },
        ),
        ",",
    );

    let mem_limits_string = itertools::join(
        memory_limits
            .into_iter()
            .map(|l| format!("<option value=\"{l}\">{l}</option>")),
        "",
    );

    std::fs::write(
        format!("regressions-{}.html", args.database_type),
        TEMPLATE
            .replace("$DATABASE_TYPE$", &args.database_type.to_string())
            .replace("$LABELS$", &label_string)
            .replace("$MEMORY_LIMITS$", &mem_limits_string)
            .replace("$DATASETS$", &dataset_string),
    )?;

    if let Some(res) = db_url {
        let url = res?;
        let mut conn = url.connect(None).await?;
        upload_results_to_database(&mut conn, &commit, &datasets).await?;
    }

    Ok(())
}
