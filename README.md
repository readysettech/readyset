# Taster: automatic performance regression tests for Rust

Taster automatically tests commits to a project written in Rust and informs a
configurable Slack channel of any failures or performance regressions.

## How to use

Add a file called `taster.toml` to the root of your crate's GitHub repo, and
configure benchmarks in it:

```
version = 2  # current taster config version, for backwards compatibility

[my-first-benchmark]
command = "cargo"
args = ["bench", "--bench", "bench", "--", "--some-flag", "--another-flag=42"]
regexs = ["(throughput): ([0-9.]+) req/sec", "(latency): ([0-9.]+)ms"]

[my-second-benchmark]
command = "cargo"
args = ["run", "--bin", "standalone_benchmark", "--", "--some-flag"]
regexs = ["(throughput): ([0-9.]+) req/sec", "(latency): ([0-9.]+)ms"]

```
Each regular expression must have one or two capture groups:
 1. The name of the metric being measured (a string; optional)
 2. The value of the benchmark result (an integer or floating point number;
    required).
If only one capture group is found, Taster assumes that it contains a number
corresponding to the benchmark result.

Finally, configure a GitHub webhook for taster that delivers notifications for
push events, and start taster:

```
cargo run -- \
    --workdir /some/workspace/dir \
    --listen_addr 127.0.0.1:4567 \
    --github_repo "https://github.com/my/repo" \
    --secret "my_secret" \
    --github_api_key "123key" \
    --slack_hook_url <SLACK_HOOK_URL> \
    --slack_channel "#chan"
```

Only `--workdir`, `--github_repo`, and `--secret` are mandatory parameters.
