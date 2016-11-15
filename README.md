# Taster: automatic performance regression tests for Rust

Still under development.

## How to use

Add a file called `taster.toml` to the root of your crate's GitHub repo, and
configure benchmarks in it:

```
[my-first-benchmark]
# that's "cargo bench"
command = "bench"
args = ["--bench", "bench", "--", "--some-flag", "--another-flag=42"]
regexs = ["([0-9.]+ req/sec)", "(latency [0-9.]+ms)"]

[my-second-benchmark]
# that's "cargo run"
command = "run"
args = ["--bin", "standalone_benchmark", "--", "--some-flag"]
regexs = ["([0-9.]+ req/sec)", "(latency [0-9.]+ms)"]

```

Then configure a GitHub webhook for taster that delivers notifications for push
events, and start taster:

```
cargo run -- \
    -w <WORKSPACE>
    -l <LISTEN_IP:PORT> \
    --github_repo="https://github.com/my/repo \
    --slack_hook_url="<SLACK_HOOK>" \
    --slack_channel="#chan"
```

Only `-w` and `--github_repo` are mandatory parameters.
