# readyset-dante-oracle

A differential-testing harness that drives the [`dante`](../dante/) SQL
generator against two database endpoints — an upstream reference engine
(MySQL or PostgreSQL) and a Readyset instance connected to that upstream —
and reports value-level divergences. Each iteration generates a query,
creates any tables it needs, seeds them with data, runs the query against
both endpoints, and diffs the result sets.

The endpoint under `--readyset-url` can be a real Readyset instance (pass
`--readyset-mode`) or a transparent proxy such as SQP (the default). If you
are writing or improving SQL patterns, see
[`public/dante/README.md`](../dante/README.md). This document is about
running the oracle.

## Contents

1. [Quickstart](#quickstart)
2. [How it works](#how-it-works)
3. [Build](#build)
4. [Prerequisites](#prerequisites)
5. [Running against MySQL](#running-against-mysql)
6. [Running against PostgreSQL](#running-against-postgresql)
7. [CLI reference](#cli-reference)
8. [Interpreting output](#interpreting-output)
9. [Targeted testing](#targeted-testing)
10. [Reproduction capture](#reproduction-capture)
11. [Antithesis assertion analysis](#antithesis-assertion-analysis)
12. [Multi-seed runs](#multi-seed-runs)
13. [Cleanup](#cleanup)

## Quickstart

One self-contained MySQL session against a local Readyset. Copy, paste,
watch the final line for `mismatched=0`.

```bash
cd public/

# Build both binaries (once).
cargo build --bin readyset --bin readyset-dante-oracle --features antithesis_sdk/full

# Fresh database and storage.
mysql -h 127.0.0.1 -P 3306 -u root -pnoria \
  -e "DROP DATABASE IF EXISTS cfuzz; CREATE DATABASE cfuzz;"
rm -rf /tmp/rs-cfuzz.db

# Start Readyset on port 3307. Every FEATURE_* must be on (see Feature flags).
FEATURE_FULL_MATERIALIZATION=true FEATURE_MATERIALIZATION_PERSISTENCE=true \
FEATURE_MIXED_COMPARISONS=true FEATURE_NON_BLOCKING_INDEX_BUILD=true \
FEATURE_PAGINATION=true FEATURE_PLACEHOLDER_INLINING=true \
FEATURE_POST_LOOKUP=true FEATURE_STRADDLED_JOINS=true FEATURE_TOPK=true \
LOG_LEVEL=error target/debug/readyset \
  --upstream-db-url mysql://root:noria@127.0.0.1:3306/cfuzz \
  --address 0.0.0.0:3307 --storage-dir /tmp/rs-cfuzz.db \
  --cache-mode deep-then-shallow --query-caching in-request-path &
sleep 8

# Run the oracle against that Readyset.
target/debug/readyset-dante-oracle \
  --readyset-mode \
  --compare-to mysql://root:noria@127.0.0.1:3306/cfuzz \
  --readyset-url mysql://root:noria@127.0.0.1:3307/cfuzz \
  --seed 42 --max-queries 200 --rows-per-table 50
```

`--readyset-mode` tells the oracle the endpoint is a real Readyset
instance, so it creates caches and runs the EXPLAIN probes. Drop it only
when the endpoint is a transparent proxy (e.g. SQP). For PostgreSQL,
per-flag detail, or targeted feature runs, read on.

## How it works

Each iteration of the inner loop:

1. **Pick a pattern.** `Generator` draws one pattern from the
   `ConstraintRegistry` according to the active `SelectionFilter` (see
   [Targeted testing](#targeted-testing)) and the running dialect's
   `DialectSupport`.
2. **Compose (optional).** Unless `--single-pattern` is set, the pattern is
   combined with zero or more partner patterns to form a deeper query.
3. **Resolve.** The resolver binds variables to concrete tables, columns,
   and parameters, producing a `QueryOutput { query, ddl, params,
   examples }`.
4. **Apply DDL.** `CREATE TABLE` statements are executed on the upstream;
   Readyset picks them up via replication.
5. **Insert data.** One row per `Constraint::Example` (using its row
   overrides) plus `--rows-per-table` random-fill rows.
6. **Issue a `CREATE DEEP CACHE`** for the query (only under
   `--readyset-mode`; without it the endpoint is treated as a transparent
   proxy and decides its own routing).
7. **Execute the query.** Once with random-fill parameters, then once per
   example with that example's parameter overrides. Each result is
   compared to the upstream's response for the same parameters.
8. **Report.** Divergences are logged at `ERROR` level (with the
   triggering example's note, if any) and counted into the per-pattern
   coverage summary printed at run end.

Examples (`Constraint::Example`) are the way a pattern author pins a
specific input to surface a known bug class deterministically; see
[Targeted testing](#targeted-testing) and the pattern-authoring guide.

## Build

```bash
cd public/
cargo build --bin readyset --bin readyset-dante-oracle --features antithesis_sdk/full
```

The `antithesis_sdk/full` feature enables real assertion output. Without
it the SDK calls are no-ops and no JSONL is written, which is fine for
local development but means [the Antithesis assertion
analyzer](#antithesis-assertion-analysis) has nothing to chew on.

## Prerequisites

- Docker (the standard local dev setup uses `public/docker-compose.yml`
  for MySQL on 3306 and PostgreSQL on 5432, credentials `root`/`noria`
  and `postgres`/`noria`).
- A built `readyset` binary and `readyset-dante-oracle` binary (see
  [Build](#build)).
- Each run needs its **own fresh database** and its **own Readyset port**.
  Concurrent runs that share a database collide on table names.

### Feature flags

Always launch Readyset with **every** `FEATURE_*` flag set to `true`.
Disabled flags silently skip whole code paths (post-lookup aggregates,
TopK, pagination, straddled joins, mixed-type comparisons, online index
builds, full materialization, placeholder inlining), which both starves
the Antithesis assertion catalog and hides real divergences. The
canonical preamble for every Readyset launch in this document is:

```bash
FEATURE_FULL_MATERIALIZATION=true \
FEATURE_MATERIALIZATION_PERSISTENCE=true \
FEATURE_MIXED_COMPARISONS=true \
FEATURE_NON_BLOCKING_INDEX_BUILD=true \
FEATURE_PAGINATION=true \
FEATURE_PLACEHOLDER_INLINING=true \
FEATURE_POST_LOOKUP=true \
FEATURE_STRADDLED_JOINS=true \
FEATURE_TOPK=true \
```

Copy this block (or export the variables in your shell) in front of
every `target/debug/readyset ...` invocation. The examples below
already include it; if you copy from elsewhere, make sure you do too.

Sanity-check the upstream:

```bash
mysql -h 127.0.0.1 -P 3306 -u root -pnoria -e "SELECT 1"
# or
PGPASSWORD=noria psql -h 127.0.0.1 -p 5432 -U postgres -c "SELECT 1"
```

## Running against MySQL

### Step 1: Fresh database

```bash
mysql -h 127.0.0.1 -P 3306 -u root -pnoria \
  -e "DROP DATABASE IF EXISTS cfuzz_s42; CREATE DATABASE cfuzz_s42;"
```

### Step 2: Start Readyset on an unused port

```bash
mkdir -p /tmp/antithesis

ANTITHESIS_SDK_LOCAL_OUTPUT=/tmp/antithesis/antithesis-readyset-s42.jsonl \
LOG_LEVEL=error \
FEATURE_FULL_MATERIALIZATION=true \
FEATURE_MATERIALIZATION_PERSISTENCE=true \
FEATURE_MIXED_COMPARISONS=true \
FEATURE_NON_BLOCKING_INDEX_BUILD=true \
FEATURE_PAGINATION=true \
FEATURE_PLACEHOLDER_INLINING=true \
FEATURE_POST_LOOKUP=true \
FEATURE_STRADDLED_JOINS=true \
FEATURE_TOPK=true \
target/debug/readyset \
  --upstream-db-url mysql://root:noria@127.0.0.1:3306/cfuzz_s42 \
  --address 0.0.0.0:3307 \
  --storage-dir /tmp/rs-cfuzz-s42.db \
  --cache-mode deep-then-shallow \
  --query-caching in-request-path
```

Wait ~8 seconds for Readyset to snapshot the empty database, then verify:

```bash
mysql -h 127.0.0.1 -P 3307 -u root -pnoria cfuzz_s42 \
  -e "SHOW READYSET STATUS"
```

Look for `Status: Online`.

### Step 3: Run the oracle

In a separate terminal:

```bash
ANTITHESIS_SDK_LOCAL_OUTPUT=/tmp/antithesis/antithesis-logictest-s42.jsonl \
LOG_LEVEL=info \
target/debug/readyset-dante-oracle \
  --readyset-mode \
  --compare-to mysql://root:noria@127.0.0.1:3306/cfuzz_s42 \
  --readyset-url mysql://root:noria@127.0.0.1:3307/cfuzz_s42 \
  --seed 42 --max-queries 200 --rows-per-table 50
```

## Running against PostgreSQL

The dialect is auto-selected from the `postgresql://` URL prefix. Use a
distinct Readyset port (e.g. 5433) so the two endpoints are
distinguishable.

PostgreSQL's per-database replication slot survives Readyset crashes; an
orphaned slot from a prior run will block `DROP DATABASE`. Always drop
the slot before recreating.

```bash
PGPASSWORD=noria psql -h 127.0.0.1 -p 5432 -U postgres \
  -c "SELECT pg_drop_replication_slot('readyset')" || true
PGPASSWORD=noria psql -h 127.0.0.1 -p 5432 -U postgres \
  -c "DROP DATABASE IF EXISTS cfuzz_pg_test"
PGPASSWORD=noria psql -h 127.0.0.1 -p 5432 -U postgres \
  -c "CREATE DATABASE cfuzz_pg_test"
rm -rf /tmp/rs-cfuzz-pg-test.db

FEATURE_FULL_MATERIALIZATION=true \
FEATURE_MATERIALIZATION_PERSISTENCE=true \
FEATURE_MIXED_COMPARISONS=true \
FEATURE_NON_BLOCKING_INDEX_BUILD=true \
FEATURE_PAGINATION=true \
FEATURE_PLACEHOLDER_INLINING=true \
FEATURE_POST_LOOKUP=true \
FEATURE_STRADDLED_JOINS=true \
FEATURE_TOPK=true \
LOG_LEVEL=info \
target/debug/readyset \
    --upstream-db-url postgresql://postgres:noria@127.0.0.1:5432/cfuzz_pg_test \
    --address 0.0.0.0:5433 \
    --storage-dir /tmp/rs-cfuzz-pg-test.db \
    --cache-mode deep-then-shallow --query-caching in-request-path &

target/debug/readyset-dante-oracle \
    --readyset-mode \
    --compare-to postgresql://postgres:noria@127.0.0.1:5432/cfuzz_pg_test \
    --readyset-url postgresql://postgres:noria@127.0.0.1:5433/cfuzz_pg_test \
    --seed 142 --max-queries 500 --rows-per-table 50 \
    --dump-repro /tmp/dante-pg-soak/repro.sql
```

To find a backgrounded Readyset for cleanup:

```bash
pgrep -fl 'target/debug/readyset --upstream-db-url postgresql'
```

## CLI reference

| Argument | Default | Description |
|---|---|---|
| `--max-queries` | `1000` | Hard upper bound on queries per run. A fallback so a run can never spin forever; the coin flip is the intended stop, so this rarely binds. |
| `--continue-probability` | `0.97` | After each query, continue with this probability, otherwise stop. The realized query count is geometric (expected ~33, median ~23), giving variable-length runs. |
| `--rows-per-table` | `100` | Random-fill rows inserted per new table. Examples add one row each on top. |
| `--seed` | random | Fixed RNG seed for deterministic replay. Pair with the same `--seed` and unchanged registry for byte-identical repros. |
| `--antithesis-entropy` | off | Use the Antithesis entropy source instead of a seed or system RNG. Only useful inside the Antithesis sandbox. |
| `--compare-to` | required | Upstream URL (`mysql://` or `postgresql://`). Dialect is inferred. |
| `--readyset-url` | required | URL of the Readyset endpoint to test. |
| `--db-op-timeout-secs` | `30` | Per-operation timeout for every DB call. Prevents fault injection from stalling a run indefinitely. |
| `--dump-repro <path>` | unset | Stream a replayable SQL script to `<path>`. See [Reproduction capture](#reproduction-capture). |
| `--required-tags <tag,..>` | unset | Restrict pattern selection to patterns carrying **all** of these tags. See [Targeted testing](#targeted-testing). |
| `--excluded-tags <tag,..>` | unset | Drop patterns carrying **any** of these tags. |
| `--single-pattern` | off | One pattern per iteration, no composition partners. Test a feature in isolation. See [Targeted testing](#targeted-testing). |
| `--keep-going` | off | Continue past non-fatal terminal SELECT errors (`ErrorClass::Other`) instead of aborting. Each is logged, surfaced as an Antithesis assertion failure, and counted as `errored`. A closed connection (`ErrorClass::Fatal`) still bails. Use for full-coverage soak runs that would otherwise stop at the first novel bug. |
| `--readyset-mode` | off | Treat `--readyset-url` as a real Readyset instance: apply the deep-cache compatibility rules, `CREATE DEEP CACHE` per query, run the EXPLAIN probes, and wait on `SHOW READYSET TABLES`. Off by default, the endpoint is a transparent proxy (e.g. SQP) and none of those run. See [Targeted testing](#targeted-testing). |

## Interpreting output

At the end of every run the harness prints a per-pattern coverage table
followed by aggregates at `INFO` level:

```
coverage totals  patterns_hit=184  patterns_total=201  total_generated=200  total_matched=198  total_mismatched=0
readyset-dante-oracle complete  matched=198  mismatched=0
```

- `matched` — upstream and Readyset returned identical results (with
  example-targeted probes counted independently of the random probe).
- `mismatched` — at least one example or random probe diverged. Each
  divergence is logged at `ERROR` level.
- `skipped` — non-deterministic queries (e.g. `LIMIT` without
  `ORDER BY`).

Individual mismatches log the SQL with parameters inlined, plus the
example note when the divergence came from an example-targeted probe:

```
Result mismatch for query 98 (example: lookup=2.6667, c1=8, c2=3 — MySQL-only match): SELECT ...
Row count mismatch for query 90: SELECT ...
```

## Targeted testing

The default run is "every pattern, composed deeply" -- broad coverage, but
a specific feature can be washed out by the rest. Two flags narrow the
focus: `--required-tags` picks *which* feature to exercise, and
`--single-pattern` picks *how* -- combined with other patterns (default) or
in isolation.

| Goal | Flags |
|---|---|
| Exercise a feature **in combination** with others (deeper queries, broad shapes) | `--required-tags <tags>` |
| Exercise a feature **in isolation** (one pattern, no partners, reproducible) | `--required-tags <tags> --single-pattern` |

Both modes still want `--readyset-mode` (below) whenever the endpoint is
real Readyset.

### Required tags (`--required-tags`, `--excluded-tags`)

Every pattern carries a list of tags (`base`, `aggregate`, `expr_eval`,
`mysql_only`, `cte`, `compound`, `lookup_key`, ...). A pattern is kept only
if it carries every required tag and none of the excluded tags; multiple
required tags are AND-ed.

With `--required-tags` alone, composition stays on: the tagged pattern is
still combined with random partners, so you test the feature **as it
appears inside larger queries**. This is the right choice for "make sure
`expr_eval` is well-covered across realistic shapes" runs.

```bash
# Expression-evaluation patterns, composed as usual.
target/debug/readyset-dante-oracle \
  --readyset-mode \
  --compare-to mysql://... --readyset-url mysql://... \
  --seed 42 --max-queries 500 \
  --required-tags expr_eval

# Narrow further -- only MySQL-tagged integer division.
--required-tags expr_eval,arithmetic,int_int
```

Browse the available tags by grepping the registry:

```bash
grep -rh 'b\.tags(' public/dante/src/registry/ | sort -u
```

### Single pattern (`--single-pattern`)

Add `--single-pattern` to test the feature **in isolation**: each iteration
resolves exactly one pattern via `try_resolve`, with no composition
partners.

This matters because composition partners (self-join, aggregate compose
helpers, awkward placeholder positions) frequently block `CREATE DEEP
CACHE`. A blocked cache forces the query to proxy upstream, where it
trivially matches and hides the very divergence you are hunting. Resolving
one pattern at a time keeps the query deep-cacheable so divergences surface
reliably.

```bash
# expr_eval, one pattern at a time -- divergences surface reliably.
target/debug/readyset-dante-oracle \
  --readyset-mode \
  --compare-to mysql://... --readyset-url mysql://... \
  --seed 42 --max-queries 200 \
  --required-tags expr_eval \
  --single-pattern
```

`--single-pattern` also drops the generator's column-reuse preference, so a
pattern needing three distinct columns of overlapping type classes (e.g.
`lookup + c1 + c2`, all Integer-ish) actually gets three columns instead of
collapsing onto the auto-allocated PK.

### Readyset mode vs proxy (`--readyset-mode`)

The oracle compares two endpoints regardless of what `--readyset-url`
points at. `--readyset-mode` controls how much Readyset-specific behavior
it drives:

- **With `--readyset-mode`** (real Readyset): the generator applies the
  deep-cache compatibility rules, every query gets a `CREATE DEEP CACHE`,
  and the EXPLAIN / `SHOW READYSET TABLES` probes run. This is what forces
  results through dataflow instead of proxying to upstream, so use it for
  any run whose goal is to find Readyset divergences.
- **Without it** (default, transparent proxy such as SQP): no
  Readyset-specific DDL or probes run, and the proxy decides its own
  routing. Compound SELECTs still get their helper VIEW so the same SQL
  runs on both sides.

Almost every targeted run wants `--readyset-mode`; omit it only when the
endpoint genuinely is not Readyset.

### Example pinning

Each pattern iteration runs one random-fill SELECT plus one SELECT per
example attached to the pattern. Each example inserts its own row (via
column-var overrides) alongside random fill, then executes a SELECT with
its parameter overrides. Divergences carry `example_note` in the log
line so the triggering probe is immediately visible. Examples are
filtered by `DialectSupport`, so a `MySqlOnly` example never runs
against PostgreSQL. See the "Example pinning" section of
`public/dante/README.md` for cell rules, `VarKind` invariants, and how to
write examples.

To stress a specific example, combine all three knobs:

```bash
target/debug/readyset-dante-oracle \
  --readyset-mode \
  --compare-to mysql://root:noria@127.0.0.1:3306/cfuzz_s42 \
  --readyset-url mysql://root:noria@127.0.0.1:3307/cfuzz_s42 \
  --seed 42 --max-queries 50 \
  --required-tags expr_eval,int_int \
  --single-pattern
```

## Reproduction capture

`--dump-repro <path>` streams every DDL, INSERT, and SELECT in execution
order to `<path>` as a self-contained replay script. Parameters are
inlined into the SELECT text so the file is directly executable against
`mysql` or `psql`. Example overrides are emitted as comments next to the
probe they pinned.

If a run fails *without* `--dump-repro`, the harness still dumps the
most-recent ring of statements to stderr so the suffix that triggered
the failure is visible in logs.

```bash
target/debug/readyset-dante-oracle ... --dump-repro /tmp/repro.sql

# Replay against a clean MySQL:
mysql -h 127.0.0.1 -P 3306 -u root -pnoria scratch < /tmp/repro.sql
```

`DROP TABLE IF EXISTS` is intentionally **not** recorded — it would
erase the seed data on replay.

## Antithesis assertion analysis

Each `assert_reachable!` call writes one line to the Antithesis JSONL
stream. After a soak run, summarise hit assertions:

```bash
python3 -c "
import json, sys, collections
hit, total = collections.Counter(), collections.Counter()
for fname in sys.argv[1:]:
    for line in open(fname):
        try:
            a = json.loads(line).get('antithesis_assert')
            if not a: continue
            total[a['message']] += 1
            if a.get('hit'): hit[a['message']] += 1
        except: pass
print(f'Hit: {len(hit)}/{len(total)} assertions')
for m in sorted(hit): print(f'  {hit[m]:3d}x  {m[:80]}')
" /tmp/antithesis/antithesis-logictest-s42.jsonl \
  /tmp/antithesis/antithesis-readyset-s42.jsonl
```

## Multi-seed runs

Each seed needs its own database name and its own Readyset port. The
one-liner below is convenient for ad-hoc loops:

```bash
DB=cfuzz_s42 PORT=3307 SEED=42 N=200 && \
mysql -h 127.0.0.1 -P 3306 -u root -pnoria -e "DROP DATABASE IF EXISTS $DB; CREATE DATABASE $DB;" && \
rm -rf /tmp/rs-$DB.db && mkdir -p /tmp/antithesis && \
ANTITHESIS_SDK_LOCAL_OUTPUT=/tmp/antithesis/antithesis-readyset-$DB.jsonl \
LOG_LEVEL=error \
FEATURE_FULL_MATERIALIZATION=true \
FEATURE_MATERIALIZATION_PERSISTENCE=true \
FEATURE_MIXED_COMPARISONS=true \
FEATURE_NON_BLOCKING_INDEX_BUILD=true \
FEATURE_PAGINATION=true \
FEATURE_PLACEHOLDER_INLINING=true \
FEATURE_POST_LOOKUP=true \
FEATURE_STRADDLED_JOINS=true \
FEATURE_TOPK=true \
target/debug/readyset \
  --upstream-db-url mysql://root:noria@127.0.0.1:3306/$DB \
  --address 0.0.0.0:$PORT --storage-dir /tmp/rs-$DB.db \
  --cache-mode deep-then-shallow --query-caching in-request-path & \
RS_PID=$! && sleep 8 && \
ANTITHESIS_SDK_LOCAL_OUTPUT=/tmp/antithesis/antithesis-logictest-$DB.jsonl \
LOG_LEVEL=info \
target/debug/readyset-dante-oracle \
  --readyset-mode \
  --compare-to mysql://root:noria@127.0.0.1:3306/$DB \
  --readyset-url mysql://root:noria@127.0.0.1:$PORT/$DB \
  --seed $SEED --max-queries $N --rows-per-table 50 ; \
kill $RS_PID 2>/dev/null; wait $RS_PID 2>/dev/null
```

Change `DB`, `PORT`, `SEED`, and `N` for each invocation.

## Cleanup

```bash
# Kill any readyset attached to this DB
pkill -f "readyset.*cfuzz_s42"

# Drop the database
mysql -h 127.0.0.1 -P 3306 -u root -pnoria \
  -e "DROP DATABASE IF EXISTS cfuzz_s42;"
# Postgres
PGPASSWORD=noria psql -h 127.0.0.1 -p 5432 -U postgres \
  -c "SELECT pg_drop_replication_slot('readyset')" || true
PGPASSWORD=noria psql -h 127.0.0.1 -p 5432 -U postgres \
  -c "DROP DATABASE IF EXISTS cfuzz_pg_test"

# Remove Readyset's local storage
rm -rf /tmp/rs-cfuzz-s42.db /tmp/rs-cfuzz-pg-test.db
```
