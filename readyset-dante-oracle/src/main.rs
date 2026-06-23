//! Constraint-based fuzz test for Readyset.
//!
//! Generates random SQL queries and DDL using `dante`, executes them
//! against both an upstream database and Readyset, and compares results. Uses a
//! single persistent `Generator` state across all queries, accumulating tables
//! and columns over the course of the run.

mod value;

use std::collections::{HashMap, HashSet};
use std::convert::TryFrom;
use std::future::Future;
use std::io::Write;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::OnceLock;
use std::time::Duration;

use antithesis_sdk::prelude::*;
use anyhow::{Context, bail};
use clap::Parser;
use dante::constraint::ExampleValue;
use dante::pattern::Pattern;
use dante::resolver::{DdlStep, ParamMeta, ParamOverride, ResolvedExample, RowOverride};
use dante::state::TableSchema;
use dante::{Generator, GeneratorConfig};
use data_generator::ColumnGenerator;
use database_utils::{DatabaseConnection, DatabaseURL, QueryableConnection};
use rand::rngs::{SmallRng, StdRng, SysRng};
use rand::{Rng, SeedableRng};
use readyset_data::DfValue;
use readyset_sql::ast::{
    Column, ColumnConstraint, ColumnSpecification, CreateTableBody, CreateTableStatement, Expr,
    IndexKeyPart, InsertStatement, Literal, Relation, SelectSpecification, SqlIdentifier, TableKey,
};
use readyset_sql::{Dialect, DialectDisplay};
use readyset_util::retry_with_exponential_backoff;
use serde_json::json;
use tracing::{debug, error, info, warn};

use crate::value::{QueryParams, Value};

/// `CREATE DEEP CACHE` retry policy. The schedule (5 attempts ×
/// 500ms/1s/2s/4s/8s ≈ 15.5s sleep budget) must keep a stuck Readyset
/// from stalling the harness on a cache that will never come up. With
/// backoff: 2 each delay doubles, so adding retries grows the worst case
/// fast — `cache_create_retry_budget_under_30s` guards regressions.
const CACHE_CREATE_RETRIES: u32 = 5;
const CACHE_CREATE_BASE_DELAY_MS: u64 = 500;
const CACHE_CREATE_BACKOFF: u64 = 2;

/// Default ring-buffer capacity for the on-error fallback path. ~1000
/// statements at ~1KB/each ≈ 1MB resident, well under typical OOM thresholds
/// for an Antithesis container even after a multi-million-statement run.
const REPRO_RING_CAPACITY: usize = 1000;

/// Retry schedule for Readyset SELECT comparisons. Readyset is eventually
/// consistent: the first SELECT may create a cache that hasn't fully
/// populated yet. Total sleep budget: ~29.5s across 9 retries.
const RETRY_DELAYS_MS: &[u64] = &[500, 1000, 2000, 3000, 4000, 4000, 5000, 5000, 5000];

/// Streamed reproduction log. The artifact reconstructs the *workload*, not
/// server-side nondeterminism (CURRENT_TIMESTAMP, AUTO_INCREMENT, sequences,
/// etc.); replays presume a fresh database.
///
/// Two write paths:
/// - Explicit (`--dump-repro`): a [`BufWriter<File>`] is opened up front and
///   each [`record_ddl`]/[`record_select`] call streams directly. Memory is
///   bounded to the BufWriter's internal buffer regardless of run length.
/// - Fallback (no flag, run failed): nothing is streamed; we keep only a
///   bounded ring buffer of the most-recent [`REPRO_RING_CAPACITY`] entries
///   and dump them to stderr on termination.
///
/// In both modes the ring buffer is also kept (ordered most-recent-first)
/// for fault-injection scenarios where the file path is unwritable: stderr
/// emission still surfaces the recent suffix of the workload.
///
/// SELECTs are recorded with parameters substituted inline via
/// [`inline_params`], so the file is directly executable against `psql` /
/// `mysql`. The raw placeholder form is preserved as a comment.
///
/// `DROP TABLE IF EXISTS` is intentionally NOT recorded: it's cross-run
/// startup hygiene that would erase seed data if the script were replayed.
#[allow(dead_code)]
struct ReproLog {
    writer: Option<std::io::BufWriter<std::fs::File>>,
    recent: std::collections::VecDeque<String>,
    capacity: usize,
    statements_total: usize,
    dialect: Dialect,
    seed_display: String,
    written_path: Option<PathBuf>,
}

#[allow(dead_code)]
impl ReproLog {
    /// Construct a log with no streaming writer (fallback-only path).
    fn new(dialect: Dialect, seed_display: String) -> Self {
        Self {
            writer: None,
            recent: std::collections::VecDeque::with_capacity(REPRO_RING_CAPACITY),
            capacity: REPRO_RING_CAPACITY,
            statements_total: 0,
            dialect,
            seed_display,
            written_path: None,
        }
    }

    /// Construct a log streaming directly to `path`. Header is written
    /// eagerly so even an aborted run leaves a parseable prefix on disk.
    fn with_writer(path: PathBuf, dialect: Dialect, seed_display: String) -> anyhow::Result<Self> {
        let f = std::fs::File::create(&path)
            .with_context(|| format!("creating repro file: {}", path.display()))?;
        let mut writer = std::io::BufWriter::new(f);
        let mut log = Self {
            writer: None,
            recent: std::collections::VecDeque::with_capacity(REPRO_RING_CAPACITY),
            capacity: REPRO_RING_CAPACITY,
            statements_total: 0,
            dialect,
            seed_display,
            written_path: Some(path),
        };
        writer.write_all(log.header().as_bytes())?;
        log.writer = Some(writer);
        Ok(log)
    }

    fn push_entry(&mut self, entry: String) {
        self.statements_total += 1;
        if let Some(w) = self.writer.as_mut() {
            // Best-effort streaming write. A write error here is logged but
            // doesn't abort the run; the bounded ring buffer still preserves
            // the recent suffix for the stderr fallback.
            if let Err(e) = writeln!(w, "{entry}\n") {
                warn!(err = %e, "repro stream write failed; falling back to ring buffer only");
                self.writer = None;
            }
        }
        if self.recent.len() == self.capacity {
            self.recent.pop_front();
        }
        self.recent.push_back(entry);
    }

    /// Record a DDL or DML statement (CREATE TABLE, ALTER TABLE, INSERT, UPDATE).
    fn record_ddl(&mut self, query_idx: usize, sql: &str) {
        self.push_entry(format!("-- query_idx={query_idx} (DDL)\n{sql};"));
    }

    /// Record a SELECT statement, inlining concrete parameter values into the
    /// SQL so the script is directly replayable. The raw placeholder form is
    /// preserved as a comment for cross-checking against the live execution.
    ///
    /// `example_ctx` carries the human-readable note and override summaries
    /// when this SELECT is an example-targeted probe rather than the random
    /// probe. The overrides are emitted as comments so triagers can replay the
    /// exact bait that surfaced a divergence.
    fn record_select(
        &mut self,
        query_idx: usize,
        pattern: &str,
        sql: &str,
        params: &[Value],
        example_ctx: Option<(&'static str, &[RowOverride], &[ParamOverride])>,
    ) {
        use std::fmt::Write as _;
        let inlined = inline_params(sql, params, self.dialect);
        let mut entry = format!("-- query_idx={query_idx} pattern={pattern}\n");
        if let Some((note, row_overrides, param_overrides)) = example_ctx {
            writeln!(entry, "-- example: {note}").expect("write to String never fails");
            for o in row_overrides {
                writeln!(
                    entry,
                    "--   row override: {}.{} = {}",
                    o.table,
                    o.column,
                    fmt_example_value(&o.value)
                )
                .expect("write to String never fails");
            }
            for o in param_overrides {
                writeln!(
                    entry,
                    "--   param override: ${} = {}",
                    o.placeholder_index + 1,
                    fmt_example_value(&o.value)
                )
                .expect("write to String never fails");
            }
        }
        if !params.is_empty() {
            writeln!(entry, "-- raw: {sql}").expect("write to String never fails");
            let vals = params
                .iter()
                .map(|v| format!("{v:?}"))
                .collect::<Vec<_>>()
                .join(", ");
            writeln!(entry, "-- params: {vals}").expect("write to String never fails");
        }
        write!(entry, "{inlined};").expect("write to String never fails");
        self.push_entry(entry);
    }

    fn header(&self) -> String {
        format!(
            "-- readyset-dante-oracle reproduction script\n\
             -- dialect: {dialect:?}\n\
             -- seed: {seed}\n\
             -- This script reproduces the SQL workload, not server-side\n\
             -- nondeterminism (CURRENT_TIMESTAMP, AUTO_INCREMENT, sequences).\n\
             -- Replay against a fresh database.\n",
            dialect = self.dialect,
            seed = self.seed_display,
        )
    }

    /// Flush the streamed writer if any. Returns the path written and the
    /// total statement count when streaming was active. Errors propagate
    /// (caller decides whether they're fatal).
    fn finish(&mut self) -> anyhow::Result<Option<(PathBuf, usize)>> {
        if let Some(mut w) = self.writer.take() {
            w.flush().context("flushing repro file")?;
            return Ok(self
                .written_path
                .clone()
                .map(|p| (p, self.statements_total)));
        }
        Ok(None)
    }

    /// Dump the bounded ring buffer to stderr with a header. Used as a
    /// last-resort fallback when the file path is unwritable (e.g.,
    /// Antithesis container temp_dirs vanish across snapshots) so the
    /// recent suffix is at least visible in logs.
    fn dump_to_stderr(&self) {
        eprintln!("{}", self.header());
        let dropped = self.statements_total.saturating_sub(self.recent.len());
        if dropped > 0 {
            eprintln!("-- truncated: dropped {dropped} earlier statements");
        }
        for stmt in &self.recent {
            eprintln!("{stmt}");
            eprintln!();
        }
    }

    /// Returns the number of statements ever recorded (not the ring length).
    fn statements_total(&self) -> usize {
        self.statements_total
    }
}

/// Adapter that bridges the Antithesis random source to `rand 0.10` [`Rng`].
///
/// `antithesis_sdk::random::AntithesisRng` implements an older `Rng`, but
/// our generator requires `rand 0.10` `Rng`. This adapter calls the
/// Antithesis random API directly.
pub struct AntithesisRngAdapter;

impl rand::rand_core::TryRng for AntithesisRngAdapter {
    type Error = std::convert::Infallible;

    fn try_next_u32(&mut self) -> Result<u32, Self::Error> {
        Ok(antithesis_sdk::random::get_random() as u32)
    }

    fn try_next_u64(&mut self) -> Result<u64, Self::Error> {
        Ok(antithesis_sdk::random::get_random())
    }

    fn try_fill_bytes(&mut self, dest: &mut [u8]) -> Result<(), Self::Error> {
        for chunk in dest.chunks_mut(8) {
            let bytes = self.try_next_u64()?.to_le_bytes();
            chunk.copy_from_slice(&bytes[..chunk.len()]);
        }
        Ok(())
    }
}

/// Convert a [`TableSchema`] to a [`CreateTableStatement`] suitable for
/// execution against a database.
fn table_schema_to_create_table(schema: &TableSchema) -> CreateTableStatement {
    let fields: Vec<ColumnSpecification> = schema
        .columns
        .iter()
        .map(|(col_name, meta)| {
            let is_pk = schema.primary_key.as_ref().is_some_and(|pk| pk == col_name);
            let mut constraints = Vec::new();
            if is_pk {
                constraints.push(ColumnConstraint::NotNull);
            }
            ColumnSpecification {
                column: Column {
                    name: col_name.clone(),
                    table: Some(schema.name.clone().into()),
                },
                sql_type: meta.sql_type.clone(),
                generated: None,
                constraints,
                comment: None,
                invisible: false,
            }
        })
        .collect();

    let keys = schema.primary_key.as_ref().map(|pk_name| {
        vec![TableKey::PrimaryKey {
            constraint_name: None,
            constraint_timing: None,
            index_name: None,
            columns: vec![IndexKeyPart::Column(Column {
                name: pk_name.clone(),
                table: None,
            })],
        }]
    });

    CreateTableStatement {
        if_not_exists: false,
        table: Relation {
            schema: None,
            name: schema.name.clone(),
        },
        body: Ok(CreateTableBody { fields, keys }),
        like: None,
        options: Ok(vec![]),
    }
}

/// Format an [`ExampleValue`] for human-readable repro-script comments.
fn fmt_example_value(v: &ExampleValue) -> String {
    let ExampleValue::Literal(s) = v;
    s.to_string()
}

/// Parse a SQL literal string into a [`DfValue`] for the given column type.
///
/// Used when materialising `ExampleValue::Literal` overrides into actual row
/// data. Example literals are authored `&'static` constants, so a value that
/// fails to parse for its column type is a test-authoring bug: panic with
/// context rather than silently binding NULL, which would let a bug-bait
/// example pass green while exercising nothing. Non-numeric types accept any
/// text as-is.
fn parse_literal(s: &str, sql_type: &readyset_sql::ast::SqlType) -> DfValue {
    use std::str::FromStr;
    use std::sync::Arc;

    use readyset_decimal::Decimal;
    use readyset_sql::ast::SqlType;

    let bad = |e: &dyn std::fmt::Debug| -> ! {
        panic!("dante example literal {s:?} is not a valid {sql_type:?}: {e:?}")
    };
    match sql_type {
        SqlType::Int(_)
        | SqlType::Int4
        | SqlType::Int8
        | SqlType::BigInt(_)
        | SqlType::SmallInt(_)
        | SqlType::TinyInt(_) => s
            .trim()
            .parse::<i64>()
            .map(DfValue::Int)
            .unwrap_or_else(|e| bad(&e)),
        SqlType::IntUnsigned(_)
        | SqlType::BigIntUnsigned(_)
        | SqlType::SmallIntUnsigned(_)
        | SqlType::TinyIntUnsigned(_)
        | SqlType::UnsignedInteger
        | SqlType::Unsigned => s
            .trim()
            .parse::<u64>()
            .map(DfValue::UnsignedInt)
            .unwrap_or_else(|e| bad(&e)),
        // Decimal/Numeric must land as `DfValue::Numeric` so the INSERT path
        // emits a true DECIMAL literal. Parsing as f64 silently demotes
        // values like `2.6667` to `DfValue::Double`, which round-trips
        // through PG/MySQL as a floating-point literal and breaks
        // bug-bait examples that depend on exact decimal arithmetic.
        SqlType::Decimal(_, _) | SqlType::Numeric(_) => Decimal::from_str(s.trim())
            .map(|d| DfValue::Numeric(Arc::new(d)))
            .unwrap_or_else(|e| bad(&e)),
        SqlType::Double | SqlType::Float | SqlType::Real => s
            .trim()
            .parse::<f64>()
            .map(DfValue::Double)
            .unwrap_or_else(|e| bad(&e)),
        _ => DfValue::from(s),
    }
}

/// Draw from `generator`, retrying up to 32 times to avoid values in `exclude`.
///
/// If all 32 draws collide the last value is returned anyway — this is a
/// best-effort avoidance for uniqueness columns, not a hard guarantee.
fn sample_avoiding<R: Rng>(
    generator: &mut ColumnGenerator,
    exclude: &HashSet<DfValue>,
    rng: &mut R,
) -> DfValue {
    let mut last = generator.r#gen(rng);
    for _ in 0..32 {
        if !exclude.contains(&last) {
            return last;
        }
        last = generator.r#gen(rng);
    }
    last
}

/// Generate rows of data for a table.
///
/// Returns `(rows, dropped_example_indices)`. `rows` contains one emitted row
/// per un-dropped example followed by random-fill rows. `dropped_example_indices`
/// lists which positions in `examples` were skipped due to PK collision with an
/// earlier example; callers should remove the corresponding `ResolvedExample`
/// entries before building SELECT probes so probes don't fire for absent rows.
///
/// If `examples` is non-empty, one row is emitted per example using its
/// `row_overrides` to pin specific column values. The remaining `random_fill`
/// rows are produced by each column's `gen_spec`. Per-column exclude sets are
/// seeded from example `Literal` values only so that `Unique`/`UniqueFrom`
/// gen-specs don't collide with the bait; they are not updated during random fill.
///
/// Each column gets its own sub-RNG seeded from `(parent_seed, column_name)` so
/// the per-column data depends only on the seed and the column name, not on
/// iteration order. Reordering schema columns must not change the rows
/// produced for a given seed (for the random-fill portion).
fn generate_rows<R: Rng>(
    schema: &TableSchema,
    random_fill: usize,
    examples: &[&ResolvedExample],
    rng: &mut R,
) -> (Vec<Vec<DfValue>>, Vec<usize>) {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let col_count = schema.columns.len();

    // Build per-column exclude sets from example Literal values only.
    // Random-fill generators (Unique/Sequential) produce distinct values on
    // their own; feeding back random-fill draws into excludes would
    // unnecessarily inflate the set and block bait-value avoidance.
    let mut excludes: Vec<HashSet<DfValue>> = vec![HashSet::new(); col_count];
    for ex in examples {
        for (col_idx, (col_name, col_meta)) in schema.columns.iter().enumerate() {
            if let Some(o) = ex
                .row_overrides
                .iter()
                .find(|o| o.table == schema.name && o.column == *col_name)
            {
                let ExampleValue::Literal(s) = &o.value;
                excludes[col_idx].insert(parse_literal(s, &col_meta.sql_type));
            }
        }
    }

    // Build per-column sub-RNGs (keyed by column name for determinism).
    let parent_seed = rng.next_u64();
    let mut generators: Vec<(ColumnGenerator, SmallRng)> = schema
        .columns
        .iter()
        .map(|(name, meta)| {
            let mut hasher = DefaultHasher::new();
            parent_seed.hash(&mut hasher);
            name.as_str().hash(&mut hasher);
            let sub_seed = hasher.finish();
            let mut sub_rng = SmallRng::seed_from_u64(sub_seed);
            let generator = meta
                .gen_spec
                .generator_for_col(meta.sql_type.clone(), &mut sub_rng);
            (generator, sub_rng)
        })
        .collect();

    let mut out: Vec<Vec<DfValue>> = Vec::with_capacity(examples.len() + random_fill);
    let mut dropped_example_indices: Vec<usize> = Vec::new();

    // Index of the primary-key column (if any) and the set of PK values
    // already committed to `out`. Pattern authors can legitimately pin the
    // same PK literal in two examples (different `param_overrides` exercising
    // the same row); without dedup, the second example would trigger a
    // duplicate-key INSERT failure that aborts the run.
    let pk_idx: Option<usize> = schema
        .primary_key
        .as_ref()
        .and_then(|pk| schema.columns.iter().position(|(name, _)| name == pk));
    let mut seen_pks: HashSet<DfValue> = HashSet::new();

    // Emit one row per example.
    for (ex_idx, ex) in examples.iter().enumerate() {
        let mut row = Vec::with_capacity(col_count);
        for (idx, (col_name, col_meta)) in schema.columns.iter().enumerate() {
            let override_for_col = ex
                .row_overrides
                .iter()
                .find(|o| o.table == schema.name && o.column == *col_name);
            match override_for_col {
                Some(o) => {
                    let ExampleValue::Literal(s) = &o.value;
                    row.push(parse_literal(s, &col_meta.sql_type));
                }
                None => {
                    let (ref mut col_gen, ref mut sub_rng) = generators[idx];
                    let v = sample_avoiding(col_gen, &excludes[idx], sub_rng);
                    row.push(v);
                }
            }
        }
        if let Some(pk_i) = pk_idx {
            let pk_val = row[pk_i].clone();
            if !seen_pks.insert(pk_val.clone()) {
                warn!(
                    table = %schema.name,
                    pk = %pk_val,
                    note = ex.note,
                    "skipping example seed row whose PK collides with an earlier example"
                );
                dropped_example_indices.push(ex_idx);
                continue;
            }
        }
        out.push(row);
    }

    // Random fill.
    for _ in 0..random_fill {
        let mut row = Vec::with_capacity(col_count);
        for (idx, _) in schema.columns.iter().enumerate() {
            let (ref mut col_gen, ref mut sub_rng) = generators[idx];
            let v = sample_avoiding(col_gen, &excludes[idx], sub_rng);
            row.push(v);
        }
        if let Some(pk_i) = pk_idx {
            let pk_val = row[pk_i].clone();
            if !seen_pks.insert(pk_val) {
                // `sample_avoiding` is best-effort (32 retries). On collision
                // we drop the random-fill row rather than insert a duplicate.
                continue;
            }
        }
        out.push(row);
    }

    (out, dropped_example_indices)
}

/// Build an [`InsertStatement`] for a batch of rows.
fn build_insert(schema: &TableSchema, data: &[Vec<DfValue>]) -> anyhow::Result<InsertStatement> {
    let columns: Vec<SqlIdentifier> = schema.columns.keys().cloned().collect();
    let insert_fields: Vec<Column> = columns
        .iter()
        .map(|c| Column {
            name: c.clone(),
            table: None,
        })
        .collect();

    // Column SQL types in projection order, used to project BOOL values to boolean literals.
    let col_types: Vec<readyset_sql::ast::SqlType> = schema
        .columns
        .values()
        .map(|m| m.sql_type.clone())
        .collect();

    let insert_data: Vec<Vec<Expr>> = data
        .iter()
        .map(|row| {
            row.iter()
                .enumerate()
                .map(|(i, v)| {
                    let lit: Literal = v.clone().try_into().unwrap_or(Literal::Null);
                    // BOOL columns get Literal::Integer(0|1) from the data-generator
                    // (DfValue::Int -> Literal::Integer); Postgres rejects an integer literal
                    // for a boolean column. Project to Literal::Boolean so the dialect printer
                    // emits TRUE/FALSE on PG and 1/0 on MySQL.
                    let lit = match (col_types.get(i), lit) {
                        (Some(readyset_sql::ast::SqlType::Bool), Literal::Integer(n)) => {
                            Literal::Boolean(n != 0)
                        }
                        (_, l) => l,
                    };
                    Expr::Literal(lit)
                })
                .collect()
        })
        .collect();

    Ok(InsertStatement {
        table: Relation {
            schema: None,
            name: schema.name.clone(),
        },
        fields: insert_fields,
        data: insert_data,
        ignore: false,
        on_duplicate: None,
    })
}

/// Return the total number of placeholder values across all [`ParamMeta`] descriptors.
fn total_param_count(params: &[ParamMeta]) -> usize {
    params.iter().map(|m| m.count as usize).sum()
}

/// Materialize concrete parameter values from [`ParamMeta`] descriptors, applying
/// any per-placeholder [`ParamOverride`]s.  When an override is present for a given
/// placeholder index its value takes precedence over the normal `gen_spec` draw.
fn materialize_params_with_overrides<R: Rng>(
    params: &[ParamMeta],
    overrides: &[ParamOverride],
    rng: &mut R,
) -> Vec<DfValue> {
    let mut out = Vec::with_capacity(total_param_count(params));
    let mut placeholder_idx = 0usize;
    for meta in params {
        // One generator per ParamMeta keeps `Unique`/`UniqueFrom` state shared
        // across all `meta.count` placeholders (e.g. IN (?, ?, ?)). Per-draw
        // construction would reset that state and emit the same value for
        // every placeholder in the slot.
        let mut generator = meta.gen_spec.generator_for_col(meta.sql_type.clone(), rng);
        for _ in 0..meta.count {
            let override_for_idx = overrides
                .iter()
                .find(|o| o.placeholder_index == placeholder_idx);
            let v = match override_for_idx {
                Some(o) => {
                    let ExampleValue::Literal(s) = &o.value;
                    parse_literal(s, &meta.sql_type)
                }
                None => generator.r#gen(rng),
            };
            out.push(v);
            placeholder_idx += 1;
        }
    }
    out
}

/// Materialize concrete parameter values from [`ParamMeta`] descriptors.
fn materialize_params<R: Rng>(params: &[ParamMeta], rng: &mut R) -> Vec<DfValue> {
    materialize_params_with_overrides(params, &[], rng)
}

/// One SELECT probe: materialized parameters plus optional example annotation
/// (note text and override summaries for repro-script comments).
#[derive(Debug)]
struct SelectProbe<'a> {
    params: Vec<Value>,
    /// `None` for the all-random probe; `Some(note)` for an example-targeted probe.
    example_note: Option<&'static str>,
    /// Row overrides from the originating [`ResolvedExample`] (empty for the random probe).
    row_overrides: &'a [RowOverride],
    /// Param overrides from the originating [`ResolvedExample`] (empty for the random probe).
    param_overrides: &'a [ParamOverride],
}

/// Build the ordered list of SELECT probes for one query iteration.
///
/// Returns one probe per execution: the first is always an all-random probe
/// (no overrides), followed by one probe per [`ResolvedExample`] with that
/// example's [`ParamOverride`]s applied. Each probe carries an optional note
/// string that identifies the example (or `None` for the random probe), plus
/// clones of the example's row and parameter overrides for repro-script
/// annotation.
///
/// Conversion from `DfValue` to `Value` can fail; errors are collected and
/// the offending probe is silently dropped. If the random probe's conversion
/// fails, returns empty vec; caller treats that as fatal. Example probe
/// failures are silently dropped from the result.
fn build_select_probes<'a, R: Rng>(
    params: &[ParamMeta],
    examples: &'a [ResolvedExample],
    select_sql: &str,
    rng: &mut R,
) -> Vec<SelectProbe<'a>> {
    let mut probes = Vec::with_capacity(1 + examples.len());

    // Always emit one all-random probe first (no overrides).
    let random_df = materialize_params_with_overrides(params, &[], rng);
    match random_df
        .into_iter()
        .map(Value::try_from)
        .collect::<Result<Vec<_>, _>>()
    {
        Ok(vals) => probes.push(SelectProbe {
            params: vals,
            example_note: None,
            row_overrides: &[],
            param_overrides: &[],
        }),
        Err(err) => {
            // Return an empty vec so the call site can surface this as a
            // fatal error rather than silently skipping the probe.
            warn!(
                err = %format!("{err:#}"),
                sql = %select_sql,
                "skipping random probe: DfValue->Value conversion failed"
            );
        }
    }

    // One probe per example, with that example's param overrides.
    for ex in examples {
        let df = materialize_params_with_overrides(params, &ex.param_overrides, rng);
        match df
            .into_iter()
            .map(Value::try_from)
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(vals) => probes.push(SelectProbe {
                params: vals,
                example_note: Some(ex.note),
                row_overrides: &ex.row_overrides,
                param_overrides: &ex.param_overrides,
            }),
            Err(err) => {
                warn!(
                    err = %format!("{err:#}"),
                    sql = %select_sql,
                    note = ex.note,
                    "skipping example probe: DfValue->Value conversion failed"
                );
            }
        }
    }

    probes
}

/// Tracks per-pattern generation and comparison outcomes across iterations.
#[derive(Debug, Default)]
struct PatternStats {
    /// pattern_name -> (generated, matched, mismatched)
    counts: HashMap<String, PatternCounts>,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
enum CacheMode {
    #[default]
    Unknown,
    Deep,
    Shallow,
    Proxy,
    /// The routing probe was deliberately not run (without --readyset-mode,
    /// where the oracle is indifferent to how the result was served).
    NotChecked,
}

impl std::fmt::Display for CacheMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            CacheMode::Unknown => "unknown",
            CacheMode::Deep => "deep",
            CacheMode::Shallow => "shallow",
            CacheMode::Proxy => "proxy",
            CacheMode::NotChecked => "not_checked",
        })
    }
}

/// Reject `--required-tags`/`--excluded-tags` values that no registered
/// pattern carries. A typo'd tag would otherwise silently select nothing,
/// so fail fast and name the offending tag.
fn validate_known_tags<'a>(
    supplied: impl IntoIterator<Item = &'a str>,
    known: &std::collections::BTreeSet<&'static str>,
) -> anyhow::Result<()> {
    for tag in supplied {
        if !known.contains(tag) {
            bail!("unknown pattern tag {tag:?}: no registered pattern carries it");
        }
    }
    Ok(())
}

#[derive(Debug, Default, Clone)]
struct PatternCounts {
    generated: usize,
    matched: usize,
    mismatched: usize,
    skipped: usize,
    /// Non-fatal terminal errors from upstream or Readyset that would
    /// otherwise abort the loop. Only incremented when `--keep-going` is
    /// set; otherwise the run exits before this counter would advance.
    errored: usize,
    deep: usize,
    shallow: usize,
    proxy: usize,
    autoparam_confirmed: usize,
    autoparam_none: usize,
}

impl PatternStats {
    fn entry(&mut self, pattern: &str) -> &mut PatternCounts {
        // Avoid the double allocation that `self.counts.entry(pattern.to_string())`
        // does on every hit: the entry API takes the key by value, so a
        // pre-allocated String is paid even for present keys. Borrow-based
        // lookup pays an alloc only on insert.
        if !self.counts.contains_key(pattern) {
            self.counts
                .insert(pattern.to_owned(), PatternCounts::default());
        }
        self.counts
            .get_mut(pattern)
            .expect("inserted above on miss")
    }

    fn record_cache_mode(&mut self, pattern: &str, mode: CacheMode) {
        let entry = self.entry(pattern);
        match mode {
            CacheMode::Deep => entry.deep += 1,
            CacheMode::Shallow => entry.shallow += 1,
            CacheMode::Proxy => entry.proxy += 1,
            CacheMode::Unknown | CacheMode::NotChecked => {}
        }
    }

    fn log_summary(&self) {
        let mut entries: Vec<_> = self.counts.iter().collect();
        entries.sort_by_key(|(a, _)| *a);

        info!("=== Pattern Coverage Summary ===");
        for (name, c) in &entries {
            info!(
                pattern = %name,
                generated = c.generated,
                matched = c.matched,
                mismatched = c.mismatched,
                skipped = c.skipped,
                errored = c.errored,
                deep = c.deep,
                shallow = c.shallow,
                proxy = c.proxy,
                autoparam_confirmed = c.autoparam_confirmed,
                autoparam_none = c.autoparam_none,
                "pattern stats"
            );
        }

        let total_generated: usize = entries.iter().map(|(_, c)| c.generated).sum();
        let total_matched: usize = entries.iter().map(|(_, c)| c.matched).sum();
        let total_mismatched: usize = entries.iter().map(|(_, c)| c.mismatched).sum();
        let total_errored: usize = entries.iter().map(|(_, c)| c.errored).sum();
        let total_deep: usize = entries.iter().map(|(_, c)| c.deep).sum();
        let total_shallow: usize = entries.iter().map(|(_, c)| c.shallow).sum();
        let total_proxy: usize = entries.iter().map(|(_, c)| c.proxy).sum();
        let never_generated: Vec<_> = entries
            .iter()
            .filter(|(_, c)| c.generated == 0)
            .map(|(name, _)| name.as_str())
            .collect();
        info!(
            patterns_hit = entries.iter().filter(|(_, c)| c.generated > 0).count(),
            patterns_total = entries.len(),
            total_generated,
            total_matched,
            total_mismatched,
            total_errored,
            total_deep,
            total_shallow,
            total_proxy,
            "coverage totals"
        );
        if !never_generated.is_empty() {
            warn!(
                patterns = ?never_generated,
                "patterns never generated during this run"
            );
        }
    }
}

/// Run the fuzz test: generate SQL queries and DDL, executing each step
/// immediately. DDL/DML goes to upstream only (Readyset replicates
/// automatically). SELECTs go to both and results are compared.
///
/// Uses a single [`Generator`] with persistent state across all queries,
/// accumulating tables and columns over the course of the run. Tables are
/// never dropped, so Readyset only has to snapshot each table once.
///
/// Compound-SELECT views (created during the run by `yolokuor`'s code path)
/// are always dropped on termination, including on `?` early returns from
/// later steps — see [`drop_all_views`]. Synchronous Drop guards can't issue
/// async statements, so this wrapper provides the equivalent guarantee at
/// the function boundary.
#[allow(clippy::too_many_arguments)]
async fn run_queries(
    dialect: Dialect,
    num_queries: usize,
    rows_per_table: usize,
    rng: &mut dyn Rng,
    upstream_url: &DatabaseURL,
    readyset_url: &DatabaseURL,
    upstream: &mut DatabaseConnection,
    readyset: &mut DatabaseConnection,
    stats: &mut PatternStats,
    repro: &mut ReproLog,
    selection_filter: dante::compat::SelectionFilter,
    single_pattern: bool,
    keep_going: bool,
    readyset_mode: bool,
) -> anyhow::Result<(usize, usize, usize)> {
    let mut created_views: Vec<String> = Vec::new();
    let result = run_queries_body(
        dialect,
        num_queries,
        rows_per_table,
        rng,
        upstream_url,
        readyset_url,
        upstream,
        readyset,
        stats,
        repro,
        &mut created_views,
        selection_filter,
        single_pattern,
        keep_going,
        readyset_mode,
    )
    .await;
    drop_all_views(upstream, &created_views).await;
    result
}

#[allow(clippy::too_many_arguments)]
async fn run_queries_body(
    dialect: Dialect,
    num_queries: usize,
    rows_per_table: usize,
    rng: &mut dyn Rng,
    upstream_url: &DatabaseURL,
    readyset_url: &DatabaseURL,
    upstream: &mut DatabaseConnection,
    readyset: &mut DatabaseConnection,
    stats: &mut PatternStats,
    repro: &mut ReproLog,
    created_views: &mut Vec<String>,
    selection_filter: dante::compat::SelectionFilter,
    single_pattern: bool,
    keep_going: bool,
    readyset_mode: bool,
) -> anyhow::Result<(usize, usize, usize)> {
    let config = GeneratorConfig {
        // In Readyset mode, apply the deep-cache compatibility rules so the
        // generator avoids shapes Readyset cannot deep-cache. Against a
        // transparent proxy those rules only narrow coverage for no benefit.
        readyset_compatible: readyset_mode,
        // Under `--single-pattern` patterns run standalone; high reuse collapses
        // distinct column variables onto the auto-allocated PK (Integer
        // columns all bind to `c0`), erasing patterns that need three
        // distinct columns of overlapping classes (e.g.
        // `where_lookup_decimal_eq_int_div_int` needs lookup + c1 + c2).
        // Fresh tables per iteration also keep bug-bait inserts isolated.
        reuse_preference: if single_pattern { 0.0 } else { 0.99 },
        ..GeneratorConfig::default()
    };
    let mut generator = Generator::new(dialect, config);
    let mut entropy = dante::entropy::Entropy::new(rng);

    // O(1) membership and dedup of repeat creations (which can occur if the
    // same name surfaces across iterations through DROP TABLE IF EXISTS +
    // recreate). Linear-Vec growth would be unbounded across long Antithesis
    // runs.
    let mut created_tables: HashSet<SqlIdentifier> = HashSet::new();
    let mut matched_count = 0usize;
    let mut mismatched_count = 0usize;
    let mut errored_count = 0usize;

    'query: for query_idx in 0..num_queries {
        let mut output = if single_pattern {
            // Direct resolve path: pick one pattern matching the filter,
            // resolve via `try_resolve` without composition. Avoids
            // composition partners (self_join, problematic placeholders,
            // etc.) that block `CREATE DEEP CACHE`.
            let pattern = generator
                .registry()
                .pick_random(&mut entropy, &selection_filter, dialect)
                .ok_or_else(|| {
                    anyhow::anyhow!("no pattern matches --required-tags / --excluded-tags filter")
                })?;
            let pattern_name = pattern.name.to_string();
            let recipe = pattern.to_recipe(0);
            let ro = dante::resolver::try_resolve(&recipe, generator.state_mut(), &mut entropy)
                .with_context(|| format!("resolving query {query_idx}"))?;
            dante::QueryOutput::from_resolver(ro, pattern_name, dialect)
        } else {
            generator
                .generate_with_ddl_filtered(&mut entropy, &selection_filter)
                .with_context(|| format!("generating query {query_idx}"))?
        };

        // Borrow rather than clone: `output` lives through the entire query
        // iteration, and every `pattern_name` use below is a `&str`. Cloning
        // a String per query was unnecessary on the hot path.
        let pattern_name: &str = &output.pattern_name;
        stats.entry(pattern_name).generated += 1;

        // Pattern alone (no query_idx) keeps the Antithesis dedup catalog
        // bounded; otherwise every iteration becomes a distinct finding.
        assert_reachable!(
            "Query generation succeeded",
            &json!({ "pattern": &pattern_name })
        );

        // Execute DDL steps on upstream.
        for step in &output.ddl {
            match step {
                DdlStep::CreateTable { name, schema } => {
                    if schema.columns.is_empty() {
                        bail!(
                            "BUG: CreateTable DDL for {name} has no columns. \
                             This is a dante bug. Query idx: {query_idx}"
                        );
                    }
                    // Drop any stale table from a previous run of the binary.
                    let drop_sql = format!("DROP TABLE IF EXISTS {name}");
                    with_reconnect_on_drop(
                        upstream,
                        upstream_url,
                        "DROP TABLE",
                        async |c: &mut DatabaseConnection| {
                            with_op_timeout("DROP TABLE", c.query_drop(&drop_sql))
                                .await
                                .with_context(|| format!("DROP TABLE on upstream: {drop_sql}"))
                        },
                    )
                    .await?;

                    let create_stmt = table_schema_to_create_table(schema);
                    let sql = create_stmt.display(dialect).to_string();
                    debug!(%sql, "executing CREATE TABLE on upstream");
                    // `drop_sql` deliberately NOT recorded: it's cross-run
                    // startup hygiene that would wipe seed data on replay.
                    repro.record_ddl(query_idx, &sql);
                    with_reconnect_on_drop(
                        upstream,
                        upstream_url,
                        "CREATE TABLE",
                        async |c: &mut DatabaseConnection| {
                            with_op_timeout("CREATE TABLE", c.query_drop(&sql))
                                .await
                                .with_context(|| format!("CREATE TABLE on upstream: {sql}"))
                        },
                    )
                    .await?;

                    // Insert seed data, including rows pinned by example
                    // row_overrides for this table so that example-targeted
                    // SELECTs have matching rows to hit. `global_indices`
                    // maps the per-table slice back into `output.examples`
                    // so dropped rows can be removed and their probes
                    // skipped.
                    let (global_indices, examples_for_table): (Vec<usize>, Vec<&ResolvedExample>) =
                        output
                            .examples
                            .iter()
                            .enumerate()
                            .filter(|(_, ex)| ex.row_overrides.iter().any(|o| o.table == *name))
                            .unzip();
                    let (rows, dropped) =
                        generate_rows(schema, rows_per_table, &examples_for_table, &mut entropy);
                    // Remove examples for dropped rows (in reverse order to keep indices valid).
                    for &local_idx in dropped.iter().rev() {
                        output.examples.remove(global_indices[local_idx]);
                    }
                    if !rows.is_empty() {
                        let insert = build_insert(schema, &rows)?;
                        let insert_sql = insert.display(dialect).to_string();
                        debug!(table = %name, rows = rows.len(), "inserting seed data");
                        repro.record_ddl(query_idx, &insert_sql);
                        with_reconnect_on_drop(
                            upstream,
                            upstream_url,
                            "INSERT seed",
                            async |c: &mut DatabaseConnection| {
                                with_op_timeout("INSERT seed", c.query_drop(&insert_sql))
                                    .await
                                    .with_context(|| format!("INSERT seed data for {name}"))
                            },
                        )
                        .await?;
                    }
                    created_tables.insert(name.clone());
                    assert_reachable!(
                        "Executed CREATE TABLE with seed data",
                        &json!({
                            "table": name.to_string(),
                            "columns": schema.columns.len(),
                            "rows": rows_per_table,
                        })
                    );
                }
                DdlStep::AddColumn {
                    table,
                    column_name,
                    meta,
                } => {
                    let sql = format!(
                        "ALTER TABLE {} ADD COLUMN {} {}",
                        table,
                        column_name,
                        meta.sql_type.display(dialect)
                    );
                    debug!(%sql, "executing ALTER TABLE on upstream");
                    repro.record_ddl(query_idx, &sql);
                    with_reconnect_on_drop(
                        upstream,
                        upstream_url,
                        "ALTER TABLE",
                        async |c: &mut DatabaseConnection| {
                            with_op_timeout("ALTER TABLE", c.query_drop(&sql))
                                .await
                                .with_context(|| format!("ALTER TABLE on upstream: {sql}"))
                        },
                    )
                    .await?;

                    let mut generator = meta
                        .gen_spec
                        .generator_for_col(meta.sql_type.clone(), &mut entropy);
                    let value = generator.r#gen(&mut entropy);
                    let lit: Literal = value.try_into().unwrap_or(Literal::Null);
                    // Bool columns get `Literal::Integer(0|1)` from the
                    // data-generator (DfValue::Int -> Literal::Integer);
                    // Postgres rejects `bool_col = 1` with no implicit
                    // coercion. Project to `Literal::Boolean` so the
                    // dialect printer emits TRUE/FALSE on PG and 1/0
                    // on MySQL.
                    let lit = match (&meta.sql_type, lit) {
                        (readyset_sql::ast::SqlType::Bool, Literal::Integer(i)) => {
                            Literal::Boolean(i != 0)
                        }
                        (_, l) => l,
                    };
                    let update_sql = format!(
                        "UPDATE {} SET {} = {}",
                        table,
                        column_name,
                        Expr::Literal(lit).display(dialect)
                    );
                    debug!(%update_sql, "backfilling new column");
                    repro.record_ddl(query_idx, &update_sql);
                    with_reconnect_on_drop(
                        upstream,
                        upstream_url,
                        "UPDATE backfill",
                        async |c: &mut DatabaseConnection| {
                            with_op_timeout("UPDATE backfill", c.query_drop(&update_sql))
                                .await
                                .with_context(|| {
                                    format!("UPDATE backfill for {table}.{column_name}")
                                })
                        },
                    )
                    .await?;

                    assert_reachable!(
                        "Executed ALTER TABLE ADD COLUMN",
                        &json!({
                            "table": table.to_string(),
                            "column": column_name.to_string(),
                        })
                    );
                }
            }
        }

        // Wait for Readyset to replicate all DDL changes before querying.
        if !output.ddl.is_empty() {
            let table_names: Vec<&SqlIdentifier> = output
                .ddl
                .iter()
                .map(|step| match step {
                    DdlStep::CreateTable { name, .. } => name,
                    DdlStep::AddColumn { table, .. } => table,
                })
                .collect();
            if !table_names.is_empty() {
                wait_for_replication(
                    upstream,
                    readyset,
                    &table_names,
                    Duration::from_secs(60),
                    readyset_mode,
                )
                .await
                .with_context(|| format!("waiting for DDL replication before query {query_idx}"))?;
            }
        }

        // Render SELECT and build per-probe parameter sets:
        // one all-random probe plus one per ResolvedExample.
        let select_sql = output.query.display(dialect).to_string();
        let has_order_by = output.query.has_order_by();
        let has_limit = output.query.has_limit();
        let select_probes =
            build_select_probes(&output.params, &output.examples, &select_sql, &mut entropy);
        // build_select_probes always includes at least the random probe, but
        // if DfValue->Value conversion failed for it the vec would be empty.
        // Surface that as a fatal error (same as the old `?` on the single-probe path).
        if select_probes.is_empty() {
            anyhow::bail!("all SELECT probes failed DfValue->Value conversion for: {select_sql}");
        }

        // Compound SELECTs can't go through readyset's ad-hoc cache path; route
        // them through a VIEW + deep cache so the compound MIR path is
        // exercised. Upstream still runs the original UNION (so we compare
        // readyset's view-of-UNION to upstream's direct UNION rather than to
        // upstream's view-of-UNION, which would hide upstream optimizer
        // divergence around view materialization).
        let readyset_select_sql = if matches!(output.query, SelectSpecification::Compound(_)) {
            let vname = SqlIdentifier::from(format!("v_union_{query_idx}"));
            let vname_sql = Relation {
                schema: None,
                name: vname.clone(),
            }
            .display(dialect)
            .to_string();
            let drop_view = format!("DROP VIEW IF EXISTS {vname_sql}");
            let create_view = format!("CREATE VIEW {vname_sql} AS {select_sql}");
            debug!(%drop_view, %create_view, "recreating view for compound SELECT");
            repro.record_ddl(query_idx, &drop_view);
            upstream
                .query_drop(&drop_view)
                .await
                .with_context(|| format!("DROP VIEW on upstream: {drop_view}"))?;
            repro.record_ddl(query_idx, &create_view);
            upstream
                .query_drop(&create_view)
                .await
                .with_context(|| format!("CREATE VIEW on upstream: {create_view}"))?;
            created_views.push(vname_sql.clone());

            let view_select = format!("SELECT * FROM {vname_sql}");
            if !readyset_mode {
                // Non-Readyset endpoint: run the view directly without the
                // cache-forcing DDL. The proxy decides routing.
                view_select
            } else {
                let create_cache = format!("CREATE DEEP CACHE FROM {view_select}");
                // Bounded retry budget so a stuck Readyset skips this view's
                // comparison instead of stalling the run on multi-minute
                // exponential backoff.
                let cache_result: Result<_, _> = retry_with_exponential_backoff!(
                    { readyset.query_drop(&create_cache).await },
                    retries: CACHE_CREATE_RETRIES,
                    delay: CACHE_CREATE_BASE_DELAY_MS,
                    backoff: CACHE_CREATE_BACKOFF,
                );
                // Don't proceed-anyway: the previous behavior ran the SELECT
                // against Readyset without a deep cache, creating a shallow
                // cache and mixing performance with correctness signal. Skip
                // the comparison instead so the run continues without a false
                // mismatch from cache-mode divergence.
                if let Err(err) = cache_result {
                    warn!(
                        %vname_sql,
                        %err,
                        query_idx,
                        "timed out creating deep cache for compound view; skipping comparison"
                    );
                    stats.entry(pattern_name).skipped += 1;
                    continue 'query;
                }

                view_select
            }
        } else if !readyset_mode {
            // Non-Readyset endpoint: skip the deep-cache DDL entirely.
            select_sql.clone()
        } else {
            // Non-compound: ask Readyset to deep-cache the SELECT so the
            // query runs through dataflow instead of being proxied upstream
            // or shallow-cached (which would still forward to upstream and
            // mask coercion divergences). Explicit DEEP CACHE so we don't
            // fall back to SHALLOW when deep compilation can't handle the
            // shape.
            let create_cache = format!("CREATE DEEP CACHE FROM {select_sql}");
            let cache_result: Result<_, _> = retry_with_exponential_backoff!(
                { readyset.query_drop(&create_cache).await },
                retries: CACHE_CREATE_RETRIES,
                delay: CACHE_CREATE_BASE_DELAY_MS,
                backoff: CACHE_CREATE_BACKOFF,
            );
            // Skip the comparison rather than proxying on failure: a proxied
            // SELECT forwards to upstream and matches it trivially, reporting a
            // false match that masks the coercion divergences this oracle
            // exists to find. Mirrors the compound-view path above.
            if let Err(err) = cache_result {
                warn!(
                    query_idx,
                    %select_sql,
                    %err,
                    "CREATE DEEP CACHE failed; skipping comparison"
                );
                stats.entry(pattern_name).skipped += 1;
                continue 'query;
            }
            select_sql.clone()
        };

        // LIMIT without ORDER BY produces non-deterministic row subsets that
        // can't meaningfully be compared between upstream and readyset.
        // Handle this at query level (before the per-probe loop) so that one
        // warmup execute exercises the cache path and all probes are skipped.
        if has_limit && !has_order_by {
            debug!(
                query_idx,
                %select_sql,
                "skipping comparison: LIMIT without ORDER BY is non-deterministic"
            );
            // Warmup ReadySet with the first (random) probe's params.
            let warmup_params = &select_probes[0].params;
            let _ = with_reconnect_on_drop(
                readyset,
                readyset_url,
                "SELECT readyset (warmup)",
                async |c: &mut DatabaseConnection| {
                    execute_select(c, &readyset_select_sql, warmup_params).await
                },
            )
            .await;
            stats.entry(pattern_name).skipped += 1;
            continue;
        }

        // Run one SELECT per probe: first the all-random probe, then one per
        // ResolvedExample. Each probe is compared independently.
        let mut query_matched_probes = 0usize;
        let mut query_mismatched_probes = 0usize;
        let mut query_errored_probes = 0usize;
        for probe in &select_probes {
            let params = &probe.params;
            let example_note = probe.example_note;
            debug!(
                %select_sql,
                param_count = params.len(),
                example_note,
                "executing SELECT on both"
            );
            if let Some(note) = probe.example_note {
                assert_reachable!(
                    "Example probe dispatched",
                    &json!({
                        "pattern": pattern_name,
                        "note": note,
                    })
                );
            }
            let example_ctx = probe
                .example_note
                .map(|note| (note, probe.row_overrides, probe.param_overrides));
            repro.record_select(query_idx, pattern_name, &select_sql, params, example_ctx);

            // Execute on upstream (deterministic source of truth).
            let upstream_results = match with_reconnect_on_drop(
                upstream,
                upstream_url,
                "SELECT upstream",
                async |c: &mut DatabaseConnection| execute_select(c, &select_sql, params).await,
            )
            .await
            {
                Ok(results) => results,
                Err(err) => match classify_error(&err) {
                    ErrorClass::Fatal => {
                        return Err(err).context(format!("SELECT on upstream: {select_sql}"));
                    }
                    ErrorClass::UpstreamKnownLimit { code } => {
                        warn!(
                            query_idx,
                            %select_sql,
                            ?code,
                            err = %format!("{err:#}"),
                            "skipping query due to upstream planner limitation"
                        );
                        stats.entry(pattern_name).skipped += 1;
                        continue 'query;
                    }
                    ErrorClass::UpstreamGeneratorBug { code } => {
                        error!(
                            query_idx,
                            %select_sql,
                            ?code,
                            err = %format!("{err:#}"),
                            "upstream rejected query (likely generator bug)"
                        );
                        assert_unreachable!(
                            "Upstream rejected generated query",
                            &json!({
                                "pattern": &pattern_name,
                                "code": code,
                            })
                        );
                        stats.entry(pattern_name).skipped += 1;
                        continue 'query;
                    }
                    class @ (ErrorClass::Other
                    | ErrorClass::ReadysetTransient
                    | ErrorClass::UpstreamTransient) => {
                        // Upstream is the deterministic source of truth: only a
                        // transient error may be skipped under --keep-going.
                        // `Other` propagates so a real upstream failure is
                        // never masked as a benign "errored".
                        if keep_going && upstream_error_tolerable_under_keep_going(&class) {
                            warn!(
                                query_idx,
                                %select_sql,
                                err = %format!("{err:#}"),
                                "upstream SELECT hit a transient error; skipping under keep-going"
                            );
                            match classify_query(
                                query_matched_probes,
                                query_mismatched_probes,
                                true,
                            ) {
                                QueryTally::Mismatched => {
                                    mismatched_count += 1;
                                    stats.entry(pattern_name).mismatched += 1;
                                }
                                _ => {
                                    errored_count += 1;
                                    stats.entry(pattern_name).errored += 1;
                                }
                            }
                            continue 'query;
                        }
                        return Err(err).context(format!("SELECT on upstream: {select_sql}"));
                    }
                },
            };
            let upstream_rows: Vec<Vec<Value>> = upstream_results;

            let mut last_mismatch: Option<String> = None;
            let mut matched = false;

            for attempt in 0..RETRY_DELAYS_MS.len() + 1 {
                let readyset_results = match execute_select(readyset, &readyset_select_sql, params)
                    .await
                {
                    Ok(r) => r,
                    Err(err) if is_connection_drop(&err) => {
                        warn!(
                            attempt,
                            query_idx,
                            err = %format!("{err:#}"),
                            "readyset connection dropped during retry, reconnecting"
                        );
                        *readyset = reconnect(readyset_url).await?;
                        if let Some(&delay_ms) = RETRY_DELAYS_MS.get(attempt) {
                            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                        }
                        continue;
                    }
                    Err(err) => match classify_error(&err) {
                        ErrorClass::ReadysetTransient => {
                            debug!(
                                attempt,
                                query_idx,
                                err = %format!("{err:#}"),
                                "transient readyset error, retrying"
                            );
                            if let Some(&delay_ms) = RETRY_DELAYS_MS.get(attempt) {
                                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                            }
                            continue;
                        }
                        _ => {
                            if keep_going {
                                error!(
                                    query_idx,
                                    %select_sql,
                                    err = %format!("{err:#}"),
                                    "readyset SELECT failed (keep-going)"
                                );
                                assert_unreachable!(
                                    "Readyset SELECT errored (keep-going swallowed)",
                                    &json!({ "pattern": &pattern_name })
                                );
                                match classify_query(
                                    query_matched_probes,
                                    query_mismatched_probes,
                                    true,
                                ) {
                                    QueryTally::Mismatched => {
                                        mismatched_count += 1;
                                        stats.entry(pattern_name).mismatched += 1;
                                    }
                                    _ => {
                                        errored_count += 1;
                                        stats.entry(pattern_name).errored += 1;
                                    }
                                }
                                continue 'query;
                            }
                            return Err(err).context(format!("SELECT on readyset: {select_sql}"));
                        }
                    },
                };
                let readyset_rows: Vec<Vec<Value>> = readyset_results;

                match compare_results(&upstream_rows, &readyset_rows, has_order_by) {
                    ComparisonOutcome::Match => {
                        matched = true;
                        break;
                    }
                    ComparisonOutcome::Mismatch { reason } => {
                        last_mismatch = Some(format!(
                            "Mismatch for query {query_idx}: {select_sql}\n{reason}"
                        ));
                    }
                }

                if let Some(&delay_ms) = RETRY_DELAYS_MS.get(attempt) {
                    let delay = Duration::from_millis(delay_ms);
                    debug!(
                        attempt,
                        delay_ms = delay.as_millis(),
                        "result mismatch, retrying after backoff"
                    );
                    tokio::time::sleep(delay).await;
                }
            }

            if matched {
                query_matched_probes += 1;
            } else {
                // `last_mismatch` is None when the entire retry budget was
                // consumed by transient errors / connection drops without ever
                // producing a comparable result. Surface them as separate
                // Antithesis findings with low-cardinality payloads.
                match last_mismatch {
                    Some(mismatch_msg) => {
                        error!(query_idx, "{mismatch_msg}");
                        query_mismatched_probes += 1;
                        assert_unreachable!(
                            "Result mismatch between upstream and Readyset after retries",
                            &json!({
                                "pattern": &pattern_name,
                                "example_note": example_note,
                            })
                        );
                        if let Some(note) = example_note {
                            assert_sometimes!(
                                true,
                                "Example probe found divergence",
                                &json!({
                                    "pattern": pattern_name,
                                    "note": note,
                                })
                            );
                        }
                    }
                    None => {
                        // Readyset never produced a comparable result, so this
                        // is an error, not a divergence. Counting it as a
                        // mismatch would let infra flakiness trip the divergence
                        // assertion and inflate the mismatch tally.
                        query_errored_probes += 1;
                        assert_unreachable!(
                            "Readyset never converged within retry budget (all transient)",
                            &json!({
                                "pattern": &pattern_name,
                                "example_note": example_note,
                            })
                        );
                        warn!(
                            query_idx,
                            %pattern_name,
                            example_note,
                            "retry budget exhausted on transient errors"
                        );
                    }
                }
            }

            // Sentinel-based autoparameterization probe for hoisting patterns.
            // Only run on the random probe (example_note is None) to avoid
            // duplicate autoparam signals with different pinned literals.
            //
            // Runs only in --readyset-mode: the probe relies on EXPLAIN
            // CREATE CACHE, which only applies to a cache-creating Readyset
            // target. Against a transparent proxy (e.g. SQP) there is no
            // autoparameterization to observe, so the probe and its
            // assertions can never be satisfied and would otherwise show as
            // a permanent failure floor.
            if example_note.is_none()
                && matched
                && Pattern::name_needs_literal_mode(pattern_name)
                && !params.is_empty()
                && readyset_mode
            {
                let (sentinel_sql, sentinels) = inline_sentinels(&select_sql, params, dialect);
                debug!(
                    query_idx,
                    %sentinel_sql,
                    sentinel_count = sentinels.len(),
                    "probing autoparameterization with sentinels"
                );

                assert_reachable!(
                    "Hoisting pattern sentinel probe executed",
                    &json!({ "pattern": &pattern_name })
                );

                if let Some(sentinel_explain) = explain_create_cache(readyset, &sentinel_sql).await
                {
                    let (parameterized, not_parameterized) =
                        classify_autoparameterization(&sentinel_explain.query_text, &sentinels);

                    debug!(
                        query_idx,
                        %pattern_name,
                        parameterized_count = parameterized.len(),
                        not_parameterized_count = not_parameterized.len(),
                        rewritten = %sentinel_explain.query_text,
                        query_id = %sentinel_explain.query_id,
                        "autoparameterization classification"
                    );

                    if !parameterized.is_empty() {
                        assert_reachable!(
                            "Hoisting pattern has autoparameterized literals",
                            &json!({
                                "pattern": &pattern_name,
                                "parameterized_indices": &parameterized,
                                "not_parameterized_indices": &not_parameterized,
                            })
                        );
                    }

                    // Only assert cache REUSE when EVERY literal was parameterized. With a mix of
                    // parameterized and inline literals, varying the inline positions between the
                    // two probe variants legitimately yields different caches — asserting reuse
                    // there is a false positive.
                    if should_assert_autoparam_reuse(&parameterized, &not_parameterized) {
                        // Verify autoparameterization via EXPLAIN CREATE
                        // CACHE on two literal variants with different concrete
                        // values.  If readyset autoparameterizes both to the
                        // same query ID, the feature is working: different
                        // literals are normalized to the same cache entry.
                        let literal_sql_1 = inline_params(&select_sql, params, dialect);

                        let new_concrete = materialize_params(&output.params, &mut entropy);
                        let new_params: Result<Vec<Value>, _> =
                            new_concrete.into_iter().map(Value::try_from).collect();
                        let new_params = match new_params {
                            Ok(v) => v,
                            Err(err) => {
                                // Surface materialization failures as Antithesis
                                // findings rather than silently skipping.
                                warn!(
                                    query_idx,
                                    %pattern_name,
                                    err = %format!("{err:#}"),
                                    "skipping autoparam sentinel probe: param materialization failed"
                                );
                                assert_unreachable!(
                                    "Param materialization failed during sentinel probe",
                                    &json!({
                                        "pattern": &pattern_name,
                                    })
                                );
                                Vec::new()
                            }
                        };

                        if !new_params.is_empty() {
                            let literal_sql_2 = inline_params(&select_sql, &new_params, dialect);

                            let explain_1 = explain_create_cache(readyset, &literal_sql_1).await;
                            let explain_2 = explain_create_cache(readyset, &literal_sql_2).await;

                            debug!(
                                query_idx,
                                %pattern_name,
                                explain_1_id = explain_1.as_ref().map(|e| e.query_id.as_str()),
                                explain_1_text =
                                    explain_1.as_ref().map(|e| e.query_text.as_str()),
                                explain_2_id = explain_2.as_ref().map(|e| e.query_id.as_str()),
                                explain_2_text =
                                    explain_2.as_ref().map(|e| e.query_text.as_str()),
                                "autoparam probe: EXPLAIN CREATE CACHE results"
                            );

                            match (explain_1, explain_2) {
                                (Some(e1), Some(e2)) if e1.query_id == e2.query_id => {
                                    // Same query ID: the autoparameterization
                                    // machinery maps both literal forms to the
                                    // identical cache entry.
                                    stats.entry(pattern_name).autoparam_confirmed += 1;
                                    assert_reachable!(
                                        "Autoparameterized cache reused for different literal values",
                                        &json!({
                                            "pattern": &pattern_name,
                                            "query_id": &e1.query_id,
                                            "query_text": &e1.query_text,
                                        })
                                    );
                                }
                                (Some(_), Some(_)) => {
                                    // Different query IDs — autoparameterization
                                    // produced different normalized forms for
                                    // different literals.  This should not happen.
                                    stats.entry(pattern_name).autoparam_none += 1;
                                    assert_unreachable!(
                                        "Autoparameterized query got different cache for different literals",
                                        &json!({
                                            "pattern": &pattern_name,
                                        })
                                    );
                                }
                                _ => {
                                    // EXPLAIN CREATE CACHE failed for one or
                                    // both variants — can't verify.
                                    debug!(
                                        query_idx,
                                        %pattern_name,
                                        "autoparam probe: EXPLAIN CREATE CACHE failed for a literal variant"
                                    );
                                }
                            }
                        }
                    } else {
                        stats.entry(pattern_name).autoparam_none += 1;
                        debug!(
                            query_idx,
                            %pattern_name,
                            parameterized_count = parameterized.len(),
                            not_parameterized_count = not_parameterized.len(),
                            "skipping autoparam reuse probe: not all literals were parameterized"
                        );
                    }
                }
            }
        }

        // Update per-query counters once after all probes complete.
        // Cache mode is per (pattern, query shape), not per probe.
        //
        // The `EXPLAIN LAST STATEMENT` probe runs only in --readyset-mode.
        // Against a transparent proxy such as SQP (the default) the oracle is
        // indifferent to routing -- deep, shallow, proxied upstream, or DuckDB
        // are all fine -- so it is skipped entirely.
        let cache_mode = if readyset_mode {
            query_cache_mode(readyset).await
        } else {
            CacheMode::NotChecked
        };
        stats.record_cache_mode(pattern_name, cache_mode);

        let query_match_status = match classify_query(
            query_matched_probes,
            query_mismatched_probes,
            query_errored_probes > 0,
        ) {
            QueryTally::Matched => {
                matched_count += 1;
                stats.entry(pattern_name).matched += 1;
                "matched"
            }
            QueryTally::Mismatched => {
                mismatched_count += 1;
                stats.entry(pattern_name).mismatched += 1;
                "mismatched"
            }
            QueryTally::Errored => {
                // A probe ran but only produced transient errors; tally it as
                // errored, distinct from a query that was never compared.
                errored_count += 1;
                stats.entry(pattern_name).errored += 1;
                "errored"
            }
            QueryTally::Inconclusive => "neither",
        };
        assert_reachable!(
            "Query comparison completed",
            &json!({
                "pattern": &pattern_name,
                "status": query_match_status,
                "cache_mode": cache_mode.to_string(),
            })
        );
        if query_match_status == "matched" {
            assert_reachable!(
                "Query results match between upstream and Readyset",
                &json!({
                    "pattern": &pattern_name,
                    "has_order_by": has_order_by,
                    "cache_mode": cache_mode.to_string(),
                })
            );
        }
    }

    // Verify that queries actually created caches. Runs only in
    // --readyset-mode; against a transparent proxy (like SQP) no caches are
    // created, so the "Deep caches created" reachability marker is not
    // applicable and would never fire.
    if readyset_mode {
        let cache_count = count_caches_for_tables(readyset, &created_tables).await;
        if cache_count == 0 && num_queries > 0 {
            warn!(
                num_queries,
                "no caches were created — all queries may have been proxied to upstream"
            );
        } else {
            info!(cache_count, "deep caches created during run");
            assert_reachable!(
                "Deep caches created",
                &json!({ "cache_count": cache_count })
            );
        }
    }

    let (deep_count, shallow_count) = count_caches_detailed(readyset, &created_tables).await;

    let compared = matched_count + mismatched_count;
    // Demoted from `assert_always!` (which fails the run when every query
    // happened to be skipped — e.g. all hit unsupported patterns or transient
    // errors) to `assert_sometimes!` so the run logs visibility without
    // surfacing a spurious "bug" when no comparisons were attempted.
    assert_sometimes!(
        compared > 0,
        "At least one query was compared",
        &json!({
            "matched": matched_count,
            "mismatched": mismatched_count,
            "total_queries": num_queries,
        })
    );

    // Demoted from `assert_always!` to a warn — shallow caches are a
    // performance signal, not a correctness invariant, and the previous
    // assert tripped on routine retry-"proceed anyway" paths and on slow
    // upstream replication. count_caches_detailed already logs a warn on
    // shallow > 0; the per-cache details are there.
    if shallow_count > 0 {
        warn!(
            deep = deep_count,
            shallow = shallow_count,
            "some caches are shallow at end of run (performance signal, not correctness)"
        );
    }

    // Hoisting-pattern coverage assertions.
    let hoisting_generated: usize = stats
        .counts
        .iter()
        .filter(|(name, _)| Pattern::name_needs_literal_mode(name))
        .map(|(_, c)| c.generated)
        .sum();
    let hoisting_matched: usize = stats
        .counts
        .iter()
        .filter(|(name, _)| Pattern::name_needs_literal_mode(name))
        .map(|(_, c)| c.matched)
        .sum();
    let hoisting_autoparam: usize = stats
        .counts
        .iter()
        .filter(|(name, _)| Pattern::name_needs_literal_mode(name))
        .map(|(_, c)| c.autoparam_confirmed)
        .sum();

    assert_sometimes!(
        hoisting_generated > 0,
        "Hoisting pattern was generated",
        &json!({
            "hoisting_generated": hoisting_generated,
            "total_queries": num_queries,
        })
    );
    assert_sometimes!(
        hoisting_matched > 0,
        "Hoisting pattern matched between upstream and Readyset",
        &json!({
            "hoisting_matched": hoisting_matched,
            "hoisting_generated": hoisting_generated,
        })
    );
    // Autoparameterization is confirmed via EXPLAIN CREATE CACHE, which only
    // applies to a cache-creating Readyset target. Without --readyset-mode
    // the sentinel probe above is skipped so hoisting_autoparam is always 0;
    // emitting this Sometimes would be a permanent unsatisfiable failure
    // against a transparent proxy. "Hoisting pattern was generated/matched"
    // stay unconditional -- they are passthrough-observable (row-set equality).
    if readyset_mode {
        assert_sometimes!(
            hoisting_autoparam > 0,
            "Hoisting pattern autoparameterization confirmed via cache reuse",
            &json!({
                "hoisting_autoparam": hoisting_autoparam,
                "hoisting_matched": hoisting_matched,
            })
        );
    }

    info!(
        matched_count,
        mismatched_count,
        hoisting_generated,
        hoisting_matched,
        hoisting_autoparam,
        tables = created_tables.len(),
        views = created_views.len(),
        deep_caches = deep_count,
        shallow_caches = shallow_count,
        "run completed"
    );
    Ok((matched_count, mismatched_count, errored_count))
}

/// Drop every view we created during the run. Always called from
/// `run_queries`, including on early-Err propagation paths, so compound-SELECT
/// views never leak across runs. Best-effort: failures are logged but not
/// surfaced (caller's original error takes precedence).
async fn drop_all_views(upstream: &mut DatabaseConnection, views: &[String]) {
    for vname in views {
        let drop_view = format!("DROP VIEW IF EXISTS {vname}");
        if let Err(err) =
            with_op_timeout("DROP VIEW (cleanup)", upstream.query_drop(&drop_view)).await
        {
            warn!(%vname, %err, "failed to drop compound-SELECT view during cleanup");
        }
    }
}

/// Per-op DB timeout, set once at startup from the CLI. Antithesis injects
/// fault scenarios (packet drops, syscall stalls) from cycle 0; without a
/// per-op bound any injected fault stalls the simulation forever and is
/// reported as "stuck workload" rather than as a found bug.
static DB_OP_TIMEOUT: OnceLock<Duration> = OnceLock::new();

/// Default per-op timeout when the CLI flag is unset (e.g., in unit tests).
const DEFAULT_DB_OP_TIMEOUT: Duration = Duration::from_secs(30);

fn db_op_timeout() -> Duration {
    *DB_OP_TIMEOUT.get().unwrap_or(&DEFAULT_DB_OP_TIMEOUT)
}

/// Wrap a DB operation in the configured per-op timeout. Required around
/// every DB call so that fault injection cannot stall a run forever.
async fn with_op_timeout<T, E, F>(op_name: &str, fut: F) -> anyhow::Result<T>
where
    F: Future<Output = Result<T, E>>,
    E: Into<anyhow::Error>,
{
    let timeout = db_op_timeout();
    match tokio::time::timeout(timeout, fut).await {
        Ok(Ok(v)) => Ok(v),
        Ok(Err(e)) => Err(e.into()),
        Err(_) => Err(anyhow::anyhow!(
            "DB operation `{op_name}` exceeded {:?} timeout",
            timeout
        )),
    }
}

/// Detect that an error from the upstream/Readyset connection means the
/// underlying TCP connection is gone (or otherwise unrecoverable without a
/// fresh connection). A long Antithesis run will eventually disconnect when
/// faults are injected; without this check the harness aborts on the first
/// drop and the run is wasted.
fn is_connection_drop(err: &anyhow::Error) -> bool {
    use database_utils::DatabaseError;

    for cause in err.chain() {
        if let Some(db) = cause.downcast_ref::<DatabaseError>() {
            return match db {
                DatabaseError::MySQL(my) => my.is_fatal(),
                DatabaseError::PostgreSQL(pg) => pg.is_closed(),
                DatabaseError::UpstreamConnectionNone => true,
                _ => false,
            };
        }
    }
    let msg = format!("{err:#}");
    const NEEDLES: &[&str] = &[
        "Connection refused",
        "connection closed",
        "Connection reset",
        "broken pipe",
        "Broken pipe",
        "os error 32",
        "os error 104",
    ];
    NEEDLES.iter().any(|n| msg.contains(n))
}

/// Build a fresh connection by calling `connect_and_setup` and propagating
/// the connection-replay (SET SESSION ...) so a reconnect is indistinguishable
/// from a fresh startup.
async fn reconnect(url: &DatabaseURL) -> anyhow::Result<DatabaseConnection> {
    connect_and_setup(url)
        .await
        .with_context(|| format!("reconnecting to {url:?}"))
}

/// True if the connect error is permanent — auth/permission/missing-database
/// — and retrying it would just block the operator on the full retry budget
/// for nothing. MySQL codes 1044/1045/1142/1698 cover access-denied class;
/// Postgres SQLSTATE class 28 covers `invalid_authorization_specification`.
fn is_permanent_connection_error(err: &anyhow::Error) -> bool {
    use database_utils::DatabaseError;
    use mysql_async::Error as MyErr;

    for cause in err.chain() {
        if let Some(db) = cause.downcast_ref::<DatabaseError>() {
            return match db {
                DatabaseError::MySQL(MyErr::Server(e)) => {
                    matches!(e.code, 1044 | 1045 | 1142 | 1698)
                }
                DatabaseError::PostgreSQL(pg) => pg
                    .as_db_error()
                    .map(|d| d.code().code().starts_with("28"))
                    .unwrap_or(false),
                _ => false,
            };
        }
    }
    false
}

/// One shared retry policy for both upstream and Readyset startup. Runs
/// `connect_and_setup` on each attempt, with real exponential backoff,
/// per-attempt logging, and fast-fail on auth/permission errors so a bad
/// URL or missing privilege surfaces immediately instead of blocking on
/// the full retry budget. Final error context records the attempt count
/// and total elapsed time so logs name the budget that was consumed.
async fn connect_and_setup_with_retry(
    url: &DatabaseURL,
    retries: usize,
    base_delay: Duration,
    backoff_multiplier: u32,
) -> anyhow::Result<DatabaseConnection> {
    let start = std::time::Instant::now();
    let mut attempt: usize = 0;
    let mut delay = base_delay;
    loop {
        match connect_and_setup(url).await {
            Ok(conn) => {
                if attempt > 0 {
                    info!(
                        attempt,
                        elapsed_ms = start.elapsed().as_millis() as u64,
                        "connected after retries"
                    );
                }
                return Ok(conn);
            }
            Err(err) if is_permanent_connection_error(&err) => {
                return Err(err).with_context(|| {
                    format!(
                        "connection error appears permanent (auth/permission); \
                         not retrying ({:?} elapsed)",
                        start.elapsed()
                    )
                });
            }
            Err(err) if attempt >= retries => {
                return Err(err).with_context(|| {
                    format!(
                        "exhausted {retries} connect retries over {:?}",
                        start.elapsed()
                    )
                });
            }
            Err(err) => {
                warn!(
                    attempt,
                    delay_ms = delay.as_millis() as u64,
                    err = %format!("{err:#}"),
                    "connect attempt failed, retrying after backoff"
                );
                tokio::time::sleep(delay).await;
                attempt += 1;
                delay = delay
                    .checked_mul(backoff_multiplier)
                    .unwrap_or(Duration::from_secs(60));
            }
        }
    }
}

/// Run `op` on `conn`. If it fails with a connection-drop error, replace
/// `conn` with a freshly-set-up connection and retry once. All other errors
/// propagate immediately.
async fn with_reconnect_on_drop<T, F>(
    conn: &mut DatabaseConnection,
    url: &DatabaseURL,
    op_name: &str,
    op: F,
) -> anyhow::Result<T>
where
    F: AsyncFn(&mut DatabaseConnection) -> anyhow::Result<T>,
{
    match op(conn).await {
        Ok(v) => Ok(v),
        Err(e) if is_connection_drop(&e) => {
            warn!(
                op = op_name,
                err = %format!("{e:#}"),
                "connection dropped, reconnecting and retrying"
            );
            *conn = reconnect(url).await?;
            op(conn).await
        }
        Err(e) => Err(e),
    }
}

/// Execute a SELECT query, using prepare/execute if params are present.
async fn execute_select(
    conn: &mut DatabaseConnection,
    sql: &str,
    params: &[Value],
) -> anyhow::Result<Vec<Vec<Value>>> {
    let sql_owned = sql.to_string();
    let results = if params.is_empty() {
        with_op_timeout("SELECT", conn.query(&sql_owned)).await?
    } else {
        let query_params = QueryParams::PositionalParams(params.to_vec());
        with_op_timeout("SELECT", conn.execute(&sql_owned, query_params)).await?
    };

    Ok(<Vec<Vec<Value>>>::try_from(results)?)
}

#[derive(Debug)]
enum ComparisonOutcome {
    Match,
    Mismatch { reason: String },
}

/// Compare upstream and Readyset query results row-by-row.
///
/// When `has_order_by` is false, sort each side's outer `Vec<row>` lex-order
/// over the row tuple before comparing. This preserves row boundaries,
/// unlike a flatten-then-sort approach which could fake matches for
/// row-permuted-but-cell-shuffled pairs (e.g. `(1,a),(2,b)` vs
/// `(1,b),(2,a)`).
///
/// Both sort and cell equality use [`Value::cmp_compat`] / [`Value::value_eq`]
/// so cross-variant numeric values (`Integer` <-> `UnsignedInteger` <->
/// `Numeric`) and naive vs offset-aware datetimes that represent the same
/// wall value sort into the same row positions and compare equal.
fn compare_results(
    upstream: &[Vec<Value>],
    readyset: &[Vec<Value>],
    has_order_by: bool,
) -> ComparisonOutcome {
    if upstream.len() != readyset.len() {
        return ComparisonOutcome::Mismatch {
            reason: format!(
                "Row count mismatch: upstream returned {} rows, readyset returned {} rows",
                upstream.len(),
                readyset.len(),
            ),
        };
    }

    fn cmp_rows(a: &[Value], b: &[Value]) -> std::cmp::Ordering {
        a.iter()
            .zip(b.iter())
            .map(|(x, y)| x.cmp_compat(y))
            .find(|o| !o.is_eq())
            .unwrap_or_else(|| a.len().cmp(&b.len()))
    }

    type Rows = Vec<Vec<Value>>;
    let sorted: Option<(Rows, Rows)> = (!has_order_by).then(|| {
        let mut u = upstream.to_vec();
        let mut r = readyset.to_vec();
        u.sort_by(|x, y| cmp_rows(x, y));
        r.sort_by(|x, y| cmp_rows(x, y));
        (u, r)
    });
    let (us, rs): (&[Vec<Value>], &[Vec<Value>]) = match &sorted {
        Some((u, r)) => (u, r),
        None => (upstream, readyset),
    };

    for (i, (urow, rrow)) in us.iter().zip(rs.iter()).enumerate() {
        if urow.len() != rrow.len() {
            return ComparisonOutcome::Mismatch {
                reason: format!(
                    "Column count mismatch at row {i}: upstream {}, readyset {}",
                    urow.len(),
                    rrow.len(),
                ),
            };
        }
        for (j, (a, b)) in urow.iter().zip(rrow.iter()).enumerate() {
            if !a.value_eq(b) {
                let preview_end = (i + 5).min(us.len());
                return ComparisonOutcome::Mismatch {
                    reason: format!(
                        "Cell mismatch at row {i}, col {j} (showing rows [{i}..{preview_end}] of {}):\n\
                         expected (upstream): {:?}\n\
                         actual (readyset):   {:?}",
                        us.len(),
                        &us[i..preview_end],
                        &rs[i..preview_end],
                    ),
                };
            }
        }
    }

    ComparisonOutcome::Match
}

/// Single classification of any error the harness sees during a query
/// attempt. Replaces three earlier order-dependent predicates that mixed
/// MySQL-only structured downcasts with substring matches and silently fell
/// through on Postgres.
///
/// Call sites consult one variant each:
/// - `Fatal` aborts the run (caller returns `Err`).
/// - `UpstreamKnownLimit` warns and skips silently — a real dialect
///   limitation we can't generate around.
/// - `UpstreamGeneratorBug` records an Antithesis `assert_unreachable!` so
///   the bad-SQL pattern surfaces in triage, then skips and continues.
/// - `ReadysetTransient` retries with backoff (cache may not be populated
///   yet).
/// - `Other` falls through to the caller's default handling.
#[derive(Debug, Clone, PartialEq, Eq)]
enum ErrorClass {
    Fatal,
    UpstreamKnownLimit {
        code: Option<u16>,
    },
    UpstreamGeneratorBug {
        code: Option<u16>,
    },
    /// Upstream-side transient infrastructure error (e.g. query timeout
    /// during an Antithesis-injected network partition). Tolerable under
    /// `--keep-going`: the probe is skipped with an event but the driver
    /// keeps running so a single fault doesn't halt the whole oracle.
    UpstreamTransient,
    ReadysetTransient,
    Other,
}

/// MySQL error codes we accept as real planner limitations, NOT generator
/// bugs. Anything off this list that comes back as a server error is
/// classified as `UpstreamGeneratorBug` so the upstream's complaint is
/// surfaced as an Antithesis assertion instead of being swept under
/// `skipped`. Historically the allowlist included 1054/1055/1060/1140/1525
/// (column-not-found, ambiguous, dup-alias, group-mix, bad-datetime) — those
/// are all our bugs and now flow through the `GeneratorBug` arm.
const MYSQL_KNOWN_LIMIT_CODES: &[u16] = &[
    1038, // ER_OUT_OF_SORTMEMORY: query exceeds MySQL's sort buffer.
];

const READYSET_TRANSIENT_NEEDLES: &[&str] = &[
    "Could not find table",
    "could not find table",
    "Couldn't find table",
    "schema generation mismatch",
];

/// Classify a query-execution error into an `ErrorClass`.
///
/// The same classification is used at both call sites, but they act on the
/// classes differently and that asymmetry is load-bearing: the upstream-SELECT
/// site skips and continues on `UpstreamKnownLimit` (the oracle cannot read the
/// upstream result, so the query is unevaluable), while the Readyset-SELECT site
/// has no skip arm for it and lets it fall through to the "Readyset SELECT
/// errored" assertion. Do not add an `UpstreamKnownLimit` skip arm at the
/// Readyset site: that would silently mask a Readyset-only divergence (e.g.
/// Readyset returning a type the oracle cannot decode that upstream returned
/// cleanly).
fn classify_error(err: &anyhow::Error) -> ErrorClass {
    use database_utils::DatabaseError;
    use mysql_async::Error as MyErr;

    for cause in err.chain() {
        if let Some(db) = cause.downcast_ref::<DatabaseError>() {
            return match db {
                DatabaseError::MySQL(MyErr::Server(e)) => classify_mysql_code(e.code),
                DatabaseError::MySQL(my) if my.is_fatal() => ErrorClass::Fatal,
                DatabaseError::MySQL(_) => ErrorClass::Other,
                DatabaseError::PostgreSQL(pg) => {
                    if pg.is_closed() {
                        ErrorClass::Fatal
                    } else if let Some(db) = pg.as_db_error() {
                        classify_pg_sqlstate(db.code())
                    } else if pg_error_is_client_decode(&pg.to_string()) {
                        // Client-side decode failure: the oracle's Value type
                        // does not handle the PG column type (e.g.
                        // _timestamp). This is a generator-side limitation,
                        // not a server or infrastructure failure. Treat as a
                        // known limit so --keep-going skips the query instead
                        // of aborting the run.
                        ErrorClass::UpstreamKnownLimit { code: None }
                    } else {
                        // Other non-db, non-closed error (IO/protocol).
                        ErrorClass::Fatal
                    }
                }
                DatabaseError::UpstreamQueryTimeout | DatabaseError::UpstreamConnectionNone => {
                    // Antithesis-injected network partitions and slow disks
                    // surface as query timeouts and connection-none; treat as
                    // transient so --keep-going lets the driver continue.
                    ErrorClass::UpstreamTransient
                }
                _ => ErrorClass::Other,
            };
        }
    }

    let msg = format!("{err:#}");
    if READYSET_TRANSIENT_NEEDLES.iter().any(|n| msg.contains(n)) {
        return ErrorClass::ReadysetTransient;
    }
    ErrorClass::Other
}

/// Whether an upstream SELECT error of this class may be skipped under
/// `--keep-going`. Upstream is the deterministic source of truth, so only
/// transient infrastructure errors are tolerable; an `Other` error (or a
/// misclassified Readyset-known-bug) on the upstream path must propagate so a
/// real upstream failure is never masked as a benign "errored".
fn upstream_error_tolerable_under_keep_going(class: &ErrorClass) -> bool {
    matches!(
        class,
        ErrorClass::ReadysetTransient | ErrorClass::UpstreamTransient,
    )
}

/// Final per-query classification used by all exit paths.
#[derive(Debug, Clone, Copy, PartialEq)]
enum QueryTally {
    Matched,
    Mismatched,
    Errored,
    Inconclusive,
}

/// Classify a query from its per-probe tallies and whether a probe errored out
/// early (e.g. a transient swallowed under --keep-going). A divergence recorded
/// by ANY probe makes the query a mismatch: it is never downgraded to a benign
/// error even when a *later* probe errors out. Only with zero recorded
/// divergences does an early error determine the outcome.
fn classify_query(
    matched_probes: usize,
    mismatched_probes: usize,
    errored_early: bool,
) -> QueryTally {
    if mismatched_probes > 0 {
        QueryTally::Mismatched
    } else if errored_early {
        QueryTally::Errored
    } else if matched_probes > 0 {
        QueryTally::Matched
    } else {
        QueryTally::Inconclusive
    }
}

fn classify_mysql_code(code: u16) -> ErrorClass {
    if MYSQL_KNOWN_LIMIT_CODES.contains(&code) {
        ErrorClass::UpstreamKnownLimit { code: Some(code) }
    } else {
        ErrorClass::UpstreamGeneratorBug { code: Some(code) }
    }
}

/// Classify a Postgres server-side SQLSTATE code.
///
/// Data-dependent errors that the generator cannot avoid (numeric overflow,
/// division by zero) are `UpstreamKnownLimit`, mirroring MySQL code 1038.
/// All other server rejections are `UpstreamGeneratorBug` so the bad-SQL
/// pattern surfaces as an Antithesis assertion.
fn classify_pg_sqlstate(state: &tokio_postgres::error::SqlState) -> ErrorClass {
    use tokio_postgres::error::SqlState;
    if *state == SqlState::NUMERIC_VALUE_OUT_OF_RANGE || *state == SqlState::DIVISION_BY_ZERO {
        ErrorClass::UpstreamKnownLimit { code: None }
    } else {
        ErrorClass::UpstreamGeneratorBug { code: None }
    }
}

/// True when a tokio_postgres error message indicates a client-side
/// deserialization failure (the oracle's `Value` type does not handle the
/// PG column type). The tokio_postgres display for `Kind::FromSql(idx)` is
/// "error deserializing column <idx>: <cause>".
///
/// This is the testable surface for the decode-error detection in
/// `classify_error`: we cannot construct a `tokio_postgres::Error` with a
/// specific `Kind` in unit tests because those constructors are `pub(crate)`,
/// so we factor the check onto the string representation.
///
/// This prefix match is brittle: it depends on tokio_postgres's `Display` for
/// `Kind::FromSql`. If a dependency bump changes that wording, decode errors
/// stop matching and fall through to the `Fatal` arm in `classify_error`,
/// reintroducing the run-aborting crash this guards against. The unit tests
/// assert against a hardcoded copy of the message, so they will NOT catch such
/// drift on their own — an integration run that triggers a real FromSql error
/// (e.g. `ARRAY_AGG` over a timestamp column) is the durable guard.
fn pg_error_is_client_decode(msg: &str) -> bool {
    msg.starts_with("error deserializing column")
}

struct SentinelInfo {
    sentinel_text: String,
}

/// Walk `sql` and invoke `on_placeholder` for each dialect-specific placeholder
/// in order. `on_placeholder` is passed the positional index (0-based) and must
/// return the replacement text. Everything else is copied verbatim.
///
/// MySQL placeholders are `?` in emission order (positional). Postgres
/// placeholders are `$1`, `$2`, ... and may appear in any order in the SQL, but
/// the generator emits them sequentially starting at 1, so we accept both
/// interleaved orderings by consulting the `$N` number directly.
fn replace_placeholders(
    sql: &str,
    dialect: Dialect,
    mut on_placeholder: impl FnMut(usize) -> String,
) -> String {
    let mut result = String::with_capacity(sql.len());
    let mut chars = sql.char_indices().peekable();
    let mut next_q_idx = 0usize;
    while let Some((_, ch)) = chars.next() {
        match (dialect, ch) {
            (Dialect::MySQL, '?') => {
                result.push_str(&on_placeholder(next_q_idx));
                next_q_idx += 1;
            }
            (Dialect::PostgreSQL, '$') => {
                // Collect the digit run following '$'.
                let mut digits = String::new();
                while let Some(&(_, c)) = chars.peek() {
                    if c.is_ascii_digit() {
                        digits.push(c);
                        chars.next();
                    } else {
                        break;
                    }
                }
                if digits.is_empty() {
                    // Literal '$' with no number — keep as-is.
                    result.push('$');
                } else {
                    // Postgres placeholders are 1-based.
                    let n: usize = digits.parse().unwrap_or(0);
                    let idx = n.saturating_sub(1);
                    result.push_str(&on_placeholder(idx));
                }
            }
            _ => result.push(ch),
        }
    }
    result
}

fn inline_sentinels(sql: &str, params: &[Value], dialect: Dialect) -> (String, Vec<SentinelInfo>) {
    let mut sentinels: Vec<Option<SentinelInfo>> = (0..params.len()).map(|_| None).collect();
    let result = replace_placeholders(sql, dialect, |idx| {
        if idx >= params.len() {
            return String::new();
        }
        let sentinel = match &params[idx] {
            Value::Integer(_) => format!("{}", 990001 + idx as i64),
            Value::Real(_) => format!("{}.{}", 990001 + idx as i64, 5),
            Value::Text(_) => format!("'__sentinel_{}__'", idx),
            Value::Null => "NULL".to_string(),
            _ => format!("{}", 990001 + idx as i64),
        };
        sentinels[idx] = Some(SentinelInfo {
            sentinel_text: sentinel.clone(),
        });
        sentinel
    });
    // If a placeholder was never encountered (shouldn't happen for generator
    // output), leave a plausible sentinel so downstream accounting stays sane.
    let sentinels: Vec<SentinelInfo> = sentinels
        .into_iter()
        .enumerate()
        .map(|(idx, s)| {
            s.unwrap_or(SentinelInfo {
                sentinel_text: format!("{}", 990001 + idx as i64),
            })
        })
        .collect();
    (result, sentinels)
}

fn inline_params(sql: &str, params: &[Value], dialect: Dialect) -> String {
    // Quote a string-shaped literal, escaping embedded single quotes.
    let quote = |s: &str| format!("'{}'", s.replace('\'', "''"));
    replace_placeholders(sql, dialect, |idx| {
        let Some(param) = params.get(idx) else {
            return "NULL".to_string();
        };
        // Exhaustive on purpose: a new `Value` variant must be given a literal
        // form here rather than silently binding NULL, which would make a
        // divergence's repro script reproduce the wrong (null) value.
        match param {
            Value::Integer(n) => n.to_string(),
            Value::UnsignedInteger(n) => n.to_string(),
            Value::Real(bits) => {
                let v = f64::from_bits(*bits);
                if v.is_finite() {
                    // `{v:?}` renders a round-trippable decimal (`2.5`, `2.0`).
                    format!("{v:?}")
                } else {
                    // NaN / +/-inf have no portable SQL literal; mark it so the
                    // repro is visibly non-replayable rather than silently wrong.
                    "/* non-finite real, unrepresentable */ NULL".to_string()
                }
            }
            Value::Numeric(d) => d.to_string(),
            Value::Text(s) => quote(s),
            Value::DateTime(dt) => quote(&dt.format("%Y-%m-%d %H:%M:%S%.6f").to_string()),
            Value::Time(t) => quote(&t.to_string()),
            Value::TimestampTz(ts) => match dialect {
                // MySQL has no TIMESTAMPTZ; project to UTC-naive, mirroring the
                // bind path in `value.rs`.
                Dialect::MySQL => {
                    quote(&ts.naive_utc().format("%Y-%m-%d %H:%M:%S%.6f").to_string())
                }
                Dialect::PostgreSQL => quote(&ts.to_rfc3339()),
            },
            Value::Null => "NULL".to_string(),
            // Rare in dante-generated queries and without a simple cross-dialect
            // literal here; mark visibly instead of silently binding NULL.
            Value::ByteArray(_) => "/* unrepresentable ByteArray param */ NULL".to_string(),
            Value::BitVector(_) => "/* unrepresentable BitVector param */ NULL".to_string(),
            Value::Json(_) => "/* unrepresentable Json param */ NULL".to_string(),
        }
    })
}

/// Result of `EXPLAIN CREATE CACHE FROM <query>`.
struct ExplainCreateCacheResult {
    /// The query ID that readyset assigns to the normalized form.
    query_id: String,
    /// The rewritten (autoparameterized) query text.
    query_text: String,
}

async fn explain_create_cache(
    readyset: &mut DatabaseConnection,
    sql: &str,
) -> Option<ExplainCreateCacheResult> {
    let explain_sql = format!("EXPLAIN CREATE CACHE FROM {sql}");
    let results = match with_op_timeout("EXPLAIN CREATE CACHE", readyset.query(&explain_sql)).await
    {
        Ok(r) => r,
        Err(err) => {
            debug!(%err, "EXPLAIN CREATE CACHE failed");
            return None;
        }
    };
    // Result is a single row with columns: [query_id, query_text, supported].
    let rows = <Vec<Vec<Value>>>::try_from(results).ok()?;
    let row = rows.first()?;
    Some(ExplainCreateCacheResult {
        query_id: row.first()?.to_string(),
        query_text: row.get(1)?.to_string(),
    })
}

fn classify_autoparameterization(
    rewritten_sql: &str,
    sentinels: &[SentinelInfo],
) -> (Vec<usize>, Vec<usize>) {
    let mut parameterized = Vec::new();
    let mut not_parameterized = Vec::new();
    for (idx, sentinel) in sentinels.iter().enumerate() {
        if rewritten_sql.contains(&sentinel.sentinel_text) {
            not_parameterized.push(idx);
        } else {
            parameterized.push(idx);
        }
    }
    (parameterized, not_parameterized)
}

/// Whether the two-literal cache-reuse assertion is valid for a query.
///
/// Reuse can only be asserted when EVERY literal that varies between the two probe variants was
/// autoparameterized. If any literal stays inline — e.g. a constant inside `substring(...)` /
/// `month(...)`, or any position Readyset does not parameterize — then two literal variants
/// legitimately canonicalize to DIFFERENT cache entries (the inline positions differ), so
/// asserting reuse would be a false positive. Requires at least one parameterized literal so there
/// is something to check.
fn should_assert_autoparam_reuse(parameterized: &[usize], not_parameterized: &[usize]) -> bool {
    !parameterized.is_empty() && not_parameterized.is_empty()
}

async fn query_cache_mode(readyset: &mut DatabaseConnection) -> CacheMode {
    let results = match with_op_timeout(
        "EXPLAIN LAST STATEMENT",
        readyset.query("EXPLAIN LAST STATEMENT"),
    )
    .await
    {
        Ok(r) => r,
        Err(err) => {
            warn!(%err, "EXPLAIN LAST STATEMENT failed");
            return CacheMode::Unknown;
        }
    };
    let rows = match <Vec<Vec<Value>>>::try_from(results) {
        Ok(r) => r,
        Err(_) => return CacheMode::Unknown,
    };
    for row in &rows {
        if row.len() >= 2 {
            let key = row[0].to_string();
            let val = row[1].to_string();
            if key.contains("Query_destination") {
                if val.starts_with("readyset(") || val == "readyset" {
                    return CacheMode::Deep;
                } else if val == "readyset_shallow" {
                    return CacheMode::Shallow;
                } else {
                    return CacheMode::Proxy;
                }
            }
        }
    }
    CacheMode::Unknown
}

/// Returns true if `query` references the SQL identifier `name` at a word
/// boundary — guards against false positives like `t1` matching `t10` or
/// `users` matching `users_audit` when `tables.contains(...)` would
/// erroneously fire. Strict ASCII word-character boundary; SQL identifier
/// charset is alphanumeric + underscore.
fn query_mentions_identifier(query: &str, name: &str) -> bool {
    if name.is_empty() {
        return false;
    }
    let bytes = query.as_bytes();
    let nlen = name.len();
    let nbytes = name.as_bytes();
    let mut i = 0;
    while i + nlen <= bytes.len() {
        if &bytes[i..i + nlen] == nbytes {
            let before_ok = i == 0 || !is_ident_byte(bytes[i - 1]);
            let after_ok = i + nlen == bytes.len() || !is_ident_byte(bytes[i + nlen]);
            if before_ok && after_ok {
                return true;
            }
        }
        i += 1;
    }
    false
}

fn is_ident_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

/// True if a `SHOW CACHES` props column marks a deep cache. Matches `deep` on
/// a word boundary so values like `deeper` don't false-positive -- the same
/// boundary guard `count_caches_detailed` applies to the query column.
fn cache_props_is_deep(props: &str) -> bool {
    query_mentions_identifier(props, "deep")
}

async fn count_caches_for_tables(
    readyset: &mut DatabaseConnection,
    tables: &HashSet<SqlIdentifier>,
) -> usize {
    let (deep, _) = count_caches_detailed(readyset, tables).await;
    deep
}

async fn count_caches_detailed(
    readyset: &mut DatabaseConnection,
    tables: &HashSet<SqlIdentifier>,
) -> (usize, usize) {
    let results = match with_op_timeout("SHOW CACHES", readyset.query("SHOW CACHES")).await {
        Ok(r) => r,
        Err(err) => {
            warn!(%err, "SHOW CACHES failed during cache verification");
            return (0, 0);
        }
    };
    let rows = match <Vec<Vec<Value>>>::try_from(results) {
        Ok(r) => r,
        Err(_) => return (0, 0),
    };
    let table_strs: Vec<String> = tables.iter().map(|t| t.to_string()).collect();
    let mut deep = 0;
    let mut shallow = 0;
    let mut shallow_queries: Vec<String> = Vec::new();
    for row in &rows {
        let mentions_table = matches!(
            row.get(2),
            Some(Value::Text(qt))
                if table_strs.iter().any(|t| query_mentions_identifier(qt, t))
        );
        if !mentions_table {
            continue;
        }
        if matches!(row.get(3), Some(Value::Text(props)) if cache_props_is_deep(props)) {
            deep += 1;
        } else {
            shallow += 1;
            if let Some(Value::Text(qt)) = row.get(2) {
                shallow_queries.push(qt.clone());
            }
        }
    }
    if shallow > 0 {
        warn!(
            shallow,
            deep,
            "some caches are shallow (not deep) — queries may not be fully exercising the dataflow"
        );
        for sq in &shallow_queries {
            warn!(query = %sq, "shallow cache query");
        }
    }
    (deep, shallow)
}

async fn query_count(conn: &mut DatabaseConnection, table: &SqlIdentifier) -> anyhow::Result<i64> {
    let count_sql = format!("SELECT count(*) FROM {}", table);
    let results = with_op_timeout("COUNT(*)", conn.query(&count_sql)).await?;
    let rows = <Vec<Vec<Value>>>::try_from(results)?;
    // Propagate parse failure so wait_for_replication doesn't declare progress
    // when both sides happen to fail-equal at -1.
    match rows.first().and_then(|r| r.first()) {
        Some(Value::Text(s)) => s
            .parse::<i64>()
            .with_context(|| format!("parsing COUNT(*) text {s:?} from {table}")),
        Some(Value::Integer(n)) => Ok(*n),
        Some(other) => bail!("unexpected COUNT(*) value type: {other:?}"),
        None => bail!("COUNT(*) returned no rows for {table}"),
    }
}

async fn online_tables(
    readyset: &mut DatabaseConnection,
) -> anyhow::Result<std::collections::HashSet<String>> {
    // Must use simple_query (not query) because the PostgreSQL extended query
    // protocol sends SHOW READYSET TABLES through the prepare path, which
    // forwards unrecognized statements to upstream where it fails.
    let results = with_op_timeout(
        "SHOW READYSET TABLES",
        readyset.simple_query("SHOW READYSET TABLES"),
    )
    .await?;
    let mut online = std::collections::HashSet::new();
    for row in results {
        let table_name = row.get(0)?;
        let status = row.get(1)?;
        if status == "Online" {
            let bare = table_name
                .rsplit('.')
                .next()
                .unwrap_or(&table_name)
                .trim_matches('`')
                .trim_matches('"')
                .to_string();
            online.insert(bare);
        }
    }
    Ok(online)
}

async fn wait_for_replication(
    upstream: &mut DatabaseConnection,
    readyset: &mut DatabaseConnection,
    tables: &[&SqlIdentifier],
    timeout: Duration,
    readyset_mode: bool,
) -> anyhow::Result<()> {
    let start = std::time::Instant::now();
    let poll_interval = Duration::from_millis(200);

    // Phase 1: Wait for row counts to match (data replication).
    for table in tables {
        let upstream_count = query_count(upstream, table)
            .await
            .with_context(|| format!("counting rows on upstream for {table}"))?;

        loop {
            match query_count(readyset, table).await {
                Ok(rs_count) if rs_count == upstream_count => break,
                Ok(rs_count) => {
                    if start.elapsed() > timeout {
                        bail!(
                            "Timed out waiting for table {table} row count to match: \
                             upstream={upstream_count}, readyset={rs_count}"
                        );
                    }
                    debug!(
                        table = %table,
                        upstream_count,
                        readyset_count = rs_count,
                        elapsed_ms = start.elapsed().as_millis(),
                        "waiting for data to replicate"
                    );
                }
                Err(_) => {
                    if start.elapsed() > timeout {
                        bail!("Timed out waiting for table {table} to be visible on Readyset");
                    }
                    debug!(
                        table = %table,
                        elapsed_ms = start.elapsed().as_millis(),
                        "waiting for table to be visible on Readyset"
                    );
                }
            }
            tokio::time::sleep(poll_interval).await;
        }
    }

    // Phase 2: Wait for SHOW READYSET TABLES to report Online status.
    // Runs only in --readyset-mode: a transparent proxy like SQP has no
    // Readyset-specific status surface, and the row-count phase above
    // already proves the proxy sees the same rows as upstream.
    if !readyset_mode {
        return Ok(());
    }
    let table_names: Vec<String> = tables.iter().map(|t| t.to_string()).collect();
    loop {
        match online_tables(readyset).await {
            Ok(online_set) => {
                let all_online = table_names.iter().all(|t| online_set.contains(t));
                if all_online {
                    break;
                }
                if start.elapsed() > timeout {
                    let missing: Vec<_> = table_names
                        .iter()
                        .filter(|t| !online_set.contains(t.as_str()))
                        .collect();
                    bail!(
                        "Timed out waiting for tables to be Online in SHOW READYSET TABLES: {missing:?}"
                    );
                }
                debug!(
                    elapsed_ms = start.elapsed().as_millis(),
                    online_count = online_set.len(),
                    needed = table_names.len(),
                    "waiting for tables to reach Online status"
                );
            }
            Err(err) => {
                if start.elapsed() > timeout {
                    bail!("Timed out waiting for SHOW READYSET TABLES: {err:#}");
                }
                debug!(
                    elapsed_ms = start.elapsed().as_millis(),
                    "SHOW READYSET TABLES failed, retrying"
                );
            }
        }
        tokio::time::sleep(poll_interval).await;
    }

    let elapsed = start.elapsed();
    if elapsed > Duration::from_millis(50) {
        debug!(
            elapsed_ms = elapsed.as_millis(),
            "replication wait completed"
        );
    }

    Ok(())
}

async fn connect_and_setup(url: &DatabaseURL) -> anyhow::Result<DatabaseConnection> {
    use database_utils::tls::ServerCertVerification;

    let mut conn = with_op_timeout("connect", url.connect(&ServerCertVerification::Default))
        .await
        .with_context(|| format!("connecting to {url:?}"))?;

    if url.is_mysql() {
        with_op_timeout(
            "SET SESSION wait_timeout",
            conn.query_drop("SET SESSION wait_timeout = 600"),
        )
        .await
        .context("setting MySQL wait_timeout")?;
        with_op_timeout(
            "SET SESSION time_zone",
            conn.query_drop("SET SESSION time_zone = '+00:00'"),
        )
        .await
        .context("setting MySQL time_zone to UTC")?;
    } else if url.is_postgres() {
        with_op_timeout(
            "CREATE EXTENSION citext",
            conn.query_drop("CREATE EXTENSION IF NOT EXISTS citext"),
        )
        .await
        .context("creating citext extension")?;
    }

    Ok(conn)
}

/// Fuzz-test Readyset using the constraint-based SQL generator.
#[derive(Parser, Debug, Clone)]
struct ConstraintFuzz {
    /// Total number of queries to generate and test.
    #[arg(long, short = 'n', default_value = "100")]
    num_queries: usize,

    /// Number of rows to insert per table when a new table is created.
    #[arg(long, default_value = "100")]
    rows_per_table: usize,

    /// Seed for the random number generator.
    #[arg(long)]
    seed: Option<u64>,

    /// Use the Antithesis entropy source instead of a random or fixed seed.
    #[arg(long)]
    antithesis_entropy: bool,

    /// URL of the upstream reference database.
    #[arg(long)]
    compare_to: String,

    /// URL of the Readyset instance to test.
    #[arg(long)]
    readyset_url: String,

    /// Per-op timeout for individual DB calls, in seconds. Bounds every
    /// upstream/Readyset query so that fault injection (Antithesis packet
    /// drops, syscall stalls) cannot stall a run forever.
    #[arg(long, default_value = "30")]
    db_op_timeout_secs: u64,

    /// Write a SQL reproduction script to this path.
    ///
    /// The file is written on error, or at the end of every run if this flag is
    /// set.  It contains all DDL, INSERT, and SELECT statements in execution
    /// order, forming a self-contained script that reproduces the database state
    /// at the point of failure.
    #[arg(long)]
    dump_repro: Option<PathBuf>,

    /// Restrict pattern selection to patterns carrying ALL of these tags.
    /// Comma-separated. Maps directly to
    /// `dante::compat::SelectionFilter::required_tags`.
    ///
    /// Example: `--required-tags expr_eval` runs only the
    /// expression-evaluation patterns so `bug_bait` coercion rows are
    /// exercised reliably instead of being washed out by the default
    /// 143-pattern primary-pick distribution.
    #[arg(long, value_delimiter = ',')]
    required_tags: Vec<String>,

    /// Exclude patterns carrying ANY of these tags. Comma-separated.
    /// Maps directly to `dante::compat::SelectionFilter::excluded_tags`.
    #[arg(long, value_delimiter = ',')]
    excluded_tags: Vec<String>,

    /// Skip composition: each iteration picks ONE pattern and resolves it
    /// alone via `resolver::try_resolve`. Composition would otherwise pair
    /// patterns with partners like `self_join` or aggregate compose helpers
    /// that block `CREATE DEEP CACHE` (Readyset rejects self-joins-on-same-
    /// column / unsupported placeholder positions), which forces the query
    /// to proxy upstream and hides expression-eval divergences. Use with
    /// `--required-tags expr_eval` for the focused expression-eval bug
    /// coverage flow.
    #[arg(long)]
    single_pattern: bool,

    /// Continue past non-fatal terminal SELECT errors instead of aborting
    /// the run. Errors classified as `ErrorClass::Other` from either the
    /// upstream or Readyset side are logged, surfaced as Antithesis
    /// assertion failures, counted as `errored`, and the loop moves on.
    /// `ErrorClass::Fatal` (closed connection) still bails because the
    /// session cannot be reused safely. Useful for collecting full
    /// coverage data during soak runs that would otherwise stop at the
    /// first novel Readyset bug.
    #[arg(long)]
    keep_going: bool,

    /// Treat the `--readyset-url` endpoint as a real Readyset instance and
    /// exercise its Readyset-specific surfaces. Enables: the deep-cache
    /// compatibility rules in the generator (skip shapes Readyset cannot
    /// deep-cache), `CREATE DEEP CACHE FROM ...` per query, the
    /// `EXPLAIN CREATE CACHE` autoparameterization probes, the
    /// `EXPLAIN LAST STATEMENT` cache-routing probe, and the
    /// `SHOW READYSET TABLES` online-status wait.
    ///
    /// Off by default: the endpoint is treated as a transparent proxy
    /// (e.g. SQP), so none of the Readyset-specific DDL or probes run.
    /// Compound SELECTs still get their helper VIEW so the same SQL runs
    /// on both sides.
    #[arg(long)]
    readyset_mode: bool,
}

impl ConstraintFuzz {
    fn dialect(&self) -> anyhow::Result<Dialect> {
        let url = DatabaseURL::from_str(&self.compare_to)
            .with_context(|| format!("parsing compare_to URL {:?}", self.compare_to))?;
        Ok(match url {
            DatabaseURL::MySQL(_) => Dialect::MySQL,
            DatabaseURL::PostgreSQL(_) => Dialect::PostgreSQL,
        })
    }

    fn make_rng(&self) -> Box<dyn Rng> {
        if let Some(seed) = self.seed {
            Box::new(StdRng::seed_from_u64(seed))
        } else if self.antithesis_entropy {
            Box::new(AntithesisRngAdapter)
        } else {
            Box::new(StdRng::try_from_rng(&mut SysRng).expect("SysRng should not fail"))
        }
    }

    #[tokio::main]
    async fn run(&self) -> anyhow::Result<()> {
        let upstream_url =
            DatabaseURL::from_str(&self.compare_to).context("parsing compare_to URL")?;
        let readyset_url =
            DatabaseURL::from_str(&self.readyset_url).context("parsing readyset_url")?;

        // Set the per-op timeout once for the lifetime of the process.
        // Subsequent .set() calls would error; re-running `run` (e.g., from
        // tests) is not supported in the same process.
        let _ = DB_OP_TIMEOUT.set(Duration::from_secs(self.db_op_timeout_secs));

        let dialect = self.dialect()?;
        let mut rng = self.make_rng();

        let seed_display = if self.antithesis_entropy {
            "antithesis".to_string()
        } else {
            self.seed.map_or("random".to_string(), |s| s.to_string())
        };
        info!(
            seed = %seed_display,
            num_queries = self.num_queries,
            rows_per_table = self.rows_per_table,
            ?dialect,
            "starting readyset-dante-oracle"
        );

        let mut stats = PatternStats::default();

        // Seed stats with all registered pattern names so we can detect
        // patterns that were never generated, and reject typo'd tag filters
        // before any DB work.
        {
            let registry = dante::ConstraintRegistry::default_registry();
            for name in registry.pattern_names() {
                stats.counts.entry(name).or_default();
            }
            validate_known_tags(
                self.required_tags
                    .iter()
                    .chain(&self.excluded_tags)
                    .map(String::as_str),
                &registry.all_tags(),
            )?;
        }

        // 5 retries × exponential backoff (1s, 2s, 4s, 8s, 16s ≈ 31s budget)
        // through the shared helper introduced in uuszulpw, so upstream and
        // Readyset use one policy. Auth/permission errors bypass retries via
        // is_permanent_connection_error.
        let mut upstream =
            connect_and_setup_with_retry(&upstream_url, 5, Duration::from_secs(1), 2)
                .await
                .context("connecting to upstream")?;
        let mut readyset =
            connect_and_setup_with_retry(&readyset_url, 5, Duration::from_secs(1), 2)
                .await
                .context("connecting to readyset")?;
        assert_reachable!("Connected to both upstream and Readyset", &json!({}));

        // Open the repro log up front when the user asked for an explicit
        // dump path so writes stream directly through a BufWriter and memory
        // stays bounded regardless of run length. Otherwise keep the bounded
        // ring buffer only and dump it to stderr on error.
        let mut repro = match self.dump_repro.clone() {
            Some(path) => ReproLog::with_writer(path, dialect, seed_display.clone())
                .context("opening reproduction script for streaming write")?,
            None => ReproLog::new(dialect, seed_display.clone()),
        };

        // Build the SelectionFilter from CLI arg slices. Empty vecs give
        // the default (no restriction) — same behavior as before.
        let selection_filter = dante::compat::SelectionFilter {
            max_depth: None,
            dialect_support: None,
            required_tags: self
                .required_tags
                .iter()
                .map(String::as_str)
                // `SelectionFilter` holds `&'static str`. CLI strings live for
                // the lifetime of the run, so `Box::leak` is the only way to
                // get a `'static` borrow without re-architecting the filter
                // type.
                .map(|s| -> &'static str { Box::leak(s.to_owned().into_boxed_str()) })
                .collect(),
            excluded_tags: self
                .excluded_tags
                .iter()
                .map(String::as_str)
                .map(|s| -> &'static str { Box::leak(s.to_owned().into_boxed_str()) })
                .collect(),
        };

        let result = run_queries(
            dialect,
            self.num_queries,
            self.rows_per_table,
            &mut *rng,
            &upstream_url,
            &readyset_url,
            &mut upstream,
            &mut readyset,
            &mut stats,
            &mut repro,
            selection_filter,
            self.single_pattern,
            self.keep_going,
            self.readyset_mode,
        )
        .await;

        match (self.dump_repro.is_some(), result.is_err()) {
            (true, _) => {
                // Explicit `--dump-repro`: flush the streamed writer. Failure
                // here surfaces as a Result error (we don't want to exit 0
                // with the artifact silently lost). Always also dump the ring
                // buffer to stderr so the recent suffix is in Antithesis
                // container logs even if the tmpfs path vanished.
                let written = repro
                    .finish()
                    .context("finalizing requested reproduction script")?;
                repro.dump_to_stderr();
                if let Some((path, n)) = written {
                    info!(
                        path = %path.display(),
                        statements = n,
                        "wrote reproduction script"
                    );
                }
            }
            (false, true) => {
                // Fallback path: nothing has been streamed; emit the bounded
                // ring buffer to stderr. The exit status is whatever
                // `run_queries` produced — cleanup never overwrites it.
                repro.dump_to_stderr();
                info!(
                    statements_total = repro.statements_total(),
                    ring_capacity = REPRO_RING_CAPACITY,
                    "dumped reproduction script (most-recent suffix) to stderr"
                );
            }
            (false, false) => {}
        }

        let (matched, mismatched, errored) = result?;

        assert_sometimes!(
            matched > 0,
            "Constraint-fuzz matched at least one query",
            &json!({ "matched": matched, "mismatched": mismatched, "errored": errored })
        );

        stats.log_summary();
        info!(
            matched,
            mismatched, errored, "readyset-dante-oracle complete"
        );

        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    antithesis_init();
    readyset_tracing::init_test_logging();
    let opts = ConstraintFuzz::parse();
    opts.run()
}

#[cfg(test)]
mod tests {
    use dante::entropy::Entropy;
    use dante::resolver::ParamMeta;
    use dante::state::{ColumnMeta, TableSchema};
    use data_generator::ColumnGenerationSpec;
    use rand::SeedableRng;
    use rand::rngs::SmallRng;
    use readyset_sql::ast::{SqlIdentifier, SqlType};

    use super::*;

    fn sample_table() -> TableSchema {
        let mut schema = TableSchema::new(SqlIdentifier::from("users"));
        schema.add_column(
            SqlIdentifier::from("id"),
            ColumnMeta {
                sql_type: SqlType::Int(None),
                gen_spec: ColumnGenerationSpec::Unique,
            },
        );
        schema.add_column(
            SqlIdentifier::from("name"),
            ColumnMeta {
                sql_type: SqlType::VarChar(Some(255)),
                gen_spec: ColumnGenerationSpec::Random,
            },
        );
        schema.add_column(
            SqlIdentifier::from("score"),
            ColumnMeta {
                sql_type: SqlType::Int(None),
                gen_spec: ColumnGenerationSpec::Uniform(DfValue::Int(1), DfValue::Int(100)),
            },
        );
        schema.primary_key = Some(SqlIdentifier::from("id"));
        schema
    }

    #[test]
    fn recorded_divergence_is_never_downgraded_to_error() {
        // A divergence recorded by an earlier probe must classify the
        // query as a mismatch even when a later probe errors out (or is
        // skipped) under --keep-going -- it must not be reported as a benign
        // error.
        // Errored-early but an earlier probe diverged -> still a mismatch.
        assert_eq!(classify_query(0, 1, true), QueryTally::Mismatched);
        // Matched + diverged + errored-early -> the divergence still wins.
        assert_eq!(classify_query(2, 1, true), QueryTally::Mismatched);
        // No divergence: the early error decides.
        assert_eq!(classify_query(1, 0, true), QueryTally::Errored);
        assert_eq!(classify_query(2, 0, false), QueryTally::Matched);
        assert_eq!(classify_query(0, 3, false), QueryTally::Mismatched);
        assert_eq!(classify_query(0, 0, false), QueryTally::Inconclusive);
        // All probes exhausted their retry budget on transient errors with no
        // comparison: errored, never a divergence.
        assert_eq!(classify_query(0, 0, true), QueryTally::Errored);
    }

    #[test]
    fn validate_known_tags_rejects_unknown_tags() {
        let known: std::collections::BTreeSet<&'static str> =
            ["expr_eval", "postgres_only"].into_iter().collect();
        validate_known_tags(["expr_eval", "postgres_only"], &known)
            .expect("known tags must validate");
        let err = validate_known_tags(["expr_eval", "typo_tag"], &known)
            .expect_err("an unknown tag must be rejected");
        assert!(
            err.to_string().contains("typo_tag"),
            "error should name the offending tag: {err}"
        );
    }

    #[test]
    fn upstream_only_tolerates_transient_under_keep_going() {
        // Upstream is the source of truth; under --keep-going only a
        // transient error may be skipped. `Other` must propagate so a real
        // upstream failure isn't masked.
        assert!(upstream_error_tolerable_under_keep_going(
            &ErrorClass::ReadysetTransient
        ));
        assert!(!upstream_error_tolerable_under_keep_going(
            &ErrorClass::Other
        ));
    }

    #[test]
    fn inline_params_emits_replayable_literals() {
        // Numeric/datetime params must emit as replayable literals, and a
        // non-finite Real must not render as a bare `NaN`, so a divergence
        // repro on those param types can be replayed.
        let num = inline_params(
            "x = $1",
            &[Value::Numeric(readyset_decimal::Decimal::from(424242))],
            Dialect::PostgreSQL,
        );
        assert!(
            num.contains("424242") && !num.contains("NULL"),
            "numeric param not emitted as a literal: {num}"
        );

        let nan = inline_params(
            "x = $1",
            &[Value::Real(f64::NAN.to_bits())],
            Dialect::PostgreSQL,
        );
        assert!(
            !nan.contains("NaN"),
            "non-finite real must not render as a bare NaN literal: {nan}"
        );
    }

    #[test]
    fn cache_props_is_deep_requires_word_boundary() {
        // "deep" must match on a word boundary; a raw substring test
        // mis-counts deep caches.
        assert!(cache_props_is_deep("deep"));
        assert!(!cache_props_is_deep("deeper"));
        assert!(!cache_props_is_deep("deepcache"));
    }

    #[test]
    #[should_panic(expected = "not a valid")]
    fn parse_literal_rejects_malformed_numeric_example() {
        // A malformed authored example literal must hard-error rather
        // than silently binding NULL, which would let a bug-bait example pass
        // green while testing nothing.
        let _ = parse_literal("8x", &SqlType::Int(None));
    }

    #[test]
    fn build_insert_renders_bool_column_as_boolean_literal() {
        // dante generates DfValue::Int(0|1) for BOOL columns; Postgres rejects an integer
        // literal for a boolean column (SQLSTATE 42804: "column is of type boolean but
        // expression is of type integer"). The seed INSERT must emit a boolean literal.
        let mut schema = TableSchema::new(SqlIdentifier::from("t0"));
        schema.add_column(
            SqlIdentifier::from("id"),
            ColumnMeta {
                sql_type: SqlType::Int(None),
                gen_spec: ColumnGenerationSpec::Unique,
            },
        );
        schema.add_column(
            SqlIdentifier::from("flag"),
            ColumnMeta {
                sql_type: SqlType::Bool,
                gen_spec: ColumnGenerationSpec::Random,
            },
        );

        let data = vec![vec![DfValue::Int(7), DfValue::Int(1)]];
        let insert = build_insert(&schema, &data).expect("build_insert should succeed");
        let sql = insert.display(Dialect::PostgreSQL).to_string();

        assert!(
            sql.contains("TRUE"),
            "BOOL column must render as a boolean literal (TRUE/FALSE), got: {sql}"
        );
    }

    #[test]
    fn cache_create_retry_budget_under_30s() {
        // Worst-case sleep budget for the CREATE DEEP CACHE retry loop.
        // Mirrors the schedule the macro performs: on each failed attempt
        // sleep `delay`, then `delay *= backoff`. Final attempt does not
        // sleep (loop breaks before sleep).
        const BUDGET_MS: u64 = 30_000;
        let mut total: u64 = 0;
        let mut delay = CACHE_CREATE_BASE_DELAY_MS;
        for _ in 0..CACHE_CREATE_RETRIES {
            total += delay;
            delay = delay.saturating_mul(CACHE_CREATE_BACKOFF);
        }
        assert!(
            total <= BUDGET_MS,
            "cache create retry sleep budget {total}ms exceeds {BUDGET_MS}ms",
        );
    }

    #[test]
    fn table_schema_to_create_table_produces_valid_sql() {
        let schema = sample_table();
        let stmt = table_schema_to_create_table(&schema);
        let sql = stmt.display(Dialect::MySQL).to_string();

        assert!(sql.contains("CREATE TABLE"), "sql: {sql}");
        assert!(sql.contains("`users`"), "sql: {sql}");
        assert!(sql.contains("`id`"), "sql: {sql}");
        assert!(sql.contains("`name`"), "sql: {sql}");
        assert!(sql.contains("`score`"), "sql: {sql}");
        assert!(sql.contains("PRIMARY KEY"), "sql: {sql}");
    }

    #[test]
    fn table_schema_to_create_table_pg_dialect() {
        let schema = sample_table();
        let stmt = table_schema_to_create_table(&schema);
        let sql = stmt.display(Dialect::PostgreSQL).to_string();

        assert!(sql.contains("CREATE TABLE"), "sql: {sql}");
        assert!(sql.contains("\"users\""), "sql: {sql}");
        assert!(sql.contains("PRIMARY KEY"), "sql: {sql}");
    }

    #[test]
    fn table_schema_no_pk() {
        let mut schema = TableSchema::new(SqlIdentifier::from("logs"));
        schema.add_column(
            SqlIdentifier::from("msg"),
            ColumnMeta {
                sql_type: SqlType::Text,
                gen_spec: ColumnGenerationSpec::Random,
            },
        );
        let stmt = table_schema_to_create_table(&schema);
        let sql = stmt.display(Dialect::MySQL).to_string();
        assert!(!sql.contains("PRIMARY KEY"), "sql: {sql}");
    }

    #[test]
    fn generate_rows_produces_correct_count() {
        let schema = sample_table();
        let mut rng = SmallRng::seed_from_u64(42);
        let (rows, _) = generate_rows(&schema, 50, &[], &mut rng);
        assert_eq!(rows.len(), 50);
        for row in &rows {
            assert_eq!(row.len(), 3, "row: {row:?}");
        }
    }

    #[test]
    fn generate_rows_unique_column_produces_unique_values() {
        let schema = sample_table();
        let mut rng = SmallRng::seed_from_u64(42);
        let (rows, _) = generate_rows(&schema, 100, &[], &mut rng);
        let ids: Vec<&DfValue> = rows.iter().map(|r| &r[0]).collect();
        let unique: std::collections::HashSet<_> = ids.iter().collect();
        assert_eq!(
            unique.len(),
            ids.len(),
            "id column should have unique values"
        );
    }

    #[test]
    fn generate_rows_independent_of_column_order() {
        // Reordering schema columns must not change the per-column rows
        // produced for a given seed. This is a determinism property:
        // replay-by-seed should be stable under column reordering. The
        // mechanism is per-column sub-seeding keyed by column name, so
        // each column's data depends only on (parent_seed, name).
        let mk_col = || ColumnMeta {
            sql_type: SqlType::Int(None),
            gen_spec: ColumnGenerationSpec::Uniform(DfValue::Int(0), DfValue::Int(1_000_000)),
        };
        let mut schema_ab = TableSchema::new(SqlIdentifier::from("t"));
        schema_ab.add_column(SqlIdentifier::from("a"), mk_col());
        schema_ab.add_column(SqlIdentifier::from("b"), mk_col());

        let mut schema_ba = TableSchema::new(SqlIdentifier::from("t"));
        schema_ba.add_column(SqlIdentifier::from("b"), mk_col());
        schema_ba.add_column(SqlIdentifier::from("a"), mk_col());

        let mut rng_ab = SmallRng::seed_from_u64(42);
        let mut rng_ba = SmallRng::seed_from_u64(42);
        let (rows_ab, _) = generate_rows(&schema_ab, 5, &[], &mut rng_ab);
        let (rows_ba, _) = generate_rows(&schema_ba, 5, &[], &mut rng_ba);

        for (row_ab, row_ba) in rows_ab.iter().zip(rows_ba.iter()) {
            // schema_ab positions: [a, b]; schema_ba positions: [b, a]
            assert_eq!(
                row_ab[0], row_ba[1],
                "column 'a' should match across orderings"
            );
            assert_eq!(
                row_ab[1], row_ba[0],
                "column 'b' should match across orderings"
            );
        }
    }

    #[test]
    fn build_insert_produces_valid_sql() {
        let schema = sample_table();
        let mut rng = SmallRng::seed_from_u64(42);
        let (rows, _) = generate_rows(&schema, 5, &[], &mut rng);
        let insert = build_insert(&schema, &rows).expect("should build insert");
        let sql = insert.display(Dialect::MySQL).to_string();

        assert!(sql.contains("INSERT INTO"), "sql: {sql}");
        assert!(sql.contains("`users`"), "sql: {sql}");
    }

    #[test]
    fn materialize_params_produces_correct_count() {
        let params = vec![
            ParamMeta {
                sql_type: SqlType::Int(None),
                gen_spec: ColumnGenerationSpec::Uniform(DfValue::Int(1), DfValue::Int(100)),
                count: 3,
            },
            ParamMeta {
                sql_type: SqlType::VarChar(Some(255)),
                gen_spec: ColumnGenerationSpec::Random,
                count: 1,
            },
        ];

        let mut rng = SmallRng::seed_from_u64(42);
        let values = materialize_params(&params, &mut rng);
        assert_eq!(values.len(), 4, "3 + 1 = 4 total params");
    }

    #[test]
    fn materialize_params_empty() {
        let mut rng = SmallRng::seed_from_u64(42);
        let values = materialize_params(&[], &mut rng);
        assert!(values.is_empty());
    }

    #[test]
    fn dfvalue_to_value_conversion() {
        let schema = sample_table();
        let mut rng = SmallRng::seed_from_u64(42);
        let (rows, _) = generate_rows(&schema, 10, &[], &mut rng);
        for row in &rows {
            for val in row {
                let result = Value::try_from(val.clone());
                assert!(
                    result.is_ok(),
                    "DfValue {:?} should convert to Value: {:?}",
                    val,
                    result.err()
                );
            }
        }
    }

    #[test]
    fn end_to_end_generator_produces_executable_ddl() {
        let mut generator = Generator::new(Dialect::MySQL, GeneratorConfig::default());
        let mut rng = SmallRng::seed_from_u64(42);
        let mut entropy = Entropy::new(&mut rng);

        let output = generator
            .generate_with_ddl(&mut entropy)
            .expect("should generate");

        for step in &output.ddl {
            match step {
                DdlStep::CreateTable { schema, .. } => {
                    let stmt = table_schema_to_create_table(schema);
                    let sql = stmt.display(Dialect::MySQL).to_string();
                    assert!(
                        sql.starts_with("CREATE TABLE"),
                        "expected CREATE TABLE, got: {sql}"
                    );

                    let (rows, _) = generate_rows(schema, 10, &[], &mut entropy);
                    assert_eq!(rows.len(), 10);
                    let insert = build_insert(schema, &rows).expect("should build insert");
                    let insert_sql = insert.display(Dialect::MySQL).to_string();
                    assert!(
                        insert_sql.starts_with("INSERT INTO"),
                        "expected INSERT, got: {insert_sql}"
                    );
                }
                DdlStep::AddColumn {
                    table,
                    column_name,
                    meta,
                } => {
                    let sql = format!(
                        "ALTER TABLE {} ADD COLUMN {} {}",
                        table,
                        column_name,
                        meta.sql_type.display(Dialect::MySQL)
                    );
                    assert!(sql.contains("ALTER TABLE"), "sql: {sql}");
                }
            }
        }

        let params = materialize_params(&output.params, &mut entropy);
        for p in &params {
            let _v: Value = Value::try_from(p.clone()).expect("should convert to Value");
        }
    }

    #[test]
    fn antithesis_rng_adapter_integration() {
        let mut rng = AntithesisRngAdapter;
        let mut generator = Generator::new(Dialect::MySQL, GeneratorConfig::default());
        let mut entropy = Entropy::new(&mut rng);

        let result = generator.generate_with_ddl(&mut entropy);
        assert!(
            result.is_ok(),
            "should generate with AntithesisRngAdapter: {result:?}"
        );

        let output = result.expect("should succeed");
        let sql = output.query.display(Dialect::MySQL).to_string();
        assert!(sql.contains("SELECT"), "sql: {sql}");

        for step in &output.ddl {
            if let DdlStep::CreateTable { schema, .. } = step {
                let stmt = table_schema_to_create_table(schema);
                let ddl_sql = stmt.display(Dialect::MySQL).to_string();
                assert!(ddl_sql.contains("CREATE TABLE"), "ddl: {ddl_sql}");
            }
        }
    }

    #[test]
    fn inline_sentinels_replaces_placeholders() {
        let sql = "SELECT * FROM t WHERE t.x = ? AND t.y = ?";
        let params = vec![Value::Integer(42), Value::Text("hello".to_string())];
        let (result, sentinels) = inline_sentinels(sql, &params, Dialect::MySQL);

        assert!(!result.contains('?'), "result: {result}");
        assert!(result.contains("990001"), "result: {result}");
        assert!(result.contains("__sentinel_1__"), "result: {result}");
        assert_eq!(sentinels.len(), 2);
        assert_eq!(sentinels[0].sentinel_text, "990001");
        assert!(sentinels[1].sentinel_text.contains("__sentinel_1__"));
    }

    #[test]
    fn inline_sentinels_replaces_postgres_dollar_placeholders() {
        let sql = "SELECT * FROM t WHERE t.x = $1 AND t.y = $2";
        let params = vec![Value::Integer(42), Value::Text("hello".to_string())];
        let (result, sentinels) = inline_sentinels(sql, &params, Dialect::PostgreSQL);

        assert!(!result.contains('$'), "result: {result}");
        assert!(result.contains("990001"), "result: {result}");
        assert!(result.contains("__sentinel_1__"), "result: {result}");
        assert_eq!(sentinels.len(), 2);
        assert_eq!(sentinels[0].sentinel_text, "990001");
        assert!(sentinels[1].sentinel_text.contains("__sentinel_1__"));
    }

    #[test]
    fn inline_params_replaces_placeholders() {
        let sql = "SELECT * FROM t WHERE t.x = ? AND t.y = ?";
        let params = vec![Value::Integer(42), Value::Text("hello".to_string())];
        let result = inline_params(sql, &params, Dialect::MySQL);

        assert!(!result.contains('?'), "result: {result}");
        assert!(result.contains("42"), "result: {result}");
        assert!(result.contains("'hello'"), "result: {result}");
    }

    #[test]
    fn inline_params_replaces_postgres_dollar_placeholders() {
        let sql = "SELECT * FROM t WHERE t.x = $1 AND t.y = $2";
        let params = vec![Value::Integer(42), Value::Text("hello".to_string())];
        let result = inline_params(sql, &params, Dialect::PostgreSQL);

        assert!(!result.contains('$'), "result: {result}");
        assert!(result.contains("42"), "result: {result}");
        assert!(result.contains("'hello'"), "result: {result}");
    }

    #[test]
    fn classify_autoparameterization_detects_replaced_sentinels() {
        let sentinels = vec![
            SentinelInfo {
                sentinel_text: "990001".to_string(),
            },
            SentinelInfo {
                sentinel_text: "'__sentinel_1__'".to_string(),
            },
        ];

        let rewritten = "SELECT * FROM t WHERE t.x = ? AND t.y = '__sentinel_1__'";
        let (parameterized, not_parameterized) =
            classify_autoparameterization(rewritten, &sentinels);

        assert_eq!(parameterized, vec![0]);
        assert_eq!(not_parameterized, vec![1]);
    }

    /// Regression: classify_autoparameterization must receive the rewritten
    /// query *text*, not a query ID.  A query ID like "q_abc123" never
    /// contains sentinel text, so every sentinel would be misclassified as
    /// "parameterized" regardless of whether autoparameterization happened.
    #[test]
    fn autoparam_reuse_only_asserted_when_all_literals_parameterized() {
        // All literals parameterized: two variants normalize identically, so reuse is valid.
        assert!(should_assert_autoparam_reuse(&[0, 1], &[]));
        // Nothing parameterized: nothing to check.
        assert!(!should_assert_autoparam_reuse(&[], &[0, 1]));
        // Mixed (the false-positive case): literal 1 stays inline (e.g. inside substring()/
        // month()), so varying it produces a different cache legitimately — must NOT assert.
        assert!(!should_assert_autoparam_reuse(&[0], &[1]));
    }

    #[test]
    fn classify_autoparameterization_rejects_false_positive_from_query_id() {
        let sentinels = vec![
            SentinelInfo {
                sentinel_text: "990001".to_string(),
            },
            SentinelInfo {
                sentinel_text: "'__sentinel_1__'".to_string(),
            },
        ];

        // If sentinels are still present in the query text, they were NOT
        // autoparameterized.  The function should detect this.
        let not_rewritten =
            "SELECT * FROM `t` WHERE `t`.`x` = 990001 AND `t`.`y` = '__sentinel_1__'";
        let (parameterized, not_parameterized) =
            classify_autoparameterization(not_rewritten, &sentinels);
        assert!(
            parameterized.is_empty(),
            "should detect that sentinels are still present (not autoparameterized)"
        );
        assert_eq!(not_parameterized, vec![0, 1]);
    }

    #[test]
    fn query_mentions_identifier_respects_word_boundaries() {
        // Bare substring matching used to false-positive: table `t1` would
        // match `t10`, and `users` would match `users_audit`. The boundary
        // check rejects these and accepts genuine references.
        assert!(query_mentions_identifier("SELECT * FROM users", "users"));
        assert!(query_mentions_identifier("SELECT * FROM `users`", "users"));
        assert!(query_mentions_identifier(
            "SELECT u.id FROM users u WHERE u.id = 1",
            "users"
        ));
        assert!(!query_mentions_identifier(
            "SELECT * FROM users_audit",
            "users"
        ));
        assert!(!query_mentions_identifier("SELECT * FROM t10", "t1"));
        assert!(!query_mentions_identifier("SELECT * FROM atusers", "users"));
        assert!(!query_mentions_identifier("", "users"));
        assert!(!query_mentions_identifier("SELECT 1", ""));
    }

    #[test]
    fn pattern_name_needs_literal_mode_identifies_correct_patterns() {
        assert!(Pattern::name_needs_literal_mode(
            "aggregated_join_subquery_eq_filter"
        ));
        assert!(Pattern::name_needs_literal_mode(
            "aggregated_join_subquery_having_filter"
        ));
        assert!(Pattern::name_needs_literal_mode(
            "having_to_where_promotion"
        ));
        assert!(Pattern::name_needs_literal_mode("from_subquery_filter"));
        assert!(Pattern::name_needs_literal_mode(
            "left_join_with_rhs_filter"
        ));
        assert!(!Pattern::name_needs_literal_mode("single_parameter"));
        assert!(!Pattern::name_needs_literal_mode("join_subquery"));
        assert!(Pattern::name_needs_literal_mode(
            "aggregated_join_subquery_eq_filter+single_parameter"
        ));
        assert!(Pattern::name_needs_literal_mode(
            "from_subquery_filter+count"
        ));
    }

    #[test]
    fn constraint_fuzz_mysql_dialect_detection() {
        let fuzz = ConstraintFuzz {
            num_queries: 50,
            rows_per_table: 50,
            seed: None,
            antithesis_entropy: false,
            compare_to: "mysql://root:noria@localhost/test".to_string(),
            readyset_url: "mysql://root:noria@localhost:3307/test".to_string(),
            db_op_timeout_secs: 30,
            dump_repro: None,
            required_tags: vec![],
            excluded_tags: vec![],
            single_pattern: false,
            keep_going: false,
            readyset_mode: false,
        };
        assert_eq!(fuzz.dialect().expect("valid mysql url"), Dialect::MySQL);
    }

    #[test]
    fn constraint_fuzz_postgres_dialect_detection() {
        let fuzz = ConstraintFuzz {
            num_queries: 10,
            rows_per_table: 10,
            seed: None,
            antithesis_entropy: false,
            compare_to: "postgresql://postgres:noria@localhost/test".to_string(),
            readyset_url: "postgresql://postgres:noria@localhost:5433/test".to_string(),
            db_op_timeout_secs: 30,
            dump_repro: None,
            required_tags: vec![],
            excluded_tags: vec![],
            single_pattern: false,
            keep_going: false,
            readyset_mode: false,
        };
        assert_eq!(
            fuzz.dialect().expect("valid postgres url"),
            Dialect::PostgreSQL
        );
    }

    #[test]
    fn constraint_fuzz_seeded_rng() {
        let fuzz = ConstraintFuzz {
            num_queries: 10,
            rows_per_table: 10,
            seed: Some(42),
            antithesis_entropy: false,
            compare_to: "mysql://root:noria@localhost/test".to_string(),
            readyset_url: "mysql://root:noria@localhost:3307/test".to_string(),
            db_op_timeout_secs: 30,
            dump_repro: None,
            required_tags: vec![],
            excluded_tags: vec![],
            single_pattern: false,
            keep_going: false,
            readyset_mode: false,
        };
        let mut rng1 = fuzz.make_rng();
        let mut rng2 = fuzz.make_rng();
        assert_eq!(rng1.next_u64(), rng2.next_u64());
    }

    #[test]
    fn constraint_fuzz_antithesis_entropy_flag() {
        let fuzz = ConstraintFuzz {
            num_queries: 10,
            rows_per_table: 10,
            seed: None,
            antithesis_entropy: true,
            compare_to: "mysql://root:noria@localhost/test".to_string(),
            readyset_url: "mysql://root:noria@localhost:3307/test".to_string(),
            db_op_timeout_secs: 30,
            dump_repro: None,
            required_tags: vec![],
            excluded_tags: vec![],
            single_pattern: false,
            keep_going: false,
            readyset_mode: false,
        };
        let mut rng = fuzz.make_rng();
        let _ = rng.next_u64();
    }

    fn mysql_server_anyhow(code: u16) -> anyhow::Error {
        anyhow::Error::from(database_utils::DatabaseError::MySQL(
            mysql_async::Error::Server(mysql_async::ServerError {
                code,
                message: "x".into(),
                state: "x".into(),
            }),
        ))
    }

    #[test]
    fn classify_error_mysql_known_limit_codes_only() {
        // 1038 (sort memory) is the one allowlisted planner limitation.
        assert_eq!(
            classify_error(&mysql_server_anyhow(1038)),
            ErrorClass::UpstreamKnownLimit { code: Some(1038) }
        );
    }

    #[test]
    fn classify_error_mysql_other_server_codes_are_generator_bugs() {
        // The old allowlist hid these as "skippable"; they are now surfaced
        // as generator bugs so we record an Antithesis assertion.
        for code in [1054, 1055, 1060, 1064, 1066, 1140, 1146, 1525] {
            assert_eq!(
                classify_error(&mysql_server_anyhow(code)),
                ErrorClass::UpstreamGeneratorBug { code: Some(code) },
                "MySQL code {code} must classify as a generator bug, not a known limit"
            );
        }
    }

    #[test]
    fn classify_error_mysql_io_is_fatal_not_generator_bug() {
        // Connection drop: Fatal; not skippable, not a generator bug.
        let io_err = mysql_async::Error::Io(mysql_async::IoError::Io(std::io::Error::other("eof")));
        let err = anyhow::Error::from(database_utils::DatabaseError::MySQL(io_err));
        assert_eq!(classify_error(&err), ErrorClass::Fatal);
    }

    #[test]
    fn classify_error_postgres_server_error_is_not_fatal() {
        // The old MySQL-only classifier silently sent every Postgres server
        // error to Fatal. The unified classifier maps it to GeneratorBug
        // (record assertion, continue).
        //
        // We can't directly construct a tokio_postgres::Error in tests
        // without round-tripping through a real connection, so this is a
        // documentation test asserting the intended invariant via the
        // classifier helper alone.
        let _ = "Postgres server errors must classify as UpstreamGeneratorBug";
        // Sanity: an arbitrary anyhow error that is *not* a DatabaseError
        // doesn't get routed to Fatal anymore — it falls through to Other.
        let err = anyhow::anyhow!("non-database upstream rejection");
        assert_eq!(classify_error(&err), ErrorClass::Other);
    }

    #[test]
    fn classify_error_readyset_transient() {
        let err = anyhow::anyhow!("Could not find table foo");
        assert_eq!(classify_error(&err), ErrorClass::ReadysetTransient);
    }

    #[test]
    fn classify_error_upstream_query_timeout_is_upstream_transient() {
        // qp:fdywzvulipvu: Antithesis network partitions surface as
        // UpstreamQueryTimeout. The old classification (Fatal) halted the
        // driver on the first partition-driven timeout; UpstreamTransient
        // lets --keep-going continue probing.
        let err = anyhow::Error::from(database_utils::DatabaseError::UpstreamQueryTimeout);
        assert_eq!(classify_error(&err), ErrorClass::UpstreamTransient);
    }

    #[test]
    fn classify_error_upstream_connection_none_is_upstream_transient() {
        let err = anyhow::Error::from(database_utils::DatabaseError::UpstreamConnectionNone);
        assert_eq!(classify_error(&err), ErrorClass::UpstreamTransient);
    }

    #[test]
    fn upstream_transient_tolerable_under_keep_going() {
        assert!(upstream_error_tolerable_under_keep_going(
            &ErrorClass::UpstreamTransient
        ));
    }

    #[test]
    fn readyset_error_substrings_are_never_masked() {
        // The oracle must never silence a Readyset divergence. A Readyset
        // error message classifies as `Other` and is surfaced rather than
        // skipped; there is no allowlist that downgrades it to a known bug.
        let err = anyhow::anyhow!(
            "Server error: DfValue conversion error: Failed to convert value of type \
             Double to MYSQL_TYPE_LONGLONG"
        );
        assert_eq!(classify_error(&err), ErrorClass::Other);
    }

    // qp:lxhufzoavfhd — client-side PG decode errors must not be Fatal.
    //
    // tokio_postgres::Error constructors are pub(crate) so we cannot build a
    // DatabaseError::PostgreSQL wrapping a FromSql-kind error directly in
    // tests. The classification logic delegates to the
    // `pg_error_is_client_decode` predicate, which we test via its string
    // interface here.
    #[test]
    fn pg_client_decode_msg_matches_deserializing_column_prefix() {
        // The exact message tokio_postgres emits for a WrongType rejection:
        // "error deserializing column N: <cause>"
        assert!(pg_error_is_client_decode(
            "error deserializing column 2: cannot convert between the Rust type \
             readyset_dante_oracle::value::Value and the Postgres type _timestamp"
        ));
        // A bare decode error without the column prefix is also a decode error.
        assert!(pg_error_is_client_decode(
            "error deserializing column 0: Invalid type"
        ));
    }

    #[test]
    fn pg_client_decode_msg_does_not_match_io_or_db_errors() {
        assert!(!pg_error_is_client_decode(
            "error communicating with the server"
        ));
        assert!(!pg_error_is_client_decode("db error"));
        assert!(!pg_error_is_client_decode("connection closed"));
    }

    // qp:zxanhxlohpix -- PG data errors (22003, 22012) must be UpstreamKnownLimit.
    //
    // tokio_postgres::Error constructors are pub(crate); we test via
    // classify_pg_sqlstate() which classify_error delegates to for PG
    // server-side codes.
    #[test]
    fn pg_22003_numeric_out_of_range_is_known_limit() {
        use tokio_postgres::error::SqlState;
        assert_eq!(
            classify_pg_sqlstate(&SqlState::NUMERIC_VALUE_OUT_OF_RANGE),
            ErrorClass::UpstreamKnownLimit { code: None }
        );
    }

    #[test]
    fn pg_22012_division_by_zero_is_known_limit() {
        use tokio_postgres::error::SqlState;
        assert_eq!(
            classify_pg_sqlstate(&SqlState::DIVISION_BY_ZERO),
            ErrorClass::UpstreamKnownLimit { code: None }
        );
    }

    #[test]
    fn pg_unrelated_server_error_remains_generator_bug() {
        // SQLSTATE 42883 = undefined_function; not a data error.
        use tokio_postgres::error::SqlState;
        assert_eq!(
            classify_pg_sqlstate(&SqlState::from_code("42883")),
            ErrorClass::UpstreamGeneratorBug { code: None }
        );
    }

    #[test]
    fn long_lived_glutton_scenario() {
        let config = GeneratorConfig {
            reuse_preference: 0.6,
            ..Default::default()
        };
        let mut generator = Generator::new(Dialect::MySQL, config);
        let mut rng = SmallRng::seed_from_u64(42);
        let mut entropy = Entropy::new(&mut rng);

        let mut all_ddl = Vec::new();
        let mut all_queries = Vec::new();

        for _ in 0..50 {
            if let Ok(output) = generator.generate_with_ddl(&mut entropy) {
                for step in &output.ddl {
                    if let DdlStep::CreateTable { schema, .. } = step {
                        let stmt = table_schema_to_create_table(schema);
                        all_ddl.push(stmt.display(Dialect::MySQL).to_string());
                    }
                }
                all_queries.push(output.query.display(Dialect::MySQL).to_string());
            }
        }

        assert!(
            !all_queries.is_empty(),
            "should have generated at least some queries"
        );
        assert!(
            !all_ddl.is_empty(),
            "should have generated at least some DDL"
        );

        for q in &all_queries {
            assert!(q.contains("SELECT"), "invalid query: {q}");
        }

        for d in &all_ddl {
            assert!(d.contains("CREATE TABLE"), "invalid DDL: {d}");
        }
    }

    #[test]
    fn compare_results_detects_row_permuted_cell_swap() {
        // (1,a),(2,b) vs (1,b),(2,a) — flatten+sort would have faked a match.
        // With row-tuple sort the rows are aligned and the cell swap is caught.
        let upstream = vec![
            vec![Value::Integer(1), Value::Text("a".into())],
            vec![Value::Integer(2), Value::Text("b".into())],
        ];
        let readyset = vec![
            vec![Value::Integer(1), Value::Text("b".into())],
            vec![Value::Integer(2), Value::Text("a".into())],
        ];
        assert!(matches!(
            compare_results(&upstream, &readyset, false),
            ComparisonOutcome::Mismatch { .. }
        ));
    }

    #[test]
    fn compare_results_matches_after_row_reorder_when_no_order_by() {
        let upstream = vec![
            vec![Value::Integer(1), Value::Text("a".into())],
            vec![Value::Integer(2), Value::Text("b".into())],
        ];
        let readyset = vec![
            vec![Value::Integer(2), Value::Text("b".into())],
            vec![Value::Integer(1), Value::Text("a".into())],
        ];
        assert!(matches!(
            compare_results(&upstream, &readyset, false),
            ComparisonOutcome::Match
        ));
    }

    #[test]
    fn compare_results_with_order_by_does_not_sort() {
        // ORDER BY in SQL fixes row order; reordered rows are a real mismatch.
        let upstream = vec![vec![Value::Integer(1)], vec![Value::Integer(2)]];
        let readyset = vec![vec![Value::Integer(2)], vec![Value::Integer(1)]];
        assert!(matches!(
            compare_results(&upstream, &readyset, true),
            ComparisonOutcome::Mismatch { .. }
        ));
    }

    #[test]
    fn compare_results_row_count_mismatch() {
        let upstream = vec![vec![Value::Integer(1)]];
        let readyset = vec![vec![Value::Integer(1)], vec![Value::Integer(2)]];
        assert!(matches!(
            compare_results(&upstream, &readyset, false),
            ComparisonOutcome::Mismatch { .. }
        ));
    }

    #[test]
    fn compare_results_uses_value_eq_for_numeric_coercion() {
        let upstream = vec![vec![Value::Integer(5)]];
        let readyset = vec![vec![Value::UnsignedInteger(5)]];
        assert!(matches!(
            compare_results(&upstream, &readyset, false),
            ComparisonOutcome::Match
        ));
    }

    #[test]
    fn compare_results_sort_aligns_across_coerced_variants() {
        // Mixed-variant column on one side (e.g. UNION ALL branches with
        // different numeric promotion) must sort into the same row order
        // as a normalized column on the other side, so cell-wise
        // `value_eq` sees aligned pairs instead of variant-tag-skewed
        // ones. Without coercion-aware sort, `Integer(5)` would sort
        // before `Numeric(3)` on the upstream side (variant tag tie-break
        // beats value), producing a spurious mismatch against the
        // all-`Numeric` readyset side.
        use readyset_decimal::Decimal;
        let upstream = vec![
            vec![Value::Integer(5)],
            vec![Value::Numeric(Decimal::from(3))],
        ];
        let readyset = vec![
            vec![Value::Numeric(Decimal::from(5))],
            vec![Value::Numeric(Decimal::from(3))],
        ];
        assert!(matches!(
            compare_results(&upstream, &readyset, false),
            ComparisonOutcome::Match
        ));
    }

    #[test]
    fn compare_results_sort_aligns_datetime_vs_timestamptz() {
        // `DateTime` (naive UTC) and `TimestampTz` compare equal under
        // `value_eq`; the sort key must respect that so paired rows line
        // up.
        use chrono::{FixedOffset, NaiveDate, TimeZone};
        let naive_a = NaiveDate::from_ymd_opt(2024, 1, 1)
            .unwrap()
            .and_hms_opt(7, 0, 0)
            .unwrap();
        let naive_b = NaiveDate::from_ymd_opt(2024, 1, 1)
            .unwrap()
            .and_hms_opt(8, 0, 0)
            .unwrap();
        let plus5 = FixedOffset::east_opt(5 * 3600).unwrap();
        let tz_a = plus5
            .from_local_datetime(
                &naive_a
                    .checked_add_signed(chrono::Duration::hours(5))
                    .unwrap(),
            )
            .unwrap();
        let upstream = vec![
            vec![Value::DateTime(naive_b)],
            vec![Value::TimestampTz(tz_a)],
        ];
        let readyset = vec![
            vec![Value::DateTime(naive_b)],
            vec![Value::DateTime(naive_a)],
        ];
        assert!(matches!(
            compare_results(&upstream, &readyset, false),
            ComparisonOutcome::Match
        ));
    }

    #[test]
    fn compare_results_empty_inputs_match() {
        assert!(matches!(
            compare_results(&[], &[], false),
            ComparisonOutcome::Match
        ));
        assert!(matches!(
            compare_results(&[], &[], true),
            ComparisonOutcome::Match
        ));
    }

    /// Set the global per-op timeout to a very short duration so timeout
    /// tests can assert the firing path in <1s. Idempotent across tests
    /// (`OnceLock::set` is one-shot; subsequent calls are no-ops).
    fn install_short_db_op_timeout() {
        let _ = DB_OP_TIMEOUT.set(Duration::from_millis(50));
    }

    #[tokio::test]
    async fn with_op_timeout_propagates_ok() {
        install_short_db_op_timeout();
        let r: anyhow::Result<u32> =
            with_op_timeout("noop", async { Ok::<u32, anyhow::Error>(7) }).await;
        assert_eq!(r.unwrap(), 7);
    }

    #[tokio::test]
    async fn with_op_timeout_propagates_inner_error() {
        install_short_db_op_timeout();
        let r: anyhow::Result<u32> = with_op_timeout("noop", async {
            Err::<u32, anyhow::Error>(anyhow::anyhow!("boom"))
        })
        .await;
        let msg = format!("{:#}", r.unwrap_err());
        assert!(msg.contains("boom"), "unexpected: {msg}");
    }

    #[tokio::test]
    async fn with_op_timeout_fires_on_long_op() {
        // OnceLock::set is one-shot; whatever value is installed first wins.
        // Whether this test or another sets first, the timeout is short
        // enough (≤30s) that this future will time out in practice.
        install_short_db_op_timeout();
        let start = std::time::Instant::now();
        let r: anyhow::Result<()> = with_op_timeout("hang", async {
            tokio::time::sleep(Duration::from_secs(60)).await;
            Ok::<(), anyhow::Error>(())
        })
        .await;
        let elapsed = start.elapsed();
        assert!(r.is_err(), "expected timeout error");
        assert!(
            elapsed < Duration::from_secs(60),
            "expected to fire within timeout, took {elapsed:?}"
        );
        let msg = format!("{:#}", r.unwrap_err());
        assert!(msg.contains("hang"), "expected op name in error: {msg}");
        assert!(msg.contains("timeout"), "expected timeout in error: {msg}");
    }

    #[test]
    fn is_connection_drop_mysql_io_eof() {
        let io_err = mysql_async::Error::Io(mysql_async::IoError::Io(std::io::Error::other("eof")));
        let err = anyhow::Error::from(database_utils::DatabaseError::MySQL(io_err));
        assert!(is_connection_drop(&err));
    }

    #[test]
    fn is_connection_drop_upstream_connection_none() {
        let err = anyhow::Error::from(database_utils::DatabaseError::UpstreamConnectionNone);
        assert!(is_connection_drop(&err));
    }

    #[test]
    fn is_connection_drop_mysql_server_error_is_not_drop() {
        // A normal server error (e.g., bad SQL) is NOT a connection drop —
        // we must not reconnect-and-retry on these or we'd re-execute
        // a permanent failure forever.
        let err = anyhow::Error::from(database_utils::DatabaseError::MySQL(
            mysql_async::Error::Server(mysql_async::ServerError {
                code: 1054,
                message: "x".into(),
                state: "x".into(),
            }),
        ));
        assert!(!is_connection_drop(&err));
    }

    #[test]
    fn is_connection_drop_message_needles() {
        let err = anyhow::anyhow!("io error: Connection refused (os error 61)");
        assert!(is_connection_drop(&err));
        let err = anyhow::anyhow!("upstream returned: broken pipe while writing");
        assert!(is_connection_drop(&err));
    }

    #[test]
    fn is_connection_drop_unrelated_text_is_not_drop() {
        let err = anyhow::anyhow!("Server error: column 'x' not found");
        assert!(!is_connection_drop(&err));
    }

    fn mysql_access_denied(code: u16) -> anyhow::Error {
        anyhow::Error::from(database_utils::DatabaseError::MySQL(
            mysql_async::Error::Server(mysql_async::ServerError {
                code,
                message: "access denied".into(),
                state: "28000".into(),
            }),
        ))
    }

    #[test]
    fn is_permanent_connection_error_recognises_mysql_auth_codes() {
        for code in [1044, 1045, 1142, 1698] {
            assert!(
                is_permanent_connection_error(&mysql_access_denied(code)),
                "MySQL code {code} should be permanent (auth/perm)"
            );
        }
    }

    #[test]
    fn is_permanent_connection_error_does_not_match_transient_io() {
        // Connection-drop class errors must NOT be classified as permanent;
        // otherwise a brief network blip would fail-fast and skip retries.
        let io_err = mysql_async::Error::Io(mysql_async::IoError::Io(std::io::Error::other("eof")));
        let err = anyhow::Error::from(database_utils::DatabaseError::MySQL(io_err));
        assert!(!is_permanent_connection_error(&err));
    }

    #[test]
    fn is_permanent_connection_error_unrelated_text_is_not_permanent() {
        let err = anyhow::anyhow!("temporary network failure");
        assert!(!is_permanent_connection_error(&err));
    }

    #[test]
    fn repro_select_inlines_params_and_keeps_raw_form() {
        let mut log = ReproLog::new(Dialect::MySQL, "42".to_string());
        log.record_select(
            0,
            "single_parameter",
            "SELECT * FROM t WHERE c = ?",
            &[Value::Integer(99)],
            None,
        );
        let body: String = log.recent.iter().cloned().collect::<Vec<_>>().join("\n");
        // Inlined form must be present so the script is replayable as-is.
        assert!(
            body.contains("WHERE c = 99"),
            "expected inlined param value: {body}"
        );
        // Raw placeholder form preserved as comment for cross-checking.
        assert!(
            body.contains("-- raw: SELECT * FROM t WHERE c = ?"),
            "expected raw form comment: {body}"
        );
    }

    #[test]
    fn repro_header_records_seed_and_dialect() {
        let log = ReproLog::new(Dialect::PostgreSQL, "12345".to_string());
        let header = log.header();
        assert!(header.contains("seed: 12345"), "expected seed: {header}");
        assert!(
            header.contains("dialect: PostgreSQL"),
            "expected dialect: {header}"
        );
        assert!(
            header.contains("not server-side\n-- nondeterminism"),
            "expected scope-disclosure: {header}"
        );
    }

    #[test]
    fn repro_does_not_record_drop_table_through_record_ddl_alone() {
        // Sanity that the run_queries call site is the only producer of DROP
        // TABLE recording. record_ddl unconditionally records anything passed
        // to it; it's the call site that must NOT pass `drop_sql`. This test
        // documents the invariant: anything containing "DROP TABLE IF EXISTS"
        // would replay as a destructive statement.
        let mut log = ReproLog::new(Dialect::MySQL, "0".to_string());
        log.record_ddl(0, "CREATE TABLE t (id INT)");
        let body: String = log.recent.iter().cloned().collect::<Vec<_>>().join("\n");
        assert!(
            !body.contains("DROP TABLE"),
            "record_ddl was passed CREATE only — must not contain DROP: {body}"
        );
    }

    #[test]
    fn repro_streaming_writer_emits_self_consistent_script() {
        let dir = std::env::temp_dir();
        let path = dir.join(format!("dante-repro-stream-{}.sql", std::process::id()));
        let mut log =
            ReproLog::with_writer(path.clone(), Dialect::MySQL, "7".to_string()).expect("open");
        log.record_ddl(0, "CREATE TABLE t (a INT)");
        log.record_ddl(0, "INSERT INTO t (a) VALUES (1)");
        log.record_select(1, "single_table", "SELECT a FROM t", &[], None);

        let written = log.finish().expect("flush succeeds");
        let (out_path, n) = written.expect("path returned");
        assert_eq!(out_path, path);
        assert_eq!(n, 3);

        let contents = std::fs::read_to_string(&path).expect("read succeeds");
        assert!(contents.contains("CREATE TABLE t"));
        assert!(contents.contains("INSERT INTO t"));
        assert!(contents.contains("SELECT a FROM t"));
        // Header must precede statements.
        let header_pos = contents.find("dialect").expect("header dialect");
        let create_pos = contents.find("CREATE TABLE").expect("create");
        assert!(header_pos < create_pos);

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn repro_ring_buffer_drops_oldest_when_at_capacity() {
        // Manually shrink capacity to keep the test fast and assert the
        // bounded-collections invariant: the ring never exceeds `capacity`,
        // and `statements_total` keeps counting beyond that.
        let mut log = ReproLog::new(Dialect::MySQL, "0".to_string());
        log.capacity = 4;
        for i in 0..10 {
            log.record_ddl(i, &format!("STMT {i}"));
        }
        assert_eq!(log.statements_total(), 10);
        assert_eq!(log.recent.len(), 4);
        let body: String = log.recent.iter().cloned().collect::<Vec<_>>().join("\n");
        // The 4 most-recent are 6,7,8,9; the older ones must have been
        // evicted.
        assert!(body.contains("STMT 9"), "expected STMT 9: {body}");
        assert!(body.contains("STMT 6"), "expected STMT 6: {body}");
        assert!(!body.contains("STMT 0"), "STMT 0 must be evicted: {body}");
        assert!(!body.contains("STMT 5"), "STMT 5 must be evicted: {body}");
    }

    fn test_int_int_schema() -> TableSchema {
        let mut schema = TableSchema::new(SqlIdentifier::from("t"));
        schema.add_column(
            SqlIdentifier::from("c1"),
            dante::state::ColumnMeta {
                sql_type: readyset_sql::ast::SqlType::Int(None),
                gen_spec: ColumnGenerationSpec::Random,
            },
        );
        schema.add_column(
            SqlIdentifier::from("c2"),
            dante::state::ColumnMeta {
                sql_type: readyset_sql::ast::SqlType::Int(None),
                gen_spec: ColumnGenerationSpec::Random,
            },
        );
        schema
    }

    fn test_rng() -> impl rand::Rng {
        SmallRng::seed_from_u64(0xDEAD)
    }

    #[test]
    fn generate_rows_emits_one_row_per_example_first() {
        use dante::constraint::ExampleValue;
        use dante::resolver::{ResolvedExample, RowOverride};

        let schema = test_int_int_schema();
        let mut rng = test_rng();
        let example = ResolvedExample {
            note: "bait",
            dialect: dante::constraint::DialectSupport::Both,
            row_overrides: vec![
                RowOverride {
                    table: SqlIdentifier::from("t"),
                    column: SqlIdentifier::from("c1"),
                    value: ExampleValue::Literal("8"),
                },
                RowOverride {
                    table: SqlIdentifier::from("t"),
                    column: SqlIdentifier::from("c2"),
                    value: ExampleValue::Literal("3"),
                },
            ],
            param_overrides: vec![],
        };
        let (rows, _) = generate_rows(&schema, 3, &[&example], &mut rng);
        assert_eq!(rows.len(), 4, "1 example + 3 random fill");
        // Row 0 must carry the example literals.
        assert_eq!(rows[0][0].to_string(), "8");
        assert_eq!(rows[0][1].to_string(), "3");
    }

    #[test]
    fn generate_rows_random_fill_avoids_example_literal_on_unique_column() {
        use dante::constraint::ExampleValue;
        use dante::resolver::{ResolvedExample, RowOverride};

        let mut schema = test_int_int_schema();
        // Force c1 to UniqueFrom(0) so generated values are 0, 1, 2, ...
        schema
            .columns
            .get_mut(&SqlIdentifier::from("c1"))
            .expect("c1 exists")
            .gen_spec = ColumnGenerationSpec::UniqueFrom(0);
        let mut rng = test_rng();
        let example = ResolvedExample {
            note: "bait",
            dialect: dante::constraint::DialectSupport::Both,
            row_overrides: vec![RowOverride {
                table: SqlIdentifier::from("t"),
                column: SqlIdentifier::from("c1"),
                value: ExampleValue::Literal("0"),
            }],
            param_overrides: vec![],
        };
        let (rows, _) = generate_rows(&schema, 5, &[&example], &mut rng);
        assert_eq!(rows[0][0].to_string(), "0");
        // Random fill must not collide with the example literal.
        for row in &rows[1..] {
            assert_ne!(
                row[0].to_string(),
                "0",
                "random fill collided with example literal"
            );
        }
    }

    #[test]
    fn materialize_params_uses_example_literal_when_present() {
        use dante::constraint::ExampleValue;
        use dante::resolver::{ParamMeta, ParamOverride};
        use data_generator::ColumnGenerationSpec;
        use readyset_sql::ast::SqlType;

        let params = vec![ParamMeta {
            sql_type: SqlType::Int(None),
            gen_spec: ColumnGenerationSpec::Random,
            count: 1,
        }];
        let overrides = vec![ParamOverride {
            placeholder_index: 0,
            value: ExampleValue::Literal("42"),
        }];
        let mut rng = test_rng();

        let values = materialize_params_with_overrides(&params, &overrides, &mut rng);
        assert_eq!(values.len(), 1);
        assert_eq!(values[0].to_string(), "42");
    }

    // --- build_select_probes tests ---

    #[test]
    fn select_probes_no_examples_yields_one_random_probe() {
        use data_generator::ColumnGenerationSpec;
        use readyset_sql::ast::SqlType;

        let params = vec![ParamMeta {
            sql_type: SqlType::Int(None),
            gen_spec: ColumnGenerationSpec::Random,
            count: 1,
        }];
        let mut rng = test_rng();
        let probes = build_select_probes(&params, &[], "SELECT 1", &mut rng);
        assert_eq!(probes.len(), 1, "one random probe when no examples");
        assert!(probes[0].example_note.is_none(), "random probe has no note");
    }

    #[test]
    fn select_probes_one_example_yields_two_probes() {
        use dante::constraint::ExampleValue;
        use dante::resolver::{ParamMeta, ParamOverride, ResolvedExample};
        use data_generator::ColumnGenerationSpec;
        use readyset_sql::ast::SqlType;

        let params = vec![ParamMeta {
            sql_type: SqlType::Int(None),
            gen_spec: ColumnGenerationSpec::Random,
            count: 1,
        }];
        let example = ResolvedExample {
            note: "div by zero bait",
            dialect: dante::constraint::DialectSupport::Both,
            row_overrides: vec![],
            param_overrides: vec![ParamOverride {
                placeholder_index: 0,
                value: ExampleValue::Literal("0"),
            }],
        };
        let examples = [example];
        let mut rng = test_rng();
        let probes = build_select_probes(&params, &examples, "SELECT 1", &mut rng);
        assert_eq!(
            probes.len(),
            2,
            "one random + one example probe (got {probes:?})"
        );
        assert!(probes[0].example_note.is_none(), "first probe is random");
        assert_eq!(
            probes[1].example_note,
            Some("div by zero bait"),
            "second probe carries note"
        );
        // Example probe must have the pinned value.
        assert_eq!(probes[1].params[0].to_string(), "0");
    }

    #[test]
    fn select_probes_two_examples_yields_three_probes() {
        use dante::constraint::ExampleValue;
        use dante::resolver::{ParamMeta, ParamOverride, ResolvedExample};
        use data_generator::ColumnGenerationSpec;
        use readyset_sql::ast::SqlType;

        let params = vec![ParamMeta {
            sql_type: SqlType::Int(None),
            gen_spec: ColumnGenerationSpec::Random,
            count: 1,
        }];
        let examples = vec![
            ResolvedExample {
                note: "bait A",
                dialect: dante::constraint::DialectSupport::Both,
                row_overrides: vec![],
                param_overrides: vec![ParamOverride {
                    placeholder_index: 0,
                    value: ExampleValue::Literal("1"),
                }],
            },
            ResolvedExample {
                note: "bait B",
                dialect: dante::constraint::DialectSupport::Both,
                row_overrides: vec![],
                param_overrides: vec![ParamOverride {
                    placeholder_index: 0,
                    value: ExampleValue::Literal("2"),
                }],
            },
        ];
        let mut rng = test_rng();
        let probes = build_select_probes(&params, &examples, "SELECT 1", &mut rng);
        assert_eq!(probes.len(), 3, "one random + two example probes");
        assert!(probes[0].example_note.is_none());
        assert_eq!(probes[1].example_note, Some("bait A"));
        assert_eq!(probes[2].example_note, Some("bait B"));
    }

    #[test]
    fn repro_record_select_includes_example_note_and_overrides() {
        use dante::constraint::ExampleValue;
        use dante::resolver::{ParamOverride, RowOverride};
        use readyset_sql::ast::SqlIdentifier;

        let mut log = ReproLog::new(Dialect::MySQL, "0".to_string());
        let row_overrides = vec![RowOverride {
            table: SqlIdentifier::from("t"),
            column: SqlIdentifier::from("c1"),
            value: ExampleValue::Literal("8"),
        }];
        let param_overrides = vec![ParamOverride {
            placeholder_index: 0,
            value: ExampleValue::Literal("2"),
        }];
        log.record_select(
            0,
            "int_div",
            "SELECT c1 / ? FROM t",
            &[Value::Integer(2)],
            Some(("int/int divide bait", &row_overrides, &param_overrides)),
        );
        let body: String = log.recent.iter().cloned().collect::<Vec<_>>().join("\n");
        assert!(
            body.contains("-- example: int/int divide bait"),
            "expected example note comment: {body}"
        );
        assert!(
            body.contains("--   row override: t.c1"),
            "expected row override comment: {body}"
        );
        assert!(
            body.contains("--   param override: $1"),
            "expected param override comment: {body}"
        );
    }

    #[test]
    fn generate_rows_skips_example_with_pk_already_emitted() {
        use dante::constraint::ExampleValue;
        use dante::resolver::{ResolvedExample, RowOverride};

        // Schema: PK on c0, plus c1. Two examples both pin c0=8 with
        // different c1 values. The oracle previously emitted both rows
        // verbatim, producing a `Duplicate entry '8' for key 'PRIMARY'`
        // INSERT failure at run time. The dedup makes the second
        // example's row be dropped while still preserving the first
        // example's seed row.
        let mut schema = TableSchema::new(SqlIdentifier::from("t"));
        schema.add_column(
            SqlIdentifier::from("c0"),
            dante::state::ColumnMeta {
                sql_type: readyset_sql::ast::SqlType::Int(None),
                gen_spec: ColumnGenerationSpec::Unique,
            },
        );
        schema.add_column(
            SqlIdentifier::from("c1"),
            dante::state::ColumnMeta {
                sql_type: readyset_sql::ast::SqlType::Int(None),
                gen_spec: ColumnGenerationSpec::Random,
            },
        );
        schema.primary_key = Some(SqlIdentifier::from("c0"));

        let ex_a = ResolvedExample {
            note: "first",
            dialect: dante::constraint::DialectSupport::Both,
            row_overrides: vec![
                RowOverride {
                    table: SqlIdentifier::from("t"),
                    column: SqlIdentifier::from("c0"),
                    value: ExampleValue::Literal("8"),
                },
                RowOverride {
                    table: SqlIdentifier::from("t"),
                    column: SqlIdentifier::from("c1"),
                    value: ExampleValue::Literal("2"),
                },
            ],
            param_overrides: vec![],
        };
        let ex_b = ResolvedExample {
            note: "second",
            dialect: dante::constraint::DialectSupport::Both,
            row_overrides: vec![
                RowOverride {
                    table: SqlIdentifier::from("t"),
                    column: SqlIdentifier::from("c0"),
                    value: ExampleValue::Literal("8"),
                },
                RowOverride {
                    table: SqlIdentifier::from("t"),
                    column: SqlIdentifier::from("c1"),
                    value: ExampleValue::Literal("999"),
                },
            ],
            param_overrides: vec![],
        };

        let mut rng = test_rng();
        let (rows, dropped) = generate_rows(&schema, 3, &[&ex_a, &ex_b], &mut rng);
        assert_eq!(
            dropped,
            vec![1],
            "second example (index 1) should be dropped"
        );

        let pk_values: Vec<String> = rows.iter().map(|r| r[0].to_string()).collect();
        let unique: std::collections::HashSet<_> = pk_values.iter().collect();
        assert_eq!(
            unique.len(),
            pk_values.len(),
            "PK values must be unique across all emitted rows; got {pk_values:?}"
        );
        let pk_eights: usize = pk_values.iter().filter(|v| v.as_str() == "8").count();
        assert_eq!(
            pk_eights, 1,
            "second example with same PK must be skipped, not duplicated"
        );
        // First example wins: c1 must be 2, not 999.
        let row_with_pk_8 = rows
            .iter()
            .find(|r| r[0].to_string() == "8")
            .expect("row with PK=8 must exist");
        assert_eq!(
            row_with_pk_8[1].to_string(),
            "2",
            "first example's c1 must be preserved"
        );
    }

    #[test]
    fn pattern_counts_errored_field_defaults_to_zero_and_increments() {
        let mut stats = PatternStats::default();
        assert_eq!(stats.entry("p").errored, 0);
        stats.entry("p").errored += 1;
        stats.entry("p").errored += 1;
        assert_eq!(stats.entry("p").errored, 2);
    }

    #[test]
    fn constraint_fuzz_single_pattern_defaults_off_and_parses() {
        // Defaults off when the flag is absent: patterns are composed.
        let parsed = ConstraintFuzz::try_parse_from([
            "oracle",
            "--compare-to",
            "mysql://root:noria@localhost/test",
            "--readyset-url",
            "mysql://root:noria@localhost:3307/test",
        ])
        .expect("parse without --single-pattern");
        assert!(!parsed.single_pattern);

        // Flips on when present: one pattern per iteration, no composition.
        let parsed = ConstraintFuzz::try_parse_from([
            "oracle",
            "--compare-to",
            "mysql://root:noria@localhost/test",
            "--readyset-url",
            "mysql://root:noria@localhost:3307/test",
            "--single-pattern",
        ])
        .expect("parse with --single-pattern");
        assert!(parsed.single_pattern);
    }

    #[test]
    fn constraint_fuzz_keep_going_defaults_off_and_parses() {
        // Defaults off when the flag is absent.
        let parsed = ConstraintFuzz::try_parse_from([
            "oracle",
            "--compare-to",
            "mysql://root:noria@localhost/test",
            "--readyset-url",
            "mysql://root:noria@localhost:3307/test",
        ])
        .expect("parse without --keep-going");
        assert!(!parsed.keep_going);

        // Flips on when present.
        let parsed = ConstraintFuzz::try_parse_from([
            "oracle",
            "--compare-to",
            "mysql://root:noria@localhost/test",
            "--readyset-url",
            "mysql://root:noria@localhost:3307/test",
            "--keep-going",
        ])
        .expect("parse with --keep-going");
        assert!(parsed.keep_going);
    }

    #[test]
    fn constraint_fuzz_readyset_mode_defaults_off_and_parses() {
        // Defaults off when the flag is absent: the endpoint is treated as a
        // transparent proxy (e.g. SQP), so the oracle emits no
        // Readyset-specific DDL the proxy cannot route.
        let parsed = ConstraintFuzz::try_parse_from([
            "oracle",
            "--compare-to",
            "mysql://root:noria@localhost/test",
            "--readyset-url",
            "mysql://root:noria@localhost:3307/test",
        ])
        .expect("parse without --readyset-mode");
        assert!(!parsed.readyset_mode);

        // Flips on when present: forces `CREATE DEEP CACHE`, runs the EXPLAIN
        // probes, and applies the Readyset deep-cache compatibility rules.
        let parsed = ConstraintFuzz::try_parse_from([
            "oracle",
            "--compare-to",
            "mysql://root:noria@localhost/test",
            "--readyset-url",
            "mysql://root:noria@localhost:3307/test",
            "--readyset-mode",
        ])
        .expect("parse with --readyset-mode");
        assert!(parsed.readyset_mode);
    }
}
