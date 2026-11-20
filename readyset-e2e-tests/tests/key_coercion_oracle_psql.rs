//! Compares Readyset with PostgreSQL on cached queries whose comparison becomes a key or a filter:
//! a lookup (`WHERE v = $1`), a join (`JOIN ... ON l.v = r.v`) and a filter between two columns
//! (`WHERE a OP b`). The tests cover each pair of types that PostgreSQL compares.
//!
//! Each table that a test reads holds each value in two rows. One is inserted before the snapshot.
//! The other, with id + `REPLICATED`, is inserted after it and arrives by replication.
//!
//! A test reports all of its findings when it fails. Each finding has one of these prefixes:
//! - `WRONG`: Readyset returns different rows from PostgreSQL.
//! - `ERROR`: Readyset fails a query that PostgreSQL answers.
//! - `PROXIED`: Readyset proxies a query that a cache must serve.
//! - `REFUSED`: Readyset refuses to cache a query that PostgreSQL accepts.
//! - `ACCEPTED`: Readyset caches a query that PostgreSQL rejects.
//! - `LITERAL`: PostgreSQL rejects a generated type, or every literal of one.
//! - `REPLICATION`: Readyset does not replicate the rows inserted after the snapshot.

use std::collections::BTreeSet;
use std::error::Error;
use std::time::Duration;

use bytes::BytesMut;
use proptest::prelude::*;
use proptest::strategy::ValueTree;
use proptest::test_runner::{Config as ProptestConfig, TestRunner};
use readyset_adapter::backend::MigrationMode;
use readyset_client_metrics::QueryDestination;
use readyset_client_test_helpers::psql_helpers::{self, PostgreSQLAdapter, last_query_info};
use readyset_client_test_helpers::{Adapter, TestBuilder, TestShutdownSender, sleep};
use readyset_data::DfValue;
use readyset_server::Handle;
use readyset_sql::ast::{SqlType, SqlTypeArbitraryOptions};
use readyset_sql::postgres_literals::{
    arbitrary_postgres_literal, postgres_edge_literals, postgres_representative_types,
    postgres_setup_sql,
};
use readyset_sql::{Dialect, DialectDisplay};
use test_utils::{tags, upstream};
use tokio_postgres::types::{Format, IsNull, ToSql, Type, to_sql_checked};
use tokio_postgres::{Client, Config, SimpleQueryMessage};

/// The id of a replicated row is the id of its snapshot row plus this.
const REPLICATED: i32 = 10_000;
/// The id of the NULL row in `t<i>` and `u<i>`, which is the id of no value.
const NULL_ID: i32 = 9_999;

/// Whether `literal` is a value that keeps Readyset from replicating its table. Each such value is
/// in a table of its own, so that Readyset replicates the table that holds the other values.
fn is_hazardous(literal: &str) -> bool {
    ["'infinity'", "'-infinity'", " BC'", "5874897", "294276"]
        .iter()
        .any(|h| literal.contains(h))
}

/// A parameter in text format, so that both servers parse the same text.
#[derive(Debug)]
struct TextFormat(String);

impl ToSql for TextFormat {
    fn to_sql(&self, _: &Type, out: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        out.extend_from_slice(self.0.as_bytes());
        Ok(IsNull::No)
    }

    fn accepts(_: &Type) -> bool {
        true
    }

    fn encode_format(&self, _: &Type) -> Format {
        Format::Text
    }

    to_sql_checked!();
}

/// A value in each form that a lookup sends, and the id of the row that holds it.
#[derive(Debug, Clone)]
struct Value {
    id: i32,
    literal: String,
    text: String,
    binary: Option<DfValue>,
}

/// A type and its values, which the tests use as rows and as parameters.
#[derive(Debug)]
struct TypedValues {
    ty: SqlType,
    name: String,
    /// The parameter type of a value, as PostgreSQL reports it: without a type modifier.
    param_type: Option<Type>,
    values: Vec<Value>,
}

impl TypedValues {
    fn safe_values(&self) -> impl Iterator<Item = &Value> {
        self.values.iter().filter(|v| !is_hazardous(&v.literal))
    }

    /// The literal in row `id`, marked `(repl)` if the row is replicated.
    fn row_label(&self, id: i32) -> String {
        let repl = if id >= REPLICATED { " (repl)" } else { "" };
        let literal = self.values.iter().find(|v| v.id == id % REPLICATED);
        format!("{}{repl}", literal.map_or("NULL", |v| &v.literal))
    }
}

/// A group of types that one test covers. The number types are split so that each test stays
/// under the test timeout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Int,
    Numeric,
    Float,
    String,
    Array,
    Other,
}

const NUMBERS: &[Kind] = &[Kind::Int, Kind::Numeric, Kind::Float];
const OTHERS: &[Kind] = &[Kind::Array, Kind::Other];

fn kind(ty: &SqlType) -> Kind {
    use SqlType::*;
    match ty {
        SmallInt(_) | Int(_) | BigInt(_) => Kind::Int,
        Numeric(_) => Kind::Numeric,
        Real | Double => Kind::Float,
        Text | VarChar(_) | Char(_) | Citext | QuotedChar => Kind::String,
        Array(_) => Kind::Array,
        _ => Kind::Other,
    }
}

/// Whether both types are numbers or have the same kind. PostgreSQL compares only such pairs.
fn same_category(l: &SqlType, r: &SqlType) -> bool {
    let category = |k| if NUMBERS.contains(&k) { Kind::Int } else { k };
    category(kind(l)) == category(kind(r))
}

/// A connection to Readyset that reopens when Readyset closes it.
struct Readyset {
    conn: Client,
    opts: Config,
}

impl Readyset {
    async fn conn(&mut self) -> &Client {
        if self.conn.is_closed() {
            self.conn = psql_helpers::connect(self.opts.clone()).await;
        }
        &self.conn
    }

    async fn create_cache(&mut self, query: &str) -> Result<(), String> {
        let sql = format!("CREATE CACHE FROM {query}");
        self.conn()
            .await
            .simple_query(&sql)
            .await
            .map(|_| ())
            .map_err(|e| message(&e))
    }

    async fn served_from_cache(&mut self) -> Result<(), String> {
        let info = last_query_info(self.conn().await).await;
        match info.destination {
            QueryDestination::Readyset(_) => Ok(()),
            dest => Err(format!("{dest:?} ({})", info.reason)),
        }
    }
}

fn message(e: &tokio_postgres::Error) -> String {
    e.as_db_error()
        .map_or_else(|| e.to_string(), |d| d.message().to_owned())
}

/// The sorted rows of a simple query that returns only integers.
fn int_rows(msgs: &[SimpleQueryMessage]) -> Vec<Vec<i32>> {
    let mut rows = msgs
        .iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(r) => Some(
                (0..r.len())
                    .map(|i| r.get(i)?.parse().ok())
                    .collect::<Option<_>>()?,
            ),
            _ => None,
        })
        .collect::<Vec<_>>();
    rows.sort();
    rows
}

/// A `WRONG` finding if `ours` differs from `expected`. `label` describes a row from its ids.
fn diff(
    what: &str,
    expected: &[Vec<i32>],
    ours: &[Vec<i32>],
    label: impl Fn(&[i32]) -> String,
) -> Option<String> {
    if expected == ours {
        return None;
    }
    let only = |a: &[Vec<i32>], b: &[Vec<i32>]| {
        a.iter()
            .filter(|r| !b.contains(r))
            .cloned()
            .collect::<Vec<_>>()
    };
    let (missing, extra) = (only(expected, ours), only(ours, expected));
    let show = |rows: &[Vec<i32>]| rows.iter().map(|r| label(r)).collect::<Vec<_>>().join("; ");
    let sources = missing
        .iter()
        .chain(&extra)
        .map(|r| match r.iter().filter(|&&id| id >= REPLICATED).count() {
            0 => "snapshot",
            n if n == r.len() => "replication",
            _ => "snapshot joined with replication",
        })
        .collect::<BTreeSet<_>>();
    Some(format!(
        "WRONG {what}: Readyset omits [{}] and adds [{}] (rows from: {})",
        show(&missing),
        show(&extra),
        sources.into_iter().collect::<Vec<_>>().join(", ")
    ))
}

/// Caches `query` in Readyset and runs it on both servers. Returns PostgreSQL's rows and Readyset's
/// rows. `Err` holds a finding, or `None` if both servers reject `query`.
async fn cache_and_read(
    pg: &Client,
    rs: &mut Readyset,
    what: &str,
    query: &str,
) -> Result<(Vec<Vec<i32>>, Vec<Vec<i32>>), Option<String>> {
    let expected = match (pg.simple_query(query).await, rs.create_cache(query).await) {
        (Ok(m), Ok(())) => int_rows(&m),
        (Err(_), Ok(())) => return Err(Some(format!("ACCEPTED {what}: PostgreSQL rejects it"))),
        (Ok(_), Err(e)) => return Err(Some(format!("REFUSED {what}: {e}"))),
        (Err(_), Err(_)) => return Err(None),
    };
    let ours = match rs.conn().await.simple_query(query).await {
        Ok(m) => int_rows(&m),
        Err(e) => return Err(Some(format!("ERROR {what}: Readyset: {}", message(&e)))),
    };
    match rs.served_from_cache().await {
        Ok(()) => Ok((expected, ours)),
        Err(dest) => Err(Some(format!("PROXIED {what}: destination {dest}"))),
    }
}

fn assert_no_findings(findings: Vec<String>) {
    assert!(
        findings.is_empty(),
        "{} findings:\n{}",
        findings.len(),
        findings.join("\n")
    );
}

/// Reads the value of `literal` and its type from PostgreSQL. `None` for NULL.
async fn value_of(pg: &Client, id: i32, literal: String) -> Result<Option<(Value, Type)>, String> {
    let sql = format!("SELECT {literal}");
    let msgs = pg.simple_query(&sql).await.map_err(|e| message(&e))?;
    let text = msgs.iter().find_map(|m| match m {
        SimpleQueryMessage::Row(r) => r.get(0).map(str::to_owned),
        _ => None,
    });
    let Some(text) = text else { return Ok(None) };
    let row = pg.query_one(&sql, &[]).await.map_err(|e| message(&e))?;
    let ty = row.columns()[0].type_().clone();
    Ok(Some((
        Value {
            id,
            literal,
            text,
            binary: row.try_get(0).ok(),
        },
        ty,
    )))
}

/// Creates database `db_name` and reads the literals of `types` from it. Omits each type that
/// PostgreSQL rejects, and each literal that it rejects, as PostgreSQL 13 rejects the special
/// NUMERIC values that later versions accept. A `LITERAL` finding means that PostgreSQL rejected
/// the type, or every literal of it.
async fn create_database(
    db_name: &str,
    types: Vec<(SqlType, Vec<String>)>,
) -> (Client, Vec<TypedValues>, Vec<String>) {
    readyset_tracing::init_test_logging();
    PostgreSQLAdapter::recreate_database(db_name).await;
    let mut cfg = psql_helpers::upstream_config();
    cfg.dbname(db_name);
    let setup = format!(
        "ALTER DATABASE {db_name} SET timezone TO 'UTC'; {}; CREATE TABLE sentinel (id int \
         PRIMARY KEY)",
        postgres_setup_sql()
    );
    psql_helpers::connect(cfg.clone())
        .await
        .simple_query(&setup)
        .await
        .unwrap();
    // A new session has the database's TimeZone and sees the new types.
    let pg = psql_helpers::connect(cfg).await;
    let (mut tys, mut findings) = (Vec::new(), Vec::new());
    for (ty, literals) in types {
        let name = ty.display(Dialect::PostgreSQL).to_string();
        if let Err(e) = pg
            .simple_query(&format!(
                "CREATE TEMP TABLE probe (v {name}); DROP TABLE probe"
            ))
            .await
        {
            findings.push(format!("LITERAL type {name}: PostgreSQL: {}", message(&e)));
            continue;
        }
        let (mut values, mut param_type) = (Vec::new(), None);
        for (id, literal) in literals.into_iter().enumerate() {
            // PostgreSQL 13 rejects the special NUMERIC values that later versions accept, so a
            // literal that it rejects is omitted.
            if let Ok(Some((v, t))) = value_of(&pg, id as i32, literal).await {
                param_type.get_or_insert(t);
                values.push(v);
            }
        }
        if values.is_empty() {
            findings.push(format!(
                "LITERAL type {name}: no literal that PostgreSQL accepts"
            ));
            continue;
        }
        tys.push(TypedValues {
            ty,
            name,
            param_type,
            values,
        });
    }
    (pg, tys, findings)
}

/// Starts Readyset on database `db_name`. Readyset snapshots the tables that exist at that time.
async fn start_readyset(
    db_name: &str,
) -> (Readyset, Handle, TestShutdownSender<PostgreSQLAdapter>) {
    let (opts, handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .migration_mode(MigrationMode::OutOfBand)
        .fallback(true)
        .replicate_db(db_name)
        .build::<PostgreSQLAdapter>()
        .await;
    let conn = psql_helpers::connect(opts.clone()).await;
    sleep().await;
    (Readyset { conn, opts }, handle, shutdown_tx)
}

const SENTINEL_QUERY: &str = "SELECT id FROM sentinel WHERE id = $1";

/// Runs `inserts` on PostgreSQL, and returns a finding if Readyset does not replicate them.
async fn replicate(pg: &Client, rs: &mut Readyset, inserts: &[String]) -> Option<String> {
    rs.create_cache(SENTINEL_QUERY).await.unwrap();
    for insert in inserts {
        pg.simple_query(insert).await.unwrap();
    }
    pg.simple_query("INSERT INTO sentinel VALUES (1)")
        .await
        .unwrap();
    for _ in 0..240 {
        if let Ok(rows) = rs.conn().await.query(SENTINEL_QUERY, &[&1i32]).await
            && !rows.is_empty()
            && rs.served_from_cache().await.is_ok()
        {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    Some("REPLICATION: Readyset did not replicate the rows inserted after the snapshot".to_owned())
}

/// An `INSERT` into `table` of `rows`, each an id and its literals. Adds `id_offset` to each id.
fn insert(table: &str, rows: &[(i32, String)], id_offset: i32) -> String {
    let rows = rows
        .iter()
        .map(|(id, r)| format!("({}, {r})", id + id_offset))
        .collect::<Vec<_>>();
    format!("INSERT INTO {table} VALUES {}", rows.join(", "))
}

/// Creates the tables for lookups and joins, then starts Readyset. For type `i`, `t<i>(id, v)`
/// holds each safe value and a NULL, `u<i>` is a copy of `t<i>` for the right side of a join, and
/// `h<i>_<id>` holds hazardous value `id` alone. Returns the inserts of the replicated rows.
async fn setup_keys(
    db_name: &str,
    types: Vec<(SqlType, Vec<String>)>,
) -> (
    Client,
    Readyset,
    Vec<TypedValues>,
    Vec<String>,
    Vec<String>,
    Handle,
    TestShutdownSender<PostgreSQLAdapter>,
) {
    let (pg, tys, findings) = create_database(db_name, types).await;
    let mut to_replicate = Vec::new();
    for (i, ty) in tys.iter().enumerate() {
        let rows = ty
            .safe_values()
            .map(|v| (v.id, v.literal.clone()))
            .collect::<Vec<_>>();
        for table in [format!("t{i}"), format!("u{i}")] {
            let create = format!("CREATE TABLE {table} (id int PRIMARY KEY, v {})", ty.name);
            pg.simple_query(&create).await.unwrap();
            // A SERIAL column rejects the NULL row, so this insert can fail.
            let null = format!("INSERT INTO {table} VALUES ({NULL_ID}, NULL)");
            let _ = pg.simple_query(&null).await;
            if !rows.is_empty() {
                pg.simple_query(&insert(&table, &rows, 0)).await.unwrap();
                to_replicate.push(insert(&table, &rows, REPLICATED));
            }
        }
        for v in ty.values.iter().filter(|v| is_hazardous(&v.literal)) {
            let h = format!("h{i}_{}", v.id);
            pg.simple_query(&format!(
                "CREATE TABLE {h} (id int PRIMARY KEY, v {}); INSERT INTO {h} VALUES (0, {})",
                ty.name, v.literal
            ))
            .await
            .unwrap();
        }
    }
    let (rs, handle, shutdown_tx) = start_readyset(db_name).await;
    (pg, rs, tys, findings, to_replicate, handle, shutdown_tx)
}

fn representative_types() -> Vec<(SqlType, Vec<String>)> {
    postgres_representative_types()
        .into_iter()
        .map(|ty| {
            let literals = postgres_edge_literals(&ty);
            (ty, literals)
        })
        .collect()
}

/// How a lookup sends its value: as a parameter of a prepared statement, or as a literal in a
/// simple query.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    TextParam,
    /// In binary format, as `DfValue` encodes it.
    BinaryParam,
    /// The value's literal, which has a cast to its type.
    CastLiteral,
    /// The value's text in quotes, with no cast. Used only for a value of the column's type.
    UntypedLiteral,
}

const MODES: [Mode; 4] = [
    Mode::TextParam,
    Mode::BinaryParam,
    Mode::CastLiteral,
    Mode::UntypedLiteral,
];

/// Looks up `value` in table `t<i>`. Returns PostgreSQL's ids, and Readyset's ids or its error.
/// `None` if PostgreSQL rejects the lookup, or if `value` lacks the binary form that `mode` needs.
async fn lookup(
    pg: &Client,
    rs: &mut Readyset,
    i: usize,
    param_type: &Type,
    value: &Value,
    mode: Mode,
) -> Option<(Vec<Vec<i32>>, Result<Vec<Vec<i32>>, String>)> {
    let ids = |rows: Vec<tokio_postgres::Row>| {
        let mut ids = rows
            .iter()
            .map(|r| vec![r.get::<_, i32>(0)])
            .collect::<Vec<_>>();
        ids.sort();
        ids
    };
    let (expected, ours) = match mode {
        Mode::CastLiteral | Mode::UntypedLiteral => {
            let literal = match mode {
                Mode::CastLiteral => value.literal.clone(),
                _ => format!("'{}'", value.text.replace('\'', "''")),
            };
            let query = format!("SELECT id FROM t{i} WHERE v = {literal}");
            let Ok(expected) = pg.simple_query(&query).await else {
                return None;
            };
            let ours = rs.conn().await.simple_query(&query).await;
            (int_rows(&expected), ours.map(|m| int_rows(&m)))
        }
        Mode::TextParam | Mode::BinaryParam => {
            let query = format!("SELECT id FROM t{i} WHERE v = $1");
            let types = std::slice::from_ref(param_type);
            let Ok(pg_stmt) = pg.prepare_typed(&query, types).await else {
                return None;
            };
            let text = TextFormat(value.text.clone());
            let p: &(dyn ToSql + Sync) = match (mode, &value.binary) {
                (Mode::TextParam, _) => &text,
                (_, Some(binary)) => binary,
                (_, None) => return None,
            };
            let Ok(expected) = pg.query(&pg_stmt, &[p]).await else {
                return None;
            };
            // Readyset proxies a statement after it fails once, so each value has a new statement.
            let ours = match rs.conn().await.prepare_typed(&query, types).await {
                Ok(stmt) => rs.conn().await.query(&stmt, &[p]).await.map(ids),
                Err(e) => Err(e),
            };
            (ids(expected), ours)
        }
    };
    Some((expected, ours.map_err(|e| message(&e))))
}

/// Looks up each of `values`, of type `param`, in column `col`, once in each of `modes`. If
/// Readyset proxies a simple query for every value, one `PROXIED` finding reports them all.
async fn lookup_findings(
    pg: &Client,
    rs: &mut Readyset,
    col: (usize, &TypedValues),
    param: &TypedValues,
    values: &[Value],
    modes: &[Mode],
) -> Vec<String> {
    let ((i, col), Some(param_type)) = (col, &param.param_type) else {
        return vec![];
    };
    let mut findings = Vec::new();
    for &mode in modes
        .iter()
        .filter(|&&m| m != Mode::UntypedLiteral || param.name == col.name)
    {
        let what = format!("lookup {} = {} ({mode:?})", col.name, param.name);
        let (mut proxied, mut served) = (Vec::new(), false);
        for value in values {
            let what = format!("{what} {}", value.literal);
            let Some((expected, ours)) = lookup(pg, rs, i, param_type, value, mode).await else {
                continue;
            };
            let ours = match ours {
                Ok(ours) => ours,
                Err(e) => {
                    let expected = expected.iter().map(|r| col.row_label(r[0]));
                    let expected = expected.collect::<Vec<_>>();
                    findings.push(format!(
                        "ERROR {what}: expected {expected:?}, Readyset: {e}"
                    ));
                    continue;
                }
            };
            match rs.served_from_cache().await {
                Err(dest) => proxied.push(format!("PROXIED {what}: destination {dest}")),
                Ok(()) => {
                    served = true;
                    findings.extend(diff(&what, &expected, &ours, |r| col.row_label(r[0])));
                }
            }
        }
        if matches!(mode, Mode::CastLiteral | Mode::UntypedLiteral) && !served && proxied.len() > 1
        {
            let n = proxied.len();
            findings.push(format!("{} (the same for all {n} values)", proxied[0]));
        } else {
            findings.extend(proxied);
        }
    }
    findings
}

/// For each column type of a kind in `kinds`, looks up every value of every type in every mode.
async fn lookups_match_postgres(db_name: &str, kinds: &[Kind]) {
    let (pg, mut rs, tys, mut findings, to_replicate, _handle, shutdown_tx) =
        setup_keys(db_name, representative_types()).await;
    let cols = tys
        .iter()
        .enumerate()
        .filter(|(_, t)| kinds.contains(&kind(&t.ty)))
        .collect::<Vec<_>>();
    for &(i, col) in &cols {
        if let Err(e) = rs
            .create_cache(&format!("SELECT id FROM t{i} WHERE v = $1"))
            .await
        {
            findings.push(format!("REFUSED lookup in {} column: {e}", col.name));
        }
        for v in col.values.iter().filter(|v| is_hazardous(&v.literal)) {
            if let Err(e) = rs
                .create_cache(&format!("SELECT id FROM h{i}_{}", v.id))
                .await
            {
                findings.push(format!("REFUSED table that holds only {}: {e}", v.literal));
            }
        }
    }
    findings.extend(replicate(&pg, &mut rs, &to_replicate).await);
    for &col in &cols {
        for param in &tys {
            findings.extend(lookup_findings(&pg, &mut rs, col, param, &param.values, &MODES).await);
        }
    }
    shutdown_tx.shutdown().await;
    assert_no_findings(findings);
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(postgres, all)]
#[ignore = "REA-7274: Readyset disagrees with PostgreSQL on some type comparisons"]
async fn number_lookups_match_postgres() {
    lookups_match_postgres("key_oracle_number_lookups", NUMBERS).await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(postgres, all)]
#[ignore = "REA-7274: Readyset disagrees with PostgreSQL on some type comparisons"]
async fn string_lookups_match_postgres() {
    lookups_match_postgres("key_oracle_string_lookups", &[Kind::String]).await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(postgres, all)]
#[ignore = "REA-7274: Readyset disagrees with PostgreSQL on some type comparisons"]
async fn other_lookups_match_postgres() {
    lookups_match_postgres("key_oracle_other_lookups", OTHERS).await;
}

/// Pairs of types that PostgreSQL refuses to join on `=`. Readyset must refuse to cache each join.
const REJECTED_JOINS: [(&str, &str); 5] = [
    ("INT", "TEXT"),
    ("NUMERIC", "VARCHAR(5)"),
    ("DATE", "TEXT"),
    ("BOOL", "TEXT"),
    ("TIMESTAMP", "VARCHAR(5)"),
];

/// Joins each type of a kind in `kinds` to each type of its category that PostgreSQL joins it to.
/// With `check_rejected`, also checks that Readyset refuses to cache `REJECTED_JOINS`.
async fn joins_match_postgres(db_name: &str, kinds: &[Kind], check_rejected: bool) {
    let (pg, mut rs, tys, mut findings, to_replicate, _handle, shutdown_tx) =
        setup_keys(db_name, representative_types()).await;
    findings.extend(replicate(&pg, &mut rs, &to_replicate).await);
    for (i, l) in tys.iter().enumerate() {
        for (j, r) in tys.iter().enumerate() {
            let rejected = check_rejected
                && REJECTED_JOINS
                    .iter()
                    .any(|&(a, b)| a == l.name && b == r.name);
            if !rejected && !(kinds.contains(&kind(&l.ty)) && same_category(&l.ty, &r.ty)) {
                continue;
            }
            let query = format!("SELECT l.id, r.id FROM t{i} l JOIN u{j} r ON l.v = r.v");
            if !rejected && pg.prepare(&query).await.is_err() {
                continue;
            }
            let what = format!("join {} = {}", l.name, r.name);
            let (expected, ours) = match cache_and_read(&pg, &mut rs, &what, &query).await {
                Ok(rows) => rows,
                Err(f) => {
                    findings.extend(f);
                    continue;
                }
            };
            let label = |p: &[i32]| format!("{} = {}", l.row_label(p[0]), r.row_label(p[1]));
            findings.extend(diff(&what, &expected, &ours, label));
        }
    }
    shutdown_tx.shutdown().await;
    assert_no_findings(findings);
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(postgres, all)]
#[ignore = "REA-7274: Readyset disagrees with PostgreSQL on some type comparisons"]
async fn number_joins_match_postgres() {
    joins_match_postgres("key_oracle_number_joins", NUMBERS, false).await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(postgres, all)]
#[ignore = "REA-7274: Readyset disagrees with PostgreSQL on some type comparisons"]
async fn string_joins_match_postgres() {
    joins_match_postgres("key_oracle_string_joins", &[Kind::String], false).await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(postgres, all)]
#[ignore = "REA-7274: Readyset disagrees with PostgreSQL on some type comparisons"]
async fn other_joins_match_postgres() {
    joins_match_postgres("key_oracle_other_joins", OTHERS, true).await;
}

/// Arbitrary PostgreSQL types that have literals, each with its edge values and some random values.
fn arbitrary_types(runner: &mut TestRunner) -> Vec<(SqlType, Vec<String>)> {
    let strategy = any_with::<SqlType>(SqlTypeArbitraryOptions {
        dialect: Dialect::PostgreSQL,
        ..Default::default()
    });
    let mut seen = BTreeSet::new();
    let mut types = Vec::new();
    while types.len() < 40 {
        let ty = strategy.new_tree(runner).unwrap().current();
        let Some(values) = arbitrary_postgres_literal(&ty) else {
            continue;
        };
        if !seen.insert(ty.display(Dialect::PostgreSQL).to_string()) {
            continue;
        }
        let mut literals = postgres_edge_literals(&ty);
        literals.extend((0..8).map(|_| values.new_tree(runner).unwrap().current()));
        types.push((ty, literals));
    }
    types
}

/// Looks up arbitrary values of arbitrary types in columns of arbitrary types, in arbitrary modes.
/// Runs every case, then lists each finding except `PROXIED`, which the edge-value tests report.
#[tags(serial)]
#[upstream(postgres, all)]
#[ignore = "REA-7274: Readyset disagrees with PostgreSQL on some type comparisons"]
#[test]
fn arbitrary_lookups_match_postgres() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut runner = TestRunner::new(ProptestConfig {
        cases: 2000,
        ..Default::default()
    });
    let types = arbitrary_types(&mut runner);
    let (pg, rs, tys, findings, _handle, shutdown_tx) = rt.block_on(async {
        let (pg, mut rs, tys, mut findings, to_replicate, handle, shutdown_tx) =
            setup_keys("key_oracle_arbitrary", types).await;
        for i in 0..tys.len() {
            let _ = rs
                .create_cache(&format!("SELECT id FROM t{i} WHERE v = $1"))
                .await;
        }
        findings.extend(replicate(&pg, &mut rs, &to_replicate).await);
        let findings = std::sync::Mutex::new(findings);
        (
            pg,
            tokio::sync::Mutex::new(rs),
            tys,
            findings,
            handle,
            shutdown_tx,
        )
    });
    let params = (0..tys.len())
        .filter(|&p| tys[p].param_type.is_some())
        .collect::<Vec<_>>();
    let strategy = (0..tys.len(), proptest::sample::select(params)).prop_flat_map(|(c, p)| {
        let values = arbitrary_postgres_literal(&tys[p].ty).expect("every type has literals");
        (
            Just(c),
            Just(p),
            values,
            proptest::sample::select(MODES[..3].to_vec()),
        )
    });
    runner
        .run(&strategy, |(c, p, literal, mode)| {
            let found = rt.block_on(async {
                let (value, _) = value_of(&pg, 0, literal).await.ok().flatten()?;
                let mut rs = rs.lock().await;
                Some(lookup_findings(&pg, &mut rs, (c, &tys[c]), &tys[p], &[value], &[mode]).await)
            });
            let reported = found
                .into_iter()
                .flatten()
                .filter(|f| !f.starts_with("PROXIED"));
            let mut findings = findings.lock().unwrap();
            for f in reported {
                if !findings.contains(&f) {
                    findings.push(f);
                }
            }
            Ok(())
        })
        .unwrap();
    rt.block_on(shutdown_tx.shutdown());
    assert_no_findings(findings.into_inner().unwrap());
}

const OPS: [&str; 6] = ["=", "<>", "<", "<=", ">", ">="];

/// A few safe values of `ty`, first those that comparisons often get wrong, and NULL.
fn filter_values(ty: &TypedValues) -> Vec<String> {
    const PREFERRED: [&str; 14] = [
        "'-0'",
        "'0'",
        "NaN",
        "Infinity",
        "' '",
        "'a '",
        "' a'",
        "'a'",
        "'A'",
        "24:00",
        "'1.5'",
        "'2'",
        "1.0",
        "test_enum",
    ];
    let safe = ty
        .safe_values()
        .map(|v| v.literal.clone())
        .collect::<Vec<_>>();
    let (first, rest): (Vec<_>, Vec<_>) = safe
        .into_iter()
        .partition(|l| PREFERRED.iter().any(|p| l.contains(p)));
    let mut values = first.into_iter().chain(rest).take(7).collect::<Vec<_>>();
    values.push("NULL".to_owned());
    values
}

/// For each pair of types of one category whose left type is of a kind in `kinds`, checks `a OP b`
/// and `NOT (a OP b)` over pairs of their values, for each operator that PostgreSQL accepts. With
/// `rows` set, each filter compares rows that carry `a` and `b` in their first position.
async fn filters_match_postgres(db_name: &str, kinds: &[Kind], as_rows: bool) {
    let (pg, tys, mut findings) = create_database(db_name, representative_types()).await;
    // Per table `p<i>_<j>`: its query, its column types, the literals of each row, and its filters.
    let mut tables = Vec::new();
    let mut to_replicate = Vec::new();
    let operands = |left: &str, right: &str| {
        if as_rows {
            (format!("ROW({left}, 1)"), format!("ROW({right}, 1)"))
        } else {
            (left.to_owned(), right.to_owned())
        }
    };
    for (i, l) in tys.iter().enumerate() {
        for (j, r) in tys.iter().enumerate() {
            if !kinds.contains(&kind(&l.ty)) || !same_category(&l.ty, &r.ty) {
                continue;
            }
            let mut filters = Vec::new();
            let (a, b) = operands("a", "b");
            let (null_a, null_b) =
                operands(&format!("NULL::{}", l.name), &format!("NULL::{}", r.name));
            for op in OPS {
                let sql = format!("SELECT {null_a} {op} {null_b}");
                if pg.prepare(&sql).await.is_ok() {
                    filters.extend([format!("{a} {op} {b}"), format!("NOT ({a} {op} {b})")]);
                }
            }
            if filters.is_empty() {
                continue;
            }
            let p = format!("p{i}_{j}");
            let create = format!(
                "CREATE TABLE {p} (id int PRIMARY KEY, a {}, b {})",
                l.name, r.name
            );
            pg.simple_query(&create).await.unwrap();
            let rv = filter_values(r);
            let rows = filter_values(l)
                .into_iter()
                .flat_map(|a| rv.iter().map(move |b| (a.clone(), b.clone())))
                .collect::<Vec<_>>();
            let numbered = rows
                .iter()
                .enumerate()
                .map(|(id, (a, b))| (id as i32, format!("{a}, {b}")));
            let numbered = numbered.collect::<Vec<_>>();
            pg.simple_query(&insert(&p, &numbered, 0)).await.unwrap();
            to_replicate.push(insert(&p, &numbered, REPLICATED));
            // `CREATE CACHE` takes one `SELECT`, so a view combines the filters into one cache.
            let union = filters
                .iter()
                .enumerate()
                .map(|(k, f)| format!("SELECT id, {k} AS k FROM {p} WHERE {f}"))
                .collect::<Vec<_>>()
                .join(" UNION ALL ");
            pg.simple_query(&format!("CREATE VIEW v{p} AS {union}"))
                .await
                .unwrap();
            tables.push((
                format!("SELECT id, k FROM v{p}"),
                format!("(a {}, b {})", l.name, r.name),
                rows,
                filters,
            ));
        }
    }
    let (mut rs, _handle, shutdown_tx) = start_readyset(db_name).await;
    findings.extend(replicate(&pg, &mut rs, &to_replicate).await);
    for (query, columns, rows, filters) in &tables {
        let what = format!("filter {columns}");
        let (expected, ours) = match cache_and_read(&pg, &mut rs, &what, query).await {
            Ok(rows) => rows,
            Err(f) => {
                findings.extend(f);
                continue;
            }
        };
        for (k, filter) in filters.iter().enumerate() {
            let selected = |got: &[Vec<i32>]| {
                got.iter()
                    .filter(|r| r[1] == k as i32)
                    .map(|r| vec![r[0]])
                    .collect::<Vec<_>>()
            };
            let label = |r: &[i32]| {
                let (a, b) = &rows[(r[0] % REPLICATED) as usize];
                format!(
                    "({a}, {b}){}",
                    if r[0] >= REPLICATED { " (repl)" } else { "" }
                )
            };
            findings.extend(diff(
                &format!("{what} WHERE {filter}"),
                &selected(&expected),
                &selected(&ours),
                label,
            ));
        }
    }
    shutdown_tx.shutdown().await;
    assert_no_findings(findings);
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(postgres, all)]
async fn int_filters_match_postgres() {
    filters_match_postgres("key_oracle_int_filters", &[Kind::Int], false).await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(postgres, all)]
async fn numeric_filters_match_postgres() {
    filters_match_postgres("key_oracle_numeric_filters", &[Kind::Numeric], false).await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(postgres, all)]
async fn float_filters_match_postgres() {
    filters_match_postgres("key_oracle_float_filters", &[Kind::Float], false).await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(postgres, all)]
async fn number_row_filters_match_postgres() {
    filters_match_postgres(
        "key_oracle_number_row_filters",
        &[Kind::Int, Kind::Numeric, Kind::Float],
        true,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(postgres, all)]
#[ignore = "REA-7274: Readyset disagrees with PostgreSQL on some type comparisons"]
async fn string_filters_match_postgres() {
    filters_match_postgres("key_oracle_string_filters", &[Kind::String], false).await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(postgres, all)]
#[ignore = "REA-7274: Readyset disagrees with PostgreSQL on some type comparisons"]
async fn other_scalar_filters_match_postgres() {
    filters_match_postgres("key_oracle_other_filters", &[Kind::Other], false).await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(postgres, all)]
#[ignore = "REA-7274: Readyset disagrees with PostgreSQL on some type comparisons"]
async fn array_filters_match_postgres() {
    filters_match_postgres("key_oracle_array_filters", &[Kind::Array], false).await;
}
