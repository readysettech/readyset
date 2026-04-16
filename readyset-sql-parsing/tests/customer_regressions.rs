//! Parity and round-trip checks for queries captured from real customer workloads.
//!
//! Customer queries are too sensitive to check into the public repo, so they live in the
//! private parent worktree under `customers/sqlparser-regressions/`. This test reads that
//! directory at runtime and, for each `.sql` file, runs every statement through the parser
//! (both nom-sql and sqlparser via `BothErrorOnMismatch`) and a display round-trip.
//!
//! File format: statements separated by blank lines and terminated by `;`. Lines beginning
//! with `--` are comments. A header directive `-- dialect: mysql|postgresql|both` (default
//! `both`) selects which dialect(s) to exercise.
//!
//! Failures are accumulated and reported at the end with `file:line` for each failing query.
//! If the directory is missing (e.g. a public-only checkout) the test is a no-op.
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::{Path, PathBuf};

use readyset_sql::{Dialect, DialectDisplay};
use readyset_sql_parsing::{ParsingPreset, parse_query_with_config};

fn customers_dir() -> PathBuf {
    // CARGO_MANIFEST_DIR is .../public/readyset-sql-parsing; customers/ lives two levels up.
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest
        .ancestors()
        .nth(2)
        .expect("CARGO_MANIFEST_DIR should have at least two ancestors")
        .join("customers")
        .join("sqlparser-regressions")
}

#[derive(Debug, Clone, Copy)]
enum DialectChoice {
    MySql,
    Postgres,
    Both,
}

impl DialectChoice {
    fn dialects(self) -> &'static [Dialect] {
        match self {
            Self::MySql => &[Dialect::MySQL],
            Self::Postgres => &[Dialect::PostgreSQL],
            Self::Both => &[Dialect::MySQL, Dialect::PostgreSQL],
        }
    }
}

fn parse_dialect_directive(content: &str) -> DialectChoice {
    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        if let Some(rest) = trimmed.strip_prefix("--") {
            let rest = rest.trim();
            if let Some(value) = rest.strip_prefix("dialect:") {
                return match value.trim().to_ascii_lowercase().as_str() {
                    "mysql" => DialectChoice::MySql,
                    "postgres" | "postgresql" | "pg" => DialectChoice::Postgres,
                    "both" => DialectChoice::Both,
                    other => panic!("unknown dialect directive `{other}`"),
                };
            }
            continue;
        }
        break;
    }
    DialectChoice::Both
}

/// Split a file into `(start_line, sql)` pairs.
///
/// Comment-only lines and blank lines outside of a statement are skipped. A statement begins
/// at the first non-blank, non-comment line and ends on the next line whose trimmed suffix
/// is `;`. The starting line is 1-indexed so it lines up with editor navigation.
fn split_queries(content: &str) -> Vec<(usize, String)> {
    let mut queries = Vec::new();
    let mut current = String::new();
    let mut start_line = 0;
    for (i, line) in content.lines().enumerate() {
        let line_num = i + 1;
        if current.is_empty() {
            let trimmed = line.trim_start();
            if trimmed.is_empty() || trimmed.starts_with("--") {
                continue;
            }
            start_line = line_num;
        }
        if !current.is_empty() {
            current.push('\n');
        }
        current.push_str(line);
        if line.trim_end().ends_with(';') {
            queries.push((start_line, std::mem::take(&mut current)));
        }
    }
    let trailing = current.trim().to_string();
    if !trailing.is_empty() {
        queries.push((start_line, trailing));
    }
    queries
}

fn check_query(dialect: Dialect, sql: &str) -> Result<(), String> {
    let config = ParsingPreset::BothErrorOnMismatch
        .into_config()
        .log_on_mismatch(true);
    let ast =
        parse_query_with_config(config, dialect, sql).map_err(|e| format!("parse failed: {e}"))?;
    let displayed = ast.display(dialect).to_string();
    let ast2 = parse_query_with_config(config, dialect, &displayed)
        .map_err(|e| format!("round-trip reparse failed: {e}\n  displayed: {displayed}"))?;
    if ast != ast2 {
        return Err(format!(
            "round-trip AST mismatch\n  displayed: {displayed}"
        ));
    }
    Ok(())
}

fn describe_panic(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else if let Some(s) = payload.downcast_ref::<&'static str>() {
        (*s).to_string()
    } else {
        "<non-string panic payload>".to_string()
    }
}

struct Failure {
    file: PathBuf,
    line: usize,
    dialect: Dialect,
    panicked: bool,
    message: String,
    sql: String,
}

fn run_file(path: &Path, failures: &mut Vec<Failure>) -> usize {
    let content = std::fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("failed to read {}: {e}", path.display()));
    let choice = parse_dialect_directive(&content);
    let queries = split_queries(&content);
    let mut count = 0;
    for (line, sql) in queries {
        for &dialect in choice.dialects() {
            count += 1;
            let result = catch_unwind(AssertUnwindSafe(|| check_query(dialect, &sql)));
            match result {
                Ok(Ok(())) => {}
                Ok(Err(msg)) => failures.push(Failure {
                    file: path.to_path_buf(),
                    line,
                    dialect,
                    panicked: false,
                    message: msg,
                    sql: sql.clone(),
                }),
                Err(payload) => failures.push(Failure {
                    file: path.to_path_buf(),
                    line,
                    dialect,
                    panicked: true,
                    message: describe_panic(payload),
                    sql: sql.clone(),
                }),
            }
        }
    }
    count
}

#[test]
fn customer_regressions() {
    let dir = customers_dir();
    if !dir.is_dir() {
        eprintln!(
            "customer_regressions: `{}` not present; skipping (public-only checkout?)",
            dir.display()
        );
        return;
    }

    let mut sql_files: Vec<PathBuf> = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("read_dir {}: {e}", dir.display()))
        .filter_map(|entry| entry.ok().map(|e| e.path()))
        .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("sql"))
        .collect();
    sql_files.sort();

    if sql_files.is_empty() {
        eprintln!(
            "customer_regressions: no .sql files in `{}`; skipping",
            dir.display()
        );
        return;
    }

    let mut failures: Vec<Failure> = Vec::new();
    let mut total = 0;
    for path in &sql_files {
        total += run_file(path, &mut failures);
    }

    if !failures.is_empty() {
        use std::fmt::Write as _;
        let mut report = String::new();
        writeln!(
            report,
            "{} failure(s) across {} dialect-query pairs in {} file(s):",
            failures.len(),
            total,
            sql_files.len()
        )
        .ok();
        for f in &failures {
            let kind = if f.panicked { "panic" } else { "error" };
            writeln!(
                report,
                "\n{}:{} [{:?}] ({kind})\n  sql: {}\n  {}",
                f.file.display(),
                f.line,
                f.dialect,
                f.sql.replace('\n', "\n       "),
                f.message.replace('\n', "\n  ")
            )
            .ok();
        }
        panic!("{report}");
    }

    eprintln!(
        "customer_regressions: {total} dialect-query pairs passed across {} file(s)",
        sql_files.len()
    );
}

#[cfg(test)]
mod unit {
    use super::*;

    #[test]
    fn split_simple() {
        let input = "-- header\n\nSELECT 1;\n\nSELECT 2;\n";
        let qs = split_queries(input);
        assert_eq!(qs.len(), 2);
        assert_eq!(qs[0].0, 3);
        assert_eq!(qs[0].1, "SELECT 1;");
        assert_eq!(qs[1].0, 5);
        assert_eq!(qs[1].1, "SELECT 2;");
    }

    #[test]
    fn split_multiline_with_indent() {
        let input = "SELECT 1,\n  2\n  FROM t;\n\n  SELECT 3;\n";
        let qs = split_queries(input);
        assert_eq!(qs.len(), 2);
        assert_eq!(qs[0].0, 1);
        assert_eq!(qs[0].1, "SELECT 1,\n  2\n  FROM t;");
        assert_eq!(qs[1].0, 5);
        assert_eq!(qs[1].1, "  SELECT 3;");
    }

    #[test]
    fn dialect_directive_parsing() {
        assert!(matches!(
            parse_dialect_directive("-- dialect: mysql\nSELECT 1;"),
            DialectChoice::MySql
        ));
        assert!(matches!(
            parse_dialect_directive("-- dialect: postgresql\nSELECT 1;"),
            DialectChoice::Postgres
        ));
        assert!(matches!(
            parse_dialect_directive("-- dialect: both\nSELECT 1;"),
            DialectChoice::Both
        ));
        assert!(matches!(
            parse_dialect_directive("SELECT 1;"),
            DialectChoice::Both
        ));
    }
}
