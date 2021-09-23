use std::convert::TryFrom;
use std::path::PathBuf;

use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncWriteExt, BufReader};

use anyhow::anyhow;
use clap::Clap;
use itertools::Itertools;

use nom_sql::{
    parse_query, Dialect, Expression, FieldDefinitionExpression, FunctionExpression, SqlQuery,
};

use crate::ast::{Record, Statement, StatementResult, Value};
use crate::upstream::{DatabaseConnection, DatabaseURL};

mod querylog;
use querylog::{Command, Entry, Session, Stream};

/// Convert a MySQL query log to a set of test scripts.
#[derive(Clap)]
pub struct FromQueryLog {
    /// URL of a reference database to connect to, execute queries frmo the log, and record the
    /// results.  Currently supports `mysql://` URLs, but may be expanded in the future.
    #[clap(long, parse(try_from_str))]
    pub database: DatabaseURL,

    /// Enable verbose output
    #[clap(long, short = 'v')]
    pub verbose: bool,

    /// Generate a separate logictest file for each client session in the querylog
    #[clap(long)]
    pub split_sessions: bool,

    /// Query log to convert
    pub input: PathBuf,

    /// Directory to output logic tests into
    pub output: PathBuf,
}

fn should_validate_results(query: &str, parsed_query: &Option<SqlQuery>) -> bool {
    if let Some(parsed_query) = parsed_query {
        if let SqlQuery::Select(ref select) = parsed_query {
            if select.tables.is_empty() {
                for field in &select.fields {
                    if let FieldDefinitionExpression::Expression { expr, .. } = field {
                        match expr {
                            Expression::Call(FunctionExpression::Call { name, .. }) => {
                                match name.as_str() {
                                    "VERSION" => return false,
                                    "DATABASE" => return false,
                                    _ => (),
                                }
                            }
                            Expression::Column(column) if column.name.starts_with("@@") => {
                                return false
                            }
                            _ => (),
                        };
                    }
                }
            }
        } else {
            return false;
        }
    } else {
        // WARNING:  Here be hacks.
        // These conditionals work around nom_sql not parsing SHOW and certain SELECT queries.
        // From what I've seen thus far, subselects and SELECT DATABASE() are the SELECT queries
        // that fail.  In a future with flawless parsing, we can move the SHOW TABLES check to be a
        // SqlQuery::Show case above, and unconditionally return false here.
        let query = query.trim().to_ascii_uppercase();
        if !query.starts_with("SELECT") {
            if query != "SHOW TABLES" {
                return false;
            }
        } else if query.split(' ').contains(&"DATABASE()") {
            return false;
        }
    }
    true
}

async fn process_query(entry: &Entry, conn: &mut DatabaseConnection) -> anyhow::Result<Record> {
    let parsed = parse_query(Dialect::MySQL, &entry.arguments).ok();
    let record = match conn.query(&entry.arguments).await {
        Ok(rows) => {
            if !should_validate_results(&entry.arguments, &parsed) {
                Record::Statement(Statement {
                    result: StatementResult::Ok,
                    command: entry.arguments.clone(),
                    conditionals: vec![],
                })
            } else {
                Record::query(entry.arguments.clone(), parsed.as_ref(), vec![], rows)
            }
        }
        Err(_) => Record::Statement(Statement {
            result: StatementResult::Error,
            command: entry.arguments.clone(),
            conditionals: vec![],
        }),
    };
    Ok(record)
}

async fn process_execute(
    session: &Session,
    entry: &Entry,
    conn: &mut DatabaseConnection,
) -> anyhow::Result<Record> {
    let parsed = parse_query(Dialect::MySQL, &entry.arguments).map_err(|e| anyhow!(e))?;
    let (stmt, values) = session
        .find_prepared_statement(&parsed)
        .ok_or_else(|| anyhow!("Prepared statement not found"))?;
    let params = values
        .into_iter()
        .map(Value::try_from)
        .collect::<Result<Vec<_>, _>>()?;
    let rows = conn.execute(stmt.to_string(), &params).await?;
    if should_validate_results(&stmt.to_string(), &Some(parsed)) {
        Ok(Record::query(stmt.to_string(), Some(stmt), params, rows))
    } else {
        Ok(Record::Statement(Statement {
            result: StatementResult::Ok,
            command: stmt.to_string(),
            conditionals: vec![],
        }))
    }
}

impl FromQueryLog {
    #[tokio::main]
    pub async fn run(self) -> anyhow::Result<()> {
        let input = File::open(&self.input).await.unwrap();
        let mut input = Stream::new(BufReader::new(input), self.split_sessions);

        while let Some((session_number, mut session)) = input.next().await {
            // It is intentional to spin up a new connection for each session, so that we match the
            // logged behavior as closely as possible.
            let mut conn = self.database.connect().await.unwrap();
            let mut output = OpenOptions::new()
                .read(false)
                .write(true)
                .create(true)
                .truncate(true)
                .append(false)
                .open(self.output.join(session_number.to_string() + ".test"))
                .await
                .unwrap();
            for entry in &session.entries {
                let record = match entry.command {
                    Command::Connect => None,
                    Command::Query => process_query(entry, &mut conn).await.ok(),
                    Command::Prepare => {
                        let parsed = match parse_query(Dialect::MySQL, &entry.arguments) {
                            Ok(v) => v,
                            Err(e) => {
                                eprintln!(
                                    "!!! (prepare) Failed to parse {}:\n{}",
                                    &entry.arguments, e
                                );
                                continue;
                            }
                        };
                        session.prepared_statements.insert(parsed);
                        None
                    }
                    Command::Execute => match process_execute(&session, entry, &mut conn).await {
                        Ok(v) => Some(v),
                        Err(e) => {
                            eprintln!("!!! (execute) Error with {}:  {}", entry.arguments, e);
                            continue;
                        }
                    },
                    Command::CloseStmt => None,
                    Command::Quit => None,
                };
                if let Some(record) = record {
                    output
                        .write(format!("{}\n", record).as_bytes())
                        .await
                        .unwrap();
                }
            }
            output.flush().await.unwrap();
        }
        Ok(())
    }
}
