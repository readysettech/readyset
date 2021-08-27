use std::path::PathBuf;

use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncWriteExt, BufReader};

use clap::Clap;
use itertools::Itertools;

use nom_sql::{
    parse_query, Dialect, Expression, FieldDefinitionExpression, FunctionExpression, SqlQuery,
};

use crate::ast::{Query, QueryParams, QueryResults, Record, SortMode, Statement, StatementResult};
use crate::upstream::DatabaseURL;

mod querylog;
use querylog::{Command, Stream};

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

impl FromQueryLog {
    #[tokio::main]
    pub async fn run(self) -> anyhow::Result<()> {
        let input = File::open(&self.input).await.unwrap();
        let mut input = Stream::new(BufReader::new(input), self.split_sessions);

        while let Some((session_number, session)) = input.next().await {
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
            for entry in session.entries {
                match entry.command {
                    Command::Connect => (),
                    Command::Query => {
                        let parsed = parse_query(Dialect::MySQL, &entry.arguments).ok();
                        let record = match conn.query(&entry.arguments).await {
                            Ok(mut rows) => {
                                if !should_validate_results(&entry.arguments, &parsed) {
                                    Record::Statement(Statement {
                                        result: StatementResult::Ok,
                                        command: entry.arguments,
                                        conditionals: vec![],
                                    })
                                } else {
                                    Record::Query(Query {
                                        label: None,
                                        column_types: None,
                                        sort_mode: Some(match parsed {
                                            Some(SqlQuery::Select(ref select))
                                                if select.order.is_some() =>
                                            {
                                                SortMode::NoSort
                                            }
                                            _ => {
                                                rows.sort();
                                                SortMode::RowSort
                                            }
                                        }),
                                        conditionals: vec![],
                                        query: entry.arguments,
                                        results: QueryResults::hash(
                                            &rows.into_iter().flatten().collect::<Vec<_>>(),
                                        ),
                                        params: QueryParams::PositionalParams(vec![]),
                                    })
                                }
                            }
                            Err(_) => Record::Statement(Statement {
                                result: StatementResult::Error,
                                command: entry.arguments,
                                conditionals: vec![],
                            }),
                        };
                        output
                            .write(format!("{}\n", record).as_bytes())
                            .await
                            .unwrap();
                    }
                    Command::Prepare => todo!(),
                    Command::Execute => todo!(),
                    Command::CloseStmt => todo!(),
                    Command::Quit => (),
                }
            }
            output.flush().await.unwrap();
        }
        Ok(())
    }
}
