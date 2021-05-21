use std::convert::TryFrom;
use std::convert::TryInto;
use std::fs::File;
use std::io::{self, Seek, SeekFrom};
use std::path::PathBuf;
use std::str::FromStr;

use anyhow::{anyhow, bail, Context};
use clap::Clap;
use colored::Colorize;
use mysql::prelude::Queryable;
use nom_sql::{parse_query, SqlQuery};
use query_generator::GeneratorState;

use crate::ast::{Query, QueryResults, Record, SortMode, Statement, Value};
use crate::runner::TestScript;

/// URL for a database to compare against. In the future we likely will want to add more of these
pub enum DatabaseURL {
    MySQL(mysql::Opts),
}

impl DatabaseURL {
    pub fn connect(&self) -> anyhow::Result<DatabaseConnection> {
        match self {
            DatabaseURL::MySQL(opts) => {
                Ok(DatabaseConnection::MySQL(mysql::Conn::new(opts.clone())?))
            }
        }
    }
}

pub enum DatabaseConnection {
    MySQL(mysql::Conn),
}

impl DatabaseConnection {
    fn query_drop<Q>(&mut self, stmt: Q) -> anyhow::Result<()>
    where
        Q: AsRef<str>,
    {
        match self {
            DatabaseConnection::MySQL(conn) => Ok(conn.query_drop(stmt)?),
        }
    }

    fn query<Q>(&mut self, query: Q) -> anyhow::Result<Vec<Vec<Value>>>
    where
        Q: AsRef<str>,
    {
        match self {
            DatabaseConnection::MySQL(conn) => conn
                .query_iter(query)?
                .map(|r| {
                    let mut r = r?;
                    Ok((0..r.columns().len())
                        .map(|c| Value::try_from(r.take::<mysql::Value, _>(c).unwrap()))
                        .collect::<Result<Vec<_>, _>>()?)
                })
                .collect::<Result<Vec<_>, _>>(),
        }
    }

    fn run_script(&mut self, script: &TestScript) -> anyhow::Result<()> {
        match self {
            DatabaseConnection::MySQL(conn) => script.run_on_mysql(conn, false),
        }
    }
}

impl FromStr for DatabaseURL {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.starts_with("mysql://") {
            Ok(Self::MySQL(mysql::Opts::from_url(s)?))
        } else {
            Err(anyhow!("Invalid URL format"))
        }
    }
}

/// Use a test script containing DDL and queries, but no data, as a seed to generate a test script
/// containing data with results compared against a reference database
#[derive(Clap)]
pub struct Generate {
    /// Test script to use as a seed. Seed scripts should contain DDL and queries, but no data.
    #[clap(parse(from_str))]
    from: PathBuf,

    /// URL of a reference database to compare to. Currently supports `mysql://` URLs, but may be
    /// expanded in the future
    #[clap(long, parse(try_from_str))]
    compare_to: DatabaseURL,

    /// File to write results to (defaults to stdout)
    #[clap(short = 'o')]
    output: Option<PathBuf>,

    /// Rows of data to generate per table
    #[clap(long, default_value = "100")]
    rows_per_table: usize,

    /// Enable verbose output
    #[clap(long, short = 'v')]
    verbose: bool,

    /// Enable randomly generating column data.
    #[clap(long)]
    random: bool,
}

fn write_output<W, I>(original_file: &mut File, new_entries: I, output: &mut W) -> io::Result<()>
where
    W: io::Write,
    I: IntoIterator<Item = Record>,
{
    original_file.seek(SeekFrom::Start(0))?;
    io::copy(original_file, output)?;
    writeln!(output)?;
    for rec in new_entries {
        writeln!(output, "{}", rec)?;
    }
    Ok(())
}

impl Generate {
    pub fn run(self) -> anyhow::Result<()> {
        let mut file = File::open(&self.from)?;
        let script = TestScript::read(self.from.clone(), &mut file)?;

        let mut hash_threshold = 20;

        let mut new_entries = vec![];

        let mut relations_to_drop = vec![];
        let mut tables = vec![];
        let mut queries = vec![];

        for record in script.records() {
            match record {
                Record::Statement(Statement { command, .. }) => {
                    match parse_query(command).map_err(|s| anyhow!("{}", s))? {
                        SqlQuery::CreateTable(tbl) => {
                            relations_to_drop.push(("TABLE", tbl.table.name.clone()));
                            tables.push(tbl)
                        }
                        SqlQuery::CreateView(view) => {
                            relations_to_drop.push(("VIEW", view.name.clone()));
                        }
                        _ => {}
                    }
                }
                Record::Query(query) => {
                    if !query.params.is_empty() {
                        bail!("Queries with params aren't supported yet");
                    }
                    queries.push(query);
                }
                Record::HashThreshold(ht) => {
                    hash_threshold = *ht;
                }
                Record::Halt => break,
            }
        }

        let mut conn = self.compare_to.connect()?;

        eprintln!(
            "{}",
            format!("==> Dropping {} relations", relations_to_drop.len()).bold()
        );
        relations_to_drop.reverse();
        for (kind, relation) in &relations_to_drop {
            if self.verbose {
                eprintln!("    > Dropping {} {}", kind, relation);
            }
            conn.query_drop(format!("DROP {} IF EXISTS {}", kind, relation))
                .with_context(|| format!("Dropping {} {}", kind, relation))?;
        }

        let tables_in_order = tables
            .iter()
            .map(|t| t.table.name.clone())
            .collect::<Vec<_>>();
        let generator = GeneratorState::from(tables);
        let insert_statements = tables_in_order
            .into_iter()
            .map(|table_name| {
                let spec = generator.table(&table_name.clone().into()).unwrap();
                let columns = spec.columns.keys().collect::<Vec<_>>();
                let data = spec.generate_data(self.rows_per_table, self.random);
                nom_sql::InsertStatement {
                    table: table_name.as_str().into(),
                    fields: Some(columns.iter().map(|cn| (*cn).clone().into()).collect()),
                    data: data
                        .into_iter()
                        .map(|mut row| {
                            columns
                                .iter()
                                .map(|col| row.remove(col).unwrap().try_into().unwrap())
                                .collect()
                        })
                        .collect(),
                    ignore: false,
                    on_duplicate: None,
                }
            })
            .collect::<Vec<_>>();

        eprintln!("{}", "==> Running original test script".bold());
        conn.run_script(&script)?;

        eprintln!(
            "{}",
            format!("==> Running {} insert statements", insert_statements.len()).bold()
        );
        for insert_statement in &insert_statements {
            if self.verbose {
                eprintln!(
                    "     > Inserting {} rows of seed data into {}",
                    self.rows_per_table, insert_statement.table
                );
            }
            conn.query_drop(insert_statement.to_string())
                .with_context(|| format!("Inserting seed data for {}", insert_statement.table))?;
        }

        new_entries.extend(
            insert_statements
                .iter()
                .map(|stmt| Record::Statement(Statement::ok(stmt.to_string()))),
        );

        let queries_with_results = queries
            .into_iter()
            .map(|q| -> anyhow::Result<Record> {
                let results = conn.query(&q.query)?;
                let values = results.into_iter().flatten().collect::<Vec<_>>();
                let query_results = if values.len() > hash_threshold {
                    QueryResults::hash(&values)
                } else {
                    QueryResults::Results(values)
                };

                Ok(Record::Query(Query {
                    results: query_results,
                    sort_mode: Some(SortMode::NoSort),
                    ..q.clone()
                }))
            })
            .collect::<Result<Vec<_>, _>>()?;

        new_entries.extend(queries_with_results);

        if let Some(out_path) = self.output {
            write_output(
                &mut file,
                new_entries,
                &mut File::create(out_path).context("Opening output file")?,
            )?;
        } else {
            write_output(&mut file, new_entries, &mut io::stdout())?;
        }

        Ok(())
    }
}
