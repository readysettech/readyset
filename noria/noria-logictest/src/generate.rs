use std::convert::TryFrom;
use std::convert::TryInto;
use std::fs::File;
use std::io::{self, Seek, SeekFrom};
use std::path::PathBuf;
use std::str::FromStr;

use anyhow::{anyhow, bail, Context};
use clap::Clap;
use colored::Colorize;
use itertools::Itertools;
use mysql::prelude::Queryable;
use mysql::Params;
use nom_sql::{parse_query, CreateTableStatement, SqlQuery};
use query_generator::{GeneratorState, Operations};

use crate::ast::{
    Query, QueryParams, QueryResults, Record, SortMode, Statement, StatementResult, Value,
};
use crate::runner::TestScript;

/// Default value for [`Seed::hash_threshold`]
const DEFAULT_HASH_THRESHOLD: usize = 20;

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

    fn execute<Q, P>(&mut self, stmt: Q, params: P) -> anyhow::Result<Vec<Vec<Value>>>
    where
        Q: AsRef<str>,
        P: Into<Params>,
    {
        match self {
            DatabaseConnection::MySQL(conn) => conn
                .exec_iter(stmt, params)?
                .map(|r| {
                    let mut r = r?;
                    Ok((0..r.columns().len())
                        .map(|c| Value::try_from(r.take::<mysql::Value, _>(c).unwrap()))
                        .collect::<Result<Vec<_>, _>>()?)
                })
                .collect(),
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

#[derive(Debug)]
enum Relation {
    Table(String),
    View(String),
}

impl Relation {
    fn kind(&self) -> &'static str {
        match self {
            Relation::Table(_) => "TABLE",
            Relation::View(_) => "VIEW",
        }
    }

    fn name(&self) -> &str {
        match self {
            Relation::Table(name) => name,
            Relation::View(name) => name,
        }
    }
}

#[derive(Debug)]
struct Seed {
    /// Relations to drop (if they exist) before seeding the reference db, to account for having
    /// previously run the test script
    relations_to_drop: Vec<Relation>,
    tables: Vec<CreateTableStatement>,
    queries: Vec<Query>,
    hash_threshold: usize,
    file: Option<File>,
    script: TestScript,
}

impl TryFrom<PathBuf> for Seed {
    type Error = anyhow::Error;

    fn try_from(path: PathBuf) -> Result<Self, Self::Error> {
        let mut file = File::open(&path)?;
        let script = TestScript::read(path.clone(), &mut file)?;

        let mut relations_to_drop = vec![];
        let mut tables = vec![];
        let mut queries = vec![];
        let mut hash_threshold = DEFAULT_HASH_THRESHOLD;

        for record in script.records() {
            match record {
                Record::Statement(Statement { command, .. }) => {
                    match parse_query(command).map_err(|s| anyhow!("{}", s))? {
                        SqlQuery::CreateTable(tbl) => {
                            relations_to_drop.push(Relation::Table(tbl.table.name.clone()));
                            tables.push(tbl)
                        }
                        SqlQuery::CreateView(view) => {
                            relations_to_drop.push(Relation::View(view.name.clone()));
                        }
                        _ => {}
                    }
                }
                Record::Query(query) => {
                    if !query.params.is_empty() {
                        bail!("Queries with params aren't supported yet");
                    }
                    queries.push(query.clone());
                }
                Record::HashThreshold(ht) => {
                    hash_threshold = *ht;
                }
                Record::Halt => break,
            }
        }

        file.seek(SeekFrom::Start(0))?;
        Ok(Seed {
            relations_to_drop,
            tables,
            queries,
            hash_threshold,
            file: Some(file),
            script,
        })
    }
}

impl TryFrom<Operations> for Seed {
    type Error = anyhow::Error;

    fn try_from(Operations(operations): Operations) -> Result<Self, Self::Error> {
        let mut gen = query_generator::GeneratorState::default();
        let queries = operations
            .into_iter()
            .map(|ops| -> anyhow::Result<Query> {
                let query = gen.generate_query(&ops);

                Ok(Query {
                    label: None,
                    column_types: None,
                    sort_mode: if query.statement.order.is_some() {
                        Some(SortMode::NoSort)
                    } else {
                        Some(SortMode::RowSort)
                    },
                    conditionals: vec![],
                    query: query.statement.to_string(),
                    results: Default::default(),
                    params: QueryParams::PositionalParams(
                        query
                            .state
                            .key()
                            .into_iter()
                            .map(|dt| dt.try_into())
                            .collect::<Result<Vec<_>, _>>()?,
                    ),
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        let mut relations_to_drop = vec![];
        let mut tables = vec![];
        let mut records = vec![];

        for (name, table) in gen.tables() {
            let create_stmt = CreateTableStatement::from(table.clone());
            records.push(Record::Statement(Statement {
                result: StatementResult::Ok,
                command: create_stmt.to_string(),
                conditionals: vec![],
            }));
            tables.push(create_stmt);
            relations_to_drop.push(Relation::Table(name.clone().into()));
        }

        Ok(Seed {
            relations_to_drop,
            tables,
            queries,
            hash_threshold: DEFAULT_HASH_THRESHOLD,
            file: None,
            script: records.into(),
        })
    }
}

/// Generate test scripts by comparing results against a reference database
///
/// The `generate` command takes either a seed script to generate from or a series of
/// [operations][0], and generates a logictest test script by running queries against a reference
/// database and saving the results
///
/// [0]: QueryGenerator::Operations
#[derive(Clap)]
pub struct Generate {
    /// Test script to use as a seed. Seed scripts should contain DDL and queries, but no data.
    #[clap(parse(from_str))]
    from: Option<PathBuf>,

    /// Comma-separated list of query operations to use to generate data
    #[clap(long)]
    operations: Option<Operations>,

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

fn write_output<W, I>(seed: Seed, new_entries: I, output: &mut W) -> io::Result<()>
where
    W: io::Write,
    I: IntoIterator<Item = Record>,
{
    writeln!(output, "# Generated by:")?;
    writeln!(output, "#     {}", std::env::args().join(" "))?;

    if let Some(mut original_file) = seed.file {
        io::copy(&mut original_file, output)?;
        writeln!(output)?;
    } else {
        for rec in seed.script.records() {
            writeln!(output, "{}", rec)?;
        }
    }

    for rec in new_entries {
        writeln!(output, "{}", rec)?;
    }
    Ok(())
}

impl Generate {
    pub fn run(mut self) -> anyhow::Result<()> {
        let mut seed = match (self.from.take(), self.operations.take()) {
            (Some(path), None) => Seed::try_from(path)?,
            (None, Some(operations)) => Seed::try_from(operations)?,
            _ => bail!("Must specify one of <from> or --operations"),
        };

        let mut conn = self.compare_to.connect()?;

        eprintln!(
            "{}",
            format!("==> Dropping {} relations", seed.relations_to_drop.len()).bold()
        );
        seed.relations_to_drop.reverse();
        for relation in &seed.relations_to_drop {
            if self.verbose {
                eprintln!("    > Dropping {} {}", relation.kind(), relation.name());
            }
            conn.query_drop(format!(
                "DROP {} IF EXISTS {}",
                relation.kind(),
                relation.name()
            ))
            .with_context(|| format!("Dropping {} {}", relation.kind(), relation.name()))?;
        }

        let tables_in_order = seed
            .tables
            .iter()
            .map(|t| t.table.name.clone())
            .collect::<Vec<_>>();
        let generator = GeneratorState::from(seed.tables.clone());
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
        conn.run_script(&seed.script)?;

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

        let new_entries = insert_statements
            .iter()
            .map(|stmt| Record::Statement(Statement::ok(stmt.to_string())));

        eprintln!("{}", format!("==> Running {} queries", seed.queries.len()));
        let hash_threshold = seed.hash_threshold;
        let queries_with_results = seed
            .queries
            .iter()
            .map(|q| -> anyhow::Result<Record> {
                let results = conn
                    .execute(&q.query, q.params.clone())
                    .with_context(|| format!("Running query {}", q.query))?;
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

        let new_entries = new_entries.chain(queries_with_results);

        if let Some(out_path) = self.output {
            write_output(
                seed,
                new_entries,
                &mut File::create(out_path).context("Opening output file")?,
            )?;
        } else {
            write_output(seed, new_entries, &mut io::stdout())?;
        }

        Ok(())
    }
}
