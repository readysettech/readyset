use std::convert::{TryFrom, TryInto};
use std::fs::File;
use std::io::{self, Seek, SeekFrom};
use std::mem;
use std::path::PathBuf;

use anyhow::{anyhow, bail, Context};
use clap::Parser;
use console::style;
use database_utils::{DatabaseConnection, DatabaseURL, QueryableConnection};
use itertools::Itertools;
use nom_sql::{
    parse_query, BinaryOperator, CreateTableStatement, DeleteStatement, Dialect, Expr, SqlQuery,
    SqlType,
};
use query_generator::{GeneratorState, ParameterMode, QuerySeed};

use crate::ast::{
    Conditional, Query, QueryParams, QueryResults, Record, SortMode, Statement, StatementResult,
    Value,
};
use crate::runner::{recreate_test_database, RunOptions, TestScript};

/// Default value for [`Seed::hash_threshold`]
const DEFAULT_HASH_THRESHOLD: usize = 20;

#[derive(Debug)]
pub(crate) struct Seed {
    tables: Vec<CreateTableStatement>,
    queries: Vec<Query>,
    generator: GeneratorState,
    hash_threshold: usize,
    script: TestScript,
}

impl TryFrom<PathBuf> for Seed {
    type Error = anyhow::Error;

    fn try_from(path: PathBuf) -> Result<Self, Self::Error> {
        let mut file = File::open(&path)?;
        let script = TestScript::read(path, &mut file)?;

        let mut tables = vec![];
        let mut queries = vec![];
        let mut hash_threshold = DEFAULT_HASH_THRESHOLD;

        for record in script.records() {
            match record {
                Record::Statement(Statement { command, .. }) => {
                    // TODO(aspen): Make dialect configurable
                    if let SqlQuery::CreateTable(tbl) =
                        parse_query(Dialect::MySQL, command).map_err(|s| anyhow!("{}", s))?
                    {
                        tables.push(tbl)
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
                Record::Halt { .. } => break,
                Record::Graphviz | Record::Sleep(_) => {}
            }
        }

        let generator = GeneratorState::from(tables.clone());

        file.seek(SeekFrom::Start(0))?;
        Ok(Seed {
            tables,
            queries,
            generator,
            hash_threshold,
            script,
        })
    }
}

async fn run_queries(
    queries: &[Query],
    conn: &mut DatabaseConnection,
    hash_threshold: usize,
) -> anyhow::Result<Vec<Record>> {
    eprintln!(
        "{}",
        style(format!("==> Running {} queries", queries.len())).bold()
    );

    let mut ret = Vec::new();
    for q in queries {
        let mut results: Vec<Vec<Value>> = conn
            .execute(&q.query, q.params.clone())
            .await
            .with_context(|| format!("Running query {}", q.query))?
            .try_into()?;

        let values: Vec<_> = match q.sort_mode.unwrap_or_default() {
            SortMode::NoSort => results.into_iter().flatten().collect(),
            SortMode::RowSort => {
                results.sort();
                results.into_iter().flatten().collect()
            }
            SortMode::ValueSort => {
                let mut vals: Vec<_> = results.into_iter().flatten().collect();
                vals.sort();
                vals
            }
        };

        let query_results = if values.len() > hash_threshold {
            QueryResults::hash(&values)
        } else {
            QueryResults::Results(values)
        };

        ret.push(Record::Query(Query {
            results: query_results,
            ..q.clone()
        }))
    }

    Ok(ret)
}

impl Seed {
    pub fn from_seeds<I>(seeds: I, dialect: nom_sql::Dialect) -> anyhow::Result<Self>
    where
        I: IntoIterator<Item = QuerySeed>,
    {
        let mut generator = query_generator::GeneratorState::with_parameter_mode(match dialect {
            Dialect::MySQL => ParameterMode::Positional,
            Dialect::PostgreSQL => ParameterMode::Numbered,
        });
        let queries = seeds
            .into_iter()
            .map(|seed| -> anyhow::Result<Query> {
                let query = generator.generate_query(seed);
                let query_string = query.statement.display(dialect).to_string();

                Ok(Query {
                    label: None,
                    column_types: None,
                    sort_mode: if query.statement.order.is_some() {
                        Some(SortMode::NoSort)
                    } else {
                        Some(SortMode::RowSort)
                    },
                    conditionals: vec![],
                    query: query_string,
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

        let mut tables = vec![];
        let mut records = vec![];

        // If we're running against postgresql and any of our tables' columns have `citext` (or
        // `citext[]`) as their type, we need to make sure that the `citext` extension exists in the
        // database. Let's do that as part of the script itself so that generated test scripts can
        // always be run against fresh pg databases
        //
        // TODO: we might want to generalize this for other types which depend on extensions, or
        // really any other DDL that's necessary for types (enums, maybe?) in the future, but for
        // now I'm doing it directly because this is the only thing that needs it
        if generator
            .tables()
            .values()
            .flat_map(|t| t.columns.values())
            .any(|col| *col.sql_type.innermost_array_type() == SqlType::Citext)
        {
            records.push(Record::Statement(Statement {
                result: StatementResult::Ok,
                command: "CREATE EXTENSION IF NOT EXISTS citext;".into(),
                // Readyset doesn't know about `CREATE EXTENSION`
                conditionals: vec![Conditional::SkipIf("readyset".into())],
            }))
        }

        for table in generator.tables_mut().values_mut() {
            table.primary_key(); // ensure the table has a primary key
            let create_stmt = CreateTableStatement::from(table.clone());

            records.push(Record::Statement(Statement {
                result: StatementResult::Ok,
                command: create_stmt.display(dialect).to_string(),
                conditionals: vec![],
            }));
            tables.push(create_stmt);
        }

        Ok(Seed {
            tables,
            queries,
            generator,
            hash_threshold: DEFAULT_HASH_THRESHOLD,
            script: records.into(),
        })
    }

    pub fn from_generate_opts(
        opts: query_generator::GenerateOpts,
        dialect: nom_sql::Dialect,
    ) -> anyhow::Result<Self> {
        Self::from_seeds(opts.into_query_seeds(), dialect)
    }

    pub async fn run(
        &mut self,
        opts: GenerateOpts,
        dialect: nom_sql::Dialect,
    ) -> anyhow::Result<&TestScript> {
        recreate_test_database(&opts.compare_to).await?;
        let mut conn = opts
            .compare_to
            .connect(None)
            .await
            .context("Connecting to comparison database")?;

        let tables_in_order = self
            .tables
            .iter()
            .map(|t| t.table.name.clone())
            .collect::<Vec<_>>();

        let data = tables_in_order
            .clone()
            .into_iter()
            .map(|table_name| {
                let spec = self.generator.table_mut(table_name.as_str()).unwrap();
                (
                    table_name,
                    spec.generate_data(opts.rows_per_table, opts.random),
                )
            })
            .collect::<Vec<_>>();

        let insert_statements = data
            .iter()
            .map(|(table_name, data)| {
                let spec = self.generator.table(table_name.as_str()).unwrap();
                let columns = spec.columns.keys().collect::<Vec<_>>();
                nom_sql::InsertStatement {
                    table: spec.name.clone().into(),
                    fields: Some(columns.iter().map(|cn| (*cn).clone().into()).collect()),
                    data: data
                        .clone()
                        .into_iter()
                        .map(|mut row| {
                            columns
                                .iter()
                                .map(|col| {
                                    Expr::Literal(row.remove(col).unwrap().try_into().unwrap())
                                })
                                .collect()
                        })
                        .collect(),
                    ignore: false,
                    on_duplicate: None,
                }
            })
            .collect::<Vec<_>>();

        eprintln!("{}", style("==> Running original test script").bold());
        self.script
            .run_on_database(
                &RunOptions {
                    verbose: opts.verbose,
                    ..Default::default()
                },
                &mut conn,
                None,
            )
            .await?;

        eprintln!(
            "{}",
            style(format!(
                "==> Running {} insert statements",
                insert_statements.len()
            ))
            .bold()
        );
        for insert_statement in &insert_statements {
            if opts.verbose {
                eprintln!(
                    "     > Inserting {} rows of seed data into {}",
                    opts.rows_per_table,
                    insert_statement.table.display_unquoted()
                );
            }
            conn.query_drop(insert_statement.display(dialect).to_string())
                .await
                .with_context(|| {
                    format!(
                        "Inserting seed data for {}",
                        insert_statement.table.display_unquoted()
                    )
                })?;
        }

        let new_entries = insert_statements
            .iter()
            .map(|stmt| Record::Statement(Statement::ok(stmt.display(dialect).to_string())));

        let hash_threshold = self.hash_threshold;
        let queries = mem::take(&mut self.queries);

        let new_entries =
            new_entries.chain(run_queries(&queries, &mut conn, hash_threshold).await?);

        if opts.include_deletes {
            let rows_to_delete = opts.rows_to_delete.unwrap_or(opts.rows_per_table / 2);

            let delete_statements: Vec<DeleteStatement> = data
                .iter()
                .map(|(table_name, data)| {
                    let spec = self.generator.table(table_name.as_str()).unwrap();
                    let table: nom_sql::Relation = spec.name.clone().into();
                    let pk = spec.primary_key.clone().ok_or_else(|| {
                        anyhow!(
                            "--include-deletes specified, but table {} missing a primary key",
                            table.display_unquoted()
                        )
                    })?;

                    Ok(data
                        .iter()
                        .take(rows_to_delete)
                        .map(|row| DeleteStatement {
                            table: table.clone(),
                            where_clause: Some(Expr::BinaryOp {
                                lhs: Box::new(Expr::Column(pk.clone().into())),
                                op: BinaryOperator::Equal,
                                rhs: Box::new(Expr::Literal(row[&pk].clone().try_into().unwrap())),
                            }),
                        })
                        .collect::<Vec<_>>())
                })
                .collect::<anyhow::Result<Vec<_>>>()?
                .into_iter()
                .flatten()
                .collect();

            let new_entries =
                new_entries.chain(delete_statements.iter().map(|stmt| {
                    Record::Statement(Statement::ok(stmt.display(dialect).to_string()))
                }));

            eprintln!(
                "{}",
                style(format!(
                    "==> Running {} delete statements",
                    delete_statements.len()
                ))
                .bold()
            );

            for delete_statement in &delete_statements {
                if opts.verbose {
                    eprintln!(
                        "     > Deleting {} rows of seed data from {}",
                        rows_to_delete,
                        delete_statement.table.display_unquoted()
                    );
                }

                conn.query_drop(delete_statement.display(dialect).to_string())
                    .await
                    .with_context(|| {
                        format!(
                            "Deleting seed data for {}",
                            delete_statement.table.display_unquoted()
                        )
                    })?;
            }

            self.script
                .extend(new_entries.chain(run_queries(&queries, &mut conn, hash_threshold).await?))
        } else {
            self.script.extend(new_entries)
        }

        Ok(&self.script)
    }
}

// shared options for generating tests
// (not a doc-comment due to https://github.com/clap-rs/clap/issues/2527)
#[derive(Parser, Debug, Clone)]
#[group(id = "ScriptOpts")]
pub struct GenerateOpts {
    /// URL of a reference database to compare to. Currently supports `mysql://` URLs, but may be
    /// expanded in the future
    #[clap(long)]
    pub compare_to: DatabaseURL,

    /// Rows of data to generate per table
    #[clap(long, default_value = "100")]
    pub rows_per_table: usize,

    /// Enable verbose output
    #[clap(long, short = 'v')]
    pub verbose: bool,

    /// Enable randomly generating column data.
    #[clap(long)]
    pub random: bool,

    /// Whether to include row deletes followed by additional queries in the generated test script.
    ///
    /// If used with a seed script, all tables must have a primary key (due to current limitations
    /// in ReadySet).
    #[clap(long)]
    pub include_deletes: bool,

    /// How many rows to delete in between queries. Ignored if `--include-deletes` is not
    /// specified. Defaults to half of --rows-per-table, rounded down
    #[clap(long)]
    pub rows_to_delete: Option<usize>,
}

impl GenerateOpts {
    pub fn dialect(&self) -> nom_sql::Dialect {
        match self.compare_to {
            DatabaseURL::MySQL(_) => nom_sql::Dialect::MySQL,
            DatabaseURL::PostgreSQL(_) => nom_sql::Dialect::PostgreSQL,
            DatabaseURL::Vitess(_) => nom_sql::Dialect::MySQL,
        }
    }
}

/// Generate test scripts by comparing results against a reference database
///
/// The `generate` command takes either a seed script to generate from or a set of [generate
/// options][0], and generates a logictest test script by running queries against a reference
/// database and saving the results
///
/// [0]: GenerateOpts
#[derive(Parser)]
pub struct Generate {
    /// Test script to use as a seed. Seed scripts should contain DDL and queries, but no data.
    pub from: Option<PathBuf>,

    #[clap(flatten)]
    pub query_options: query_generator::GenerateOpts,

    #[clap(flatten)]
    pub script_options: GenerateOpts,

    /// File to write results to (defaults to stdout)
    #[clap(short = 'o')]
    pub output: Option<PathBuf>,
}

fn write_output<W>(script: &TestScript, output: &mut W) -> io::Result<()>
where
    W: io::Write,
{
    writeln!(output, "# Generated by:")?;
    writeln!(output, "#     {}", std::env::args().join(" "))?;

    for rec in script.records() {
        writeln!(output, "{}", rec)?;
    }

    Ok(())
}

impl Generate {
    #[tokio::main]
    pub async fn run(mut self) -> anyhow::Result<()> {
        let dialect = self.script_options.dialect();
        let mut seed = match self.from.take() {
            Some(path) => Seed::try_from(path)?,
            None => Seed::from_generate_opts(self.query_options.clone(), dialect)?,
        };

        let script = seed.run(self.script_options, dialect).await?;

        if let Some(out_path) = self.output {
            write_output(
                script,
                &mut File::create(out_path).context("Opening output file")?,
            )?;
        } else {
            write_output(script, &mut io::stdout())?;
        }

        Ok(())
    }
}
