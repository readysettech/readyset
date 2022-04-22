use anyhow::{anyhow, Context, Result};
use clap::Parser;
use console::style;
use database_utils::{DatabaseConnection, DatabaseStatement, DatabaseType, DatabaseURL};
use noria_data::DataType;
use prettytable::Table;
use rustyline::error::ReadlineError;
use rustyline::validate::{ValidationContext, ValidationResult, Validator};
use rustyline::Editor;
use rustyline_derive::{Completer, Helper, Highlighter, Hinter};

#[derive(Parser)]
struct Options {
    /// URL of the database to connect to.
    ///
    /// Should be a URL starting with either `mysql://` or `postgresql://`
    database_url: DatabaseURL,
}

enum Command<'a> {
    Help,
    Normal(&'a str),
    Prepare(&'a str),
    ExecutePrepared {
        statement_id: usize,
        params: Vec<&'a str>,
    },
}

impl<'a> Command<'a> {
    fn parse(s: &'a str) -> Result<Self> {
        if s.trim_end_matches(';').trim().to_lowercase() == "help" {
            Ok(Self::Help)
        } else if let Some(query) = s.strip_prefix("prepare ") {
            Ok(Self::Prepare(query))
        } else if let Some(exec) = s.strip_prefix("execute ") {
            let (statement_id, params) = exec.split_once(' ').unwrap_or((exec, ""));
            let statement_id = statement_id.parse().context("parsing statement ID")?;
            let params = params.split(", ").collect();

            Ok(Self::ExecutePrepared {
                statement_id,
                params,
            })
        } else {
            Ok(Self::Normal(s))
        }
    }
}

struct ReplContext {
    options: Options,
    connection: DatabaseConnection,
    prepared_statements: Vec<DatabaseStatement>,
}

impl ReplContext {
    async fn new(options: Options) -> Result<Self> {
        let connection = options
            .database_url
            .connect()
            .await
            .context("Connecting to database")?;

        Ok(Self {
            options,
            connection,
            prepared_statements: Default::default(),
        })
    }

    fn make_prompt(&self) -> String {
        let mut url = format!(
            "{}://",
            match self.options.database_url.database_type() {
                DatabaseType::MySQL => "mysql",
                DatabaseType::PostgreSQL => "postgresql",
            }
        );
        if let Some(user) = self.options.database_url.user() {
            url.push_str(&format!("{}@", user))
        }
        url.push_str(self.options.database_url.host());
        if let Some(db_name) = self.options.database_url.db_name() {
            url.push_str(&format!("/{}", db_name));
        }

        format!("[{}] ❯ ", style(url).bold())
    }

    async fn handle_command(&mut self, cmd: &str) -> Result<()> {
        match Command::parse(cmd)? {
            Command::Help => {
                println!(
                    "{}",
                    textwrap::fill(
                        "\nWelcome to the ReadySet REPL!\n\n\
                    By default, all commands will be sent unchanged to the database (remember to end \
                    each command with a semicolon)\n\n\
                    Queries can be prefixed with `prepare` to create a new prepared statement. After \
                    preparing the statement, the ID of the statement will be written to standard \
                    output. \n\n\
                    To execute that prepared statement, run:\n\n    \
                    ❯ execute <statement_id> <param1>, <param2>, <param3>...;\n",
                        80
                    )
                )
            }
            Command::Normal(query) => {
                let res = self.connection.query(query).await?;
                print_result(res);
            }
            Command::Prepare(query) => {
                let statement_id = self.prepared_statements.len();
                let stmt = self.connection.prepare(query).await?;
                self.prepared_statements.push(stmt);

                println!("Prepared statement id: {}", statement_id);
            }
            Command::ExecutePrepared {
                statement_id,
                params,
            } => {
                let statement = self
                    .prepared_statements
                    .get(statement_id)
                    .ok_or_else(|| anyhow!("Prepared statement {} not found", statement_id))?;
                let res = self.connection.execute(statement.clone(), params).await?;
                print_result(res);
            }
        }

        Ok(())
    }
}

fn print_result(rows: Vec<Vec<DataType>>) {
    let mut table = Table::new();
    table.set_format(*prettytable::format::consts::FORMAT_NO_BORDER_LINE_SEPARATOR);
    for row in rows {
        table.add_row(row.into());
    }
    print!("\n{}", table);
}

#[derive(Completer, Hinter, Helper, Highlighter)]
struct WaitForSemicolon;

impl Validator for WaitForSemicolon {
    fn validate(&self, ctx: &mut ValidationContext) -> rustyline::Result<ValidationResult> {
        if ctx.input().ends_with(';') || ctx.input().trim() == "help" {
            Ok(ValidationResult::Valid(None))
        } else {
            Ok(ValidationResult::Incomplete)
        }
    }

    fn validate_while_typing(&self) -> bool {
        false
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let options: Options = Options::parse();
    let mut context = ReplContext::new(options).await?;

    println!("Welcome to the ReadySet REPL!\n\nType `help` for help.\n");

    let mut rl = Editor::new();
    rl.set_helper(Some(WaitForSemicolon));
    let _ = rl.load_history(".readyset-history");
    loop {
        match rl.readline(&context.make_prompt()) {
            Ok(cmd) => {
                match context.handle_command(&cmd).await {
                    Err(err) => {
                        eprintln!("Error: {:#}", err);
                    }
                    Ok(()) => {
                        rl.add_history_entry(&cmd);
                    }
                }
                println!();
            }
            Err(ReadlineError::Interrupted) => {
                eprintln!("Exiting (Ctrl-C)");
                rl.save_history(".readyset-history")
                    .context("saving history")?;
                return Ok(());
            }
            Err(ReadlineError::Eof) => {
                eprintln!("Exiting (EOF)");
                rl.save_history(".readyset-history")
                    .context("saving history")?;
                return Ok(());
            }
            Err(err) => {
                eprintln!("Error: {:#}", err);
            }
        }
    }
}
