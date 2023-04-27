use anyhow::{anyhow, Context, Result};
use clap::Parser;
use console::style;
use database_utils::{DatabaseConnection, DatabaseStatement, DatabaseType, DatabaseURL};
use prettytable::Table;
use readyset_data::DfValue;
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

#[derive(Debug, Clone)]
enum Command<'a> {
    Help,
    Normal(&'a str),
    Prepare(&'a str),
    ExecutePrepared {
        statement_id: usize,
        params: Vec<&'a str>,
    },
}

mod parse {
    use nom::branch::alt;
    use nom::bytes::complete::{tag, take_while1};
    use nom::character::complete::multispace1;
    use nom::character::is_space;
    use nom::combinator::{all_consuming, map_res, value};
    use nom::sequence::terminated;
    use nom::IResult;

    use super::Command;

    pub(super) fn command(i: &str) -> IResult<&str, Command> {
        all_consuming(terminated(alt((help, prepare, execute, normal)), tag(";")))(i)
    }

    fn help(i: &str) -> IResult<&str, Command> {
        value(Command::Help, tag("help"))(i)
    }

    fn prepare(i: &str) -> IResult<&str, Command> {
        let (i, _) = tag("prepare")(i)?;
        let (i, _) = multispace1(i)?;
        Ok(("", Command::Prepare(i.trim_end_matches(';'))))
    }

    fn execute(i: &str) -> IResult<&str, Command> {
        let (i, _) = tag("execute")(i)?;
        let (i, _) = multispace1(i)?;
        let (i, statement_id) = map_res(take_while1(|c| !is_space(c as u8)), |s: &str| {
            s.parse::<usize>()
        })(i)?;
        let params = i
            .trim_end_matches(';')
            .split(", ")
            .filter(|p| !p.is_empty())
            .collect();
        Ok((
            "",
            Command::ExecutePrepared {
                statement_id,
                params,
            },
        ))
    }

    fn normal(i: &str) -> IResult<&str, Command> {
        Ok(("", Command::Normal(i.trim_end_matches(';'))))
    }
}

impl<'a> Command<'a> {
    fn parse(s: &'a str) -> Result<Self> {
        match parse::command(s) {
            Ok((_, cmd)) => Ok(cmd),
            Err(e) => Err(anyhow!("Error parsing command: {e}")),
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
            .connect(None)
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

fn print_result(rows: Vec<Vec<DfValue>>) {
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

    let mut rl = Editor::new()?;
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
                        rl.add_history_entry(&cmd)?;
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
