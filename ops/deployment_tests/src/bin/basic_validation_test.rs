// This program is a basic validation test that confirms the following:
// - We can install DDL that creates Noria base tables.
// - We can perform writes to a customer database.
// - Writes to a customer database are propagated
// - We can create a Noria view.
//
// Arguments:
// | long                | short | env                | Description                            |
// |---------------------|-------|--------------------|----------------------------------------|
// | -adapter            | N/A   | N/A                | ReadySet Adapter MySQL String          |
// | -rds                | N/A   | N/A                | MySQL DB MySQL String                  |
// | -prometheus_address | -p    | PROMETHEUS_ADDRESS | Host and port for Prometheus API calls |
// | -migration_mode     | N/A   | N/A                | ReadySet Migration Mode                |
//
// Example usage:
// readyset_rds=mysql://$adapter_user:$adapter_pass@$rds_host:$adapter_port/$adapter_name
// readyset_adapter=mysql://$adapter_user:$adapter_pass@$adapter_host:$adapter_port/$adapter_name
// prometheus_addr=$prom_host:9091
//
// basic_validation_test
// --adapter $readyset_adapter
// --rds $readyset_rds
// --prometheus-address $prometheus_addr
// --migration-mode explicit-migration
use std::str::FromStr;

use anyhow::bail;
use clap::{ArgEnum, Parser};
use console::style;
use deployment_tools::prometheus;
use indicatif::{ProgressBar, ProgressStyle};
use mysql_async::prelude::Queryable;
use mysql_async::Error;

#[derive(Parser)]
#[clap(name = "basic_validation_test")]
struct BasicValidationTest {
    #[clap(long)]
    adapter: String,

    #[clap(long)]
    rds: String,

    /// Address of a prometheus server to execute PromQL against.
    #[clap(long, short = 'p', env = "PROMETHEUS_ADDRESS")]
    prometheus_address: String,

    #[clap(long, default_value = "explicit-migration")]
    migration_mode: MigrationMode,
}

#[derive(ArgEnum, Clone)]
enum MigrationMode {
    ExplicitMigration,
    AsyncMigration,
    RequestPath,
}

impl FromStr for MigrationMode {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "explicit-migration" => MigrationMode::ExplicitMigration,
            "async-migration" => MigrationMode::AsyncMigration,
            "request-path" => MigrationMode::RequestPath,
            _ => bail!("Invalid migration mode"),
        })
    }
}

fn step(current: &mut u32, total: u32, message: &str) {
    println!(
        "{} {}",
        style(format!("[{}/{}]", current, total)).bold().dim(),
        message
    );
    *current += 1;
}

impl BasicValidationTest {
    pub async fn run(self) -> anyhow::Result<()> {
        let mut current = 1;
        let total = match self.migration_mode {
            MigrationMode::ExplicitMigration => 9,
            MigrationMode::AsyncMigration => 9,
            MigrationMode::RequestPath => 8,
        };

        let opts = mysql_async::Opts::from_url(&self.rds)?;
        let mut rds = mysql_async::Conn::new(opts.clone()).await?;

        // Step 1: Initialize the RDS.
        step(&mut current, total, "Creating table t1 through the RDS");
        let res = rds
            .query_drop(
                r"CREATE TABLE t1 (
                  uid INT,
                  value INT
                );",
            )
            .await;

        let mut should_skip_insert = false;
        match res {
            Err(Error::Server(e)) => {
                should_skip_insert = true;
                println!("CREATE TABLE failed with {}", e);
                println!(
                    "Table may already exist, skipping data insertion and attempting to continue."
                )
            }
            Err(e) => return Err(e.into()),
            _ => {}
        }

        // Step 2: Insert data into the table to query.
        if should_skip_insert {
            step(
                &mut current,
                total,
                "Skipping... Inserting sample data into t1 through the RDS",
            );
        } else {
            step(
                &mut current,
                total,
                "Inserting sample data into t1 through the RDS",
            );

            rds.query_drop(r"INSERT INTO t1 VALUES (1,4),(2,4),(3,4),(4,4),(5,4),(6,7);")
                .await?;
        }

        step(&mut current, total, "Connecting to the adapter!");
        let opts = mysql_async::Opts::from_url(&self.adapter)?;
        let mut adapter = mysql_async::Conn::new(opts.clone()).await?;

        // Step 3: Execute a query!
        step(
            &mut current,
            total,
            "Querying the adapter through prepare and execute",
        );
        match self.migration_mode {
            MigrationMode::ExplicitMigration => println!("This will be executed against the RDS (explicit-migrations enabled)"),
            MigrationMode::AsyncMigration => println!("This will be executed against the RDS but trigger a migration in Noria (async-migrations enabled)"),
            MigrationMode::RequestPath => println!("This will be executed against Noria and trigger a migration"),
        }

        let result = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            adapter.exec("SELECT * FROM t1 where uid = ?", (3,)),
        )
        .await;

        if let Ok(result) = result {
            let result: Vec<(i32, i32)> = result?;
            if result.len() != 1 {
                println!("Incorrect number of rows returned");
            } else if (result[0].0, result[0].1) != (3i32, 4i32) {
                println!("Incorrect result returned from query");
            }
        } else {
            println!("Query did not complete in 1 second");
        }

        // Step 4: Create a connection to prometheus.

        step(
            &mut current,
            total,
            "Creating prometheus client to check metrics",
        );
        let client = prometheus::client(&self.prometheus_address).await?;

        // Waiting for metrics before the next step.
        let pb = ProgressBar::new_spinner();
        pb.enable_steady_tick(120);
        pb.set_style(ProgressStyle::default_spinner().template("{spinner:.blue} {msg}"));
        pb.set_message("Waiting 40 seconds for metrics to be propagated...");
        std::thread::sleep(std::time::Duration::from_secs(40));
        println!();

        // Step 5: Check that these queries executed against the correct database.
        match self.migration_mode {
            MigrationMode::ExplicitMigration | MigrationMode::AsyncMigration => {
                step(
                    &mut current,
                    total,
                    "Checking that the query executed against the RDS",
                );
                prometheus::verify_db_queried(&client, "mysql").await?;
            }
            MigrationMode::RequestPath => {
                step(
                    &mut current,
                    total,
                    "Checking that the query executed against Noria",
                );
                prometheus::verify_db_queried(&client, "noria").await?;
            }
        }

        // Step 6: Extra checks for the explicit and async migration case.
        match self.migration_mode {
            MigrationMode::ExplicitMigration => {
                step(
                    &mut current,
                    total,
                    "Allowing the query in the customer database",
                );
                adapter
                    .query_drop(r"CREATE CACHE test FROM SELECT * FROM t1 where uid = ?")
                    .await?;
            }
            MigrationMode::AsyncMigration => {
                let pb = ProgressBar::new_spinner();
                pb.enable_steady_tick(120);
                pb.set_style(ProgressStyle::default_spinner().template("{spinner:.blue} {msg}"));
                pb.set_message("Waiting just a little bit longer to make sure we have performed the migration async...");
                std::thread::sleep(std::time::Duration::from_secs(20));
                println!();
            }
            MigrationMode::RequestPath => {
                return Ok(());
            }
        }

        step(
            &mut current,
            total,
            "Querying the adapter through prepare and execute again!",
        );

        let result = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            adapter.exec("SELECT * FROM t1 where uid = ?", (3,)),
        )
        .await;

        if let Ok(result) = result {
            let result: Vec<(i32, i32)> = result?;
            if result.len() != 1 {
                println!("Incorrect number of rows returned");
            } else if (result[0].0, result[0].1) != (3i32, 4i32) {
                println!("Incorrect result returned from query");
            }
        } else {
            println!("Query did not complete in 1 second");
        }

        // Waiting for metrics before the next step.
        let pb = ProgressBar::new_spinner();
        pb.enable_steady_tick(120);
        pb.set_style(ProgressStyle::default_spinner().template("{spinner:.blue} {msg}"));
        pb.set_message("Waiting 40 seconds for metrics to be propagated...");
        std::thread::sleep(std::time::Duration::from_secs(40));
        println!();

        step(
            &mut current,
            total,
            "Checking that the query executed against Noria",
        );
        prometheus::verify_db_queried(&client, "noria").await?;

        println!();

        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let query = BasicValidationTest::parse();
    let r = query.run().await;

    if r.is_err() {
        println!("{}", style("Failed :(").bold().green(),);
    } else {
        println!("{}", style("Success!").bold().green(),);
    }

    r
}
