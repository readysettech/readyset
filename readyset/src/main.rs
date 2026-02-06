use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::process::exit;

use clap::Parser;
use database_utils::DatabaseType;
#[cfg(feature = "failure_injection")]
use fail::FailScenario;
use readyset::mysql::MySqlHandler;
use readyset::psql::PsqlHandler;
use readyset::verify::verify;
use readyset::{init_adapter_runtime, init_adapter_tracing, NoriaAdapter, Options};
use tracing::{error, info};

fn main() -> anyhow::Result<()> {
    #[cfg(feature = "failure_injection")]
    let _fail_scenario = FailScenario::setup();

    let mut options = Options::parse();
    let rt = init_adapter_runtime()?;

    // When cache_mode is shallow, replication and the query sampler are not needed
    // since shallow caches don't use dataflow.
    if options.cache_mode == readyset_client::CacheMode::Shallow {
        options
            .server_worker_options
            .replicator_config
            .replication_enabled = false;
        options.sampler_sample_rate = 0.0;
    }

    let maybe_tracing_guard = match options.verify {
        true => None,
        false => Some(init_adapter_tracing(&rt, &options)?),
    };

    if options.verify_skip {
        info!("Config verification skipped due to --verify-skip");
    } else if let Err(e) = rt.block_on(verify(&options)) {
        error!("{e}");
        if options.verify {
            eprintln!("{e}");
        }
        exit(1);
    } else {
        let msg = "Config verification successful!";
        info!("{msg}");
        if options.verify {
            println!("{msg}");
            exit(0);
        }
    };

    let _tracing_guard = match maybe_tracing_guard {
        Some(guard) => guard,
        None => init_adapter_tracing(&rt, &options)?,
    };

    match options.database_type()? {
        DatabaseType::MySQL => NoriaAdapter {
            description: "MySQL adapter for Readyset.",
            default_address: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 3307),
            connection_handler: MySqlHandler {
                enable_statement_logging: options.tracing.statement_logging,
                tls_acceptor: options.tls_acceptor()?,
                tls_mode: options.tls_mode,
            },
            database_type: DatabaseType::MySQL,
            parse_dialect: readyset_sql::Dialect::MySQL,
            expr_dialect: readyset_data::Dialect::DEFAULT_MYSQL,
        }
        .run(rt, options),
        DatabaseType::PostgreSQL => NoriaAdapter {
            description: "PostgreSQL adapter for Readyset.",
            default_address: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 5433),
            connection_handler: PsqlHandler::new(readyset::psql::Config {
                options: options.psql_options.clone(),
                enable_statement_logging: options.tracing.statement_logging,
                tls_acceptor: options.tls_acceptor()?,
                tls_mode: options.tls_mode,
            })?,
            database_type: DatabaseType::PostgreSQL,
            parse_dialect: readyset_sql::Dialect::PostgreSQL,
            expr_dialect: readyset_data::Dialect::DEFAULT_POSTGRESQL,
        }
        .run(rt, options),
    }
}
