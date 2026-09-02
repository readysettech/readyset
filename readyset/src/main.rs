use std::fs;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::path::Path;
use std::process::exit;

use clap::Parser;
#[cfg(feature = "failure_injection")]
use fail::FailScenario;
use mysql_srv::{AuthCache, AuthKeys};
use tracing::{error, info};

use database_utils::DatabaseType;
use readyset::mysql::MySqlHandler;
use readyset::psql::PsqlHandler;
use readyset::verify::verify;
use readyset::{init_adapter_runtime, init_adapter_tracing, NoriaAdapter, Options};
use readyset_client::CacheMode;
use readyset_server::STORAGE_RESET_MARKER;

fn main() -> anyhow::Result<()> {
    antithesis_sdk::antithesis_init();

    #[cfg(feature = "failure_injection")]
    let _fail_scenario = FailScenario::setup();

    let mut options = Options::parse();
    options.resolve_auto_cache();
    options.resolve_parsing_preset();
    options.resolve_shallow_cache_allow_all();
    let rt = init_adapter_runtime()?;

    // When cache_mode is shallow, replication and the query sampler are not needed
    // since shallow caches don't use dataflow.
    if options.cache_mode == CacheMode::Shallow {
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

    let deployment_dir = options
        .server_worker_options
        .storage_dir(&options.deployment);
    let reset_marker = deployment_dir.join(STORAGE_RESET_MARKER);
    if reset_marker.exists() {
        reset_storage_dir(&deployment_dir, &reset_marker)?;
    }

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
        DatabaseType::MySQL => {
            AuthKeys::initialize(Some(deployment_dir)).expect("failed to initialize auth RSA keys");

            NoriaAdapter {
                description: "MySQL adapter for Readyset.",
                default_addresses: vec![SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 3307)],
                connection_handler: MySqlHandler {
                    enable_statement_logging: options.tracing.statement_logging,
                    tls_acceptor: options.tls_acceptor()?,
                    tls_mode: options.tls_mode,
                    auth_cache: AuthCache::new(),
                    mysql_authentication_method: options.mysql_options.mysql_authentication_method,
                },
                database_type: DatabaseType::MySQL,
                parse_dialect: readyset_sql::Dialect::MySQL,
                expr_dialect: readyset_data::Dialect::DEFAULT_MYSQL,
            }
            .run(rt, options)
        }
        DatabaseType::PostgreSQL => NoriaAdapter {
            description: "PostgreSQL adapter for Readyset.",
            default_addresses: vec![
                SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 5433),
                SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 5433),
            ],
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

fn reset_storage_dir(storage_dir: &Path, marker: &Path) -> anyhow::Result<()> {
    info!(dir = %storage_dir.display(), "Resetting storage directory");
    for entry in fs::read_dir(storage_dir)? {
        let entry = entry?;
        if entry.path() == marker {
            continue;
        }
        if entry.file_type()?.is_dir() {
            fs::remove_dir_all(entry.path())?;
        } else {
            fs::remove_file(entry.path())?;
        }
    }
    Ok(fs::remove_file(marker)?)
}
