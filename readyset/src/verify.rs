use std::env::current_dir;
use std::fmt::{Display, Formatter};
use std::path::PathBuf;

use anyhow::{anyhow, bail, ensure, Error, Result};
use dir_size::get_size_in_bytes;
use regex::{Captures, Regex};
use sysinfo::Disks;
use tracing::info;

use database_utils::tls::ServerCertVerification;
use database_utils::{
    DatabaseConnection, DatabaseError, DatabaseType, DatabaseURL, QueryableConnection,
};
use readyset_sql::Dialect;
use replicators::MYSQL_INTERNAL_DBS;

use crate::Options;

/// When estimating how much space we need to snapshot, multiply the estimated requirement by this
/// amount to err on the side of being too cautious.
const DISK_SPACE_REQUIREMENT_FACTOR: u64 = 2;

/// A collection of all the verification failures we see.
#[derive(Debug, thiserror::Error)]
struct Errors(Vec<Error>);

impl Display for Errors {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "Config verification failed (override this check with --verify-skip):"
        )?;
        for (i, e) in self.0.iter().enumerate() {
            writeln!(f, "  {}: {e}", i + 1)?;
        }
        Ok(())
    }
}

fn verify_database_type(options: &Options) -> Result<DatabaseType> {
    let database_type = options
        .database_type()
        .map_err(|e| anyhow!("Failed to determine upstream database type: {e}"))?;
    info!("Verified upstream database type");
    Ok(database_type)
}

fn verify_cdc_url(options: &Options) -> Result<DatabaseURL> {
    let url = options
        .server_worker_options
        .replicator_config
        .get_cdc_db_url()
        .map_err(|e| anyhow!("Failed to verify CDC URL: {e}"))?;
    info!("Verified upstream CDC URL");
    Ok(url)
}

fn verify_upstream_url(options: &Options) -> Result<DatabaseURL> {
    let url = options
        .server_worker_options
        .replicator_config
        .upstream_db_url
        .as_ref()
        .ok_or_else(|| anyhow!("Failed to determine upstream URL"))?;
    let url = url
        .parse()
        .map_err(|e| anyhow!("Invalid upstream URL: {e}"))?;
    info!("Verified upstream URL");
    Ok(url)
}

async fn verify_tls(options: &Options) -> Result<ServerCertVerification> {
    let verification =
        ServerCertVerification::from(&options.server_worker_options.replicator_config)
            .await
            .map_err(|e| anyhow!("Failed to verify TLS configuration: {e}"))?;
    info!("Verified TLS configuration");
    Ok(verification)
}

async fn verify_connection(
    url: &DatabaseURL,
    verification: &ServerCertVerification,
    name: &str,
) -> Result<DatabaseConnection> {
    let conn = url
        .connect(verification)
        .await
        .map_err(|e| anyhow!("Failed to establish {name} connection: {e}"))?;
    info!("Verified and established {name} connection");
    Ok(conn)
}

async fn query_one_value<T>(conn: &mut DatabaseConnection, query: &str) -> Result<T>
where
    T: mysql_common::value::convert::FromValue + tokio_postgres::types::FromSqlOwned,
{
    Ok(conn
        .query(query)
        .await?
        .into_iter()
        .get_single_row()?
        .get(0)?)
}

async fn get_version(conn: &mut DatabaseConnection) -> Result<(String, i32, i32)> {
    let version: String = query_one_value(
        conn,
        match conn.dialect() {
            Dialect::MySQL => "SELECT @@version",
            Dialect::PostgreSQL => "SHOW server_version",
        },
    )
    .await
    .map_err(|e| anyhow!("Failed to query upstream database version: {e}"))?;

    let captures = match Regex::new(r"^(?P<major>\d+)\.(?P<minor>\d+).*$")
        .unwrap()
        .captures(&version)
    {
        Some(captures) => captures,
        None => bail!(r#"Failed to parse upstream database version from "{version}""#),
    };

    fn extract(captures: &Captures, name: &str) -> Result<i32> {
        let Some(m) = captures.name(name) else {
            bail!("Couldn't find number");
        };
        Ok(m.as_str().parse()?)
    }

    let major = extract(&captures, "major").map_err(|e| {
        anyhow!(r#"Failed to fetch upstream database major version from "{version}": {e}"#)
    })?;
    let minor = extract(&captures, "minor").map_err(|e| {
        anyhow!(r#"Failed to fetch upstream database minor version from "{version}": {e}"#)
    })?;

    Ok((version, major, minor))
}

async fn verify_version(conn: &mut DatabaseConnection) -> Result<()> {
    let (version, major, minor) = get_version(conn).await?;
    match conn.dialect() {
        Dialect::MySQL => ensure!(
            major >= 8 || (major, minor) == (5, 7),
            "Upstream MySQL version too old: {version}"
        ),
        Dialect::PostgreSQL => ensure!(
            major >= 13,
            "Upstream PostgreSQL version too old: {version}"
        ),
    };
    info!("Verified upstream version: {version}");
    Ok(())
}

async fn verify_replication(conn: &mut DatabaseConnection) -> Result<()> {
    match conn.dialect() {
        Dialect::MySQL => {
            let (enabled, format, image, encryption): (bool, String, String, bool) = async {
                let row = conn
                    .query(
                        "SELECT @@log_bin, @@binlog_format, @@binlog_row_image, \
                                @@binlog_encryption",
                    )
                    .await?
                    .into_iter()
                    .get_single_row()?;
                Ok::<_, DatabaseError>((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
            }
            .await
            .map_err(|e| anyhow!("Failed to retrieve binlog settings: {e}"))?;
            ensure!(enabled, "Upstream does not have binlogs enabled");
            ensure!(
                format == "ROW",
                "Upstream binlog_format needs to be ROW, but was: {format}"
            );
            ensure!(
                image == "FULL" || image == "MINIMAL",
                "Upstream binlog_row_image needs to be FULL or MINIMAL, but was: {image}"
            );
            ensure!(!encryption, "Upstream binlog_encryption is not supported");
        }
        Dialect::PostgreSQL => {
            if let Ok(rds_logical) =
                query_one_value::<String>(conn, "SHOW rds.logical_replication").await
            {
                ensure!(
                    rds_logical == "on",
                    "Upstream rds.logical_replication needs to be on, but was off"
                );
            }
            let wal: String = query_one_value(conn, "SHOW wal_level")
                .await
                .map_err(|e| anyhow!("Failed to query wal_level: {e}"))?;
            ensure!(
                wal == "logical",
                "Upstream wal_level needs to be logical, but was: {wal}"
            );
        }
    }
    info!("Verified replication configuration");
    Ok(())
}

async fn verify_permissions(conn: &mut DatabaseConnection, options: &Options) -> Result<()> {
    match conn.dialect() {
        Dialect::MySQL => {
            let grants: Vec<Vec<String>> = conn.query("SHOW GRANTS").await?.try_into()?;
            let grants: Vec<String> = grants.into_iter().flatten().collect();
            macro_rules! ensure_permission {
                ($permission:literal) => {
                    ensure!(
                        grants.iter().any(|grant| grant.contains($permission)),
                        concat!("Readyset user missing ", $permission, " permissions")
                    );
                };
            }
            ensure_permission!("REPLICATION CLIENT");
            ensure_permission!("REPLICATION SLAVE");
        }
        Dialect::PostgreSQL => {
            let config = &options.server_worker_options.replicator_config;
            if !config.disable_setup_ddl_replication && !config.disable_create_publication {
                let has_super: bool = query_one_value(
                    conn,
                    "SELECT usesuper FROM pg_user where usename = CURRENT_USER",
                )
                .await?;
                let has_super = has_super || {
                    // RDS has its own representation of super.
                    let has_super: Result<String> = query_one_value(
                        conn,
                        "SELECT rolname FROM pg_roles WHERE \
                           pg_has_role(CURRENT_USER, rolname, 'member') AND \
                           rolname = 'rds_superuser'",
                    )
                    .await;
                    has_super.is_ok()
                };
                ensure!(has_super, "Readyset user missing SUPERUSER permissions");
            }
        }
    }
    info!("Verified upstream permissions");
    Ok(())
}

async fn verify_disk_space(conn: &mut DatabaseConnection, options: &Options) -> Result<()> {
    let config = &options.server_worker_options.replicator_config;
    if config.replication_tables.is_some() || config.replication_tables_ignore.is_some() {
        // Don't try to figure out required space if specific tables are asked for.
        return Ok(());
    }

    let deployment_dir = options
        .server_worker_options
        .storage_dir(&options.deployment);
    let display = deployment_dir.display().to_string();
    let mut dir = deployment_dir.clone();
    // Remove deployment directory from path, as it likely doesn't exist for new deployments.
    dir.pop();
    let dir = if dir.is_absolute() {
        dir
    } else if dir == PathBuf::from(".") {
        // Storage directory is default relative path.  Use current, absolute directory.
        current_dir().map_err(|e| anyhow!("Could not determine current directory: {e}"))?
    } else if dir.exists() {
        // Path is some other relative directory that exists.  Try to canonicalize it.
        dir.canonicalize()
            .map_err(|e| anyhow!("Failed to canonicalize storage directory {display}: {e}"))?
    } else {
        // Path is some relative path that does not exist.  Skip verification because canonicalize
        // requires the path to exist, and let's not modify the filesystem during verification.
        return Ok(());
    };

    // Find the most-specific (longest) mountpoint holding us.
    let disks = Disks::new_with_refreshed_list();
    let disk = disks
        .iter()
        .filter(|disk| dir.starts_with(disk.mount_point()))
        .max_by_key(|disk| disk.mount_point().as_os_str().len())
        .ok_or_else(|| {
            anyhow!("Could not find mountpoint containing storage directory {display}")
        })?;

    let free_bytes = disk.available_space();
    let used_bytes = if deployment_dir.exists() {
        // A preexisting deployment exists, get its size to discount it from the estimate.
        get_size_in_bytes(&deployment_dir).map_err(|e| {
            anyhow!("Failed to determine consumed space in directory {display}: {e}")
        })?
    } else {
        0
    };

    let query = match conn.dialect() {
        Dialect::MySQL => &format!(
            "SELECT COALESCE(SUM(data_length), 0) FROM information_schema.tables \
               WHERE table_schema NOT IN ({})",
            MYSQL_INTERNAL_DBS
                .iter()
                .map(|db| format!("'{db}'"))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        Dialect::PostgreSQL => "SELECT pg_database_size(current_database())",
    };
    let estimated_total_bytes: i64 = query_one_value(conn, query)
        .await
        .map_err(|e| anyhow!("Failed to query estimated snapshot size: {e}"))?;
    let estimated_total_bytes = estimated_total_bytes as u64 * DISK_SPACE_REQUIREMENT_FACTOR;
    let estimated_required_bytes = estimated_total_bytes.saturating_sub(used_bytes);

    ensure!(
        free_bytes > estimated_required_bytes,
        "Estimated space required for snapshot is {estimated_required_bytes} bytes, but only have \
         {free_bytes} free bytes in storage directory {display}"
    );

    info!("Verified disk space for snapshot");
    Ok(())
}

/// Run through a list of checks to make sure we're good to go before attempting to snapshot.
pub async fn verify(options: &Options) -> Result<()> {
    let mut errors = Vec::new();
    macro_rules! add_err {
        ($result:expr) => {
            match $result {
                Ok(r) => Some(r),
                Err(e) => {
                    errors.push(e);
                    None
                }
            }
        };
    }

    if options.verify_fail {
        let e: Result<()> = Err(anyhow!("Simulated failure (caused by --verify-fail)"));
        add_err!(e);
    }

    let verification = add_err!(verify_tls(options).await);
    add_err!(verify_database_type(options));
    let cdc_url = add_err!(verify_cdc_url(options));
    let upstream_url = add_err!(verify_upstream_url(options));

    let mut conn = None;
    if let (Some(verification), Some(cdc_url)) = (&verification, &cdc_url) {
        conn = add_err!(verify_connection(cdc_url, verification, "CDC").await);
        if let Some(upstream_url) = &upstream_url {
            if cdc_url != upstream_url {
                conn = add_err!(verify_connection(upstream_url, verification, "upstream").await);
            }
        }
    }

    if let Some(conn) = &mut conn {
        add_err!(verify_version(conn).await);
        add_err!(verify_replication(conn).await);
        add_err!(verify_permissions(conn, options).await);
        add_err!(verify_disk_space(conn, options).await);
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(Errors(errors).into())
    }
}
