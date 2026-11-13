use std::env::current_dir;
use std::fmt::{Display, Formatter};
use std::fs::read_dir;
use std::iter;
use std::sync::Arc;

use anyhow::{anyhow, bail, ensure, Error, Result};
use futures::future::join_all;
use regex::{Captures, Regex};
use sysinfo::Disks;
use tracing::{info, warn};

use database_utils::tls::ServerCertVerification;
use database_utils::{
    validate_servers, DatabaseConnection, DatabaseError, DatabaseType, DatabaseURL,
    QueryableConnection, UpstreamServers,
};
use readyset_client::consensus::StandaloneAuthority;
use readyset_sql::ast::SqlIdentifier;
use readyset_sql::Dialect;
use replicators::{replication_slot_name, resnapshot_slot_name, MYSQL_INTERNAL_DBS};

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

fn verify_cdc_servers(options: &Options) -> Result<UpstreamServers> {
    let servers = options
        .server_worker_options
        .replicator_config
        .cdc_servers()
        .map_err(|e| anyhow!("Failed to verify CDC URL: {e}"))?;
    info!("Verified upstream CDC URL");
    Ok(servers)
}

fn verify_upstream_servers(options: &Options) -> Result<UpstreamServers> {
    let servers = options
        .server_worker_options
        .replicator_config
        .upstream_servers()
        .map_err(|e| anyhow!("Failed to verify upstream URL: {e}"))?
        .ok_or_else(|| anyhow!("Failed to determine upstream URL"))?;
    info!("Verified upstream URL");
    Ok(servers)
}

/// Connect to every server in the list, so a bad failover target is caught at startup. Returns
/// the first reachable server, or an error if none is reachable.
async fn verify_servers(
    servers: &UpstreamServers,
    verification: &ServerCertVerification,
    options: &Options,
    label: &str,
) -> Result<DatabaseURL> {
    let timeout = options
        .server_worker_options
        .replicator_config
        .upstream_connect_timeout;
    let outcome = validate_servers(None, servers, verification, timeout)
        .await
        .map_err(|e| anyhow!("{label} {e}"))?;
    for (server, error) in &outcome.unreachable {
        warn!(%server, %error, "{label} server is unreachable; continuing without it");
    }
    for (server, warning) in &outcome.warnings {
        warn!(%server, %warning, "{label} server differs from the list");
    }
    let url = outcome
        .verified
        .first()
        .map(|server| server.url().clone())
        .ok_or_else(|| anyhow!("Failed to establish {label} connection to any server"))?;
    info!(
        reachable = outcome.verified.len(),
        unreachable = outcome.unreachable.len(),
        "Verified {label} servers"
    );
    Ok(url)
}

async fn verify_tls(options: &Options) -> Result<Arc<ServerCertVerification>> {
    let verification =
        ServerCertVerification::from(&options.server_worker_options.replicator_config)
            .await
            .map_err(|e| anyhow!("Failed to verify TLS configuration: {e}"))?;
    info!("Verified TLS configuration");
    Ok(Arc::new(verification))
}

async fn verify_connection(
    url: &DatabaseURL,
    verification: &ServerCertVerification,
    report_name: Option<&str>,
) -> Result<DatabaseConnection> {
    let conn = url.connect(verification).await;
    let name = match report_name {
        Some(name) => name,
        None => return Ok(conn?),
    };
    let conn = conn.map_err(|e| anyhow!("Failed to establish {name} connection: {e}"))?;
    info!("Verified and established {name} connection");
    Ok(conn)
}

async fn verify_user(
    mut url: DatabaseURL,
    verification: Arc<ServerCertVerification>,
    user: String,
    pass: String,
) -> Result<()> {
    url.set_user(&user);
    url.set_password(&pass);
    verify_connection(&url, &verification, None).await?;
    Ok(())
}

async fn verify_users(
    url: &DatabaseURL,
    verification: Arc<ServerCertVerification>,
    options: &Options,
) -> Result<()> {
    let mut users = Vec::new();
    let allow_all = options.allow_unauthenticated_connections;
    for (user, credentials) in options.get_allowed_users(allow_all)? {
        let mut passwords = vec![(user.clone(), credentials.current.0)];
        if let Some(old) = credentials.old {
            passwords.push((format!("{user} (old password)"), old.0));
        }
        for (label, password) in passwords {
            users.push((
                label,
                tokio::spawn(verify_user(
                    url.clone(),
                    Arc::clone(&verification),
                    user.clone(),
                    password,
                )),
            ));
        }
    }
    let users = join_all(
        users
            .into_iter()
            .map(|(user, handle)| async move { Ok((user, handle.await?)) }),
    )
    .await
    .into_iter()
    .collect::<Result<Vec<(String, Result<()>)>>>()?;

    let mut failed: Vec<(String, Error)> = users
        .into_iter()
        .filter_map(|(user, res)| match res {
            Ok(()) => None,
            Err(e) => Some((user, e)),
        })
        .collect();
    failed.sort_by(|a, b| a.0.cmp(&b.0));

    if !failed.is_empty() {
        let s = if failed.len() == 1 { "" } else { "s" };
        let users = failed
            .iter()
            .map(|(user, err)| format!("{user} ({err})"))
            .collect::<Vec<_>>()
            .join(", ");
        bail!("Failed to establish connection for user{s}: {users}");
    }
    Ok(())
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
            let (enabled, format, image): (bool, String, String) = async {
                let row = conn
                    .query("SELECT @@log_bin, @@binlog_format, @@binlog_row_image")
                    .await?
                    .into_iter()
                    .get_single_row()?;
                Ok::<_, DatabaseError>((row.get(0)?, row.get(1)?, row.get(2)?))
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
            if let Ok(encryption) =
                query_one_value::<bool>(conn, "SELECT @@binlog_encryption").await
            {
                // MySQL 5.7 doesn't have this.
                ensure!(!encryption, "Upstream binlog_encryption is not supported");
            }
            if let Ok(v1) =
                query_one_value::<bool>(conn, "SELECT @@log_bin_use_v1_row_events").await
            {
                // Removed in MySQL 8.3.
                ensure!(!v1, "Upstream log_bin_use_v1_row_events is not supported");
            }
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

/// A Postgres logical replication slot is bound to the database it was created in, but slot names
/// are unique across the whole cluster. If a slot with the name Readyset would use already exists
/// for a different database, Readyset cannot use it and the replicator would otherwise crash-loop
/// on `START_REPLICATION`. Fail fast here with an actionable message instead.
///
/// `conn` must be connected to `catalog`.
async fn verify_replication_slot(
    conn: &mut DatabaseConnection,
    options: &Options,
    catalog: Option<&SqlIdentifier>,
) -> Result<()> {
    match conn.dialect() {
        Dialect::PostgreSQL => {}
        Dialect::MySQL => return Ok(()),
    }

    let config = &options.server_worker_options.replicator_config;
    let slot_name = replication_slot_name(config, catalog);
    let row = conn
        .query(&format!(
            "SELECT database, current_database() FROM pg_replication_slots \
             WHERE slot_name = '{slot_name}'"
        ))
        .await
        .map_err(|e| anyhow!("Failed to query replication slots: {e}"))?
        .into_iter()
        .next();

    if let Some(row) = row {
        let owning_db: String = row.get(0)?;
        let current_db: String = row.get(1)?;
        ensure!(
            owning_db == current_db,
            "Replication slot \"{slot_name}\" already exists for database \"{owning_db}\", but \
             this instance replicates \"{current_db}\". Use a distinct --replication-server-id per \
             Readyset deployment connected to the same upstream cluster, or drop the stale slot."
        );
    }

    info!(slot = slot_name, "Verified replication slot");
    Ok(())
}

/// Check that the upstream has enough replication slots and wal senders for the URL's database and
/// each of `catalogs`. Each database uses one wal sender and up to two slots, for replication and
/// resnapshot. Slots and active wal senders that belong to anyone else count against the limits.
async fn verify_replication_limits(
    conn: &mut DatabaseConnection,
    options: &Options,
    catalogs: &[String],
) -> Result<()> {
    match conn.dialect() {
        Dialect::PostgreSQL => {}
        Dialect::MySQL => return Ok(()),
    }

    let config = &options.server_worker_options.replicator_config;
    let catalogs: Vec<SqlIdentifier> = catalogs.iter().map(|c| c.as_str().into()).collect();
    let own_slots = iter::once(None)
        .chain(catalogs.iter().map(Some))
        .flat_map(|catalog| {
            let slot = replication_slot_name(config, catalog);
            [resnapshot_slot_name(&slot), slot]
        })
        .map(|slot| format!("'{slot}'"))
        .collect::<Vec<_>>()
        .join(", ");
    let row = conn
        .query(&format!(
            "SELECT current_setting('max_replication_slots')::int, \
             current_setting('max_wal_senders')::int, \
             (SELECT count(*)::int FROM pg_replication_slots WHERE slot_name NOT IN ({own_slots})), \
             (SELECT count(*)::int FROM pg_stat_replication r WHERE NOT EXISTS \
              (SELECT 1 FROM pg_replication_slots s \
               WHERE s.active_pid = r.pid AND s.slot_name IN ({own_slots})))"
        ))
        .await
        .and_then(|rows| rows.into_iter().get_single_row())
        .map_err(|e| anyhow!("Failed to query replication limits: {e}"))?;
    let max_slots: i32 = row.get(0)?;
    let max_senders: i32 = row.get(1)?;
    let other_slots: i32 = row.get(2)?;
    let other_senders: i32 = row.get(3)?;

    // The URL's database is not in `catalogs`.
    let databases = catalogs.len() as i32 + 1;
    let needed = 2 * databases;
    ensure!(
        max_slots - other_slots >= needed,
        "Upstream max_replication_slots is {max_slots} with {other_slots} slots used by others, \
         but Readyset needs {needed}"
    );
    ensure!(
        max_senders - other_senders >= databases,
        "Upstream max_wal_senders is {max_senders} with {other_senders} used by others, but \
         Readyset needs {databases}"
    );

    info!("Verified replication limits");
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
                    // RDS and Neon have their own representation of super.
                    let has_super: Result<String> = query_one_value(
                        conn,
                        "SELECT rolname FROM pg_roles WHERE \
                           pg_has_role(CURRENT_USER, rolname, 'member') AND \
                           rolname IN ('rds_superuser', 'neon_superuser')",
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

/// Check that the storage directory has room to snapshot the databases of `conns`. The first
/// connection is to the URL's database.
async fn verify_disk_space(conns: &mut [DatabaseConnection], options: &Options) -> Result<()> {
    let config = &options.server_worker_options.replicator_config;
    if config.replication_tables.is_some() || config.replication_tables_ignore.is_some() {
        // Don't try to figure out required space if specific tables are asked for.
        return Ok(());
    }

    let deployment_dir = options
        .server_worker_options
        .storage_dir(&options.deployment);
    // The standalone Authority opens its store in the deployment directory before verification.
    let authority = StandaloneAuthority::db_path(&deployment_dir, &options.deployment);
    if read_dir(&deployment_dir)
        .is_ok_and(|mut files| files.any(|f| !f.is_ok_and(|f| f.path() == authority)))
    {
        info!("Verified disk space for snapshot (files already exist)");
        return Ok(());
    }

    let display = deployment_dir.display().to_string();
    let mut dir = deployment_dir.clone();
    // Remove deployment directory from path, as it likely doesn't exist for new deployments.
    dir.pop();
    let dir = if dir.is_absolute() {
        dir
    } else if *dir == *"." {
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

    let mut estimated_bytes = 0;
    for conn in conns.iter_mut() {
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
        let bytes: i64 = query_one_value(conn, query)
            .await
            .map_err(|e| anyhow!("Failed to query estimated snapshot size: {e}"))?;
        estimated_bytes += bytes as u64;
    }
    let estimated_bytes = estimated_bytes * DISK_SPACE_REQUIREMENT_FACTOR;
    let free_bytes = disk.available_space();
    ensure!(
        free_bytes > estimated_bytes,
        "Estimated space required for snapshot is {estimated_bytes} bytes, but only have \
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

    let replication_enabled = options
        .server_worker_options
        .replicator_config
        .replication_enabled;

    let verification = add_err!(verify_tls(options).await);
    add_err!(verify_database_type(options));
    let config = &options.server_worker_options.replicator_config;
    if !config.replication_catalogs.is_empty() {
        add_err!(options.verify_replication_catalogs());
    }
    let upstream_servers = add_err!(verify_upstream_servers(options));
    let upstream_url = match (&verification, &upstream_servers) {
        (Some(verification), Some(servers)) => {
            add_err!(verify_servers(servers, verification, options, "upstream").await)
        }
        _ => None,
    };

    if let (Some(verification), Some(upstream_url)) = (&verification, &upstream_url) {
        add_err!(verify_users(upstream_url, Arc::clone(verification), options).await);
    }

    if replication_enabled {
        let cdc_servers = add_err!(verify_cdc_servers(options));
        // The CDC list defaults to the upstream list, which was just verified.
        let cdc_url = match (&verification, &cdc_servers) {
            (Some(_), Some(servers)) if Some(servers) == upstream_servers.as_ref() => {
                upstream_url.clone()
            }
            (Some(verification), Some(servers)) => {
                add_err!(verify_servers(servers, verification, options, "CDC").await)
            }
            _ => None,
        };
        let conn = if let (Some(verification), Some(cdc_url)) = (&verification, &cdc_url) {
            add_err!(verify_connection(cdc_url, verification, Some("CDC")).await)
        } else {
            None
        };
        let catalogs = add_err!(options.replication_catalogs().await).unwrap_or_default();
        if let (Some(verification), Some(cdc_url), Some(mut conn)) = (&verification, &cdc_url, conn)
        {
            add_err!(verify_version(&mut conn).await);
            add_err!(verify_replication(&mut conn).await);
            add_err!(verify_replication_slot(&mut conn, options, None).await);
            add_err!(verify_permissions(&mut conn, options).await);
            add_err!(verify_replication_limits(&mut conn, options, &catalogs).await);
            let mut conns = vec![conn];
            for catalog in &catalogs {
                let mut url = cdc_url.clone();
                url.set_db_name(catalog.clone());
                let name = format!("CDC (database {catalog})");
                let conn = add_err!(verify_connection(&url, verification, Some(&name)).await);
                if let Some(mut conn) = conn {
                    let catalog = SqlIdentifier::from(catalog.as_str());
                    add_err!(verify_replication_slot(&mut conn, options, Some(&catalog)).await);
                    conns.push(conn);
                }
            }
            add_err!(verify_disk_space(&mut conns, options).await);
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(Errors(errors).into())
    }
}

#[cfg(test)]
mod tests {
    use std::env;

    use clap::Parser;
    use temp_dir::TempDir;
    use test_utils::{tags, upstream};
    use tokio_postgres::NoTls;

    use super::*;

    /// The database replicated in addition to the URL's database.
    const OTHER_DB: &str = "noria_other";

    fn pgsql_url(db: &str) -> String {
        format!(
            "postgresql://postgres:noria@{}:{}/{db}",
            env::var("PGHOST").unwrap_or_else(|_| "127.0.0.1".into()),
            env::var("PGPORT").unwrap_or_else(|_| "5432".into()),
        )
    }

    /// Connect to the URL's database, and create [`OTHER_DB`] if it is missing.
    async fn connect_creating_other_db() -> tokio_postgres::Client {
        let (client, conn) = tokio_postgres::connect(&pgsql_url("noria"), NoTls)
            .await
            .unwrap();
        tokio::spawn(conn);
        let exists: bool = client
            .query_one(
                "SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)",
                &[&OTHER_DB],
            )
            .await
            .unwrap()
            .get(0);
        if !exists {
            client
                .simple_query(&format!("CREATE DATABASE {OTHER_DB}"))
                .await
                .unwrap();
        }
        client
    }

    /// Options that replicate the URL's database and `databases`. `server_id` must be unique to
    /// the test.
    fn options(dir: &TempDir, server_id: &str, databases: &str) -> Options {
        Options::try_parse_from([
            "readyset",
            "--deployment",
            server_id,
            "--authority",
            "standalone",
            "--storage-dir",
            dir.path().to_str().unwrap(),
            "--upstream-db-url",
            &pgsql_url("noria"),
            "--allow-unauthenticated-connections",
            "--cache-mode",
            "deep",
            "--replication-server-id",
            server_id,
            "--replication-databases",
            databases,
        ])
        .unwrap()
    }

    #[tokio::test(flavor = "multi_thread")]
    #[tags(serial)]
    #[upstream(postgres)]
    async fn verify_two_databases() {
        connect_creating_other_db().await;
        let dir = TempDir::new().unwrap();
        verify(&options(&dir, "verify_two", OTHER_DB))
            .await
            .unwrap();
    }

    /// A slot with the other database's slot name that belongs to the URL's database fails.
    #[tokio::test(flavor = "multi_thread")]
    #[tags(serial)]
    #[upstream(postgres)]
    async fn verify_other_database_slot_in_wrong_database() {
        let client = connect_creating_other_db().await;
        let dir = TempDir::new().unwrap();
        let options = options(&dir, "verify_wrong_db", OTHER_DB);
        let config = &options.server_worker_options.replicator_config;
        let slot = replication_slot_name(config, Some(&OTHER_DB.into()));
        client
            .execute(
                "SELECT pg_create_logical_replication_slot($1, 'pgoutput')",
                &[&slot],
            )
            .await
            .unwrap();

        let result = verify(&options).await;
        client
            .execute("SELECT pg_drop_replication_slot($1)", &[&slot])
            .await
            .unwrap();
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains(&format!("Replication slot \"{slot}\"")),
            "{err}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    #[tags(serial)]
    #[upstream(postgres)]
    async fn verify_missing_database() {
        connect_creating_other_db().await;
        let dir = TempDir::new().unwrap();
        let options = options(&dir, "verify_missing", "noria_verify_missing");
        let err = verify(&options).await.unwrap_err().to_string();
        assert!(
            err.contains("Failed to establish CDC (database noria_verify_missing) connection"),
            "{err}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    #[tags(serial)]
    #[upstream(postgres)]
    async fn verify_limits() {
        let client = connect_creating_other_db().await;
        let max_slots: String = client
            .query_one("SHOW max_replication_slots", &[])
            .await
            .unwrap()
            .get(0);
        let max_slots: usize = max_slots.parse().unwrap();
        let dir = TempDir::new().unwrap();
        let options = options(&dir, "verify_limits", OTHER_DB);
        let verification = verify_tls(&options).await.unwrap();
        let url: DatabaseURL = pgsql_url("noria").parse().unwrap();
        let mut conn = url.connect(&verification).await.unwrap();

        verify_replication_limits(&mut conn, &options, &[OTHER_DB.into()])
            .await
            .unwrap();
        let catalogs: Vec<String> = (0..max_slots).map(|i| format!("db{i}")).collect();
        let err = verify_replication_limits(&mut conn, &options, &catalogs)
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("max_replication_slots"), "{err}");
    }
}
