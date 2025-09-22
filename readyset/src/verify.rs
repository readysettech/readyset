use std::fmt::{Display, Formatter};

use anyhow::{anyhow, bail, ensure, Error, Result};
use regex::{Captures, Regex};
use tracing::info;

use database_utils::tls::ServerCertVerification;
use database_utils::{DatabaseConnection, DatabaseType, DatabaseURL, QueryableConnection};
use readyset_sql::Dialect;

use crate::Options;

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

async fn get_version(conn: &mut DatabaseConnection) -> Result<(String, i32, i32)> {
    let version: String = async {
        conn.query(match conn.dialect() {
            Dialect::MySQL => "SELECT @@version",
            Dialect::PostgreSQL => "SHOW server_version",
        })
        .await?
        .into_iter()
        .get_single_row()?
        .get(0)
    }
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
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(Errors(errors).into())
    }
}
