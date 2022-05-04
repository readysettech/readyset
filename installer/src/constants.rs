/// A simple API key used to verify that a user was given permission to use the installer. Security
/// constraints are very low for this.
pub const API_KEY: &str = "fb1c9ee4bb847f02ec0b5546a6655835";

/// The image prefix for of all our internally held images.
pub const IMG_PREFIX: &str = "305232526136.dkr.ecr.us-east-2.amazonaws.com";

/// The image tag for the consul container
pub const CONSUL_TAG: &str = "consul";

/// The image tag for non-aarch64 mysql container
#[cfg(not(target_arch = "aarch64"))]
pub const MYSQL_TAG: &str = "mysql:8.0";

/// The image tag for aarch64 mysql container
#[cfg(target_arch = "aarch64")]
pub const MYSQL_TAG: &str = "mysql:8.0-oracle";

/// The image tag for the postgres container
pub const POSTGRES_TAG: &str = "postgres:13";

/// The postfix for our hosted readyset-server image.
pub const READYSET_SERVER_POSTFIX: &str = "/readyset-server";

/// The postfix for our hosted readyset-mysql adapter image.
pub const READYSET_MYSQL_POSTFIX: &str = "/readyset-mysql";

/// The postfix for our hosted readyset-psql adapter image.
pub const READYSET_POSTGRES_POSTFIX: &str = "/readyset-psql";

/// The current release tag for our readyset-server and readyset-mysql images.
pub const READYSET_TAG: &str = "release-2873f79074683714545e55386f752e78189c1883";

/// Used if there is no paired version specified.
/// Hardcoded to the last public release template set.
pub const CFN_VERSION: &str = "2022-05-04";

// TODO: TEMPORARY CONSTANTS. SHOULD BE REMOVED WHEN WE HAVE AN ARTIFACT REGISTRY.

/// Readyset self hosted container images prefix.
pub const READYSET_URL_PREFIX: &str = "https://launch.readyset.io/docker-images/latest/";

/// Server image location minus release and tar.gz ending.
pub const READYSET_SERVER_FILE_PREFIX: &str = "readyset-server";

/// readyset-mysql adapter image location minus release and tar.gz ending.
pub const READYSET_MYSQL_ADAPTER_FILE_PREFIX: &str = "readyset-mysql";

/// readyset-PSQL adapter image location minus release and tar.gz ending.
pub const READYSET_PSQL_ADAPTER_FILE_PREFIX: &str = "readyset-psql";
