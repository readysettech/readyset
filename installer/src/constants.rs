/// The image prefix for of all our internally held images.
pub const IMG_PREFIX: &str = "305232526136.dkr.ecr.us-east-2.amazonaws.com";

/// The postfix for our internally mirrored consul image.
pub const CONSUL_POSTFIX: &str = "/mirror/consul";

/// The postfix for our internally mirrored mysql 8.0 image.
pub const MYSQL_POSTFIX: &str = "/mirror/mysql:8.0";

/// The postfix for our hosted readyset-server image.
pub const READYSET_SERVER_POSTFIX: &str = "/readyset-server";

/// The postfix for our hosted readyset-mysql adapter image.
pub const READYSET_MYSQL_POSTFIX: &str = "/readyset-mysql";

/// The current release tag for our readyset-server and readyset-mysql images.
pub const READYSET_TAG: &str = "release-eb53b0bd611d8205c96e4afea52ada38945f2565";
