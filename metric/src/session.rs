/// Counter: The number of SET statements that were disallowed.
pub const QUERY_LOG_SET_DISALLOWED: &str = "readyset_query_log_set_disallowed";

/// Counter: The number of times autocommit was disabled via SET autocommit=0.
pub const SET_AUTOCOMMIT_DISABLED: &str = "readyset_set_autocommit_disabled";

/// Counter: The number of times autocommit was re-enabled via SET autocommit=1.
pub const SET_AUTOCOMMIT_ENABLED: &str = "readyset_set_autocommit_enabled";

/// Counter: The number of times a given character set has been seen in SET statements
///
/// - type: The type of usage of the character set
///     - `SET <variable>` maps to the variable name, should be one of:
///         - `character_set_client`
///         - `character_set_server`
///         - `character_set_results`
///         - `character_set_connection`
///         - `collation_connection`
///         - `collation_server`
///     - `SET CHARACTER SET` maps to `character_set`
///     - `SET NAMES` maps to `names`
///     - Connection handshake and `COM_CHANGE_USER` map to `protocol`
/// - charset: The lowercased name of the character set/collation, or the u16 numeric id
pub const CHARACTER_SET_USAGE: &str = "readyset_mysql_character_set_usage";
