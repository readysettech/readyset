//! A weighted list of upstream servers, as accepted by `--upstream-db-url` and `--cdc-db-url`.
//!
//! Each flag takes one or more URLs, repeated or comma-separated. Each URL takes an optional
//! `|<weight>` suffix in `1..=100` (default 50); higher is preferred, as in MySQL's
//! `replication_asynchronous_connection_failover` source list.
//!
//! ```text
//! --upstream-db-url 'mysql://u:p@a:3306/db|60' --upstream-db-url mysql://u:p@b:3306/db
//! ```
//!
//! A comma inside a password must be percent-encoded. A pipe inside a password is fine.
//!
//! Only MySQL accepts more than one server: a Postgres slot and LSN belong to one cluster, so
//! there is no position to fail over with.

use std::fmt;
use std::str::FromStr;

use readyset_util::redacted::RedactedString;
use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use thiserror::Error;

use crate::error::DatabaseURLParseError;
use crate::{DatabaseType, DatabaseURL};

pub const MIN_UPSTREAM_SERVER_WEIGHT: u8 = 1;
pub const MAX_UPSTREAM_SERVER_WEIGHT: u8 = 100;
pub const DEFAULT_UPSTREAM_SERVER_WEIGHT: u8 = 50;

const ENTRY_SEPARATOR: char = ',';
const WEIGHT_SEPARATOR: char = '|';

/// Error type for parsing an [`UpstreamServer`] or an [`UpstreamServers`] list.
#[derive(Debug, Error)]
pub enum UpstreamServersParseError {
    #[error("The upstream server list is empty")]
    Empty,

    #[error(
        "Invalid upstream server weight `{value}`; expected an integer in \
         {MIN_UPSTREAM_SERVER_WEIGHT}..={MAX_UPSTREAM_SERVER_WEIGHT}"
    )]
    InvalidWeight { value: String },

    #[error("Invalid upstream server URL: {0}")]
    Url(#[from] DatabaseURLParseError),

    #[error(
        "Upstream server {display} is {database_type}, but the first server in the list is \
         {expected}; every server in a list must use the same database type"
    )]
    DialectMismatch {
        display: String,
        database_type: DatabaseType,
        expected: DatabaseType,
    },

    #[error(
        "Upstream server {display} names database {database:?}, but the first server in the list \
         names {expected:?}; every server in a list must name the same database"
    )]
    DatabaseMismatch {
        display: String,
        database: Option<String>,
        expected: Option<String>,
    },

    #[error(
        "Upstream server {display} uses different credentials than the first server in the list; \
         every server in a list must use the same user and password"
    )]
    CredentialsMismatch { display: String },

    #[error("Upstream server {display} appears more than once in the list")]
    Duplicate { display: String },

    #[error("Multiple upstream servers are not supported for {database_type}")]
    UnsupportedDialect { database_type: DatabaseType },
}

/// One server in an [`UpstreamServers`] list.
#[derive(Clone, PartialEq, Eq)]
pub struct UpstreamServer {
    url: DatabaseURL,
    raw_url: RedactedString,
    weight: u8,
    display: String,
}

impl UpstreamServer {
    /// The parsed URL of this server.
    pub fn url(&self) -> &DatabaseURL {
        &self.url
    }

    /// The URL of this server as it was configured, without the weight suffix. Contains
    /// credentials; use [`display`](Self::display) for anything that ends up in a log line.
    pub fn raw_url(&self) -> &RedactedString {
        &self.raw_url
    }

    /// The configured weight, in `1..=100`.
    pub fn weight(&self) -> u8 {
        self.weight
    }

    /// `host:port`, safe to log and to show to operators.
    pub fn display(&self) -> &str {
        &self.display
    }
}

impl fmt::Display for UpstreamServer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.display)
    }
}

/// Keeps the URL's credentials out of Debug output.
impl fmt::Debug for UpstreamServer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UpstreamServer")
            .field("display", &self.display)
            .field("weight", &self.weight)
            .finish_non_exhaustive()
    }
}

/// Parses `<url>` or `<url>|<weight>`.
impl FromStr for UpstreamServer {
    type Err = UpstreamServersParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim();
        let host = s.rfind('@').map_or_default(|at| at + 1);
        let (raw_url, weight) = match s[host..].rfind(WEIGHT_SEPARATOR) {
            Some(offset) => {
                let (raw_url, value) = s.split_at(host + offset);
                (raw_url.trim(), parse_weight(value[1..].trim())?)
            }
            None => (s, DEFAULT_UPSTREAM_SERVER_WEIGHT),
        };
        let url: DatabaseURL = raw_url.parse()?;
        let display = format!("{}:{}", url.host().unwrap_or_default(), url.port());
        Ok(Self {
            url,
            raw_url: RedactedString(raw_url.to_string()),
            weight,
            display,
        })
    }
}

fn parse_weight(value: &str) -> Result<u8, UpstreamServersParseError> {
    value
        .parse::<u8>()
        .ok()
        .filter(|w| (MIN_UPSTREAM_SERVER_WEIGHT..=MAX_UPSTREAM_SERVER_WEIGHT).contains(w))
        .ok_or_else(|| UpstreamServersParseError::InvalidWeight {
            value: value.to_string(),
        })
}

/// The highest-weight server in `servers`, the first one on a tie.
pub(crate) fn preferred(servers: &[UpstreamServer]) -> Option<&UpstreamServer> {
    servers
        .iter()
        .reduce(|best, s| if s.weight > best.weight { s } else { best })
}

/// A non-empty list of upstream servers sharing one dialect, database name, and set of
/// credentials, ordered by weight descending. Ties keep their configured order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpstreamServers {
    servers: Vec<UpstreamServer>,
}

impl UpstreamServers {
    /// Checks that `servers` form a valid list and orders them by weight.
    pub fn new(mut servers: Vec<UpstreamServer>) -> Result<Self, UpstreamServersParseError> {
        let Some(first) = servers.first() else {
            return Err(UpstreamServersParseError::Empty);
        };
        let database_type = first.url.database_type();
        for (i, server) in servers.iter().enumerate().skip(1) {
            if server.url.database_type() != database_type {
                return Err(UpstreamServersParseError::DialectMismatch {
                    display: server.display.clone(),
                    database_type: server.url.database_type(),
                    expected: database_type,
                });
            }
            if server.url.db_name() != first.url.db_name() {
                return Err(UpstreamServersParseError::DatabaseMismatch {
                    display: server.display.clone(),
                    database: server.url.db_name().map(str::to_owned),
                    expected: first.url.db_name().map(str::to_owned),
                });
            }
            if server.url.user() != first.url.user()
                || server.url.password() != first.url.password()
            {
                return Err(UpstreamServersParseError::CredentialsMismatch {
                    display: server.display.clone(),
                });
            }
            if servers[..i].iter().any(|s| s.display == server.display) {
                return Err(UpstreamServersParseError::Duplicate {
                    display: server.display.clone(),
                });
            }
        }
        if servers.len() > 1 && !database_type.supports_failover() {
            return Err(UpstreamServersParseError::UnsupportedDialect { database_type });
        }

        servers.sort_by_key(|s| std::cmp::Reverse(s.weight));
        Ok(Self { servers })
    }

    /// Number of servers in the list, always at least one.
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.servers.len()
    }

    /// Whether the list has exactly one server.
    pub fn is_single(&self) -> bool {
        self.servers.len() == 1
    }

    /// Servers in weight order, highest first.
    pub fn iter(&self) -> impl Iterator<Item = &UpstreamServer> {
        self.servers.iter()
    }

    /// The server at `index` in weight order.
    pub fn get(&self, index: usize) -> Option<&UpstreamServer> {
        self.servers.get(index)
    }

    /// The highest-weight server.
    pub fn preferred(&self) -> &UpstreamServer {
        // `new` rejects an empty list.
        &self.servers[0]
    }

    /// The index of the server whose `host:port` is `display`.
    pub fn position(&self, display: &str) -> Option<usize> {
        self.servers.iter().position(|s| s.display == display)
    }

    /// The database type shared by every server in the list.
    pub fn database_type(&self) -> DatabaseType {
        self.preferred().url.database_type()
    }

    /// The database name shared by every server in the list.
    pub fn db_name(&self) -> Option<&str> {
        self.preferred().url.db_name()
    }

    /// The `host:port` of every server, in weight order.
    pub fn displays(&self) -> Vec<&str> {
        self.servers.iter().map(|s| s.display.as_str()).collect()
    }

    /// The list in `--upstream-db-url` form, with explicit weights. Contains credentials.
    pub fn raw_list(&self) -> RedactedString {
        RedactedString(
            self.servers
                .iter()
                .map(|s| format!("{}{WEIGHT_SEPARATOR}{}", s.raw_url.as_str(), s.weight))
                .collect::<Vec<_>>()
                .join(","),
        )
    }

    /// The servers, in weight order.
    pub fn into_vec(self) -> Vec<UpstreamServer> {
        self.servers
    }
}

/// Parses the comma-separated form produced by [`UpstreamServers::raw_list`].
impl FromStr for UpstreamServers {
    type Err = UpstreamServersParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.trim().is_empty() {
            return Err(UpstreamServersParseError::Empty);
        }
        Self::new(
            s.split(ENTRY_SEPARATOR)
                .map(str::parse)
                .collect::<Result<_, _>>()?,
        )
    }
}

/// Serializes as the `--upstream-db-url` string.
impl Serialize for UpstreamServers {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.raw_list())
    }
}

impl<'de> Deserialize<'de> for UpstreamServers {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        String::deserialize(deserializer)?
            .parse()
            .map_err(D::Error::custom)
    }
}

/// Serde for the server lists in [`UpstreamConfig`](crate::UpstreamConfig): `null` when empty,
/// otherwise the `--upstream-db-url` string.
pub(crate) mod config_list {
    use super::*;

    pub fn serialize<S: Serializer>(
        servers: &[UpstreamServer],
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        match UpstreamServers::new(servers.to_vec()) {
            Ok(list) => serializer.serialize_some(&list),
            Err(UpstreamServersParseError::Empty) => serializer.serialize_none(),
            Err(e) => Err(serde::ser::Error::custom(e)),
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Vec<UpstreamServer>, D::Error> {
        Ok(Option::<UpstreamServers>::deserialize(deserializer)?
            .map(UpstreamServers::into_vec)
            .unwrap_or_default())
    }
}

/// Clap parser for [`UpstreamServer`] whose errors leave out the raw value, which holds the
/// password.
#[derive(Debug, Clone)]
pub struct UpstreamServerValueParser;

impl clap::builder::TypedValueParser for UpstreamServerValueParser {
    type Value = UpstreamServer;

    fn parse_ref(
        &self,
        cmd: &clap::Command,
        arg: Option<&clap::Arg>,
        value: &std::ffi::OsStr,
    ) -> Result<Self::Value, clap::Error> {
        let make_error = |message: String| {
            let name = arg.map(ToString::to_string).unwrap_or_default();
            clap::Error::raw(
                clap::error::ErrorKind::ValueValidation,
                format!("invalid value for {name}: {message}\n"),
            )
            .with_cmd(cmd)
        };
        let value = value
            .to_str()
            .ok_or_else(|| make_error("not valid UTF-8".to_string()))?;
        value
            .parse()
            .map_err(|e: UpstreamServersParseError| make_error(e.to_string()))
    }
}

impl clap::builder::ValueParserFactory for UpstreamServer {
    type Parser = UpstreamServerValueParser;

    fn value_parser() -> Self::Parser {
        UpstreamServerValueParser
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(s: &str) -> UpstreamServers {
        s.parse().expect("list parses")
    }

    fn parse_err(s: &str) -> String {
        s.parse::<UpstreamServers>()
            .expect_err("list must be rejected")
            .to_string()
    }

    #[test]
    fn single_url_is_a_one_element_list_with_default_weight() {
        assert_eq!(
            parse("mysql://u:p@a:3306/db"),
            UpstreamServers {
                servers: vec![UpstreamServer {
                    url: "mysql://u:p@a:3306/db".parse().unwrap(),
                    raw_url: RedactedString("mysql://u:p@a:3306/db".into()),
                    weight: DEFAULT_UPSTREAM_SERVER_WEIGHT,
                    display: "a:3306".into(),
                }],
            }
        );
    }

    #[test]
    fn default_mysql_port_is_filled_in() {
        assert_eq!(
            parse("mysql://u:p@a/db"),
            UpstreamServers {
                servers: vec![UpstreamServer {
                    url: "mysql://u:p@a/db".parse().unwrap(),
                    raw_url: RedactedString("mysql://u:p@a/db".into()),
                    weight: DEFAULT_UPSTREAM_SERVER_WEIGHT,
                    display: "a:3306".into(),
                }],
            }
        );
    }

    #[test]
    fn weights_order_the_list_highest_first_and_ties_keep_config_order() {
        assert_eq!(
            parse(
                "mysql://u:p@a:3306/db|10, mysql://u:p@b:3306/db|60 ,mysql://u:p@c:3306/db|60,\
                 mysql://u:p@d:3306/db"
            ),
            UpstreamServers {
                servers: vec![
                    UpstreamServer {
                        url: "mysql://u:p@b:3306/db".parse().unwrap(),
                        raw_url: RedactedString("mysql://u:p@b:3306/db".into()),
                        weight: 60,
                        display: "b:3306".into(),
                    },
                    UpstreamServer {
                        url: "mysql://u:p@c:3306/db".parse().unwrap(),
                        raw_url: RedactedString("mysql://u:p@c:3306/db".into()),
                        weight: 60,
                        display: "c:3306".into(),
                    },
                    UpstreamServer {
                        url: "mysql://u:p@d:3306/db".parse().unwrap(),
                        raw_url: RedactedString("mysql://u:p@d:3306/db".into()),
                        weight: DEFAULT_UPSTREAM_SERVER_WEIGHT,
                        display: "d:3306".into(),
                    },
                    UpstreamServer {
                        url: "mysql://u:p@a:3306/db".parse().unwrap(),
                        raw_url: RedactedString("mysql://u:p@a:3306/db".into()),
                        weight: 10,
                        display: "a:3306".into(),
                    },
                ],
            }
        );
    }

    #[test]
    fn every_punctuation_character_round_trips_through_a_percent_encoded_password() {
        for c in (b'!'..=b'~')
            .filter(u8::is_ascii_punctuation)
            .map(char::from)
        {
            let raw_url = format!("mysql://u:a%{:02X}b@h:3306/db", c as u8);
            let parsed = parse(&format!("{raw_url}|5"));
            assert_eq!(
                parsed,
                UpstreamServers {
                    servers: vec![UpstreamServer {
                        url: raw_url.parse().unwrap(),
                        raw_url: RedactedString(raw_url.to_string()),
                        weight: 5,
                        display: "h:3306".into(),
                    }],
                },
                "{c}"
            );
            assert_eq!(
                parsed.preferred().url().password(),
                Some(format!("a{c}b").as_str()),
                "{c}"
            );
        }
    }

    #[test]
    fn a_raw_pipe_in_a_password_is_part_of_the_url() {
        for (input, raw_url, weight, password) in [
            (
                "mysql://u:p|w@a:3306/db|5",
                "mysql://u:p|w@a:3306/db",
                5,
                "p|w",
            ),
            (
                "mysql://u:pw|@a:3306/db",
                "mysql://u:pw|@a:3306/db",
                50,
                "pw|",
            ),
        ] {
            let parsed = parse(input);
            assert_eq!(
                parsed,
                UpstreamServers {
                    servers: vec![UpstreamServer {
                        url: raw_url.parse().unwrap(),
                        raw_url: RedactedString(raw_url.to_string()),
                        weight,
                        display: "a:3306".into(),
                    }],
                }
            );
            assert_eq!(parsed.preferred().url().password(), Some(password));
        }
    }

    #[test]
    fn invalid_weights_are_rejected_without_echoing_credentials() {
        for weight in ["0", "101", "-1", "abc", "", "nope"] {
            assert_eq!(
                parse_err(&format!("mysql://root:se|cret@a:3306/db|{weight}")),
                format!(
                    "Invalid upstream server weight `{weight}`; expected an integer in 1..=100"
                )
            );
        }
    }

    #[test]
    fn empty_lists_and_entries_are_rejected() {
        assert_eq!(parse_err(""), "The upstream server list is empty");
        assert_eq!(parse_err("   "), "The upstream server list is empty");
        let url_error = ""
            .parse::<DatabaseURL>()
            .expect_err("empty url")
            .to_string();
        let expected = format!("Invalid upstream server URL: {url_error}");
        assert_eq!(parse_err("mysql://u:p@a:3306/db,"), expected);
        assert_eq!(parse_err(",mysql://u:p@a:3306/db"), expected);
    }

    #[test]
    fn bad_urls_are_rejected() {
        let url_error = "nonsense"
            .parse::<DatabaseURL>()
            .expect_err("bad url")
            .to_string();
        assert_eq!(
            parse_err("mysql://u:p@a:3306/db,nonsense"),
            format!("Invalid upstream server URL: {url_error}")
        );
    }

    #[test]
    fn dialects_must_match() {
        assert_eq!(
            parse_err("mysql://u:p@a:3306/db,postgresql://u:p@b:5432/db"), // trufflehog:ignore
            "Upstream server b:5432 is postgresql, but the first server in the list is mysql; \
             every server in a list must use the same database type"
        );
    }

    #[test]
    fn postgresql_accepts_one_server_only() {
        assert_eq!(
            parse("postgres://u:p@b/db"), // trufflehog:ignore
            UpstreamServers {
                servers: vec![UpstreamServer {
                    url: "postgres://u:p@b/db".parse().unwrap(), // trufflehog:ignore
                    raw_url: RedactedString("postgres://u:p@b/db".into()), // trufflehog:ignore
                    weight: DEFAULT_UPSTREAM_SERVER_WEIGHT,
                    display: "b:5432".into(),
                }],
            }
        );
        assert_eq!(
            parse_err("postgresql://u:p@a:5432/db|60,postgresql://u:p@b:5432/db|40"), // trufflehog:ignore
            "Multiple upstream servers are not supported for postgresql"
        );
    }

    #[test]
    fn database_names_must_match() {
        assert_eq!(
            parse_err("mysql://u:p@a:3306/db,mysql://u:p@b:3306/other"),
            "Upstream server b:3306 names database Some(\"other\"), but the first server in the \
             list names Some(\"db\"); every server in a list must name the same database"
        );
    }

    #[test]
    fn credentials_must_match() {
        let expected = "Upstream server b:3306 uses different credentials than the first server in \
                        the list; every server in a list must use the same user and password";
        assert_eq!(
            parse_err("mysql://u:p@a:3306/db,mysql://u:other@b:3306/db"),
            expected
        );
        assert_eq!(
            parse_err("mysql://u:p@a:3306/db,mysql://v:p@b:3306/db"),
            expected
        );
        assert_eq!(
            parse_err("mysql://u:p@a:3306/db,mysql://u@b:3306/db"),
            expected
        );
    }

    #[test]
    fn duplicate_servers_are_rejected() {
        assert_eq!(
            parse_err("mysql://u:p@a:3306/db|10,mysql://u:p@a:3306/db|20"),
            "Upstream server a:3306 appears more than once in the list"
        );
    }

    #[test]
    fn preferred_picks_the_first_of_the_highest_weight() {
        let servers = vec![
            UpstreamServer {
                url: "mysql://u:p@a:3306/db".parse().unwrap(),
                raw_url: RedactedString("mysql://u:p@a:3306/db".into()),
                weight: 10,
                display: "a:3306".into(),
            },
            UpstreamServer {
                url: "mysql://u:p@b:3306/db".parse().unwrap(),
                raw_url: RedactedString("mysql://u:p@b:3306/db".into()),
                weight: 60,
                display: "b:3306".into(),
            },
            UpstreamServer {
                url: "mysql://u:p@c:3306/db".parse().unwrap(),
                raw_url: RedactedString("mysql://u:p@c:3306/db".into()),
                weight: 60,
                display: "c:3306".into(),
            },
        ];
        assert_eq!(preferred(&servers), Some(&servers[1]));
        assert_eq!(preferred(&[]), None);
    }

    #[test]
    fn config_accessors_default_cdc_to_upstream_and_pin_one_server() {
        let a = UpstreamServer {
            url: "mysql://u:p@a:3306/db".parse().unwrap(),
            raw_url: RedactedString("mysql://u:p@a:3306/db".into()),
            weight: 10,
            display: "a:3306".into(),
        };
        let b = UpstreamServer {
            url: "mysql://u:p@b:3306/db".parse().unwrap(),
            raw_url: RedactedString("mysql://u:p@b:3306/db".into()),
            weight: 90,
            display: "b:3306".into(),
        };
        let config = crate::UpstreamConfig {
            upstream_db_url: vec![a.clone(), b.clone()],
            ..Default::default()
        };
        let upstream = UpstreamServers {
            servers: vec![b.clone(), a.clone()],
        };
        assert_eq!(config.upstream_servers().unwrap(), Some(upstream.clone()));
        assert_eq!(config.cdc_list().unwrap(), None);
        assert_eq!(config.cdc_servers().unwrap(), upstream);
        assert_eq!(config.preferred_upstream_url(), Some(b.url()));
        assert_eq!(&config.preferred_cdc_url().unwrap(), b.url());

        let pinned = config.for_server(&a);
        assert_eq!(pinned.upstream_db_url, vec![a.clone()]);
        assert_eq!(pinned.cdc_db_url, vec![a.clone()]);

        let c = UpstreamServer {
            url: "mysql://r:p@c:3306/db".parse().unwrap(),
            raw_url: RedactedString("mysql://r:p@c:3306/db".into()),
            weight: DEFAULT_UPSTREAM_SERVER_WEIGHT,
            display: "c:3306".into(),
        };
        let split = crate::UpstreamConfig {
            cdc_db_url: vec![c.clone()],
            ..config
        };
        assert_eq!(split.upstream_servers().unwrap(), Some(upstream));
        assert_eq!(
            split.cdc_list().unwrap(),
            Some(UpstreamServers {
                servers: vec![c.clone()],
            })
        );
        assert_eq!(
            split.cdc_servers().unwrap(),
            UpstreamServers { servers: vec![c] }
        );

        let unset = crate::UpstreamConfig::default();
        assert_eq!(unset.upstream_servers().unwrap(), None);
        assert_eq!(unset.preferred_upstream_url(), None);
        assert_eq!(
            unset.cdc_servers().unwrap_err().to_string(),
            "URL parse failed: Please provide --cdc-db-url or --upstream-db-url"
        );
    }

    #[test]
    fn serde_round_trips_through_the_config_string() {
        let parsed = parse("mysql://u:p@a:3306/db|10,mysql://u:p@b:3306/db");
        let json = serde_json::to_string(&parsed).expect("list serializes");
        let back: UpstreamServers = serde_json::from_str(&json).expect("list deserializes");
        assert_eq!(back, parsed);

        let err = serde_json::from_str::<UpstreamServers>("\"nonsense\"")
            .expect_err("invalid list must be rejected");
        assert_eq!(
            err.to_string(),
            "Invalid upstream server URL: Invalid database URL format; database URLs must start \
             with either mysql:// or postgresql://"
        );
    }

    #[test]
    fn config_serde_accepts_a_persisted_single_url() {
        let config: crate::UpstreamConfig =
            serde_json::from_str(r#"{"upstream_db_url": "mysql://u:p@a:3306/db"}"#)
                .expect("config deserializes");
        assert_eq!(
            config.upstream_db_url,
            vec![UpstreamServer {
                url: "mysql://u:p@a:3306/db".parse().unwrap(),
                raw_url: RedactedString("mysql://u:p@a:3306/db".into()),
                weight: DEFAULT_UPSTREAM_SERVER_WEIGHT,
                display: "a:3306".into(),
            }]
        );
        assert_eq!(config.cdc_db_url, vec![]);

        let json = serde_json::to_value(&config).expect("config serializes");
        assert_eq!(json["upstream_db_url"], "mysql://u:p@a:3306/db|50");
        assert_eq!(json["cdc_db_url"], serde_json::Value::Null);
    }

    #[test]
    fn clap_errors_never_echo_credentials() {
        use clap::Parser;

        let err = crate::UpstreamConfig::try_parse_from([
            "test",
            "--upstream-db-url",
            "mysql://root:secret@a:3306/db,nonsense",
        ])
        .expect_err("invalid list must be rejected");
        assert_eq!(
            err.to_string(),
            "error: invalid value for --upstream-db-url <UPSTREAM_DB_URL>: Invalid upstream server \
             URL: Invalid database URL format; database URLs must start with either mysql:// or \
             postgresql://\n"
        );
    }

    #[test]
    fn display_and_debug_never_contain_credentials() {
        let parsed = parse("mysql://user:secret@a:3306/db|10");
        let server = parsed.preferred();
        assert_eq!(server.display(), "a:3306");
        assert_eq!(server.to_string(), "a:3306");
        assert_eq!(
            format!("{server:?}"),
            "UpstreamServer { display: \"a:3306\", weight: 10, .. }"
        );
        assert_eq!(
            format!("{parsed:?}"),
            "UpstreamServers { servers: [UpstreamServer { display: \"a:3306\", weight: 10, .. }] }"
        );
    }
}
