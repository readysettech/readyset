use std::str::FromStr;

use url::Url;

use crate::error::DatabaseURLParseError;

#[derive(Debug, Clone)]
pub struct VitessConfig {
    pub host: String,
    pub grpc_port: u16,
    pub mysql_port: u16,

    pub username: String,
    pub password: String,

    pub keyspace: String,
    pub shard: String,
}

impl VitessConfig {
    pub fn from_str(s: &str) -> Result<Self, DatabaseURLParseError> {
        let url = Url::parse(s).map_err(|_| DatabaseURLParseError::InvalidFormat)?;

        if url.scheme() != "vitess" {
            return Err(DatabaseURLParseError::InvalidFormat);
        }

        Ok(Self {
            host: url.host_str().unwrap().to_string(),
            grpc_port: url.port().unwrap_or(15301) as u16,
            mysql_port: get_param(&url, "mysql_port", 15302)?,
            username: url.username().to_string(),
            password: url.password().unwrap_or("").to_string(),
            keyspace: url.path().trim_start_matches('/').to_string(),
            shard: get_param(&url, "shard", "0".to_string())?,
        })
    }

    pub fn keyspace(&self) -> String {
        self.keyspace.clone()
    }

    pub fn grpc_url(&self) -> String {
        format!(
            "http://{}:{}@{}:{}/{}",
            self.username, self.password, self.host, self.grpc_port, self.keyspace
        )
    }
}

fn get_param<T: FromStr>(url: &Url, param: &str, default: T) -> Result<T, DatabaseURLParseError> {
    url.query_pairs()
        .find_map(|(k, v)| if k == param { Some(v.parse()) } else { None })
        .unwrap_or_else(|| Ok(default))
        .map_err(|_| DatabaseURLParseError::InvalidFormat)
}
