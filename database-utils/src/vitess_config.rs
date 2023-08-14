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
        // TODO: actually parse the string
        Ok(Self {
            host: "localhost".to_string(),
            grpc_port: 15301,
            mysql_port: 15302,
            username: "root".to_string(),
            password: "".to_string(),
            keyspace: "commerce".to_string(),
            shard: "0".to_string(),
        })
    }
}
