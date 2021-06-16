//! Documents the set of metrics that are currently being recorded within
//! a noria-client.

/// Identifies the database that is being adapted to
/// communicate with Noria.
pub enum DatabaseType {
    Mysql,
    Psql,
}

impl From<DatabaseType> for String {
    fn from(database_type: DatabaseType) -> Self {
        match database_type {
            DatabaseType::Mysql => "mysql".to_owned(),
            DatabaseType::Psql => "psql".to_owned(),
        }
    }
}
