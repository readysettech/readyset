/// A SQL column type, including any column attributes. (CHAR, for example, has a length attribute).
pub use nom_sql::SqlType as ColType;

/// A PostgreSQL data type that can be received from, or sent to, a PostgreSQL frontend.
pub use postgres_types::Type;
