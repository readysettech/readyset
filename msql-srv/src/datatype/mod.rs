//! MySQL-specific datatypes and data representation

mod mysql_time;

pub use self::mysql_time::{ConvertError, MysqlTime};
