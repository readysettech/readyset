use mysql_async::consts::ColumnType;
use mysql_async::Value;
use nom_sql::SqlType;
use query_generator::RandomGenerator;
use std::convert::TryInto;

pub(crate) fn random_value_for_sql_type(sql_type: &ColumnType) -> Value {
    use ColumnType::*;
    let t = match sql_type {
        // TODO(justin): Abstract random value generation to utilities crate
        MYSQL_TYPE_LONGLONG => SqlType::UnsignedInt(None),
        MYSQL_TYPE_DATETIME => SqlType::DateTime(None),
        t => unimplemented!("Unsupported type: {:?}", t),
    };

    let random = RandomGenerator::from(t);

    random.gen().try_into().unwrap()
}
