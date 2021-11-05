use mysql_async::Value;
use nom_sql::SqlType;
use query_generator::RandomGenerator;
use std::convert::TryInto;

/// Uses a random generator that uses the bounds of the SqlType to determine the
/// range to generate random values in. This is unlikely to randomly generate
/// values within the range of base tables.
pub(crate) fn random_value_for_sql_type(sql_type: &SqlType) -> Value {
    let random = RandomGenerator::from(sql_type.clone());
    random.gen().try_into().unwrap()
}
