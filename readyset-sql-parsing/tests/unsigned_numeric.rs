use readyset_sql::Dialect;
use readyset_sql::ast::{CreateTableStatement, SqlQuery, SqlType};
use readyset_sql_parsing::{ParsingPreset, parse_query_with_config};

#[test]
fn unsigned_decimal_and_float_types_convert_to_signed_types() {
    for (ty, expected) in [
        ("decimal(20, 4) unsigned", SqlType::Decimal(20, 4)),
        ("dec(10) unsigned", SqlType::Decimal(10, 0)),
        ("float unsigned", SqlType::Float),
        ("real unsigned", SqlType::Real),
        ("double unsigned", SqlType::Double),
        ("double precision unsigned", SqlType::Double),
    ] {
        let sql = format!("CREATE TABLE t (x {ty})");
        match parse_query_with_config(ParsingPreset::OnlySqlparser, Dialect::MySQL, &sql) {
            Ok(SqlQuery::CreateTable(CreateTableStatement { body: Ok(body), .. })) => {
                assert_eq!(body.fields[0].sql_type, expected, "{sql}")
            }
            other => panic!("{sql}: {other:?}"),
        }
    }
}
