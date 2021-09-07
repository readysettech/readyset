use noria_client::backend::BackendBuilder;
use noria_client::test_helpers::{self, Deployment};

mod common;
use common::PostgreSQLAdapter;
use postgres::NoTls;

fn setup(deployment: &Deployment) -> postgres::Config {
    test_helpers::setup::<PostgreSQLAdapter>(
        BackendBuilder::new().require_authentication(false),
        deployment,
        true,
        true,
    )
}

mod types {
    use std::fmt::Display;

    use super::*;
    use postgres::types::{FromSql, ToSql};
    use proptest::prelude::ProptestConfig;
    use test_helpers::sleep;

    fn test_type_roundtrip<T, V>(type_name: T, val: V)
    where
        T: Display,
        V: ToSql + Sync + PartialEq,
        for<'a> V: FromSql<'a>,
    {
        let d = Deployment::new("type_test");
        let config = setup(&d);
        let mut client = config.connect(NoTls).unwrap();
        client.simple_query("DROP TABLE IF EXISTS t").unwrap();
        client
            .simple_query(&format!("CREATE TABLE t (x {})", type_name))
            .unwrap();

        // check writes (going to fallback)
        client
            .execute("INSERT INTO t (x) VALUES ($1)", &[&val])
            .unwrap();

        sleep();

        // check values coming out of noria
        let star_results = client.query("SELECT * FROM t", &[]).unwrap();
        assert_eq!(star_results.len(), 1);
        assert_eq!(star_results[0].get::<_, V>(0), val);

        // check parameter parsing
        let count_where_result = client
            .query_one("SELECT count(*) FROM t WHERE x = $1", &[&val])
            .unwrap()
            .get::<_, i32>(0);
        assert_eq!(count_where_result, 1);

        // check parameter passing and value returning when going through fallback
        let fallback_result = client
            .query_one("SELECT x FROM (SELECT x FROM t WHERE x = $1) sq", &[&val])
            .unwrap()
            .get::<_, V>(0);
        assert_eq!(fallback_result, val);
    }

    macro_rules! test_types {
        ($($(#[$meta:meta])*$test_name:ident($pg_type_name: expr, $rust_type: ty);)+) => {
            $(test_types!(@impl, $(#[$meta])* $test_name, $pg_type_name, $rust_type);)+
        };

        (@impl, $(#[$meta:meta])* $test_name: ident, $pg_type_name: expr, $rust_type: ty) => {
            // these are pretty slow, so we only run a few cases at a time
            #[test_strategy::proptest(ProptestConfig {
                cases: 10,
                ..ProptestConfig::default()
            })]
            $(#[$meta])*
            fn $test_name(val: $rust_type) {
                test_type_roundtrip($pg_type_name, val);
            }
        }
    }

    // https://docs.rs/tokio-postgres/0.7.2/tokio_postgres/types/trait.ToSql.html#types
    test_types! {
        #[ignore] bool_bool("bool", bool);
        #[ignore] char_i8("char", i8);
        #[ignore] smallint_i16("smallint", i16);
        #[ignore] int_i32("integer", i32);
        #[ignore] oid_u32("oid", u32);
        #[ignore] bigint_i64("bigint", i64);
        #[ignore] real_f32("real", f32);
        #[ignore] double_f64("double precision", f64);
        #[ignore] text_string("text", String);
    }
}
