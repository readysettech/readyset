use noria_client::backend::noria_connector::ReadBehavior;
use noria_client::backend::BackendBuilder;
use noria_client_test_helpers::psql_helpers::PostgreSQLAdapter;
use noria_server::Handle;

mod common;
use common::connect;

async fn setup() -> (tokio_postgres::Config, Handle) {
    noria_client_test_helpers::setup::<PostgreSQLAdapter>(
        BackendBuilder::new().require_authentication(false),
        true,
        true,
        true,
        ReadBehavior::Blocking,
    )
    .await
}

mod types {
    use std::fmt::Display;

    use eui48::MacAddress;
    use launchpad::arbitrary::{
        arbitrary_bitvec, arbitrary_date_time, arbitrary_decimal, arbitrary_json,
        arbitrary_json_without_f64, arbitrary_mac_address, arbitrary_naive_date,
        arbitrary_naive_time, arbitrary_systemtime, arbitrary_uuid,
    };
    use noria_client_test_helpers::sleep;
    use proptest::prelude::ProptestConfig;
    use rust_decimal::Decimal;
    use tokio_postgres::types::{FromSql, ToSql};
    use uuid::Uuid;

    use super::*;

    async fn test_type_roundtrip<T, V>(type_name: T, val: V)
    where
        T: Display,
        V: ToSql + Sync + PartialEq,
        for<'a> V: FromSql<'a>,
    {
        let (config, _handle) = setup().await;
        let client = connect(config).await;

        sleep().await;

        client
            .simple_query(&format!("CREATE TABLE t (x {})", type_name))
            .await
            .unwrap();

        // check writes (going to fallback)
        client
            .execute("INSERT INTO t (x) VALUES ($1)", &[&val])
            .await
            .unwrap();

        sleep().await;
        sleep().await;

        // check values coming out of noria
        let star_results = client.query("SELECT * FROM t", &[]).await.unwrap();

        assert_eq!(star_results.len(), 1);
        assert_eq!(star_results[0].get::<_, V>(0), val);

        // check parameter parsing
        if format!("{}", type_name).as_str() != "json" {
            let count_where_result = client
                .query_one("SELECT count(*) FROM t WHERE x = $1", &[&val])
                .await
                .unwrap()
                .get::<_, i64>(0);
            assert_eq!(count_where_result, 1);
        }
        // check parameter passing and value returning when going through fallback
        // TODO(DAN): below statement fails for UUID and JSON
        /*
        let fallback_result = client
            .query_one("SELECT x FROM (SELECT x FROM t WHERE x = $1) sq", &[&val])
            .unwrap()
            .get::<_, V>(0);
        assert_eq!(fallback_result, val);
        */
    }

    macro_rules! test_types {
        ($($(#[$meta:meta])*$test_name:ident($pg_type_name: expr, $(#[$strategy:meta])*$rust_type: ty);)+) => {
            $(test_types!(@impl, $(#[$meta])* $test_name, $pg_type_name, $(#[$strategy])* $rust_type);)+
        };

        (@impl, $(#[$meta:meta])* $test_name: ident, $pg_type_name: expr, $(#[$strategy:meta])* $rust_type: ty) => {
            // these are pretty slow, so we only run a few cases at a time
            #[test_strategy::proptest(ProptestConfig {
                cases: 5,
                ..ProptestConfig::default()
            })]
            #[serial_test::serial]
            $(#[$meta])*
            fn $test_name($(#[$strategy])* val: $rust_type) {
                let rt = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                rt.block_on(test_type_roundtrip($pg_type_name, val));
            }
        };
    }

    // https://docs.rs/tokio-postgres/0.7.2/tokio_postgres/types/trait.ToSql.html#types
    test_types! {
        bool_bool("bool", bool);
        smallint_i16("smallint", i16);
        int_i32("integer", i32);
        oid_u32("oid", u32);
        bigint_i64("bigint", i64);
        real_f32("real", f32);
        double_f64("double precision", f64);
        text_string("text", String);
        bytea_bytes("bytea", Vec<u8>);
        // TODO(fran): Add numeric with precision and scale when we start correctly
        //  handling them.
        numeric_decimal("numeric", #[strategy(arbitrary_decimal())] Decimal);
        decimal("decimal", #[strategy(arbitrary_decimal())] Decimal);
        timestamp_systemtime("timestamp", #[strategy(arbitrary_systemtime())] std::time::SystemTime);
        macaddr_string("macaddr", #[strategy(arbitrary_mac_address())] MacAddress);
        uuid_string("uuid", #[strategy(arbitrary_uuid())] Uuid);
        date_naivedate("date", #[strategy(arbitrary_naive_date())] chrono::NaiveDate);
        time_naivetime("time", #[strategy(arbitrary_naive_time())] chrono::NaiveTime);
        json_string("json", #[strategy(arbitrary_json())] serde_json::Value);
        jsonb_string("jsonb", #[strategy(arbitrary_json_without_f64())] serde_json::Value);
        bit_bitvec("bit", #[strategy(arbitrary_bitvec(1..=1))] bit_vec::BitVec);
        bit_sized_bitvec("bit(20)", #[strategy(arbitrary_bitvec(20..=20))] bit_vec::BitVec);
        varbit_unlimited_bitvec("varbit", #[strategy(arbitrary_bitvec(0..=20))] bit_vec::BitVec);
        varbit_bitvec("varbit(10)", #[strategy(arbitrary_bitvec(0..=10))] bit_vec::BitVec);
        bit_varying_unlimited_bitvec("bit varying", #[strategy(arbitrary_bitvec(0..=20))] bit_vec::BitVec);
        bit_varying_bitvec("bit varying(10)", #[strategy(arbitrary_bitvec(0..=10))] bit_vec::BitVec);
        timestamp_tz_datetime("timestamp with time zone", #[strategy(arbitrary_date_time())] chrono::DateTime::<chrono::FixedOffset>);
    }
}
