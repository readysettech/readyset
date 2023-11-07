use readyset_adapter::backend::BackendBuilder;
use readyset_client_test_helpers::psql_helpers::PostgreSQLAdapter;
use readyset_client_test_helpers::TestBuilder;
use readyset_server::Handle;
use readyset_util::shutdown::ShutdownSender;

mod common;
use common::connect;

async fn setup() -> (tokio_postgres::Config, Handle, ShutdownSender) {
    TestBuilder::new(BackendBuilder::new().require_authentication(false))
        .fallback(true)
        .build::<PostgreSQLAdapter>()
        .await
}

mod types {
    use std::fmt::Display;
    use std::panic::{AssertUnwindSafe, RefUnwindSafe};
    use std::time::Duration;

    use cidr::IpInet;
    use eui48::MacAddress;
    use proptest::collection::vec;
    use proptest::prelude::*;
    use proptest::string::string_regex;
    use readyset_adapter::backend::QueryDestination;
    use readyset_client_test_helpers::psql_helpers::{last_query_info, upstream_config};
    use readyset_client_test_helpers::sleep;
    use readyset_data::DfValue;
    use readyset_util::arbitrary::{
        arbitrary_bitvec, arbitrary_date_time, arbitrary_decimal, arbitrary_ipinet, arbitrary_json,
        arbitrary_json_without_f64, arbitrary_mac_address, arbitrary_naive_date,
        arbitrary_naive_time, arbitrary_systemtime, arbitrary_uuid,
    };
    use readyset_util::eventually;
    use rust_decimal::Decimal;
    use tokio_postgres::types::{FromSql, ToSql};
    use tokio_postgres::NoTls;
    use tracing::log::info;
    use uuid::Uuid;

    use super::*;

    async fn test_type_roundtrip<T, V>(type_name: T, vals: Vec<V>)
    where
        T: Display,
        V: ToSql + Sync + PartialEq + RefUnwindSafe,
        for<'a> V: FromSql<'a>,
    {
        let (config, _handle, shutdown_tx) = setup().await;
        let mut client = connect(config).await;

        client
            .simple_query(&format!("CREATE TABLE t (id int, x {})", type_name))
            .await
            .unwrap();

        for (i, v) in vals.iter().enumerate() {
            // check writes (going to fallback)
            client
                .execute("INSERT INTO t VALUES ($1, $2)", &[&(i as i32), v])
                .await
                .unwrap();
        }

        // check values coming out of noria
        eventually!(run_test: {
            client.query("SELECT x FROM t ORDER BY id", &[])
                .await
                .unwrap()
                .iter()
                .map(|row| row.get::<_, V>(0))
                .collect::<Vec<_>>()
        }, then_assert: |results| {
            assert_eq!(results, vals);
        });

        // check parameter parsing
        if type_name.to_string().as_str() != "json" {
            for v in vals.iter() {
                let count_where_result = client
                    .query_one(
                        format!("SELECT count(*) FROM t WHERE x = cast($1 as {type_name})")
                            .as_str(),
                        &[v],
                    )
                    .await
                    .unwrap()
                    .get::<_, i64>(0);
                assert!(count_where_result >= 1);
            }
        }

        // Can't compare JSON for equality in postgres
        if type_name.to_string() != "json" {
            // check parameter passing and value returning when going through fallback

            for v in vals.iter() {
                let fallback_result = client
                    .transaction()
                    .await
                    .unwrap()
                    .query("SELECT x FROM t WHERE x = $1", &[v])
                    .await
                    .unwrap()
                    .get(0)
                    .expect("Should have at least one value equal to {v:?}")
                    .get::<_, V>(0);
                assert_eq!(fallback_result, *v);
            }
        }

        shutdown_tx.shutdown().await;
    }

    macro_rules! test_types {
        ($($(#[$meta:meta])* $test_name:ident($pg_type_name: expr, $rust_type: ty $(, $strategy:expr)?);)+) => {
            $(test_types!(@impl, $(#[$meta])* $test_name, $pg_type_name, $rust_type $(, $strategy)?);)+
        };

        (@impl, $(#[$meta:meta])* $test_name: ident, $pg_type_name: expr, $rust_type: ty) => {
            test_types!(@impl, $(#[$meta])* $test_name, $pg_type_name, $rust_type, any::<$rust_type>());
        };

        (@impl, $(#[$meta:meta])* $test_name: ident, $pg_type_name: expr, $rust_type: ty, $strategy: expr) => {
            // these are pretty slow, so we only run a few cases at a time
            #[test_strategy::proptest(ProptestConfig {
                cases: 5,
                max_shrink_iters: 200, // May need many shrink iters for large vecs
                ..ProptestConfig::default()
            })]
            #[serial_test::serial]
            $(#[$meta])*
            fn $test_name(#[strategy(vec($strategy, 1..20))] vals: Vec<$rust_type>) {
                readyset_tracing::init_test_logging();
                let rt = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                rt.block_on(test_type_roundtrip($pg_type_name, vals));
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
        // If we happen to generate 0 for char_i8, we hit the failure described in REA-3271. Once
        // that issue is fixed, we should remove the filter from the strategy here.
        char_i8("\"char\"", i8, any::<i8>().prop_filter("Workaround for REA-3271", |v| *v != 0));
        text_string("text", String);
        bpchar_string("bpchar", String);
        bytea_bytes("bytea", Vec<u8>);
        name_string("name", String, string_regex("[a-zA-Z0-9]{1,63}").unwrap());
        // TODO(fran): Add numeric with precision and scale when we start correctly
        //  handling them.
        numeric_decimal("numeric", Decimal, arbitrary_decimal());
        decimal("decimal", Decimal, arbitrary_decimal());
        timestamp_systemtime("timestamp", std::time::SystemTime, arbitrary_systemtime());
        inet_ipaddr("inet", IpInet, arbitrary_ipinet());
        macaddr_string("macaddr", MacAddress, arbitrary_mac_address());
        uuid_string("uuid", Uuid, arbitrary_uuid());
        date_naivedate("date", chrono::NaiveDate, arbitrary_naive_date());
        time_naivetime("time", chrono::NaiveTime, arbitrary_naive_time());
        json_string("json", serde_json::Value, arbitrary_json());
        jsonb_string("jsonb", serde_json::Value, arbitrary_json_without_f64());
        bit_bitvec("bit", bit_vec::BitVec, arbitrary_bitvec(1..=1));
        bit_sized_bitvec("bit(20)", bit_vec::BitVec, arbitrary_bitvec(20..=20));
        varbit_unlimited_bitvec("varbit", bit_vec::BitVec, arbitrary_bitvec(0..=20));
        varbit_bitvec("varbit(10)", bit_vec::BitVec, arbitrary_bitvec(0..=10));
        bit_varying_unlimited_bitvec("bit varying", bit_vec::BitVec, arbitrary_bitvec(0..=20));
        bit_varying_bitvec("bit varying(10)", bit_vec::BitVec, arbitrary_bitvec(0..=10));
        timestamp_tz_datetime("timestamp with time zone", chrono::DateTime::<chrono::FixedOffset>, arbitrary_date_time());
        text_array("text[]", Vec<String>);
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial_test::serial]
    async fn regclass() {
        let (config, _handle, shutdown_tx) = setup().await;
        let client = connect(config).await;

        client
            .simple_query("CREATE TABLE t (id int primary key)")
            .await
            .unwrap();

        client
            .query_one("SELECT 't'::regclass", &[])
            .await
            .unwrap()
            .get::<_, DfValue>(0);

        shutdown_tx.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial_test::serial]
    async fn regproc() {
        let (upstream, upstream_conn) = upstream_config()
            .dbname("postgres")
            .connect(NoTls)
            .await
            .unwrap();
        tokio::spawn(upstream_conn);

        let upstream_res: DfValue = upstream
            .query_one("SELECT typinput FROM pg_type WHERE typname = 'bool'", &[])
            .await
            .unwrap()
            .get(0);

        let (config, _handle, shutdown_tx) = setup().await;
        let client = connect(config).await;

        let typinput: DfValue = client
            .query_one("SELECT typinput FROM pg_type WHERE typname = 'bool'", &[])
            .await
            .unwrap()
            .get(0);
        assert_eq!(typinput, upstream_res);

        let typname: String = client
            .query_one(
                "SELECT typname FROM pg_type WHERE typinput = $1",
                &[&typinput],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(typname, "bool");

        shutdown_tx.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial_test::serial]
    async fn enums() {
        readyset_tracing::init_test_logging();
        let (config, _handle, shutdown_tx) = setup().await;
        let client = connect(config.clone()).await;

        info!("Creating type abc");
        client
            .simple_query("CREATE TYPE abc AS ENUM ('a', 'b', 'c');")
            .await
            .unwrap();

        #[derive(Debug, PartialEq, Eq, PartialOrd, Ord, ToSql, FromSql)]
        #[postgres(name = "abc")]
        enum Abc {
            #[postgres(name = "a")]
            A,
            #[postgres(name = "b")]
            B,
            #[postgres(name = "c")]
            C,
        }
        use Abc::*;

        info!("Creating table t");
        client
            .simple_query("CREATE TABLE t (id serial primary key, x abc);")
            .await
            .unwrap();

        client
            .simple_query("INSERT INTO t (x) VALUES ('b'), ('c'), ('a'), ('a')")
            .await
            .unwrap();

        info!("Creating table t_nulls");
        client
            .simple_query("CREATE TABLE t_nulls (x abc);")
            .await
            .unwrap();

        client
            .simple_query("INSERT INTO t_nulls (x) VALUES ('b'), ('c'), ('a'), (null)")
            .await
            .unwrap();

        info!("Creating table t_s");
        client
            .simple_query("CREATE TABLE t_s (x text);")
            .await
            .unwrap();

        client
            .simple_query("INSERT INTO t_s (x) VALUES ('b'), ('c'), ('a'), ('a')")
            .await
            .unwrap();

        info!("Creating table t_arr");
        client
            .simple_query("CREATE TABLE t_arr (x abc[])")
            .await
            .unwrap();

        client
            .simple_query("INSERT INTO t_arr (x) VALUES (ARRAY['a'::abc, 'b'::abc, 'c'::abc]);")
            .await
            .unwrap();

        info!("going to eventually loop now");
        eventually!(
            run_test: {
                let mut project_eq_res = client
                    .query("SELECT x = 'a' FROM t", &[])
                    .await
                    .unwrap()
                    .into_iter()
                    .map(|r| r.get(0))
                    .collect::<Vec<bool>>();
                project_eq_res.sort();
                let dest = last_query_info(&client).await.destination;
                (project_eq_res, dest)
            },
            then_assert: |(project_eq_res, dest)| {
                assert_eq!(project_eq_res, vec![false, false, true, true]);
                assert_eq!(dest, QueryDestination::Readyset);
            }
        );

        shutdown_tx.shutdown().await;
    }
}
