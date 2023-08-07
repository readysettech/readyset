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
    use std::panic::RefUnwindSafe;
    use std::time::Duration;

    use cidr::IpInet;
    use eui48::MacAddress;
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
    use uuid::Uuid;

    use super::*;

    async fn test_type_roundtrip<T, V>(type_name: T, val: V)
    where
        T: Display,
        V: ToSql + Sync + PartialEq + RefUnwindSafe,
        for<'a> V: FromSql<'a>,
    {
        let (config, _handle, shutdown_tx) = setup().await;
        let mut client = connect(config).await;

        client
            .simple_query(&format!("CREATE TABLE t (x {})", type_name))
            .await
            .unwrap();

        // check writes (going to fallback)
        client
            .execute("INSERT INTO t (x) VALUES ($1)", &[&val])
            .await
            .unwrap();

        // check values coming out of noria
        eventually!(run_test: {
            client.query("SELECT * FROM t", &[])
                .await
                .unwrap()
                .iter()
                .map(|row| row.get::<_, V>(0))
                .collect::<Vec<_>>()
        }, then_assert: |star_results| {
            assert_eq!(star_results.len(), 1);
            assert_eq!(star_results[0], val);
        });

        // check parameter parsing
        if type_name.to_string().as_str() != "json" {
            let count_where_result = client
                .query_one(
                    format!("SELECT count(*) FROM t WHERE x = cast($1 as {type_name})").as_str(),
                    &[&val],
                )
                .await
                .unwrap()
                .get::<_, i64>(0);
            assert_eq!(count_where_result, 1);
        }

        // Can't compare JSON for equality in postgres
        if type_name.to_string() != "json" {
            // check parameter passing and value returning when going through fallback

            let fallback_result = client
                .transaction()
                .await
                .unwrap()
                .query_one("SELECT x FROM t WHERE x = $1", &[&val])
                .await
                .unwrap()
                .get::<_, V>(0);
            assert_eq!(fallback_result, val);
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
                ..ProptestConfig::default()
            })]
            #[serial_test::serial]
            $(#[$meta])*
            fn $test_name(#[strategy($strategy)] val: $rust_type) {
                readyset_tracing::init_test_logging();
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
        char_i8("\"char\"", i8);
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

        client
            .simple_query("CREATE TABLE t (id serial primary key, x abc);")
            .await
            .unwrap();

        client
            .simple_query("INSERT INTO t (x) VALUES ('b'), ('c'), ('a'), ('a')")
            .await
            .unwrap();

        client
            .simple_query("CREATE TABLE t_nulls (x abc);")
            .await
            .unwrap();

        client
            .simple_query("INSERT INTO t_nulls (x) VALUES ('b'), ('c'), ('a'), (null)")
            .await
            .unwrap();

        client
            .simple_query("CREATE TABLE t_s (x text);")
            .await
            .unwrap();

        client
            .simple_query("INSERT INTO t_s (x) VALUES ('b'), ('c'), ('a'), ('a')")
            .await
            .unwrap();

        client
            .simple_query("CREATE TABLE t_arr (x abc[])")
            .await
            .unwrap();

        client
            .simple_query("INSERT INTO t_arr (x) VALUES (ARRAY['a'::abc, 'b'::abc, 'c'::abc]);")
            .await
            .unwrap();

        sleep().await;

        let mut project_eq_res = client
            .query("SELECT x = 'a' FROM t", &[])
            .await
            .unwrap()
            .into_iter()
            .map(|r| r.get(0))
            .collect::<Vec<bool>>();
        project_eq_res.sort();
        assert_eq!(project_eq_res, vec![false, false, true, true]);

        assert_eq!(
            last_query_info(&client).await.destination,
            QueryDestination::Readyset
        );

        let where_eq_res: i64 = client
            .query_one(
                "WITH a AS (SELECT COUNT(*) AS c FROM t WHERE x = 'a') SELECT c FROM a",
                &[],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(where_eq_res, 2);

        assert_eq!(
            last_query_info(&client).await.destination,
            QueryDestination::Readyset
        );

        assert_eq!(
            last_query_info(&client).await.destination,
            QueryDestination::Readyset
        );

        let upquery_res = client
            .query("SELECT x FROM t WHERE x = $1", &[&B])
            .await
            .unwrap()
            .into_iter()
            .map(|r| r.get(0))
            .collect::<Vec<Abc>>();
        assert_eq!(upquery_res, vec![B]);

        assert_eq!(
            last_query_info(&client).await.destination,
            QueryDestination::Readyset
        );

        let mut nulls_res = client
            .query("SELECT x FROM t_nulls", &[])
            .await
            .unwrap()
            .into_iter()
            .map(|r| r.get(0))
            .collect::<Vec<Option<Abc>>>();
        nulls_res.sort();
        assert_eq!(nulls_res, vec![None, Some(A), Some(B), Some(C)]);

        assert_eq!(
            last_query_info(&client).await.destination,
            QueryDestination::Readyset
        );

        let sort_res = client
            .query("SELECT x FROM t ORDER BY x ASC", &[])
            .await
            .unwrap()
            .into_iter()
            .map(|r| r.get(0))
            .collect::<Vec<Abc>>();
        assert_eq!(sort_res, vec![A, A, B, C]);

        assert_eq!(
            last_query_info(&client).await.destination,
            QueryDestination::Readyset
        );

        let mut range_res = client
            .query("SELECT x FROM t WHERE x >= $1", &[&B])
            .await
            .unwrap()
            .into_iter()
            .map(|r| r.get(0))
            .collect::<Vec<Abc>>();
        range_res.sort();
        assert_eq!(range_res, vec![B, C]);

        assert_eq!(
            last_query_info(&client).await.destination,
            QueryDestination::Readyset
        );

        let mut cast_res = client
            .query("select cast(x as abc) from t_s", &[])
            .await
            .unwrap()
            .into_iter()
            .map(|r| r.get(0))
            .collect::<Vec<Abc>>();
        cast_res.sort();
        assert_eq!(cast_res, vec![A, A, B, C]);

        assert_eq!(
            last_query_info(&client).await.destination,
            QueryDestination::Readyset
        );

        let array_res = client
            .query_one("SELECT x FROM t_arr", &[])
            .await
            .unwrap()
            .get::<_, Vec<Abc>>(0);
        assert_eq!(array_res, vec![A, B, C]);

        assert_eq!(
            last_query_info(&client).await.destination,
            QueryDestination::Readyset
        );

        let proxied_parameter_res = client
            .query_one(
                "SELECT COUNT(*) FROM (SELECT * FROM t WHERE x = $1) sq",
                &[&A],
            )
            .await
            .unwrap()
            .get::<_, i64>(0);
        assert_eq!(proxied_parameter_res, 2);

        let proxied_array_parameter_res = client
            .query_one(
                "SELECT COUNT(*) FROM (SELECT * FROM t WHERE x = ANY($1)) sq",
                &[&vec![A, B]],
            )
            .await
            .unwrap()
            .get::<_, i64>(0);
        assert_eq!(proxied_array_parameter_res, 3);

        client
            .simple_query("UPDATE t SET x = 'c' WHERE id = 1")
            .await
            .unwrap();

        let post_update_res = client
            .query("SELECT x FROM t ORDER BY x ASC", &[])
            .await
            .unwrap()
            .into_iter()
            .map(|r| r.get(0))
            .collect::<Vec<Abc>>();
        assert_eq!(post_update_res, vec![A, A, C, C]);

        client
            .simple_query("UPDATE t SET x = 'b' WHERE id = 1")
            .await
            .unwrap();

        client
            .simple_query("ALTER TYPE abc ADD VALUE 'd'")
            .await
            .unwrap();

        client
            .simple_query("INSERT INTO t (x) VALUES ('d')")
            .await
            .unwrap();

        client
            .simple_query("INSERT INTO t_s (x) VALUES ('d')")
            .await
            .unwrap();

        #[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
        enum Abcd {
            A,
            B,
            C,
            D,
        }

        impl<'a> FromSql<'a> for Abcd {
            fn from_sql(
                _ty: &postgres_types::Type,
                raw: &'a [u8],
            ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
                match raw {
                    b"a" => Ok(Self::A),
                    b"b" => Ok(Self::B),
                    b"c" => Ok(Self::C),
                    b"d" => Ok(Self::D),
                    r => Err(format!("Unknown variant: '{}'", String::from_utf8_lossy(r)).into()),
                }
            }

            fn accepts(ty: &postgres_types::Type) -> bool {
                ty.name() == "abc"
            }
        }

        sleep().await;

        client
            .simple_query("CREATE CACHE FROM SELECT x FROM t ORDER BY x ASC")
            .await
            .unwrap();
        let _ = client.query("SELECT x FROM t ORDER BY x ASC", &[]).await;

        let sort_res = client
            .query("SELECT x FROM t ORDER BY x ASC", &[])
            .await
            .unwrap()
            .into_iter()
            .map(|r| r.get(0))
            .collect::<Vec<Abcd>>();
        assert_eq!(sort_res, vec![Abcd::A, Abcd::A, Abcd::B, Abcd::C, Abcd::D]);

        client
            .simple_query("CREATE CACHE FROM select cast(x as abc) from t_s")
            .await
            .unwrap();

        let _ = client.query("select cast(x as abc) from t_s", &[]).await;

        let mut cast_res = client
            .query("select cast(x as abc) from t_s", &[])
            .await
            .unwrap()
            .into_iter()
            .map(|r| r.get(0))
            .collect::<Vec<Abcd>>();
        cast_res.sort();
        assert_eq!(cast_res, vec![Abcd::A, Abcd::A, Abcd::B, Abcd::C, Abcd::D]);

        assert_eq!(
            last_query_info(&client).await.destination,
            QueryDestination::Readyset
        );

        client.simple_query("DROP TABLE t;").await.unwrap();
        client.simple_query("DROP TYPE abc CASCADE").await.unwrap();

        client
            .simple_query("CREATE TYPE abc AS ENUM ('c', 'b', 'a');")
            .await
            .unwrap();

        client
            .simple_query("CREATE TABLE t (x abc);")
            .await
            .unwrap();

        client
            .simple_query("INSERT INTO t (x) VALUES ('b'), ('c'), ('a'), ('a')")
            .await
            .unwrap();

        sleep().await;

        client
            .simple_query("CREATE CACHE FROM SELECT x FROM t ORDER BY x ASC")
            .await
            .unwrap();
        let _ = client.query("SELECT x FROM t ORDER BY x ASC", &[]).await;

        let sort_res = client
            .query("SELECT x FROM t ORDER BY x ASC", &[])
            .await
            .unwrap()
            .into_iter()
            .map(|r| r.get(0))
            .collect::<Vec<Abc>>();
        assert_eq!(sort_res, vec![C, B, A, A]);

        assert_eq!(
            last_query_info(&client).await.destination,
            QueryDestination::Readyset
        );

        // Rename the type

        client
            .simple_query("ALTER TYPE abc RENAME TO cba")
            .await
            .unwrap();
        client
            .simple_query("CREATE TABLE t2 (x cba)")
            .await
            .unwrap();
        client
            .simple_query("INSERT INTO t2 (x) VALUES ('c')")
            .await
            .unwrap();

        sleep().await;

        // tokio-postgres appears to maintain a cache of oid -> custom type, so we have to reconnect
        // to force it to see any changes we're making to our type.
        let client = connect(config.clone()).await;

        #[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
        enum Cba {
            A,
            B,
            C,
        }

        impl<'a> FromSql<'a> for Cba {
            fn from_sql(
                _ty: &postgres_types::Type,
                raw: &'a [u8],
            ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
                match raw {
                    b"a" => Ok(Self::A),
                    b"b" => Ok(Self::B),
                    b"c" => Ok(Self::C),
                    r => Err(format!("Unknown variant: '{}'", String::from_utf8_lossy(r)).into()),
                }
            }

            fn accepts(ty: &postgres_types::Type) -> bool {
                ty.name() == "cba"
            }
        }

        client
            .simple_query("CREATE CACHE FROM select x from t2")
            .await
            .unwrap();

        let post_rename_res = client
            .query("SELECT x FROM t2", &[])
            .await
            .unwrap()
            .into_iter()
            .map(|r| r.get(0))
            .collect::<Vec<Cba>>();
        assert_eq!(post_rename_res, vec![Cba::C]);

        assert_eq!(
            last_query_info(&client).await.destination,
            QueryDestination::Readyset
        );

        // Rename a variant
        client
            .simple_query("ALTER TYPE cba RENAME VALUE 'c' TO 'c2'")
            .await
            .unwrap();

        sleep().await;

        // Reconnect as before
        let client = connect(config).await;

        #[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
        enum Cba2 {
            A,
            B,
            C,
        }

        impl<'a> FromSql<'a> for Cba2 {
            fn from_sql(
                _ty: &postgres_types::Type,
                raw: &'a [u8],
            ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
                match raw {
                    b"a" => Ok(Self::A),
                    b"b" => Ok(Self::B),
                    b"c2" => Ok(Self::C),
                    r => Err(format!("Unknown variant: '{}'", String::from_utf8_lossy(r)).into()),
                }
            }

            fn accepts(ty: &postgres_types::Type) -> bool {
                ty.name() == "cba"
            }
        }

        client
            .simple_query("CREATE CACHE FROM select x from t2")
            .await
            .unwrap();

        let post_rename_res = client
            .query("SELECT x FROM t2", &[])
            .await
            .unwrap()
            .into_iter()
            .map(|r| r.get(0))
            .collect::<Vec<Cba2>>();
        assert_eq!(post_rename_res, vec![Cba2::C]);

        assert_eq!(
            last_query_info(&client).await.destination,
            QueryDestination::Readyset
        );

        shutdown_tx.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial_test::serial]
    async fn enum_as_pk() {
        readyset_tracing::init_test_logging();
        let (config, _handle, shutdown_tx) = setup().await;
        let client = connect(config.clone()).await;

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

        client
            .simple_query("CREATE TABLE t (id abc primary key, x abc);")
            .await
            .unwrap();

        client
            .simple_query("INSERT INTO t (id, x) VALUES ('a', 'a'), ('b', 'c')")
            .await
            .unwrap();

        sleep().await;

        let res = client
            .query_one("SELECT id, x FROM t WHERE id = $1", &[&B])
            .await
            .unwrap();
        assert_eq!(res.get::<_, Abc>(0), B);
        assert_eq!(res.get::<_, Abc>(1), C);

        client
            .simple_query("UPDATE t SET x = 'b' WHERE id = 'b'")
            .await
            .unwrap();

        sleep().await;

        let res = client
            .query_one("SELECT id, x FROM t WHERE id = $1", &[&B])
            .await
            .unwrap();
        assert_eq!(res.get::<_, Abc>(0), B);
        assert_eq!(res.get::<_, Abc>(1), B);

        client
            .simple_query("DELETE FROM t WHERE id = 'b'")
            .await
            .unwrap();

        sleep().await;

        let res = client
            .query("SELECT id, x FROM t WHERE id = $1", &[&B])
            .await
            .unwrap();
        assert!(res.is_empty());

        shutdown_tx.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial_test::serial]
    async fn alter_enum_complex_variant_changes() {
        readyset_tracing::init_test_logging();
        let (config, _handle, shutdown_tx) = setup().await;
        let client = connect(config.clone()).await;

        client
            .simple_query("CREATE TYPE abc AS ENUM ('a', 'b', 'c');")
            .await
            .unwrap();

        client
            .simple_query("CREATE TABLE t (x abc);")
            .await
            .unwrap();

        client
            .simple_query("INSERT INTO t (x) VALUES ('b'), ('c'), ('a'), ('a')")
            .await
            .unwrap();

        // Add a new variant in the middle of the list
        client
            .simple_query("ALTER TYPE abc ADD VALUE 'ab' BEFORE 'b'")
            .await
            .unwrap();

        #[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
        enum Abc {
            A,
            AB,
            B,
            C,
        }

        impl<'a> FromSql<'a> for Abc {
            fn from_sql(
                _ty: &postgres_types::Type,
                raw: &'a [u8],
            ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
                match raw {
                    b"a" => Ok(Self::A),
                    b"ab" => Ok(Self::AB),
                    b"b" => Ok(Self::B),
                    b"c" => Ok(Self::C),
                    r => Err(format!("Unknown variant: '{}'", String::from_utf8_lossy(r)).into()),
                }
            }

            fn accepts(ty: &postgres_types::Type) -> bool {
                ty.name() == "abc"
            }
        }

        client
            .simple_query("INSERT INTO t (x) VALUES ('ab')")
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_secs(5)).await;

        let post_insert_value_res = client
            .query("SELECT x FROM t ORDER BY x ASC", &[])
            .await
            .unwrap()
            .into_iter()
            .map(|r| r.get(0))
            .collect::<Vec<Abc>>();

        assert_eq!(
            last_query_info(&client).await.destination,
            QueryDestination::Readyset
        );

        assert_eq!(
            post_insert_value_res,
            vec![Abc::A, Abc::A, Abc::AB, Abc::B, Abc::C]
        );

        shutdown_tx.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial_test::serial]
    async fn citext() {
        readyset_tracing::init_test_logging();
        let (config, _handle, shutdown_tx) = setup().await;
        let client = connect(config.clone()).await;

        client
            .simple_query("CREATE EXTENSION IF NOT EXISTS citext")
            .await
            .unwrap();

        client
            .simple_query("CREATE TABLE t (x citext);")
            .await
            .unwrap();

        client
            .simple_query("INSERT INTO t (x) VALUES ('a');")
            .await
            .unwrap();

        let stmt = client
            .prepare("SELECT * FROM t WHERE x = $1")
            .await
            .unwrap();
        let res = client.query_one(&stmt, &[&"A"]).await.unwrap();
        assert_eq!(res.get::<_, String>(0), "a");

        assert_eq!(
            last_query_info(&client).await.destination,
            QueryDestination::Readyset
        );

        shutdown_tx.shutdown().await;
    }
}
