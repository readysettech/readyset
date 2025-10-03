use readyset_client_test_helpers::TestBuilder;
use readyset_client_test_helpers::psql_helpers::PostgreSQLAdapter;
use readyset_server::{DurabilityMode, Handle};
use readyset_util::shutdown::ShutdownSender;
use test_utils::tags;

mod common;

async fn setup() -> (tokio_postgres::Config, Handle, ShutdownSender) {
    TestBuilder::default()
        .fallback(true)
        // TODO(mvzink): Switch to DeleteOnExit after fixing REA-5757
        .durability_mode(DurabilityMode::MemoryOnly)
        .build::<PostgreSQLAdapter>()
        .await
}

mod types {
    use std::fmt::Display;
    use std::panic::{AssertUnwindSafe, RefUnwindSafe};
    use std::time::Duration;

    use assert_matches::assert_matches;
    use cidr::IpInet;
    use eui48::MacAddress;
    use pretty_assertions::assert_eq;
    use proptest::collection::vec;
    use proptest::prelude::*;
    use proptest::string::string_regex;
    use readyset_adapter::backend::QueryDestination;
    use readyset_client_test_helpers::psql_helpers::{connect, last_query_info, upstream_config};
    use readyset_client_test_helpers::{Adapter, sleep};
    use readyset_data::DfValue;
    use readyset_decimal::Decimal;
    use readyset_util::arbitrary::{
        arbitrary_bitvec, arbitrary_date_time, arbitrary_decimal, arbitrary_ipinet, arbitrary_json,
        arbitrary_json_without_f64, arbitrary_mac_address, arbitrary_naive_date,
        arbitrary_naive_time, arbitrary_systemtime, arbitrary_uuid,
    };
    use readyset_util::eventually;
    use tokio_postgres::types::{FromSql, ToSql};
    use tokio_postgres::{NoTls, SimpleQueryMessage};
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
            .simple_query(&format!("CREATE TABLE t (id int, x {type_name})"))
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
                    .first()
                    .unwrap_or_else(|| panic!("Should have at least one value equal to {v:?}"))
                    .get::<_, V>(0);
                assert_eq!(fallback_result, *v);
            }
        }

        shutdown_tx.shutdown().await;
    }

    macro_rules! test_types {
        ($($(#[$meta:meta])* $test_name:ident($pg_type_name: expr_2021, $rust_type: ty $(, $strategy:expr_2021)?);)+) => {
            $(test_types!(@impl, $(#[$meta])* $test_name, $pg_type_name, $rust_type $(, $strategy)?);)+
        };

        (@impl, $(#[$meta:meta])* $test_name: ident, $pg_type_name: expr_2021, $rust_type: ty) => {
            test_types!(@impl, $(#[$meta])* $test_name, $pg_type_name, $rust_type, any::<$rust_type>());
        };

        (@impl, $(#[$meta:meta])* $test_name: ident, $pg_type_name: expr_2021, $rust_type: ty, $strategy: expr_2021) => {
            #[tags(serial, slow, no_retry, postgres_upstream)]
            // these are pretty slow, so we only run a few cases at a time
            #[test_strategy::proptest(ProptestConfig {
                cases: 5,
                max_shrink_iters: 200, // May need many shrink iters for large vecs
                ..ProptestConfig::default()
            })]
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
        // TODO(fran): Add numeric with precision and scale when we start correctly handling them.
        // XXX(mvzink): We could generate much larger decimals, but they quickly take too long to
        // de/serialize (e.g. reaching 1s for a 50k digit value). See comment in `Decimal::from_sql`
        numeric_decimal("numeric", Decimal, arbitrary_decimal(1000, 255));
        decimal("decimal", Decimal, arbitrary_decimal(1000, 255));
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
    #[tags(serial, postgres_upstream)]
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
    #[tags(serial, postgres_upstream)]
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
    #[tags(serial, postgres_upstream)]
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
            .simple_query("CREATE TABLE enumt (id serial primary key, x abc);")
            .await
            .unwrap();

        client
            .simple_query("INSERT INTO enumt (x) VALUES ('b'), ('c'), ('a'), ('a')")
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

        eventually!(
            run_test: {
                let mut project_eq_res = client
                    .query("SELECT x = 'a' FROM enumt", &[])
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
                assert_matches!(dest, QueryDestination::Readyset(_));
            }
        );

        let where_eq_res: i64 = client
            .query_one(
                "WITH a AS (SELECT COUNT(*) AS c FROM enumt WHERE x = 'a') SELECT c FROM a",
                &[],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(where_eq_res, 2);

        assert_matches!(
            last_query_info(&client).await.destination,
            QueryDestination::Readyset(_)
        );

        assert_matches!(
            last_query_info(&client).await.destination,
            QueryDestination::Readyset(_)
        );

        let upquery_res = client
            .query("SELECT x FROM enumt WHERE x = $1", &[&B])
            .await
            .unwrap()
            .into_iter()
            .map(|r| r.get(0))
            .collect::<Vec<Abc>>();
        assert_eq!(upquery_res, vec![B]);

        assert_matches!(
            last_query_info(&client).await.destination,
            QueryDestination::Readyset(_)
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

        assert_matches!(
            last_query_info(&client).await.destination,
            QueryDestination::Readyset(_)
        );

        let sort_res = client
            .query("SELECT x FROM enumt ORDER BY x ASC", &[])
            .await
            .unwrap()
            .into_iter()
            .map(|r| r.get(0))
            .collect::<Vec<Abc>>();
        assert_eq!(sort_res, vec![A, A, B, C]);

        assert_matches!(
            last_query_info(&client).await.destination,
            QueryDestination::Readyset(_)
        );

        let mut range_res = client
            .query("SELECT x FROM enumt WHERE x >= $1", &[&B])
            .await
            .unwrap()
            .into_iter()
            .map(|r| r.get(0))
            .collect::<Vec<Abc>>();
        range_res.sort();
        assert_eq!(range_res, vec![B, C]);

        assert_matches!(
            last_query_info(&client).await.destination,
            QueryDestination::Readyset(_)
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

        assert_matches!(
            last_query_info(&client).await.destination,
            QueryDestination::Readyset(_)
        );

        let array_res = client
            .query_one("SELECT x FROM t_arr", &[])
            .await
            .unwrap()
            .get::<_, Vec<Abc>>(0);
        assert_eq!(array_res, vec![A, B, C]);

        assert_matches!(
            last_query_info(&client).await.destination,
            QueryDestination::Readyset(_)
        );

        let proxied_parameter_res = client
            .query_one(
                "SELECT COUNT(*) FROM (SELECT * FROM enumt WHERE x = $1) sq",
                &[&A],
            )
            .await
            .unwrap()
            .get::<_, i64>(0);
        assert_eq!(proxied_parameter_res, 2);

        let proxied_array_parameter_res = client
            .query_one(
                "SELECT COUNT(*) FROM (SELECT * FROM enumt WHERE x = ANY($1)) sq",
                &[&vec![A, B]],
            )
            .await
            .unwrap()
            .get::<_, i64>(0);
        assert_eq!(proxied_array_parameter_res, 3);

        client
            .simple_query("UPDATE enumt SET x = 'c' WHERE id = 1")
            .await
            .unwrap();

        eventually!(
            run_test: {
                client
                    .query("SELECT x FROM enumt ORDER BY x ASC", &[])
                    .await
                    .unwrap()
                    .into_iter()
                    .map(|r| r.get(0))
                    .collect::<Vec<Abc>>()
            },
            then_assert: |post_update_res| {
                assert_eq!(post_update_res, vec![A, A, C, C]);
            }
        );

        client
            .simple_query("UPDATE enumt SET x = 'b' WHERE id = 1")
            .await
            .unwrap();

        client
            .simple_query("ALTER TYPE abc ADD VALUE 'd'")
            .await
            .unwrap();

        client
            .simple_query("INSERT INTO enumt (x) VALUES ('d')")
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
            .simple_query("CREATE CACHE FROM SELECT x FROM enumt ORDER BY x ASC")
            .await
            .unwrap();
        let _ = client
            .query("SELECT x FROM enumt ORDER BY x ASC", &[])
            .await;

        let sort_res = client
            .query("SELECT x FROM enumt ORDER BY x ASC", &[])
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

        assert_matches!(
            last_query_info(&client).await.destination,
            QueryDestination::Readyset(_)
        );

        client
            .simple_query("CREATE TYPE abc_rev AS ENUM ('c', 'b', 'a');")
            .await
            .unwrap();

        client
            .simple_query("CREATE TABLE enumt2 (x abc_rev);")
            .await
            .unwrap();

        client
            .simple_query("INSERT INTO enumt2 (x) VALUES ('b'), ('c'), ('a'), ('a')")
            .await
            .unwrap();

        sleep().await;

        #[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
        enum AbcRev {
            C,
            B,
            A,
        }

        impl<'a> FromSql<'a> for AbcRev {
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
                ty.name() == "abc_rev"
            }
        }

        client
            .simple_query("CREATE CACHE FROM SELECT x FROM enumt2 ORDER BY x ASC")
            .await
            .unwrap();
        let _ = client
            .query("SELECT x FROM enumt2 ORDER BY x ASC", &[])
            .await;

        eventually!(
            run_test: {
                client
                    .query("SELECT x FROM enumt2 ORDER BY x ASC", &[])
                    .await
                    .unwrap()
                    .into_iter()
                    .map(|r| r.get(0))
                    .collect::<Vec<AbcRev>>()
            },
            then_assert: |sort_res| {
                assert_eq!(sort_res, vec![AbcRev::C, AbcRev::B, AbcRev::A, AbcRev::A]);
            }
        );

        assert_matches!(
            last_query_info(&client).await.destination,
            QueryDestination::Readyset(_)
        );

        // Rename the type

        client
            .simple_query("ALTER TYPE abc_rev RENAME TO cba")
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

        assert_matches!(
            last_query_info(&client).await.destination,
            QueryDestination::Readyset(_)
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

        eventually!(
            run_test: {
                let post_rename_res = client
                    .query("SELECT x FROM t2", &[])
                    .await
                    .unwrap()
                    .into_iter()
                    .map(|r| r.get(0))
                    .collect::<Vec<Cba2>>();
                let dest = last_query_info(&client).await.destination;

                (post_rename_res, dest)
            },
            then_assert: |(post_rename_res, dest)| {
                assert_eq!(post_rename_res, vec![Cba2::C]);
                assert_matches!(dest, QueryDestination::Readyset(_));
            }
        );

        shutdown_tx.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[tags(serial, postgres_upstream)]
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

        eventually!(
            run_test: {
                let res = client
                    .query_one("SELECT id, x FROM t WHERE id = $1", &[&B])
                    .await
                    .unwrap();
                (res.get::<_, Abc>(0), res.get::<_, Abc>(1))
            },
            then_assert: |res| {
                assert_eq!(res, (B, B))
            }
        );

        client
            .simple_query("DELETE FROM t WHERE id = 'b'")
            .await
            .unwrap();

        sleep().await;

        eventually!(
            run_test: {
                client
                    .query("SELECT id, x FROM t WHERE id = $1", &[&B])
                    .await
                    .unwrap()
                    .into_iter()
                    .map(|row| (row.get::<_, Abc>(0), row.get::<_, Abc>(1)))
                    .collect::<Vec<_>>()
            },
            then_assert: |res| {
                assert_eq!(res, vec![]);
            }
        );

        shutdown_tx.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[tags(serial, postgres_upstream)]
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

        eventually!(
            run_test: {
                let post_insert_value_res = client
                    .query("SELECT x FROM t ORDER BY x ASC", &[])
                    .await
                    .unwrap()
                    .into_iter()
                    .map(|r| r.get(0))
                    .collect::<Vec<Abc>>();
                let dest = last_query_info(&client).await.destination;
                (post_insert_value_res, dest)
            },
            then_assert: |(post_insert_value_res, dest)| {
                assert_matches!(dest, QueryDestination::Readyset(_));
                assert_eq!(
                    post_insert_value_res,
                    vec![Abc::A, Abc::A, Abc::AB, Abc::B, Abc::C]
                );
            }
        );

        shutdown_tx.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[tags(serial, postgres_upstream)]
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

        // Wait until the `CREATE TABLE` has been replicated
        eventually!(run_test: {
            let result = client
                .simple_query("CREATE CACHE FROM SELECT * FROM t WHERE x = $1")
                .await;
            AssertUnwindSafe(move || result)
        }, then_assert: |result| {
            result().unwrap()
        });

        let stmt = client
            .prepare("SELECT * FROM t WHERE x = $1")
            .await
            .unwrap();

        eventually!(
            run_test: {
                let res = client.query_one(&stmt, &[&"A"]).await;
                let dest = last_query_info(&client).await.destination;
                AssertUnwindSafe(move || (res, dest))
            },
            then_assert: |res| {
                let (res, dest) = res();
                assert_eq!(res.unwrap().get::<_, String>(0), "a");
                assert_matches!(dest, QueryDestination::Readyset(_));
            }
        );

        shutdown_tx.shutdown().await;
    }

    // Tests that even if a hole is filled in a reader node before we issue any writes, a
    // subsequent write for the same key still makes it to the reader
    #[tokio::test(flavor = "multi_thread")]
    #[tags(serial, postgres_upstream)]
    async fn citext_read_before_write() {
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

        // Wait until the `CREATE TABLE` has been replicated
        eventually!(run_test: {
            let result = client
                .simple_query("CREATE CACHE FROM SELECT * FROM t WHERE x = $1")
                .await;
            AssertUnwindSafe(move || result)
        }, then_assert: |result| {
            result().unwrap()
        });

        let stmt = client
            .prepare("SELECT * FROM t WHERE x = $1")
            .await
            .unwrap();

        // Fill the hole in the reader with a value of type `TinyText` with a citext collation
        let res = client.query(&stmt, &[&"A"]).await.unwrap();
        assert!(res.is_empty());

        // Insert a value, which causes us to replicate a `TinyText` value that must be coerced
        // to have the citext collation
        client
            .simple_query("INSERT INTO t (x) VALUES ('a');")
            .await
            .unwrap();

        eventually!(
            run_test: {
                let res = client.query_one(&stmt, &[&"A"]).await;
                let dest = last_query_info(&client).await.destination;
                AssertUnwindSafe(move || (res, dest))
            },
            then_assert: |res| {
                let (res, dest) = res();
                assert_eq!(res.unwrap().get::<_, String>(0), "a");
                assert_matches!(dest, QueryDestination::Readyset(_));
            }
        );

        shutdown_tx.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[tags(serial, postgres_upstream)]
    async fn date_only() {
        readyset_tracing::init_test_logging();
        PostgreSQLAdapter::recreate_database("psql_date_only_test").await;
        let mut psql_config = upstream_config();
        psql_config.dbname("psql_date_only_test");
        let psql_client = connect(psql_config).await;

        psql_client
            .simple_query("CREATE TABLE t (x DATE)")
            .await
            .unwrap();
        psql_client
            .simple_query("INSERT INTO t VALUES ('2021-01-01')")
            .await
            .unwrap();

        let (rs_config, _handle, shutdown_tx) = TestBuilder::default()
            .replicate_db("psql_date_only_test".to_string())
            .recreate_database(false)
            .build::<PostgreSQLAdapter>()
            .await;
        let rs_client = connect(rs_config.clone()).await;

        rs_client
            .simple_query("CREATE CACHE FROM SELECT * FROM t WHERE x = $1")
            .await
            .unwrap();

        eventually!(run_test: {
            rs_client.simple_query("SELECT * FROM t WHERE x = '2021-01-01'")
                .await
                .unwrap()
                .iter()
                .filter_map(|message| match message {
                    SimpleQueryMessage::Row(row) => Some(row.get(0).unwrap().to_owned()),
                    _ => None
                })
                .collect::<Vec<_>>()
        }, then_assert: |rs_rows| {
            assert_eq!(rs_rows, vec!["2021-01-01"]);
        });

        psql_client
            .simple_query("INSERT INTO t VALUES ('2021-01-02')")
            .await
            .unwrap();

        eventually!(run_test: {
            rs_client.simple_query("SELECT * FROM t WHERE x = '2021-01-02'")
                .await
                .unwrap()
                .iter()
                .filter_map(|message| match message {
                    SimpleQueryMessage::Row(row) => Some(row.get(0).unwrap().to_owned()),
                    _ => None
                })
                .collect::<Vec<_>>()
        }, then_assert: |rs_rows| {
            assert_eq!(rs_rows, vec!["2021-01-02"]);
        });

        shutdown_tx.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[tags(serial, slow, postgres_upstream)]
    async fn regression_text_array() {
        readyset_tracing::init_test_logging();
        let vals = vec![vec!["0.".to_string()]];
        test_type_roundtrip("text[]", vals).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[tags(serial, slow, postgres_upstream)]
    #[ignore = "REA-5757"]
    async fn regression_large_text_array_multiple_matches() {
        readyset_tracing::init_test_logging();
        let vals = vec![vec!["1".to_string(); 1500]; 15];
        test_type_roundtrip("text[]", vals).await;
    }
}
