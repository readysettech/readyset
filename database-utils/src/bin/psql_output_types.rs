use std::collections::HashMap;
use std::iter::zip;

const PSQL_TYPES: &[&str] = &[
    "smallint",
    "integer",
    "bigint",
    "real",
    "double precision",
    "numeric",
];
const PSQL_OPS: &[&str] = &["+", "-", "*", "/", "%"];
const READYSET_TYPES: &[&str] = &[
    "SmallInt",
    "Int",
    "BigInt",
    "Float",
    "Double",
    "DEFAULT_NUMERIC",
];
const READYSET_OPS: &[&str] = &["Add", "Subtract", "Multiply", "Divide", "Modulo"];

async fn test(
    client: &tokio_postgres::Client,
    map: &HashMap<&str, &str>,
    a: usize,
    op: usize,
    b: usize,
) -> Option<String> {
    client
        .simple_query("drop table if exists t1, t2, t3")
        .await
        .unwrap();

    client
        .simple_query(&format!("create table t1 (a {})", PSQL_TYPES[a]))
        .await
        .unwrap();
    client
        .simple_query(&format!("create table t2 (b {})", PSQL_TYPES[b],))
        .await
        .unwrap();

    let out: String = if (client
        .simple_query(&format!(
            "create table t3 as select a {} b as c from t1, t2",
            PSQL_OPS[op]
        ))
        .await)
        .is_ok()
    {
        let rows = client
            .query(
                "select pg_catalog.format_type(a.atttypid, a.atttypmod)
            from pg_catalog.pg_attribute a
            where a.attrelid = (select oid from pg_class where relname = 't3') and a.attnum > 0",
                &[],
            )
            .await
            .unwrap();
        let ty: String = rows[0].get(0);

        if let Some(ty) = map.get(ty.as_str()) {
            ty.to_string()
        } else {
            panic!("unknown type: {}", ty);
        }
    } else {
        "Unknown".to_string()
    };

    Some(format!(
        "(DfType::{}, BinaryOperator::{}, DfType::{}), DfType::{}",
        READYSET_TYPES[a], READYSET_OPS[op], READYSET_TYPES[b], out,
    ))
}

#[tokio::main]
async fn main() {
    let mut map = HashMap::new();
    for (&my, &rs) in zip(PSQL_TYPES, READYSET_TYPES) {
        map.insert(my, rs);
    }

    let mut config = tokio_postgres::Config::new();
    config.host("127.0.0.1").user("postgres").password("noria");
    let (client1, conn) = config.connect(tokio_postgres::NoTls).await.unwrap();
    tokio::spawn(conn);

    client1
        .simple_query("drop database if exists rstest")
        .await
        .unwrap();
    client1
        .simple_query("create database rstest")
        .await
        .unwrap();

    config.dbname("rstest");
    let (client2, conn) = config.connect(tokio_postgres::NoTls).await.unwrap();
    tokio::spawn(conn);

    // no attempt at formatting; use of cargo fmt is expected
    println!("////////////////////////////////////////////////////////////////////////////////");
    println!("//");
    println!("//             THIS FILE IS MACHINE-GENERATED!!!  DO NOT EDIT!!!");
    println!("//");
    println!("// To regenerate this file:");
    println!("//");
    println!("// cargo run -p database-utils --bin psql_output_types > \\");
    println!("//     dataflow-expression/src/promotion/psql.rs");
    println!("// cargo fmt");
    println!("//");
    println!("////////////////////////////////////////////////////////////////////////////////");
    println!();
    println!("use std::collections::HashMap;");
    println!("use std::sync::OnceLock;");
    println!("use crate::DfType;");
    println!("use crate::BinaryOperator;");
    println!();
    println!(
        "pub(crate) fn output_type(left: &DfType, op: &BinaryOperator, right: &DfType) -> \
        Option<DfType> {{"
    );
    println!("static MAP: OnceLock<HashMap<(DfType, BinaryOperator, DfType), DfType>> = ");
    println!("OnceLock::new();");
    println!("let map = MAP.get_or_init(build_map);");
    println!("map.get(&(left.clone(), *op, right.clone())).cloned()");
    println!("}}");
    println!();
    println!("fn build_map() -> HashMap<(DfType, BinaryOperator, DfType), DfType> {{");
    println!("let mut map = HashMap::new();");

    for a in 0..PSQL_TYPES.len() {
        for b in 0..PSQL_TYPES.len() {
            for op in 0..PSQL_OPS.len() {
                if let Some(args) = test(&client2, &map, a, op, b).await {
                    println!("map.insert({});", args);
                }
            }
        }
    }

    println!("map");
    println!("}}");

    drop(client2);
    client1.simple_query("drop database rstest").await.unwrap();
}
