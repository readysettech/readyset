use std::collections::HashMap;
use std::iter::zip;

use mysql_async::Value;
use mysql_async::prelude::Queryable;

const MYSQL_TYPES: &[&str] = &["tinyint", "smallint", "mediumint", "int", "bigint"];
const MYSQL_SIGNED: &[&str] = &["", "unsigned"];
const MYSQL_OPS: &[&str] = &["+", "-", "*", "/"];
const READYSET_TYPES: &[&str] = &["TinyInt", "SmallInt", "MediumInt", "Int", "BigInt"];
const READYSET_SIGNED: &[&str] = &["", "Unsigned"];
const READYSET_OPS: &[&str] = &["Add", "Subtract", "Multiply", "Divide"];

async fn test(
    conn: &mut mysql_async::Conn,
    map: &HashMap<&str, &str>,
    a: usize,
    a_sign: usize,
    op: usize,
    b: usize,
    b_sign: usize,
) -> String {
    conn.query_drop("drop table if exists t1, t2, t3")
        .await
        .unwrap();

    conn.query_drop(format!(
        "create table t1 (a {} {})",
        MYSQL_TYPES[a], MYSQL_SIGNED[a_sign]
    ))
    .await
    .unwrap();
    conn.query_drop(format!(
        "create table t2 (b {} {})",
        MYSQL_TYPES[b], MYSQL_SIGNED[b_sign]
    ))
    .await
    .unwrap();
    conn.query_drop(format!(
        "create table t3 as select a {} b as c from t1, t2",
        MYSQL_OPS[op]
    ))
    .await
    .unwrap();

    let row = conn
        .query_first::<mysql_async::Row, _>("show create table t3")
        .await
        .unwrap()
        .unwrap()
        .unwrap();

    let val = &row[1];
    let Value::Bytes(val) = val else {
        panic!("not bytes");
    };
    let val = String::from_utf8(val.clone()).unwrap();
    let val = val.split('\n').nth(1).unwrap(); // get second line
    let mut res = val.trim().split(' ').skip(1); // skip column name
    let ty = res.next().unwrap(); // type; rest is [unsigned] DEFAULT NULL
    let mut unsigned = res.next().unwrap() == "unsigned";

    let out = if let Some(ty) = map.get(ty) {
        ty.to_string()
    } else if ty.starts_with("decimal") {
        unsigned = false; // XXX we have no unsigned numeric
        let ty = &ty[8..ty.len() - 1]; // remove leading "decimal(" and trailing ")"
        let mut ty = ty.split(',');
        let (x, y) = (ty.next().unwrap(), ty.next().unwrap());
        format!("Numeric {{ prec: {x}, scale: {y} }}")
    } else {
        panic!("unknown type: {ty}");
    };

    format!(
        "(DfType::{}{}, BinaryOperator::{}, DfType::{}{}), DfType::{}{}",
        READYSET_SIGNED[a_sign],
        READYSET_TYPES[a],
        READYSET_OPS[op],
        READYSET_SIGNED[b_sign],
        READYSET_TYPES[b],
        READYSET_SIGNED[unsigned as usize],
        out,
    )
}

#[tokio::main]
async fn main() {
    let opts = mysql_async::OptsBuilder::default()
        .ip_or_hostname("127.0.0.1")
        .user(Some("root"))
        .pass(Some("noria"))
        .prefer_socket(false);
    let mut conn = mysql_async::Conn::new(opts).await.unwrap();

    let mut map = HashMap::new();
    for (&my, &rs) in zip(MYSQL_TYPES, READYSET_TYPES) {
        map.insert(my, rs);
    }

    conn.query_drop("create database if not exists rstest")
        .await
        .unwrap();
    conn.query_drop("use rstest").await.unwrap();

    // no attempt at formatting; use of cargo fmt is expected
    println!("////////////////////////////////////////////////////////////////////////////////");
    println!("//");
    println!("//             THIS FILE IS MACHINE-GENERATED!!!  DO NOT EDIT!!!");
    println!("//");
    println!("// To regenerate this file:");
    println!("//");
    println!("// cargo run -p database-utils --bin mysql_output_types > \\");
    println!("//     dataflow-expression/src/promotion/mysql.rs");
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

    for a in 0..MYSQL_TYPES.len() {
        for b in 0..MYSQL_TYPES.len() {
            for op in 0..MYSQL_OPS.len() {
                for a_sign in 0..MYSQL_SIGNED.len() {
                    for b_sign in 0..MYSQL_SIGNED.len() {
                        let args = test(&mut conn, &map, a, a_sign, op, b, b_sign).await;
                        println!("map.insert({args});");
                    }
                }
            }
        }
    }

    println!("map");
    println!("}}");

    conn.query_drop("drop database rstest").await.unwrap();
}
