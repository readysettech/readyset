use std::collections::HashMap;
use std::iter::zip;

use mysql_async::Value;
use mysql_async::prelude::Queryable;

// Float and double are included without unsigned variants. MySQL deprecated UNSIGNED for FLOAT,
// DOUBLE, and DECIMAL as of 8.0.17 (see https://dev.mysql.com/doc/refman/8.0/en/numeric-type-syntax.html),
// and ReadySet has no UnsignedFloat/UnsignedDouble variants.
#[rustfmt::skip]
const MYSQL_TYPES: &[&str] = &[
    "tinyint", "smallint", "mediumint", "int", "bigint",
    "tinyint unsigned", "smallint unsigned", "mediumint unsigned", "int unsigned", "bigint unsigned",
    "float", "double",
];
const MYSQL_OPS: &[&str] = &["+", "-", "*", "/", "%"];
#[rustfmt::skip]
const READYSET_TYPES: &[&str] = &[
    "TinyInt", "SmallInt", "MediumInt", "Int", "BigInt",
    "UnsignedTinyInt", "UnsignedSmallInt", "UnsignedMediumInt", "UnsignedInt", "UnsignedBigInt",
    "Float", "Double",
];
const READYSET_OPS: &[&str] = &["Add", "Subtract", "Multiply", "Divide", "Modulo"];

async fn test(
    conn: &mut mysql_async::Conn,
    type_map: &HashMap<&str, &str>,
    a: usize,
    op: usize,
    b: usize,
) -> String {
    conn.query_drop("drop table if exists t1, t2, t3")
        .await
        .unwrap();

    conn.query_drop(format!("create table t1 (a {})", MYSQL_TYPES[a]))
        .await
        .unwrap();
    conn.query_drop(format!("create table t2 (b {})", MYSQL_TYPES[b]))
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

    // MySQL's UNSIGNED attribute is deprecated for FLOAT, DOUBLE, and DECIMAL as of 8.0.17
    // (see https://dev.mysql.com/doc/refman/8.0/en/numeric-type-syntax.html).
    // ReadySet has no UnsignedFloat/UnsignedDouble/UnsignedNumeric variants, so we drop the
    // unsigned flag for these types.
    if ty == "float" || ty == "double" {
        unsigned = false;
    }

    let out = if let Some(ty) = type_map.get(ty) {
        ty.to_string()
    } else if ty.starts_with("decimal") {
        unsigned = false;
        let ty = &ty[8..ty.len() - 1]; // remove leading "decimal(" and trailing ")"
        let mut ty = ty.split(',');
        let (x, y) = (ty.next().unwrap(), ty.next().unwrap());
        format!("Numeric {{ prec: {x}, scale: {y} }}")
    } else {
        panic!("unknown type: {ty}");
    };

    let unsigned_prefix = if unsigned { "Unsigned" } else { "" };
    format!(
        "(DfType::{}, BinaryOperator::{}, DfType::{}), DfType::{}{}",
        READYSET_TYPES[a], READYSET_OPS[op], READYSET_TYPES[b], unsigned_prefix, out,
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

    let mut type_map = HashMap::new();
    for (&my, &rs) in zip(MYSQL_TYPES, READYSET_TYPES) {
        type_map.insert(my, rs);
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
                let args = test(&mut conn, &type_map, a, op, b).await;
                println!("map.insert({args});");
            }
        }
    }

    println!("map");
    println!("}}");

    conn.query_drop("drop database rstest").await.unwrap();
}
