////////////////////////////////////////////////////////////////////////////////
//
//             THIS FILE IS MACHINE-GENERATED!!!  DO NOT EDIT!!!
//
// To regenerate this file:
//
// cargo run -p database-utils --bin psql_output_types > \
//     dataflow-expression/src/promotion/psql.rs
// cargo fmt
//
////////////////////////////////////////////////////////////////////////////////

use std::collections::HashMap;
use std::sync::OnceLock;

use crate::{BinaryOperator, DfType};

pub(crate) fn output_type(left: &DfType, op: &BinaryOperator, right: &DfType) -> Option<DfType> {
    static MAP: OnceLock<HashMap<(DfType, BinaryOperator, DfType), DfType>> = OnceLock::new();
    let map = MAP.get_or_init(build_map);
    map.get(&(left.clone(), *op, right.clone())).cloned()
}

fn build_map() -> HashMap<(DfType, BinaryOperator, DfType), DfType> {
    let mut map = HashMap::new();
    map.insert(
        (DfType::SmallInt, BinaryOperator::Add, DfType::SmallInt),
        DfType::SmallInt,
    );
    map.insert(
        (DfType::SmallInt, BinaryOperator::Subtract, DfType::SmallInt),
        DfType::SmallInt,
    );
    map.insert(
        (DfType::SmallInt, BinaryOperator::Multiply, DfType::SmallInt),
        DfType::SmallInt,
    );
    map.insert(
        (DfType::SmallInt, BinaryOperator::Divide, DfType::SmallInt),
        DfType::SmallInt,
    );
    map.insert(
        (DfType::SmallInt, BinaryOperator::Add, DfType::Int),
        DfType::Int,
    );
    map.insert(
        (DfType::SmallInt, BinaryOperator::Subtract, DfType::Int),
        DfType::Int,
    );
    map.insert(
        (DfType::SmallInt, BinaryOperator::Multiply, DfType::Int),
        DfType::Int,
    );
    map.insert(
        (DfType::SmallInt, BinaryOperator::Divide, DfType::Int),
        DfType::Int,
    );
    map.insert(
        (DfType::SmallInt, BinaryOperator::Add, DfType::BigInt),
        DfType::BigInt,
    );
    map.insert(
        (DfType::SmallInt, BinaryOperator::Subtract, DfType::BigInt),
        DfType::BigInt,
    );
    map.insert(
        (DfType::SmallInt, BinaryOperator::Multiply, DfType::BigInt),
        DfType::BigInt,
    );
    map.insert(
        (DfType::SmallInt, BinaryOperator::Divide, DfType::BigInt),
        DfType::BigInt,
    );
    map.insert(
        (DfType::Int, BinaryOperator::Add, DfType::SmallInt),
        DfType::Int,
    );
    map.insert(
        (DfType::Int, BinaryOperator::Subtract, DfType::SmallInt),
        DfType::Int,
    );
    map.insert(
        (DfType::Int, BinaryOperator::Multiply, DfType::SmallInt),
        DfType::Int,
    );
    map.insert(
        (DfType::Int, BinaryOperator::Divide, DfType::SmallInt),
        DfType::Int,
    );
    map.insert((DfType::Int, BinaryOperator::Add, DfType::Int), DfType::Int);
    map.insert(
        (DfType::Int, BinaryOperator::Subtract, DfType::Int),
        DfType::Int,
    );
    map.insert(
        (DfType::Int, BinaryOperator::Multiply, DfType::Int),
        DfType::Int,
    );
    map.insert(
        (DfType::Int, BinaryOperator::Divide, DfType::Int),
        DfType::Int,
    );
    map.insert(
        (DfType::Int, BinaryOperator::Add, DfType::BigInt),
        DfType::BigInt,
    );
    map.insert(
        (DfType::Int, BinaryOperator::Subtract, DfType::BigInt),
        DfType::BigInt,
    );
    map.insert(
        (DfType::Int, BinaryOperator::Multiply, DfType::BigInt),
        DfType::BigInt,
    );
    map.insert(
        (DfType::Int, BinaryOperator::Divide, DfType::BigInt),
        DfType::BigInt,
    );
    map.insert(
        (DfType::BigInt, BinaryOperator::Add, DfType::SmallInt),
        DfType::BigInt,
    );
    map.insert(
        (DfType::BigInt, BinaryOperator::Subtract, DfType::SmallInt),
        DfType::BigInt,
    );
    map.insert(
        (DfType::BigInt, BinaryOperator::Multiply, DfType::SmallInt),
        DfType::BigInt,
    );
    map.insert(
        (DfType::BigInt, BinaryOperator::Divide, DfType::SmallInt),
        DfType::BigInt,
    );
    map.insert(
        (DfType::BigInt, BinaryOperator::Add, DfType::Int),
        DfType::BigInt,
    );
    map.insert(
        (DfType::BigInt, BinaryOperator::Subtract, DfType::Int),
        DfType::BigInt,
    );
    map.insert(
        (DfType::BigInt, BinaryOperator::Multiply, DfType::Int),
        DfType::BigInt,
    );
    map.insert(
        (DfType::BigInt, BinaryOperator::Divide, DfType::Int),
        DfType::BigInt,
    );
    map.insert(
        (DfType::BigInt, BinaryOperator::Add, DfType::BigInt),
        DfType::BigInt,
    );
    map.insert(
        (DfType::BigInt, BinaryOperator::Subtract, DfType::BigInt),
        DfType::BigInt,
    );
    map.insert(
        (DfType::BigInt, BinaryOperator::Multiply, DfType::BigInt),
        DfType::BigInt,
    );
    map.insert(
        (DfType::BigInt, BinaryOperator::Divide, DfType::BigInt),
        DfType::BigInt,
    );
    map
}
