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

use crate::BinaryOperator;
use crate::DfType;
use std::collections::HashMap;
use std::sync::OnceLock;

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
        (DfType::SmallInt, BinaryOperator::Modulo, DfType::SmallInt),
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
        (DfType::SmallInt, BinaryOperator::Modulo, DfType::Int),
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
        (DfType::SmallInt, BinaryOperator::Modulo, DfType::BigInt),
        DfType::BigInt,
    );
    map.insert(
        (DfType::SmallInt, BinaryOperator::Add, DfType::Float),
        DfType::Double,
    );
    map.insert(
        (DfType::SmallInt, BinaryOperator::Subtract, DfType::Float),
        DfType::Double,
    );
    map.insert(
        (DfType::SmallInt, BinaryOperator::Multiply, DfType::Float),
        DfType::Double,
    );
    map.insert(
        (DfType::SmallInt, BinaryOperator::Divide, DfType::Float),
        DfType::Double,
    );
    map.insert(
        (DfType::SmallInt, BinaryOperator::Modulo, DfType::Float),
        DfType::Unknown,
    );
    map.insert(
        (DfType::SmallInt, BinaryOperator::Add, DfType::Double),
        DfType::Double,
    );
    map.insert(
        (DfType::SmallInt, BinaryOperator::Subtract, DfType::Double),
        DfType::Double,
    );
    map.insert(
        (DfType::SmallInt, BinaryOperator::Multiply, DfType::Double),
        DfType::Double,
    );
    map.insert(
        (DfType::SmallInt, BinaryOperator::Divide, DfType::Double),
        DfType::Double,
    );
    map.insert(
        (DfType::SmallInt, BinaryOperator::Modulo, DfType::Double),
        DfType::Unknown,
    );
    map.insert(
        (
            DfType::SmallInt,
            BinaryOperator::Add,
            DfType::DEFAULT_NUMERIC,
        ),
        DfType::DEFAULT_NUMERIC,
    );
    map.insert(
        (
            DfType::SmallInt,
            BinaryOperator::Subtract,
            DfType::DEFAULT_NUMERIC,
        ),
        DfType::DEFAULT_NUMERIC,
    );
    map.insert(
        (
            DfType::SmallInt,
            BinaryOperator::Multiply,
            DfType::DEFAULT_NUMERIC,
        ),
        DfType::DEFAULT_NUMERIC,
    );
    map.insert(
        (
            DfType::SmallInt,
            BinaryOperator::Divide,
            DfType::DEFAULT_NUMERIC,
        ),
        DfType::DEFAULT_NUMERIC,
    );
    map.insert(
        (
            DfType::SmallInt,
            BinaryOperator::Modulo,
            DfType::DEFAULT_NUMERIC,
        ),
        DfType::DEFAULT_NUMERIC,
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
    map.insert(
        (DfType::Int, BinaryOperator::Modulo, DfType::SmallInt),
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
        (DfType::Int, BinaryOperator::Modulo, DfType::Int),
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
        (DfType::Int, BinaryOperator::Modulo, DfType::BigInt),
        DfType::BigInt,
    );
    map.insert(
        (DfType::Int, BinaryOperator::Add, DfType::Float),
        DfType::Double,
    );
    map.insert(
        (DfType::Int, BinaryOperator::Subtract, DfType::Float),
        DfType::Double,
    );
    map.insert(
        (DfType::Int, BinaryOperator::Multiply, DfType::Float),
        DfType::Double,
    );
    map.insert(
        (DfType::Int, BinaryOperator::Divide, DfType::Float),
        DfType::Double,
    );
    map.insert(
        (DfType::Int, BinaryOperator::Modulo, DfType::Float),
        DfType::Unknown,
    );
    map.insert(
        (DfType::Int, BinaryOperator::Add, DfType::Double),
        DfType::Double,
    );
    map.insert(
        (DfType::Int, BinaryOperator::Subtract, DfType::Double),
        DfType::Double,
    );
    map.insert(
        (DfType::Int, BinaryOperator::Multiply, DfType::Double),
        DfType::Double,
    );
    map.insert(
        (DfType::Int, BinaryOperator::Divide, DfType::Double),
        DfType::Double,
    );
    map.insert(
        (DfType::Int, BinaryOperator::Modulo, DfType::Double),
        DfType::Unknown,
    );
    map.insert(
        (DfType::Int, BinaryOperator::Add, DfType::DEFAULT_NUMERIC),
        DfType::DEFAULT_NUMERIC,
    );
    map.insert(
        (
            DfType::Int,
            BinaryOperator::Subtract,
            DfType::DEFAULT_NUMERIC,
        ),
        DfType::DEFAULT_NUMERIC,
    );
    map.insert(
        (
            DfType::Int,
            BinaryOperator::Multiply,
            DfType::DEFAULT_NUMERIC,
        ),
        DfType::DEFAULT_NUMERIC,
    );
    map.insert(
        (DfType::Int, BinaryOperator::Divide, DfType::DEFAULT_NUMERIC),
        DfType::DEFAULT_NUMERIC,
    );
    map.insert(
        (DfType::Int, BinaryOperator::Modulo, DfType::DEFAULT_NUMERIC),
        DfType::DEFAULT_NUMERIC,
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
        (DfType::BigInt, BinaryOperator::Modulo, DfType::SmallInt),
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
        (DfType::BigInt, BinaryOperator::Modulo, DfType::Int),
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
    map.insert(
        (DfType::BigInt, BinaryOperator::Modulo, DfType::BigInt),
        DfType::BigInt,
    );
    map.insert(
        (DfType::BigInt, BinaryOperator::Add, DfType::Float),
        DfType::Double,
    );
    map.insert(
        (DfType::BigInt, BinaryOperator::Subtract, DfType::Float),
        DfType::Double,
    );
    map.insert(
        (DfType::BigInt, BinaryOperator::Multiply, DfType::Float),
        DfType::Double,
    );
    map.insert(
        (DfType::BigInt, BinaryOperator::Divide, DfType::Float),
        DfType::Double,
    );
    map.insert(
        (DfType::BigInt, BinaryOperator::Modulo, DfType::Float),
        DfType::Unknown,
    );
    map.insert(
        (DfType::BigInt, BinaryOperator::Add, DfType::Double),
        DfType::Double,
    );
    map.insert(
        (DfType::BigInt, BinaryOperator::Subtract, DfType::Double),
        DfType::Double,
    );
    map.insert(
        (DfType::BigInt, BinaryOperator::Multiply, DfType::Double),
        DfType::Double,
    );
    map.insert(
        (DfType::BigInt, BinaryOperator::Divide, DfType::Double),
        DfType::Double,
    );
    map.insert(
        (DfType::BigInt, BinaryOperator::Modulo, DfType::Double),
        DfType::Unknown,
    );
    map.insert(
        (DfType::BigInt, BinaryOperator::Add, DfType::DEFAULT_NUMERIC),
        DfType::DEFAULT_NUMERIC,
    );
    map.insert(
        (
            DfType::BigInt,
            BinaryOperator::Subtract,
            DfType::DEFAULT_NUMERIC,
        ),
        DfType::DEFAULT_NUMERIC,
    );
    map.insert(
        (
            DfType::BigInt,
            BinaryOperator::Multiply,
            DfType::DEFAULT_NUMERIC,
        ),
        DfType::DEFAULT_NUMERIC,
    );
    map.insert(
        (
            DfType::BigInt,
            BinaryOperator::Divide,
            DfType::DEFAULT_NUMERIC,
        ),
        DfType::DEFAULT_NUMERIC,
    );
    map.insert(
        (
            DfType::BigInt,
            BinaryOperator::Modulo,
            DfType::DEFAULT_NUMERIC,
        ),
        DfType::DEFAULT_NUMERIC,
    );
    map.insert(
        (DfType::Float, BinaryOperator::Add, DfType::SmallInt),
        DfType::Double,
    );
    map.insert(
        (DfType::Float, BinaryOperator::Subtract, DfType::SmallInt),
        DfType::Double,
    );
    map.insert(
        (DfType::Float, BinaryOperator::Multiply, DfType::SmallInt),
        DfType::Double,
    );
    map.insert(
        (DfType::Float, BinaryOperator::Divide, DfType::SmallInt),
        DfType::Double,
    );
    map.insert(
        (DfType::Float, BinaryOperator::Modulo, DfType::SmallInt),
        DfType::Unknown,
    );
    map.insert(
        (DfType::Float, BinaryOperator::Add, DfType::Int),
        DfType::Double,
    );
    map.insert(
        (DfType::Float, BinaryOperator::Subtract, DfType::Int),
        DfType::Double,
    );
    map.insert(
        (DfType::Float, BinaryOperator::Multiply, DfType::Int),
        DfType::Double,
    );
    map.insert(
        (DfType::Float, BinaryOperator::Divide, DfType::Int),
        DfType::Double,
    );
    map.insert(
        (DfType::Float, BinaryOperator::Modulo, DfType::Int),
        DfType::Unknown,
    );
    map.insert(
        (DfType::Float, BinaryOperator::Add, DfType::BigInt),
        DfType::Double,
    );
    map.insert(
        (DfType::Float, BinaryOperator::Subtract, DfType::BigInt),
        DfType::Double,
    );
    map.insert(
        (DfType::Float, BinaryOperator::Multiply, DfType::BigInt),
        DfType::Double,
    );
    map.insert(
        (DfType::Float, BinaryOperator::Divide, DfType::BigInt),
        DfType::Double,
    );
    map.insert(
        (DfType::Float, BinaryOperator::Modulo, DfType::BigInt),
        DfType::Unknown,
    );
    map.insert(
        (DfType::Float, BinaryOperator::Add, DfType::Float),
        DfType::Float,
    );
    map.insert(
        (DfType::Float, BinaryOperator::Subtract, DfType::Float),
        DfType::Float,
    );
    map.insert(
        (DfType::Float, BinaryOperator::Multiply, DfType::Float),
        DfType::Float,
    );
    map.insert(
        (DfType::Float, BinaryOperator::Divide, DfType::Float),
        DfType::Float,
    );
    map.insert(
        (DfType::Float, BinaryOperator::Modulo, DfType::Float),
        DfType::Unknown,
    );
    map.insert(
        (DfType::Float, BinaryOperator::Add, DfType::Double),
        DfType::Double,
    );
    map.insert(
        (DfType::Float, BinaryOperator::Subtract, DfType::Double),
        DfType::Double,
    );
    map.insert(
        (DfType::Float, BinaryOperator::Multiply, DfType::Double),
        DfType::Double,
    );
    map.insert(
        (DfType::Float, BinaryOperator::Divide, DfType::Double),
        DfType::Double,
    );
    map.insert(
        (DfType::Float, BinaryOperator::Modulo, DfType::Double),
        DfType::Unknown,
    );
    map.insert(
        (DfType::Float, BinaryOperator::Add, DfType::DEFAULT_NUMERIC),
        DfType::Double,
    );
    map.insert(
        (
            DfType::Float,
            BinaryOperator::Subtract,
            DfType::DEFAULT_NUMERIC,
        ),
        DfType::Double,
    );
    map.insert(
        (
            DfType::Float,
            BinaryOperator::Multiply,
            DfType::DEFAULT_NUMERIC,
        ),
        DfType::Double,
    );
    map.insert(
        (
            DfType::Float,
            BinaryOperator::Divide,
            DfType::DEFAULT_NUMERIC,
        ),
        DfType::Double,
    );
    map.insert(
        (
            DfType::Float,
            BinaryOperator::Modulo,
            DfType::DEFAULT_NUMERIC,
        ),
        DfType::Unknown,
    );
    map.insert(
        (DfType::Double, BinaryOperator::Add, DfType::SmallInt),
        DfType::Double,
    );
    map.insert(
        (DfType::Double, BinaryOperator::Subtract, DfType::SmallInt),
        DfType::Double,
    );
    map.insert(
        (DfType::Double, BinaryOperator::Multiply, DfType::SmallInt),
        DfType::Double,
    );
    map.insert(
        (DfType::Double, BinaryOperator::Divide, DfType::SmallInt),
        DfType::Double,
    );
    map.insert(
        (DfType::Double, BinaryOperator::Modulo, DfType::SmallInt),
        DfType::Unknown,
    );
    map.insert(
        (DfType::Double, BinaryOperator::Add, DfType::Int),
        DfType::Double,
    );
    map.insert(
        (DfType::Double, BinaryOperator::Subtract, DfType::Int),
        DfType::Double,
    );
    map.insert(
        (DfType::Double, BinaryOperator::Multiply, DfType::Int),
        DfType::Double,
    );
    map.insert(
        (DfType::Double, BinaryOperator::Divide, DfType::Int),
        DfType::Double,
    );
    map.insert(
        (DfType::Double, BinaryOperator::Modulo, DfType::Int),
        DfType::Unknown,
    );
    map.insert(
        (DfType::Double, BinaryOperator::Add, DfType::BigInt),
        DfType::Double,
    );
    map.insert(
        (DfType::Double, BinaryOperator::Subtract, DfType::BigInt),
        DfType::Double,
    );
    map.insert(
        (DfType::Double, BinaryOperator::Multiply, DfType::BigInt),
        DfType::Double,
    );
    map.insert(
        (DfType::Double, BinaryOperator::Divide, DfType::BigInt),
        DfType::Double,
    );
    map.insert(
        (DfType::Double, BinaryOperator::Modulo, DfType::BigInt),
        DfType::Unknown,
    );
    map.insert(
        (DfType::Double, BinaryOperator::Add, DfType::Float),
        DfType::Double,
    );
    map.insert(
        (DfType::Double, BinaryOperator::Subtract, DfType::Float),
        DfType::Double,
    );
    map.insert(
        (DfType::Double, BinaryOperator::Multiply, DfType::Float),
        DfType::Double,
    );
    map.insert(
        (DfType::Double, BinaryOperator::Divide, DfType::Float),
        DfType::Double,
    );
    map.insert(
        (DfType::Double, BinaryOperator::Modulo, DfType::Float),
        DfType::Unknown,
    );
    map.insert(
        (DfType::Double, BinaryOperator::Add, DfType::Double),
        DfType::Double,
    );
    map.insert(
        (DfType::Double, BinaryOperator::Subtract, DfType::Double),
        DfType::Double,
    );
    map.insert(
        (DfType::Double, BinaryOperator::Multiply, DfType::Double),
        DfType::Double,
    );
    map.insert(
        (DfType::Double, BinaryOperator::Divide, DfType::Double),
        DfType::Double,
    );
    map.insert(
        (DfType::Double, BinaryOperator::Modulo, DfType::Double),
        DfType::Unknown,
    );
    map.insert(
        (DfType::Double, BinaryOperator::Add, DfType::DEFAULT_NUMERIC),
        DfType::Double,
    );
    map.insert(
        (
            DfType::Double,
            BinaryOperator::Subtract,
            DfType::DEFAULT_NUMERIC,
        ),
        DfType::Double,
    );
    map.insert(
        (
            DfType::Double,
            BinaryOperator::Multiply,
            DfType::DEFAULT_NUMERIC,
        ),
        DfType::Double,
    );
    map.insert(
        (
            DfType::Double,
            BinaryOperator::Divide,
            DfType::DEFAULT_NUMERIC,
        ),
        DfType::Double,
    );
    map.insert(
        (
            DfType::Double,
            BinaryOperator::Modulo,
            DfType::DEFAULT_NUMERIC,
        ),
        DfType::Unknown,
    );
    map.insert(
        (
            DfType::DEFAULT_NUMERIC,
            BinaryOperator::Add,
            DfType::SmallInt,
        ),
        DfType::DEFAULT_NUMERIC,
    );
    map.insert(
        (
            DfType::DEFAULT_NUMERIC,
            BinaryOperator::Subtract,
            DfType::SmallInt,
        ),
        DfType::DEFAULT_NUMERIC,
    );
    map.insert(
        (
            DfType::DEFAULT_NUMERIC,
            BinaryOperator::Multiply,
            DfType::SmallInt,
        ),
        DfType::DEFAULT_NUMERIC,
    );
    map.insert(
        (
            DfType::DEFAULT_NUMERIC,
            BinaryOperator::Divide,
            DfType::SmallInt,
        ),
        DfType::DEFAULT_NUMERIC,
    );
    map.insert(
        (
            DfType::DEFAULT_NUMERIC,
            BinaryOperator::Modulo,
            DfType::SmallInt,
        ),
        DfType::DEFAULT_NUMERIC,
    );
    map.insert(
        (DfType::DEFAULT_NUMERIC, BinaryOperator::Add, DfType::Int),
        DfType::DEFAULT_NUMERIC,
    );
    map.insert(
        (
            DfType::DEFAULT_NUMERIC,
            BinaryOperator::Subtract,
            DfType::Int,
        ),
        DfType::DEFAULT_NUMERIC,
    );
    map.insert(
        (
            DfType::DEFAULT_NUMERIC,
            BinaryOperator::Multiply,
            DfType::Int,
        ),
        DfType::DEFAULT_NUMERIC,
    );
    map.insert(
        (DfType::DEFAULT_NUMERIC, BinaryOperator::Divide, DfType::Int),
        DfType::DEFAULT_NUMERIC,
    );
    map.insert(
        (DfType::DEFAULT_NUMERIC, BinaryOperator::Modulo, DfType::Int),
        DfType::DEFAULT_NUMERIC,
    );
    map.insert(
        (DfType::DEFAULT_NUMERIC, BinaryOperator::Add, DfType::BigInt),
        DfType::DEFAULT_NUMERIC,
    );
    map.insert(
        (
            DfType::DEFAULT_NUMERIC,
            BinaryOperator::Subtract,
            DfType::BigInt,
        ),
        DfType::DEFAULT_NUMERIC,
    );
    map.insert(
        (
            DfType::DEFAULT_NUMERIC,
            BinaryOperator::Multiply,
            DfType::BigInt,
        ),
        DfType::DEFAULT_NUMERIC,
    );
    map.insert(
        (
            DfType::DEFAULT_NUMERIC,
            BinaryOperator::Divide,
            DfType::BigInt,
        ),
        DfType::DEFAULT_NUMERIC,
    );
    map.insert(
        (
            DfType::DEFAULT_NUMERIC,
            BinaryOperator::Modulo,
            DfType::BigInt,
        ),
        DfType::DEFAULT_NUMERIC,
    );
    map.insert(
        (DfType::DEFAULT_NUMERIC, BinaryOperator::Add, DfType::Float),
        DfType::Double,
    );
    map.insert(
        (
            DfType::DEFAULT_NUMERIC,
            BinaryOperator::Subtract,
            DfType::Float,
        ),
        DfType::Double,
    );
    map.insert(
        (
            DfType::DEFAULT_NUMERIC,
            BinaryOperator::Multiply,
            DfType::Float,
        ),
        DfType::Double,
    );
    map.insert(
        (
            DfType::DEFAULT_NUMERIC,
            BinaryOperator::Divide,
            DfType::Float,
        ),
        DfType::Double,
    );
    map.insert(
        (
            DfType::DEFAULT_NUMERIC,
            BinaryOperator::Modulo,
            DfType::Float,
        ),
        DfType::Unknown,
    );
    map.insert(
        (DfType::DEFAULT_NUMERIC, BinaryOperator::Add, DfType::Double),
        DfType::Double,
    );
    map.insert(
        (
            DfType::DEFAULT_NUMERIC,
            BinaryOperator::Subtract,
            DfType::Double,
        ),
        DfType::Double,
    );
    map.insert(
        (
            DfType::DEFAULT_NUMERIC,
            BinaryOperator::Multiply,
            DfType::Double,
        ),
        DfType::Double,
    );
    map.insert(
        (
            DfType::DEFAULT_NUMERIC,
            BinaryOperator::Divide,
            DfType::Double,
        ),
        DfType::Double,
    );
    map.insert(
        (
            DfType::DEFAULT_NUMERIC,
            BinaryOperator::Modulo,
            DfType::Double,
        ),
        DfType::Unknown,
    );
    map.insert(
        (
            DfType::DEFAULT_NUMERIC,
            BinaryOperator::Add,
            DfType::DEFAULT_NUMERIC,
        ),
        DfType::DEFAULT_NUMERIC,
    );
    map.insert(
        (
            DfType::DEFAULT_NUMERIC,
            BinaryOperator::Subtract,
            DfType::DEFAULT_NUMERIC,
        ),
        DfType::DEFAULT_NUMERIC,
    );
    map.insert(
        (
            DfType::DEFAULT_NUMERIC,
            BinaryOperator::Multiply,
            DfType::DEFAULT_NUMERIC,
        ),
        DfType::DEFAULT_NUMERIC,
    );
    map.insert(
        (
            DfType::DEFAULT_NUMERIC,
            BinaryOperator::Divide,
            DfType::DEFAULT_NUMERIC,
        ),
        DfType::DEFAULT_NUMERIC,
    );
    map.insert(
        (
            DfType::DEFAULT_NUMERIC,
            BinaryOperator::Modulo,
            DfType::DEFAULT_NUMERIC,
        ),
        DfType::DEFAULT_NUMERIC,
    );
    map
}
