use std::fmt::Display;

use readyset_errors::{unsupported, ReadySetResult};
use readyset_sql::ast::FunctionExpr;
use serde::{Deserialize, Serialize};

use crate::ops::grouped::aggregate::Aggregation;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WindowOperation {
    RowNumber,
    Rank,
    DenseRank,
    Max,
    Min,
    Aggregation(Aggregation),
}

impl WindowOperation {
    pub fn from_fn(fn_expr: FunctionExpr) -> ReadySetResult<WindowOperation> {
        Ok(match fn_expr {
            FunctionExpr::RowNumber => WindowOperation::RowNumber,
            FunctionExpr::Max { .. } => WindowOperation::Max,
            FunctionExpr::Min { .. } => WindowOperation::Min,
            FunctionExpr::CountStar | FunctionExpr::Count { .. } => {
                WindowOperation::Aggregation(Aggregation::Count)
            }
            FunctionExpr::Sum { .. } => WindowOperation::Aggregation(Aggregation::Sum),
            FunctionExpr::Avg { .. } => WindowOperation::Aggregation(Aggregation::Avg),
            FunctionExpr::Rank => WindowOperation::Rank,
            FunctionExpr::DenseRank => WindowOperation::DenseRank,
            _ => unsupported!("Function {fn_expr:?} is not supported as a window function"),
        })
    }
}

impl Display for WindowOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WindowOperation::RowNumber => write!(f, "row_number"),
            WindowOperation::Rank => write!(f, "rank"),
            WindowOperation::DenseRank => write!(f, "dense_rank"),
            WindowOperation::Max => write!(f, "max"),
            WindowOperation::Min => write!(f, "min"),
            // TODO: Make Aggregation impl Display
            WindowOperation::Aggregation(a) => write!(f, "{a:?}"),
        }
    }
}
