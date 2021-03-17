use nom_sql::{
    ConditionBase, ConditionExpression, Expression, FunctionExpression, SqlQuery, Table,
};

pub trait ReferredTables {
    fn referred_tables(&self) -> Vec<Table>;
}

impl ReferredTables for SqlQuery {
    fn referred_tables(&self) -> Vec<Table> {
        match *self {
            SqlQuery::CreateTable(ref ctq) => vec![ctq.table.clone()],
            SqlQuery::AlterTable(ref atq) => vec![atq.table.clone()],
            SqlQuery::Insert(ref iq) => vec![iq.table.clone()],
            SqlQuery::Select(ref sq) => sq.tables.to_vec(),
            SqlQuery::CompoundSelect(ref csq) => {
                csq.selects
                    .iter()
                    .fold(Vec::new(), |mut acc, &(_, ref sq)| {
                        acc.extend(sq.tables.to_vec());
                        acc
                    })
            }
            _ => unreachable!(),
        }
    }
}

impl ReferredTables for ConditionExpression {
    fn referred_tables(&self) -> Vec<Table> {
        let mut tables = Vec::new();
        match *self {
            ConditionExpression::LogicalOp(ref ct) | ConditionExpression::ComparisonOp(ref ct) => {
                for t in ct
                    .left
                    .referred_tables()
                    .into_iter()
                    .chain(ct.right.referred_tables().into_iter())
                {
                    if !tables.contains(&t) {
                        tables.push(t);
                    }
                }
            }
            ConditionExpression::Base(ConditionBase::Field(ref f)) => {
                if let Some(ref t) = f.table {
                    let t = Table::from(t.as_ref());
                    if !tables.contains(&t) {
                        tables.push(t);
                    }
                }
            }
            _ => unimplemented!(),
        }
        tables
    }
}

/// Returns true if the given [`FunctionExpression`] represents an aggregate function
pub(crate) fn is_aggregate(function: &FunctionExpression) -> bool {
    match function {
        FunctionExpression::Avg(_, _)
        | FunctionExpression::Count(_, _)
        | FunctionExpression::CountStar
        | FunctionExpression::Sum(_, _)
        | FunctionExpression::Max(_)
        | FunctionExpression::Min(_)
        | FunctionExpression::GroupConcat(_, _) => true,
        FunctionExpression::Cast(_, _) => false,
        // For now, assume all "generic" function calls are not aggregates
        FunctionExpression::Generic(_, _) => false,
    }
}

/// Rturns true if *any* of the recursive subexpressions of the given [`Expression`] contain an
/// aggregate
pub(crate) fn contains_aggregate(expr: &Expression) -> bool {
    match expr {
        Expression::Arithmetic(_) => false,
        Expression::Call(f) => is_aggregate(f),
        Expression::Literal(_) => false,
        Expression::Column { .. } => false,
    }
}
