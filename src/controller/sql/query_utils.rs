use nom_sql::{ConditionBase, ConditionExpression, SqlQuery, Table};

pub trait ReferredTables {
    fn referred_tables(&self) -> Vec<Table>;
}

impl ReferredTables for SqlQuery {
    fn referred_tables(&self) -> Vec<Table> {
        match *self {
            SqlQuery::CreateTable(ref ctq) => vec![ctq.table.clone()],
            SqlQuery::Insert(ref iq) => vec![iq.table.clone()],
            SqlQuery::Select(ref sq) => sq.tables.iter().cloned().collect(),
        }
    }
}

impl ReferredTables for ConditionExpression {
    fn referred_tables(&self) -> Vec<Table> {
        let mut tables = Vec::new();
        match *self {
            ConditionExpression::LogicalOp(ref ct) | ConditionExpression::ComparisonOp(ref ct) => {
                for t in ct.left
                    .referred_tables()
                    .into_iter()
                    .chain(ct.right.referred_tables().into_iter())
                {
                    if !tables.contains(&t) {
                        tables.push(t);
                    }
                }
            }
            ConditionExpression::Base(ConditionBase::Field(ref f)) => match f.table {
                Some(ref t) => {
                    let t = Table::from(t.as_ref());
                    if !tables.contains(&t) {
                        tables.push(t);
                    }
                }
                None => (),
            },
            _ => unimplemented!(),
        }
        tables
    }
}
