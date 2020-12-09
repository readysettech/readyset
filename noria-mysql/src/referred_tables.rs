use nom_sql::{
    CompoundSelectStatement, ConditionBase, ConditionExpression, SelectSpecification, SqlQuery,
    Table,
};

pub trait ReferredTables {
    fn referred_tables(&self) -> Vec<Table>;
}

impl ReferredTables for SqlQuery {
    fn referred_tables(&self) -> Vec<Table> {
        let handle_compound_select = |csq: &CompoundSelectStatement| {
            csq.selects
                .iter()
                .fold(Vec::new(), |mut acc, &(_, ref sq)| {
                    acc.extend(sq.tables.iter().cloned().collect::<Vec<_>>());
                    acc
                })
        };

        match *self {
            SqlQuery::Set(_) => vec![],
            SqlQuery::CreateTable(ref ctq) => vec![ctq.table.clone()],
            SqlQuery::CreateView(ref cvq) => match *cvq.definition {
                SelectSpecification::Simple(ref sq) => sq.tables.iter().cloned().collect(),
                SelectSpecification::Compound(ref csq) => handle_compound_select(csq),
            },
            SqlQuery::Insert(ref iq) => vec![iq.table.clone()],
            SqlQuery::Select(ref sq) => sq.tables.iter().cloned().collect(),
            SqlQuery::CompoundSelect(ref csq) => handle_compound_select(csq),
            SqlQuery::DropTable(ref dtq) => dtq.tables.iter().cloned().collect(),
            // TODO(malte): this ignores the possibility of nested selections in the WHERE clause;
            // that's okay since we only support primary key deletes at this point.
            SqlQuery::Delete(ref dq) => vec![dq.table.clone()],
            SqlQuery::Update(ref uq) => vec![uq.table.clone()],
            SqlQuery::AlterTable(ref atq) => vec![atq.table.clone()],
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
