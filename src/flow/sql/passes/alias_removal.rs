use flow::sql::passes::Pass;
use nom_sql::parser::{Column, ConditionBase, ConditionExpression, ConditionTree, FieldExpression,
                      SqlQuery};

use std::collections::HashMap;

pub struct AliasRemoval {
    table_aliases: HashMap<String, String>,
    #[allow(dead_code)]
    column_aliases: HashMap<String, Column>,
}

impl AliasRemoval {
    fn rewrite_conditional(&self, ce: ConditionExpression) -> ConditionExpression {
        let translate_column = |f: Column| {
            let new_f = match f.table {
                None => f,
                Some(t) => {
                    Column {
                        name: f.name,
                        table: if self.table_aliases.contains_key(&t) {
                            Some(self.table_aliases[&t].clone())
                        } else {
                            Some(t)
                        },
                    }
                }
            };
            ConditionExpression::Base(ConditionBase::Field(new_f))
        };

        let translate_ct_arm =
            |i: Option<Box<ConditionExpression>>| -> Option<Box<ConditionExpression>> {
                match i {
                    Some(bce) => {
                        let new_ce = match *bce {
                            ConditionExpression::Base(ConditionBase::Field(f)) => {
                                translate_column(f)
                            }
                            ConditionExpression::Base(b) => ConditionExpression::Base(b),
                            x => self.rewrite_conditional(x),
                        };
                        Some(Box::new(new_ce))
                    }
                    x => x,
                }
            };

        match ce {
            ConditionExpression::ComparisonOp(ct) => {
                let l = translate_ct_arm(ct.left);
                let r = translate_ct_arm(ct.right);
                let rewritten_ct = ConditionTree {
                    operator: ct.operator,
                    left: l,
                    right: r,
                };
                ConditionExpression::ComparisonOp(rewritten_ct)
            }
            ConditionExpression::LogicalOp(ct) => {
                let rewritten_ct = ConditionTree {
                    operator: ct.operator,
                    left: match ct.left {
                        Some(lct) => Some(Box::new(self.rewrite_conditional(*lct))),
                        x => x,
                    },
                    right: match ct.right {
                        Some(rct) => Some(Box::new(self.rewrite_conditional(*rct))),
                        x => x,
                    },
                };
                ConditionExpression::LogicalOp(rewritten_ct)
            }
            x => x,
        }
    }

    pub fn new() -> AliasRemoval {
        AliasRemoval {
            table_aliases: HashMap::new(),
            column_aliases: HashMap::new(),
        }
    }
}

impl Pass for AliasRemoval {
    fn apply(&mut self, q: SqlQuery) -> SqlQuery {
        match q {
            // nothing to do for INSERTs, as they cannot have aliases
            SqlQuery::Insert(i) => SqlQuery::Insert(i),
            SqlQuery::Select(mut sq) => {
                // Collect table aliases
                for t in sq.tables.iter() {
                    match t.alias {
                        None => (),
                        Some(ref a) => {
                            self.table_aliases.insert(a.clone(), t.name.clone());
                        }
                    }
                }
                // Remove them from fields
                sq.fields = match sq.fields {
                    FieldExpression::All => FieldExpression::All,
                    FieldExpression::Seq(fs) => {
                        let new_fs = fs.into_iter()
                            .map(|f| {
                                match f.table {
                                    None => f,
                                    Some(t) => {
                                        Column {
                                            name: f.name,
                                            table: if self.table_aliases.contains_key(&t) {
                                                Some(self.table_aliases[&t].clone())
                                            } else {
                                                Some(t)
                                            },
                                        }
                                    }
                                }
                            })
                            .collect();
                        FieldExpression::Seq(new_fs)
                    }
                };
                // Remove them from conditions
                sq.where_clause = match sq.where_clause {
                    None => None,
                    Some(wc) => Some(self.rewrite_conditional(wc)),
                };
                SqlQuery::Select(sq)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use flow::sql::passes::Pass;
    use nom_sql::SelectStatement;
    use nom_sql::parser::{Column, FieldExpression, SqlQuery, Table};
    use super::AliasRemoval;

    #[test]
    fn it_removes_aliases() {
        use nom_sql::parser::{ConditionBase, ConditionExpression, ConditionTree, Operator};

        let q = SelectStatement {
            tables: vec![Table {
                             name: String::from("PaperTag"),
                             alias: Some(String::from("t")),
                         }],
            fields: FieldExpression::Seq(vec![Column::from("t.id")]),
            where_clause: Some(ConditionExpression::ComparisonOp(ConditionTree {
                operator: Operator::Equal,
                left: Some(Box::new(ConditionExpression::Base(
                            ConditionBase::Field(
                                Column::from("t.id"))
                            ))),
                right: Some(Box::new(ConditionExpression::Base(ConditionBase::Placeholder))),
            })),
            ..Default::default()
        };
        let mut ar = AliasRemoval::new();
        let res = ar.apply(SqlQuery::Select(q));
        // Table alias removed in field list
        match res {
            SqlQuery::Select(tq) => {
                assert_eq!(tq.fields,
                           FieldExpression::Seq(vec![Column::from("PaperTag.id")]));
                assert_eq!(tq.where_clause,
                           Some(ConditionExpression::ComparisonOp(ConditionTree {
                               operator: Operator::Equal,
                               left: Some(Box::new(ConditionExpression::Base(
                                       ConditionBase::Field(
                                           Column::from("PaperTag.id"))
                                       ))),
                               right: Some(Box::new(ConditionExpression::Base(ConditionBase::Placeholder))),
                           })));
            }
            // if we get anything other than a selection query back, something really weird is up
            _ => panic!(),
        }
    }
}
