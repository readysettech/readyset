use nom_sql::parser::{ConditionBase, ConditionExpression, ConditionTree, FieldExpression, Operator};
use nom_sql::SelectStatement;

use std::collections::HashMap;
use std::string::String;
use std::vec::Vec;

#[derive(Clone, Debug)]
pub struct QueryGraphNode {
    pub rel_name: String,
    pub predicates: Vec<ConditionTree>,
    pub columns: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct QueryGraphEdge {
    pub join_predicates: Vec<ConditionTree>,
}

#[derive(Clone, Debug)]
pub struct QueryGraph {
    pub relations: HashMap<String, QueryGraphNode>,
    pub edges: HashMap<(String, String), QueryGraphEdge>,
}

impl QueryGraph {
    fn new() -> QueryGraph {
        QueryGraph {
            relations: HashMap::new(),
            edges: HashMap::new(),
        }
    }
}

// 1. Extract any predictates with placeholder parameters. We push these down to the edge
//    nodes, since we cannot instantiate the parameters inside the data flow graph (except for
//    non-materialized nodes).
// 2. Extract local predicates
// 3. Extract join predicates
// 4. Collect remaining predicates as global predicates
fn classify_conditionals(ce: &ConditionExpression,
                         mut local: &mut HashMap<String, Vec<ConditionTree>>,
                         mut join: &mut Vec<ConditionTree>,
                         mut global: &mut Vec<ConditionTree>) {
    match *ce {
        ConditionExpression::LogicalOp(ref ct) => {
            // conjunction, check both sides (which must be selection predicates or
            // atomatic selection predicates)
            classify_conditionals(ct.left.as_ref().unwrap(),
                                  &mut local,
                                  &mut join,
                                  &mut global);
            classify_conditionals(ct.right.as_ref().unwrap(),
                                  &mut local,
                                  &mut join,
                                  &mut global);
        }
        ConditionExpression::ComparisonOp(ref ct) => {
            // atomic selection predicate
            if let ConditionExpression::Base(ref l) = *ct.left.as_ref().unwrap().as_ref() {
                if let ConditionExpression::Base(ref r) = *ct.right.as_ref().unwrap().as_ref() {
                    match *r {
                        ConditionBase::Field(_) => {
                            // column/column comparison
                            if let ConditionBase::Field(_) = *l {
                                if ct.operator == Operator::Equal {
                                    // equi-join between two tables
                                    join.push(ct.clone());
                                } else {
                                    // non-equi-join?
                                    unimplemented!();
                                }
                            } else {
                                panic!("left hand side of comparison must be field");
                            }
                        }
                        ConditionBase::Literal(_) => {
                            if let ConditionBase::Field(ref lf) = *l {
                                // TODO(malte): this fails hard if lf.table is None
                                if !local.contains_key(lf.table.as_ref().unwrap()) {
                                    local.insert(lf.table.clone().unwrap(), Vec::new());
                                }
                                local.get_mut(lf.table.as_ref().unwrap()).unwrap().push(ct.clone());
                            }
                        }
                        ConditionBase::Placeholder => {
                            // can't do anything about placeholders, so ignore them
                            ()
                        }
                    }
                };
            };
        }
        ConditionExpression::Base(_) => {
            // don't expect to see a base here: we ought to exit when classifying its
            // parent selection predicate
            panic!("encountered unexpected standalone base of condition expression");
        }
    }
}

pub fn to_query_graph(st: &SelectStatement) -> Result<QueryGraph, String> {
    let mut qg = QueryGraph::new();

    // a handy closure for making new relation nodes
    let new_node = |rel: String,
                    preds: Vec<ConditionTree>,
                    st: &SelectStatement|
                    -> QueryGraphNode {
        QueryGraphNode {
            rel_name: rel.clone(),
            predicates: preds,
            columns: match st.fields {
                FieldExpression::All => unimplemented!(),
                FieldExpression::Seq(ref s) => {
                    s.iter()
                        .cloned()
                        .filter(|c| match c.table.as_ref() {
                            None => panic!("No table name set for column {} on {}", c.name, rel),
                            Some(t) => *t == rel,
                        })
                        .map(|c| c.name)
                        .collect()
                }
            },
        }
    };

    if let Some(ref cond) = st.where_clause {
        let mut join_predicates = Vec::new();
        let mut local_predicates = HashMap::new();
        let mut global_predicates = Vec::<ConditionTree>::new();
        // Let's classify the predicates we have in the query
        classify_conditionals(cond,
                              &mut local_predicates,
                              &mut join_predicates,
                              &mut global_predicates);

        // Now we're ready to build the query graph
        // 1. Set up nodes for each relation that we have local predicates for
        for (rel, preds) in local_predicates.into_iter() {
            let n = new_node(rel.clone(), preds, &st);
            qg.relations.insert(rel, n);
        }
        // 2. Add edges for each pair of joined relations
        // TODO(malte): This is pretty heavily into cloning things all over, which makes it both
        // inefficient and hideous. Maybe we can reengineer the data structures to require less of
        // that?
        for jp in join_predicates.into_iter() {
            // We have a ConditionExpression, but both sides of it are ConditionBase of type Field
            if let ConditionExpression::Base(ConditionBase::Field(ref l)) =
                *jp.left.as_ref().unwrap().as_ref() {
                if let ConditionExpression::Base(ConditionBase::Field(ref r)) =
                    *jp.right.as_ref().unwrap().as_ref() {
                    qg.edges
                        .entry((l.table.clone().unwrap(), r.table.clone().unwrap()))
                        .or_insert(QueryGraphEdge { join_predicates: vec![] })
                        .join_predicates
                        .push(jp.clone());
                    // XXX(malte): push join columns into projected column set as well. This isn't
                    // strictly required, and eagerly pushes more columns than needed, but makes a
                    // naive version of the graph construction work.
                    if !qg.relations
                        .entry(l.table.as_ref().unwrap().clone())
                        .or_insert(new_node(l.table.as_ref().unwrap().clone(), vec![], &st))
                        .columns
                        .contains(&l.name) {
                        qg.relations
                            .get_mut(l.table.as_ref().unwrap())
                            .unwrap()
                            .columns
                            .push(l.name.clone());
                    }
                    if !qg.relations
                        .entry(r.table.as_ref().unwrap().clone())
                        .or_insert(new_node(r.table.as_ref().unwrap().clone(), vec![], &st))
                        .columns
                        .contains(&r.name) {
                        qg.relations
                            .get_mut(r.table.as_ref().unwrap())
                            .unwrap()
                            .columns
                            .push(r.name.clone());
                    }
                }
            }
        }
    }
    // 3. Add any relations mentioned in the query but not in any conditionals.
    // This is also needed so that we don't end up with an empty query graph when there are no
    // conditionals, but rather with a one-node query graph that has no predicates.
    for table in st.tables.iter() {
        if !qg.relations.contains_key(table.name.as_str()) {
            qg.relations.insert(table.name.clone(),
                                new_node(table.name.clone(), Vec::new(), &st));
        }
    }

    Ok(qg)
}
