use nom_sql::{Column, ConditionBase, ConditionExpression, ConditionTree, FieldExpression, Operator};
use nom_sql::SelectStatement;

use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::string::String;
use std::vec::Vec;

use flow::sql::query_signature::QuerySignature;

#[derive(Clone, Debug, PartialEq)]
pub struct QueryGraphNode {
    pub rel_name: String,
    pub predicates: Vec<ConditionTree>,
    pub columns: Vec<Column>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum QueryGraphEdge {
    Join(Vec<ConditionTree>),
    GroupBy(Vec<Column>),
}

#[derive(Clone, Debug, PartialEq)]
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

    /// Used to get a concise signature for a query graph. The `hash` member can be used to check
    /// for identical sets of relations and attributes covered (as per Finkelstein algorithm),
    /// while `relations` and `attributes` as `HashSet`s that allow for efficient subset checks.
    pub fn signature(&self) -> QuerySignature {
        use std::collections::hash_map::DefaultHasher;

        let mut hasher = DefaultHasher::new();
        let rels = self.relations
            .keys()
            .map(|r| String::as_str(r))
            .collect();

        // Compute relations part of hash
        let mut r_vec: Vec<&str> = self.relations.keys().map(String::as_str).collect();
        r_vec.sort();
        for r in &r_vec {
            r.hash(&mut hasher);
        }

        let mut attrs = HashSet::<&Column>::new();
        let mut attrs_vec = Vec::<&Column>::new();
        for n in self.relations.values() {
            for p in &n.predicates {
                for c in &p.contained_columns() {
                    attrs_vec.push(c);
                    attrs.insert(c);
                }
            }
        }
        for e in self.edges.values() {
            match *e {
                QueryGraphEdge::Join(ref join_predicates) => {
                    for p in join_predicates {
                        for c in &p.contained_columns() {
                            attrs_vec.push(c);
                            attrs.insert(c);
                        }
                    }
                }
                QueryGraphEdge::GroupBy(ref cols) => {
                    for c in cols {
                        attrs_vec.push(c);
                        attrs.insert(c);
                    }
                }
            }
        }

        // Compute attributes part of hash
        attrs_vec.sort();
        for a in &attrs_vec {
            a.hash(&mut hasher);
        }

        QuerySignature {
            relations: rels,
            attributes: attrs,
            hash: hasher.finish(),
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
    use std::cmp::Ordering;

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
                        ConditionBase::Field(ref fr) => {
                            // column/column comparison
                            if let ConditionBase::Field(ref fl) = *l {
                                if ct.operator == Operator::Equal {
                                    // equi-join between two tables
                                    let mut join_ct = ct.clone();
                                    if let Ordering::Less = fr.table
                                        .as_ref()
                                        .cmp(&fl.table.as_ref()) {
                                        use std::mem;
                                        mem::swap(&mut join_ct.left, &mut join_ct.right);
                                    }
                                    join.push(join_ct);
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
    let new_node =
        |rel: String, preds: Vec<ConditionTree>, st: &SelectStatement| -> QueryGraphNode {
            QueryGraphNode {
                rel_name: rel.clone(),
                predicates: preds,
                columns: match st.fields {
                    FieldExpression::All => unimplemented!(),
                    FieldExpression::Seq(ref s) => {
                        s.iter()
                            .cloned()
                            .filter(|c| {
                                match c.table.as_ref() {
                                    None => {
                                        match c.function {
                                            // XXX(malte): don't drop aggregation columns
                                            Some(_) => false,
                                            None => {
                                                panic!("No table name set for column {} on {}",
                                                       c.name,
                                                       rel)
                                            }
                                        }
                                    }
                                    Some(t) => *t == rel,
                                }
                            })
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
        for (rel, preds) in local_predicates {
            let n = new_node(rel.clone(), preds, st);
            qg.relations.insert(rel, n);
        }
        // 2. Add edges for each pair of joined relations
        // TODO(malte): This is pretty heavily into cloning things all over, which makes it both
        // inefficient and hideous. Maybe we can reengineer the data structures to require less of
        // that?
        for jp in join_predicates {
            // We have a ConditionExpression, but both sides of it are ConditionBase of type Field
            if let ConditionExpression::Base(ConditionBase::Field(ref l)) =
                *jp.left.as_ref().unwrap().as_ref() {
                if let ConditionExpression::Base(ConditionBase::Field(ref r)) =
                    *jp.right.as_ref().unwrap().as_ref() {
                    let mut e = qg.edges
                        .entry((l.table.clone().unwrap(), r.table.clone().unwrap()))
                        .or_insert_with(|| QueryGraphEdge::Join(vec![]));
                    match *e {
                        QueryGraphEdge::Join(ref mut preds) => preds.push(jp.clone()),
                        _ => panic!("Expected join edge for join condition {:#?}", jp),
                    };
                    // XXX(malte): push join columns into projected column set as well. This isn't
                    // strictly required, and eagerly pushes more columns than needed, but makes a
                    // naive version of the graph construction work.
                    {
                        let left = &mut qg.relations
                            .entry(l.table.as_ref().unwrap().clone())
                            .or_insert_with(|| {
                                new_node(l.table.as_ref().unwrap().clone(), vec![], st)
                            });
                        if !left.columns.iter().any(|c| c.name == l.name) {
                            left.columns.push(l.clone());
                        }
                    }

                    {
                        let right = &mut qg.relations
                            .entry(r.table.as_ref().unwrap().clone())
                            .or_insert_with(|| {
                                new_node(r.table.as_ref().unwrap().clone(), vec![], st)
                            });
                        if !right.columns.iter().any(|c| c.name == r.name) {
                            right.columns.push(r.clone());
                        }
                    }
                }
            }
        }
    }

    // 3. Add any relations mentioned in the query but not in any conditionals.
    // This is also needed so that we don't end up with an empty query graph when there are no
    // conditionals, but rather with a one-node query graph that has no predicates.
    for table in &st.tables {
        if !qg.relations.contains_key(table.name.as_str()) {
            qg.relations.insert(table.name.clone(),
                                new_node(table.name.clone(), Vec::new(), st));
        }
    }

    // 4. Add query graph nodes for any computed columns, which won't be represented in the
    //    nodes corresponding to individual relations.
    match st.fields {
        FieldExpression::All => panic!("Stars should have been expanded by now!"),
        FieldExpression::Seq(ref fields) => {
            for column in fields.iter() {
                match column.function {
                    None => (),  // we've already dealt with this column as part of some relation
                    Some(_) => {
                        // add a special node representing the computed columns
                        // TODO(malte): the predicates here should probably reflect HAVING
                        // conditions, if any are present
                        let mut n = new_node(String::from("computed_columns"), vec![], st);
                        n.columns.push(column.clone());
                        qg.relations.insert(String::from("computed_columns"), n);
                    }
                }
            }
        }
    }
    match st.group_by {
        None => (),
        Some(ref clause) => {
            // println!("{:#?}", clause);
            for column in &clause.columns {
                // add an edge for each relation whose columns appear in the GROUP BY clause
                let mut e = qg.edges
                    .entry((String::from("computed_columns"),
                            column.table.as_ref().unwrap().clone()))
                    .or_insert_with(|| QueryGraphEdge::GroupBy(vec![]));
                match *e {
                    QueryGraphEdge::GroupBy(ref mut cols) => cols.push(column.clone()),
                    _ => unreachable!(),
                }
            }
        }
    }

    Ok(qg)
}
