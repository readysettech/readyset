use nom_sql::{Column, ConditionBase, ConditionExpression, ConditionTree, FieldExpression,
              JoinConstraint, JoinOperator, JoinRightSide, Literal, Operator};
use nom_sql::SelectStatement;
use nom_sql::ConditionExpression::*;

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::string::String;
use std::vec::Vec;

use sql::query_signature::QuerySignature;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct LiteralColumn {
    pub name: String,
    pub table: Option<String>,
    pub value: Literal,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum OutputColumn {
    Data(Column),
    Literal(LiteralColumn),
}

impl Ord for OutputColumn {
    fn cmp(&self, other: &OutputColumn) -> Ordering {
        match *self {
            OutputColumn::Data(Column {
                ref name,
                ref table,
                ..
            }) |
            OutputColumn::Literal(LiteralColumn {
                ref name,
                ref table,
                ..
            }) => match *other {
                OutputColumn::Data(Column {
                    name: ref other_name,
                    table: ref other_table,
                    ..
                }) |
                OutputColumn::Literal(LiteralColumn {
                    name: ref other_name,
                    table: ref other_table,
                    ..
                }) => if table.is_some() && other_table.is_some() {
                    match table.cmp(&other_table) {
                        Ordering::Equal => name.cmp(&other_name),
                        x => x,
                    }
                } else {
                    name.cmp(&other_name)
                },
            },
        }
    }
}

impl PartialOrd for OutputColumn {
    fn partial_cmp(&self, other: &OutputColumn) -> Option<Ordering> {
        match *self {
            OutputColumn::Data(Column {
                ref name,
                ref table,
                ..
            }) |
            OutputColumn::Literal(LiteralColumn {
                ref name,
                ref table,
                ..
            }) => match *other {
                OutputColumn::Data(Column {
                    name: ref other_name,
                    table: ref other_table,
                    ..
                }) |
                OutputColumn::Literal(LiteralColumn {
                    name: ref other_name,
                    table: ref other_table,
                    ..
                }) => if table.is_some() && other_table.is_some() {
                    match table.cmp(&other_table) {
                        Ordering::Equal => Some(name.cmp(&other_name)),
                        x => Some(x),
                    }
                } else if table.is_none() && other_table.is_none() {
                    Some(name.cmp(&other_name))
                } else {
                    None
                },
            },
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct QueryGraphNode {
    pub rel_name: String,
    pub predicates: Vec<ConditionExpression>,
    pub columns: Vec<Column>,
    pub parameters: Vec<Column>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum QueryGraphEdge {
    Join(Vec<ConditionTree>),
    LeftJoin(Vec<ConditionTree>),
    GroupBy(Vec<Column>),
}

#[derive(Clone, Debug, PartialEq)]
pub struct QueryGraph {
    /// Relations mentioned in the query.
    pub relations: HashMap<String, QueryGraphNode>,
    /// Joins and GroupBys in the query.
    pub edges: HashMap<(String, String), QueryGraphEdge>,
    /// Final set of projected columns in this query; may include literals in addition to the
    /// columns reflected in individual relations' `QueryGraphNode` structures.
    pub columns: Vec<OutputColumn>,
}

impl QueryGraph {
    fn new() -> QueryGraph {
        QueryGraph {
            relations: HashMap::new(),
            edges: HashMap::new(),
            columns: Vec::new(),
        }
    }

    /// Returns the set of columns on which this query is parameterized. They can come from
    /// multiple tables involved in the query.
    pub fn parameters<'a>(&'a self) -> Vec<&'a Column> {
        self.relations
            .values()
            .fold(Vec::new(), |mut acc: Vec<&'a Column>, ref qgn| {
                acc.extend(qgn.parameters.iter());
                acc
            })
    }

    /// Used to get a concise signature for a query graph. The `hash` member can be used to check
    /// for identical sets of relations and attributes covered (as per Finkelstein algorithm),
    /// while `relations` and `attributes` as `HashSet`s that allow for efficient subset checks.
    pub fn signature(&self) -> QuerySignature {
        use std::collections::hash_map::DefaultHasher;

        let mut hasher = DefaultHasher::new();
        let rels = self.relations.keys().map(|r| String::as_str(r)).collect();

        // Compute relations part of hash
        let mut r_vec: Vec<&str> = self.relations.keys().map(String::as_str).collect();
        r_vec.sort();
        for r in &r_vec {
            r.hash(&mut hasher);
        }

        // Collect attributes from predicates and projected columns
        let mut attrs = HashSet::<&Column>::new();
        let mut attrs_vec = Vec::<&Column>::new();
        for n in self.relations.values() {
            for p in &n.predicates {
                match *p {
                    ComparisonOp(ref ct) | LogicalOp(ref ct) => for c in &ct.contained_columns() {
                        attrs_vec.push(c);
                        attrs.insert(c);
                    },
                    _ => unreachable!(),
                }
            }
        }
        for e in self.edges.values() {
            match *e {
                QueryGraphEdge::Join(ref join_predicates) |
                QueryGraphEdge::LeftJoin(ref join_predicates) => for p in join_predicates {
                    for c in &p.contained_columns() {
                        attrs_vec.push(c);
                        attrs.insert(c);
                    }
                },
                QueryGraphEdge::GroupBy(ref cols) => for c in cols {
                    attrs_vec.push(c);
                    attrs.insert(c);
                },
            }
        }

        // Compute attributes part of hash
        attrs_vec.sort();
        for a in &attrs_vec {
            a.hash(&mut hasher);
        }

        let mut proj_columns: Vec<&OutputColumn> = self.columns.iter().collect();
        // Compute projected columns part of hash. We sort here since the order in which columns
        // appear does not matter for query graph equivalence.
        proj_columns.sort();
        for c in proj_columns {
            c.hash(&mut hasher);
        }

        QuerySignature {
            relations: rels,
            attributes: attrs,
            hash: hasher.finish(),
        }
    }
}

/// Splits top level conjunctions into multiple predicates
fn split_conjunctions(ces: Vec<ConditionExpression>) -> Vec<ConditionExpression> {
    let mut new_ces = Vec::new();
    for ce in ces {
        match ce {
            ConditionExpression::LogicalOp(ref ct) => {
                match ct.operator {
                    Operator::And => {
                        new_ces.extend(split_conjunctions(vec![*ct.left.clone()]));
                        new_ces.extend(split_conjunctions(vec![*ct.right.clone()]));
                    }
                    _ => {
                        new_ces.push(ce.clone());
                    }
                };
            }
            _ => {
                new_ces.push(ce.clone());
            }
        }
    }

    new_ces
}

// 1. Extract any predicates with placeholder parameters. We push these down to the edge
//    nodes, since we cannot instantiate the parameters inside the data flow graph (except for
//    non-materialized nodes).
// 2. Extract local predicates
// 3. Extract join predicates
// 4. Collect remaining predicates as global predicates
fn classify_conditionals(
    ce: &ConditionExpression,
    local: &mut HashMap<String, Vec<ConditionExpression>>,
    join: &mut Vec<ConditionTree>,
    global: &mut Vec<ConditionTree>,
    params: &mut Vec<Column>,
) {
    use std::cmp::Ordering;

    // Handling OR and AND expressions requires some care as there are some corner cases.
    //    a) we don't support OR expressions with predicates with placeholder parameters,
    //       because these expressions are meaningless in the Soup context.
    //    b) we don't support OR expressions with join predicates because they are weird and
    //       too hard.
    //    c) we don't support OR expressions between different tables (e.g table1.x = 1 OR
    //       table2.y= 42). this is a global predicate according to finkelstein algorithm
    //       and we don't support these yet.

    match *ce {
        ConditionExpression::LogicalOp(ref ct) => {
            // conjunction, check both sides (which must be selection predicates or
            // atomatic selection predicates)
            let mut new_params = Vec::new();
            let mut new_join = Vec::new();
            let mut new_local = HashMap::new();
            classify_conditionals(
                ct.left.as_ref(),
                &mut new_local,
                &mut new_join,
                global,
                &mut new_params,
            );
            classify_conditionals(
                ct.right.as_ref(),
                &mut new_local,
                &mut new_join,
                global,
                &mut new_params,
            );

            match ct.operator {
                Operator::And => for (t, ces) in new_local {
                    assert!(
                        ces.len() <= 2,
                        "can only combine two or fewer ConditionExpression's"
                    );
                    if ces.len() == 2 {
                        let new_ce = ConditionExpression::LogicalOp(ConditionTree {
                            operator: Operator::And,
                            left: Box::new(ces.first().unwrap().clone()),
                            right: Box::new(ces.last().unwrap().clone()),
                        });

                        let e = local.entry(t.to_string()).or_insert(Vec::new());
                        e.push(new_ce);
                    } else {
                        let e = local.entry(t.to_string()).or_insert(Vec::new());
                        e.extend(ces);
                    }
                },
                Operator::Or => {
                    assert!(
                        new_join.is_empty(),
                        "can't handle OR expressions between join predicates"
                    );
                    assert!(
                        new_params.is_empty(),
                        "can't handle OR expressions between query parameter predicates"
                    );
                    assert_eq!(
                        new_local.keys().len(),
                        1,
                        "can't handle OR expressions between different tables"
                    );
                    for (t, ces) in new_local {
                        assert_eq!(ces.len(), 2, "should combine only 2 ConditionExpression's");
                        let new_ce = ConditionExpression::LogicalOp(ConditionTree {
                            operator: Operator::Or,
                            left: Box::new(ces.first().unwrap().clone()),
                            right: Box::new(ces.last().unwrap().clone()),
                        });

                        let e = local.entry(t.to_string()).or_insert(Vec::new());
                        e.push(new_ce);
                    }
                }
                _ => unreachable!(),
            }

            join.extend(new_join);
            params.extend(new_params);
        }
        ConditionExpression::ComparisonOp(ref ct) => {
            // atomic selection predicate
            if let ConditionExpression::Base(ref l) = *ct.left.as_ref() {
                if let ConditionExpression::Base(ref r) = *ct.right.as_ref() {
                    match *r {
                        // right-hand side is field, so this must be a comma join
                        ConditionBase::Field(ref fr) => {
                            // column/column comparison --> comma join
                            if let ConditionBase::Field(ref fl) = *l {
                                if ct.operator == Operator::Equal || ct.operator == Operator::In {
                                    // equi-join between two tables
                                    let mut join_ct = ct.clone();
                                    if let Ordering::Less =
                                        fr.table.as_ref().cmp(&fl.table.as_ref())
                                    {
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
                        // right-hand side is a literal, so this is a predicate
                        ConditionBase::Literal(_) => {
                            if let ConditionBase::Field(ref lf) = *l {
                                // we assume that implied table names have previously been expanded
                                // and thus all columns carry table names
                                assert!(lf.table.is_some());
                                let e =
                                    local.entry(lf.table.clone().unwrap()).or_insert(Vec::new());
                                e.push(ce.clone());
                            }
                        }
                        // right-hand side is a placeholder, so this must be a query parameter
                        ConditionBase::Placeholder => if let ConditionBase::Field(ref lf) = *l {
                            params.push(lf.clone());
                        },
                        ConditionBase::NestedSelect(_) => unimplemented!(),
                    }
                };
            };
        }
        ConditionExpression::Base(_) => {
            // don't expect to see a base here: we ought to exit when classifying its
            // parent selection predicate
            panic!("encountered unexpected standalone base of condition expression");
        }
        ConditionExpression::NegationOp(_) => {
            panic!("negation should have been removed earlier");
        }
    }
}

pub fn to_query_graph(st: &SelectStatement) -> Result<QueryGraph, String> {
    let mut qg = QueryGraph::new();

    // a handy closure for making new relation nodes
    let new_node = |rel: String,
                    preds: Vec<ConditionExpression>,
                    st: &SelectStatement|
     -> QueryGraphNode {
        QueryGraphNode {
            rel_name: rel.clone(),
            predicates: preds,
            columns: st.fields
                .iter()
                .filter_map(|field| match *field {
                    // unreachable because SQL rewrite passes will have expanded these already
                    FieldExpression::All => unreachable!(),
                    FieldExpression::AllInTable(_) => unreachable!(),
                    // No need to do anything for literals here, as they aren't associated with a
                    // relation (and thus have no QGN)
                    FieldExpression::Literal(_) => None,
                    FieldExpression::Col(ref c) => {
                        match c.table.as_ref() {
                            None => {
                                match c.function {
                                    // XXX(malte): don't drop aggregation columns
                                    Some(_) => None,
                                    None => {
                                        panic!("No table name set for column {} on {}", c.name, rel)
                                    }
                                }
                            }
                            Some(t) => if *t == rel {
                                Some(c.clone())
                            } else {
                                None
                            },
                        }
                    }
                })
                .collect(),
            parameters: Vec::new(),
        }
    };

    // 1. Add any relations mentioned in the query to the query graph.
    // This is needed so that we don't end up with an empty query graph when there are no
    // conditionals, but rather with a one-node query graph that has no predicates.
    for table in &st.tables {
        qg.relations.insert(
            table.name.clone(),
            new_node(table.name.clone(), Vec::new(), st),
        );
    }
    for jc in &st.join {
        match jc.right {
            JoinRightSide::Table(ref table) => if !qg.relations.contains_key(&table.name) {
                qg.relations.insert(
                    table.name.clone(),
                    new_node(table.name.clone(), Vec::new(), st),
                );
            },
            _ => unimplemented!(),
        }
    }

    // 2. Add edges for each pair of joined relations. Note that we must keep track of the join
    //    predicates here already, but more may be added when processing the WHERE clause lateron.
    let mut join_predicates = Vec::new();
    let wrapcol = |tbl: &str, col: &str| -> Box<ConditionExpression> {
        let col = Column::from(format!("{}.{}", tbl, col).as_str());
        Box::new(ConditionExpression::Base(ConditionBase::Field(col)))
    };
    // 2a. Explicit joins
    // The table specified in the query is available for USING joins.
    let prev_table = Some(st.tables.last().as_ref().unwrap().name.clone());
    for jc in &st.join {
        match jc.right {
            JoinRightSide::Table(ref table) => {
                // will be defined by join constraint
                let left_table;
                let right_table;

                let join_pred = match jc.constraint {
                    JoinConstraint::On(ref cond) => {
                        use sql::query_utils::ReferredTables;

                        // find all distinct tables mentioned in the condition
                        // conditions for now.
                        let mut tables_mentioned: Vec<String> =
                            cond.referred_tables().into_iter().map(|t| t.name).collect();

                        match *cond {
                            ConditionExpression::ComparisonOp(ref ct) => {
                                assert_eq!(tables_mentioned.len(), 2);
                                // XXX(malte): these should always be in query order; I think they
                                // usually are in practice, but there is no guarantee since we just
                                // extract them in whatever order they're mentiond in the join
                                // predicate in
                                left_table = tables_mentioned.remove(0);
                                right_table = tables_mentioned.remove(0);

                                // the condition tree might specify tables in opposite order to
                                // their join order in the query; if so, flip them
                                // TODO(malte): this only deals with simple, flat join
                                // conditions for now.
                                let l = match *ct.left.as_ref() {
                                    ConditionExpression::Base(ConditionBase::Field(ref f)) => f,
                                    _ => unimplemented!(),
                                };
                                let r = match *ct.right.as_ref() {
                                    ConditionExpression::Base(ConditionBase::Field(ref f)) => f,
                                    _ => unimplemented!(),
                                };
                                if *l.table.as_ref().unwrap() == right_table &&
                                    *r.table.as_ref().unwrap() == left_table
                                {
                                    ConditionTree {
                                        operator: ct.operator.clone(),
                                        left: ct.right.clone(),
                                        right: ct.left.clone(),
                                    }
                                } else {
                                    ct.clone()
                                }
                            }
                            _ => panic!("join condition is not a comparison!"),
                        }
                    }
                    JoinConstraint::Using(ref cols) => {
                        assert_eq!(cols.len(), 1);
                        let col = cols.iter().next().unwrap();

                        left_table = prev_table.as_ref().unwrap().clone();
                        right_table = table.name.clone();

                        ConditionTree {
                            operator: Operator::Equal,
                            left: wrapcol(&left_table, &col.name),
                            right: wrapcol(&right_table, &col.name),
                        }
                    }
                };

                // add edge for join
                let mut _e = qg.edges
                    .entry((left_table.clone(), right_table.clone()))
                    .or_insert_with(|| match jc.operator {
                        JoinOperator::LeftJoin => QueryGraphEdge::LeftJoin(vec![join_pred]),
                        JoinOperator::Join => QueryGraphEdge::Join(vec![join_pred]),
                        _ => unimplemented!(),
                    });
            }
            _ => unimplemented!(),
        }
    }

    if let Some(ref cond) = st.where_clause {
        let mut local_predicates = HashMap::new();
        let mut global_predicates = Vec::<ConditionTree>::new();
        let mut query_parameters = Vec::new();
        // Let's classify the predicates we have in the query
        classify_conditionals(
            cond,
            &mut local_predicates,
            &mut join_predicates,
            &mut global_predicates,
            &mut query_parameters,
        );

        for (_, ces) in local_predicates.iter_mut() {
            *ces = split_conjunctions(ces.clone());
        }

        // 1. Add local predicates for each node that has them
        for (rel, preds) in local_predicates {
            if !qg.relations.contains_key(&rel) {
                // can't have predicates on tables that do not appear in the FROM part of the
                // statement
                panic!(
                    "predicate(s) {:?} on relation {} that is not in query graph",
                    preds,
                    rel
                );
            } else {
                qg.relations.get_mut(&rel).unwrap().predicates.extend(preds);
            }
        }

        // 2. Add predicates for implied (comma) joins
        for jp in join_predicates {
            // We have a ConditionExpression, but both sides of it are ConditionBase of type Field
            if let ConditionExpression::Base(ConditionBase::Field(ref l)) = *jp.left.as_ref() {
                if let ConditionExpression::Base(ConditionBase::Field(ref r)) = *jp.right.as_ref() {
                    // If tables aren't already in the relations, add them.
                    if !qg.relations.contains_key(&l.table.clone().unwrap()) {
                        qg.relations.insert(
                            l.table.clone().unwrap(),
                            new_node(l.table.clone().unwrap(), Vec::new(), st),
                        );
                    }

                    if !qg.relations.contains_key(&r.table.clone().unwrap()) {
                        qg.relations.insert(
                            r.table.clone().unwrap(),
                            new_node(r.table.clone().unwrap(), Vec::new(), st),
                        );
                    }

                    let e = qg.edges
                        .entry((l.table.clone().unwrap(), r.table.clone().unwrap()))
                        .or_insert_with(|| QueryGraphEdge::Join(vec![]));
                    match *e {
                        QueryGraphEdge::Join(ref mut preds) => preds.push(jp.clone()),
                        _ => panic!("Expected join edge for join condition {:#?}", jp),
                    };
                }
            }
        }

        // 3. Add any columns that are query parameters, and which therefore must appear in the leaf
        //    node for this query. Such columns will be carried all the way through the operators
        //    implementing the query (unlike in a traditional query plan, where the predicates on
        //    parameters might be evaluated sooner).
        for column in query_parameters.into_iter() {
            match column.table {
                None => panic!("each parameter's column must have an associated table!"),
                Some(ref table) => {
                    let rel = qg.relations.get_mut(table).unwrap();
                    if !rel.columns.contains(&column) {
                        rel.columns.push(column.clone());
                    }
                    // the parameter column is included in the projected columns of the output, but
                    // we also separately register it as a parameter so that we can set keys
                    // correctly on the leaf view
                    rel.parameters.push(column.clone());
                }
            }
        }
    }

    // 4. Add query graph nodes for any computed columns, which won't be represented in the
    //    nodes corresponding to individual relations.
    for field in st.fields.iter() {
        match *field {
            FieldExpression::All | FieldExpression::AllInTable(_) => {
                panic!("Stars should have been expanded by now!")
            }
            FieldExpression::Literal(ref l) => {
                qg.columns.push(OutputColumn::Literal(LiteralColumn {
                    name: String::from("literal"),
                    table: None,
                    value: l.clone(),
                }));
            }
            FieldExpression::Col(ref c) => {
                match c.function {
                    None => (), // we've already dealt with this column as part of some relation
                    Some(_) => {
                        // add a special node representing the computed columns; if it already
                        // exists, add another computed column to it
                        let n = qg.relations
                            .entry(String::from("computed_columns"))
                            .or_insert_with(
                                || new_node(String::from("computed_columns"), vec![], st),
                            );
                        n.columns.push(c.clone());
                    }
                }
                qg.columns.push(OutputColumn::Data(c.clone()));
            }
        }
    }

    match st.group_by {
        None => (),
        Some(ref clause) => {
            for column in &clause.columns {
                // add an edge for each relation whose columns appear in the GROUP BY clause
                let e = qg.edges
                    .entry((
                        String::from("computed_columns"),
                        column.table.as_ref().unwrap().clone(),
                    ))
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
