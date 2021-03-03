use nom_sql::{
    ArithmeticBase, ArithmeticExpression, ArithmeticItem, BinaryOperator, Column, ConditionBase,
    ConditionExpression, ConditionTree, FieldDefinitionExpression, FieldValueExpression,
    JoinConstraint, JoinOperator, JoinRightSide, Literal, Table,
};
use nom_sql::{OrderType, SelectStatement};

use std::hash::{Hash, Hasher};
use std::string::String;
use std::vec::Vec;
use std::{cmp::Ordering, num::NonZeroU64};
use std::{collections::HashMap, convert::TryInto};

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct LiteralColumn {
    pub name: String,
    pub table: Option<String>,
    pub value: Literal,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ArithmeticColumn {
    pub name: String,
    pub table: Option<String>,
    pub expression: ArithmeticExpression,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum OutputColumn {
    Data(Column),
    Arithmetic(ArithmeticColumn),
    Literal(LiteralColumn),
}

impl Ord for OutputColumn {
    fn cmp(&self, other: &OutputColumn) -> Ordering {
        match *self {
            OutputColumn::Arithmetic(ArithmeticColumn {
                ref name,
                ref table,
                ..
            })
            | OutputColumn::Data(Column {
                ref name,
                ref table,
                ..
            })
            | OutputColumn::Literal(LiteralColumn {
                ref name,
                ref table,
                ..
            }) => match *other {
                OutputColumn::Arithmetic(ArithmeticColumn {
                    name: ref other_name,
                    table: ref other_table,
                    ..
                })
                | OutputColumn::Data(Column {
                    name: ref other_name,
                    table: ref other_table,
                    ..
                })
                | OutputColumn::Literal(LiteralColumn {
                    name: ref other_name,
                    table: ref other_table,
                    ..
                }) => {
                    if table.is_some() && other_table.is_some() {
                        match table.cmp(&other_table) {
                            Ordering::Equal => name.cmp(&other_name),
                            x => x,
                        }
                    } else {
                        name.cmp(&other_name)
                    }
                }
            },
        }
    }
}

impl PartialOrd for OutputColumn {
    fn partial_cmp(&self, other: &OutputColumn) -> Option<Ordering> {
        match *self {
            OutputColumn::Arithmetic(ArithmeticColumn {
                ref name,
                ref table,
                ..
            })
            | OutputColumn::Data(Column {
                ref name,
                ref table,
                ..
            })
            | OutputColumn::Literal(LiteralColumn {
                ref name,
                ref table,
                ..
            }) => match *other {
                OutputColumn::Arithmetic(ArithmeticColumn {
                    name: ref other_name,
                    table: ref other_table,
                    ..
                })
                | OutputColumn::Data(Column {
                    name: ref other_name,
                    table: ref other_table,
                    ..
                })
                | OutputColumn::Literal(LiteralColumn {
                    name: ref other_name,
                    table: ref other_table,
                    ..
                }) => {
                    if table.is_some() && other_table.is_some() {
                        match table.cmp(&other_table) {
                            Ordering::Equal => Some(name.cmp(&other_name)),
                            x => Some(x),
                        }
                    } else if table.is_none() && other_table.is_none() {
                        Some(name.cmp(&other_name))
                    } else {
                        None
                    }
                }
            },
        }
    }
}

#[derive(Clone, Debug, Hash, PartialEq)]
pub struct JoinRef {
    pub src: String,
    pub dst: String,
    pub index: usize,
}

#[derive(Clone, Debug, Hash, PartialEq)]
pub struct QueryGraphNode {
    pub rel_name: String,
    pub predicates: Vec<ConditionExpression>,
    pub columns: Vec<Column>,
    pub parameters: Vec<(Column, nom_sql::BinaryOperator)>,
}

#[derive(Clone, Debug, Hash, PartialEq)]
pub enum QueryGraphEdge {
    Join(Vec<ConditionTree>),
    LeftJoin(Vec<ConditionTree>),
    GroupBy(Vec<Column>),
}

#[derive(Clone, Debug, Hash, PartialEq)]
pub struct Pagination {
    pub order: Vec<(Column, OrderType)>,
    pub limit: Option<u64>,
    pub offset: Option<NonZeroU64>,
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
    /// Establishes an order for join predicates. Each join predicate can be identified by
    /// its (src, dst) pair, and its index in the array of predicates.
    pub join_order: Vec<JoinRef>,
    /// Global predicates (not associated with a particular relation)
    pub global_predicates: Vec<ConditionExpression>,
    /// The pagination (order, limit, offset) for the query, if any
    pub pagination: Option<Pagination>,
}

impl QueryGraph {
    fn new() -> Self {
        Default::default()
    }

    /// Returns the set of columns on which this query is parameterized. They can come from
    /// multiple tables involved in the query.
    pub fn parameters<'a>(&'a self) -> Vec<&'a (Column, nom_sql::BinaryOperator)> {
        self.relations.values().fold(
            Vec::new(),
            |mut acc: Vec<&'a (Column, nom_sql::BinaryOperator)>, qgn| {
                acc.extend(qgn.parameters.iter());
                acc
            },
        )
    }

    pub fn exact_hash(&self) -> u64 {
        use std::collections::hash_map::DefaultHasher;

        let mut s = DefaultHasher::new();
        self.hash(&mut s);
        s.finish()
    }
}

impl Default for QueryGraph {
    fn default() -> Self {
        QueryGraph {
            relations: HashMap::new(),
            edges: HashMap::new(),
            columns: Vec::new(),
            join_order: Vec::new(),
            global_predicates: Vec::new(),
            pagination: None,
        }
    }
}

#[allow(clippy::derive_hash_xor_eq)]
impl Hash for QueryGraph {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // sorted iteration over relations, edges to ensure consistent hash
        let mut rels: Vec<(&String, &QueryGraphNode)> = self.relations.iter().collect();
        rels.sort_by(|a, b| a.0.cmp(b.0));
        rels.hash(state);
        let mut edges: Vec<(&(String, String), &QueryGraphEdge)> = self.edges.iter().collect();
        edges.sort_by(|a, b| match (a.0).0.cmp(&(b.0).0) {
            Ordering::Equal => (a.0).1.cmp(&(b.0).1),
            x => x,
        });
        edges.hash(state);

        // columns and join_order are Vecs, so already ordered
        self.columns.hash(state);
        self.join_order.hash(state);
        self.global_predicates.hash(state);
        self.pagination.hash(state);
    }
}

/// Splits top level conjunctions into multiple predicates
fn split_conjunctions(ces: Vec<ConditionExpression>) -> Vec<ConditionExpression> {
    let mut new_ces = Vec::new();
    for ce in ces {
        match ce {
            ConditionExpression::LogicalOp(ref ct) => {
                match ct.operator {
                    BinaryOperator::And => {
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
    tables: &[Table],
    local: &mut HashMap<String, Vec<ConditionExpression>>,
    join: &mut Vec<ConditionTree>,
    global: &mut Vec<ConditionExpression>,
    params: &mut Vec<(Column, nom_sql::BinaryOperator)>,
) {
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
            // first, we recurse on both sides, collected the result of nested predicate analysis
            // in separate collections. What do do with these depends on whether we're an AND or an
            // OR clause:
            //  1) AND can be split into separate local predicates one one or more tables
            //  2) OR predictes must be preserved in their entirety, and we only use the nested
            //     local predicates discovered to decide if the OR is over one table (so it can
            //     remain a local predicate) or over several (so it must be a global predicate)
            let mut new_params = Vec::new();
            let mut new_join = Vec::new();
            let mut new_local = HashMap::new();
            let mut new_global = Vec::new();

            classify_conditionals(
                ct.left.as_ref(),
                tables,
                &mut new_local,
                &mut new_join,
                &mut new_global,
                &mut new_params,
            );
            classify_conditionals(
                ct.right.as_ref(),
                tables,
                &mut new_local,
                &mut new_join,
                &mut new_global,
                &mut new_params,
            );

            match ct.operator {
                BinaryOperator::And => {
                    //
                    for (t, ces) in new_local {
                        // conjunction, check if either side had a local predicate
                        assert!(
                            ces.len() <= 2,
                            "can only combine two or fewer ConditionExpression's"
                        );
                        if ces.len() == 2 {
                            let new_ce = ConditionExpression::LogicalOp(ConditionTree {
                                operator: BinaryOperator::And,
                                left: Box::new(ces.first().unwrap().clone()),
                                right: Box::new(ces.last().unwrap().clone()),
                            });

                            let e = local.entry(t.to_string()).or_default();
                            e.push(new_ce);
                        } else {
                            let e = local.entry(t.to_string()).or_default();
                            e.extend(ces);
                        }
                    }

                    // one side of the AND might be a global predicate, so we need to keep
                    // new_global around
                    global.extend(new_global);
                }
                BinaryOperator::Or => {
                    assert!(
                        new_join.is_empty(),
                        "can't handle OR expressions between join predicates"
                    );
                    assert!(
                        new_params.is_empty(),
                        "can't handle OR expressions between query parameter predicates"
                    );
                    if new_local.keys().len() == 1 && new_global.is_empty() {
                        // OR over a single table => local predicate
                        let (t, ces) = new_local.into_iter().next().unwrap();
                        assert_eq!(ces.len(), 2, "should combine only 2 ConditionExpressions");
                        let new_ce = ConditionExpression::LogicalOp(ConditionTree {
                            operator: BinaryOperator::Or,
                            left: Box::new(ces.first().unwrap().clone()),
                            right: Box::new(ces.last().unwrap().clone()),
                        });

                        let e = local.entry(t.to_string()).or_default();
                        e.push(new_ce);
                    } else {
                        // OR between different tables => global predicate
                        global.push(ce.clone())
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
                        // right-hand side is field, so this could be a comma join
                        // or a security policy using UserContext
                        ConditionBase::Field(ref rf) => {
                            // column/column comparison
                            if let ConditionBase::Field(ref lf) = *l {
                                if lf.table.is_some()
                                    && tables
                                        .contains(&Table::from(lf.table.as_ref().unwrap().as_str()))
                                    && rf.table.is_some()
                                    && tables
                                        .contains(&Table::from(rf.table.as_ref().unwrap().as_str()))
                                    && lf.table != rf.table
                                {
                                    // both columns' tables appear in table list and the tables are
                                    // different --> comma join
                                    if ct.operator == BinaryOperator::Equal
                                        || ct.operator == BinaryOperator::In
                                    {
                                        // equi-join between two tables
                                        let mut join_ct = ct.clone();
                                        if let Ordering::Less =
                                            rf.table.as_ref().cmp(&lf.table.as_ref())
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
                                    // not a comma join, just an ordinary comparison with a
                                    // computed column. This must be a global predicate because it
                                    // crosses "tables" (the computed column has no associated
                                    // table)
                                    global.push(ce.clone());
                                }
                            } else {
                                panic!("left hand side of comparison must be field");
                            }
                        }
                        // right-hand side is a placeholder, so this must be a query parameter
                        ConditionBase::Literal(Literal::Placeholder(_)) => {
                            if let ConditionBase::Field(ref lf) = *l {
                                params.push((lf.clone(), ct.operator.clone()));
                            }
                        }
                        // right-hand side is a non-placeholder literal, so this is a predicate
                        ConditionBase::Literal(_) => {
                            if let ConditionBase::Field(ref lf) = *l {
                                // we assume that implied table names have previously been expanded
                                // and thus all non-computed columns carry table names
                                if lf.table.is_some() {
                                    let e = local.entry(lf.table.clone().unwrap()).or_default();
                                    e.push(ce.clone());
                                } else {
                                    // comparisons between computed columns and literals are global
                                    // predicates
                                    global.push(ce.clone());
                                }
                            }
                        }
                        ConditionBase::LiteralList(_) => (),
                        ConditionBase::NestedSelect(_) => unimplemented!(),
                    }
                };
            };
        }
        ConditionExpression::Bracketed(ref inner) => {
            let mut new_params = Vec::new();
            let mut new_join = Vec::new();
            let mut new_local = HashMap::new();
            classify_conditionals(
                inner.as_ref(),
                tables,
                &mut new_local,
                &mut new_join,
                global,
                &mut new_params,
            );
            join.extend(new_join);
            params.extend(new_params);
        }
        ConditionExpression::Base(_) => {
            // don't expect to see a base here: we ought to exit when classifying its
            // parent selection predicate
            unreachable!("encountered unexpected standalone base of condition expression");
        }
        ConditionExpression::NegationOp(_) => {
            unreachable!("negation should have been removed earlier");
        }
        ConditionExpression::Arithmetic(_) => unimplemented!(),
        ConditionExpression::ExistsOp(_) => unimplemented!(),
        ConditionExpression::Between { .. } => {
            unreachable!("Between should have been removed earlier")
        }
    }
}

#[allow(clippy::cognitive_complexity)]
pub fn to_query_graph(st: &SelectStatement) -> Result<QueryGraph, String> {
    let mut qg = QueryGraph::new();

    // a handy closure for making new relation nodes
    let new_node =
        |rel: String, preds: Vec<ConditionExpression>, st: &SelectStatement| -> QueryGraphNode {
            QueryGraphNode {
                rel_name: rel.clone(),
                predicates: preds,
                columns: st
                    .fields
                    .iter()
                    .filter_map(|field| match *field {
                        // unreachable because SQL rewrite passes will have expanded these already
                        FieldDefinitionExpression::All => {
                            unreachable!("* should have been expanded already")
                        }
                        FieldDefinitionExpression::AllInTable(_) => {
                            unreachable!("<table>.* should have been expanded already")
                        }
                        // No need to do anything for literals and arithmetic expressions here, as they
                        // aren't associated with a relation (and thus have no QGN)
                        FieldDefinitionExpression::Value(_) => None,
                        FieldDefinitionExpression::Col(ref c) => {
                            match c.table.as_ref() {
                                None => {
                                    match c.function {
                                        // XXX(malte): don't drop aggregation columns
                                        Some(_) => None,
                                        None => panic!(
                                            "No table name set for column {} on {}",
                                            c.name, rel
                                        ),
                                    }
                                }
                                Some(t) => {
                                    if *t == rel {
                                        Some(c.clone())
                                    } else {
                                        None
                                    }
                                }
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
            JoinRightSide::Table(ref table) => {
                if !qg.relations.contains_key(&table.name) {
                    qg.relations.insert(
                        table.name.clone(),
                        new_node(table.name.clone(), Vec::new(), st),
                    );
                }
            }
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
                        use crate::controller::sql::query_utils::ReferredTables;

                        // find all distinct tables mentioned in the condition
                        // conditions for now.
                        let mut tables_mentioned: Vec<String> =
                            cond.referred_tables().into_iter().map(|t| t.name).collect();

                        match *cond {
                            ConditionExpression::ComparisonOp(ref ct) => {
                                if tables_mentioned.len() == 2 {
                                    // tables can appear in any order in the join predicate, but
                                    // we cannot just rely on that order, since it may lead us to
                                    // flip LEFT JOINs by accident (yes, this happened)
                                    if tables_mentioned[1] != table.name {
                                        // tables are in the wrong order in join predicate, swap
                                        tables_mentioned.swap(0, 1);
                                        assert_eq!(tables_mentioned[1], table.name);
                                    }
                                    left_table = tables_mentioned.remove(0);
                                    right_table = tables_mentioned.remove(0);
                                } else if tables_mentioned.len() == 1 {
                                    // just one table mentioned --> this is a self-join
                                    left_table = tables_mentioned.remove(0);
                                    right_table = left_table.clone();
                                } else {
                                    unreachable!("more than 2 tables mentioned in join condition!");
                                };

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
                                if *l.table.as_ref().unwrap() == right_table
                                    && *r.table.as_ref().unwrap() == left_table
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
                            operator: BinaryOperator::Equal,
                            left: wrapcol(&left_table, &col.name),
                            right: wrapcol(&right_table, &col.name),
                        }
                    }
                };

                // add edge for join
                let mut _e = qg
                    .edges
                    .entry((left_table.clone(), right_table.clone()))
                    .or_insert_with(|| match jc.operator {
                        JoinOperator::LeftJoin | JoinOperator::LeftOuterJoin => {
                            QueryGraphEdge::LeftJoin(vec![join_pred])
                        }
                        JoinOperator::Join | JoinOperator::InnerJoin => {
                            QueryGraphEdge::Join(vec![join_pred])
                        }
                        _ => unimplemented!(),
                    });
            }
            _ => unimplemented!(),
        }
    }

    if let Some(ref cond) = st.where_clause {
        let mut local_predicates = HashMap::new();
        let mut global_predicates = Vec::new();
        let mut query_parameters = Vec::new();
        // Let's classify the predicates we have in the query
        classify_conditionals(
            cond,
            &st.tables,
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
                    preds, rel
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
                    qg.relations
                        .entry(l.table.clone().unwrap())
                        .or_insert_with(|| new_node(l.table.clone().unwrap(), Vec::new(), st));

                    qg.relations
                        .entry(r.table.clone().unwrap())
                        .or_insert_with(|| new_node(r.table.clone().unwrap(), Vec::new(), st));

                    let e = qg
                        .edges
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
        for (column, operator) in query_parameters.into_iter() {
            match column.table {
                None => {
                    return Err(format!("each parameter's column must have an associated table! (no such column \"{}\")", column).to_string());
                }
                Some(ref table) => {
                    let rel = qg.relations.get_mut(table).unwrap();
                    if !rel.columns.contains(&column) {
                        rel.columns.push(column.clone());
                    }
                    // the parameter column is included in the projected columns of the output, but
                    // we also separately register it as a parameter so that we can set keys
                    // correctly on the leaf view
                    rel.parameters.push((column.clone(), operator.clone()));
                }
            }
        }

        // 4. Add global predicates
        qg.global_predicates = global_predicates;
    }

    // Adds a computed column to the query graph if the given column has a function:
    let add_computed_column = |query_graph: &mut QueryGraph, column: &Column| {
        match column.function {
            None => (), // we've already dealt with this column as part of some relation
            Some(_) => {
                // add a special node representing the computed columns; if it already
                // exists, add another computed column to it
                let n = query_graph
                    .relations
                    .entry(String::from("computed_columns"))
                    .or_insert_with(|| new_node(String::from("computed_columns"), vec![], st));

                n.columns.push(column.clone());
            }
        }
    };

    // 4. Add query graph nodes for any computed columns, which won't be represented in the
    //    nodes corresponding to individual relations.
    for field in st.fields.iter() {
        match *field {
            FieldDefinitionExpression::All | FieldDefinitionExpression::AllInTable(_) => {
                panic!("Stars should have been expanded by now!")
            }
            FieldDefinitionExpression::Value(FieldValueExpression::Literal(ref l)) => {
                qg.columns.push(OutputColumn::Literal(LiteralColumn {
                    name: match l.alias {
                        Some(ref a) => a.to_string(),
                        None => l.value.to_string(),
                    },
                    table: None,
                    value: l.value.clone(),
                }));
            }
            FieldDefinitionExpression::Value(FieldValueExpression::Arithmetic(ref a)) => {
                if let ArithmeticItem::Base(ArithmeticBase::Column(ref c)) = a.ari.left {
                    add_computed_column(&mut qg, c);
                }

                if let ArithmeticItem::Base(ArithmeticBase::Column(ref c)) = a.ari.right {
                    add_computed_column(&mut qg, c);
                }

                qg.columns.push(OutputColumn::Arithmetic(ArithmeticColumn {
                    name: a.alias.clone().unwrap_or_else(|| a.to_string()),
                    table: None,
                    expression: a.clone(),
                }));
            }
            FieldDefinitionExpression::Col(ref c) => {
                add_computed_column(&mut qg, c);
                qg.columns.push(OutputColumn::Data(c.clone()));
            }
        }
    }

    match st.group_by {
        None => (),
        Some(ref clause) => {
            for column in &clause.columns {
                // add an edge for each relation whose columns appear in the GROUP BY clause
                let e = qg
                    .edges
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

    if let Some(ref order) = st.order {
        qg.pagination = Some(Pagination {
            order: order.columns.clone(),
            limit: st.limit.clone().map(|lim| lim.limit),
            offset: st.limit.clone().and_then(|lim| lim.offset.try_into().ok()),
        })
    }

    // create initial join order
    {
        let mut sorted_edges: Vec<(&(String, String), &QueryGraphEdge)> = qg.edges.iter().collect();
        // Sort the edges to ensure deterministic join order.
        sorted_edges.sort_by(|&(a, _), &(b, _)| {
            let src_ord = b.0.cmp(&a.0);
            if src_ord == Ordering::Equal {
                a.1.cmp(&b.1)
            } else {
                src_ord
            }
        });

        for (&(ref src, ref dst), edge) in sorted_edges {
            match *edge {
                QueryGraphEdge::Join(ref jps) => qg.join_order.extend(
                    jps.iter()
                        .enumerate()
                        .map(|(idx, _)| JoinRef {
                            src: src.clone(),
                            dst: dst.clone(),
                            index: idx,
                        })
                        .collect::<Vec<_>>(),
                ),
                QueryGraphEdge::LeftJoin(ref jps) => qg.join_order.extend(
                    jps.iter()
                        .enumerate()
                        .map(|(idx, _)| JoinRef {
                            src: src.clone(),
                            dst: dst.clone(),
                            index: idx,
                        })
                        .collect::<Vec<_>>(),
                ),
                QueryGraphEdge::GroupBy(_) => continue,
            }
        }
    }

    Ok(qg)
}
