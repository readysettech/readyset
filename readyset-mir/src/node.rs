use std::collections::HashSet;
use std::fmt::{Debug, Display, Error, Formatter};

use dataflow::ops;
use nom_sql::Relation;
use readyset_errors::{internal, ReadySetResult};
use serde::{Deserialize, Serialize};

pub use self::node_inner::{MirNodeInner, ProjectExpr, ViewKeyColumn};
use crate::DfNodeIndex;

pub mod node_inner;

/// Helper enum to avoid having separate `make_aggregation_node` and `make_extremum_node` functions
pub enum GroupedNodeType {
    Aggregation(ops::grouped::aggregate::Aggregation),
    Extremum(ops::grouped::extremum::Extremum),
}

/// A node in the MIR graph, that represent some abstract computation
/// on rows produced by its parents or itself.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MirNode {
    // TODO(fran): Get rid of the name (ENG-2113).
    name: Relation,
    /// The set of queries that own (make use of) this node.
    ///
    /// # Invariants
    /// - There must always be at least one owner for a node, except if the node is a base table or
    ///   if the node is being set up.
    owners: HashSet<Relation>,
    pub inner: MirNodeInner,
    df_node_index: Option<DfNodeIndex>,
}

impl MirNode {
    /// Creates a new [`MirNode`].
    pub fn new(name: Relation, inner: MirNodeInner) -> MirNode {
        MirNode {
            name,
            inner,
            owners: HashSet::new(),
            df_node_index: None,
        }
    }

    pub fn name(&self) -> &Relation {
        &self.name
    }

    /// The index of the dataflow node assigned to this node.
    pub fn df_node_index(&self) -> Option<DfNodeIndex> {
        self.df_node_index
    }

    /// Assigns the given dataflow node index to this node.
    /// This returns an error if the node already had a dataflow node assigned.
    pub fn assign_df_node_index(&mut self, df_node_address: DfNodeIndex) -> ReadySetResult<()> {
        if self.df_node_index.is_some() {
            internal!("Cannot set DF node to a MIR node that already has one!");
        }
        self.df_node_index = Some(df_node_address);
        Ok(())
    }

    /// The set of queries that own (make use of) this node.
    /// See <a href="struct.MirNode.html#structfield.owners">`owners`</a> for more information.
    pub fn owners(&self) -> &HashSet<Relation> {
        &self.owners
    }

    /// Adds the given owner to the set of owners of this node.
    /// Returns whether the owner was newly inserted or not.
    pub fn add_owner(&mut self, owner: Relation) -> bool {
        self.owners.insert(owner)
    }

    /// Removes a given owner from the set of owners of this node.
    /// Returns whether the owner was present in the set.
    pub fn remove_owner(&mut self, owner: &Relation) -> bool {
        self.owners.remove(owner)
    }

    /// Whether or not the given relation is an owner of this node.
    /// See <a href="struct.MirNode.html#structfield.owners">`owners`</a> for more
    /// information.
    pub fn is_owned_by(&self, owner: &Relation) -> bool {
        self.owners.contains(owner)
    }

    /// Retains only the owners specified by the predicate.
    pub fn retain_owners<F>(&mut self, f: F)
    where
        F: FnMut(&Relation) -> bool,
    {
        self.owners.retain(f)
    }

    pub fn is_base(&self) -> bool {
        matches!(self.inner, MirNodeInner::Base { .. })
    }
}

impl Display for MirNode {
    fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
        write!(
            f,
            "{:?} | name: {} | owners: {:?}",
            self.inner.description(),
            self.name.display_unquoted(),
            self.owners
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod columns {
        use common::IndexType;
        use dataflow::ops::grouped::aggregate::Aggregation;
        use dataflow::ops::grouped::extremum::Extremum;
        use dataflow::ops::union::DuplicateMode;
        use nom_sql::{BinaryOperator, ColumnSpecification, Expr, OrderType, SqlType};
        use readyset_client::ViewPlaceholder;

        use super::*;
        use crate::graph::MirGraph;
        use crate::{Column, NodeIndex};

        fn base1(graph: &mut MirGraph) -> NodeIndex {
            graph.add_node(MirNode::new(
                "base".into(),
                MirNodeInner::Base {
                    pg_meta: None,
                    column_specs: vec![
                        ColumnSpecification {
                            column: "base.a".into(),
                            sql_type: SqlType::Int(None),
                            constraints: vec![],
                            comment: None,
                        },
                        ColumnSpecification {
                            column: "base.b".into(),
                            sql_type: SqlType::Int(None),
                            constraints: vec![],
                            comment: None,
                        },
                    ],
                    primary_key: None,
                    unique_keys: vec![].into(),
                },
            ))
        }

        fn base2(graph: &mut MirGraph) -> NodeIndex {
            graph.add_node(MirNode::new(
                "base2".into(),
                MirNodeInner::Base {
                    pg_meta: None,
                    column_specs: vec![
                        ColumnSpecification {
                            column: "base2.a".into(),
                            sql_type: SqlType::Int(None),
                            constraints: vec![],
                            comment: None,
                        },
                        ColumnSpecification {
                            column: "base2.b".into(),
                            sql_type: SqlType::Int(None),
                            constraints: vec![],
                            comment: None,
                        },
                    ],
                    primary_key: None,
                    unique_keys: vec![].into(),
                },
            ))
        }

        fn has_columns_single_parent(inner: MirNodeInner, expected_cols: Vec<Column>) {
            let mut graph = MirGraph::new();
            let base = base1(&mut graph);
            let node = graph.add_node(MirNode::new("n".into(), inner));
            graph.add_edge(base, node, 0);
            let cols = graph.columns(node);
            assert_eq!(cols, expected_cols);
        }

        fn same_columns_as_parent(inner: MirNodeInner) {
            has_columns_single_parent(
                inner,
                vec![
                    Column::new(Some("base"), "a"),
                    Column::new(Some("base"), "b"),
                ],
            )
        }

        #[test]
        fn alias_table() {
            has_columns_single_parent(
                MirNodeInner::AliasTable { table: "at".into() },
                vec![Column::new(Some("at"), "a"), Column::new(Some("at"), "b")],
            )
        }

        #[test]
        fn aggregation() {
            has_columns_single_parent(
                MirNodeInner::Aggregation {
                    on: Column::new(Some("base"), "a"),
                    group_by: vec![Column::new(Some("base"), "b")],
                    output_column: Column::named("agg"),
                    kind: Aggregation::Sum,
                },
                vec![Column::new(Some("base"), "b"), Column::named("agg")],
            );
        }

        #[test]
        fn extremum() {
            has_columns_single_parent(
                MirNodeInner::Extremum {
                    on: Column::new(Some("base"), "a"),
                    group_by: vec![Column::new(Some("base"), "b")],
                    output_column: Column::named("agg"),
                    kind: Extremum::Max,
                },
                vec![Column::new(Some("base"), "b"), Column::named("agg")],
            );
        }

        #[test]
        fn project() {
            has_columns_single_parent(
                MirNodeInner::Project {
                    emit: vec![
                        ProjectExpr::Column(Column::new(Some("base"), "b")),
                        ProjectExpr::Expr {
                            alias: "expr".into(),
                            expr: Expr::BinaryOp {
                                lhs: Box::new(Expr::Column("base.a".into())),
                                op: BinaryOperator::Add,
                                rhs: Box::new(Expr::Literal(1.into())),
                            },
                        },
                        ProjectExpr::Expr {
                            alias: "lit".into(),
                            expr: Expr::Literal(2.into()),
                        },
                    ],
                },
                vec![
                    Column::new(Some("base"), "b"),
                    Column::named("expr"),
                    Column::named("lit"),
                ],
            )
        }

        #[test]
        fn filter() {
            same_columns_as_parent(MirNodeInner::Filter {
                conditions: Expr::BinaryOp {
                    lhs: Box::new(Expr::Column("base.a".into())),
                    op: BinaryOperator::Equal,
                    rhs: Box::new(Expr::Literal(1.into())),
                },
            })
        }

        #[test]
        fn join() {
            let mut graph = MirGraph::new();
            let base1 = base1(&mut graph);
            let base2 = base2(&mut graph);

            let project = vec![
                Column {
                    table: Some("base".into()),
                    name: "a".into(),
                    aliases: vec![Column::new(Some("base2"), "a")],
                },
                Column::new(Some("base"), "b"),
                Column::new(Some("base2"), "b"),
            ];
            let j = graph.add_node(MirNode::new(
                "j".into(),
                MirNodeInner::Join {
                    on: vec![(
                        Column::new(Some("base"), "a"),
                        Column::new(Some("base2"), "a"),
                    )],
                    project: project.clone(),
                },
            ));
            graph.add_edge(base1, j, 0);
            graph.add_edge(base2, j, 1);

            let cols = graph.columns(j);
            assert_eq!(cols, project);
        }

        #[test]
        fn union() {
            let mut graph = MirGraph::new();
            let base1 = base1(&mut graph);
            let base2 = base2(&mut graph);

            let base2_alias_table = graph.add_node(MirNode::new(
                "base2_alias_table".into(),
                MirNodeInner::AliasTable {
                    table: "base".into(),
                },
            ));
            graph.add_edge(base2, base2_alias_table, 0);

            let union = graph.add_node(MirNode::new(
                "union".into(),
                MirNodeInner::Union {
                    emit: vec![
                        vec![
                            Column::new(Some("base"), "a"),
                            Column::new(Some("base"), "b"),
                        ],
                        vec![
                            Column::new(Some("base"), "a"),
                            Column::new(Some("base"), "b"),
                        ],
                    ],
                    duplicate_mode: DuplicateMode::BagUnion,
                },
            ));
            graph.add_edge(base1, union, 0);
            graph.add_edge(base2_alias_table, union, 1);

            let cols = graph.columns(union);
            assert_eq!(
                cols,
                vec![
                    Column::new(Some("base"), "a"),
                    Column::new(Some("base"), "b")
                ]
            );
        }

        #[test]
        fn leaf() {
            same_columns_as_parent(MirNodeInner::Leaf {
                keys: vec![(
                    Column::new(Some("base"), "a"),
                    ViewPlaceholder::OneToOne(1, BinaryOperator::Equal),
                )],
                index_type: IndexType::HashMap,
                lowered_to_df: false,
                order_by: None,
                limit: None,
                returned_cols: None,
                default_row: None,
                aggregates: None,
            })
        }

        #[test]
        fn topk() {
            same_columns_as_parent(MirNodeInner::TopK {
                order: Some(vec![(
                    Column::new(Some("base"), "a"),
                    OrderType::OrderAscending,
                )]),
                group_by: vec![Column::new(Some("base"), "b")],
                limit: 3,
            })
        }

        #[test]
        fn paginate() {
            has_columns_single_parent(
                MirNodeInner::Paginate {
                    order: Some(vec![(
                        Column::new(Some("base"), "a"),
                        OrderType::OrderAscending,
                    )]),
                    group_by: vec![Column::new(Some("base"), "b")],
                    limit: 3,
                },
                vec![
                    Column::new(Some("base"), "a"),
                    Column::new(Some("base"), "b"),
                    Column::named("__page_number"),
                ],
            )
        }

        #[test]
        fn join_aggregates() {
            let mut graph = MirGraph::new();
            let base = base1(&mut graph);
            let count = graph.add_node(MirNode::new(
                "count".into(),
                MirNodeInner::Aggregation {
                    on: Column::new(Some("base"), "a"),
                    group_by: vec![Column::new(Some("base"), "b")],
                    output_column: Column::named("count"),
                    kind: Aggregation::Count,
                },
            ));
            graph.add_edge(base, count, 0);

            let sum = graph.add_node(MirNode::new(
                "count".into(),
                MirNodeInner::Aggregation {
                    on: Column::new(Some("base"), "a"),
                    group_by: vec![Column::new(Some("base"), "b")],
                    output_column: Column::named("sum"),
                    kind: Aggregation::Sum,
                },
            ));
            graph.add_edge(base, sum, 0);

            let ja = graph.add_node(MirNode::new(
                "join_aggregates".into(),
                MirNodeInner::JoinAggregates,
            ));
            graph.add_edge(count, ja, 0);
            graph.add_edge(sum, ja, 1);

            let columns = graph.columns(ja);
            assert_eq!(
                columns,
                vec![
                    Column::new(Some("base"), "b"),
                    Column::named("count"),
                    Column::named("sum"),
                ]
            )
        }

        #[test]
        fn join_aggregates_with_dupes_at_end() {
            let mut graph = MirGraph::new();
            let agg1 = graph.add_node(MirNode::new(
                "agg1".into(),
                MirNodeInner::Aggregation {
                    on: Column::named("c"),
                    group_by: vec![Column::named("a"), Column::named("b")],
                    output_column: Column::named("sum(c)"),
                    kind: Aggregation::Sum,
                },
            ));

            let agg2 = graph.add_node(MirNode::new(
                "agg2".into(),
                MirNodeInner::Aggregation {
                    on: Column::named("c"),
                    group_by: vec![Column::named("a"), Column::named("b")],
                    output_column: Column::named("sum(c)"),
                    kind: Aggregation::Sum,
                },
            ));

            let join_aggs = graph.add_node(MirNode::new(
                "join_aggs".into(),
                MirNodeInner::JoinAggregates,
            ));
            graph.add_edge(agg1, join_aggs, 0);
            graph.add_edge(agg2, join_aggs, 1);

            let columns = graph.columns(join_aggs);
            assert_eq!(
                columns,
                vec![
                    Column::named("a"),
                    Column::named("b"),
                    Column::named("sum(c)")
                ]
            );
        }

        #[test]
        fn distinct() {
            has_columns_single_parent(
                MirNodeInner::Distinct {
                    group_by: vec![Column::new(Some("base"), "a")],
                },
                vec![
                    Column::new(Some("base"), "a"),
                    Column::named("__distinct_count"),
                ],
            )
        }
    }

    mod referenced_columns {
        use dataflow::ops::grouped::aggregate::Aggregation;
        use nom_sql::{Expr, Literal};

        use super::*;
        use crate::graph::MirGraph;
        use crate::Column;

        #[test]
        fn project_doesnt_include_literal_and_expr_names() {
            let mut graph = MirGraph::new();
            let node = graph.add_node(MirNode::new(
                "project".into(),
                MirNodeInner::Project {
                    emit: vec![
                        ProjectExpr::Column(Column::new(Some("base"), "project")),
                        ProjectExpr::Expr {
                            alias: "expr".into(),
                            expr: Expr::Literal(Literal::from(0)),
                        },
                        ProjectExpr::Expr {
                            alias: "literal".into(),
                            expr: Expr::Literal(0.into()),
                        },
                    ],
                },
            ));
            let referenced = graph.referenced_columns(node);
            assert_eq!(referenced, vec![Column::new(Some("base"), "project")])
        }

        #[test]
        fn aggregate() {
            let mut graph = MirGraph::new();
            let node = graph.add_node(MirNode::new(
                "aggregate".into(),
                MirNodeInner::Aggregation {
                    on: Column::named("on"),
                    group_by: vec![Column::named("gb_a"), Column::named("gb_b")],
                    output_column: Column::named("output"),
                    kind: Aggregation::Count,
                },
            ));
            let mut referenced = graph.referenced_columns(node);
            referenced.sort_by(|a, b| a.name.cmp(&b.name));
            assert_eq!(
                referenced,
                vec![
                    Column::named("gb_a"),
                    Column::named("gb_b"),
                    Column::named("on"),
                ]
            );
        }
    }

    mod find_source_for_child_column {
        use std::collections::HashSet;

        use nom_sql::{ColumnSpecification, SqlType};

        use crate::graph::MirGraph;
        use crate::node::node_inner::MirNodeInner;
        use crate::node::MirNode;
        use crate::Column;

        // tests the simple case where the child column has no alias, therefore mapping to the
        // parent column with the same name
        #[test]
        fn with_no_alias() {
            let cspec = |n: &str| -> ColumnSpecification {
                ColumnSpecification::new(nom_sql::Column::from(n), SqlType::Text)
            };

            let mut graph = MirGraph::new();
            let a = graph.add_node(MirNode {
                name: "a".into(),
                owners: HashSet::new(),
                inner: MirNodeInner::Base {
                    pg_meta: None,
                    column_specs: vec![cspec("c1"), cspec("c2"), cspec("c3")],
                    primary_key: Some([Column::from("c1")].into()),
                    unique_keys: Default::default(),
                },
                df_node_index: None,
            });

            let child_column = Column::from("c3");

            let idx = graph
                .find_source_for_child_column(a, &child_column)
                .unwrap();
            assert_eq!(idx, 2);
        }

        // tests the case where the child column has an alias, therefore mapping to the parent
        // column with the same name as the alias
        #[test]
        fn with_alias() {
            let child_column = Column {
                table: Some("table".into()),
                name: "child".into(),
                aliases: vec![Column {
                    table: Some("table".into()),
                    name: "c3".into(),
                    aliases: vec![],
                }],
            };

            let cspec = |n: &str| -> ColumnSpecification {
                ColumnSpecification::new(
                    nom_sql::Column {
                        name: n.into(),
                        table: Some("table".into()),
                    },
                    SqlType::Text,
                )
            };

            let mut graph = MirGraph::new();
            let a = graph.add_node(MirNode {
                name: "a".into(),
                owners: HashSet::new(),
                inner: MirNodeInner::Base {
                    pg_meta: None,
                    column_specs: vec![cspec("c1"), cspec("c2"), cspec("c3")],
                    primary_key: Some([Column::from("c1")].into()),
                    unique_keys: Default::default(),
                },
                df_node_index: None,
            });

            let idx = graph
                .find_source_for_child_column(a, &child_column)
                .unwrap();
            assert_eq!(idx, 2);
        }

        // tests the case where the child column is named the same thing as a parent column BUT has
        // an alias. Typically, this alias would map to a different parent column however
        // for testing purposes that column is missing here to ensure it will not match with
        // the wrong column.
        #[test]
        fn with_alias_to_parent_column() {
            let child_column = Column {
                table: Some("table".into()),
                name: "c1".into(),
                aliases: vec![Column {
                    table: Some("table".into()),
                    name: "other_name".into(),
                    aliases: vec![],
                }],
            };

            let cspec = |n: &str| -> ColumnSpecification {
                ColumnSpecification::new(nom_sql::Column::from(n), SqlType::Text)
            };

            let mut graph = MirGraph::new();
            let a = graph.add_node(MirNode {
                name: "a".into(),
                owners: HashSet::new(),
                inner: MirNodeInner::Base {
                    pg_meta: None,
                    column_specs: vec![cspec("c1")],
                    primary_key: Some([Column::from("c1")].into()),
                    unique_keys: Default::default(),
                },
                df_node_index: None,
            });

            assert_eq!(graph.find_source_for_child_column(a, &child_column), None);
        }
    }
}
