use readyset_errors::ReadySetResult;
use tracing::{trace, trace_span};

use crate::column::Column;
use crate::query::MirQuery;

pub(crate) fn pull_all_required_columns(query: &mut MirQuery<'_>) -> ReadySetResult<()> {
    let mut queue = vec![query.leaf()];

    while let Some(mn) = queue.pop() {
        let span = trace_span!("pulling columns", node = %mn.index());
        let _guard = span.enter();

        // a node needs all of the columns it projects into its output
        // however, it may also need *additional* columns to perform its functionality; consider,
        // e.g., a filter that filters on a column that it doesn't project
        let parent_cols = query
            .ancestors(mn)?
            .iter()
            .flat_map(|&a| query.graph.columns(a))
            .collect::<Vec<_>>();
        let needed_columns: Vec<Column> = query
            .graph
            .referenced_columns(mn)
            .into_iter()
            .filter(|c| !parent_cols.contains(c))
            .collect();

        let mut found: Vec<&Column> = Vec::new();
        for parent_idx in query.ancestors(mn)? {
            if query.is_root(parent_idx) {
                // base, do nothing
                continue;
            }

            for c in &needed_columns {
                if !found.contains(&c) {
                    trace!(
                        column = %c,
                        parent = %parent_idx.index(),
                        "Node needs column, looking in parent"
                    );
                    if query.graph.provides_column(parent_idx, c) {
                        trace!(
                            column = %c,
                            parent = %parent_idx.index(),
                            "Found column in parent"
                        );
                        query.graph.add_column(parent_idx, c.clone())?;
                        found.push(c);
                    }
                }
            }
            queue.push(parent_idx);
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use common::IndexType;
    use dataflow::ops::grouped::aggregate::Aggregation;
    use nom_sql::{
        BinaryOperator, ColumnSpecification, Expr, FunctionExpr, Literal, Relation, SqlType,
    };
    use readyset_client::ViewPlaceholder;

    use super::*;
    use crate::graph::MirGraph;
    use crate::node::node_inner::MirNodeInner;
    use crate::node::{MirNode, ProjectExpr};
    use crate::NodeIndex;

    fn create_base_node(mir_graph: &mut MirGraph) -> NodeIndex {
        mir_graph.add_node(MirNode::new(
            "base".into(),
            MirNodeInner::Base {
                column_specs: vec![
                    ColumnSpecification {
                        column: nom_sql::Column::from("a"),
                        sql_type: SqlType::Int(None),
                        constraints: vec![],
                        comment: None,
                    },
                    ColumnSpecification {
                        column: nom_sql::Column::from("b"),
                        sql_type: SqlType::Int(None),
                        constraints: vec![],
                        comment: None,
                    },
                    ColumnSpecification {
                        column: nom_sql::Column::from("c"),
                        sql_type: SqlType::Int(None),
                        constraints: vec![],
                        comment: None,
                    },
                ],
                primary_key: Some([Column::from("a")].into()),
                unique_keys: Default::default(),
            },
        ))
    }

    #[test]
    fn changing_index() {
        let query_name: Relation = "changing_index".into();
        let mut mir_graph = MirGraph::new();
        let base = create_base_node(&mut mir_graph);
        mir_graph[base].add_owner(query_name.clone());

        // SUM(b)
        let grp = mir_graph.add_node(MirNode::new(
            "grp".into(),
            MirNodeInner::Aggregation {
                on: "b".into(),
                group_by: vec![],
                output_column: Column::named("agg"),
                kind: Aggregation::Sum,
            },
        ));
        mir_graph[grp].add_owner(query_name.clone());
        mir_graph.add_edge(base, grp, 0);

        let condition_expression_1 = Expr::BinaryOp {
            op: BinaryOperator::Equal,
            lhs: Box::new(Expr::Column("a".into())),
            rhs: Box::new(Expr::Literal(Literal::Integer(1))),
        };

        // σ[a = 1]
        let fil = mir_graph.add_node(MirNode::new(
            "fil".into(),
            MirNodeInner::Filter {
                conditions: condition_expression_1.clone(),
            },
        ));
        mir_graph[fil].add_owner(query_name.clone());
        mir_graph.add_edge(grp, fil, 0);

        // a, agg, IFNULL(c, 0) as c0
        let prj = mir_graph.add_node(MirNode::new(
            "prj".into(),
            MirNodeInner::Project {
                emit: vec![
                    ProjectExpr::Column("a".into()),
                    ProjectExpr::Column("agg".into()),
                    ProjectExpr::Expr {
                        alias: "c0".into(),
                        expr: Expr::Call(FunctionExpr::Call {
                            name: "ifnull".into(),
                            arguments: vec![Expr::Column("c".into()), Expr::Literal(0.into())],
                        }),
                    },
                ],
            },
        ));
        mir_graph[prj].add_owner(query_name.clone());
        mir_graph.add_edge(fil, prj, 0);

        let mut query = MirQuery::new(query_name, prj, &mut mir_graph);

        pull_all_required_columns(&mut query).unwrap();

        assert_eq!(
            mir_graph.columns(grp),
            &[Column::from("a"), Column::from("c"), Column::from("agg")]
        );

        // The filter has to add the column in the same place as the aggregate
        assert_eq!(
            mir_graph.columns(fil),
            &[Column::from("a"), Column::from("c"), Column::from("agg")]
        );

        // The filter has to filter on the correct field
        match &mir_graph[fil].inner {
            MirNodeInner::Filter { conditions, .. } => {
                assert_eq!(conditions, &condition_expression_1)
            }
            _ => unreachable!(),
        };
    }

    #[test]
    fn unprojected_leaf_key() {
        let query_name: Relation = "unprojected_leaf_key".into();
        let mut mir_graph = MirGraph::new();
        let base = create_base_node(&mut mir_graph);
        mir_graph[base].add_owner(query_name.clone());

        let prj = mir_graph.add_node(MirNode::new(
            "prj".into(),
            MirNodeInner::Project {
                emit: vec![ProjectExpr::Column("a".into())],
            },
        ));
        mir_graph[prj].add_owner(query_name.clone());
        mir_graph.add_edge(base, prj, 0);

        let alias_table = mir_graph.add_node(MirNode::new(
            "alias_table".into(),
            MirNodeInner::AliasTable {
                table: "unprojected_leaf_key".into(),
            },
        ));
        mir_graph[alias_table].add_owner(query_name.clone());
        mir_graph.add_edge(prj, alias_table, 0);

        let leaf = mir_graph.add_node(MirNode::new(
            "leaf".into(),
            MirNodeInner::leaf(
                vec![(
                    Column::named("b").aliased_as_table("unprojected_leaf_key"),
                    ViewPlaceholder::OneToOne(1, BinaryOperator::Equal),
                )],
                IndexType::HashMap,
            ),
        ));
        mir_graph[leaf].add_owner(query_name.clone());
        mir_graph.add_edge(alias_table, leaf, 0);

        let mut query = MirQuery::new(query_name, leaf, &mut mir_graph);

        pull_all_required_columns(&mut query).unwrap();

        assert_eq!(
            mir_graph.columns(prj),
            vec![Column::named("a"), Column::named("b")]
        );
    }
}
