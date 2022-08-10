use readyset_errors::ReadySetResult;

use crate::column::Column;
use crate::query::MirQuery;

pub(crate) fn pull_all_required_columns(query: &mut MirQuery<'_>) -> ReadySetResult<()> {
    let mut queue = vec![query.leaf()];

    while let Some(mn) = queue.pop() {
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
                if !found.contains(&c) && query.graph.provides_column(parent_idx, c) {
                    query.graph.add_column(parent_idx, c.clone())?;
                    found.push(c);
                }
            }
            queue.push(parent_idx);
        }
    }

    Ok(())
}

// TODO(fran): Uncomment and fix compilation issues
// #[cfg(test)]
// mod tests {
//     use common::IndexType;
//     use dataflow::ops::grouped::aggregate::Aggregation;
//     use nom_sql::{BinaryOperator, ColumnSpecification, Expr, FunctionExpr, Literal, SqlType};
//     use readyset::ViewPlaceholder;
//     use crate::graph::MirSupergraph;
//
//     use super::*;
//     use crate::node::node_inner::MirNodeInner;
//     use crate::node::MirNode;
//
//     #[test]
//     fn changing_index() {
//         let mut supergraph = MirSupergraph::new();
//         let base = MirNode::temp_new(
//             "base".into(),
//             MirNodeInner::Base {
//                 column_specs: vec![
//                     (
//                         ColumnSpecification {
//                             column: nom_sql::Column::from("a"),
//                             sql_type: SqlType::Int(None),
//                             constraints: vec![],
//                             comment: None,
//                         },
//                         None,
//                     ),
//                     (
//                         ColumnSpecification {
//                             column: nom_sql::Column::from("b"),
//                             sql_type: SqlType::Int(None),
//                             constraints: vec![],
//                             comment: None,
//                         },
//                         None,
//                     ),
//                     (
//                         ColumnSpecification {
//                             column: nom_sql::Column::from("c"),
//                             sql_type: SqlType::Int(None),
//                             constraints: vec![],
//                             comment: None,
//                         },
//                         None,
//                     ),
//                 ],
//                 temp_column_specs: vec![
//                     ColumnSpecification {
//                         column: nom_sql::Column::from("a"),
//                         sql_type: SqlType::Int(None),
//                         constraints: vec![],
//                         comment: None,
//                     },
//                     ColumnSpecification {
//                         column: nom_sql::Column::from("b"),
//                         sql_type: SqlType::Int(None),
//                         constraints: vec![],
//                         comment: None,
//                     },
//                     ColumnSpecification {
//                         column: nom_sql::Column::from("c"),
//                         sql_type: SqlType::Int(None),
//                         constraints: vec![],
//                         comment: None,
//                     },
//                 ],
//                 primary_key: Some([Column::from("a")].into()),
//                 unique_keys: Default::default(),
//                 adapted_over: None,
//             },
//         );
//
//         // SUM(b)
//         let grp = MirNode::temp_new(
//             "grp".into(),
//             MirNodeInner::Aggregation {
//                 on: "b".into(),
//                 group_by: vec![],
//                 output_column: Column::named("agg"),
//                 kind: Aggregation::Sum,
//             },
//         );
//
//         let condition_expression_1 = Expr::BinaryOp {
//             op: BinaryOperator::Equal,
//             lhs: Box::new(Expr::Column("a".into())),
//             rhs: Box::new(Expr::Literal(Literal::Integer(1))),
//         };
//
//         // σ[a = 1]
//         let fil = MirNode::new(
//             "fil".into(),
//             0,
//             MirNodeInner::Filter {
//                 conditions: condition_expression_1.clone(),
//             },
//             vec![MirNodeRef::downgrade(&grp)],
//             vec![],
//         );
//
//         // a, agg, IFNULL(c, 0) as c0
//         let prj = MirNode::new(
//             "prj".into(),
//             0,
//             MirNodeInner::Project {
//                 emit: vec!["a".into(), "agg".into()],
//                 expressions: vec![(
//                     "c0".into(),
//                     Expr::Call(FunctionExpr::Call {
//                         name: "ifnull".into(),
//                         arguments: vec![Expr::Column("c".into()), Expr::Literal(0.into())],
//                     }),
//                 )],
//                 literals: vec![],
//             },
//             vec![MirNodeRef::downgrade(&fil)],
//             vec![],
//         );
//
//         let mut query = MirQuery {
//             name: "changing_index".into(),
//             roots: vec![base],
//             leaf: prj,
//             fields: vec!["a".into(), "agg".into()],
//         };
//
//         pull_all_required_columns(&mut query).unwrap();
//
//         assert_eq!(
//             grp.borrow().columns(),
//             &[Column::from("a"), Column::from("c"), Column::from("agg")]
//         );
//
//         // The filter has to add the column in the same place as the aggregate
//         assert_eq!(
//             fil.borrow().columns(),
//             &[Column::from("a"), Column::from("c"), Column::from("agg")]
//         );
//
//         // The filter has to filter on the correct field
//         match &fil.borrow().inner {
//             MirNodeInner::Filter { conditions, .. } => {
//                 assert_eq!(conditions, &condition_expression_1)
//             }
//             _ => unreachable!(),
//         };
//     }
//
//     #[test]
//     fn unprojected_leaf_key() {
//         let base = MirNode::new(
//             "base".into(),
//             0,
//             MirNodeInner::Base {
//                 column_specs: vec![
//                     (
//                         ColumnSpecification {
//                             column: nom_sql::Column::from("a"),
//                             sql_type: SqlType::Int(None),
//                             constraints: vec![],
//                             comment: None,
//                         },
//                         None,
//                     ),
//                     (
//                         ColumnSpecification {
//                             column: nom_sql::Column::from("b"),
//                             sql_type: SqlType::Int(None),
//                             constraints: vec![],
//                             comment: None,
//                         },
//                         None,
//                     ),
//                     (
//                         ColumnSpecification {
//                             column: nom_sql::Column::from("c"),
//                             sql_type: SqlType::Int(None),
//                             constraints: vec![],
//                             comment: None,
//                         },
//                         None,
//                     ),
//                 ],
//                 temp_column_specs: vec![
//                     ColumnSpecification {
//                         column: nom_sql::Column::from("a"),
//                         sql_type: SqlType::Int(None),
//                         constraints: vec![],
//                         comment: None,
//                     },
//                     ColumnSpecification {
//                         column: nom_sql::Column::from("b"),
//                         sql_type: SqlType::Int(None),
//                         constraints: vec![],
//                         comment: None,
//                     },
//                     ColumnSpecification {
//                         column: nom_sql::Column::from("c"),
//                         sql_type: SqlType::Int(None),
//                         constraints: vec![],
//                         comment: None,
//                     },
//                 ],
//                 primary_key: Some([Column::from("a")].into()),
//                 unique_keys: Default::default(),
//                 adapted_over: None,
//             },
//             vec![],
//             vec![],
//         );
//
//         let prj = MirNode::new(
//             "prj".into(),
//             0,
//             MirNodeInner::Project {
//                 emit: vec!["a".into()],
//                 expressions: vec![],
//                 literals: vec![],
//             },
//             vec![MirNodeRef::downgrade(&base)],
//             vec![],
//         );
//
//         let alias_table = MirNode::new(
//             "alias_table".into(),
//             0,
//             MirNodeInner::AliasTable {
//                 table: "unprojected_leaf_key".into(),
//             },
//             vec![MirNodeRef::downgrade(&prj)],
//             vec![],
//         );
//
//         let leaf = MirNode::new(
//             "leaf".into(),
//             0,
//             MirNodeInner::leaf(
//                 vec![(
//                     Column::named("b").aliased_as_table("unprojected_leaf_key"),
//                     ViewPlaceholder::OneToOne(1),
//                 )],
//                 IndexType::HashMap,
//             ),
//             vec![MirNodeRef::downgrade(&alias_table)],
//             vec![],
//         );
//
//         let mut query = MirQuery {
//             name: "unprojected_leaf_key".into(),
//             roots: vec![base],
//             leaf,
//             fields: vec!["a".into()],
//         };
//
//         pull_all_required_columns(&mut query).unwrap();
//
//         assert_eq!(
//             prj.borrow().columns(),
//             vec![Column::named("a"), Column::named("b")]
//         );
//     }
// }
