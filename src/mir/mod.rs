use nom_sql::{Column, Operator};
use std::collections::HashMap;
use std::rc::Rc;

use flow::Migration;
use flow::core::{NodeAddress, DataType};
use ops;
use ops::topk::OrderedRecordComparator;
use sql::QueryFlowParts;

pub struct MirQuery {
    pub name: String,
    pub roots: Vec<Rc<MirNode>>,
    pub leaf: Rc<MirNode>,
}

impl MirQuery {
    pub fn singleton(name: &str, node: MirNode) -> MirQuery {
        let rcn = Rc::new(node);
        MirQuery {
            name: String::from(name),
            roots: vec![rcn.clone()],
            leaf: rcn,
        }
    }

    pub fn into_flow_parts(mut self, mut mig: &mut Migration) -> QueryFlowParts {
        let mut new_nodes = Vec::new();

        // starting at the roots, add nodes in topological order
        for n in self.roots.drain(..) {
            new_nodes.extend(n.into_flow_parts(mig));
        }

        let leaf = new_nodes.iter()
            .last()
            .unwrap()
            .clone()
            .into();

        QueryFlowParts {
            name: self.name,
            new_nodes: new_nodes,
            reused_nodes: vec![],
            query_leaf: leaf,
        }
    }

    pub fn optimize(self) -> MirQuery {
        // XXX(malte): currently a no-op
        self
    }
}

pub struct MirNode {
    pub name: String,
    pub from_version: u64,
    pub columns: Vec<Column>,
    pub inner: MirNodeType,
    pub ancestors: Vec<Rc<MirNode>>,
    pub children: Vec<Rc<MirNode>>,
}

impl MirNode {
    pub fn columns(&self) -> &[Column] {
        self.columns.as_slice()
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn versioned_name(&self) -> String {
        format!("{}_v{}", self.name, self.from_version)
    }

    /// Produce a compact, human-readable description of this node; analogous to the method of the
    /// same name on `Ingredient`.
    ///
    ///  Symbol   Description
    /// --------|-------------
    ///    B    |  Base
    ///    ||   |  Concat
    ///    ⧖    |  Latest
    ///    γ    |  Group by
    ///   |*|   |  Count
    ///    𝛴    |  Sum
    ///    ⋈    |  Join
    ///    ⋉    |  Left join
    ///    ⋃    |  Union
    fn description(&self) -> String {
        unimplemented!()
    }

    /// Translate a column in this ingredient into the corresponding column(s) in
    /// parent ingredients. None for the column means that the parent doesn't
    /// have an associated column. Similar to `resolve`, but does not depend on
    /// materialization, and returns results even for computed columns.
    fn parent_columns(&self, column: Column) -> Vec<(String, Option<Column>)> {
        unimplemented!()
    }

    /// Resolve where the given column originates from. If the view is materialized, or the value is
    /// otherwise created by this view, None should be returned.
    fn resolve_column(&self, column: Column) -> Option<Vec<(String, Column)>> {
        unimplemented!()
    }

    fn into_flow_parts(&self, mig: &mut Migration) -> Vec<NodeAddress> {
        let name = self.name.clone();
        self.inner.into_flow_parts(&name, mig)
    }
}
pub enum MirNodeType {
    /// over column, group_by columns
    Aggregation(Column, Vec<Column>),
    /// columns, keys (non-compound)
    Base(Vec<Column>, Vec<Column>),
    /// over column, group_by columns
    Extremum(Column, Vec<Column>),
    /// filter conditions (one for each parent column)
    Filter(Vec<(Operator, DataType)>),
    /// over column, separator
    GroupConcat(Column, String),
    /// no extra info required
    Identity,
    /// on left column, on right column, emit columns
    Join(Column, Column, Vec<Column>),
    /// on left column, on right column, emit columns
    LeftJoin(Column, Column, Vec<Column>),
    /// group columns
    Latest(Vec<Column>),
    /// emit columns
    Project(Vec<Column>),
    /// emit columns
    Permute(Vec<Column>),
    /// emit columns left, emit columns right
    Union(Vec<Column>, Vec<Column>),
    /// order function, group columns, k
    TopK(Box<OrderedRecordComparator>, Vec<Column>, usize),
}

impl MirNodeType {
    fn into_flow_parts(&self, name: &str, mut mig: &mut Migration) -> Vec<NodeAddress> {
        match *self {
            MirNodeType::Base(ref cols, ref keys) => {
                if keys.len() > 0 {
                    let pkey_column_ids = keys.iter()
                        .map(|pkc| {
                                 //assert_eq!(pkc.table.as_ref().unwrap(), name);
                                 cols.iter().position(|c| c == pkc).unwrap()
                             })
                        .collect();
                    let n = mig.add_ingredient(name,
                                               cols.iter()
                                                   .map(|c| &c.name)
                                                   .collect::<Vec<_>>()
                                                   .as_slice(),
                                               ops::base::Base::new(pkey_column_ids));
                    vec![n]
                } else {
                    let n = mig.add_ingredient(name,
                                               cols.iter()
                                                   .map(|c| &c.name)
                                                   .collect::<Vec<_>>()
                                                   .as_slice(),
                                               ops::base::Base::default());
                    vec![n]
                }
            }
            _ => unimplemented!(),
        }
    }
}
