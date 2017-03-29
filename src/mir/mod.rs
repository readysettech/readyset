use nom_sql::{Column, Operator};
use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt::{Error, Formatter, Debug};
use std::rc::Rc;

use flow::Migration;
use flow::core::{NodeAddress, DataType};
use ops;
use ops::grouped::aggregate::Aggregation as AggregationKind;
use ops::grouped::extremum::Extremum as ExtremumKind;
use ops::join::Builder as JoinBuilder;
use ops::permute::Permute;
use ops::topk::OrderedRecordComparator;
use sql::QueryFlowParts;

#[derive(Clone, Debug)]
pub enum FlowNode {
    New(NodeAddress),
    Existing(NodeAddress),
}

pub type MirNodeRef = Rc<RefCell<MirNode>>;

#[derive(Clone, Debug)]
pub struct MirQuery {
    pub name: String,
    pub roots: Vec<MirNodeRef>,
    pub leaf: MirNodeRef,
}

impl MirQuery {
    pub fn singleton(name: &str, node: MirNodeRef) -> MirQuery {
        MirQuery {
            name: String::from(name),
            roots: vec![node.clone()],
            leaf: node,
        }
    }

    pub fn into_flow_parts(&mut self, mut mig: &mut Migration) -> QueryFlowParts {
        use std::collections::VecDeque;

        let mut new_nodes = Vec::new();
        let mut reused_nodes = Vec::new();

        // starting at the roots, add nodes in topological order
        // XXX(malte): topo sort
        let mut node_queue = VecDeque::new();
        node_queue.extend(self.roots.iter().cloned());
        while !node_queue.is_empty() {
            let n = node_queue.pop_front().unwrap();
            let flow_node = n.borrow_mut().into_flow_parts(mig);
            match flow_node {
                FlowNode::New(na) => new_nodes.push(na),
                FlowNode::Existing(na) => reused_nodes.push(na),
            }
            node_queue.extend(n.borrow()
                                  .children
                                  .iter()
                                  .cloned());
        }

        let leaf_na = match *self.leaf
                   .borrow()
                   .flow_node
                   .as_ref()
                   .expect("Leaf must have FlowNode by now") {
            FlowNode::New(na) |
            FlowNode::Existing(na) => na,
        };

        QueryFlowParts {
            name: self.name.clone(),
            new_nodes: new_nodes,
            reused_nodes: reused_nodes,
            query_leaf: leaf_na,
        }
    }

    pub fn optimize(self) -> MirQuery {
        // XXX(malte): currently a no-op
        self
    }
}

pub struct MirNode {
    name: String,
    from_version: usize,
    columns: Vec<Column>,
    inner: MirNodeType,
    ancestors: Vec<MirNodeRef>,
    children: Vec<MirNodeRef>,
    pub flow_node: Option<FlowNode>,
}

impl MirNode {
    pub fn new(name: &str,
               v: usize,
               columns: Vec<Column>,
               inner: MirNodeType,
               ancestors: Vec<MirNodeRef>,
               children: Vec<MirNodeRef>)
               -> Self {
        MirNode {
            name: String::from(name),
            from_version: v,
            columns: columns,
            inner: inner,
            ancestors: ancestors.clone(),
            children: children.clone(),
            flow_node: None,
        }
    }

    pub fn reuse(node: MirNodeRef, v: usize) -> Self {
        let rcn = node.clone();

        MirNode {
            name: node.borrow().name.clone(),
            from_version: v,
            columns: node.borrow().columns.clone(),
            inner: MirNodeType::Reuse { node: rcn },
            ancestors: node.borrow().ancestors.clone(),
            children: node.borrow().children.clone(),
            flow_node: None, // will be set in `into_flow_parts`
        }
    }

    pub fn add_ancestor(&mut self, a: MirNodeRef) {
        self.ancestors.push(a)
    }

    pub fn add_child(&mut self, c: MirNodeRef) {
        self.children.push(c)
    }

    pub fn ancestors(&self) -> &[MirNodeRef] {
        self.ancestors.as_slice()
    }

    pub fn children(&self) -> &[MirNodeRef] {
        self.children.as_slice()
    }

    pub fn columns(&self) -> &[Column] {
        self.columns.as_slice()
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn versioned_name(&self) -> String {
        format!("{}:v{}", self.name, self.from_version)
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
        format!("{}: {} / {} columns",
                self.versioned_name(),
                self.inner.description(),
                self.columns.len())
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

    fn into_flow_parts(&mut self, mig: &mut Migration) -> FlowNode {
        let name = self.name.clone();
        match self.flow_node {
            None => {
                let flow_node = match self.inner {
                    MirNodeType::Base { ref columns, ref keys } => {
                        make_base_node(&name, columns, keys, mig)
                    }
                    MirNodeType::Join { ref on_left, ref on_right, ref project } => {
                        assert_eq!(self.ancestors.len(), 2);
                        let left = self.ancestors[0].clone();
                        let right = self.ancestors[1].clone();
                        make_join_node(&name, left, right, on_left, on_right, project, mig)
                    }
                    MirNodeType::Project { ref emit, ref literals } => {
                        assert_eq!(self.ancestors.len(), 1);
                        let parent = self.ancestors[0].clone();
                        // XXX(malte): fix
                        make_permute_node(&name, parent, emit, mig)
                    }
                    MirNodeType::Permute { ref emit } => {
                        assert_eq!(self.ancestors.len(), 1);
                        let parent = self.ancestors[0].clone();
                        make_permute_node(&name, parent, emit, mig)
                    }
                    MirNodeType::Reuse { ref node } => {
                        match *node.borrow()
                           .flow_node
                           .as_ref()
                           .expect("Reused MirNode must have FlowNode") {
                    // "New" => flow node was originally created for the node that we
                    // are reusing
                    FlowNode::New(na) |
                    // "Existing" => flow node was already reused from some other MIR node
                    FlowNode::Existing(na) => FlowNode::Existing(na),
                }
                    }
                    _ => unimplemented!(),
                };

                self.flow_node = Some(flow_node.clone());
                flow_node
            }
            Some(ref flow_node) => flow_node.clone(),
        }
    }
}

pub enum MirNodeType {
    /// over column, group_by columns
    Aggregation {
        on: Column,
        group_by: Vec<Column>,
        kind: AggregationKind,
    },
    /// columns, keys (non-compound)
    Base {
        columns: Vec<Column>,
        keys: Vec<Column>,
    },
    /// over column, group_by columns
    Extremum {
        on: Column,
        group_by: Vec<Column>,
        kind: ExtremumKind,
    },
    /// filter conditions (one for each parent column)
    Filter { conditions: Vec<Option<(Operator, DataType)>>, },
    /// over column, separator
    GroupConcat { on: Column, separator: String },
    /// no extra info required
    Identity,
    /// left node, right node, on left columns, on right columns, emit columns
    Join {
        on_left: Vec<Column>,
        on_right: Vec<Column>,
        project: Vec<Column>,
    },
    /// on left column, on right column, emit columns
    LeftJoin {
        on_left: Vec<Column>,
        on_right: Vec<Column>,
        project: Vec<Column>,
    },
    /// group columns
    Latest { group_by: Vec<Column> },
    /// emit columns
    Project {
        emit: Vec<Column>,
        literals: Vec<(String, DataType)>,
    },
    /// emit columns
    Permute { emit: Vec<Column> },
    /// emit columns left, emit columns right
    Union {
        emit_left: Vec<Column>,
        emit_right: Vec<Column>,
    },
    /// order function, group columns, k
    TopK {
        order: Box<OrderedRecordComparator>,
        group_by: Vec<Column>,
        k: usize,
    },
    /// reuse another node
    Reuse { node: MirNodeRef },
    /// leaf (reader) node, keys
    Leaf { node: MirNodeRef, keys: Vec<Column> },
}

impl MirNodeType {
    fn description(&self) -> String {
        format!("{:?}", self)
    }
}

impl Debug for MirNode {
    fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
        write!(f, "{}", self.description())
    }
}

impl Debug for MirNodeType {
    fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
        match *self {
            MirNodeType::Base { ref columns, ref keys } => {
                write!(f,
                       "B [⚷: {}]",
                       keys.iter()
                           .map(|c| c.name.as_str())
                           .collect::<Vec<_>>()
                           .join(","))
            }
            MirNodeType::Join { ref on_left, ref on_right, ref project } => write!(f, "⋈ []"),
            MirNodeType::Reuse { ref node } => write!(f, "Reuse [{:#?}]", node),
            MirNodeType::Permute { ref emit } => {
                write!(f,
                       "π [{}]",
                       emit.iter()
                           .map(|c| c.name.as_str())
                           .collect::<Vec<_>>()
                           .join(", "))
            }
            MirNodeType::Project { ref emit, ref literals } => {
                write!(f,
                       "π [{}{}]",
                       emit.iter()
                           .map(|c| c.name.as_str())
                           .collect::<Vec<_>>()
                           .join(", "),
                       if literals.len() > 0 {
                           format!(", lit: {}",
                                   literals.iter()
                                       .map(|&(ref n, ref v)| format!("{}: {}", n, v))
                                       .collect::<Vec<_>>()
                                       .join(", "))
                       } else {
                           format!("")
                       })
            }
            MirNodeType::Filter { ref conditions } => {
                use regex::Regex;

                let escape = |s: &str| Regex::new("([<>])").unwrap().replace_all(s, "\\$1");
                write!(f,
                       "σ[{}]",
                       conditions.iter()
                           .enumerate()
                           .filter_map(|(i, ref e)| match e.as_ref() {
                                           Some(&(ref op, ref x)) => {
                                               Some(format!("f{} {} {}",
                                                            i,
                                                            escape(&format!("{}", op)),
                                                            x))
                                           }
                                           None => None,
                                       })
                           .collect::<Vec<_>>()
                           .as_slice()
                           .join(", "))
            }
            _ => unimplemented!(),
        }
    }
}

fn make_base_node(name: &str,
                  cols: &Vec<Column>,
                  keys: &Vec<Column>,
                  mut mig: &mut Migration)
                  -> FlowNode {
    let node = if keys.len() > 0 {
        let pkey_column_ids = keys.iter()
            .map(|pkc| {
                     //assert_eq!(pkc.table.as_ref().unwrap(), name);
                     cols.iter().position(|c| c == pkc).unwrap()
                 })
            .collect();
        mig.add_ingredient(name,
                           cols.iter()
                               .map(|c| &c.name)
                               .collect::<Vec<_>>()
                               .as_slice(),
                           ops::base::Base::new(pkey_column_ids))
    } else {
        mig.add_ingredient(name,
                           cols.iter()
                               .map(|c| &c.name)
                               .collect::<Vec<_>>()
                               .as_slice(),
                           ops::base::Base::default())
    };
    FlowNode::New(node)
}

fn make_join_node(name: &str,
                  left: MirNodeRef,
                  right: MirNodeRef,
                  on_left: &Vec<Column>,
                  on_right: &Vec<Column>,
                  proj_cols: &Vec<Column>,
                  mut mig: &mut Migration)
                  -> FlowNode {
    let projected_cols_left: Vec<Column> = left.borrow()
        .columns
        .iter()
        .filter(|c| proj_cols.contains(c))
        .cloned()
        .collect();
    let projected_cols_right: Vec<Column> = right.borrow()
        .columns
        .iter()
        .filter(|c| proj_cols.contains(c))
        .cloned()
        .collect();

    let tuples_for_cols = |n: MirNodeRef, cols: &Vec<Column>| -> Vec<(NodeAddress, usize)> {
        cols.iter()
            .map(|c| {
                let na = match *n.borrow()
                           .flow_node
                           .as_ref()
                           .expect("must have flow node") {
                    FlowNode::New(na) => na,
                    FlowNode::Existing(na) => na,
                };
                (na,
                 n.borrow()
                     .columns
                     .iter()
                     .position(|ref nc| *nc == c)
                     .unwrap())
            })
            .collect()
    };

    // non-join columns projected are the union of the ancestors' projected columns
    // TODO(malte): this will need revisiting when we do smart reuse
    let mut join_proj_config = tuples_for_cols(left.clone(), &projected_cols_left);
    join_proj_config.extend(tuples_for_cols(right.clone(), &projected_cols_right));

    // join columns need us to generate join group configs for the operator
    let mut left_join_group = vec![0; projected_cols_left.len()];
    let mut right_join_group = vec![0; projected_cols_right.len()];

    let join_column_pairs = on_left.into_iter().zip(on_right.into_iter());
    for (i, (l, r)) in join_column_pairs.enumerate() {
        // implied equality between i^th left/right column (equi-join only)
        left_join_group[left.borrow()
            .columns
            .iter()
            .position(|ref nc| *nc == l)
            .unwrap()] = i + 1;
        right_join_group[right.borrow()
            .columns
            .iter()
            .position(|ref nc| *nc == r)
            .unwrap()] = i + 1;
    }

    let left_na = match left.borrow().flow_node {
        Some(FlowNode::New(na)) |
        Some(FlowNode::Existing(na)) => na,
        None => panic!("Join parents must have FlowNodes by now!"),
    };
    let right_na = match right.borrow().flow_node {
        Some(FlowNode::New(na)) |
        Some(FlowNode::Existing(na)) => na,
        None => panic!("Join parents must have FlowNodes by now!"),
    };

    let j = JoinBuilder::new(join_proj_config)
        .from(left_na, left_join_group)
        .join(right_na, right_join_group);
    let fields = projected_cols_left.into_iter()
        .chain(projected_cols_right.into_iter())
        .map(|c| c.name.clone())
        .collect::<Vec<String>>();
    let n = mig.add_ingredient(String::from(name), fields.as_slice(), j);

    FlowNode::New(n)
}

fn make_permute_node(name: &str,
                     parent: MirNodeRef,
                     emit: &Vec<Column>,
                     mut mig: &mut Migration)
                     -> FlowNode {
    let fields = emit.iter().map(|c| c.name.clone()).collect::<Vec<String>>();
    let projected_column_ids = emit.iter()
        .map(|c| {
            parent.borrow()
                .columns
                .iter()
                .position(|ref nc| *nc == c)
                .unwrap()
        })
        .collect::<Vec<_>>();
    let parent_na = match *parent.borrow()
               .flow_node
               .as_ref()
               .expect("parent must have flow node") {
        FlowNode::New(na) |
        FlowNode::Existing(na) => na,
    };
    let n = mig.add_ingredient(String::from(name),
                               fields.as_slice(),
                               Permute::new(parent_na, projected_column_ids.as_slice()));
    FlowNode::New(n)
}
