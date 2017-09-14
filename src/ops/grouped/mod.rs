use std::fmt;
use std::collections::HashMap;

use flow::prelude::*;

// pub mod latest;
pub mod aggregate;
pub mod concat;
pub mod extremum;

/// Trait for implementing operations that collapse a group of records into a single record.
///
/// Implementors of this trait can be used as nodes in a `flow::FlowGraph` by wrapping them in a
/// `GroupedOperator`.
///
/// At a high level, the operator is expected to work in the following way:
///
///  - if a group has no records, its aggregated value is `GroupedOperation::zero()`
///  - if a group has one record `r`, its aggregated value is
///
///    ```rust,ignore
///    self.succ(self.zero(), vec![self.one(r, true), _])
///    ```
///
///  - if a group has current value `v` (as returned by `GroupedOperation::succ()`), and a set of
///    records `[rs]` arrives for the group, the updated value is
///
///    ```rust,ignore
///    self.succ(v, rs.map(|(r, is_positive, ts)| (self.one(r, is_positive), ts)).collect())
///    ```
pub trait GroupedOperation: fmt::Debug + Clone {
    /// The type used to represent a single
    type Diff: 'static;

    /// Called once before any other methods in this trait are called.
    ///
    /// Implementors should use this call to initialize any cache state and to pre-compute
    /// optimized configuration structures to quickly execute the other trait methods.
    ///
    /// `parent` is a reference to the single ancestor node of this node in the flow graph.
    fn setup(&mut self, parent: &Node);

    /// List the columns used to group records.
    ///
    /// All records with the same value for the returned columns are assigned to the same group.
    fn group_by(&self) -> &[usize];

    /// Extract the aggregation value from a single record.
    fn to_diff(&self, record: &[DataType], is_positive: bool) -> Self::Diff;

    /// Given the given `current` value, and a number of changes for a group (`diffs`), compute the
    /// updated group value.
    fn apply(&self, current: Option<&DataType>, diffs: Vec<Self::Diff>) -> DataType;

    fn description(&self) -> String;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupedOperator<T: GroupedOperation> {
    src: IndexPair,
    inner: T,

    // some cache state
    us: Option<IndexPair>,
    cols: usize,

    // precomputed datastructures
    group_by: Vec<usize>,
    out_key: Vec<usize>,
    colfix: Vec<usize>,
}

impl<T: GroupedOperation> GroupedOperator<T> {
    pub fn new(src: NodeIndex, op: T) -> GroupedOperator<T> {
        GroupedOperator {
            src: src.into(),
            inner: op,

            us: None,
            cols: 0,
            group_by: Vec::new(),
            out_key: Vec::new(),
            colfix: Vec::new(),
        }
    }
}

impl<T: GroupedOperation + Send + 'static> Ingredient for GroupedOperator<T>
where
    Self: Into<NodeOperator>,
{
    fn take(&mut self) -> NodeOperator {
        Clone::clone(self).into()
    }

    fn ancestors(&self) -> Vec<NodeIndex> {
        vec![self.src.as_global()]
    }

    fn on_connected(&mut self, g: &Graph) {
        let srcn = &g[self.src.as_global()];

        // give our inner operation a chance to initialize
        self.inner.setup(srcn);

        // group by all columns
        self.cols = srcn.fields().len();
        self.group_by.extend(self.inner.group_by().iter().cloned());
        self.group_by.sort();
        // cache the range of our output keys
        self.out_key = (0..self.group_by.len()).collect();

        // build a translation mechanism for going from output columns to input columns
        let colfix: Vec<_> = (0..self.cols)
            .into_iter()
            .filter_map(|col| {
                if self.group_by.iter().any(|c| c == &col) {
                    // since the generated value goes at the end,
                    // this is the n'th output value
                    Some(col)
                } else {
                    // this column does not appear in output
                    None
                }
            })
            .collect();
        self.colfix.extend(colfix.into_iter());
    }

    fn on_commit(&mut self, us: NodeIndex, remap: &HashMap<NodeIndex, IndexPair>) {
        // who's our parent really?
        self.src.remap(remap);

        // who are we?
        self.us = Some(remap[&us]);
    }

    fn on_input(
        &mut self,
        from: LocalNodeIndex,
        rs: Records,
        _: &mut Tracer,
        _: &DomainNodes,
        state: &StateMap,
    ) -> ProcessingResult {
        debug_assert_eq!(from, *self.src);

        if rs.is_empty() {
            return ProcessingResult {
                results: rs,
                misses: vec![],
            };
        }

        // First, we want to be smart about multiple added/removed rows with same group.
        // For example, if we get a -, then a +, for the same group, we don't want to
        // execute two queries.
        let mut consolidate = HashMap::new();
        for rec in rs {
            let val = self.inner.to_diff(&rec[..], rec.is_positive());

            let mut group = rec.extract().0;
            for (i, &col) in self.group_by.iter().enumerate() {
                group[i] = group[col].clone();
            }
            group.resize(self.group_by.len(), DataType::None);
            consolidate.entry(group).or_insert_with(Vec::new).push(val);
        }

        let us = self.us.unwrap();
        let mut misses = Vec::new();
        let mut out = Vec::with_capacity(2 * consolidate.len());
        for (group, diffs) in consolidate {
            // find the current value for this group
            let db = state
                .get(&*us)
                .expect("grouped operators must have their own state materialized");

            let old = match db.lookup(&self.out_key[..], &KeyType::from(&group[..])) {
                LookupResult::Some(rs) => {
                    debug_assert!(rs.len() <= 1, "a group had more than 1 result");
                    rs.get(0)
                }
                LookupResult::Missing => {
                    misses.push(Miss {
                        node: *us,
                        columns: self.out_key.clone(),
                        key: group,
                    });
                    continue;
                }
            };

            let (current, new) = {
                use std::borrow::Cow;

                // current value is in the last output column
                // or "" if there is no current group
                let current = old.map(|r| Cow::Borrowed(&r[r.len() - 1]));

                // new is the result of applying all diffs for the group to the current value
                let new = self.inner.apply(current.as_ref().map(|v| &**v), diffs);
                (current, new)
            };

            match current {
                Some(ref current) if new == **current => {
                    // no change
                }
                _ => {
                    if let Some(old) = old {
                        // revoke old value
                        debug_assert!(current.is_some());
                        out.push(Record::Negative((**old).clone()));
                    }

                    // emit positive, which is group + new.
                    let mut rec = group;
                    rec.push(new);
                    out.push(Record::Positive(rec));
                }
            }
        }

        ProcessingResult {
            results: out.into(),
            misses: misses,
        }
    }

    fn suggest_indexes(&self, this: NodeIndex) -> HashMap<NodeIndex, (Vec<usize>, bool)> {
        // index by our primary key
        Some((this, (self.out_key.clone(), true)))
            .into_iter()
            .collect()
    }

    fn resolve(&self, col: usize) -> Option<Vec<(NodeIndex, usize)>> {
        if col == self.cols - 1 {
            return None;
        }
        Some(vec![(self.src.as_global(), self.colfix[col])])
    }

    fn description(&self) -> String {
        self.inner.description()
    }

    fn parent_columns(&self, column: usize) -> Vec<(NodeIndex, Option<usize>)> {
        if column == self.cols - 1 {
            return vec![(self.src.as_global(), None)];
        }
        vec![(self.src.as_global(), Some(self.colfix[column]))]
    }

    fn is_selective(&self) -> bool {
        true
    }
}
