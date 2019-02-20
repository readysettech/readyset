use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt;

use prelude::*;

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
    fn apply(
        &self,
        current: Option<&DataType>,
        diffs: &mut Iterator<Item = Self::Diff>,
    ) -> DataType;

    fn description(&self, detailed: bool) -> String;
    fn over_columns(&self) -> Vec<usize>;
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

    pub fn over_columns(&self) -> Vec<usize> {
        self.inner.over_columns()
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
            .filter(|col| {
                // since the generated value goes at the end,
                // this is the n'th output value
                // otherwise this column does not appear in output
                self.group_by.iter().any(|c| c == col)
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
        _: &mut Executor,
        from: LocalNodeIndex,
        rs: Records,
        _: &mut Tracer,
        replay_key_cols: Option<&[usize]>,
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

        let group_by = &self.group_by;
        let cmp = |a: &Record, b: &Record| {
            group_by
                .iter()
                .map(|&col| &a[col])
                .cmp(group_by.iter().map(|&col| &b[col]))
        };

        // First, we want to be smart about multiple added/removed rows with same group.
        // For example, if we get a -, then a +, for the same group, we don't want to
        // execute two queries. We'll do this by sorting the batch by our group by.
        let mut rs: Vec<_> = rs.into();
        rs.sort_by(&cmp);

        // find the current value for this group
        let us = self.us.unwrap();
        let db = state
            .get(*us)
            .expect("grouped operators must have their own state materialized");

        let mut misses = Vec::new();
        let mut out = Vec::new();
        {
            let out_key = &self.out_key;
            let mut handle_group =
                |inner: &mut T,
                 group_rs: ::std::vec::Drain<Record>,
                 mut diffs: ::std::vec::Drain<_>| {
                    let mut group_rs = group_rs.peekable();

                    let mut group = Vec::with_capacity(group_by.len() + 1);
                    {
                        let group_r = group_rs.peek().unwrap();
                        let mut group_by_i = 0;
                        for (col, v) in group_r.iter().enumerate() {
                            if col == group_by[group_by_i] {
                                group.push(v.clone());
                                group_by_i += 1;
                                if group_by_i == group_by.len() {
                                    break;
                                }
                            }
                        }
                    }

                    let rs = {
                        match db.lookup(&out_key[..], &KeyType::from(&group[..])) {
                            LookupResult::Some(rs) => {
                                debug_assert!(rs.len() <= 1, "a group had more than 1 result");
                                rs
                            }
                            LookupResult::Missing => {
                                misses.extend(group_rs.map(|r| Miss {
                                    on: *us,
                                    lookup_idx: out_key.clone(),
                                    lookup_cols: group_by.clone(),
                                    replay_cols: replay_key_cols.map(Vec::from),
                                    record: r.extract().0,
                                }));
                                return;
                            }
                        }
                    };

                    let old = rs.into_iter().next();
                    // current value is in the last output column
                    // or "" if there is no current group
                    let current = old.as_ref().map(|rows| match rows {
                        Cow::Borrowed(rs) => Cow::Borrowed(&rs[rs.len() - 1]),
                        Cow::Owned(rs) => Cow::Owned(rs[rs.len() - 1].clone()),
                    });

                    // new is the result of applying all diffs for the group to the current value
                    let new = inner.apply(current.as_ref().map(|v| &**v), &mut diffs as &mut _);
                    match current {
                        Some(ref current) if new == **current => {
                            // no change
                        }
                        _ => {
                            if let Some(old) = old {
                                // revoke old value
                                debug_assert!(current.is_some());
                                out.push(Record::Negative(old.into_owned()));
                            }

                            // emit positive, which is group + new.
                            let mut rec = group;
                            rec.push(new);
                            out.push(Record::Positive(rec));
                        }
                    }
                };

            let mut diffs = Vec::new();
            let mut group_rs = Vec::new();
            for r in rs {
                if !group_rs.is_empty() && cmp(&group_rs[0], &r) != Ordering::Equal {
                    handle_group(&mut self.inner, group_rs.drain(..), diffs.drain(..));
                }

                diffs.push(self.inner.to_diff(&r[..], r.is_positive()));
                group_rs.push(r);
            }
            assert!(!diffs.is_empty());
            handle_group(&mut self.inner, group_rs.drain(..), diffs.drain(..));
        }

        ProcessingResult {
            results: out.into(),
            misses,
        }
    }

    fn suggest_indexes(&self, this: NodeIndex) -> HashMap<NodeIndex, (Vec<usize>, bool)> {
        // index by our primary key
        Some((this, (self.out_key.clone(), true)))
            .into_iter()
            .collect()
    }

    fn resolve(&self, col: usize) -> Option<Vec<(NodeIndex, usize)>> {
        if col == self.colfix.len() {
            return None;
        }
        Some(vec![(self.src.as_global(), self.colfix[col])])
    }

    fn description(&self, detailed: bool) -> String {
        self.inner.description(detailed)
    }

    fn parent_columns(&self, column: usize) -> Vec<(NodeIndex, Option<usize>)> {
        if column == self.colfix.len() {
            return vec![(self.src.as_global(), None)];
        }
        vec![(self.src.as_global(), Some(self.colfix[column]))]
    }

    fn is_selective(&self) -> bool {
        true
    }
}
