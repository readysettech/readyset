use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::convert::TryInto;
use std::fmt;

use maplit::hashmap;
use nom_sql::SqlType;
use noria_errors::{internal_err, ReadySetResult};
use serde::{Deserialize, Serialize};

use crate::prelude::*;
use crate::processing::{ColumnSource, IngredientLookupResult, LookupIndex, LookupMode};

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
    fn setup(&mut self, parent: &Node) -> ReadySetResult<()>;

    /// List the columns used to group records.
    ///
    /// All records with the same value for the returned columns are assigned to the same group.
    fn group_by(&self) -> &[usize];

    /// Extract the aggregation value from a single record.
    fn to_diff(&self, record: &[DataType], is_positive: bool) -> ReadySetResult<Self::Diff>;

    /// Given the given `current` value, and a number of changes for a group (`diffs`), compute the
    /// updated group value.
    ///
    /// A return value of [`None`] indicates that the operator has lost the ability to construct
    /// state for the operator, and needs to start from the beginning (eg, an `extremum` had the
    /// extreme value deleted).
    fn apply(
        &self,
        current: Option<&DataType>,
        diffs: &mut dyn Iterator<Item = Self::Diff>,
    ) -> ReadySetResult<Option<DataType>>;

    fn description(&self, detailed: bool) -> String;

    /// The column that is being aggregated over.
    fn over_column(&self) -> usize;

    /// Defines the output column type for the Grouped Operation if possible.
    /// Returns None if the type of of the output varies depending on data type of over column
    /// (e.g. SUM can be either int or float)
    /// Other operators like Count (int) and Concat (text) always have the same column type.
    fn output_col_type(&self) -> Option<SqlType>;

    /// Returns the empty value for this aggregate, if any. Groups that have the empty value in
    /// their output column will be omitted from results
    fn empty_value(&self) -> Option<DataType> {
        None
    }

    /// Returns whether the empty value should be emitted for this aggregate.
    fn emit_empty(&self) -> bool {
        false
    }
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

    pub fn over_column(&self) -> usize {
        self.inner.over_column()
    }

    pub fn output_col_type(&self) -> Option<SqlType> {
        self.inner.output_col_type()
    }
}

/// Extract a copy of all values in the record being targeted by the group
fn get_group_values(group_by: &[usize], row: &Record) -> Vec<DataType> {
    // This attribute is only here, because `is_sorted` is unstable. I didn't
    // want to add a `feature` to the crate for a debug assertion thus I guarded
    // both the `feature` and this assertion are with `cfg(debug)` (see also
    // `lib.rs`)
    #[cfg(debug)]
    debug_assert!(group_by.is_sorted());
    let mut group = Vec::with_capacity(group_by.len() + 1);
    for &group_idx in group_by {
        group.push(row[group_idx].clone())
    }
    debug_assert_eq!(group.len(), group_by.len());
    group
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
        // FIXME(eta): this error should be properly propagated!
        self.inner.setup(srcn).unwrap();

        // group by all columns
        self.cols = srcn.fields().len();
        self.group_by.extend(self.inner.group_by().iter().cloned());
        // cache the range of our output keys
        self.out_key = (0..self.group_by.len()).collect();
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
        replay: &ReplayContext,
        nodes: &DomainNodes,
        state: &StateMap,
    ) -> ReadySetResult<ProcessingResult> {
        debug_assert_eq!(from, *self.src);

        // TODO(peter): We can't just exit if rs is empty because it means that we never emit 0 for
        // empty tables on count operations. It also means we emit an empty result for
        // GROUP_CONCAT(col1) with no group by, when we should emit NULL.
        if rs.is_empty() {
            return Ok(ProcessingResult {
                results: rs,
                ..Default::default()
            });
        }

        let group_by = self.group_by.clone();
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
        let db = state.get(*us).ok_or_else(|| {
            internal_err("grouped operators must have their own state materialized")
        })?;

        let mut misses = Vec::new();
        let mut lookups = Vec::new();
        let mut out = Vec::new();
        {
            let mut handle_group = |this: &mut Self,
                                    group_rs: ::std::vec::Drain<Record>,
                                    mut diffs: ::std::vec::Drain<_>|
             -> ReadySetResult<()> {
                let mut group_rs = group_rs.peekable();

                let group = get_group_values(&group_by, group_rs.peek().unwrap());

                let rs = {
                    match db.lookup(&this.out_key[..], &KeyType::from(&group[..])) {
                        LookupResult::Some(rs) => {
                            if replay.is_partial() {
                                lookups.push(Lookup {
                                    on: *us,
                                    cols: this.out_key.clone(),
                                    key: group
                                        .clone()
                                        .try_into()
                                        .map_err(|_| internal_err("Empty group"))?,
                                });
                            }

                            debug_assert!(rs.len() <= 1, "a group had more than 1 result");
                            rs
                        }
                        LookupResult::Missing => {
                            misses.extend(group_rs.map(|r| {
                                Miss::builder()
                                    .on(*us)
                                    .lookup_idx(this.out_key.clone())
                                    .lookup_key(group_by.clone())
                                    .replay(replay)
                                    .record(r.into_row())
                                    .build()
                            }));
                            return Ok(());
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
                let new = match this.inner.apply(current.as_deref(), &mut diffs as &mut _)? {
                    Some(v) => v,
                    None => {
                        // we lost the grouped state, so we need to start afresh.
                        // let's query the parent for ALL records in this group, and then feed them
                        // through, starting with a blank `current` value.
                        let all_group_rs = {
                            match this.lookup(
                                *this.src,
                                &group_by,
                                &KeyType::from(&group[..]),
                                nodes,
                                state,
                                LookupMode::Strict,
                            )? {
                                IngredientLookupResult::Miss => {
                                    // We missed in our parent! This is fine, we can just emit a
                                    // miss and drop the write like normal.
                                    //
                                    // Note that despite what you may think (and what we thought
                                    // originally), this *doesn't* need to do any downstream
                                    // evictions the way joins do (see [note:
                                    // downstream-join-evictions] for more about that). This is
                                    // because if we miss, that means our child *can't* have this
                                    // key, so any update we'd emit would hit a hole anyway! See
                                    // also: https://readysettech.atlassian.net/browse/ENG-471
                                    misses.extend(group_rs.map(|r| {
                                        Miss::builder()
                                            .on(*this.src)
                                            .lookup_idx(group_by.clone())
                                            .lookup_key(group_by.clone())
                                            .replay(replay)
                                            .record(r.into_row())
                                            .build()
                                    }));
                                    return Ok(());
                                }
                                IngredientLookupResult::Records(rs) => {
                                    if replay.is_partial() {
                                        lookups.push(Lookup {
                                            on: *this.src,
                                            cols: group_by.clone(),
                                            key: group
                                                .clone()
                                                .try_into()
                                                .map_err(|_| internal_err("Empty group"))?,
                                        });
                                    }
                                    rs.into_iter()
                                        .map(|x| match x {
                                            Ok(r) => Ok(r.into_owned()),
                                            Err(e) => Err(e),
                                        })
                                        .collect::<ReadySetResult<Vec<_>>>()?
                                }
                            }
                        };
                        let diffs = all_group_rs
                            .into_iter()
                            .map(|x| this.inner.to_diff(&x, true))
                            .collect::<ReadySetResult<Vec<_>>>()?;
                        this.inner
                            .apply(None, &mut diffs.into_iter())?
                            .unwrap_or_else(|| this.inner.empty_value().unwrap_or(DataType::None))
                    }
                };
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

                        // emit positive, which is group + new, unless it's the empty value
                        // For some aggregates, if there is no group by then we should still
                        // emit the zero value rather than ignore it.
                        if this.inner.emit_empty() || !this.inner.empty_value().contains(&new) {
                            let mut rec = group;
                            rec.push(new);
                            out.push(Record::Positive(rec));
                        }
                    }
                }
                Ok(())
            };

            let mut diffs = Vec::new();
            let mut group_rs = Vec::new();
            for r in rs {
                if !group_rs.is_empty() && cmp(&group_rs[0], &r) != Ordering::Equal {
                    handle_group(self, group_rs.drain(..), diffs.drain(..))?;
                }
                diffs.push(self.inner.to_diff(&r[..], r.is_positive())?);
                group_rs.push(r);
            }
            handle_group(self, group_rs.drain(..), diffs.drain(..))?;
        }

        Ok(ProcessingResult {
            results: out.into(),
            lookups,
            misses,
        })
    }

    fn suggest_indexes(&self, this: NodeIndex) -> HashMap<NodeIndex, LookupIndex> {
        hashmap! {
            // index the parent for state repopulation purposes
            self.src.as_global() => LookupIndex::Strict(Index::hash_map(self.group_by.clone())),
            // index by our primary key
            this => LookupIndex::Strict(Index::hash_map(self.out_key.clone()))
        }
    }

    fn column_source(&self, cols: &[usize]) -> ColumnSource {
        let mapped_cols = cols
            .iter()
            .filter_map(|x| self.group_by.get(*x).copied())
            .collect::<Vec<_>>();
        if mapped_cols.len() != cols.len() {
            ColumnSource::RequiresFullReplay(vec1![self.src.as_global()])
        } else {
            ColumnSource::exact_copy(self.src.as_global(), mapped_cols.try_into().unwrap())
        }
    }

    fn description(&self, detailed: bool) -> String {
        self.inner.description(detailed)
    }

    fn is_selective(&self) -> bool {
        true
    }
}
