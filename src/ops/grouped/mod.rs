use ops;
use query;
use shortcut;

use std::fmt;
use std::collections::HashSet;
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
pub trait GroupedOperation: fmt::Debug {
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

    /// The zero value for this operation, if there is one.
    ///
    /// If some, this is used to determine what zero-record to revoke when the first record for a
    /// group arrives, as well as to initialize the fold value when a query is performed. Otherwise,
    /// no record is revoked when the first record arrives for a group.
    fn zero(&self) -> Option<query::DataType>;

    /// Extract the aggregation value from a single record.
    fn to_diff(&self, record: &[query::DataType], is_positive: bool) -> Self::Diff;

    /// Given the given `current` value, and a number of changes for a group (`diffs`), compute the
    /// updated group value. When the group is empty, current is set to the zero value.
    fn apply(&self, current: &Option<query::DataType>, diffs: Vec<Self::Diff>) -> query::DataType;

    fn description(&self) -> String;
}

#[derive(Debug)]
pub struct GroupedOperator<'a, T: GroupedOperation> {
    src: NodeAddress,
    inner: T,

    // some cache state
    us: Option<NodeAddress>,
    cols: usize,

    // precomputed datastructures
    group: HashSet<usize>,
    cond: Vec<shortcut::Condition<'a, query::DataType>>,
    colfix: Vec<usize>,
}

impl<'a, T: GroupedOperation> GroupedOperator<'a, T> {
    pub fn new(src: NodeAddress, op: T) -> GroupedOperator<'a, T> {
        GroupedOperator {
            src: src,
            inner: op,

            us: None,
            cols: 0,
            group: HashSet::new(),
            cond: Vec::new(),
            colfix: Vec::new(),
        }
    }
}

impl<'a, T: GroupedOperation + Send> Ingredient for GroupedOperator<'a, T> {
    fn ancestors(&self) -> Vec<NodeAddress> {
        vec![self.src]
    }

    fn should_materialize(&self) -> bool {
        true
    }

    fn will_query(&self, materialized: bool) -> bool {
        !materialized
    }

    fn on_connected(&mut self, g: &Graph) {
        let srcn = &g[self.src.as_global()];

        // give our inner operation a chance to initialize
        self.inner.setup(srcn);

        // group by all columns
        self.cols = srcn.fields().len();
        self.group.extend(self.inner.group_by().iter().cloned());

        // construct condition for querying into ourselves
        self.cond = (0..self.group.len())
            .into_iter()
            .map(|col| {
                shortcut::Condition {
                    column: col,
                    cmp: shortcut::Comparison::Equal(shortcut::Value::new(query::DataType::None)),
                }
            })
            .collect::<Vec<_>>();

        // the query into our own output (above) uses *output* column indices
        // but when we try to fill it, we have *input* column indices
        // build a translation mechanism for going from the former to the latter
        let colfix: Vec<_> = (0..self.cols)
            .into_iter()
            .filter_map(|col| {
                if self.group.contains(&col) {
                    // the next output column is this column
                    Some(col)
                } else {
                    // this column does not appear in output
                    None
                }
            })
            .collect();
        self.colfix.extend(colfix.into_iter());
    }

    fn on_commit(&mut self, us: NodeAddress, remap: &HashMap<NodeAddress, NodeAddress>) {
        // who's our parent really?
        self.src = remap[&self.src];

        // who are we?
        self.us = Some(us);
    }

    fn on_input(&mut self, input: Message, _: &DomainNodes, state: &StateMap) -> Option<Update> {
        debug_assert_eq!(input.from, self.src);

        match input.data {
            ops::Update::Records(rs) => {
                if rs.is_empty() {
                    return None;
                }

                // First, we want to be smart about multiple added/removed rows with same group.
                // For example, if we get a -, then a +, for the same group, we don't want to
                // execute two queries.
                let mut consolidate = HashMap::new();
                for rec in rs {
                    let (r, pos) = rec.extract();
                    let val = self.inner.to_diff(&r[..], pos);
                    let group = r.into_iter()
                        .enumerate()
                        .map(|(i, v)| if self.group.contains(&i) {
                            Some(v)
                        } else {
                            None
                        })
                        .collect::<Vec<_>>();

                    consolidate.entry(group).or_insert_with(Vec::new).push(val);
                }

                let mut out = Vec::with_capacity(2 * consolidate.len());
                for (group, diffs) in consolidate {
                    // find the current value for this group
                    let current = {
                        // Construct the query we'll need to query into ourselves
                        let mut q = self.cond.clone();

                        for s in &mut q {
                            s.cmp = shortcut::Comparison::Equal(
                                shortcut::Value::using(
                                    group[self.colfix[s.column]]
                                    .as_ref()
                                    .unwrap()
                                )
                            );
                        }

                        // find the current value for this group
                        match state.get(self.us.as_ref().unwrap()) {
                            Some(db) => {
                                let mut rs = db.find(&q[..]);
                                // current value is in the last output column
                                // or "" if there is no current group
                                let cur = rs.next()
                                    .map(|r| r[r.len() - 1].clone().into())
                                    .unwrap_or(self.inner.zero());
                                assert_eq!(rs.count(), 0, "a group had more than 1 result");
                                cur
                            }
                            None => {
                                // TODO
                                // query ancestor (self.query?) based on self.group columns
                                unimplemented!()
                            }
                        }
                    };

                    // new is the result of applying all diffs for the group to the current value
                    let new = self.inner.apply(&current, diffs);

                    match current {
                        None => {
                            // emit positive, which is group + new.
                            let rec = group.into_iter()
                                .filter_map(|v| v)
                                .chain(Some(new.into()).into_iter())
                                .collect();
                            out.push(ops::Record::Positive(rec));
                        }
                        Some(ref current) if &new == current => {
                            // no change
                        }
                        Some(current) => {
                            // construct prefix of output record used for both - and +
                            let mut rec = Vec::with_capacity(group.len() + 1);
                            rec.extend(group.into_iter().filter_map(|v| v));

                            // revoke old value
                            rec.push(current.into());
                            out.push(ops::Record::Negative(rec.clone()));

                            // remove the old value from the end of the record
                            rec.pop();

                            // emit new value
                            rec.push(new.into());
                            out.push(ops::Record::Positive(rec));
                        }
                    }
                }

                ops::Update::Records(out).into()
            }
        }
    }

    fn query(&self,
             q: Option<&query::Query>,
             domain: &DomainNodes,
             states: &StateMap)
             -> ops::Datas {
        use std::iter;

        // we're fetching everything from our parent
        let mut params = None;

        // however, if there are some conditions that filters over one of our group-bys, we should
        // use those as parameters to speed things up.
        if let Some(q) = q {
            params = Some(q.having.iter().map(|c| {
                // FIXME: we could technically support querying over the output of the operator,
                // but we'd have to restructure this function a fair bit so that we keep that part
                // of the query around for after we've got the results back. We'd then need to do
                // another filtering pass over the results of query. Unclear if that's worth it.
                assert!(c.column < self.colfix.len(),
                        "filtering on group operation output is not supported");

                shortcut::Condition{
                    column: self.colfix[c.column],
                    cmp: c.cmp.clone(),
                }
            }).collect::<Vec<_>>());

            if params.as_ref().unwrap().is_empty() {
                params = None;
            }
        }

        // now, query our ancestor, and aggregate into groups.
        let q = params.map(|ps| {
            query::Query::new(&iter::repeat(true)
                                  .take(self.cols)
                                  .collect::<Vec<_>>(),
                              ps)
        });

        let rx = if let Some(state) = states.get(&self.src) {
            // parent is materialized
            state.find(q.as_ref().map(|q| &q.having[..]).unwrap_or(&[]))
                .map(|r| r.iter().cloned().collect())
                .collect()
        } else {
            // parent is not materialized, query into parent
            domain[&self.src].borrow().query(q.as_ref(), domain, states)
        };

        // FIXME: having an order by would be nice here, so that we didn't have to keep the entire
        // aggregated state in memory until we've seen all rows.
        let mut consolidate = HashMap::new();
        for rec in rx {
            let val = self.inner.to_diff(&rec[..], true);
            let group = rec.into_iter()
                .enumerate()
                .filter_map(|(i, v)| if self.group.contains(&i) {
                    Some(v)
                } else {
                    None
                })
                .collect::<Vec<_>>();

            let mut cur = consolidate.entry(group).or_insert_with(|| self.inner.zero());

            // FIXME: this might turn out to be really expensive, since apply() is allowed to be an
            // expensive operation (like for group_concat). we *could* accumulate all records into
            // a Vec first, and then call apply() only once after all records have been read, but
            // that would mean much higher (and potentially unnecessary) memory usage. Ideally, we
            // would allow the GroupOperation to define its own aggregation container for this kind
            // of use-case, but we leave that as future work for now.
            let next = self.inner.apply(&cur, vec![val]);
            *cur = Some(next);
        }

        consolidate.into_iter()
            .map(|(mut group, val)| {
                group.push(val.unwrap());
                // TODO: respect q.select
                group
            })
            .collect()
    }

    fn suggest_indexes(&self, this: NodeAddress) -> HashMap<NodeAddress, Vec<usize>> {
        // index all group by columns,
        // which are the first self.group.len() columns of our output
        Some((this, (0..self.group.len()).collect()))
            .into_iter()
            .collect()
    }

    fn resolve(&self, col: usize) -> Option<Vec<(NodeAddress, usize)>> {
        if col == self.cols - 1 {
            return None;
        }
        Some(vec![(self.src, self.colfix[col])])
    }

    fn description(&self) -> String {
        self.inner.description()
    }
}
