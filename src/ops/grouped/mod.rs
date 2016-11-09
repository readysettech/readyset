use ops;
use flow;
use query;
use backlog;
use shortcut;
use ops::NodeOp;
use ops::NodeType;

use std::fmt;
use std::collections::HashSet;
use std::collections::HashMap;

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
    fn setup(&mut self, parent: &ops::V);

    /// List the columns used to group records.
    ///
    /// All records with the same value for the returned columns are assigned to the same group.
    fn group_by(&self) -> &[usize];

    /// The zero value for this operation.
    ///
    /// This is used to determine what zero-record to revoke when the first record for a group
    /// arrives, as well as to initialize the fold value when a query is performed.
    fn zero(&self) -> query::DataType;

    /// Extract the aggregation value from a single record.
    fn to_diff(&self, record: &[query::DataType], is_positive: bool) -> Self::Diff;

    /// Given the given `current` value, and a number of changes for a group (`diffs`), compute the
    /// updated group value.
    fn apply(&self, current: &query::DataType, diffs: Vec<(Self::Diff, i64)>) -> query::DataType;
}

#[derive(Debug)]
pub struct GroupedOperator<T: GroupedOperation> {
    src: flow::NodeIndex,
    inner: T,

    srcn: Option<ops::V>,
    cols: usize,
    group: HashSet<usize>,
    cond: Vec<shortcut::Condition<query::DataType>>,
    colfix: Vec<usize>,
}

impl From<GroupedOperator<concat::GroupConcat>> for NodeType {
    fn from(b: GroupedOperator<concat::GroupConcat>) -> NodeType {
        NodeType::GroupConcat(b)
    }
}

// impl From<GroupedOperator<latest::Latest>> for NodeType {
//     fn from(b: GroupedOperator<latest::Latest>) -> NodeType {
//         NodeType::Latest(b)
//     }
// }

impl From<GroupedOperator<aggregate::Aggregator>> for NodeType {
    fn from(b: GroupedOperator<aggregate::Aggregator>) -> NodeType {
        NodeType::Aggregate(b)
    }
}

impl From<GroupedOperator<extremum::ExtremumOperator>> for NodeType {
    fn from(b: GroupedOperator<extremum::ExtremumOperator>) -> NodeType {
        NodeType::Extremum(b)
    }
}

impl<T: GroupedOperation> GroupedOperator<T> {
    pub fn new(src: flow::NodeIndex, op: T) -> GroupedOperator<T> {
        GroupedOperator {
            src: src,
            inner: op,

            srcn: None,
            cols: 0,
            group: HashSet::new(),
            cond: Vec::new(),
            colfix: Vec::new(),
        }
    }
}

impl<T: GroupedOperation> NodeOp for GroupedOperator<T> {
    fn prime(&mut self, g: &ops::Graph) -> Vec<flow::NodeIndex> {
        // who's our parent?
        self.srcn = g[self.src].as_ref().cloned();

        // give our inner operation a chance to initialize
        self.inner.setup(self.srcn.as_ref().unwrap());

        // group by all columns
        self.cols = self.srcn.as_ref().unwrap().args().len();
        self.group.extend(self.inner.group_by().iter().cloned());

        // construct condition for querying into ourselves
        self.cond = (0..self.group.len())
            .into_iter()
            .map(|col| {
                shortcut::Condition {
                    column: col,
                    cmp: shortcut::Comparison::Equal(shortcut::Value::Const(query::DataType::None)),
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

        vec![self.src]
    }

    fn forward(&self,
               u: ops::Update,
               src: flow::NodeIndex,
               _: i64,
               db: Option<&backlog::BufferedStore>)
               -> Option<ops::Update> {

        assert_eq!(src, self.src);

        // Construct the query we'll need to query into ourselves
        let mut q = self.cond.clone();

        match u {
            ops::Update::Records(rs) => {
                if rs.is_empty() {
                    return None;
                }

                // First, we want to be smart about multiple added/removed rows with same group.
                // For example, if we get a -, then a +, for the same group, we don't want to
                // execute two queries.
                let mut consolidate = HashMap::new();
                for rec in rs {
                    let (r, pos, ts) = rec.extract();
                    let val = self.inner.to_diff(&r[..], pos);
                    let group = r.into_iter()
                        .enumerate()
                        .map(|(i, v)| {
                            if self.group.contains(&i) {
                                Some(v)
                            } else {
                                None
                            }
                        })
                        .collect::<Vec<_>>();

                    consolidate.entry(group).or_insert_with(Vec::new).push((val, ts));
                }

                let mut out = Vec::with_capacity(2 * consolidate.len());
                for (mut group, diffs) in consolidate {
                    // note that each value in group is an Option so that we can take/swap without
                    // having to .remove or .insert into the HashMap (which is much more expensive)
                    // it should only be None for self.over

                    // build a query for this group
                    for s in &mut q {
                        s.cmp = shortcut::Comparison::Equal(
                            shortcut::Value::Const(
                                group[self.colfix[s.column]]
                                .take()
                                .unwrap()
                            )
                        );
                    }

                    // find the current value for this group
                    let (current, old_ts) = match db {
                        Some(db) => {
                            db.find_and(&q[..], Some(i64::max_value()), |rs| {
                                assert!(rs.len() <= 1, "a group had more than 1 result");
                                // current value is in the last output column
                                // or "" if there is no current group
                                rs.into_iter()
                                    .next()
                                    .map(|(r, ts)| (r[r.len() - 1].clone().into(), ts))
                                    .unwrap_or((self.inner.zero(), 0))
                            })
                        }
                        None => {
                            // TODO
                            // query ancestor (self.query?) based on self.group columns
                            unimplemented!()
                        }
                    };

                    // get back values from query (to avoid cloning)
                    for s in &mut q {
                        if let shortcut::Comparison::Equal(shortcut::Value::Const(ref mut v)) =
                               s.cmp {
                            use std::mem;

                            let mut x = query::DataType::None;
                            mem::swap(&mut x, v);
                            group[self.colfix[s.column]] = Some(x);
                        }
                    }

                    // new ts is the max change timestamp
                    let new_ts = diffs.iter().map(|&(_, ts)| ts).max().unwrap();

                    // new is the result of applying all diffs for the group to the current value
                    let new = self.inner.apply(&current, diffs);

                    if new != current {
                        // construct prefix of output record used for both - and +
                        let mut rec = Vec::with_capacity(group.len() + 1);
                        rec.extend(group.into_iter().filter_map(|v| v));

                        // revoke old value
                        rec.push(current.into());
                        out.push(ops::Record::Negative(rec.clone(), old_ts));

                        // remove the old value from the end of the record
                        rec.pop();

                        // emit new value
                        rec.push(new.into());
                        out.push(ops::Record::Positive(rec, new_ts));
                    }
                }

                if out.is_empty() {
                    None
                } else {
                    Some(ops::Update::Records(out))
                }
            }
        }
    }

    fn query(&self, q: Option<&query::Query>, ts: i64) -> ops::Datas {
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

        let rx = self.srcn.as_ref().unwrap().find(q.as_ref(), Some(ts));

        // FIXME: having an order by would be nice here, so that we didn't have to keep the entire
        // aggregated state in memory until we've seen all rows.
        let mut consolidate = HashMap::new();
        for (rec, ts) in rx {
            use std::cmp;

            let val = self.inner.to_diff(&rec[..], true);
            let group = rec.into_iter()
                .enumerate()
                .filter_map(|(i, v)| {
                    if self.group.contains(&i) {
                        Some(v)
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();

            let mut cur = consolidate.entry(group).or_insert_with(|| (self.inner.zero(), ts));

            // FIXME: this might turn out to be really expensive, since succ() is allowed to be an
            // expensive operation (like for group_concat). we *could* accumulate all records into
            // a Vec first, and then call succ() only once after all records have been read, but
            // that would mean much higher (and potentially unnecessary) memory usage. ideally, we
            // would allow the GroupOperation to define its own aggregation container for this kind
            // of use-case, but we leave that as future work for now.
            let next = self.inner.apply(&cur.0, vec![(val, ts)]);
            cur.0 = next;

            // timestamp should always reflect the latest influencing record
            cur.1 = cmp::max(ts, cur.1);
        }

        consolidate.into_iter()
            .map(|(mut group, (val, ts))| {
                group.push(val);
                // TODO: respect q.select
                (group, ts)
            })
            .collect()
    }

    fn suggest_indexes(&self, this: flow::NodeIndex) -> HashMap<flow::NodeIndex, Vec<usize>> {
        // index all group by columns
        Some((this, self.group.iter().cloned().collect()))
            .into_iter()
            .collect()
    }

    fn resolve(&self, col: usize) -> Option<Vec<(flow::NodeIndex, usize)>> {
        if col == self.srcn.as_ref().unwrap().args().len() - 1 {
            return None;
        }
        Some(vec![(self.src, self.colfix[col])])
    }
}
