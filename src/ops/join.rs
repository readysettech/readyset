use std::sync;
use std::iter;
use std::collections::HashMap;
use std::collections::HashSet;

use flow::prelude::*;

#[derive(Debug, Clone)]
struct JoinTarget {
    on: (usize, usize),
    select: Vec<bool>,
    outer: bool,
}

#[derive(Debug, Clone)]
struct Join {
    against: HashMap<NodeAddress, JoinTarget>,
    node: NodeAddress,
}

/// Convenience struct for building join nodes.
pub struct Builder {
    emit: Vec<(NodeAddress, usize)>,
    join: HashMap<NodeAddress, (bool, Vec<usize>)>,
}

impl Builder {
    /// Build a new join operator.
    ///
    /// `emit` dictates, for each output column, which source and column should be used.
    pub fn new(emit: Vec<(NodeAddress, usize)>) -> Self {
        Builder {
            emit: emit,
            join: HashMap::new(),
        }
    }

    /// Set the source view for this join.
    ///
    /// This is semantically identical to `join`, except that it also asserts that this is the
    /// first view being added. The first view is of particular importance as it dictates the
    /// behavior of later *left* joins (when they are added).
    pub fn from(self, node: NodeAddress, groups: Vec<usize>) -> Self {
        assert!(self.join.is_empty());
        self.join(node, groups)
    }

    /// Also join with the given `node`.
    ///
    /// `groups` gives the group assignments for each output column of `node`. Columns across join
    /// sources that share a group are used to join rows from those sources by equality. Thus, each
    /// group number can appear at most once for each view.
    ///
    /// Let us look at a SQL join such as
    ///
    /// ```sql
    /// SELECT a.0, b.0
    /// FROM a JOIN b USING (a.0 == b.1)
    /// ```
    ///
    /// Assuming `a` has two columns and `b` has three, the map would look like this:
    ///
    /// ```rust,ignore
    /// Builder::new(vec![(a, 0), (b, 0)]).from(a, vec![1, 0]).join(b, vec![0, 1, 0]);
    /// ```
    pub fn join(mut self, node: NodeAddress, groups: Vec<usize>) -> Self {
        assert!(self.join.insert(node, (false, groups)).is_none());
        self
    }

    /// Also perform a left join against the given `node`.
    ///
    /// The semantics of this is similar to the SQL notion of a `LEFT JOIN`, namely that records
    /// from other tables that join against this table will always be present in the output,
    /// regardless of whether matching records exist in `node`. For such *zero rows*, all columns
    /// emitted from this node will be set to `DataType::None`.
    pub fn left_join(mut self, node: NodeAddress, groups: Vec<usize>) -> Self {
        assert!(self.join.insert(node, (true, groups)).is_none());
        self
    }
}

impl From<Builder> for Joiner {
    fn from(b: Builder) -> Joiner {
        if b.join.len() != 2 {
            // only two-way joins are currently supported
            unimplemented!();
        }

        // we technically want this assert, but we don't have self.nodes until .prime() has been
        // called. unfortunately, at that time, we don't have .join in the original format, and so
        // the debug doesn't makes sense. it's probably not worth carrying along the original join
        // map just to verify this, but maybe...
        // assert!(self.nodes.iter().all(|(ni, n)| self.join[ni].len() == n.args().len()));

        // the format of `join` is convenient for users, but not particulary convenient for lookups
        // the particular use-case we want to be efficient is:
        //
        //  - we are given a record from `src`
        //  - for each other parent `p`, we want to know which columns of `p` to constrain, and
        //    which values in the `src` record those correspond to
        //
        // so, we construct a map of the form
        //
        //   src: NodeAddress => {
        //     p: NodeAddress => [(srci, pi), ...]
        //   }
        //
        let join = b.join
            .iter()
            .map(|(&src, &(_, ref srcg))| {
                // which groups are bound to which columns?
                let g2c = srcg.iter()
                    .enumerate()
                    .filter_map(|(c, &g)| if g == 0 { None } else { Some((g, c)) })
                    .collect::<HashMap<_, _>>();

                // for every other view
                let other = b.join
                    .iter()
                    .filter_map(|(&p, &(outer, ref pg))| {
                        // *other* view
                        if p == src {
                            return None;
                        }
                        // look through the group assignments for that other view
                        let pg: Vec<_> = pg.iter()
                            .enumerate()
                            .filter_map(|(pi, g)| {
                                            // look for ones that share a group with us
                                            g2c.get(g).map(|srci| {
                                                               // and emit that mapping
                                                               (*srci, pi)
                                                           })
                                        })
                            .collect();

                        // if there are no shared columns, don't join against this view
                        if pg.is_empty() {
                            return None;
                        }
                        // but if there are, emit the mapping we found
                        assert_eq!(pg.len(), 1, "can only join on one key for now");
                        Some((p,
                              JoinTarget {
                                  on: pg.into_iter().next().unwrap(),
                                  outer: outer,
                                  select: Vec::new(),
                              }))
                    })
                    .collect();

                (src,
                 Join {
                     against: other,
                     node: src,
                 })
            })
            .collect();

        Joiner {
            emit: b.emit,
            join: join,
        }
    }
}

use flow::node;
impl Into<node::Type> for Builder {
    fn into(self) -> node::Type {
        let j: Joiner = self.into();
        node::Type::Internal(Box::new(j) as Box<Ingredient>)
    }
}

/// Joiner provides a 2-way join between two views.
///
/// It shouldn't be *too* hard to extend this to `n`-way joins, but it would require restructuring
/// `.join` such that it can express "query this view first, then use one of its columns to query
/// this other view".
#[derive(Debug, Clone)]
pub struct Joiner {
    emit: Vec<(NodeAddress, usize)>,
    join: HashMap<NodeAddress, Join>,
}

impl Joiner {
    fn join<'a>
        (&'a self,
         left: (NodeAddress, sync::Arc<Vec<DataType>>),
         domain: &DomainNodes,
         states: &StateMap)
         -> Result<Box<Iterator<Item = Vec<DataType>> + 'a>, (NodeAddress, usize, DataType)> {

        // NOTE: this only works for two-way joins
        let other = *self.join
                         .keys()
                         .find(|&other| other != &left.0)
                         .unwrap();
        let this = &self.join[&left.0];
        let target = &this.against[&other];

        // send the parameters to start the query.
        let rx: Vec<_> = match self.lookup(other,
                                           &[target.on.1],
                                           &KeyType::Single(&left.1[target.on.0]),
                                           domain,
                                           states) {
            None => unreachable!("joins must have inputs materialized"),
            Some(None) => {
                // partial materialization miss
                return Err((other, target.on.1, left.1[target.on.0].clone()));
            }
            Some(Some(rs)) => rs.cloned().collect(),
        };

        if rx.is_empty() && target.outer {
            return Ok(Box::new(Some(self.emit
                                        .iter()
                                        .map(|&(source, column)| {
                                                 if source == other {
                                                     DataType::None
                                                 } else {
                                                     // this clone is unnecessary
                                                     left.1[column].clone()
                                                 }
                                             })
                                        .collect::<Vec<_>>())
                                       .into_iter()));
        }

        Ok(Box::new(rx.into_iter().map(move |right| {
            // weave together r and j according to join rules
            self.emit
                .iter()
                .map(|&(source, column)| {
                    if source == other {
                        // FIXME: this clone is unnecessary.
                        // it's tricky to remove though, because it means we'd need to
                        // be removing things from right. what if a later column also needs
                        // to select from right? we'd need to keep track of which things we
                        // have removed, and subtract that many from the index of the
                        // later column. ugh.
                        right[column].clone()
                    } else {
                        left.1[column].clone()
                    }
                })
                .collect()
        })))
    }
}

impl Ingredient for Joiner {
    fn take(&mut self) -> Box<Ingredient> {
        Box::new(Clone::clone(self))
    }

    fn ancestors(&self) -> Vec<NodeAddress> {
        self.join
            .keys()
            .cloned()
            .collect()
    }

    fn should_materialize(&self) -> bool {
        false
    }

    fn is_join(&self) -> bool {
        true
    }

    fn must_replay_among(&self, empty: &HashSet<NodeAddress>) -> Option<HashSet<NodeAddress>> {
        // we want to replay an ancestor that we are *not* doing an outer join against
        // it's not *entirely* clear how to extract that from self.join, but we'll use the
        // following heuristic: find an ancestor that is never performed an outer join against.
        let mut options: HashSet<_> = self.join.keys().collect();
        for left in self.join.values() {
            for right in left.against.keys() {
                if left.against[right].outer {
                    options.remove(right);
                }
            }
        }
        assert!(!options.is_empty());

        // we may have multiple options in the case of an inner join
        // if any of them are empty, choose that one, since our output is also empty!
        for &option in &options {
            if empty.contains(option) {
                // no need to look any further, just replay this
                let mut options = HashSet::new();
                options.insert(*option);
                return Some(options);
            }
        }

        // we don't know which of these we prefer, so we leave it up to the materialization code to
        // pick among them for us.
        Some(options.into_iter().map(|&ni| ni).collect())
    }

    fn will_query(&self, _: bool) -> bool {
        true
    }

    fn on_connected(&mut self, g: &Graph) {
        for j in self.join.values_mut() {
            for (t, jt) in &mut j.against {
                jt.select =
                    iter::repeat(true).take(g[*t.as_global()].fields().len()).collect::<Vec<_>>();
            }
        }
    }

    fn on_commit(&mut self, _: NodeAddress, remap: &HashMap<NodeAddress, NodeAddress>) {
        // our ancestors may have been remapped
        // we thus need to fix up any node indices that could have changed
        for (from, to) in remap {
            if from == to {
                continue;
            }

            if let Some(mut j) = self.join.remove(from) {
                j.node = *to;
                assert!(self.join.insert(*to, j).is_none());
            }

            for j in self.join.values_mut() {
                if let Some(t) = j.against.remove(from) {
                    assert!(j.against.insert(*to, t).is_none());
                }
            }
        }

        for &mut (ref mut ni, _) in &mut self.emit {
            *ni = remap[&*ni];
        }
    }

    fn on_input(&mut self,
                from: NodeAddress,
                rs: Records,
                nodes: &DomainNodes,
                state: &StateMap)
                -> ProcessingResult {
        // okay, so here's what's going on:
        // the record(s) we receive are all from one side of the join. we need to query the
        // other side(s) for records matching the incoming records on that side's join
        // fields.

        let rs2 = rs.clone(); // well, this clone is annoying

        // TODO: we should be clever here, and only query once per *distinct join value*,
        // instead of once per received record.
        let mut rs = rs.into_iter().map(|rec| {
            let (r, pos) = rec.extract();

            self.join((from, r), nodes, state).map(|it| {
                it.map(move |res| {
                           // return new row with appropriate sign
                           if pos {
                               Record::Positive(sync::Arc::new(res))
                           } else {
                               Record::Negative(sync::Arc::new(res))
                           }
                       })
            })
        });

        let mut results = Vec::new();
        while let Some(it) = rs.next() {
            match it {
                Ok(rs) => results.extend(rs),
                Err((node, col, key)) => {
                    return ProcessingResult::NeedReplay {
                               node: node,
                               columns: vec![col],
                               key: vec![key],
                               was: rs2,
                           };
                }
            }
        }

        ProcessingResult::Done(results.into())
    }

    fn suggest_indexes(&self, _this: NodeAddress) -> HashMap<NodeAddress, Vec<usize>> {
        // index all join fields
        self.join
            .iter()
            // for every left
            .flat_map(|(left, rs)| {
                // for every right
                rs.against.iter().flat_map(move |(right, rs)| {
                    // emit both the left binding
                    vec![(left, rs.on.0), (right, rs.on.1)]
                })
            })
            // we now have (NodeAddress, usize) for every join column.
            .fold(HashMap::new(), |mut hm, (node, col)| {
                hm.entry(*node).or_insert(vec![col]);
                hm
            })
    }

    fn resolve(&self, col: usize) -> Option<Vec<(NodeAddress, usize)>> {
        Some(vec![self.emit[col].clone()])
    }

    fn description(&self) -> String {
        let emit = self.emit
            .iter()
            .map(|&(src, col)| format!("{}:{}", src, col))
            .collect::<Vec<_>>()
            .join(", ");
        let joins = self.join
            .iter()
            .flat_map(|(left, rs)| {
                rs.against
                    .iter()
                    .filter(move |&(right, _)| left < right)
                    .map(move |(right, rs)| {
                             let op = if rs.outer { "⋉" } else { "⋈" };
                             format!("{}:{} {} {}:{}", left, rs.on.0, op, right, rs.on.1)
                         })
            })
            .collect::<Vec<_>>()
            .join(", ");
        format!("[{}] {}", emit, joins)
    }

    fn parent_columns(&self, col: usize) -> Vec<(NodeAddress, Option<usize>)> {
        // we know where this column comes from through self.emit.
        let (origin, ocol) = self.emit[col];
        let mut source = vec![(origin, Some(ocol))];

        // however, we *also* want to check if this column compares equal to a column in another
        // ancestor, so that we can detect key provenance through both sides of the join.
        let j = &self.join[&origin];
        assert!(j.against.len() == 1); // only two-way joins for now

        // figure out how we join with the other view
        let (&other, &JoinTarget { on: (lcol, rcol), .. }) = j.against
            .iter()
            .next()
            .unwrap();

        // if we join on the same column that col resolves to,
        // then we know that self[col] also comes from other[rcol].
        if lcol == ocol {
            source.push((other, Some(rcol)));
        }

        source
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use ops;

    fn setup(left: bool) -> (ops::test::MockGraph, NodeAddress, NodeAddress) {
        let mut g = ops::test::MockGraph::new();
        let l = g.add_base("left", &["l0", "l1"]);
        let r = g.add_base("right", &["r0", "r1"]);

        // join on first field
        let b = Builder::new(vec![(l, 0), (l, 1), (r, 1)]).from(l, vec![1, 0]);
        let b = if left {
            b.left_join(r, vec![1, 0])
        } else {
            b.join(r, vec![1, 0])
        };

        let j: Joiner = b.into();
        g.set_op("join", &["j0", "j1", "j2"], j, false);
        g.seed(l, vec![1.into(), "a".into()]);
        g.seed(l, vec![2.into(), "b".into()]);
        g.seed(l, vec![3.into(), "c".into()]);
        g.seed(r, vec![1.into(), "x".into()]);
        g.seed(r, vec![1.into(), "y".into()]);
        g.seed(r, vec![2.into(), "z".into()]);

        let (l, r) = (g.to_local(l), g.to_local(r));
        (g, l, r)
    }

    #[test]
    fn it_describes() {
        let (j, l, r) = setup(false);
        assert_eq!(j.node().description(),
                   format!("[{}:0, {}:1, {}:1] {}:0 ⋈ {}:0", l, l, r, l, r));
    }

    #[test]
    fn it_describes_left() {
        let (j, l, r) = setup(true);
        assert_eq!(j.node().description(),
                   format!("[{}:0, {}:1, {}:1] {}:0 ⋉ {}:0", l, l, r, l, r));
    }

    fn forward_non_weird(mut j: ops::test::MockGraph, l: NodeAddress, r: NodeAddress) {
        // these are the data items we have to work with
        // these are in left
        let l_a1 = vec![1.into(), "a".into()];
        let l_b2 = vec![2.into(), "b".into()];
        // let l_c3 = vec![3.into(), "c".into()]; // considered weird
        // these are in right
        let r_x1 = vec![1.into(), "x".into()];
        let r_y1 = vec![1.into(), "y".into()];
        let r_z2 = vec![2.into(), "z".into()];

        // *************************************
        // forward from the left
        // *************************************

        // forward b2 from left; should produce [b2*z2]
        // we're expecting to only match z2
        assert_eq!(j.one_row(l, l_b2.clone(), false),
                   vec![vec![2.into(), "b".into(), "z".into()]].into());

        // forward a1 from left; should produce [a1*x1, a1*y1]
        let rs = j.one_row(l, l_a1.clone(), false);
        // we're expecting two results: x1 and y1
        assert_eq!(rs.len(), 2);
        // they should all be positive since input was positive
        assert!(rs.iter().all(|r| r.is_positive()));
        // they should all have the correct values from the provided left
        assert!(rs.iter().all(|r| r.rec()[0] == 1.into() && r.rec()[1] == "a".into()));
        // and both join results should be present
        assert!(rs.iter().any(|r| r.rec()[2] == "x".into()));
        assert!(rs.iter().any(|r| r.rec()[2] == "y".into()));

        // *************************************
        // forward from the right
        // *************************************

        // forward x1 from right; should produce [a1*x1]
        assert_eq!(j.one_row(r, r_x1.clone(), false),
                   vec![vec![1.into(), "a".into(), "x".into()]].into());

        // forward y1 from right; should produce [a1*y1]
        // NOTE: because we use r_y1.into(), left's timestamp will be set to 0
        assert_eq!(j.one_row(r, r_y1.clone(), false),
                   vec![vec![1.into(), "a".into(), "y".into()]].into());

        // forward z2 from right; should produce [b2*z2]
        // NOTE: because we use r_z2.into(), left's timestamp will be set to 0, and thus
        // right's (b2's) timestamp will be used.
        assert_eq!(j.one_row(r, r_z2.clone(), false),
                   vec![vec![2.into(), "b".into(), "z".into()]].into());
    }

    #[test]
    fn it_works() {
        let (mut j, l, r) = setup(false);
        let l_c3 = vec![3.into(), "c".into()];

        // forward c3 from left; should produce [] since no records in right are 3
        let rs = j.one_row(l, l_c3.clone(), false);
        // right has no records with value 3
        assert_eq!(rs.len(), 0);

        forward_non_weird(j, l, r);
    }

    #[test]
    fn it_works_left() {
        let (mut j, l, r) = setup(true);

        let l_c3 = vec![3.into(), "c".into()];

        // forward c3 from left; should produce [c3 + None] since no records in right are 3
        let rs = j.one_row(l, l_c3.clone(), false);
        // right has no records with value 3, so we're expecting a single record with None
        // for all columns output from the (non-existing) right record
        assert_eq!(rs.len(), 1);
        // that row should be positive
        assert!(rs.iter().all(|r| r.is_positive()));
        // and should have the correct values from the provided left
        assert!(rs.iter().all(|r| r.rec()[0] == 3.into() && r.rec()[1] == "c".into()));
        // and None for the remaining column
        assert!(rs.iter().any(|r| r.rec()[2] == DataType::None));

        forward_non_weird(j, l, r);
    }

    #[test]
    fn it_suggests_indices() {
        use std::collections::HashMap;
        let me = NodeAddress::mock_global(2.into());
        let (j, l, r) = setup(false);
        let hm: HashMap<_, _> = vec![(l, vec![0]), /* join column for left */
                                     (r, vec![0]) /* join column for right */]
                .into_iter()
                .collect();
        assert_eq!(j.node().suggest_indexes(me), hm);
    }

    #[test]
    fn it_resolves() {
        let (j, l, r) = setup(false);
        assert_eq!(j.node().resolve(0), Some(vec![(l, 0)]));
        assert_eq!(j.node().resolve(1), Some(vec![(l, 1)]));
        assert_eq!(j.node().resolve(2), Some(vec![(r, 1)]));
    }
}
