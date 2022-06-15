use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use std::convert::TryInto;
use std::mem;

use itertools::Itertools;
use launchpad::Indices;
use nom_sql::OrderType;
use serde::{Deserialize, Serialize};

use crate::ops::utils::Order;
use crate::prelude::*;
use crate::processing::{ColumnMiss, LookupIndex};

/// Data structure used internally to Paginate to track rows currently within a group. Contains a
/// reference to the `order` of the operator itself to allow for a custom Ord implementation, which
/// compares records in reverse order for efficient determination of the minimum record via
/// insertion into a [`BinaryHeap`]
#[derive(Debug)]
struct CurrentRecord<'op, 'state> {
    /// Underlying row for the record.
    ///
    /// For rows loaded from the state, the last column will contain the page number as-is from the
    /// state. For new rows received in the batch, the row will be one column shorter.
    row: Cow<'state, [DataType]>,
    order: &'op Order,
}

impl<'op, 'state> Ord for CurrentRecord<'op, 'state> {
    fn cmp(&self, other: &Self) -> Ordering {
        debug_assert_eq!(self.order, other.order);
        self.order
            .cmp(self.row.as_ref(), other.row.as_ref())
            .reverse()
    }
}

impl<'op, 'state> PartialOrd for CurrentRecord<'op, 'state> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<'op, 'state> PartialOrd<[DataType]> for CurrentRecord<'op, 'state> {
    fn partial_cmp(&self, other: &[DataType]) -> Option<Ordering> {
        Some(self.order.cmp(self.row.as_ref(), other))
    }
}

impl<'op, 'state> PartialEq for CurrentRecord<'op, 'state> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl<'op, 'state> PartialEq<[DataType]> for CurrentRecord<'op, 'state> {
    fn eq(&self, other: &[DataType]) -> bool {
        self.partial_cmp(other).contains(&Ordering::Equal)
    }
}

impl<'op, 'state> Eq for CurrentRecord<'op, 'state> {}

#[derive(Clone, Serialize, Deserialize)]
pub struct Paginate {
    /// The direct Ingredient or Base ancestor of this node
    src: IndexPair,
    /// The index of this node. Used to look up into our own state
    our_index: Option<IndexPair>,
    /// The column index of the page number column emitted by this node
    ///
    /// This is always equal to the number of columns in the parent node (there is one more output
    /// column than input columns)
    ///
    /// Set during [`Ingredient::on_connected`]
    page_number_col: Option<usize>,
    /// The List of column indices that we're grouping by
    group_by: Vec<usize>,
    /// The ordering for our records
    order: Order,
    /// The page length (LIMIT) field of our Paginator. We only handle fixed LIMITs
    limit: usize,
}

impl Paginate {
    pub fn new(
        src: NodeIndex,
        order: Vec<(usize, OrderType)>,
        group_by: Vec<usize>,
        limit: usize,
    ) -> Self {
        Paginate {
            src: src.into(),
            our_index: None,
            page_number_col: None,
            group_by,
            order: order.into(),
            limit,
        }
    }

    /// Project the columns we are grouping by out of the given record
    fn project_group<'rec, R>(&self, rec: &'rec R) -> ReadySetResult<Vec<&'rec DataType>>
    where
        R: Indices<'static, usize, Output = DataType> + ?Sized,
    {
        rec.indices(self.group_by.clone())
            .map_err(|_| ReadySetError::InvalidRecordLength)
    }

    /// Return the column index of the page number column output by this node (which will always be
    /// the last column)
    ///
    /// # Panics
    ///
    /// Panics if called before [`Ingredient::on_connected`]
    fn page_number_column(&self) -> usize {
        self.page_number_col
            .expect("page_number_column called before Ingredient::on_connected")
    }

    fn post_group<'op, 'state>(
        &'op self,
        out: &mut Vec<Record>,
        current_group: &mut BinaryHeap<CurrentRecord<'op, 'state>>,
    ) {
        let mut current_page_num = 0u64;
        let mut current_page_size = 0usize;
        let current_group = mem::take(current_group).into_sorted_vec();
        for CurrentRecord { mut row, .. } in current_group {
            if let Some(page_number) = row.get(self.page_number_column()) {
                // if the row already has a page number, that means it started out in the group
                if *page_number != current_page_num.into() {
                    // if the page number is different, we need to emit a negative for the old page
                    // number and a positive for the new one
                    out.push(Record::Negative(row.clone().into()));
                    row.to_mut()[self.page_number_column()] = current_page_num.into();
                    out.push(Record::Positive(row.into()));
                }
            } else {
                row.to_mut().push(current_page_num.into());
                out.push(row.into_owned().into());
            }

            current_page_size += 1;
            if current_page_size >= self.limit {
                current_page_num += 1;
                current_page_size = 0;
            }
        }
    }
}

impl Ingredient for Paginate {
    fn take(&mut self) -> NodeOperator {
        self.clone().into()
    }

    fn ancestors(&self) -> Vec<NodeIndex> {
        vec![self.src.as_global()]
    }

    fn on_connected(&mut self, graph: &Graph) {
        self.page_number_col = Some(graph[self.src.as_global()].columns().len());
    }

    fn on_commit(&mut self, us: NodeIndex, remap: &HashMap<NodeIndex, IndexPair>) {
        self.src.remap(remap);
        self.our_index = Some(remap[&us]);
    }

    fn on_input<'a>(
        &mut self,
        from: LocalNodeIndex,
        rs: Records,
        replay: &ReplayContext,
        _nodes: &DomainNodes,
        state: &'a StateMap,
    ) -> ReadySetResult<ProcessingResult> {
        debug_assert_eq!(from, *self.src);

        if rs.is_empty() {
            return Ok(ProcessingResult {
                results: rs,
                ..Default::default()
            });
        }

        let mut rs = Vec::from(rs);
        rs.sort_by(|a: &Record, b: &Record| {
            self.project_group(&***a)
                .unwrap_or_default()
                .cmp(&self.project_group(&***b).unwrap_or_default())
                .then_with(|| self.order.cmp(&***a, &***b))
        });

        let us = self.our_index.unwrap();
        let db = state
            .get(*us)
            .ok_or_else(|| internal_err("paginate must have its own state materialized"))?;

        let mut current_group_key: Vec<DataType> = vec![];
        let mut current_group: BinaryHeap<CurrentRecord> = BinaryHeap::new();
        let mut group_missed = false;

        let mut out = vec![];
        let mut lookups = vec![];
        let mut misses = vec![];

        for r in rs {
            let record_group = self.project_group(r.rec())?;
            if current_group_key.iter().cmp(record_group.iter().copied()) != Ordering::Equal {
                // New group!
                if !current_group_key.is_empty() {
                    self.post_group(&mut out, &mut current_group);
                }

                // Clear and extend to reuse the allocation
                current_group_key.clear();
                current_group_key.extend(record_group.into_iter().cloned());

                // Load all pages for the group into memory
                match db.lookup(&self.group_by, &KeyType::from(&current_group_key)) {
                    LookupResult::Some(local_records) => {
                        if replay.is_partial() {
                            lookups.push(Lookup {
                                on: *us,
                                cols: self.group_by.clone(),
                                key: current_group_key.clone().try_into().expect("Empty group"),
                            });
                        }

                        group_missed = false;
                        current_group.extend(local_records.into_iter().map(|row| CurrentRecord {
                            row: row.clone(),
                            order: &self.order,
                        }));
                    }
                    LookupResult::Missing => {
                        group_missed = true;
                    }
                }
            }

            if group_missed {
                misses.push(
                    Miss::builder()
                        .on(*us)
                        .lookup_idx(self.group_by.clone())
                        .lookup_key(self.group_by.clone())
                        .replay(replay)
                        .record(r.into_row())
                        .build(),
                );
                continue;
            }

            match r {
                Record::Positive(r) => current_group.push(CurrentRecord {
                    row: Cow::Owned(r),
                    order: &self.order,
                }),
                Record::Negative(r) => {
                    let mut found = false;
                    current_group.retain(|CurrentRecord { row, .. }| {
                        if found {
                            // We've already removed one copy of this row, don't need to do any
                            // more
                            return true;
                        }
                        if row[..self.page_number_column()] == *r {
                            found = true;
                            out.push(Record::Negative(row.clone().into()));
                            return false;
                        }

                        true
                    })
                }
            }
        }

        if !current_group.is_empty() {
            self.post_group(&mut out, &mut current_group);
        }

        Ok(ProcessingResult {
            results: out.into(),
            lookups,
            misses,
        })
    }

    fn suggest_indexes(&self, this: NodeIndex) -> HashMap<NodeIndex, LookupIndex> {
        HashMap::from([(
            this,
            LookupIndex::Strict(Index::hash_map(self.group_by.clone())),
        )])
    }

    fn column_source(&self, cols: &[usize]) -> ColumnSource {
        if cols.contains(&self.page_number_column()) {
            if cols.len() == 1 {
                // Ungrouped page lookups (currently) require a full replay
                return ColumnSource::RequiresFullReplay(vec1![self.src.as_global()]);
            }

            #[allow(clippy::unwrap_used)]
            // Once we remove the page number column, we have to have at least one column left
            // (because we just checked len > 1)
            let columns = cols
                .iter()
                .copied()
                .filter(|c| *c != self.page_number_column())
                .collect::<Vec<_>>()
                .try_into()
                .unwrap();
            ColumnSource::GeneratedFromColumns(vec1![ColumnRef {
                node: self.our_index.unwrap().as_global(),
                columns,
            }])
        } else {
            ColumnSource::ExactCopy(ColumnRef {
                node: self.src.as_global(),
                columns: cols.to_vec().try_into().unwrap(),
            })
        }
    }

    fn handle_upquery(&mut self, miss: ColumnMiss) -> ReadySetResult<Vec<ColumnMiss>> {
        let page_number_column = miss
            .column_indices
            .iter()
            .position(|ci| *ci == self.page_number_column())
            .expect("handle_upquery invariant");

        Ok(vec![ColumnMiss {
            node: *self.our_index.unwrap(),
            column_indices: self.group_by.clone().try_into().unwrap(),
            missed_keys: miss.missed_keys.mapped(|k| {
                k.map_endpoints(|mut r| {
                    r.remove(page_number_column)
                        .expect("handle_upquery invariant");
                    r
                })
            }),
        }])
    }

    fn description(&self, detailed: bool) -> String {
        if !detailed {
            return "Paginate".into();
        }

        format!(
            "Paginate limit={} γ[{}] o[{}]",
            self.limit,
            self.group_by.iter().join(", "),
            self.order
        )
    }

    fn is_selective(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ops::test::MockGraph;
    use crate::processing::ColumnMiss;

    fn setup() -> (MockGraph, IndexPair) {
        let mut g = MockGraph::new();
        let s = g.add_base("source", &["x", "y"]);

        // order by x where y = ?
        g.set_op(
            "paginate",
            &["x", "y", "page"],
            Paginate::new(
                s.as_global(),
                vec![(0, OrderType::OrderDescending)],
                vec![1],
                3,
            ),
            true,
        );
        (g, s)
    }

    fn with_page(row: &[DataType], page_num: u64) -> Vec<DataType> {
        let mut res = row.to_vec();
        res.push(page_num.into());
        res
    }

    #[test]
    fn column_source_for_group_by() {
        let (g, s) = setup();
        let src = g.node().column_source(&[1]);
        assert_eq!(
            src,
            ColumnSource::ExactCopy(ColumnRef {
                node: s.as_global(),
                columns: vec1![1]
            })
        );
    }

    #[test]
    fn column_source_for_grouped_page_lookup() {
        let (g, _) = setup();
        let src = g.node().column_source(&[1, 2]);
        assert_eq!(
            src,
            ColumnSource::GeneratedFromColumns(vec1![ColumnRef {
                node: g.node_index().as_global(),
                columns: vec1![1],
            }])
        );
    }

    #[test]
    fn column_source_for_page_only_lookup() {
        let (g, s) = setup();
        let src = g.node().column_source(&[2]);
        assert_eq!(src, ColumnSource::RequiresFullReplay(vec1![s.as_global()]));
    }

    #[test]
    fn suggest_indexes() {
        let (g, _) = setup();
        let res = g.node().suggest_indexes(g.node_index().as_global());
        assert_eq!(res.len(), 1);
        assert_eq!(
            res[&g.node_index().as_global()],
            LookupIndex::Strict(Index::hash_map(vec![1]))
        );
    }

    #[test]
    fn handle_upquery_for_page_query() {
        let (g, _) = setup();
        let res = g
            .node_mut()
            .handle_upquery(ColumnMiss {
                node: *g.node_index(),
                column_indices: vec1![1, 2],
                missed_keys: vec1![vec1![DataType::from("a"), DataType::from(1)].into()],
            })
            .unwrap();

        assert_eq!(res.len(), 1);
        assert_eq!(
            *res.first().unwrap(),
            ColumnMiss {
                node: *g.node_index(),
                column_indices: vec1![1],
                missed_keys: vec1![vec1![DataType::from("a")].into()]
            }
        );
    }

    #[test]
    fn first_row_first_page() {
        let (mut g, _) = setup();

        let r1a: Vec<DataType> = vec![1.into(), "a".into()];
        let r1a_p0: Vec<DataType> = vec![1.into(), "a".into(), 0.into()];

        let res = g.narrow_one_row(r1a, true);
        assert_eq!(res, vec![r1a_p0].into());
    }

    #[test]
    fn two_pages() {
        let (mut g, _) = setup();

        let r1a = vec![1.into(), "a".into()];
        let r2a = vec![2.into(), "a".into()];
        let r3a = vec![3.into(), "a".into()];
        let r4a = vec![4.into(), "a".into()];
        let res = g.narrow_one(
            vec![r1a.clone(), r2a.clone(), r3a.clone(), r4a.clone()],
            true,
        );

        assert_eq!(
            res,
            vec![
                with_page(&r1a, 0),
                with_page(&r2a, 0),
                with_page(&r3a, 0),
                with_page(&r4a, 1)
            ]
            .into()
        );
    }

    #[test]
    fn multiple_groups() {
        let (mut g, _) = setup();

        let r1a = vec![1.into(), "a".into()];
        let r2a = vec![2.into(), "a".into()];
        let r3a = vec![3.into(), "a".into()];
        let r4a = vec![4.into(), "a".into()];
        let r1b = vec![1.into(), "b".into()];
        let r2b = vec![2.into(), "b".into()];
        let r3b = vec![3.into(), "b".into()];
        let r4b = vec![4.into(), "b".into()];

        let res = g.narrow_one(
            vec![
                r1a.clone(),
                r2a.clone(),
                r3a.clone(),
                r4a.clone(),
                r1b.clone(),
                r2b.clone(),
                r3b.clone(),
                r4b.clone(),
            ],
            true,
        );

        assert_eq!(
            res,
            vec![
                with_page(&r1a, 0),
                with_page(&r2a, 0),
                with_page(&r3a, 0),
                with_page(&r4a, 1),
                with_page(&r1b, 0),
                with_page(&r2b, 0),
                with_page(&r3b, 0),
                with_page(&r4b, 1)
            ]
            .into()
        );
    }

    #[test]
    fn write_to_second_page() {
        let (mut g, _) = setup();

        let r1a = vec![1.into(), "a".into()];
        let r2a = vec![2.into(), "a".into()];
        let r3a = vec![3.into(), "a".into()];
        let r4a = vec![4.into(), "a".into()];

        g.narrow_one(vec![r1a, r2a, r3a], true);

        let res = g.narrow_one_row(r4a.clone(), true);
        assert_eq!(res, vec![with_page(&r4a, 1)].into());
    }

    #[test]
    fn shift_between_pages() {
        let (mut g, _) = setup();

        let r1a = vec![1.into(), "a".into()];
        let r2a = vec![2.into(), "a".into()];
        let r3a = vec![3.into(), "a".into()];
        let r4a = vec![4.into(), "a".into()];

        g.narrow_one(vec![r1a, r2a, r4a.clone()], true);

        let res = g.narrow_one_row(r3a.clone(), true);
        assert_eq!(
            res,
            vec![
                (with_page(&r3a, 0), true),
                (with_page(&r4a, 0), false),
                (with_page(&r4a, 1), true),
            ]
            .into()
        );
    }

    #[test]
    fn negative_record_shifting_page() {
        let (mut g, _) = setup();

        let r1a = vec![1.into(), "a".into()];
        let r2a = vec![2.into(), "a".into()];
        let r3a = vec![3.into(), "a".into()];
        let r4a = vec![4.into(), "a".into()];

        g.narrow_one(vec![r1a, r2a, r3a.clone(), r4a.clone()], true);

        let res = g.narrow_one_row((r3a.clone(), false), true);
        assert_eq!(
            res,
            vec![
                (with_page(&r3a, 0), false),
                (with_page(&r4a, 1), false),
                (with_page(&r4a, 0), true),
            ]
            .into()
        );
    }
}
