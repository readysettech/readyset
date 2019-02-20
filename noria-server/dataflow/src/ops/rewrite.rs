use prelude::*;
use std::collections::{HashMap, HashSet};

/// A Rewrite data-flow operator.
/// This node rewrites a column from a subset of records to a pre-determined value.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Rewrite {
    src: IndexPair,
    signal: IndexPair,

    rw_col: usize,
    value: DataType,

    signal_key: usize,
}

impl Rewrite {
    /// Creates a new instance of Rewrite.
    ///
    /// `src` and `signal` are the parent nodes of the operator.
    /// `src` forwards records that may or may not have their columns rewritten,
    /// while `signal` signals which of those records should actually be
    /// rewritten.
    /// `signal_key` specifies what key to use for lookups in the signalling parent
    /// to determine if a row should be rewritten or not.
    /// `rw_col` specifies which column is being rewritten and `value` dictates
    /// the value the column is rewritten to.
    pub fn new(
        src: NodeIndex,
        signal: NodeIndex,
        rw_col: usize,
        value: DataType,
        signal_key: usize,
    ) -> Rewrite {
        Rewrite {
            src: src.into(),
            signal: signal.into(),
            rw_col,
            value,
            signal_key,
        }
    }

    fn rewrite(&self, mut r: Vec<DataType>) -> Vec<DataType> {
        r[self.rw_col] = self.value.clone();
        r
    }
}

impl Ingredient for Rewrite {
    fn take(&mut self) -> NodeOperator {
        Clone::clone(self).into()
    }

    fn ancestors(&self) -> Vec<NodeIndex> {
        vec![self.src.as_global(), self.signal.as_global()]
    }

    fn is_join(&self) -> bool {
        true
    }

    fn must_replay_among(&self) -> Option<HashSet<NodeIndex>> {
        Some(Some(self.src.as_global()).into_iter().collect())
    }

    fn on_connected(&mut self, _: &Graph) {}

    fn on_commit(&mut self, _: NodeIndex, remap: &HashMap<NodeIndex, IndexPair>) {
        self.src.remap(remap);
        self.signal.remap(remap);
    }

    fn on_input(
        &mut self,
        _: &mut Executor,
        from: LocalNodeIndex,
        rs: Records,
        _: &mut Tracer,
        replay_key_cols: Option<&[usize]>,
        nodes: &DomainNodes,
        state: &StateMap,
    ) -> ProcessingResult {
        debug_assert!(from == *self.src || from == *self.signal);
        let mut misses = Vec::new();
        let mut emit_rs = Vec::with_capacity(rs.len());

        if rs.is_empty() {
            return ProcessingResult {
                results: rs,
                misses: vec![],
            };
        }

        // TODO: it would be nice to just use `rs`,
        // instead of creating a new `emit_rs`
        for r in rs {
            if from == *self.src {
                let key = r[self.signal_key].clone();
                // ask signal if column should be rewritten
                let rc = self
                    .lookup(*self.signal, &[0], &KeyType::Single(&key), nodes, state)
                    .unwrap();

                if rc.is_none() {
                    misses.push(Miss {
                        on: *self.signal,
                        lookup_idx: vec![0],
                        lookup_cols: vec![self.signal_key],
                        replay_cols: replay_key_cols.map(Vec::from),
                        record: r.extract().0,
                    });
                    continue;
                }

                let mut rc = rc.unwrap().peekable();
                if rc.peek().is_none() {
                    emit_rs.push(r);
                } else {
                    let (r, positive) = r.extract();
                    let row = (self.rewrite(r), positive).into();
                    emit_rs.push(row);
                }
            } else if from == *self.signal {
                let key = r[0].clone();
                let other_rows = self
                    .lookup(
                        *self.src,
                        &[self.signal_key],
                        &KeyType::Single(&key),
                        nodes,
                        state,
                    )
                    .unwrap();

                if other_rows.is_none() {
                    // replays always happen from the `src` side,
                    // so replay_key_col must be None
                    assert_eq!(replay_key_cols, None);
                    misses.push(Miss {
                        on: *self.src,
                        lookup_idx: vec![self.signal_key],
                        lookup_cols: vec![0],
                        replay_cols: replay_key_cols.map(Vec::from),
                        record: r.extract().0,
                    });
                    continue;
                }

                let mut other_rows = other_rows.unwrap().peekable();
                if other_rows.peek().is_none() {
                    continue;
                } else {
                    while other_rows.peek().is_some() {
                        let other = other_rows.next().unwrap().to_vec();

                        emit_rs.push((other.clone(), !r.is_positive()).into());
                        emit_rs.push((self.rewrite(other), r.is_positive()).into());
                    }
                }
            }
        }

        ProcessingResult {
            results: emit_rs.into(),
            misses,
        }
    }

    fn suggest_indexes(&self, _: NodeIndex) -> HashMap<NodeIndex, (Vec<usize>, bool)> {
        vec![
            (self.signal.as_global(), (vec![0], true)),
            (self.src.as_global(), (vec![self.signal_key], true)),
        ]
        .into_iter()
        .collect()
    }

    fn resolve(&self, col: usize) -> Option<Vec<(NodeIndex, usize)>> {
        // We can't resolve the rewritten column
        if col == self.rw_col {
            None
        } else {
            Some(vec![(self.src.as_global(), col)])
        }
    }

    fn description(&self, detailed: bool) -> String {
        if !detailed {
            return String::from("Rw");
        }

        format!("Rw[{}]", self.rw_col)
    }

    fn parent_columns(&self, column: usize) -> Vec<(NodeIndex, Option<usize>)> {
        vec![(self.src.as_global(), Some(column))]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use ops;

    fn setup() -> (ops::test::MockGraph, IndexPair, IndexPair) {
        let mut g = ops::test::MockGraph::new();
        let src = g.add_base("src", &["id", "rw_col"]);
        let signal = g.add_base("signal", &["id"]);

        let rw = Rewrite::new(src.as_global(), signal.as_global(), 1, "NONE".into(), 0);

        g.set_op("rewrite", &["rw0", "rw1"], rw, false);
        (g, src, signal)
    }

    #[test]
    fn it_works() {
        let (mut rw, src, should_rw) = setup();

        let src_a1 = vec![1.into(), "a".into()];
        let src_b2 = vec![2.into(), "b".into()];

        let rw1 = vec![1.into()];
        let rw2 = vec![2.into()];

        let result = vec![(vec![1.into(), "a".into()], true)].into();
        rw.seed(src, src_a1.clone());
        let rs = rw.one_row(src, src_a1.clone(), false);
        assert_eq!(rs, result);

        rw.seed(should_rw, rw2.clone());
        rw.one_row(should_rw, rw2.clone(), false);

        // forward [2, b] to src; should be rewritten and produce [2, "NONE"].
        let result = vec![(vec![2.into(), "NONE".into()], true)].into();
        rw.seed(src, src_b2.clone());
        let rs = rw.one_row(src, src_b2.clone(), false);
        assert_eq!(rs, result);

        // forward 1 to signal; should produce Positive([1, "NONE"]) and Negative([1, "a"]).
        let result = vec![
            (vec![1.into(), "a".into()], false),
            (vec![1.into(), "NONE".into()], true),
        ]
        .into();
        rw.seed(should_rw, rw1.clone());
        let rs = rw.one_row(should_rw, rw1.clone(), false);
        assert_eq!(rs, result);
    }
}
