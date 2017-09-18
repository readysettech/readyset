use std::collections::{HashMap, HashSet};

use flow::prelude::*;

#[derive(Clone, Debug, Serialize, Deserialize)]
enum Emit {
    AllFrom(IndexPair),
    Project {
        emit: HashMap<IndexPair, Vec<usize>>,

        // generated
        emit_l: Map<Vec<usize>>,
        cols: HashMap<IndexPair, usize>,
        cols_l: Map<usize>,
    },
}

/// A union of a set of views.
#[derive(Debug, Serialize, Deserialize)]
pub struct Union {
    emit: Emit,
    replay_key: Option<Map<usize>>,
    replay_pieces: HashMap<DataType, Map<Records>>,
}

impl Clone for Union {
    fn clone(&self) -> Self {
        Union {
            emit: self.emit.clone(),
            // nothing can have been received yet
            replay_key: None,
            replay_pieces: HashMap::new(),
        }
    }
}

impl Union {
    /// Construct a new union operator.
    ///
    /// When receiving an update from node `a`, a union will emit the columns selected in `emit[a]`.
    /// `emit` only supports omitting columns, not rearranging them.
    pub fn new(emit: HashMap<NodeIndex, Vec<usize>>) -> Union {
        assert!(!emit.is_empty());
        for emit in emit.values() {
            let mut last = &emit[0];
            for i in emit {
                if i < last {
                    unimplemented!();
                }
                last = i;
            }
        }
        let emit = emit.into_iter().map(|(k, v)| (k.into(), v)).collect();
        Union {
            emit: Emit::Project {
                emit,
                emit_l: Map::new(),
                cols: HashMap::new(),
                cols_l: Map::new(),
            },
            replay_key: None,
            replay_pieces: HashMap::new(),
        }
    }

    /// Construct a new union operator meant to de-shard a sharded data-flow subtree.
    pub fn new_deshard(parent: NodeIndex) -> Union {
        Union {
            emit: Emit::AllFrom(parent.into()),
            replay_key: None,
            replay_pieces: HashMap::new(),
        }
    }
}

impl Ingredient for Union {
    fn take(&mut self) -> NodeOperator {
        Clone::clone(self).into()
    }

    fn ancestors(&self) -> Vec<NodeIndex> {
        match self.emit {
            Emit::AllFrom(p) => vec![p.as_global()],
            Emit::Project { ref emit, .. } => emit.keys().map(|k| k.as_global()).collect(),
        }
    }

    fn on_connected(&mut self, g: &Graph) {
        if let Emit::Project {
            ref mut cols,
            ref emit,
            ..
        } = self.emit
        {
            cols.extend(emit.keys().map(|&n| (n, g[n.as_global()].fields().len())));
        }
    }

    fn on_commit(&mut self, _: NodeIndex, remap: &HashMap<NodeIndex, IndexPair>) {
        match self.emit {
            Emit::Project {
                ref mut emit,
                ref mut cols,
                ref mut emit_l,
                ref mut cols_l,
            } => {
                use std::mem;
                let mapped_emit = emit.drain()
                    .map(|(mut k, v)| {
                        k.remap(remap);
                        emit_l.insert(*k, v.clone());
                        (k, v)
                    })
                    .collect();
                let mapped_cols = cols.drain()
                    .map(|(mut k, v)| {
                        k.remap(remap);
                        cols_l.insert(*k, v.clone());
                        (k, v)
                    })
                    .collect();
                mem::replace(emit, mapped_emit);
                mem::replace(cols, mapped_cols);
            }
            Emit::AllFrom(ref mut p) => {
                p.remap(remap);
            }
        }
    }

    fn on_input(
        &mut self,
        from: LocalNodeIndex,
        rs: Records,
        _: &mut Tracer,
        _: Option<usize>,
        _: &DomainNodes,
        _: &StateMap,
    ) -> ProcessingResult {
        match self.emit {
            Emit::AllFrom(_) => ProcessingResult {
                results: rs,
                misses: Vec::new(),
            },
            Emit::Project { ref emit_l, .. } => {
                let rs = rs.into_iter()
                    .map(move |rec| {
                        let (r, pos) = rec.extract();

                        // yield selected columns for this source
                        // TODO: if emitting all in same order then avoid clone
                        let res = emit_l[&from].iter().map(|&col| r[col].clone()).collect();

                        // return new row with appropriate sign
                        if pos {
                            Record::Positive(res)
                        } else {
                            Record::Negative(res)
                        }
                    })
                    .collect();
                ProcessingResult {
                    results: rs,
                    misses: Vec::new(),
                }
            }
        }
    }

    fn on_input_raw(
        &mut self,
        from: LocalNodeIndex,
        rs: Records,
        tracer: &mut Tracer,
        is_replay_of: Option<(usize, &HashSet<Vec<DataType>>)>,
        nshards: usize,
        n: &DomainNodes,
        s: &StateMap,
    ) -> RawProcessingResult {
        use std::mem;

        // NOTE: in the special case of us being a shard merge node (i.e., when
        // self.emit.is_empty()), `from` will *actually* hold the shard index of
        // the sharded egress that sent us this record. this should make everything
        // below just work out.
        match is_replay_of {
            None => {
                if self.replay_key.is_none() || self.replay_pieces.is_empty() {
                    // no replay going on, so we're done.
                    return RawProcessingResult::Regular(
                        self.on_input(from, rs, tracer, None, n, s),
                    );
                }

                // partial replays are flowing through us, and at least one piece is being waited
                // for. we need to keep track of any records that succeed a replay piece (and thus
                // aren't included in it) before the other pieces come in. note that it's perfectly
                // safe for us to also forward them, since they'll just be dropped when they miss
                // in the downstream node. in fact, we *must* forward them, becuase there may be
                // *other* nodes downstream that do *not* have holes for the key in question.
                for r in &rs {
                    let k = self.replay_key.as_ref().unwrap()[&from];
                    if let Some(ref mut pieces) = self.replay_pieces.get_mut(&r[k]) {
                        if let Some(ref mut rs) = pieces.get_mut(&from) {
                            // we've received a replay piece from this ancestor already for this
                            // key, and are waiting for replay pieces from other ancestors. we need
                            // to incorporate this record into the replay piece so that it doesn't
                            // end up getting lost.
                            rs.push(r.clone());
                        } else {
                            // we haven't received a replay piece for this key from this ancestor
                            // yet, so we know that the eventual replay piece must include this
                            // record.
                        }
                    } else {
                        // we're not waiting on replay pieces for this key
                    }
                }

                RawProcessingResult::Regular(self.on_input(from, rs, tracer, None, n, s))
            }
            Some((key_col, key_vals)) => {
                // FIXME: with multi-partial indices, we may now need to track *multiple* ongoing
                // replays!

                if self.replay_key.is_none() {
                    // the replay key is for our *output* column
                    // which might translate to different columns in our inputs
                    match self.emit {
                        Emit::AllFrom(_) => {
                            self.replay_key = Some(Some((from, key_col)).into_iter().collect());
                        }
                        Emit::Project { ref emit_l, .. } => {
                            self.replay_key = Some(
                                emit_l
                                    .iter()
                                    .map(|(src, emit)| (src, emit[key_col]))
                                    .collect(),
                            );
                        }
                    }
                }

                let mut rs_by_key = rs.into_iter().map(|r| (r[key_col].clone(), r)).fold(
                    HashMap::new(),
                    |mut hm, (key, r)| {
                        hm.entry(key).or_insert_with(Records::default).push(r);
                        hm
                    },
                );


                let required = match self.emit {
                    Emit::AllFrom(_) => nshards,
                    Emit::Project { ref emit, .. } => emit.len(),
                };

                // we're going to pull a little hack here for the sake of performance.
                // (heard that before...)
                // we can't borrow self in both closures below, even though `self.on_input` doesn't
                // access `self.replay_pieces`. if only the compiler was more clever. we get around
                // this by mem::swapping a temporary (empty) HashMap (which doesn't allocate).
                let mut replay_pieces_tmp = HashMap::with_capacity(0);
                mem::swap(&mut self.replay_pieces, &mut replay_pieces_tmp);

                let mut released = HashSet::new();
                let rs = {
                    key_vals
                        .iter()
                        .filter_map(|key| {
                            debug_assert_eq!(key.len(), 1);
                            let key = &key[0];
                            let rs = rs_by_key.remove(key).unwrap_or_else(Records::default);

                            // store this replay piece
                            use std::collections::hash_map::Entry;
                            match replay_pieces_tmp.entry(key.clone()) {
                                Entry::Occupied(e) => {
                                    assert!(!e.get().contains_key(&from));
                                    if e.get().len() == required - 1 {
                                        // release!
                                        let mut m = e.remove();
                                        m.insert(from, rs);
                                        Some((key, m))
                                    } else {
                                        e.into_mut().insert(from, rs);
                                        None
                                    }
                                }
                                Entry::Vacant(h) => {
                                    let mut m = Map::new();
                                    m.insert(from, rs);
                                    if required == 1 {
                                        Some((key, m))
                                    } else {
                                        h.insert(m);
                                        None
                                    }
                                }
                            }
                        })
                        .flat_map(|(key, map)| {
                            released.insert(vec![key.clone()]);
                            map.into_iter()
                        })
                        .flat_map(|(from, rs)| {
                            self.on_input(from, rs, tracer, Some(key_col), n, s).results
                        })
                        .collect()
                };

                // and swap back replay pieces
                mem::swap(&mut self.replay_pieces, &mut replay_pieces_tmp);

                if !released.is_empty() {
                    RawProcessingResult::ReplayPiece(rs, released)
                } else {
                    // no. need to keep buffering (and emit nothing)
                    RawProcessingResult::Captured
                }
            }
        }
    }

    fn suggest_indexes(&self, _: NodeIndex) -> HashMap<NodeIndex, (Vec<usize>, bool)> {
        // index nothing (?)
        HashMap::new()
    }

    fn resolve(&self, col: usize) -> Option<Vec<(NodeIndex, usize)>> {
        match self.emit {
            Emit::AllFrom(p) => Some(vec![(p.as_global(), col)]),
            Emit::Project { ref emit, .. } => Some(
                emit.iter()
                    .map(|(src, emit)| (src.as_global(), emit[col]))
                    .collect(),
            ),
        }
    }

    fn description(&self) -> String {
        // Ensure we get a consistent output by sorting.
        match self.emit {
            Emit::AllFrom(_) => "⊍".to_string(),
            Emit::Project { ref emit, .. } => {
                let mut emit = emit.iter().collect::<Vec<_>>();
                emit.sort();
                emit.iter()
                    .map(|&(src, emit)| {
                        let cols = emit.iter()
                            .map(|e| e.to_string())
                            .collect::<Vec<_>>()
                            .join(", ");
                        format!("{}:[{}]", src.as_global().index(), cols)
                    })
                    .collect::<Vec<_>>()
                    .join(" ⋃ ")
            }
        }
    }
    fn parent_columns(&self, col: usize) -> Vec<(NodeIndex, Option<usize>)> {
        match self.emit {
            Emit::AllFrom(p) => vec![(p.as_global(), Some(col))],
            Emit::Project { ref emit, .. } => emit.iter()
                .map(|(src, emit)| (src.as_global(), Some(emit[col])))
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use ops;

    fn setup() -> (ops::test::MockGraph, IndexPair, IndexPair) {
        let mut g = ops::test::MockGraph::new();
        let l = g.add_base("left", &["l0", "l1"]);
        let r = g.add_base("right", &["r0", "r1", "r2"]);

        let mut emits = HashMap::new();
        emits.insert(l.as_global(), vec![0, 1]);
        emits.insert(r.as_global(), vec![0, 2]);
        g.set_op("union", &["u0", "u1"], Union::new(emits), false);
        (g, l, r)
    }

    #[test]
    fn it_describes() {
        let (u, l, r) = setup();
        assert_eq!(
            u.node().description(),
            format!("{}:[0, 1] ⋃ {}:[0, 2]", l, r)
        );
    }

    #[test]
    fn it_works() {
        let (mut u, l, r) = setup();

        // forward from left should emit original record
        let left = vec![1.into(), "a".into()];
        assert_eq!(u.one_row(l, left.clone(), false), vec![left].into());

        // forward from right should emit subset record
        let right = vec![1.into(), "skipped".into(), "x".into()];
        assert_eq!(
            u.one_row(r, right.clone(), false),
            vec![vec![1.into(), "x".into()]].into()
        );
    }

    #[test]
    fn it_suggests_indices() {
        use std::collections::HashMap;
        let (u, _, _) = setup();
        let me = 1.into();
        assert_eq!(u.node().suggest_indexes(me), HashMap::new());
    }

    #[test]
    fn it_resolves() {
        let (u, l, r) = setup();
        let r0 = u.node().resolve(0);
        assert!(
            r0.as_ref()
                .unwrap()
                .iter()
                .any(|&(n, c)| n == l.as_global() && c == 0)
        );
        assert!(
            r0.as_ref()
                .unwrap()
                .iter()
                .any(|&(n, c)| n == r.as_global() && c == 0)
        );
        let r1 = u.node().resolve(1);
        assert!(
            r1.as_ref()
                .unwrap()
                .iter()
                .any(|&(n, c)| n == l.as_global() && c == 1)
        );
        assert!(
            r1.as_ref()
                .unwrap()
                .iter()
                .any(|&(n, c)| n == r.as_global() && c == 2)
        );
    }
}
