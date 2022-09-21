use std::borrow::{Borrow, Cow};
use std::collections::{btree_map, BTreeMap, HashMap, HashSet};
use std::hash::Hash;
use std::{iter, ops};

use dataflow_state::Rows;
use itertools::Either;
use launchpad::Indices;
use readyset::internal::LocalNodeIndex;
use readyset::KeyComparison;
use tracing::trace;
use vec1::Vec1;

use super::{RemappedKeys, TriggerEndpoint};
use crate::prelude::*;
use crate::NodeMap;

/// Information about the subset of a replay path that is relevant to a particular domain.
///
/// For more information about replay paths, see the [docs page][]
///
/// [docs page]: http://docs/dataflow/replay_paths.html
#[derive(Debug)]
pub(crate) struct ReplayPath {
    pub(super) source: Option<LocalNodeIndex>,
    /// Partial index (if any) at the *target* of this replay path.
    pub(super) target_index: Option<Index>,
    /// The nodes in the replay path.
    pub(super) path: Vec1<ReplayPathSegment>,
    pub(super) notify_done: bool,
    pub(crate) partial_unicast_sharder: Option<NodeIndex>,
    pub(super) trigger: TriggerEndpoint,
}

impl ReplayPath {
    /// Return a reference to the last [`ReplayPathSegment`] of this replay path
    pub(crate) fn last_segment(&self) -> &ReplayPathSegment {
        self.path.last()
    }

    /// If the target of this replay path is in this domain, return the node index of that target
    pub(crate) fn target_node(&self) -> Option<LocalNodeIndex> {
        self.path
            .iter()
            .find(|segment| segment.is_target)
            .map(|segment| segment.node)
            .or(self.source)
    }
}

/// Newtype wrapper for [`LocalNodeIndex`] explicitly signifying that it is the *destination* (the
/// last node) of a replay path.
///
/// Used to avoid mixing up argument order when specifying both the target and the destination of a
/// replay path adjacent to each other in a function
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) struct Destination(pub(crate) LocalNodeIndex);

/// Newtype wrapper for [`LocalNodeIndex`] explicitly signifying that it is the *target* of a replay
/// path.
///
/// Used to avoid mixing up argument order when specifying both the target and the destination of a
/// replay path adjacent to each other in a function
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) struct Target(pub(crate) LocalNodeIndex);

/// Information required to add a new replay path to the set of replay paths.
///
/// Used primarily as a temporary data struct to avoid an unwieldly-long argument list in
/// [`ReplayPaths::insert`]
pub(super) struct ReplayPathSpec {
    pub(super) tag: Tag,
    pub(super) source: Option<LocalNodeIndex>,
    pub(super) source_index: Option<Index>,
    pub(super) path: Vec1<ReplayPathSegment>,
    pub(super) partial_unicast_sharder: Option<NodeIndex>,
    pub(super) notify_done: bool,
    pub(super) trigger: TriggerEndpoint,
}

/// Information about the source of some generated columns in a node
#[derive(Debug)]
struct GeneratedColumns {
    /// The downstream tag for a replay path which will attempt to query an index on these
    /// generated columns
    downstream_tag: Tag,
    /// The set of columns which these columns are generated from
    generated_from: Vec1<ColumnRef<LocalNodeIndex>>,
}

/// Data structure containing all the information about the replay paths that go through a single
/// domain. This struct contains multiple ways of efficiently resolving replay paths, including
/// looking them up by the index they can fill, and by the [`Tag`] which uniquely identifies them
///
/// In addition, this data structure tracks the set of nodes in a domain which contain [generated
/// columns][], and which tags will attempt to perform replays into those generated columns
///
/// For more information about replay paths, see the [docs page][]
///
/// [generated columns]: noria_dataflow::processing::ColumnSource::GeneratedFromColumns
/// [docs page]: http://docs/dataflow/replay_paths.html
#[derive(Debug, Default)]
pub(super) struct ReplayPaths {
    /// Map of replay paths by tag
    by_tag: BTreeMap<Tag, ReplayPath>,

    /// Map from destination nodes of replay paths, to *target* nodes of replay paths, to indexes,
    /// to tags for those replay paths.
    ///
    /// The target of a replay path will almost always be the same as the destination, except in
    /// the case of *extended* replay paths. See [the docs section on straddled
    /// joins][straddled-joins] for more information about extended replay paths
    ///
    /// [straddled-joins]: http://docs/dataflow/replay_paths.html#straddled-joins
    by_dst: NodeMap<NodeMap<HashMap<Index, Vec<Tag>>>>,

    /// Map from nodes, to columns which are "generated" by that node, meaning those columns do not
    /// appear unchanged in exactly one of that node's parents, to information about the source of
    /// those generated columns.
    ///
    /// If a node's columns appear as a key of this map, then misses on those columns require the
    /// use of [`Ingredient::handle_upquery`]
    generated_columns: NodeMap<HashMap<Vec<usize>, Vec<GeneratedColumns>>>,
}

impl ReplayPaths {
    /// Look up the replay path with the given tag in the set of replay paths, and return a
    /// reference to it if it exists.
    pub(super) fn get(&self, tag: Tag) -> Option<&ReplayPath> {
        self.by_tag.get(&tag)
    }

    /// Look up the replay path with the given tag in the set of replay paths, and return a
    /// mutable reference to it if it exists.
    pub(super) fn get_mut(&mut self, tag: Tag) -> Option<&mut ReplayPath> {
        self.by_tag.get_mut(&tag)
    }

    /// Look up the list of tags, if any, identifying replay paths targeting the given index in in
    /// the given target node, and destined for the given destination node.
    ///
    /// The target of a replay path will almost always be the same as the destination, except in the
    /// case of *extended* replay paths. See [the docs section on straddled joins][straddled-joins]
    /// for more information about extended replay paths
    ///
    /// [straddled-joins]: http://docs/dataflow/replay_paths.html#straddled-joins
    pub(super) fn tags_for_index(
        &self,
        Destination(destination): Destination,
        Target(target): Target,
        index: &Index,
    ) -> Option<&Vec<Tag>> {
        let indexes = self.by_dst.get(destination)?.get(target)?;
        indexes.get(index).or_else(|| {
            // we might be doing what is effectively a point lookup into a BTree index if we do
            // a lookup of a double-ended range where both ends are inclusive bounds of the same
            // value - if that happens, we still need to resolve the tag for the BTree index,
            // not the Hash index.
            if index.index_type == IndexType::HashMap {
                indexes.get(&Index::new(IndexType::BTreeMap, index.columns.clone()))
            } else {
                None
            }
        })
    }

    /// Construct an iterator over all downstream paths and keys which "depend on" the given keys
    /// and rows in the given index, and hence need to be evicted after those keys and rows are
    /// evicted.
    pub(super) fn downstream_dependent_paths<'a>(
        &'a self,
        node: LocalNodeIndex,
        index: &'a Index,
        keys: &'a [KeyComparison],
        rows: Rows,
        remapped_keys: &'a RemappedKeys,
    ) -> impl Iterator<Item = (Tag, &'a ReplayPath, Cow<'a, [KeyComparison]>)> + 'a {
        // TODO: this is a linear walk of replay paths -- we should make that not linear
        self.by_tag
            .iter()
            .filter(move |(_, path)| path.source == Some(node))
            .flat_map(move |(tag, path)| {
                match &path.trigger {
                    TriggerEndpoint::Local(key) | TriggerEndpoint::Start(key) => {
                        let maybe_downstream_keys = path.target_node().and_then(|target| {
                            path.target_index.as_ref().and_then(|target_index| {
                                remapped_keys.get(
                                    path.last_segment().node,
                                    target,
                                    &target_index.columns,
                                    keys,
                                )
                            })
                        });

                        if let Some(downstream) = maybe_downstream_keys {
                            Either::Left(downstream.map(move |(tag, keys)| {
                                (tag, self.get(tag).unwrap(), Cow::Borrowed(keys))
                            }))
                        } else if self
                            .generated_columns
                            .get(node)
                            .and_then(|gc| gc.get(&key.columns))
                            .into_iter()
                            .any(|gcs| {
                                gcs.iter().any(|gc| {
                                    gc.downstream_tag == *tag
                                        && gc.generated_from.iter().any(|cr| {
                                            cr.node == node && cr.columns == index.columns
                                        })
                                })
                            })
                        {
                            // If the key we evicted from generates a key, use the column
                            // indices in the generated key to build up a new set of keys to
                            // evict downstream. Conceptually, this is because a generated key
                            // means that key "depends on" the key we just evicted
                            trace!(
                                ?tag,
                                ?key,
                                "Rewriting keys to evict through generated columns"
                            );
                            let keys = Cow::Owned(
                                rows.set_iter()
                                    .map(|(r, _)| {
                                        r.cloned_indices(key.columns.iter().copied())
                                            .unwrap()
                                            .try_into()
                                            .unwrap()
                                    })
                                    .collect::<HashSet<_>>()
                                    .into_iter()
                                    .collect(),
                            );

                            Either::Right(Either::Left(iter::once((*tag, path, keys))))
                        } else if key == index {
                            // TODO: what if the key is the same, but with the columns in another
                            // order?
                            Either::Right(Either::Left(iter::once((
                                *tag,
                                path,
                                Cow::Borrowed(keys),
                            ))))
                        } else {
                            Either::Right(Either::Right(iter::empty()))
                        }
                    }
                    _ => Either::Right(Either::Right(iter::empty())),
                }
            })
    }

    /// If the given set of columns in the given node are generated, return the tag for the replay
    /// path with those columns at its source.
    pub(super) fn tags_for_generated_columns<Q>(
        &self,
        node: LocalNodeIndex,
        cols: &Q,
    ) -> Option<Vec<Tag>>
    where
        Vec<usize>: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        self.generated_columns
            .get(node)?
            .get(cols)
            .map(|gcs| gcs.iter().map(|gc| gc.downstream_tag).collect())
    }

    /// Are the given columns in the given node generated?
    pub(super) fn columns_are_generated<Q>(&self, node: LocalNodeIndex, columns: &Q) -> bool
    where
        Vec<usize>: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        self.generated_columns
            .get(node)
            .map_or(false, |by_cols| by_cols.contains_key(columns))
    }

    /// Return a list of all replay paths that pass through `from` into `node`, represented as the
    /// tag for the replay path, and the set of columns in the `node` which that replay path fills
    pub(super) fn paths_through(&self, node: &Node, from: NodeIndex) -> Vec<(Tag, Vec<usize>)> {
        // TODO: this is a linear walk of replay paths -- we should make that not linear
        self.by_tag
            .iter()
            .filter_map(|(&tag, rp)| {
                rp.path
                    .iter()
                    .find(|segment| segment.node == node.local_addr())
                    .and_then(|segment| segment.partial_index.as_ref())
                    .and_then(|index| {
                        // we need to find the *input* column that produces that output.
                        //
                        // if one of the columns for this replay path's keys does not
                        // resolve into the ancestor we got the update from, we don't need
                        // to issue an eviction for that path. this is because we *missed*
                        // on the join column in the other side, so we *know* it can't have
                        // forwarded anything related to the write we're now handling.
                        index
                            .columns
                            .iter()
                            .map(|&k| {
                                node.parent_columns(k)
                                    .into_iter()
                                    .find(|&(ni, _)| ni == from)
                                    .and_then(|k| k.1)
                            })
                            .collect::<Option<Vec<_>>>()
                    })
                    .map(move |k| (tag, k))
            })
            .collect()
    }

    /// Insert a new replay path into this set of replay paths
    pub(super) fn insert(&mut self, path: ReplayPathSpec) -> ReadySetResult<()> {
        let ReplayPathSpec {
            tag,
            source,
            source_index,
            path,
            partial_unicast_sharder,
            notify_done,
            trigger,
        } = path;

        let target_index = if let TriggerEndpoint::End { .. } | TriggerEndpoint::Local(..) = trigger
        {
            let (target_node, target_index) =
                if let Some(target_segment) = path.iter().find(|segment| segment.is_target) {
                    (
                        target_segment.node,
                        target_segment.partial_index.clone().unwrap(),
                    )
                } else {
                    (
                        source.ok_or_else(|| {
                            internal_err!("Path without target must have source in the same domain")
                        })?,
                        source_index.ok_or_else(|| {
                            // I think?
                            internal_err!("Partial replay path must have an index at the source")
                        })?,
                    )
                };

            self.by_dst
                .entry(path.last().node)
                .or_default()
                .entry(target_node)
                .or_default()
                .entry(target_index.clone())
                .or_default()
                .push(tag);

            Some(target_index)
        } else {
            None
        };

        self.by_tag.insert(
            tag,
            ReplayPath {
                source,
                target_index,
                path,
                notify_done,
                partial_unicast_sharder,
                trigger,
            },
        );

        Ok(())
    }

    /// Record that a given set of columns are generated by a node, and that a particular tag is
    /// going to want to perform replays sourced at those columns
    pub(super) fn insert_generated_columns(
        &mut self,
        node: LocalNodeIndex,
        columns: Vec<usize>,
        generated_from: Vec1<ColumnRef<LocalNodeIndex>>,
        tag: Tag,
    ) {
        self.generated_columns
            .entry(node)
            .or_default()
            .entry(columns)
            .or_default()
            .push(GeneratedColumns {
                downstream_tag: tag,
                generated_from,
            });
    }
}

impl ops::Index<Tag> for ReplayPaths {
    type Output = ReplayPath;

    fn index(&self, tag: Tag) -> &Self::Output {
        &self.by_tag[&tag]
    }
}

impl<'a> IntoIterator for &'a ReplayPaths {
    type Item = (&'a Tag, &'a ReplayPath);

    type IntoIter = btree_map::Iter<'a, Tag, ReplayPath>;

    fn into_iter(self) -> Self::IntoIter {
        self.by_tag.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_then_resolve_single_path() {
        let mut paths = ReplayPaths::default();
        paths
            .insert(ReplayPathSpec {
                tag: Tag::new(1),
                source: Some(LocalNodeIndex::make(0)),
                source_index: Some(Index::hash_map(vec![0])),
                path: vec1![ReplayPathSegment {
                    node: LocalNodeIndex::make(1),
                    force_tag_to: None,
                    partial_index: Some(Index::hash_map(vec![0])),
                    is_target: true
                }],
                partial_unicast_sharder: None,
                notify_done: false,
                trigger: TriggerEndpoint::Local(Index::hash_map(vec![0])),
            })
            .unwrap();

        let resolved_tags = paths.tags_for_index(
            Destination(LocalNodeIndex::make(1)),
            Target(LocalNodeIndex::make(1)),
            &Index::hash_map(vec![0]),
        );

        assert_eq!(resolved_tags, Some(&vec![Tag::new(1)]));
    }

    #[test]
    fn insert_then_resolve_extended_path() {
        let mut paths = ReplayPaths::default();
        paths
            .insert(ReplayPathSpec {
                tag: Tag::new(1),
                source: Some(LocalNodeIndex::make(0)),
                source_index: Some(Index::hash_map(vec![0])),
                path: vec1![ReplayPathSegment {
                    node: LocalNodeIndex::make(1),
                    force_tag_to: None,
                    partial_index: Some(Index::hash_map(vec![1, 2])),
                    is_target: false
                }],
                partial_unicast_sharder: None,
                notify_done: false,
                trigger: TriggerEndpoint::Local(Index::hash_map(vec![0])),
            })
            .unwrap();

        let resolved_tags = paths.tags_for_index(
            Destination(LocalNodeIndex::make(1)),
            Target(LocalNodeIndex::make(0)),
            &Index::hash_map(vec![0]),
        );

        assert_eq!(resolved_tags, Some(&vec![Tag::new(1)]));
    }

    mod downstream_dependent_paths {
        use dataflow_state::Row;

        use super::*;

        #[test]
        fn generated_from_self() {
            // Node l0 generates columns [0, 1] from columns [0] in itself
            let mut paths = ReplayPaths::default();
            paths
                .insert(ReplayPathSpec {
                    tag: Tag::new(1),
                    source: Some(LocalNodeIndex::make(0)),
                    source_index: Some(Index::hash_map(vec![0, 1])),
                    path: vec1![ReplayPathSegment {
                        node: LocalNodeIndex::make(1),
                        force_tag_to: None,
                        partial_index: Some(Index::hash_map(vec![0, 1])),
                        is_target: false
                    }],
                    partial_unicast_sharder: None,
                    notify_done: false,
                    trigger: TriggerEndpoint::Start(Index::hash_map(vec![0, 1])),
                })
                .unwrap();
            paths.insert_generated_columns(
                LocalNodeIndex::make(0),
                vec![0, 1],
                vec1![ColumnRef {
                    node: LocalNodeIndex::make(0),
                    columns: vec1![0]
                }],
                Tag::new(1),
            );

            // We just evicted [Equal([1])] from columns [0], resulting in {[1, "a"]}
            let index = Index::hash_map(vec![0]);
            let evicted_keys = [KeyComparison::Equal(vec1![DfValue::from(1)])];
            let remapped_keys = Default::default();
            let res = paths
                .downstream_dependent_paths(
                    LocalNodeIndex::make(0),
                    &index,
                    &evicted_keys,
                    vec![Row::from(vec![DfValue::from(1), DfValue::from("a")])]
                        .into_iter()
                        .collect(),
                    &remapped_keys,
                )
                .collect::<Vec<_>>();

            // Now, we should evict Equal([1, "a"]) in the downstream path
            assert_eq!(res.len(), 1);
            let (tag, _path, keys) = res.first().unwrap();
            assert_eq!(*tag, Tag::new(1));
            assert_eq!(
                *keys,
                vec![KeyComparison::Equal(vec1![
                    DfValue::from(1),
                    DfValue::from("a")
                ])]
            )
        }
    }
}
