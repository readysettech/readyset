use crate::payload::ReplayPieceContext;
use crate::prelude::NodeIndex;
use crate::Packet;
use common::DataType;
use noria::KeyComparison;
use noria_errors::{internal, ReadySetResult};
use serde::{Deserialize, Serialize};
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::ops::Bound;
use vec1::Vec1;

type ColumnIndexes = Vec<usize>;

/// The set of keys that have been requested for a particular node
#[derive(Serialize, Deserialize, PartialEq, Clone, Default, Debug)]
pub struct NodeKeys {
    /// The allowed keys, grouped by the column indexes they act upon.
    keys: HashMap<ColumnIndexes, HashSet<KeyComparison>>,
}

/// The goal of the [`PacketFilter`] is to avoid sending updates for keys when the destination
/// domain does not have those keys materialized (to avoid unnecessary network traffic), given that
/// those updates will just be discarded at said domain.
/// The [`PacketFilter`] then works by observing which keys are replayed and evicted to build an
/// internal allowlist, which is then used to filter regular update messages. Filtering is disabled
/// by default, and can be enabled by adding the desired destination domain's ingress node to the
/// filter list.
#[derive(Serialize, Deserialize, PartialEq, Clone, Default)]
pub struct PacketFilter {
    /// Stores the information needed to filter [`Record`]s from [`Packet::Message`]s.
    requested_keys: HashMap<NodeIndex, NodeKeys>,
}

impl PacketFilter {
    /// Adds a node to the filter list. This means that the target node
    /// will have its packets processed (and not just skipped) by this [`PacketFilter`].
    /// See [`PacketFilter::process`] for more information on the processing logic.
    pub fn add_for_filtering(&mut self, target: NodeIndex) {
        self.requested_keys
            .entry(target)
            .or_insert_with(NodeKeys::default);
    }

    /// Processes a given [`Packet`], provided with some contextual information.
    /// The processing modifies the packet in-place.
    ///
    /// ## Processing logic
    ///
    /// The processing works as follows. Given a packet meant for a target node that is marked for
    /// filtering:
    /// 1. If the packet is a replay piece ([`Packet::ReplayPiece`]), then the requested keys are
    ///    stored in the filter, along the column indexes they predicate upon.
    /// 2. If the packet is an eviction message ([`Packet::EvictKeys`]), the evicted keys are
    ///    removed from the filter.
    /// 3. If the packet is an update ([`Packet::Message`]), then the packet is stripped down of any
    ///    record that does not comply with the stored keys. If no record survives the filtering,
    ///    then the whole packet should be dropped.
    ///
    /// ## Arguments
    ///
    /// * packet: The [`Packet`] to be analyzed and potentially modified.
    /// * keyed_by: An [`Option`] of [`Vec<usize>`] representing the indexes of the columns of a
    ///   [`Record`], that are used by [`KeyComparison`]s. These indexes should be present whenever
    ///   there's a [`Packet::ReplayPiece`].
    /// * target: The [`NodeIndex`] of the node that will receive the [`Packet`].
    ///
    /// ## Returns
    ///
    /// The result is a [`ReadySetResult<bool>`], which will only be an error if an invariant was
    /// violated during processing.
    /// The [`bool`] determines whether the packet should be sent or dropped.
    pub fn process(
        &mut self,
        packet: &mut Packet,
        keyed_by: Option<&[usize]>,
        target: NodeIndex,
    ) -> ReadySetResult<bool> {
        // First, check if the target node needs to go through the filtering process.
        let node_keys = match self.requested_keys.get_mut(&target) {
            None => return Ok(true),
            Some(node_keys) => node_keys,
        };

        match packet {
            Packet::Message { data, .. } => {
                // If the packet is an update, we must check what keys the target node has previously requested.
                // Based on that, we'll filter the records from the Packet.
                if node_keys.keys.is_empty() {
                    // If no keys were previously requested by the target node, then that
                    // means that all packets must be dropped until it requests some keys.
                    return Ok(false);
                }
                data.retain(|record| {
                    let row = record.row();
                    // TODO(grfn): Make the asymptotics here better
                    node_keys.keys.iter().any(|(ci, keys)| {
                        keys.iter().any(|key| match key {
                            // Here we filter the records based on the keys that the target node
                            // requested previously.
                            KeyComparison::Equal(ref cond) => {
                                check_bound(ci, row, cond, |d1, d2| d1 == d2)
                            }
                            KeyComparison::Range((ref lower_bound, ref upper_bound)) => {
                                check_lower_bound(lower_bound, ci, row)
                                    && check_upper_bound(upper_bound, ci, row)
                            }
                        })
                    })
                });
                // If no records survived the filtering, then signal that the Packet should be
                // dropped.  Otherwise, return the new updated packet with the filtered records.
                Ok(!data.is_empty())
            }
            Packet::ReplayPiece {
                context: ReplayPieceContext::Partial { for_keys, .. },
                ..
            } => {
                // If we are processing a replay piece for a partial replay,
                // then the "keyed-by" parameter must be present.
                match keyed_by {
                    None => {
                        // If it's not, return an error.
                        internal!("The keyed-by parameter must be present for replay messages")
                    }
                    // We add the keys to the node's allowlist information.
                    Some(column_indexes) => self.add_keys(
                        target,
                        column_indexes.to_vec(),
                        &for_keys.iter().cloned().collect::<Vec<_>>(),
                    ),
                }
                Ok(true)
            }
            Packet::EvictKeys {
                link: _,
                tag: _,
                keys,
            } => {
                // We iterate through the keys that must be evicted, as
                // instructed by the packet.
                for k in keys {
                    for (_, allowlisted_keys) in node_keys.keys.iter_mut() {
                        allowlisted_keys.remove(k);
                    }
                }
                Ok(true)
            }
            // For any other packet, we signal to just send the Packet.
            _ => Ok(true),
        }
    }

    /// Adds the given keys to the target node's allowlist information.
    fn add_keys(&mut self, target: NodeIndex, column_indexes: Vec<usize>, keys: &[KeyComparison]) {
        match self.requested_keys.entry(target) {
            Entry::Occupied(mut entry) => {
                let wd = entry.get_mut();
                wd.keys
                    .entry(column_indexes)
                    .or_insert_with(HashSet::default)
                    .extend(keys.iter().cloned());
            }
            Entry::Vacant(_) => (),
        }
    }
}

fn check_lower_bound(lower_bound: &Bound<Vec1<DataType>>, ci: &[usize], row: &[DataType]) -> bool {
    match lower_bound {
        Bound::Included(bound) => {
            check_bound(ci, row, bound, |d1: &DataType, d2: &DataType| d1 >= d2)
        }
        Bound::Excluded(bound) => {
            check_bound(ci, row, bound, |d1: &DataType, d2: &DataType| d1 > d2)
        }
        Bound::Unbounded => true,
    }
}

fn check_upper_bound(upper_bound: &Bound<Vec1<DataType>>, ci: &[usize], row: &[DataType]) -> bool {
    match upper_bound {
        Bound::Included(bound) => {
            check_bound(ci, row, bound, |d1: &DataType, d2: &DataType| d1 <= d2)
        }
        Bound::Excluded(bound) => {
            check_bound(ci, row, bound, |d1: &DataType, d2: &DataType| d1 < d2)
        }
        Bound::Unbounded => true,
    }
}

fn check_bound<T>(ci: &[usize], row: &[DataType], keys: &[DataType], check: T) -> bool
where
    T: Fn(&DataType, &DataType) -> bool,
{
    ci.iter()
        .enumerate()
        .all(|(i, &col_index)| check(&row[col_index], &keys[i]))
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::prelude::LocalNodeIndex;
    use common::Link;
    use common::Tag;
    use std::convert::TryInto;
    use vec1::vec1;

    #[test]
    fn process_random_packet() {
        let original_packet = Packet::Spin;
        let mut processed_packet = original_packet.clone();
        let ni = NodeIndex::new(3);

        let mut processor = PacketFilter::default();

        let should_send = processor.process(&mut processed_packet, None, ni).unwrap();

        assert!(
            should_send,
            "The process should be signaling that the packet can be sent"
        );
        assert_eq!(
            original_packet, processed_packet,
            "The packet should still be the same"
        );
        assert!(
            processor.requested_keys.is_empty(),
            "The allowlist should not have been modified"
        );
    }

    mod update_processing {
        use super::*;
        use common::Record;
        use maplit::hashset;
        use std::ops::Bound;

        #[test]
        fn process_no_keys_associated() {
            let mut records = Vec::new();
            records.push(Record::Positive(vec![
                11.into(),
                "text1-1".try_into().unwrap(),
                "text1-2".try_into().unwrap(),
                12.into(),
                "text1-3".try_into().unwrap(),
            ]));
            records.push(Record::Positive(vec![
                21.into(),
                "text2-1".try_into().unwrap(),
                "text2-2".try_into().unwrap(),
                22.into(),
                "text2-3".try_into().unwrap(),
            ]));
            let mut packet = create_packet(records);
            let ni = NodeIndex::new(3);

            let key = KeyComparison::Equal(vec1![
                "not_present_in_any_record".try_into().unwrap(),
                12.into()
            ]);
            let mut keys = HashSet::new();
            keys.insert(key);

            let mut allowlist = HashMap::new();
            allowlist.insert(ni, NodeKeys::default());

            let mut processor = PacketFilter {
                requested_keys: allowlist,
            };

            let should_send = processor.process(&mut packet, None, ni).unwrap();

            assert!(
                !should_send,
                "The process should be signaling that the packet should not be sent"
            );
        }

        #[test]
        fn multiple_replayed_keys() {
            let mk_replay = |key, data| Packet::ReplayPiece {
                link: create_link(),
                tag: Tag::new(1),
                data,
                context: ReplayPieceContext::Partial {
                    for_keys: hashset! { key },
                    requesting_shard: 0,
                    unishard: false,
                },
            };

            let mut packet_filter = PacketFilter::default();
            packet_filter.add_for_filtering(NodeIndex::new(3));

            // process a replay of one key
            let mut replay_1 = mk_replay(
                KeyComparison::Equal(vec1![1.into()]),
                vec![Record::Positive(vec![1.into(), 1.into()])].into(),
            );
            packet_filter
                .process(&mut replay_1, Some(&[1]), NodeIndex::new(3))
                .unwrap();

            // process a replay of another key
            let mut replay_2 = mk_replay(
                KeyComparison::Equal(vec1![2.into()]),
                vec![Record::Positive(vec![2.into(), 2.into()])].into(),
            );
            packet_filter
                .process(&mut replay_2, Some(&[1]), NodeIndex::new(3))
                .unwrap();

            // process a write to one of those keys
            let original = create_packet(vec![Record::Positive(vec![1.into(), 2.into()])]);
            let mut packet = original.clone();
            let res = packet_filter
                .process(&mut packet, Some(&[1]), NodeIndex::new(3))
                .unwrap();

            assert!(res);
            assert_eq!(original, packet);
        }

        #[test]
        fn process_filter_all_messages() {
            let mut records = Vec::new();
            let record = Record::Positive(vec![
                11.into(),
                "text1-1".try_into().unwrap(),
                "text1-2".try_into().unwrap(),
                12.into(),
                "text1-3".try_into().unwrap(),
            ]);
            records.push(record);
            records.push(Record::Positive(vec![
                21.into(),
                "text2-1".try_into().unwrap(),
                "text2-2".try_into().unwrap(),
                22.into(),
                "text2-3".try_into().unwrap(),
            ]));
            let mut packet = create_packet(records);
            let column_indexes = vec![1usize, 3usize];

            let ni = NodeIndex::new(3);

            let key = KeyComparison::Equal(vec1![
                "not_present_in_any_record".try_into().unwrap(),
                12.into()
            ]);
            let mut keys = HashSet::new();
            keys.insert(key);

            let mut keys_by_col_index = HashMap::new();
            keys_by_col_index.insert(column_indexes, keys);

            let mut allowlist = HashMap::new();
            allowlist.insert(
                ni,
                NodeKeys {
                    keys: keys_by_col_index,
                },
            );

            let mut processor = PacketFilter {
                requested_keys: allowlist,
            };

            let should_send = processor.process(&mut packet, None, ni).unwrap();

            assert!(
                !should_send,
                "The process should be signaling that the packet should not be sent"
            );
        }

        #[test]
        fn process_filter_one_message() {
            let mut records = Vec::new();
            let record = Record::Positive(vec![
                11.into(),
                "text1-1".try_into().unwrap(),
                "text1-2".try_into().unwrap(),
                12.into(),
                "text1-3".try_into().unwrap(),
            ]);
            records.push(record.clone());
            records.push(Record::Positive(vec![
                21.into(),
                "text2-1".try_into().unwrap(),
                "text2-2".try_into().unwrap(),
                22.into(),
                "text2-3".try_into().unwrap(),
            ]));
            let mut packet = create_packet(records);
            let link = *packet.link_mut();
            let column_indexes = vec![1usize, 3usize];

            let ni = NodeIndex::new(3);

            let key = KeyComparison::Equal(vec1!["text1-1".try_into().unwrap(), 12.into()]);
            let mut keys = HashSet::new();
            keys.insert(key);

            let mut keys_by_col_index = HashMap::new();
            keys_by_col_index.insert(column_indexes, keys);

            let mut allowlist = HashMap::new();
            allowlist.insert(
                ni,
                NodeKeys {
                    keys: keys_by_col_index,
                },
            );

            let mut processor = PacketFilter {
                requested_keys: allowlist,
            };

            let should_send = processor.process(&mut packet, None, ni).unwrap();

            assert!(
                should_send,
                "The process should be signaling that the packet can be sent"
            );
            assert_eq!(
                link,
                packet.link_mut().clone(),
                "The links in the packet should still be the same"
            );
            let new_records = packet.take_data();
            assert_eq!(1, new_records.len(), "There should be only one record");
            let new_record = new_records[0].clone();
            assert_eq!(record, new_record, "The record in the packet is wrong");
        }

        #[test]
        fn process_filter_range_message() {
            let mut records = Vec::new();
            let record = Record::Positive(vec![
                11.into(),
                "text1-1".try_into().unwrap(),
                "text1-2".try_into().unwrap(),
                12.into(),
                "text1-3".try_into().unwrap(),
            ]);
            records.push(record.clone());
            records.push(Record::Positive(vec![
                21.into(),
                "text2-1".try_into().unwrap(),
                "text2-2".try_into().unwrap(),
                22.into(),
                "text2-3".try_into().unwrap(),
            ]));
            let mut packet = create_packet(records);
            let link = *packet.link_mut();
            let column_indexes = vec![3usize];

            let ni = NodeIndex::new(3);

            let key = KeyComparison::Range((
                Bound::Included(vec1![10.into()]),
                Bound::Excluded(vec1![20.into()]),
            ));
            let mut keys = HashSet::new();
            keys.insert(key);

            let mut keys_by_col_index = HashMap::new();
            keys_by_col_index.insert(column_indexes, keys);

            let mut allowlist = HashMap::new();
            allowlist.insert(
                ni,
                NodeKeys {
                    keys: keys_by_col_index,
                },
            );

            let mut processor = PacketFilter {
                requested_keys: allowlist,
            };

            let should_send = processor.process(&mut packet, None, ni).unwrap();

            assert!(
                should_send,
                "The process should be signaling that the packet can be sent"
            );
            assert_eq!(
                link,
                *packet.link_mut(),
                "The links in the packet should still be the same"
            );
            let new_records = packet.take_data();
            assert_eq!(1, new_records.len(), "There should be only one record");
            let new_record = new_records[0].clone();
            assert_eq!(record, new_record, "The record in the packet is wrong");
        }

        fn create_packet(records: Vec<Record>) -> Packet {
            Packet::Message {
                link: create_link(),
                data: records.into(),
            }
        }
    }

    mod replay_processing {
        use super::*;

        #[test]
        fn process_replay_no_keyed_by() {
            let mut keys = HashSet::new();
            keys.insert(KeyComparison::Equal(vec1![
                "text1-1".try_into().unwrap(),
                12.into()
            ]));
            let mut packet = create_packet(Some(keys));
            let ni = NodeIndex::new(3);

            let mut allowlist = HashMap::new();
            allowlist.insert(
                ni,
                NodeKeys {
                    keys: HashMap::new(),
                },
            );

            let mut processor = PacketFilter {
                requested_keys: allowlist,
            };

            let result = processor.process(&mut packet, None, ni);

            assert!(result.is_err(), "Calls to process with a replay message packet should always be passed a keyed-by vector of column indexes");
        }

        #[test]
        fn process_regular() {
            let original_packet = create_packet(None);
            let mut processed_packet = original_packet.clone();
            let ni = NodeIndex::new(3);

            let mut processor = PacketFilter::default();

            let should_send = processor.process(&mut processed_packet, None, ni).unwrap();

            assert!(
                should_send,
                "The process should be signaling that the packet can be sent"
            );
            assert_eq!(
                original_packet, processed_packet,
                "The packet should still be the same"
            );
        }

        #[test]
        fn process_partial_equal_keys() {
            let mut keys = HashSet::new();
            keys.insert(KeyComparison::Equal(vec1![
                "text1-1".try_into().unwrap(),
                12.into()
            ]));

            let col_indexes = vec![1usize, 3usize];

            let original_packet = create_packet(Some(keys.clone()));
            let mut processed_packet = original_packet.clone();
            let ni = NodeIndex::new(3);

            let mut allowlist = HashMap::new();
            allowlist.insert(ni, NodeKeys::default());

            let mut processor = PacketFilter {
                requested_keys: allowlist,
            };

            let should_send = processor
                .process(&mut processed_packet, Some(&col_indexes), ni)
                .unwrap();

            assert!(
                should_send,
                "The process should be signaling that the packet can be sent"
            );
            assert_eq!(
                original_packet, processed_packet,
                "The packet should still be the same"
            );
            let new_allowlist = &processor.requested_keys;
            match new_allowlist.get(&ni) {
                None => panic!("Filtering should be enabled for the target node"),
                Some(wd) => match wd.keys.get(&col_indexes) {
                    None => panic!("The column indexes should be present"),
                    Some(k) => assert_eq!(keys, k.to_owned(), "The allowlisted keys are wrong"),
                },
            }
        }

        fn create_packet(keys: Option<HashSet<KeyComparison>>) -> Packet {
            let context = if let Some(k) = keys {
                ReplayPieceContext::Partial {
                    for_keys: k,
                    requesting_shard: 0,
                    unishard: false,
                }
            } else {
                ReplayPieceContext::Regular { last: false }
            };
            Packet::ReplayPiece {
                link: create_link(),
                tag: Tag::new(1),
                data: Default::default(),
                context,
            }
        }
    }

    mod evict_processing {
        use super::*;

        #[test]
        fn process_evict_all_keys() {
            let mut keys = HashSet::new();
            keys.insert(KeyComparison::Equal(vec1![
                "text1-1".try_into().unwrap(),
                12.into()
            ]));

            let original_packet = create_packet(keys.iter().cloned().collect::<Vec<_>>());
            let mut processed_packet = original_packet.clone();
            let column_indexes = vec![1usize, 3usize];

            let ni = NodeIndex::new(3);

            let mut keys_by_col_index = HashMap::new();
            keys_by_col_index.insert(column_indexes.clone(), keys);

            let mut allowlist = HashMap::new();
            allowlist.insert(
                ni,
                NodeKeys {
                    keys: keys_by_col_index,
                },
            );

            let mut processor = PacketFilter {
                requested_keys: allowlist,
            };

            let should_send = processor.process(&mut processed_packet, None, ni).unwrap();

            assert!(
                should_send,
                "The process should be signaling that the packet can be sent"
            );
            assert_eq!(
                original_packet, processed_packet,
                "The packet should still be the same"
            );
            match processor.requested_keys.get(&ni) {
                None => panic!("Filtering should be enabled for the target node"),
                Some(wd) => match wd.keys.get(&column_indexes) {
                    None => panic!("The column indexes should be present"),
                    Some(k) => assert!(k.is_empty(), "There should be no more allowlisted keys"),
                },
            }
        }

        #[test]
        fn process_evict_some_keys() {
            let mut keys = HashSet::new();
            keys.insert(KeyComparison::Equal(vec1![
                "text1-1".try_into().unwrap(),
                12.into()
            ]));

            let original_packet = create_packet(keys.iter().cloned().collect::<Vec<_>>());
            let mut processed_packet = original_packet.clone();
            let key = KeyComparison::Equal(vec1!["text2-2".try_into().unwrap(), 22.into()]);
            keys.insert(key.clone());

            let column_indexes = vec![1usize, 3usize];

            let ni = NodeIndex::new(3);

            let mut keys_by_col_index = HashMap::new();
            keys_by_col_index.insert(column_indexes.clone(), keys);

            let mut allowlist = HashMap::new();
            allowlist.insert(
                ni,
                NodeKeys {
                    keys: keys_by_col_index,
                },
            );

            let mut processor = PacketFilter {
                requested_keys: allowlist,
            };

            let should_send = processor.process(&mut processed_packet, None, ni).unwrap();

            assert!(
                should_send,
                "The process should be signaling that the packet can be sent"
            );
            assert_eq!(
                original_packet, processed_packet,
                "The packet should still be the same"
            );
            let new_allowlist = processor.requested_keys;
            assert_eq!(
                1,
                new_allowlist.len(),
                "There should be only one entry for allowlisted nodes"
            );
            match new_allowlist.get(&ni) {
                None => panic!("Filtering should be enabled for the target node"),
                Some(wd) => match wd.keys.get(&column_indexes) {
                    None => panic!("The column indexes should be present"),
                    Some(k) => {
                        assert_eq!(1, k.len(), "There should be only one allowlisted key");
                        assert_eq!(
                            key,
                            k.iter().next().unwrap().to_owned(),
                            "The allowlisted key is wrong"
                        );
                    }
                },
            }
        }

        #[test]
        fn process_evict_no_keys() {
            let mut keys = HashSet::new();
            keys.insert(KeyComparison::Equal(vec1![
                "text1-1".try_into().unwrap(),
                12.into()
            ]));

            let original_packet = create_packet(keys.iter().cloned().collect::<Vec<_>>());
            let mut processed_packet = original_packet.clone();

            let column_indexes = vec![1usize, 3usize];

            let ni = NodeIndex::new(3);

            let mut keys_by_col_index = HashMap::new();
            keys_by_col_index.insert(column_indexes, keys);

            let mut allowlist = HashMap::new();
            allowlist.insert(
                ni,
                NodeKeys {
                    keys: keys_by_col_index,
                },
            );

            let ni_alt = NodeIndex::new(4);

            let mut processor = PacketFilter {
                requested_keys: allowlist.clone(),
            };

            let should_send = processor
                .process(&mut processed_packet, None, ni_alt)
                .unwrap();

            assert!(
                should_send,
                "The process should be signaling that the packet can be sent"
            );
            assert_eq!(
                original_packet, processed_packet,
                "The packet should still be the same"
            );
            assert_eq!(
                allowlist, processor.requested_keys,
                "The allowlist shouldn't have changed"
            );
        }

        fn create_packet(keys: Vec<KeyComparison>) -> Packet {
            Packet::EvictKeys {
                link: create_link(),
                tag: Tag::new(1),
                keys,
            }
        }
    }

    fn create_link() -> Link {
        Link {
            src: unsafe { LocalNodeIndex::make(1) },
            dst: unsafe { LocalNodeIndex::make(2) },
        }
    }
}
