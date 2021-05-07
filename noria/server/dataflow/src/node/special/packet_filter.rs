use crate::payload::ReplayPieceContext;
use crate::prelude::NodeIndex;
use crate::Packet;
use common::DataType;
use noria::{internal, KeyComparison, ReadySetResult};
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::ops::Bound;
use vec1::Vec1;

type ColumnIndexes = Vec<usize>;

/// The goal of the [`PacketFilter`] is to avoid sending updates for keys when the destination domain
/// does not have those keys materialized (to avoid unnecessary network traffic),
/// given that those updates will just be discarded at said domain.
/// The [`PacketFilter`] then works by observing which keys are replayed and evicted to build
/// an internal whitelist, which is then used to filter regular update messages.
/// Filtering is disabled by default, and can be enabled by adding the desired destination domain's
/// ingress node to the filter list.
#[derive(Serialize, Deserialize, PartialEq, Clone, Default)]
pub struct PacketFilter {
    /// Stores the information needed to filter [`Record`]s from [`Packet::Message`]s.
    whitelist: HashMap<NodeIndex, WhitelistData>,
}

/// The information required to filter updates for
/// partially materialized nodes
#[derive(Serialize, Deserialize, PartialEq, Clone, Default, Debug)]
pub struct WhitelistData {
    /// The whitelisted keys, grouped by the column indexes they act upon.
    keys: HashMap<ColumnIndexes, HashSet<KeyComparison>>,
}

impl PacketFilter {
    /// Adds a node to the filter list. This means that the target node
    /// will have its packets processed (and not just skipped) by this [`PacketFilter`].
    /// See [`PacketFilter::process`] for more information on the processing logic.
    pub fn add_for_filtering(&mut self, target: NodeIndex) {
        self.whitelist
            .entry(target)
            .or_insert_with(WhitelistData::default);
    }

    /// Processes a given [`Packet`], provided with some contextual information.
    /// The processing modifies the packet in-place.
    ///
    /// ## Processing logic
    /// The processing works as follows. Given a packet meant for a target node that is marked for filtering:
    /// 1. If the packet is a replay piece ([`Packet::ReplayPiece`]), then the requested keys
    /// are stored in the filter, along the column indexes they predicate upon.
    /// 2. If the packet is an eviction message ([`Packet::EvictKeys`]), the evicted keys
    /// are removed from the filter.
    /// 3. If the packet is an update ([`Packet::Message`]), then the packet is stripped down
    /// of any record that does not comply with the stored keys. If no record survives the filtering,
    /// then the whole packet should be dropped.
    ///
    /// ## Arguments
    /// * packet: The [`Packet`] to be analyzed and potentially modified.
    /// * keyed_by: An [`Option`] of [`Vec<usize>`] representing the indexes of the columns of a [`Record`],
    /// that are used by [`KeyComparison`]s. These indexes should be present whenever there's a [`Packet::ReplayPiece`].
    /// * target: The [`NodeIndex`] of the node that will receive the [`Packet`].
    ///
    /// ## Returns
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
        return match self.whitelist.get_mut(&target) {
            None => Ok(true),
            Some(wd) => match packet {
                Packet::Message { data, .. } => {
                    // If the packet is an update, we must check what keys the target node has previously requested.
                    // Based on that, we'll filter the records from the Packet.
                    if wd.keys.is_empty() {
                        // If no keys were previously requested by the target node, then that
                        // means that all packets must be dropped until it requests some keys.
                        return Ok(false);
                    }
                    data.retain(|record| {
                        let row = record.row();

                        for (ci, keys) in &wd.keys {
                            for key in keys {
                                match key {
                                    // Here we filter the records based on the keys that the target node
                                    // requested previously.
                                    KeyComparison::Equal(ref cond) => {
                                        if !check_bound(ci, row, cond, |d1, d2| d1 == d2) {
                                            return false;
                                        }
                                    }
                                    KeyComparison::Range((ref lower_bound, ref upper_bound)) => {
                                        if !check_lower_bound(lower_bound, ci, row)
                                            || !check_upper_bound(upper_bound, ci, row)
                                        {
                                            return false;
                                        }
                                    }
                                }
                            }
                        }
                        true
                    });
                    // If no records survived the filtering, then signal
                    // that the Packet should be dropped.
                    if data.is_empty() {
                        return Ok(false);
                    }
                    // Otherwise, return the new updated packet with the filtered records.
                    Ok(true)
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
                        // We add the keys to the node's whitelist information.
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
                        for (_, whitelisted_keys) in wd.keys.iter_mut() {
                            whitelisted_keys.remove(k);
                        }
                    }
                    Ok(true)
                }
                // For any other packet, we signal to just send the Packet.
                _ => Ok(true),
            },
        };
    }

    /// Adds the given keys to the target node's whitelist information.
    fn add_keys(&mut self, target: NodeIndex, column_indexes: Vec<usize>, keys: &[KeyComparison]) {
        match self.whitelist.entry(target) {
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
