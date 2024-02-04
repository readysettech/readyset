//!
//! This module contains a struct that aggregates all the metrics collected for a domain.
//! To make the metrics performant, it holds handles to all the required metrics for
//! fast operations, wherever possible.

use std::collections::BTreeMap;
use std::convert::TryInto;
use std::time::Duration;

use metrics::{gauge, register_counter, register_histogram, Counter, Histogram, SharedString};
use nom_sql::Relation;
use readyset_client::metrics::recorded;
use strum::{EnumCount, IntoEnumIterator};

use crate::domain::{LocalNodeIndex, ReplicaAddress, Tag};
use crate::{NodeMap, Packet, PacketDiscriminants};

/// Contains handles to the various metrics collected for a domain.
/// Whenever possible the handles are generated at init time, others
/// that require dynamic labels are created on demand and stored in
/// a BTreeMap or a NodeMap.
pub(super) struct DomainMetrics {
    index: SharedString,
    shard: SharedString,
    eviction_requests: Counter,
    eviction_time: Histogram,
    eviction_size: Histogram,

    packets_sent: [Counter; PacketDiscriminants::COUNT],

    // using a BTree to look up metrics by tag/node, BTree is faster than HashMap for u32/u64 keys
    chunked_replay_start_time: BTreeMap<Tag, (Counter, Histogram)>,
    total_replay_time: BTreeMap<Tag, (Counter, Histogram)>,
    seed_replay_time: BTreeMap<Tag, (Counter, Histogram)>,
    finish_replay_time: BTreeMap<Tag, (Counter, Histogram)>,
    total_forward_time: BTreeMap<(LocalNodeIndex, LocalNodeIndex), (Counter, Histogram)>,
    replay_misses: BTreeMap<(LocalNodeIndex, Tag), Counter>,

    reader_replay_request_time: NodeMap<(Counter, Histogram)>,
    chunked_replay_time: NodeMap<(Counter, Histogram)>,
    base_table_lookups: NodeMap<Counter>,
}

impl DomainMetrics {
    pub(super) fn new(replica_address: ReplicaAddress) -> Self {
        let index: SharedString = replica_address.domain_index.index().to_string().into();
        let shard: SharedString = replica_address.shard.to_string().into();

        let packets_sent: Vec<Counter> = PacketDiscriminants::iter()
            .map(|d| {
                let name: &'static str = d.into();
                register_counter!(recorded::DOMAIN_PACKET_SENT,
                                  "domain" => index.clone(),
                                  "shard" => shard.clone(),
                                  "packet_type" => name,
                )
            })
            .collect();

        DomainMetrics {
            eviction_requests: register_counter!(recorded::EVICTION_REQUESTS, vec![],),
            eviction_time: register_histogram!(recorded::EVICTION_TIME, vec![]),
            eviction_size: register_histogram!(recorded::EVICTION_FREED_MEMORY, vec![],),
            chunked_replay_start_time: Default::default(),
            chunked_replay_time: Default::default(),
            total_replay_time: Default::default(),
            seed_replay_time: Default::default(),
            finish_replay_time: Default::default(),
            total_forward_time: Default::default(),
            replay_misses: Default::default(),
            packets_sent: packets_sent.try_into().ok().unwrap(),
            reader_replay_request_time: Default::default(),
            base_table_lookups: Default::default(),
            shard,
            index,
        }
    }

    pub(super) fn inc_eviction_requests(&self) {
        self.eviction_requests.increment(1);
    }

    pub(super) fn rec_eviction_time(&self, time: Duration, total_freed: u64) {
        self.eviction_time.record(time.as_micros() as f64);
        self.eviction_size.record(total_freed as f64);
    }

    pub(super) fn rec_chunked_replay_start_time(&mut self, tag: Tag, time: Duration) {
        if let Some((ctr, histo)) = self.chunked_replay_start_time.get(&tag) {
            ctr.increment(time.as_micros() as u64);
            histo.record(time.as_micros() as f64);
        } else {
            let ctr = register_counter!(
                recorded::DOMAIN_TOTAL_CHUNKED_REPLAY_START_TIME,
                "domain" => self.index.clone(),
                "shard" => self.shard.clone(),
                "tag" => tag.to_string()
            );

            let histo = register_histogram!(
                recorded::DOMAIN_CHUNKED_REPLAY_START_TIME,
                "domain" => self.index.clone(),
                "shard" => self.shard.clone(),
                "tag" => tag.to_string()
            );

            ctr.increment(time.as_micros() as u64);
            histo.record(time.as_micros() as f64);

            self.chunked_replay_start_time.insert(tag, (ctr, histo));
        }
    }

    pub(super) fn recorders_for_chunked_replay(
        &mut self,
        from_node: LocalNodeIndex,
    ) -> (Counter, Histogram) {
        if let Some(recorders) = self.chunked_replay_time.get(from_node) {
            recorders.clone()
        } else {
            let recorders = (
                register_counter!(
                    recorded::DOMAIN_TOTAL_CHUNKED_REPLAY_TIME,
                    "domain" => self.index.clone(),
                    "shard" => self.shard.clone(),
                    "from_node" => from_node.to_string()
                ),
                register_histogram!(
                    recorded::DOMAIN_CHUNKED_REPLAY_TIME,
                    "domain" => self.index.clone(),
                    "shard" => self.shard.clone(),
                    "from_node" => from_node.to_string()
                ),
            );

            self.chunked_replay_time
                .insert(from_node, recorders.clone());
            recorders
        }
    }

    pub(super) fn rec_replay_time(&mut self, tag: Tag, cache_name: &Relation, time: Duration) {
        if let Some((ctr, histo)) = self.total_replay_time.get(&tag) {
            ctr.increment(time.as_micros() as u64);
            histo.record(time.as_micros() as f64);
        } else {
            let ctr = register_counter!(
                recorded::DOMAIN_TOTAL_REPLAY_TIME,
                "domain" => self.index.clone(),
                "shard" => self.shard.clone(),
                "tag" => tag.to_string(),
                "cache_name" => cache_name_to_string(cache_name)
            );

            let histo = register_histogram!(
                recorded::DOMAIN_REPLAY_TIME,
                "domain" => self.index.clone(),
                "shard" => self.shard.clone(),
                "tag" => tag.to_string(),
                "cache_name" => cache_name_to_string(cache_name)
            );

            ctr.increment(time.as_micros() as u64);
            histo.record(time.as_micros() as f64);

            self.total_replay_time.insert(tag, (ctr, histo));
        }
    }

    pub(super) fn rec_seed_replay_time(&mut self, tag: Tag, cache_name: &Relation, time: Duration) {
        if let Some((ctr, histo)) = self.seed_replay_time.get(&tag) {
            ctr.increment(time.as_micros() as u64);
            histo.record(time.as_micros() as f64);
        } else {
            let ctr = register_counter!(
                recorded::DOMAIN_TOTAL_SEED_REPLAY_TIME,
                "domain" => self.index.clone(),
                "shard" => self.shard.clone(),
                "tag" => tag.to_string(),
                "cache_name" => cache_name_to_string(cache_name)
            );

            let histo = register_histogram!(
                recorded::DOMAIN_SEED_REPLAY_TIME,
                "domain" => self.index.clone(),
                "shard" => self.shard.clone(),
                "tag" => tag.to_string(),
                "cache_name" => cache_name_to_string(cache_name)
            );

            ctr.increment(time.as_micros() as u64);
            histo.record(time.as_micros() as f64);

            self.seed_replay_time.insert(tag, (ctr, histo));
        }
    }

    pub(super) fn rec_finish_replay_time(
        &mut self,
        tag: Tag,
        cache_name: &Relation,
        time: Duration,
    ) {
        if let Some((ctr, histo)) = self.finish_replay_time.get(&tag) {
            ctr.increment(time.as_micros() as u64);
            histo.record(time.as_micros() as f64);
        } else {
            let ctr = register_counter!(
                recorded::DOMAIN_TOTAL_FINISH_REPLAY_TIME,
                "domain" => self.index.clone(),
                "shard" => self.shard.clone(),
                "tag" => tag.to_string(),
                "cache_name" => cache_name_to_string(cache_name)
            );

            let histo = register_histogram!(
                recorded::DOMAIN_FINISH_REPLAY_TIME,
                "domain" => self.index.clone(),
                "shard" => self.shard.clone(),
                "tag" => tag.to_string(),
                "cache_name" => cache_name_to_string(cache_name)
            );

            ctr.increment(time.as_micros() as u64);
            histo.record(time.as_micros() as f64);

            self.finish_replay_time.insert(tag, (ctr, histo));
        }
    }

    pub(super) fn rec_forward_time(
        &mut self,
        src: LocalNodeIndex,
        dst: LocalNodeIndex,
        time: Duration,
    ) {
        if let Some((ctr, histo)) = self.total_forward_time.get(&(src, dst)) {
            ctr.increment(time.as_micros() as u64);
            histo.record(time.as_micros() as f64);
        } else {
            let ctr = register_counter!(
                recorded::DOMAIN_TOTAL_FORWARD_TIME,
                "from_node" => src.to_string(),
                "to_node" => dst.to_string(),
                "domain" => self.index.clone(),
                "shard" => self.shard.clone(),
            );

            let histo = register_histogram!(
                recorded::DOMAIN_FORWARD_TIME,
                "from_node" => src.to_string(),
                "to_node" => dst.to_string(),
                "domain" => self.index.clone(),
                "shard" => self.shard.clone(),
            );

            ctr.increment(time.as_micros() as u64);
            histo.record(time.as_micros() as f64);

            self.total_forward_time.insert((src, dst), (ctr, histo));
        }
    }

    pub(super) fn rec_reader_replay_time(
        &mut self,
        node: LocalNodeIndex,
        cache_name: &Relation,
        time: Duration,
    ) {
        if let Some((ctr, histo)) = self.reader_replay_request_time.get(node) {
            ctr.increment(time.as_micros() as u64);
            histo.record(time.as_micros() as f64);
        } else {
            let ctr = register_counter!(
                recorded::DOMAIN_READER_TOTAL_REPLAY_REQUEST_TIME,
                "domain" => self.index.clone(),
                "shard" => self.shard.clone(),
                "node" => node.to_string(),
                "cache_name" => cache_name_to_string(cache_name)
            );

            let histo = register_histogram!(
                recorded::DOMAIN_READER_REPLAY_REQUEST_TIME,
                "domain" => self.index.clone(),
                "shard" => self.shard.clone(),
                "node" => node.to_string(),
                "cache_name" => cache_name_to_string(cache_name)
            );

            ctr.increment(time.as_micros() as u64);
            histo.record(time.as_micros() as f64);

            self.reader_replay_request_time.insert(node, (ctr, histo));
        }
    }

    pub(super) fn inc_replay_misses(
        &mut self,
        miss_in: LocalNodeIndex,
        needed_for: Tag,
        cache_name: &Relation,
        n: usize,
    ) {
        if let Some(ctr) = self.replay_misses.get(&(miss_in, needed_for)) {
            ctr.increment(n as u64);
        } else {
            let ctr = register_counter!(
                recorded::DOMAIN_REPLAY_MISSES,
                "domain" => self.index.clone(),
                "shard" => self.shard.clone(),
                "miss_in" => miss_in.id().to_string(),
                "needed_for" => needed_for.to_string(),
                "cache_name" => cache_name_to_string(cache_name)
            );

            ctr.increment(n as u64);
            self.replay_misses.insert((miss_in, needed_for), ctr);
        }
    }

    pub(super) fn inc_packets_sent(&mut self, packet: &Packet) {
        let discriminant: PacketDiscriminants = packet.into();
        self.packets_sent[discriminant as usize].increment(1);
    }

    pub(super) fn inc_base_table_lookups(&mut self, node: LocalNodeIndex, cache_name: &Relation) {
        if let Some(ctr) = self.base_table_lookups.get(node) {
            ctr.increment(1);
        } else {
            let ctr = register_counter!(
                recorded::BASE_TABLE_LOOKUP_REQUESTS,
                "domain" => self.index.clone(),
                "shard" => self.shard.clone(),
                "node" => node.to_string(),
                "cache_name" => cache_name_to_string(cache_name)
            );
            ctr.increment(1);
            self.base_table_lookups.insert(node, ctr);
        }
    }

    pub(super) fn set_reader_state_size(&self, name: &Relation, size: u64) {
        gauge!(
            recorded::READER_STATE_SIZE_BYTES,
            size as f64,
            "name" => cache_name_to_string(name),
        );
    }

    pub(super) fn set_base_table_size(&self, name: &Relation, size: u64) {
        gauge!(
            recorded::ESTIMATED_BASE_TABLE_SIZE_BYTES,
            size as f64,
            "table_name" => cache_name_to_string(name),
        );
    }
}

/// Converts the given cache_name to a string by invoking `display_unquoted()`. This method should
/// only be used for converting cache names to strings for the purposes of including them as metric
/// labels.
fn cache_name_to_string(cache_name: &Relation) -> String {
    cache_name.display_unquoted().to_string()
}
