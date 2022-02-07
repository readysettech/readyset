//!
//! This module contains a struct that aggregates all the metrics collected for a domain.
//! To make the metrics performant, it holds handles to all the required metrics for
//! fast operations, wherever possible.
//!

use std::collections::BTreeMap;
use std::convert::TryInto;
use std::time::Duration;

use metrics::{
    register_counter, register_gauge, register_histogram, Counter, Gauge, Histogram, Label,
    SharedString,
};
use noria::internal::DomainIndex;
use noria::metrics::recorded;
use strum::{EnumCount, IntoEnumIterator};

use crate::domain::{LocalNodeIndex, Tag};
use crate::{NodeMap, Packet, PacketDiscriminants};

/// Contains hanldes to the varius metrics collected for a domain.
/// Whenever possible the handles are generated at init time, others
/// that require dynamic labels are created on demand and stored in
/// a BTreeMap or a NodeMap.
pub(super) struct DomainMetrics {
    index: SharedString,
    shard: SharedString,
    eviction_requests: Counter,
    eviction_time: Histogram,
    eviction_size: Histogram,

    partial_state_size: Gauge,
    reader_state_size: Gauge,
    base_table_size: Gauge,
    total_node_state_size: Gauge,

    packets_sent: [Counter; PacketDiscriminants::COUNT],

    // using a BTree to look up metrics by tag/node, BTree is faster than HashMap for u32/u64 keys
    chuncked_replay_start_time: BTreeMap<Tag, (Counter, Histogram)>,
    total_replay_time: BTreeMap<Tag, (Counter, Histogram)>,
    seed_replay_time: BTreeMap<Tag, (Counter, Histogram)>,
    finish_replay_time: BTreeMap<Tag, (Counter, Histogram)>,
    total_forward_time: BTreeMap<(LocalNodeIndex, LocalNodeIndex), (Counter, Histogram)>,
    replay_misses: BTreeMap<(LocalNodeIndex, Tag), Counter>,

    reader_replay_request_time: NodeMap<(Counter, Histogram)>,
    chuncked_replay_time: NodeMap<(Counter, Histogram)>,
    base_table_lookups: NodeMap<Counter>,
    node_state_size: NodeMap<Gauge>,
}

impl DomainMetrics {
    pub(super) fn new(index: DomainIndex, shard: usize) -> Self {
        let index: SharedString = index.index().to_string().into();
        let shard: SharedString = shard.to_string().into();

        let labels = vec![
            Label::new("domain", index.clone()),
            Label::new("shard", shard.clone()),
        ];

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
            partial_state_size: register_gauge!(
                recorded::DOMAIN_PARTIAL_STATE_SIZE_BYTES,
                labels.clone()
            ),
            reader_state_size: register_gauge!(
                recorded::DOMAIN_READER_STATE_SIZE_BYTES,
                labels.clone()
            ),
            base_table_size: register_gauge!(
                recorded::DOMAIN_ESTIMATED_BASE_TABLE_SIZE_BYTES,
                labels.clone()
            ),
            total_node_state_size: register_gauge!(
                recorded::DOMAIN_TOTAL_NODE_STATE_SIZE_BYTES,
                labels.clone()
            ),

            eviction_requests: register_counter!(
                recorded::DOMAIN_EVICTION_REQUESTS,
                labels.clone()
            ),
            eviction_time: register_histogram!(recorded::DOMAIN_EVICTION_TIME, labels.clone()),
            eviction_size: register_histogram!(recorded::DOMAIN_EVICTION_FREED_MEMORY, labels),
            chuncked_replay_start_time: Default::default(),
            chuncked_replay_time: Default::default(),
            total_replay_time: Default::default(),
            seed_replay_time: Default::default(),
            finish_replay_time: Default::default(),
            total_forward_time: Default::default(),
            replay_misses: Default::default(),
            packets_sent: packets_sent.try_into().ok().unwrap(),
            reader_replay_request_time: Default::default(),
            base_table_lookups: Default::default(),
            node_state_size: Default::default(),
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
        if let Some((ctr, histo)) = self.chuncked_replay_start_time.get(&tag) {
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

            self.chuncked_replay_start_time.insert(tag, (ctr, histo));
        }
    }

    pub(super) fn recorders_for_chunked_replay(
        &mut self,
        from_node: LocalNodeIndex,
    ) -> (Counter, Histogram) {
        if let Some(recorders) = self.chuncked_replay_time.get(from_node) {
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

            self.chuncked_replay_time
                .insert(from_node, recorders.clone());
            recorders
        }
    }

    pub(super) fn rec_replay_time(&mut self, tag: Tag, time: Duration) {
        if let Some((ctr, histo)) = self.total_replay_time.get(&tag) {
            ctr.increment(time.as_micros() as u64);
            histo.record(time.as_micros() as f64);
        } else {
            let ctr = register_counter!(
                recorded::DOMAIN_TOTAL_REPLAY_TIME,
                "domain" => self.index.clone(),
                "shard" => self.shard.clone(),
                "tag" => tag.to_string()
            );

            let histo = register_histogram!(
                recorded::DOMAIN_REPLAY_TIME,
                "domain" => self.index.clone(),
                "shard" => self.shard.clone(),
                "tag" => tag.to_string()
            );

            ctr.increment(time.as_micros() as u64);
            histo.record(time.as_micros() as f64);

            self.total_replay_time.insert(tag, (ctr, histo));
        }
    }

    pub(super) fn rec_seed_replay_time(&mut self, tag: Tag, time: Duration) {
        if let Some((ctr, histo)) = self.seed_replay_time.get(&tag) {
            ctr.increment(time.as_micros() as u64);
            histo.record(time.as_micros() as f64);
        } else {
            let ctr = register_counter!(
                recorded::DOMAIN_TOTAL_SEED_REPLAY_TIME,
                "domain" => self.index.clone(),
                "shard" => self.shard.clone(),
                "tag" => tag.to_string()
            );

            let histo = register_histogram!(
                recorded::DOMAIN_SEED_REPLAY_TIME,
                "domain" => self.index.clone(),
                "shard" => self.shard.clone(),
                "tag" => tag.to_string()
            );

            ctr.increment(time.as_micros() as u64);
            histo.record(time.as_micros() as f64);

            self.seed_replay_time.insert(tag, (ctr, histo));
        }
    }

    pub(super) fn rec_finish_replay_time(&mut self, tag: Tag, time: Duration) {
        if let Some((ctr, histo)) = self.finish_replay_time.get(&tag) {
            ctr.increment(time.as_micros() as u64);
            histo.record(time.as_micros() as f64);
        } else {
            let ctr = register_counter!(
                recorded::DOMAIN_TOTAL_FINISH_REPLAY_TIME,
                "domain" => self.index.clone(),
                "shard" => self.shard.clone(),
                "tag" => tag.to_string()
            );

            let histo = register_histogram!(
                recorded::DOMAIN_FINISH_REPLAY_TIME,
                "domain" => self.index.clone(),
                "shard" => self.shard.clone(),
                "tag" => tag.to_string()
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
                "domain" => self.index.clone(),
                "shard" => self.shard.clone(),
                "from_node" => src.to_string(),
                "to_node" => dst.to_string(),
            );

            let histo = register_histogram!(
                recorded::DOMAIN_FORWARD_TIME,
                "domain" => self.index.clone(),
                "shard" => self.shard.clone(),
                "from_node" => src.to_string(),
                "to_node" => dst.to_string(),
            );

            ctr.increment(time.as_micros() as u64);
            histo.record(time.as_micros() as f64);

            self.total_forward_time.insert((src, dst), (ctr, histo));
        }
    }

    pub(super) fn rec_reader_replay_time(&mut self, node: LocalNodeIndex, time: Duration) {
        if let Some((ctr, histo)) = self.reader_replay_request_time.get(node) {
            ctr.increment(time.as_micros() as u64);
            histo.record(time.as_micros() as f64);
        } else {
            let ctr = register_counter!(
                recorded::DOMAIN_READER_TOTAL_REPLAY_REQUEST_TIME,
                "domain" => self.index.clone(),
                "shard" => self.shard.clone(),
                "node" => node.to_string(),
            );

            let histo = register_histogram!(
                recorded::DOMAIN_READER_REPLAY_REQUEST_TIME,
                "domain" => self.index.clone(),
                "shard" => self.shard.clone(),
                "node" => node.to_string(),
            );

            ctr.increment(time.as_micros() as u64);
            histo.record(time.as_micros() as f64);

            self.reader_replay_request_time.insert(node, (ctr, histo));
        }
    }

    pub(super) fn inc_replay_misses(&mut self, miss_in: LocalNodeIndex, needed_for: Tag, n: usize) {
        if let Some(ctr) = self.replay_misses.get(&(miss_in, needed_for)) {
            ctr.increment(n as u64);
        } else {
            let ctr = register_counter!(
                recorded::DOMAIN_TOTAL_CHUNKED_REPLAY_START_TIME,
                "domain" => self.index.clone(),
                "shard" => self.shard.clone(),
                "miss_in" => miss_in.id().to_string(),
                "needed_for" => needed_for.to_string()
            );

            ctr.increment(n as u64);
            self.replay_misses.insert((miss_in, needed_for), ctr);
        }
    }

    pub(super) fn inc_packets_sent(&mut self, packet: &Packet) {
        let discriminant: PacketDiscriminants = packet.into();
        self.packets_sent[discriminant as usize].increment(1);
    }

    pub(super) fn inc_base_table_lookups(&mut self, node: LocalNodeIndex) {
        if let Some(ctr) = self.base_table_lookups.get(node) {
            ctr.increment(1);
        } else {
            let ctr = register_counter!(
                recorded::BASE_TABLE_LOOKUP_REQUESTS,
                "domain" => self.index.clone(),
                "shard" => self.shard.clone(),
                "node" => node.to_string(),
            );
            ctr.increment(1);
            self.base_table_lookups.insert(node, ctr);
        }
    }

    pub(super) fn set_state_sizes(&self, partial: u64, reader: u64, base: u64, node: u64) {
        self.partial_state_size.set(partial as f64);
        self.reader_state_size.set(reader as f64);
        self.base_table_size.set(base as f64);
        self.total_node_state_size.set(node as f64);
    }

    pub(super) fn set_node_state_size(&mut self, node: LocalNodeIndex, size: u64) {
        if let Some(gauge) = self.node_state_size.get(node) {
            gauge.set(size as f64);
        } else {
            let gauge = register_gauge!(
                recorded::DOMAIN_NODE_STATE_SIZE_BYTES,
                "domain" => self.index.clone(),
                "shard" => self.shard.clone(),
                "node" => node.to_string(),
            );
            gauge.set(size as f64);
            self.node_state_size.insert(node, gauge);
        }
    }
}
