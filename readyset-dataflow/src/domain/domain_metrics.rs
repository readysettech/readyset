//!
//! This module contains a struct that aggregates all the metrics collected for a domain.
//! To make the metrics performant, it holds handles to all the required metrics for
//! fast operations, wherever possible.

use std::time::Duration;

use metrics::{counter, gauge, histogram};
use nom_sql::Relation;
use readyset_client::metrics::recorded;

use crate::{Packet, PacketDiscriminants};

/// Contains handles to the various metrics collected for a domain.
/// Whenever possible the handles are generated at init time, others
/// that require dynamic labels are created on demand and stored in
/// a BTreeMap or a NodeMap.
pub(super) struct DomainMetrics {
    /// Whether to record metrics that include metric labels with high cardinality. This flag
    /// should be used very sparingly, as the cost of emitting these metrics could be quite high!
    verbose: bool,
}

impl DomainMetrics {
    pub(super) fn new(verbose: bool) -> Self {
        DomainMetrics { verbose }
    }

    pub(super) fn inc_eviction_requests(&self) {
        counter!(recorded::EVICTION_REQUESTS).increment(1);
    }

    pub(super) fn rec_eviction_time(&self, time: Duration, total_freed: usize) {
        histogram!(recorded::EVICTION_TIME).record(time.as_micros() as f64);
        histogram!(recorded::EVICTION_FREED_MEMORY).record(total_freed as f64);
    }

    pub(super) fn rec_chunked_replay_start_time(&mut self, time: Duration) {
        counter!(recorded::DOMAIN_TOTAL_CHUNKED_REPLAY_START_TIME)
            .increment(time.as_micros() as u64);

        histogram!(recorded::DOMAIN_CHUNKED_REPLAY_START_TIME,).record(time.as_micros() as f64);
    }

    pub(super) fn rec_replay_time(&mut self, cache_name: &Relation, time: Duration) {
        if self.verbose {
            counter!(
                recorded::DOMAIN_TOTAL_REPLAY_TIME,
                "cache_name" => cache_name_to_string(cache_name)
            )
            .increment(time.as_micros() as u64);

            histogram!(
                recorded::DOMAIN_REPLAY_TIME,
                "cache_name" => cache_name_to_string(cache_name)
            )
            .record(time.as_micros() as f64);
        }
    }

    pub(super) fn rec_seed_replay_time(&mut self, cache_name: &Relation, time: Duration) {
        if self.verbose {
            counter!(
                recorded::DOMAIN_TOTAL_SEED_REPLAY_TIME,
                "cache_name" => cache_name_to_string(cache_name)
            )
            .increment(time.as_micros() as u64);

            histogram!(
                recorded::DOMAIN_SEED_REPLAY_TIME,
                "cache_name" => cache_name_to_string(cache_name)
            )
            .record(time.as_micros() as f64);
        }
    }

    pub(super) fn rec_finish_replay_time(&mut self, cache_name: &Relation, time: Duration) {
        if self.verbose {
            counter!(
                recorded::DOMAIN_TOTAL_FINISH_REPLAY_TIME,
                "cache_name" => cache_name_to_string(cache_name)
            )
            .increment(time.as_micros() as u64);

            histogram!(
                recorded::DOMAIN_FINISH_REPLAY_TIME,
                "cache_name" => cache_name_to_string(cache_name)
            )
            .record(time.as_micros() as f64);
        }
    }

    pub(super) fn rec_forward_time_input(&mut self, time: Duration) {
        counter!(recorded::DOMAIN_TOTAL_FORWARD_TIME,
            "packet_type" => "input"
        )
        .increment(time.as_micros() as u64);
        histogram!(
            recorded::DOMAIN_FORWARD_TIME,
            "packet_type" => "input"
        )
        .record(time.as_micros() as f64);
    }

    pub(super) fn rec_forward_time_message(&mut self, time: Duration) {
        counter!(recorded::DOMAIN_TOTAL_FORWARD_TIME,
            "packet_type" => "message"
        )
        .increment(time.as_micros() as u64);
        histogram!(
            recorded::DOMAIN_FORWARD_TIME,
            "packet_type" => "message"
        )
        .record(time.as_micros() as f64);
    }

    pub(super) fn rec_reader_replay_time(&mut self, cache_name: &Relation, time: Duration) {
        if self.verbose {
            counter!(
                recorded::DOMAIN_READER_TOTAL_REPLAY_REQUEST_TIME,
                "cache_name" => cache_name_to_string(cache_name)
            )
            .increment(time.as_micros() as u64);

            histogram!(
                recorded::DOMAIN_READER_REPLAY_REQUEST_TIME,
                "cache_name" => cache_name_to_string(cache_name)
            )
            .record(time.as_micros() as f64);
        }
    }

    pub(super) fn inc_replay_misses(&mut self, cache_name: &Relation, n: usize) {
        counter!(
            recorded::DOMAIN_REPLAY_MISSES,
            "cache_name" => cache_name_to_string(cache_name)
        )
        .increment(n as u64);
    }

    pub(super) fn inc_packets_sent(&mut self, packet: &Packet) {
        let discriminant: PacketDiscriminants = packet.into();
        let packet_type: &'static str = discriminant.into();

        counter!(
            recorded::DOMAIN_PACKET_SENT,
            "packet_type" => packet_type
        )
        .increment(1);
    }

    pub(super) fn set_reader_state_size(&self, name: &Relation, size: usize) {
        gauge!(
            recorded::READER_STATE_SIZE_BYTES,
            "name" => cache_name_to_string(name),
        )
        .set(size as f64);
    }

    pub(super) fn set_base_table_size(&self, name: &Relation, size: usize) {
        if self.verbose {
            gauge!(
                recorded::ESTIMATED_BASE_TABLE_SIZE_BYTES,
                "table_name" => cache_name_to_string(name),
            )
            .set(size as f64);
        }
    }

    pub(super) fn inc_base_table_lookups(&mut self, cache_name: &Relation, table_name: &Relation) {
        if self.verbose {
            counter!(
                recorded::BASE_TABLE_LOOKUP_REQUESTS,
                "cache_name" => cache_name_to_string(cache_name),
                "table_name" => cache_name_to_string(table_name)
            )
            .increment(1);
        }
    }
}

/// Converts the given cache_name to a string by invoking `display_unquoted()`. This method should
/// only be used for converting cache names to strings for the purposes of including them as metric
/// labels.
fn cache_name_to_string(cache_name: &Relation) -> String {
    cache_name.display_unquoted().to_string()
}
