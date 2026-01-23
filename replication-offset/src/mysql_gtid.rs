//! MySQL GTID (Global Transaction Identifier) support for replication.
//!
//! This module provides types for tracking MySQL GTIDs during replication:
//! - [`GtidSource`]: Identifier for a GTID source (server UUID + optional tag)
//! - [`GtidEvent`]: A single GTID with optional internal event tracking
//! - [`GtidSet`]: A set of GTIDs partitioned by [`GtidSource`]
//! - [`GtidRange`]: A range of GTID sequence numbers
//!
//! ## Event Index Tracking
//!
//! MySQL transactions consist of multiple binlog events:
//! `GTID_EVENT` → `WRITE_ROWS`/`UPDATE_ROWS`/`DELETE_ROWS` → `XID_EVENT`
//!
//! Readyset processes events individually, not atomically. If we crash mid-transaction,
//! we need to know which events within a GTID were already applied. The `event_index`
//! field in [`GtidEvent`] tracks this position.
//!
//! **Important**: The `event_index` is internal-only and MUST NOT be advertised to MySQL
//! when reporting our executed GTID set. MySQL expects standard GTID format without
//! any event position information.

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;

use readyset_errors::{ReadySetError, ReadySetResult, replication_failed, replication_failed_err};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Identifier for a GTID source within a MySQL topology.
///
/// At minimum this consists of the MySQL server UUID. Newer versions of MySQL optionally support
/// tagged GTIDs; for forward compatibility we allow for an optional tag component to distinguish
/// multiple replication streams from the same UUID.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize, Deserialize)]
#[serde(into = "String", try_from = "String")]
pub struct GtidSource {
    pub server_uuid: Uuid,
    pub tag: Option<String>,
}

impl fmt::Display for GtidSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.tag {
            Some(tag) => write!(f, "{}:{}", self.server_uuid, tag),
            None => write!(f, "{}", self.server_uuid),
        }
    }
}

impl From<GtidSource> for String {
    fn from(source: GtidSource) -> Self {
        source.to_string()
    }
}

impl TryFrom<String> for GtidSource {
    type Error = uuid::Error;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        s.parse()
    }
}

impl FromStr for GtidSource {
    type Err = uuid::Error;

    /// Parse a GTID source from a string in the format "uuid" or "uuid:tag".
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some((uuid_str, tag)) = s.split_once(':') {
            Ok(GtidSource {
                server_uuid: Uuid::parse_str(uuid_str)?,
                tag: Some(tag.to_owned()),
            })
        } else {
            Ok(GtidSource {
                server_uuid: Uuid::parse_str(s)?,
                tag: None,
            })
        }
    }
}

/// Representation of a single GTID with optional internal event tracking.
///
/// The `event_index` field tracks position within the transaction's binlog events.
/// This is used for crash recovery to resume from the exact event position.
///
/// # Event Index
///
/// The `event_index` is:
/// - `0` when the GTID_EVENT is first received (no row events applied yet)
/// - Incremented after each row event (WRITE_ROWS, UPDATE_ROWS, DELETE_ROWS)
/// - Reset when a new GTID_EVENT is received
///
/// **Important**: The `event_index` is internal-only for crash recovery. When advertising
/// our executed GTID set to MySQL, we MUST NOT include this field. Use [`GtidEvent::to_mysql_string()`]
/// for MySQL-compatible output.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize)]
pub struct GtidEvent {
    key: GtidSource,
    sequence_number: u64,
    /// Internal event counter within this GTID's transaction.
    ///
    /// This tracks how many row events have been processed within this GTID.
    /// Used for crash recovery to skip already-applied events on reconnect.
    ///
    /// **MUST NOT be advertised to MySQL** - use [`to_mysql_string()`](Self::to_mysql_string)
    /// when sending GTID sets to the upstream server.
    #[serde(default)]
    event_index: u64,
}

impl GtidEvent {
    /// Create a new GTID with the given parameters and event_index of 0.
    pub fn new(key: GtidSource, sequence_number: u64) -> Self {
        Self {
            key,
            sequence_number,
            event_index: 0,
        }
    }

    /// Return a reference to the source for this GTID (UUID + optional tag).
    pub fn source(&self) -> &GtidSource {
        &self.key
    }

    /// Return the transaction sequence number.
    pub fn sequence_number(&self) -> u64 {
        self.sequence_number
    }

    /// Return the current event index within this transaction.
    pub fn event_index(&self) -> u64 {
        self.event_index
    }

    /// Increment the event index after processing a row event.
    pub fn advance_event(&mut self) {
        self.event_index += 1;
    }

    /// Returns the MySQL-compatible string representation (without event_index).
    ///
    /// Use this when advertising GTIDs to MySQL. The standard format is:
    /// - Without tag: `{uuid}:{sequence_number}`
    /// - With tag: `{uuid}:{tag}:{sequence_number}`
    pub fn to_mysql_string(&self) -> String {
        match &self.key.tag {
            Some(tag) => format!("{}:{}:{}", self.key.server_uuid, tag, self.sequence_number),
            None => format!("{}:{}", self.key.server_uuid, self.sequence_number),
        }
    }
}

impl fmt::Display for GtidEvent {
    /// Display format includes event_index for debugging purposes.
    ///
    /// Format: `{uuid}:{sequence_number}@{event_index}` (or with tag: `{uuid}:{tag}:{sequence_number}@{event_index}`)
    ///
    /// Note: For MySQL-compatible output, use [`Self::to_mysql_string`] instead.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.key.tag {
            Some(tag) => write!(
                f,
                "{}:{}:{}@{}",
                self.key.server_uuid, tag, self.sequence_number, self.event_index
            ),
            None => write!(
                f,
                "{}:{}@{}",
                self.key.server_uuid, self.sequence_number, self.event_index
            ),
        }
    }
}

/// A range of GTID sequence numbers, inclusive on both ends.
///
/// MySQL stores GTID ranges as inclusive ranges (e.g., `1-5` means 1, 2, 3, 4, 5).
/// The driver ([`mysql_common::GnoInterval`]) requires exclusive end, so we convert when building SID blocks.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize)]
pub struct GtidRange {
    /// Inclusive start of the range
    pub start: u64,
    /// Inclusive end of the range
    pub end: u64,
}

impl GtidRange {
    /// Create a new range. Returns an error if start > end.
    pub fn new(start: u64, end: u64) -> ReadySetResult<Self> {
        if start > end {
            replication_failed!("GtidRange start ({start}) must be <= end ({end})");
        }
        Ok(Self { start, end })
    }

    /// Create a single-number range [n, n]
    pub fn single(n: u64) -> Self {
        Self { start: n, end: n }
    }

    /// Check if this range contains the given sequence number
    pub fn contains(&self, n: u64) -> bool {
        n >= self.start && n <= self.end
    }

    /// Check if this range overlaps or touches another range
    pub fn overlaps_or_touches(&self, other: &Self) -> bool {
        self.start <= other.end.saturating_add(1) && other.start <= self.end.saturating_add(1)
    }

    /// Merge this range with another range if they overlap or touch
    /// Returns the merged range, or None if they don't overlap or touch
    pub fn merge(&self, other: &Self) -> Option<Self> {
        if self.overlaps_or_touches(other) {
            Some(Self {
                start: self.start.min(other.start),
                end: self.end.max(other.end),
            })
        } else {
            None
        }
    }
}

/// A set of GTIDs partitioned by [`GtidSource`].
///
/// Each key maps to a list of ranges representing the GTIDs that have been observed.
/// Ranges are stored sorted and non-overlapping. This matches MySQL's internal representation
/// and allows us to handle gaps in GTID sequences (e.g., from `SET gtid_next` or disabled
/// `replica_preserve_commit_order`).
///
/// ## Pending GTID Tracking
///
/// The set can optionally track a "pending" GTID that is currently being processed but
/// not yet committed. This is used for crash recovery:
/// - When a GTID_EVENT is received, set it as pending with `set_pending()`
/// - After each row event, increment the pending GTID's event_index
/// - When XID_EVENT (commit) is received, finalize with `finalize_pending()`
/// - On crash recovery, the pending GTID tells us where to resume within the transaction
#[derive(Clone, PartialEq, Eq, Debug, Default, Serialize, Deserialize)]
pub struct GtidSet {
    entries: BTreeMap<GtidSource, Vec<GtidRange>>,
    /// The currently in-progress GTID, if any.
    ///
    /// This represents a transaction that has started (GTID_EVENT received) but not yet
    /// committed (XID_EVENT not yet received). The `event_index` tracks how many row
    /// events have been applied within this transaction.
    pending: Option<GtidEvent>,
}

impl GtidSet {
    pub fn new() -> Self {
        Self::default()
    }

    /// Check if the set is empty (no committed GTIDs)
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Return the number of UUID keys in the set
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Iterate over the committed ranges in the set
    pub fn iter(&self) -> impl Iterator<Item = (&GtidSource, &Vec<GtidRange>)> {
        self.entries.iter()
    }

    /// Get the committed ranges for a given GTID key
    pub fn get(&self, key: &GtidSource) -> Option<&Vec<GtidRange>> {
        self.entries.get(key)
    }

    /// Get the currently pending GTID, if any
    pub fn pending(&self) -> Option<&GtidEvent> {
        self.pending.as_ref()
    }

    /// Get a mutable reference to the pending GTID, if any
    pub fn pending_mut(&mut self) -> Option<&mut GtidEvent> {
        self.pending.as_mut()
    }

    /// Set the pending GTID (called when GTID_EVENT is received)
    ///
    /// The event_index should be 0 when first setting the pending GTID.
    pub fn set_pending(&mut self, gtid: GtidEvent) {
        self.pending = Some(gtid);
    }

    /// Clear the pending GTID without finalizing (e.g., on ROLLBACK)
    pub fn clear_pending(&mut self) {
        self.pending = None;
    }

    /// Finalize the pending GTID (called when XID_EVENT/COMMIT is received)
    ///
    /// This adds the pending GTID's sequence number to the committed set and clears
    /// the pending state.
    pub fn finalize_pending(&mut self) {
        if let Some(gtid) = self.pending.take() {
            self.advance(gtid.key.clone(), gtid.sequence_number);
        }
    }

    /// Adds a single-number range [sequence_number, sequence_number] and merges adjacent ranges.
    /// Cascading merges are performed: if n-1 or n+1 exist in ranges, they are merged.
    pub fn advance(&mut self, key: GtidSource, sequence_number: u64) {
        let ranges = self.entries.entry(key).or_default();

        let mut new_range = GtidRange::single(sequence_number);

        // Find the contiguous span of ranges that overlap/touch new_range.
        // Since ranges are sorted and non-overlapping, this span is contiguous.
        let first = ranges.partition_point(|r| r.end.saturating_add(1) < new_range.start);
        let last =
            ranges[first..].partition_point(|r| r.start <= new_range.end.saturating_add(1)) + first;

        // Merge all overlapping ranges into new_range
        for range in &ranges[first..last] {
            new_range.start = new_range.start.min(range.start);
            new_range.end = new_range.end.max(range.end);
        }

        // Replace the span [first..last] with the single merged range
        ranges.splice(first..last, std::iter::once(new_range));
    }

    /// Convenience method to advance using a GtidEvent
    pub fn advance_gtid(&mut self, gtid: &GtidEvent) {
        self.advance(gtid.key.clone(), gtid.sequence_number);
    }

    /// Merge adjacent ranges in a sorted list of ranges (in-place, O(n))
    fn merge_adjacent_ranges(ranges: &mut Vec<GtidRange>) {
        if ranges.len() <= 1 {
            return;
        }

        let mut write = 0;
        for read in 1..ranges.len() {
            if ranges[write].overlaps_or_touches(&ranges[read]) {
                ranges[write].end = ranges[write].end.max(ranges[read].end);
            } else {
                write += 1;
                ranges[write] = ranges[read].clone();
            }
        }
        ranges.truncate(write + 1);
    }

    /// Merge new ranges into existing ranges, then merge adjacent ranges.
    /// Both existing and new ranges are assumed to be already sorted.
    fn merge_sorted_ranges(&mut self, key: GtidSource, new: Vec<GtidRange>) {
        let existing = self.entries.entry(key).or_default();

        // Take ownership of existing to avoid clone; merge-sort both sorted lists
        let old = std::mem::take(existing);
        let mut merged = Vec::with_capacity(old.len() + new.len());
        let mut old_iter = old.into_iter().peekable();
        let mut new_iter = new.into_iter().peekable();

        loop {
            let take_old = match (old_iter.peek(), new_iter.peek()) {
                (Some(e), Some(n)) => *e <= *n,
                (Some(_), None) => true,
                (None, Some(_)) => false,
                (None, None) => break,
            };
            if take_old {
                merged.push(old_iter.next().expect("peeked"));
            } else {
                merged.push(new_iter.next().expect("peeked"));
            }
        }

        *existing = merged;
        Self::merge_adjacent_ranges(existing);
    }

    /// Returns `true` if `gtid`'s sequence number falls within any committed range for its key.
    ///
    /// Note: This does NOT check the pending GTID.
    pub fn contains(&self, gtid: &GtidEvent) -> bool {
        self.get(&gtid.key).is_some_and(|ranges| {
            ranges
                .iter()
                .any(|range| range.contains(gtid.sequence_number))
        })
    }

    /// Check if `self` is a superset of `other` (i.e., `self` contains all GTIDs in `other`).
    pub fn is_superset_of(&self, other: &Self) -> bool {
        // We only need to check keys in `other`: for each key in `other`,
        // `self` must contain all of its ranges.
        for (key, other_ranges) in &other.entries {
            if other_ranges.is_empty() {
                continue;
            }
            let self_ranges = self.entries.get(key).map(|v| v.as_slice()).unwrap_or(&[]);
            if !Self::ranges_contained_in(other_ranges, self_ranges) {
                return false;
            }
        }
        true
    }

    /// Check if all ranges in `lhs` are contained in `rhs`.
    ///
    /// Both inputs must be sorted by `start` with non-overlapping ranges (invariant
    /// maintained by `advance()`, `merge_sorted_ranges()`, and all insertion paths).
    /// Uses a two-pointer scan for O(M+N) instead of O(M*N).
    fn ranges_contained_in(lhs: &[GtidRange], rhs: &[GtidRange]) -> bool {
        if lhs.is_empty() {
            return true;
        }
        if rhs.is_empty() {
            return false;
        }

        let mut r = 0;
        for l_range in lhs {
            // Advance rhs pointer until we find a range that could contain l_range
            while r < rhs.len() && rhs[r].end < l_range.start {
                r += 1;
            }
            // Check if the current rhs range fully contains l_range
            if r >= rhs.len() || rhs[r].start > l_range.start || rhs[r].end < l_range.end {
                return false;
            }
        }
        true
    }

    /// This method compares `self` and `other`, returning an [`Ordering`] result.
    pub fn try_partial_cmp(&self, other: &Self) -> ReadySetResult<Ordering> {
        // Walk both sorted BTreeMaps in lockstep to avoid allocating a merged key set.
        let mut self_iter = self.entries.iter().peekable();
        let mut other_iter = other.entries.iter().peekable();

        let mut is_subset = true;
        let mut is_superset = true;

        loop {
            let (key, self_ranges, other_ranges) = match (self_iter.peek(), other_iter.peek()) {
                (Some((sk, _)), Some((ok, _))) => match sk.cmp(ok) {
                    Ordering::Less => {
                        let (k, v) = self_iter.next().expect("peeked");
                        (k, v.as_slice(), &[] as &[GtidRange])
                    }
                    Ordering::Greater => {
                        let (k, v) = other_iter.next().expect("peeked");
                        (k, &[] as &[GtidRange], v.as_slice())
                    }
                    Ordering::Equal => {
                        let (k, sv) = self_iter.next().expect("peeked");
                        let (_, ov) = other_iter.next().expect("peeked");
                        (k, sv.as_slice(), ov.as_slice())
                    }
                },
                (Some(_), None) => {
                    let (k, v) = self_iter.next().expect("peeked");
                    (k, v.as_slice(), &[] as &[GtidRange])
                }
                (None, Some(_)) => {
                    let (k, v) = other_iter.next().expect("peeked");
                    (k, &[] as &[GtidRange], v.as_slice())
                }
                (None, None) => break,
            };

            let _ = key; // used for iteration only

            if self_ranges.is_empty() && other_ranges.is_empty() {
                continue;
            }

            if !Self::ranges_contained_in(self_ranges, other_ranges) {
                is_subset = false;
            }
            if !Self::ranges_contained_in(other_ranges, self_ranges) {
                is_superset = false;
            }
        }

        let entries_ordering = if is_subset && is_superset {
            Ordering::Equal
        } else if is_subset {
            Ordering::Less
        } else if is_superset {
            Ordering::Greater
        } else {
            replication_failed!(
                "Incomparable GTID sets: neither is a subset of the other \
                 (self={self}, other={other})"
            );
        };

        // If completed GTIDs are equal, compare pending GTIDs.
        if entries_ordering == Ordering::Equal {
            Ok(match (&self.pending, &other.pending) {
                (None, None) => Ordering::Equal,
                (Some(_), None) => Ordering::Greater,
                (None, Some(_)) => Ordering::Less,
                (Some(self_pending), Some(other_pending)) => self_pending.cmp(other_pending),
            })
        } else {
            Ok(entries_ordering)
        }
    }

    /// Parse a textual GTID set (as produced by `@@GLOBAL.gtid_executed`).
    /// This follows the format described in https://dev.mysql.com/doc/refman/8.4/en/replication-gtids-concepts.html
    pub fn parse(gtid_set: &str) -> ReadySetResult<Self> {
        if gtid_set.trim().is_empty() {
            return Ok(Self::default());
        }

        let mut entries = Self::default();
        for raw_entry in gtid_set.split(',') {
            let trimmed = raw_entry.trim();
            if trimmed.is_empty() {
                continue;
            }
            entries.parse_single_entry(trimmed)?;
        }

        Ok(entries)
    }

    /// Parse a single GTID entry, adding it to the set.
    ///
    /// MySQL formats all entries for a single UUID in one colon-separated string, including
    /// multiple tag groups:
    ///
    /// ```text
    /// uuid:1-121:readtest:1-3:repltest:1-6:sessiontag:1-3
    /// ```
    ///
    /// This represents untagged intervals (`1-121`) plus three tagged interval groups.
    /// See <https://dev.mysql.com/doc/refman/8.4/en/replication-gtids-concepts.html>.
    fn parse_single_entry(&mut self, entry: &str) -> ReadySetResult<()> {
        let segments: Vec<&str> = entry.split(':').collect();
        if segments.len() < 2 {
            replication_failed!("Malformed GTID entry '{entry}'");
        }

        let uuid_segment = segments[0];
        let server_uuid = Uuid::parse_str(uuid_segment).map_err(|e| {
            replication_failed_err!("Invalid UUID '{uuid_segment}' in GTID entry '{entry}': {e}")
        })?;

        // Parse groups of [tag:]interval[:interval]* from the remaining segments.
        // Each non-interval segment starts a new tag group; interval segments accumulate
        // into the current group.
        let mut current_tag: Option<String> = None;
        let mut current_ranges: Vec<GtidRange> = Vec::new();
        let mut flushed_any = false;

        for &segment in &segments[1..] {
            if is_interval_component(segment) {
                let (start, end) = parse_interval(segment, entry)?;
                current_ranges.push(GtidRange::new(start, end)?);
            } else {
                // Flush the previous group if it has ranges, then start a new tag group.
                if !current_ranges.is_empty() {
                    current_ranges.sort();
                    let key = GtidSource {
                        server_uuid,
                        tag: current_tag.take(),
                    };
                    self.merge_sorted_ranges(key, std::mem::take(&mut current_ranges));
                    flushed_any = true;
                } else if current_tag.is_some() {
                    // Two consecutive tags without any interval in between — malformed.
                    replication_failed!(
                        "Tag '{}' has no intervals in GTID entry '{entry}'",
                        current_tag.as_ref().expect("checked above")
                    );
                }
                validate_tag(segment, entry)?;
                current_tag = Some(segment.to_string());
            }
        }

        // Flush the last group.
        if current_ranges.is_empty() {
            if !flushed_any {
                replication_failed!("GTID entry '{entry}' has no valid ranges");
            }
            // A trailing tag with no ranges is malformed.
            if current_tag.is_some() {
                replication_failed!("Missing GTID ranges for tag in entry '{entry}'");
            }
        } else {
            current_ranges.sort();
            let key = GtidSource {
                server_uuid,
                tag: current_tag,
            };
            self.merge_sorted_ranges(key, current_ranges);
        }

        Ok(())
    }

    /// Returns the MySQL-compatible string representation of the committed GTID set.
    ///
    /// This excludes the pending GTID and any event_index information.
    /// Use this when advertising GTIDs to MySQL.
    ///
    /// The output groups all entries for the same UUID into a single colon-separated
    /// string, matching MySQL's canonical format:
    ///
    /// ```text
    /// uuid:1-121:tag1:1-3:tag2:1-6
    /// ```
    ///
    /// The `BTreeMap` ordering guarantees that keys with the same UUID (but different
    /// tags) are adjacent, with the untagged entry (`tag: None`) first.
    pub fn to_mysql_string(&self) -> String {
        let mut result = String::new();
        let mut current_uuid: Option<&Uuid> = None;

        for (key, ranges) in &self.entries {
            let same_uuid = current_uuid == Some(&key.server_uuid);

            if !same_uuid {
                // New UUID group — emit comma separator (unless first entry)
                if current_uuid.is_some() {
                    result.push(',');
                }
                current_uuid = Some(&key.server_uuid);
                result.push_str(&key.server_uuid.to_string());
            }

            // Emit tag prefix (or just ':' for untagged entry)
            if let Some(tag) = &key.tag {
                result.push(':');
                result.push_str(tag);
            }

            // Emit intervals
            for range in ranges {
                result.push(':');
                if range.start == range.end {
                    result.push_str(&range.start.to_string());
                } else {
                    result.push_str(&range.start.to_string());
                    result.push('-');
                    result.push_str(&range.end.to_string());
                }
            }
        }

        result
    }
}

impl FromStr for GtidSet {
    type Err = ReadySetError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

impl fmt::Display for GtidSet {
    /// Display format shows committed GTIDs and pending GTID (if any) for debugging.
    ///
    /// Note: For MySQL-compatible output, use [`Self::to_mysql_string`] instead.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_mysql_string())?;

        if let Some(pending) = &self.pending {
            write!(f, " [pending: {}]", pending)?;
        }

        Ok(())
    }
}

impl PartialOrd for GtidSet {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.try_partial_cmp(other).ok()
    }
}

// Check if a string is a valid GTID interval component. Tags should return false
// as they match the pattern [a-z_][a-z0-9_]{0,31}
fn is_interval_component(segment: &str) -> bool {
    !segment.is_empty() && segment.bytes().all(|c| matches!(c, b'0'..=b'9' | b'-'))
}

/// Validate that a GTID tag matches the MySQL 8.4 specification: `[a-z_][a-z0-9_]{0,31}`.
fn validate_tag(tag: &str, entry: &str) -> ReadySetResult<()> {
    if tag.is_empty() || tag.len() > 32 {
        replication_failed!("Invalid GTID tag length ({}) in entry '{entry}'", tag.len());
    }
    let bytes = tag.as_bytes();
    if !matches!(bytes[0], b'a'..=b'z' | b'_') {
        replication_failed!(
            "Invalid first character '{}' in GTID tag '{tag}' in entry '{entry}'",
            bytes[0] as char
        );
    }
    if !bytes[1..]
        .iter()
        .all(|&b| matches!(b, b'a'..=b'z' | b'0'..=b'9' | b'_'))
    {
        replication_failed!("Invalid character in GTID tag '{tag}' in entry '{entry}'");
    }
    Ok(())
}

fn parse_interval(segment: &str, original: &str) -> ReadySetResult<(u64, u64)> {
    let mut pieces = segment.split('-');
    let start = pieces
        .next()
        .ok_or_else(|| replication_failed_err!("Malformed GTID range in '{original}'"))?
        .parse::<u64>()
        .map_err(|e| replication_failed_err!("Invalid GTID range in '{original}': {e}"))?;

    let maybe_end = pieces.next();
    if pieces.next().is_some() {
        replication_failed!("Malformed GTID range in '{original}'");
    }

    let end = match maybe_end {
        Some(end) => end
            .parse::<u64>()
            .map_err(|e| replication_failed_err!("Invalid GTID range in '{original}': {e}"))?,
        None => start,
    };

    if start == 0 || end == 0 {
        replication_failed!("GTID interval has zero bounds in '{original}'");
    }

    if end < start {
        replication_failed!("GTID interval has decreasing bounds in '{original}'");
    }

    Ok((start, end))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn uuid() -> Uuid {
        Uuid::parse_str("3E11FA47-71CA-11E1-9E33-C80AA9429562").unwrap()
    }

    fn other_uuid() -> Uuid {
        Uuid::parse_str("7E11FA47-71CA-11E1-9E33-C80AA9429563").unwrap()
    }

    fn gtid(uuid: Uuid, tag: Option<String>, seq: u64) -> GtidEvent {
        GtidEvent::new(
            GtidSource {
                server_uuid: uuid,
                tag,
            },
            seq,
        )
    }

    mod gtid_event {
        use super::*;

        #[test]
        fn new_has_zero_event_index() {
            let gtid = gtid(uuid(), None, 42);
            assert_eq!(gtid.event_index(), 0);
        }

        #[test]
        fn advance_event_increments_index() {
            let mut gtid = gtid(uuid(), None, 42);
            gtid.advance_event();
            assert_eq!(gtid.event_index(), 1);
            gtid.advance_event();
            assert_eq!(gtid.event_index(), 2);
        }

        #[test]
        fn to_mysql_string_excludes_event_index() {
            let mut gtid = gtid(uuid(), None, 42);
            gtid.advance_event();
            gtid.advance_event();

            // MySQL string should NOT include event_index
            let mysql_str = gtid.to_mysql_string();
            assert_eq!(mysql_str, format!("{}:42", uuid()));
            assert!(!mysql_str.contains('@'));
        }

        #[test]
        fn display_includes_event_index() {
            let mut gtid = gtid(uuid(), None, 42);
            gtid.advance_event();

            // Display format SHOULD include event_index for debugging
            let display_str = gtid.to_string();
            assert!(display_str.contains("@1"));
        }

        #[test]
        fn with_tag_to_mysql_string() {
            let gtid = gtid(uuid(), Some("blue".into()), 11);
            assert_eq!(gtid.to_mysql_string(), format!("{}:blue:11", uuid()));
        }

        #[test]
        fn with_tag_display_includes_event_index() {
            let mut gtid = gtid(uuid(), Some("red".into()), 42);
            gtid.advance_event();
            gtid.advance_event();

            // Display format should include tag and event_index
            let display_str = gtid.to_string();
            assert!(display_str.contains(":red:"));
            assert!(display_str.contains(":42@2"));
        }

        #[test]
        fn cmp_includes_uuid_and_tag() {
            let u1 = Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa").unwrap();
            let u2 = Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap();

            // Same sequence_number but different UUIDs — must not compare as equal
            let a = gtid(u1, None, 5);
            let b = gtid(u2, None, 5);
            assert_ne!(a.cmp(&b), Ordering::Equal);

            // Same UUID, same sequence_number, different tags — must not compare as equal
            let c = gtid(u1, Some("red".into()), 5);
            let d = gtid(u1, Some("blue".into()), 5);
            assert_ne!(c.cmp(&d), Ordering::Equal);

            // Same full identity — must be equal
            let e = gtid(u1, Some("red".into()), 5);
            let f = gtid(u1, Some("red".into()), 5);
            assert_eq!(e.cmp(&f), Ordering::Equal);
        }
    }

    mod gtid_range {
        use super::*;

        #[test]
        fn single() {
            let range = GtidRange::single(5);
            assert_eq!(range.start, 5);
            assert_eq!(range.end, 5);
        }

        #[test]
        fn contains() {
            let range = GtidRange::new(1, 5).unwrap();
            assert!(range.contains(1));
            assert!(range.contains(3));
            assert!(range.contains(5));
            assert!(!range.contains(0));
            assert!(!range.contains(6));
        }

        #[test]
        fn overlaps_or_touches() {
            let r1 = GtidRange::new(1, 5).unwrap();
            let r2 = GtidRange::new(5, 10).unwrap(); // Touches
            let r3 = GtidRange::new(3, 7).unwrap(); // Overlaps
            let r4 = GtidRange::new(6, 10).unwrap(); // Adjacent
            let r5 = GtidRange::new(11, 15).unwrap(); // Separate

            assert!(r1.overlaps_or_touches(&r2));
            assert!(r1.overlaps_or_touches(&r3));
            assert!(r1.overlaps_or_touches(&r4));
            assert!(!r1.overlaps_or_touches(&r5));
        }

        #[test]
        fn merge() {
            let r1 = GtidRange::new(1, 5).unwrap();
            let r2 = GtidRange::new(6, 10).unwrap();
            let r3 = GtidRange::new(3, 7).unwrap();
            let r4 = GtidRange::new(11, 15).unwrap();

            let merged = r1.merge(&r2).unwrap();
            assert_eq!(merged.start, 1);
            assert_eq!(merged.end, 10);

            let merged = r1.merge(&r3).unwrap();
            assert_eq!(merged.start, 1);
            assert_eq!(merged.end, 7);

            assert!(r1.merge(&r4).is_none());
        }
    }

    mod gtid_set {
        use super::*;

        #[test]
        fn pending_workflow() {
            let mut set = GtidSet::new();

            // No pending initially
            assert!(set.pending().is_none());

            // Set pending GTID
            set.set_pending(gtid(uuid(), None, 42));
            assert!(set.pending().is_some());
            assert_eq!(set.pending().unwrap().sequence_number(), 42);
            assert_eq!(set.pending().unwrap().event_index(), 0);

            // Advance event index
            set.pending_mut().unwrap().advance_event();
            assert_eq!(set.pending().unwrap().event_index(), 1);

            // GTID not in committed set yet
            let check_gtid = gtid(uuid(), None, 42);
            assert!(!set.contains(&check_gtid));

            // Finalize pending
            set.finalize_pending();
            assert!(set.pending().is_none());

            // Now GTID is in committed set
            assert!(set.contains(&check_gtid));
        }

        #[test]
        fn clear_pending_on_rollback() {
            let mut set = GtidSet::new();

            set.set_pending(gtid(uuid(), None, 42));
            set.pending_mut().unwrap().advance_event();

            // Rollback - clear pending without finalizing
            set.clear_pending();
            assert!(set.pending().is_none());

            // GTID should NOT be in committed set
            let check_gtid = gtid(uuid(), None, 42);
            assert!(!set.contains(&check_gtid));
        }

        #[test]
        fn to_mysql_string_excludes_pending() {
            let mut set = GtidSet::new();

            // Add a committed GTID
            set.advance(
                GtidSource {
                    server_uuid: uuid(),
                    tag: None,
                },
                10,
            );

            // Set a pending GTID with a different sequence number
            let mut pending = gtid(uuid(), None, 99999);
            pending.advance_event();
            set.set_pending(pending);

            // MySQL string should only show committed GTIDs
            let mysql_str = set.to_mysql_string();
            assert!(mysql_str.contains(":10")); // committed GTID
            assert!(!mysql_str.contains("99999")); // pending should not appear
            assert!(!mysql_str.contains("pending"));
        }

        #[test]
        fn parses_simple() {
            let set = GtidSet::parse("3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5").unwrap();
            assert_eq!(set.len(), 1);
            let ranges = set
                .get(&GtidSource {
                    server_uuid: uuid(),
                    tag: None,
                })
                .unwrap();
            assert_eq!(ranges.len(), 1);
            assert_eq!(ranges[0].start, 1);
            assert_eq!(ranges[0].end, 5);
        }

        #[test]
        fn parses_multiple_entries() {
            let set = GtidSet::parse(
                "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-10, \
                 7E11FA47-71CA-11E1-9E33-C80AA9429563:5-8:10-12",
            )
            .unwrap();
            assert_eq!(set.len(), 2);
            let ranges = set
                .get(&GtidSource {
                    server_uuid: uuid(),
                    tag: None,
                })
                .unwrap();
            assert_eq!(ranges.len(), 1);
            assert_eq!(ranges[0].start, 1);
            assert_eq!(ranges[0].end, 10);

            let other_ranges = set
                .get(&GtidSource {
                    server_uuid: other_uuid(),
                    tag: None,
                })
                .unwrap();
            // Ranges 5-8 and 10-12 should be separate (gap at 9)
            assert_eq!(other_ranges.len(), 2);
        }

        #[test]
        fn comparison_detects_subset() {
            let mut lhs = GtidSet::new();
            lhs.advance(
                GtidSource {
                    server_uuid: uuid(),
                    tag: None,
                },
                10,
            );
            let mut rhs = lhs.clone();
            rhs.advance(
                GtidSource {
                    server_uuid: uuid(),
                    tag: None,
                },
                12,
            );

            // lhs is a proper subset of rhs
            assert!(lhs.try_partial_cmp(&rhs).unwrap().is_lt());
            // rhs is NOT a proper subset of lhs, so returns Greater
            assert!(rhs.try_partial_cmp(&lhs).unwrap().is_gt());
        }

        #[test]
        fn comparison_with_pending() {
            // Create two sets with the same completed entries
            let mut set_without_pending = GtidSet::new();
            set_without_pending.advance(
                GtidSource {
                    server_uuid: uuid(),
                    tag: None,
                },
                10,
            );

            let mut set_with_pending = set_without_pending.clone();
            // Start a new transaction (GTID 11) at event index 0
            set_with_pending.set_pending(gtid(uuid(), None, 11));

            // Set with pending GTID should be GREATER than set without
            // (we've started processing a new transaction)
            assert!(
                set_with_pending
                    .try_partial_cmp(&set_without_pending)
                    .unwrap()
                    .is_gt()
            );
            assert!(
                set_without_pending
                    .try_partial_cmp(&set_with_pending)
                    .unwrap()
                    .is_lt()
            );

            // Two sets with same entries and same pending should be equal
            let mut set_with_pending_clone = set_with_pending.clone();
            assert!(
                set_with_pending
                    .try_partial_cmp(&set_with_pending_clone)
                    .unwrap()
                    .is_eq()
            );

            // Advancing event index should make position greater
            set_with_pending.pending_mut().unwrap().advance_event();
            assert!(
                set_with_pending
                    .try_partial_cmp(&set_with_pending_clone)
                    .unwrap()
                    .is_gt()
            );

            // Higher sequence number in pending should be greater
            set_with_pending_clone.finalize_pending(); // Complete GTID 11
            set_with_pending_clone.set_pending(gtid(uuid(), None, 12));
            // set_with_pending has pending 11@1
            // set_with_pending_clone has completed 11 and pending 12@0
            // set_with_pending_clone has more completed entries, so it's greater
            assert!(
                set_with_pending_clone
                    .try_partial_cmp(&set_with_pending)
                    .unwrap()
                    .is_gt()
            );
        }

        #[test]
        fn advance_merges_adjacent_ranges() {
            let mut set = GtidSet::new();
            let key = GtidSource {
                server_uuid: uuid(),
                tag: None,
            };

            // Advance to 5, creates [5,5]
            set.advance(key.clone(), 5);
            // Advance to 6, should merge with [5,5] to create [5,6]
            set.advance(key.clone(), 6);

            let ranges = set.get(&key).unwrap();
            assert_eq!(ranges.len(), 1);
            assert_eq!(ranges[0].start, 5);
            assert_eq!(ranges[0].end, 6);
        }

        #[test]
        fn advance_cascading_merge() {
            let mut set = GtidSet::new();
            let key = GtidSource {
                server_uuid: uuid(),
                tag: None,
            };

            // Create ranges [1,1] and [3,3]
            set.advance(key.clone(), 1);
            set.advance(key.clone(), 3);

            // Advance to 2, should merge with both [1,1] and [3,3] to create [1,3]
            set.advance(key.clone(), 2);

            let ranges = set.get(&key).unwrap();
            assert_eq!(ranges.len(), 1);
            assert_eq!(ranges[0].start, 1);
            assert_eq!(ranges[0].end, 3);
        }

        #[test]
        fn contains_with_gaps() {
            let mut set = GtidSet::new();
            let key = GtidSource {
                server_uuid: uuid(),
                tag: None,
            };

            set.advance(key.clone(), 5);
            set.advance(key.clone(), 10);

            // Should have [5,5] and [10,10] (separate ranges)
            let gtid1 = gtid(uuid(), None, 5);
            let gtid2 = gtid(uuid(), None, 10);
            let gtid3 = gtid(uuid(), None, 7);

            assert!(set.contains(&gtid1));
            assert!(set.contains(&gtid2));
            assert!(!set.contains(&gtid3)); // 7 is in the gap
        }

        #[test]
        fn parses_tagged_entry() {
            let set =
                GtidSet::parse(&format!("{}:tag:1-4,{}:10-12", uuid(), other_uuid())).unwrap();
            let ranges = set
                .get(&GtidSource {
                    server_uuid: uuid(),
                    tag: Some("tag".into()),
                })
                .unwrap();
            assert_eq!(ranges.len(), 1);
            assert_eq!(ranges[0].start, 1);
            assert_eq!(ranges[0].end, 4);
        }

        #[test]
        fn serialization_roundtrip_with_pending() {
            let mut set = GtidSet::new();
            set.advance(
                GtidSource {
                    server_uuid: uuid(),
                    tag: None,
                },
                10,
            );

            let mut pending = gtid(uuid(), None, 42);
            pending.advance_event();
            pending.advance_event();
            set.set_pending(pending);

            let serialized = bincode::serialize(&set).unwrap();
            let deserialized: GtidSet = bincode::deserialize(&serialized).unwrap();

            assert_eq!(set.entries, deserialized.entries);
            assert_eq!(set.pending, deserialized.pending);
            assert_eq!(deserialized.pending().unwrap().event_index(), 2);
        }

        #[test]
        fn serialization_roundtrip_without_pending() {
            let mut set = GtidSet::new();
            set.advance(
                GtidSource {
                    server_uuid: uuid(),
                    tag: None,
                },
                10,
            );

            let serialized = bincode::serialize(&set).unwrap();
            let deserialized: GtidSet = bincode::deserialize(&serialized).unwrap();

            assert_eq!(set.entries, deserialized.entries);
            assert!(deserialized.pending().is_none());
        }

        #[test]
        fn source_display_with_tag() {
            let key_without_tag = GtidSource {
                server_uuid: uuid(),
                tag: None,
            };
            assert_eq!(key_without_tag.to_string(), uuid().to_string());

            let key_with_tag = GtidSource {
                server_uuid: uuid(),
                tag: Some("blue".into()),
            };
            assert_eq!(key_with_tag.to_string(), format!("{}:blue", uuid()));
        }

        #[test]
        fn with_tagged_entries() {
            let mut set = GtidSet::new();

            // Add untagged entry
            let untagged_key = GtidSource {
                server_uuid: uuid(),
                tag: None,
            };
            set.advance(untagged_key.clone(), 5);

            // Add tagged entry with same UUID but different tag
            let tagged_key = GtidSource {
                server_uuid: uuid(),
                tag: Some("blue".into()),
            };
            set.advance(tagged_key.clone(), 10);

            // Both should be present as separate entries
            assert_eq!(set.len(), 2);

            let untagged_ranges = set.get(&untagged_key).unwrap();
            assert_eq!(untagged_ranges[0].start, 5);
            assert_eq!(untagged_ranges[0].end, 5);

            let tagged_ranges = set.get(&tagged_key).unwrap();
            assert_eq!(tagged_ranges[0].start, 10);
            assert_eq!(tagged_ranges[0].end, 10);
        }

        #[test]
        fn to_mysql_string_with_tags() {
            let mut set = GtidSet::new();

            // Add untagged entry
            set.advance(
                GtidSource {
                    server_uuid: uuid(),
                    tag: None,
                },
                5,
            );

            // Add tagged entry (same UUID, different tag)
            set.advance(
                GtidSource {
                    server_uuid: uuid(),
                    tag: Some("blue".into()),
                },
                10,
            );

            let mysql_str = set.to_mysql_string();
            // Canonical format groups all tags under one UUID:
            // uuid:5:blue:10
            assert_eq!(mysql_str, format!("{}:5:blue:10", uuid()));
        }

        #[test]
        fn contains_with_tag() {
            let mut set = GtidSet::new();

            // Add tagged entry
            set.advance(
                GtidSource {
                    server_uuid: uuid(),
                    tag: Some("blue".into()),
                },
                10,
            );

            // Check contains with matching tag
            let gtid_with_tag = gtid(uuid(), Some("blue".into()), 10);
            assert!(set.contains(&gtid_with_tag));

            // Check contains with different tag - should NOT match
            let gtid_different_tag = gtid(uuid(), Some("red".into()), 10);
            assert!(!set.contains(&gtid_different_tag));

            // Check contains with no tag - should NOT match
            let gtid_no_tag = gtid(uuid(), None, 10);
            assert!(!set.contains(&gtid_no_tag));
        }

        #[test]
        fn pending_with_tag() {
            let mut set = GtidSet::new();

            // Set pending GTID with tag
            set.set_pending(gtid(uuid(), Some("blue".into()), 42));

            assert!(set.pending().is_some());
            assert_eq!(set.pending().unwrap().source().tag, Some("blue".into()));
            assert_eq!(set.pending().unwrap().sequence_number(), 42);

            // Advance event index
            set.pending_mut().unwrap().advance_event();
            assert_eq!(set.pending().unwrap().event_index(), 1);

            // Finalize pending
            set.finalize_pending();
            assert!(set.pending().is_none());

            // Should be in committed set with correct tag
            let check_gtid = gtid(uuid(), Some("blue".into()), 42);
            assert!(set.contains(&check_gtid));

            // Different tag should not match
            let different_tag = gtid(uuid(), None, 42);
            assert!(!set.contains(&different_tag));
        }

        #[test]
        fn comparison_with_tags() {
            // Create two sets with tagged entries
            let mut set1 = GtidSet::new();
            set1.advance(
                GtidSource {
                    server_uuid: uuid(),
                    tag: Some("blue".into()),
                },
                10,
            );

            let mut set2 = set1.clone();
            set2.advance(
                GtidSource {
                    server_uuid: uuid(),
                    tag: Some("blue".into()),
                },
                15,
            );

            // set1 < set2 (subset)
            assert!(set1.try_partial_cmp(&set2).unwrap().is_lt());
            assert!(set2.try_partial_cmp(&set1).unwrap().is_gt());

            // Adding untagged entry to set1 makes them incomparable in subset terms
            set1.advance(
                GtidSource {
                    server_uuid: uuid(),
                    tag: None,
                },
                20,
            );

            // set1 has [blue:10, untagged:20], set2 has [blue:1-15]
            // Neither is a subset of the other — must return an error
            assert!(set1.try_partial_cmp(&set2).is_err());
            assert!(set1.partial_cmp(&set2).is_none());
        }

        #[test]
        fn parses_multiple_tagged_entries_same_uuid() {
            // Parse GTID set with multiple tags for the same UUID
            let set = GtidSet::parse(&format!(
                "{}:1-5,{}:blue:10-15,{}:red:20-25",
                uuid(),
                uuid(),
                uuid()
            ))
            .unwrap();

            assert_eq!(set.len(), 3);

            // Check untagged
            let untagged = set
                .get(&GtidSource {
                    server_uuid: uuid(),
                    tag: None,
                })
                .unwrap();
            assert_eq!(untagged[0].start, 1);
            assert_eq!(untagged[0].end, 5);

            // Check blue tag
            let blue = set
                .get(&GtidSource {
                    server_uuid: uuid(),
                    tag: Some("blue".into()),
                })
                .unwrap();
            assert_eq!(blue[0].start, 10);
            assert_eq!(blue[0].end, 15);

            // Check red tag
            let red = set
                .get(&GtidSource {
                    server_uuid: uuid(),
                    tag: Some("red".into()),
                })
                .unwrap();
            assert_eq!(red[0].start, 20);
            assert_eq!(red[0].end, 25);
        }

        #[test]
        fn parses_mysql_native_multi_tag_format() {
            // MySQL returns all tag groups for a single UUID in one colon-separated entry:
            //   uuid:1-121:readtest:1-3:repltest:1-6:sessiontag:1-3
            let input = format!("{}:1-121:readtest:1-3:repltest:1-6:sessiontag:1-3", uuid());
            let set = GtidSet::parse(&input).unwrap();

            assert_eq!(set.len(), 4);

            let untagged = set
                .get(&GtidSource {
                    server_uuid: uuid(),
                    tag: None,
                })
                .unwrap();
            assert_eq!(untagged[0].start, 1);
            assert_eq!(untagged[0].end, 121);

            let readtest = set
                .get(&GtidSource {
                    server_uuid: uuid(),
                    tag: Some("readtest".into()),
                })
                .unwrap();
            assert_eq!(readtest[0].start, 1);
            assert_eq!(readtest[0].end, 3);

            let repltest = set
                .get(&GtidSource {
                    server_uuid: uuid(),
                    tag: Some("repltest".into()),
                })
                .unwrap();
            assert_eq!(repltest[0].start, 1);
            assert_eq!(repltest[0].end, 6);

            let sessiontag = set
                .get(&GtidSource {
                    server_uuid: uuid(),
                    tag: Some("sessiontag".into()),
                })
                .unwrap();
            assert_eq!(sessiontag[0].start, 1);
            assert_eq!(sessiontag[0].end, 3);
        }

        #[test]
        fn parses_mysql_native_tag_only_format() {
            // When the UUID only has tagged GTIDs and no untagged ones.
            let input = format!("{}:blue:1-5:red:10-20", uuid());
            let set = GtidSet::parse(&input).unwrap();

            assert_eq!(set.len(), 2);

            let blue = set
                .get(&GtidSource {
                    server_uuid: uuid(),
                    tag: Some("blue".into()),
                })
                .unwrap();
            assert_eq!(blue[0].start, 1);
            assert_eq!(blue[0].end, 5);

            let red = set
                .get(&GtidSource {
                    server_uuid: uuid(),
                    tag: Some("red".into()),
                })
                .unwrap();
            assert_eq!(red[0].start, 10);
            assert_eq!(red[0].end, 20);
        }

        #[test]
        fn parses_mysql_native_multi_tag_with_multiple_intervals() {
            // Tag groups can have multiple intervals.
            let input = format!("{}:1-5:10-20:mytag:1-3:7-9", uuid());
            let set = GtidSet::parse(&input).unwrap();

            assert_eq!(set.len(), 2);

            let untagged = set
                .get(&GtidSource {
                    server_uuid: uuid(),
                    tag: None,
                })
                .unwrap();
            assert_eq!(untagged.len(), 2);
            assert_eq!(untagged[0].start, 1);
            assert_eq!(untagged[0].end, 5);
            assert_eq!(untagged[1].start, 10);
            assert_eq!(untagged[1].end, 20);

            let mytag = set
                .get(&GtidSource {
                    server_uuid: uuid(),
                    tag: Some("mytag".into()),
                })
                .unwrap();
            assert_eq!(mytag.len(), 2);
            assert_eq!(mytag[0].start, 1);
            assert_eq!(mytag[0].end, 3);
            assert_eq!(mytag[1].start, 7);
            assert_eq!(mytag[1].end, 9);
        }

        #[test]
        fn serialization_roundtrip_with_tagged_pending() {
            let mut set = GtidSet::new();

            // Add tagged committed entry
            set.advance(
                GtidSource {
                    server_uuid: uuid(),
                    tag: Some("blue".into()),
                },
                10,
            );

            // Set tagged pending GTID
            let mut pending = gtid(uuid(), Some("red".into()), 42);
            pending.advance_event();
            pending.advance_event();
            set.set_pending(pending);

            let serialized = bincode::serialize(&set).unwrap();
            let deserialized: GtidSet = bincode::deserialize(&serialized).unwrap();

            assert_eq!(set.entries, deserialized.entries);
            assert_eq!(set.pending, deserialized.pending);
            assert_eq!(
                deserialized.pending().unwrap().source().tag,
                Some("red".into())
            );
        }

        #[test]
        fn incomparable_sets_return_error() {
            let u = uuid();
            // set1 has uuid:untagged:1-10
            let mut set1 = GtidSet::new();
            set1.advance(
                GtidSource {
                    server_uuid: u,
                    tag: None,
                },
                10,
            );

            // set2 has uuid:blue:1-5 (different key, so neither is a subset)
            let mut set2 = GtidSet::new();
            set2.advance(
                GtidSource {
                    server_uuid: u,
                    tag: Some("blue".into()),
                },
                5,
            );

            assert!(set1.try_partial_cmp(&set2).is_err());
            assert!(set2.try_partial_cmp(&set1).is_err());
            assert!(set1.partial_cmp(&set2).is_none());
        }

        #[test]
        fn parser_rejects_orphan_tag_group() {
            // Two consecutive tags without any interval: "uuid:tag1:tag2:1-5"
            let input = format!("{}:tag1:tag2:1-5", uuid());
            let result = GtidSet::parse(&input);
            assert!(result.is_err(), "should reject orphan tag group");
        }

        #[test]
        fn parser_rejects_trailing_tag_without_intervals() {
            let input = format!("{}:1-5:orphan", uuid());
            let result = GtidSet::parse(&input);
            assert!(
                result.is_err(),
                "should reject trailing tag without intervals"
            );
        }

        #[test]
        fn ranges_contained_in_fragmented() {
            // lhs has multiple small ranges that must each be found in separate rhs ranges
            let superset =
                GtidSet::parse(&format!("{}:1-10,{}:1-20", uuid(), other_uuid())).unwrap();
            // subset has fragmented ranges that must each be found in rhs
            let subset =
                GtidSet::parse(&format!("{}:3-5:8-10,{}:1-15", uuid(), other_uuid())).unwrap();

            // subset ⊆ superset
            assert!(subset.try_partial_cmp(&superset).unwrap().is_le());

            // Remove the high end from superset for other_uuid to make them incomparable
            let not_superset =
                GtidSet::parse(&format!("{}:1-10,{}:1-10", uuid(), other_uuid())).unwrap();
            // subset has other_uuid:1-15 which is NOT contained in not_superset other_uuid:1-10
            assert!(subset.try_partial_cmp(&not_superset).is_err());
        }

        #[test]
        fn parser_rejects_uppercase_tag() {
            let input = format!("{}:BadTag:1-5", uuid());
            let result = GtidSet::parse(&input);
            assert!(result.is_err(), "should reject tag with uppercase letters");
        }

        #[test]
        fn parser_rejects_tag_too_long() {
            let long_tag = "a".repeat(33);
            let input = format!("{}:{}:1-5", uuid(), long_tag);
            let result = GtidSet::parse(&input);
            assert!(
                result.is_err(),
                "should reject tag longer than 32 characters"
            );
        }

        #[test]
        fn parser_rejects_tag_starting_with_digit() {
            let input = format!("{}:1tag:1-5", uuid());
            let result = GtidSet::parse(&input);
            assert!(result.is_err(), "should reject tag starting with digit");
        }

        #[test]
        fn parser_accepts_valid_tag_at_max_length() {
            let tag = "a".repeat(32);
            let input = format!("{}:{}:1-5", uuid(), tag);
            let result = GtidSet::parse(&input);
            assert!(result.is_ok(), "should accept 32-character tag");
        }

        #[test]
        fn parser_accepts_tag_with_underscore_start() {
            let input = format!("{}:_my_tag:1-5", uuid());
            let result = GtidSet::parse(&input);
            assert!(result.is_ok(), "should accept tag starting with underscore");
        }

        #[test]
        fn to_mysql_string_roundtrip_untagged() {
            let input = format!("{}:1-10:15-20,{}:5-8", uuid(), other_uuid());
            let set = GtidSet::parse(&input).expect("should parse");
            let output = set.to_mysql_string();
            let reparsed = GtidSet::parse(&output).expect("should re-parse");
            assert_eq!(set, reparsed);
        }

        #[test]
        fn to_mysql_string_roundtrip_tagged() {
            let input = format!("{}:1-121:readtest:1-3:repltest:1-6:sessiontag:1-3", uuid());
            let set = GtidSet::parse(&input).expect("should parse");
            let output = set.to_mysql_string();
            let reparsed = GtidSet::parse(&output).expect("should re-parse");
            assert_eq!(set, reparsed);
        }

        #[test]
        fn to_mysql_string_roundtrip_multi_uuid_tagged() {
            let input = format!("{}:1-10:blue:5-8,{}:red:1-3", uuid(), other_uuid());
            let set = GtidSet::parse(&input).expect("should parse");
            let output = set.to_mysql_string();
            let reparsed = GtidSet::parse(&output).expect("should re-parse");
            assert_eq!(set, reparsed);
        }

        #[test]
        fn to_mysql_string_groups_tags_under_uuid() {
            // Build a set with untagged + two tags for the same UUID
            let mut set = GtidSet::new();
            let key_none = GtidSource {
                server_uuid: uuid(),
                tag: None,
            };
            let key_blue = GtidSource {
                server_uuid: uuid(),
                tag: Some("blue".into()),
            };
            let key_red = GtidSource {
                server_uuid: uuid(),
                tag: Some("red".into()),
            };

            set.advance(key_none, 100);
            set.advance(key_blue, 10);
            set.advance(key_red, 5);

            let output = set.to_mysql_string();
            // UUID should appear exactly once
            let uuid_str = uuid().to_string();
            assert_eq!(
                output.matches(&uuid_str).count(),
                1,
                "UUID should appear once; got: {output}"
            );
            // Canonical form: uuid:100:blue:10:red:5
            assert_eq!(output, format!("{uuid_str}:100:blue:10:red:5"));
        }

        #[test]
        fn to_mysql_string_canonical_matches_mysql_native_format() {
            // The native format MySQL outputs for multi-tag GTIDs
            let canonical = format!("{}:1-121:readtest:1-3:repltest:1-6:sessiontag:1-3", uuid());
            let set = GtidSet::parse(&canonical).expect("should parse");
            let output = set.to_mysql_string();
            assert_eq!(
                output, canonical,
                "output should match MySQL canonical form"
            );
        }

        #[test]
        fn to_mysql_string_tag_only_uuid() {
            // UUID with only tagged entries (no untagged)
            let mut set = GtidSet::new();
            set.advance(
                GtidSource {
                    server_uuid: uuid(),
                    tag: Some("alpha".into()),
                },
                10,
            );
            set.advance(
                GtidSource {
                    server_uuid: uuid(),
                    tag: Some("beta".into()),
                },
                20,
            );
            let output = set.to_mysql_string();
            assert_eq!(output, format!("{}:alpha:10:beta:20", uuid()));
        }

        #[test]
        fn to_mysql_string_multi_range_with_tags() {
            // UUID with multi-range untagged and multi-range tagged
            let input = format!("{}:1-5:10-20:mytag:1-3:7-9", uuid());
            let set = GtidSet::parse(&input).expect("should parse");
            let output = set.to_mysql_string();
            assert_eq!(output, input);
        }

        #[test]
        fn iter_produces_one_entry_per_key_with_all_ranges() {
            // This property is critical for build_sid_block correctness: each key
            // must map to exactly one entry containing ALL of its ranges, so that
            // COM_BINLOG_DUMP_GTID sends one SID per (UUID, tag) with all intervals.
            let input = format!("{}:1-5:10-20:mytag:1-3:7-9,{}:1-100", uuid(), other_uuid());
            let set = GtidSet::parse(&input).expect("should parse");

            let entries: Vec<_> = set.iter().collect();
            // 3 keys: uuid:None, uuid:mytag, other_uuid:None
            assert_eq!(entries.len(), 3);

            // Each entry should have ALL ranges for its key
            let untagged = set
                .get(&GtidSource {
                    server_uuid: uuid(),
                    tag: None,
                })
                .expect("untagged key should exist");
            assert_eq!(untagged.len(), 2, "untagged should have 2 ranges");

            let tagged = set
                .get(&GtidSource {
                    server_uuid: uuid(),
                    tag: Some("mytag".into()),
                })
                .expect("tagged key should exist");
            assert_eq!(tagged.len(), 2, "tagged should have 2 ranges");
        }
    }
}
