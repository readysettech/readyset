//! On-disk format versioning for persistent state.
//!
//! RocksDB keys are produced by a multi-step pipeline, and a change to *any* step produces
//! different on-disk bytes, making existing data unreadable. The pipeline is:
//!
//! 1. **`PointKey::from()`** — calls [`DfValue::normalize()`] on each value (e.g. normalizes
//!    `Decimal` representations).
//! 2. **`PointKey::serialize()`** — calls [`DfValue::transform_for_serialized_key()`] on each
//!    value (e.g. widens `Float` to `f64`, produces collation sort keys for text, normalizes
//!    `TimestampTz` via `normalize_for_key()`), then wraps multi-element keys in tuples.
//! 3. **`serialize_key()`** — wraps the serialized `PointKey` bytes with a length prefix and
//!    optional extra data (epoch + sequence number for non-unique keys).
//!
//! [`PERSISTENT_STATE_VERSION`] is stored in [`super::PersistentMeta`] alongside each RocksDB
//! database and checked at startup. If the persisted version does not match, the database is
//! deleted and re-snapshotted from upstream.

use std::borrow::Cow;
use std::sync::Arc;

use bit_vec::BitVec;
use mysql_time::MySqlTime;
use readyset_client::internal::{Index, IndexType};
use readyset_data::{Array, Collation, DfValue, TinyText};
use readyset_decimal::Decimal;
use replication_offset::mysql::MySqlPosition;
use replication_offset::postgres::PostgresPosition;
use replication_offset::{GtidSet, ReplicationOffset};

use super::{serialize_key, PersistentMeta, PointKey};

/// Version number for the on-disk persistent state format.
///
/// Bump this version when making ANY change that affects the on-disk byte representation of
/// stored data, including:
///
/// - Changes to `DfValue`'s `Serialize` or `Deserialize` impls (covers both row data and key
///   contents — row data is bincode-serialized `Vec<DfValue>` directly, keys go through the
///   pipeline below)
/// - Changes to [`DfValue::transform_for_serialized_key()`] (key normalization before
///   serialization)
/// - Changes to [`DfValue::normalize()`] (called when constructing [`PointKey`]s)
/// - Changes to [`PointKey`]'s `Serialize` impl (tuple/seq wrapping)
/// - Changes to [`serialize_key()`](super::serialize_key) (length-prefix wrapping, extra data
///   encoding)
pub(super) const PERSISTENT_STATE_VERSION: u8 = 7;

/// Returns labeled single-element `DfValue`s exercising each normalization path in the key
/// serialization pipeline.
///
/// Each entry is `(human_label, value)`. The label is used in test failure messages to
/// identify which key diverged. Sentinel variants (`Default`, `Max`, `PassThrough`) are
/// excluded: although `Default` and `Max` have working unit-variant `Serialize` impls,
/// they are not user-data and never appear in persisted base-table rows or keys.
/// `Default` is replaced before reaching the storage layer; `Max` is a transient
/// upper-bound marker for range scans; `PassThrough` carries an opaque type byte stream
/// that's never persisted as a key.
fn example_single_values() -> Vec<(&'static str, DfValue)> {
    vec![
        ("None", DfValue::None),
        ("Int(0)", DfValue::Int(0)),
        ("Int(MAX)", DfValue::Int(i64::MAX)),
        ("Int(MIN)", DfValue::Int(i64::MIN)),
        ("UnsignedInt(0)", DfValue::UnsignedInt(0)),
        ("UnsignedInt(MAX)", DfValue::UnsignedInt(u64::MAX)),
        ("Float(MAX)", DfValue::Float(f32::MAX)),
        ("Float(0)", DfValue::Float(0.0)),
        ("Float(MIN)", DfValue::Float(f32::MIN)),
        ("Double(MAX)", DfValue::Double(f64::MAX)),
        ("Double(0)", DfValue::Double(0.0)),
        ("Double(MIN)", DfValue::Double(f64::MIN)),
        ("Text", DfValue::Text("aaaaaaaaaaaaaaaaaa".into())),
        (
            "TinyText",
            DfValue::TinyText(TinyText::from_slice(b"a", Collation::Utf8).expect("valid tinytext")),
        ),
        (
            "TimestampTz",
            DfValue::TimestampTz("2023-12-16 17:44:00".parse().expect("valid timestamp")),
        ),
        (
            "ByteArray",
            DfValue::ByteArray(Arc::new(b"aaaaaaaaaaaa".to_vec())),
        ),
        ("Numeric(MIN)", DfValue::Numeric(Arc::new(Decimal::MIN))),
        ("Numeric(MAX)", DfValue::Numeric(Arc::new(Decimal::MAX))),
        (
            "Numeric(42.42)",
            DfValue::Numeric(Arc::new(Decimal::try_from(42.42).expect("valid decimal"))),
        ),
        (
            "Time",
            DfValue::Time(MySqlTime::from_bytes(b"1112").expect("valid time")),
        ),
        (
            "BitVector",
            DfValue::BitVector(Arc::new(BitVec::from_bytes(b"aaaaaaaaa"))),
        ),
        (
            "Array",
            DfValue::Array(Arc::new(Array::from(vec![DfValue::from("aaaaaaaaa")]))),
        ),
    ]
}

/// Builds a labeled set of serialized RocksDB keys exercising every normalization path in
/// the key serialization pipeline described in the [module docs](self).
///
/// Each entry is `(human_label, key_bytes)`. The label appears only in test failure
/// messages — it is *not* persisted in the golden file. Co-locating label and bytes in a
/// single function eliminates the parallel-`Vec` synchronization hazard.
///
/// Used by the `key_serialization_backwards_compatibility` test and the
/// `make_serialized_keys` example to produce/verify a golden reference file.
///
/// Each `DfValue` is run through the full pipeline:
///
/// 1. `PointKey::from()` — calls `DfValue::normalize()` (e.g. `Decimal` normalization)
/// 2. `PointKey::serialize()` — calls `DfValue::transform_for_serialized_key()` (e.g. float
///    widening, collation sort keys, `TimestampTz::normalize_for_key()`)
/// 3. `serialize_key()` — length-prefixed bincode with optional extra data
///
/// A change to any of these steps will produce different bytes, causing the golden-file test to
/// fail.
pub fn example_serialized_keys() -> Vec<(&'static str, Vec<u8>)> {
    let single_values = example_single_values();

    let mut keys: Vec<(&'static str, Vec<u8>)> = Vec::new();

    // Empty key (exercises PointKey::Empty -> serialize_unit())
    keys.push(("Empty", serialize_key(&PointKey::Empty, ())));

    // Single-element keys exercising each normalization path:
    //   - Float: widened to f64 by transform_for_serialized_key
    //   - Text/TinyText: collation sort key by transform_for_serialized_key
    //   - TimestampTz: normalize_for_key by transform_for_serialized_key
    //   - Numeric: Decimal::normalize by PointKey::from -> DfValue::normalize
    for (label, v) in &single_values {
        let pk = PointKey::from(std::iter::once(v.clone()));
        keys.push((label, serialize_key(&pk, ())));
    }

    // Multi-element keys exercising each PointKey tuple variant.
    // Double (2 elements)
    let double = PointKey::from(vec![DfValue::from(42_i64), DfValue::from("hello")]);
    keys.push(("Double(2-element)", serialize_key(&double, ())));

    // Tri (3 elements)
    let triple = PointKey::from(vec![
        DfValue::Float(1.5),
        DfValue::TimestampTz("2024-01-01 00:00:00".parse().expect("valid timestamp")),
        DfValue::Numeric(Arc::new(Decimal::try_from(99.99).expect("valid decimal"))),
    ]);
    keys.push(("Tri(3-element)", serialize_key(&triple, ())));

    // Quad (4 elements)
    let quad = PointKey::from(vec![
        DfValue::from(1_i64),
        DfValue::from(2_i64),
        DfValue::from(3_i64),
        DfValue::from(4_i64),
    ]);
    keys.push(("Quad(4-element)", serialize_key(&quad, ())));

    // Quin (5 elements)
    let quin = PointKey::from(vec![
        DfValue::from(1_i64),
        DfValue::from(2_i64),
        DfValue::from(3_i64),
        DfValue::from(4_i64),
        DfValue::from(5_i64),
    ]);
    keys.push(("Quin(5-element)", serialize_key(&quin, ())));

    // Sex (6 elements — last tuple variant before Multi/SerializeSeq at 7)
    let sex = PointKey::from(vec![
        DfValue::from(1_i64),
        DfValue::from(2_i64),
        DfValue::from(3_i64),
        DfValue::from(4_i64),
        DfValue::from(5_i64),
        DfValue::from(6_i64),
    ]);
    keys.push(("Sex(6-element)", serialize_key(&sex, ())));

    // Multi (7+ elements, exercises SerializeSeq path in PointKey::serialize)
    let multi = PointKey::from(vec![
        DfValue::from(1_i64),
        DfValue::from(2_i64),
        DfValue::from(3_i64),
        DfValue::from(4_i64),
        DfValue::from(5_i64),
        DfValue::from(6_i64),
        DfValue::from(7_i64),
    ]);
    keys.push(("Multi(7-element)", serialize_key(&multi, ())));

    // Key with extra data (simulates non-unique primary key with epoch+seq)
    let pk_with_extra = PointKey::from(std::iter::once(DfValue::from(100_i64)));
    keys.push((
        "Single+extra(epoch+seq)",
        serialize_key(&pk_with_extra, (1u64, 2u64)),
    ));

    keys
}

/// Builds an example "row" of `DfValue`s exercising every serializable variant. Row data in
/// RocksDB is stored as `bincode`-serialized `Vec<DfValue>` directly — *not* through the
/// key pipeline — so this covers a different code path than [`example_serialized_keys`].
///
/// Reuses [`example_single_values`] so that adding a new `DfValue` variant covers both the
/// key path and the row path automatically.
///
/// Used by the `row_serialization_backwards_compatibility` test and the
/// `make_serialized_row` example to produce/verify the reference file.
pub fn example_serialized_row() -> Vec<DfValue> {
    example_single_values()
        .into_iter()
        .map(|(_, v)| v)
        .collect()
}

/// Builds a set of [`PersistentMeta`] instances covering every [`ReplicationOffset`] variant
/// and both [`IndexType`]s. Serialized to JSON for comparison against a golden reference file.
///
/// Used by the `meta_serialization_backwards_compatibility` test and the
/// `make_serialized_meta` example to produce/verify the reference file.
pub fn example_serialized_metas() -> serde_json::Value {
    let metas: Vec<PersistentMeta<'static>> = vec![
        // No replication offset
        PersistentMeta {
            persistent_state_version: PERSISTENT_STATE_VERSION,
            indices: vec![Index::new(IndexType::HashMap, vec![0])],
            epoch: 1,
            replication_offset: None,
        },
        // MySQL binlog-position offset
        PersistentMeta {
            persistent_state_version: PERSISTENT_STATE_VERSION,
            indices: vec![
                Index::new(IndexType::HashMap, vec![0, 1]),
                Index::new(IndexType::BTreeMap, vec![2]),
            ],
            epoch: 42,
            replication_offset: Some(Cow::Owned(ReplicationOffset::MySql(
                MySqlPosition::from_file_name_and_position("binlog.000001".to_owned(), 154)
                    .expect("valid mysql position"),
            ))),
        },
        // Postgres LSN offset
        PersistentMeta {
            persistent_state_version: PERSISTENT_STATE_VERSION,
            indices: vec![Index::new(IndexType::HashMap, vec![0])],
            epoch: 100,
            replication_offset: Some(Cow::Owned(ReplicationOffset::Postgres(PostgresPosition {
                commit_lsn: 12345.into(),
                lsn: 6789.into(),
            }))),
        },
        // MySQL GTID offset (exercises BTreeMap, GtidSource custom serde, GtidRange)
        PersistentMeta {
            persistent_state_version: PERSISTENT_STATE_VERSION,
            indices: vec![Index::new(IndexType::BTreeMap, vec![0, 1, 2])],
            epoch: 200,
            replication_offset: Some(Cow::Owned(ReplicationOffset::Gtid(
                GtidSet::parse("3E11FA47-71CA-11E1-9E33-C80AA9429562:1-10")
                    .expect("valid gtid set"),
            ))),
        },
    ];

    serde_json::to_value(&metas).expect("failed to serialize example metas")
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use bincode::Options;
    use pretty_assertions::assert_eq;
    use readyset_data::DfValueKind;

    use super::*;

    /// Returns one canonical DfValue per serializable variant, alongside an exhaustive
    /// match that forces a compiler error when DfValue gains a variant.
    ///
    /// When a new variant is added: classify it in the match as `true` (serializable,
    /// stored as RocksDB keys) or `false` (sentinel). If `true`, add a representative
    /// to the list AND add entries to [`example_single_values()`].
    fn serializable_representatives() -> Vec<DfValue> {
        // Intentionally NOT using matches!() — the exhaustive match forces a compiler
        // error when DfValue gains a variant, ensuring this function stays current.
        #[allow(clippy::match_like_matches_macro)]
        fn is_serializable(v: &DfValue) -> bool {
            match v {
                DfValue::None
                | DfValue::Int(_)
                | DfValue::UnsignedInt(_)
                | DfValue::Float(_)
                | DfValue::Double(_)
                | DfValue::Text(_)
                | DfValue::TinyText(_)
                | DfValue::TimestampTz(_)
                | DfValue::Time(_)
                | DfValue::ByteArray(_)
                | DfValue::Numeric(_)
                | DfValue::BitVector(_)
                | DfValue::Array(_) => true,
                // Sentinels: never stored as RocksDB keys
                DfValue::PassThrough(_) | DfValue::Default | DfValue::Max => false,
            }
        }

        let reps = vec![
            DfValue::None,
            DfValue::Int(0),
            DfValue::UnsignedInt(0),
            DfValue::Float(0.0),
            DfValue::Double(0.0),
            DfValue::Text("".into()),
            DfValue::TinyText(TinyText::from_slice(b"", Collation::Utf8).expect("valid tinytext")),
            DfValue::TimestampTz("2023-01-01 00:00:00".parse().expect("valid timestamp")),
            DfValue::Time(MySqlTime::from_bytes(b"0000").expect("valid time")),
            DfValue::ByteArray(Arc::new(vec![])),
            DfValue::Numeric(Arc::new(Decimal::from(0))),
            DfValue::BitVector(Arc::new(BitVec::new())),
            DfValue::Array(Arc::new(Array::from(vec![]))),
        ];

        for v in &reps {
            assert!(is_serializable(v), "{v:?} classified as sentinel");
        }

        reps
    }

    /// Checks that [`example_single_values()`] covers every serializable DfValue variant.
    /// If this fails, you added a new DfValue variant without adding it to the golden-file
    /// inputs.
    #[test]
    fn example_keys_cover_all_serializable_variants() {
        let required: HashSet<DfValueKind> = serializable_representatives()
            .iter()
            .map(DfValueKind::from)
            .collect();

        let covered: HashSet<DfValueKind> = example_single_values()
            .iter()
            .map(|(_, v)| DfValueKind::from(v))
            .collect();

        let missing: Vec<_> = required.difference(&covered).collect();
        assert!(
            missing.is_empty(),
            "example_serialized_keys() is missing variants: {missing:?}"
        );
    }

    /// Checks that the full key serialization pipeline produces the same bytes as the reference
    /// file. If this test fails, you have made a backwards-incompatible change to the on-disk
    /// key format. In that case:
    ///
    /// 1. Bump [`PERSISTENT_STATE_VERSION`]
    /// 2. Run `cargo run -p dataflow-state --example make_serialized_keys`
    ///    to regenerate the reference file
    #[test]
    fn key_serialization_backwards_compatibility() {
        let current = example_serialized_keys();
        let reference: Vec<Vec<u8>> = bincode::options()
            .deserialize(include_bytes!("../../tests/serialized-keys.bincode"))
            .expect("failed to deserialize reference keys");
        assert_eq!(
            reference.len(),
            current.len(),
            "number of example keys changed (expected {}, got {}); regenerate the reference file",
            reference.len(),
            current.len(),
        );
        for (i, ((label, cur), refr)) in current.iter().zip(reference.iter()).enumerate() {
            assert_eq!(refr, cur, "key {i} ({label}) differs from reference");
        }
    }

    /// Checks that the bincode-serialized golden row deserializes back to the same
    /// `Vec<DfValue>` produced by [`example_serialized_row`]. Row data in RocksDB is stored
    /// as bincode `Vec<DfValue>` directly (not through the key pipeline), so this catches
    /// `DfValue::Deserialize` regressions that the key-format test would miss. If this test
    /// fails, you have made a backwards-incompatible change to `DfValue` deserialization. In
    /// that case:
    ///
    /// 1. Bump [`PERSISTENT_STATE_VERSION`]
    /// 2. Run `cargo run -p dataflow-state --example make_serialized_row`
    ///    to regenerate the reference file
    #[test]
    fn row_serialization_backwards_compatibility() {
        let current = example_serialized_row();
        let reference: Vec<DfValue> = bincode::options()
            .deserialize(include_bytes!("../../tests/serialized-row.bincode"))
            .expect("failed to deserialize reference row");
        assert_eq!(
            reference.len(),
            current.len(),
            "number of example values changed (expected {}, got {}); regenerate the reference file",
            reference.len(),
            current.len(),
        );
        for (i, ((label, expected), got)) in example_single_values()
            .iter()
            .zip(reference.iter())
            .enumerate()
        {
            assert_eq!(expected, got, "row[{i}] ({label}) deserialized differently");
        }
    }

    /// Checks that PersistentMeta JSON serialization (including ReplicationOffset, Index, and
    /// epoch) matches the golden reference file. If this test fails, someone changed the
    /// serialization format of a type stored in PersistentMeta. In that case:
    ///
    /// 1. Bump [`PERSISTENT_STATE_VERSION`]
    /// 2. Run `cargo run -p dataflow-state --example make_serialized_meta`
    ///    to regenerate the reference file
    #[test]
    fn meta_serialization_backwards_compatibility() {
        let current = example_serialized_metas();
        let reference: serde_json::Value =
            serde_json::from_str(include_str!("../../tests/serialized-meta.json"))
                .expect("failed to parse reference meta JSON");
        assert_eq!(current, reference, "PersistentMeta serialization changed");
    }

    /// Checks that the golden reference file can be deserialized back into PersistentMeta
    /// instances with correct field values. This catches changes that would break reading
    /// existing RocksDB metadata, including silent field defaulting.
    #[test]
    fn meta_deserialization_backwards_compatibility() {
        let reference: Vec<PersistentMeta<'static>> =
            serde_json::from_str(include_str!("../../tests/serialized-meta.json"))
                .expect("failed to deserialize reference meta JSON into PersistentMeta");
        assert_eq!(reference.len(), 4, "expected 4 example metas in reference");

        // Every example meta is written with the current PERSISTENT_STATE_VERSION; if this
        // assertion fails it means the version field silently defaulted or the on-disk
        // representation drifted from what the producer wrote.
        for (i, meta) in reference.iter().enumerate() {
            assert_eq!(
                meta.persistent_state_version, PERSISTENT_STATE_VERSION,
                "reference[{i}] version field did not round-trip"
            );
        }

        // [0]: no replication offset, single HashMap index on column 0
        assert_eq!(reference[0].epoch, 1);
        assert!(reference[0].replication_offset.is_none());
        assert_eq!(
            reference[0].indices,
            vec![Index::new(IndexType::HashMap, vec![0])]
        );

        // [1]: MySQL binlog offset at "binlog.000001":154, two indices (HashMap + BTreeMap)
        assert_eq!(reference[1].epoch, 42);
        assert_eq!(
            reference[1].indices,
            vec![
                Index::new(IndexType::HashMap, vec![0, 1]),
                Index::new(IndexType::BTreeMap, vec![2]),
            ]
        );
        match reference[1].replication_offset.as_deref() {
            Some(ReplicationOffset::MySql(pos)) => {
                let expected =
                    MySqlPosition::from_file_name_and_position("binlog.000001".to_owned(), 154)
                        .expect("valid mysql position");
                assert_eq!(pos, &expected);
            }
            other => panic!("expected MySql offset, got {other:?}"),
        }

        // [2]: Postgres LSN offset at commit_lsn=12345, lsn=6789
        assert_eq!(reference[2].epoch, 100);
        assert_eq!(
            reference[2].indices,
            vec![Index::new(IndexType::HashMap, vec![0])]
        );
        match reference[2].replication_offset.as_deref() {
            Some(ReplicationOffset::Postgres(pos)) => {
                assert_eq!(
                    pos,
                    &PostgresPosition {
                        commit_lsn: 12345.into(),
                        lsn: 6789.into(),
                    }
                );
            }
            other => panic!("expected Postgres offset, got {other:?}"),
        }

        // [3]: MySQL GTID offset, single BTreeMap index on three columns
        assert_eq!(reference[3].epoch, 200);
        assert_eq!(
            reference[3].indices,
            vec![Index::new(IndexType::BTreeMap, vec![0, 1, 2])]
        );
        match reference[3].replication_offset.as_deref() {
            Some(ReplicationOffset::Gtid(gtid)) => {
                let expected = GtidSet::parse("3E11FA47-71CA-11E1-9E33-C80AA9429562:1-10")
                    .expect("valid gtid set");
                assert_eq!(gtid, &expected);
            }
            other => panic!("expected Gtid offset, got {other:?}"),
        }
    }

    /// PersistentMeta uses `#[serde(alias = "serde_version")]` so existing RocksDB
    /// metadata written under the old field name still deserializes after the rename.
    /// Verifies the migration path: read with the old name, write with the new name.
    #[test]
    fn meta_alias_accepts_legacy_field_name() {
        let legacy_json = r#"{
            "serde_version": 6,
            "indices": [{"index_type": "HashMap", "columns": [0]}],
            "epoch": 7,
            "replication_offset": null
        }"#;
        let meta: PersistentMeta<'static> =
            serde_json::from_str(legacy_json).expect("legacy serde_version field name accepted");
        assert_eq!(meta.epoch, 7);
        assert!(meta.replication_offset.is_none());
    }
}
