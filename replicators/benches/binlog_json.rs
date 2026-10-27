//! Measures the conversion replication applies to each JSON value in a MySQL binlog row event.
//!
//! Run with `cargo bench -p replicators --features bench --bench binlog_json`.

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use mysql_common::binlog::jsonb;
use mysql_common::io::ParseBuf;
use replicators::bench_binlog_json_to_df_value;
use serde_json::{json, Value};

const OBJECT: u8 = 0x00;
const ARRAY: u8 = 0x02;
const LITERAL: u8 = 0x04;
const INT16: u8 = 0x05;
const INT64: u8 = 0x09;
const DOUBLE: u8 = 0x0b;
const STRING: u8 = 0x0c;

/// Encode `json` in MySQL's binary JSON format, using small (16-bit offset) containers only.
fn encode(json: &Value) -> Vec<u8> {
    let mut out = vec![type_of(json)];
    encode_payload(json, &mut out);
    out
}

fn type_of(json: &Value) -> u8 {
    match json {
        Value::Object(_) => OBJECT,
        Value::Array(_) => ARRAY,
        Value::Null | Value::Bool(_) => LITERAL,
        Value::Number(n) if n.is_f64() => DOUBLE,
        Value::Number(n) if n.as_i64().is_some_and(|i| i16::try_from(i).is_ok()) => INT16,
        Value::Number(_) => INT64,
        Value::String(_) => STRING,
    }
}

/// The value stored in a container entry in place of an offset, for types small enough to inline.
fn inlined(json: &Value) -> Option<u16> {
    match json {
        Value::Null => Some(0),
        Value::Bool(true) => Some(1),
        Value::Bool(false) => Some(2),
        Value::Number(_) if type_of(json) == INT16 => {
            Some(json.as_i64().expect("int16 value") as i16 as u16)
        }
        _ => None,
    }
}

fn encode_payload(json: &Value, out: &mut Vec<u8>) {
    match json {
        Value::Object(map) => {
            // MySQL stores keys sorted by length, then bytewise.
            let mut fields = map.iter().collect::<Vec<_>>();
            fields.sort_by(|(a, _), (b, _)| a.len().cmp(&b.len()).then(a.cmp(b)));
            let keys = fields.iter().map(|(k, _)| k.as_str()).collect::<Vec<_>>();
            let values = fields.iter().map(|(_, v)| *v).collect::<Vec<_>>();
            encode_container(&keys, &values, out);
        }
        Value::Array(array) => encode_container(&[], &array.iter().collect::<Vec<_>>(), out),
        Value::Number(n) if n.is_f64() => {
            out.extend(n.as_f64().expect("double value").to_le_bytes())
        }
        Value::Number(n) => out.extend(n.as_i64().expect("int64 value").to_le_bytes()),
        Value::String(s) => {
            let mut len = s.len();
            loop {
                let byte = (len & 0x7f) as u8;
                len >>= 7;
                out.push(if len > 0 { byte | 0x80 } else { byte });
                if len == 0 {
                    break;
                }
            }
            out.extend(s.as_bytes());
        }
        Value::Null | Value::Bool(_) => unreachable!("literals are always inlined"),
    }
}

/// Write an object (`keys` non-empty) or array: element count, total size, key entries, value
/// entries, then keys and values. Offsets are relative to the start of the container.
fn encode_container(keys: &[&str], values: &[&Value], out: &mut Vec<u8>) {
    let start = out.len();
    let u16_at = |out: &mut Vec<u8>, pos: usize, v: usize| {
        out[pos..pos + 2].copy_from_slice(&u16::try_from(v).expect("small container").to_le_bytes())
    };
    let value_entries = start + 4 + keys.len() * 4;
    out.resize(value_entries + values.len() * 3, 0);
    u16_at(out, start, values.len());

    for (i, key) in keys.iter().enumerate() {
        let offset = out.len() - start;
        u16_at(out, start + 4 + i * 4, offset);
        u16_at(out, start + 6 + i * 4, key.len());
        out.extend(key.as_bytes());
    }
    for (i, value) in values.iter().enumerate() {
        let entry = value_entries + i * 3;
        out[entry] = type_of(value);
        match inlined(value) {
            Some(inline) => u16_at(out, entry + 1, inline as usize),
            None => {
                let offset = out.len() - start;
                u16_at(out, entry + 1, offset);
                encode_payload(value, out);
            }
        }
    }
    let size = out.len() - start;
    u16_at(out, start + 2, size);
}

fn documents() -> Vec<(&'static str, Value)> {
    let mixed = json!({
        "id": 12345, "active": true, "deleted": false, "parent": null,
        "name": "Jane Doe", "email": "jane@example.com", "country": "BR",
        "plan": "enterprise", "status": "active", "currency": "USD",
        "created_at": "2026-10-27 12:00:00", "notes": "line\nwith \"quotes\"",
        "count": 42, "views": 9876543210u64, "rank": -7, "age": 30, "score": 98,
        "balance": 1234.56, "ratio": 0.0000001, "big": 1e15, "huge": 1e30, "rate": 1.5,
        "tags": ["red", "green", "blue", "yellow", "black"],
        "points": [1.25, 2.5, 3.75, 100.0, 0.1],
        "address": {
            "street": "Av. Paulista", "number": 1000, "city": "Sao Paulo",
            "zip": "01310-100", "lat": -23.5613, "lng": -46.6565,
        },
        "limits": {"daily": 100, "monthly": 3000, "burst": 2.5e-3, "enabled": true},
    });
    let doubles = Value::Array(
        (0..50)
            .map(|i| json!(f64::from(i) * 1.37e-3 + 10f64.powi(i % 20 - 8)))
            .collect(),
    );
    let strings = Value::Object(
        (0..50)
            .map(|i| (format!("field_{i}"), json!(format!("value number {i}"))))
            .collect(),
    );
    vec![("mixed", mixed), ("doubles", doubles), ("strings", strings)]
}

fn bench_binlog_json(c: &mut Criterion) {
    let mut group = c.benchmark_group("binlog_json_to_df_value");
    for (name, json) in documents() {
        let bytes = encode(&json);
        let value: jsonb::Value = ParseBuf(&bytes).parse(()).expect("valid binary JSON");
        group.bench_function(name, |b| {
            b.iter(|| bench_binlog_json_to_df_value(black_box(&value)).expect("conversion"))
        });
    }
    group.finish();
}

criterion_group!(benches, bench_binlog_json);
criterion_main!(benches);
