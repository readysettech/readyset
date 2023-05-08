use std::convert::TryFrom;
use std::fmt;
use std::sync::Arc;

use bit_vec::BitVec;
use chrono::NaiveDateTime;
use mysql_time::MySqlTime;
use rust_decimal::Decimal;
use serde::de::{DeserializeSeed, EnumAccess, VariantAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use serde_bytes::{ByteBuf, Bytes};
use strum::VariantNames;
use strum_macros::{EnumString, EnumVariantNames, FromRepr};

use crate::{Array, Collation, DfValue, Text, TimestampTz, TinyText};

impl DfValue {
    /// Version number for the current implementations of [`serde::Deserialize`] and
    /// [`serde::Serialize`] for [`DfValue`]. Any data serialized with an older version *can not* be
    /// deserialized with the later version.
    ///
    /// This constant exists so that it can be persisted alongside any serialized [`DfValue`]s (eg
    /// in base tables) and checked on startup against the current number, to determine if we've
    /// made backwards-incompatible changes to deserialization since that data was serialized
    // Note for developers: Remember to increment this number, and run `cargo run --example
    // make_serialized_row`, every time we make a backwards incompatible change to deserialization
    // of DfValue! Hopefully `test::deserialize_backwards_compatibility` will automatically catch
    // that, but it's worth being extra careful, as that test is not perfect.
    pub const SERDE_VERSION: u8 = 1;

    /// Reference example "row" of `DfValue`s to check against for backwards compatible
    /// deserialization.
    ///
    /// This is only exported so it can be used by `examples/make_serialized_row.rs`
    pub fn example_row() -> Vec<DfValue> {
        vec![
            DfValue::None,
            DfValue::Int(0),
            DfValue::Int(i64::MAX),
            DfValue::Int(i64::MIN),
            DfValue::UnsignedInt(0),
            DfValue::UnsignedInt(u64::MAX),
            DfValue::Float(f32::MAX),
            DfValue::Float(0.0),
            DfValue::Float(f32::MIN),
            DfValue::Double(f64::MAX),
            DfValue::Double(0.0),
            DfValue::Double(f64::MIN),
            DfValue::Text("aaaaaaaaaaaaaaaaaa".into()),
            DfValue::TinyText(TinyText::from_slice(b"a").unwrap()),
            DfValue::TimestampTz("2023-12-16 17:44:00".parse().unwrap()),
            DfValue::Time(MySqlTime::from_bytes(b"1112").unwrap()),
            DfValue::ByteArray(Arc::new(b"aaaaaaaaaaaa".to_vec())),
            DfValue::Numeric(Arc::new(Decimal::MIN)),
            DfValue::Numeric(Arc::new(Decimal::MAX)),
            DfValue::BitVector(Arc::new(BitVec::from_bytes(b"aaaaaaaaa"))),
            DfValue::Array(Arc::new(Array::from(vec![DfValue::from("aaaaaaaaa")]))),
            DfValue::Max,
        ]
    }
}

#[derive(EnumVariantNames, EnumString, FromRepr, Clone, Copy)]
enum Variant {
    None,
    Int,
    Double,
    Text,
    Time,
    Float,
    ByteArray,
    Numeric,
    BitVector,
    TimestampTz,
    Array,
    Max,
}

enum TextOrTinyText {
    Text(Text),
    TinyText(TinyText),
}

/// Wrapper struct that allows serializing a reference to a `str` as if it were a text [`DfValue`]
pub struct TextRef<'a>(pub &'a str);

impl<'a> Serialize for TextRef<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serialize_variant(
            serializer,
            Variant::Text,
            &(Collation::default(), Bytes::new(self.0.as_bytes())),
        )
    }
}

#[inline(always)]
fn serialize_variant<S, T>(serializer: S, variant: Variant, value: &T) -> Result<S::Ok, S::Error>
where
    S: serde::ser::Serializer,
    T: ?Sized + serde::Serialize,
{
    // The compiler should be able to inline the constant name here, so no lookups are done
    // at runtime
    let variant_name = Variant::VARIANTS[variant as usize];
    serializer.serialize_newtype_variant("DfValue", variant as _, variant_name, value)
}

impl serde::ser::Serialize for DfValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        match &self {
            DfValue::None => serializer.serialize_unit_variant(
                "DfValue",
                Variant::None as _,
                Variant::VARIANTS[Variant::None as usize],
            ),
            DfValue::Int(v) => serialize_variant(serializer, Variant::Int, &i128::from(*v)),
            DfValue::UnsignedInt(v) => serialize_variant(serializer, Variant::Int, &i128::from(*v)),
            DfValue::Float(f) => serialize_variant(serializer, Variant::Float, &f.to_bits()),
            DfValue::Double(f) => serialize_variant(serializer, Variant::Double, &f.to_bits()),
            DfValue::Text(v) => serialize_variant(
                serializer,
                Variant::Text,
                &(v.collation(), Bytes::new(v.as_bytes())),
            ),
            DfValue::TinyText(v) => serialize_variant(
                serializer,
                Variant::Text,
                &(v.collation(), Bytes::new(v.as_bytes())),
            ),
            DfValue::Time(v) => serialize_variant(serializer, Variant::Time, &v),
            DfValue::ByteArray(a) => {
                serialize_variant(serializer, Variant::ByteArray, Bytes::new(a.as_ref()))
            }
            DfValue::Numeric(d) => serialize_variant(serializer, Variant::Numeric, &d),
            DfValue::BitVector(bits) => serialize_variant(serializer, Variant::BitVector, &bits),
            DfValue::TimestampTz(ts) => {
                let extra = ts.extra;
                let nt = ts.to_chrono();
                let ts = nt.naive_utc().timestamp() as u64 as u128
                    + ((nt.naive_utc().timestamp_subsec_nanos() as u128) << 64);
                serialize_variant(serializer, Variant::TimestampTz, &(ts, extra))
            }
            DfValue::Array(vs) => serialize_variant(serializer, Variant::Array, &vs),
            DfValue::PassThrough(v) => Err(serde::ser::Error::custom(format_args!(
                "PassThrough value of type {} not supported in dataflow graph",
                v.ty
            ))),
            DfValue::Max => serializer.serialize_unit_variant(
                "DfValue",
                Variant::Max as _,
                Variant::VARIANTS[Variant::Max as usize],
            ),
        }
    }
}

impl<'de> Deserialize<'de> for DfValue {
    fn deserialize<D>(deserializer: D) -> Result<DfValue, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct FieldVisitor;
        impl<'de> serde::de::Visitor<'de> for FieldVisitor {
            type Value = Variant;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("variant identifier")
            }

            fn visit_u64<E>(self, val: u64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                if let Some(f) = Variant::from_repr(val as _) {
                    Ok(f)
                } else {
                    Err(serde::de::Error::invalid_value(
                        serde::de::Unexpected::Unsigned(val),
                        &"variant index 0 <= i < 11",
                    ))
                }
            }

            fn visit_str<E>(self, val: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                val.parse()
                    .map_err(|_| serde::de::Error::unknown_variant(val, Variant::VARIANTS))
            }

            fn visit_bytes<E>(self, val: &[u8]) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match std::str::from_utf8(val).map(|s| s.parse()) {
                    Ok(Ok(field)) => Ok(field),
                    _ => Err(serde::de::Error::unknown_variant(
                        &String::from_utf8_lossy(val),
                        Variant::VARIANTS,
                    )),
                }
            }
        }

        impl<'de> serde::Deserialize<'de> for Variant {
            #[inline]
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                serde::Deserializer::deserialize_identifier(deserializer, FieldVisitor)
            }
        }

        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = DfValue;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("enum DfValue")
            }

            fn visit_enum<A>(self, data: A) -> Result<Self::Value, A::Error>
            where
                A: EnumAccess<'de>,
            {
                match EnumAccess::variant(data)? {
                    (Variant::None, variant) => {
                        VariantAccess::unit_variant(variant).map(|_| DfValue::None)
                    }
                    (Variant::Int, variant) => VariantAccess::newtype_variant::<i128>(variant)
                        .and_then(|x| {
                            DfValue::try_from(x).map_err(|_| {
                                serde::de::Error::invalid_value(
                                    serde::de::Unexpected::Other(x.to_string().as_str()),
                                    &"integer (i128)",
                                )
                            })
                        }),
                    (Variant::Float, variant) => VariantAccess::newtype_variant::<u32>(variant)
                        .map(|b| DfValue::Float(f32::from_bits(b))),
                    (Variant::Double, variant) => VariantAccess::newtype_variant::<u64>(variant)
                        .map(|b| DfValue::Double(f64::from_bits(b))),
                    (Variant::Numeric, variant) => {
                        VariantAccess::newtype_variant::<Decimal>(variant)
                            .map(|d| DfValue::Numeric(Arc::new(d)))
                    }
                    (Variant::Text, variant) => {
                        VariantAccess::newtype_variant::<TextOrTinyText>(variant).map(|tt| match tt
                        {
                            TextOrTinyText::TinyText(tt) => DfValue::TinyText(tt),
                            TextOrTinyText::Text(t) => DfValue::Text(t),
                        })
                    }
                    (Variant::Time, variant) => {
                        VariantAccess::newtype_variant::<MySqlTime>(variant).map(DfValue::Time)
                    }
                    (Variant::ByteArray, variant) => {
                        VariantAccess::newtype_variant::<ByteBuf>(variant)
                            .map(|v| DfValue::ByteArray(Arc::new(v.into_vec())))
                    }
                    (Variant::BitVector, variant) => {
                        VariantAccess::newtype_variant::<BitVec>(variant)
                            .map(|bits| DfValue::BitVector(Arc::new(bits)))
                    }
                    (Variant::TimestampTz, variant) => VariantAccess::newtype_variant::<(
                        u128,
                        [u8; 3],
                    )>(variant)
                    .map(|(ts, extra)| {
                        // We deserialize the NaiveDateTime by extracting nsecs from the top 64 bits
                        // of the encoded i128, and secs from the low 64 bits
                        let datetime = NaiveDateTime::from_timestamp(ts as _, (ts >> 64) as _);
                        DfValue::TimestampTz(TimestampTz { datetime, extra })
                    }),
                    (Variant::Array, variant) => {
                        VariantAccess::newtype_variant(variant).map(DfValue::Array)
                    }
                    (Variant::Max, variant) => {
                        VariantAccess::unit_variant(variant).map(|_| DfValue::Max)
                    }
                }
            }
        }

        deserializer.deserialize_enum("DfValue", Variant::VARIANTS, Visitor)
    }
}

impl<'de: 'a, 'a> Deserialize<'de> for TextOrTinyText {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // Once we grab the collation out of the first element, we need to pass that in *to the
        // construction* of the Text in the second element - this is the hoop we have to jump
        // through to make serde deserializers stateful
        struct FoundCollation(Collation);

        impl<'de> DeserializeSeed<'de> for FoundCollation {
            type Value = TextOrTinyText;

            fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
            where
                D: Deserializer<'de>,
            {
                struct TextVisitor(Collation);

                impl<'de> Visitor<'de> for TextVisitor {
                    type Value = TextOrTinyText;

                    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                        formatter.write_str("a byte array")
                    }

                    fn visit_seq<V>(self, mut visitor: V) -> Result<Self::Value, V::Error>
                    where
                        V: serde::de::SeqAccess<'de>,
                    {
                        let len = std::cmp::min(visitor.size_hint().unwrap_or(0), 4096);
                        let mut bytes = Vec::with_capacity(len);

                        while let Some(b) = visitor.next_element()? {
                            bytes.push(b);
                        }

                        match TinyText::from_slice(&bytes) {
                            Ok(mut tt) => {
                                tt.set_collation(self.0);
                                Ok(TextOrTinyText::TinyText(tt))
                            }
                            _ => Ok(TextOrTinyText::Text(Text::from_slice_with_collation(
                                &bytes, self.0,
                            ))),
                        }
                    }

                    fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
                    where
                        E: serde::de::Error,
                    {
                        match TinyText::from_slice(v) {
                            Ok(mut tt) => {
                                tt.set_collation(self.0);
                                Ok(TextOrTinyText::TinyText(tt))
                            }
                            _ => Ok(TextOrTinyText::Text(Text::from_slice_with_collation(
                                v, self.0,
                            ))),
                        }
                    }
                }

                deserializer.deserialize_bytes(TextVisitor(self.0))
            }
        }

        struct TupleVisitor;

        impl<'de> Visitor<'de> for TupleVisitor {
            type Value = TextOrTinyText;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a tuple of (collation, text)")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let collation = seq
                    .next_element::<Collation>()?
                    .ok_or_else(|| serde::de::Error::invalid_length(0, &self))?;

                seq.next_element_seed(FoundCollation(collation))?
                    .ok_or_else(|| serde::de::Error::invalid_length(1, &self))
            }
        }

        deserializer.deserialize_tuple(2, TupleVisitor)
    }
}

#[cfg(test)]
mod tests {
    use test_strategy::proptest;

    use super::*;

    /// This test checks that a reference payload of `bincode`-serialized `DfValue`s can be
    /// deserialized using the *current* implementation of [`serde::Deserialize`]. If this test
    /// fails, you've probably just introduced a backwards-incompatible change to deserialization.
    /// In that case, you have two options:
    ///
    /// 1. Fix the backwards incompatibility, if possible. Sometimes this is trivial, and if so,
    ///    this should be the preferred option
    /// 2. Bump the serde version for [`DfValue`]. Increment `DfValue::SERDE_VERSION` (at the top of
    ///    this file), and run `cargo run --example make_serialized_row` to overwrite the reference
    ///    serialized row for this serde version
    #[test]
    fn deserialize_backwards_compatibility() {
        assert_eq!(
            bincode::deserialize::<Vec<DfValue>>(include_bytes!("../tests/serialized-row.bincode"))
                .unwrap(),
            DfValue::example_row()
        );
    }

    #[proptest]
    fn text_serialize_bincode_round_trip(s: String, collation: Collation) {
        let input = DfValue::from_str_and_collation(&s, collation);
        let serialized = bincode::serialize(&input).unwrap();
        let rt = bincode::deserialize::<DfValue>(&serialized).unwrap();
        assert_eq!(
            <&str>::try_from(&rt).unwrap(),
            <&str>::try_from(&input).unwrap()
        );
        assert_eq!(rt.collation(), input.collation());
    }

    #[proptest]
    fn text_ref_serializes_same_as_text_value(s: String) {
        let text_ref = bincode::serialize(&TextRef(&s)).unwrap();
        let text_value = bincode::serialize(&DfValue::from(s)).unwrap();
        assert_eq!(text_ref, text_value);
    }

    #[proptest]
    fn serialize_round_trip(v: DfValue) {
        let serialized = bincode::serialize(&v).unwrap();
        let rt = bincode::deserialize::<DfValue>(&serialized).unwrap();
        assert_eq!(rt, v);
    }
}
