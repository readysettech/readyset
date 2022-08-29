use std::convert::TryFrom;
use std::fmt;
use std::sync::Arc;

use bit_vec::BitVec;
use chrono::NaiveDateTime;
use mysql_time::MysqlTime;
use rust_decimal::Decimal;
use serde::de::{EnumAccess, VariantAccess, Visitor};
use serde::{Deserialize, Deserializer};
use serde_bytes::{ByteBuf, Bytes};
use strum::VariantNames;
use strum_macros::{EnumString, EnumVariantNames, FromRepr};

use crate::{DfValue, Text, TimestampTz, TinyText};

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
            DfValue::Text(v) => {
                serialize_variant(serializer, Variant::Text, Bytes::new(v.as_bytes()))
            }
            DfValue::TinyText(v) => {
                serialize_variant(serializer, Variant::Text, Bytes::new(v.as_bytes()))
            }
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
            DfValue::PassThrough(_) => Err(serde::ser::Error::custom(
                "PassThrough not supported in dataflow graph",
            )),
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
                                    serde::de::Unexpected::Other(format!("{}", x).as_str()),
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
                        VariantAccess::newtype_variant::<MysqlTime>(variant).map(DfValue::Time)
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
        struct TextVisitor;

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
                    Ok(tt) => Ok(TextOrTinyText::TinyText(tt)),
                    _ => Ok(TextOrTinyText::Text(Text::from_slice_unchecked(&bytes))),
                }
            }

            fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match TinyText::from_slice(v) {
                    Ok(tt) => Ok(TextOrTinyText::TinyText(tt)),
                    _ => Ok(TextOrTinyText::Text(Text::from_slice_unchecked(v))),
                }
            }
        }

        deserializer.deserialize_bytes(TextVisitor)
    }
}
