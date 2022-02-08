use std::convert::TryFrom;
use std::fmt;
use std::sync::Arc;

use bit_vec::BitVec;
use chrono::NaiveDateTime;
use mysql_time::MysqlTime;
use rust_decimal::Decimal;
use serde::de::{EnumAccess, VariantAccess, Visitor};
use serde::ser::SerializeTupleVariant;
use serde::{Deserialize, Deserializer};
use serde_bytes::{ByteBuf, Bytes};
use strum::VariantNames;
use strum_macros::{EnumString, EnumVariantNames, FromRepr};

use crate::{DataType, Text, TimestampTz, TinyText};

#[derive(EnumVariantNames, EnumString, FromRepr, Clone, Copy)]
enum Field {
    None,
    Int,
    Double,
    Text,
    Timestamp,
    Time,
    Float,
    ByteArray,
    Numeric,
    BitVector,
    TimestampTz,
    Max,
}

enum TextOrTinyText {
    Text(Text),
    TinyText(TinyText),
}

#[inline(always)]
fn serialize_variant<S, T>(serializer: S, variant: Field, value: &T) -> Result<S::Ok, S::Error>
where
    S: serde::ser::Serializer,
    T: ?Sized + serde::Serialize,
{
    // The compiler should be able to inline the constant name here, so no lookups are done at
    // runtime
    let variant_name = Field::VARIANTS[variant as usize];
    serializer.serialize_newtype_variant("DataType", variant as _, variant_name, value)
}

#[inline(always)]
fn serialize_tuple<S>(
    serializer: S,
    variant: Field,
    len: usize,
) -> Result<S::SerializeTupleVariant, S::Error>
where
    S: serde::ser::Serializer,
{
    // The compiler should be able to inline the constant name here, so no lookups are done at
    // runtime
    let variant_name = Field::VARIANTS[variant as usize];
    serializer.serialize_tuple_variant("DataType", variant as _, variant_name, len)
}

impl serde::ser::Serialize for DataType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        match &self {
            DataType::None => serializer.serialize_unit_variant(
                "DataType",
                Field::None as _,
                Field::VARIANTS[Field::None as usize],
            ),
            DataType::Int(v) => serialize_variant(serializer, Field::Int, &i128::from(*v)),
            DataType::UnsignedInt(v) => serialize_variant(serializer, Field::Int, &i128::from(*v)),
            DataType::Float(f, prec) => {
                let mut tv = serialize_tuple(serializer, Field::Float, 2)?;
                tv.serialize_field(f)?;
                tv.serialize_field(prec)?;
                tv.end()
            }
            DataType::Double(f, prec) => {
                let mut tv = serialize_tuple(serializer, Field::Double, 2)?;
                tv.serialize_field(f)?;
                tv.serialize_field(prec)?;
                tv.end()
            }
            DataType::Text(v) => {
                serialize_variant(serializer, Field::Text, Bytes::new(v.as_bytes()))
            }
            DataType::TinyText(v) => {
                serialize_variant(serializer, Field::Text, Bytes::new(v.as_bytes()))
            }
            DataType::Time(v) => serialize_variant(serializer, Field::Time, &v),
            DataType::ByteArray(a) => {
                serialize_variant(serializer, Field::ByteArray, Bytes::new(a.as_ref()))
            }
            DataType::Numeric(d) => serialize_variant(serializer, Field::Numeric, &d),
            DataType::BitVector(bits) => serialize_variant(serializer, Field::BitVector, &bits),
            DataType::TimestampTz(ts) => {
                let extra = ts.extra;
                let nt = ts.to_chrono();
                let ts = nt.naive_utc().timestamp() as u64 as u128
                    + ((nt.naive_utc().timestamp_subsec_nanos() as u128) << 64);
                serialize_variant(serializer, Field::TimestampTz, &(ts, extra))
            }
            DataType::Max => serializer.serialize_unit_variant(
                "DataType",
                Field::Max as _,
                Field::VARIANTS[Field::Max as usize],
            ),
        }
    }
}

impl<'de> Deserialize<'de> for DataType {
    fn deserialize<D>(deserializer: D) -> Result<DataType, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct FieldVisitor;
        impl<'de> serde::de::Visitor<'de> for FieldVisitor {
            type Value = Field;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("variant identifier")
            }

            fn visit_u64<E>(self, val: u64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                if let Some(f) = Field::from_repr(val as _) {
                    Ok(f)
                } else {
                    Err(serde::de::Error::invalid_value(
                        serde::de::Unexpected::Unsigned(val),
                        &"variant index 0 <= i < 12",
                    ))
                }
            }

            fn visit_str<E>(self, val: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                val.parse()
                    .map_err(|_| serde::de::Error::unknown_variant(val, Field::VARIANTS))
            }

            fn visit_bytes<E>(self, val: &[u8]) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match std::str::from_utf8(val).map(|s| s.parse()) {
                    Ok(Ok(field)) => Ok(field),
                    _ => Err(serde::de::Error::unknown_variant(
                        &String::from_utf8_lossy(val),
                        Field::VARIANTS,
                    )),
                }
            }
        }

        impl<'de> serde::Deserialize<'de> for Field {
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
            type Value = DataType;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("enum DataType")
            }

            fn visit_enum<A>(self, data: A) -> Result<Self::Value, A::Error>
            where
                A: EnumAccess<'de>,
            {
                match EnumAccess::variant(data)? {
                    (Field::None, variant) => {
                        VariantAccess::unit_variant(variant).map(|_| DataType::None)
                    }
                    (Field::Int, variant) => VariantAccess::newtype_variant::<i128>(variant)
                        .and_then(|x| {
                            DataType::try_from(x).map_err(|_| {
                                serde::de::Error::invalid_value(
                                    serde::de::Unexpected::Other(format!("{}", x).as_str()),
                                    &"integer (i128)",
                                )
                            })
                        }),
                    (Field::Float, variant) => {
                        struct Visitor;
                        impl<'de> serde::de::Visitor<'de> for Visitor {
                            type Value = DataType;
                            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                                fmt::Formatter::write_str(
                                    formatter,
                                    "tuple variant DataType::Float",
                                )
                            }
                            #[inline]
                            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
                            where
                                A: serde::de::SeqAccess<'de>,
                            {
                                let f = seq.next_element()?.ok_or_else(|| {
                                    serde::de::Error::invalid_length(
                                        0usize,
                                        &"tuple variant DataType::Float with 2 elements",
                                    )
                                })?;
                                let prec = seq.next_element()?.ok_or_else(|| {
                                    serde::de::Error::invalid_length(
                                        0usize,
                                        &"tuple variant DataType::Float with 2 elements",
                                    )
                                })?;
                                Ok(DataType::Float(f, prec))
                            }
                        }
                        VariantAccess::tuple_variant(variant, 4usize, Visitor)
                    }
                    (Field::Double, variant) => {
                        struct Visitor;
                        impl<'de> serde::de::Visitor<'de> for Visitor {
                            type Value = DataType;
                            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                                fmt::Formatter::write_str(
                                    formatter,
                                    "tuple variant DataType::Double",
                                )
                            }
                            #[inline]
                            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
                            where
                                A: serde::de::SeqAccess<'de>,
                            {
                                let f = seq.next_element()?.ok_or_else(|| {
                                    serde::de::Error::invalid_length(
                                        0usize,
                                        &"tuple variant DataType::Double with 2 elements",
                                    )
                                })?;
                                let prec = seq.next_element()?.ok_or_else(|| {
                                    serde::de::Error::invalid_length(
                                        0usize,
                                        &"tuple variant DataType::Double with 2 elements",
                                    )
                                })?;
                                Ok(DataType::Double(f, prec))
                            }
                        }
                        VariantAccess::tuple_variant(variant, 4usize, Visitor)
                    }
                    (Field::Numeric, variant) => VariantAccess::newtype_variant::<Decimal>(variant)
                        .map(|d| DataType::Numeric(Arc::new(d))),
                    (Field::Text, variant) => {
                        VariantAccess::newtype_variant::<TextOrTinyText>(variant).map(|tt| match tt
                        {
                            TextOrTinyText::TinyText(tt) => DataType::TinyText(tt),
                            TextOrTinyText::Text(t) => DataType::Text(t),
                        })
                    }
                    // We deserialize the NaiveDateTime by extracting nsecs from the top 64 bits of
                    // the encoded i128, and secs from the low 64 bits
                    (Field::Timestamp, variant) => VariantAccess::newtype_variant::<u128>(variant)
                        .map(|r| NaiveDateTime::from_timestamp(r as _, (r >> 64) as _).into()),
                    (Field::Time, variant) => {
                        VariantAccess::newtype_variant::<MysqlTime>(variant).map(DataType::Time)
                    }
                    (Field::ByteArray, variant) => {
                        VariantAccess::newtype_variant::<ByteBuf>(variant)
                            .map(|v| DataType::ByteArray(Arc::new(v.into_vec())))
                    }
                    (Field::BitVector, variant) => {
                        VariantAccess::newtype_variant::<BitVec>(variant)
                            .map(|bits| DataType::BitVector(Arc::new(bits)))
                    }
                    (Field::TimestampTz, variant) => VariantAccess::newtype_variant::<(
                        u128,
                        [u8; 3],
                    )>(variant)
                    .map(|(ts, extra)| {
                        let datetime = NaiveDateTime::from_timestamp(ts as _, (ts >> 64) as _);
                        DataType::TimestampTz(TimestampTz { datetime, extra })
                    }),
                    (Field::Max, variant) => {
                        VariantAccess::unit_variant(variant).map(|_| DataType::Max)
                    }
                }
            }
        }

        deserializer.deserialize_enum("DataType", Field::VARIANTS, Visitor)
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
