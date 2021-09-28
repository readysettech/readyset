use crate::data::DataType;
use chrono::NaiveDateTime;
use mysql_time::MysqlTime;
use rust_decimal::Decimal;
use serde::de::{EnumAccess, VariantAccess};
use serde::ser::SerializeTupleVariant;
use serde_bytes::{ByteBuf, Bytes};
use std::borrow::{Borrow, Cow};
use std::convert::{TryFrom, TryInto};
use std::fmt;
use std::sync::Arc;

impl serde::ser::Serialize for DataType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        match &self {
            DataType::None => serializer.serialize_unit_variant("DataType", 0, "None"),
            DataType::Int(v) => {
                serializer.serialize_newtype_variant("DataType", 1, "Int", &i128::from(*v))
            }
            DataType::UnsignedInt(v) => {
                serializer.serialize_newtype_variant("DataType", 1, "Int", &i128::from(*v))
            }
            DataType::BigInt(v) => {
                serializer.serialize_newtype_variant("DataType", 1, "Int", &i128::from(*v))
            }
            DataType::UnsignedBigInt(v) => {
                serializer.serialize_newtype_variant("DataType", 1, "Int", &i128::from(*v))
            }
            DataType::Float(f, prec) => {
                let mut tv = serializer.serialize_tuple_variant("DataType", 7, "Float", 2)?;
                tv.serialize_field(f)?;
                tv.serialize_field(prec)?;
                tv.end()
            }
            DataType::Double(f, prec) => {
                let mut tv = serializer.serialize_tuple_variant("DataType", 2, "Double", 2)?;
                tv.serialize_field(f)?;
                tv.serialize_field(prec)?;
                tv.end()
            }
            DataType::Text(v) => serializer.serialize_newtype_variant("DataType", 3, "Text", v),
            DataType::Timestamp(v) => {
                // We serialize the NaiveDateTime as seconds in the low 64 bits of u128 and subsec nanos in the high 64 bits
                // NOTE: don't be tempted to remove the intermediate step of casting to u64, as it will propagate the sign bit
                let ts =
                    v.timestamp() as u64 as u128 + ((v.timestamp_subsec_nanos() as u128) << 64);
                serializer.serialize_newtype_variant("DataType", 4, "Timestamp", &ts)
            }
            DataType::Time(v) => serializer.serialize_newtype_variant("DataType", 5, "Time", &v),
            DataType::TinyText(v) => {
                let mut b = [0u8; 16];
                b[..v.len()].copy_from_slice(v);
                serializer.serialize_newtype_variant(
                    "DataType",
                    6,
                    "TinyText",
                    &i128::from_le_bytes(b),
                )
            }
            DataType::ByteArray(array) => serializer.serialize_newtype_variant(
                "DataType",
                8,
                "ByteArray",
                Bytes::new(array.as_ref()),
            ),
            DataType::Numeric(d) => {
                serializer.serialize_newtype_variant("DataType", 9, "Numeric", &d)
            }
        }
    }
}

impl<'de> serde::Deserialize<'de> for DataType {
    fn deserialize<D>(deserializer: D) -> Result<DataType, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        enum Field {
            None,
            Int,
            Double,
            Text,
            Timestamp,
            Time,
            TinyText,
            Float,
            ByteArray,
            Numeric,
        }
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
                match val {
                    0u64 => Ok(Field::None),
                    1u64 => Ok(Field::Int),
                    2u64 => Ok(Field::Double),
                    3u64 => Ok(Field::Text),
                    4u64 => Ok(Field::Timestamp),
                    5u64 => Ok(Field::Time),
                    6u64 => Ok(Field::TinyText),
                    7u64 => Ok(Field::Float),
                    8u64 => Ok(Field::ByteArray),
                    9u64 => Ok(Field::Numeric),
                    _ => Err(serde::de::Error::invalid_value(
                        serde::de::Unexpected::Unsigned(val),
                        &"variant index 0 <= i < 10",
                    )),
                }
            }
            fn visit_str<E>(self, val: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match val {
                    "None" => Ok(Field::None),
                    "Int" => Ok(Field::Int),
                    "Float" => Ok(Field::Float),
                    "Double" => Ok(Field::Double),
                    "Text" => Ok(Field::Text),
                    "Timestamp" => Ok(Field::Timestamp),
                    "Time" => Ok(Field::Time),
                    "TinyText" => Ok(Field::TinyText),
                    "ByteArray" => Ok(Field::ByteArray),
                    "Numeric" => Ok(Field::Numeric),
                    _ => Err(serde::de::Error::unknown_variant(val, VARIANTS)),
                }
            }
            fn visit_bytes<E>(self, val: &[u8]) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match val {
                    b"None" => Ok(Field::None),
                    b"Int" => Ok(Field::Int),
                    b"Float" => Ok(Field::Float),
                    b"Double" => Ok(Field::Double),
                    b"Text" => Ok(Field::Text),
                    b"Timestamp" => Ok(Field::Timestamp),
                    b"Time" => Ok(Field::Time),
                    b"TinyText" => Ok(Field::TinyText),
                    b"ByteArray" => Ok(Field::ByteArray),
                    b"Numeric" => Ok(Field::Numeric),
                    _ => Err(serde::de::Error::unknown_variant(
                        &String::from_utf8_lossy(val),
                        VARIANTS,
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
                        match VariantAccess::unit_variant(variant) {
                            Ok(val) => val,
                            Err(err) => {
                                return Err(err);
                            }
                        };
                        Ok(DataType::None)
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
                        VariantAccess::newtype_variant::<Cow<'_, [u8]>>(variant).and_then(|x| {
                            let x: &[u8] = x.borrow();
                            DataType::try_from(x).map_err(|_| {
                                serde::de::Error::invalid_value(
                                    serde::de::Unexpected::Bytes(x),
                                    &"valid utf-8 or short TinyText",
                                )
                            })
                        })
                    }
                    // We deserialize the NaiveDateTime by extracting nsecs from the top 64 bits of the encoded i128, and secs from the low 64 bits
                    (Field::Timestamp, variant) => VariantAccess::newtype_variant::<u128>(variant)
                        .map(|r| NaiveDateTime::from_timestamp(r as _, (r >> 64) as _).into()),
                    (Field::Time, variant) => VariantAccess::newtype_variant::<MysqlTime>(variant)
                        .map(|v| DataType::Time(Arc::new(v))),
                    (Field::TinyText, variant) => VariantAccess::newtype_variant::<i128>(variant)
                        .map(|r| DataType::TinyText(r.to_le_bytes()[..15].try_into().unwrap())),
                    (Field::ByteArray, variant) => {
                        VariantAccess::newtype_variant::<ByteBuf>(variant)
                            .map(|v| DataType::ByteArray(Arc::new(v.into_vec())))
                    }
                }
            }
        }

        const VARIANTS: &[&str] = &[
            "None",
            "Int",
            "Double",
            "Text",
            "Timestamp",
            "Time",
            "TinyText",
            "Float",
            "ByteArray",
        ];
        deserializer.deserialize_enum("DataType", VARIANTS, Visitor)
    }
}
