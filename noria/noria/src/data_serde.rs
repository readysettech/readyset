use crate::data::DataType;
use chrono::NaiveDateTime;
use serde::ser::SerializeTupleVariant;
use std::convert::TryFrom;
use std::fmt;

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
            DataType::Real(v1, v2) => {
                let mut tv = serializer.serialize_tuple_variant("DataType", 2, "Real", 2)?;
                tv.serialize_field(&v1)?;
                tv.serialize_field(&v2)?;
                tv.end()
            }
            DataType::Text(v) => {
                serializer.serialize_newtype_variant("DataType", 3, "Text", v.to_bytes())
            }
            DataType::TinyText(v) => {
                let vu8 = match v.iter().position(|&i| i == 0) {
                    Some(null) => &v[0..null],
                    None => v,
                };
                serializer.serialize_newtype_variant("DataType", 3, "Text", &vu8)
            }
            DataType::Timestamp(v) => {
                serializer.serialize_newtype_variant("DataType", 4, "Timestamp", &v)
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
            Field0,
            Field1,
            Field2,
            Field3,
            Field4,
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
                    0u64 => Ok(Field::Field0),
                    1u64 => Ok(Field::Field1),
                    2u64 => Ok(Field::Field2),
                    3u64 => Ok(Field::Field3),
                    4u64 => Ok(Field::Field4),
                    _ => Err(serde::de::Error::invalid_value(
                        serde::de::Unexpected::Unsigned(val),
                        &"variant index 0 <= i < 5",
                    )),
                }
            }
            fn visit_str<E>(self, val: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match val {
                    "None" => Ok(Field::Field0),
                    "Int" => Ok(Field::Field1),
                    "Real" => Ok(Field::Field2),
                    "Text" => Ok(Field::Field3),
                    "Timestamp" => Ok(Field::Field4),
                    _ => Err(serde::de::Error::unknown_variant(val, VARIANTS)),
                }
            }
            fn visit_bytes<E>(self, val: &[u8]) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match val {
                    b"None" => Ok(Field::Field0),
                    b"Int" => Ok(Field::Field1),
                    b"Real" => Ok(Field::Field2),
                    b"Text" => Ok(Field::Field3),
                    b"Timestamp" => Ok(Field::Field4),
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
                A: serde::de::EnumAccess<'de>,
            {
                match match serde::de::EnumAccess::variant(data) {
                    Ok(val) => val,
                    Err(err) => {
                        return Err(err);
                    }
                } {
                    (Field::Field0, variant) => {
                        match serde::de::VariantAccess::unit_variant(variant) {
                            Ok(val) => val,
                            Err(err) => {
                                return Err(err);
                            }
                        };
                        Ok(DataType::None)
                    }
                    (Field::Field1, variant) => {
                        serde::de::VariantAccess::newtype_variant::<i128>(variant).and_then(|x| {
                            DataType::try_from(x).map_err(|_| {
                                serde::de::Error::invalid_value(
                                    serde::de::Unexpected::Other(format!("{}", x).as_str()),
                                    &"integer (i128)",
                                )
                            })
                        })
                    }
                    (Field::Field2, variant) => {
                        struct Visitor;
                        impl<'de> serde::de::Visitor<'de> for Visitor {
                            type Value = DataType;
                            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                                fmt::Formatter::write_str(formatter, "tuple variant DataType::Real")
                            }
                            #[inline]
                            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
                            where
                                A: serde::de::SeqAccess<'de>,
                            {
                                let field0 =
                                    match match serde::de::SeqAccess::next_element::<i64>(&mut seq)
                                    {
                                        Ok(val) => val,
                                        Err(err) => {
                                            return Err(err);
                                        }
                                    } {
                                        Some(val) => val,
                                        None => {
                                            return Err(serde::de::Error::invalid_length(
                                                0usize,
                                                &"tuple variant DataType::Real with 2 elements",
                                            ));
                                        }
                                    };
                                let field1 =
                                    match match serde::de::SeqAccess::next_element::<i32>(&mut seq)
                                    {
                                        Ok(val) => val,
                                        Err(err) => {
                                            return Err(err);
                                        }
                                    } {
                                        Some(val) => val,
                                        None => {
                                            return Err(serde::de::Error::invalid_length(
                                                1usize,
                                                &"tuple variant DataType::Real with 2 elements",
                                            ));
                                        }
                                    };
                                Ok(DataType::Real(field0, field1))
                            }
                        }
                        serde::de::VariantAccess::tuple_variant(variant, 2usize, Visitor)
                    }
                    (Field::Field3, variant) => {
                        serde::de::VariantAccess::newtype_variant::<&'_ [u8]>(variant).and_then(
                            |x| {
                                DataType::try_from(x).map_err(|_| {
                                    serde::de::Error::invalid_value(
                                        serde::de::Unexpected::Bytes(x),
                                        &"valid utf-8 or short TinyText",
                                    )
                                })
                            },
                        )
                    }
                    (Field::Field4, variant) => Result::map(
                        serde::de::VariantAccess::newtype_variant::<NaiveDateTime>(variant),
                        DataType::Timestamp,
                    ),
                }
            }
        }

        const VARIANTS: &[&str] = &[
            "None",
            "Int",
            "UnsignedInt",
            "BigInt",
            "UnsignedBigInt",
            "Real",
            "Text",
            "Timestamp",
        ];
        deserializer.deserialize_enum("DataType", VARIANTS, Visitor)
    }
}
