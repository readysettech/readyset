use crate::data::DataType;
use chrono::NaiveDateTime;
use serde::ser::SerializeTupleVariant;
use std::convert::TryFrom;

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
        #[allow(non_camel_case_types)]
        enum __Field {
            __field0,
            __field1,
            __field2,
            __field3,
            __field4,
        }
        struct __FieldVisitor;
        impl<'de> serde::de::Visitor<'de> for __FieldVisitor {
            type Value = __Field;
            fn expecting(
                &self,
                __formatter: &mut serde::export::Formatter<'_>,
            ) -> serde::export::fmt::Result {
                serde::export::Formatter::write_str(__formatter, "variant identifier")
            }
            fn visit_u64<__E>(self, __value: u64) -> serde::export::Result<Self::Value, __E>
            where
                __E: serde::de::Error,
            {
                match __value {
                    0u64 => serde::export::Ok(__Field::__field0),
                    1u64 => serde::export::Ok(__Field::__field1),
                    2u64 => serde::export::Ok(__Field::__field2),
                    3u64 => serde::export::Ok(__Field::__field3),
                    4u64 => serde::export::Ok(__Field::__field4),
                    _ => serde::export::Err(serde::de::Error::invalid_value(
                        serde::de::Unexpected::Unsigned(__value),
                        &"variant index 0 <= i < 5",
                    )),
                }
            }
            fn visit_str<__E>(self, __value: &str) -> serde::export::Result<Self::Value, __E>
            where
                __E: serde::de::Error,
            {
                match __value {
                    "None" => serde::export::Ok(__Field::__field0),
                    "Int" => serde::export::Ok(__Field::__field1),
                    "Real" => serde::export::Ok(__Field::__field2),
                    "Text" => serde::export::Ok(__Field::__field3),
                    "Timestamp" => serde::export::Ok(__Field::__field4),
                    _ => serde::export::Err(serde::de::Error::unknown_variant(__value, VARIANTS)),
                }
            }
            fn visit_bytes<__E>(self, __value: &[u8]) -> serde::export::Result<Self::Value, __E>
            where
                __E: serde::de::Error,
            {
                match __value {
                    b"None" => serde::export::Ok(__Field::__field0),
                    b"Int" => serde::export::Ok(__Field::__field1),
                    b"Real" => serde::export::Ok(__Field::__field2),
                    b"Text" => serde::export::Ok(__Field::__field3),
                    b"Timestamp" => serde::export::Ok(__Field::__field4),
                    _ => {
                        let __value = &serde::export::from_utf8_lossy(__value);
                        serde::export::Err(serde::de::Error::unknown_variant(__value, VARIANTS))
                    }
                }
            }
        }
        impl<'de> serde::Deserialize<'de> for __Field {
            #[inline]
            fn deserialize<__D>(__deserializer: __D) -> serde::export::Result<Self, __D::Error>
            where
                __D: serde::Deserializer<'de>,
            {
                serde::Deserializer::deserialize_identifier(__deserializer, __FieldVisitor)
            }
        }

        struct __Visitor;
        impl<'de> serde::de::Visitor<'de> for __Visitor {
            type Value = DataType;
            fn expecting(
                &self,
                __formatter: &mut serde::export::Formatter<'_>,
            ) -> serde::export::fmt::Result {
                serde::export::Formatter::write_str(__formatter, "enum DataType")
            }

            fn visit_enum<__A>(self, __data: __A) -> serde::export::Result<Self::Value, __A::Error>
            where
                __A: serde::de::EnumAccess<'de>,
            {
                match match serde::de::EnumAccess::variant(__data) {
                    serde::export::Ok(__val) => __val,
                    serde::export::Err(__err) => {
                        return serde::export::Err(__err);
                    }
                } {
                    (__Field::__field0, __variant) => {
                        match serde::de::VariantAccess::unit_variant(__variant) {
                            serde::export::Ok(__val) => __val,
                            serde::export::Err(__err) => {
                                return serde::export::Err(__err);
                            }
                        };
                        serde::export::Ok(DataType::None)
                    }
                    (__Field::__field1, __variant) => {
                        serde::de::VariantAccess::newtype_variant::<i128>(__variant).and_then(|x| {
                            DataType::try_from(x).map_err(|_| {
                                serde::de::Error::invalid_value(
                                    serde::de::Unexpected::Other(format!("{}", x).as_str()),
                                    &"integer (i128)",
                                )
                            })
                        })
                    }
                    (__Field::__field2, __variant) => {
                        struct __Visitor;
                        impl<'de> serde::de::Visitor<'de> for __Visitor {
                            type Value = DataType;
                            fn expecting(
                                &self,
                                __formatter: &mut serde::export::Formatter<'_>,
                            ) -> serde::export::fmt::Result {
                                serde::export::Formatter::write_str(
                                    __formatter,
                                    "tuple variant DataType::Real",
                                )
                            }
                            #[inline]
                            fn visit_seq<__A>(
                                self,
                                mut __seq: __A,
                            ) -> serde::export::Result<Self::Value, __A::Error>
                            where
                                __A: serde::de::SeqAccess<'de>,
                            {
                                let __field0 = match match serde::de::SeqAccess::next_element::<i64>(
                                    &mut __seq,
                                ) {
                                    serde::export::Ok(__val) => __val,
                                    serde::export::Err(__err) => {
                                        return serde::export::Err(__err);
                                    }
                                } {
                                    serde::export::Some(__value) => __value,
                                    serde::export::None => {
                                        return serde::export::Err(
                                            serde::de::Error::invalid_length(
                                                0usize,
                                                &"tuple variant DataType::Real with 2 elements",
                                            ),
                                        );
                                    }
                                };
                                let __field1 = match match serde::de::SeqAccess::next_element::<i32>(
                                    &mut __seq,
                                ) {
                                    serde::export::Ok(__val) => __val,
                                    serde::export::Err(__err) => {
                                        return serde::export::Err(__err);
                                    }
                                } {
                                    serde::export::Some(__value) => __value,
                                    serde::export::None => {
                                        return serde::export::Err(
                                            serde::de::Error::invalid_length(
                                                1usize,
                                                &"tuple variant DataType::Real with 2 elements",
                                            ),
                                        );
                                    }
                                };
                                serde::export::Ok(DataType::Real(__field0, __field1))
                            }
                        }
                        serde::de::VariantAccess::tuple_variant(__variant, 2usize, __Visitor)
                    }
                    (__Field::__field3, __variant) => serde::de::VariantAccess::newtype_variant::<
                        &'_ [u8],
                    >(__variant)
                    .and_then(|x| {
                        DataType::try_from(x).map_err(|_| {
                            serde::de::Error::invalid_value(
                                serde::de::Unexpected::Bytes(x),
                                &"valid utf-8 or short TinyText",
                            )
                        })
                    }),
                    (__Field::__field4, __variant) => serde::export::Result::map(
                        serde::de::VariantAccess::newtype_variant::<NaiveDateTime>(__variant),
                        DataType::Timestamp,
                    ),
                }
            }
        }

        const VARIANTS: &'static [&'static str] = &[
            "None",
            "Int",
            "UnsignedInt",
            "BigInt",
            "UnsignedBigInt",
            "Real",
            "Text",
            "Timestamp",
        ];
        deserializer.deserialize_enum("DataType", VARIANTS, __Visitor)
    }
}
