use std::sync::Arc;

use bit_vec::BitVec;
use readyset_data::DfValue;
use readyset_vitess_data::vstream_value_to_noria_value;
use vitess_grpc::query::Type;

#[test]
fn int_parsing() {
    [
        Type::Int8,
        Type::Int16,
        Type::Int24,
        Type::Int32,
        Type::Int64,
    ]
    .iter()
    .for_each(|t| {
        let res = vstream_value_to_noria_value("42".as_bytes(), *t, None);
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), DfValue::Int(42));

        let res = vstream_value_to_noria_value("hello".as_bytes(), *t, None);
        assert!(res.is_err());
    });
}

#[test]
fn date_parsing() {
    let res = vstream_value_to_noria_value("2023-07-24".as_bytes(), Type::Date, None);
    assert!(res.is_ok());
    assert_eq!(res.unwrap(), "2023-07-24".try_into().unwrap());

    let res = vstream_value_to_noria_value("hello".as_bytes(), Type::Date, None);
    assert!(res.is_err());
}

#[test]
fn datetime_parsing() {
    let res = vstream_value_to_noria_value("2023-07-24 12:34:56".as_bytes(), Type::Datetime, None);
    assert!(res.is_ok());
    assert_eq!(res.unwrap(), "2023-07-24 12:34:56".try_into().unwrap());

    let res = vstream_value_to_noria_value("hello".as_bytes(), Type::Datetime, None);
    assert!(res.is_err());
}

#[test]
fn time_parsing() {
    let res = vstream_value_to_noria_value("-12:34:56".as_bytes(), Type::Time, None);
    assert!(res.is_ok());
    assert_eq!(res.unwrap(), "-12:34:56".try_into().unwrap());

    let res = vstream_value_to_noria_value("12:34:56".as_bytes(), Type::Time, None);
    assert!(res.is_ok());
    assert_eq!(res.unwrap(), "12:34:56".try_into().unwrap());

    let res = vstream_value_to_noria_value("120:34:56".as_bytes(), Type::Time, None);
    assert!(res.is_ok());
    assert_eq!(res.unwrap(), "120:34:56".try_into().unwrap());

    let res = vstream_value_to_noria_value("hello".as_bytes(), Type::Time, None);
    assert!(res.is_err());
}

#[test]
fn timestamp_parsing() {
    let res = vstream_value_to_noria_value(
        "2038-01-19 03:14:07.499999".as_bytes(),
        Type::Timestamp,
        None,
    );
    assert!(res.is_ok());
    assert_eq!(
        res.unwrap(),
        "2038-01-19 03:14:07.499999".try_into().unwrap()
    );

    let res = vstream_value_to_noria_value("hello".as_bytes(), Type::Timestamp, None);
    assert!(res.is_err());
}

#[test]
fn year_parsing() {
    let res = vstream_value_to_noria_value("2023".as_bytes(), Type::Year, None);
    assert!(res.is_ok());
    assert_eq!(res.unwrap(), DfValue::Int(2023));

    let res = vstream_value_to_noria_value("hello".as_bytes(), Type::Year, None);
    assert!(res.is_err());
}

#[test]
fn text_parsing() {
    let res = vstream_value_to_noria_value("Hello, world!".as_bytes(), Type::Text, None);
    assert!(res.is_ok());
    assert_eq!(res.unwrap(), DfValue::Text("Hello, world!".into()));

    let res = vstream_value_to_noria_value("Lorem ipsum.".as_bytes(), Type::Char, None);
    assert!(res.is_ok());
    assert_eq!(res.unwrap(), DfValue::Text("Lorem ipsum.".into()));

    let res = vstream_value_to_noria_value("Short text".as_bytes(), Type::Varchar, None);
    assert!(res.is_ok());
    assert_eq!(res.unwrap(), DfValue::Text("Short text".into()));

    let json = "{\"x\": 123}";
    let res = vstream_value_to_noria_value(json.as_bytes(), Type::Json, None);
    assert!(res.is_ok());
    assert_eq!(res.unwrap(), DfValue::Text(json.into()));
}

#[test]
fn binary_parsing() {
    let raw_value = "binary data".as_bytes();
    let binary_types = [Type::Blob, Type::Binary, Type::Varbinary];

    for binary_type in binary_types.iter() {
        let res = vstream_value_to_noria_value(raw_value, *binary_type, None);
        assert!(res.is_ok());
        assert_eq!(
            res.unwrap(),
            DfValue::ByteArray(Arc::new(raw_value.to_vec()))
        );
    }
}

#[test]
fn bit_parsing_single_byte() {
    let res = vstream_value_to_noria_value(&[0b10101010], Type::Bit, None);
    assert!(res.is_ok());
    let expected = BitVec::from_bytes(&[0b10101010]);
    assert_eq!(res.unwrap(), DfValue::BitVector(Arc::new(expected)));
}

#[test]
fn bit_parsing_multiple_bytes() {
    let res = vstream_value_to_noria_value(&[0b10101010, 0b11001100], Type::Bit, None);
    assert!(res.is_ok());
    let expected = BitVec::from_bytes(&[0b10101010, 0b11001100]);
    assert_eq!(res.unwrap(), DfValue::BitVector(Arc::new(expected)));
}

#[test]
fn empty_field() {
    let res = vstream_value_to_noria_value(&[], Type::Int8, None);
    assert!(res.is_ok());
    assert_eq!(res.unwrap(), DfValue::None);
}
