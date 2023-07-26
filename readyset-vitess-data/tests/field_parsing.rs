use std::sync::Arc;

use bit_vec::BitVec;
use readyset_data::DfValue;
use readyset_vitess_data::Column;
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
        let column = Column::new("test", *t, "int", 0);
        let res = column.vstream_value_to_noria_value("42".as_bytes());
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), DfValue::Int(42));

        let res = column.vstream_value_to_noria_value("hello".as_bytes());
        assert!(res.is_err());
    });
}

#[test]
fn date_parsing() {
    let column = Column::new("test", Type::Date, "date", 0);
    let res = column.vstream_value_to_noria_value("2023-07-24".as_bytes());
    assert!(res.is_ok());
    assert_eq!(res.unwrap(), "2023-07-24".try_into().unwrap());

    let res = column.vstream_value_to_noria_value("hello".as_bytes());
    assert!(res.is_err());
}

#[test]
fn datetime_parsing() {
    let column = Column::new("test", Type::Datetime, "datetime", 0);
    let res = column.vstream_value_to_noria_value("2023-07-24 12:34:56".as_bytes());
    assert!(res.is_ok());
    assert_eq!(res.unwrap(), "2023-07-24 12:34:56".try_into().unwrap());

    let res = column.vstream_value_to_noria_value("hello".as_bytes());
    assert!(res.is_err());
}

#[test]
fn time_parsing() {
    let column = Column::new("test", Type::Time, "time", 0);
    let res = column.vstream_value_to_noria_value("-12:34:56".as_bytes());
    assert!(res.is_ok());
    assert_eq!(res.unwrap(), "-12:34:56".try_into().unwrap());

    let res = column.vstream_value_to_noria_value("12:34:56".as_bytes());
    assert!(res.is_ok());
    assert_eq!(res.unwrap(), "12:34:56".try_into().unwrap());

    let res = column.vstream_value_to_noria_value("120:34:56".as_bytes());
    assert!(res.is_ok());
    assert_eq!(res.unwrap(), "120:34:56".try_into().unwrap());

    let res = column.vstream_value_to_noria_value("hello".as_bytes());
    assert!(res.is_err());
}

#[test]
fn timestamp_parsing() {
    let column = Column::new("test", Type::Timestamp, "timestamp", 0);
    let res = column.vstream_value_to_noria_value("2038-01-19 03:14:07.499999".as_bytes());
    assert!(res.is_ok());
    assert_eq!(
        res.unwrap(),
        "2038-01-19 03:14:07.499999".try_into().unwrap()
    );

    let res = column.vstream_value_to_noria_value("hello".as_bytes());
    assert!(res.is_err());
}

#[test]
fn year_parsing() {
    let column = Column::new("test", Type::Year, "year", 0);
    let res = column.vstream_value_to_noria_value("2023".as_bytes());
    assert!(res.is_ok());
    assert_eq!(res.unwrap(), DfValue::Int(2023));

    let res = column.vstream_value_to_noria_value("hello".as_bytes());
    assert!(res.is_err());
}

#[test]
fn text_parsing() {
    let column = Column::new("test", Type::Text, "text", 0);
    let res = column.vstream_value_to_noria_value("Hello, world!".as_bytes());
    assert!(res.is_ok());
    assert_eq!(res.unwrap(), DfValue::Text("Hello, world!".into()));

    let column = Column::new("test", Type::Char, "char(100)", 0);
    let res = column.vstream_value_to_noria_value("Lorem ipsum.".as_bytes());
    assert!(res.is_ok());
    assert_eq!(res.unwrap(), DfValue::Text("Lorem ipsum.".into()));

    let column = Column::new("test", Type::Varchar, "varchar(100)", 0);
    let res = column.vstream_value_to_noria_value("Short text".as_bytes());
    assert!(res.is_ok());
    assert_eq!(res.unwrap(), DfValue::Text("Short text".into()));

    let json = "{\"x\": 123}";
    let column = Column::new("test", Type::Json, "json", 0);
    let res = column.vstream_value_to_noria_value(json.as_bytes());
    assert!(res.is_ok());
    assert_eq!(res.unwrap(), DfValue::Text(json.into()));
}

#[test]
fn binary_parsing() {
    let binary_types = [
        (Type::Blob, "blob"),
        (Type::Binary, "binary"),
        (Type::Varbinary, "varbinary"),
    ];
    let raw_value = "binary data".as_bytes();

    for (binary_type, type_name) in binary_types.iter() {
        let column = Column::new("test", *binary_type, type_name, 0);
        let res = column.vstream_value_to_noria_value(raw_value);
        assert!(res.is_ok());
        assert_eq!(
            res.unwrap(),
            DfValue::ByteArray(Arc::new(raw_value.to_vec()))
        );
    }
}

#[test]
fn bit_parsing_single_byte() {
    let column = Column::new("test", Type::Bit, "bit(8)", 0);
    let res = column.vstream_value_to_noria_value(&[0b10101010]);
    assert!(res.is_ok());
    let expected = BitVec::from_bytes(&[0b10101010]);
    assert_eq!(res.unwrap(), DfValue::BitVector(Arc::new(expected)));
}

#[test]
fn bit_parsing_multiple_bytes() {
    let column = Column::new("test", Type::Bit, "bit(8)", 0);
    let res = column.vstream_value_to_noria_value(&[0b10101010, 0b11001100]);
    assert!(res.is_ok());
    let expected = BitVec::from_bytes(&[0b10101010, 0b11001100]);
    assert_eq!(res.unwrap(), DfValue::BitVector(Arc::new(expected)));
}

#[test]
fn empty_field() {
    let column = Column::new("test", Type::Bit, "bit(8)", 0);
    let res = column.vstream_value_to_noria_value(&[]);
    assert!(res.is_ok());
    assert_eq!(res.unwrap(), DfValue::None);
}
