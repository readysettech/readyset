use readyset_data::DfValue;
use readyset_vitess_data::field_parsing::vstream_value_to_noria_value;
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
        let res = vstream_value_to_noria_value("42".as_bytes(), *t);
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), DfValue::Int(42));

        let res = vstream_value_to_noria_value("hello".as_bytes(), Type::Int16);
        assert!(res.is_err());
    });
}
