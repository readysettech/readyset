use std::sync::Arc;

use readyset_data::DfValue;
use readyset_vitess_data::vitess::Table;
use readyset_vitess_data::vstream_row_to_noria_row;
use vitess_grpc::query::Row;

mod helpers;
use helpers::*;

fn products_table() -> Table {
    let mut table = Table::new(KEYSPACE, TABLE);
    table.set_columns(&vec![field_sku(), field_description(), field_price()]);
    table
}

#[test]
fn basics() {
    let insert_row = Row {
        lengths: vec![2, 10, 2],
        values: vec![
            112, 49, // p1
            112, 114, 111, 100, 117, 99, 116, 32, 35, 49, // product #1
            52, 50, // 42
        ],
    };

    let res = vstream_row_to_noria_row(&insert_row, &products_table());
    assert!(res.is_ok());
    assert_eq!(
        res.unwrap(),
        vec![
            DfValue::Text("p1".into()),
            DfValue::ByteArray(Arc::new(vec![112, 114, 111, 100, 117, 99, 116, 32, 35, 49])),
            DfValue::Int(42),
        ]
    );
}

#[test]
fn null_fields() {
    let insert_row = Row {
        lengths: vec![3, -1, -1],
        values: vec![49, 48, 48], // 100
    };

    let res = vstream_row_to_noria_row(&insert_row, &products_table());
    assert!(res.is_ok());

    let res = res.unwrap();
    assert_eq!(res.len(), 3);
    assert_eq!(res[0], DfValue::Text("100".into()));
    assert_eq!(res[1], DfValue::None);
    assert_eq!(res[2], DfValue::None);
}
