use std::sync::Arc;

use readyset_data::DfValue;
use readyset_vitess_data::row_parsing::vstream_row_to_noria_row;
use vitess_grpc::binlogdata::FieldEvent;
use vitess_grpc::query::{Field, Row, Type};

/*
Events from a test transaction:

Received Vitess event: VEvent { r#type: Begin, timestamp: 1690163027, gtid: "", statement: "", row_event: None, field_event: None, vgtid: None, journal: None, dml: "", current_time: 1690163027246482079, last_p_k_event: None, keyspace: "commerce", shard: "0", throttled: false }
Received Vitess event: VEvent { r#type: Field, timestamp: 1690163027, gtid: "", statement: "", row_event: None, field_event: Some(FieldEvent { table_name: "commerce.product", fields: [Field { name: "sku", r#type: Varbinary, table: "product", org_table: "product", database: "vt_commerce_0", org_name: "sku", column_length: 128, charset: 63, decimals: 0, flags: 20611, column_type: "varbinary(128)" }, Field { name: "description", r#type: Varbinary, table: "product", org_table: "product", database: "vt_commerce_0", org_name: "description", column_length: 128, charset: 63, decimals: 0, flags: 128, column_type: "varbinary(128)" }, Field { name: "price", r#type: Int64, table: "product", org_table: "product", database: "vt_commerce_0", org_name: "price", column_length: 20, charset: 63, decimals: 0, flags: 32768, column_type: "bigint" }], keyspace: "commerce", shard: "0" }), vgtid: None, journal: None, dml: "", current_time: 1690163027251180504, last_p_k_event: None, keyspace: "commerce", shard: "0", throttled: false }
Received Vitess event: VEvent { r#type: Row, timestamp: 1690163027, gtid: "", statement: "", row_event: Some(RowEvent { table_name: "commerce.product", row_changes: [RowChange { before: None, after: Some(Row { lengths: [2, 10, 2], values: [112, 49, 112, 114, 111, 100, 117, 99, 116, 32, 35, 49, 52, 50] }) }], keyspace: "commerce", shard: "0" }), field_event: None, vgtid: None, journal: None, dml: "", current_time: 1690163027251254254, last_p_k_event: None, keyspace: "commerce", shard: "0", throttled: false }
Received Vitess event: VEvent { r#type: Vgtid, timestamp: 0, gtid: "", statement: "", row_event: None, field_event: None, vgtid: Some(VGtid { shard_gtids: [ShardGtid { keyspace: "commerce", shard: "0", gtid: "MySQL56/3a8d9716-29c2-11ee-a892-16f97e25d375:1-27", table_p_ks: [] }] }), journal: None, dml: "", current_time: 0, last_p_k_event: None, keyspace: "commerce", shard: "0", throttled: false }
Received Vitess event: VEvent { r#type: Commit, timestamp: 1690163027, gtid: "", statement: "", row_event: None, field_event: None, vgtid: None, journal: None, dml: "", current_time: 1690163027251273421, last_p_k_event: None, keyspace: "commerce", shard: "0", throttled: false }

*/

#[test]
fn insert() {
    let keyspace = String::from("commerce");
    let table = String::from("product");
    let full_table_name = format!("{}.{}", keyspace, table);
    let db_name = String::from("vt_commerce_0");

    let field = FieldEvent {
        table_name: full_table_name.clone(),
        fields: vec![
            Field {
                name: String::from("sku"),
                r#type: Type::Varchar as i32,
                table: table.clone(),
                org_table: table.clone(),
                database: db_name.clone(),
                org_name: String::from("sku"),
                column_length: 400,
                charset: 255,
                decimals: 0,
                flags: 20483,
                column_type: String::from("varchar(100)"),
            },
            Field {
                name: String::from("description"),
                r#type: Type::Varbinary as i32,
                table: table.clone(),
                org_table: table.clone(),
                database: db_name.clone(),
                org_name: String::from("description"),
                column_length: 128,
                charset: 63,
                decimals: 0,
                flags: 128,
                column_type: String::from("varbinary(128)"),
            },
            Field {
                name: String::from("price"),
                r#type: Type::Int64 as i32,
                table: table.clone(),
                org_table: table.clone(),
                database: db_name.clone(),
                org_name: String::from("price"),
                column_length: 20,
                charset: 63,
                decimals: 0,
                flags: 32768,
                column_type: String::from("bigint"),
            },
        ],
        keyspace,
        shard: String::from("0"),
    };

    let insert_row = Row {
        lengths: vec![2, 10, 2],
        values: vec![
            112, 49, // p1
            112, 114, 111, 100, 117, 99, 116, 32, 35, 49, // product #1
            52, 50, // 42
        ],
    };

    let res = vstream_row_to_noria_row(&insert_row, &field);
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
