use std::sync::Arc;

use readyset_data::DfValue;
use readyset_vitess_data::row_parsing::vstream_row_to_noria_row;
use vitess_grpc::binlogdata::FieldEvent;
use vitess_grpc::query::{Field, Row, Type};

#[test]
fn basics() {
    let keyspace = "commerce".to_string();
    let table = "product".to_string();
    let full_table_name = format!("{}.{}", keyspace, table);
    let db_name = "vt_commerce_0".to_string();

    let field = FieldEvent {
        table_name: full_table_name.clone(),
        fields: vec![
            Field {
                name: "sku".to_string(),
                r#type: Type::Varchar as i32,
                table: table.clone(),
                org_table: table.clone(),
                database: db_name.clone(),
                org_name: "sku".to_string(),
                column_length: 400,
                charset: 255,
                decimals: 0,
                flags: 20483,
                column_type: "varchar(100)".to_string(),
            },
            Field {
                name: "description".to_string(),
                r#type: Type::Varbinary as i32,
                table: table.clone(),
                org_table: table.clone(),
                database: db_name.clone(),
                org_name: "description".to_string(),
                column_length: 128,
                charset: 63,
                decimals: 0,
                flags: 128,
                column_type: "varbinary(128)".to_string(),
            },
            Field {
                name: "price".to_string(),
                r#type: Type::Int64 as i32,
                table: table.clone(),
                org_table: table.clone(),
                database: db_name.clone(),
                org_name: "price".to_string(),
                column_length: 20,
                charset: 63,
                decimals: 0,
                flags: 32768,
                column_type: "bigint".to_string(),
            },
        ],
        keyspace,
        shard: "0".to_string(),
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

#[test]
fn null_fields() {
    let keyspace = "commerce".to_string();
    let table = "product".to_string();
    let full_table_name = format!("{}.{}", keyspace, table);
    let db_name = "vt_commerce_0".to_string();

    let field = FieldEvent {
        table_name: full_table_name.clone(),
        fields: vec![
            Field {
                name: "sku".to_string(),
                r#type: Type::Varchar as i32,
                table: table.clone(),
                org_table: table.clone(),
                database: db_name.clone(),
                org_name: "sku".to_string(),
                column_length: 400,
                charset: 255,
                decimals: 0,
                flags: 20483,
                column_type: "varchar(100)".to_string(),
            },
            Field {
                name: "description".to_string(),
                r#type: Type::Varbinary as i32,
                table: table.clone(),
                org_table: table.clone(),
                database: db_name.clone(),
                org_name: "description".to_string(),
                column_length: 128,
                charset: 63,
                decimals: 0,
                flags: 128,
                column_type: "varbinary(128)".to_string(),
            },
            Field {
                name: "price".to_string(),
                r#type: Type::Int64 as i32,
                table: table.clone(),
                org_table: table.clone(),
                database: db_name.clone(),
                org_name: "price".to_string(),
                column_length: 20,
                charset: 63,
                decimals: 0,
                flags: 32768,
                column_type: "bigint".to_string(),
            },
        ],
        keyspace,
        shard: "0".to_string(),
    };

    let insert_row = Row {
        lengths: vec![3, -1, -1],
        values: vec![49, 48, 48], // 100
    };

    let res = vstream_row_to_noria_row(&insert_row, &field);
    assert!(res.is_ok());

    let res = res.unwrap();
    assert_eq!(res.len(), 3);
    assert_eq!(res[0], DfValue::Text("100".into()));
    assert_eq!(res[1], DfValue::None);
    assert_eq!(res[2], DfValue::None);
}
