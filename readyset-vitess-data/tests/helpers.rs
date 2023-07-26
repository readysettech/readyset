use vitess_grpc::binlogdata::FieldEvent;
use vitess_grpc::query::{Field, Type};

pub const KEYSPACE: &str = "commerce";
pub const TABLE: &str = "product";
const DB_NAME: &str = "vt_commerce_0";

#[allow(dead_code)]
pub fn field_event(fields: Vec<Field>) -> FieldEvent {
    FieldEvent {
        table_name: format!("{}.{}", KEYSPACE, TABLE),
        keyspace: KEYSPACE.to_string(),
        shard: "0".to_string(),
        fields,
    }
}

pub fn field_sku() -> Field {
    Field {
        name: "sku".to_string(),
        r#type: Type::Varchar as i32,
        table: TABLE.to_string(),
        org_table: TABLE.to_string(),
        database: DB_NAME.to_string(),
        org_name: "sku".to_string(),
        column_length: 400,
        charset: 255,
        decimals: 0,
        flags: 20483,
        column_type: "varchar(100)".to_string(),
    }
}

pub fn field_price() -> Field {
    Field {
        name: "price".to_string(),
        r#type: Type::Int64 as i32,
        table: TABLE.to_string(),
        org_table: TABLE.to_string(),
        database: DB_NAME.to_string(),
        org_name: "price".to_string(),
        column_length: 20,
        charset: 63,
        decimals: 0,
        flags: 32768,
        column_type: "bigint".to_string(),
    }
}

pub fn field_description() -> Field {
    Field {
        name: "description".to_string(),
        r#type: Type::Varbinary as i32,
        table: TABLE.to_string(),
        org_table: TABLE.to_string(),
        database: DB_NAME.to_string(),
        org_name: "description".to_string(),
        column_length: 128,
        charset: 63,
        decimals: 0,
        flags: 128,
        column_type: "varbinary(128)".to_string(),
    }
}
