use anyhow::{anyhow, Result};
use chrono::Duration;
use chrono::NaiveDateTime;
use noria::{DataType, Modification};
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::convert::TryInto;

#[derive(Debug, Deserialize)]
pub struct EventKey {
    pub schema: Schema,
    pub payload: KeyPayload,
}

#[derive(Debug, Deserialize)]
pub struct KeyPayload {
    #[serde(flatten)]
    pub fields: HashMap<String, Value>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum EventValue {
    SchemaChange(SchemaChange),
    DataChange(DataChange),
}

#[derive(Debug, Deserialize)]
pub struct SchemaChange {
    pub schema: Schema,
    pub payload: SchemaChangePayload,
}

#[derive(Debug, Deserialize)]
pub struct DataChange {
    pub schema: Schema,
    pub payload: DataChangePayload,
}

#[derive(Debug, Deserialize)]
pub struct Schema {
    pub name: String,
    pub optional: bool,
    pub fields: Vec<Field>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SchemaChangePayload {
    pub ddl: String,
    pub database_name: String,
    pub source: Source,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "op")]
pub enum DataChangePayload {
    #[serde(rename = "c")]
    Create(CreatePayload),
    #[serde(rename = "u")]
    Update(UpdatePayload),
    #[serde(rename = "d")]
    Delete { source: Source },
}

#[derive(Debug, Deserialize)]
pub struct CreatePayload {
    pub source: Source,
    pub after: HashMap<String, Value>,
}

#[derive(Debug, Deserialize)]
pub struct UpdatePayload {
    pub source: Source,
    pub before: HashMap<String, Value>,
    pub after: HashMap<String, Value>,
}

#[derive(Debug, Deserialize)]
pub struct Source {
    pub table: String,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum Field {
    StructField { fields: Vec<PrimitiveField> },
    PrimitiveField(PrimitiveField),
}

#[derive(Debug, Deserialize)]
pub struct PrimitiveField {
    pub field: String,
    #[serde(rename = "type")]
    pub typ: String,
    pub optional: bool,
}

pub fn field_to_datatype(f: &PrimitiveField, v: &Value) -> Result<DataType> {
    let field_type = f.typ.to_owned();
    let semantic_type = f.field.to_owned();
    match v {
        Value::Null => Ok(DataType::None),
        Value::Bool(v) => Ok(DataType::Int(*v as i32)),
        Value::String(v) => {
            if semantic_type == "io.debezium.time.ZonedTimestamp" {
                Ok(DataType::Timestamp(NaiveDateTime::parse_from_str(
                    v.as_str(),
                    "%+",
                )?))
            } else if (field_type == "bytes")
                && (semantic_type == "org.apache.kafka.connect.data.Decimal")
            {
                unimplemented!("Set decimal.handling.mode to double in SQL Connector Conf.")
            } else {
                Ok(DataType::try_from(v.as_str())?)
            }
        }
        Value::Number(v) => {
            if semantic_type == "org.apache.kafka.connect.data.Date" {
                Ok(DataType::Timestamp(
                    NaiveDateTime::from_timestamp(0, 0)
                        .checked_add_signed(Duration::days(v.as_i64().unwrap()))
                        .unwrap(),
                ))
            } else if semantic_type == "org.apache.kafka.connect.data.Time" {
                // Noria doesnt have a time datatype, thus using BigInt as an alternative
                // It stores the number of microseconds since midnight as an int64
                Ok(DataType::BigInt(v.as_i64().unwrap()))
            } else if semantic_type == "org.apache.kafka.connect.data.Timestamp" {
                Ok(DataType::Timestamp(
                    NaiveDateTime::from_timestamp(0, 0)
                        .checked_add_signed(Duration::milliseconds(v.as_i64().unwrap()))
                        .unwrap(),
                ))
            } else if matches!(field_type.as_str(), "int32" | "int16" | "int8") {
                Ok(DataType::Int(v.as_i64().unwrap().try_into().unwrap()))
            } else if field_type == "int64" {
                Ok(DataType::BigInt(v.as_i64().unwrap()))
            } else if matches!(field_type.as_str(), "double" | "float32" | "float64") {
                Ok(DataType::from(v.as_f64().unwrap()))
            } else {
                unimplemented!("Type not implemented!")
            }
        }
        _ => Ok(DataType::None),
    }
}

impl EventKey {
    pub fn get_pk_datatype(&self) -> Result<DataType> {
        let pk_field = &self.schema.fields[0];
        match pk_field {
            Field::PrimitiveField(f) => field_to_datatype(&f, &self.payload.fields[&f.field]),
            _ => Err(anyhow!("Primary Key can only be a primitive field.")),
        }
    }
}

impl CreatePayload {
    pub fn get_create_vector(&self, after_schema: &Field) -> Result<Vec<DataType>> {
        match after_schema {
            Field::StructField {
                fields: after_field_schema,
            } => {
                let mut insert_vec = Vec::new();
                for f in after_field_schema.iter() {
                    let field_value = &self.after[&f.field];
                    let new_datatype = field_to_datatype(f, field_value)?;
                    insert_vec.push(new_datatype)
                }
                Ok(insert_vec)
            }
            _ => Err(anyhow!("After Field has to be a struct field.")),
        }
    }
}

impl UpdatePayload {
    pub fn get_update_vector(
        &self,
        after_schema: &Field,
    ) -> Result<Vec<(usize, Modification)>, anyhow::Error> {
        match after_schema {
            Field::StructField {
                fields: after_field_schema,
            } => {
                let mut modifications = Vec::new();
                for (i, f) in after_field_schema.iter().enumerate() {
                    let field_value = &self.after[&f.field];
                    let new_datatype = field_to_datatype(f, field_value)?;
                    let modification: Modification = Modification::Set(new_datatype);
                    modifications.push((i, modification))
                }
                Ok(modifications)
            }
            _ => Err(anyhow!("After Field has to be a struct field.")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_change_event_key_event() {
        let json = r#"
        {
            "schema": { 
               "type": "struct",
               "name": "mysql-server-1.inventory.customers.Key", 
               "optional": false, 
               "fields": [ 
                 {
                   "field": "id",
                   "type": "int32",
                   "optional": false
                 }
               ]
             },
            "payload": { 
               "id": 1001
            }
        }"#;
        let parsed: EventKey = serde_json::from_str(json).unwrap();
        assert_eq!(parsed.payload.fields["id"], 1001);
        assert_eq!(parsed.schema.fields.len(), 1);
    }

    #[test]
    fn parse_schema_change_event_event() {
        let json = r#"
        {
            "schema": { 
               "type": "struct",
               "name": "mysql-server-1.inventory.customers.Key", 
               "optional": false, 
               "fields": [ 
                {
                    "field": "ddl",
                    "type": "string",
                    "optional": false
                  }
               ]
             },
            "payload": { 
                "databaseName": "inventory",
                "ddl": "CREATE TABLE products ( id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255) NOT NULL, description VARCHAR(512), weight FLOAT ); ALTER TABLE products AUTO_INCREMENT = 101;",
                "source" : {
                    "table": "products"
                }
            }
        }"#;
        let parsed: EventValue = serde_json::from_str(json).unwrap();
        assert!(matches!(
            parsed,
            EventValue::SchemaChange(SchemaChange {
                schema: _,
                payload:
                    SchemaChangePayload {
                        ddl: _,
                        database_name: _,
                        source: _,
                    },
            })
        ));
    }

    #[test]
    fn parse_create_change_event_event() {
        let json = r#"
        {
            "schema": { 
              "type": "struct",
              "fields": [
                {
                  "type": "struct",
                  "fields": [],
                  "optional": true,
                  "name": "mysql-server-1.inventory.customers.Value", 
                  "field": "before"
                },
                {
                  "type": "struct",
                  "fields": [
                    {
                      "type": "int32",
                      "optional": false,
                      "field": "id"
                    }
                  ],
                  "optional": true,
                  "name": "mysql-server-1.inventory.customers.Value",
                  "field": "after"
                }
              ],
              "optional": false,
              "name": "mysql-server-1.inventory.customers.Envelope" 
            },
            "payload": { 
              "op": "c",
              "before": null, 
              "after": { 
                "id": 1004
              },
              "source": { 
                "db": "inventory",
                "table": "customers"
              }
            }
          }"#;
        let parsed: EventValue = serde_json::from_str(json).unwrap();
        assert!(matches!(
            parsed,
            EventValue::DataChange(DataChange {
                schema: _,
                payload: DataChangePayload::Create(_),
            })
        ));
    }

    #[test]
    fn parse_update_change_event_event() {
        let json = r#"
        {
            "schema": { 
              "type": "struct",
              "fields": [
                {
                  "type": "struct",
                  "fields": [{
                    "type": "int32",
                    "optional": false,
                    "field": "id"
                  }],
                  "optional": true,
                  "name": "mysql-server-1.inventory.customers.Value", 
                  "field": "before"
                },
                {
                  "type": "struct",
                  "fields": [
                    {
                      "type": "int32",
                      "optional": false,
                      "field": "id"
                    }
                  ],
                  "optional": true,
                  "name": "mysql-server-1.inventory.customers.Value",
                  "field": "after"
                }
              ],
              "optional": false,
              "name": "mysql-server-1.inventory.customers.Envelope" 
            },
            "payload": { 
              "op": "u",
              "before": { 
                "id": 1002
              },
              "after": { 
                "id": 1004
              },
              "source": { 
                "db": "inventory",
                "table": "customers"
              }
            }
          }"#;
        let parsed: EventValue = serde_json::from_str(json).unwrap();
        assert!(matches!(
            parsed,
            EventValue::DataChange(DataChange {
                schema: _,
                payload: DataChangePayload::Update(_),
            })
        ));
    }

    #[test]
    fn parse_delete_change_event_event() {
        let json = r#"
        {
            "schema": { 
              "type": "struct",
              "fields": [
                {
                  "type": "struct",
                  "fields": [{
                    "type": "int32",
                    "optional": false,
                    "field": "id"
                  }],
                  "optional": true,
                  "name": "mysql-server-1.inventory.customers.Value", 
                  "field": "before"
                },
                {
                  "type": "struct",
                  "fields": [
                    {
                      "type": "int32",
                      "optional": false,
                      "field": "id"
                    }
                  ],
                  "optional": true,
                  "name": "mysql-server-1.inventory.customers.Value",
                  "field": "after"
                }
              ],
              "optional": false,
              "name": "mysql-server-1.inventory.customers.Envelope" 
            },
            "payload": { 
              "op": "d",
              "before": null, 
              "after": null,
              "source": { 
                "db": "inventory",
                "table": "customers"
              }
            }
          }"#;
        let parsed: EventValue = serde_json::from_str(json).unwrap();
        assert!(matches!(
            parsed,
            EventValue::DataChange(DataChange {
                schema: _,
                payload: DataChangePayload::Delete { source: _ },
            })
        ));
    }

    // Using the following link for type information
    // https://debezium.io/documentation/reference/connectors/mysql.html#mysql-data-types
    fn test_json_to_datatype_helper(
        json_str: &str,
        field_type: &str,
        semantic_type: &str,
    ) -> DataType {
        let field = PrimitiveField {
            field: semantic_type.to_string(),
            typ: field_type.to_string(),
            optional: false,
        };
        let parsed_value: Value = serde_json::from_str(json_str).unwrap();

        field_to_datatype(&field, &parsed_value).unwrap()
    }
    #[test]
    fn parse_basic_types() {
        assert_eq!(
            test_json_to_datatype_helper("true", "boolean", ""),
            DataType::Int(1)
        );
        assert_eq!(
            test_json_to_datatype_helper("false", "boolean", ""),
            DataType::Int(0)
        );
        assert_eq!(
            test_json_to_datatype_helper("42", "int16", ""),
            DataType::Int(42)
        );
        assert_eq!(
            test_json_to_datatype_helper("-42", "int32", ""),
            DataType::Int(-42)
        );
        assert_eq!(
            test_json_to_datatype_helper("2020", "int32", "io.debezium.time.Year"),
            DataType::Int(2020)
        );
        assert_eq!(
            test_json_to_datatype_helper("42", "int64", ""),
            DataType::BigInt(42)
        );
        assert_eq!(
            test_json_to_datatype_helper("-4.14", "float32", ""),
            DataType::from(-4.14)
        );
        assert_eq!(
            test_json_to_datatype_helper("4.14", "float64", ""),
            DataType::from(4.14)
        );
        assert!(matches!(
            test_json_to_datatype_helper("\"noria\"", "string", ""),
            DataType::TinyText(_)
        ));
        assert!(matches!(
            test_json_to_datatype_helper(
                "\"string with more than TINYTEXT(15) width\"",
                "string",
                ""
            ),
            DataType::Text(_)
        ));
    }

    #[test]
    fn parse_temporal_types() {
        assert_eq!(
            test_json_to_datatype_helper(
                "18646", // Number of days from Unix time for Date: 01-19-2020
                "int32",
                "org.apache.kafka.connect.data.Date"
            ),
            DataType::Timestamp(
                NaiveDateTime::parse_from_str("2021-01-19T00:00:00+00:00", "%+").unwrap()
            )
        );
        assert_eq!(
            test_json_to_datatype_helper(
                "1611080613",
                "int64",
                "org.apache.kafka.connect.data.Time"
            ),
            DataType::BigInt(1611080613)
        );
        assert_eq!(
            test_json_to_datatype_helper(
                "1611080613000", // Milliseconds from unix time
                "int64",
                "org.apache.kafka.connect.data.Timestamp"
            ),
            DataType::Timestamp(
                NaiveDateTime::parse_from_str("2021-01-19T18:23:33+00:00", "%+").unwrap()
            )
        );
    }
}
