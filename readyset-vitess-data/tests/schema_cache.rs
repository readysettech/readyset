use readyset_vitess_data::vitess::SchemaCache;

mod helpers;
use helpers::*;

#[test]
fn process_field_event() {
    let field_event = field_event(vec![field_sku(), field_description(), field_price()]);

    let mut schema = SchemaCache::new(KEYSPACE);
    assert_eq!(schema.tables.len(), 0);

    schema.process_field_event(&field_event);

    assert_eq!(schema.tables.len(), 1);
    assert!(schema.tables.contains_key(TABLE));

    let table_schema = schema.tables.get(TABLE).unwrap();

    assert_eq!(table_schema.columns.len(), 3);

    assert!(table_schema.columns.iter().any(|c| c.name == "sku"));
    assert!(table_schema.columns.iter().any(|c| c.name == "description"));
    assert!(table_schema.columns.iter().any(|c| c.name == "price"));
}

#[test]
fn schema_update() {
    let mut field_event = field_event(vec![field_sku()]);

    let mut schema = SchemaCache::new(KEYSPACE);
    assert_eq!(schema.tables.len(), 0);

    schema.process_field_event(&field_event);
    assert_eq!(schema.tables.len(), 1);

    let table = schema.tables.get(TABLE).unwrap();
    assert_eq!(table.columns.len(), 1);

    field_event.fields.push(field_description());

    schema.process_field_event(&field_event);
    assert_eq!(schema.tables.len(), 1);

    let table = schema.tables.get(TABLE).unwrap();
    assert_eq!(table.columns.len(), 2);
}
