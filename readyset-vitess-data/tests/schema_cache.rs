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

    let table = schema.tables.get(TABLE).unwrap();

    assert_eq!(table.columns_count(), 3);

    assert!(table.column_by_name("sku").is_some());
    assert!(table.column_by_name("description").is_some());
    assert!(table.column_by_name("price").is_some());
}

#[test]
fn schema_update() {
    let mut field_event = field_event(vec![field_sku()]);

    let mut schema = SchemaCache::new(KEYSPACE);
    assert_eq!(schema.tables.len(), 0);

    schema.process_field_event(&field_event);
    assert_eq!(schema.tables.len(), 1);

    let table = schema.tables.get(TABLE).unwrap();
    assert_eq!(table.columns_count(), 1);

    field_event.fields.push(field_description());

    schema.process_field_event(&field_event);
    assert_eq!(schema.tables.len(), 1);

    let table = schema.tables.get(TABLE).unwrap();
    assert_eq!(table.columns_count(), 2);
}
