use nom_sql::{self, ColumnSpecification, CreateTableStatement};

#[derive(Debug)]
pub enum Schema {
    Table(CreateTableStatement),
    View(Vec<ColumnSpecification>),
}

pub(crate) fn convert_schema(schema: &Schema) -> Vec<msql_srv::Column> {
    match schema {
        Schema::Table(CreateTableStatement { ref fields, .. }) | Schema::View(ref fields) => {
            fields.iter().map(|c| c.convert_column()).collect()
        }
    }
}

pub(crate) fn schema_for_column(schema: &Schema, c: &nom_sql::Column) -> msql_srv::Column {
    if let Some(ref table) = &c.table {
        match schema {
            Schema::Table(CreateTableStatement { ref fields, .. }) => {
                let colspec = fields
                    .iter()
                    .find(|cc| cc.column.name == c.name)
                    .unwrap_or_else(|| panic!("column {} not found", c.name));

                assert_eq!(colspec.column.name, c.name);

                colspec.convert_column()
            }
            Schema::View(ref fields) => fields
                .iter()
                .find(|cs| cs.column == *c)
                .unwrap_or_else(|| panic!("column {} not found in view {}", c.name, table))
                .convert_column(),
        }
    } else {
        // no table specified on column
        msql_srv::Column {
            table: "".into(),
            column: c.name.clone(),
            coltype: msql_srv::ColumnType::MYSQL_TYPE_LONG,
            colflags: msql_srv::ColumnFlags::empty(),
        }
    }
}
