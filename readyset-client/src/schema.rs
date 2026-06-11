use std::borrow::Cow;
use std::collections::HashMap;

use readyset_data::{Collation, DfType, Dialect};
use readyset_errors::{internal_err, ReadySetResult};
use readyset_sql::ast::{
    Column, ColumnConstraint, ColumnSpecification, Relation, SqlIdentifier, SqlType,
};
use serde::{Deserialize, Serialize};

/// Identifies the source base table column for a projected column
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnBase {
    /// The name of the column in the base table
    pub column: SqlIdentifier,
    /// The name of the base table for this column
    pub table: Relation,
    /// A list of constraints on the column
    pub constraints: Vec<ColumnConstraint>,
    /// If known, the PostgreSQL OID for the column's base table
    pub table_oid: Option<u32>,
    /// If known, the PostgreSQL `attnum` for the column
    pub attnum: Option<i16>,
    /// Original SQL type
    pub sql_type: SqlType,
}

impl ColumnBase {
    pub fn has_default(&self) -> bool {
        self.constraints
            .iter()
            .any(|c| matches!(c, ColumnConstraint::DefaultValue(_)))
    }

    pub fn is_not_null(&self) -> bool {
        self.constraints
            .iter()
            .any(|c| matches!(c, ColumnConstraint::NotNull))
    }
}

/// Combines the specification for a columns with its base name
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnSchema {
    /// The name of the column
    pub column: Column,
    /// The column's type
    pub column_type: DfType,
    /// If the column is an alias, this field represents its base column
    pub base: Option<ColumnBase>,
}

impl ColumnSchema {
    /// Create a new ColumnSchema from a ColumnSpecification representing a column directly in a
    /// base table with the given name.
    pub fn from_base(
        spec: ColumnSpecification,
        table: Relation,
        dialect: Dialect,
    ) -> ReadySetResult<Self> {
        let collation = spec
            .get_collation()
            .map(|name| Collation::get_or_default(dialect, name));
        Ok(Self {
            base: Some(ColumnBase {
                column: spec.column.name.clone(),
                table,
                constraints: spec.constraints,
                table_oid: None,
                attnum: None,
                sql_type: spec.sql_type.clone(),
            }),
            column: spec.column,
            column_type: DfType::from_sql_type(
                &spec.sql_type,
                dialect,
                |_| None, /* Custom types not allowed for inserts via the adapter */
                collation,
            )?,
        })
    }

    /// Consume the schema, returning the type for the column
    pub fn into_type(self) -> DfType {
        self.column_type
    }
}

/// Schema describing the columns of a `SELECT` result set returned to the client.
#[derive(Debug, Clone)]
pub struct SelectSchema<'a> {
    pub schema: Cow<'a, [ColumnSchema]>,
    pub columns: Cow<'a, [SqlIdentifier]>,
}

impl SelectSchema<'_> {
    pub fn into_owned(self) -> SelectSchema<'static> {
        SelectSchema {
            schema: Cow::Owned(self.schema.into_owned()),
            columns: Cow::Owned(self.columns.into_owned()),
        }
    }
}

/// A `ViewSchema` is used to describe the columns of a stored ReadySet
/// view as a vector of columns. The ViewSchema contains a vector with all
/// projected columns and a vector with columns returned to the client.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ViewSchema {
    /// The set of columns returned to the client when executing this query.
    returned_cols: Vec<ColumnSchema>,
    /// The set of columns projected at the noria flowgraph reader node.
    projected_cols: Vec<ColumnSchema>,
}

/// SchemaType is passed to most ViewSchema functions to select between the two
/// schemas contained in the ViewSchema struct.
pub enum SchemaType {
    /// Used to select the schema returned to the client when executing this
    /// query.
    ReturnedSchema,
    /// Used to select the schema projected at the noria flowgraph reader node.
    ProjectedSchema,
}

impl ViewSchema {
    /// Create a ViewSchema from returned and projected column schema vectors
    pub fn new(returned_cols: Vec<ColumnSchema>, projected_cols: Vec<ColumnSchema>) -> ViewSchema {
        ViewSchema {
            returned_cols,
            projected_cols,
        }
    }

    /// Get the schema specified by the schema type
    pub fn schema(&self, schema_type: SchemaType) -> &[ColumnSchema] {
        match schema_type {
            SchemaType::ReturnedSchema => &self.returned_cols,
            SchemaType::ProjectedSchema => &self.projected_cols,
        }
    }

    /// Return a vector specifying the types of the columns for the requested indices
    pub fn col_types<I>(&self, indices: I, schema_type: SchemaType) -> ReadySetResult<Vec<&DfType>>
    where
        I: IntoIterator<Item = usize>,
    {
        let schema = self.schema(schema_type);
        indices
            .into_iter()
            .map(|i| schema.get(i).map(|c| &c.column_type))
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| internal_err!("Schema expects valid column indices"))
    }

    /// Convert the given iterator [`Columns`] to a `Vec` of [`ColumnSchema`]. The columns match if
    /// either the column name matches (the alias) or the real base name
    pub fn to_cols<'a, 'b, T>(
        &'a self,
        cols: T,
        schema_type: SchemaType,
    ) -> ReadySetResult<Vec<&'a ColumnSchema>>
    where
        T: IntoIterator<Item = &'b Column>,
    {
        let mut by_name = HashMap::new();
        let mut by_base_name = HashMap::new();
        for cs in self.schema(schema_type) {
            by_name.insert(&cs.column.name, cs);
            if let Some(base) = &cs.base {
                by_base_name.insert(&base.column, cs);
            }
        }

        cols.into_iter()
            .map(move |c| {
                by_name
                    .get(&c.name)
                    .or_else(|| by_base_name.get(&c.name))
                    .copied()
                    .ok_or_else(|| internal_err!("Column {} not found", c.display_unquoted()))
            })
            .collect()
    }

    /// Get the indices of the columns in the schema that correspond to the list of provided
    /// [`readyset_sql::ast::Column`]. The columns match if either the column name matches (the
    /// alias) or the real base name
    pub fn indices_for_cols<'a, T>(
        &self,
        cols: T,
        schema_type: SchemaType,
    ) -> ReadySetResult<Vec<usize>>
    where
        T: Iterator<Item = &'a Column>,
    {
        let schema = self.schema(schema_type);

        cols.map(|c| {
            schema.iter().position(|e| {
                e.column.name == c.name
                    || e.base.as_ref().map(|b| b.column == c.name).unwrap_or(false)
            })
        })
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| internal_err!("Schema expects all columns to be present"))
    }
}
