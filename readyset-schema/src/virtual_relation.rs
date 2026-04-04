use std::any::Any;
use std::fmt::{self, Debug};
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::builder::{
    BinaryBuilder, BooleanBuilder, Float32Builder, Float64Builder, Int32Builder, Int64Builder,
    StringBuilder, UInt32Builder, UInt64Builder,
};
use datafusion::arrow::array::{ArrayBuilder, RecordBatch, make_builder};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::{Session, TableProvider};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::TableType;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use futures::future::Future;
use linkme::distributed_slice;

use readyset_data::{DfType, DfValue};
use readyset_errors::{ReadySetResult, internal_err};
use readyset_sql::Dialect;

use crate::replication_lag_vrel::ReplicationLagInfo;
use crate::shallow_vrels::ShallowInfo;

/// The rows yielded by a vrel read.
pub type VrelRows = Box<dyn Iterator<Item = Vec<DfValue>> + Send>;

/// A future that produces an iterator over the vrel's rows.
pub type VrelRead = Pin<Box<dyn Future<Output = ReadySetResult<VrelRows>> + Send>>;

/// Context passed into the function that starts a vrel read.
pub struct VrelContext {
    pub dialect: Dialect,
    pub shallow: Arc<dyn ShallowInfo>,
    pub repl_lag: Arc<dyn ReplicationLagInfo>,
}

/// A function that produces a new vrel read.
type VrelReadDispatch = fn(&VrelContext) -> VrelRead;

/// A virtual relation registration entry, discovered via linker set.
pub struct VrelDefinition {
    pub name: &'static str,
    pub columns: &'static [(&'static str, DfType)],
    pub do_read: VrelReadDispatch,
}

/// The set of vrel registrations.
#[distributed_slice]
pub static VREL: [VrelDefinition];

/// Register a virtual relation (vrel) into the Readyset schema.
///
/// This macro can be invoked from any linked file.  Each registration specifies the name of the
/// vrel, its column schema, and a function that will later be invoked when the vrel is read.  The
/// read function is passed a `VrelContext` and must return a `VrelRead`, which is a future that
/// produces an iterator over rows of Vec<DfValue>.  If your vrel needs additional state during
/// this construction, consider adding state to the `VrelContext`.
///
/// A simple example:
/// ```ignore
/// const SOME_VREL_SCHEMA: &[(&str, DfType)] = &[
///     ("id", DfType::UnsignedBigInt),
///     ("name", DfType::DEFAULT_TEXT),
///     ("note", DfType::DEFAULT_TEXT),
/// ];
///
/// fn some_vrel_read(ctx: &VrelContext) -> VrelRead {
///     let dialect = ctx.dialect;
///     Box::pin(async move {
///         let rows = vec![
///             vec![
///                 DfValue::UnsignedInt(1),
///                 DfValue::from("alice"),
///                 DfValue::from(format!("speaking {dialect}")),
///             ],
///             vec![
///                 DfValue::UnsignedInt(2),
///                 DfValue::from("bob"),
///                 DfValue::from(format!("speaking {dialect}")),
///             ],
///         ];
///         let rows: VrelRows = Box::new(rows.into_iter());
///         Ok(rows)
///     })
/// }
/// bind_vrel!(some_vrel, SOME_VREL_SCHEMA, some_vrel_read);
/// ```
///
/// Selecting the vrel would look something like this:
/// ```text
/// readyset-mysql> SELECT * FROM readyset.some_vrel;
/// +------+-------+----------------+
/// | id   | name  | note           |
/// +------+-------+----------------+
/// |    1 | alice | speaking MySQL |
/// |    2 | bob   | speaking MySQL |
/// +------+-------+----------------+
/// 2 rows in set (0.03 sec)
/// ```
#[macro_export]
macro_rules! bind_vrel {
    ($name:ident, $columns:expr, $read_fn:path) => {
        const _: () = {
            #[::linkme::distributed_slice($crate::virtual_relation::VREL)]
            static ENTRY: $crate::virtual_relation::VrelDefinition =
                $crate::virtual_relation::VrelDefinition {
                    name: stringify!($name),
                    columns: $columns,
                    do_read: $read_fn,
                };
        };
    };
}

/// Create and return the registered vrels.
pub(crate) fn init_vrels(ctx: &Arc<VrelContext>) -> Vec<(&'static str, Arc<dyn TableProvider>)> {
    VREL.iter()
        .map(|reg| {
            let arrow_schema = build_arrow_schema(reg.columns);
            let provider: Arc<dyn TableProvider> = Arc::new(VrelTableProvider::new(
                arrow_schema,
                reg.do_read,
                Arc::clone(ctx),
            ));
            (reg.name, provider)
        })
        .collect()
}

/// Build an Arrow schema from `(name, DfType)` column definitions.
fn build_arrow_schema(columns: &[(&str, DfType)]) -> SchemaRef {
    let fields: Vec<Field> = columns
        .iter()
        .map(|(name, dftype)| Field::new(*name, dftype_to_arrow(dftype), true))
        .collect();
    Arc::new(Schema::new(fields))
}

/// Convert a `DfType` to the equivalent Arrow `DataType`.
fn dftype_to_arrow(dftype: &DfType) -> DataType {
    match dftype {
        DfType::TinyInt | DfType::SmallInt | DfType::MediumInt | DfType::Int => DataType::Int32,
        DfType::BigInt => DataType::Int64,
        DfType::UnsignedTinyInt
        | DfType::UnsignedSmallInt
        | DfType::UnsignedMediumInt
        | DfType::UnsignedInt => DataType::UInt32,
        DfType::UnsignedBigInt => DataType::UInt64,
        DfType::Float => DataType::Float32,
        DfType::Double => DataType::Float64,
        DfType::Bool => DataType::Boolean,
        DfType::Blob | DfType::Binary(_) | DfType::VarBinary(_) => DataType::Binary,
        DfType::Text(_) | DfType::VarChar(_, _) | DfType::Char(_, _) => DataType::Utf8,
        // Fallback: represent as UTF-8 string
        _ => DataType::Utf8,
    }
}

/// Append a single `DfValue` to the appropriate Arrow `ArrayBuilder`.
fn append_dfvalue(
    builder: &mut dyn ArrayBuilder,
    val: DfValue,
    dt: &DataType,
) -> Result<(), DataFusionError> {
    macro_rules! downcast {
        ($builder_ty:ty) => {
            builder
                .as_any_mut()
                .downcast_mut::<$builder_ty>()
                .ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "expected {} for {dt:?}",
                        stringify!($builder_ty)
                    ))
                })
        };
    }

    match dt {
        DataType::Int32 => {
            let b = downcast!(Int32Builder)?;
            match val {
                DfValue::None => b.append_null(),
                DfValue::Int(v) => b.append_value(v as i32),
                DfValue::UnsignedInt(v) => b.append_value(v as i32),
                other => {
                    return Err(DataFusionError::Execution(format!(
                        "cannot convert {other:?} to Int32"
                    )));
                }
            }
        }
        DataType::Int64 => {
            let b = downcast!(Int64Builder)?;
            match val {
                DfValue::None => b.append_null(),
                DfValue::Int(v) => b.append_value(v),
                DfValue::UnsignedInt(v) => b.append_value(v as i64),
                other => {
                    return Err(DataFusionError::Execution(format!(
                        "cannot convert {other:?} to Int64"
                    )));
                }
            }
        }
        DataType::UInt32 => {
            let b = downcast!(UInt32Builder)?;
            match val {
                DfValue::None => b.append_null(),
                DfValue::UnsignedInt(v) => b.append_value(v as u32),
                DfValue::Int(v) => b.append_value(v as u32),
                other => {
                    return Err(DataFusionError::Execution(format!(
                        "cannot convert {other:?} to UInt32"
                    )));
                }
            }
        }
        DataType::UInt64 => {
            let b = downcast!(UInt64Builder)?;
            match val {
                DfValue::None => b.append_null(),
                DfValue::UnsignedInt(v) => b.append_value(v),
                DfValue::Int(v) => b.append_value(v as u64),
                other => {
                    return Err(DataFusionError::Execution(format!(
                        "cannot convert {other:?} to UInt64"
                    )));
                }
            }
        }
        DataType::Float32 => {
            let b = downcast!(Float32Builder)?;
            match val {
                DfValue::None => b.append_null(),
                DfValue::Float(v) => b.append_value(v),
                DfValue::Double(v) => b.append_value(v as f32),
                DfValue::Int(v) => b.append_value(v as f32),
                other => {
                    return Err(DataFusionError::Execution(format!(
                        "cannot convert {other:?} to Float32"
                    )));
                }
            }
        }
        DataType::Float64 => {
            let b = downcast!(Float64Builder)?;
            match val {
                DfValue::None => b.append_null(),
                DfValue::Double(v) => b.append_value(v),
                DfValue::Float(v) => b.append_value(v as f64),
                DfValue::Int(v) => b.append_value(v as f64),
                other => {
                    return Err(DataFusionError::Execution(format!(
                        "cannot convert {other:?} to Float64"
                    )));
                }
            }
        }
        DataType::Boolean => {
            let b = downcast!(BooleanBuilder)?;
            match val {
                DfValue::None => b.append_null(),
                DfValue::Int(v) => b.append_value(v != 0),
                DfValue::UnsignedInt(v) => b.append_value(v != 0),
                other => {
                    return Err(DataFusionError::Execution(format!(
                        "cannot convert {other:?} to Boolean"
                    )));
                }
            }
        }
        DataType::Binary => {
            let b = downcast!(BinaryBuilder)?;
            match val {
                DfValue::None => b.append_null(),
                DfValue::ByteArray(v) => b.append_value(v.as_slice()),
                other => {
                    return Err(DataFusionError::Execution(format!(
                        "cannot convert {other:?} to Binary"
                    )));
                }
            }
        }
        _ => {
            // Fallback: convert everything to string representation
            let b = downcast!(StringBuilder)?;
            match val {
                DfValue::None => b.append_null(),
                DfValue::Text(v) => b.append_value(v.as_str()),
                DfValue::TinyText(v) => b.append_value(v.as_str()),
                DfValue::Int(v) => b.append_value(v.to_string()),
                DfValue::UnsignedInt(v) => b.append_value(v.to_string()),
                DfValue::Float(v) => b.append_value(v.to_string()),
                DfValue::Double(v) => b.append_value(v.to_string()),
                other => b.append_value(format!("{other:?}")),
            }
        }
    }
    Ok(())
}

/// Build a `RecordBatch` from an iterator of rows.  Applies an optional projection and limit.
fn build_record_batch(
    schema: &SchemaRef,
    projection: Option<&Vec<usize>>,
    limit: Option<usize>,
    rows: VrelRows,
) -> Result<RecordBatch, DataFusionError> {
    let num_cols = schema.fields().len();
    let projected_schema = if let Some(proj) = projection {
        Arc::new(schema.project(proj)?)
    } else {
        Arc::clone(schema)
    };

    // Map from source column index to its position in the builders vec, or None if skipped.
    let col_map: Vec<Option<usize>> = (0..num_cols)
        .map(|i| {
            if let Some(proj) = projection {
                proj.iter().position(|&p| p == i)
            } else {
                Some(i)
            }
        })
        .collect();

    let mut builders: Vec<Box<dyn ArrayBuilder>> = projected_schema
        .fields()
        .iter()
        .map(|f| make_builder(f.data_type(), 0))
        .collect();

    for (row_count, row) in rows.enumerate() {
        if limit.is_some_and(|l| row_count >= l) {
            break;
        }
        if row.len() != num_cols {
            return Err(DataFusionError::External(Box::new(internal_err!(
                "vrel row has {} columns, schema expects {num_cols}",
                row.len()
            ))));
        }
        for (col_idx, col) in row.into_iter().enumerate() {
            if let Some(builder_idx) = col_map[col_idx] {
                append_dfvalue(
                    builders[builder_idx].as_mut(),
                    col,
                    projected_schema.field(builder_idx).data_type(),
                )?;
            }
        }
    }

    let columns = builders.iter_mut().map(|b| b.finish()).collect();
    Ok(RecordBatch::try_new(projected_schema, columns)?)
}

/// The implementation of a vrel to hand to DataFusion.
struct VrelTableProvider {
    schema: SchemaRef,
    do_read: VrelReadDispatch,
    ctx: Arc<VrelContext>,
}

impl Debug for VrelTableProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VrelTableProvider")
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
}

impl VrelTableProvider {
    fn new(schema: SchemaRef, do_read: VrelReadDispatch, ctx: Arc<VrelContext>) -> Self {
        Self {
            schema,
            do_read,
            ctx,
        }
    }
}

#[async_trait]
impl TableProvider for VrelTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[datafusion::logical_expr::Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        Ok(Arc::new(VrelExecutionPlan::new(
            Arc::clone(&self.ctx),
            Arc::clone(&self.schema),
            self.do_read,
            projection.cloned(),
            limit,
        )?))
    }
}

/// An `ExecutionPlan` that calls do_read on `execute()` to produce rows.
struct VrelExecutionPlan {
    ctx: Arc<VrelContext>,
    schema: SchemaRef,
    do_read: VrelReadDispatch,
    projection: Option<Vec<usize>>,
    limit: Option<usize>,
    properties: PlanProperties,
}

impl Debug for VrelExecutionPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VrelExecutionPlan")
            .field("schema", &self.schema)
            .field("projection", &self.projection)
            .field("limit", &self.limit)
            .finish_non_exhaustive()
    }
}

impl VrelExecutionPlan {
    fn new(
        ctx: Arc<VrelContext>,
        schema: SchemaRef,
        do_read: VrelReadDispatch,
        projection: Option<Vec<usize>>,
        limit: Option<usize>,
    ) -> Result<Self, DataFusionError> {
        let projected_schema = if let Some(ref proj) = projection {
            Arc::new(schema.project(proj)?)
        } else {
            Arc::clone(&schema)
        };

        let eq_properties = EquivalenceProperties::new(Arc::clone(&projected_schema));
        let properties = PlanProperties::new(
            eq_properties,
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );

        Ok(Self {
            ctx,
            schema,
            do_read,
            projection,
            limit,
            properties,
        })
    }
}

impl DisplayAs for VrelExecutionPlan {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "VrelExecutionPlan")
    }
}

impl ExecutionPlan for VrelExecutionPlan {
    fn name(&self) -> &str {
        "VrelExecutionPlan"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        if !children.is_empty() {
            return Err(DataFusionError::Plan(
                "VrelExecutionPlan has no children".into(),
            ));
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        if partition != 0 {
            return Err(DataFusionError::Execution(format!(
                "VrelExecutionPlan only supports 1 partition, got {partition}"
            )));
        }

        let read = (self.do_read)(&self.ctx);
        let schema = Arc::clone(&self.schema);
        let projection = self.projection.clone();
        let limit = self.limit;

        let projected_schema = if let Some(ref proj) = projection {
            Arc::new(schema.project(proj)?)
        } else {
            Arc::clone(&schema)
        };

        let stream = futures::stream::once(async move {
            let rows = read
                .await
                .map_err(|e| DataFusionError::Execution(format!("vrel do_read error: {e}")))?;

            build_record_batch(&schema, projection.as_ref(), limit, rows)
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            projected_schema,
            stream,
        )))
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{Array, Int32Array, StringArray};

    use readyset_client::query::QueryId;
    use readyset_shallow::{CacheEntryInfo, CacheInfo};
    use readyset_sql::Dialect;
    use readyset_sql::ast::Relation;

    use super::*;

    #[test]
    fn test_dftype_to_arrow() {
        assert_eq!(dftype_to_arrow(&DfType::Int), DataType::Int32);
        assert_eq!(dftype_to_arrow(&DfType::BigInt), DataType::Int64);
        assert_eq!(dftype_to_arrow(&DfType::UnsignedInt), DataType::UInt32);
        assert_eq!(dftype_to_arrow(&DfType::UnsignedBigInt), DataType::UInt64);
        assert_eq!(dftype_to_arrow(&DfType::Float), DataType::Float32);
        assert_eq!(dftype_to_arrow(&DfType::Double), DataType::Float64);
        assert_eq!(dftype_to_arrow(&DfType::Bool), DataType::Boolean);
        assert_eq!(dftype_to_arrow(&DfType::Blob), DataType::Binary);
    }

    #[test]
    fn test_dfvalue_to_arrow_int32() {
        let rows = vec![
            vec![DfValue::Int(1)],
            vec![DfValue::Int(2)],
            vec![DfValue::None],
        ];
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, true)]));
        let batch = build_record_batch(&schema, None, None, Box::new(rows.into_iter()))
            .expect("conversion succeeds");
        assert_eq!(batch.num_rows(), 3);
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("is Int32Array");
        assert_eq!(col.value(0), 1);
        assert_eq!(col.value(1), 2);
        assert!(col.is_null(2));
    }

    #[test]
    fn test_dfvalue_to_arrow_utf8() {
        let rows = vec![
            vec![DfValue::from("hello")],
            vec![DfValue::None],
            vec![DfValue::Int(42)],
        ];
        let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, true)]));
        let batch = build_record_batch(&schema, None, None, Box::new(rows.into_iter()))
            .expect("conversion succeeds");
        assert_eq!(batch.num_rows(), 3);
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("is StringArray");
        assert_eq!(col.value(0), "hello");
        assert!(col.is_null(1));
        assert_eq!(col.value(2), "42");
    }

    #[test]
    fn test_build_record_batch_with_projection() {
        let rows = vec![
            vec![
                DfValue::Int(1),
                DfValue::from("alice"),
                DfValue::Double(1.5),
            ],
            vec![DfValue::Int(2), DfValue::from("bob"), DfValue::Double(2.5)],
        ];
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("score", DataType::Float64, true),
        ]));
        // Project only columns 0 and 2
        let proj = vec![0, 2];
        let batch = build_record_batch(&schema, Some(&proj), None, Box::new(rows.into_iter()))
            .expect("conversion succeeds");
        assert_eq!(batch.num_columns(), 2);
        assert_eq!(batch.schema().field(0).name(), "id");
        assert_eq!(batch.schema().field(1).name(), "score");
    }

    const TEST_USERS_SCHEMA: &[(&str, DfType)] =
        &[("id", DfType::BigInt), ("name", DfType::DEFAULT_TEXT)];

    fn test_users_read(_ctx: &VrelContext) -> VrelRead {
        Box::pin(async {
            let rows: VrelRows = Box::new(
                vec![
                    vec![DfValue::Int(1), DfValue::from("alice")],
                    vec![DfValue::Int(2), DfValue::from("bob")],
                    vec![DfValue::Int(3), DfValue::from("charlie")],
                ]
                .into_iter(),
            );
            Ok(rows)
        })
    }
    bind_vrel!(test_users, TEST_USERS_SCHEMA, test_users_read);

    #[tokio::test]
    async fn test_vrel_query_through_datafusion() {
        struct NoopShallow;
        impl crate::ShallowInfo for NoopShallow {
            fn list_caches(
                &self,
                _query_id: Option<QueryId>,
                _name: Option<&Relation>,
            ) -> Vec<CacheInfo> {
                vec![]
            }
            fn list_entries(
                &self,
                _query_id: Option<QueryId>,
                _limit: Option<usize>,
            ) -> Vec<CacheEntryInfo> {
                vec![]
            }
        }

        let shallow = Arc::new(NoopShallow);
        let no_lag = Arc::new(crate::replication_lag_vrel::NoReplicationLag);
        let schema = crate::ReadysetSchema::init("test_db", Dialect::MySQL, &shallow, &no_lag)
            .expect("init succeeds");

        let session = schema.session();

        // Basic select
        let result = session
            .query("SELECT * FROM test_users")
            .await
            .expect("query");
        let mut iter = result.borrowed_iter();
        let mut count = 0;
        while iter.next_row().is_some() {
            count += 1;
        }
        assert_eq!(count, 3);

        // Select with limit
        let result = session
            .query("SELECT * FROM test_users LIMIT 2")
            .await
            .expect("query with limit");
        let mut iter = result.borrowed_iter();
        let mut count = 0;
        while iter.next_row().is_some() {
            count += 1;
        }
        assert_eq!(count, 2);

        // Select with filter
        let result = session
            .query("SELECT name FROM test_users WHERE id = 2")
            .await
            .expect("query with filter");
        let mut iter = result.borrowed_iter();
        let (cols, row_idx) = iter.next_row().expect("one row");
        let col: Vec<_> = cols.collect();
        let name = col[0]
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("is StringArray");
        assert_eq!(name.value(row_idx), "bob");
        assert!(iter.next_row().is_none());
    }
}
