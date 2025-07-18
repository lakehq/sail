use std::any::Any;
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::fmt::{self, Debug};
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, TimeZone};
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{
    DataType as ArrowDataType, Field, Schema as ArrowSchema, SchemaRef,
    SchemaRef as ArrowSchemaRef, TimeUnit,
};
use datafusion::catalog::memory::DataSourceExec;
use datafusion::catalog::Session;
use datafusion::common::config::ConfigOptions;
use datafusion::common::scalar::ScalarValue;
use datafusion::common::stats::Statistics;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{
    Column, DFSchema, DataFusionError, Result as DataFusionResult, ToDFSchema,
};
use datafusion::config::TableParquetOptions;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::physical_plan::{
    wrap_partition_type_in_dict, wrap_partition_value_in_dict, FileGroup, FileScanConfigBuilder,
    FileSource, ParquetSource,
};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::execution::context::{SessionContext, TaskContext};
use datafusion::logical_expr::execution_props::ExecutionProps;
use datafusion::logical_expr::simplify::SimplifyContext;
use datafusion::logical_expr::utils::conjunction;
use datafusion::logical_expr::{
    BinaryExpr, Expr, LogicalPlan, Operator, TableProviderFilterPushDown,
};
use datafusion::optimizer::simplify_expressions::ExprSimplifier;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder, MetricsSet};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use deltalake::errors::{DeltaResult, DeltaTableError};
use deltalake::kernel::{Add, EagerSnapshot, Snapshot};
use deltalake::logstore::LogStoreRef;
use deltalake::table::state::DeltaTableState;
use object_store::ObjectMeta;
use serde::{Deserialize, Serialize};
use url::Url;

use crate::delta_datafusion::schema_adapter::DeltaSchemaAdapterFactory;
/// [Credit]: <https://github.com/delta-io/delta-rs/blob/3607c314cbdd2ad06c6ee0677b92a29f695c71f3/crates/core/src/delta_datafusion/mod.rs>
pub(crate) const PATH_COLUMN: &str = "__delta_rs_path";

// pub mod cdf;

mod schema_adapter;

/// Convert DeltaTableError to DataFusionError
pub fn delta_to_datafusion_error(err: DeltaTableError) -> DataFusionError {
    match err {
        DeltaTableError::Arrow { source } => DataFusionError::ArrowError(source, None),
        DeltaTableError::Io { source } => DataFusionError::IoError(source),
        DeltaTableError::ObjectStore { source } => DataFusionError::ObjectStore(source),
        DeltaTableError::Parquet { source } => DataFusionError::ParquetError(source),
        _ => DataFusionError::External(Box::new(err)),
    }
}

/// Convert DataFusionError to DeltaTableError
pub fn datafusion_to_delta_error(err: DataFusionError) -> DeltaTableError {
    match err {
        DataFusionError::ArrowError(source, _) => DeltaTableError::Arrow { source },
        DataFusionError::IoError(source) => DeltaTableError::Io { source },
        DataFusionError::ObjectStore(source) => DeltaTableError::ObjectStore { source },
        DataFusionError::ParquetError(source) => DeltaTableError::Parquet { source },
        _ => DeltaTableError::Generic(err.to_string()),
    }
}

fn create_object_store_url(location: &Url) -> ObjectStoreUrl {
    use object_store::path::DELIMITER;
    ObjectStoreUrl::parse(format!(
        "delta-rs://{}-{}{}",
        location.scheme(),
        location.host_str().unwrap_or("-"),
        location.path().replace(DELIMITER, "-").replace(':', "-")
    ))
    .expect("Invalid object store url.")
}

/// Convenience trait for calling common methods on snapshot hierarchies
pub trait DataFusionMixins {
    /// The physical datafusion schema of a table
    fn arrow_schema(&self) -> DeltaResult<ArrowSchemaRef>;

    /// Get the table schema as an [`ArrowSchemaRef`]
    fn input_schema(&self) -> DeltaResult<ArrowSchemaRef>;
}

impl DataFusionMixins for Snapshot {
    fn arrow_schema(&self) -> DeltaResult<ArrowSchemaRef> {
        arrow_schema_impl(self, true)
    }

    fn input_schema(&self) -> DeltaResult<ArrowSchemaRef> {
        arrow_schema_impl(self, false)
    }
}

impl DataFusionMixins for EagerSnapshot {
    fn arrow_schema(&self) -> DeltaResult<ArrowSchemaRef> {
        arrow_schema_from_struct_type(self.schema(), self.metadata().partition_columns(), true)
    }

    fn input_schema(&self) -> DeltaResult<ArrowSchemaRef> {
        arrow_schema_from_struct_type(self.schema(), self.metadata().partition_columns(), false)
    }
}

impl DataFusionMixins for DeltaTableState {
    fn arrow_schema(&self) -> DeltaResult<ArrowSchemaRef> {
        self.snapshot().arrow_schema()
    }

    fn input_schema(&self) -> DeltaResult<ArrowSchemaRef> {
        self.snapshot().input_schema()
    }
}

fn arrow_schema_from_snapshot(
    snapshot: &Snapshot,
    wrap_partitions: bool,
) -> DeltaResult<ArrowSchemaRef> {
    let meta = snapshot.metadata();
    let schema = snapshot.schema();

    let fields = schema
        .fields()
        .filter(|f| !meta.partition_columns().contains(&f.name().to_string()))
        .map(|f| {
            let field_name = f.name().to_string();
            let field_type = arrow_type_from_delta_type(f.data_type())?;
            Ok(Field::new(field_name, field_type, f.is_nullable()))
        })
        .chain(meta.partition_columns().iter().map(|partition_col| {
            let f = schema
                .field(partition_col)
                .expect("Partition column should exist in schema");
            let field_name = f.name().to_string();
            let field_type = arrow_type_from_delta_type(f.data_type())?;
            let field = Field::new(field_name, field_type, f.is_nullable());
            let corrected = if wrap_partitions {
                match field.data_type() {
                    // Only dictionary-encode types that may be large
                    // https://github.com/apache/arrow-datafusion/pull/5545
                    ArrowDataType::Utf8
                    | ArrowDataType::LargeUtf8
                    | ArrowDataType::Binary
                    | ArrowDataType::LargeBinary => {
                        wrap_partition_type_in_dict(field.data_type().clone())
                    }
                    _ => field.data_type().clone(),
                }
            } else {
                field.data_type().clone()
            };
            Ok(field.with_data_type(corrected))
        }))
        .collect::<Result<Vec<Field>, DeltaTableError>>()?;

    Ok(Arc::new(ArrowSchema::new(fields)))
}

fn arrow_schema_from_struct_type(
    schema: &deltalake::kernel::StructType,
    partition_columns: &[String],
    wrap_partitions: bool,
) -> DeltaResult<ArrowSchemaRef> {
    let fields = schema
        .fields()
        .filter(|f| !partition_columns.contains(&f.name().to_string()))
        .map(|f| {
            // Convert StructField to Arrow Field
            let field_name = f.name().to_string();
            let field_type = arrow_type_from_delta_type(f.data_type())?;
            Ok(Field::new(field_name, field_type, f.is_nullable()))
        })
        .chain(
            // We need stable order between logical and physical schemas, but the order of
            // partitioning columns is not always the same in the json schema and the array
            partition_columns.iter().map(|partition_col| {
                let f = schema
                    .field(partition_col)
                    .expect("Partition column should exist in schema");
                let field_name = f.name().to_string();
                let field_type = arrow_type_from_delta_type(f.data_type())?;
                let field = Field::new(field_name, field_type, f.is_nullable());
                let corrected = if wrap_partitions {
                    match field.data_type() {
                        // Only dictionary-encode types that may be large
                        // https://github.com/apache/arrow-datafusion/pull/5545
                        ArrowDataType::Utf8
                        | ArrowDataType::LargeUtf8
                        | ArrowDataType::Binary
                        | ArrowDataType::LargeBinary => {
                            wrap_partition_type_in_dict(field.data_type().clone())
                        }
                        _ => field.data_type().clone(),
                    }
                } else {
                    field.data_type().clone()
                };
                Ok(field.with_data_type(corrected))
            }),
        )
        .collect::<Result<Vec<Field>, DeltaTableError>>()?;

    Ok(Arc::new(ArrowSchema::new(fields)))
}

fn arrow_type_from_delta_type(
    delta_type: &deltalake::kernel::DataType,
) -> DeltaResult<ArrowDataType> {
    use deltalake::kernel::DataType as DeltaType;

    Ok(match delta_type {
        DeltaType::Primitive(primitive) => {
            use deltalake::kernel::PrimitiveType;
            match primitive {
                PrimitiveType::String => ArrowDataType::Utf8,
                PrimitiveType::Long => ArrowDataType::Int64,
                PrimitiveType::Integer => ArrowDataType::Int32,
                PrimitiveType::Short => ArrowDataType::Int16,
                PrimitiveType::Byte => ArrowDataType::Int8,
                PrimitiveType::Float => ArrowDataType::Float32,
                PrimitiveType::Double => ArrowDataType::Float64,
                PrimitiveType::Boolean => ArrowDataType::Boolean,
                PrimitiveType::Binary => ArrowDataType::Binary,
                PrimitiveType::Date => ArrowDataType::Date32,
                PrimitiveType::Timestamp => ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
                PrimitiveType::TimestampNtz => {
                    ArrowDataType::Timestamp(TimeUnit::Microsecond, None)
                }
                PrimitiveType::Decimal(decimal_type) => {
                    ArrowDataType::Decimal128(decimal_type.precision(), decimal_type.scale() as i8)
                }
            }
        }
        DeltaType::Array(array_type) => {
            let element_type = arrow_type_from_delta_type(array_type.element_type())?;
            ArrowDataType::List(Arc::new(Field::new(
                "element",
                element_type,
                array_type.contains_null(),
            )))
        }
        DeltaType::Map(map_type) => {
            let key_type = arrow_type_from_delta_type(map_type.key_type())?;
            let value_type = arrow_type_from_delta_type(map_type.value_type())?;
            ArrowDataType::Map(
                Arc::new(Field::new(
                    "entries",
                    ArrowDataType::Struct(
                        vec![
                            Arc::new(Field::new("key", key_type, false)),
                            Arc::new(Field::new(
                                "value",
                                value_type,
                                map_type.value_contains_null(),
                            )),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            )
        }
        DeltaType::Struct(struct_type) => {
            let fields = struct_type
                .fields()
                .map(|f| {
                    let field_type = arrow_type_from_delta_type(f.data_type())?;
                    Ok(Arc::new(Field::new(
                        f.name().to_string(),
                        field_type,
                        f.is_nullable(),
                    )))
                })
                .collect::<Result<Vec<_>, DeltaTableError>>()?;
            ArrowDataType::Struct(fields.into())
        }
    })
}

fn arrow_schema_impl(snapshot: &Snapshot, wrap_partitions: bool) -> DeltaResult<ArrowSchemaRef> {
    arrow_schema_from_snapshot(snapshot, wrap_partitions)
}

pub(crate) fn files_matching_predicate<'a>(
    snapshot: &'a EagerSnapshot,
    filters: &[Expr],
) -> DeltaResult<Box<dyn Iterator<Item = Add> + 'a>> {
    let adds: Vec<Add> = snapshot.file_actions()?.collect();
    if filters.is_empty() {
        return Ok(Box::new(adds.into_iter()));
    }

    // Get partition columns and their schema
    let metadata = snapshot.metadata();
    let partition_columns = metadata.partition_columns();
    if partition_columns.is_empty() {
        // No partition columns, cannot filter based on partitions
        return Ok(Box::new(adds.into_iter()));
    }

    // Create partition schema for evaluation
    let table_schema = snapshot.schema();
    let partition_schema_fields: Vec<Field> = partition_columns
        .iter()
        .filter_map(|col_name| {
            if let Some(field) = table_schema.field(col_name) {
                let field_name = field.name().to_string();
                if let Ok(field_type) = arrow_type_from_delta_type(field.data_type()) {
                    Some(Field::new(field_name, field_type, field.is_nullable()))
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    if partition_schema_fields.is_empty() {
        return Ok(Box::new(adds.into_iter()));
    }

    let partition_schema = Arc::new(ArrowSchema::new(partition_schema_fields));

    // For expression evaluation
    let context = SessionContext::new();
    let df_schema = partition_schema
        .clone()
        .to_dfschema()
        .map_err(|e| DeltaTableError::Generic(e.to_string()))?;

    let physical_exprs: Result<Vec<_>, _> = filters
        .iter()
        .map(|filter| {
            let simplified = simplify_expr(&context, &df_schema, filter.clone());
            Ok(simplified)
        })
        .collect();

    let physical_exprs = physical_exprs.map_err(|e: DeltaTableError| e)?;

    let filtered = adds.into_iter().filter(move |add| {
        let partition_batch =
            match create_partition_batch_for_file(add, partition_columns, &partition_schema) {
                Ok(batch) => batch,
                Err(_) => return true, // Fallback if partition batch creation fails.
            };

        for physical_expr in &physical_exprs {
            match physical_expr.evaluate(&partition_batch) {
                Ok(columnar_value) => match columnar_value {
                    datafusion::logical_expr::ColumnarValue::Array(array) => {
                        if let Some(bool_array) = array
                            .as_any()
                            .downcast_ref::<datafusion::arrow::array::BooleanArray>(
                        ) {
                            if !bool_array.is_empty() && !bool_array.value(0) {
                                return false;
                            }
                        }
                    }
                    datafusion::logical_expr::ColumnarValue::Scalar(scalar) => {
                        if let ScalarValue::Boolean(Some(false)) = scalar {
                            return false;
                        }
                    }
                },
                Err(_) => return true, // Fallback if evaluation fails.
            }
        }
        true
    });

    Ok(Box::new(filtered))
}

/// Create a RecordBatch containing partition values for a single file
fn create_partition_batch_for_file(
    add: &Add,
    partition_columns: &[String],
    schema: &ArrowSchemaRef,
) -> DeltaResult<RecordBatch> {
    use datafusion::arrow::array::ArrayRef;
    use datafusion::arrow::record_batch::RecordBatch;

    let mut arrays: Vec<ArrayRef> = Vec::new();

    for column_name in partition_columns {
        let field = schema
            .field_with_name(column_name)
            .map_err(|e| DeltaTableError::Generic(format!("Field not found: {e}")))?;

        let partition_value = add.partition_values.get(column_name);

        let array: ArrayRef = match partition_value {
            Some(Some(value)) => {
                match to_correct_scalar_value(
                    &serde_json::Value::String(value.clone()),
                    field.data_type(),
                ) {
                    Ok(Some(scalar)) => scalar.to_array_of_size(1).map_err(|e| {
                        DeltaTableError::Generic(format!("Failed to create array: {e}"))
                    })?,
                    Ok(None) => get_null_of_arrow_type(field.data_type())?
                        .to_array_of_size(1)
                        .map_err(|e| {
                            DeltaTableError::Generic(format!("Failed to create null array: {e}"))
                        })?,
                    Err(_) => get_null_of_arrow_type(field.data_type())?
                        .to_array_of_size(1)
                        .map_err(|e| {
                            DeltaTableError::Generic(format!("Failed to create default array: {e}"))
                        })?,
                }
            }
            Some(None) | None => {
                // Null value
                get_null_of_arrow_type(field.data_type())?
                    .to_array_of_size(1)
                    .map_err(|e| {
                        DeltaTableError::Generic(format!("Failed to create null array: {e}"))
                    })?
            }
        };

        arrays.push(array);
    }
    RecordBatch::try_new(schema.clone(), arrays)
        .map_err(|e| DeltaTableError::Generic(format!("Failed to create RecordBatch: {e}")))
}

// Extension trait to add datafusion_table_statistics method to DeltaTableState
trait DeltaTableStateExt {
    fn datafusion_table_statistics(&self) -> Option<Statistics>;
}

impl DeltaTableStateExt for DeltaTableState {
    fn datafusion_table_statistics(&self) -> Option<Statistics> {
        // let log_data = self.snapshot().log_data();
        // log_data.statistics()
        unimplemented!();
    }
}

/// The logical schema for a Deltatable is different from the protocol level schema since partition
/// columns must appear at the end of the schema. This is to align with how partition are handled
/// at the physical level
pub(crate) fn df_logical_schema(
    snapshot: &DeltaTableState,
    file_column_name: &Option<String>,
    schema: Option<ArrowSchemaRef>,
) -> DeltaResult<SchemaRef> {
    let input_schema = match schema {
        Some(schema) => schema,
        None => snapshot.input_schema()?,
    };
    let table_partition_cols = &snapshot.metadata().partition_columns();

    let mut fields: Vec<Arc<Field>> = input_schema
        .fields()
        .iter()
        .filter(|f| !table_partition_cols.contains(f.name()))
        .cloned()
        .collect();

    for partition_col in table_partition_cols.iter() {
        fields.push(Arc::new(
            input_schema
                .field_with_name(partition_col)
                .expect("Partition column should exist in input schema")
                .to_owned(),
        ));
    }

    if let Some(file_column_name) = file_column_name {
        fields.push(Arc::new(Field::new(
            file_column_name,
            ArrowDataType::Utf8,
            true,
        )));
    }

    Ok(Arc::new(ArrowSchema::new(fields)))
}

#[derive(Debug, Clone)]
/// Used to specify if additional metadata columns are exposed to the user
pub struct DeltaScanConfigBuilder {
    /// Include the source path for each record. The name of this column is determined by `file_column_name`
    include_file_column: bool,
    /// Column name that contains the source path.
    ///
    /// If include_file_column is true and the name is None then it will be auto-generated
    /// Otherwise the user provided name will be used
    file_column_name: Option<String>,
    /// Whether to wrap partition values in a dictionary encoding to potentially save space
    wrap_partition_values: Option<bool>,
    /// Whether to push down filter in end result or just prune the files
    enable_parquet_pushdown: bool,
    /// Schema to scan table with
    schema: Option<SchemaRef>,
}

impl Default for DeltaScanConfigBuilder {
    fn default() -> Self {
        DeltaScanConfigBuilder {
            include_file_column: false,
            file_column_name: None,
            wrap_partition_values: None,
            enable_parquet_pushdown: true,
            schema: None,
        }
    }
}

impl DeltaScanConfigBuilder {
    /// Construct a new instance of `DeltaScanConfigBuilder`
    pub fn new() -> Self {
        Self::default()
    }

    /// Indicate that a column containing a records file path is included.
    /// Column name is generated and can be determined once this Config is built
    #[allow(dead_code)]
    pub fn with_file_column(mut self, include: bool) -> Self {
        self.include_file_column = include;
        self.file_column_name = None;
        self
    }

    /// Indicate that a column containing a records file path is included and column name is user defined.
    #[allow(dead_code)]
    pub fn with_file_column_name<S: ToString>(mut self, name: &S) -> Self {
        self.file_column_name = Some(name.to_string());
        self.include_file_column = true;
        self
    }

    /// Whether to wrap partition values in a dictionary encoding
    #[allow(dead_code)]
    pub fn wrap_partition_values(mut self, wrap: bool) -> Self {
        self.wrap_partition_values = Some(wrap);
        self
    }

    /// Allow pushdown of the scan filter
    /// When disabled the filter will only be used for pruning files
    #[allow(dead_code)]
    pub fn with_parquet_pushdown(mut self, pushdown: bool) -> Self {
        self.enable_parquet_pushdown = pushdown;
        self
    }

    /// Use the provided [SchemaRef] for the [DeltaScan]
    #[allow(dead_code)]
    pub fn with_schema(mut self, schema: SchemaRef) -> Self {
        self.schema = Some(schema);
        self
    }

    /// Build a DeltaScanConfig and ensure no column name conflicts occur during downstream processing
    pub fn build(&self, snapshot: &DeltaTableState) -> DeltaResult<DeltaScanConfig> {
        let file_column_name = if self.include_file_column {
            let input_schema = snapshot.input_schema()?;
            let mut column_names: HashSet<&String> = HashSet::new();
            for field in input_schema.fields.iter() {
                column_names.insert(field.name());
            }

            match &self.file_column_name {
                Some(name) => {
                    if column_names.contains(name) {
                        return Err(DeltaTableError::Generic(format!(
                            "Unable to add file path column since column with name {name} exits"
                        )));
                    }

                    Some(name.to_owned())
                }
                None => {
                    let prefix = PATH_COLUMN;
                    let mut idx = 0;
                    let mut name = prefix.to_owned();

                    while column_names.contains(&name) {
                        idx += 1;
                        name = format!("{prefix}_{idx}");
                    }

                    Some(name)
                }
            }
        } else {
            None
        };

        Ok(DeltaScanConfig {
            file_column_name,
            wrap_partition_values: self.wrap_partition_values.unwrap_or(true),
            enable_parquet_pushdown: self.enable_parquet_pushdown,
            schema: self.schema.clone(),
        })
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
/// Include additional metadata columns during a [`DeltaScan`]
pub struct DeltaScanConfig {
    /// Include the source path for each record
    pub file_column_name: Option<String>,
    /// Wrap partition values in a dictionary encoding
    pub wrap_partition_values: bool,
    /// Allow pushdown of the scan filter
    pub enable_parquet_pushdown: bool,
    /// Schema to read as
    pub schema: Option<SchemaRef>,
}

/// A wrapper for parquet scans
#[derive(Debug)]
pub struct DeltaScan {
    /// The URL of the ObjectStore root
    pub table_uri: String,
    /// Column that contains an index that maps to the original metadata Add
    pub config: DeltaScanConfig,
    /// The parquet scan to wrap
    pub parquet_scan: Arc<dyn ExecutionPlan>,
    /// The schema of the table to be used when evaluating expressions
    pub logical_schema: Arc<ArrowSchema>,
    /// Metrics for scan reported via DataFusion
    metrics: ExecutionPlanMetricsSet,
}

// TODO: Wire related logic
// #[derive(Debug, Serialize, Deserialize)]
// struct DeltaScanWire {
//     pub table_uri: String,
//     pub config: DeltaScanConfig,
//     pub logical_schema: Arc<ArrowSchema>,
// }

impl DisplayAs for DeltaScan {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        write!(f, "DeltaScan")
    }
}

impl ExecutionPlan for DeltaScan {
    fn name(&self) -> &str {
        "DeltaScan"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.parquet_scan.schema()
    }

    fn properties(&self) -> &PlanProperties {
        self.parquet_scan.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.parquet_scan]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Plan(format!(
                "DeltaScan wrong number of children {}",
                children.len()
            )));
        }
        Ok(Arc::new(DeltaScan {
            table_uri: self.table_uri.clone(),
            config: self.config.clone(),
            parquet_scan: children[0].clone(),
            logical_schema: self.logical_schema.clone(),
            metrics: self.metrics.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        self.parquet_scan.execute(partition, context)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> DataFusionResult<Statistics> {
        // let partition_stats = self.parquet_scan.partition_statistics()?;
        Ok(Statistics::default())
    }

    fn repartitioned(
        &self,
        target_partitions: usize,
        config: &ConfigOptions,
    ) -> DataFusionResult<Option<Arc<dyn ExecutionPlan>>> {
        self.parquet_scan.repartitioned(target_partitions, config)
    }
}

pub(crate) struct DeltaScanBuilder<'a> {
    snapshot: &'a DeltaTableState,
    log_store: LogStoreRef,
    filter: Option<Expr>,
    session: &'a dyn Session,
    projection: Option<&'a Vec<usize>>,
    limit: Option<usize>,
    files: Option<&'a [Add]>,
    config: Option<DeltaScanConfig>,
}

impl<'a> DeltaScanBuilder<'a> {
    pub fn new(
        snapshot: &'a DeltaTableState,
        log_store: LogStoreRef,
        session: &'a dyn Session,
    ) -> Self {
        DeltaScanBuilder {
            snapshot,
            log_store,
            filter: None,
            session,
            projection: None,
            limit: None,
            files: None,
            config: None,
        }
    }

    pub fn with_filter(mut self, filter: Option<Expr>) -> Self {
        self.filter = filter;
        self
    }

    pub fn with_files(mut self, files: &'a [Add]) -> Self {
        self.files = Some(files);
        self
    }

    pub fn with_projection(mut self, projection: Option<&'a Vec<usize>>) -> Self {
        self.projection = projection;
        self
    }

    pub fn with_limit(mut self, limit: Option<usize>) -> Self {
        self.limit = limit;
        self
    }

    pub fn with_scan_config(mut self, config: DeltaScanConfig) -> Self {
        self.config = Some(config);
        self
    }

    pub async fn build(self) -> DeltaResult<DeltaScan> {
        let config = match self.config {
            Some(config) => config,
            None => DeltaScanConfigBuilder::new().build(self.snapshot)?,
        };

        let schema = match config.schema.clone() {
            Some(value) => Ok(value),
            None => self.snapshot.arrow_schema(),
        }?;

        let logical_schema = df_logical_schema(
            self.snapshot,
            &config.file_column_name,
            Some(schema.clone()),
        )?;

        let logical_schema = if let Some(used_columns) = self.projection {
            let mut fields = vec![];
            for idx in used_columns {
                fields.push(logical_schema.field(*idx).to_owned());
            }
            // partition filters with Exact pushdown were removed from projection by DF optimizer,
            // we need to add them back for the predicate pruning to work
            if let Some(expr) = &self.filter {
                for c in expr.column_refs() {
                    let idx = logical_schema.index_of(c.name.as_str())?;
                    if !used_columns.contains(&idx) {
                        fields.push(logical_schema.field(idx).to_owned());
                    }
                }
            }
            // Ensure all partition columns are included in logical schema
            let table_partition_cols = self.snapshot.metadata().partition_columns();
            for partition_col in table_partition_cols.iter() {
                if let Ok(idx) = logical_schema.index_of(partition_col.as_str()) {
                    if !used_columns.contains(&idx) && !fields.iter().any(|f| f.name() == partition_col) {
                        fields.push(logical_schema.field(idx).to_owned());
                    }
                }
            }
            Arc::new(ArrowSchema::new(fields))
        } else {
            logical_schema
        };

        let context = SessionContext::new();
        let df_schema = logical_schema
            .clone()
            .to_dfschema()
            .map_err(|e| DeltaTableError::Generic(e.to_string()))?;

        let logical_filter = self
            .filter
            .clone()
            .map(|expr| simplify_expr(&context, &df_schema, expr));

        // only inexact filters should be pushed down to the data source, doing otherwise
        // will make stats inexact and disable datafusion optimizations like AggregateStatistics
        let pushdown_filter = self
            .filter
            .clone()
            .filter(|_| config.enable_parquet_pushdown)
            .map(|expr| simplify_expr(&context, &df_schema, expr));

        let table_partition_cols = self.snapshot.metadata().partition_columns();
        let file_schema = Arc::new(ArrowSchema::new(
            schema
                .fields()
                .iter()
                .filter(|f| !table_partition_cols.contains(f.name()))
                .cloned()
                .collect::<Vec<_>>(),
        ));

        let (files, files_scanned, files_pruned, _pruning_mask) = match self.files {
            Some(files) => {
                let files = files.to_owned();
                let files_scanned = files.len();
                (files, files_scanned, 0, None::<Vec<bool>>)
            }
            None => {
                // early return in case we have no push down filters or limit
                if logical_filter.is_none() && self.limit.is_none() {
                    let files = self.snapshot.file_actions()?;
                    let files_scanned = files.len();
                    (files, files_scanned, 0, None::<Vec<bool>>)
                } else {
                    // Use files_matching_predicate to get filtered files
                    let filters = if let Some(filter) = &self.filter {
                        vec![filter.clone()]
                    } else {
                        vec![]
                    };

                    let files: Vec<Add> =
                        files_matching_predicate(self.snapshot.snapshot(), &filters)?.collect();
                    let files_scanned = files.len();
                    let total_files = self.snapshot.files_count();
                    let files_pruned = total_files - files_scanned;
                    (files, files_scanned, files_pruned, None::<Vec<bool>>)
                }
            }
        };

        // TODO we group files together by their partition values. If the table is partitioned
        // we may be able to reduce the number of groups by combining groups with the same partition values
        let mut file_groups: HashMap<Vec<ScalarValue>, Vec<PartitionedFile>> = HashMap::new();
        let table_partition_cols = &self.snapshot.metadata().partition_columns();

        for action in files.iter() {
            let mut part = partitioned_file_from_action(&action, table_partition_cols, &schema);

            if config.file_column_name.is_some() {
                let partition_value = if config.wrap_partition_values {
                    wrap_partition_value_in_dict(ScalarValue::Utf8(Some(action.path.clone())))
                } else {
                    ScalarValue::Utf8(Some(action.path.clone()))
                };
                part.partition_values.push(partition_value);
            }

            file_groups
                .entry(part.partition_values.clone())
                .or_default()
                .push(part);
        }

        let mut table_partition_cols = table_partition_cols
            .iter()
            .map(|col| {
                let field = schema
                    .field_with_name(col)
                    .expect("Column should exist in schema");
                let corrected = if config.wrap_partition_values {
                    match field.data_type() {
                        ArrowDataType::Utf8
                        | ArrowDataType::LargeUtf8
                        | ArrowDataType::Binary
                        | ArrowDataType::LargeBinary => {
                            wrap_partition_type_in_dict(field.data_type().clone())
                        }
                        _ => field.data_type().clone(),
                    }
                } else {
                    field.data_type().clone()
                };
                Field::new(col.clone(), corrected, true)
            })
            .collect::<Vec<_>>();

        if let Some(file_column_name) = &config.file_column_name {
            let field_name_datatype = if config.wrap_partition_values {
                wrap_partition_type_in_dict(ArrowDataType::Utf8)
            } else {
                ArrowDataType::Utf8
            };
            table_partition_cols.push(Field::new(
                file_column_name.clone(),
                field_name_datatype,
                false,
            ));
        }

        let stats = Statistics::new_unknown(&schema);

        let parquet_options = TableParquetOptions {
            global: self.session.config().options().execution.parquet.clone(),
            ..Default::default()
        };

        // Create the base ParquetSource and apply predicate if needed
        let mut parquet_source = ParquetSource::new(parquet_options);

        if let Some(predicate) = pushdown_filter {
            if config.enable_parquet_pushdown {
                parquet_source = parquet_source.with_predicate(predicate);
            }
        }

        // Apply schema adapter factory and get the file source
        let file_source = parquet_source
            .with_schema_adapter_factory(Arc::new(DeltaSchemaAdapterFactory {}))
            .map_err(datafusion_to_delta_error)?;

        // Create object store URL using delta-rs object_store_url logic
        // This generates a unique URL with only scheme and authority for DataFusion
        let object_store_url = create_object_store_url(&self.log_store.config().location);

        // Register the object store with DataFusion's RuntimeEnv so it can resolve the custom URL
        self.session
            .runtime_env()
            .register_object_store(object_store_url.as_ref(), self.log_store.object_store(None));

        let file_scan_config =
            FileScanConfigBuilder::new(object_store_url, file_schema, file_source)
                .with_file_groups(
                    // If all files were filtered out, we still need to emit at least one partition to
                    // pass datafusion sanity checks.
                    //
                    // See https://github.com/apache/datafusion/issues/11322
                    if file_groups.is_empty() {
                        vec![FileGroup::from(vec![])]
                    } else {
                        file_groups.into_values().map(FileGroup::from).collect()
                    },
                )
                .with_statistics(stats)
                .with_projection(self.projection.cloned())
                .with_limit(self.limit)
                .with_table_partition_cols(table_partition_cols)
                .build();

        let metrics = ExecutionPlanMetricsSet::new();
        MetricBuilder::new(&metrics)
            .global_counter("files_scanned")
            .add(files_scanned);
        MetricBuilder::new(&metrics)
            .global_counter("files_pruned")
            .add(files_pruned);

        Ok(DeltaScan {
            table_uri: self.log_store.root_uri().to_string(),
            parquet_scan: DataSourceExec::from_data_source(file_scan_config),
            config,
            logical_schema,
            metrics,
        })
    }
}

fn simplify_expr(
    context: &SessionContext,
    df_schema: &DFSchema,
    expr: Expr,
) -> Arc<dyn PhysicalExpr> {
    // Simplify the expression first
    let props = ExecutionProps::new();
    let simplify_context = SimplifyContext::new(&props).with_schema(df_schema.clone().into());
    let simplifier = ExprSimplifier::new(simplify_context).with_max_cycles(10);
    let simplified = simplifier
        .simplify(expr)
        .expect("Failed to simplify expression");

    context
        .create_physical_expr(simplified, df_schema)
        .expect("Failed to create physical expression")
}

#[allow(dead_code)]
fn prune_file_statistics(
    record_batches: &[RecordBatch],
    pruning_mask: Vec<bool>,
) -> Vec<RecordBatch> {
    record_batches
        .iter()
        .zip(pruning_mask.iter())
        .filter_map(|(batch, keep)| if *keep { Some(batch.clone()) } else { None })
        .collect()
}

pub(crate) fn get_null_of_arrow_type(t: &ArrowDataType) -> DeltaResult<ScalarValue> {
    Ok(match t {
        ArrowDataType::Null => ScalarValue::Null,
        ArrowDataType::Boolean => ScalarValue::Boolean(None),
        ArrowDataType::Int8 => ScalarValue::Int8(None),
        ArrowDataType::Int16 => ScalarValue::Int16(None),
        ArrowDataType::Int32 => ScalarValue::Int32(None),
        ArrowDataType::Int64 => ScalarValue::Int64(None),
        ArrowDataType::UInt8 => ScalarValue::UInt8(None),
        ArrowDataType::UInt16 => ScalarValue::UInt16(None),
        ArrowDataType::UInt32 => ScalarValue::UInt32(None),
        ArrowDataType::UInt64 => ScalarValue::UInt64(None),
        ArrowDataType::Float16 => ScalarValue::Float32(None),
        ArrowDataType::Float32 => ScalarValue::Float32(None),
        ArrowDataType::Float64 => ScalarValue::Float64(None),
        ArrowDataType::Timestamp(TimeUnit::Second, tz) => {
            ScalarValue::TimestampSecond(None, tz.clone())
        }
        ArrowDataType::Timestamp(TimeUnit::Millisecond, tz) => {
            ScalarValue::TimestampMillisecond(None, tz.clone())
        }
        ArrowDataType::Timestamp(TimeUnit::Microsecond, tz) => {
            ScalarValue::TimestampMicrosecond(None, tz.clone())
        }
        ArrowDataType::Timestamp(TimeUnit::Nanosecond, tz) => {
            ScalarValue::TimestampNanosecond(None, tz.clone())
        }
        ArrowDataType::Date32 => ScalarValue::Date32(None),
        ArrowDataType::Date64 => ScalarValue::Date64(None),
        ArrowDataType::Time32(_) => ScalarValue::Time32Second(None),
        ArrowDataType::Time64(_) => ScalarValue::Time64Microsecond(None),
        ArrowDataType::Duration(_) => ScalarValue::DurationSecond(None),
        ArrowDataType::Interval(_) => ScalarValue::IntervalYearMonth(None),
        ArrowDataType::Binary => ScalarValue::Binary(None),
        ArrowDataType::FixedSizeBinary(size) => ScalarValue::FixedSizeBinary(*size, None),
        ArrowDataType::LargeBinary => ScalarValue::LargeBinary(None),
        ArrowDataType::Utf8 => ScalarValue::Utf8(None),
        ArrowDataType::LargeUtf8 => ScalarValue::LargeUtf8(None),
        ArrowDataType::List(_) => {
            return Err(DeltaTableError::Generic(
                "List type not supported for null values".to_string(),
            ))
        }
        ArrowDataType::FixedSizeList(_, _) => {
            return Err(DeltaTableError::Generic(
                "FixedSizeList type not supported for null values".to_string(),
            ))
        }
        ArrowDataType::LargeList(_) => {
            return Err(DeltaTableError::Generic(
                "LargeList type not supported for null values".to_string(),
            ))
        }
        ArrowDataType::Struct(_) => {
            return Err(DeltaTableError::Generic(
                "Struct type not supported for null values".to_string(),
            ))
        }
        ArrowDataType::Union(_, _) => {
            return Err(DeltaTableError::Generic(
                "Union type not supported for null values".to_string(),
            ))
        }
        ArrowDataType::Dictionary(_, _) => {
            return Err(DeltaTableError::Generic(
                "Dictionary type not supported for null values".to_string(),
            ))
        }
        ArrowDataType::Decimal128(_, _) => ScalarValue::Decimal128(None, 10, 0),
        ArrowDataType::Decimal256(_, _) => {
            return Err(DeltaTableError::Generic(
                "Decimal256 type not supported for null values".to_string(),
            ))
        }
        ArrowDataType::Map(_, _) => {
            return Err(DeltaTableError::Generic(
                "Map type not supported for null values".to_string(),
            ))
        }
        ArrowDataType::RunEndEncoded(_, _) => {
            return Err(DeltaTableError::Generic(
                "RunEndEncoded type not supported for null values".to_string(),
            ))
        }
        ArrowDataType::ListView(_) => {
            return Err(DeltaTableError::Generic(
                "ListView type not supported for null values".to_string(),
            ))
        }
        ArrowDataType::LargeListView(_) => {
            return Err(DeltaTableError::Generic(
                "LargeListView type not supported for null values".to_string(),
            ))
        }
        ArrowDataType::Utf8View => ScalarValue::Utf8View(None),
        ArrowDataType::BinaryView => ScalarValue::BinaryView(None),
    })
}

fn partitioned_file_from_action(
    action: &Add,
    partition_columns: &[String],
    schema: &ArrowSchema,
) -> PartitionedFile {
    let partition_values = partition_columns
        .iter()
        .map(|part| {
            action
                .partition_values
                .get(part)
                .map(|val| {
                    schema
                        .field_with_name(part)
                        .map(|field| match val {
                            Some(value) => to_correct_scalar_value(
                                &serde_json::Value::String(value.to_string()),
                                field.data_type(),
                            )
                            .unwrap_or(Some(ScalarValue::Null))
                            .unwrap_or(ScalarValue::Null),
                            None => get_null_of_arrow_type(field.data_type())
                                .unwrap_or(ScalarValue::Null),
                        })
                        .unwrap_or(ScalarValue::Null)
                })
                .unwrap_or(ScalarValue::Null)
        })
        .collect::<Vec<_>>();

    let ts_secs = action.modification_time / 1000;
    let ts_ns = (action.modification_time % 1000) * 1_000_000;
    let last_modified = chrono::Utc.from_utc_datetime(
        &chrono::DateTime::from_timestamp(ts_secs, ts_ns as u32)
            .expect("Failed to create timestamp from seconds and nanoseconds")
            .naive_utc(),
    );
    PartitionedFile {
        object_meta: ObjectMeta {
            last_modified,
            ..action
                .try_into()
                .expect("Failed to convert action to ObjectMeta")
        },
        partition_values,
        extensions: None,
        range: None,
        statistics: None,
        metadata_size_hint: None,
    }
}

fn parse_date(
    stat_val: &serde_json::Value,
    field_dt: &ArrowDataType,
) -> DataFusionResult<ScalarValue> {
    match stat_val {
        serde_json::Value::String(s) => {
            let date = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
                .map_err(|_| DataFusionError::Execution("Failed to parse date".to_string()))?;
            match field_dt {
                ArrowDataType::Date32 => Ok(ScalarValue::Date32(Some(
                    date.signed_duration_since(
                        chrono::NaiveDate::from_ymd_opt(1970, 1, 1)
                            .expect("Failed to create epoch date"),
                    )
                    .num_days() as i32,
                ))),
                ArrowDataType::Date64 => Ok(ScalarValue::Date64(Some(
                    date.signed_duration_since(
                        chrono::NaiveDate::from_ymd_opt(1970, 1, 1)
                            .expect("Failed to create epoch date"),
                    )
                    .num_milliseconds(),
                ))),
                _ => Err(DataFusionError::Execution("Invalid date type".to_string())),
            }
        }
        _ => Err(DataFusionError::Execution(
            "Date value must be a string".to_string(),
        )),
    }
}

fn parse_timestamp(
    stat_val: &serde_json::Value,
    field_dt: &ArrowDataType,
) -> DataFusionResult<ScalarValue> {
    match stat_val {
        serde_json::Value::String(s) => {
            let timestamp = DateTime::parse_from_rfc3339(s)
                .map_err(|_| DataFusionError::Execution("Failed to parse timestamp".to_string()))?;
            match field_dt {
                ArrowDataType::Timestamp(TimeUnit::Second, tz) => Ok(ScalarValue::TimestampSecond(
                    Some(timestamp.timestamp()),
                    tz.clone(),
                )),
                ArrowDataType::Timestamp(TimeUnit::Millisecond, tz) => {
                    Ok(ScalarValue::TimestampMillisecond(
                        Some(timestamp.timestamp_millis()),
                        tz.clone(),
                    ))
                }
                ArrowDataType::Timestamp(TimeUnit::Microsecond, tz) => {
                    Ok(ScalarValue::TimestampMicrosecond(
                        Some(timestamp.timestamp_micros()),
                        tz.clone(),
                    ))
                }
                ArrowDataType::Timestamp(TimeUnit::Nanosecond, tz) => {
                    Ok(ScalarValue::TimestampNanosecond(
                        Some(timestamp.timestamp_nanos_opt().unwrap_or(0)),
                        tz.clone(),
                    ))
                }
                _ => Err(DataFusionError::Execution(
                    "Invalid timestamp type".to_string(),
                )),
            }
        }
        _ => Err(DataFusionError::Execution(
            "Timestamp value must be a string".to_string(),
        )),
    }
}

pub(crate) fn to_correct_scalar_value(
    stat_val: &serde_json::Value,
    field_dt: &ArrowDataType,
) -> DataFusionResult<Option<ScalarValue>> {
    match stat_val {
        serde_json::Value::Array(_) => Ok(None),
        serde_json::Value::Object(_) => Ok(None),
        serde_json::Value::Null => Ok(Some(
            get_null_of_arrow_type(field_dt).map_err(|e| DataFusionError::External(Box::new(e)))?,
        )),
        serde_json::Value::String(string_val) => match field_dt {
            ArrowDataType::Timestamp(_, _) => Ok(Some(parse_timestamp(stat_val, field_dt)?)),
            ArrowDataType::Date32 => Ok(Some(parse_date(stat_val, field_dt)?)),
            _ => Ok(Some(ScalarValue::try_from_string(
                string_val.to_owned(),
                field_dt,
            )?)),
        },
        other => match field_dt {
            ArrowDataType::Timestamp(_, _) => Ok(Some(parse_timestamp(stat_val, field_dt)?)),
            ArrowDataType::Date32 => Ok(Some(parse_date(stat_val, field_dt)?)),
            _ => Ok(Some(ScalarValue::try_from_string(
                other.to_string(),
                field_dt,
            )?)),
        },
    }
}

// TODO: implement DeltaDataChecker
// #[derive(Clone, Default)]
// pub struct DeltaDataChecker {
//     constraints: Vec<Constraint>,
//     invariants: Vec<Invariant>,
//     generated_columns: Vec<GeneratedColumn>,
//     non_nullable_columns: Vec<String>,
//     ctx: SessionContext,
// }
// impl DeltaDataChecker {}

/// A Delta table provider that enables additional metadata columns to be included during the scan
#[derive(Debug)]
pub struct DeltaTableProvider {
    snapshot: DeltaTableState,
    log_store: LogStoreRef,
    config: DeltaScanConfig,
    schema: Arc<ArrowSchema>,
    files: Option<Vec<Add>>,
}

impl DeltaTableProvider {
    /// Build a DeltaTableProvider
    pub fn try_new(
        snapshot: DeltaTableState,
        log_store: LogStoreRef,
        config: DeltaScanConfig,
    ) -> DeltaResult<Self> {
        Ok(DeltaTableProvider {
            schema: df_logical_schema(&snapshot, &config.file_column_name, config.schema.clone())?,
            snapshot,
            log_store,
            config,
            files: None,
        })
    }

    /// Define which files to consider while building a scan, for advanced usecases
    #[allow(dead_code)]
    pub fn with_files(mut self, files: Vec<Add>) -> DeltaTableProvider {
        self.files = Some(files);
        self
    }
}

#[async_trait]
impl TableProvider for DeltaTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Arc<ArrowSchema> {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn get_table_definition(&self) -> Option<&str> {
        None
    }

    fn get_logical_plan(&self) -> Option<Cow<'_, LogicalPlan>> {
        None
    }

    async fn scan(
        &self,
        session: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let filter_expr = conjunction(filters.iter().cloned());

        let mut scan = DeltaScanBuilder::new(&self.snapshot, self.log_store.clone(), session)
            .with_projection(projection)
            .with_limit(limit)
            .with_filter(filter_expr)
            .with_scan_config(self.config.clone());

        if let Some(files) = &self.files {
            scan = scan.with_files(files);
        }
        Ok(Arc::new(
            scan.build().await.map_err(delta_to_datafusion_error)?,
        ))
    }

    fn supports_filters_pushdown(
        &self,
        filter: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        let partition_cols = self.snapshot.metadata().partition_columns().as_slice();
        Ok(get_pushdown_filters(filter, partition_cols))
    }

    fn statistics(&self) -> Option<Statistics> {
        self.snapshot.datafusion_table_statistics()
    }
}

fn get_pushdown_filters(
    filter: &[&Expr],
    partition_cols: &[String],
) -> Vec<TableProviderFilterPushDown> {
    filter
        .iter()
        .cloned()
        .map(|expr| {
            let applicable = expr_is_exact_predicate_for_cols(partition_cols, expr);
            if !expr.column_refs().is_empty() && applicable {
                TableProviderFilterPushDown::Exact
            } else {
                TableProviderFilterPushDown::Inexact
            }
        })
        .collect()
}

// inspired from datafusion::listing::helpers, but adapted to only stats based pruning
fn expr_is_exact_predicate_for_cols(partition_cols: &[String], expr: &Expr) -> bool {
    let mut is_applicable = true;
    expr.apply(|expr| match expr {
        Expr::Column(Column { ref name, .. }) => {
            is_applicable &= partition_cols.contains(name);

            // TODO: decide if we should constrain this to Utf8 columns (including views, dicts etc)

            if is_applicable {
                Ok(TreeNodeRecursion::Jump)
            } else {
                Ok(TreeNodeRecursion::Stop)
            }
        }
        Expr::BinaryExpr(BinaryExpr { ref op, .. }) => {
            is_applicable &= matches!(
                op,
                Operator::And
                    | Operator::Or
                    | Operator::NotEq
                    | Operator::Eq
                    | Operator::Gt
                    | Operator::GtEq
                    | Operator::Lt
                    | Operator::LtEq
            );
            if is_applicable {
                Ok(TreeNodeRecursion::Continue)
            } else {
                Ok(TreeNodeRecursion::Stop)
            }
        }
        Expr::Literal(..)
        | Expr::Not(_)
        | Expr::IsNotNull(_)
        | Expr::IsNull(_)
        | Expr::Between(_)
        | Expr::InList(_) => Ok(TreeNodeRecursion::Continue),
        _ => {
            is_applicable = false;
            Ok(TreeNodeRecursion::Stop)
        }
    })
    .expect("Failed to apply expression transformation");
    is_applicable
}

// TODO: implement FindFiles related logic
// pub(crate) struct FindFilesExprProperties {
//     pub partition_columns: Vec<String>,
//     pub partition_only: bool,
//     pub result: DeltaResult<()>,
// }

// TODO: implement DeltaColumn (maybe not needed?)
// pub struct DeltaColumn {
//     inner: Column,
// }
