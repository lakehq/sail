//! Datafusion integration for Delta Table
//!
//! Example:
//!
//! ```rust
//! use std::sync::Arc;
//! use datafusion::execution::context::SessionContext;
//!
//! async {
//!   let mut ctx = SessionContext::new();
//!   let table = deltalake_core::open_table("./tests/data/simple_table")
//!       .await
//!       .unwrap();
//!   ctx.register_table("demo", Arc::new(table)).unwrap();
//!
//!   let batches = ctx
//!       .sql("SELECT * FROM demo").await.unwrap()
//!       .collect()
//!       .await.unwrap();
//! };
//! ```

use std::any::Any;
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::fmt::{self, Debug};
use std::sync::Arc;

// use arrow_select::concat::concat_batches;
// use arrow_select::filter::filter_record_batch;
use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};
use datafusion::arrow::array::types::UInt16Type;
use datafusion::arrow::array::{
    Array, BooleanArray, DictionaryArray, RecordBatch, StringArray, TypedDictionaryArray,
};
use datafusion::arrow::compute::kernels::cast::{cast_with_options, CastOptions};
use datafusion::arrow::datatypes::{
    DataType as ArrowDataType, Field, Schema as ArrowSchema, SchemaRef,
    SchemaRef as ArrowSchemaRef, TimeUnit,
};
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::catalog::memory::DataSourceExec;
use datafusion::catalog::{Session, TableProviderFactory};
use datafusion::common::config::ConfigOptions;
use datafusion::common::scalar::ScalarValue;
use datafusion::common::stats::Statistics;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion, TreeNodeVisitor};
use datafusion::common::{
    Column, DFSchema, DataFusionError, Result as DataFusionResult, TableReference, ToDFSchema,
};
use datafusion::config::TableParquetOptions;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::physical_plan::{
    wrap_partition_type_in_dict, wrap_partition_value_in_dict, FileGroup, FileScanConfigBuilder,
    FileSource, ParquetSource,
};
use datafusion::datasource::{MemTable, TableProvider, TableType};
use datafusion::execution::context::{SessionConfig, SessionContext, SessionState, TaskContext};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::execution_props::ExecutionProps;
use datafusion::logical_expr::logical_plan::CreateExternalTable;
use datafusion::logical_expr::simplify::SimplifyContext;
use datafusion::logical_expr::utils::{conjunction, split_conjunction};
use datafusion::logical_expr::{
    col, BinaryExpr, Expr, Extension, LogicalPlan, Operator, TableProviderFilterPushDown,
    Volatility,
};
use datafusion::optimizer::simplify_expressions::ExprSimplifier;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_optimizer::pruning::{PruningPredicate, PruningStatistics};
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::limit::LocalLimitExec;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder, MetricsSet};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use datafusion::sql::planner::ParserOptions;
// use datafusion_proto::logical_plan::LogicalExtensionCodec;
// use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
use deltalake::errors::{DeltaResult, DeltaTableError};
use deltalake::kernel::{
    Add, DataCheck, EagerSnapshot, Invariant, LogDataHandler, Snapshot, StructTypeExt,
};
use deltalake::logstore::LogStoreRef;
use deltalake::table::builder::ensure_table_uri;
use deltalake::table::state::DeltaTableState;
use deltalake::table::{Constraint, GeneratedColumn};
use deltalake::{open_table, open_table_with_storage_options, DeltaTable};
use either::Either;
use futures::TryStreamExt;
use object_store::ObjectMeta;
use serde::{Deserialize, Serialize};
use url::Url;

use crate::delta_datafusion::expr::parse_predicate_expression;
use crate::delta_datafusion::schema_adapter::DeltaSchemaAdapterFactory;

pub(crate) const PATH_COLUMN: &str = "__delta_rs_path";

pub mod cdf;
pub mod expr;
pub mod logical;
pub mod physical;
pub mod planner;

pub use cdf::scan::DeltaCdfTableProvider;

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

// Use explicit conversion functions instead

/// Convenience trait for calling common methods on snapshot hierarchies
pub trait DataFusionMixins {
    /// The physical datafusion schema of a table
    fn arrow_schema(&self) -> DeltaResult<ArrowSchemaRef>;

    /// Get the table schema as an [`ArrowSchemaRef`]
    fn input_schema(&self) -> DeltaResult<ArrowSchemaRef>;

    /// Parse an expression string into a datafusion [`Expr`]
    fn parse_predicate_expression(
        &self,
        expr: impl AsRef<str>,
        df_state: &SessionState,
    ) -> DeltaResult<Expr>;
}

impl DataFusionMixins for Snapshot {
    fn arrow_schema(&self) -> DeltaResult<ArrowSchemaRef> {
        arrow_schema_impl(self, true)
    }

    fn input_schema(&self) -> DeltaResult<ArrowSchemaRef> {
        arrow_schema_impl(self, false)
    }

    fn parse_predicate_expression(
        &self,
        expr: impl AsRef<str>,
        df_state: &SessionState,
    ) -> DeltaResult<Expr> {
        let schema = DFSchema::try_from(self.arrow_schema()?.as_ref().to_owned())
            .map_err(|e| DeltaTableError::Generic(e.to_string()))?;
        parse_predicate_expression(&schema, expr, df_state)
    }
}

impl DataFusionMixins for EagerSnapshot {
    fn arrow_schema(&self) -> DeltaResult<ArrowSchemaRef> {
        arrow_schema_from_struct_type(self.schema(), &self.metadata().partition_columns, true)
    }

    fn input_schema(&self) -> DeltaResult<ArrowSchemaRef> {
        arrow_schema_from_struct_type(self.schema(), &self.metadata().partition_columns, false)
    }

    fn parse_predicate_expression(
        &self,
        expr: impl AsRef<str>,
        df_state: &SessionState,
    ) -> DeltaResult<Expr> {
        let schema = DFSchema::try_from(self.arrow_schema()?.as_ref().to_owned())
            .map_err(|e| DeltaTableError::Generic(e.to_string()))?;
        parse_predicate_expression(&schema, expr, df_state)
    }
}

impl DataFusionMixins for DeltaTableState {
    fn arrow_schema(&self) -> DeltaResult<ArrowSchemaRef> {
        self.snapshot().arrow_schema()
    }

    fn input_schema(&self) -> DeltaResult<ArrowSchemaRef> {
        self.snapshot().input_schema()
    }

    fn parse_predicate_expression(
        &self,
        expr: impl AsRef<str>,
        df_state: &SessionState,
    ) -> DeltaResult<Expr> {
        self.snapshot().parse_predicate_expression(expr, df_state)
    }
}

fn arrow_schema_from_snapshot(
    snapshot: &Snapshot,
    wrap_partitions: bool,
) -> DeltaResult<ArrowSchemaRef> {
    let meta = snapshot.metadata();
    let schema = meta.schema()?;

    let fields = schema
        .fields()
        .filter(|f| !meta.partition_columns.contains(&f.name().to_string()))
        .map(|f| {
            // Convert StructField to Arrow Field
            let field_name = f.name().to_string();
            let field_type = arrow_type_from_delta_type(f.data_type())?;
            Ok(Field::new(field_name, field_type, f.is_nullable()))
        })
        .chain(
            // We need stable order between logical and physical schemas, but the order of
            // partitioning columns is not always the same in the json schema and the array
            meta.partition_columns.iter().map(|partition_col| {
                let f = schema.field(partition_col).unwrap();
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
                let f = schema.field(partition_col).unwrap();
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
                PrimitiveType::Decimal(decimal_type) => ArrowDataType::Decimal128(
                    decimal_type.precision() as u8,
                    decimal_type.scale() as i8,
                ),
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

    // Filter files based on predicates
    let filtered = adds.into_iter().filter(|_add| {
        // TODO: Evaluate predicates against partition values
        true // For now return all files
    });

    Ok(Box::new(filtered))
}

pub(crate) fn get_path_column<'a>(
    batch: &'a RecordBatch,
    path_column: &str,
) -> DeltaResult<TypedDictionaryArray<'a, UInt16Type, StringArray>> {
    unimplemented!();
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

pub(crate) fn register_store(store: LogStoreRef, env: Arc<RuntimeEnv>) {
    let url = &store.config().location;
    env.register_object_store(url, store.object_store(None));
}

fn object_store_url(location: &Url) -> ObjectStoreUrl {
    use object_store::path::DELIMITER;
    ObjectStoreUrl::parse(format!(
        "delta-rs://{}-{}{}",
        location.scheme(),
        location.host_str().unwrap_or("-"),
        location.path().replace(DELIMITER, "-").replace(':', "-")
    ))
    .expect("Invalid object store url.")
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
    let table_partition_cols = &snapshot.metadata().partition_columns;

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
                .unwrap()
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
    pub fn with_file_column(mut self, include: bool) -> Self {
        self.include_file_column = include;
        self.file_column_name = None;
        self
    }

    /// Indicate that a column containing a records file path is included and column name is user defined.
    pub fn with_file_column_name<S: ToString>(mut self, name: &S) -> Self {
        self.file_column_name = Some(name.to_string());
        self.include_file_column = true;
        self
    }

    /// Whether to wrap partition values in a dictionary encoding
    pub fn wrap_partition_values(mut self, wrap: bool) -> Self {
        self.wrap_partition_values = Some(wrap);
        self
    }

    /// Allow pushdown of the scan filter
    /// When disabled the filter will only be used for pruning files
    pub fn with_parquet_pushdown(mut self, pushdown: bool) -> Self {
        self.enable_parquet_pushdown = pushdown;
        self
    }

    /// Use the provided [SchemaRef] for the [DeltaScan]
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

#[derive(Debug, Serialize, Deserialize)]
struct DeltaScanWire {
    pub table_uri: String,
    pub config: DeltaScanConfig,
    pub logical_schema: Arc<ArrowSchema>,
}

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
        self.parquet_scan.statistics()
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

        let file_schema = schema.clone();

        let (files, files_scanned, files_pruned, pruning_mask) = match self.files {
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
        let table_partition_cols = &self.snapshot.metadata().partition_columns;

        for action in files {
            let mut partition_values = Vec::new();
            for partition_col in table_partition_cols.iter() {
                let partition_value = action
                    .partition_values
                    .get(partition_col)
                    .map(|val| {
                        val.as_ref()
                            .map(|v| {
                                let field = logical_schema.field_with_name(partition_col).unwrap();
                                to_correct_scalar_value(
                                    &serde_json::Value::String(v.to_string()),
                                    field.data_type(),
                                )
                                .unwrap_or(Some(ScalarValue::Null))
                                .unwrap_or(ScalarValue::Null)
                            })
                            .unwrap_or_else(|| {
                                let field = logical_schema.field_with_name(partition_col).unwrap();
                                get_null_of_arrow_type(field.data_type())
                                    .unwrap_or(ScalarValue::Null)
                            })
                    })
                    .unwrap_or(ScalarValue::Null);

                if config.wrap_partition_values {
                    partition_values.push(wrap_partition_value_in_dict(partition_value));
                } else {
                    partition_values.push(partition_value);
                }
            }

            let part = partitioned_file_from_action(&action, table_partition_cols, &logical_schema);
            file_groups.entry(partition_values).or_default().push(part);
        }

        let mut table_partition_cols = table_partition_cols
            .iter()
            .map(|col| {
                let field = logical_schema.field_with_name(col).unwrap();
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

        let file_scan_config = FileScanConfigBuilder::new(
            object_store_url(&self.log_store.config().location),
            file_schema,
            file_source,
        )
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
    let simplified = simplifier.simplify(expr).unwrap();

    context.create_physical_expr(simplified, df_schema).unwrap()
}

fn prune_file_statistics(
    record_batches: &Vec<RecordBatch>,
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
    let mut partition_values = Vec::new();
    for partition_col in partition_columns {
        let value = action
            .partition_values
            .get(partition_col)
            .and_then(|v| v.as_ref())
            .map(|v| {
                let field = schema.field_with_name(partition_col).unwrap();
                to_correct_scalar_value(
                    &serde_json::Value::String(v.to_string()),
                    field.data_type(),
                )
                .unwrap_or(Some(ScalarValue::Null))
                .unwrap_or(ScalarValue::Null)
            })
            .unwrap_or(ScalarValue::Null);
        partition_values.push(value);
    }

    PartitionedFile {
        object_meta: ObjectMeta {
            location: object_store::path::Path::parse(&action.path).unwrap(),
            size: action.size as u64,
            e_tag: None,
            last_modified: chrono::Utc.timestamp_nanos(0),
            version: None,
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
                    date.signed_duration_since(chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
                        .num_days() as i32,
                ))),
                ArrowDataType::Date64 => Ok(ScalarValue::Date64(Some(
                    date.signed_duration_since(
                        chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap(),
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

pub(crate) async fn execute_plan_to_batch(
    state: &SessionState,
    plan: Arc<dyn ExecutionPlan>,
) -> DeltaResult<datafusion::arrow::record_batch::RecordBatch> {
    unimplemented!();
}

/// Responsible for checking batches of data conform to table's invariants, constraints and nullability.
#[derive(Clone, Default)]
pub struct DeltaDataChecker {
    constraints: Vec<Constraint>,
    invariants: Vec<Invariant>,
    generated_columns: Vec<GeneratedColumn>,
    non_nullable_columns: Vec<String>,
    ctx: SessionContext,
}

impl DeltaDataChecker {
    /// Create a new DeltaDataChecker with no invariants or constraints
    pub fn empty() -> Self {
        unimplemented!();
    }

    /// Create a new DeltaDataChecker with a specified set of invariants
    pub fn new_with_invariants(invariants: Vec<Invariant>) -> Self {
        unimplemented!();
    }

    /// Create a new DeltaDataChecker with a specified set of constraints
    pub fn new_with_constraints(constraints: Vec<Constraint>) -> Self {
        unimplemented!();
    }

    /// Create a new DeltaDataChecker with a specified set of generated columns
    pub fn new_with_generated_columns(generated_columns: Vec<GeneratedColumn>) -> Self {
        unimplemented!();
    }

    /// Specify the Datafusion context
    pub fn with_session_context(mut self, context: SessionContext) -> Self {
        unimplemented!();
    }

    /// Add the specified set of constraints to the current DeltaDataChecker's constraints
    pub fn with_extra_constraints(mut self, constraints: Vec<Constraint>) -> Self {
        unimplemented!();
    }

    /// Create a new DeltaDataChecker
    pub fn new(snapshot: &DeltaTableState) -> Self {
        unimplemented!();
    }

    /// Check that a record batch conforms to table's invariants.
    ///
    /// If it does not, it will return [DeltaTableError::InvalidData] with a list
    /// of values that violated each invariant.
    pub async fn check_batch(&self, record_batch: &RecordBatch) -> Result<(), DeltaTableError> {
        unimplemented!();
    }

    /// Return true if all the nullability checks are valid
    fn check_nullability(&self, record_batch: &RecordBatch) -> Result<bool, DeltaTableError> {
        unimplemented!();
    }

    async fn enforce_checks<C: DataCheck>(
        &self,
        record_batch: &RecordBatch,
        checks: &[C],
    ) -> Result<(), DeltaTableError> {
        unimplemented!();
    }
}

/// A codec for deltalake physical plans
// #[derive(Debug)]
// pub struct DeltaPhysicalCodec {}

// impl PhysicalExtensionCodec for DeltaPhysicalCodec {
//     fn try_decode(
//         &self,
//         buf: &[u8],
//         inputs: &[Arc<dyn ExecutionPlan>],
//         _registry: &dyn FunctionRegistry,
//     ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
//         unimplemented!();
//     }

//     fn try_encode(
//         &self,
//         node: Arc<dyn ExecutionPlan>,
//         buf: &mut Vec<u8>,
//     ) -> Result<(), DataFusionError> {
//         unimplemented!();
//     }
// }

/// Does serde on DeltaTables
// #[derive(Debug)]
// pub struct DeltaLogicalCodec {}

// impl LogicalExtensionCodec for DeltaLogicalCodec {
//     fn try_decode(
//         &self,
//         _buf: &[u8],
//         _inputs: &[LogicalPlan],
//         _ctx: &SessionContext,
//     ) -> Result<Extension, DataFusionError> {
//         unimplemented!();
//     }

//     fn try_encode(&self, _node: &Extension, _buf: &mut Vec<u8>) -> Result<(), DataFusionError> {
//         unimplemented!();
//     }

//     fn try_decode_table_provider(
//         &self,
//         buf: &[u8],
//         _table_ref: &TableReference,
//         _schema: SchemaRef,
//         _ctx: &SessionContext,
//     ) -> Result<Arc<dyn TableProvider>, DataFusionError> {
//         unimplemented!();
//     }

//     fn try_encode_table_provider(
//         &self,
//         _table_ref: &TableReference,
//         node: Arc<dyn TableProvider>,
//         buf: &mut Vec<u8>,
//     ) -> Result<(), DataFusionError> {
//         unimplemented!();
//     }
// }

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
        register_store(self.log_store.clone(), session.runtime_env().clone());
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
        let partition_cols = self.snapshot.metadata().partition_columns.as_slice();
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
    .unwrap();
    is_applicable
}

/// Responsible for creating deltatables
#[derive(Debug)]
pub struct DeltaTableFactory {}

#[async_trait]
impl TableProviderFactory for DeltaTableFactory {
    async fn create(
        &self,
        _ctx: &dyn Session,
        cmd: &CreateExternalTable,
    ) -> datafusion::error::Result<Arc<dyn TableProvider>> {
        unimplemented!();
    }
}

pub(crate) struct FindFilesExprProperties {
    pub partition_columns: Vec<String>,
    pub partition_only: bool,
    pub result: DeltaResult<()>,
}

/// Ensure only expressions that make sense are accepted, check for
/// non-deterministic functions, and determine if the expression only contains
/// partition columns
impl TreeNodeVisitor<'_> for FindFilesExprProperties {
    type Node = Expr;

    fn f_down(&mut self, expr: &Self::Node) -> datafusion::common::Result<TreeNodeRecursion> {
        unimplemented!();
    }
}

#[derive(Debug, Hash, Eq, PartialEq)]
/// Representing the result of the [find_files] function.
pub struct FindFiles {
    /// A list of `Add` objects that match the given predicate
    pub candidates: Vec<Add>,
    /// Was a physical read to the datastore required to determine the candidates
    pub partition_scan: bool,
}

fn join_batches_with_add_actions(
    batches: Vec<RecordBatch>,
    mut actions: HashMap<String, Add>,
    path_column: &str,
    dict_array: bool,
) -> DeltaResult<Vec<Add>> {
    unimplemented!();
}

/// Determine which files contain a record that satisfies the predicate
pub(crate) async fn find_files_scan(
    snapshot: &DeltaTableState,
    log_store: LogStoreRef,
    state: &SessionState,
    expression: Expr,
) -> DeltaResult<Vec<Add>> {
    unimplemented!();
}

pub(crate) async fn scan_memory_table(
    snapshot: &DeltaTableState,
    predicate: &Expr,
) -> DeltaResult<Vec<Add>> {
    unimplemented!();
}

/// Finds files in a snapshot that match the provided predicate.
pub async fn find_files(
    snapshot: &DeltaTableState,
    log_store: LogStoreRef,
    state: &SessionState,
    predicate: Option<Expr>,
) -> DeltaResult<FindFiles> {
    unimplemented!();
}

/// A wrapper for sql_parser's ParserOptions to capture sane default table defaults
pub struct DeltaParserOptions {
    inner: ParserOptions,
}

impl Default for DeltaParserOptions {
    fn default() -> Self {
        unimplemented!();
    }
}

impl From<DeltaParserOptions> for ParserOptions {
    fn from(value: DeltaParserOptions) -> Self {
        unimplemented!();
    }
}

/// A wrapper for Deltafusion's SessionConfig to capture sane default table defaults
pub struct DeltaSessionConfig {
    inner: SessionConfig,
}

impl Default for DeltaSessionConfig {
    fn default() -> Self {
        unimplemented!();
    }
}

impl From<DeltaSessionConfig> for SessionConfig {
    fn from(value: DeltaSessionConfig) -> Self {
        unimplemented!();
    }
}

/// A wrapper for Deltafusion's SessionContext to capture sane default table defaults
pub struct DeltaSessionContext {
    inner: SessionContext,
}

impl Default for DeltaSessionContext {
    fn default() -> Self {
        unimplemented!();
    }
}

impl From<DeltaSessionContext> for SessionContext {
    fn from(value: DeltaSessionContext) -> Self {
        unimplemented!();
    }
}

/// A wrapper for Deltafusion's Column to preserve case-sensitivity during string conversion
pub struct DeltaColumn {
    inner: Column,
}

impl From<&str> for DeltaColumn {
    fn from(c: &str) -> Self {
        unimplemented!();
    }
}

/// Create a column, cloning the string
impl From<&String> for DeltaColumn {
    fn from(c: &String) -> Self {
        unimplemented!();
    }
}

/// Create a column, reusing the existing string
impl From<String> for DeltaColumn {
    fn from(c: String) -> Self {
        unimplemented!();
    }
}

impl From<DeltaColumn> for Column {
    fn from(value: DeltaColumn) -> Self {
        unimplemented!();
    }
}

/// Create a column, resuing the existing datafusion column
impl From<Column> for DeltaColumn {
    fn from(c: Column) -> Self {
        unimplemented!();
    }
}
