use std::any::Any;
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::fmt::{self, Debug};
use std::sync::Arc;

use async_trait::async_trait;
use chrono::TimeZone;
use datafusion::arrow::array::{DictionaryArray, RecordBatch, StringArray};
use datafusion::arrow::compute::{cast_with_options, CastOptions};
use datafusion::arrow::datatypes::{
    DataType as ArrowDataType, Field, Schema as ArrowSchema, SchemaRef,
    SchemaRef as ArrowSchemaRef, TimeUnit, UInt16Type,
};
use datafusion::arrow::error::ArrowError;
use datafusion::catalog::memory::DataSourceExec;
use datafusion::catalog::Session;
use datafusion::common::config::ConfigOptions;
use datafusion::common::scalar::ScalarValue;
use datafusion::common::stats::Statistics;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion, TreeNodeVisitor};
use datafusion::common::{
    Column, DFSchema, DataFusionError, Result as DFResult, Result as DataFusionResult, ToDFSchema,
};
use datafusion::config::TableParquetOptions;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::memory::MemTable;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::physical_plan::{
    wrap_partition_type_in_dict, wrap_partition_value_in_dict, FileGroup, FileScanConfigBuilder,
    FileSource, ParquetSource,
};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::execution::context::{SessionContext, SessionState, TaskContext};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::execution_props::ExecutionProps;
use datafusion::logical_expr::planner::ExprPlanner;
use datafusion::logical_expr::simplify::SimplifyContext;
use datafusion::logical_expr::utils::{conjunction, split_conjunction};
use datafusion::logical_expr::{
    col, AggregateUDF, BinaryExpr, Expr, LogicalPlan, Operator, ScalarUDF,
    TableProviderFilterPushDown, TableSource, Volatility,
};
use datafusion::optimizer::simplify_expressions::ExprSimplifier;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_optimizer::pruning::PruningPredicate;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::limit::LocalLimitExec;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder, MetricsSet};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion::sql::planner::{ContextProvider, SqlToRel};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;
use datafusion::sql::sqlparser::tokenizer::Tokenizer;
use deltalake::errors::{DeltaResult, DeltaTableError};
use deltalake::kernel::{Add, EagerSnapshot, Snapshot};
use deltalake::logstore::LogStoreRef;
use deltalake::table::state::DeltaTableState;
use object_store::path::Path;
use object_store::ObjectMeta;
use serde::{Deserialize, Serialize};
use url::Url;

use crate::delta_datafusion::schema_adapter::DeltaSchemaAdapterFactory;
use crate::kernel::log_data::SailLogDataHandler;
/// [Credit]: <https://github.com/delta-io/delta-rs/blob/3607c314cbdd2ad06c6ee0677b92a29f695c71f3/crates/core/src/delta_datafusion/mod.rs>
pub(crate) const PATH_COLUMN: &str = "__delta_rs_path";

mod schema_adapter;

/// Convert DeltaTableError to DataFusionError
pub fn delta_to_datafusion_error(err: DeltaTableError) -> DataFusionError {
    match err {
        DeltaTableError::Arrow { source } => DataFusionError::ArrowError(Box::new(source), None),
        DeltaTableError::Io { source } => DataFusionError::IoError(source),
        DeltaTableError::ObjectStore { source } => DataFusionError::ObjectStore(Box::new(source)),
        DeltaTableError::Parquet { source } => DataFusionError::ParquetError(Box::new(source)),
        _ => DataFusionError::External(Box::new(err)),
    }
}

/// Convert DataFusionError to DeltaTableError
pub fn datafusion_to_delta_error(err: DataFusionError) -> DeltaTableError {
    match err {
        DataFusionError::ArrowError(source, _) => DeltaTableError::Arrow { source: *source },
        DataFusionError::IoError(source) => DeltaTableError::Io { source },
        DataFusionError::ObjectStore(source) => DeltaTableError::ObjectStore { source: *source },
        DataFusionError::ParquetError(source) => DeltaTableError::Parquet { source: *source },
        _ => DeltaTableError::Generic(err.to_string()),
    }
}

pub(crate) fn create_object_store_url(location: &Url) -> DeltaResult<ObjectStoreUrl> {
    ObjectStoreUrl::parse(&location[..url::Position::BeforePath]).map_err(datafusion_to_delta_error)
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
            #[allow(clippy::expect_used)]
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
                #[allow(clippy::expect_used)]
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

// Extension trait to add datafusion_table_statistics method to DeltaTableState
trait DeltaTableStateExt {
    fn datafusion_table_statistics(&self, mask: Option<Vec<bool>>) -> Option<Statistics>;
}

impl DeltaTableStateExt for DeltaTableState {
    fn datafusion_table_statistics(&self, _mask: Option<Vec<bool>>) -> Option<Statistics> {
        unimplemented!("datafusion_table_statistics is not implemented for DeltaTableState");
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
        #[allow(clippy::expect_used)]
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
        // Delegate to the wrapped DataSourceExec
        self.parquet_scan.children()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        // DeltaScan wraps a DataSourceExec, so we delegate the rewriting to the wrapped plan
        let new_parquet_scan = Arc::clone(&self.parquet_scan).with_new_children(children)?;
        Ok(Arc::new(DeltaScan {
            table_uri: self.table_uri.clone(),
            config: self.config.clone(),
            parquet_scan: new_parquet_scan,
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
            // Change from `arrow_schema` to input_schema for Spark compatibility
            None => self.snapshot.input_schema(),
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
                    if !used_columns.contains(&idx)
                        && !fields.iter().any(|f| f.name() == partition_col)
                    {
                        fields.push(logical_schema.field(idx).to_owned());
                    }
                }
            }
            Arc::new(ArrowSchema::new(fields))
        } else {
            logical_schema
        };

        let df_schema = logical_schema
            .clone()
            .to_dfschema()
            .map_err(datafusion_to_delta_error)?;

        let logical_filter = self
            .filter
            .clone()
            .map(|expr| simplify_expr(self.session.runtime_env().clone(), &df_schema, expr));

        let pushdown_filter = self
            .filter
            .clone()
            .and_then(|expr| {
                let predicates = split_conjunction(&expr);
                let pushdown_filters = get_pushdown_filters(
                    &predicates,
                    self.snapshot.metadata().partition_columns().as_slice(),
                );

                let filtered_predicates = predicates
                    .into_iter()
                    .zip(pushdown_filters.into_iter())
                    .filter_map(|(filter, pushdown)| {
                        if pushdown == TableProviderFilterPushDown::Inexact {
                            Some((*filter).clone())
                        } else {
                            None
                        }
                    });
                conjunction(filtered_predicates)
            })
            .map(|expr| simplify_expr(self.session.runtime_env().clone(), &df_schema, expr));

        let table_partition_cols = self.snapshot.metadata().partition_columns();
        let file_schema = Arc::new(ArrowSchema::new(
            schema
                .fields()
                .iter()
                .filter(|f| !table_partition_cols.contains(f.name()))
                .cloned()
                .collect::<Vec<_>>(),
        ));

        let log_data = SailLogDataHandler::new(
            self.log_store.clone(),
            self.snapshot.load_config().clone(),
            Some(self.snapshot.version()),
        )
        .await?;

        let (files, files_scanned, files_pruned, pruning_mask) = match self.files {
            Some(files) => {
                let files = files.to_owned();
                let files_scanned = files.len();
                (files, files_scanned, 0, None)
            }
            None => {
                // early return in case we have no push down filters or limit
                if logical_filter.is_none() && self.limit.is_none() {
                    let files: Vec<Add> = self.snapshot.file_actions_iter()?.collect();
                    let files_scanned = files.len();
                    (files, files_scanned, 0, None)
                } else {
                    let num_containers = log_data.num_containers();

                    let files_to_prune = if let Some(predicate) = &logical_filter {
                        let pruning_predicate =
                            PruningPredicate::try_new(predicate.clone(), logical_schema.clone())
                                .map_err(datafusion_to_delta_error)?;
                        pruning_predicate
                            .prune(&log_data)
                            .map_err(datafusion_to_delta_error)?
                    } else {
                        vec![true; num_containers]
                    };

                    // needed to enforce limit and deal with missing statistics
                    let mut pruned_without_stats = vec![];
                    let mut rows_collected = 0;
                    let mut files = vec![];

                    for (action, keep) in self
                        .snapshot
                        .file_actions_iter()?
                        .zip(files_to_prune.iter().cloned())
                    {
                        // prune file based on predicate pushdown
                        if keep {
                            // prune file based on limit pushdown
                            if let Some(limit) = self.limit {
                                if let Some(stats) = action.get_stats()? {
                                    if rows_collected <= limit as i64 {
                                        rows_collected += stats.num_records;
                                        files.push(action.to_owned());
                                    } else {
                                        break;
                                    }
                                } else {
                                    // some files are missing stats; skipping but storing them
                                    // in a list in case we can't reach the target limit
                                    pruned_without_stats.push(action.to_owned());
                                }
                            } else {
                                files.push(action.to_owned());
                            }
                        }
                    }

                    if let Some(limit) = self.limit {
                        if rows_collected < limit as i64 {
                            files.extend(pruned_without_stats);
                        }
                    }

                    let files_scanned = files.len();
                    let files_pruned = num_containers - files_scanned;
                    (files, files_scanned, files_pruned, Some(files_to_prune))
                }
            }
        };

        // TODO we group files together by their partition values. If the table is partitioned
        // we may be able to reduce the number of groups by combining groups with the same partition values
        let mut file_groups: HashMap<Vec<ScalarValue>, Vec<PartitionedFile>> = HashMap::new();
        let table_partition_cols = &self.snapshot.metadata().partition_columns();

        for action in files.iter() {
            let mut part = partitioned_file_from_action(action, table_partition_cols, &schema);

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

        // Rewrite the file groups so that the file paths are prepended with
        // the Delta table location.
        file_groups.iter_mut().for_each(|(_, files)| {
            files.iter_mut().for_each(|file| {
                file.object_meta.location = Path::from(format!(
                    "{}{}{}",
                    self.log_store.config().location.path(),
                    object_store::path::DELIMITER,
                    file.object_meta.location
                ));
            });
        });

        let mut table_partition_cols = table_partition_cols
            .iter()
            .map(|col| {
                #[allow(clippy::expect_used)]
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

        let stats = log_data
            .statistics(pruning_mask)
            .unwrap_or_else(|| Statistics::new_unknown(&schema));

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

        let object_store_url = create_object_store_url(&self.log_store.config().location)?;
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
    runtime_env: Arc<RuntimeEnv>,
    df_schema: &DFSchema,
    expr: Expr,
) -> Arc<dyn PhysicalExpr> {
    // Simplify the expression first
    let props = ExecutionProps::new();
    let simplify_context = SimplifyContext::new(&props).with_schema(df_schema.clone().into());
    let simplifier = ExprSimplifier::new(simplify_context).with_max_cycles(10);
    #[allow(clippy::expect_used)]
    let simplified = simplifier
        .simplify(expr)
        .expect("Failed to simplify expression");

    // Create a minimal SessionState to create physical expression
    let session_state = SessionStateBuilder::new()
        .with_runtime_env(runtime_env)
        .build();

    #[allow(clippy::expect_used)]
    session_state
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
                            None => ScalarValue::try_new_null(field.data_type())
                                .unwrap_or(ScalarValue::Null),
                        })
                        .unwrap_or(ScalarValue::Null)
                })
                .unwrap_or(ScalarValue::Null)
        })
        .collect::<Vec<_>>();

    let ts_secs = action.modification_time / 1000;
    let ts_ns = (action.modification_time % 1000) * 1_000_000;
    #[allow(clippy::expect_used)]
    let last_modified = chrono::Utc.from_utc_datetime(
        &chrono::DateTime::from_timestamp(ts_secs, ts_ns as u32)
            .expect("Failed to create timestamp from seconds and nanoseconds")
            .naive_utc(),
    );
    PartitionedFile {
        #[allow(clippy::expect_used)]
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
    let string = match stat_val {
        serde_json::Value::String(s) => s.to_owned(),
        _ => stat_val.to_string(),
    };

    let time_micro = ScalarValue::try_from_string(string, &ArrowDataType::Date32)?;
    let cast_arr = cast_with_options(
        &time_micro.to_array()?,
        field_dt,
        &CastOptions {
            safe: false,
            ..Default::default()
        },
    )?;
    ScalarValue::try_from_array(&cast_arr, 0)
}

fn parse_timestamp(
    stat_val: &serde_json::Value,
    field_dt: &ArrowDataType,
) -> DataFusionResult<ScalarValue> {
    let string = match stat_val {
        serde_json::Value::String(s) => s.to_owned(),
        _ => stat_val.to_string(),
    };

    let time_micro = ScalarValue::try_from_string(
        string,
        &ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
    )?;
    let cast_arr = cast_with_options(
        &time_micro.to_array()?,
        field_dt,
        &CastOptions {
            safe: false,
            ..Default::default()
        },
    )?;
    ScalarValue::try_from_array(&cast_arr, 0)
}

pub(crate) fn to_correct_scalar_value(
    stat_val: &serde_json::Value,
    field_dt: &ArrowDataType,
) -> DataFusionResult<Option<ScalarValue>> {
    match stat_val {
        serde_json::Value::Array(_) => Ok(None),
        serde_json::Value::Object(_) => Ok(None),
        serde_json::Value::Null => Ok(Some(
            ScalarValue::try_new_null(field_dt)
                .map_err(|e| DataFusionError::External(Box::new(e)))?,
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
        let plan = scan.build().await.map_err(delta_to_datafusion_error)?;
        Ok(Arc::new(plan))
    }

    fn supports_filters_pushdown(
        &self,
        filter: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        let partition_cols = self.snapshot.metadata().partition_columns().as_slice();
        Ok(get_pushdown_filters(filter, partition_cols))
    }

    fn statistics(&self) -> Option<Statistics> {
        self.snapshot.datafusion_table_statistics(Option::None)
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
    #[allow(clippy::expect_used)]
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

pub(crate) fn get_path_column<'a>(
    batch: &'a RecordBatch,
    path_column: &str,
) -> DeltaResult<impl Iterator<Item = Option<&'a str>>> {
    let err = || DeltaTableError::Generic("Unable to obtain Delta-rs path column".to_string());
    let dict_array = batch
        .column_by_name(path_column)
        .ok_or_else(err)?
        .as_any()
        .downcast_ref::<DictionaryArray<UInt16Type>>()
        .ok_or_else(err)?;

    let values = dict_array
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(err)?;

    Ok(dict_array
        .keys()
        .iter()
        .map(move |key| key.and_then(|k| values.value(k as usize).into())))
}

pub(crate) struct FindFilesExprProperties {
    pub partition_columns: Vec<String>,
    pub partition_only: bool,
    pub result: DeltaResult<()>,
}

impl TreeNodeVisitor<'_> for FindFilesExprProperties {
    type Node = Expr;

    fn f_down(&mut self, expr: &Self::Node) -> datafusion::common::Result<TreeNodeRecursion> {
        match expr {
            Expr::Column(c) => {
                if !self.partition_columns.contains(&c.name) {
                    self.partition_only = false;
                }
            }
            Expr::ScalarVariable(_, _)
            | Expr::Literal(_, _)
            | Expr::Alias(_)
            | Expr::BinaryExpr(_)
            | Expr::Like(_)
            | Expr::SimilarTo(_)
            | Expr::Not(_)
            | Expr::IsNotNull(_)
            | Expr::IsNull(_)
            | Expr::IsTrue(_)
            | Expr::IsFalse(_)
            | Expr::IsUnknown(_)
            | Expr::IsNotTrue(_)
            | Expr::IsNotFalse(_)
            | Expr::IsNotUnknown(_)
            | Expr::Negative(_)
            | Expr::InList { .. }
            | Expr::Between(_)
            | Expr::Case(_)
            | Expr::Cast(_)
            | Expr::TryCast(_) => (),
            Expr::ScalarFunction(scalar_function) => {
                match scalar_function.func.signature().volatility {
                    Volatility::Immutable => (),
                    _ => {
                        self.result = Err(DeltaTableError::Generic(format!(
                            "Find files predicate contains nondeterministic function {}",
                            scalar_function.func.name()
                        )));
                        return Ok(TreeNodeRecursion::Stop);
                    }
                }
            }
            _ => {
                self.result = Err(DeltaTableError::Generic(format!(
                    "Find files predicate contains unsupported expression {expr}"
                )));
                return Ok(TreeNodeRecursion::Stop);
            }
        }

        Ok(TreeNodeRecursion::Continue)
    }
}

#[derive(Debug, Hash, Eq, PartialEq)]
pub struct FindFiles {
    pub candidates: Vec<Add>,
    pub partition_scan: bool,
}

fn join_batches_with_add_actions(
    batches: Vec<RecordBatch>,
    mut actions: HashMap<String, Add>,
    path_column: &str,
    dict_array: bool,
) -> DeltaResult<Vec<Add>> {
    let mut files = Vec::with_capacity(batches.iter().map(|batch| batch.num_rows()).sum());
    for batch in batches {
        let err = || DeltaTableError::Generic("Unable to obtain Delta-rs path column".to_string());

        let iter: Box<dyn Iterator<Item = Option<&str>>> = if dict_array {
            let array = get_path_column(&batch, path_column)?;
            Box::new(array)
        } else {
            let array = batch
                .column_by_name(path_column)
                .ok_or_else(err)?
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(err)?;
            Box::new(array.iter())
        };

        for path in iter {
            let path = path.ok_or(DeltaTableError::Generic(format!(
                "{path_column} cannot be null"
            )))?;

            match actions.remove(path) {
                Some(action) => files.push(action),
                None => {
                    return Err(DeltaTableError::Generic(
                        "Unable to map __delta_rs_path to action.".to_owned(),
                    ))
                }
            }
        }
    }
    Ok(files)
}

pub(crate) async fn find_files_scan(
    snapshot: &DeltaTableState,
    log_store: LogStoreRef,
    state: &SessionState,
    expression: Expr,
) -> DeltaResult<Vec<Add>> {
    let candidate_map: HashMap<String, Add> = snapshot
        .file_actions_iter()?
        .map(|add| (add.path.clone(), add.to_owned()))
        .collect();

    let scan_config = DeltaScanConfigBuilder {
        include_file_column: true,
        ..Default::default()
    }
    .build(snapshot)?;

    let logical_schema = df_logical_schema(snapshot, &scan_config.file_column_name, None)?;

    let mut used_columns = expression
        .column_refs()
        .into_iter()
        .map(|column| logical_schema.index_of(&column.name))
        .collect::<Result<Vec<usize>, ArrowError>>()?;
    #[allow(clippy::unwrap_used)]
    used_columns.push(logical_schema.index_of(scan_config.file_column_name.as_ref().unwrap())?);

    let scan = DeltaScanBuilder::new(snapshot, log_store, state)
        .with_filter(Some(expression.clone()))
        .with_projection(Some(&used_columns))
        .with_scan_config(scan_config)
        .build()
        .await?;
    let scan = Arc::new(scan);

    let _config = &scan.config;
    let input_schema = scan.logical_schema.as_ref().to_owned();
    let input_dfschema = input_schema
        .clone()
        .try_into()
        .map_err(datafusion_to_delta_error)?;

    let predicate_expr = state
        .create_physical_expr(Expr::IsTrue(Box::new(expression.clone())), &input_dfschema)
        .map_err(datafusion_to_delta_error)?;

    let filter: Arc<dyn ExecutionPlan> = Arc::new(
        FilterExec::try_new(predicate_expr, scan.clone()).map_err(datafusion_to_delta_error)?,
    );
    let limit: Arc<dyn ExecutionPlan> = Arc::new(LocalLimitExec::new(filter, 1));

    let task_ctx = Arc::new(TaskContext::from(state));
    let mut partitions = Vec::new();
    for i in 0..limit.output_partitioning().partition_count() {
        let stream = limit
            .execute(i, task_ctx.clone())
            .map_err(datafusion_to_delta_error)?;
        let data = datafusion::physical_plan::common::collect(stream)
            .await
            .map_err(datafusion_to_delta_error)?;
        partitions.extend(data);
    }

    let map = candidate_map.into_iter().collect::<HashMap<String, Add>>();

    join_batches_with_add_actions(partitions, map, PATH_COLUMN, true)
}

pub(crate) async fn scan_memory_table(
    snapshot: &DeltaTableState,
    state: &SessionState,
    predicate: &Expr,
) -> DeltaResult<Vec<Add>> {
    let actions = snapshot.file_actions()?;

    let batch = snapshot.add_actions_table(true)?;
    let mut arrays = Vec::new();
    let mut fields = Vec::new();

    let schema = batch.schema();

    arrays.push(
        batch
            .column_by_name("path")
            .ok_or(DeltaTableError::Generic(
                "Column with name `path` does not exist".to_owned(),
            ))?
            .to_owned(),
    );
    fields.push(Field::new(PATH_COLUMN, ArrowDataType::Utf8, false));

    for field in schema.fields() {
        #[allow(clippy::unwrap_used)]
        if field.name().starts_with("partition.") {
            let name = field.name().strip_prefix("partition.").unwrap();

            arrays.push(batch.column_by_name(field.name()).unwrap().to_owned());
            fields.push(Field::new(
                name,
                field.data_type().to_owned(),
                field.is_nullable(),
            ));
        }
    }

    let schema = Arc::new(ArrowSchema::new(fields));
    let batch = RecordBatch::try_new(schema, arrays)?;
    let mem_table =
        MemTable::try_new(batch.schema(), vec![vec![batch]]).map_err(datafusion_to_delta_error)?;

    let ctx = SessionContext::new_with_state(state.clone());
    let mut df = ctx
        .read_table(Arc::new(mem_table))
        .map_err(datafusion_to_delta_error)?;
    df = df
        .filter(predicate.to_owned())
        .map_err(datafusion_to_delta_error)?
        .select(vec![col(PATH_COLUMN)])
        .map_err(datafusion_to_delta_error)?;
    let batches = df.collect().await.map_err(datafusion_to_delta_error)?;

    let map = actions
        .into_iter()
        .map(|action| (action.path.clone(), action))
        .collect::<HashMap<String, Add>>();

    join_batches_with_add_actions(batches, map, PATH_COLUMN, false)
}

pub async fn find_files(
    snapshot: &DeltaTableState,
    log_store: LogStoreRef,
    state: &SessionState,
    predicate: Option<Expr>,
) -> DeltaResult<FindFiles> {
    let current_metadata = snapshot.metadata();

    match &predicate {
        Some(predicate) => {
            let mut expr_properties = FindFilesExprProperties {
                partition_only: true,
                partition_columns: current_metadata.partition_columns().clone(),
                result: Ok(()),
            };

            TreeNode::visit(predicate, &mut expr_properties).map_err(datafusion_to_delta_error)?;
            expr_properties.result?;

            if expr_properties.partition_only {
                let candidates = scan_memory_table(snapshot, state, predicate).await?;
                Ok(FindFiles {
                    candidates,
                    partition_scan: true,
                })
            } else {
                let candidates =
                    find_files_scan(snapshot, log_store, state, predicate.to_owned()).await?;

                Ok(FindFiles {
                    candidates,
                    partition_scan: false,
                })
            }
        }
        None => Ok(FindFiles {
            candidates: snapshot.file_actions()?,
            partition_scan: true,
        }),
    }
}

/// Simple context provider for Delta Lake expression parsing
pub(crate) struct DeltaContextProvider<'a> {
    state: &'a SessionState,
}

impl<'a> DeltaContextProvider<'a> {
    fn new(state: &'a SessionState) -> Self {
        DeltaContextProvider { state }
    }
}

impl ContextProvider for DeltaContextProvider<'_> {
    fn get_table_source(
        &self,
        _name: datafusion::common::TableReference,
    ) -> DFResult<Arc<dyn TableSource>> {
        unimplemented!("DeltaContextProvider does not support table sources")
    }

    fn get_expr_planners(&self) -> &[Arc<dyn ExprPlanner>] {
        #[allow(clippy::disallowed_methods)]
        self.state.expr_planners()
    }

    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>> {
        self.state.scalar_functions().get(name).cloned()
    }

    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>> {
        self.state.aggregate_functions().get(name).cloned()
    }

    fn get_window_meta(&self, name: &str) -> Option<Arc<datafusion::logical_expr::WindowUDF>> {
        self.state.window_functions().get(name).cloned()
    }

    fn get_variable_type(&self, _var: &[String]) -> Option<ArrowDataType> {
        unimplemented!("DeltaContextProvider does not support variables")
    }

    fn options(&self) -> &ConfigOptions {
        self.state.config_options()
    }

    fn udf_names(&self) -> Vec<String> {
        self.state.scalar_functions().keys().cloned().collect()
    }

    fn udaf_names(&self) -> Vec<String> {
        self.state.aggregate_functions().keys().cloned().collect()
    }

    fn udwf_names(&self) -> Vec<String> {
        self.state.window_functions().keys().cloned().collect()
    }
}

/// Parse a string predicate into a DataFusion `Expr`
pub fn parse_predicate_expression(
    schema: &DFSchema,
    expr: impl AsRef<str>,
    state: &SessionState,
) -> DeltaResult<Expr> {
    let dialect = &GenericDialect {};
    let mut tokenizer = Tokenizer::new(dialect, expr.as_ref());
    let tokens = tokenizer
        .tokenize()
        .map_err(|err| DeltaTableError::Generic(format!("Failed to tokenize expression: {err}")))?;

    let sql = Parser::new(dialect)
        .with_tokens(tokens)
        .parse_expr()
        .map_err(|err| DeltaTableError::Generic(format!("Failed to parse expression: {err}")))?;

    let context_provider = DeltaContextProvider::new(state);
    let sql_to_rel = SqlToRel::new(&context_provider);

    sql_to_rel
        .sql_to_expr(sql, schema, &mut Default::default())
        .map_err(|err| {
            DeltaTableError::Generic(format!("Failed to convert SQL to expression: {err}"))
        })
}
