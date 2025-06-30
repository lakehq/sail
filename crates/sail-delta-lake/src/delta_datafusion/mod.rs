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

use datafusion::arrow::array::types::UInt16Type;
use datafusion::arrow::array::{
    Array, BooleanArray, DictionaryArray, RecordBatch, StringArray, TypedDictionaryArray,
};
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::arrow::compute::kernels::cast::{cast_with_options, CastOptions};
use datafusion::arrow::datatypes::{
    DataType as ArrowDataType, Field, Schema as ArrowSchema, SchemaRef,
    SchemaRef as ArrowSchemaRef, TimeUnit,
};
// use arrow_select::concat::concat_batches;
// use arrow_select::filter::filter_record_batch;
use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};
use datafusion::catalog::memory::DataSourceExec;
use datafusion::catalog::{Session, TableProviderFactory};
use datafusion::common::scalar::ScalarValue;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion, TreeNodeVisitor};
use datafusion::common::{
    config::ConfigOptions, Column, DFSchema, DataFusionError, Result as DataFusionResult,
    TableReference, ToDFSchema,
};
use datafusion::config::TableParquetOptions;
use datafusion::datasource::physical_plan::{
    wrap_partition_type_in_dict, wrap_partition_value_in_dict, FileGroup, FileScanConfigBuilder,
    ParquetSource,
};
use datafusion::datasource::{listing::PartitionedFile, MemTable, TableProvider, TableType};
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
    Statistics,
};
use datafusion::sql::planner::ParserOptions;
// use datafusion_proto::logical_plan::LogicalExtensionCodec;
// use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
use either::Either;
use futures::TryStreamExt;

use object_store::ObjectMeta;
use serde::{Deserialize, Serialize};

use url::Url;

use crate::delta_datafusion::expr::parse_predicate_expression;
use crate::delta_datafusion::schema_adapter::DeltaSchemaAdapterFactory;

use deltalake::errors::{DeltaResult, DeltaTableError};
use deltalake::kernel::{
    Add, DataCheck, EagerSnapshot, Invariant, LogDataHandler, Snapshot, StructTypeExt,
};
use deltalake::logstore::LogStoreRef;
use deltalake::table::builder::ensure_table_uri;
use deltalake::table::state::DeltaTableState;
use deltalake::table::{Constraint, GeneratedColumn};
use deltalake::{open_table, open_table_with_storage_options, DeltaTable};

pub(crate) const PATH_COLUMN: &str = "__delta_rs_path";

pub mod cdf;
pub mod expr;
pub mod logical;
pub mod physical;
pub mod planner;

pub use cdf::scan::DeltaCdfTableProvider;

mod schema_adapter;

// impl From<DeltaTableError> for DataFusionError {
//     fn from(err: DeltaTableError) -> Self {
//         unimplemented!();
//     }
// }

// impl From<DataFusionError> for DeltaTableError {
//     fn from(err: DataFusionError) -> Self {
//         unimplemented!();
//     }
// }

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
        unimplemented!();
    }

    fn input_schema(&self) -> DeltaResult<ArrowSchemaRef> {
        unimplemented!();
    }

    fn parse_predicate_expression(
        &self,
        expr: impl AsRef<str>,
        df_state: &SessionState,
    ) -> DeltaResult<Expr> {
        unimplemented!();
    }
}

impl DataFusionMixins for EagerSnapshot {
    fn arrow_schema(&self) -> DeltaResult<ArrowSchemaRef> {
        unimplemented!();
    }

    fn input_schema(&self) -> DeltaResult<ArrowSchemaRef> {
        unimplemented!();
    }

    fn parse_predicate_expression(
        &self,
        expr: impl AsRef<str>,
        df_state: &SessionState,
    ) -> DeltaResult<Expr> {
        unimplemented!();
    }
}

impl DataFusionMixins for DeltaTableState {
    fn arrow_schema(&self) -> DeltaResult<ArrowSchemaRef> {
        unimplemented!();
    }

    fn input_schema(&self) -> DeltaResult<ArrowSchemaRef> {
        unimplemented!();
    }

    fn parse_predicate_expression(
        &self,
        expr: impl AsRef<str>,
        df_state: &SessionState,
    ) -> DeltaResult<Expr> {
        unimplemented!();
    }
}

fn _arrow_schema(snapshot: &Snapshot, wrap_partitions: bool) -> DeltaResult<ArrowSchemaRef> {
    unimplemented!();
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

// impl DeltaTableState {
//     /// Provide table level statistics to Datafusion
//     pub fn datafusion_table_statistics(&self) -> Option<Statistics> {
//         unimplemented!();
//     }
// }

pub(crate) fn register_store(store: LogStoreRef, env: Arc<RuntimeEnv>) {
    unimplemented!();
}

/// The logical schema for a Deltatable is different from the protocol level schema since partition
/// columns must appear at the end of the schema. This is to align with how partition are handled
/// at the physical level
pub(crate) fn df_logical_schema(
    snapshot: &DeltaTableState,
    file_column_name: &Option<String>,
    schema: Option<ArrowSchemaRef>,
) -> DeltaResult<SchemaRef> {
    unimplemented!();
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
        unimplemented!();
    }
}

impl DeltaScanConfigBuilder {
    /// Construct a new instance of `DeltaScanConfigBuilder`
    pub fn new() -> Self {
        unimplemented!();
    }

    /// Indicate that a column containing a records file path is included.
    /// Column name is generated and can be determined once this Config is built
    pub fn with_file_column(mut self, include: bool) -> Self {
        unimplemented!();
    }

    /// Indicate that a column containing a records file path is included and column name is user defined.
    pub fn with_file_column_name<S: ToString>(mut self, name: &S) -> Self {
        unimplemented!();
    }

    /// Whether to wrap partition values in a dictionary encoding
    pub fn wrap_partition_values(mut self, wrap: bool) -> Self {
        unimplemented!();
    }

    /// Allow pushdown of the scan filter
    /// When disabled the filter will only be used for pruning files
    pub fn with_parquet_pushdown(mut self, pushdown: bool) -> Self {
        unimplemented!();
    }

    /// Use the provided [SchemaRef] for the [DeltaScan]
    pub fn with_schema(mut self, schema: SchemaRef) -> Self {
        unimplemented!();
    }

    /// Build a DeltaScanConfig and ensure no column name conflicts occur during downstream processing
    pub fn build(&self, snapshot: &DeltaTableState) -> DeltaResult<DeltaScanConfig> {
        unimplemented!();
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
        unimplemented!();
    }

    pub fn with_filter(mut self, filter: Option<Expr>) -> Self {
        unimplemented!();
    }

    pub fn with_files(mut self, files: &'a [Add]) -> Self {
        unimplemented!();
    }

    pub fn with_projection(mut self, projection: Option<&'a Vec<usize>>) -> Self {
        unimplemented!();
    }

    pub fn with_limit(mut self, limit: Option<usize>) -> Self {
        unimplemented!();
    }

    pub fn with_scan_config(mut self, config: DeltaScanConfig) -> Self {
        unimplemented!();
    }

    pub async fn build(self) -> DeltaResult<DeltaScan> {
        unimplemented!();
    }
}

fn simplify_expr(
    context: &SessionContext,
    df_schema: &DFSchema,
    expr: Expr,
) -> Arc<dyn PhysicalExpr> {
    unimplemented!();
}

fn prune_file_statistics(
    record_batches: &Vec<RecordBatch>,
    pruning_mask: Vec<bool>,
) -> Vec<RecordBatch> {
    unimplemented!();
}

// #[async_trait]
// impl TableProvider for DeltaTable {
//     fn as_any(&self) -> &dyn Any {
//         unimplemented!();
//     }

//     fn schema(&self) -> Arc<ArrowSchema> {
//         unimplemented!();
//     }

//     fn table_type(&self) -> TableType {
//         unimplemented!();
//     }

//     fn get_table_definition(&self) -> Option<&str> {
//         unimplemented!();
//     }

//     fn get_logical_plan(&self) -> Option<Cow<'_, LogicalPlan>> {
//         unimplemented!();
//     }

//     async fn scan(
//         &self,
//         session: &dyn Session,
//         projection: Option<&Vec<usize>>,
//         filters: &[Expr],
//         limit: Option<usize>,
//     ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
//         unimplemented!();
//     }

//     fn supports_filters_pushdown(
//         &self,
//         filter: &[&Expr],
//     ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
//         unimplemented!();
//     }

//     fn statistics(&self) -> Option<Statistics> {
//         unimplemented!();
//     }
// }

fn get_pushdown_filters(
    filter: &[&Expr],
    partition_cols: &[String],
) -> Vec<TableProviderFilterPushDown> {
    unimplemented!();
}

fn expr_is_exact_predicate_for_cols(partition_cols: &[String], expr: &Expr) -> bool {
    unimplemented!();
}

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
        unimplemented!();
    }

    /// Define which files to consider while building a scan, for advanced usecases
    pub fn with_files(mut self, files: Vec<Add>) -> DeltaTableProvider {
        unimplemented!();
    }
}

#[async_trait]
impl TableProvider for DeltaTableProvider {
    fn as_any(&self) -> &dyn Any {
        unimplemented!();
    }

    fn schema(&self) -> Arc<ArrowSchema> {
        unimplemented!();
    }

    fn table_type(&self) -> TableType {
        unimplemented!();
    }

    fn get_table_definition(&self) -> Option<&str> {
        unimplemented!();
    }

    fn get_logical_plan(&self) -> Option<Cow<'_, LogicalPlan>> {
        unimplemented!();
    }

    async fn scan(
        &self,
        session: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        unimplemented!();
    }

    fn supports_filters_pushdown(
        &self,
        filter: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        unimplemented!();
    }

    fn statistics(&self) -> Option<Statistics> {
        unimplemented!();
    }
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
        unimplemented!();
    }
}

impl ExecutionPlan for DeltaScan {
    fn name(&self) -> &str {
        unimplemented!();
    }

    fn as_any(&self) -> &dyn Any {
        unimplemented!();
    }

    fn schema(&self) -> SchemaRef {
        unimplemented!();
    }

    fn properties(&self) -> &PlanProperties {
        unimplemented!();
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        unimplemented!();
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        unimplemented!();
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        unimplemented!();
    }

    fn metrics(&self) -> Option<MetricsSet> {
        unimplemented!();
    }

    fn statistics(&self) -> DataFusionResult<Statistics> {
        unimplemented!();
    }

    fn repartitioned(
        &self,
        target_partitions: usize,
        config: &ConfigOptions,
    ) -> DataFusionResult<Option<Arc<dyn ExecutionPlan>>> {
        unimplemented!();
    }
}

pub(crate) fn get_null_of_arrow_type(t: &ArrowDataType) -> DeltaResult<ScalarValue> {
    unimplemented!();
}

fn partitioned_file_from_action(
    action: &Add,
    partition_columns: &[String],
    schema: &ArrowSchema,
) -> PartitionedFile {
    unimplemented!();
}

fn parse_date(
    stat_val: &serde_json::Value,
    field_dt: &ArrowDataType,
) -> DataFusionResult<ScalarValue> {
    unimplemented!();
}

fn parse_timestamp(
    stat_val: &serde_json::Value,
    field_dt: &ArrowDataType,
) -> DataFusionResult<ScalarValue> {
    unimplemented!();
}

pub(crate) fn to_correct_scalar_value(
    stat_val: &serde_json::Value,
    field_dt: &ArrowDataType,
) -> DataFusionResult<Option<ScalarValue>> {
    unimplemented!();
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
