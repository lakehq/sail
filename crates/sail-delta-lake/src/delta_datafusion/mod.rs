use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::sync::Arc;

use chrono::TimeZone;
use datafusion::arrow::array::{BooleanArray, DictionaryArray, RecordBatch, StringArray};
use datafusion::arrow::compute::{cast_with_options, filter_record_batch, CastOptions};
use datafusion::arrow::datatypes::{
    DataType as ArrowDataType, Field, Schema as ArrowSchema, SchemaRef,
    SchemaRef as ArrowSchemaRef, TimeUnit, UInt16Type,
};
use datafusion::common::config::ConfigOptions;
use datafusion::common::scalar::ScalarValue;
use datafusion::common::stats::Statistics;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, DFSchema, DataFusionError, Result};
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::physical_plan::wrap_partition_type_in_dict;
use datafusion::execution::SessionState;
use datafusion::logical_expr::planner::ExprPlanner;
use datafusion::logical_expr::{
    AggregateUDF, BinaryExpr, Expr, Operator, ScalarUDF, TableProviderFilterPushDown, TableSource,
};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::expressions::Column as PhysicalColumn;
use datafusion::sql::planner::{ContextProvider, SqlToRel};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;
use datafusion::sql::sqlparser::tokenizer::Tokenizer;
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use deltalake::errors::{DeltaResult, DeltaTableError};
use deltalake::kernel::Add;
use object_store::ObjectMeta;
use serde::{Deserialize, Serialize};
use url::Url;

use crate::kernel::snapshot::{EagerSnapshot, LogDataHandler, Snapshot};
use crate::table::DeltaTableState;
/// [Credit]: <https://github.com/delta-io/delta-rs/blob/3607c314cbdd2ad06c6ee0677b92a29f695c71f3/crates/core/src/delta_datafusion/mod.rs>
pub(crate) const PATH_COLUMN: &str = "__delta_rs_path";
pub mod provider;
pub(crate) mod schema_rewriter;
pub mod type_converter;
pub use provider::DeltaTableProvider;

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
            .map_err(datafusion_to_delta_error)?;
        parse_predicate_expression(&schema, expr, df_state)
    }
}

impl DataFusionMixins for EagerSnapshot {
    fn arrow_schema(&self) -> DeltaResult<ArrowSchemaRef> {
        arrow_schema_from_struct_type(self.schema(), self.metadata().partition_columns(), true)
    }

    fn input_schema(&self) -> DeltaResult<ArrowSchemaRef> {
        arrow_schema_from_struct_type(self.schema(), self.metadata().partition_columns(), false)
    }

    fn parse_predicate_expression(
        &self,
        expr: impl AsRef<str>,
        df_state: &SessionState,
    ) -> DeltaResult<Expr> {
        let schema = DFSchema::try_from(self.arrow_schema()?.as_ref().to_owned())
            .map_err(datafusion_to_delta_error)?;
        parse_predicate_expression(&schema, expr, df_state)
    }
}

impl DataFusionMixins for DeltaTableState {
    fn arrow_schema(&self) -> DeltaResult<ArrowSchemaRef> {
        Ok(Arc::new(self.schema().try_into_arrow()?))
    }

    fn input_schema(&self) -> DeltaResult<ArrowSchemaRef> {
        self.arrow_schema()
    }

    fn parse_predicate_expression(
        &self,
        expr: impl AsRef<str>,
        df_state: &SessionState,
    ) -> DeltaResult<Expr> {
        let schema = DFSchema::try_from(self.arrow_schema()?.as_ref().to_owned())
            .map_err(datafusion_to_delta_error)?;
        parse_predicate_expression(&schema, expr, df_state)
    }
}

impl DataFusionMixins for LogDataHandler<'_> {
    fn arrow_schema(&self) -> DeltaResult<ArrowSchemaRef> {
        // _arrow_schema(self.table_configuration(), true)
        unimplemented!("arrow_schema for LogDataHandler");
    }

    fn input_schema(&self) -> DeltaResult<ArrowSchemaRef> {
        // _arrow_schema(self.table_configuration(), false)
        unimplemented!("input_schema for LogDataHandler");
    }

    fn parse_predicate_expression(
        &self,
        expr: impl AsRef<str>,
        df_state: &SessionState,
    ) -> DeltaResult<Expr> {
        let schema = DFSchema::try_from(self.arrow_schema()?.as_ref().to_owned())
            .map_err(datafusion_to_delta_error)?;
        parse_predicate_expression(&schema, expr, df_state)
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
            let field: Field = f.try_into_arrow()?;
            let field_type = field.data_type().clone();
            Ok(Field::new(field_name, field_type, f.is_nullable()))
        })
        .chain(meta.partition_columns().iter().map(|partition_col| {
            #[allow(clippy::expect_used)]
            let f = schema
                .field(partition_col)
                .expect("Partition column should exist in schema");
            let field: Field = f.try_into_arrow()?;
            let field_name = f.name().to_string();
            let field_type = field.data_type().clone();
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
    schema: &delta_kernel::schema::StructType,
    partition_columns: &[String],
    wrap_partitions: bool,
) -> DeltaResult<ArrowSchemaRef> {
    let fields = schema
        .fields()
        .filter(|f| !partition_columns.contains(&f.name().to_string()))
        .map(|f| {
            // Convert StructField to Arrow Field
            let field_name = f.name().to_string();
            let field: Field = f.try_into_arrow()?;
            let field_type = field.data_type().clone();
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
                let field: Field = f.try_into_arrow()?;
                let field_name = f.name().to_string();
                let field_type = field.data_type().clone();
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

fn arrow_schema_impl(snapshot: &Snapshot, wrap_partitions: bool) -> DeltaResult<ArrowSchemaRef> {
    arrow_schema_from_snapshot(snapshot, wrap_partitions)
}

// Extension trait to add datafusion_table_statistics method to DeltaTableState
trait DeltaTableStateExt {
    fn datafusion_table_statistics(&self, mask: Option<&[bool]>) -> Option<Statistics>;
}

impl DeltaTableStateExt for DeltaTableState {
    fn datafusion_table_statistics(&self, mask: Option<&[bool]>) -> Option<Statistics> {
        if let Some(mask) = mask {
            let es = self.snapshot();
            let boolean_array = BooleanArray::from(mask.to_vec());
            let pruned_files = filter_record_batch(&es.files, &boolean_array).ok()?;
            LogDataHandler::new(&pruned_files, es.table_configuration()).statistics()
        } else {
            self.snapshot().log_data().statistics()
        }
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
    wrap_partition_values: bool,
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
            wrap_partition_values: true,
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

    // /// Indicate that a column containing a records file path is included and column name is user defined.
    // #[allow(dead_code)]
    // pub fn with_file_column_name<S: ToString>(mut self, name: &S) -> Self {
    //     self.file_column_name = Some(name.to_string());
    //     self.include_file_column = true;
    //     self
    // }

    // /// Whether to wrap partition values in a dictionary encoding
    // #[allow(dead_code)]
    // pub fn wrap_partition_values(mut self, wrap: bool) -> Self {
    //     self.wrap_partition_values = Some(wrap);
    //     self
    // }

    // /// Allow pushdown of the scan filter
    // /// When disabled the filter will only be used for pruning files
    // #[allow(dead_code)]
    // pub fn with_parquet_pushdown(mut self, pushdown: bool) -> Self {
    //     self.enable_parquet_pushdown = pushdown;
    //     self
    // }

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
            wrap_partition_values: self.wrap_partition_values,
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

fn partitioned_file_from_action(
    action: &Add,
    partition_columns: &[String],
    schema: &ArrowSchema,
) -> PartitionedFile {
    let partition_values = partition_columns
        .iter()
        .map(|part| {
            let partition_value = match action.partition_values.get(part) {
                Some(val) => val,
                None => return ScalarValue::Null,
            };

            let field = match schema.field_with_name(part) {
                Ok(field) => field,
                Err(_) => return ScalarValue::Null,
            };

            // Convert partition value to ScalarValue
            match partition_value {
                Some(value) => to_correct_scalar_value(
                    &serde_json::Value::String(value.to_string()),
                    field.data_type(),
                )
                .ok()
                .flatten()
                .unwrap_or(ScalarValue::Null),
                None => ScalarValue::try_new_null(field.data_type()).unwrap_or(ScalarValue::Null),
            }
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

fn parse_date(stat_val: &serde_json::Value, field_dt: &ArrowDataType) -> Result<ScalarValue> {
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

fn parse_timestamp(stat_val: &serde_json::Value, field_dt: &ArrowDataType) -> Result<ScalarValue> {
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
) -> Result<Option<ScalarValue>> {
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

/// Extract column names referenced by a PhysicalExpr
pub(crate) fn collect_physical_columns(expr: &Arc<dyn PhysicalExpr>) -> HashSet<String> {
    let mut columns = HashSet::<String>::new();
    let _ = expr.apply(|expr| {
        if let Some(column) = expr.as_any().downcast_ref::<PhysicalColumn>() {
            columns.insert(column.name().to_string());
        }
        Ok(TreeNodeRecursion::Continue)
    });
    columns
}

pub(crate) fn join_batches_with_add_actions(
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
    ) -> Result<Arc<dyn TableSource>> {
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
