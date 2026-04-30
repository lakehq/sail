use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::catalog::Session;
use datafusion::common::plan_datafusion_err;
use datafusion::physical_expr::{
    create_physical_sort_exprs, LexOrdering, LexRequirement, PhysicalSortRequirement,
};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::{not_impl_err, plan_err, Constraints, DFSchema, Result};
use datafusion_expr::expr::Sort;
use datafusion_expr::TableSource;

use crate::catalog::CatalogPartitionField;
use crate::extension::SessionExtension;
use crate::logical_expr::ExprWithSource;

/// File path metadata column for row-level modifications (MERGE, UPDATE, DELETE).
pub const MERGE_FILE_COLUMN: &str = "__sail_file_path";

/// File-local row index metadata column for row-level modifications that write deletion vectors.
pub const MERGE_ROW_INDEX_COLUMN: &str = "__sail_file_row_index";

/// Row-level operation type column appended to the expanded MERGE output.
/// Value is one of the [`RowLevelOperationType`] integer constants.
pub const OPERATION_COLUMN: &str = "__sail_operation_type";

/// A layer of options that can be applied to a data source.
/// Multiple layers are used to represent different sources of options,
/// applied in order so that later layers override earlier ones.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum OptionLayer {
    /// Options stored as table properties in a catalog.
    TablePropertyList { items: Vec<(String, String)> },
    /// Options provided by the data source operation.
    OptionList { items: Vec<(String, String)> },
    /// The location of the data source.
    TableLocation { value: String },
    /// Time travel: read data as of a specific timestamp.
    AsOfTimestamp { value: DateTime<Utc> },
    /// Time travel: read data as of a specific integer version.
    AsOfIntegerVersion { value: i64 },
    /// Time travel: read data as of a specific string version (e.g. a branch or tag name).
    AsOfStringVersion { value: String },
}

impl OptionLayer {
    /// Converts this option layer into an opaque key-value map.
    ///
    /// This is used for data sources that have not yet migrated to the typed
    /// option system. The returned map can be passed to existing code that
    /// accepts `HashMap<String, String>`.
    pub fn into_opaque_options(self) -> HashMap<String, String> {
        match self {
            OptionLayer::TablePropertyList { items } => items.into_iter().collect(),
            OptionLayer::OptionList { items } => items.into_iter().collect(),
            OptionLayer::TableLocation { .. }
            | OptionLayer::AsOfTimestamp { .. }
            | OptionLayer::AsOfIntegerVersion { .. }
            | OptionLayer::AsOfStringVersion { .. } => HashMap::new(),
        }
    }
}

/// Row-level operation type tag.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(i32)]
pub enum RowLevelOperationType {
    Delete = 1,
    Update = 2,
    Insert = 3,
}

impl RowLevelOperationType {
    pub fn as_i32(self) -> i32 {
        self as i32
    }
}

/// Materialization strategy for row-level modifications.
///
/// - `Eager`: rewrite affected files (Copy-on-Write).
/// - `MergeOnRead`: write delete files at write time, merge at read time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum MergeStrategy {
    #[default]
    Eager,
    MergeOnRead,
}

/// Returns true for lakehouse formats that support row-level modifications.
pub fn is_lakehouse_format(format: &str) -> bool {
    format.eq_ignore_ascii_case("delta") || format.eq_ignore_ascii_case("iceberg")
}

/// Implemented by [`TableSource`]s that can expose a per-row file path column
/// for row-level modifications (MERGE targeted rewrite).
pub trait MergeCapableSource: Send + Sync {
    /// Returns the file column name if already configured.
    fn file_column_name(&self) -> Option<&str>;

    /// Returns a reconfigured source with the file column enabled.
    fn with_file_column(&self, name: &str) -> Result<Arc<dyn TableSource>>;

    /// Returns the file-local row index column name if already configured.
    fn row_index_column_name(&self) -> Option<&str>;

    /// Returns a reconfigured source with the file-local row index column enabled.
    fn with_row_index_column(&self, name: &str) -> Result<Arc<dyn TableSource>>;
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, PartialOrd)]
pub enum SinkMode {
    ErrorIfExists,
    IgnoreIfExists,
    Append,
    Overwrite,
    OverwriteIf { condition: Box<ExprWithSource> },
    OverwritePartitions,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum PhysicalSinkMode {
    ErrorIfExists,
    IgnoreIfExists,
    Append,
    Overwrite,
    OverwriteIf {
        /// Driver-side logical predicate. Omitted on remote workers.
        condition: Option<Box<ExprWithSource>>,
        /// SQL source string used by commit metadata.
        source: Option<String>,
    },
    OverwritePartitions,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, PartialOrd)]
pub struct BucketBy {
    pub columns: Vec<String>,
    pub num_buckets: usize,
}

/// Information required to create a data source.
#[derive(Debug, Clone)]
pub struct SourceInfo {
    pub paths: Vec<String>,
    /// The (optional) schema of the data source including partitioning columns.
    pub schema: Option<Schema>,
    pub constraints: Constraints,
    pub partition_by: Vec<String>,
    pub bucket_by: Option<BucketBy>,
    pub sort_order: Vec<Sort>,
    /// The layers of options for the data source.
    /// A later layer can override earlier ones.
    pub options: Vec<OptionLayer>,
}

/// Information required to create a data writer.
#[derive(Debug, Clone)]
pub struct SinkInfo {
    pub input: Arc<dyn ExecutionPlan>,
    pub mode: PhysicalSinkMode,
    pub partition_by: Vec<CatalogPartitionField>,
    pub bucket_by: Option<BucketBy>,
    pub sort_order: Option<LexRequirement>,
    /// The sets of options for the data sink.
    /// A later set of options can override earlier ones.
    /// The path for the sink is stored under the `"path"` key in options.
    pub options: Vec<OptionLayer>,
    /// The logical schema of the writer's input, if available. This schema can
    /// preserve arrow field metadata that the physical planner may strip (e.g.
    /// metadata attached via `Expr::Alias::with_metadata`). Table formats can use
    /// this to recover column-level metadata such as `delta.generationExpression`.
    pub logical_schema: Option<datafusion_common::DFSchemaRef>,
}

/// Returns the path from options, or `None` if not set.
/// Checks the `"path"` key first, then `"location"`.
/// Key comparison is case-insensitive.
pub fn find_path_in_options(options: &[OptionLayer]) -> Option<String> {
    let find = |key: &str| -> Option<String> {
        for layer in options.iter().rev() {
            let items = match layer {
                OptionLayer::OptionList { items } => items,
                OptionLayer::TablePropertyList { items } => items,
                _ => continue,
            };
            if let Some(v) = items.iter().find_map(|(k, v)| {
                if k.eq_ignore_ascii_case(key) {
                    Some(v.clone())
                } else {
                    None
                }
            }) {
                return Some(v);
            }
        }
        None
    };
    find("path").or_else(|| find("location"))
}

/// The kind of row-level DML command being executed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RowLevelCommand {
    Delete,
    Update,
    Merge,
}

/// Target table information shared by all row-level operations.
#[derive(Debug, Clone)]
pub struct RowLevelTargetInfo {
    pub table_name: Vec<String>,
    pub path: String,
    pub partition_by: Vec<String>,
    pub options: Vec<OptionLayer>,
}

/// Operation metadata used to construct commit log `operationParameters`.
#[derive(Debug, Clone)]
pub struct MergePredicateInfo {
    pub action_type: String,
    pub predicate: Option<String>,
}

/// Override metadata for operation commit logs.
#[derive(Debug, Clone)]
pub enum OperationOverride {
    Merge {
        predicate: Option<String>,
        merge_predicate: Option<String>,
        matched_predicates: Vec<MergePredicateInfo>,
        not_matched_predicates: Vec<MergePredicateInfo>,
        not_matched_by_source_predicates: Vec<MergePredicateInfo>,
    },
}

/// Unified information for all row-level write operations (DELETE, UPDATE, MERGE).
#[derive(Debug, Clone)]
pub struct RowLevelWriteInfo {
    pub command: RowLevelCommand,
    pub target: RowLevelTargetInfo,
    /// Condition for DELETE/UPDATE. `None` for MERGE.
    pub condition: Option<ExprWithSource>,
    /// Pre-expanded physical plan for writing (MERGE, future UPDATE).
    pub expanded_input: Option<Arc<dyn ExecutionPlan>>,
    /// Physical plan that yields touched file paths (MERGE targeted rewrite).
    pub touched_file_plan: Option<Arc<dyn ExecutionPlan>>,
    /// Physical plan that yields target file path and file-local row index rows to delete via DVs.
    pub deletion_vector_plan: Option<Arc<dyn ExecutionPlan>>,
    pub with_schema_evolution: bool,
    /// Override for commit operation metadata.
    pub operation_override: Option<OperationOverride>,
    /// Materialization strategy. Defaults to [`MergeStrategy::Eager`].
    pub merge_strategy: MergeStrategy,
}

// TODO: MERGE schema evolution end-to-end
// - Expand sink schema during MERGE: detect source-only columns (case-insensitive), keep target order, append new cols, project source/NULL for them.
// - Emit Metadata (and Protocol if required) in writer/commit so the new schema is persisted and readable.
// - Reading: time-travel must stay on the requested version; non-time-travel can refresh to latest snapshot to see new schema.

/// A trait for preparing physical execution for a specific format.
#[async_trait]
pub trait TableFormat: Send + Sync {
    /// Returns the name of the format.
    fn name(&self) -> &str;

    /// Creates a logical [`TableSource`] for read.
    async fn create_source(
        &self,
        ctx: &dyn Session,
        info: SourceInfo,
    ) -> Result<Arc<dyn TableSource>>;

    /// Creates a `ExecutionPlan` for write.
    async fn create_writer(
        &self,
        ctx: &dyn Session,
        info: SinkInfo,
    ) -> Result<Arc<dyn ExecutionPlan>>;

    /// Creates an `ExecutionPlan` for row-level operations (DELETE, UPDATE, MERGE).
    async fn create_row_level_writer(
        &self,
        ctx: &dyn Session,
        info: RowLevelWriteInfo,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let _ = (ctx, info);
        not_impl_err!(
            "Row-level operations are not yet implemented for {} format",
            self.name()
        )
    }

    /// Returns the materialization strategy for row-level modifications.
    /// Defaults to [`MergeStrategy::Eager`]. Override for Merge-on-Read formats.
    fn merge_strategy(&self) -> MergeStrategy {
        MergeStrategy::Eager
    }

    /// Alters table properties (SET/UNSET TBLPROPERTIES).
    ///
    /// `changes` is a list of `(key, value)` pairs where `value` is `Some(v)` to set a property,
    /// or `None` to unset/remove it. When `if_exists` is `false`, implementations MUST error if
    /// an UNSET key is not present on the table; when `if_exists` is `true`, UNSET for a missing
    /// key is a no-op. The implementation is responsible for committing these changes to the
    /// underlying table storage (e.g., writing a new Delta log entry).
    async fn alter_table_properties(
        &self,
        runtime_env: Arc<datafusion::execution::runtime_env::RuntimeEnv>,
        path: &str,
        changes: Vec<(String, Option<String>)>,
        if_exists: bool,
    ) -> Result<()> {
        let _ = (runtime_env, path, changes, if_exists);
        not_impl_err!(
            "Table properties alteration not supported for {} format",
            self.name()
        )
    }
}

/// Thread-safe registry of available `TableFormat` implementations.
#[derive(Default)]
pub struct TableFormatRegistry {
    formats: RwLock<HashMap<String, Arc<dyn TableFormat>>>,
}

impl TableFormatRegistry {
    pub fn new() -> Self {
        Self {
            formats: RwLock::new(HashMap::new()),
        }
    }

    pub fn register(&self, format: Arc<dyn TableFormat>) -> Result<()> {
        let mut formats = self
            .formats
            .write()
            .map_err(|_| plan_datafusion_err!("table format registry poisoned"))?;
        formats.insert(format.name().to_lowercase(), format);
        Ok(())
    }

    pub fn get(&self, name: &str) -> Result<Arc<dyn TableFormat>> {
        let formats = self
            .formats
            .read()
            .map_err(|_| plan_datafusion_err!("table format registry poisoned"))?;
        formats
            .get(&name.to_lowercase())
            .cloned()
            .ok_or_else(|| plan_datafusion_err!("No table format found for: {name}"))
    }
}

impl SessionExtension for TableFormatRegistry {
    fn name() -> &'static str {
        "TableFormatRegistry"
    }
}

pub fn create_sort_order(
    session: &dyn Session,
    sort_by: Vec<Sort>,
    schema: &DFSchema,
) -> Result<Option<LexRequirement>> {
    let expr = create_physical_sort_exprs(sort_by.as_slice(), schema, session.execution_props())?;
    let ordering = LexOrdering::new(expr);
    if let Some(ordering) = ordering {
        Ok(LexRequirement::new(
            ordering
                .into_iter()
                .map(PhysicalSortRequirement::from)
                .collect::<Vec<_>>(),
        ))
    } else {
        Ok(None)
    }
}

/// Given a schema and a list of partition columns, returns the partition columns
/// with their data types, and a schema with the partition columns removed.
pub fn get_partition_columns_and_file_schema(
    schema: &Schema,
    partition_by: Vec<String>,
) -> Result<(Vec<(String, DataType)>, Schema)> {
    let partition_columns = partition_by
        .into_iter()
        .map(|col| {
            let mut candidates = schema
                .fields()
                .iter()
                .filter(|f| f.name().eq_ignore_ascii_case(&col));
            match (candidates.next(), candidates.next()) {
                (Some(field), None) => Ok((col, field.data_type().clone())),
                _ => {
                    plan_err!("missing or ambiguous partition column: {col}")
                }
            }
        })
        .collect::<Result<Vec<_>>>()?;
    let file_schema_fields = schema
        .fields()
        .iter()
        .filter(|f| {
            !partition_columns
                .iter()
                .any(|(col, _)| col.eq_ignore_ascii_case(f.name()))
        })
        .cloned()
        .collect::<Vec<_>>();
    let file_schema = Schema::new(file_schema_fields);
    Ok((partition_columns, file_schema))
}
