use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use datafusion::arrow::datatypes::{FieldRef, Schema};
use datafusion::catalog::Session;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_expr::{
    LexOrdering, LexRequirement, PhysicalSortRequirement, create_physical_sort_exprs,
};
use datafusion_common::{Constraints, DFSchema, Result, plan_err};
use datafusion_expr::TableSource;
use datafusion_expr::expr::Sort;

use crate::catalog::{CatalogPartitionField, LakehouseExecutionContext};
use crate::logical_expr::ExprWithSource;

/// File path metadata column for row-level modifications (MERGE, UPDATE, DELETE).
pub const MERGE_FILE_COLUMN: &str = "__sail_file_path";

/// File-local row index metadata column for row-level modifications that write deletion vectors.
pub const MERGE_ROW_INDEX_COLUMN: &str = "__sail_file_row_index";

/// Row-level operation type column appended to expanded row-level write output.
///
/// This is internal Sail metadata. Format writers may use it to route rows,
/// collect operation metrics, or produce low-level delete artifacts, but must
/// remove it before persisting user data.
/// Value is one of the [`RowLevelOperationType`] integer constants.
pub const OPERATION_COLUMN: &str = "__sail_operation_type";

/// Reserved private write option name. User-visible option layers must reject this key.
pub const CATALOG_TABLE_OPTION: &str = "__sail.catalog.table";

/// Internal column carrying pre-aggregated MERGE source row counts on
/// [`RowLevelOperationType::SourceMetric`] rows.
pub const MERGE_SOURCE_METRIC_COLUMN: &str = "__sail_merge_source_metric";

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
    /// This is used when a data source consumes untyped key-value options.
    /// The returned map can be passed to code that accepts `HashMap<String, String>`.
    pub fn into_opaque_options(self) -> HashMap<String, String> {
        match self {
            OptionLayer::TablePropertyList { items } => items
                .into_iter()
                .map(|(key, value)| {
                    if let Some(key) = key.strip_prefix("option.") {
                        (key.to_string(), value)
                    } else {
                        (key, value)
                    }
                })
                .collect(),
            OptionLayer::OptionList { items } => items.into_iter().collect(),
            OptionLayer::TableLocation { .. }
            | OptionLayer::AsOfTimestamp { .. }
            | OptionLayer::AsOfIntegerVersion { .. }
            | OptionLayer::AsOfStringVersion { .. } => HashMap::new(),
        }
    }
}

/// Internal row intent tag for row-level write plans.
///
/// The numeric values are not table-format protocol values. They are stable
/// within Sail physical plans so logical expansion and format writers can share
/// a compact representation of per-row intent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(i32)]
pub enum RowLevelOperationType {
    /// Existing target row is rewritten unchanged.
    Copy = 0,
    /// Existing target row is deleted.
    Delete = 1,
    /// Existing target row is rewritten with updated values.
    Update = 2,
    /// Source row is inserted as a new target row.
    Insert = 3,
    /// Source row participates in metrics or checks but is not written.
    Noop = 4,
    /// Matched target row is deleted by a MERGE clause.
    MatchedDelete = 5,
    /// Matched target row is updated by a MERGE clause.
    MatchedUpdate = 6,
    /// Target-only row is deleted by a MERGE clause.
    NotMatchedBySourceDelete = 7,
    /// Target-only row is updated by a MERGE clause.
    NotMatchedBySourceUpdate = 8,
    /// Metric-only row carrying a MERGE source row count.
    SourceMetric = 9,
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
    /// Unified lakehouse catalog context for catalog-coordinated reads.
    pub lakehouse_table: Option<LakehouseExecutionContext>,
    /// The (optional) schema of the data source including partitioning columns.
    pub schema: Option<Schema>,
    pub constraints: Constraints,
    pub partition_by: Vec<String>,
    pub bucket_by: Option<BucketBy>,
    pub sort_order: Vec<Sort>,
    /// The layers of options for the data source.
    /// A later layer can override earlier ones.
    pub options: Vec<OptionLayer>,
    /// Whether reads match the requested columns case-sensitively against the
    /// physical file schema. Spark defaults to case-insensitive matching
    /// (`spark.sql.caseSensitive=false`). This only affects formats that
    /// reconcile a requested schema against files on read (e.g. Parquet); it is
    /// inert for formats that resolve their schema from metadata.
    pub read_case_sensitive: bool,
}

impl SourceInfo {
    pub fn catalog_table(&self) -> Option<&[String]> {
        self.lakehouse_table
            .as_ref()
            .map(|context| context.catalog_table())
    }
}

/// Information required to create a data writer.
#[derive(Debug, Clone)]
pub struct SinkInfo {
    pub input: LogicalPlan,
    pub mode: SinkMode,
    pub partition_by: Vec<CatalogPartitionField>,
    pub bucket_by: Option<BucketBy>,
    pub sort_order: Vec<Sort>,
    /// The sets of options for the data sink.
    /// A later set of options can override earlier ones.
    /// The path for the sink is stored under the `"path"` key in options.
    pub options: Vec<OptionLayer>,
    /// Unified lakehouse catalog context for catalog-coordinated writes.
    pub lakehouse_table: Option<LakehouseExecutionContext>,
}

impl SinkInfo {
    pub fn catalog_table(&self) -> Option<&[String]> {
        self.lakehouse_table
            .as_ref()
            .map(|context| context.catalog_table())
    }
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
    find("path")
        .filter(|v| !v.trim().is_empty())
        .or_else(|| find("location").filter(|v| !v.trim().is_empty()))
}

/// The kind of row-level DML command being executed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RowLevelCommand {
    Delete,
    Update,
    Merge,
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

/// Given a schema and a list of partition column names, returns the partition fields
/// and a schema with the partition columns removed.
pub fn get_partition_columns_and_file_schema(
    schema: &Schema,
    partition_by: Vec<String>,
) -> Result<(Vec<FieldRef>, Schema)> {
    let partition_fields = partition_by
        .into_iter()
        .map(|col| {
            let mut candidates = schema
                .fields()
                .iter()
                .filter(|f| f.name().eq_ignore_ascii_case(&col));
            match (candidates.next(), candidates.next()) {
                (Some(field), None) => Ok(field.clone()),
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
            !partition_fields
                .iter()
                .any(|p| f.name().eq_ignore_ascii_case(p.name()))
        })
        .cloned()
        .collect::<Vec<_>>();
    let file_schema = Schema::new(file_schema_fields);
    Ok((partition_fields, file_schema))
}
