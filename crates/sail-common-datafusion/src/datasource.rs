use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use datafusion::arrow::datatypes::{FieldRef, Schema, SchemaRef};
use datafusion::catalog::Session;
use datafusion::common::plan_datafusion_err;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_expr::{
    LexOrdering, LexRequirement, PhysicalSortRequirement, create_physical_sort_exprs,
};
use datafusion_common::{
    Constraints, DFSchema, DFSchemaRef, Result, not_impl_datafusion_err, plan_err,
};
use datafusion_expr::expr::Sort;
use datafusion_expr::{Expr, TableSource};

use crate::catalog::{CatalogPartitionField, LakehouseExecutionContext};
use crate::extension::SessionExtension;
use crate::lakesource::LakeSource;
use crate::logical_expr::ExprWithSource;

/// File path metadata column for row-level modifications (MERGE, UPDATE, DELETE).
pub const MERGE_FILE_COLUMN: &str = "__sail_file_path";

/// File-local row index metadata column for row-level modifications that write deletion vectors.
pub const MERGE_ROW_INDEX_COLUMN: &str = "__sail_file_row_index";

/// Row-level operation type column appended to expanded row-level write output.
///
/// This is internal Sail metadata. Source writers may use it to route rows,
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
/// The numeric values are not storage protocol values. They are stable within
/// Sail physical plans so logical expansion and source writers can share
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

/// Information required to create a logical DELETE plan for a lake source.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct DeleteInfo {
    pub target_plan: Arc<LogicalPlan>,
    pub target: RowLevelTarget,
    pub condition: Option<ExprWithSource>,
    pub input_schema: DFSchemaRef,
    pub resolved_target_field_names: Vec<String>,
}

/// Information required to create a logical UPDATE plan for a lake source.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct UpdateInfo {
    pub target_plan: Arc<LogicalPlan>,
    pub target: RowLevelTarget,
    pub condition: Option<ExprWithSource>,
    pub assignments: Vec<UpdateAssignment>,
    pub input_schema: DFSchemaRef,
    pub resolved_target_field_names: Vec<String>,
    pub case_sensitive: bool,
    pub generated_column_exprs: Vec<(String, Expr)>,
    pub check_constraint_exprs: Vec<DeltaCheckConstraintExpr>,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct UpdateAssignment {
    pub column: String,
    pub value: Expr,
}

/// Information required to create a logical MERGE plan for a lake source.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct MergeInfo {
    pub target: Arc<LogicalPlan>,
    pub source: Arc<LogicalPlan>,
    pub options: MergeIntoOptions,
    pub input_schema: DFSchemaRef,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct MergeIntoOptions {
    pub target_alias: Option<String>,
    pub source_alias: Option<String>,
    pub target: RowLevelTarget,
    pub with_schema_evolution: bool,
    pub case_sensitive: bool,
    /// Resolved logical schemas from analysis time (before any rewrites)
    pub resolved_target_schema: DFSchemaRef,
    pub resolved_source_schema: DFSchemaRef,
    /// User-facing field names for target and source, resolved from opaque IDs
    /// at plan resolution time. Used by MERGE expansion to map opaque IDs back
    /// to real column names without the fragile recover-field-names heuristic.
    pub resolved_target_field_names: Vec<String>,
    pub resolved_source_field_names: Vec<String>,
    /// User-facing source column name and its collision-free internal alias.
    pub source_column_aliases: Vec<(String, String)>,
    pub on_condition: ExprWithSource,
    pub matched_clauses: Vec<MergeMatchedClause>,
    pub not_matched_by_source_clauses: Vec<MergeNotMatchedBySourceClause>,
    pub not_matched_by_target_clauses: Vec<MergeNotMatchedByTargetClause>,
    /// Pre-analyzed join equality keys extracted from the ON condition (target, source)
    pub join_key_pairs: Vec<(Expr, Expr)>,
    /// Residual predicates from the ON condition that are not equality join keys
    pub residual_predicates: Vec<Expr>,
    /// Predicates from ON that only touch target columns (useful for early pruning)
    pub target_only_predicates: Vec<Expr>,
    /// Generation expressions for generated columns in the target table.
    /// Each entry is `(column_name, resolved_expr)` where `resolved_expr` initially
    /// references target schema field IDs and is rewritten to actual column names
    /// by MERGE expansion before being applied as a post-processing projection.
    pub generated_column_exprs: Vec<(String, Expr)>,
    /// Resolved target column defaults used by explicit MERGE inserts.
    pub default_column_exprs: Vec<(String, Expr)>,
    /// Delta CHECK constraint expressions for the target table.
    pub check_constraint_exprs: Vec<DeltaCheckConstraintExpr>,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, PartialOrd)]
pub struct RowLevelTarget {
    pub table_name: Vec<String>,
    pub format: String,
    pub location: String,
    pub partition_by: Vec<String>,
    pub options: Vec<OptionLayer>,
    pub lakehouse_table: Option<LakehouseExecutionContext>,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct MergeMatchedClause {
    pub condition: Option<ExprWithSource>,
    pub action: MergeMatchedAction,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum MergeMatchedAction {
    Delete,
    UpdateAll,
    UpdateSet(Vec<MergeAssignment>),
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct MergeNotMatchedBySourceClause {
    pub condition: Option<ExprWithSource>,
    pub action: MergeNotMatchedBySourceAction,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum MergeNotMatchedBySourceAction {
    Delete,
    UpdateSet(Vec<MergeAssignment>),
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct MergeNotMatchedByTargetClause {
    pub condition: Option<ExprWithSource>,
    pub action: MergeNotMatchedByTargetAction,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum MergeNotMatchedByTargetAction {
    InsertAll,
    InsertColumns {
        columns: Vec<String>,
        values: Vec<Expr>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct MergeAssignment {
    pub column: String,
    pub value: Expr,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct DeltaCheckConstraintExpr {
    pub name: String,
    pub expression: String,
    pub expr: Expr,
    pub violation: DeltaConstraintViolation,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum DeltaConstraintViolation {
    Check,
    NotNull { column: String },
    Invariant { column: String },
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

/// A named data source that plans logical reads and writes.
#[async_trait]
pub trait DataSource: Send + Sync {
    /// Returns the name of the data source.
    fn name(&self) -> &str;

    /// Returns this data source's lake capability, if supported.
    ///
    /// The owned receiver keeps the source alive while callers use the
    /// capability across asynchronous operations.
    fn as_lake_source(self: Arc<Self>) -> Option<Arc<dyn LakeSource>> {
        None
    }

    /// Creates a logical [`TableSource`] for read.
    async fn create_source(
        &self,
        ctx: &dyn Session,
        info: SourceInfo,
    ) -> Result<Arc<dyn TableSource>>;

    /// Infers the logical schema for planning without requiring callers to construct a read source.
    async fn infer_schema(&self, ctx: &dyn Session, info: SourceInfo) -> Result<SchemaRef> {
        Ok(self.create_source(ctx, info).await?.schema())
    }

    /// Creates a logical plan for write.
    async fn create_writer(&self, ctx: &dyn Session, info: SinkInfo) -> Result<LogicalPlan>;
}

/// Thread-safe registry of named data and lake sources.
#[derive(Default)]
pub struct DataSourceRegistry {
    sources: RwLock<HashMap<String, Arc<dyn DataSource>>>,
}

impl DataSourceRegistry {
    pub fn new() -> Self {
        Self {
            sources: RwLock::new(HashMap::new()),
        }
    }

    pub fn register_data_source(&self, source: Arc<dyn DataSource>) -> Result<()> {
        let mut sources = self
            .sources
            .write()
            .map_err(|_| plan_datafusion_err!("data source registry poisoned"))?;
        let name = source.name().to_lowercase();
        sources.insert(name, source);
        Ok(())
    }

    pub fn get_data_source(&self, name: &str) -> Result<Arc<dyn DataSource>> {
        let sources = self
            .sources
            .read()
            .map_err(|_| plan_datafusion_err!("data source registry poisoned"))?;
        sources
            .get(&name.to_lowercase())
            .cloned()
            .ok_or_else(|| missing_data_source_error(name))
    }

    pub fn get_lake_source(&self, name: &str) -> Result<Arc<dyn LakeSource>> {
        let source = {
            let sources = self
                .sources
                .read()
                .map_err(|_| plan_datafusion_err!("data source registry poisoned"))?;
            sources
                .get(&name.to_lowercase())
                .cloned()
                .ok_or_else(|| missing_lake_source_error(name))?
        };
        source.as_lake_source().ok_or_else(|| {
            not_impl_datafusion_err!("Data source '{name}' does not support lake operations")
        })
    }

    /// Returns the optional lakehouse capability for a registered data source.
    pub fn get_lake_source_if_supported(&self, name: &str) -> Result<Option<Arc<dyn LakeSource>>> {
        let sources = self
            .sources
            .read()
            .map_err(|_| plan_datafusion_err!("data source registry poisoned"))?;
        Ok(sources
            .get(&name.to_lowercase())
            .cloned()
            .and_then(|source| source.as_lake_source()))
    }
}

fn missing_data_source_error(name: &str) -> datafusion::common::DataFusionError {
    if name.eq_ignore_ascii_case("jdbc") {
        plan_datafusion_err!(
            "No data source found for: {name}. \
             The JDBC data source is provided by pysail and must be registered before use: \
             `from pysail.spark.datasource.jdbc import JdbcDataSource`; \
             `spark.dataSource.register(JdbcDataSource)`"
        )
    } else {
        plan_datafusion_err!("No data source found for: {name}")
    }
}

fn missing_lake_source_error(name: &str) -> datafusion::common::DataFusionError {
    plan_datafusion_err!("No lake source found for: {name}")
}

impl SessionExtension for DataSourceRegistry {
    fn name() -> &'static str {
        "DataSourceRegistry"
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

#[cfg(test)]
mod tests {
    use async_trait::async_trait;

    use super::*;

    struct TestDataSource;

    #[async_trait]
    impl DataSource for TestDataSource {
        fn name(&self) -> &str {
            "test"
        }

        async fn create_source(
            &self,
            _ctx: &dyn Session,
            _info: SourceInfo,
        ) -> Result<Arc<dyn TableSource>> {
            plan_err!("test source does not create tables")
        }

        async fn create_writer(&self, _ctx: &dyn Session, _info: SinkInfo) -> Result<LogicalPlan> {
            plan_err!("test source does not create writers")
        }
    }

    struct TestLakeSource;

    #[async_trait]
    impl DataSource for TestLakeSource {
        fn name(&self) -> &str {
            "test"
        }

        fn as_lake_source(self: Arc<Self>) -> Option<Arc<dyn LakeSource>> {
            Some(self)
        }

        async fn create_source(
            &self,
            _ctx: &dyn Session,
            _info: SourceInfo,
        ) -> Result<Arc<dyn TableSource>> {
            plan_err!("test source does not create tables")
        }

        async fn create_writer(&self, _ctx: &dyn Session, _info: SinkInfo) -> Result<LogicalPlan> {
            plan_err!("test source does not create writers")
        }
    }

    impl LakeSource for TestLakeSource {}

    #[test]
    fn lake_source_registration_replaces_an_ordinary_source() -> Result<()> {
        let registry = DataSourceRegistry::new();

        registry.register_data_source(Arc::new(TestDataSource))?;
        assert!(registry.get_data_source("test").is_ok());
        assert!(registry.get_lake_source("test").is_err());

        registry.register_data_source(Arc::new(TestLakeSource))?;
        assert!(registry.get_data_source("test").is_ok());
        assert!(registry.get_lake_source("test").is_ok());
        Ok(())
    }

    #[test]
    fn ordinary_data_source_registration_replaces_a_lake_source() -> Result<()> {
        let registry = DataSourceRegistry::new();
        registry.register_data_source(Arc::new(TestLakeSource))?;
        assert!(registry.get_lake_source("test").is_ok());

        registry.register_data_source(Arc::new(TestDataSource))?;
        assert!(registry.get_data_source("test").is_ok());
        assert!(registry.get_lake_source("test").is_err());
        Ok(())
    }

    #[test]
    fn ordinary_data_source_has_no_lake_capability() -> Result<()> {
        let registry = DataSourceRegistry::new();
        registry.register_data_source(Arc::new(TestDataSource))?;

        assert!(registry.get_data_source("test").is_ok());
        assert!(registry.get_lake_source_if_supported("test")?.is_none());
        assert!(matches!(
            registry.get_lake_source("test"),
            Err(datafusion_common::DataFusionError::NotImplemented(_))
        ));
        Ok(())
    }

    #[test]
    fn missing_jdbc_data_source_error_includes_registration_hint() -> std::result::Result<(), String>
    {
        let registry = DataSourceRegistry::new();
        let error = match registry.get_data_source("jdbc") {
            Ok(_) => return Err("expected missing jdbc data source error".to_string()),
            Err(error) => error.to_string(),
        };

        assert!(error.contains("No data source found for: jdbc"));
        assert!(error.contains("from pysail.spark.datasource.jdbc import JdbcDataSource"));
        assert!(error.contains("spark.dataSource.register(JdbcDataSource)"));
        Ok(())
    }

    #[test]
    fn missing_non_jdbc_data_source_error_stays_generic() -> std::result::Result<(), String> {
        let registry = DataSourceRegistry::new();
        let error = match registry.get_data_source("unknown") {
            Ok(_) => return Err("expected missing unknown data source error".to_string()),
            Err(error) => error.to_string(),
        };

        assert_eq!(
            error,
            "Error during planning: No data source found for: unknown"
        );
        Ok(())
    }

    #[test]
    fn missing_lake_source_error_stays_generic() -> std::result::Result<(), String> {
        let registry = DataSourceRegistry::new();
        let error = match registry.get_lake_source("unknown") {
            Ok(_) => return Err("expected missing unknown lake source error".to_string()),
            Err(error) => error.to_string(),
        };

        assert_eq!(
            error,
            "Error during planning: No lake source found for: unknown"
        );
        Ok(())
    }
}
