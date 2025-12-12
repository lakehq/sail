use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::plan_datafusion_err;
use datafusion::physical_expr::{
    create_physical_sort_exprs, LexOrdering, LexRequirement, PhysicalExpr, PhysicalSortRequirement,
};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::{not_impl_err, plan_err, Constraints, DFSchema, Result};
use datafusion_expr::expr::Sort;
use datafusion_expr::Expr;

use crate::extension::SessionExtension;

#[derive(Clone, Debug, Eq, PartialEq, Hash, PartialOrd)]
pub enum SinkMode {
    ErrorIfExists,
    IgnoreIfExists,
    Append,
    Overwrite,
    OverwriteIf { condition: Box<Expr> },
    OverwritePartitions,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum PhysicalSinkMode {
    ErrorIfExists,
    IgnoreIfExists,
    Append,
    Overwrite,
    OverwriteIf { condition: Arc<dyn PhysicalExpr> },
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
    /// The sets of options for the data source.
    /// A later set of options can override earlier ones.
    pub options: Vec<HashMap<String, String>>,
}

/// Information required to create a data writer.
#[derive(Debug, Clone)]
pub struct SinkInfo {
    pub input: Arc<dyn ExecutionPlan>,
    pub path: String,
    pub mode: PhysicalSinkMode,
    pub partition_by: Vec<String>,
    pub bucket_by: Option<BucketBy>,
    pub sort_order: Option<LexRequirement>,
    /// The sets of options for the data sink.
    /// A later set of options can override earlier ones.
    pub options: Vec<HashMap<String, String>>,
}

/// Information required to create a data deleter.
#[derive(Debug, Clone)]
pub struct DeleteInfo {
    pub path: String,
    pub condition: Option<Arc<dyn PhysicalExpr>>,
    /// The sets of options for the data deletion.
    /// A later set of options can override earlier ones.
    pub options: Vec<HashMap<String, String>>,
}

/// A trait for preparing physical execution for a specific format.
#[async_trait]
pub trait TableFormat: Send + Sync {
    /// Returns the name of the format.
    fn name(&self) -> &str;

    /// Creates a `TableProvider` for read.
    async fn create_provider(
        &self,
        ctx: &dyn Session,
        info: SourceInfo,
    ) -> Result<Arc<dyn TableProvider>>;

    /// Creates a `ExecutionPlan` for write.
    async fn create_writer(
        &self,
        ctx: &dyn Session,
        info: SinkInfo,
    ) -> Result<Arc<dyn ExecutionPlan>>;

    /// Creates a `ExecutionPlan` for delete.
    async fn create_deleter(
        &self,
        ctx: &dyn Session,
        info: DeleteInfo,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let _ = (ctx, info);
        not_impl_err!(
            "DELETE operation is not yet implemented for {} format",
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
