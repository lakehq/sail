use std::sync::Arc;

use datafusion::catalog::Session;
use datafusion::common::Result;
use datafusion::physical_plan::ExecutionPlan;
use sail_common_datafusion::datasource::PhysicalSinkMode;
use url::Url;

use crate::physical_plan::writer_exec::IcebergWriterExec;

/// Table configuration required for building the Iceberg write pipeline
pub struct IcebergTableConfig {
    pub table_url: Url,
    pub partition_columns: Vec<String>,
    pub table_exists: bool,
}

/// Builder for Iceberg write plan: Input -> Projection -> Repartition -> Sort -> Writer -> Commit
pub struct IcebergPlanBuilder<'a> {
    input: Arc<dyn ExecutionPlan>,
    table_config: IcebergTableConfig,
    sink_mode: PhysicalSinkMode,
    sort_order: Option<Vec<datafusion::physical_expr::PhysicalSortExpr>>,
    session: &'a dyn Session,
}

impl<'a> IcebergPlanBuilder<'a> {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        table_config: IcebergTableConfig,
        sink_mode: PhysicalSinkMode,
        sort_order: Option<Vec<datafusion::physical_expr::PhysicalSortExpr>>,
        session: &'a dyn Session,
    ) -> Self {
        Self {
            input,
            table_config,
            sink_mode,
            sort_order,
            session,
        }
    }

    pub async fn build(self) -> Result<Arc<dyn ExecutionPlan>> {
        // For v1: only Append is supported; other modes validated earlier
        let mut current: Arc<dyn ExecutionPlan> = self.input;

        // Reuse Delta helper pipeline semantics
        use datafusion::physical_expr::expressions::Column;
        use datafusion::physical_expr::PhysicalExpr;
        use datafusion::physical_plan::projection::ProjectionExec;
        use datafusion::physical_plan::repartition::RepartitionExec;
        use datafusion::physical_plan::sorts::sort::SortExec;
        use datafusion::physical_plan::Partitioning;

        // Projection: move partition columns to the end (same behavior as Delta)
        let input_schema = current.schema();
        let mut projection_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = Vec::new();
        let mut part_idx = std::collections::HashMap::new();
        let part_set: std::collections::HashSet<&String> =
            self.table_config.partition_columns.iter().collect();
        for (i, f) in input_schema.fields().iter().enumerate() {
            if part_set.contains(f.name()) {
                part_idx.insert(f.name().clone(), i);
            } else {
                projection_exprs.push((Arc::new(Column::new(f.name(), i)), f.name().clone()));
            }
        }
        for name in &self.table_config.partition_columns {
            let idx = *part_idx.get(name).ok_or_else(|| {
                datafusion::common::DataFusionError::Plan(format!(
                    "Partition column '{}' not found in schema",
                    name
                ))
            })?;
            projection_exprs.push((Arc::new(Column::new(name, idx)), name.clone()));
        }
        current = Arc::new(ProjectionExec::try_new(projection_exprs, current)?);

        // Repartition: hash by partition columns when present; else round-robin
        let repartitioning = if self.table_config.partition_columns.is_empty() {
            Partitioning::RoundRobinBatch(4)
        } else {
            let schema = current.schema();
            let n = schema.fields().len();
            let k = self.table_config.partition_columns.len();
            let exprs: Vec<Arc<dyn PhysicalExpr>> = (n - k..n)
                .zip(self.table_config.partition_columns.iter())
                .map(|(idx, name)| Arc::new(Column::new(name, idx)) as Arc<dyn PhysicalExpr>)
                .collect();
            Partitioning::Hash(exprs, 4)
        };
        current = Arc::new(RepartitionExec::try_new(current, repartitioning)?);

        // Sort: by provided sort order (already includes partition columns if needed)
        if let Some(sort_exprs) = self.sort_order.clone() {
            let lex = datafusion::physical_expr::LexOrdering::new(sort_exprs).ok_or_else(|| {
                datafusion::common::DataFusionError::Internal("Invalid sort order".to_string())
            })?;
            let sort_exec = SortExec::new(lex, current);
            current = Arc::new(sort_exec);
        }

        // Writer
        let table_url_clone = self.table_config.table_url.clone();
        current = Arc::new(IcebergWriterExec::new(
            current,
            table_url_clone.clone(),
            self.table_config.partition_columns,
            self.sink_mode,
            self.table_config.table_exists,
        ));

        // Commit
        current = Arc::new(
            crate::physical_plan::commit::commit_exec::IcebergCommitExec::new(
                current,
                table_url_clone,
            ),
        );

        Ok(current)
    }
}
