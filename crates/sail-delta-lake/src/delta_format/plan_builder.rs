use std::sync::Arc;

use datafusion::physical_expr::{LexOrdering, LexRequirement, PhysicalSortExpr};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::{ExecutionPlan, Partitioning};
use datafusion_common::Result;
use datafusion_physical_expr::expressions::col;
use sail_common_datafusion::datasource::{PhysicalSinkMode, TableDeltaOptions};
use url::Url;

use super::commit_exec::DeltaCommitExec;
use super::writer_exec::DeltaWriterExec;

/// Builder for creating a Delta Lake execution plan with the specified structure:
/// Input -> Project -> Repartition -> Sort -> Writer -> Commit
pub struct DeltaPlanBuilder {
    input: Arc<dyn ExecutionPlan>,
    table_url: Url,
    options: TableDeltaOptions,
    partition_columns: Vec<String>,
    sink_mode: PhysicalSinkMode,
    table_exists: bool,
    sort_order: Option<LexRequirement>,
}

impl DeltaPlanBuilder {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        table_url: Url,
        options: TableDeltaOptions,
        partition_columns: Vec<String>,
        sink_mode: PhysicalSinkMode,
        table_exists: bool,
        sort_order: Option<LexRequirement>,
    ) -> Self {
        Self {
            input,
            table_url,
            options,
            partition_columns,
            sink_mode,
            table_exists,
            sort_order,
        }
    }

    /// Build the complete execution plan chain
    pub fn build(self) -> Result<Arc<dyn ExecutionPlan>> {
        let current_plan = self.input.clone();

        // 1. Project Node
        let current_plan = self.add_projection_node(current_plan)?;

        // 2. Repartition Node
        let current_plan = self.add_repartition_node(current_plan)?;

        // 3. Sort Node
        let current_plan = self.add_sort_node(current_plan)?;

        // 4. Writer Node
        let current_plan = self.add_writer_node(current_plan)?;

        // 5. Commit Node
        let current_plan = self.add_commit_node(current_plan)?;

        Ok(current_plan)
    }

    fn add_projection_node(&self, input: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
        let schema = input.schema();
        let num_fields = schema.fields().len();

        // Create a simple identity projection
        // TODO: Add actual column transformations if needed
        #[allow(clippy::unwrap_used)]
        let expressions: Vec<_> = (0..num_fields)
            .map(|i| {
                let field = schema.field(i);
                (col(field.name(), &schema).unwrap(), field.name().clone())
            })
            .collect();

        Ok(Arc::new(ProjectionExec::try_new(expressions, input)?))
    }

    fn add_repartition_node(
        &self,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Determine partitioning strategy based on partition columns
        let partitioning = if self.partition_columns.is_empty() {
            // TODO: No partition columns, use single partition for simplicity
            Partitioning::RoundRobinBatch(1)
        } else {
            // Hash partition by partition columns
            let partition_exprs: Result<Vec<_>> = self
                .partition_columns
                .iter()
                .map(|col_name| col(col_name, &input.schema()))
                .collect();

            let partition_exprs = partition_exprs?;
            Partitioning::Hash(partition_exprs, 4) // TODO: Make partition count configurable
        };

        Ok(Arc::new(RepartitionExec::try_new(input, partitioning)?))
    }

    fn add_sort_node(&self, input: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
        if let Some(sort_requirements) = &self.sort_order {
            let sort_exprs: Result<Vec<PhysicalSortExpr>> = sort_requirements
                .iter()
                .map(|req| {
                    Ok(PhysicalSortExpr {
                        expr: req.expr.clone(),
                        options: req.options.unwrap_or_default(),
                    })
                })
                .collect();

            let sort_exprs = sort_exprs?;
            if !sort_exprs.is_empty() {
                if let Some(lex_ordering) = LexOrdering::new(sort_exprs) {
                    return Ok(Arc::new(SortExec::new(lex_ordering, input)));
                }
            }
        }

        // Default sorting by partition columns if no explicit sort order
        if !self.partition_columns.is_empty() {
            let sort_exprs: Result<Vec<PhysicalSortExpr>> = self
                .partition_columns
                .iter()
                .map(|col_name| {
                    Ok(PhysicalSortExpr::new(
                        col(col_name, &input.schema())?,
                        datafusion::arrow::compute::SortOptions::default(),
                    ))
                })
                .collect();

            let sort_exprs = sort_exprs?;
            if let Some(lex_ordering) = LexOrdering::new(sort_exprs) {
                return Ok(Arc::new(SortExec::new(lex_ordering, input)));
            }
        }

        // No sorting needed
        Ok(input)
    }

    fn add_writer_node(&self, input: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
        let schema = input.schema();
        Ok(Arc::new(DeltaWriterExec::new(
            input,
            self.table_url.clone(),
            self.options.clone(),
            self.partition_columns.clone(),
            self.sink_mode.clone(),
            self.table_exists,
            schema,
        )))
    }

    fn add_commit_node(&self, input: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(DeltaCommitExec::new(
            input,
            self.table_url.clone(),
            self.partition_columns.clone(),
            self.table_exists,
            self.input.schema(), // Use original input schema for metadata
        )))
    }
}
