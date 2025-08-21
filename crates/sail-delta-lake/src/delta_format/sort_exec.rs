use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use datafusion::arrow::compute::SortOptions;
use datafusion::common::Result as DFResult;
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{LexOrdering, LexRequirement, PhysicalSortExpr};
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use datafusion_physical_expr::expressions::col;

use super::project_exec::PARTITION_COLUMN_PREFIX;

/// DeltaSortExec is a wrapper that encapsulates the logic for
/// data sorting for Delta Lake writes.
#[derive(Debug)]
pub struct DeltaSortExec {
    input: Arc<dyn ExecutionPlan>,
    /// Internal SortExec instance
    sort_exec: Arc<SortExec>,
    /// Original partition column names for display purposes
    partition_columns: Vec<String>,
    /// User-specified sort requirements
    sort_order: Option<LexRequirement>,
}

impl DeltaSortExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        partition_columns: Vec<String>, // Original partition column names
        sort_order: Option<LexRequirement>,
    ) -> DFResult<Self> {
        // Build sort expressions
        let mut sort_exprs: Vec<PhysicalSortExpr> = partition_columns
            .iter()
            .map(|name| {
                let prefixed_name = format!("{}{}", PARTITION_COLUMN_PREFIX, name);
                Ok(PhysicalSortExpr {
                    expr: col(&prefixed_name, &input.schema())?,
                    options: SortOptions::default(), // Default ascending
                })
            })
            .collect::<DFResult<_>>()?;

        // Add user-specified sort columns
        if let Some(ref user_sort_order) = sort_order {
            for req in user_sort_order {
                sort_exprs.push(PhysicalSortExpr {
                    expr: req.expr.clone(),
                    options: req.options.unwrap_or_default(),
                });
            }
        }

        let sort_exec = if !sort_exprs.is_empty() {
            let lex_ordering = LexOrdering::new(sort_exprs).ok_or_else(|| {
                DataFusionError::Internal("Failed to create LexOrdering".to_string())
            })?;
            // We've already partitioned data in the Repartition node,
            // sorting is only within each physical partition, so we must preserve partitioning.
            Arc::new(SortExec::new(lex_ordering, input.clone()).with_preserve_partitioning(true))
        } else {
            // No sorting needed, create a minimal SortExec with empty ordering
            use datafusion::arrow::compute::SortOptions;
            use datafusion::common::ScalarValue;
            use datafusion_physical_expr::expressions::lit;

            let dummy_expr = PhysicalSortExpr {
                expr: lit(ScalarValue::Int32(Some(1))),
                options: SortOptions::default(),
            };
            let lex_ordering = LexOrdering::new(vec![dummy_expr]).ok_or_else(|| {
                DataFusionError::Internal("Failed to create dummy LexOrdering".to_string())
            })?;
            Arc::new(SortExec::new(lex_ordering, input.clone()))
        };

        Ok(Self {
            input,
            sort_exec,
            partition_columns,
            sort_order,
        })
    }

    /// Get partition columns
    pub fn partition_columns(&self) -> &[String] {
        &self.partition_columns
    }

    /// Get sort order
    pub fn sort_order(&self) -> Option<&LexRequirement> {
        self.sort_order.as_ref()
    }
}

impl ExecutionPlan for DeltaSortExec {
    fn name(&self) -> &'static str {
        "DeltaSortExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        self.sort_exec.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(
                "DeltaSortExec should have exactly one child".to_string(),
            ));
        }

        Ok(Arc::new(DeltaSortExec::new(
            children[0].clone(),
            self.partition_columns.clone(),
            self.sort_order.clone(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        self.sort_exec.execute(partition, context)
    }
}

impl DisplayAs for DeltaSortExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "DeltaSortExec: partition_columns={:?}, sort_order={:?}",
                    self.partition_columns, self.sort_order
                )
            }
            DisplayFormatType::TreeRender => {
                write!(f, "DeltaSortExec")
            }
        }
    }
}
