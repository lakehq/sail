use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::Result;
use datafusion::logical_expr::{Extension, LogicalPlan, TableScan, TableSource};
use sail_common_datafusion::logical_rewriter::LogicalRewriter;

use crate::logical::handle::{DeltaTableHandle, DeltaTableHandleInner};
use crate::logical::plan::DeltaTableScanNode;
use crate::logical::table_source::DeltaTableSource;

#[derive(Debug, Clone)]
pub struct RewriteDeltaTableSource;

impl LogicalRewriter for RewriteDeltaTableSource {
    fn name(&self) -> &str {
        "rewrite_delta_table_source"
    }

    fn rewrite(&self, plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        plan.transform_up(|plan| match plan {
            LogicalPlan::TableScan(TableScan {
                table_name,
                source,
                projection,
                projected_schema,
                filters,
                fetch,
            }) => {
                let Some(source) = source.as_any().downcast_ref::<DeltaTableSource>() else {
                    return Ok(Transformed::no(LogicalPlan::TableScan(TableScan {
                        table_name,
                        source,
                        projection,
                        projected_schema,
                        filters,
                        fetch,
                    })));
                };

                let handle = DeltaTableHandle::new(DeltaTableHandleInner {
                    snapshot: source.snapshot().clone(),
                    log_store: source.log_store().clone(),
                    config: source.config().clone(),
                    schema: source.schema(),
                });

                Ok(Transformed::yes(LogicalPlan::Extension(Extension {
                    node: Arc::new(DeltaTableScanNode::try_new(
                        table_name, handle, projection, filters, fetch,
                    )?),
                })))
            }
            _ => Ok(Transformed::no(plan)),
        })
    }
}
