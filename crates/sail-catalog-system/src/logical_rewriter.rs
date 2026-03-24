use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::Result;
use datafusion::logical_expr::{Extension, LogicalPlan, TableScan};
use sail_common_datafusion::logical_rewriter::LogicalRewriter;

use crate::logical_plan::SystemTableNode;
use crate::table_source::SystemTableSource;

#[derive(Debug, Clone)]
pub struct RewriteSystemTableSource;

impl LogicalRewriter for RewriteSystemTableSource {
    fn name(&self) -> &str {
        "rewrite_system_table_source"
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
                let Some(source) = source.as_any().downcast_ref::<SystemTableSource>() else {
                    return Ok(Transformed::no(LogicalPlan::TableScan(TableScan {
                        table_name,
                        source,
                        projection,
                        projected_schema,
                        filters,
                        fetch,
                    })));
                };
                Ok(Transformed::yes(LogicalPlan::Extension(Extension {
                    node: Arc::new(SystemTableNode::try_new(
                        table_name,
                        source.table(),
                        projection,
                        filters,
                        fetch,
                    )?),
                })))
            }
            _ => Ok(Transformed::no(plan)),
        })
    }
}
