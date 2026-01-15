use std::any::Any;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, Result};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableSource};

use crate::gen::catalog::SystemTable;

pub struct SystemTableSource {
    schema: SchemaRef,
    table: SystemTable,
}

impl SystemTableSource {
    pub fn try_new(table: SystemTable) -> Result<Self> {
        let schema = table.schema()?;
        Ok(Self { schema, table })
    }

    pub fn table(&self) -> SystemTable {
        self.table
    }
}

impl TableSource for SystemTableSource {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        let columns = match self.table {
            SystemTable::Sessions => ["session_id"],
        };
        filters
            .iter()
            .map(|filter| {
                for col in &columns {
                    if is_single_column_predicate(filter, col)? {
                        return Ok(TableProviderFilterPushDown::Exact);
                    }
                }
                Ok(TableProviderFilterPushDown::Unsupported)
            })
            .collect()
    }
}

fn is_single_column_predicate(expr: &Expr, column: &str) -> Result<bool> {
    let mut valid = true;
    expr.apply(|e| {
        if let Expr::Column(Column { name, .. }) = e {
            if name != column {
                valid = false;
                return Ok(TreeNodeRecursion::Stop);
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(valid)
}
