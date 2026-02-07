use std::any::Any;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Result;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableSource};
use sail_common_datafusion::system::catalog::SystemTable;

use crate::predicate::is_column_logical_predicate;

pub struct SystemTableSource {
    schema: SchemaRef,
    table: SystemTable,
}

impl SystemTableSource {
    pub fn new(table: SystemTable) -> Self {
        let schema = table.schema();
        Self { schema, table }
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
        let columns: &[&str] = match self.table {
            SystemTable::Jobs | SystemTable::Stages | SystemTable::Tasks => {
                &["session_id", "job_id"]
            }
            SystemTable::Sessions => &["session_id"],
            SystemTable::Workers => &["session_id", "worker_id"],
        };
        filters
            .iter()
            .map(|filter| {
                for col in columns {
                    if is_column_logical_predicate(filter, col)? {
                        return Ok(TableProviderFilterPushDown::Exact);
                    }
                }
                Ok(TableProviderFilterPushDown::Unsupported)
            })
            .collect()
    }
}
