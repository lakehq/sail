//! A fully analyzed write. Data source planning runs only after native inputs,
//! sort expressions, and overwrite predicates have been checked and lowered.
use std::sync::Arc;

use datafusion::prelude::SessionContext;
use datafusion_expr::{Extension, LogicalPlan};
use sail_catalog::provider::CatalogPartitionField;
use sail_common_datafusion::catalog::LakehouseExecutionContext;
use sail_common_datafusion::datasource::{
    BucketBy, DataSourceRegistry, OptionLayer, SinkInfo, SinkMode,
};
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::logical_expr::ExprWithSource;

use super::model::*;
use crate::error::PlanResult;

#[derive(Debug)]
pub struct WriteOperation {
    pub input: QueryNode,
    pub format: String,
    pub mode: WriteDisposition,
    pub partition_by: Vec<CatalogPartitionField>,
    pub sort_order: Vec<SortKey>,
    pub bucket_by: Option<BucketBy>,
    pub options: Vec<OptionLayer>,
    pub lakehouse_table: Option<LakehouseExecutionContext>,
    pub preconditions: Vec<CommandNode>,
}

#[derive(Debug)]
pub enum WriteDisposition {
    ErrorIfExists,
    IgnoreIfExists,
    Append,
    Overwrite,
    OverwritePartitions,
    OverwriteIf {
        condition: Box<ExprNode>,
        schema: QuerySchema,
        source: Option<String>,
    },
}

impl WriteOperation {
    pub(super) async fn lower(self, session: &SessionContext) -> PlanResult<LogicalPlan> {
        let input_schema = self.input.schema.resolved("write input")?.clone();
        let sort_order = self
            .sort_order
            .iter()
            .map(|sort| {
                Ok(datafusion_expr::expr::Sort::new(
                    super::lowering::external_expression(&sort.expression, &input_schema)?,
                    sort.ascending,
                    sort.nulls_first,
                ))
            })
            .collect::<PlanResult<_>>()?;
        let input = super::lowering::external_plan(self.input)?;
        let mode = match self.mode {
            WriteDisposition::ErrorIfExists => SinkMode::ErrorIfExists,
            WriteDisposition::IgnoreIfExists => SinkMode::IgnoreIfExists,
            WriteDisposition::Append => SinkMode::Append,
            WriteDisposition::Overwrite => SinkMode::Overwrite,
            WriteDisposition::OverwritePartitions => SinkMode::OverwritePartitions,
            WriteDisposition::OverwriteIf {
                condition,
                schema,
                source,
            } => SinkMode::OverwriteIf {
                condition: Box::new(ExprWithSource::new(
                    super::lowering::external_expression(&condition, &schema)?,
                    source,
                )),
            },
        };
        let mut preconditions = vec![];
        for command in self.preconditions {
            preconditions.push(Arc::new(command.lower(session).await?));
        }
        let registry = session.extension::<DataSourceRegistry>()?;
        let plan = registry
            .get_data_source(&self.format)?
            .create_writer(
                &session.state(),
                SinkInfo {
                    input,
                    mode,
                    partition_by: self.partition_by,
                    sort_order,
                    bucket_by: self.bucket_by,
                    options: self.options,
                    lakehouse_table: self.lakehouse_table,
                },
            )
            .await?;
        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(sail_logical_plan::barrier::BarrierNode::new(
                preconditions,
                Arc::new(plan),
            )),
        }))
    }
}
