use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{Result, ScalarValue, plan_err};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{Column, Literal};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::projection::ProjectionExec;
use sail_common_datafusion::datasource::PhysicalSinkMode;

use super::context::PlannerContext;
use super::utils::prepare_delta_writer_input;
use crate::change_data_feed::CHANGE_TYPE_COLUMN;
use crate::physical_plan::{DeltaWriteContext, DeltaWriterExec, DeltaWriterExecOptions};

pub(super) fn tag_change_rows(
    input: Arc<dyn ExecutionPlan>,
    schema: &SchemaRef,
    change_type: &str,
) -> Result<Arc<dyn ExecutionPlan>> {
    let mut expressions = schema
        .fields()
        .iter()
        .map(|field| {
            Ok((
                Arc::new(Column::new(
                    field.name(),
                    input.schema().index_of(field.name())?,
                )) as Arc<dyn PhysicalExpr>,
                field.name().clone(),
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    expressions.push((
        Arc::new(Literal::new(ScalarValue::Utf8(Some(
            change_type.to_string(),
        )))),
        CHANGE_TYPE_COLUMN.to_string(),
    ));
    Ok(Arc::new(ProjectionExec::try_new(expressions, input)?))
}

pub(super) fn build_change_data_writer(
    ctx: &PlannerContext<'_>,
    input: Arc<dyn ExecutionPlan>,
    mut options: DeltaWriterExecOptions,
    write_context: &DeltaWriteContext,
    partition_columns: &[String],
) -> Result<Arc<dyn ExecutionPlan>> {
    // Change rows and data rows are separate effects and must evaluate identically.
    input.apply(|node| {
        node.apply_expressions(&mut |root| {
            root.apply(|expression| {
                if expression.is_volatile_node() {
                    return plan_err!(
                        "Change data feed writes do not support non-deterministic expressions"
                    );
                }
                Ok(TreeNodeRecursion::Continue)
            })
        })?;
        Ok(TreeNodeRecursion::Continue)
    })?;
    options.change_data = true;
    let input = prepare_delta_writer_input(input, partition_columns, None)?;
    Ok(Arc::new(DeltaWriterExec::new(
        input,
        ctx.table_url().clone(),
        options,
        ctx.metadata_configuration().clone(),
        partition_columns.to_vec(),
        PhysicalSinkMode::Append,
        true,
        write_context.final_schema_ref()?,
        write_context.clone(),
        ctx.lakehouse_table().cloned(),
    )?))
}
