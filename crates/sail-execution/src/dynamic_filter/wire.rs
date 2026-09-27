use std::sync::Arc;

use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::common::{Result, plan_err};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{BinaryExpr, Column, DynamicFilterPhysicalExpr, lit};
use datafusion::physical_plan::joins::HashTableLookupExpr;
use datafusion_proto::protobuf::{PhysicalBinaryExprNode, PhysicalExprNode, physical_expr_node};
use prost::Message;

use crate::driver::r#gen::DynamicFilterUpdate;
use crate::proto::{
    RemoteExecutionCodec, decode_remote_physical_expr, encode_remote_physical_expr,
};

pub(super) const MAX_FILTER_BYTES: usize = 1024 * 1024;

/// A canonical filter shares state with all of its remapped consumers in one task.
#[derive(Clone)]
pub(crate) struct DynamicFilterBinding {
    pub filter: Arc<dyn PhysicalExpr>,
}

impl DynamicFilterBinding {
    pub fn apply(&self, update: &DynamicFilterUpdate, context: &TaskContext) -> Result<()> {
        let schema = if update.schema.is_empty() {
            Schema::empty()
        } else {
            crate::proto::decode::try_decode_schema(&update.schema)?
        };
        let children = self.filter.children();
        if !schema.fields().is_empty() && schema.fields().len() != children.len() {
            return plan_err!("dynamic filter child count changed during execution");
        }
        let predicate = decode_remote_physical_expr(
            context,
            &RemoteExecutionCodec,
            &update.predicate,
            &schema,
        )?;
        let predicate = predicate
            .transform_down(|expr| {
                if let Some(column) = expr.downcast_ref::<Column>() {
                    if column.index() >= schema.fields().len() {
                        return plan_err!("dynamic filter column has no wire input");
                    }
                    let Some(child) = children.get(column.index()) else {
                        return plan_err!("dynamic filter column index is out of bounds");
                    };
                    Ok(Transformed::new(
                        Arc::clone(child),
                        true,
                        TreeNodeRecursion::Jump,
                    ))
                } else {
                    Ok(Transformed::no(expr))
                }
            })
            .data()?;
        let Some(filter) = self.filter.downcast_ref::<DynamicFilterPhysicalExpr>() else {
            return plan_err!("dynamic filter binding is not a dynamic filter");
        };
        filter.update(predicate)?;
        if update.complete {
            filter.mark_complete();
        }
        Ok(())
    }
}

/// Ship expressions against positional filter inputs, independent of scan projection
/// and column remapping. Live hash tables never cross the control plane.
pub(super) fn snapshot(
    filter: &Arc<dyn PhysicalExpr>,
    schema: &Schema,
    complete: bool,
) -> Result<DynamicFilterUpdate> {
    let Some(dynamic) = filter.downcast_ref::<DynamicFilterPhysicalExpr>() else {
        return plan_err!("expected a dynamic filter producer");
    };
    let children = filter.children();
    let fields = children
        .iter()
        .enumerate()
        .map(|(index, child)| {
            Ok(Field::new(
                format!("filter_input_{index}"),
                child.data_type(schema)?,
                true,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    let wire_schema = Schema::new(fields);
    let generation = filter.snapshot_generation();
    let predicate = dynamic
        .current()?
        .transform_down(|expr| {
            if let Some(index) = children
                .iter()
                .position(|child| child.as_ref() == expr.as_ref())
            {
                Ok(Transformed::yes(
                    Arc::new(Column::new(wire_schema.field(index).name(), index)) as _,
                ))
            } else {
                Ok(Transformed::no(expr))
            }
        })
        .data()?;
    let predicate = transferable_predicate(predicate)?;
    let mut predicate = encode_remote_physical_expr(&RemoteExecutionCodec, &predicate)?;
    if predicate.len() > MAX_FILTER_BYTES {
        predicate = true_predicate()?;
    }
    Ok(DynamicFilterUpdate {
        expression_id: filter
            .expression_id()
            .ok_or_else(|| datafusion::common::plan_datafusion_err!("dynamic filter has no ID"))?,
        generation,
        predicate,
        schema: crate::proto::encode::try_encode_schema(&wire_schema)?,
        complete,
    })
}

fn transferable_predicate(expr: Arc<dyn PhysicalExpr>) -> Result<Arc<dyn PhysicalExpr>> {
    if expr.is::<HashTableLookupExpr>() {
        return Ok(lit(true));
    }
    if let Some(binary) = expr.downcast_ref::<BinaryExpr>()
        && matches!(binary.op(), Operator::And | Operator::Or)
    {
        return Ok(Arc::new(BinaryExpr::new(
            transferable_predicate(binary.left().clone())?,
            *binary.op(),
            transferable_predicate(binary.right().clone())?,
        )));
    }
    // Unsupported optional predicates weaken to TRUE, never to a rejecting filter.
    match encode_remote_physical_expr(&RemoteExecutionCodec, &expr) {
        Ok(bytes) if bytes.len() <= MAX_FILTER_BYTES => Ok(expr),
        _ => Ok(lit(true)),
    }
}

pub(super) fn true_predicate() -> Result<Vec<u8>> {
    encode_remote_physical_expr(&RemoteExecutionCodec, &lit(true))
}

pub(super) fn union_predicates(updates: &[&DynamicFilterUpdate]) -> Result<Vec<u8>> {
    if updates
        .iter()
        .map(|update| update.predicate.len())
        .sum::<usize>()
        > MAX_FILTER_BYTES
    {
        return true_predicate();
    }
    let operands = updates
        .iter()
        .map(|update| {
            PhysicalExprNode::decode(update.predicate.as_slice()).map_err(|e| {
                datafusion::common::plan_datafusion_err!("invalid dynamic predicate: {e}")
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let predicate = PhysicalExprNode {
        expr_id: None,
        expr_type: Some(physical_expr_node::ExprType::BinaryExpr(Box::new(
            PhysicalBinaryExprNode {
                l: None,
                r: None,
                op: "Or".into(),
                operands,
            },
        ))),
    }
    .encode_to_vec();
    if predicate.len() > MAX_FILTER_BYTES {
        true_predicate()
    } else {
        Ok(predicate)
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{BooleanArray, Int64Array, StringArray};
    use datafusion::arrow::datatypes::DataType;
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::physical_plan::filter::FilterExec;
    use datafusion::prelude::SessionContext;
    use datafusion_proto::protobuf::PhysicalPlanNode;

    use super::*;
    use crate::proto::decode::decode_task_plan;
    use crate::proto::encode_remote_physical_plan;

    #[test]
    fn remote_updates_preserve_projected_consumer_columns() -> Result<()> {
        let producer_schema = Schema::new(vec![
            Field::new("label", DataType::Utf8, false),
            Field::new("k", DataType::Int64, false),
        ]);
        let key: Arc<dyn PhysicalExpr> = Arc::new(Column::new("k", 1));
        let producer: Arc<dyn PhysicalExpr> =
            Arc::new(DynamicFilterPhysicalExpr::new(vec![key.clone()], lit(true)));
        let id = producer
            .expression_id()
            .ok_or_else(|| datafusion::common::plan_datafusion_err!("missing ID"))?;
        let consumer = producer
            .clone()
            .with_new_children(vec![Arc::new(Column::new("k", 0))])?;
        let consumer_schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("label", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            consumer_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 7])),
                Arc::new(StringArray::from(vec!["a", "b"])),
            ],
        )?;
        let scan = DataSourceExec::from_data_source(MemorySourceConfig::try_new(
            &[vec![batch.clone()]],
            consumer_schema,
            None,
        )?);
        let plan: Arc<dyn ExecutionPlan> = Arc::new(FilterExec::try_new(consumer, scan)?);
        let bytes = encode_remote_physical_plan(&RemoteExecutionCodec, plan)?;
        let proto = PhysicalPlanNode::decode(bytes.as_slice())
            .map_err(|e| datafusion::common::plan_datafusion_err!("{e}"))?;
        let context = SessionContext::new().task_ctx();
        let (plan, bindings) = decode_task_plan(&context, &proto)?;
        let filter = producer
            .downcast_ref::<DynamicFilterPhysicalExpr>()
            .ok_or_else(|| datafusion::common::plan_datafusion_err!("expected filter"))?;
        filter.update(Arc::new(BinaryExpr::new(key, Operator::Eq, lit(7_i64))))?;
        bindings[&id].apply(&snapshot(&producer, &producer_schema, true)?, &context)?;
        let plan = plan
            .downcast_ref::<FilterExec>()
            .ok_or_else(|| datafusion::common::plan_datafusion_err!("expected FilterExec"))?;
        let values = plan.predicate().evaluate(&batch)?.into_array(2)?;
        let values = values
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| datafusion::common::plan_datafusion_err!("expected Boolean array"))?;
        assert_eq!(
            values.iter().collect::<Vec<_>>(),
            vec![Some(false), Some(true)]
        );
        Ok(())
    }
}
