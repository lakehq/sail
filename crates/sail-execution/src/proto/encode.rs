use std::sync::Arc;

use datafusion::arrow::datatypes::{FieldRef, Schema};
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{Result, plan_err};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::source::{DataSource, DataSourceExec};
use datafusion::physical_expr::{HigherOrderFunctionExpr, PhysicalExpr};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::generated::datafusion_common as gen_datafusion_common;
use datafusion_proto::physical_plan::{PhysicalExtensionCodec, PhysicalProtoConverterExtension};
use datafusion_proto::protobuf::{PhysicalExprNode, PhysicalPlanNode};
use prost::Message;
use sail_function::scalar::array::spark_array_aggregate::SparkArrayAggregate;
use sail_function::scalar::array::spark_array_exists::SparkArrayExists;
use sail_function::scalar::array::spark_array_filter::SparkArrayFilter;
use sail_function::scalar::array::spark_array_forall::SparkArrayForall;
use sail_function::scalar::array::spark_array_sort::SparkArraySort;
use sail_function::scalar::array::spark_array_transform::SparkArrayTransform;
use sail_physical_plan::data_source::RemoteDataSourceExec;

use crate::plan::r#gen;
use crate::plan::r#gen::higher_order_udf::HigherOrderUdfKind;
use crate::proto::converter::RemotePhysicalProtoConverter;

pub fn encode_remote_physical_plan(
    codec: &dyn PhysicalExtensionCodec,
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Vec<u8>> {
    let plan = plan
        .transform(|node| {
            if let Some(data_source) = node.downcast_ref::<DataSourceExec>() {
                let node =
                    Arc::new(RemoteDataSourceExec::new(data_source)) as Arc<dyn ExecutionPlan>;
                Ok(Transformed::yes(node))
            } else {
                Ok(Transformed::no(node))
            }
        })
        .data()?;
    try_encode_physical_plan(codec, plan)
}

pub fn encode_driver_physical_plan(
    codec: &dyn PhysicalExtensionCodec,
    plan: Arc<dyn ExecutionPlan>,
    partition: usize,
) -> Result<Vec<u8>> {
    let plan = select_memory_source_partition(plan, partition)?;
    encode_remote_physical_plan(codec, plan)
}

fn select_memory_source_partition(
    plan: Arc<dyn ExecutionPlan>,
    partition: usize,
) -> Result<Arc<dyn ExecutionPlan>> {
    plan.transform(|node| {
        let Some(data_source) = node.downcast_ref::<DataSourceExec>() else {
            return Ok(Transformed::no(node));
        };
        let Some(memory) = data_source
            .data_source()
            .downcast_ref::<MemorySourceConfig>()
        else {
            return Ok(Transformed::no(node));
        };
        if partition >= memory.partitions().len() {
            return plan_err!(
                "memory source partition {partition} is out of bounds for {} partitions",
                memory.partitions().len()
            );
        }

        let mut partitions = vec![vec![]; memory.partitions().len()];
        partitions[partition] = memory.partitions()[partition].clone();
        let source = MemorySourceConfig::try_new(
            &partitions,
            memory.original_schema(),
            memory.projection().clone(),
        )?
        .with_show_sizes(memory.show_sizes())
        .try_with_sort_information(memory.sort_information().to_vec())?
        .with_limit(memory.fetch());
        Ok(Transformed::yes(
            DataSourceExec::from_data_source(source) as Arc<dyn ExecutionPlan>
        ))
    })
    .data()
}

pub fn encode_remote_physical_expr(
    codec: &dyn PhysicalExtensionCodec,
    expr: &Arc<dyn PhysicalExpr>,
) -> Result<Vec<u8>> {
    try_encode_physical_expr(codec, expr)
}

pub(super) fn try_encode_message<M>(message: M) -> Result<Vec<u8>>
where
    M: Message,
{
    Ok(message.encode_to_vec())
}

pub(super) fn try_encode_schema(schema: &Schema) -> Result<Vec<u8>> {
    try_encode_message::<gen_datafusion_common::Schema>(schema.try_into()?)
}

pub(super) fn try_encode_field_ref(field: &FieldRef) -> Result<Vec<u8>> {
    try_encode_message::<gen_datafusion_common::Field>(field.as_ref().try_into()?)
}

pub(super) fn try_encode_physical_plan(
    codec: &dyn PhysicalExtensionCodec,
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Vec<u8>> {
    try_encode_message(physical_plan_to_proto(codec, plan)?)
}

pub(super) fn physical_plan_to_proto(
    codec: &dyn PhysicalExtensionCodec,
    plan: Arc<dyn ExecutionPlan>,
) -> Result<PhysicalPlanNode> {
    PhysicalPlanNode::try_from_physical_plan_with_converter(
        plan,
        codec,
        &RemotePhysicalProtoConverter {},
    )
}

pub(super) fn try_encode_physical_expr(
    codec: &dyn PhysicalExtensionCodec,
    expr: &Arc<dyn PhysicalExpr>,
) -> Result<Vec<u8>> {
    try_encode_message(physical_expr_to_proto(codec, expr)?)
}

pub(super) fn physical_expr_to_proto(
    codec: &dyn PhysicalExtensionCodec,
    expr: &Arc<dyn PhysicalExpr>,
) -> Result<PhysicalExprNode> {
    let converter = RemotePhysicalProtoConverter;
    converter.physical_expr_to_proto(expr, codec)
}

pub(super) fn try_encode_higher_order_udf(
    hof: &HigherOrderFunctionExpr,
) -> Result<r#gen::HigherOrderUdf> {
    let udf_inner = hof.fun().inner().as_ref() as &dyn std::any::Any;
    let udf_kind = if let Some(filter) = udf_inner.downcast_ref::<SparkArrayFilter>() {
        HigherOrderUdfKind::Filter(r#gen::SparkArrayFilterUdf {
            index_first: filter.is_index_first(),
        })
    } else if let Some(transform) = udf_inner.downcast_ref::<SparkArrayTransform>() {
        HigherOrderUdfKind::Transform(r#gen::SparkArrayTransformUdf {
            index_first: transform.is_index_first(),
        })
    } else if let Some(aggregate) = udf_inner.downcast_ref::<SparkArrayAggregate>() {
        HigherOrderUdfKind::Aggregate(r#gen::SparkArrayAggregateUdf {
            element_first: aggregate.is_element_first(),
        })
    } else if udf_inner.is::<SparkArrayExists>() {
        HigherOrderUdfKind::Exists(r#gen::SparkArrayExistsUdf {})
    } else if udf_inner.is::<SparkArrayForall>() {
        HigherOrderUdfKind::Forall(r#gen::SparkArrayForallUdf {})
    } else if let Some(sort) = udf_inner.downcast_ref::<SparkArraySort>() {
        HigherOrderUdfKind::Sort(r#gen::SparkArraySortUdf {
            swapped: sort.is_swapped(),
        })
    } else {
        return plan_err!("unsupported higher-order function: {}", hof.name());
    };
    Ok(r#gen::HigherOrderUdf {
        higher_order_udf_kind: Some(udf_kind),
    })
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};

    use super::select_memory_source_partition;

    #[test]
    fn memory_source_encoding_keeps_only_the_driver_task_partition() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let first = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![1]))],
        )
        .unwrap();
        let second = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![2]))],
        )
        .unwrap();
        let plan: Arc<dyn ExecutionPlan> = MemorySourceConfig::try_new_exec(
            &[vec![first], vec![second]],
            Arc::clone(&schema),
            None,
        )
        .unwrap();

        let selected = select_memory_source_partition(plan, 1).unwrap();
        assert_eq!(selected.output_partitioning().partition_count(), 2);
        let selected = selected
            .downcast_ref::<DataSourceExec>()
            .unwrap()
            .data_source()
            .downcast_ref::<MemorySourceConfig>()
            .unwrap();

        assert_eq!(selected.partitions().len(), 2);
        assert!(selected.partitions()[0].is_empty());
        assert_eq!(selected.partitions()[1][0].num_rows(), 1);
    }
}
