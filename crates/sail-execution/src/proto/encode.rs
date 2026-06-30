use std::sync::Arc;

use datafusion::arrow::datatypes::{FieldRef, Schema};
use datafusion::common::{plan_err, Result};
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

use crate::plan::gen;
use crate::plan::gen::higher_order_udf::HigherOrderUdfKind;
use crate::proto::converter::RemotePhysicalProtoConverter;

pub fn try_encode_message<M>(message: M) -> Result<Vec<u8>>
where
    M: Message,
{
    Ok(message.encode_to_vec())
}

pub fn try_encode_schema(schema: &Schema) -> Result<Vec<u8>> {
    try_encode_message::<gen_datafusion_common::Schema>(schema.try_into()?)
}

pub fn try_encode_field_ref(field: &FieldRef) -> Result<Vec<u8>> {
    try_encode_message::<gen_datafusion_common::Field>(field.as_ref().try_into()?)
}

pub fn try_encode_physical_plan(
    codec: &dyn PhysicalExtensionCodec,
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Vec<u8>> {
    try_encode_message(physical_plan_to_proto(codec, plan)?)
}

pub fn physical_plan_to_proto(
    codec: &dyn PhysicalExtensionCodec,
    plan: Arc<dyn ExecutionPlan>,
) -> Result<PhysicalPlanNode> {
    RemotePhysicalProtoConverter {}.execution_plan_to_proto(&plan, codec)
}

pub fn try_encode_physical_expr(
    codec: &dyn PhysicalExtensionCodec,
    expr: &Arc<dyn PhysicalExpr>,
) -> Result<Vec<u8>> {
    try_encode_message(physical_expr_to_proto(codec, expr)?)
}

pub fn physical_expr_to_proto(
    codec: &dyn PhysicalExtensionCodec,
    expr: &Arc<dyn PhysicalExpr>,
) -> Result<PhysicalExprNode> {
    let converter = RemotePhysicalProtoConverter;
    converter.physical_expr_to_proto(expr, codec)
}

pub fn try_encode_higher_order_udf(hof: &HigherOrderFunctionExpr) -> Result<gen::HigherOrderUdf> {
    let udf_inner = hof.fun().inner().as_ref() as &dyn std::any::Any;
    let udf_kind = if let Some(filter) = udf_inner.downcast_ref::<SparkArrayFilter>() {
        HigherOrderUdfKind::Filter(gen::SparkArrayFilterUdf {
            index_first: filter.is_index_first(),
        })
    } else if let Some(transform) = udf_inner.downcast_ref::<SparkArrayTransform>() {
        HigherOrderUdfKind::Transform(gen::SparkArrayTransformUdf {
            index_first: transform.is_index_first(),
        })
    } else if let Some(aggregate) = udf_inner.downcast_ref::<SparkArrayAggregate>() {
        HigherOrderUdfKind::Aggregate(gen::SparkArrayAggregateUdf {
            element_first: aggregate.is_element_first(),
        })
    } else if udf_inner.is::<SparkArrayExists>() {
        HigherOrderUdfKind::Exists(gen::SparkArrayExistsUdf {})
    } else if udf_inner.is::<SparkArrayForall>() {
        HigherOrderUdfKind::Forall(gen::SparkArrayForallUdf {})
    } else if let Some(sort) = udf_inner.downcast_ref::<SparkArraySort>() {
        HigherOrderUdfKind::Sort(gen::SparkArraySortUdf {
            swapped: sort.is_swapped(),
        })
    } else {
        return plan_err!("unsupported higher-order function: {}", hof.name());
    };
    Ok(gen::HigherOrderUdf {
        higher_order_udf_kind: Some(udf_kind),
    })
}
