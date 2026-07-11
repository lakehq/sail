use std::sync::Arc;

use datafusion::arrow::datatypes::{Field, FieldRef, Schema};
use datafusion::common::{Result, plan_datafusion_err};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::HigherOrderUDF;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::generated::datafusion_common as gen_datafusion_common;
use datafusion_proto::physical_plan::{
    PhysicalExtensionCodec, PhysicalPlanDecodeContext, PhysicalProtoConverterExtension,
};
use datafusion_proto::protobuf::{PhysicalExprNode, PhysicalPlanNode};
use prost::Message;
use sail_function::scalar::array::spark_array_aggregate::SparkArrayAggregate;
use sail_function::scalar::array::spark_array_exists::SparkArrayExists;
use sail_function::scalar::array::spark_array_filter::SparkArrayFilter;
use sail_function::scalar::array::spark_array_forall::SparkArrayForall;
use sail_function::scalar::array::spark_array_sort::SparkArraySort;
use sail_function::scalar::array::spark_array_transform::SparkArrayTransform;

use crate::plan::r#gen;
use crate::plan::r#gen::higher_order_udf::HigherOrderUdfKind;
use crate::proto::converter::RemotePhysicalProtoConverter;

pub fn decode_remote_physical_plan(
    ctx: &TaskContext,
    codec: &dyn PhysicalExtensionCodec,
    buf: &[u8],
) -> Result<Arc<dyn ExecutionPlan>> {
    // The remote codec decodes data sources directly into `DataSourceExec`.
    // `RemoteDataSourceExec` is only used before encoding to bypass DataFusion's
    // built-in `DataSourceExec` codec.
    try_decode_physical_plan(ctx, codec, buf)
}

pub fn decode_remote_physical_expr(
    ctx: &TaskContext,
    codec: &dyn PhysicalExtensionCodec,
    buf: &[u8],
    schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    try_decode_physical_expr(ctx, codec, buf, schema)
}

pub(super) fn try_decode_message<M>(buf: &[u8]) -> Result<M>
where
    M: Message + Default,
{
    let message =
        M::decode(buf).map_err(|e| plan_datafusion_err!("failed to decode message: {e}"))?;
    Ok(message)
}

pub(super) fn try_decode_schema(buf: &[u8]) -> Result<Schema> {
    let schema = try_decode_message::<gen_datafusion_common::Schema>(buf)?;
    Ok((&schema).try_into()?)
}

pub(super) fn try_decode_field_ref(buf: &[u8]) -> Result<FieldRef> {
    let field = try_decode_message::<gen_datafusion_common::Field>(buf)?;
    let field: Field = (&field).try_into()?;
    Ok(Arc::new(field))
}

pub(super) fn try_decode_physical_plan(
    ctx: &TaskContext,
    codec: &dyn PhysicalExtensionCodec,
    buf: &[u8],
) -> Result<Arc<dyn ExecutionPlan>> {
    let plan = try_decode_message::<PhysicalPlanNode>(buf)?;
    proto_to_physical_plan(ctx, codec, &plan)
}

pub(super) fn proto_to_physical_plan(
    ctx: &TaskContext,
    codec: &dyn PhysicalExtensionCodec,
    plan: &PhysicalPlanNode,
) -> Result<Arc<dyn ExecutionPlan>> {
    plan.try_into_physical_plan_with_converter(ctx, codec, &RemotePhysicalProtoConverter {})
}

pub(super) fn try_decode_physical_expr(
    ctx: &TaskContext,
    codec: &dyn PhysicalExtensionCodec,
    buf: &[u8],
    schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    let expr = try_decode_message::<PhysicalExprNode>(buf)?;
    proto_to_physical_expr(ctx, codec, &expr, schema)
}

pub(super) fn proto_to_physical_expr(
    ctx: &TaskContext,
    codec: &dyn PhysicalExtensionCodec,
    expr: &PhysicalExprNode,
    schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    let converter = RemotePhysicalProtoConverter;
    converter.proto_to_physical_expr(expr, schema, &PhysicalPlanDecodeContext::new(ctx, codec))
}

pub(super) fn try_decode_higher_order_udf(
    udf: &r#gen::HigherOrderUdf,
) -> Result<Arc<HigherOrderUDF>> {
    let udf_kind = udf
        .higher_order_udf_kind
        .as_ref()
        .cloned()
        .ok_or_else(|| plan_datafusion_err!("missing higher-order function UDF"))?;
    Ok(match udf_kind {
        HigherOrderUdfKind::Filter(r#gen::SparkArrayFilterUdf { index_first }) => {
            if index_first {
                Arc::new(HigherOrderUDF::new_from_impl(
                    SparkArrayFilter::new_index_first(),
                ))
            } else {
                Arc::new(HigherOrderUDF::new_from_impl(SparkArrayFilter::new()))
            }
        }
        HigherOrderUdfKind::Transform(r#gen::SparkArrayTransformUdf { index_first }) => {
            if index_first {
                Arc::new(HigherOrderUDF::new_from_impl(
                    SparkArrayTransform::new_index_first(),
                ))
            } else {
                Arc::new(HigherOrderUDF::new_from_impl(SparkArrayTransform::new()))
            }
        }
        HigherOrderUdfKind::Aggregate(r#gen::SparkArrayAggregateUdf { element_first }) => {
            if element_first {
                Arc::new(HigherOrderUDF::new_from_impl(
                    SparkArrayAggregate::new_element_first(),
                ))
            } else {
                Arc::new(HigherOrderUDF::new_from_impl(SparkArrayAggregate::new()))
            }
        }
        HigherOrderUdfKind::Exists(r#gen::SparkArrayExistsUdf {}) => {
            Arc::new(HigherOrderUDF::new_from_impl(SparkArrayExists::new()))
        }
        HigherOrderUdfKind::Forall(r#gen::SparkArrayForallUdf {}) => {
            Arc::new(HigherOrderUDF::new_from_impl(SparkArrayForall::new()))
        }
        HigherOrderUdfKind::Sort(r#gen::SparkArraySortUdf { swapped }) => {
            if swapped {
                Arc::new(HigherOrderUDF::new_from_impl(SparkArraySort::new_swapped()))
            } else {
                Arc::new(HigherOrderUDF::new_from_impl(SparkArraySort::new()))
            }
        }
    })
}
