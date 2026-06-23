use std::sync::Arc;

use datafusion::arrow::datatypes::{Field, FieldRef, Schema};
use datafusion::common::{plan_datafusion_err, Result};
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
use sail_function::scalar::array::spark_array_filter::SparkArrayFilter;

use crate::plan::gen;
use crate::plan::gen::higher_order_udf::HigherOrderUdfKind;
use crate::proto::converter::RemotePhysicalProtoConverter;

pub fn try_decode_message<M>(buf: &[u8]) -> Result<M>
where
    M: Message + Default,
{
    let message =
        M::decode(buf).map_err(|e| plan_datafusion_err!("failed to decode message: {e}"))?;
    Ok(message)
}

pub fn try_decode_schema(buf: &[u8]) -> Result<Schema> {
    let schema = try_decode_message::<gen_datafusion_common::Schema>(buf)?;
    Ok((&schema).try_into()?)
}

pub fn try_decode_field_ref(buf: &[u8]) -> Result<FieldRef> {
    let field = try_decode_message::<gen_datafusion_common::Field>(buf)?;
    let field: Field = (&field).try_into()?;
    Ok(Arc::new(field))
}

pub fn try_decode_physical_plan(
    ctx: &TaskContext,
    codec: &dyn PhysicalExtensionCodec,
    buf: &[u8],
) -> Result<Arc<dyn ExecutionPlan>> {
    let plan = try_decode_message::<PhysicalPlanNode>(buf)?;
    proto_to_physical_plan(ctx, codec, &plan)
}

pub fn proto_to_physical_plan(
    ctx: &TaskContext,
    codec: &dyn PhysicalExtensionCodec,
    plan: &PhysicalPlanNode,
) -> Result<Arc<dyn ExecutionPlan>> {
    plan.try_into_physical_plan_with_converter(ctx, codec, &RemotePhysicalProtoConverter {})
}

pub fn try_decode_physical_expr(
    ctx: &TaskContext,
    codec: &dyn PhysicalExtensionCodec,
    buf: &[u8],
    schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    let expr = try_decode_message::<PhysicalExprNode>(buf)?;
    proto_to_physical_expr(ctx, codec, &expr, schema)
}

pub fn proto_to_physical_expr(
    ctx: &TaskContext,
    codec: &dyn PhysicalExtensionCodec,
    expr: &PhysicalExprNode,
    schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    let converter = RemotePhysicalProtoConverter;
    converter.proto_to_physical_expr(expr, schema, &PhysicalPlanDecodeContext::new(ctx, codec))
}

pub fn try_decode_higher_order_udf(udf: &gen::HigherOrderUdf) -> Result<Arc<HigherOrderUDF>> {
    let udf_kind = udf
        .higher_order_udf_kind
        .as_ref()
        .cloned()
        .ok_or_else(|| plan_datafusion_err!("missing higher-order function UDF"))?;
    Ok(match udf_kind {
        HigherOrderUdfKind::Filter(gen::SparkArrayFilterUdf { index_first }) => {
            if index_first {
                Arc::new(HigherOrderUDF::new_from_impl(
                    SparkArrayFilter::new_index_first(),
                ))
            } else {
                Arc::new(HigherOrderUDF::new_from_impl(SparkArrayFilter::new()))
            }
        }
    })
}
