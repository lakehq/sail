use std::fmt::{Debug, Formatter};

use std::sync::Arc;

use datafusion::arrow::datatypes::{Field, FieldRef, Schema};
use datafusion::common::{plan_datafusion_err, plan_err, Result};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{
    AggregateUDFImpl, HigherOrderUDF, LambdaParametersProgress, ScalarUDFImpl, ValueOrLambda,
};
use datafusion::physical_expr::expressions::{LambdaExpr, LambdaVariable};
use datafusion::physical_expr::{
    AcrossPartitions, ConstExpr, EquivalenceProperties, HigherOrderFunctionExpr, LexOrdering,
    LexRequirement, Partitioning, PhysicalExpr, PhysicalSortExpr,
};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::physical_plan::from_proto::{
    parse_physical_expr_with_converter, parse_protobuf_partitioning,
};
use datafusion_proto::physical_plan::to_proto::serialize_physical_expr_with_converter;
use datafusion_proto::physical_plan::{
    PhysicalExtensionCodec, PhysicalPlanDecodeContext, PhysicalProtoConverterExtension,
};
use datafusion_proto::protobuf::{
    physical_expr_node, PhysicalExprNode, PhysicalExtensionExprNode, PhysicalPlanNode,
};

use crate::plan::gen;
use crate::plan::gen::extended_physical_expr_node::ExprKind;
use crate::plan::gen::higher_order_udf::HigherOrderUdfKind;
use crate::plan::gen::{
    ExtendedPhysicalExprNode, HigherOrderUdfExprNode, LambdaExprNode, LambdaVariableExprNode,
};
use crate::proto::physical_proto_converter::RemotePhysicalProtoConverter;
use datafusion_proto::generated::datafusion_common as gen_datafusion_common;
use prost::Message;
use sail_function::scalar::array::spark_array_filter::SparkArrayFilter;

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
    let plan = PhysicalPlanNode::decode(buf)
        .map_err(|e| plan_datafusion_err!("failed to decode plan: {e}"))?;
    plan.try_into_physical_plan_with_converter(ctx, codec, &RemotePhysicalProtoConverter {})
}

pub fn try_decode_physical_expr(
    ctx: &TaskContext,
    codec: &dyn PhysicalExtensionCodec,
    buf: &[u8],
    schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    parse_physical_expr_with_converter(
        &PhysicalExprNode::decode(buf)
            .map_err(|e| plan_datafusion_err!("failed to decode expr: {e}"))?,
        schema,
        &PhysicalPlanDecodeContext::new(ctx, codec),
        &RemotePhysicalProtoConverter {},
    )
}
