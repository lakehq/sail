use std::fmt::{Debug, Formatter};

use std::sync::Arc;

use datafusion::arrow::datatypes::{FieldRef, Schema};
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
use datafusion_proto::physical_plan::to_proto::{
    serialize_partitioning, serialize_physical_expr_with_converter,
};
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
    Ok(PhysicalPlanNode::try_from_physical_plan_with_converter(
        plan,
        codec,
        &RemotePhysicalProtoConverter {},
    )?
    .encode_to_vec())
}

pub fn try_encode_physical_expr(
    codec: &dyn PhysicalExtensionCodec,
    expr: &Arc<dyn PhysicalExpr>,
) -> Result<PhysicalExprNode> {
    Ok(serialize_physical_expr_with_converter(
        expr,
        codec,
        &RemotePhysicalProtoConverter {},
    )?)
}
