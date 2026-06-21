use std::fmt::{Debug, Formatter};

use std::sync::Arc;

use datafusion::arrow::datatypes::{FieldRef, Schema};
use datafusion::common::{plan_datafusion_err, plan_err, Result};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{
    AggregateUDFImpl, HigherOrderUDF, LambdaParametersProgress, ScalarUDFImpl, ValueOrLambda,
};
use datafusion::physical_expr::expressions::{LambdaExpr, LambdaVariable};
use datafusion::physical_expr::{HigherOrderFunctionExpr, PhysicalExpr};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::physical_plan::to_proto::serialize_physical_expr_with_converter;
use datafusion_proto::physical_plan::{
    DefaultPhysicalProtoConverter, PhysicalExtensionCodec, PhysicalPlanDecodeContext,
    PhysicalProtoConverterExtension,
};
use datafusion_proto::protobuf::{
    physical_expr_node, PhysicalExprNode, PhysicalExtensionExprNode, PhysicalPlanNode,
};

use prost::Message;
use sail_function::scalar::array::spark_array_filter::SparkArrayFilter;

use crate::plan::gen;
use crate::plan::gen::extended_physical_expr_node::ExprKind;
use crate::plan::gen::higher_order_udf::HigherOrderUdfKind;
use crate::plan::gen::{
    ExtendedPhysicalExprNode, HigherOrderUdfExprNode, LambdaExprNode, LambdaVariableExprNode,
};

pub struct RemotePhysicalProtoConverter;

impl Debug for RemotePhysicalProtoConverter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "RemotePhysicalProtoConverter")
    }
}

impl PhysicalProtoConverterExtension for RemotePhysicalProtoConverter {
    fn proto_to_execution_plan(
        &self,
        proto: &PhysicalPlanNode,
        ctx: &PhysicalPlanDecodeContext<'_>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.default_proto_to_execution_plan(proto, ctx)
    }

    fn execution_plan_to_proto(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        codec: &dyn PhysicalExtensionCodec,
    ) -> Result<PhysicalPlanNode> {
        PhysicalPlanNode::try_from_physical_plan_with_converter(Arc::clone(plan), codec, self)
    }

    fn proto_to_physical_expr(
        &self,
        proto: &PhysicalExprNode,
        input_schema: &Schema,
        ctx: &PhysicalPlanDecodeContext<'_>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        match decode_remote_expr_kind(proto)? {
            Some((ExprKind::HigherOrderUdf(node), inputs)) => {
                self.higher_order_proto_to_expr(node, inputs, input_schema, ctx)
            }
            Some((ExprKind::LambdaVariable(node), _)) => {
                let index = usize::try_from(node.index).map_err(|_| {
                    plan_datafusion_err!(
                        "LambdaVariable index {} does not fit in usize",
                        node.index
                    )
                })?;
                let field = input_schema
                    .fields()
                    .get(index)
                    .ok_or_else(|| {
                        plan_datafusion_err!(
                            "LambdaVariable index {index} out of bounds for schema with {} fields",
                            input_schema.fields().len()
                        )
                    })?
                    .clone();
                Ok(Arc::new(LambdaVariable::new(index, field)))
            }
            Some((ExprKind::Lambda(_), _)) => {
                plan_err!("lambda expressions must be decoded as higher-order function arguments")
            }
            _ => self.default_proto_to_physical_expr(proto, input_schema, ctx),
        }
    }

    fn physical_expr_to_proto(
        &self,
        expr: &Arc<dyn PhysicalExpr>,
        codec: &dyn PhysicalExtensionCodec,
    ) -> Result<PhysicalExprNode> {
        if let Some(hof) = expr.downcast_ref::<HigherOrderFunctionExpr>() {
            return self.higher_order_expr_to_proto(expr, hof, codec);
        }
        if let Some(lambda) = expr.downcast_ref::<LambdaExpr>() {
            return self.lambda_expr_to_proto(expr, lambda, codec);
        }
        if let Some(var) = expr.downcast_ref::<LambdaVariable>() {
            return extension_expr_to_proto(
                expr,
                ExprKind::LambdaVariable(LambdaVariableExprNode {
                    index: var.index() as u64,
                }),
                vec![],
            );
        }
        serialize_physical_expr_with_converter(expr, codec, self)
    }
}

impl RemotePhysicalProtoConverter {
    fn higher_order_expr_to_proto(
        &self,
        expr: &Arc<dyn PhysicalExpr>,
        hof: &HigherOrderFunctionExpr,
        codec: &dyn PhysicalExtensionCodec,
    ) -> Result<PhysicalExprNode> {
        let inputs = expr
            .children()
            .into_iter()
            .map(|child| self.physical_expr_to_proto(child, codec))
            .collect::<Result<_>>()?;
        extension_expr_to_proto(
            expr,
            ExprKind::HigherOrderUdf(HigherOrderUdfExprNode {
                udf: Some(try_encode_higher_order_udf(hof)?),
            }),
            inputs,
        )
    }

    fn lambda_expr_to_proto(
        &self,
        expr: &Arc<dyn PhysicalExpr>,
        lambda: &LambdaExpr,
        codec: &dyn PhysicalExtensionCodec,
    ) -> Result<PhysicalExprNode> {
        let inputs = expr
            .children()
            .into_iter()
            .map(|child| self.physical_expr_to_proto(child, codec))
            .collect::<Result<_>>()?;
        extension_expr_to_proto(
            expr,
            ExprKind::Lambda(LambdaExprNode {
                params: lambda.params().to_vec(),
            }),
            inputs,
        )
    }

    fn higher_order_proto_to_expr(
        &self,
        node: HigherOrderUdfExprNode,
        inputs: &[PhysicalExprNode],
        input_schema: &Schema,
        ctx: &PhysicalPlanDecodeContext<'_>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let fun = try_decode_higher_order_udf(node.udf)?;
        let mut decoded_values = Vec::with_capacity(inputs.len());
        let mut value_or_lambda = Vec::with_capacity(inputs.len());

        for input in inputs {
            if lambda_proto_parts(input)?.is_some() {
                decoded_values.push(None);
                value_or_lambda.push(ValueOrLambda::Lambda(None));
            } else {
                let expr = self.proto_to_physical_expr(input, input_schema, ctx)?;
                value_or_lambda.push(ValueOrLambda::Value(expr.return_field(input_schema)?));
                decoded_values.push(Some(expr));
            }
        }

        let param_sets = match fun.lambda_parameters(0, &value_or_lambda)? {
            LambdaParametersProgress::Complete(params) => params,
            LambdaParametersProgress::Partial(_) => {
                return plan_err!("`{}` returned partial lambda parameters", fun.name())
            }
        };

        let mut lambda_index = 0usize;
        let args = inputs
            .iter()
            .enumerate()
            .map(|(index, input)| {
                if let Some((params, body)) = lambda_proto_parts(input)? {
                    let fields = param_sets.get(lambda_index).ok_or_else(|| {
                        plan_datafusion_err!("missing lambda parameter fields for `{}`", fun.name())
                    })?;
                    lambda_index += 1;

                    let schema = extend_lambda_schema(input_schema, &params, fields);
                    let body = self.proto_to_physical_expr(body, &schema, ctx)?;
                    Ok(Arc::new(LambdaExpr::try_new(params, body)?) as Arc<dyn PhysicalExpr>)
                } else {
                    decoded_values[index].clone().ok_or_else(|| {
                        plan_datafusion_err!("missing decoded higher-order argument")
                    })
                }
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Arc::new(HigherOrderFunctionExpr::try_new_with_schema(
            fun,
            args,
            input_schema,
            Arc::clone(ctx.task_ctx().session_config().options()),
        )?))
    }
}

pub(crate) fn encode_physical_plan_for_remote(
    plan: Arc<dyn ExecutionPlan>,
    codec: &dyn PhysicalExtensionCodec,
) -> Result<Vec<u8>> {
    let converter = RemotePhysicalProtoConverter;
    Ok(
        PhysicalPlanNode::try_from_physical_plan_with_converter(plan, codec, &converter)?
            .encode_to_vec(),
    )
}

pub(crate) fn decode_physical_plan_for_remote(
    buf: &[u8],
    ctx: &TaskContext,
    codec: &dyn PhysicalExtensionCodec,
) -> Result<Arc<dyn ExecutionPlan>> {
    let plan = PhysicalPlanNode::decode(buf)
        .map_err(|e| plan_datafusion_err!("failed to decode plan: {e}"))?;
    let converter = RemotePhysicalProtoConverter;
    plan.try_into_physical_plan_with_converter(ctx, codec, &converter)
}

pub(crate) fn encode_physical_expr_for_remote(
    expr: &Arc<dyn PhysicalExpr>,
    codec: &dyn PhysicalExtensionCodec,
) -> Result<Vec<u8>> {
    let converter = RemotePhysicalProtoConverter;
    Ok(converter
        .physical_expr_to_proto(expr, codec)?
        .encode_to_vec())
}

pub(crate) fn decode_physical_expr_for_remote(
    buf: &[u8],
    ctx: &TaskContext,
    schema: &Schema,
    codec: &dyn PhysicalExtensionCodec,
) -> Result<Arc<dyn PhysicalExpr>> {
    let proto = PhysicalExprNode::decode(buf)
        .map_err(|e| plan_datafusion_err!("failed to decode expr: {e}"))?;
    let converter = RemotePhysicalProtoConverter;
    converter.proto_to_physical_expr(&proto, schema, &PhysicalPlanDecodeContext::new(ctx, codec))
}

fn extension_expr_to_proto(
    expr: &Arc<dyn PhysicalExpr>,
    expr_kind: ExprKind,
    inputs: Vec<PhysicalExprNode>,
) -> Result<PhysicalExprNode> {
    let node = ExtendedPhysicalExprNode {
        expr_kind: Some(expr_kind),
    };
    Ok(PhysicalExprNode {
        expr_id: expr.expression_id(),
        expr_type: Some(physical_expr_node::ExprType::Extension(
            PhysicalExtensionExprNode {
                expr: node.encode_to_vec(),
                inputs,
            },
        )),
    })
}

fn decode_remote_expr_kind(
    proto: &PhysicalExprNode,
) -> Result<Option<(ExprKind, &[PhysicalExprNode])>> {
    let Some(physical_expr_node::ExprType::Extension(extension)) = proto.expr_type.as_ref() else {
        return Ok(None);
    };
    let node = ExtendedPhysicalExprNode::decode(extension.expr.as_slice())
        .map_err(|e| plan_datafusion_err!("failed to decode physical expr: {e}"))?;
    let expr_kind = node
        .expr_kind
        .ok_or_else(|| plan_datafusion_err!("missing physical expr node"))?;
    Ok(Some((expr_kind, extension.inputs.as_slice())))
}

fn lambda_proto_parts(
    proto: &PhysicalExprNode,
) -> Result<Option<(Vec<String>, &PhysicalExprNode)>> {
    match decode_remote_expr_kind(proto)? {
        Some((ExprKind::Lambda(node), inputs)) => {
            let [body] = inputs else {
                return plan_err!("LambdaExpr expects exactly one input, got {}", inputs.len());
            };
            Ok(Some((node.params, body)))
        }
        _ => Ok(None),
    }
}

fn extend_lambda_schema(base: &Schema, params: &[String], fields: &[FieldRef]) -> Schema {
    let mut output: Vec<FieldRef> = base.fields().iter().map(Arc::clone).collect();
    for (name, field) in params.iter().zip(fields) {
        output.push(Arc::new(field.as_ref().clone().with_name(name)));
    }
    Schema::new(output)
}

fn try_encode_higher_order_udf(hof: &HigherOrderFunctionExpr) -> Result<gen::HigherOrderUdf> {
    let udf_inner = hof.fun().inner().as_ref() as &dyn std::any::Any;
    let udf_kind = if let Some(filter) = udf_inner.downcast_ref::<SparkArrayFilter>() {
        HigherOrderUdfKind::Filter(gen::SparkArrayFilterUdf {
            index_first: filter.is_index_first(),
        })
    } else {
        return plan_err!("unsupported higher-order function: {}", hof.name());
    };
    Ok(gen::HigherOrderUdf {
        higher_order_udf_kind: Some(udf_kind),
    })
}

fn try_decode_higher_order_udf(udf: Option<gen::HigherOrderUdf>) -> Result<Arc<HigherOrderUDF>> {
    let udf_kind = udf
        .and_then(|udf| udf.higher_order_udf_kind)
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
