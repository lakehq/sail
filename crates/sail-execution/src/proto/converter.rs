use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use datafusion::arrow::datatypes::{ArrowPrimitiveType, FieldRef, Float16Type, Schema};
use datafusion::common::metadata::FieldMetadata;
use datafusion::common::{Result, ScalarValue, plan_datafusion_err, plan_err};
use datafusion::logical_expr::physical_planning_context::ScalarSubqueryResults;
use datafusion::logical_expr::{LambdaParametersProgress, ValueOrLambda};
use datafusion::physical_expr::expressions::{LambdaExpr, LambdaVariable, Literal};
use datafusion::physical_expr::{HigherOrderFunctionExpr, PhysicalExpr};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::physical_plan::to_proto::serialize_physical_expr_with_converter;
use datafusion_proto::physical_plan::{
    PhysicalExtensionCodec, PhysicalPlanDecodeContext, PhysicalPlanNodeExt,
    PhysicalProtoConverterExtension,
};
use datafusion_proto::protobuf::{
    PhysicalExprNode, PhysicalExtensionExprNode, PhysicalPlanNode, physical_expr_node,
};
use prost::Message;

use crate::plan::r#gen::extended_physical_expr_node::ExprKind;
use crate::plan::r#gen::{
    ExtendedPhysicalExprNode, Float16LiteralExprNode, HigherOrderUdfExprNode, LambdaExprNode,
    LambdaVariableExprNode,
};
use crate::proto::decode::{try_decode_field_ref, try_decode_higher_order_udf};
use crate::proto::encode::{try_encode_field_ref, try_encode_higher_order_udf};

#[derive(Default)]
pub(super) struct RemotePhysicalProtoConverter {
    decoded_expressions: RefCell<HashMap<u64, Arc<dyn PhysicalExpr>>>,
}

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
        if let Some(results) = ctx.scalar_subquery_results() {
            let converter = ScopedPhysicalProtoConverter {
                inner: self,
                results,
            };
            converter.default_proto_to_execution_plan(proto, ctx)
        } else {
            self.default_proto_to_execution_plan(proto, ctx)
        }
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
        let decoded = match decode_remote_expr_kind(proto)? {
            Some((ExprKind::Float16Literal(node), inputs)) => {
                if !inputs.is_empty() {
                    return plan_err!("Float16Literal expects no inputs, got {}", inputs.len());
                }
                let value = node
                    .bits
                    .map(|bits| -> Result<_> {
                        let bits = u16::try_from(bits).map_err(|_| {
                            plan_datafusion_err!("Float16Literal bits {bits} do not fit in u16")
                        })?;
                        Ok(<Float16Type as ArrowPrimitiveType>::Native::from_bits(bits))
                    })
                    .transpose()?;
                Ok(Arc::new(Literal::new_with_metadata(
                    ScalarValue::Float16(value),
                    Some(FieldMetadata::from(node.metadata)),
                )) as Arc<dyn PhysicalExpr>)
            }
            Some((ExprKind::HigherOrderUdf(node), inputs)) => {
                self.higher_order_proto_to_expr(node, inputs, input_schema, ctx)
            }
            Some((ExprKind::LambdaVariable(node), _)) => {
                let field = try_decode_field_ref(&node.field)?;
                let index = usize::try_from(node.index).map_err(|_| {
                    plan_datafusion_err!(
                        "LambdaVariable index {} does not fit in usize",
                        node.index
                    )
                })?;
                Ok(Arc::new(LambdaVariable::new(index, field)) as Arc<dyn PhysicalExpr>)
            }
            Some((ExprKind::Lambda(node), inputs)) => {
                let [body] = inputs else {
                    return plan_err!("LambdaExpr expects exactly one input, got {}", inputs.len());
                };
                let body = self.proto_to_physical_expr(body, input_schema, ctx)?;
                Ok(Arc::new(LambdaExpr::try_new(node.params, body)?) as Arc<dyn PhysicalExpr>)
            }
            _ => self.default_proto_to_physical_expr(proto, input_schema, ctx),
        }?;

        let Some(expression_id) = proto.expr_id else {
            return Ok(decoded);
        };

        let cached = self
            .decoded_expressions
            .borrow()
            .get(&expression_id)
            .cloned();
        if let Some(cached) = cached {
            let children = decoded.children().into_iter().cloned().collect();
            return cached.with_new_children(children);
        }

        self.decoded_expressions
            .borrow_mut()
            .insert(expression_id, Arc::clone(&decoded));
        Ok(decoded)
    }

    fn physical_expr_to_proto(
        &self,
        expr: &Arc<dyn PhysicalExpr>,
        codec: &dyn PhysicalExtensionCodec,
    ) -> Result<PhysicalExprNode> {
        if let Some(literal) = expr.downcast_ref::<Literal>()
            && let ScalarValue::Float16(value) = literal.value()
        {
            // DataFusion's scalar protobuf widens non-null Float16 values to Float32.
            return extension_expr_to_proto(
                expr,
                ExprKind::Float16Literal(Float16LiteralExprNode {
                    bits: value.map(|value| u32::from(value.to_bits())),
                    metadata: literal.return_field(&Schema::empty())?.metadata().clone(),
                }),
                vec![],
            );
        }
        if let Some(hof) = expr.downcast_ref::<HigherOrderFunctionExpr>() {
            return self.higher_order_expr_to_proto(expr, hof, codec);
        }
        if let Some(lambda) = expr.downcast_ref::<LambdaExpr>() {
            return self.lambda_expr_to_proto(expr, lambda, codec);
        }
        if let Some(var) = expr.downcast_ref::<LambdaVariable>() {
            let index = u32::try_from(var.index()).map_err(|_| {
                plan_datafusion_err!("LambdaVariable index {} does not fit in u32", var.index())
            })?;
            return extension_expr_to_proto(
                expr,
                ExprKind::LambdaVariable(LambdaVariableExprNode {
                    index,
                    field: try_encode_field_ref(var.field())?,
                }),
                vec![],
            );
        }
        serialize_physical_expr_with_converter(expr, codec, self)
    }
}

/// Extension codecs receive a TaskContext rather than a PhysicalPlanDecodeContext.
/// Carry the enclosing scalar scope through their converter so that decoding
/// embedded plans and expressions does not lose the shared results container.
struct ScopedPhysicalProtoConverter<'a> {
    inner: &'a RemotePhysicalProtoConverter,
    results: &'a ScalarSubqueryResults,
}

impl PhysicalProtoConverterExtension for ScopedPhysicalProtoConverter<'_> {
    fn proto_to_execution_plan(
        &self,
        proto: &PhysicalPlanNode,
        ctx: &PhysicalPlanDecodeContext<'_>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let ctx = self.restore_scalar_scope(ctx);
        self.inner.proto_to_execution_plan(proto, &ctx)
    }

    fn proto_to_physical_expr(
        &self,
        proto: &PhysicalExprNode,
        input_schema: &Schema,
        ctx: &PhysicalPlanDecodeContext<'_>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let ctx = self.restore_scalar_scope(ctx);
        self.inner.proto_to_physical_expr(proto, input_schema, &ctx)
    }

    fn execution_plan_to_proto(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        codec: &dyn PhysicalExtensionCodec,
    ) -> Result<PhysicalPlanNode> {
        self.inner.execution_plan_to_proto(plan, codec)
    }

    fn physical_expr_to_proto(
        &self,
        expr: &Arc<dyn PhysicalExpr>,
        codec: &dyn PhysicalExtensionCodec,
    ) -> Result<PhysicalExprNode> {
        self.inner.physical_expr_to_proto(expr, codec)
    }
}

impl ScopedPhysicalProtoConverter<'_> {
    fn restore_scalar_scope<'a>(
        &self,
        ctx: &PhysicalPlanDecodeContext<'a>,
    ) -> PhysicalPlanDecodeContext<'a> {
        // A nested ScalarSubqueryExec establishes its own scope, which must win
        // over the enclosing scope retained for extension codec callbacks.
        if ctx.scalar_subquery_results().is_some() {
            ctx.clone()
        } else {
            ctx.with_scalar_subquery_results(self.results.clone())
        }
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
        let udf = node
            .udf
            .ok_or_else(|| plan_datafusion_err!("missing higher-order function UDF"))?;
        let fun = try_decode_higher_order_udf(&udf)?;
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
                return plan_err!("`{}` returned partial lambda parameters", fun.name());
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
    Schema::new_with_metadata(output, base.metadata().clone())
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::DataType;
    use datafusion::execution::TaskContext;
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::BinaryExpr;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::projection::ProjectionExec;
    use datafusion_proto::physical_plan::DefaultPhysicalExtensionCodec;

    use super::*;

    fn round_trip(expr: &Arc<dyn PhysicalExpr>) -> Result<Arc<dyn PhysicalExpr>> {
        let converter = RemotePhysicalProtoConverter::default();
        let codec = DefaultPhysicalExtensionCodec {};
        let task_ctx = TaskContext::default();
        let ctx = PhysicalPlanDecodeContext::new(&task_ctx, &codec);
        let proto = converter.physical_expr_to_proto(expr, &codec)?;
        let proto = PhysicalExprNode::decode(proto.encode_to_vec().as_slice())
            .map_err(|e| plan_datafusion_err!("{e}"))?;
        converter.proto_to_physical_expr(&proto, &Schema::empty(), &ctx)
    }

    #[test]
    fn float16_literal_round_trip_preserves_bits_and_metadata() -> Result<()> {
        let metadata = HashMap::from([("source".to_string(), "literal".to_string())]);
        // Finite values, subnormals, signed zeros/infinities, and NaN payloads.
        for bits in [
            Some(0x3c00),
            Some(0xc100),
            Some(0x0001),
            Some(0x8001),
            Some(0x0000),
            Some(0x8000),
            Some(0x7c00),
            Some(0xfc00),
            Some(0x7e01),
            Some(0xfd23),
            None,
        ] {
            let value = bits.map(<Float16Type as ArrowPrimitiveType>::Native::from_bits);
            let expr: Arc<dyn PhysicalExpr> = Arc::new(Literal::new_with_metadata(
                ScalarValue::Float16(value),
                Some(FieldMetadata::from(metadata.clone())),
            ));
            let decoded = round_trip(&expr)?;
            let literal = decoded
                .downcast_ref::<Literal>()
                .ok_or_else(|| plan_datafusion_err!("expected literal"))?;
            let ScalarValue::Float16(value) = literal.value() else {
                return plan_err!("Float16 literal changed type");
            };
            assert_eq!(value.map(|value| value.to_bits()), bits);
            assert_eq!(decoded.data_type(&Schema::empty())?, DataType::Float16);
            assert_eq!(decoded.nullable(&Schema::empty())?, bits.is_none());
            assert_eq!(
                decoded.return_field(&Schema::empty())?,
                expr.return_field(&Schema::empty())?
            );
        }
        Ok(())
    }

    #[test]
    fn float32_literal_keeps_standard_encoding() -> Result<()> {
        for value in [Some(1.25), None] {
            let expr: Arc<dyn PhysicalExpr> = Arc::new(Literal::new(ScalarValue::Float32(value)));
            let proto = RemotePhysicalProtoConverter::default()
                .physical_expr_to_proto(&expr, &DefaultPhysicalExtensionCodec {})?;
            assert!(matches!(
                proto.expr_type,
                Some(physical_expr_node::ExprType::Literal(_))
            ));
            let decoded = round_trip(&expr)?;
            assert_eq!(
                decoded
                    .downcast_ref::<Literal>()
                    .ok_or_else(|| plan_datafusion_err!("expected literal"))?
                    .value(),
                &ScalarValue::Float32(value)
            );
        }
        Ok(())
    }

    #[test]
    fn float16_literal_rejects_invalid_payloads() -> Result<()> {
        let converter = RemotePhysicalProtoConverter::default();
        let codec = DefaultPhysicalExtensionCodec {};
        let task_ctx = TaskContext::default();
        let ctx = PhysicalPlanDecodeContext::new(&task_ctx, &codec);
        let expr: Arc<dyn PhysicalExpr> = Arc::new(Literal::new(ScalarValue::Float16(None)));
        for (bits, inputs, message) in [
            (Some(65536), vec![], "do not fit in u16"),
            (Some(u32::MAX), vec![], "do not fit in u16"),
            (None, vec![PhysicalExprNode::default()], "expects no inputs"),
        ] {
            let proto = extension_expr_to_proto(
                &expr,
                ExprKind::Float16Literal(Float16LiteralExprNode {
                    bits,
                    metadata: HashMap::new(),
                }),
                inputs,
            )?;
            let error = converter
                .proto_to_physical_expr(&proto, &Schema::empty(), &ctx)
                .err()
                .ok_or_else(|| plan_datafusion_err!("invalid payload was accepted"))?;
            assert!(error.to_string().contains(message), "{error}");
        }
        Ok(())
    }

    #[test]
    fn float16_literal_nested_in_projection_round_trip() -> Result<()> {
        let converter = RemotePhysicalProtoConverter::default();
        let codec = DefaultPhysicalExtensionCodec {};
        let task_ctx = TaskContext::default();
        let ctx = PhysicalPlanDecodeContext::new(&task_ctx, &codec);
        let value = <Float16Type as ArrowPrimitiveType>::Native::from_bits(0x3c00);
        let literal: Arc<dyn PhysicalExpr> =
            Arc::new(Literal::new(ScalarValue::Float16(Some(value))));
        let expression: Arc<dyn PhysicalExpr> =
            Arc::new(BinaryExpr::new(Arc::clone(&literal), Operator::Eq, literal));
        let plan: Arc<dyn ExecutionPlan> = Arc::new(ProjectionExec::try_new(
            vec![(expression, "equal".to_string())],
            Arc::new(EmptyExec::new(Arc::new(Schema::empty()))),
        )?);
        let proto = converter.execution_plan_to_proto(&plan, &codec)?;
        let proto = PhysicalPlanNode::decode(proto.encode_to_vec().as_slice())
            .map_err(|e| plan_datafusion_err!("{e}"))?;
        let decoded = converter.proto_to_execution_plan(&proto, &ctx)?;
        let projection = decoded
            .downcast_ref::<ProjectionExec>()
            .ok_or_else(|| plan_datafusion_err!("expected projection"))?;
        let expression = &projection.expr()[0].expr;
        let binary = expression
            .downcast_ref::<BinaryExpr>()
            .ok_or_else(|| plan_datafusion_err!("expected binary expression"))?;
        for child in [binary.left(), binary.right()] {
            assert_eq!(
                child
                    .downcast_ref::<Literal>()
                    .ok_or_else(|| plan_datafusion_err!("expected literal"))?
                    .value(),
                &ScalarValue::Float16(Some(value))
            );
        }
        assert_eq!(decoded.schema(), plan.schema());
        Ok(())
    }
}
