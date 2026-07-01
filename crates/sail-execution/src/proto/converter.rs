use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use datafusion::arrow::datatypes::{FieldRef, Schema};
use datafusion::common::config::CsvOptions;
use datafusion::common::{internal_err, plan_datafusion_err, plan_err, Result};
use datafusion::datasource::physical_plan::CsvSource;
use datafusion::datasource::source::DataSourceExec;
use datafusion::logical_expr::{LambdaParametersProgress, ValueOrLambda};
use datafusion::physical_expr::expressions::{LambdaExpr, LambdaVariable};
use datafusion::physical_expr::{HigherOrderFunctionExpr, PhysicalExpr};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::physical_plan::from_proto::{
    parse_protobuf_file_scan_config, parse_table_schema_from_proto,
};
use datafusion_proto::physical_plan::to_proto::serialize_physical_expr_with_converter;
use datafusion_proto::physical_plan::{
    PhysicalExtensionCodec, PhysicalPlanDecodeContext, PhysicalProtoConverterExtension,
};
use datafusion_proto::protobuf::{
    self, physical_expr_node, physical_plan_node, PhysicalExprNode, PhysicalExtensionExprNode,
    PhysicalPlanNode,
};
use prost::Message;

use crate::plan::gen::extended_physical_expr_node::ExprKind;
use crate::plan::gen::{
    ExtendedPhysicalExprNode, HigherOrderUdfExprNode, LambdaExprNode, LambdaVariableExprNode,
};
use crate::proto::decode::{try_decode_field_ref, try_decode_higher_order_udf};
use crate::proto::encode::{try_encode_field_ref, try_encode_higher_order_udf};

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
        match proto.physical_plan_type.as_ref() {
            Some(physical_plan_node::PhysicalPlanType::CsvScan(scan)) => {
                self.csv_scan_proto_to_execution_plan(scan, ctx)
            }
            _ => self.default_proto_to_execution_plan(proto, ctx),
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
                let field = try_decode_field_ref(&node.field)?;
                Ok(Arc::new(LambdaVariable::new(index, field)))
            }
            Some((ExprKind::Lambda(node), inputs)) => {
                let [body] = inputs else {
                    return plan_err!("LambdaExpr expects exactly one input, got {}", inputs.len());
                };
                let body = self.proto_to_physical_expr(body, input_schema, ctx)?;
                Ok(Arc::new(LambdaExpr::try_new(node.params, body)?))
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

    fn csv_scan_proto_to_execution_plan(
        &self,
        scan: &protobuf::CsvScanExecNode,
        ctx: &PhysicalPlanDecodeContext<'_>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let base_conf = scan
            .base_conf
            .as_ref()
            .ok_or_else(|| plan_datafusion_err!("missing CSV file scan base config"))?;
        let escape = match &scan.optional_escape {
            Some(protobuf::csv_scan_exec_node::OptionalEscape::Escape(escape)) => {
                Some(csv_single_byte(escape, "escape")?)
            }
            None => None,
        };
        let comment = match &scan.optional_comment {
            Some(protobuf::csv_scan_exec_node::OptionalComment::Comment(comment)) => {
                Some(csv_single_byte(comment, "comment")?)
            }
            None => None,
        };

        let table_schema = parse_table_schema_from_proto(base_conf)?;
        let csv_options = CsvOptions {
            has_header: Some(scan.has_header),
            delimiter: csv_single_byte(&scan.delimiter, "delimiter")?,
            quote: csv_single_byte(&scan.quote, "quote")?,
            escape,
            comment,
            newlines_in_values: Some(scan.newlines_in_values),
            truncated_rows: Some(scan.truncate_rows),
            ..Default::default()
        };
        let source = Arc::new(CsvSource::new(table_schema).with_csv_options(csv_options));
        let config = parse_protobuf_file_scan_config(base_conf, ctx, self, source)?;
        Ok(DataSourceExec::from_data_source(config))
    }
}

fn csv_single_byte(value: &str, description: &str) -> Result<u8> {
    if value.len() == 1 {
        Ok(value.as_bytes()[0])
    } else {
        internal_err!("Invalid CSV {description}: expected single character, got {value}")
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
