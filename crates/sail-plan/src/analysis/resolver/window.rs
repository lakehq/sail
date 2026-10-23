use arrow_schema::DataType;
use datafusion_common::ScalarValue;
use datafusion_expr::registry::FunctionRegistry;
use datafusion_expr::{WindowFrame, WindowFrameBound, WindowFrameUnits, WindowFunctionDefinition};
use sail_common::spec;

use super::{NamedExpression, Resolver};
use crate::analysis::ir::*;
use crate::analysis::typing;
use crate::error::{PlanError, PlanResult};

impl Resolver<'_, '_> {
    #[async_recursion::async_recursion]
    pub(super) async fn window_expression(
        &mut self,
        id: ExpressionId,
        expression: spec::Expr,
        window: spec::Window,
        schema: &PlanSchema,
    ) -> PlanResult<NamedExpression> {
        let (call, inline) = match expression {
            spec::Expr::UnresolvedFunction(call) => (call, None),
            spec::Expr::CommonInlineUserDefinedFunction(function) => {
                let spec::CommonInlineUserDefinedFunction {
                    function_name,
                    deterministic,
                    is_distinct,
                    arguments,
                    function,
                } = function;
                let definition = self.python_definition(function)?;
                (
                    spec::UnresolvedFunction {
                        function_name: spec::ObjectName::bare(function_name),
                        arguments,
                        named_arguments: vec![],
                        is_distinct,
                        is_user_defined_function: true,
                        is_internal: None,
                        ignore_nulls: None,
                        filter: None,
                        order_by: None,
                    },
                    Some((definition, deterministic)),
                )
            }
            _ => return Err(PlanError::analysis("window expression must be a function")),
        };
        let name = Vec::<String>::from(call.function_name)
            .join(".")
            .to_ascii_lowercase();
        if call.order_by.is_some() {
            return Err(PlanError::unsupported("window WITHIN GROUP"));
        }
        let python = if inline.is_some() {
            inline
        } else {
            self.registered_function(&name)?
                .as_ref()
                .map(Self::registered_python)
                .transpose()?
                .flatten()
        };
        let (function, arguments, display_name) = if let Some((definition, deterministic)) = python
        {
            let mut requests = call.arguments;
            requests.extend(call.named_arguments.into_iter().map(|(key, value)| {
                spec::Expr::NamedArgument {
                    key: key.into(),
                    value: Box::new(value),
                }
            }));
            let resolved = self
                .python_arguments(
                    id,
                    &name,
                    definition,
                    deterministic,
                    call.is_distinct,
                    requests,
                    schema,
                    sail_python_udf::udf::pyspark_udaf::PySparkAggregateMode::Window,
                )
                .await?;
            let ExpressionNode::AggregateFunction {
                function,
                arguments,
                ..
            } = resolved
                .expression
                .node
                .into_resolved("Python window function")?
            else {
                return Err(PlanError::analysis(
                    "unknown window function: scalar Python UDF",
                ));
            };
            (
                WindowFunctionDefinition::AggregateUDF(function),
                arguments,
                resolved.name,
            )
        } else {
            if !call.named_arguments.is_empty() {
                return Err(PlanError::unsupported("window named arguments"));
            }
            let function = if let Some(function) = super::window_functions::window_kernel(&name)
                .or_else(|| self.context.session.udwf(&name).ok())
            {
                WindowFunctionDefinition::WindowUDF(function)
            } else {
                WindowFunctionDefinition::AggregateUDF(if name == "median" {
                    datafusion::functions_aggregate::median::median_udaf()
                } else {
                    super::aggregate_functions::aggregate_kernel(
                        if name == "array_join" {
                            "listagg"
                        } else {
                            &name
                        },
                        self.context.config.ansi_mode,
                    )
                    .map(Ok)
                    .unwrap_or_else(|| self.aggregate_kernel(&name))?
                })
            };
            let mut arguments = vec![];
            let mut names = vec![];
            for argument in call.arguments {
                if name == "count"
                    && matches!(argument, spec::Expr::UnresolvedStar { .. })
                    && !call.is_distinct
                {
                    names.push("1".into());
                    arguments.push(self.literal(ScalarValue::Int64(Some(1))));
                } else {
                    let argument = self.expression(argument, schema).await?;
                    arguments.push(argument.expression);
                    names.push(argument.name);
                }
            }
            (function, arguments, format!("{name}({})", names.join(", ")))
        };
        let window = match window {
            spec::Window::Named(name) => self
                .windows
                .get(&name.as_ref().to_ascii_lowercase())
                .cloned()
                .ok_or_else(|| {
                    PlanError::analysis(format!("undefined window: {}", name.as_ref()))
                })?,
            window => window,
        };
        let spec::Window::Unnamed {
            cluster_by,
            partition_by,
            order_by,
            frame,
        } = window
        else {
            return Err(PlanError::analysis(
                "window definition must not reference another named window",
            ));
        };
        if !cluster_by.is_empty() {
            return Err(PlanError::unsupported("native window CLUSTER BY"));
        }
        let mut partition = vec![];
        for expression in partition_by {
            partition.push(self.expression(expression, schema).await?.expression);
        }
        let mut order = vec![];
        for sort in order_by {
            order.push(self.sort(sort, schema).await?);
        }
        let frame = if let Some(frame) = frame {
            let units = match frame.frame_type {
                spec::WindowFrameType::Row => WindowFrameUnits::Rows,
                spec::WindowFrameType::Range => WindowFrameUnits::Range,
            };
            WindowFrame::new_bounds(
                units,
                self.window_bound(frame.lower, units, schema).await?,
                self.window_bound(frame.upper, units, schema).await?,
            )
        } else {
            WindowFrame::new(if order.is_empty() { None } else { Some(false) })
        };
        let order_types = order
            .iter()
            .map(|sort| {
                Ok(sort
                    .expression
                    .result_type
                    .resolved("window order type")?
                    .data_type
                    .clone())
            })
            .collect::<PlanResult<Vec<_>>>()?;
        let frame = sail_type_system::window::resolve_frame(frame, &order_types)?;
        let filter = if let Some(filter) = call.filter {
            let filter = self.expression(*filter, schema).await?.expression;
            Some(Box::new(typing::coerce(
                filter,
                &DataType::Boolean,
                &mut self.context.ids,
            )?))
        } else {
            None
        };
        let expression = self.spark_window(
            &name,
            function,
            arguments,
            super::window_functions::WindowOptions {
                partition_by: partition,
                order_by: order,
                frame,
                aggregate: super::aggregate_functions::AggregateOptions {
                    window: true,
                    distinct: call.is_distinct,
                    ignore_nulls: call.ignore_nulls,
                    filter,
                    order_by: vec![],
                },
            },
        )?;
        Ok(NamedExpression {
            name: display_name,
            expression,
        })
    }

    async fn window_bound(
        &mut self,
        bound: spec::WindowFrameBoundary,
        units: WindowFrameUnits,
        schema: &PlanSchema,
    ) -> PlanResult<WindowFrameBound> {
        let (value, preceding) = match bound {
            spec::WindowFrameBoundary::CurrentRow => return Ok(WindowFrameBound::CurrentRow),
            spec::WindowFrameBoundary::UnboundedPreceding => {
                return Ok(WindowFrameBound::Preceding(ScalarValue::UInt64(None)));
            }
            spec::WindowFrameBoundary::UnboundedFollowing => {
                return Ok(WindowFrameBound::Following(ScalarValue::UInt64(None)));
            }
            spec::WindowFrameBoundary::Preceding(value) => (value, Some(true)),
            spec::WindowFrameBoundary::Following(value) => (value, Some(false)),
            spec::WindowFrameBoundary::Value(value) => (value, None),
        };
        let expression = self.expression(*value, schema).await?.expression;
        let value = crate::analysis::lowering::evaluate_constant(&expression)?;
        if units == WindowFrameUnits::Range {
            if value.is_null() {
                return Err(PlanError::analysis("window boundary value cannot be null"));
            }
            let zero = ScalarValue::new_zero(&value.data_type())?;
            let negative = value.partial_cmp(&zero) == Some(std::cmp::Ordering::Less);
            if preceding.is_some() && negative {
                return Err(PlanError::analysis(
                    "window boundary offset must be nonnegative",
                ));
            }
            let direction = preceding.unwrap_or(negative);
            let value = if negative {
                value.arithmetic_negate()?
            } else {
                value
            };
            return Ok(if direction {
                WindowFrameBound::Preceding(value)
            } else {
                WindowFrameBound::Following(value)
            });
        }
        let value = value.cast_to(&DataType::Int64)?;
        let ScalarValue::Int64(Some(value)) = value else {
            return Err(PlanError::analysis(
                "window boundary requires a non-null integer",
            ));
        };
        if preceding.is_some() && value < 0 {
            return Err(PlanError::analysis(
                "window boundary offset must be nonnegative",
            ));
        }
        if preceding.is_none() {
            match value {
                i64::MIN => return Ok(WindowFrameBound::Preceding(ScalarValue::UInt64(None))),
                i64::MAX => return Ok(WindowFrameBound::Following(ScalarValue::UInt64(None))),
                0 => return Ok(WindowFrameBound::CurrentRow),
                _ => {}
            }
        }
        let preceding = preceding.unwrap_or(value < 0);
        let value = ScalarValue::UInt64(Some(value.unsigned_abs()));
        Ok(if preceding {
            WindowFrameBound::Preceding(value)
        } else {
            WindowFrameBound::Following(value)
        })
    }
}
