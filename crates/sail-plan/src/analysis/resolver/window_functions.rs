use std::sync::Arc;

use arrow_schema::DataType;
use datafusion_common::ScalarValue;
use datafusion_expr::expr::NullTreatment;
use datafusion_expr::{WindowFrame, WindowFunctionDefinition, WindowUDF};
use sail_common_datafusion::utils::items::ItemTaker;

use super::Resolver;
use super::aggregate_functions::AggregateOptions;
use crate::analysis::deferred::Deferred;
use crate::analysis::model::*;
use crate::analysis::typing;
use crate::error::{PlanError, PlanResult};

pub(super) struct WindowOptions {
    pub partition_by: Vec<ExprNode>,
    pub order_by: Vec<SortKey>,
    pub frame: WindowFrame,
    pub aggregate: AggregateOptions,
}

pub(super) fn window_kernel(name: &str) -> Option<Arc<WindowUDF>> {
    use datafusion::functions_window::{cume_dist, lead_lag, nth_value, rank, row_number};
    Some(match name {
        "first" | "first_value" | "any_value" => nth_value::first_value_udwf(),
        "last" | "last_value" => nth_value::last_value_udwf(),
        "nth_value" => nth_value::nth_value_udwf(),
        "lag" => lead_lag::lag_udwf(),
        "lead" => lead_lag::lead_udwf(),
        "rank" => rank::rank_udwf(),
        "dense_rank" => rank::dense_rank_udwf(),
        "percent_rank" => rank::percent_rank_udwf(),
        "row_number" => row_number::row_number_udwf(),
        "cume_dist" => cume_dist::cume_dist_udwf(),
        "ntile" => sail_function::window::spark_ntile_udwf(),
        _ => return None,
    })
}

impl Resolver<'_, '_> {
    pub(super) fn spark_window(
        &mut self,
        name: &str,
        function: WindowFunctionDefinition,
        mut arguments: Vec<ExprNode>,
        mut options: WindowOptions,
    ) -> PlanResult<ExprNode> {
        if let WindowFunctionDefinition::AggregateUDF(function) = function {
            let name = if name == "array_join" {
                "listagg"
            } else {
                name
            };
            let expression = self.spark_aggregate(name, function, arguments, options.aggregate)?;
            return self.aggregate_to_window(
                expression,
                &options.partition_by,
                &options.order_by,
                &options.frame,
            );
        }
        let mut function = function;
        if matches!(
            name,
            "first" | "first_value" | "any_value" | "last" | "last_value"
        ) {
            if arguments.len() == 2 {
                if options.aggregate.ignore_nulls.is_some() {
                    return Err(PlanError::invalid(
                        "arguments conflict with IGNORE NULLS clause",
                    ));
                }
                let (value, flag) = arguments.two()?;
                options.aggregate.ignore_nulls =
                    Some(match flag.kind.into_resolved("null treatment")? {
                        ExprKind::Literal(ScalarValue::Boolean(Some(flag))) => flag,
                        _ => {
                            return Err(PlanError::invalid(
                                "requires a boolean literal as the second argument",
                            ));
                        }
                    });
                arguments = vec![value];
            }
            crate::analysis::typing::coercion::validate_arity(name, arguments.len(), 1)?;
            if options.aggregate.ignore_nulls == Some(true) {
                function =
                    WindowFunctionDefinition::WindowUDF(if matches!(name, "last" | "last_value") {
                        sail_function::window::spark_last_value_udwf(true)
                    } else {
                        sail_function::window::spark_first_value_udwf(true)
                    });
            }
        } else if name == "nth_value" && arguments.len() == 3 {
            let flag = arguments
                .pop()
                .ok_or_else(|| PlanError::internal("missing nth_value flag"))?;
            options.aggregate.ignore_nulls =
                match flag.kind.into_resolved("nth_value null treatment")? {
                    ExprKind::Literal(ScalarValue::Boolean(flag)) => flag,
                    _ => {
                        return Err(PlanError::invalid(
                            "nth_value third argument should be boolean scalar",
                        ));
                    }
                };
        }
        let resolved = crate::analysis::typing::aggregate::resolve_window(
            &function,
            &typing::argument_fields(&arguments)?,
        )?;
        let arguments =
            typing::coerce_arguments(arguments, &resolved.arguments, &mut self.context.ids)?;
        let result = ExprNode {
            id: self.context.ids.expression(),
            result_type: Deferred::Resolved(ExprType::from_field(&resolved.result)),
            kind: Deferred::Resolved(ExprKind::WindowFunction {
                function,
                arguments,
                partition_by: options.partition_by,
                order_by: options.order_by,
                frame: options.frame,
                filter: options.aggregate.filter,
                distinct: options.aggregate.distinct,
                null_treatment: options.aggregate.ignore_nulls.map(|ignore| {
                    if ignore {
                        NullTreatment::IgnoreNulls
                    } else {
                        NullTreatment::RespectNulls
                    }
                }),
            }),
        };
        if resolved.result.data_type() == &DataType::UInt64 {
            self.cast(result, DataType::Int32)
        } else {
            Ok(result)
        }
    }

    fn aggregate_to_window(
        &mut self,
        expression: ExprNode,
        partition: &[ExprNode],
        order: &[SortKey],
        frame: &WindowFrame,
    ) -> PlanResult<ExprNode> {
        if let Deferred::Resolved(ExprKind::AggregateFunction {
            function,
            arguments,
            distinct,
            filter,
            null_treatment,
            ..
        }) = expression.kind
        {
            let partition_by = partition
                .iter()
                .map(|e| self.copy_expression(e))
                .collect::<PlanResult<_>>()?;
            let order_by = order
                .iter()
                .map(|s| {
                    Ok(SortKey {
                        expression: self.copy_expression(&s.expression)?,
                        ascending: s.ascending,
                        nulls_first: s.nulls_first,
                    })
                })
                .collect::<PlanResult<_>>()?;
            Ok(ExprNode {
                id: expression.id,
                result_type: expression.result_type,
                kind: Deferred::Resolved(ExprKind::WindowFunction {
                    function: WindowFunctionDefinition::AggregateUDF(function),
                    arguments,
                    partition_by,
                    order_by,
                    frame: frame.clone(),
                    distinct,
                    filter,
                    null_treatment,
                }),
            })
        } else {
            expression.map_children(&mut |e| self.aggregate_to_window(e, partition, order, frame))
        }
    }
}
