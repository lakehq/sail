use std::sync::Arc;

use arrow_schema::{DataType, TimeUnit};
use datafusion_common::ScalarValue;
use datafusion_expr::{HigherOrderUDF, ScalarUDF, ScalarUDFImpl};
use sail_common::utils::string::SPARK_WHITESPACE_OR_ISO_CONTROL_CHARACTERS;
use sail_function::scalar::array::spark_sequence::{SparkSequence, SparkSequenceLazy};
use sail_function::scalar::datetime::convert_tz::ConvertTz;
use sail_function::scalar::datetime::spark_date::SparkDate;
use sail_function::scalar::datetime::spark_timestamp::SparkTimestamp;

use super::Resolver;
use crate::analysis::deferred::Deferred;
use crate::analysis::ir::*;
use crate::error::{PlanError, PlanResult};

impl Resolver<'_, '_> {
    pub(super) fn sequence(
        &mut self,
        arguments: Vec<LogicalExpression>,
    ) -> PlanResult<LogicalExpression> {
        if !matches!(arguments.len(), 2 | 3) {
            return Err(PlanError::invalid(format!(
                "sequence requires 2 or 3 arguments, got {}",
                arguments.len()
            )));
        }
        let types = arguments
            .iter()
            .map(|a| {
                Ok(a.result_type
                    .resolved("sequence argument")?
                    .data_type
                    .clone())
            })
            .collect::<PlanResult<Vec<_>>>()?;
        let config = &self.context.config;
        let udf = SparkSequence::new(config.session_timezone.clone(), config.ansi_mode);
        let targets = udf.coerce_types(&types)?;
        let arguments = arguments
            .into_iter()
            .zip(types)
            .zip(targets)
            .map(|((argument, source), target)| {
                if source == target {
                    return Ok(argument);
                }
                match (&source, &target) {
                    (source, target) if source.is_string() && target.is_integer() => {
                        let spaces = self.literal(ScalarValue::Utf8(Some(
                            SPARK_WHITESPACE_OR_ISO_CONTROL_CHARACTERS.into(),
                        )));
                        let argument = self.scalar("btrim", vec![argument, spaces])?;
                        self.cast(argument, DataType::Int64)
                    }
                    (source, DataType::Date32) if source.is_string() => {
                        let spaces = self.literal(ScalarValue::Utf8(Some(
                            SPARK_WHITESPACE_OR_ISO_CONTROL_CHARACTERS.into(),
                        )));
                        let argument = self.scalar("btrim", vec![argument, spaces])?;
                        self.scalar_udf(
                            Arc::new(ScalarUDF::from(SparkDate::new(false))),
                            vec![argument],
                        )
                    }
                    (source, DataType::Timestamp(TimeUnit::Microsecond, timezone))
                        if source.is_string() =>
                    {
                        let function = ScalarUDF::from(SparkTimestamp::try_new(
                            timezone.clone(),
                            self.context.config.ansi_mode,
                            false,
                        )?);
                        self.scalar_udf(Arc::new(function), vec![argument])
                    }
                    (
                        DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, None),
                        DataType::Timestamp(TimeUnit::Microsecond, Some(timezone)),
                    ) => {
                        let ntz =
                            self.cast(argument, DataType::Timestamp(TimeUnit::Microsecond, None))?;
                        if crate::analysis::typing::temporal::is_spark_compatible_arrow_fixed_offset(
                            timezone.as_ref(),
                        ) {
                            return self.cast(ntz, target);
                        }
                        let source = self.literal(ScalarValue::Utf8(Some(timezone.to_string())));
                        let utc = self.literal(ScalarValue::Utf8(Some("UTC".into())));
                        let value = self.scalar_udf(
                            Arc::new(ScalarUDF::from(ConvertTz::new(false))),
                            vec![source, utc, ntz],
                        )?;
                        let value = self.cast(value, DataType::Int64)?;
                        self.cast(value, target)
                    }
                    _ => self.cast(argument, target),
                }
            })
            .collect::<PlanResult<Vec<_>>>()?;
        let mut has_python = false;
        for argument in &arguments {
            has_python |= argument.any(&|node| match node {
                ExpressionNode::ScalarFunction { function, .. } => {
                    function
                        .inner()
                        .is::<sail_python_udf::udf::pyspark_udf::PySparkUDF>()
                }
                _ => false,
            })?;
        }
        if has_python {
            return self.scalar_udf(Arc::new(ScalarUDF::from(udf)), arguments);
        }
        self.lazy_arguments(
            Arc::new(HigherOrderUDF::new_from_impl(SparkSequenceLazy::new(udf))),
            arguments,
        )
    }

    pub(super) fn lazy_arguments(
        &mut self,
        function: Arc<HigherOrderUDF>,
        arguments: Vec<LogicalExpression>,
    ) -> PlanResult<LogicalExpression> {
        self.mixed_lazy_arguments(
            function,
            arguments
                .into_iter()
                .map(datafusion_expr::ValueOrLambda::Lambda)
                .collect(),
        )
    }

    pub(super) fn mixed_lazy_arguments(
        &mut self,
        function: Arc<HigherOrderUDF>,
        arguments: Vec<datafusion_expr::ValueOrLambda<LogicalExpression, LogicalExpression>>,
    ) -> PlanResult<LogicalExpression> {
        use datafusion_expr::{LambdaParametersProgress, ValueOrLambda};
        let fields = arguments
            .iter()
            .map(|a| {
                Ok(match a {
                    ValueOrLambda::Value(value) => {
                        ValueOrLambda::Value(value.result_type.resolved("lazy value")?.to_field(""))
                    }
                    ValueOrLambda::Lambda(body) => {
                        ValueOrLambda::Lambda(body.result_type.resolved("lazy body")?.to_field(""))
                    }
                })
            })
            .collect::<PlanResult<Vec<_>>>()?;
        let inference = fields
            .iter()
            .map(|f| match f {
                ValueOrLambda::Value(f) => ValueOrLambda::Value(f.clone()),
                ValueOrLambda::Lambda(f) => ValueOrLambda::Lambda(Some(f.clone())),
            })
            .collect::<Vec<_>>();
        let LambdaParametersProgress::Complete(parameters) =
            function.lambda_parameters(0, &inference)?
        else {
            return Err(PlanError::internal(
                "lazy argument parameter inference did not complete",
            ));
        };
        let lambda_count = arguments
            .iter()
            .filter(|a| matches!(a, ValueOrLambda::Lambda(_)))
            .count();
        crate::analysis::typing::coercion::validate_arity(
            function.name(),
            parameters.len(),
            lambda_count,
        )?;
        let literals = arguments
            .iter()
            .map(|a| match a {
                ValueOrLambda::Value(value) => match &value.node {
                    Deferred::Resolved(ExpressionNode::Literal(value)) => Some(value),
                    _ => None,
                },
                _ => None,
            })
            .collect::<Vec<_>>();
        let typing = crate::analysis::typing::higher_order::resolve(&function, &fields, &literals)?;
        let mut parameters = parameters.into_iter();
        let arguments = arguments
            .into_iter()
            .zip(typing.arguments)
            .map(|(argument, target)| {
                Ok(match (argument, target) {
                    (ValueOrLambda::Value(value), ValueOrLambda::Value(target)) => {
                        FunctionArgument::Value(Box::new(
                            self.cast(value, target.data_type().clone())?,
                        ))
                    }
                    (ValueOrLambda::Lambda(body), ValueOrLambda::Lambda(_)) => {
                        let fields = parameters
                            .next()
                            .ok_or_else(|| PlanError::internal("missing lazy parameters"))?;
                        let parameters = PlanSchema {
                            attributes: fields
                                .iter()
                                .enumerate()
                                .map(|(i, f)| {
                                    self.attribute(
                                        format!("lazy_{i}"),
                                        ExpressionType::from_field(f),
                                    )
                                })
                                .collect(),
                        };
                        FunctionArgument::Lambda {
                            id: self.context.ids.expression(),
                            parameters,
                            body: Box::new(body),
                        }
                    }
                    _ => return Err(PlanError::internal("lazy coercion changed argument kind")),
                })
            })
            .collect::<PlanResult<Vec<_>>>()?;
        Ok(LogicalExpression {
            id: self.context.ids.expression(),
            result_type: Deferred::Resolved(ExpressionType::from_field(&typing.result)),
            node: Deferred::Resolved(ExpressionNode::HigherOrderFunction {
                function,
                arguments,
            }),
        })
    }
}
