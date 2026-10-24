use std::sync::Arc;

use arrow_schema::{DataType, TimeUnit};
use datafusion_common::ScalarValue;
use datafusion_expr::{HigherOrderUDF, Operator, ScalarUDF};
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::datetime::spark_timestamp::SparkTimestamp;
use sail_function::scalar::predicate::rewrite_like_pattern::RewriteLikePatternFunc;
use sail_function::scalar::spark_to_string::SparkToUtf8;

use super::Resolver;
use crate::analysis::deferred::Deferred;
use crate::analysis::ir::*;
use crate::analysis::typing::predicate::*;
use crate::error::{PlanError, PlanResult};

impl Resolver<'_, '_> {
    pub(super) fn predicate_function(
        &mut self,
        name: &str,
        values: &mut Vec<LogicalExpression>,
    ) -> PlanResult<Option<LogicalExpression>> {
        if !matches!(
            name,
            "in" | "like" | "ilike" | "rlike" | "regexp" | "regexp_like"
        ) {
            return Ok(None);
        }
        let values = std::mem::take(values);
        if name == "in" {
            let (value, list) = values.at_least_one()?;
            return self.in_list(value, list, false).map(Some);
        }

        let regex = matches!(name, "rlike" | "regexp" | "regexp_like");
        let (value, mut pattern, escape) = match values.len() {
            2 => {
                let (a, b) = values.two()?;
                (a, b, None)
            }
            3 if !regex => {
                let (a, b, c) = values.three()?;
                (a, b, Some(c))
            }
            n => {
                return Err(PlanError::invalid(format!(
                    "{name} has an invalid argument count: {n}"
                )));
            }
        };
        let mut escape_char = None;
        if let Some(escape) = escape {
            let escape = crate::analysis::lowering::evaluate_constant(&escape)?;
            let value = match escape {
                ScalarValue::Utf8(Some(value))
                | ScalarValue::Utf8View(Some(value))
                | ScalarValue::LargeUtf8(Some(value)) => value,
                _ => {
                    return Err(PlanError::invalid(
                        "escape character must be a string literal",
                    ));
                }
            };
            let mut chars = value.chars();
            let (Some(character), None) = (chars.next(), chars.next()) else {
                return Err(PlanError::invalid(
                    "escape character must be a single character",
                ));
            };
            escape_char = Some(character);
            if character != '\\' {
                let character = self.literal(ScalarValue::Utf8(Some(character.to_string())));
                pattern = self.scalar_udf(
                    Arc::new(ScalarUDF::from(RewriteLikePatternFunc::new())),
                    vec![pattern, character],
                )?;
                escape_char = Some('\\');
            }
        }
        Ok(Some(self.pattern(
            value,
            pattern,
            regex,
            name == "ilike",
            escape_char,
        )?))
    }

    pub(super) fn pattern(
        &mut self,
        value: LogicalExpression,
        pattern: LogicalExpression,
        regex: bool,
        case_insensitive: bool,
        escape_char: Option<char>,
    ) -> PlanResult<LogicalExpression> {
        let value = self.cast(value, DataType::Utf8)?;
        let pattern = self.cast(pattern, DataType::Utf8)?;
        let nullable = value.result_type.resolved("pattern value")?.nullable
            || pattern.result_type.resolved("pattern")?.nullable;
        let node = if regex {
            ExpressionNode::SimilarTo {
                expression: Box::new(value),
                pattern: Box::new(pattern),
                negated: false,
                escape_char,
                case_insensitive,
            }
        } else {
            ExpressionNode::Like {
                expression: Box::new(value),
                pattern: Box::new(pattern),
                negated: false,
                escape_char,
                case_insensitive,
            }
        };
        Ok(LogicalExpression {
            id: self.context.ids.expression(),
            node: Deferred::Resolved(node),
            result_type: Deferred::Resolved(ExpressionType::new(DataType::Boolean, nullable)),
        })
    }
}

impl Resolver<'_, '_> {
    pub(super) fn predicate_pair(
        &mut self,
        mut left: LogicalExpression,
        operator: Operator,
        mut right: LogicalExpression,
    ) -> PlanResult<(LogicalExpression, LogicalExpression)> {
        if !is_comparison(operator) {
            return Ok((left, right));
        }
        let l = left
            .result_type
            .resolved("predicate left")?
            .data_type
            .clone();
        let r = right
            .result_type
            .resolved("predicate right")?
            .data_type
            .clone();
        let config = self.context.config.clone();
        if !config.ansi_mode
            && config.legacy_type_coercion_datetime_to_string
            && is_ordering_comparison(operator)
        {
            if is_date_or_timestamp_ltz(&l) && r.is_string() {
                left = self.predicate_string(left)?;
                return Ok((left, right));
            }
            if l.is_string() && is_date_or_timestamp_ltz(&r) {
                right = self.predicate_string(right)?;
                return Ok((left, right));
            }
        }
        if let Some(timezone) = timestamp_parse_timezone_for_type(&l, &config.session_timezone)
            && r.is_string()
        {
            right = self.predicate_timestamp(right, timezone)?;
        } else if let Some(timezone) =
            timestamp_parse_timezone_for_type(&r, &config.session_timezone)
            && l.is_string()
        {
            left = self.predicate_timestamp(left, timezone)?;
        }
        Ok((left, right))
    }

    fn predicate_timestamp(
        &mut self,
        value: LogicalExpression,
        timezone: Option<Arc<str>>,
    ) -> PlanResult<LogicalExpression> {
        self.scalar_udf(
            Arc::new(ScalarUDF::from(SparkTimestamp::try_new(
                timezone,
                self.context.config.ansi_mode,
                false,
            )?)),
            vec![value],
        )
    }

    fn predicate_string(&mut self, mut value: LogicalExpression) -> PlanResult<LogicalExpression> {
        let field = value
            .result_type
            .resolved("predicate string input")?
            .clone();
        if field.data_type.is_string() {
            return Ok(value);
        }
        if let DataType::Timestamp(unit, Some(_)) = field.data_type {
            value = self.cast(
                value,
                DataType::Timestamp(unit, Some(self.context.config.session_timezone.clone())),
            )?;
        }
        let metadata = interval_metadata(&value, &field.data_type)?;
        let mut args = vec![value];
        if let Some(metadata) = metadata {
            args.push(
                self.literal(ScalarValue::Utf8(Some(
                    metadata
                        .to_json()
                        .map_err(|e| PlanError::analysis(e.to_string()))?,
                ))),
            );
        }
        self.scalar_udf(Arc::new(ScalarUDF::from(SparkToUtf8::new())), args)
    }

    pub(super) fn in_arguments(
        &mut self,
        values: Vec<LogicalExpression>,
    ) -> PlanResult<Vec<LogicalExpression>> {
        let types = values
            .iter()
            .map(|v| Ok(v.result_type.resolved("IN input")?.data_type.clone()))
            .collect::<PlanResult<Vec<_>>>()?;
        let config = self.context.config.clone();
        let Some(policy) = find_timestamp_string_in_coercion(
            &types,
            &config.session_timezone,
            config.ansi_mode,
            config.case_sensitive,
        ) else {
            return Ok(values);
        };
        values
            .into_iter()
            .map(|value| self.in_coercion(value, &policy))
            .collect()
    }

    fn in_coercion(
        &mut self,
        mut value: LogicalExpression,
        policy: &TimestampStringInCoercion,
    ) -> PlanResult<LogicalExpression> {
        let data_type = value.result_type.resolved("IN coercion")?.data_type.clone();
        if !timestamp_string_coercion_requires_rewrite(&data_type, policy) {
            return Ok(value);
        }
        match policy {
            TimestampStringInCoercion::ToString => {
                if data_type.is_null() {
                    self.cast(value, DataType::Utf8)
                } else {
                    self.predicate_string(value)
                }
            }
            TimestampStringInCoercion::ToTimestamp(timezone) => {
                if data_type.is_string() {
                    return self.predicate_timestamp(value, timezone.clone());
                }
                if timezone.is_some()
                    && matches!(
                        data_type,
                        DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, None)
                    )
                {
                    let value = self.predicate_string(value)?;
                    return self.predicate_timestamp(value, timezone.clone());
                }
                self.cast(
                    value,
                    DataType::Timestamp(TimeUnit::Microsecond, timezone.clone()),
                )
            }
            TimestampStringInCoercion::List {
                null_input_type,
                element,
            } => {
                if data_type.is_null() {
                    value = self.cast(value, null_input_type.clone())?;
                }
                let field = match &value.result_type.resolved("IN array")?.data_type {
                    DataType::List(field) | DataType::LargeList(field) => field.clone(),
                    _ => return Ok(value),
                };
                let parameter =
                    self.attribute("element".into(), ExpressionType::from_field(&field));
                let variable = LogicalExpression {
                    id: self.context.ids.expression(),
                    result_type: parameter.result_type.clone(),
                    node: Deferred::Resolved(ExpressionNode::LambdaVariable(parameter.id)),
                };
                let body = self.in_coercion(variable, element)?;
                let function = Arc::new(HigherOrderUDF::new_from_impl(
                    sail_function::scalar::array::spark_array_transform::SparkArrayTransform::new(),
                ));
                let fields = vec![
                    datafusion_expr::ValueOrLambda::Value(
                        value.result_type.resolved("IN array")?.to_field(""),
                    ),
                    datafusion_expr::ValueOrLambda::Lambda(
                        body.result_type
                            .resolved("IN transformed element")?
                            .to_field(""),
                    ),
                ];
                let result = crate::analysis::typing::higher_order::resolve(
                    function.as_ref(),
                    &fields,
                    &[None, None],
                )?;
                Ok(LogicalExpression {
                    id: self.context.ids.expression(),
                    result_type: Deferred::Resolved(ExpressionType::from_field(&result.result)),
                    node: Deferred::Resolved(ExpressionNode::HigherOrderFunction {
                        function,
                        arguments: vec![
                            FunctionArgument::Value(Box::new(value)),
                            FunctionArgument::Lambda {
                                id: self.context.ids.expression(),
                                parameters: PlanSchema {
                                    attributes: vec![parameter],
                                },
                                body: Box::new(body),
                            },
                        ],
                    }),
                })
            }
            TimestampStringInCoercion::Struct {
                null_input_type,
                field_names,
                fields: policies,
            } => {
                if data_type.is_null() {
                    value = self.cast(value, null_input_type.clone())?;
                }
                let DataType::Struct(fields) = &value.result_type.resolved("IN struct")?.data_type
                else {
                    return Ok(value);
                };
                let names = (0..fields.len())
                    .map(|i| format!("__in_field_{i}"))
                    .collect::<Vec<_>>();
                let fields = fields
                    .iter()
                    .zip(&names)
                    .map(|(f, n)| Arc::new(f.as_ref().clone().with_name(n)))
                    .collect::<Vec<_>>();
                value = self.scalar_udf(
                    Arc::new(ScalarUDF::from(
                        sail_function::scalar::spark_struct_rename::SparkStructRename::new(
                            DataType::Struct(fields.into()),
                        ),
                    )),
                    vec![value],
                )?;
                for (name, policy) in names.iter().zip(policies) {
                    let Some(policy) = policy else {
                        continue;
                    };
                    let copied = self.copy_expression(&value)?;
                    let key = self.literal(ScalarValue::Utf8(Some(name.clone())));
                    let field = self.scalar("get_field", vec![copied, key])?;
                    let field = self.in_coercion(field, policy)?;
                    value = self.scalar_udf(
                        Arc::new(ScalarUDF::from(
                            sail_function::scalar::update_struct_field::UpdateStructField::new(
                                vec![name.clone()],
                            ),
                        )),
                        vec![value, field],
                    )?;
                }
                let DataType::Struct(fields) =
                    &value.result_type.resolved("IN updated struct")?.data_type
                else {
                    return Err(PlanError::internal(
                        "IN struct update did not return struct",
                    ));
                };
                let fields = fields
                    .iter()
                    .zip(field_names)
                    .map(|(f, n)| Arc::new(f.as_ref().clone().with_name(n)))
                    .collect::<Vec<_>>();
                self.scalar_udf(
                    Arc::new(ScalarUDF::from(
                        sail_function::scalar::spark_struct_rename::SparkStructRename::new(
                            DataType::Struct(fields.into()),
                        ),
                    )),
                    vec![value],
                )
            }
        }
    }

    pub(super) fn in_list(
        &mut self,
        value: LogicalExpression,
        list: Vec<LogicalExpression>,
        negated: bool,
    ) -> PlanResult<LogicalExpression> {
        let compare = matches!(value.result_type.resolved("IN input")?.data_type, DataType::List(_) | DataType::LargeList(_))
            && list.iter().any(null_literal)
            && !value.any(&|node| matches!(node, ExpressionNode::ScalarFunction { function, .. } if function.signature().volatility == datafusion_expr::Volatility::Volatile))?;
        let mut values = vec![value];
        values.extend(list);
        let mut values = self.in_arguments(values)?.into_iter();
        let value = values
            .next()
            .ok_or_else(|| PlanError::internal("missing IN input"))?;
        let list = values.collect::<Vec<_>>();
        if compare && !list.is_empty() {
            let mut combined = None;
            for item in list {
                let copied = self.copy_expression(&value)?;
                let equal = self.binary(copied, Operator::Eq, item)?;
                combined = Some(match combined {
                    Some(left) => self.binary(left, Operator::Or, equal)?,
                    None => equal,
                });
            }
            let result = combined.ok_or_else(|| PlanError::internal("missing IN comparison"))?;
            return if negated {
                self.not(result)
            } else {
                Ok(result)
            };
        }
        crate::analysis::typing::resolve_in_list(
            self.context.ids.expression(),
            value,
            list,
            negated,
            &mut self.context.ids,
        )
    }
}

fn null_literal(value: &LogicalExpression) -> bool {
    match &value.node {
        Deferred::Resolved(ExpressionNode::Literal(value)) => value.is_null(),
        Deferred::Resolved(
            ExpressionNode::Cast { expression, .. } | ExpressionNode::WithMetadata(expression),
        ) => null_literal(expression),
        _ => false,
    }
}

fn interval_metadata(
    value: &LogicalExpression,
    data_type: &DataType,
) -> PlanResult<Option<sail_common::spec::SparkIntervalMetadata>> {
    use sail_common::spec::{SAIL_SPARK_INTERVAL_METADATA_KEY, SparkIntervalMetadata};
    let field = value.result_type.resolved("interval metadata")?;
    if &field.data_type != data_type {
        return Ok(None);
    }
    if let Some(metadata) = field.metadata.get(SAIL_SPARK_INTERVAL_METADATA_KEY) {
        return SparkIntervalMetadata::from_json(metadata)
            .map(Some)
            .map_err(|e| PlanError::analysis(e.to_string()));
    }
    let mut combined: Option<SparkIntervalMetadata> = None;
    value.clone().map_children::<PlanError>(&mut |child| {
        if let Some(metadata) = interval_metadata(&child, data_type)? {
            combined = Some(match combined {
                Some(current) => current
                    .wider(metadata)
                    .ok_or_else(|| PlanError::analysis("incompatible interval metadata"))?,
                None => metadata,
            });
        }
        Ok(child)
    })?;
    Ok(combined)
}
