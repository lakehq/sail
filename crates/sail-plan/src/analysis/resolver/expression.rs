use std::sync::Arc;

use arrow_schema::DataType;
use async_recursion::async_recursion;
use datafusion_common::ScalarValue;
use datafusion_expr::Operator;
use sail_common::spec;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::session::plan::PlanService;
use sail_common_datafusion::utils::items::ItemTaker;

use super::{NamedExpression, Resolver};
use crate::analysis::data_type::DataTypeResolver;
use crate::analysis::deferred::Deferred;
use crate::analysis::ir::*;
use crate::analysis::typing;
use crate::error::{PlanError, PlanResult};

impl Resolver<'_, '_> {
    pub(super) fn types(&self) -> DataTypeResolver<'_> {
        DataTypeResolver {
            config: &self.context.config,
            allow_large_var_types: false,
        }
    }

    #[async_recursion]
    pub(super) async fn expression(
        &mut self,
        request: spec::Expr,
        schema: &PlanSchema,
    ) -> PlanResult<NamedExpression> {
        if let Some((_, value)) = self
            .expression_replacements
            .iter()
            .find(|(original, _)| original == &request)
        {
            let name = value.name.clone();
            let expression = value.expression.clone();
            return Ok(NamedExpression {
                name,
                expression: self.copy_expression(&expression)?,
            });
        }
        let id = self.context.ids.expression();
        if matches!(request, spec::Expr::DefaultColumnValue) {
            return Ok(NamedExpression {
                name: "DEFAULT".into(),
                expression: LogicalExpression {
                    id,
                    node: Deferred::unresolved(request),
                    result_type: Deferred::Resolved(ExpressionType::new(DataType::Null, true)),
                },
            });
        }
        let distinct_test = matches!(&request, spec::Expr::IsDistinctFrom { .. });
        let (name, node, result_type) = match request {
            spec::Expr::CommonInlineUserDefinedFunction(function) => {
                return self.inline_python(id, function, schema).await;
            }
            spec::Expr::SortOrder(sort) => return self.expression(*sort.child, schema).await,
            spec::Expr::UnresolvedRegex { col_name, plan_id } => {
                return self
                    .expand_regex(&col_name, plan_id, schema)?
                    .one()
                    .map_err(Into::into);
            }
            spec::Expr::UnresolvedDate { value } => {
                let literal = self.types().date_literal(&value)?;
                return self.expression(spec::Expr::Literal(literal), schema).await;
            }
            spec::Expr::UnresolvedTime { value } => {
                let literal = self.types().time_literal(&value)?;
                return self.expression(spec::Expr::Literal(literal), schema).await;
            }
            spec::Expr::UnresolvedTimestamp {
                value,
                timestamp_type,
            } => {
                let literal = self.types().timestamp_literal(&value, timestamp_type)?;
                return self.expression(spec::Expr::Literal(literal), schema).await;
            }
            spec::Expr::Literal(literal) => {
                let metadata = literal.spark_interval_metadata()?;
                let value = self.types().resolve_literal(literal)?;
                let name = self
                    .context
                    .session
                    .extension::<PlanService>()?
                    .plan_formatter()
                    .literal_to_string(&value, &self.context.config.session_timezone)?;
                let mut result_type = ExpressionType::new(value.data_type(), value.is_null());
                if let Some(metadata) = metadata {
                    result_type.metadata.insert(
                        spec::SAIL_SPARK_INTERVAL_METADATA_KEY.into(),
                        metadata.to_json()?,
                    );
                }
                (name, ExpressionNode::Literal(value), result_type)
            }
            spec::Expr::UnresolvedAttribute {
                name,
                plan_id,
                is_metadata_column,
            } => {
                if is_metadata_column {
                    return Err(PlanError::unsupported("native metadata column binding"));
                }
                let parts: Vec<String> = name.into();
                if plan_id.is_none()
                    && let Some(variable) = self.lambda_variable(id, &parts)?
                {
                    return Ok(variable);
                }
                let result = self.resolve_attribute(id, &parts, plan_id, schema);
                if result.is_err() && plan_id.is_none() && parts.len() == 1 {
                    let name = parts[0].to_ascii_lowercase();
                    if matches!(
                        name.as_str(),
                        "current_date"
                            | "current_timestamp"
                            | "current_time"
                            | "current_user"
                            | "user"
                            | "session_user"
                    ) {
                        let name = if matches!(name.as_str(), "user" | "session_user") {
                            "current_user"
                        } else {
                            &name
                        };
                        let mut value = self
                            .expression(
                                super::write_input::parse_expression(&format!("{name}()"))?,
                                schema,
                            )
                            .await?;
                        value.name = if name == "current_time" {
                            "current_time(6)".into()
                        } else {
                            format!("{name}()")
                        };
                        return Ok(value);
                    }
                }
                return result;
            }

            spec::Expr::UnresolvedNamedLambdaVariable(variable) => {
                let name: Vec<String> = variable.name.into();
                return self.lambda_variable(id, &name)?.ok_or_else(|| {
                    PlanError::analysis(format!("unresolved lambda variable: {}", name.join(".")))
                });
            }
            spec::Expr::Alias {
                expr,
                name,
                metadata,
            } => {
                let inner = self.expression(*expr, schema).await?;
                let name = if name.is_empty() {
                    inner.name
                } else {
                    String::from(name.one()?)
                };
                let mut result_type = inner.expression.result_type.resolved("alias type")?.clone();
                if let Some(metadata) = metadata {
                    result_type.metadata.extend(metadata);
                }
                (
                    name,
                    ExpressionNode::WithMetadata(Box::new(inner.expression)),
                    result_type,
                )
            }

            spec::Expr::Cast {
                expr,
                cast_to_type,
                rename: _,
                is_try,
            } => {
                let inner = self.expression(*expr, schema).await?;
                return self.explicit_cast(inner, cast_to_type, is_try);
            }
            spec::Expr::UpdateFields {
                struct_expression,
                field_name,
                value_expression,
            } => {
                let source = self.expression(*struct_expression, schema).await?;
                let fields: Vec<String> = field_name.into();
                let (operation, function, arguments) = if let Some(value) = value_expression {
                    let value = self.expression(*value, schema).await?;
                    (
                        format!("WithField({})", value.name),
                        datafusion_expr::ScalarUDF::from(
                            sail_function::scalar::update_struct_field::UpdateStructField::new(
                                fields,
                            ),
                        ),
                        vec![source.expression, value.expression],
                    )
                } else {
                    (
                        "dropfield()".into(),
                        datafusion_expr::ScalarUDF::from(
                            sail_function::scalar::drop_struct_field::DropStructField::new(fields),
                        ),
                        vec![source.expression],
                    )
                };
                let name = match source.name.strip_suffix(')') {
                    Some(prefix) if prefix.starts_with("update_fields(") => {
                        format!("{prefix}, {operation})")
                    }
                    _ => format!("update_fields({}, {operation})", source.name),
                };
                return Ok(NamedExpression {
                    name,
                    expression: typing::resolve_scalar_call(
                        id,
                        Arc::new(function),
                        arguments,
                        &mut self.context.ids,
                    )?,
                });
            }
            spec::Expr::UnresolvedExtractValue { child, extraction } => {
                let recoverable = recoverable_extraction(&child);
                let qualified_name = matches!(
                    &*child,
                    spec::Expr::UnresolvedAttribute {
                        plan_id: Some(_),
                        ..
                    }
                );
                let child = self.expression(*child, schema).await?;
                let extraction = self.expression(*extraction, schema).await?;
                let display = (qualified_name
                    && matches!(
                        child
                            .expression
                            .result_type
                            .resolved("extraction input")?
                            .data_type,
                        DataType::Struct(_)
                    ))
                .then(|| format!("{}.{}", child.name, extraction.name));
                let source = child.expression.clone();
                let mut result = self.extract_value(id, child, extraction);
                if let (Ok(value), Some(name)) = (&mut result, display) {
                    value.name = name;
                }
                if result.is_err() && recoverable {
                    self.discard_extraction_scope(&source)?;
                }
                return result;
            }
            spec::Expr::Window {
                window_function,
                window,
            } => {
                return self
                    .window_expression(id, *window_function, window, schema)
                    .await;
            }
            spec::Expr::UnresolvedFunction(function) => {
                return self.function(id, function, schema).await;
            }
            spec::Expr::CallFunction {
                function_name,
                arguments,
            } => {
                return self
                    .function(
                        id,
                        spec::UnresolvedFunction {
                            function_name,
                            arguments,
                            named_arguments: vec![],
                            is_distinct: false,
                            is_user_defined_function: false,
                            is_internal: None,
                            ignore_nulls: None,
                            filter: None,
                            order_by: None,
                        },
                        schema,
                    )
                    .await;
            }
            spec::Expr::Between {
                expr,
                negated,
                low,
                high,
            } => {
                let value = self.expression(*expr, schema).await?;
                let low = self.expression(*low, schema).await?;
                let high = self.expression(*high, schema).await?;
                let value_name = value.name;
                let low_name = low.name;
                let high_name = high.name;
                let (value_expression, low_expression) =
                    self.predicate_pair(value.expression, Operator::GtEq, low.expression)?;
                let (value_expression, high_expression) =
                    self.predicate_pair(value_expression, Operator::LtEq, high.expression)?;
                let value = NamedExpression {
                    name: value_name,
                    expression: value_expression,
                };
                let low = NamedExpression {
                    name: low_name,
                    expression: low_expression,
                };
                let high = NamedExpression {
                    name: high_name,
                    expression: high_expression,
                };
                let fields = typing::argument_fields(&[
                    value.expression.clone(),
                    low.expression.clone(),
                    high.expression.clone(),
                ])?;
                let common = crate::analysis::typing::coercion::comparison_type(
                    &fields
                        .iter()
                        .map(|f| f.data_type().clone())
                        .collect::<Vec<_>>(),
                )?;
                let nullable = fields.iter().any(|f| f.is_nullable());
                (
                    format!(
                        "({} {}BETWEEN {} AND {})",
                        value.name,
                        if negated { "NOT " } else { "" },
                        low.name,
                        high.name
                    ),
                    ExpressionNode::Between {
                        expression: Box::new(typing::coerce(
                            value.expression,
                            &common,
                            &mut self.context.ids,
                        )?),
                        low: Box::new(typing::coerce(
                            low.expression,
                            &common,
                            &mut self.context.ids,
                        )?),
                        high: Box::new(typing::coerce(
                            high.expression,
                            &common,
                            &mut self.context.ids,
                        )?),
                        negated,
                    },
                    ExpressionType::new(DataType::Boolean, nullable),
                )
            }
            spec::Expr::IsDistinctFrom { left, right }
            | spec::Expr::IsNotDistinctFrom { left, right } => {
                let left = self.expression(*left, schema).await?;
                let right = self.expression(*right, schema).await?;
                let operator = if distinct_test {
                    Operator::IsDistinctFrom
                } else {
                    Operator::IsNotDistinctFrom
                };
                return Ok(NamedExpression {
                    name: format!("({} {} {})", left.name, operator, right.name),
                    expression: self.binary(left.expression, operator, right.expression)?,
                });
            }
            spec::Expr::SimilarTo {
                expr,
                pattern,
                negated,
                escape_char,
                case_insensitive,
            } => {
                let value = self.expression(*expr, schema).await?;
                let pattern = self.expression(*pattern, schema).await?;
                let nullable = value
                    .expression
                    .result_type
                    .resolved("SIMILAR TO input")?
                    .nullable
                    || pattern
                        .expression
                        .result_type
                        .resolved("SIMILAR TO pattern")?
                        .nullable;
                (
                    format!("({} SIMILAR TO {})", value.name, pattern.name),
                    ExpressionNode::SimilarTo {
                        expression: Box::new(typing::coerce(
                            value.expression,
                            &DataType::Utf8,
                            &mut self.context.ids,
                        )?),
                        pattern: Box::new(typing::coerce(
                            pattern.expression,
                            &DataType::Utf8,
                            &mut self.context.ids,
                        )?),
                        negated,
                        escape_char,
                        case_insensitive,
                    },
                    ExpressionType::new(DataType::Boolean, nullable),
                )
            }
            spec::Expr::InSubquery {
                expr,
                subquery,
                negated,
            } => {
                if let spec::Expr::UnresolvedFunction(function) = expr.as_ref()
                    && function.function_name.parts() == [spec::Identifier::from("struct")]
                    && function.arguments.len() > 1
                {
                    return self
                        .multi_column_in_subquery(
                            function.arguments.clone(),
                            *subquery,
                            negated,
                            schema,
                        )
                        .await;
                }
                let value = self.expression(*expr, schema).await?;
                let query = self.subquery(*subquery, schema).await?;
                let [attribute] = query.schema.resolved("IN subquery")?.attributes.as_slice()
                else {
                    return Err(PlanError::analysis("IN subquery must return one column"));
                };
                let right_name = attribute.name.clone();
                let right = self.reference(attribute)?;
                let (left, right) = self.in_arguments(vec![value.expression, right])?.two()?;
                let left_field = left.result_type.resolved("IN left")?;
                let right_field = right.result_type.resolved("IN right")?;
                let nullable = left_field.nullable || right_field.nullable;
                let common = crate::analysis::typing::coercion::comparison_type(&[
                    left_field.data_type.clone(),
                    right_field.data_type.clone(),
                ])?;
                let left = self.cast(left, common.clone())?;
                let right = self.cast(right, common)?;
                let query = self.project(
                    query,
                    vec![NamedExpression {
                        name: right_name,
                        expression: right,
                    }],
                )?;
                (
                    format!(
                        "({} {}IN (subquery))",
                        value.name,
                        if negated { "NOT " } else { "" }
                    ),
                    ExpressionNode::InSubquery {
                        expression: Box::new(left),
                        query: Box::new(query),
                        negated,
                    },
                    ExpressionType::new(DataType::Boolean, nullable),
                )
            }
            spec::Expr::InList {
                expr,
                list,
                negated,
            } => {
                let value = self.expression(*expr, schema).await?;
                let mut values = vec![];
                let mut names = vec![];
                for item in list {
                    let item = self.expression(item, schema).await?;
                    names.push(item.name);
                    values.push(item.expression);
                }
                return Ok(NamedExpression {
                    name: format!(
                        "({} {}IN ({}))",
                        value.name,
                        if negated { "NOT " } else { "" },
                        names.join(", ")
                    ),
                    expression: self.in_list(value.expression, values, negated)?,
                });
            }
            spec::Expr::IsNull(expr) => {
                let inner = self.expression(*expr, schema).await?;
                (
                    format!("({} IS NULL)", inner.name),
                    ExpressionNode::IsNull(Box::new(inner.expression)),
                    ExpressionType::new(DataType::Boolean, false),
                )
            }
            spec::Expr::IsNotNull(expr) => {
                let inner = self.expression(*expr, schema).await?;
                (
                    format!("({} IS NOT NULL)", inner.name),
                    ExpressionNode::IsNotNull(Box::new(inner.expression)),
                    ExpressionType::new(DataType::Boolean, false),
                )
            }
            spec::Expr::IsTrue(expr) => {
                return self.truth_test(id, *expr, Some(true), false, schema).await;
            }
            spec::Expr::IsNotTrue(expr) => {
                return self.truth_test(id, *expr, Some(true), true, schema).await;
            }
            spec::Expr::IsFalse(expr) => {
                return self.truth_test(id, *expr, Some(false), false, schema).await;
            }
            spec::Expr::IsNotFalse(expr) => {
                return self.truth_test(id, *expr, Some(false), true, schema).await;
            }
            spec::Expr::IsUnknown(expr) => {
                return self.truth_test(id, *expr, None, false, schema).await;
            }
            spec::Expr::IsNotUnknown(expr) => {
                return self.truth_test(id, *expr, None, true, schema).await;
            }
            spec::Expr::ScalarSubquery { subquery } => {
                let query = self.subquery(*subquery, schema).await?;
                let [field] = query
                    .schema
                    .resolved("scalar subquery output")?
                    .attributes
                    .as_slice()
                else {
                    return Err(PlanError::analysis(
                        "scalar subquery must return one column",
                    ));
                };
                let mut result_type = field.result_type.resolved("scalar subquery type")?.clone();
                result_type.nullable = true;
                (
                    "scalarsubquery()".into(),
                    ExpressionNode::ScalarSubquery(Box::new(query)),
                    result_type,
                )
            }
            spec::Expr::Exists { subquery, negated } => {
                let query = self.subquery(*subquery, schema).await?;
                (
                    "exists()".into(),
                    ExpressionNode::Exists {
                        query: Box::new(query),
                        negated,
                    },
                    ExpressionType::new(DataType::Boolean, false),
                )
            }
            spec::Expr::IdentifierClause { expr } => {
                let name = self.identifier(*expr).await?;
                return self.resolve_attribute(id, &Vec::<String>::from(name), None, schema);
            }
            spec::Expr::Subquery {
                plan_id,
                subquery_type,
                in_subquery_values,
                negated,
            } => {
                return self
                    .referenced_subquery(
                        plan_id,
                        subquery_type,
                        in_subquery_values,
                        negated,
                        schema,
                    )
                    .await;
            }
            spec::Expr::Placeholder(name) => {
                let key = if self.parameters.contains_key(&name) {
                    name.clone()
                } else {
                    name.strip_prefix([':', '$']).unwrap_or(&name).to_string()
                };
                let value = self
                    .parameters
                    .get(&key)
                    .cloned()
                    .ok_or_else(|| PlanError::analysis(format!("unbound parameter: {name}")))?;
                let result_type = ExpressionType::new(value.data_type(), value.is_null());
                (name, ExpressionNode::Literal(value), result_type)
            }
            expression => {
                return Err(PlanError::unsupported(format!(
                    "native expression resolution: {expression:?}"
                )));
            }
        };
        Ok(NamedExpression {
            name,
            expression: LogicalExpression {
                id,
                node: Deferred::Resolved(node),
                result_type: Deferred::Resolved(result_type),
            },
        })
    }

    #[async_recursion::async_recursion]
    pub(super) async fn subquery(
        &mut self,
        query: spec::QueryPlan,
        schema: &PlanSchema,
    ) -> PlanResult<LogicalPlan> {
        let depth = self.outer.len();
        let recovery = self.recovery.take();
        if let Some(scope) = recovery {
            self.outer_recovery.insert(depth, scope);
        }
        self.outer.push(schema.clone());
        let aliases = std::mem::take(&mut self.expression_aliases);
        let replacements = std::mem::take(&mut self.expression_replacements);
        let result = self.query(query).await;
        self.expression_aliases = aliases;
        self.expression_replacements = replacements;
        self.outer.pop();
        self.recovery = self.outer_recovery.remove(&depth);
        self.visible_output(result?)
    }

    #[async_recursion::async_recursion]
    async fn truth_test(
        &mut self,
        id: ExpressionId,
        expression: spec::Expr,
        value: Option<bool>,
        negated: bool,
        schema: &PlanSchema,
    ) -> PlanResult<NamedExpression> {
        let inner = self.expression(expression, schema).await?;
        let expression =
            typing::coerce(inner.expression, &DataType::Boolean, &mut self.context.ids)?;
        Ok(NamedExpression {
            name: format!(
                "({} IS {}{})",
                inner.name,
                if negated { "NOT " } else { "" },
                match value {
                    Some(true) => "TRUE",
                    Some(false) => "FALSE",
                    None => "UNKNOWN",
                }
            ),
            expression: LogicalExpression {
                id,
                node: Deferred::Resolved(ExpressionNode::TruthTest {
                    expression: Box::new(expression),
                    value,
                    negated,
                }),
                result_type: Deferred::Resolved(ExpressionType::new(DataType::Boolean, false)),
            },
        })
    }

    #[async_recursion::async_recursion]
    pub(super) async fn sort(
        &mut self,
        sort: spec::SortOrder,
        schema: &PlanSchema,
    ) -> PlanResult<SortExpression> {
        let ascending = !matches!(sort.direction, spec::SortDirection::Descending);
        Ok(SortExpression {
            expression: self.expression(*sort.child, schema).await?.expression,
            ascending,
            nulls_first: match sort.null_ordering {
                spec::NullOrdering::Unspecified => ascending,
                spec::NullOrdering::NullsFirst => true,
                spec::NullOrdering::NullsLast => false,
            },
        })
    }

    #[async_recursion::async_recursion]
    async fn function(
        &mut self,
        id: ExpressionId,
        function: spec::UnresolvedFunction,
        schema: &PlanSchema,
    ) -> PlanResult<NamedExpression> {
        let spec::UnresolvedFunction {
            function_name,
            mut arguments,
            named_arguments,
            is_distinct,
            is_user_defined_function: _,
            is_internal: _,
            ignore_nulls,
            filter,
            mut order_by,
        } = function;
        let original_name = Vec::<String>::from(function_name).join(".");
        let name = original_name.to_ascii_lowercase();
        let registered = self.registered_function(&name)?;
        if let Some(function) = &registered
            && let Some((definition, deterministic)) = Self::registered_python(function)?
        {
            if ignore_nulls.is_some() || filter.is_some() || order_by.is_some() {
                return Err(PlanError::analysis("invalid scalar function clause"));
            }
            arguments.extend(named_arguments.into_iter().map(|(key, value)| {
                spec::Expr::NamedArgument {
                    key: key.into(),
                    value: Box::new(value),
                }
            }));
            return self
                .python_arguments(
                    id,
                    &name,
                    definition,
                    deterministic,
                    is_distinct,
                    arguments,
                    schema,
                    sail_python_udf::udf::pyspark_udaf::PySparkAggregateMode::Grouped,
                )
                .await;
        }
        if !named_arguments.is_empty() {
            return Err(PlanError::unsupported("native named function arguments"));
        }
        if matches!(name.as_str(), "aggregate" | "reduce") && arguments.len() == 3 {
            let parameter = spec::UnresolvedNamedLambdaVariable {
                name: spec::ObjectName::bare("__finish"),
            };
            arguments.push(spec::Expr::LambdaFunction {
                function: Box::new(spec::Expr::UnresolvedNamedLambdaVariable(parameter.clone())),
                arguments: vec![parameter],
            });
        }
        if let Some(function) = super::lambda::kernel(&name, &self.context.config)
            && (arguments.iter().any(super::lambda::is_lambda)
                || matches!(
                    name.as_str(),
                    "map_filter" | "transform_keys" | "transform_values"
                ))
        {
            if is_distinct || ignore_nulls.is_some() || filter.is_some() || order_by.is_some() {
                return Err(PlanError::analysis("invalid higher-order function clause"));
            }
            return self
                .higher_order(id, name, function, arguments, schema)
                .await;
        }
        if name == "mode"
            && let Some(sorts) = order_by.take()
        {
            if !arguments.is_empty() {
                return Err(PlanError::invalid(
                    "mode() with WITHIN GROUP does not take arguments",
                ));
            }
            let sort = sorts.one()?;
            let tie_break = if sort.direction == spec::SortDirection::Descending {
                "highest"
            } else {
                "lowest"
            };
            arguments = vec![
                *sort.child,
                spec::Expr::Literal(spec::Literal::Utf8 {
                    value: Some(tie_break.to_string()),
                }),
            ];
        }
        let mut values = Vec::with_capacity(arguments.len());
        let mut names = Vec::with_capacity(arguments.len());
        let argument_count = arguments.len();
        for (index, mut argument) in arguments.into_iter().enumerate() {
            if index == 0
                && argument_count == 3
                && matches!(
                    name.as_str(),
                    "dateadd"
                        | "date_add"
                        | "timestampadd"
                        | "timestampdiff"
                        | "date_diff"
                        | "datediff"
                )
                && let spec::Expr::UnresolvedAttribute { name, .. } = &argument
            {
                argument = spec::Expr::Literal(spec::Literal::Utf8 {
                    value: Some(Vec::<String>::from(name.clone()).join(".")),
                });
            }
            if name == "count"
                && matches!(argument, spec::Expr::UnresolvedStar { .. })
                && !is_distinct
            {
                names.push("1".into());
                values.push(LogicalExpression::literal(
                    self.context.ids.expression(),
                    ScalarValue::Int64(Some(1)),
                ));
            } else if let spec::Expr::UnresolvedRegex { col_name, plan_id } = argument {
                for value in self.expand_regex(&col_name, plan_id, schema)? {
                    names.push(value.name);
                    values.push(value.expression);
                }
            } else if let spec::Expr::UnresolvedStar {
                target,
                plan_id,
                wildcard_options,
            } = argument
            {
                for argument in self
                    .expand_star(target, plan_id, wildcard_options, schema)
                    .await?
                {
                    names.push(argument.name);
                    values.push(argument.expression);
                }
            } else {
                let argument = self.expression(argument, schema).await?;
                names.push(argument.name);
                values.push(argument.expression);
            }
        }
        let display_name = self
            .context
            .session
            .extension::<PlanService>()?
            .plan_formatter()
            .function_to_string(
                &original_name,
                names.iter().map(String::as_str).collect(),
                is_distinct,
            )?;
        let scalar_clause = || -> PlanResult<()> {
            if is_distinct || ignore_nulls.is_some() || filter.is_some() || order_by.is_some() {
                return Err(PlanError::analysis("invalid scalar function clause"));
            }
            Ok(())
        };
        let operator = match name.as_str() {
            "=" | "==" => Some(Operator::Eq),
            "!=" | "<>" => Some(Operator::NotEq),
            "<" => Some(Operator::Lt),
            "<=" => Some(Operator::LtEq),
            ">" => Some(Operator::Gt),
            ">=" => Some(Operator::GtEq),
            "<=>" | "equal_null" => Some(Operator::IsNotDistinctFrom),
            "shiftleft" | "<<" => Some(Operator::BitwiseShiftLeft),
            "shiftright" | ">>" => Some(Operator::BitwiseShiftRight),
            "and" => Some(Operator::And),
            "or" => Some(Operator::Or),
            "+" if values.len() == 2 => Some(Operator::Plus),
            "-" if values.len() == 2 => Some(Operator::Minus),
            "*" => Some(Operator::Multiply),
            "/" => Some(Operator::Divide),
            "%" => Some(Operator::Modulo),
            "&" => Some(Operator::BitwiseAnd),
            "|" => Some(Operator::BitwiseOr),
            "^" => Some(Operator::BitwiseXor),
            _ => None,
        };
        let expression = if let Some(function) = registered {
            scalar_clause()?;
            typing::resolve_scalar_call(id, Arc::new(function), values, &mut self.context.ids)?
        } else if let Some(expression) = self.math_function(&name, &mut values)? {
            scalar_clause()?;
            expression
        } else if let Some(expression) = self.collection_function(&name, &mut values)? {
            scalar_clause()?;
            expression
        } else if let Some(expression) = self.array_function(&name, &mut values, &names)? {
            scalar_clause()?;
            expression
        } else if let Some(expression) = self.predicate_function(&name, &mut values)? {
            scalar_clause()?;
            expression
        } else if let Some(expression) = self.data_function(&name, &mut values)? {
            scalar_clause()?;
            expression
        } else if let Some(expression) = self.conditional_function(&name, &mut values)? {
            scalar_clause()?;
            expression
        } else if let Some(expression) = self.misc_function(&name, &mut values, &names)? {
            scalar_clause()?;
            expression
        } else if let Some(expression) = self.string_function(&name, &mut values)? {
            scalar_clause()?;
            expression
        } else if let Some(expression) = self.datetime_function(&name, &mut values)? {
            scalar_clause()?;
            expression
        } else if let Some(expression) = self.timezone_function(&name, &mut values)? {
            scalar_clause()?;
            expression
        } else if let Some(expression) = self.datetime_arithmetic_function(&name, &mut values)? {
            scalar_clause()?;
            expression
        } else if let Some(operator) = operator {
            scalar_clause()?;
            let (left, right) = values.two()?;
            self.binary(left, operator, right)?
        } else if matches!(name.as_str(), "+" | "-" | "positive" | "negative") {
            scalar_clause()?;
            let value = values.one()?;
            let result_type = value
                .result_type
                .resolved("unary arithmetic input")?
                .clone();
            if !result_type.data_type.is_numeric()
                && !matches!(
                    result_type.data_type,
                    DataType::Interval(_) | DataType::Duration(_)
                )
            {
                return Err(PlanError::analysis(
                    "unary arithmetic requires a numeric or interval value",
                ));
            }
            if matches!(name.as_str(), "+" | "positive") {
                value
            } else {
                LogicalExpression {
                    id,
                    node: Deferred::Resolved(ExpressionNode::Negative(Box::new(value))),
                    result_type: Deferred::Resolved(result_type),
                }
            }
        } else if matches!(name.as_str(), "not" | "!" | "isnull" | "isnotnull") {
            scalar_clause()?;
            let value = values.one()?;
            let nullable = value.result_type.resolved("predicate input")?.nullable;
            let (node, nullable) = match name.as_str() {
                "isnull" => (ExpressionNode::IsNull(Box::new(value)), false),
                "isnotnull" => (ExpressionNode::IsNotNull(Box::new(value)), false),
                _ => (
                    ExpressionNode::Not(Box::new(typing::coerce(
                        value,
                        &DataType::Boolean,
                        &mut self.context.ids,
                    )?)),
                    nullable,
                ),
            };
            LogicalExpression {
                id,
                node: Deferred::Resolved(node),
                result_type: Deferred::Resolved(ExpressionType::new(DataType::Boolean, nullable)),
            }
        } else if name == "named_struct" {
            scalar_clause()?;
            if values.is_empty() || !values.len().is_multiple_of(2) {
                return Err(PlanError::analysis(
                    "named_struct requires name/value pairs",
                ));
            }
            let mut fields = vec![];
            let mut args = vec![];
            let mut values = values.into_iter();
            while let Some(name) = values.next() {
                let name = crate::analysis::lowering::evaluate_constant(&name)?;
                fields.push(
                    name.try_as_str()
                        .flatten()
                        .ok_or_else(|| {
                            PlanError::analysis("named_struct field name must be a string literal")
                        })?
                        .to_string(),
                );
                args.push(
                    values
                        .next()
                        .ok_or_else(|| PlanError::internal("missing struct value"))?,
                );
            }
            self.scalar_udf(
                Arc::new(datafusion_expr::ScalarUDF::from(
                    sail_function::scalar::struct_function::StructFunction::new(fields),
                )),
                args,
            )?
        } else if name == "struct" {
            scalar_clause()?;
            let field_names = values
                .iter()
                .zip(&names)
                .enumerate()
                .map(|(i, (value, name))| {
                    if matches!(
                        value.node,
                        Deferred::Resolved(
                            ExpressionNode::Attribute(_)
                                | ExpressionNode::OuterAttribute { .. }
                                | ExpressionNode::WithMetadata(_)
                                | ExpressionNode::LambdaVariable(_)
                        )
                    ) || matches!(&value.node, Deferred::Resolved(ExpressionNode::ScalarFunction { function, .. }) if function.name() == "get_field") {
                        name.clone()
                    } else {
                        format!("col{}", i + 1)
                    }
                })
                .collect();
            let function = datafusion_expr::ScalarUDF::from(
                sail_function::scalar::struct_function::StructFunction::new(field_names),
            );
            typing::resolve_scalar_call(id, Arc::new(function), values, &mut self.context.ids)?
        } else if let Some(function) = super::functions::scalar_kernel(&name, &self.context.config)
            .or_else(|| self.scalar_kernel(&name).ok())
            .or_else(|| super::functions::default_scalar_kernel(&name))
        {
            scalar_clause()?;
            typing::resolve_scalar_call(id, function, values, &mut self.context.ids)?
        } else if let Some(function) =
            super::aggregate_functions::aggregate_kernel(&name, self.context.config.ansi_mode)
                .or_else(|| self.aggregate_kernel(&name).ok())
        {
            let filter = if let Some(filter) = filter {
                Some(Box::new(self.expression(*filter, schema).await?.expression))
            } else {
                None
            };
            let mut resolved_order = vec![];
            for sort in order_by.unwrap_or_default() {
                resolved_order.push(self.sort(sort, schema).await?);
            }
            self.spark_aggregate(
                &name,
                function,
                values,
                super::aggregate_functions::AggregateOptions {
                    window: false,
                    distinct: is_distinct,
                    ignore_nulls,
                    filter,
                    order_by: resolved_order,
                },
            )?
        } else {
            return Err(PlanError::unsupported(format!(
                "native function resolution: {name}"
            )));
        };
        Ok(NamedExpression {
            name: display_name,
            expression,
        })
    }
}

fn recoverable_extraction(request: &spec::Expr) -> bool {
    match request {
        spec::Expr::UnresolvedAttribute { .. } | spec::Expr::UpdateFields { .. } => true,
        spec::Expr::Alias { expr, .. } | spec::Expr::UnresolvedExtractValue { child: expr, .. } => {
            recoverable_extraction(expr)
        }
        spec::Expr::UnresolvedFunction(function) => {
            matches!(function.function_name.parts(), [name] if name.as_ref().eq_ignore_ascii_case("case"))
        }
        _ => false,
    }
}
