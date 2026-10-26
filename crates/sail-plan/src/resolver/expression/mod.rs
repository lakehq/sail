use std::sync::Arc;

use arrow_schema::DataType;
use async_recursion::async_recursion;
use datafusion_expr::Operator;
use sail_common::spec;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::session::plan::PlanService;
use sail_common_datafusion::utils::items::ItemTaker;

use crate::analyzer::deferred::Deferred;
use crate::analyzer::model::*;
use crate::analyzer::typing;
use crate::error::{PlanError, PlanResult};
use crate::resolver::data_type::DataTypeResolver;
use crate::resolver::{NamedExpr, PlanResolver};

impl PlanResolver<'_> {
    pub(in crate::resolver) fn types(&self) -> DataTypeResolver<'_> {
        DataTypeResolver {
            config: &self.context.config,
            allow_large_var_types: false,
        }
    }

    #[async_recursion]
    pub(in crate::resolver) async fn resolve_named_expression(
        &mut self,
        request: spec::Expr,
        schema: &QuerySchema,
    ) -> PlanResult<NamedExpr> {
        if let Some((_, value)) = self
            .expression_replacements
            .iter()
            .find(|(original, _)| original == &request)
        {
            let name = value.name.clone();
            let expression = value.expression.clone();
            return Ok(NamedExpr {
                name,
                expression: self
                    .context
                    .function_context()
                    .copy_expression(&expression)?,
            });
        }
        let id = self.context.ids.expression();
        if matches!(request, spec::Expr::DefaultColumnValue) {
            return Ok(NamedExpr {
                name: "DEFAULT".into(),
                expression: Expr {
                    id,
                    kind: Deferred::unresolved(request),
                    result_type: Deferred::Resolved(ExprType::new(DataType::Null, true)),
                },
            });
        }
        let distinct_test = matches!(&request, spec::Expr::IsDistinctFrom { .. });
        let (name, node, result_type) = match request {
            spec::Expr::CommonInlineUserDefinedFunction(function) => {
                return self.inline_python(id, function, schema).await;
            }
            spec::Expr::SortOrder(sort) => {
                return self.resolve_named_expression(*sort.child, schema).await;
            }
            spec::Expr::UnresolvedRegex { col_name, plan_id } => {
                return self
                    .expand_regex(&col_name, plan_id, schema)?
                    .one()
                    .map_err(Into::into);
            }
            spec::Expr::UnresolvedDate { value } => {
                let literal = self.types().date_literal(&value)?;
                return self
                    .resolve_named_expression(spec::Expr::Literal(literal), schema)
                    .await;
            }
            spec::Expr::UnresolvedTime { value } => {
                let literal = self.types().time_literal(&value)?;
                return self
                    .resolve_named_expression(spec::Expr::Literal(literal), schema)
                    .await;
            }
            spec::Expr::UnresolvedTimestamp {
                value,
                timestamp_type,
            } => {
                let literal = self.types().timestamp_literal(&value, timestamp_type)?;
                return self
                    .resolve_named_expression(spec::Expr::Literal(literal), schema)
                    .await;
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
                let mut result_type = ExprType::new(value.data_type(), value.is_null());
                if let Some(metadata) = metadata {
                    result_type.metadata.insert(
                        spec::SAIL_SPARK_INTERVAL_METADATA_KEY.into(),
                        metadata.to_json()?,
                    );
                }
                (name, ExprKind::Literal(value), result_type)
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
                            .resolve_named_expression(
                                crate::resolver::command::write_input::parse_expression(&format!(
                                    "{name}()"
                                ))?,
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
                let inner = self.resolve_named_expression(*expr, schema).await?;
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
                    ExprKind::WithMetadata(Box::new(inner.expression)),
                    result_type,
                )
            }

            spec::Expr::Cast {
                expr,
                cast_to_type,
                rename: _,
                is_try,
            } => {
                let inner = self.resolve_named_expression(*expr, schema).await?;
                return self.explicit_cast(inner, cast_to_type, is_try);
            }
            spec::Expr::UpdateFields {
                struct_expression,
                field_name,
                value_expression,
            } => {
                let source = self
                    .resolve_named_expression(*struct_expression, schema)
                    .await?;
                let fields: Vec<String> = field_name.into();
                let (operation, function, arguments) = if let Some(value) = value_expression {
                    let value = self.resolve_named_expression(*value, schema).await?;
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
                return Ok(NamedExpr {
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
                let child = self.resolve_named_expression(*child, schema).await?;
                let extraction = self.resolve_named_expression(*extraction, schema).await?;
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
                return self.resolve_function(id, function, schema).await;
            }
            spec::Expr::CallFunction {
                function_name,
                arguments,
            } => {
                return self
                    .resolve_function(
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
                let value = self.resolve_named_expression(*expr, schema).await?;
                let low = self.resolve_named_expression(*low, schema).await?;
                let high = self.resolve_named_expression(*high, schema).await?;
                let value_name = value.name;
                let low_name = low.name;
                let high_name = high.name;
                let (value_expression, low_expression) = crate::resolver::coerce_timestamp_pair(
                    &mut self.context.function_context(),
                    value.expression,
                    Operator::GtEq,
                    low.expression,
                )?;
                let (value_expression, high_expression) = crate::resolver::coerce_timestamp_pair(
                    &mut self.context.function_context(),
                    value_expression,
                    Operator::LtEq,
                    high.expression,
                )?;
                let value = NamedExpr {
                    name: value_name,
                    expression: value_expression,
                };
                let low = NamedExpr {
                    name: low_name,
                    expression: low_expression,
                };
                let high = NamedExpr {
                    name: high_name,
                    expression: high_expression,
                };
                let fields = typing::argument_fields(&[
                    value.expression.clone(),
                    low.expression.clone(),
                    high.expression.clone(),
                ])?;
                let common = crate::analyzer::typing::coercion::comparison_type(
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
                    ExprKind::Between {
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
                    ExprType::new(DataType::Boolean, nullable),
                )
            }
            spec::Expr::IsDistinctFrom { left, right }
            | spec::Expr::IsNotDistinctFrom { left, right } => {
                let left = self.resolve_named_expression(*left, schema).await?;
                let right = self.resolve_named_expression(*right, schema).await?;
                let operator = if distinct_test {
                    Operator::IsDistinctFrom
                } else {
                    Operator::IsNotDistinctFrom
                };
                return Ok(NamedExpr {
                    name: format!("({} {} {})", left.name, operator, right.name),
                    expression: self.context.function_context().binary(
                        left.expression,
                        operator,
                        right.expression,
                    )?,
                });
            }
            spec::Expr::SimilarTo {
                expr,
                pattern,
                negated,
                escape_char,
                case_insensitive,
            } => {
                let value = self.resolve_named_expression(*expr, schema).await?;
                let pattern = self.resolve_named_expression(*pattern, schema).await?;
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
                    ExprKind::SimilarTo {
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
                    ExprType::new(DataType::Boolean, nullable),
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
                let value = self.resolve_named_expression(*expr, schema).await?;
                let query = self.resolve_expression_subquery(*subquery, schema).await?;
                let [attribute] = query.schema.resolved("IN subquery")?.attributes.as_slice()
                else {
                    return Err(PlanError::analysis("IN subquery must return one column"));
                };
                let right_name = attribute.name.clone();
                let right = self.reference(attribute)?;
                let (left, right) = crate::resolver::in_arguments(
                    &mut self.context.function_context(),
                    vec![value.expression, right],
                )?
                .two()?;
                let left_field = left.result_type.resolved("IN left")?;
                let right_field = right.result_type.resolved("IN right")?;
                let nullable = left_field.nullable || right_field.nullable;
                let common = crate::analyzer::typing::coercion::comparison_type(&[
                    left_field.data_type.clone(),
                    right_field.data_type.clone(),
                ])?;
                let left = self.context.function_context().cast(left, common.clone())?;
                let right = self.context.function_context().cast(right, common)?;
                let query = self.project(
                    query,
                    vec![NamedExpr {
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
                    ExprKind::InSubquery {
                        expression: Box::new(left),
                        query: Box::new(query),
                        negated,
                    },
                    ExprType::new(DataType::Boolean, nullable),
                )
            }
            spec::Expr::InList {
                expr,
                list,
                negated,
            } => {
                let value = self.resolve_named_expression(*expr, schema).await?;
                let mut values = vec![];
                let mut names = vec![];
                for item in list {
                    let item = self.resolve_named_expression(item, schema).await?;
                    names.push(item.name);
                    values.push(item.expression);
                }
                return Ok(NamedExpr {
                    name: format!(
                        "({} {}IN ({}))",
                        value.name,
                        if negated { "NOT " } else { "" },
                        names.join(", ")
                    ),
                    expression: crate::resolver::in_list(
                        &mut self.context.function_context(),
                        value.expression,
                        values,
                        negated,
                    )?,
                });
            }
            spec::Expr::IsNull(expr) => {
                let inner = self.resolve_named_expression(*expr, schema).await?;
                (
                    format!("({} IS NULL)", inner.name),
                    ExprKind::IsNull(Box::new(inner.expression)),
                    ExprType::new(DataType::Boolean, false),
                )
            }
            spec::Expr::IsNotNull(expr) => {
                let inner = self.resolve_named_expression(*expr, schema).await?;
                (
                    format!("({} IS NOT NULL)", inner.name),
                    ExprKind::IsNotNull(Box::new(inner.expression)),
                    ExprType::new(DataType::Boolean, false),
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
                let query = self.resolve_expression_subquery(*subquery, schema).await?;
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
                    ExprKind::ScalarSubquery(Box::new(query)),
                    result_type,
                )
            }
            spec::Expr::Exists { subquery, negated } => {
                let query = self.resolve_expression_subquery(*subquery, schema).await?;
                (
                    "exists()".into(),
                    ExprKind::Exists {
                        query: Box::new(query),
                        negated,
                    },
                    ExprType::new(DataType::Boolean, false),
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
                let result_type = ExprType::new(value.data_type(), value.is_null());
                (name, ExprKind::Literal(value), result_type)
            }
            expression => {
                return Err(PlanError::unsupported(format!(
                    "native expression resolution: {expression:?}"
                )));
            }
        };
        Ok(NamedExpr {
            name,
            expression: Expr {
                id,
                kind: Deferred::Resolved(node),
                result_type: Deferred::Resolved(result_type),
            },
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

pub(in crate::resolver) mod attribute;
pub(in crate::resolver) mod cast;
pub(in crate::resolver) mod function;
pub(in crate::resolver) mod grouping;
pub(in crate::resolver) mod lambda;
pub(in crate::resolver) mod predicate;
pub(in crate::resolver) mod sort;
pub(in crate::resolver) mod subquery;
pub(in crate::resolver) mod udf;
pub(in crate::resolver) mod wildcard;
pub(in crate::resolver) mod window;
