use std::sync::Arc;

use arrow_schema::DataType;
use datafusion::functions::{core, string};
use datafusion::functions_nested;
use datafusion_common::ScalarValue;
use datafusion_expr::ScalarUDF;

use crate::analyzer::deferred::Deferred;
use crate::analyzer::model::*;
use crate::analyzer::scope::ResolutionScope;
use crate::analyzer::typing;
use crate::error::{PlanError, PlanResult};
use crate::resolver::{NamedExpr, PlanResolver};

impl PlanResolver<'_> {
    pub(in crate::resolver) fn validate_source_binding(
        &self,
        parts: &[String],
        plan_id: Option<i64>,
    ) -> PlanResult<()> {
        if let Some(schemas) = plan_id.and_then(|id| self.state.bindings.get(&id)) {
            for schema in schemas {
                // A DataFrame reference binds at its source, before projections
                // remove one of the ambiguous candidates.
                for length in (1..=parts.len()).rev() {
                    if ResolutionScope::new(schema, [])
                        .with_case_sensitive(self.context.config.case_sensitive)
                        .resolve_optional(&parts[..length], None)?
                        .is_some()
                    {
                        break;
                    }
                }
            }
        }
        Ok(())
    }

    pub(in crate::resolver) fn resolve_attribute(
        &mut self,
        id: ExprId,
        parts: &[String],
        plan_id: Option<i64>,
        schema: &QuerySchema,
    ) -> PlanResult<NamedExpr> {
        self.validate_source_binding(parts, plan_id)?;
        if let Some(result) = self.recovery_attribute(id, parts, plan_id, schema) {
            return result;
        }
        for length in (1..=parts.len()).rev() {
            let Some(bound) = ResolutionScope::new(schema, [])
                .with_case_sensitive(self.context.config.case_sensitive)
                .resolve_optional(&parts[..length], plan_id)?
            else {
                continue;
            };
            let source = if bound.depth == 0 {
                schema
            } else {
                &self.outer[self.outer.len() - bound.depth]
            };
            let mut value = NamedExpr {
                name: source.attribute(bound.id)?.name.clone(),
                expression: Expr {
                    id: if length == parts.len() {
                        id
                    } else {
                        self.state.ids.expression()
                    },
                    kind: Deferred::Resolved(if bound.depth == 0 {
                        ExprKind::Attribute(bound.id)
                    } else {
                        ExprKind::OuterAttribute {
                            depth: bound.depth,
                            attribute: bound.id,
                        }
                    }),
                    result_type: Deferred::Resolved(bound.result_type),
                },
            };
            for (i, field) in parts[length..].iter().enumerate() {
                let extraction = NamedExpr {
                    name: field.clone(),
                    expression: Expr::literal(
                        self.state.ids.expression(),
                        ScalarValue::Utf8(Some(field.clone())),
                    ),
                };
                let selected_id = if i + length + 1 == parts.len() {
                    id
                } else {
                    self.state.ids.expression()
                };
                value = self.extract_value(selected_id, value, extraction)?;
                value.name.clone_from(field);
            }
            return Ok(value);
        }
        if let Some(value) = self.resolve_outer_attribute(id, parts, plan_id)? {
            return Ok(value);
        }
        if plan_id.is_none()
            && let Some((name, fields)) = parts.split_first()
        {
            let mut matches = self
                .expression_aliases
                .iter()
                .filter(|value| self.name_key(&value.name) == self.name_key(name));
            if let Some(value) = matches.next() {
                if matches.any(|other| {
                    !other
                        .expression
                        .equivalent(&value.expression)
                        .unwrap_or(false)
                }) {
                    return Err(PlanError::analysis(format!("ambiguous attribute: {name}")));
                }
                let expression = value.expression.clone();
                let mut value = NamedExpr {
                    name: name.clone(),
                    expression: self
                        .context
                        .function_context(&mut self.state.ids)
                        .copy_expression(&expression)?,
                };
                for field in fields {
                    let extraction = NamedExpr {
                        name: field.clone(),
                        expression: self
                            .context
                            .function_context(&mut self.state.ids)
                            .literal(ScalarValue::Utf8(Some(field.clone()))),
                    };
                    let id = self.state.ids.expression();
                    value = self.extract_value(id, value, extraction)?;
                }
                return Ok(value);
            }
        }
        Err(PlanError::analysis(format!(
            "unresolved attribute: {} (cannot resolve attribute)",
            parts.join(".")
        )))
    }

    fn resolve_outer_attribute(
        &mut self,
        id: ExprId,
        parts: &[String],
        plan_id: Option<i64>,
    ) -> PlanResult<Option<NamedExpr>> {
        let outer = std::mem::take(&mut self.outer);
        let aliases = std::mem::take(&mut self.expression_aliases);
        let current = self.recovery.take();
        let mut result = Ok(None);
        for index in (0..outer.len()).rev() {
            self.recovery = self.outer_recovery.remove(&index);
            let candidate = self.resolve_attribute(id, parts, plan_id, &outer[index]);
            if let Some(scope) = self.recovery.take() {
                self.outer_recovery.insert(index, scope);
            }
            match candidate {
                Ok(mut value) => {
                    value.expression =
                        value
                            .expression
                            .transform_up::<PlanError>(&mut |mut value| {
                                if let Deferred::Resolved(ExprKind::Attribute(attribute)) =
                                    value.kind
                                {
                                    value.kind = Deferred::Resolved(ExprKind::OuterAttribute {
                                        depth: outer.len() - index,
                                        attribute,
                                    });
                                }
                                Ok(value)
                            })?;
                    result = Ok(Some(value));
                    break;
                }
                Err(PlanError::AnalysisError(message))
                    if message.starts_with("unresolved attribute:") => {}
                Err(error) => {
                    result = Err(error);
                    break;
                }
            }
        }
        self.recovery = current;
        self.expression_aliases = aliases;
        self.outer = outer;
        result
    }

    pub(in crate::resolver) fn extract_value(
        &mut self,
        id: ExprId,
        child: NamedExpr,
        mut extraction: NamedExpr,
    ) -> PlanResult<NamedExpr> {
        let data_type = &child
            .expression
            .result_type
            .resolved("extraction input")?
            .data_type;
        let mut name = format!("{}[{}]", child.name, extraction.name);
        let function = match data_type {
            DataType::Struct(fields) => {
                name.clone_from(&extraction.name);
                let ExprKind::Literal(value) =
                    extraction.expression.kind.resolved("struct selector")?
                else {
                    return Err(PlanError::analysis("extraction must be a literal string"));
                };
                let Some(requested) = value.try_as_str().flatten() else {
                    return Err(PlanError::analysis(
                        "struct selector must be a non-null string literal",
                    ));
                };
                let mut matches = fields
                    .iter()
                    .filter(|field| self.name_key(field.name()) == self.name_key(requested));
                let field = matches.next().ok_or_else(|| {
                    PlanError::analysis(format!(
                        "[FIELD_NOT_FOUND] cannot resolve attribute: missing struct field: {requested}"
                    ))
                })?;
                if matches.next().is_some() {
                    return Err(PlanError::analysis(format!(
                        "ambiguous reference to the field: {requested}"
                    )));
                }
                extraction.expression.kind = Deferred::Resolved(ExprKind::Literal(
                    ScalarValue::Utf8(Some(field.name().clone())),
                ));
                core::get_field()
            }
            DataType::Map(_, _) => {
                let extracted = typing::resolve_scalar_call(
                    self.state.ids.expression(),
                    functions_nested::map_extract::map_extract_udf(),
                    vec![child.expression, extraction.expression],
                    &mut self.state.ids,
                )?;
                let first = Expr::literal(self.state.ids.expression(), ScalarValue::Int64(Some(1)));
                return Ok(NamedExpr {
                    name,
                    expression: typing::resolve_scalar_call(
                        id,
                        functions_nested::extract::array_element_udf(),
                        vec![extracted, first],
                        &mut self.state.ids,
                    )?,
                });
            }
            DataType::List(field)
            | DataType::LargeList(field)
            | DataType::FixedSizeList(field, _)
                if matches!(field.data_type(), DataType::Struct(_))
                    && matches!(
                        extraction.expression.kind,
                        Deferred::Resolved(ExprKind::Literal(ScalarValue::Utf8(_)))
                    ) =>
            {
                let DataType::Struct(fields) = field.data_type() else {
                    unreachable!()
                };
                let key = crate::analyzer::lowering::evaluate_constant(&extraction.expression)?;
                let requested = key
                    .try_as_str()
                    .flatten()
                    .ok_or_else(|| PlanError::analysis("extraction must be a literal string"))?;
                let mut matches = fields
                    .iter()
                    .filter(|field| self.name_key(field.name()) == self.name_key(requested));
                let field = matches.next().ok_or_else(|| {
                    PlanError::analysis(format!(
                        "[FIELD_NOT_FOUND] cannot resolve attribute: missing struct field: {requested}"
                    ))
                })?;
                if matches.next().is_some() {
                    return Err(PlanError::analysis(format!(
                        "ambiguous reference to the field: {requested}"
                    )));
                }
                extraction.expression = self
                    .context
                    .function_context(&mut self.state.ids)
                    .literal(ScalarValue::Utf8(Some(field.name().clone())));
                Arc::new(ScalarUDF::from(
                    sail_function::scalar::array_struct_field::ArrayStructField::new(),
                ))
            }
            DataType::List(_)
            | DataType::LargeList(_)
            | DataType::FixedSizeList(_, _)
            | DataType::ListView(_)
            | DataType::LargeListView(_) => {
                let array = child.expression;
                let index = self
                    .context
                    .function_context(&mut self.state.ids)
                    .cast(extraction.expression, DataType::Int64)?;
                let index_copy = self
                    .context
                    .function_context(&mut self.state.ids)
                    .copy_expression(&index)?;
                let one = self
                    .context
                    .function_context(&mut self.state.ids)
                    .literal(ScalarValue::Int64(Some(1)));
                let position = self.context.function_context(&mut self.state.ids).binary(
                    index_copy,
                    datafusion_expr::Operator::Plus,
                    one,
                )?;
                let array_copy = self
                    .context
                    .function_context(&mut self.state.ids)
                    .copy_expression(&array)?;
                let element = self
                    .context
                    .function_context(&mut self.state.ids)
                    .scalar_udf(
                        functions_nested::extract::array_element_udf(),
                        vec![array_copy, position],
                    )?;
                let expression = if self.context.config.ansi_mode {
                    let array_copy = self
                        .context
                        .function_context(&mut self.state.ids)
                        .copy_expression(&array)?;
                    let length = self
                        .context
                        .function_context(&mut self.state.ids)
                        .scalar_udf(
                            functions_nested::length::array_length_udf(),
                            vec![array_copy],
                        )?;
                    let length = self
                        .context
                        .function_context(&mut self.state.ids)
                        .cast(length, DataType::Int64)?;
                    let index_copy = self
                        .context
                        .function_context(&mut self.state.ids)
                        .copy_expression(&index)?;
                    let length_copy = self
                        .context
                        .function_context(&mut self.state.ids)
                        .copy_expression(&length)?;
                    let high = self.context.function_context(&mut self.state.ids).binary(
                        index_copy,
                        datafusion_expr::Operator::GtEq,
                        length_copy,
                    )?;
                    let index_copy = self
                        .context
                        .function_context(&mut self.state.ids)
                        .copy_expression(&index)?;
                    let zero = self
                        .context
                        .function_context(&mut self.state.ids)
                        .literal(ScalarValue::Int64(Some(0)));
                    let negative = self.context.function_context(&mut self.state.ids).binary(
                        index_copy,
                        datafusion_expr::Operator::Lt,
                        zero,
                    )?;
                    let invalid = self.context.function_context(&mut self.state.ids).binary(
                        high,
                        datafusion_expr::Operator::Or,
                        negative,
                    )?;
                    let present = self
                        .context
                        .function_context(&mut self.state.ids)
                        .is_not_null(array);
                    let invalid = self.context.function_context(&mut self.state.ids).binary(
                        present,
                        datafusion_expr::Operator::And,
                        invalid,
                    )?;
                    let prefix = self.context.function_context(&mut self.state.ids).literal(
                        ScalarValue::Utf8(Some("[INVALID_ARRAY_INDEX] The index ".into())),
                    );
                    let index = self
                        .context
                        .function_context(&mut self.state.ids)
                        .cast(index, DataType::Utf8)?;
                    let middle = self.context.function_context(&mut self.state.ids).literal(
                        ScalarValue::Utf8(Some(" is out of bounds. The array has ".into())),
                    );
                    let length = self
                        .context
                        .function_context(&mut self.state.ids)
                        .cast(length, DataType::Utf8)?;
                    let suffix = self.context.function_context(&mut self.state.ids).literal(ScalarValue::Utf8(Some(" elements. Use the SQL function `get()` to tolerate accessing element at invalid index and return NULL instead.".into())));
                    let message = self
                        .context
                        .function_context(&mut self.state.ids)
                        .scalar_udf(
                            string::concat(),
                            vec![prefix, index, middle, length, suffix],
                        )?;
                    let error = self
                        .context
                        .function_context(&mut self.state.ids)
                        .scalar_udf(
                            Arc::new(ScalarUDF::from(
                                sail_function::scalar::misc::raise_error::RaiseError::new(),
                            )),
                            vec![message],
                        )?;
                    let error = self.context.function_context(&mut self.state.ids).cast(
                        error,
                        element
                            .result_type
                            .resolved("array element")?
                            .data_type
                            .clone(),
                    )?;
                    self.context
                        .function_context(&mut self.state.ids)
                        .case(vec![(invalid, error)], Some(element))?
                } else {
                    element
                };
                return Ok(NamedExpr { name, expression });
            }
            _ => {
                return Err(PlanError::analysis(format!(
                    "cannot extract a field from {data_type}"
                )));
            }
        };
        Ok(NamedExpr {
            name,
            expression: typing::resolve_scalar_call(
                id,
                function,
                vec![child.expression, extraction.expression],
                &mut self.state.ids,
            )?,
        })
    }
}
